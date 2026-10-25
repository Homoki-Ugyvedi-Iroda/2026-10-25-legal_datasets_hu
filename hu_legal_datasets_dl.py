#!/usr/bin/env python3
"""
hu_legal_dl.py - polite, resumable downloader for publicly available Hungarian legal documents.

One file, nine sources. Every source is used the same way:

    python hu_legal_dl.py list                      # sources and what each one needs
    python hu_legal_dl.py index    <source>         # list what exists -> local SQLite index (metadata only)
    python hu_legal_dl.py download <source>         # fetch the next batch of documents, slowly, then exit
    python hu_legal_dl.py status   [<source>]       # progress
    python hu_legal_dl.py export   <source>         # index + local file paths -> CSV

Sources (sizes are rough orders of magnitude seen in autumn 2026)
-----------------------------------------------------------------
    bhgy          court decisions, eakta.birosag.hu (Birosagi Hatarozatok Gyujtemenye)   ~220k decisions
    ab            Constitutional Court decisions, 1990-now                               ~13k records, ~1 GB
    gvh           Competition Authority (GVH) decisions, court decisions, sector reports ~5k records, ~2 GB
    naih          Data-protection authority (NAIH) decisions and guidance                ~500 PDFs, ~150 MB
    nkfh          Consumer-protection authority (NKFH) decisions                         ~4.7k PDFs, ~2.5 GB
    nmhh          Media and infocommunications authority (4 sections)                    ~22k items, ~2.4 GB
    sztnh         Copyright / industrial-property expert-body opinions (e-Kutatas)       ~830 PDFs, ~140 MB
    domain        .hu domain-dispute decisions and opinions (domain.hu)                  ~740 files, ~40 MB
    hirdetmenyek  land sale / lease contracts (hirdetmenyek.gov.hu)                      rolling window, GBs

Install
-------
    pip install requests beautifulsoup4            # all sources
    pip install playwright && playwright install chromium     # only: nkfh (index), nmhh (hirkszolg), sztnh (index)

Typical use
-----------
    python hu_legal_dl.py index naih --contact you@example.org
    python hu_legal_dl.py download naih --contact you@example.org --allowed-hours 20-7
    python hu_legal_dl.py status

Everything lands under --data-dir (default ./data): <data-dir>/<source>/index.sqlite, files/, run.lock.
Index and download are separate steps and both are resumable: stop a run at any time and start it again.
Re-run `index` regularly to pick up new documents (new items are added as "pending"; nothing is downloaded twice).

Common options (all commands)
-----------------------------
    --contact       your e-mail, added to the User-Agent so site operators can reach you (recommended)
    --allowed-hours e.g. 20-7: work only between 20:00 and 07:00 local time; the run ends cleanly when the window closes
    --delay         seconds between requests (default depends on the source; +-jitter, doubles after errors)
    --years / --sections   restrict what is indexed / downloaded (sections mean different things per source:
                    gvh/naih/domain/nmhh: the site's sections; sztnh: szjszt,iszt; hirdetmenyek: notice types)

Politeness: one request at a time, jittered delay, exponential back-off on 429 / 5xx / network errors (honours
Retry-After), stop after --max-errors failures in a row, a lock file against overlapping runs, a descriptive
User-Agent. Nothing here logs in, solves captchas or tries to get around access restrictions.

Legal note: the documents are public, but many contain personal data (names of natural persons, land-parcel
data). Check each site's terms, data-protection rules and re-use conditions before storing long-term or
republishing. This project is not affiliated with any of the sites.
"""

import argparse
import csv
import datetime as dt
import hashlib
import html as htmlmod
import json
import logging
import os
import random
import re
import signal
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlparse

log = logging.getLogger("hu_legal_dl")


# =========================================================================== infrastructure
class Stop(Exception):
    """Clean end of a run (outside allowed hours, time limit, too many errors)."""
    code = 0


class TooManyErrors(Stop):
    code = 2


class Gone(Exception):
    """The document no longer exists on the site (not an error)."""


class _Wait(Exception):
    def __init__(self, msg, wait):
        super().__init__(msg)
        self.wait = wait


def slug(s, maxlen=80):
    s = re.sub(r"[\\/:*?\"<>|\s]+", "_", (s or "").strip())
    return s.strip("._")[:maxlen] or "unknown"


def now():
    return dt.datetime.now().isoformat(timespec="seconds")


def parse_years(spec):
    """'2020-2024,2018' -> {2018, 2020, ..., 2024}; empty -> None (= all)."""
    if not spec:
        return None
    years = set()
    for part in spec.split(","):
        if "-" in part:
            a, b = (int(x) for x in part.split("-"))
            years.update(range(min(a, b), max(a, b) + 1))
        else:
            years.add(int(part))
    return years


def parse_set(spec):
    return {x.strip() for x in spec.split(",") if x.strip()} if spec else None


def find_year(*texts, last=False):
    for t in texts:
        ys = re.findall(r"(?<!\d)(19[89]\d|20[0-3]\d)(?!\d)", t or "")
        if ys:
            return int(ys[-1] if last else ys[0])
    return None


def ysort(year, key):
    return f"{year or 0:04d}|{key}"


def in_allowed_hours(spec):
    """spec like '20-7' (overnight) or '9-17'; empty = always."""
    if not spec:
        return True
    a, b = (int(x) for x in spec.split("-"))
    h = dt.datetime.now().hour
    return (a <= h < b) if a < b else (h >= a or h < b)


def user_agent(args):
    ua = "Mozilla/5.0 (compatible; hu-legal-dl/1.0; research archive"
    if args.contact:
        ua += f"; contact: {args.contact}"
    return ua + ")"


def soup_of(text):
    try:
        from bs4 import BeautifulSoup
    except ImportError:
        sys.exit("This source needs BeautifulSoup:  pip install beautifulsoup4")
    try:
        return BeautifulSoup(text, "lxml")
    except Exception:  # noqa: BLE001  (lxml not installed)
        return BeautifulSoup(text, "html.parser")


def need_playwright():
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        sys.exit("This step needs Playwright:  pip install playwright && playwright install chromium")
    return sync_playwright


class Lock:
    """Prevents overlapping runs. A lock file untouched for 30 minutes counts as stale."""
    STALE_AFTER = 30 * 60

    def __init__(self, path):
        self.path = Path(path)

    def __enter__(self):
        if self.path.exists():
            age = time.time() - self.path.stat().st_mtime
            if age < self.STALE_AFTER:
                sys.exit(f"Another run seems active (lock file {self.path} touched {age / 60:.0f} min ago).")
            log.info("Removing stale lock file from an earlier interrupted run.")
        self.path.write_text(str(os.getpid()))
        return self

    def touch(self):
        try:
            self.path.touch()
        except OSError:
            pass

    def __exit__(self, *a):
        self.path.unlink(missing_ok=True)


class Client:
    """requests session with polite retry / exponential back-off."""

    def __init__(self, args):
        import requests
        self.max_errors = args.max_errors
        self.s = requests.Session()
        self.s.headers["User-Agent"] = user_agent(args)
        self.consecutive_errors = 0
        self.error_events = 0       # lets the pacer notice that a retry just happened

    def request(self, method, url, **kw):
        backoff = 30
        for attempt in range(1, 6):
            try:
                r = self.s.request(method, url, timeout=120, **kw)
                if r.status_code == 429 or r.status_code >= 500:
                    ra = r.headers.get("Retry-After", "")
                    raise _Wait(f"HTTP {r.status_code}", int(ra) if ra.isdigit() else backoff)
                r.raise_for_status()
                self.consecutive_errors = 0
                return r
            except Exception as e:  # noqa: BLE001
                if getattr(getattr(e, "response", None), "status_code", None) in (403, 404, 410):
                    raise       # permanent for this URL: do not hammer
                wait = e.wait if isinstance(e, _Wait) else backoff
                self.consecutive_errors += 1
                self.error_events += 1
                log.warning("%s %s failed (%s), attempt %d - waiting %ds", method, url[-70:], e, attempt, wait)
                if self.consecutive_errors >= self.max_errors:
                    raise TooManyErrors("Too many consecutive errors - stopping this run; try again later.")
                time.sleep(wait + random.uniform(0, 5))
                backoff = min(backoff * 2, 900)
        raise RuntimeError(f"Giving up on {url}")

    def get(self, url, **kw):
        return self.request("GET", url, **kw)


SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    key        TEXT PRIMARY KEY,          -- the source's own id (or URL)
    section    TEXT,                      -- sub-collection of the source
    year       INTEGER,
    title      TEXT,
    url        TEXT,
    meta       TEXT DEFAULT '{}',         -- JSON: source-specific details
    sort       TEXT,                      -- download order
    status     TEXT DEFAULT 'pending',    -- pending | done | failed | gone
    attempts   INTEGER DEFAULT 0,
    error      TEXT,
    saved      TEXT,                      -- JSON list of saved files, relative to files/
    bytes      INTEGER,
    first_seen TEXT,
    downloaded_at TEXT
);
CREATE INDEX IF NOT EXISTS ix_status ON items(status);
CREATE TABLE IF NOT EXISTS progress (      -- what the index step has already walked (resume points)
    key TEXT PRIMARY KEY, status TEXT, value TEXT, updated TEXT
);
"""


class DB:
    def __init__(self, path):
        self.c = sqlite3.connect(path)
        self.c.row_factory = sqlite3.Row
        self.c.executescript(SCHEMA)

    def add(self, key, section, year, title, url, meta=None, sort=None):
        cur = self.c.execute(
            "INSERT OR IGNORE INTO items (key,section,year,title,url,meta,sort,first_seen) VALUES (?,?,?,?,?,?,?,?)",
            (key, section, year, title, url, json.dumps(meta or {}, ensure_ascii=False),
             sort if sort is not None else ysort(year, key), now()))
        return cur.rowcount

    def progress(self, key):
        r = self.c.execute("SELECT status,value FROM progress WHERE key=?", (key,)).fetchone()
        return (r["status"], json.loads(r["value"] or "{}")) if r else (None, {})

    def set_progress(self, key, status, **value):
        self.c.execute("REPLACE INTO progress VALUES (?,?,?,?)", (key, status, json.dumps(value), now()))

    def commit(self):
        self.c.commit()


class Ctx:
    """Everything a source needs during one run."""

    def __init__(self, args, src):
        self.args, self.src = args, src
        self.dir = Path(args.data_dir) / src.name
        self.out = self.dir / "files"
        self.out.mkdir(parents=True, exist_ok=True)
        self.db = DB(self.dir / "index.sqlite")
        self.client = Client(args)
        self.lock = None
        self.deadline = None
        self.state = {}
        self.base_delay = args.delay if args.delay is not None else src.delay
        self.cur_delay = self.base_delay
        self._errs = 0

    def check(self):
        """Call regularly: keeps the lock fresh and ends the run when the time window closes."""
        if self.lock:
            self.lock.touch()
        if not in_allowed_hours(self.args.allowed_hours):
            raise Stop("Outside allowed hours - stopping; run again later to continue.")
        if self.deadline and time.time() > self.deadline:
            raise Stop("Time limit reached - stopping.")

    def pause(self):
        """Polite pause between requests: doubles after errors (max 60 s), creeps back to the base delay."""
        errs = self.client.error_events
        if errs > self._errs:
            self.cur_delay = min(self.cur_delay * 2, 60)
        else:
            self.cur_delay = max(self.cur_delay * 0.95, self.base_delay)
        self._errs = errs
        time.sleep(self.cur_delay * random.uniform(0.9, 1.4))

    def save(self, rel, data):
        """Write bytes to files/<rel> via a .part file; returns the relative path as stored in the index."""
        path = self.out / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".part")
        tmp.write_bytes(data)
        tmp.replace(path)
        return Path(rel).as_posix()


class Source:
    name = help = ""
    delay = 2.0             # default seconds between requests
    newest_first = True     # download order

    def index(self, ctx):
        raise NotImplementedError

    def fetch(self, ctx, item):
        """Download one item. Returns (saved_paths, total_bytes[, extra_meta]); raise on failure, Gone if removed."""
        raise NotImplementedError


# =========================================================================== bhgy: court decisions (eakta)
class Bhgy(Source):
    name = "bhgy"
    help = "court decisions, eakta.birosag.hu (search API, split per year x court, 10,000-hit cap per query)"
    delay = 4.0

    BASE = "https://eakta.birosag.hu"
    CAP = 10000
    PAGE_SIZE = 100
    DEFAULTS = {
        "ResultCount": "100", "ResultStartIndex": "0", "ResultSortExpression": "Id asc",
        "KeresoSzo": "", "KeresoSzoOperator": "Osszes", "Rezume": "", "RezumeOperator": "Osszes",
        "JogTerulet": "", "MeghozoBirosag": "", "ErintettKozigazgatsaiSzerv": "",
        "Azonosito": "", "HatarozatFajta": "", "MeghozatalIdejeTol": "", "MeghozatalIdejeIg": "",
        "Ugycsoport": "", "Ugytargy": "", "BefejezesMod": "", "ItelkezesiGyakorlat": "",
        "EUJoganyag": "", "EJEBJoganyag": "", "Kollegium": "", "HatarozatTipus": "", "EgyediAzonosito": "",
        "Rendezes": "BirosagiSzint", "HierarchikusPertargy": "", "NemHivatkozhato": "nem",
        "NemHivatkozhatoSzoveg": "",
    }
    EXT = {"application/rtf": ".rtf", "text/rtf": ".rtf", "application/pdf": ".pdf",
           "application/msword": ".doc",
           "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx"}

    def index(self, ctx):
        c, db, args = ctx.client, ctx.db, ctx.args
        page = c.get(self.BASE + "/anonimizalt-hatarozatok").text

        def options(select_id):
            m = re.search(r'<select[^>]*id="%s"[^>]*>(.*?)</select>' % select_id, page, re.S)
            vals = [htmlmod.unescape(v) for v in re.findall(r'<option[^>]*value="([^"]*)"', m.group(1))] if m else []
            return [v for v in vals if v]

        courts, groups = options("Birosag"), options("Ugycsoport")
        log.info("%d courts, %d case groups found on the search page", len(courts), len(groups))

        def search(**filters):
            r = c.request("POST", self.BASE + "/AnonimizaltHatarozat/Search?Area=", data={**self.DEFAULTS, **filters},
                          headers={"X-Requested-With": "XMLHttpRequest",
                                   "Accept": "application/json, text/javascript, */*; q=0.01"})
            j = r.json()
            if not j.get("Success"):
                raise RuntimeError(f"Search failed: {j.get('Message')}")
            return j

        def label(f):
            return " / ".join(v for k, v in f.items() if v and k in ("MeghozatalIdejeTol", "MeghozoBirosag", "Ugycsoport"))

        def partition(filters, depth):
            key = json.dumps(filters, ensure_ascii=False, sort_keys=True)
            status, val = db.progress(key)
            if status == "done" and not args.refresh:
                return
            ctx.check()
            total = search(**filters, ResultCount="1")["Count"]
            ctx.pause()
            if total == 0:
                db.set_progress(key, "done", total=0)
                db.commit()
                return
            children = []
            if total >= self.CAP:       # too many hits for one query: split by court, then by case group
                if depth == 0:
                    children = [{**filters, "MeghozoBirosag": b} for b in courts]
                elif depth == 1:
                    children = [{**filters, "Ugycsoport": g} for g in groups]
            if children:
                for ch in children:
                    partition(ch, depth + 1)
                db.set_progress(key, "split", total=total)
                db.commit()
                return
            if total >= self.CAP:
                log.warning("Partition still capped at %d, only the first %d retrievable: %s",
                            total, self.CAP, label(filters))
            start = 0 if (args.refresh or status is None) else val.get("next", 0)
            limit = min(total, self.CAP)
            while start < limit:
                ctx.check()
                items = search(**filters, ResultStartIndex=str(start)).get("List") or []
                new = 0
                for it in items:
                    year = int(it.get("HatarozatEve") or 0) or None
                    court = it.get("MeghozoBirosag") or ""
                    new += db.add(it["IndexId"], court, year, it.get("Azonosito"), "",
                                  {"kollegium": it.get("Kollegium"), "jogterulet": it.get("JogTerulet"),
                                   "egyedi_azonosito": it.get("EgyediAzonosito")},
                                  sort=f"{year or 0:04d}|{court}|{it.get('Azonosito')}")
                start += self.PAGE_SIZE
                db.set_progress(key, "open", total=total, next=start)
                db.commit()
                log.info("%s: %d/%d (+%d new)", label(filters), min(start, limit), total, new)
                if not items:
                    break
                ctx.pause()
            db.set_progress(key, "done", total=total)
            db.commit()
            if "MeghozoBirosag" in filters and "Ugycsoport" not in filters and total < self.CAP:
                have = db.c.execute("SELECT COUNT(*) FROM items WHERE year=? AND section=?",
                                    (int(filters["MeghozatalIdejeTol"]), filters["MeghozoBirosag"])).fetchone()[0]
                if have < total:
                    log.warning("%s: site reports %d but %d indexed - run `index --refresh` for this year later.",
                                label(filters), total, have)

        years = parse_years(args.years) or set(range(1990, dt.date.today().year + 1))
        for y in sorted(years, reverse=True):
            partition({"MeghozatalIdejeTol": str(y), "MeghozatalIdejeIg": str(y)}, 0)

    def fetch(self, ctx, it):
        r = ctx.client.get(self.BASE + "/AnonimizaltHatarozat/DownloadHatarozatMobile",
                           params={"Area": "", "birosagName": it["section"], "ugyszam": it["title"],
                                   "azonosito": it["key"]})
        ct = r.headers.get("Content-Type", "").lower()
        if "html" in ct or "json" in ct or len(r.content) < 200:
            raise RuntimeError(f"unexpected response ({ct}, {len(r.content)} bytes)")
        cd = r.headers.get("Content-Disposition", "")
        m = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)', cd)
        if m and "." in m.group(1):
            ext = os.path.splitext(m.group(1).strip())[1].lower()
        else:
            ext = self.EXT.get(ct.split(";")[0].strip(), ".bin")
        rel = (Path(str(it["year"] or "unknown_year")) / slug(it["section"])
               / f"{slug(it['title'])}__{slug(it['key'], 40)}{ext}")
        return [ctx.save(rel, r.content)], len(r.content)


# =========================================================================== ab: Constitutional Court
class Ab(Source):
    name = "ab"
    help = "Constitutional Court decisions 1990-now (JSON API per year; case record HTML + decision PDFs)"
    delay = 1.5

    API = "https://api.alkotmanybirosag.hu/Talalat"
    PARAMS = ("hatarozat_sorszam", "hatarozat_evszam", "ugyszam_sorszam", "ugyszam_evszam", "dontes_szerv",
              "befejezo_dontes_tartalma", "rendelkezo_resz", "indoklas", "velemenyek", "alkotmanybiro",
              "ugyszaki_jelleg", "lezaras_modja", "inditvanyozo_tipusa", "eljaras_tipusa", "ugyallapot",
              "alkotpanasz_ugyall", "jogszabaly", "lenyeg")

    @staticmethod
    def first(x):
        return (x[0] if isinstance(x, list) and x else x or "") or ""

    def api_url(self, year):
        q = "&".join(f"{k}={year if k == 'hatarozat_evszam' else ''}" for k in self.PARAMS)
        return f"{self.API}?OpenAgent=&{q}&feltetel1=2&feltetel2=2&befejezes_tipusa="

    def parse_item(self, it):
        f = self.first
        url = f(it.get("url"))
        m = re.search(r"/0/([0-9A-Fa-f]{32})", url)
        if not m:
            return None
        num = f(it.get("sor1")).strip()
        km = re.search(r"AB\s+(.+)$", num)
        d = re.search(r"(\d{4})\.(\d{2})\.(\d{2})", f(it.get("sor5")))
        return {"key": m.group(1).upper(), "url": url.replace("http://", "https://"), "number": num,
                "kind": km.group(1).strip() if km else "",
                "case_no": re.sub(r"\s*számú AB ügy$", "", f(it.get("sor2"))),
                "subject": f(it.get("sor3")).strip(), "decided": "-".join(d.groups()) if d else None}

    def index(self, ctx):
        c, db, args = ctx.client, ctx.db, ctx.args
        this_year = dt.date.today().year
        for y in sorted(parse_years(args.years) or set(range(1990, this_year + 1))):
            status, _ = db.progress(f"year:{y}")
            if status == "done" and y < this_year and not args.refresh:
                continue
            ctx.check()
            try:
                data = c.get(self.api_url(y)).json()
            except Stop:
                raise
            except Exception as e:  # noqa: BLE001
                log.error("Year %d: %s - skipped", y, e)
                continue
            new = 0
            for it in data:
                r = self.parse_item(it)
                if r:
                    new += db.add(r["key"], r["kind"] or "other", y, r["number"], r["url"],
                                  {"case_no": r["case_no"], "subject": r["subject"], "decided": r["decided"]})
            db.set_progress(f"year:{y}", "done", items=len(data))
            db.commit()
            log.info("%d: %d decisions (+%d new)", y, len(data), new)
            ctx.pause()

    def fetch(self, ctx, it):
        c, mode = ctx.client, ctx.args.attachments
        r = c.get(it["url"])
        if not r.encoding or r.encoding.lower() == "iso-8859-1":
            r.encoding = r.apparent_encoding
        files = []
        for a in soup_of(r.text).find_all("a", href=True):
            href = htmlmod.unescape(a["href"])
            if "/_FILE/" in href and all(h != urljoin(it["url"], href) for h, _ in files):
                files.append((urljoin(it["url"], href), unquote(href.rsplit("/", 1)[-1])))
        stem = f"{slug(it['title'], 60)}__{it['key'][:8]}"
        folder = Path(str(it["year"]))
        html_bytes = (f"<!-- source: {it['url']} ; saved {now()} -->\n" + r.text).encode("utf-8")
        saved, total = [ctx.save(folder / f"{stem}.html", html_bytes)], len(html_bytes)
        if mode != "none":
            for href, name in files:
                n = name.lower()
                # "decision" mode: only the decision PDF itself, not petitions / supplements
                if mode == "decision" and (not n.endswith(".pdf") or re.search(r"indít|indit|indkieg|anonim", n)):
                    continue
                ctx.pause()
                pr = c.get(href)
                if n.endswith(".pdf") and not pr.content.startswith(b"%PDF"):
                    raise RuntimeError(f"not a PDF: {name}")
                saved.append(ctx.save(folder / f"{stem}__{slug(name, 80)}", pr.content))
                total += len(pr.content)
        return saved, total


# =========================================================================== domain: .hu domain disputes
class Domain(Source):
    name = "domain"
    help = "domain.hu decisions and opinions (static page + info.domain.hu tables)"

    MAIN = "https://www.domain.hu/dontesek-es-allasfoglalasok/"
    INFO = "https://info.domain.hu"
    SECTIONS = [
        {"key": "dd", "folder": "dd_dontesek", "h4": "Domain döntnöki eljárások döntései"},
        {"key": "rd", "folder": "rd_dontesek", "h4": "Regisztrációs döntnöki eljárások döntései"},
        {"key": "jtb", "folder": "jtb_allasfoglalasok", "h4": "A Jogi Tanácsadó Bizottság"},
        {"key": "tt_elvi", "folder": "tt_elvi", "list": INFO + "/tt/elvi_allasfoglalasok"},
        {"key": "tt_eseti", "folder": "tt_eseti", "list": INFO + "/tt/eseti_allasfoglalasok"},
    ]
    NUM_RE = re.compile(r"^\s*(\d+/\d{4}\.?\s*\([^)]*\))")

    def parse_main(self, page_html):
        """-> [(section_key, url, title)] from the domain.hu page (headings split the sections)."""
        soup = soup_of(page_html)
        root = soup.find("main") or soup
        cur, out, seen = None, [], set()
        for el in root.find_all(["h4", "a"]):
            if el.name == "h4":
                txt = " ".join(el.get_text().split())
                cur = next((s["key"] for s in self.SECTIONS if s.get("h4") and txt.startswith(s["h4"])), None)
                continue
            href = el.get("href") or ""
            if cur is None or "/app/uploads/" not in href or not href.lower().split("?")[0].endswith(".pdf"):
                continue
            url = urljoin(self.MAIN, href)
            if url not in seen:
                seen.add(url)
                out.append((cur, url, " ".join(el.get_text().split())))
        return out

    def parse_tt(self, page_html, list_url):
        soup = soup_of(page_html)
        root = soup.find("main") or soup
        out, seen = [], set()
        for tr in root.find_all("tr"):
            a = tr.find("a", href=True)
            if not a:
                continue
            url = urljoin(list_url, a["href"])
            if url in seen:
                continue
            seen.add(url)
            tds = tr.find_all("td")
            out.append((" ".join(tds[0].get_text().split()) if len(tds) > 1 else "", url,
                        " ".join(a.get_text().split())))
        return out

    def index(self, ctx):
        c, db = ctx.client, ctx.db
        for sec, url, title in self.parse_main(c.get(self.MAIN).text):
            m = self.NUM_RE.match(title)
            y = re.search(r"/uploads/(\d{4})/", url)
            db.add(url, sec, int(y.group(1)) if y else find_year(title), title, url,
                   {"number": m.group(1) if m else ""})
        db.commit()
        for sec in self.SECTIONS:
            if "list" not in sec:
                continue
            ctx.check()
            ctx.pause()
            for number, url, title in self.parse_tt(c.get(sec["list"]).text, sec["list"]):
                db.add(url, sec["key"], find_year(number, url), title, url, {"number": number})
            db.commit()
        for sec in self.SECTIONS:
            n = db.c.execute("SELECT COUNT(*) FROM items WHERE section=?", (sec["key"],)).fetchone()[0]
            log.info("%s: %d documents", sec["folder"], n)

    def fetch(self, ctx, it):
        r = ctx.client.get(it["url"])
        is_pdf = it["url"].lower().endswith(".pdf")
        if is_pdf and not r.content.startswith(b"%PDF"):
            raise RuntimeError(f"not a PDF ({r.headers.get('Content-Type')})")
        folder = next(s["folder"] for s in self.SECTIONS if s["key"] == it["section"])
        base = unquote(os.path.basename(urlparse(it["url"]).path))
        number = it["meta"].get("number", "")
        label = slug(number or it["title"], 60) if is_pdf else slug(number, 40)
        name = f"{label}__{slug(base, 90)}" if label != "unknown" else slug(base, 120)
        # raw bytes: the old info.domain.hu pages are windows-1250 and keep their charset declaration
        return [ctx.save(Path(folder) / str(it["year"] or "unknown") / name, r.content)], len(r.content)


# =========================================================================== hirdetmenyek: land contracts
class Hirdetmenyek(Source):
    name = "hirdetmenyek"
    help = ("land sale / lease contract notices, hirdetmenyek.gov.hu (JSON API). The register keeps notices only "
            "for a limited time: run regularly. Scans have no text layer.")
    delay = 1.0
    newest_first = False        # oldest first: those are the next to disappear

    BASE = "https://hirdetmenyek.gov.hu"

    def index(self, ctx):
        c, db, args = ctx.client, ctx.db, ctx.args
        wanted = parse_set(args.sections) or {"foldelovasarlasos"}
        types = None if "all" in wanted else wanted
        page, seen, new = 0, 0, 0
        size = 1000
        while True:
            ctx.check()
            q = ("order=asc&targy=&kategoria=&forrasIntezmenyNeve=&ugyiratSzamIktatasiSzam=&telepules=&nev=&idoszak="
                 f"&adottNap=&szo=&pageIndex={page}&pageSize={size}&sort=kifuggesztesNapja")
            j = c.get(f"{self.BASE}/api/hirdetmenyek?{q}").json()
            rows = j.get("rows", [])
            for r in rows:
                seen += 1
                posted = (r.get("kifuggesztesNapja") or "")[:10]
                tipus = r.get("hirdetmenyTipusNev")
                if (types and tipus not in types) or posted < args.min_date:
                    continue
                new += db.add(str(r["id"]), tipus, int(posted[:4]) if posted[:4].isdigit() else None,
                              r.get("targy"), f"{self.BASE}/api/hirdetmenyek/reszletezo/{r['id']}",
                              {"posted": posted, "source": (r.get("forrasIntezmenyNeve") or "").strip(),
                               "file_no": r.get("ugyiratszam"), "reg_no": r.get("iktatasiszam")},
                              sort=f"{posted}|{int(r['id']):012d}")
            db.commit()
            if len(rows) < size or seen >= j.get("total", 0):
                break
            page += 1
            ctx.pause()
        log.info("Index: %d notices seen, %d new", seen, new)

    @staticmethod
    def first_attr(attrs, name):
        for k in (name + "1", name):
            if attrs.get(k):
                return str(attrs[k])
        return None

    def fetch(self, ctx, it):
        c = ctx.client
        try:
            j = c.get(it["url"]).json()
        except Exception as e:  # noqa: BLE001
            if getattr(getattr(e, "response", None), "status_code", None) in (404, 410):
                raise Gone("removed from the register")
            raise
        d, attrs = j.get("hirdetmenyDTO") or {}, j.get("attributumok") or {}
        posted = it["meta"].get("posted", "")
        folder = Path(it["section"] or "unknown") / (posted[:7] or "unknown")
        nid = it["key"]
        saved = [ctx.save(folder / f"{nid}__details.json", json.dumps(j, ensure_ascii=False, indent=1).encode("utf-8"))]
        total = 0
        for att in j.get("csatolmanyok") or []:
            ctx.pause()
            pr = c.get(f"{self.BASE}/api/csatolmany/{att['id']}")
            name = slug(unquote(att.get("fajlNev") or "attachment"), 100)
            saved.append(ctx.save(folder / f"{nid}__{att['id']}_{name}", pr.content))
            total += len(pr.content)
        fa = self.first_attr
        return saved, total, {
            "subtype": j.get("altipus"), "deadline": (d.get("lejaratNapja") or "")[:10],
            "municipality": fa(attrs, "telepules"), "parcel": fa(attrs, "helyrajzi_szam"),
            "area": fa(attrs, "terulet"), "land_use": fa(attrs, "muvelesi_ag"),
            "price": fa(attrs, "haszonber") or fa(attrs, "vetelar")}


# =========================================================================== gvh: competition authority
class Gvh(Source):
    name = "gvh"
    help = "GVH decisions (1997-now), court decisions (Vj), final sector reports; HTML pages + PDFs"

    BASE = "https://www.gvh.hu"
    D = "/dontesek/"
    ATTACHMENT_TYPES = {
        "application/msword": ".doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx"}

    def gvh_base(self, y):
        d = self.D + "versenyhivatali_dontesek/"
        return d + (f"dontesek-{y}" if y >= 2021 else f"dontesek_{y}" if y >= 2014 else f"archiv/dontesek_{y}")

    def court_base(self, y):
        d = self.D + "birosagi_dontesek/"
        if y == 2023:
            return d + "birosagi-dontesek_2023"
        return d + (f"birosagi-dontesek-{y}" if y >= 2019 else f"birosagi_dontesek_{y}" if y >= 2011
                    else f"archiv/birosagi_dontesek_{y}")

    def sections(self):
        return [
            {"key": "versenyhivatali", "folder": "versenyhivatali_dontesek", "title_re": None,
             "parts": [(y, self.gvh_base(y)) for y in range(dt.date.today().year, 1996, -1)]},
            {"key": "birosagi", "folder": "birosagi_dontesek", "title_re": None,
             "parts": [(y, self.court_base(y)) for y in range(2023, 1995, -1)]},
            {"key": "agazati", "folder": "agazati_vegleges_jelentesek", "title_re": re.compile(r"^Végleges jelentés"),
             "parts": [(None, self.D + "agazati_vizsgalatok_piacelemzesek/agazati_vizsgalatok")]},
        ]

    @staticmethod
    def list_page_items(soup, base, title_re=None):
        """-> ([(href, text)], next_href) from one list page."""
        root = soup.find("main") or soup
        items, nxt = {}, None
        for a in root.find_all("a", href=True):
            href = htmlmod.unescape(a["href"])
            txt = " ".join(a.get_text().split())
            if "pageNumber" in href:
                if txt.startswith("Következő"):
                    nxt = href
                continue
            if "acpid" in href or not txt:      # language-switch links
                continue
            if href.startswith(base + "/") and href not in items and (not title_re or title_re.search(txt)):
                items[href] = txt
        return list(items.items()), nxt

    def index(self, ctx):
        c, db, args = ctx.client, ctx.db, ctx.args
        years, wanted, this_year = parse_years(args.years), parse_set(args.sections), dt.date.today().year
        for sec in self.sections():
            if wanted and sec["key"] not in wanted:
                continue
            for y, base in sec["parts"]:
                if years is not None and y is not None and y not in years:
                    continue
                key = f"{sec['key']}:{y}"
                status, _ = db.progress(key)
                if status == "done" and y is not None and y < this_year and not args.refresh:
                    continue
                url, pages, found, new, ok = self.BASE + base, 0, 0, 0, True
                while url and pages < 100:
                    ctx.check()
                    try:
                        r = c.get(url)
                    except Stop:
                        raise
                    except Exception as e:  # noqa: BLE001
                        log.error("%s: %s - skipping", key, e)
                        ok = False
                        break
                    items, nxt = self.list_page_items(soup_of(r.text), base, sec["title_re"])
                    for href, txt in items:
                        new += db.add(href, sec["key"], y, txt, href)
                    found += len(items)
                    pages += 1
                    db.commit()
                    url = urljoin(url, nxt) if nxt else None
                    ctx.pause()
                if ok:
                    db.set_progress(key, "done", pages=pages, items=found)
                    db.commit()
                    log.info("%s: %d pages, %d items (+%d new)", key, pages, found, new)

    def extract_detail(self, page_html, section):
        """-> (content_html, decision_date, [file hrefs]). Site-wide files (cookie notice etc.) are ignored."""
        root = soup_of(page_html)
        root = root.find("main") or root
        cols = [c for c in root.select("div.column") if "sidebar" not in " ".join(c.get("class", []))]
        content = max(cols, key=lambda c: len(c.get_text())) if cols else root
        if section == "agazati":        # the detail page repeats the whole list table
            for t in content.find_all("table"):
                t.decompose()
        m = re.search(r"(\d{4})\.\s*(\d{2})\.\s*(\d{2})", content.get_text(" "))
        files, seen = [], set()
        for a in root.find_all("a", href=True):
            href = htmlmod.unescape(a["href"])
            path = parse_qs(urlparse(href).query).get("path", [""])[0]
            if ("pfile" in href and path.startswith(self.D)) or re.search(r"/dontesek/.*\.pdf(\?|$)", href, re.I):
                k = (path or href).lower()
                if k not in seen:
                    seen.add(k)
                    files.append(href)
        return str(content), ("-".join(m.groups()) if m else None), files

    def fetch(self, ctx, it):
        c = ctx.client
        url, section, year = it["url"], it["section"], it["year"]
        r = c.get(urljoin(self.BASE, url))
        content, date, files = self.extract_detail(r.text, section)
        folder = Path(next(s["folder"] for s in self.sections() if s["key"] == section))
        if year:
            folder = folder / str(year)
        stem = slug(it["title"], 70) + "__" + hashlib.sha1(url.encode()).hexdigest()[:6]
        body = (f"<!-- source: {self.BASE}{url} ; saved {now()} -->\n" + content).encode("utf-8")
        saved, total = [ctx.save(folder / f"{stem}.html", body)], len(body)
        for i, href in enumerate(files):
            ctx.pause()
            pr = c.get(urljoin(self.BASE + url, href))
            if not pr.content:      # the server answers 200 with an empty body for some broken links
                log.warning("Empty file on server, skipped: %s", it["title"])
                continue
            p = parse_qs(urlparse(href).query).get("path", [urlparse(href).path])[0]
            fname = re.sub(r"\.pdf1?$", "", os.path.basename(p), flags=re.I) + ".pdf"
            if not pr.content.startswith(b"%PDF"):      # Word / Excel attachments are kept under their real type
                ext = self.ATTACHMENT_TYPES.get(pr.headers.get("Content-Type", "").split(";")[0].strip().lower())
                if not ext or pr.content.lstrip()[:1] == b"<":
                    raise RuntimeError(f"not a PDF: {href[-60:]} ({pr.headers.get('Content-Type')})")
                fname = re.sub(r"\.pdf$", "", fname) + ext
            saved.append(ctx.save(folder / f"{stem}__{i + 1}_{slug(fname, 50)}", pr.content))
            total += len(pr.content)
        return saved, total, {"decision_date": date}


# =========================================================================== naih: data-protection authority
class Naih(Source):
    name = "naih"
    help = "NAIH decisions, data-protection and freedom-of-information guidance (Joomla/Phoca download lists)"

    BASE = "https://naih.hu"
    SECTIONS = {
        "hatarozatok_vegzesek": "/hatarozatok-vegzesek",
        "adatvedelmi_allasfoglalasok": "/adatvedelmi-allasfoglalasok",
        "adatvedelmi_tajekoztatok_kozlemenyek": "/dontesek-adatvedelem-tajekoztatok-koezlemenyek",
        "infoszab_allasfoglalasok": "/dontesek-infoszab-allasfoglalasok",
    }
    MONTHS = {"január": 1, "február": 2, "március": 3, "április": 4, "május": 5, "június": 6, "július": 7,
              "augusztus": 8, "szeptember": 9, "október": 10, "november": 11, "december": 12}

    def parse_date(self, text):
        m = re.search(r"(\d{4})\.\s*([a-záéíóöőúüű]+)\s+(\d{1,2})", (text or "").lower())
        if not m or m.group(2) not in self.MONTHS:
            return None
        return f"{int(m.group(1)):04d}-{self.MONTHS[m.group(2)]:02d}-{int(m.group(3)):02d}"

    def parse_list(self, page_html):
        out, seen = [], set()
        for box in soup_of(page_html).select("div.pd-filebox"):
            a = box.select_one(".pd-filename a[href]") or box.find("a", href=re.compile(r"download="))
            if not a:
                continue
            href = htmlmod.unescape(a["href"])
            m = re.search(r"download=(\d+)(?::([^&]*))?", href)
            if not m or m.group(1) in seen:
                continue
            seen.add(m.group(1))
            d = box.select_one(".pd-fl-m")
            out.append({"id": m.group(1), "slug": m.group(2) or "", "title": " ".join(a.get_text().split()),
                        "date": self.parse_date(d.get_text() if d else ""), "href": href})
        return out

    def index(self, ctx):
        c, db = ctx.client, ctx.db
        wanted = parse_set(ctx.args.sections)
        for key, path in self.SECTIONS.items():
            if wanted and key not in wanted:
                continue
            ctx.check()
            items = self.parse_list(c.get(f"{self.BASE}{path}?limit=0").text)     # whole list in one request
            if not items:
                log.warning("%s: no items found - has the page layout changed?", key)
            new = sum(db.add(it["id"], key, int(it["date"][:4]) if it["date"] else None, it["title"], it["href"],
                             {"date": it["date"], "slug": it["slug"]}) for it in items)
            db.commit()
            log.info("%s: %d items (+%d new)", key, len(items), new)
            ctx.pause()

    def fetch(self, ctx, it):
        r = ctx.client.get(urljoin(self.BASE, it["url"]))
        if not r.content.startswith(b"%PDF"):
            raise RuntimeError(f"not a PDF ({r.headers.get('Content-Type')}, {len(r.content)} bytes)")
        cd = r.headers.get("Content-Disposition", "")
        m = re.search(r"filename\*\s*=\s*(?:UTF-8|utf-8)''([^;]+)", cd)
        fname = unquote(m.group(1)) if m else None
        if not fname:
            m = re.search(r'filename\s*=\s*"?([^";]+)"?', cd)
            fname = m.group(1).strip() if m else slug(it["meta"].get("slug") or it["title"], 80) + ".pdf"
        rel = Path(it["section"]) / (str(it["year"]) if it["year"] else "no_date") / f"{it['key']}__{slug(fname, 120)}"
        return [ctx.save(rel, r.content)], len(r.content)


# =========================================================================== nkfh: consumer protection (browser index)
class Nkfh(Source):
    name = "nkfh"
    help = ("consumer-protection decisions, nkfh.gov.hu. The index step needs Playwright: the list page loads its data "
            "from an API that only answers the page's own front end, so the index is read through the page.")
    delay = 3.0

    BASE = "https://nkfh.gov.hu"
    LIST_URL = BASE + "/hatarozatok"

    # Click the pager's "next" arrow.
    JS_NEXT = r"""() => {
      const one = [...document.querySelectorAll('button')].find(b => /^\d+$/.test(b.innerText.trim()));
      if (!one) return 'nopager';
      const nav = one.closest('ul').parentElement;
      const sv = [...nav.querySelectorAll('button')].filter(b => b.querySelector('svg'));
      const nx = sv[sv.length - 1];
      if (!nx || nx.disabled) return 'last';
      nx.click(); return 'clicked';
    }"""
    # Page 1 arrives inside the server-rendered Nuxt payload.
    JS_SSR = r"""() => {
      const d = (window.__NUXT__ || {}).data || {};
      for (const k of Object.keys(d)) { const v = d[k]; if (v && Array.isArray(v.data) && v.meta) return v; }
      return null;
    }"""
    # Full-archive mode: the page's own front end sends request headers to its JSON API (the API answers 403 without
    # them). A small hook records the headers the page itself uses; further list requests are then issued from inside
    # the page with the same headers. They are never read into Python, printed or written to disk. The date filter
    # that limits the web page to the last ~180 days is left out. Use --window-only to see only what the page shows.
    JS_HOOK = r"""() => {
      if (window.__hooked) return true;
      const of = window.fetch; window.__hdrs = null;
      window.fetch = function(...a) {
        try { const u = (typeof a[0] === 'string' ? a[0] : a[0].url) || '';
              if (u.includes('/api/decisions') && a[1] && a[1].headers) window.__hdrs = a[1].headers; } catch (e) {}
        return of.apply(this, a); };
      window.__of = of; window.__hooked = true; return true; }"""
    JS_CLICK2 = r"""() => { const b = [...document.querySelectorAll('button')].find(b => b.innerText.trim() === '2');
                           if (b) b.click(); return !!b; }"""
    JS_API_PAGE = r"""async ({start, limit}) => {
      const url = 'https://api.nkfh.gov.hu/api/decisions?locale=hu-HU&populate=decisionFile&sort=id%3Aasc'
                + '&pagination%5Blimit%5D=' + limit + '&pagination%5Bstart%5D=' + start;
      const r = await window.__of(url, {headers: window.__hdrs});
      if (r.status !== 200) return {status: r.status};
      const j = await r.json(); return {status: 200, data: j.data, total: j.meta.pagination.total}; }"""

    def store(self, db, items):
        new = 0
        for it in items:
            f = it.get("decisionFile") or {}
            if not f.get("url"):
                continue
            pub = it.get("publishedAt") or it.get("createdAt") or ""
            key = it.get("documentId") or str(it.get("id"))
            new += db.add(key, "decisions", int(pub[:4]) if pub[:4].isdigit() else None, it.get("caseId"), f.get("url"),
                          {"authority_ref": it.get("authorityRefId"), "authority": it.get("authority"),
                           "participant": it.get("participantName"), "effective_date": it.get("effectiveDate"),
                           "legal_basis": it.get("legalBasis"), "published_at": pub, "size_kb": f.get("size")},
                          sort=f"{pub}|{key}")
        db.commit()
        return new

    def api_index(self, page, ctx):
        page.evaluate(self.JS_HOOK)
        page.evaluate(self.JS_CLICK2)
        for _ in range(40):
            if page.evaluate("() => !!window.__hdrs"):
                break
            time.sleep(0.25)
        else:
            log.warning("Could not observe the page's own API request headers.")
            return False
        start, total, new = 0, None, 0
        while total is None or start < total:
            ctx.check()
            res = page.evaluate(self.JS_API_PAGE, {"start": start, "limit": 100})
            if res.get("status") != 200:
                log.warning("API answered HTTP %s at offset %d", res.get("status"), start)
                return start > 0
            total = res["total"]
            new += self.store(ctx.db, res["data"])
            start += 100
            log.info("API offset %d/%d (+%d new so far)", min(start, total), total, new)
            ctx.pause()
        log.info("Full archive indexed: site reports %d decisions.", total)
        return True

    def index(self, ctx):
        sync_playwright = need_playwright()
        captured = []

        def on_response(resp):
            if "/api/decisions" in resp.url and resp.request.method == "GET":
                try:
                    captured.append(resp.json())
                except Exception:  # noqa: BLE001
                    pass

        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=not ctx.args.show_browser)
            page = browser.new_context(user_agent=user_agent(ctx.args), locale="hu-HU").new_page()
            page.on("response", on_response)
            page.goto(self.LIST_URL, wait_until="networkidle", timeout=120_000)
            ctx.check()
            if not ctx.args.window_only:
                if self.api_index(page, ctx):
                    browser.close()
                    return
                log.warning("Full-archive mode failed - falling back to the page's own list.")
                page.goto(self.LIST_URL, wait_until="networkidle", timeout=120_000)
            # window mode: page 1 from the server-rendered payload, then click through the pager
            ssr = page.evaluate(self.JS_SSR)
            if ssr:
                self.store(ctx.db, ssr["data"])
                log.info("Page 1: %d of %d decisions", len(ssr["data"]), ssr["meta"]["pagination"]["total"])
            first_link = "() => document.querySelector('a[href*=\"cdn.nkfh.gov.hu\"]')?.href"
            seen_first, stuck, pages = page.evaluate(first_link), 0, 1
            while True:
                ctx.check()
                n_before = len(captured)
                if page.evaluate(self.JS_NEXT) in ("last", "nopager"):
                    break
                for _ in range(60):
                    time.sleep(0.25)
                    if page.evaluate(first_link) != seen_first:
                        break
                else:
                    stuck += 1
                    if stuck >= 3:
                        break
                    continue
                seen_first = page.evaluate(first_link)
                time.sleep(0.5)
                pages += 1
                if len(captured) > n_before:
                    log.info("Page %d: +%d new", pages, self.store(ctx.db, captured[-1].get("data") or []))
                else:
                    log.warning("No API payload captured for page %d", pages)
                ctx.pause()
            browser.close()

    def fetch(self, ctx, it):
        r = ctx.client.get(urljoin(self.BASE + "/", it["url"]))     # the API sometimes returns bare /uploads/... paths
        ct = r.headers.get("Content-Type", "").lower()
        is_docx = r.content.startswith(b"PK\x03\x04") and b"word/" in r.content     # Word files served as .pdf
        if (not r.content.startswith(b"%PDF") and not is_docx) or "html" in ct:
            raise RuntimeError(f"not a PDF ({ct}, {len(r.content)} bytes)")
        fname = os.path.basename(urlparse(it["url"]).path)
        if is_docx:
            fname = os.path.splitext(fname)[0] + ".docx"
        rel = (Path(str(it["year"] or "no_year")) / slug(it["meta"].get("authority"))
               / f"{slug(it['title'])}__{fname}")
        return [ctx.save(rel, r.content)], len(r.content)


# =========================================================================== nmhh: media / infocommunications authority
class Nmhh(Source):
    name = "nmhh"
    help = ("NMHH: market-definition decisions, e-advertising fines, Media Council decisions (nmhh.hu) and the "
            "hirkszolg register of providers' general terms (needs Playwright for the list)")

    BASE = "https://nmhh.hu"
    HK_BASE = "https://hirkszolg.nmhh.hu"
    HK_INDEX = HK_BASE + "/kulso/app/index.jsp?v=55"
    SECTIONS = [
        {"key": "piacmeghatarozas", "kind": "docs", "path": "/hirkozles/szabalyozas/piacmeghatarozasi-dontesek"},
        {"key": "elektronikus_hirdetes", "kind": "docs", "path": "/hatosagi-dontesek-elektronikus-hirdetes"},
        {"key": "mediatanacs", "kind": "articles", "path": "/mediatanacs/dontesek",
         "title_re": re.compile(r"Médiatanács", re.I)},
        {"key": "hirkszolg_aszf", "kind": "hirkszolg", "path": None},
    ]

    def parse_list(self, soup, kind, title_re=None):
        """-> [(key, href, title, row_text, [file hrefs])] from one nmhh.hu list page (div.pw-card results)."""
        out, seen = [], set()
        for card in soup.select(".pw-card"):
            if kind == "docs":
                links = [htmlmod.unescape(a["href"]) for a in card.select('a[href*="/dokumentum/"]')]
                if not links:
                    continue
                m = re.search(r"/dokumentum/(\d+)", links[0])
                key, href = (m.group(1) if m else links[0]), links[0]
            else:
                a = card.select_one('a[href*="/cikk/"]')
                if not a:
                    continue
                href, links = htmlmod.unescape(a["href"]), []
                if title_re and not title_re.search(" ".join(a.get_text().split())):
                    continue
                m = re.search(r"/cikk/(\d+)", href)
                key = m.group(1) if m else href
            if key in seen:
                continue
            seen.add(key)
            text = " ".join(card.get_text(" ").split())
            h = card.find(["h2", "h3", "h4", "a"])
            title = " ".join(h.get_text().split()) if h and h.get_text(strip=True) else text
            out.append((key, href, title[:300], text[:500], list(dict.fromkeys(links))))
        return out

    def parse_hirkszolg(self, page_html):
        """-> {file_id: {"href", "rows"}}: one file is often attached to many rows, so files are de-duplicated."""
        files = {}
        for a in soup_of(page_html).find_all("a", href=True):
            href = htmlmod.unescape(a["href"])
            m = re.search(r"download-normal\.jsp.*?file=(\d+)", href)
            if not m:
                continue
            tr = a.find_parent("tr")
            cells = [" ".join(td.get_text(" ").split()) for td in tr.find_all("td")] if tr else []
            rowtxt = " | ".join(x for x in cells if x)
            e = files.setdefault(m.group(1), {"href": href, "rows": []})
            if rowtxt and rowtxt not in e["rows"]:
                e["rows"].append(rowtxt)
        return files

    def hirkszolg_html(self, ctx):
        """Open the register in a real browser, press the list button, show all rows, return the HTML."""
        sync_playwright = need_playwright()
        with sync_playwright() as p:
            b = p.chromium.launch(headless=not ctx.args.show_browser)
            pg = b.new_context(user_agent=user_agent(ctx.args)).new_page()
            pg.goto(self.HK_INDEX, wait_until="networkidle", timeout=90000)
            pg.click("button[type=submit], input[type=submit]")
            pg.wait_for_selector("a[href*='download-normal']", timeout=90000)
            pg.evaluate(r"""() => { const el = document.querySelector('#rowPerPages');
                if (el && window.jQuery) { window.jQuery('#rowPerPages').val(999999).trigger('change'); }
                const opts = [...document.querySelectorAll('[name=rowPerPages],[name=rowsPerPage]')];
                if (opts.length) { const o = opts[opts.length - 1]; o.checked = true; o.click(); } }""")
            pg.wait_for_timeout(4000)
            html_ = pg.content()
            b.close()
        return html_

    def index(self, ctx):
        c, db, args = ctx.client, ctx.db, ctx.args
        wanted = parse_set(args.sections)
        for sec in self.SECTIONS:
            key = sec["key"]
            if wanted and key not in wanted:
                continue
            if db.progress(key)[0] == "done" and not args.refresh:
                log.info("%s: already indexed (use --refresh to walk it again)", key)
                continue
            if sec["kind"] == "hirkszolg":
                try:
                    files = self.parse_hirkszolg(self.hirkszolg_html(ctx))
                except Exception as e:  # noqa: BLE001
                    log.error("hirkszolg index failed (%s). It needs Playwright + Chromium.", e)
                    continue
                for fid, e in files.items():
                    db.add(f"{key}|{fid}", key, find_year(" ".join(e["rows"])[:400], last=True),
                           (e["rows"][0] if e["rows"] else fid)[:300], e["href"],
                           {"files": [e["href"]], "rows": " || ".join(e["rows"])[:2000]})
                db.set_progress(key, "done", items=len(files))
                db.commit()
                log.info("%s: %d unique files", key, len(files))
                continue
            page, found, new, ok = 1, 0, 0, True
            while page <= 2000:
                ctx.check()
                url = f"{self.BASE}{sec['path']}?HNDTYPE=SEARCH&name=doc&page={page}&fld_sort=score_insdt"
                try:
                    r = c.get(url)
                except Stop:
                    raise
                except Exception as e:  # noqa: BLE001
                    log.error("%s page %d: %s - stopping this section", key, page, e)
                    ok = False
                    break
                items = self.parse_list(soup_of(r.text), sec["kind"], sec.get("title_re"))
                fresh = 0
                for k, href, title, text, files in items:
                    fresh += db.add(f"{key}|{k}", key, find_year(title, text, last=True), title, href, {"files": files})
                new += fresh
                found += len(items)
                db.commit()
                if page % 25 == 0:
                    log.info("%s: page %d, %d items so far", key, page, found)
                if not items or (fresh == 0 and page > 1):     # empty, or past the end (the site repeats the last page)
                    break
                page += 1
                ctx.pause()
            if ok:
                db.set_progress(key, "done", pages=page, items=found)
                db.commit()
                log.info("%s: %d items (+%d new)", key, found, new)

    def fetch(self, ctx, it):
        c = ctx.client
        stem = it["key"].split("|", 1)[1]
        kind = next(s["kind"] for s in self.SECTIONS if s["key"] == it["section"])
        folder = Path(it["section"])
        if kind == "hirkszolg":
            if not ctx.state.get("hk_session"):     # visit the register once, like a browser, to get a session cookie
                c.get(self.HK_INDEX)
                ctx.state["hk_session"] = True
                ctx.pause()
            r = c.get(urljoin(self.HK_BASE + "/kulso/app/", it["meta"]["files"][0]))
            if "html" in r.headers.get("Content-Type", "").lower() and not r.content.startswith(b"%PDF"):
                raise RuntimeError("got HTML instead of a document (session needed?)")
            cd = r.headers.get("Content-Disposition", "")
            m = re.search(r"filename\*?=(?:UTF-8'')?\"?([^\";]+)", cd)
            name = slug(unquote(m.group(1)), 120) if m else f"{stem}.pdf"
            return [ctx.save(folder / f"{stem}__{name}", r.content)], len(r.content)
        hrefs, year, saved, total = it["meta"].get("files", []), it["year"], [], 0
        if kind == "articles":
            r = c.get(urljoin(self.BASE, it["url"]))
            root = soup_of(r.text)
            root = root.find("article") or root.find("main") or root
            if not year:
                m = re.search(r"(\d{4})\.\s*(\d{2})\.\s*(\d{2})", root.get_text(" "))
                year = int(m.group(1)) if m else None
            hrefs = list(dict.fromkeys(htmlmod.unescape(a["href"]) for a in root.find_all("a", href=True)
                                       if "/dokumentum/" in a["href"]))
            folder = folder / str(year or "unknown")
            body = (f"<!-- source: {self.BASE}{it['url']} ; saved {now()} -->\n" + str(root)).encode("utf-8")
            saved.append(ctx.save(folder / f"{stem}__{slug(it['title'], 60)}.html", body))
            total += len(body)
            ctx.pause()
        else:
            folder = folder / str(year or "unknown")
        for i, href in enumerate(hrefs):
            if i:
                ctx.pause()
            pr = c.get(urljoin(self.BASE, href))
            name = slug(unquote(os.path.basename(urlparse(href).path)) or "document", 120)
            saved.append(ctx.save(folder / f"{stem}__{name}", pr.content))
            total += len(pr.content)
        return saved, total, {"_year": year}


# =========================================================================== sztnh: expert-body opinions (browser index)
class Sztnh(Source):
    name = "sztnh"
    help = ("Copyright (szjszt) and industrial-property (iszt) expert-body opinions, epub.sztnh.gov.hu e-Kutatas "
            "(needs Playwright for the list; the PDFs are plain downloads). Use --sections szjszt or iszt.")

    BASE = "https://epub.sztnh.gov.hu"
    FORM = BASE + "/e-kutatas/?lang=HU"
    BODIES = {"szjszt": "Szerzői Jogi Szakértő Testület szakvéleményei",
              "iszt": "Iparjogvédelmi Szakértői Testület szakvéleményei"}

    JS_ROWS = r"""(body) => {
      const href = s => { const m = (s.getAttribute('onclick')||'').match(/openWindowUrl\('([^']+)'/); return m ? m[1] : ''; };
      const rows = [];
      for (const tr of document.querySelectorAll('tr')) {
        if (tr.cells.length < 5) continue;
        if (body === 'szjszt') {   // case no | title | field | requester | opinion | [link] | court decision | has one
          if (tr.cells.length < 6) continue;
          const op = tr.cells[4].querySelector('[onclick*=openWindowUrl]');
          if (!op) continue;
          const cc = tr.cells[6] ? tr.cells[6].querySelector('[onclick*=openWindowUrl]') : null;
          rows.push({ugyszam: tr.cells[0].innerText.trim(), title: tr.cells[1].innerText.trim(),
                     area: tr.cells[2].innerText.trim(), requester: tr.cells[3].innerText.trim(),
                     href: href(op), court_name: cc ? cc.innerText.trim() : '', court_href: cc ? href(cc) : '', year: ''});
        } else {                   // iszt: case no | subject | requester | year | opinion
          const op = tr.cells[4].querySelector('[onclick*=openWindowUrl]');
          if (!op) continue;
          rows.push({ugyszam: tr.cells[0].innerText.trim(), title: '', area: tr.cells[1].innerText.trim(),
                     requester: tr.cells[2].innerText.trim(), href: href(op), court_name: '', court_href: '',
                     year: tr.cells[3].innerText.trim()});
        }
      }
      const m = document.body.innerText.match(/Összes találat:\s*(\d+)/);
      return {rows, total: m ? parseInt(m[1], 10) : null, error: /Adatkapcsolat hiba/.test(document.body.innerText)};
    }"""

    def fetch_rows(self, ctx, body):
        """One browser session: empty search in the body's form -> (rows, total). Raises on failure."""
        sync_playwright = need_playwright()
        with sync_playwright() as p:
            b = p.chromium.launch(headless=not ctx.args.show_browser)
            try:
                pg = b.new_context(user_agent=user_agent(ctx.args)).new_page()
                pg.goto(self.FORM, wait_until="networkidle", timeout=90000)
                pg.get_by_text(self.BODIES[body], exact=True).first.click(timeout=30000)
                pg.wait_for_selector("[name=searchButtonLabel]", timeout=30000)
                pg.click("[name=searchButtonLabel]")
                pg.wait_for_function(r"() => /Összes találat:\s*\d+/.test(document.body.innerText) || "
                                     r"/Adatkapcsolat hiba/.test(document.body.innerText)", timeout=120000)
                pg.wait_for_timeout(2000)
                res = pg.evaluate(self.JS_ROWS, body)
            finally:
                b.close()
        if res.get("error") or not res.get("rows"):
            raise RuntimeError("e-Kutatas returned an error / no rows (the backend is flaky)")
        return res["rows"], res.get("total")

    def index(self, ctx):
        for body in (parse_set(ctx.args.sections) or set(self.BODIES)):
            wait = 60
            for attempt in range(1, 6):
                try:
                    rows, total = self.fetch_rows(ctx, body)
                    break
                except Exception as e:  # noqa: BLE001
                    log.warning("%s: index attempt %d/5 failed: %s", body, attempt, e)
                    if attempt == 5:
                        raise TooManyErrors(f"Giving up on {body}; try again later.")
                    time.sleep(wait)
                    wait = min(wait * 2, 900)
            new = 0
            for r in rows:
                year = int(r["year"]) if str(r.get("year", "")).isdigit() else find_year(r["ugyszam"])
                new += ctx.db.add(r["href"], body, year, r["ugyszam"], r["href"],
                                  {"subject": r["title"] or r["area"], "area": r["area"], "requester": r["requester"],
                                   "court_name": r["court_name"] or None, "court_href": r["court_href"] or None})
            ctx.db.commit()
            log.info("%s: %d rows read (site says %s), %d new", body, len(rows), total, new)
            if total and len(rows) != total:
                log.warning("%s: row count differs from the site's total (%d vs %d): some rows have an unusual layout; "
                            "rerun, or check them in the browser.", body, len(rows), total)

    def pdf(self, ctx, href):
        r = ctx.client.get(urljoin(self.BASE, href))
        if not r.content.startswith(b"%PDF"):
            raise RuntimeError(f"not a PDF: {href[-40:]} ({r.headers.get('Content-Type')})")
        return r.content

    def fetch(self, ctx, it):
        folder = Path(it["section"]) / str(it["year"] or "unknown")
        stem = slug(it["title"], 40)
        data = self.pdf(ctx, it["url"])
        saved, total = [ctx.save(folder / f"{stem}__opinion.pdf", data)], len(data)
        m = it["meta"]
        if m.get("court_href"):
            ctx.pause()
            data = self.pdf(ctx, m["court_href"])
            cn = re.sub(r"\.pdf$", "", m.get("court_name") or "court_decision", flags=re.I)
            saved.append(ctx.save(folder / f"{stem}__court_{slug(cn, 60)}.pdf", data))
            total += len(data)
        return saved, total


# =========================================================================== commands
SOURCES = {s.name: s for s in (Bhgy(), Ab(), Gvh(), Naih(), Nkfh(), Nmhh(), Sztnh(), Domain(), Hirdetmenyek())}


def cmd_list(args):
    for s in SOURCES.values():
        print(f"{s.name:13} {s.help}")


def cmd_index(args):
    src = SOURCES[args.source]
    ctx = Ctx(args, src)
    with Lock(ctx.dir / "run.lock") as ctx.lock:
        try:
            src.index(ctx)
        finally:
            ctx.db.commit()
    log.info("Index: %d items in %s", ctx.db.c.execute("SELECT COUNT(*) FROM items").fetchone()[0],
             ctx.dir / "index.sqlite")


def cmd_download(args):
    src = SOURCES[args.source]
    ctx = Ctx(args, src)
    ctx.deadline = time.time() + args.max_minutes * 60 if args.max_minutes else None
    sections, years = parse_set(args.sections), parse_years(args.years)
    q = "SELECT * FROM items WHERE (status='pending' OR (status='failed' AND attempts < ?))"
    params = [args.max_attempts]
    if sections:
        q += f" AND section IN ({','.join('?' * len(sections))})"
        params += sorted(sections)
    if years:
        q += f" AND year IN ({','.join('?' * len(years))})"
        params += sorted(years)
    q += f" ORDER BY sort {'DESC' if src.newest_first else 'ASC'} LIMIT ?"
    rows = ctx.db.c.execute(q, params + [args.batch]).fetchall()
    if not rows:
        log.info("Nothing left to download.")
        return
    log.info("Processing %d items (delay %.1fs + jitter)", len(rows), ctx.base_delay)
    ok = fail = gone = 0
    with Lock(ctx.dir / "run.lock") as ctx.lock:
        try:
            for n, row in enumerate(rows, 1):
                ctx.check()
                it = dict(row)
                it["meta"] = json.loads(it["meta"] or "{}")
                try:
                    res = src.fetch(ctx, it)
                    saved, total = res[0], res[1]
                    extra = dict(res[2]) if len(res) > 2 else {}
                    year = extra.pop("_year", None) or it["year"]
                    ctx.db.c.execute(
                        "UPDATE items SET status='done', year=?, saved=?, bytes=?, meta=?, downloaded_at=?, error=NULL, "
                        "attempts=attempts+1 WHERE key=?",
                        (year, json.dumps(saved), total, json.dumps({**it["meta"], **extra}, ensure_ascii=False),
                         now(), it["key"]))
                    ok += 1
                except Stop:
                    raise
                except Gone as e:
                    ctx.db.c.execute("UPDATE items SET status='gone', error=? WHERE key=?", (str(e), it["key"]))
                    gone += 1
                except Exception as e:  # noqa: BLE001
                    ctx.db.c.execute("UPDATE items SET status='failed', error=?, attempts=attempts+1 WHERE key=?",
                                     (str(e)[:500], it["key"]))
                    fail += 1
                    log.warning("Failed %s: %s", it["title"] or it["key"], e)
                ctx.db.commit()
                if n % 25 == 0:
                    log.info("%d/%d (ok %d, failed %d, gone %d)", n, len(rows), ok, fail, gone)
                ctx.pause()
        finally:
            ctx.db.commit()
            log.info("Run finished: %d done, %d failed, %d gone.", ok, fail, gone)
    cmd_status(args)


def cmd_status(args):
    names = [args.source] if getattr(args, "source", None) else sorted(SOURCES)
    for name in names:
        path = Path(args.data_dir) / name / "index.sqlite"
        if not path.exists():
            continue
        db = DB(path)
        stats = dict(db.c.execute("SELECT status, COUNT(*) FROM items GROUP BY status").fetchall())
        mb = db.c.execute("SELECT COALESCE(SUM(bytes),0) FROM items").fetchone()[0] / 1048576
        print(f"{name}: {sum(stats.values())} indexed | " + ", ".join(f"{k}: {v}" for k, v in sorted(stats.items()))
              + f" | {mb:.0f} MB downloaded")
        secs = db.c.execute("""SELECT section, COUNT(*), COALESCE(SUM(status='done'),0), COALESCE(SUM(bytes),0)
                               FROM items GROUP BY section ORDER BY section""").fetchall()
        for sec, n, d, b in (secs if 1 < len(secs) <= 12 else []):     # per-section lines only when useful
            print(f"    {sec}: {d}/{n} done, {b / 1048576:.0f} MB")


def cmd_export(args):
    db = DB(Path(args.data_dir) / args.source / "index.sqlite")
    cols = ["key", "section", "year", "title", "url", "status", "saved", "bytes", "downloaded_at", "meta"]
    with Path(args.csv or f"{args.source}_index.csv").open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(cols)
        w.writerows(db.c.execute(f"SELECT {','.join(cols)} FROM items ORDER BY sort"))
    print(f"Wrote {args.csv or args.source + '_index.csv'}")


def build_parser():
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--data-dir", default="data", help="where indexes and files are kept (default ./data)")
    common.add_argument("--contact", default="", help="your e-mail, added to the User-Agent (recommended)")
    common.add_argument("--allowed-hours", default="", help="e.g. 20-7 = only run 20:00-07:00 local time")
    common.add_argument("--delay", type=float, default=None, help="seconds between requests (default per source)")
    common.add_argument("--max-errors", type=int, default=5, help="stop after this many consecutive failed requests")
    select = argparse.ArgumentParser(add_help=False)
    select.add_argument("--years", default="", help="e.g. 2020-2026 or 2024,2025 (default: all)")
    select.add_argument("--sections", default="", help="comma list of the source's sections (see the docstring)")

    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("list", help="show the sources")

    pi = sub.add_parser("index", parents=[common, select], help="build / update the local index")
    pi.add_argument("source", choices=sorted(SOURCES))
    pi.add_argument("--refresh", action="store_true", help="walk already finished parts again to pick up new items")
    pi.add_argument("--min-date", default="2015-01-01", help="hirdetmenyek: skip notices posted before this date")
    pi.add_argument("--window-only", action="store_true",
                    help="nkfh: only the list the web page itself shows (~180 days) instead of the whole archive")
    pi.add_argument("--show-browser", action="store_true", help="nkfh/nmhh/sztnh: show the browser window (debugging)")

    pd = sub.add_parser("download", parents=[common, select], help="download the next batch")
    pd.add_argument("source", choices=sorted(SOURCES))
    pd.add_argument("--batch", type=int, default=1000, help="items per run (default 1000)")
    pd.add_argument("--max-minutes", type=float, default=0, help="stop after this many minutes (0 = no limit)")
    pd.add_argument("--max-attempts", type=int, default=3, help="retry failed items up to this many times")
    pd.add_argument("--attachments", choices=["decision", "all", "none"], default="decision",
                    help="ab: which PDF attachments to save besides the record page")

    ps = sub.add_parser("status", parents=[common], help="progress")
    ps.add_argument("source", nargs="?", choices=sorted(SOURCES))
    pe = sub.add_parser("export", parents=[common], help="write the index to CSV")
    pe.add_argument("source", choices=sorted(SOURCES))
    pe.add_argument("--csv", default="")
    return p


def main():
    args = build_parser().parse_args()
    if args.cmd == "list":
        return cmd_list(args)
    Path(args.data_dir).mkdir(parents=True, exist_ok=True)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
                        handlers=[logging.StreamHandler(),
                                  logging.FileHandler(Path(args.data_dir) / "hu_legal_dl.log", encoding="utf-8")])
    signal.signal(signal.SIGTERM, lambda *a: sys.exit(0))
    try:
        {"index": cmd_index, "download": cmd_download, "status": cmd_status, "export": cmd_export}[args.cmd](args)
    except Stop as e:
        log.log(logging.ERROR if e.code else logging.INFO, "%s", e)
        sys.exit(e.code)


if __name__ == "__main__":
    main()
