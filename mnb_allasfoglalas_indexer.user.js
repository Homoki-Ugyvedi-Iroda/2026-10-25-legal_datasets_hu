// ==UserScript==
// @name         MNB állásfoglalás – indexer (id, title, category -> CSV)
// @match        https://allasfoglalaskereso.mnb.hu/*
// @grant        GM_registerMenuCommand
// @run-at       document-idle
// ==/UserScript==
//
// Reads only the result lists (no document is downloaded): for every item it records
//   id, title, category path, sector, year, result page, download URL
// and saves everything as one CSV (UTF-8 with BOM, opens correctly in Excel).
//
// Menu (Tampermonkey icon on the MNB results page):
//   "Start indexing from this page"   – walks through the pages with real "Következő" clicks
//   "Stop"                            – stops after the current page
//   "Save index CSV now"              – saves what has been collected so far
//   "Clear collected index"           – empties the stored list
// The list survives the page reloads (localStorage), so a stopped run can be resumed:
// start again on the page where it stopped; items are de-duplicated by id.
//
// CHANGE from the previous version: the site's "Következő" button reads its OWN hidden
// page-state from a shared <form>, not the page you're actually looking at. If you reach
// a page by jumping (rather than by clicking "Következő" repeatedly), that hidden state can
// be stale, so clicking "Következő" silently returns an unexpected page (often back near the
// start) instead of erroring out. This version checks, after every click, that the page
// number actually changed the way it should and STOPS with a clear reason instead of
// continuing to scrape the wrong pages.

(() => {
  const MAX_PAGES = 200;               // safety limit per run (the full list is about 97 pages)
  const PAGE_DELAY = [4000, 8000];     // ms before clicking "Következő"
  const KEY = 'MNB_INDEX_ROWS';

  const sleep  = ms => new Promise(r => setTimeout(r, ms));
  const jitter = ([a, b]) => a + Math.random() * (b - a);
  const load = () => { try { return JSON.parse(localStorage.getItem(KEY) || '[]'); } catch (_) { return []; } };
  const store = rows => localStorage.setItem(KEY, JSON.stringify(rows));
  const stop = msg => { sessionStorage.removeItem('MNB_IDX_RUN'); console.warn('[MNB-IDX] STOPPED: ' + msg); alert('Indexer stopped: ' + msg); };

  function toCsv(rows) {
    const cell = v => '"' + String(v ?? '').replace(/"/g, '""') + '"';
    const head = ['id', 'title', 'category', 'sector', 'year', 'page', 'url'];
    const body = rows.slice().sort((a, b) => (+b.id) - (+a.id))
      .map(r => head.map(h => cell(r[h])).join(','));
    return '﻿' + head.join(',') + '\r\n' + body.join('\r\n') + '\r\n';
  }

  function saveCsv(rows) {
    const blob = new Blob([toCsv(rows)], { type: 'text/csv;charset=utf-8' });
    const a = Object.assign(document.createElement('a'), {
      href: URL.createObjectURL(blob),
      download: `MNB_allasfoglalas_index_${new Date().toISOString().slice(0, 10)}.csv`,
    });
    document.body.appendChild(a); a.click(); a.remove();
    setTimeout(() => URL.revokeObjectURL(a.href), 10000);
  }

  function currentPageNo() {
    const txt = (document.querySelector('main .page-item.active')?.textContent || '').trim();
    const n = parseInt(txt, 10);
    return Number.isFinite(n) ? n : null;
  }

  function scrape() {
    const pageNo = currentPageNo();
    return [...document.querySelectorAll('main a[href*="/Home/Download"]')].map(a => {
      const u = new URL(a.getAttribute('href'), location.origin);
      const row = a.closest('.row') || a.parentElement;
      const parts = (row.querySelector('.text-muted')?.textContent || '')
        .split('·').map(s => s.trim()).filter(Boolean);
      return {
        id: u.searchParams.get('documentid'),
        title: a.textContent.trim(),
        category: parts.join(' > '),
        sector: parts[1] || '',
        year: parts[parts.length - 1] || '',
        page: pageNo,
        url: u.href,
      };
    }).filter(r => r.id);
  }

  GM_registerMenuCommand('Start indexing from this page', () => {
    const pageNo = currentPageNo();
    sessionStorage.MNB_IDX_RUN = '1';
    sessionStorage.MNB_IDX_PAGES = '0';
    sessionStorage.MNB_IDX_LAST_PAGE = '';   // no "previous page" expectation yet
    console.log(`[MNB-IDX] starting on page ${pageNo ?? '?'}`);
    run();
  });
  GM_registerMenuCommand('Stop', () => { sessionStorage.removeItem('MNB_IDX_RUN'); alert('Indexer: will stop after the current page.'); });
  GM_registerMenuCommand('Save index CSV now', () => { const r = load(); r.length ? saveCsv(r) : alert('The index is empty.'); });
  GM_registerMenuCommand('Clear collected index', () => { if (confirm('Delete the collected index?')) { store([]); alert('Cleared.'); } });

  if (sessionStorage.MNB_IDX_RUN === '1') run();

  async function run() {
    if (sessionStorage.MNB_IDX_RUN !== '1') return;
    if (/Request Rejected/i.test(document.title)) return stop('page was rejected');

    const pageNo = currentPageNo();
    const lastPageRaw = sessionStorage.MNB_IDX_LAST_PAGE;
    const lastPage = lastPageRaw === '' ? null : +lastPageRaw;

    // Continuity guard: after the very first scrape of a run, every subsequent page we land
    // on (via a "Következő" click) must be exactly lastPage + 1. If it isn't, the site's
    // pagination state has drifted (most often: it silently reset toward page 1) — stop
    // instead of scraping pages you didn't ask for.
    if (lastPage !== null && pageNo !== null && pageNo !== lastPage + 1) {
      return stop(`page jumped from ${lastPage} to ${pageNo} instead of ${lastPage + 1} — ` +
        `the site's "Következő" button is not continuing from where you started. ` +
        `Reload the results list, click "Következő" by hand until you are really on the ` +
        `page you want, then use "Start indexing from this page" again.`);
    }

    const items = scrape();
    if (!items.length) return;                                   // not a results page

    const map = new Map(load().map(r => [r.id, r]));
    items.forEach(r => map.set(r.id, r));
    const rows = [...map.values()];
    store(rows);

    const n = +(sessionStorage.MNB_IDX_PAGES || 0) + 1;
    sessionStorage.MNB_IDX_PAGES = n;
    sessionStorage.MNB_IDX_LAST_PAGE = String(pageNo ?? '');
    console.log(`[MNB-IDX] page ${pageNo ?? '?'}: ${items.length} items (ids ${items[items.length - 1].id}-${items[0].id}), ${rows.length} collected in total`);

    const next = [...document.querySelectorAll('main button[name="Page"]')]
      .find(b => /Következő/.test(b.textContent) && !b.disabled);
    if (!next || n >= MAX_PAGES) {
      stop(next ? `reached MAX_PAGES (${MAX_PAGES})` : 'last page reached');
      saveCsv(rows);
      alert(`Indexer finished: ${rows.length} items saved to CSV.`);
      return;
    }
    await sleep(jitter(PAGE_DELAY));
    if (sessionStorage.MNB_IDX_RUN === '1') next.click();        // real click -> reload -> run() again
  }
})();
