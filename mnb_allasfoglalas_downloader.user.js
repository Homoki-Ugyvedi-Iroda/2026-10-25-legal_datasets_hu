// ==UserScript==
// @name         MNB állásfoglalás – polite page-by-page downloader
// @match        https://allasfoglalaskereso.mnb.hu/*
// @grant        GM_registerMenuCommand
// @run-at       document-idle
// ==/UserScript==
//
// The site's firewall (F5 "TSPD") serves a JavaScript check instead of the PDF to anything
// that is not a real browser navigation, so downloads must go through real tabs.
// Each link is opened in a tab (the check runs, the PDF is then saved by Chrome),
// the script waits until the tab closes itself, pauses, and moves on.
//
// PREREQUISITES (Chrome):
//   chrome://settings/content/pdfDocuments           -> "Download PDFs"
//   Site settings for allasfoglalaskereso.mnb.hu     -> Pop-ups and redirects: Allow
//                                                       Automatic downloads:   Allow
//   chrome://settings/downloads                      -> "Ask where to save each file": off
//
// START / STOP: Tampermonkey icon on the MNB results page ->
//   "▶ Start downloading from this page"  /  "■ Stop"

(async () => {
  const MAX_PAGES  = 95;                // pages per run, counted from the page you start on
  const FILE_DELAY = [2000, 4000];     // ms between files
  const PAGE_DELAY = [10000, 15000];   // ms before clicking "Következő"
  const TAB_TIMEOUT_S = 12;            // give each tab this long, then close it and move on

  const sleep  = ms => new Promise(r => setTimeout(r, ms));
  const jitter = ([a, b]) => a + Math.random() * (b - a);
  const stop = msg => { sessionStorage.removeItem('MNB_RUN'); console.warn('[MNB] STOPPED: ' + msg); alert('MNB downloader stopped: ' + msg); };

  GM_registerMenuCommand('▶ Start downloading from this page', () => {
    sessionStorage.MNB_RUN = '1'; sessionStorage.MNB_PAGES = '0';
    run();                                   // start right away – no reload (safe on page 2+)
  });
  GM_registerMenuCommand('■ Stop', () => { sessionStorage.removeItem('MNB_RUN'); alert('MNB downloader: will stop after the current file.'); });

  if (sessionStorage.MNB_RUN === '1') run();   // continues automatically after each "Következő" reload

  async function run() {
    if (sessionStorage.MNB_RUN !== '1') return;
    if (/Request Rejected/i.test(document.title)) return stop('page itself was rejected');

    const links = [...document.querySelectorAll('main a[href*="/Home/Download"]')];
    if (!links.length) return;                                  // not a results page

    const done = +(sessionStorage.MNB_PAGES || 0);
    if (done >= MAX_PAGES) return stop(`reached MAX_PAGES (${MAX_PAGES})`);
    console.log(`[MNB] page ${done + 1}: ${links.length} documents`);

    for (const a of links) {
      if (sessionStorage.MNB_RUN !== '1') return console.log('[MNB] stopped by user');
      const title = a.textContent.trim().slice(0, 80);

      const w = window.open(a.href, '_blank');
      if (!w) return stop('pop-up blocked – allow pop-ups for allasfoglalaskereso.mnb.hu and start again');

      // Wait for a clear outcome; never close the tab early (that would cancel the download).
      let outcome = 'timeout';
      for (let t = 0; t < TAB_TIMEOUT_S; t++) {
        await sleep(1000);
        if (w.closed) { outcome = 'closed'; break; }             // Chrome closed it = file saved
        let docTitle = '';
        try { docTitle = w.document.title; } catch (_) { outcome = 'cross-origin'; break; }
        if (/Request Rejected/i.test(docTitle)) { outcome = 'rejected'; break; }
        // otherwise the firewall check is still running: keep waiting
      }
      if (outcome === 'rejected') { try { w.close(); } catch (_) {} return stop(`rejected at ${a.href}`); }
      // Chrome often leaves the tab open even though the file was saved, so close it ourselves.
      try { if (!w.closed) w.close(); } catch (_) {}
      console.log(`[MNB] opened (${outcome}): ` + title);
      await sleep(jitter(FILE_DELAY));
    }

    sessionStorage.MNB_PAGES = done + 1;
    const next = [...document.querySelectorAll('main button[name="Page"]')]
      .find(b => /Következő/.test(b.textContent) && !b.disabled);
    if (!next) return stop('last page reached');
    await sleep(jitter(PAGE_DELAY));
    if (sessionStorage.MNB_RUN === '1') next.click();   // real click → form POST → reload → script runs again
  }
})();