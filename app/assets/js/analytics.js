/* Data Darbar — GoatCounter analytics (cookieless, no consent banner needed).
   Self-contained: loads on every page next to shell.js (etl/apply_shell.py
   puts it there; build_seo.py and build_catalogue.py on generated pages).
   Auto-counts a pageview on load; adds a few lightweight engagement events.
   GoatCounter skips localhost / file:// automatically, so local dev is not counted. */
(function () {
  // Exclude local previews, staging and old hosts from production reporting.
  if (location.hostname !== 'darbar.adaad.org') return;
  // Pageviews go through the first-party relay at s.adaad.org, which forwards
  // them to GoatCounter's count API with the visitor's IP and User-Agent.
  // Tracker blocklists match gc.zgo.at and goatcounter.com, so the counter
  // script is served from here (assets/js/tally.js, a copy of count.js) and
  // the beacon never leaves adaad.org. Relay source: Adaad/site/analytics-relay.
  var ENDPOINT = 'https://s.adaad.org/v/d';

  // Config object must exist before the counter script loads.
  window.goatcounter = window.goatcounter || {};
  window.goatcounter.endpoint = ENDPOINT;

  var s = document.createElement('script');
  s.async = true;
  s.src = '/assets/js/tally.js';
  (document.head || document.body).appendChild(s);

  // Which product page are we on? (index|map|trade|finance|poverty)
  function pageSlug() {
    var f = (location.pathname.replace(/\/$/, '').split('/').pop() || 'index.html').replace(/\.html?$/, '');
    return f || 'index';
  }

  // Send a custom event (no-op until count.js has loaded).
  function ev(name, title) {
    if (window.goatcounter && typeof window.goatcounter.count === 'function') {
      window.goatcounter.count({ path: name, title: title || name, event: true });
    }
  }

  var page = pageSlug();

  // Delegated click tracking — robust to per-page markup differences.
  document.addEventListener('click', function (e) {
    var t = e.target;
    if (!t || !t.closest) return;

    // Landing-page product cards → open-<product>
    var card = t.closest('.pcard');
    if (card && card.getAttribute('href')) {
      var dest = card.getAttribute('href').replace(/\/$/, '').replace(/\.html?$/, '').replace(/^.*\//, '') || 'index';
      ev('open-' + dest, 'Open ' + dest);
      return;
    }

    // CSV exports: .csvbtn (trade/finance), #downloadData (map), or any download link
    if (t.closest('.csvbtn') || t.closest('#downloadData') || t.closest('a[download]')) {
      ev('csv-' + page, 'CSV export — ' + page);
      return;
    }

    // Per-chart share buttons (trade/finance)
    if (t.closest('.sharebtn')) {
      ev('share-' + page, 'Share — ' + page);
      return;
    }
  }, true);
})();
