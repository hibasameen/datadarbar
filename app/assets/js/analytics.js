/* Data Darbar — GoatCounter analytics (cookieless, no consent banner needed).
   Self-contained: loads on every page next to nav.js / modals.js.
   Auto-counts a pageview on load; adds a few lightweight engagement events.
   GoatCounter skips localhost / file:// automatically, so local dev is not counted. */
(function () {
  var ENDPOINT = 'https://datadarbar.goatcounter.com/count';

  // Config object must exist before count.js loads.
  window.goatcounter = window.goatcounter || {};

  // Load the official counter script (mirrors GoatCounter's recommended snippet).
  var s = document.createElement('script');
  s.async = true;
  s.src = '//gc.zgo.at/count.js';
  s.setAttribute('data-goatcounter', ENDPOINT);
  (document.head || document.body).appendChild(s);

  // Which product page are we on? (index|map|trade|finance|poverty)
  function pageSlug() {
    var f = (location.pathname.split('/').pop() || 'index.html').replace(/\.html?$/, '');
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
      var dest = card.getAttribute('href').replace(/\.html?$/, '').replace(/^.*\//, '') || 'index';
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
