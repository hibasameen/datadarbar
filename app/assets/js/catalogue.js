/* catalogue.js — the catalogue's two interactions: filter the index by kind and
   by a search over tables AND their fields, and copy names and code on a
   table's page. The index is complete without it; this only narrows it. */
(function () {
  'use strict';
  var $ = function (s, r) { return (r || document).querySelector(s); };
  var $$ = function (s, r) { return Array.prototype.slice.call((r || document).querySelectorAll(s)); };
  var esc = function (s) {
    return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  };
  var CAT = null, loading = null;
  function catalog() {
    if (CAT) return Promise.resolve(CAT);
    return loading || (loading = fetch('/data/warehouse/catalog.json', { cache: 'no-cache' })
      .then(function (r) { return r.json(); })
      .then(function (c) { CAT = c; return c; }));
  }

  function copy(text, btn) {
    var done = function () {
      btn.classList.add('done');
      setTimeout(function () { btn.classList.remove('done'); }, 1200);
    };
    if (navigator.clipboard) navigator.clipboard.writeText(text).then(done, function () {});
  }
  $$('[data-copy]').forEach(function (b) {
    b.addEventListener('click', function () { copy(b.getAttribute('data-copy'), b); });
  });
  $$('[data-copy-code]').forEach(function (b) {
    b.addEventListener('click', function () {
      var pre = b.parentNode.querySelector('pre');
      if (pre) copy(pre.textContent, b);
    });
  });

  var dict = $('#cdict');
  if (dict) dict.addEventListener('click', function () {
    catalog().then(function (cat) {
      if (!window.DD_EXPORT) return;
      var md = DD_EXPORT.readme(cat, cat.tables, { title: 'Data Darbar warehouse — data dictionary' });
      var a = document.createElement('a');
      a.href = URL.createObjectURL(new Blob([md], { type: 'text/markdown;charset=utf-8' }));
      a.download = 'data-darbar-data-dictionary.md';
      a.click();
      setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
    });
  });

  /* ── the index ─────────────────────────────────────────────────────── */
  var q = $('#cq');
  if (!q) return;
  var kind = 'all';
  var rows = $$('.crow[data-table]');
  var geoSec = $('.csec[data-kind="geography"]');

  function mark(text, term) {
    var i = text.toLowerCase().indexOf(term);
    if (i < 0) return esc(text);
    return esc(text.slice(0, i)) + '<mark>' + esc(text.slice(i, i + term.length)) + '</mark>'
         + esc(text.slice(i + term.length));
  }

  function apply() {
    var term = q.value.trim().toLowerCase();
    var searching = term.length >= 2;
    var byName = {};
    if (CAT) CAT.tables.forEach(function (t) { byName[t.name] = t; });
    var shown = 0, fields = 0;
    rows.forEach(function (li) {
      var t = byName[li.dataset.table];
      var okKind = kind === 'all' || li.dataset.kind === kind;
      var hitFields = [];
      var okText = true;
      if (searching) {
        var hay = li.textContent.toLowerCase() + ' ' + (t ? (t.source || '') + ' ' + (t.notes || '') : '');
        okText = hay.indexOf(term) >= 0;
        if (t) t.columns.forEach(function (c) {
          if ((c.name + ' ' + (c.description || '')).toLowerCase().indexOf(term) >= 0) hitFields.push(c);
        });
        okText = okText || hitFields.length > 0;
      }
      var show = okKind && okText;
      li.hidden = !show;
      var box = li.querySelector('.crow-fields');
      if (show && hitFields.length) {
        shown += 0;
        fields += hitFields.length;
        box.hidden = false;
        box.innerHTML = hitFields.slice(0, 4).map(function (c) {
          return '<span><code>' + mark(c.name, term) + '</code> '
               + mark(c.description || '', term) + '</span>';
        }).join('') + (hitFields.length > 4
          ? '<span class="cmuted">and ' + (hitFields.length - 4) + ' more fields</span>' : '');
      } else { box.hidden = true; box.innerHTML = ''; }
      if (show) shown += 1;
    });
    // Geography is a card and a list at rest; filtered, it is rows like the rest.
    var narrowed = searching || kind !== 'all';
    if (geoSec) {
      $('.cgeo', geoSec).hidden = narrowed;
      $('.ctable', geoSec).hidden = !narrowed;
    }
    $$('.csec[data-kind]').forEach(function (sec) {
      var any = $$('.crow[data-table]', sec).some(function (li) { return !li.hidden; });
      sec.hidden = narrowed && !any;
    });
    $$('#conventions,#machines').forEach(function (s) { s.hidden = narrowed; });
    $('#cempty').hidden = !(narrowed && shown === 0);
    $('#cstat').textContent = narrowed
      ? shown + (shown === 1 ? ' table' : ' tables')
        + (fields ? ' · ' + fields + (fields === 1 ? ' matching field' : ' matching fields') : '')
      : '';
  }

  $$('.cpill').forEach(function (b) {
    b.addEventListener('click', function () {
      kind = b.dataset.kind;
      $$('.cpill').forEach(function (x) { x.setAttribute('aria-pressed', x === b ? 'true' : 'false'); });
      apply();
    });
  });
  var timer;
  q.addEventListener('input', function () {
    clearTimeout(timer);
    timer = setTimeout(function () { catalog().then(apply, apply); }, 90);
  });
  q.addEventListener('focus', function () { catalog(); }, { once: true });
  q.addEventListener('keydown', function (e) {
    if (e.key === 'Escape') { q.value = ''; apply(); }
  });

  if (location.hash === '#fields') {
    q.placeholder = 'Search every field in every table…';
    // not on a touch screen, where it would raise the keyboard on arrival
    if (!(window.matchMedia && window.matchMedia('(pointer: coarse)').matches)) q.focus();
  }
})();
