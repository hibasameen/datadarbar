/* Data Darbar - district profiles: find an indicator on the page.

   A profile carries about 180 rows in ten tables, and a reader who came for
   one of them - stunting, internet use, the walk to a clinic - should not
   have to scroll for it. The box in the contents bar completes over the rows
   this page actually has, so every suggestion is a figure for this district,
   shown with its value; choosing one scrolls to the row, marks it, and puts
   its address in the URL so the link can be shared.

   The index is read from the tables themselves rather than shipped as data:
   the page already says everything the search needs, and a second copy is a
   second thing to keep in step. What the page does not have is one click
   further: the last suggestion carries the words to the Places map, which
   searches all of its indicators with this district still selected. */
(function () {
  var box = document.getElementById('dpInd'), list = document.getElementById('dpIndList');
  if (!box || !list) return;
  var wrap = box.closest('.dp-find');
  var code = box.getAttribute('data-code') || '';

  function norm(s) {
    return String(s).toLowerCase().replace(/[–—-]/g, ' ')
      .replace(/[^a-z0-9% ]+/g, ' ').replace(/\s+/g, ' ').trim();
  }
  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  /* Words a reader types that the labels do not use. Added to what a row is
     matched on, never shown. */
  var ALSO = [
    [/literacy|literate/, 'read write education'],
    [/stunt|wasted|underweight/, 'nutrition malnutrition children'],
    [/multidimensional|mpi/, 'poverty poor'],
    [/labour force|unemployment|in work|employed/, 'jobs employment work'],
    [/40°c|temperature|warm/, 'heat hot climate weather'],
    [/daily maximum|daily minimum/, 'temperature heat climate weather'],
    [/rainfall|monsoon/, 'rain precipitation climate weather'],
    [/pm2\.5/, 'air pollution quality smog'],
    [/health facility|care/, 'hospital clinic health access distance'],
    [/emigrants/, 'migration overseas abroad diaspora'],
    [/cases|judges/, 'courts justice backlog'],
    [/contracepti|family planning/, 'birth control fertility'],
    [/toilet|defecation|sanitation/, 'sanitation wash latrine'],
    [/piped water|drinking water|e\. coli/, 'water wash'],
    [/internet|smartphone|mobile/, 'digital phone ict'],
    [/out of school|in school|enrolment|attended school/, 'schooling education children'],
    [/cattle|buffalo|sheep|goats/, 'livestock animals'],
    [/tree cover|grassland|cropland|bare ground|built up/, 'land cover forest'],
    [/population|household size|sex ratio|density/, 'people census demographics'],
  ];

  var rows = [];
  [].forEach.call(document.querySelectorAll('.dp-sec'), function (sec) {
    var h = sec.querySelector('h2'), title = h ? h.textContent : '';
    [].forEach.call(sec.querySelectorAll('.dp-table'), function (tbl) {
      var where = tbl.getAttribute('data-find') || title;
      [].forEach.call(tbl.querySelectorAll('tbody tr'), function (tr) {
        var th = tr.querySelector('th'), td = tr.querySelector('td.dp-v');
        if (!th) return;
        var label = th.textContent.trim(), low = label.toLowerCase(), extra = '';
        ALSO.forEach(function (a) { if (a[0].test(low)) extra += ' ' + a[1]; });
        rows.push({ tr: tr, label: label, where: where,
                    val: td ? td.textContent.replace(/[†‡]/g, '').trim() : '',
                    name: norm(label), hay: norm(label + ' ' + where + extra) });
      });
    });
  });
  if (!rows.length) return;
  wrap.hidden = false;

  function match(q) {
    q = norm(q);
    if (!q) return [];
    var toks = q.split(' '), out = [];
    rows.forEach(function (r, i) {
      for (var k = 0; k < toks.length; k++) if (r.hay.indexOf(toks[k]) < 0) return;
      /* best first: the label begins with what was typed, then every word
         typed begins a word of the label, then anything else; page order
         breaks ties, which is the order the tables were written in */
      var s = 2;
      if (r.name.indexOf(q) === 0) s = 0;
      else if (toks.every(function (t) { return (' ' + r.name).indexOf(' ' + t) >= 0; })) s = 1;
      out.push({ r: r, s: s, i: i });
    });
    out.sort(function (a, b) { return a.s - b.s || a.i - b.i; });
    return out.map(function (o) { return o.r; });
  }

  var hits = [], sel = -1, total = 0, MAX = 8;

  function render() {
    var q = box.value.trim();
    if (!q) { list.hidden = true; list.innerHTML = ''; box.setAttribute('aria-expanded', 'false'); return; }
    var h = hits.map(function (r, i) {
      return '<li role="option" id="dpInd-' + i + '" data-i="' + i + '" aria-selected="' + (i === sel) + '"'
        + (i === sel ? ' class="is-sel"' : '') + '><span class="dp-find-k"><b>' + esc(r.label) + '</b>'
        + '<i>' + esc(r.where) + '</i></span><span class="dp-find-v">' + esc(r.val) + '</span></li>';
    }).join('');
    if (!hits.length) h += '<li class="dp-find-none" role="presentation">No row on this page matches.</li>';
    else if (total > hits.length) h += '<li class="dp-find-none" role="presentation">' + (total - hits.length) + ' more: keep typing to narrow.</li>';
    var m = hits.length;
    h += '<li role="option" id="dpInd-' + m + '" data-i="' + m + '" aria-selected="' + (m === sel) + '" class="dp-find-map'
      + (m === sel ? ' is-sel' : '') + '"><a href="/places.html?lv=district&amp;p=' + encodeURIComponent(code)
      + '&amp;q=' + encodeURIComponent(q) + '">Search every indicator on the map for “' + esc(q) + '” →</a></li>';
    list.innerHTML = h;
    list.hidden = false;
    box.setAttribute('aria-expanded', 'true');
    box.setAttribute('aria-activedescendant', sel >= 0 ? 'dpInd-' + sel : '');
  }

  function update() {
    var all = match(box.value);
    total = all.length; hits = all.slice(0, MAX);
    sel = box.value.trim() ? 0 : -1;
    render();
  }

  var still = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;
  function mark(tr, scroll) {
    [].forEach.call(document.querySelectorAll('.dp-hit'), function (x) { x.classList.remove('dp-hit'); });
    tr.classList.add('dp-hit');
    if (scroll) tr.scrollIntoView({ block: 'center', behavior: still ? 'auto' : 'smooth' });
    setTimeout(function () { tr.classList.remove('dp-hit'); }, 4000);
  }

  function go(i) {
    if (i === hits.length) {                       // the map
      var a = list.querySelector('.dp-find-map a');
      if (a) location.href = a.href;
      return;
    }
    var r = hits[i];
    if (!r) return;
    list.hidden = true; box.setAttribute('aria-expanded', 'false');
    box.value = r.label; box.blur();
    /* The address follows the row chosen. A row with no id clears it rather
       than leave the last row's there for the next person to copy. */
    history.replaceState(null, '', r.tr.id ? '#' + r.tr.id : location.pathname + location.search);
    mark(r.tr, true);
  }

  box.addEventListener('input', update);
  box.addEventListener('focus', function () { if (box.value.trim()) update(); });
  box.addEventListener('keydown', function (e) {
    var n = hits.length + 1;
    if (e.key === 'ArrowDown' && !list.hidden) { sel = (sel + 1) % n; render(); e.preventDefault(); }
    else if (e.key === 'ArrowUp' && !list.hidden) { sel = (sel - 1 + n) % n; render(); e.preventDefault(); }
    else if (e.key === 'Enter' && sel >= 0 && !list.hidden) { e.preventDefault(); go(sel); }
    else if (e.key === 'Escape') { list.hidden = true; box.setAttribute('aria-expanded', 'false'); }
  });
  /* mousedown, not click: the row has to be read before the box loses focus */
  list.addEventListener('mousedown', function (e) {
    var li = e.target.closest('li[data-i]');
    if (!li || e.target.closest('a')) return;
    e.preventDefault();
    go(+li.getAttribute('data-i'));
  });
  document.addEventListener('click', function (e) {
    if (!wrap.contains(e.target)) { list.hidden = true; box.setAttribute('aria-expanded', 'false'); }
  });
  /* "/" goes to the box from anywhere on the page, as it does on most sites
     with one search field */
  document.addEventListener('keydown', function (e) {
    if (e.key !== '/' || e.metaKey || e.ctrlKey || e.altKey) return;
    var t = e.target, tag = t && t.tagName;
    if (tag === 'INPUT' || tag === 'TEXTAREA' || tag === 'SELECT' || (t && t.isContentEditable)) return;
    e.preventDefault();
    box.focus(); box.select();
    wrap.scrollIntoView({ block: 'center', behavior: still ? 'auto' : 'smooth' });
  });

  /* A shared link to a row opens on it, marked. */
  function fromHash() {
    var id = decodeURIComponent(location.hash.slice(1));
    if (id.indexOf('i-') !== 0) return;
    var tr = document.getElementById(id);
    if (tr && tr.tagName === 'TR') mark(tr, true);
  }
  window.addEventListener('hashchange', fromHash);
  fromHash();
})();
