/*
   query.js — the SQL console's interface.
   Pairs with warehouse.js (the engine) and catalog.json (the dictionary).
   Nothing here names a Data Darbar table, so a sister site can reuse the file
   and swap only DD_QUERY_CONFIG and its catalogue.

   The rail finds things: tables grouped by the catalogue's kinds, searchable
   down to their fields, plus the worked examples and this browser's history.
   Clicking a name inserts it at the cursor, because an analyst writing a join
   wants the second table's name where they are typing, not a new query.
*/
(function () {
  'use strict';

  var CFG = window.DD_QUERY_CONFIG || {};
  var wh = window.DDWarehouse.create({
    base: CFG.base || 'data/warehouse/', esm: CFG.esm, bundles: CFG.bundles
  });
  var $ = function (id) { return document.getElementById(id); };
  var CAT = null, last = null, sortBy = null;
  var MAX_RENDER = 1000, HISTORY_KEY = 'dd.query.history', HISTORY_MAX = 40;

  /* ── helpers ───────────────────────────────────────────────────────── */
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  }
  function mb(b) { return b >= 1e6 ? (b / 1e6).toFixed(1) + ' MB' : Math.max(1, Math.round(b / 1e3)) + ' KB'; }
  function num(n) { return Number(n).toLocaleString('en-US'); }
  function mark(text, term) {
    text = String(text || '');
    var i = term ? text.toLowerCase().indexOf(term) : -1;
    if (i < 0) return esc(text);
    return esc(text.slice(0, i)) + '<mark>' + esc(text.slice(i, i + term.length)) + '</mark>' + esc(text.slice(i + term.length));
  }
  /* DuckDB hands back BigInt and Date; normalise once so the table, the CSV
     and the sort all agree. */
  function cell(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === 'bigint') return Number(v);
    if (v instanceof Date) return v.toISOString().slice(0, 10);
    if (typeof v === 'object') return JSON.stringify(v);
    return v;
  }
  var NUMERIC = /^(TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT|FLOAT|DOUBLE|DECIMAL|REAL|INT)/i;
  function isNum(c) { return NUMERIC.test(c.type || ''); }
  function fmt(v) {
    if (v === null) return '<span class="nul">NULL</span>';
    if (typeof v === 'number') {
      return Number.isInteger(v) ? num(v) : v.toLocaleString('en-US', { maximumFractionDigits: 4 });
    }
    return esc(v);
  }
  function track(name) {
    if (window.goatcounter && typeof window.goatcounter.count === 'function') {
      window.goatcounter.count({ path: name, title: name, event: true });
    }
  }
  function flash(btn, msg) {
    var old = btn.innerHTML;
    btn.classList.add('done'); btn.textContent = msg;
    setTimeout(function () { btn.classList.remove('done'); btn.innerHTML = old; }, 1400);
  }
  function store(fn) { try { return fn(); } catch (e) { return null; } }

  /* ── editor ────────────────────────────────────────────────────────── */
  var sql = null;
  function gutter() {
    var n = sql.value.split('\n').length, out = '';
    for (var i = 1; i <= n; i++) out += i + '\n';
    $('gutter').textContent = out;
    $('gutter').scrollTop = sql.scrollTop;
  }
  function setSql(s) { sql.value = s; gutter(); sql.focus(); }
  /* Insert where the cursor is, padding with a space or a comma only where
     the text on either side needs one. */
  function insert(text, list) {
    var a = sql.selectionStart, b = sql.selectionEnd, v = sql.value;
    var before = v.slice(0, a), after = v.slice(b);
    var pre = '';
    if (before && !/[\s(,.]$/.test(before)) pre = list && /\w$/.test(before) ? ', ' : ' ';
    var post = after && !/^[\s),;.]/.test(after) ? ' ' : '';
    sql.value = before + pre + text + post + after;
    var at = (before + pre + text).length;
    sql.focus(); sql.setSelectionRange(at, at);
    gutter();
  }
  function currentSql() {
    var a = sql.selectionStart, b = sql.selectionEnd;
    var s = b > a ? sql.value.slice(a, b) : sql.value;
    return s.trim().replace(/;\s*$/, '');
  }

  /* ── rail: tables ──────────────────────────────────────────────────── */
  var ICON_T = '<svg width="10" height="10" viewBox="0 0 10 10" aria-hidden="true"><path d="M3 1.5 7 5 3 8.5" fill="none" stroke="currentColor" stroke-width="1.6"/></svg>';
  var open = {};
  function starter(t) { return 'SELECT *\nFROM ' + t.name + '\nLIMIT 100;'; }

  function renderTables() {
    var term = $('find').value.trim().toLowerCase();
    var kinds = CAT.kinds || [{ key: '', label: 'Tables' }];
    var h = '', shown = 0;
    kinds.forEach(function (k) {
      var ts = CAT.tables.filter(function (t) { return (t.kind || '') === k.key; });
      var part = '';
      ts.forEach(function (t) {
        var cols = t.columns, fieldHit = false;
        if (term.length >= 2) {
          var hitName = (t.name + ' ' + (t.description || '')).toLowerCase().indexOf(term) >= 0;
          var hits = cols.filter(function (c) {
            return (c.name + ' ' + (c.description || '')).toLowerCase().indexOf(term) >= 0;
          });
          if (!hitName && !hits.length) return;
          // Matching fields are the answer to the search: open and show them.
          if (hits.length) { cols = hits; fieldHit = true; }
        }
        shown++;
        var isOpen = open[t.name] || fieldHit;
        part += '<div class="qt' + (isOpen ? ' open' : '') + '" data-t="' + esc(t.name) + '">'
          + '<div class="qt-head"><button class="qt-tog" type="button" aria-label="Show the fields of ' + esc(t.name) + '" aria-expanded="' + (isOpen ? 'true' : 'false') + '">' + ICON_T + '</button>'
          + '<button class="qt-name" type="button" title="Insert ' + esc(t.name) + ' at the cursor">' + mark(t.name, term) + '</button>'
          + '<span class="qt-n">' + num(t.rows) + '</span>'
          + '<span class="qt-dot' + (wh.registered[t.name] ? ' on' : '') + '" title="' + (wh.registered[t.name] ? 'loaded' : (t.bytes < 2e6 ? 'loads at start' : 'loads when a query names it · ' + mb(t.bytes))) + '"></span></div>'
          + '<div class="qt-body"><p class="qt-desc">' + esc(t.description) + '</p>'
          + '<div class="qt-acts"><button class="qmini" type="button" data-act="starter">SELECT *</button>'
          + '<button class="qmini" type="button" data-act="extract" title="Download every row as CSV">Whole table · ' + mb(t.bytes) + '</button>'
          + '<a class="qmini" href="datasets/' + esc(t.name.replace(/_/g, '-')) + '/" target="_blank" rel="noopener">Catalogue ↗</a></div>'
          + '<ul class="qcols">' + cols.map(function (c) {
              return '<li><button class="qcol" type="button" data-col="' + esc(c.name) + '" title="' + esc(c.description || '') + '">'
                + '<code>' + mark(c.name, term) + '</code><span class="ty">' + esc(String(c.type).toLowerCase()) + '</span>'
                + '<span class="d">' + mark(c.description || '', term) + '</span></button></li>';
            }).join('') + '</ul></div></div>';
      });
      if (part) h += '<div class="qgroup">' + esc(k.label) + '</div>' + part;
    });
    $('pane-tables').innerHTML = h || '<p class="qnote">No table or field matches “' + esc(term) + '”.</p>';
  }

  function wireTables() {
    $('pane-tables').addEventListener('click', function (e) {
      var box = e.target.closest('.qt'); if (!box) return;
      var t = CAT.tables.filter(function (x) { return x.name === box.dataset.t; })[0];
      if (e.target.closest('.qt-tog')) {
        open[t.name] = !box.classList.contains('open');
        box.classList.toggle('open', open[t.name]);
        e.target.closest('.qt-tog').setAttribute('aria-expanded', open[t.name] ? 'true' : 'false');
      } else if (e.target.closest('.qt-name')) {
        insert(t.name, false);
      } else if (e.target.closest('.qcol')) {
        insert(e.target.closest('.qcol').dataset.col, true);
      } else if (e.target.closest('[data-act="starter"]')) {
        setSql(starter(t)); run();
      } else if (e.target.closest('[data-act="extract"]')) {
        extract(t, e.target.closest('[data-act="extract"]'));
      }
    });
  }

  /* ── rail: examples and history ────────────────────────────────────── */
  function firstLine(s) { return String(s).split('\n')[0]; }
  function renderExamples() {
    var term = $('find').value.trim().toLowerCase();
    var ex = (CAT.examples || []).map(function (x, i) { return [x, i]; }).filter(function (p) {
      return !term || (p[0].title + ' ' + p[0].sql).toLowerCase().indexOf(term) >= 0;
    });
    $('pane-examples').innerHTML = ex.length ? ex.map(function (p) {
      return '<button class="qex" type="button" data-i="' + p[1] + '"><b>' + mark(p[0].title, term) + '</b>'
        + '<span>' + esc(firstLine(p[0].sql)) + '</span></button>';
    }).join('') : '<p class="qnote">No example matches.</p>';
  }
  function history() { return store(function () { return JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]'); }) || []; }
  function remember(s, rows) {
    var h = history().filter(function (x) { return x.sql !== s; });
    h.unshift({ sql: s, rows: rows, at: Date.now() });
    store(function () { localStorage.setItem(HISTORY_KEY, JSON.stringify(h.slice(0, HISTORY_MAX))); });
    if (!$('pane-history').hidden) renderHistory();
  }
  function ago(t) {
    var m = Math.round((Date.now() - t) / 60000);
    return m < 1 ? 'just now' : m < 60 ? m + ' min ago' : m < 1440 ? Math.round(m / 60) + ' h ago' : Math.round(m / 1440) + ' d ago';
  }
  function renderHistory() {
    var term = $('find').value.trim().toLowerCase();
    var h = history().filter(function (x) { return !term || x.sql.toLowerCase().indexOf(term) >= 0; });
    $('pane-history').innerHTML = h.length
      ? h.map(function (x, i) {
          return '<button class="qex" type="button" data-h="' + i + '"><b>' + esc(firstLine(x.sql)) + '</b>'
            + '<span>' + ago(x.at) + ' · ' + num(x.rows) + ' rows</span></button>';
        }).join('') + '<p class="qnote">Kept in this browser only. <button class="qmini" type="button" id="clearHist">Clear</button></p>'
      : '<p class="qnote">Queries you run are listed here, in this browser only.</p>';
    $('pane-history')._list = h;
  }
  function wirePanes() {
    $('pane-examples').addEventListener('click', function (e) {
      var b = e.target.closest('.qex'); if (!b) return;
      setSql(CAT.examples[+b.dataset.i].sql); run();
    });
    $('pane-history').addEventListener('click', function (e) {
      if (e.target.id === 'clearHist') {
        store(function () { localStorage.removeItem(HISTORY_KEY); }); renderHistory(); return;
      }
      var b = e.target.closest('.qex'); if (!b) return;
      setSql($('pane-history')._list[+b.dataset.h].sql); run();
    });
    Array.prototype.forEach.call(document.querySelectorAll('.qtab'), function (tab) {
      tab.addEventListener('click', function () { showPane(tab.dataset.pane); });
    });
    var timer;
    $('find').addEventListener('input', function () {
      clearTimeout(timer); timer = setTimeout(renderPane, 80);
    });
  }
  var pane = 'tables';
  function showPane(p) {
    pane = p;
    Array.prototype.forEach.call(document.querySelectorAll('.qtab'), function (t) {
      t.setAttribute('aria-selected', t.dataset.pane === p ? 'true' : 'false');
    });
    ['tables', 'examples', 'history'].forEach(function (k) { $('pane-' + k).hidden = k !== p; });
    $('find').placeholder = p === 'tables' ? 'Find a table or field…' : p === 'examples' ? 'Find an example…' : 'Find in your history…';
    renderPane();
  }
  function renderPane() {
    if (pane === 'tables') renderTables();
    else if (pane === 'examples') renderExamples();
    else renderHistory();
  }

  /* ── run and render ────────────────────────────────────────────────── */
  function status(s) { $('status').textContent = s || ''; }
  function run() {
    var s = currentSql();
    if (!s) return;
    $('err').hidden = true;
    $('runbtn').disabled = true;
    status('Running…');
    wh.query(s, function (t, frac) {
      status('Loading ' + t.name + ': ' + Math.round(frac * 100) + '% of ' + mb(t.bytes) + '…');
    }).then(function (res) {
      last = res; last.sql = s; sortBy = null;
      render();
      if (pane === 'tables') renderTables();
      // The link carries the whole editor, not just a selection that was run.
      window.history.replaceState(null, '', '#q=' + encodeURIComponent(sql.value.trim()));
      remember(s, res.rows.length);
      track('query-run');
    }).catch(function (e) {
      last = null;
      $('out').innerHTML = '';
      $('meta').textContent = 'The query failed.';
      $('dl').hidden = $('tsv').hidden = true;
      $('err').hidden = false;
      $('err').textContent = e.message || String(e);
      track('query-error');
    }).then(function () { $('runbtn').disabled = false; status(''); });
  }

  function sorted() {
    if (!sortBy) return last.rows;
    var k = sortBy.col, dir = sortBy.dir;
    return last.rows.map(function (r, i) { return [r, i]; }).sort(function (a, b) {
      var x = cell(a[0][k]), y = cell(b[0][k]);
      if (x === y) return a[1] - b[1];
      if (x === null) return 1;
      if (y === null) return -1;
      return (x < y ? -1 : 1) * dir;
    }).map(function (p) { return p[0]; });
  }

  function render() {
    var rows = sorted(), cols = last.columns;
    $('meta').innerHTML = '<b>' + num(rows.length) + '</b> row' + (rows.length === 1 ? '' : 's')
      + ' · ' + last.ms.toFixed(0) + ' ms'
      + (rows.length > MAX_RENDER ? ' · showing the first ' + num(MAX_RENDER) + '; the download has them all' : '');
    $('dl').hidden = $('tsv').hidden = !rows.length;
    if (!rows.length) { $('out').innerHTML = '<div class="qempty">The query ran and returned no rows.</div>'; return; }
    var h = '<table><thead><tr><th class="rn" aria-label="Row"></th>';
    cols.forEach(function (c) {
      var s = sortBy && sortBy.col === c.name ? (sortBy.dir > 0 ? 'ascending' : 'descending') : 'none';
      h += '<th data-c="' + esc(c.name) + '" aria-sort="' + s + '"' + (isNum(c) ? ' class="num"' : '')
        + ' title="Sort by ' + esc(c.name) + '"><span class="c">' + esc(c.name) + '</span><span class="ty">' + esc(String(c.type).toLowerCase()) + '</span></th>';
    });
    h += '</tr></thead><tbody>';
    rows.slice(0, MAX_RENDER).forEach(function (r, i) {
      h += '<tr><td class="rn">' + (i + 1) + '</td>';
      cols.forEach(function (c) {
        var v = cell(r[c.name]);
        h += '<td' + (typeof v === 'number' ? ' class="n"' : '') + '>' + fmt(v) + '</td>';
      });
      h += '</tr>';
    });
    $('out').innerHTML = h + '</tbody></table>';
  }

  /* ── export ────────────────────────────────────────────────────────── */
  function withNotes() { return $('withNotes').checked; }
  function saveBlob(blob, name) {
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob); a.download = name; a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }
  function deliver(data, base, tables, opts) {
    if (withNotes() && window.DD_EXPORT) {
      var md = DD_EXPORT.readme(wh.catalog, tables, opts);
      var z = DD_EXPORT.zip([{ name: base + '.csv', data: data }, { name: 'README.md', data: md }]);
      saveBlob(new Blob([z], { type: 'application/zip' }), base + '.zip');
    } else {
      saveBlob(new Blob([data], { type: 'text/csv;charset=utf-8' }), base + '.csv');
    }
  }
  function delimited(res, rows, sep) {
    var cols = res.columns.map(function (c) { return c.name; });
    var q = sep === ',' ? function (v) {
      if (v === null) return '';
      v = String(v);
      return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
    } : function (v) { return v === null ? '' : String(v).replace(/[\t\n]/g, ' '); };
    return [cols.map(q).join(sep)].concat(rows.map(function (r) {
      return cols.map(function (c) { return q(cell(r[c])); }).join(sep);
    })).join('\n');
  }
  function csv() {
    if (!last) return;
    deliver(delimited(last, sorted(), ','), 'data-darbar-query', wh.tablesIn(last.sql),
            { sql: last.sql, title: 'Data Darbar query result', rows: last.rows.length });
    track('csv-download');
  }
  function tsv() {
    if (!last || !navigator.clipboard) return;
    var rows = sorted().slice(0, 20000);
    navigator.clipboard.writeText(delimited(last, rows, '\t')).then(function () {
      flash($('tsv'), rows.length < last.rows.length ? 'Copied the first 20,000' : 'Copied ' + num(rows.length) + ' rows');
    }, function () {});
  }
  function extract(t, btn) {
    var q = 'SELECT * FROM "' + t.name + '"';
    btn.disabled = true;
    status('Extracting ' + t.name + '…');
    var prog = function (tt, frac) { status('Loading ' + tt.name + ': ' + Math.round(frac * 100) + '% of ' + mb(tt.bytes) + '…'); };
    // COPY to a file is the fast path; if the WASM filesystem refuses, run the
    // query and serialise the rows instead.
    wh.exportCsv(q, prog).catch(function () {
      return wh.query(q, prog).then(function (res) { return new TextEncoder().encode(delimited(res, res.rows, ',')); });
    }).then(function (bytes) {
      deliver(bytes, t.name, [t], { sql: q, title: t.name, rows: t.rows });
      status(t.name + ': ' + num(t.rows) + ' rows, ' + mb(bytes.length) + (withNotes() ? ' with method notes' : ''));
      renderTables();
      track('table-extract');
    }).catch(function (e) {
      $('err').hidden = false;
      $('err').textContent = 'Could not extract ' + t.name + ': ' + (e.message || e);
      status('');
    }).then(function () { btn.disabled = false; });
  }
  function share() {
    var url = location.href.split('#')[0] + '#q=' + encodeURIComponent(sql.value.trim());
    (navigator.clipboard ? navigator.clipboard.writeText(url) : Promise.reject())
      .then(function () { flash($('share'), 'Link copied'); }, function () {});
  }

  /* ── boot ──────────────────────────────────────────────────────────── */
  var booted = false;
  function boot() {
    if (booted) return;
    booted = true;
    if (location.protocol === 'file:') {
      $('boot').innerHTML = '<b>Open this page over HTTP.</b><br>The SQL engine runs in a Web Worker, which '
        + 'browsers block on <code>file://</code>. Run <code>python3 -m http.server</code> in the '
        + '<code>app/</code> folder, or use the live site.';
      return;
    }
    sql = $('sql');
    sql.addEventListener('keydown', function (e) {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); run(); }
      if (e.key === 'Tab' && !e.shiftKey) {
        e.preventDefault();
        var s = this.selectionStart;
        this.value = this.value.slice(0, s) + '  ' + this.value.slice(this.selectionEnd);
        this.selectionStart = this.selectionEnd = s + 2;
        gutter();
      }
    });
    sql.addEventListener('input', gutter);
    sql.addEventListener('scroll', function () { $('gutter').scrollTop = sql.scrollTop; });
    if (!/Mac|iPhone|iPad/.test(navigator.platform)) {
      Array.prototype.forEach.call(document.querySelectorAll('.qkbd'), function (k) { k.textContent = 'Ctrl+⏎'; });
    }
    $('runbtn').addEventListener('click', run);
    $('dl').addEventListener('click', csv);
    $('tsv').addEventListener('click', tsv);
    $('share').addEventListener('click', share);
    $('out').addEventListener('click', function (e) {
      var th = e.target.closest('th[data-c]'); if (!th || !last) return;
      var c = th.dataset.c;
      sortBy = sortBy && sortBy.col === c ? (sortBy.dir > 0 ? { col: c, dir: -1 } : null) : { col: c, dir: 1 };
      render();
    });

    wh.init(function (m) { $('bootmsg').textContent = m; }).then(function (info) {
      CAT = info.catalog;
      wireTables(); wirePanes();
      renderTables();
      $('boot').hidden = true;
      $('console').hidden = false;
      var eng = info.rangeRequests ? 'DuckDB-WASM · large tables read by byte range'
                                   : 'DuckDB-WASM · large tables downloaded in full';
      $('engine').textContent = eng; $('engine2').textContent = eng;
      var m = /#q=([\s\S]*)$/.exec(location.hash);
      setSql(m ? decodeURIComponent(m[1]) : CAT.examples[0].sql);
      run();
    }).catch(function (e) {
      $('bootmsg').innerHTML = '<b>Could not start the engine.</b><br>' + esc(e.message || e);
    });
  }
  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
