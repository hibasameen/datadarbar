/*
   query.js — UI for the in-browser SQL console.
   Pairs with warehouse.js (the engine) and catalog.json (the data dictionary).
   Everything here is presentation; nothing is specific to which catalogue is loaded,
   so a sister site can reuse this file unchanged and only swap DD_QUERY_CONFIG.
*/
(function () {
  'use strict';

  var CFG = window.DD_QUERY_CONFIG || {};
  var wh = window.DDWarehouse.create({
    base: CFG.base || 'data/warehouse/',
    esm: CFG.esm, bundles: CFG.bundles
  });
  var $ = function (id) { return document.getElementById(id); };
  var lastResult = null;

  /* ── helpers ───────────────────────────────────────────────────────────── */

  function esc(s) {
    return String(s).replace(/[&<>"]/g, function (c) {
      return { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c];
    });
  }

  function mb(b) {
    return b >= 1e6 ? (b / 1e6).toFixed(1) + ' MB' : Math.max(1, Math.round(b / 1e3)) + ' KB';
  }

  function num(n) { return n.toLocaleString('en-US'); }

  /* DuckDB hands back BigInt for 64-bit ints and Date for dates; normalise once
     so the renderer, the CSV writer and JSON.stringify all agree. */
  function cell(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === 'bigint') return Number(v);
    if (v instanceof Date) return v.toISOString().slice(0, 10);
    if (typeof v === 'object') return JSON.stringify(v);
    return v;
  }

  function fmt(v) {
    if (v === null) return '<i class="nul">NULL</i>';
    if (typeof v === 'number') {
      return '<span class="n">' + (Number.isInteger(v) ? num(v)
        : v.toLocaleString('en-US', { maximumFractionDigits: 4 })) + '</span>';
    }
    return esc(v);
  }

  /* ── schema sidebar ────────────────────────────────────────────────────── */

  function renderCatalog(cat) {
    var h = '';
    cat.tables.forEach(function (t) {
      h += '<div class="tbl" data-table="' + t.name + '">'
        +   '<div class="tbl-head">'
        +     '<span class="tbl-name">' + esc(t.name) + '</span>'
        +     '<span class="tbl-meta">' + num(t.rows) + ' rows · ' + mb(t.bytes) + '</span>'
        +     '<span class="chip" data-chip="' + t.name + '">' + (t.bytes < 2e6 ? 'ready' : 'on demand') + '</span>'
        +   '</div>'
        +   '<div class="tbl-desc">' + esc(t.description) + '</div>'
        +   '<div class="tbl-cols">';
      t.columns.forEach(function (c) {
        h += '<div class="col" title="' + esc(c.description || '') + '">'
          +   '<code>' + esc(c.name) + '</code><span class="ty">' + esc(c.type.toLowerCase()) + '</span>'
          +   (c.description ? '<span class="cd">' + esc(c.description) + '</span>' : '')
          +  '</div>';
      });
      h += '</div>'
        +  '<div class="tbl-notes">' + esc(t.notes || '') + '</div>'
        +  '<div class="tbl-src">Source: ' + esc(t.source || '—') + '</div>'
        + '</div>';
    });
    $('tables').innerHTML = h;

    var ex = '';
    (cat.examples || []).forEach(function (q, i) {
      ex += '<button class="ex" data-i="' + i + '">' + esc(q.title) + '</button>';
    });
    $('examples').innerHTML = ex;
  }

  /* Listeners live outside renderCatalog so re-rendering the sidebar can never
     stack duplicate handlers (two toggles on one click cancel each other out). */
  function wireSidebar(cat) {
    $('tables').addEventListener('click', function (e) {
      var head = e.target.closest('.tbl-head');
      if (head) {
        var box = head.parentNode;
        // Clicking the name drops a starter query in the editor; clicking elsewhere
        // on the header just expands the schema. Two intents, one row.
        if (e.target.classList.contains('tbl-name')) {
          setSql('SELECT *\nFROM ' + box.dataset.table + '\nLIMIT 100;');
        } else {
          box.classList.toggle('open');
        }
      }
    });

    $('examples').addEventListener('click', function (e) {
      var b = e.target.closest('.ex');
      if (b) { setSql(cat.examples[+b.dataset.i].sql); run(); }
    });
  }

  function markLoaded() {
    Object.keys(wh.registered).forEach(function (name) {
      var c = document.querySelector('[data-chip="' + name + '"]');
      if (c) { c.textContent = 'loaded'; c.className = 'chip on'; }
    });
  }

  /* ── editor ────────────────────────────────────────────────────────────── */

  function setSql(s) {
    $('sql').value = s;
    $('sql').focus();
  }

  function currentSql() { return $('sql').value.trim().replace(/;\s*$/, ''); }

  /* ── run ───────────────────────────────────────────────────────────────── */

  function run() {
    var sql = currentSql();
    if (!sql) return;
    $('err').style.display = 'none';
    $('runbtn').disabled = true;
    status('Running…');
    wh.query(sql, function (t, frac) {
      status('Loading ' + t.name + ' — ' + Math.round(frac * 100) + '% of ' + mb(t.bytes) + '…');
    }).then(function (res) {
      lastResult = res;
      markLoaded();
      render(res);
      history.replaceState(null, '', '#q=' + encodeURIComponent(sql));
      track('query-run');
    }).catch(function (e) {
      lastResult = null;
      $('out').innerHTML = '';
      $('meta').textContent = '';
      $('err').style.display = 'block';
      $('err').textContent = e.message || String(e);
      status('');
      track('query-error');
    }).then(function () { $('runbtn').disabled = false; });
  }

  var MAX_RENDER = 500;

  function render(res) {
    var rows = res.rows, cols = res.columns;
    status('');
    $('meta').innerHTML = num(rows.length) + ' row' + (rows.length === 1 ? '' : 's')
      + ' · ' + res.ms.toFixed(0) + ' ms'
      + (rows.length > MAX_RENDER ? ' · showing first ' + num(MAX_RENDER) : '');
    $('dl').style.display = rows.length ? 'inline-flex' : 'none';
    $('share').style.display = 'inline-flex';

    if (!rows.length) { $('out').innerHTML = '<div class="empty">No rows.</div>'; return; }

    var h = '<table><thead><tr>';
    cols.forEach(function (c) { h += '<th>' + esc(c.name) + '<span class="ty">' + esc(c.type.toLowerCase()) + '</span></th>'; });
    h += '</tr></thead><tbody>';
    rows.slice(0, MAX_RENDER).forEach(function (r) {
      h += '<tr>';
      cols.forEach(function (c) { h += '<td>' + fmt(cell(r[c.name])) + '</td>'; });
      h += '</tr>';
    });
    $('out').innerHTML = h + '</tbody></table>';
  }

  function status(s) {
    $('status').textContent = s;
    $('status').style.display = s ? 'inline' : 'none';
  }

  /* ── export ────────────────────────────────────────────────────────────── */

  function csv() {
    if (!lastResult) return;
    var cols = lastResult.columns.map(function (c) { return c.name; });
    var q = function (v) {
      if (v === null) return '';
      v = String(v);
      return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
    };
    var lines = [cols.map(q).join(',')];
    lastResult.rows.forEach(function (r) {
      lines.push(cols.map(function (c) { return q(cell(r[c])); }).join(','));
    });
    var blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8' });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'data-darbar-query.csv';
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  function share() {
    var url = location.href;
    (navigator.clipboard ? navigator.clipboard.writeText(url) : Promise.reject())
      .then(function () { flash($('share'), 'Link copied'); })
      .catch(function () { prompt('Copy this link:', url); });
  }

  function flash(btn, msg) {
    var old = btn.textContent;
    btn.textContent = msg;
    setTimeout(function () { btn.textContent = old; }, 1400);
  }

  function track(name) {
    if (window.goatcounter && typeof window.goatcounter.count === 'function') {
      window.goatcounter.count({ path: name, title: name, event: true });
    }
  }

  /* ── boot ──────────────────────────────────────────────────────────────── */

  var booted = false;

  function boot() {
    if (booted) return;   // DOMContentLoaded can arrive after a manual call
    booted = true;
    // DuckDB-WASM needs Workers and real HTTP responses; neither exists on file://.
    if (location.protocol === 'file:') {
      $('boot').innerHTML = '<b>Open this page over HTTP.</b><br>The SQL engine runs in a Web '
        + 'Worker, which browsers block on <code>file://</code>. Try '
        + '<code>python3 -m http.server</code> in the <code>app/</code> folder, or use the live site.';
      return;
    }

    $('sql').addEventListener('keydown', function (e) {
      if ((e.metaKey || e.ctrlKey) && e.key === 'Enter') { e.preventDefault(); run(); }
      if (e.key === 'Tab') {
        e.preventDefault();
        var s = this.selectionStart;
        this.value = this.value.slice(0, s) + '  ' + this.value.slice(this.selectionEnd);
        this.selectionStart = this.selectionEnd = s + 2;
      }
    });
    $('runbtn').addEventListener('click', run);
    $('dl').addEventListener('click', csv);
    $('share').addEventListener('click', share);

    wh.init(function (m) { $('bootmsg').textContent = m; }).then(function (info) {
      renderCatalog(info.catalog);
      wireSidebar(info.catalog);
      markLoaded();
      $('boot').style.display = 'none';
      $('console').style.display = 'flex';
      $('engine').textContent = info.rangeRequests
        ? 'DuckDB-WASM · large tables read by byte range'
        : 'DuckDB-WASM · large tables downloaded in full';

      var m = /#q=([\s\S]*)$/.exec(location.hash);
      if (m) { setSql(decodeURIComponent(m[1])); run(); }
      else { setSql(info.catalog.examples[0].sql); run(); }
    }).catch(function (e) {
      $('bootmsg').innerHTML = '<b>Could not start the engine.</b><br>' + esc(e.message || e);
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
