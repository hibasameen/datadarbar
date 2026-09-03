/* Data Darbar — export helpers for the SQL console.
   Pure functions, no DOM: a store-only ZIP writer (so a CSV can travel with its
   method notes as one download, without a library) and the README builder that
   turns catalog.json entries into those notes. Exposed as window.DD_EXPORT and,
   under Node, as module.exports, so the test suite can run them headless. */
(function (root) {
  'use strict';

  /* ── CRC-32 (the only arithmetic a ZIP needs) ─────────────────────────── */
  var CRC_TABLE = (function () {
    var t = new Uint32Array(256);
    for (var n = 0; n < 256; n++) {
      var c = n;
      for (var k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
      t[n] = c >>> 0;
    }
    return t;
  })();
  function crc32(bytes) {
    var c = 0xFFFFFFFF;
    for (var i = 0; i < bytes.length; i++) c = CRC_TABLE[(c ^ bytes[i]) & 0xFF] ^ (c >>> 8);
    return (c ^ 0xFFFFFFFF) >>> 0;
  }

  function utf8(s) { return new TextEncoder().encode(s); }

  /* DOS date/time fields, as the ZIP spec wants them */
  function dosTime(d) {
    return { time: (d.getHours() << 11) | (d.getMinutes() << 5) | (d.getSeconds() >> 1),
             date: ((d.getFullYear() - 1980) << 9) | ((d.getMonth() + 1) << 5) | d.getDate() };
  }

  /* files: [{name, data: Uint8Array|string}] -> Uint8Array of a valid .zip
     (method 0 = stored; every reader handles it and CSV is what it is) */
  function zip(files, when) {
    var now = dosTime(when || new Date()), parts = [], central = [], offset = 0;
    files.forEach(function (f) {
      var name = utf8(f.name), data = typeof f.data === 'string' ? utf8(f.data) : f.data;
      var crc = crc32(data);
      var local = new DataView(new ArrayBuffer(30));
      local.setUint32(0, 0x04034b50, true); local.setUint16(4, 20, true); local.setUint16(6, 0x0800, true);
      local.setUint16(8, 0, true); local.setUint16(10, now.time, true); local.setUint16(12, now.date, true);
      local.setUint32(14, crc, true); local.setUint32(18, data.length, true); local.setUint32(22, data.length, true);
      local.setUint16(26, name.length, true); local.setUint16(28, 0, true);
      parts.push(new Uint8Array(local.buffer), name, data);
      var cd = new DataView(new ArrayBuffer(46));
      cd.setUint32(0, 0x02014b50, true); cd.setUint16(4, 20, true); cd.setUint16(6, 20, true); cd.setUint16(8, 0x0800, true);
      cd.setUint16(10, 0, true); cd.setUint16(12, now.time, true); cd.setUint16(14, now.date, true);
      cd.setUint32(16, crc, true); cd.setUint32(20, data.length, true); cd.setUint32(24, data.length, true);
      cd.setUint16(28, name.length, true); cd.setUint16(30, 0, true); cd.setUint16(32, 0, true);
      cd.setUint16(34, 0, true); cd.setUint16(36, 0, true); cd.setUint32(38, 0, true); cd.setUint32(42, offset, true);
      central.push(new Uint8Array(cd.buffer), name);
      offset += 30 + name.length + data.length;
    });
    var cdSize = central.reduce(function (a, p) { return a + p.length; }, 0);
    var end = new DataView(new ArrayBuffer(22));
    end.setUint32(0, 0x06054b50, true); end.setUint16(4, 0, true); end.setUint16(6, 0, true);
    end.setUint16(8, files.length, true); end.setUint16(10, files.length, true);
    end.setUint32(12, cdSize, true); end.setUint32(16, offset, true); end.setUint16(20, 0, true);
    var all = parts.concat(central, [new Uint8Array(end.buffer)]);
    var total = all.reduce(function (a, p) { return a + p.length; }, 0);
    var out = new Uint8Array(total), pos = 0;
    all.forEach(function (p) { out.set(p, pos); pos += p.length; });
    return out;
  }

  /* ── method notes ─────────────────────────────────────────────────────── */
  /* cat: catalog.json; tables: the catalogue entries the export draws on;
     opts: {sql, title, site, rows}. Returns markdown. */
  function readme(cat, tables, opts) {
    opts = opts || {};
    var site = opts.site || 'https://hibasameen.github.io/datadarbar/';
    var L = [];
    L.push('# ' + (opts.title || 'Data Darbar extract'));
    L.push('');
    L.push('Extracted ' + new Date().toISOString().slice(0, 10) + ' from the Data Darbar warehouse (build ' +
           (cat.generated || '?') + ', catalogue v' + (cat.version || '?') + ').');
    if (opts.rows != null) L.push('Rows: ' + Number(opts.rows).toLocaleString('en-US') + '.');
    L.push('');
    if (opts.sql) {
      L.push('## Query');
      L.push('');
      L.push('```sql');
      L.push(opts.sql.trim());
      L.push('```');
      L.push('');
    }
    tables.forEach(function (t) {
      L.push('## Table `' + t.name + '`');
      L.push('');
      if (t.description) L.push(t.description);
      L.push('');
      var facts = [];
      if (t.unit) facts.push('**Unit:** ' + t.unit);
      if (t.source) facts.push('**Source:** ' + t.source);
      if (t.rows != null) facts.push('**Rows in warehouse:** ' + Number(t.rows).toLocaleString('en-US'));
      if (t.file) facts.push('**Parquet:** ' + site + 'data/warehouse/' + t.file);
      if (facts.length) { L.push(facts.join('  \n')); L.push(''); }
      if (t.notes) { L.push('### Caveats'); L.push(''); L.push(t.notes); L.push(''); }
      if (t.datasets && t.datasets.length) {
        L.push('### Datasets with observations'); L.push('');
        L.push('| dataset_code | dataset | subject | series | span |');
        L.push('|---|---|---|---|---|');
        t.datasets.forEach(function (d) {
          L.push('| `' + d.code + '` | ' + d.name + ' | ' + d.subject + ' | ' + d.series + ' | ' + d.since + ' → ' + d.upto + ' |');
        });
        L.push('');
      }
      if (t.columns && t.columns.length) {
        L.push('### Columns'); L.push('');
        L.push('| column | type | description |');
        L.push('|---|---|---|');
        t.columns.forEach(function (c) {
          L.push('| `' + c.name + '` | ' + String(c.type || '').toLowerCase() + ' | ' +
                 String(c.description || '').replace(/\|/g, '\\|') + ' |');
        });
        L.push('');
      }
    });
    L.push('## Method');
    L.push('');
    L.push('Every table is built by a reproducible pipeline from the published source ' +
           '(PBS and Finance Division PDF tables parsed programmatically, survey microdata, and ' +
           'State Bank series fetched from the EasyData API), cleaned into a DuckDB + Parquet ' +
           'warehouse and exported unchanged to these files. Survey-based district figures are ' +
           'sample estimates; trade is customs-basis (PBS) on the trade tables and payments-basis ' +
           '(SBP) on the State Bank tables, and the two will not match. The full methodology ' +
           '— district crosswalks, the multidimensional poverty index, the budget parser, the ' +
           'SBP hierarchies and identity checks — is under **Methodology** at ' + site + '.');
    L.push('');
    L.push('## Licence and citation');
    L.push('');
    L.push((cat.license || 'Derived data CC BY 4.0') + '. The underlying statistics are public ' +
           'publications of the Government of Pakistan and the State Bank of Pakistan.');
    L.push('');
    L.push('Suggested citation: Sameen, H. (' + new Date().getFullYear() + '). *Data Darbar: ' +
           (tables.length === 1 ? tables[0].name : 'warehouse extract') + '*. ' + site +
           ' — derived from ' + uniq(tables.map(function (t) { return t.source; }).filter(Boolean)).join('; ') + '.');
    L.push('');
    return L.join('\n');
  }

  function uniq(a) { return a.filter(function (v, i) { return a.indexOf(v) === i; }); }

  var api = { zip: zip, crc32: crc32, readme: readme };
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
  root.DD_EXPORT = api;
})(typeof window !== 'undefined' ? window : globalThis);
