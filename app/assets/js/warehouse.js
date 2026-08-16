/*
   warehouse.js — a serverless SQL engine over static Parquet.
   ------------------------------------------------------------------
   Boots DuckDB-WASM in the browser and points it at a folder of Parquet files
   described by a catalog.json. There is no backend: the "database" is a set of
   ordinary static assets on the CDN, and every query runs in the user's tab.

   Deliberately free of any Data Darbar specifics so a sister site (e.g.
   Aiwan-e-Jamhoor) can drop this in and only supply its own catalog:

     const wh = DDWarehouse.create({ base: 'data/warehouse/' });
     await wh.init(msg => console.log(msg));
     const res = await wh.query('SELECT 1');

   Loading strategy
   ----------------
   Small tables are fetched whole and registered as buffers at boot, so the first
   query is instant. Anything above `eagerLimit` is registered lazily, on the first
   query that mentions it. If the host answers HTTP range requests (GitHub Pages,
   Netlify, R2 and S3 all do), large tables are registered by URL instead of being
   downloaded — DuckDB then reads only the columns and row groups the query needs,
   which is usually a small fraction of the file. If ranges are unsupported we fall
   back to a full download, so the page degrades in speed but never in function.
*/
window.DDWarehouse = (function () {
  'use strict';

  var DUCKDB_ESM = 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

  function create(opts) {
    opts = opts || {};
    var base = opts.base || 'data/warehouse/';
    var eagerLimit = opts.eagerLimit || 2 * 1024 * 1024; // bytes
    // Where the engine itself comes from. Defaults to jsDelivr; pass `esm` + `bundles`
    // to self-host the wasm (offline use, or a CI environment with no CDN access).
    var esmUrl = opts.esm || DUCKDB_ESM;
    var bundleOverride = opts.bundles || null;
    var duckdb = null, db = null, conn = null;
    var catalog = null;
    var registered = {};   // table -> 'buffer' | 'url'
    var rangeOK = null;
    var pending = {};      // table -> in-flight promise (dedupe concurrent loads)

    /* Parquet files are immutable *for a given build*, but a rebuild reuses the same
       names — so stamp the catalogue's build id onto every data URL. Without it a
       returning visitor can hold a stale table indefinitely behind a CDN cache. */
    function url(file, raw) {
      var stamp = (!raw && catalog && catalog.generated) ? '?v=' + encodeURIComponent(catalog.generated) : '';
      return base + file + stamp;
    }

    /* Does the host serve byte ranges? One cheap probe decides the strategy for
       every large table. A 206 is proof; anything else we treat as "no". */
    function probeRanges(file) {
      return fetch(url(file), { headers: { Range: 'bytes=0-1' } })
        .then(function (r) { return r.status === 206; })
        .catch(function () { return false; });
    }

    function init(onProgress) {
      var say = onProgress || function () {};
      say('Fetching catalogue…');
      return fetch(url('catalog.json', true), { cache: 'no-cache' })
        .then(function (r) {
          if (!r.ok) throw new Error('catalog.json not found (HTTP ' + r.status + ')');
          return r.json();
        })
        .then(function (cat) {
          catalog = cat;
          say('Starting the SQL engine…');
          return import(/* webpackIgnore: true */ esmUrl);
        })
        .then(function (mod) {
          duckdb = mod;
          return duckdb.selectBundle(bundleOverride || duckdb.getJsDelivrBundles());
        })
        .then(function (bundle) {
          // The worker script lives on a different origin, so it has to be wrapped
          // in a same-origin blob before the browser will run it as a Worker.
          var abs = new URL(bundle.mainWorker, location.href).href;
          var workerUrl = URL.createObjectURL(new Blob(
            ['importScripts("' + abs + '");'], { type: 'text/javascript' }));
          var worker = new Worker(workerUrl);
          db = new duckdb.AsyncDuckDB(new duckdb.VoidLogger(), worker);
          return db.instantiate(bundle.mainModule, bundle.pthreadWorker)
            .then(function () { URL.revokeObjectURL(workerUrl); });
        })
        .then(function () { return db.connect(); })
        .then(function (c) {
          conn = c;
          var big = catalog.tables.filter(function (t) { return t.bytes >= eagerLimit; });
          return big.length ? probeRanges(big[0].file) : false;
        })
        .then(function (ok) {
          rangeOK = ok;
          var eager = catalog.tables.filter(function (t) { return t.bytes < eagerLimit; });
          say('Loading ' + eager.length + ' tables…');
          return Promise.all(eager.map(function (t) { return load(t); }));
        })
        .then(function () { return { catalog: catalog, rangeRequests: rangeOK }; });
    }

    /* Register one table with DuckDB and expose it as a view under its plain name,
       so users write `FROM trade_hs8`, not `FROM parquet_scan('trade_hs8.parquet')`. */
    function load(t, onProgress) {
      if (registered[t.name]) return Promise.resolve();
      if (pending[t.name]) return pending[t.name];
      var lazy = t.bytes >= eagerLimit && rangeOK;
      var p = (lazy
        ? db.registerFileURL(t.file, new URL(url(t.file), location.href).href,
                             duckdb.DuckDBDataProtocol.HTTP, false)
        : fetchBuffer(t, onProgress).then(function (buf) {
            return db.registerFileBuffer(t.file, buf);
          })
      ).then(function () {
        return conn.query('CREATE OR REPLACE VIEW "' + t.name +
                          '" AS SELECT * FROM parquet_scan(\'' + t.file + '\')');
      }).then(function () {
        registered[t.name] = lazy ? 'url' : 'buffer';
        delete pending[t.name];
      }).catch(function (e) {
        delete pending[t.name];
        throw new Error('Could not load table "' + t.name + '": ' + e.message);
      });
      pending[t.name] = p;
      return p;
    }

    /* Streamed fetch so a 10 MB table can report progress rather than hanging. */
    function fetchBuffer(t, onProgress) {
      return fetch(url(t.file)).then(function (r) {
        if (!r.ok) throw new Error('HTTP ' + r.status);
        if (!r.body || !onProgress) return r.arrayBuffer();
        var total = +(r.headers.get('content-length') || t.bytes) || 0;
        var reader = r.body.getReader(), chunks = [], got = 0;
        return (function pump() {
          return reader.read().then(function (res) {
            if (res.done) {
              var out = new Uint8Array(got), at = 0;
              chunks.forEach(function (c) { out.set(c, at); at += c.length; });
              return out;
            }
            chunks.push(res.value); got += res.value.length;
            if (total) onProgress(got / total);
            return pump();
          });
        })();
      }).then(function (b) { return b instanceof Uint8Array ? b : new Uint8Array(b); });
    }

    /* Which catalogued tables does this SQL mention? Word-boundary matching is
       enough: it can only over-select (loading a table we didn't need), never
       under-select in a way that breaks a query, because an unmatched name would
       have failed to resolve anyway. */
    function tablesIn(sql) {
      var stripped = sql.replace(/'[^']*'/g, "''").replace(/--[^\n]*/g, '');
      return catalog.tables.filter(function (t) {
        return new RegExp('(^|[^\\w.])' + t.name + '($|[^\\w])', 'i').test(stripped);
      });
    }

    function query(sql, onProgress) {
      var needed = tablesIn(sql).filter(function (t) { return !registered[t.name]; });
      var t0;
      return Promise.all(needed.map(function (t) {
        return load(t, function (f) { onProgress && onProgress(t, f); });
      })).then(function () {
        t0 = performance.now();
        return conn.query(sql);
      }).then(function (tbl) {
        return {
          columns: tbl.schema.fields.map(function (f) {
            return { name: f.name, type: String(f.type) };
          }),
          rows: tbl.toArray().map(function (r) { return r.toJSON(); }),
          ms: performance.now() - t0
        };
      });
    }

    return {
      init: init,
      query: query,
      load: load,
      get catalog() { return catalog; },
      get registered() { return registered; },
      get rangeRequests() { return rangeOK; },
      tablesIn: tablesIn,
      fileUrl: url
    };
  }

  return { create: create };
})();
