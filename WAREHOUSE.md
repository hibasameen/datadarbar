# The queryable warehouse

Data Darbar ships a public SQL console at `/query.html`. There is no backend: the data
lives in Parquet files served as static assets, and DuckDB-WASM runs the query inside the
visitor's browser tab.

```
etl/build_web_warehouse.py  →  app/data/warehouse/{*.parquet, catalog.json}
                                       ↑
       app/query.html  →  assets/js/query.js (UI)  →  assets/js/warehouse.js (engine)
```

## Why this shape

The data is read-only, small (≈10 MB), and rebuilt on a human timescale. Under those
conditions a server buys nothing and costs something: a process to keep alive, a cold
start, an egress bill, a rate limiter, an outage surface. Pushing the compute to the
client removes all of it, and gets *faster* the second time because the browser caches the
files. A server only starts to earn its keep when you need authentication, write paths,
per-user metering, or tables too large to ship — none of which apply here.

The one thing you give up is control over what people run. That is fine: every query runs
on the user's own CPU, so a pathological query hurts only them.

## Rebuilding

```bash
python3 etl/build_web_warehouse.py            # reads the iCloud desktop warehouse
python3 etl/build_web_warehouse.py --src /path/to/data_darbar_warehouse
```

It reads `app/data/districts.json`, `app/data/poverty_data.js`, `etl/mouza2020/*.csv`,
the macro Parquet files from the desktop warehouse, and `app/assets/js/app.js` (for
indicator labels — the label dictionary is derived from `INDICATOR_GROUPS`, so the console
and the map can't drift apart). Rerun it after any `build_dataset.py` run, or after
regenerating the Mouza Census crosswalk in `etl/mouza2020`.

`catalog.json` carries a `generated` date which is appended to every Parquet URL as
`?v=…`, so a rebuild busts the CDN cache automatically.

## Loading strategy

| table size | at boot | on first query that mentions it |
|---|---|---|
| < 2 MB | fetched whole, registered as a buffer | — |
| ≥ 2 MB | nothing | registered by URL (byte-range reads) if the host supports `Range`, else downloaded whole with a progress indicator |

So `district_indicators`, `mpi_districts` and friends (≈0.4 MB total) are queryable
instantly, and the 10 MB `trade_hs8` only costs anything if you actually touch it. GitHub
Pages, Netlify, Cloudflare R2 and S3 all answer range requests, so in practice DuckDB
reads only the columns and row groups a query needs.

## The catalog.json contract

`warehouse.js` and `query.js` know nothing about Pakistan. They only know this file:

```jsonc
{
  "name": "Data Darbar",
  "version": 1,
  "generated": "2026-08-16",          // cache-busting stamp
  "license": "…",
  "tables": [{
    "name": "mpi_districts",          // the SQL identifier users type
    "file": "mpi_districts.parquet",  // relative to the catalog
    "bytes": 9184,                    // drives eager vs lazy loading
    "rows": 141,
    "description": "one sentence, shown in the sidebar",
    "notes": "caveats — units, double-counting traps, suppression rules",
    "source": "provenance line",
    "columns": [{ "name": "mpi", "type": "DOUBLE", "description": "…" }],
    "datasets": [ … ]                 // optional: sub-collections with observations
                                      // (sbp_series_catalog lists its 33 SBP datasets)
  }],
  "examples": [{ "title": "Ten poorest districts", "sql": "SELECT …" }]
}
```

The same file feeds three consumers: the console sidebar, `dictionary.html` (the
human-readable data dictionary, rendered client-side so it cannot drift from the data,
with a Markdown download) and the method-notes README that `export.js` bundles into
every download when "with method notes" is ticked. Write `notes` once, carefully.

The `notes` field is doing real work. A public SQL console hands people a loaded gun —
`trade_hs8` contains both commodity totals and country breakdowns, so summing naively
double-counts — and the sidebar is the only place to warn them before they publish a
number.

## Reusing this on another site

Nothing in `warehouse.js` or `query.js` is Data Darbar specific. To stand the same console
up on a sister site (e.g. Aiwan-e-Jamhoor):

1. Copy `app/assets/js/warehouse.js` and `app/assets/js/query.js` across unchanged.
2. Copy `app/query.html`, swap the header/footer markup and the CSS variables for that
   site's palette. The only functional part is the `#console` block and:
   ```html
   <script>window.DD_QUERY_CONFIG = { base: 'data/warehouse/' };</script>
   ```
3. Write a build script that emits Parquet plus a `catalog.json` matching the contract
   above. Anything that produces Parquet works — DuckDB's `COPY (…) TO 'x.parquet'` is the
   shortest path.

Optional `DD_QUERY_CONFIG` keys, for self-hosting the engine instead of loading it from
jsDelivr (useful offline or in CI):

```js
{ base: 'data/warehouse/',
  esm: 'vendor/duckdb/duckdb-browser.mjs',
  bundles: {
    mvp: { mainModule: 'vendor/duckdb/duckdb-mvp.wasm', mainWorker: 'vendor/duckdb/duckdb-browser-mvp.worker.js' },
    eh:  { mainModule: 'vendor/duckdb/duckdb-eh.wasm',  mainWorker: 'vendor/duckdb/duckdb-browser-eh.worker.js' }
  } }
```

## The same files as an API

The Parquet files are public URLs, so the "API" is just HTTP:

```python
import duckdb
duckdb.sql("INSTALL httpfs; LOAD httpfs;")
duckdb.sql("""
  SELECT name, prov, mpi
  FROM 'https://hibasameen.github.io/datadarbar/data/warehouse/mpi_districts.parquet'
  WHERE low_n = 0 ORDER BY mpi DESC LIMIT 10
""").show()
```

`catalog.json` is the machine-readable schema. If a REST interface is ever wanted on top,
it can be a thin wrapper over these same files — the storage layer doesn't change.

## Testing

`query.html` needs HTTP; the SQL engine runs in a Web Worker, which browsers refuse to
start from `file://`. Locally:

```bash
cd app && python3 -m http.server 8000   # then open http://localhost:8000/query.html
```
