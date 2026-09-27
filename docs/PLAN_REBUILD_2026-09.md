# Rebuild plan — Data Darbar, September 2026

Five things were asked for, and they are not five independent projects:

1. Rebuild the site on the new structure and architecture
2. Build the analysts' view — catalogue, geographies, crosswalks
3. Build Places off the warehouse, if it does not slow the page
4. Ingest the new data into the warehouse, changing the schema where it needs it
5. Visualise the new data in the views it belongs in

(3) and (4) both turn on one decision that has not been made yet, and (5) cannot
start before (4). This plan orders them by what actually blocks what, states the
one architectural decision that ties four of the five together, and gives each
phase an exit test in the form this project already uses.

Design: `docs/REDESIGN_IA_2026-09.md`, canvas at
https://claude.ai/artifact/StSRFF6wMqL92izqYWjrCk.
Readable copy of this plan: https://claude.ai/artifact/D4VvoE3crATDwahGVa9mhn
(this file is canonical; the page is the same content, laid out).

**Nothing here publishes.** Every phase ends at a commit on a branch. Deployment
is a separate decision and is not in this plan.

## Where we are

| | state |
|---|---|
| Census panels | Both on PBS's Digital Census 2023 frame; 37,971 mappable series; unit map decides every unit's shape |
| Geography | `districts_2023_geo.js` (157), `tehsils_2023_geo.js` (591 census units of 650); district, sub-district and unit crosswalks all balance against PBS |
| Warehouse | 28 tables, 32.9 MB, byte-range reads confirmed working on the deployed host |
| Curated map data | 296 indicators shipped as pre-baked JS: `census_data.js` is 1,826 KB of blocking script at first paint |
| Site | 11 pages organised by source; two stylesheets plus inline `<style>` on six pages |
| New data | ~79 MB from four PBS portals, pulled 27 Sep, none ingested |
| State data | 4 pipelines exist unpublished (LJCP, regional police, Sindh police, Sindh FIR); NEPRA and disasters do not exist at all |

## The decision that ties it together

Places is meant to have one picker over everything. Today the census half is
queried from Parquet and the curated half is a 1.8 MB JavaScript blob, and the
new district data — emigrants, crops, census entities — would be a third shape
again if each source arrived as its own table with its own columns.

**The census pattern already solves this, and it generalises:** a source-faithful
table that a researcher would recognise, plus a small derived index that the
picker reads. `census_panel_2023` and `census_series_index` are exactly that,
and the index is 0.16 MB against the panel's 9 MB.

So: keep every source's own table, in its own shape, for the catalogue and for
Query — a crops table should have area, production and yield, not a flattened
`value` column. Then derive **`place_indicators`** and **`place_indicator_index`**
across all of them, keyed on the PBS 2023 codes, and let the picker read only
the index.

This is the spine of phases 1 through 3, and it is what makes (3), (4) and (5)
one piece of work rather than three. It is also the schema change the ask
anticipates: the new tables are additive, but the derived pair is new.

## Phase 0 — Gates and ground

Nothing large should start until these are settled. All three are small.

**0.1 Measure the engine on the deployed site.** This is the gate on (3). Cold
first paint, today's build against a build with `census_data.js` removed and the
engine booting. The DuckDB WASM module instantiates inside a Worker, so it never
appears in the page's resource timing and has not been measured — this needs a
real deployed measurement, not a local one, because `python3 -m http.server`
does not serve byte ranges and makes every local panel read look like a full
download.

*Decision rule.* Warehouse-only if cold first paint is at or below today's
1,920 KB path. If the engine costs more than it saves, fall back to the hybrid:
geometry as a file, one small eager Parquet for the default layer, engine
deferred until a layer needs it. Either way the map draws geometry first and
fills values when they arrive, so the page is never blank waiting on data.

**Measured, 27 Sep — the rule fails, decisively.** Sizing the exact bundles
`selectBundle()` fetches from jsDelivr, over the wire, brotli-encoded:

| | wire | raw |
|---|---:|---:|
| `duckdb-eh.wasm` | **6,060 KB** | 34,824 KB |
| `duckdb-browser-eh.worker.js` | 182 KB | 743 KB |
| `apache-arrow` | 46 KB | 195 KB |
| `duckdb-wasm` shim | 7 KB | 25 KB |
| **engine total** | **6,295 KB** | |

Against `census_data.js` at 1,826 KB uncompressed, or ~350 KB if the host
compresses it — the site's own gzip state could not be read from the browser
(the control CDN reported no `content-encoding` either, so the header is being
stripped after decoding) and is worth checking directly, because turning
compression on would be a larger, cheaper win than any architecture change.

Either way the engine loses: **3.4× worse uncompressed, 18× worse compressed.**
Warehouse-only on first paint is off.

**What replaces it: one source, two deliveries.** The warehouse stays the single
source of truth — `place_indicators` is built there, catalogued there, queryable
there — and the build emits a compact payload for the page. The page has one
source and a derived artefact, which is exactly the relationship
`census_series_index` already has to the panels. The rule for which delivery is
mechanical and belongs in the build: small enough to ship → generated payload,
no engine; too big to ship → Parquet over byte ranges, engine on demand. The
census panels are 17 MB and stay on DuckDB, loaded when a census layer is
picked, which is already how they work.

This still answers "one approach for all of Places" where it matters — one
pipeline, one source of truth, one picker — without putting 6 MB in front of a
first-time visitor on a phone.

**0.2 Settle the concurrent-agent question.** A second agent has had write
access to this repository. Before parallel work starts, establish who owns which
paths, or stop the other writer. A silent overwrite mid-rebuild is expensive to
unpick.

**0.3 The shared shell.** One stylesheet, one header, one footer, one nav; About,
Methodology and Contact collapse into Methods. This blocks every page rebuild
and is worth doing before any of them. Two stylesheets and six pages with inline
`<style>` is where the drift lives today.

> **Exit test.** A measurement recorded with a decision written down; a stated
> owner for every path under `app/` and `etl/`; every page rendering through one
> stylesheet and one header.

**Status, 27 Sep: 0.1 and 0.3 done, 0.2 open.**

0.3 found the chrome had drifted further than "two stylesheets" suggested: only
`map.html` linked a stylesheet at all, six pages carried 64 KB of inline CSS
between them, and of the 90 rules appearing on three or more pages, **31 had
different bodies** — `.header-link`, `.header-logo`, `.header-title`,
`.mobile-nav` among them. There was no common subset to hoist, so the shell was
written once from the design and the pages converted onto it.

- `app/assets/css/shell.css` owns the chrome; **192 duplicated rules removed**
  from six pages and both stylesheets.
- `etl/apply_shell.py` writes the header and footer into every page from one
  definition, so they cannot drift again. `--check` fails if any page is stale,
  which is a CI gate when there is CI.
- `app/assets/js/shell.js` replaces `nav.js` and owns the mobile menu. The
  toggle had been bound in `app.js`, inline on six pages, and **not at all on
  about, methodology and methods**, where the button was there and did nothing.
- `methods.html` merges About and Methodology, generated by
  `etl/build_methods_page.py` so that 23 KB of provenance is moved rather than
  retyped; sections regrouped onto Places and Economy.
- The nav carries Places · Economy · State | Catalogue · Query · Methods, with
  State shown but not linked, because it has nothing behind it yet.

## Phase 1 — The place spine

*Blocked by 0.1 (shape of the answer), 0.3 (nothing visual until the shell).*

**1.1 Re-key the curated district data to PBS 2023.** `district_indicators` is
on the 147-district 2015 frame; the census layers are on PBS's 136-of-157. The
unit map already does this arbitration for the census and the same treatment
applies — including the parts that are not clean: a merged district's figures
add, a split district's do not divide.

**1.2 Define `place_indicators` and `place_indicator_index`.** Long format,
keyed `(level, place_code, indicator_id, year)`, covering curated and census
alike; the index mirrors `census_series_index` and carries the counts that make
a thin layer read as thin. Target: the index stays under the 2 MB eager limit.

**1.3 Migrate the 296 curated indicators into it**, tehsil layers included —
Mouza, poverty, satellite and school access have no district-table equivalent
today and are the reason `district_indicators` alone is not a drop-in.

**1.4 Publish the geographies and crosswalks** — the analysts' ask (2), pulled
forward because the work is already done and this phase is where it is
documented anyway. Boundaries page centred on the "which key joins what" table
(`pbs_code`, `dd_id`, `dds_id`, `dk`, `adm3_pcode`); the district, sub-district
and unit crosswalks published with method, evidence and status per row.

> **Exit test.** Every district and tehsil indicator on the site resolves through
> one key and one index. The 2023 population totals 241,499,431 and 2017 totals
> 207,684,626 read off `place_indicators`, as they do off the panels today.

## Phase 2 — Ingest

*Blocked by 1.2 — each source lands as its own table and then feeds the index.*
Independent of each other, so this phase parallelises.

| # | source | rows | goes to |
|---|---|---|---|
| 2.1 | Emigrants by district 2011–2024 | 2,190 | Places, Economy |
| 2.2 | Crops by district × FY | 78,425 | Places |
| 2.3 | Census entity counts, district + tehsil | 15,387 | Places |
| 2.4 | GDP growth, GVA annual and quarterly, indicators | 1,575 | Economy |
| 2.5 | Trade by country, by group, monthly totals | 75,750 | Economy |
| 2.6 | FBR tax collection by head 1992–2024 | 264 | State |
| 2.7 | Diaspora country-level: remittances, skills, destinations | — | Economy |

Each registers in `build_web_warehouse.py` with full column documentation and a
notes field saying what reconciles and what does not. Three carry known
problems that belong in the notes, not in a reader's lap:

- **Emigrants** carry `district_code` — the same PBS code the new layer uses —
  so they need no crosswalk at all. The easiest win here.
- **Crops** are on a pre-2023 frame: 123 districts, Karachi as one, none of
  Duki, Surab, Chaman, Korangi, Keamari, the Kohistans or Upper Chitral. Needs
  1.1's treatment. Two crop ids are unlabelled on the portal and one is
  sizeable — flag, do not quietly drop.
- **Trade** whole-year rows from the portal overstate: 38.3 against a published
  32.1 USD bn of exports for 2024-25. Use `FY_from_quarters` and ship
  `trade_reconciliation_fy_period` beside it. This is aggregate trade and does
  not replace `trade_hs8`.

> **Exit test.** Every new table in the catalogue with its notes; a repeat build
> byte-identical; each source's own published total reproduced from the
> warehouse, or the discrepancy written down.

## Phase 3 — Places

*Blocked by 1.2 and 2.1–2.3.*

**3.1 The picker** — one search field over 4,347 indicators with ten browsable
topics, reading `place_indicator_index`. Search matches indicator names; year,
rural/urban and sex are facets chosen after, not search results.

**3.2 The year control** — 2017 · 2023 · Change as one control over one
indicator, rather than the year being part of the topic as it is today. Change
should prefer PBS's own restated `POPULATION 2017` where it covers the
indicator — exact for all 136 districts, no arithmetic — and fall back to the
unit map elsewhere, greying what genuinely cannot be compared.

**3.3 Readable labels.** The panel currently prints
`POPULATION - 2017 / ALL SEXES · ALL SEXES`: PBS's indicator and column heading
concatenated, redundant whenever they agree. Fine as an audit trail, not as a
picker entry.

**3.4 The right panel** — headline value, change, rank, ranking list, profile
link. Today it shows the selected series and any boundary note.

**3.5 Drop `census_data.js` and `districts.json`**, subject to 0.1. This is
where the 1.8 MB comes off.

**3.6 Facilities overlay** — schools and health facilities as points over any
choropleth; fold `poverty.html` in and retire it.

> **Exit test.** Every district and tehsil indicator on the site reachable from
> one picker, on one frame, through one data path; cold first paint no worse
> than today's.

## Phase 4 — Economy and State

*Economy blocked by 0.3 and 2.4–2.5. State's Public money theme blocked only by
2.6 — everything else in State is blocked on data that does not exist yet.*

**4.1 Economy** merges `finance.html`, `money.html` and `trade.html` onto the
topic-tree and primary-card template: four themes, fifteen topics, one chart
card with its variants as a segmented control. No more stacked scrolls.

**4.2 New Economy charts** off 2.4, 2.5 and 2.7 — GDP growth 1952–2025 with the
government of the day labelled, GVA by sector, trade by partner, remittances.

**4.3 State: Public money.** The budget moves here from Economy — it reads as
the state's own accounts rather than a sector of the economy, and it is the only
State theme whose data is published today, so State stops being blocked
entirely. `budget_lines` plus 2.6.

**4.4 State: publish the four existing pipelines.** LJCP, regional police, Sindh
police, Sindh FIR all have ETL and none reaches the warehouse. This is the bulk
of State and is ingestion work, not design work.

**4.5 State: NEPRA and weather disasters** do not exist in any form. Acquisition
track, run separately, not allowed to block 4.1–4.4.

> **Exit test.** Economy and State on the same template as each other, each
> theme either populated or visibly marked as not yet collected — with the
> coverage strip the design calls for, since these series are patchy.

## Phase 5 — The analysts' shelf

*Blocked by 2 (a catalogue documents what exists) — except 1.4, already done.*

**5.1 Catalogue** with the kind facet — Boundaries · Crosswalks · Census panels ·
Place indicators · Economic series · State — and Geography pinned first, because
everything joins on its keys.

**5.2 Census panel page** — the coverage matrix of indicator × census ×
comparability, the boundary-handling rule, and the dictionary with span,
numerator, denominator and comparability.

**5.3 Query and dictionary** refreshed against the new tables.

> **Exit test.** Every table on the site has a catalogue entry a stranger could
> join from without asking a question.

## Carried over, not yet scheduled

Real work, outstanding, and easy to lose:

- **Republish the 2017 panel.** The release directory and the site both carry
  the pre-Kohistan-fix version. The fix is in the working panel, not the release.
- **Release machinery** — `build_all_2017.py`, a release manifest, an input lock
  for the 2017 draft, a CI gate. Without it a rebuild is not reproducible on
  anyone else's machine.
- **2017 sub-district residue** — 47 units undrawn where the redrawing was
  genuinely many-to-many. Correct to leave, worth revisiting if a better 2017
  boundary set turns up.
- **Census timeline 1951–2023** — the design's consistent-boundary panel. It
  needs the earlier censuses, which are not extracted. Large, and properly its
  own project.

## Risks

| risk | why it bites | what to do |
|---|---|---|
| Engine cost unmeasured | Gates phase 3, and half the point of the rebuild | 0.1, before anything depends on it |
| Second writer on the repo | Silent overwrite mid-rebuild | 0.2, before parallel work |
| Crops frame mismatch | 123 districts, wrong vintage, quietly plausible | 1.1 treatment, coverage stated per indicator |
| Trade year totals | Overstate by a fifth, and look fine | Quarters only; ship the reconciliation table |
| NEPRA and disasters absent | State looks half-built | Separate track; mark uncollected rather than leave blank |
| Scope of the timeline | Could swallow the rebuild | Out of this plan |

## Order

Phase 0 first and quickly. Phase 1 is the spine and everything visual waits on
it. Phase 2 parallelises and should start as soon as 1.2 fixes the shape. Phases
3 and 4 can run together once their data lands. Phase 5 follows 2, apart from
1.4, which is already in hand.

The shortest path to something visibly better: 0.3, then 1.1–1.2, then 2.1 —
emigrants is a day's work, joins without a crosswalk, and is a map nobody has
seen before.
