# SBP EasyData → warehouse

Source: <https://easydata.sbp.org.pk> · API docs under **Help → Developer Guide**
Pipeline: `build/build_sbp.py` · Tests: `build/test_build_sbp.py` (20 tests, offline)

## Quick start

**Dependencies: `duckdb` only.** The pipeline deliberately does not use pandas or pyarrow —
`pandas.to_parquet` requires a pyarrow version pandas approves of, and on a conda base env pip and
the interpreter can disagree about which pyarrow is installed (pip reporting 23.x while
`import pyarrow` yields 11.x, which pandas 3 rejects). DuckDB is needed anyway and writes parquet
itself, so `cmd_load` uses that. Fetching needs nothing beyond the standard library.

```bash
pip3 install duckdb
export SBP_API_KEY=your_key          # rotates every 90 days
cd data_darbar_warehouse/build

python3 test_build_sbp.py            # 27 offline tests, spends no API budget
python3 build_sbp.py catalog         # 226 requests, ~1 hr at default rate
python3 build_sbp.py plan --select   # cost the curated pull, 0 requests
python3 build_sbp.py observations --select --tier headline
python3 build_sbp.py observations --select --tier core
python3 build_sbp.py load            # parquet + attach into data_darbar.duckdb
```

`catalog` and `observations` checkpoint after **every** request. Ctrl-C, a crash,
or hitting the daily cap are all safe — just rerun the same command to resume.

## What has been extracted (complete as of 2026-08-31)

**The headline and core tiers are DONE: 1,336 series, 306,875 observations, 33 datasets**, plus the
full 226-dataset catalogue (22,974 series inventoried). Zero fetch failures. Total API spend across
both days: 1,873 requests.

Getting there took ~2.8 hours of paced fetching — the binding constraint is the **250-requests-per-hour
cap**, not the 2,000/day one (confirmed: a 429 did not clear after 2.5 minutes, whereas the separate
short-burst limiter recovers in ~50 seconds). `build_sbp.py` paces at 4/min to stay just under it.

| subject area | series | observations | span |
|---|---:|---:|---|
| Monetary & financial | 713 | 178,058 | Jun-2001 → Aug-2026 |
| External sector | 226 | 90,020 | **Aug-1947** → Jul-2026 |
| Real sector | 345 | 37,013 | Jun-2000 → Jul-2026 |
| Debt profile | 6 | 936 | Jul-2013 → Jun-2026 |
| Public finance | 43 | 657 | Jun-2007 → Jun-2025 |
| Interest rates | 3 | 191 | **Jan-1956** → Apr-2026 |

Largest single holdings: the full KIBOR/KIBID curve (18 tenors, 92,864 daily obs), weighted average
lending and deposit rates (145 / 39,295), PKR against 24 currencies back to Aug-1947 (48 / 38,624),
remittances by source country (36 / 22,211 from Jul-1972), consumer confidence (118 / 12,862),
BOP monthly summary (74 / 11,534), depository corporations (50 / 10,900).

Integrity on the finished set: 0 orphan observations, 0 duplicate (series, date) pairs, and all
25,451 NULLs carry a non-`Normal` status — none is a silent zero.

### Re-running

```bash
pip3 install duckdb
export SBP_API_KEY=...
cd data_darbar_warehouse/build
python3 build_sbp.py catalog                        # refreshes the 226-dataset inventory
python3 build_sbp.py plan --select --tier headline core
python3 build_sbp.py observations --select --tier headline core
python3 build_sbp.py load                           # idempotent; safe to re-run
cd ../../datadarbar && python3 etl/build_web_warehouse.py
```

Still unfetched by design: the `granular` tier (country-level trade and services, FDI by country,
~1,090 series) and `deep_history` (BOP FY1949-2012, ~750). Add `--tier granular deep_history` when
wanted — each is roughly another day of budget.

| what | series | obs | span |
|---|---|---|---|
| Remittances by country | 36 | 22,211 | Jul-1972 → Jul-2026 |
| Depository corporations survey | 50 | 10,900 | Jun-2008 → Jul-2026 |
| CPI, all cuts | 60 | 7,260 | Jul-2016 → Jul-2026 |
| Quarterly broad money M2 | 47 | 2,303 | Jun-2014 → Jun-2026 |
| Segment-wise advances & NPLs (partial, 33/72) | 33 | 1,947 | Dec-2010 → Jun-2025 |
| Banking asset quality | 15 | 885 | Dec-2010 → Jun-2025 |
| Central government debt | 52 | 8,185 | Jun-2010 → Jun-2026 |
| Exports/imports of goods & services | 32 | 8,096 | Jul-2005 → Jul-2026 |
| Province-wise advances & deposits | 162 | 7,290 | Jun-2001 → **Jun-2023** |
| Gold & FX reserves | 19 | 7,064 | **Jun-1948** → Jul-2026 |
| 6-month KIBOR (daily) | 1 | 5,516 | Jun-2005 → Aug-2026 |
| Auto production & sales | 14 | 3,710 | Jul-2004 → Jul-2026 |
| Electricity generation | 13 | 2,197 | Jul-2012 → Jul-2026 |
| Debt & liabilities summary | 27 | 1,782 | Jun-2009 → Jun-2026 |
| Monetary aggregates M3 | 11 | 1,463 | Jun-2006 → Jul-2026 |
| LSM quantum index | 11 | 1,320 | Jul-2016 → Jun-2026 |
| POL sales by sector | 8 | 1,240 | Jul-2013 → May-2026 |
| PKR per USD | 1 | 948 | **Aug-1947** → Jul-2026 |
| National savings schemes | 6 | 936 | Jul-2013 → Jun-2026 |
| Roshan Digital Account | 11 | 781 | Sep-2020 → Jul-2026 |
| CPI (5 cuts) | 5 | 605 | Jul-2016 → Jul-2026 |
| REER / NEER | 2 | 602 | Jul-2001 → Jul-2026 |
| Provincial receipts & expenditures | 30 | 410 | Jun-2011 → Jun-2024 |
| Quarterly GDP | 8 | 344 | Sep-2015 → Mar-2026 |
| EPU index | 2 | 331 | Aug-2010 → Jul-2026 |
| Seasonally-adjusted remittances | 1 | 253 | Jul-2005 → Jul-2026 |
| Consolidated public finance | 13 | 247 | Jun-2007 → Jun-2025 |
| Policy / repo / reverse-repo rates | 3 | 191 | **Jan-1956** → Apr-2026 |
| Real GDP + growth | 2 | 54 | Jun-2000 → Jun-2026 |

Integrity: 0 orphan observations, 0 duplicate (series, date) pairs, 1,454 NULLs all carrying
status `Missing value`. 126 catalogue rows have no observations — those are series whose dataset
metadata was fetched but whose data wasn't (CPI has 60 series, we took 5; exchange rates 48, took 1).

**Still outstanding from the core tier:** the rest of CPI (55), the other 47 currencies, the full
KIBOR/KIBID curve (17), and the larger banking datasets — weighted average lending/deposit rates
(145), consumer/business confidence (118/107), payment systems (112), BOP monthly summary (74),
segment-wise NPLs (72), depository corporations (50), quarterly M2 (47).

### Verification performed

- **Public finance identities hold exactly** (FY2024-25): revenue − expenditure = fiscal balance;
  fiscal balance + interest = primary balance; revenue − current expenditure = revenue balance;
  tax + non-tax = total revenue (±1 on rounding). Fiscal deficit −5.41% of GDP.
- **Goods & services balance** = (32,343 + 8,450) − (59,146 + 11,286) = −29,639 Mn USD, exact.
- **Province-wise deposits** match values read independently off the EasyData HTML table
  (Punjab 11,415,544.11; Sindh 7,600,035.10; all-Pakistan 24,683,416.30 Mn PKR).
- **GDP** reconciles with `national_accounts` (39,777,011.25 vs 39,777,010).

## Where it lands in the app

Two places: the **Monetary & External** page (`app/money.html`) and the SQL console.

### Monetary & External — the sixth product page (was "Money & Prices", renamed 2026-09-02)

Five topics: the rupee, prices, interest rates, external balance, money & banks.
Twelve charts, built with the same sidebar/topic/hash conventions as `finance.html`.

Data comes from `etl/build_money.py`, which selects the ~32 series the page charts and
emits `app/data/money_data.js` (`window.DD_MONEY`, **203 KB**) following the
`poverty_data.js` convention. It deliberately does *not*:

- ship the whole warehouse — of 1,336 series only 32 are charted;
- read parquet in the browser — `query.html` pulls DuckDB-WASM from jsDelivr for that,
  which is several MB of engine before a pixel is drawn. Fine for a SQL console, absurd
  for a line chart. A `<script>` payload also keeps the page working under `file://`,
  like every other chart page here.

For comparison, `finance.html`'s bundle is 2.8 MB.

Daily KIBOR is thinned to month-end for the page (5,516 points × 5 tenors is invisible
detail at 1,100px); the console still has the full daily series.

Two data-integrity guards live in the build:
- the remittance partition is asserted against SBP's published total, so the build fails
  rather than shipping the 42% double-count;
- `remit_months` records how many months each fiscal year covers, so the page defaults to
  the last **complete** year. Without it the page opened on FY2026-27 — a single month —
  and showed $3.6bn where the prior year was $41.6bn, reading as a 90% collapse.

Rebuild after any refresh:

```bash
python3 etl/build_money.py            # after build_sbp.py load
```

#### Treemap drill-down (added 2026-09-01)

Every tile of the current-account treemap can be clicked open **in place** (nested
treemap — Hib rejected the earlier zoom-to-new-view because it lost the dollars-in /
dollars-out context). `bopOpen` is a Set of open paths; heading click closes a subtree; `Esc`
collapses all; deep link `bz=gx.1|gm.2.0`; CSV exports every open top-level tree. Children
areas are scaled to the parent's headline value (goods: through-banks total ≠ BOP by freight,
tooltip says so). `build_money.py::build_drill` assembles the trees:

| tile | source | levels | identity checked |
|---|---|---|---|
| goods exports / imports | `BOP_XRECCG_M` (75) / `BOP_MRECCG_M` (72) | up to 4 | groups = through banks; − freight = fob; + other = BOP total = BPM6 headline (±$1.5m, every FY) |
| services exports / imports | `BOP_XMGS_M` | 1 (12 types) | types = total = BPM6 headline |
| remittances | `BOP_WR_M` partition | 1 (15 sources) | = BPM6 P00190 within 2% |
| other transfers in | `BOP_BPM6SUM_M` P00150–P00210 | 2 | official + remittances + personal + other = P00140 |
| income paid abroad | `FI_REPATSEC_M` FI-per-sector + residual | up to 4 | sectors = PRS0010; residual (interest & other) = P00110 − PRS0010 ≥ 0 |

The sector list is nested too (Power = Thermal + Hydel + Coal; Communications ⊃ Telecom, IT ⊃
Software/Hardware/IT Service; Automobiles ⊃ Cars/Buses/Motorcycles; Electronics ⊃
Consumer/Industrial) — the naive sum of the 50 FI series is 28% above SBP's total, so
`infer_tree` is applied there as well. FY2024-25: income paid $9.8bn = $7.6bn interest &
other + $2.2bn repatriated profits (Financial $385m, Power $399m of which coal $181m).

**The commodity hierarchy is inferred, not published.** SBP lists the 70/67 series in
document order with no level marker ("Food Group, Rice, Basmati rice, rice Other than
Basmati, Fish…"). `infer_tree` treats a series as a parent when the top-level items
after it sum to it in every fiscal year — memoised recursive descent; without the
cache it is exponential and a 120 s timeout looked like a hang. It recovers
Transport Group › Road Motor Vehicles › CKD › Motor Cars (CKD) exactly as the names
imply, and the build refuses to ship if it finds fewer than 4 groups / 20 members.

**Goods tiles are on a different basis from the headline.** The commodity tables are
receipts/payments *through banks*; the BOP figure deducts freight (exports ~$0.9bn,
imports ~$2bn) and adds "other" flows. The zoomed header shows both numbers rather
than rescaling tiles to force them to add up.

**Bridge files.** The monthly commodity and repatriation series are in the `granular`
tier and not yet in the parquet. Until `build_sbp.py observations --tier granular` is
run locally, FY totals exported from the browser sit in
`sbp_state/raw/fytot__goods.json` (147 series, thousand USD, sha256 `de915dd2…7d902`)
and `fytot__repat.json`; `build_drill` prefers parquet when it has rows and falls back
to the bridge, so the files become redundant automatically.

### The SQL console

```bash
# in the datadarbar repo, after build_sbp.py load has run
python3 etl/build_web_warehouse.py
```

That picks up `sbp_series_catalog.parquet` and `sbp_observations.parquet` from this
folder, writes them into `app/data/warehouse/`, registers both in `catalog.json`
with full column documentation, and appends seven worked example queries to the
ones `query.html` already offers. If the two files aren't there yet the step prints
`sbp… skipped` and everything else builds as before — so it's safe to run at any time.

The examples added are: the rupee since 1947; the real policy rate (an `ASOF JOIN`,
because the policy rate only moves when SBP meets and can't be joined month-to-month
against CPI); remittances by source country; urban vs rural food inflation; deposits
by province; a coverage/staleness audit of the catalogue; and an SBP-vs-PBS GDP
reconciliation.

**Reserved for later:** a sixth page, *Money & Prices*, for the monetary and external
material — the rupee, inflation, rates, reserves, BOP, banking. Deliberately not part
of `finance.html`, which is annual, structural, real-economy and PBS/MoF-sourced;
this data is monthly-to-daily and monetary. Note also that `finance.html` loads a
2.8 MB pre-baked `econ_data.js` bundle — a new page should read the parquet warehouse
through `warehouse.js` instead of growing a second bundle of that size.

Two smaller placements worth doing whenever the page happens: province-wise advances
and deposits as a 9-region layer on `map.html` (the only mappable SBP data), and
remittances by source country beside export destinations on `trade.html`.

## The constraint that shapes everything

There is no bulk-data endpoint. **One HTTP request per series**, and:

| limit | value | source |
|---|---|---|
| daily | 2,000 requests | documented |
| hourly | 250 requests | documented |
| burst | ~35 in ~30s → HTTP 429, recovers after ~50s | observed 2026-08-16 |

A request returns a series' *entire* history when `start_date`/`end_date` are
passed (verified: 649 monthly observations back to Jul-1972 in one call), so
cost is flat at 1 request per series regardless of series length.

Default pacing is 4 requests/minute (240/hr) with a burst of 3 and exponential
backoff on 429. `--daily-budget` defaults to 1900, leaving headroom under the
2,000 cap so the key stays usable for ad-hoc queries the same day.

## How big is it

226 datasets. A survey of 36 of them (16%, mostly External Sector, which is the
most granular) found **5,965 series** — mean 166/dataset. Extrapolating with
allowance for the much smaller Public Finance and Social Sector datasets, expect
roughly **20,000–30,000 series total**, i.e. **10–15 days** of daily API budget
for a complete mirror. Run `build_sbp.py plan` after `catalog` for the real number.

Largest surveyed: SCRA daily position by country (495), FDI abroad by economic
group (408), FPI equity by economic group (408), FPI by country (384), FDI abroad
by country (336), BOP FY2003-12 (336), inward direct investment positions (290).

## Curated selection (`build/sbp_selection.json`)

Four tiers, sized against measured series counts rather than guesses. Select with
`--select [--tier NAME ...]`; omit `--tier` to take everything in the file.

| tier | series | cost | what it is |
|---|---|---|---|
| `headline` | ~18 | minutes | Exact keys for a macro dashboard: PKR/USD, REER/NEER, policy rate, 6M KIBOR, CPI (national/urban/rural/food), remittances, real GDP + growth, EPU |
| `core` | ~1,320 | <1 day | The full small/medium datasets behind those headlines, so breakouts don't need another run |
| `granular` | ~1,090 | ~1 day | Trade and services by country, FDI by country — for treemaps and flow maps |
| `deep_history` | ~750 | <1 day | Balance of payments FY1949→2012 and GDP at 1980-81 factor cost, 1950-2000 |

`headline` + `core` is the recommended first pull: one day of budget, and enough
for every chart finance.html plausibly needs.

Picks worth knowing about:

- **`TS_GP_IR_SIRPR_AH.SBPOL0030`** — policy rate, as-needed since **Jan-1956**.
  The longest machine-readable Pakistani policy-rate series I'm aware of.
- **`TS_GP_ER_FAERPKR_M.E00220`** — average PKR per U.S. Dollar, monthly from
  **Aug-1947**. The dataset carries 48 series (24 currencies × level and
  appreciation/depreciation); `core` takes all of them.
- **`TS_GP_PT_CPI_M`** — 60 series. The urban-vs-rural *food* CPI gap
  (`P00041516` vs `P00051516`) is a better story than headline CPI alone.
- **`TS_GP_BOP_WR_M`** — 36 series, remittances by source country from Jul-1972.
  The one `core` dataset with an obvious map/treemap rendering.
- **`TS_GP_ES_SEREXP_M` / `SERIMP_M`** (in `granular`) — services trade by
  country. Genuinely absent from the PBS customs data in `trade_hs8`.
- **`TS_GP_BAM_ADVDEP_HY`** — 162 series, the only sub-national dataset worth
  mapping (9 regions × 9 borrower/depositor categories). Stops Jun-2023.

Deliberately excluded: `TS_GP_BAM_LPVTFIN_M` (324), `TS_GP_BOP_SCRACOU_M` (495),
`TS_GP_ES_FDIABRDEG_Y` / `FPIEIBYEG_Y` (408 each) and the other outward-investment
matrices — high request cost, thin analytical payoff for this site. Add them later
with `--dataset` if a specific question needs them.

## Schema

`load` writes `sbp_series_catalog.parquet` and `sbp_observations.parquet`, then:

| object | kind | notes |
|---|---|---|
| `sbp_series_catalog` | table | one row per series: name, unit, frequency, coverage, description, subject area |
| `sbp_observations` | **view over parquet** | `series_key, dataset_code, obs_date, value, status, comment` |
| `sbp_series` | view | the two joined — the one you normally want |
| `sbp_annual_fy` | view | adds `fiscal_year` for June-ending annual observations |

`sbp_observations` is parquet-backed for the same reason as `trade_hs8`: the file
will outgrow the 20 MB `device_commit` cap. **Open DuckDB from inside the
warehouse folder** or the relative path won't resolve.

## Getting data out of the browser

The sandbox shell is blocked from `easydata.sbp.org.pk` by a proxy allowlist, and `web_fetch`
truncates bodies over ~20 KB. Anything larger has to come through the Browser pane, and that
channel is **hand-copied text — it is not lossless**. Roughly one single-character corruption
per 4 × 13,000-char slices, plus occasional silent truncation of long strings.

So every transfer goes through a SHA-256 gate, and the payload is condensed first:

1. Fetch into page memory, then **condense** — factor descriptions and dates into lookup tables
   and pack observations as `dateIdx:value|status` tokens. One dataset went 1.9 MB → 114 KB
   with no information loss.
   A second, bigger win (**v2**, `"v": 2` in the payload): scale each series' values to
   **exact integers** by 10^dp and store **deltas**. Slowly-varying series then become runs of tiny
   numbers that gzip loves — 46% smaller than v1 on a debt dataset, more on rate series. The scaling
   is done with string arithmetic, so there is no float drift; encoder and decoder were round-trip
   tested on 9,167 real values and 60 whole series before use.
2. gzip → base64 → slice, computing a SHA-256 per slice **in the browser**.
3. Copy each slice out in **1,000-character pieces**, verifying each piece's hash before moving on.
   2,000 still flips characters occasionally; 13,000 in one shot truncates outright.
4. Reassemble, then check base64 hash, decoded-JSON hash, and structure counts.

`build/sbp_state_from_raw.py` reads the resulting `cond__*.json` (and verbatim `meta__`/`data__`/
`bulk__`) files back into pipeline state. If a helper is defined in the page, give it a unique name —
parallel agents sharing one tab have collided on `window.__p2` and silently got the wrong granularity.

## Gotchas found while building this

- **Two date formats.** The `/data` endpoint returns ISO (`2026-07-31`); the
  `/meta` endpoint returns `31-Jul-1972`. Both are handled.
- **Fiscal years are stamped with their end date.** FY2023-24 → `2024-06-30`.
  Verified against `national_accounts`: GDP at constant basic prices 2015-16 for
  2023-24 is 39,777,011.25 Mn PKR in EasyData and 39,777,010 in the warehouse.
  Not every annual series is fiscal though — population and literacy are calendar
  years, so `sbp_annual_fy.fiscal_year` is NULL for those by design.
- **Suppressed values arrive as `"-"`** and must become NULL, not 0. The
  `Observation Status` column ("Normal", "Not available", …) is preserved.
- **Numbers can be bare `.5`** with no leading zero, and use thousands separators.
- **Expired keys return 401**, not a friendly message. The pipeline turns that
  into an explicit "regenerate your key" error.
- **Some responses are not valid JSON**, in two distinct ways, and both are handled by
  `decode_payload()`:
  1. *Raw control characters* inside string literals — `TS_GP_RL_CCSIND_M` fails with "Bad control
     character in string literal" at position ~39,602. `strict=False` covers this.
  2. *Unescaped double quotes* inside string values — the CPI moving-average series return
     `"The observation status is missing "MIS" due to trimmed inflation..."`. **No conforming parser
     accepts this and `strict=False` does not help**; it needs the quote repair in
     `repair_inner_quotes()`, which treats a quote as a terminator only when the next non-space
     character is one of `, ] } :`. This hit 16 CPI series and killed a whole fetch loop before
     it was handled.
- **A series in a pack with no observations is not "fetched."** Packs carry catalogue rows for series
  whose data was never downloaded; recording those in `checkpoint.json` as done makes `build_sbp.py`
  skip them permanently. `sbp_state_from_raw.py` rebuilds `obs_done` from raw rather than unioning,
  and `test_sbp_state.py` guards it.

## What this source will *not* do

**There is no district-level data** — zero datasets match "district". The finest
geography is province/region (Punjab, Sindh, KPK, Balochistan, Islamabad, GB,
FATA, AJK), in only 5 datasets, and the two SBP ones stop at **Jun-2023**:

- `TS_GP_BAM_ADVDEP_HY` — province-wise advances & deposits, 162 series, half-yearly Jun-2001→Jun-2023
- `TS_GP_BAM_DISTUTI_HY` — province/region-wise disbursement & utilisation, Dec-2010→Jun-2023
- `TS_GP_PF_PRES*_Y` — provincial receipts & expenditures (MoF), annual 1982→2024

So this feeds the finance/economy time-series pages, not the district choropleths.

## Overlap with what's already in the warehouse

| EasyData | warehouse | relationship |
|---|---|---|
| Import/export by HS2 and by country, monthly | `trade_hs8` | **payments basis** vs PBS **customs basis** — different measures, useful cross-check |
| GDP at constant basic prices 2015-16 | `national_accounts` | same PBS source, values match exactly |
| Federal/provincial receipts & expenditures | `budget_lines` | MoF actuals vs Budget-in-Brief estimates |
| LSM quantum indices, all base years | `lsm_qim`, `lsm_sector_indices` | same PBS source |

Genuinely net-new: exchange rates from Aug-1947 (daily and monthly, PKR and USD
legs, NEER/REER), country-wise remittances monthly from Jul-1972, policy rate
from Jan-1956, FX and gold reserves from Jun-1948, full BOP back to FY1949,
FDI/FPI by country and ISIC sector, banking-sector FSIs and NPLs, payment
systems, Roshan Digital Account, the EPU index, and every CPI base-year vintage
from 1959-60 onward.
