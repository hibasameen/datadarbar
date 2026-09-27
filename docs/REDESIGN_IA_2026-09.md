# Data Darbar redesign — information architecture (agreed 27 Sep 2026)

Design canvas (mockups, 11 artboards): https://claude.ai/artifact/StSRFF6wMqL92izqYWjrCk
Status: agreed direction, not built. This file is the spec to build from.

## Why

The live site is organised by data source (PBS national accounts → GDP & Budget, PBS trade → Trade
Atlas, SBP → Monetary & External), so six products sit as peers behind one "Data ▾" dropdown, three
chart pages each stack 8–12 charts and need a "Getting around" banner, the map hides 14 topics behind
three nested dropdowns, and two CSS worlds (styles.css vs inline) let chrome drift. New data (LJCP
courts, police, NEPRA, schools, census panel, boundaries) has no home in that structure.

## Top-level structure

Header, every page:  Places · Economy · State  |  Catalogue · Query · Methods
Footer: © · licences · sources (PBS ↗, SBP ↗) · Adaad · Aiwan-e-Jamhoor
About + Methodology + Contact collapse into one Methods page. The "Data ▾" dropdown goes.
Landing: hero, three explorer cards (green / gold / teal), then a "For analysts" shelf
(Catalogue · Query · Dictionary). No other sections.

Routing rule: anything with a district or tehsil key appears as a Places topic. Time series by
sector live in Economy; time series by institution (courts, police, utilities, services) live in
State. A series can have two doors (e.g. court pendency: State chart + Places map) but one table.

## Places (map)

Left panel, top to bottom:
- Geography toggle: Districts · Tehsils · Census panel (consistent-boundary units).
- Indicator: one searchable field (replaces Topic → Dataset → Indicator), with a browsable topic
  list beneath (Demographics, Education, Employment, Household welfare, Poverty & wealth, Housing &
  infrastructure, Health & children, Schools & access to care, Rural facilities (Mouza 2020),
  Satellite & environment). Poverty & Wealth stops being a separate page.
  **The field searches 4,347 indicators** — see "How many indicators" below. It searches indicator
  names; the facets of the one you pick (census year, rural/urban, sex) are chosen after, not
  searched through.
- Facilities on the map: overlay toggles (Government schools 121k, Health facilities). Points draw
  over any choropleth; solid dot = school-level fix, hollow = placed at centroid (coord_precision);
  cluster at national zoom, resolve to points past tehsil zoom.
Map: indicator legend + year control (2017 · 2023 · Change) live on the map, bottom-left; district
search + province filter top-left; Share/CSV top-right.
Right panel: selected unit, headline value, change since previous census, rank, ranking list,
"Full district profile →"; with a facilities overlay on, counts by type for the selected unit.

Census panel mode: the year control becomes a timeline 1951 · 1961 · 1972 · 1981 · 1998 · 2017 ·
2023. A "Compare across" span (2017–23 / 1981–23 / 1951–23) sets the frame: longer span → fewer,
larger consistent units; every unit exists in every census shown. Per indicator each census dot is
solid (certified comparable), hatched gold (published, definition differs — drawn hatched on the
map, starred on the trajectory, never compared) or hollow (not in that census). Right panel shows
the unit's trajectory and biggest-change ranking over the selected span.

## Economy (one explorer; merges finance.html, money.html, trade.html)

Sidebar = search + topic tree grouped by question:
- Output & growth: Structure of GDP · Composition of growth · Industry & factories · How sectors feed each other
- Trade: What Pakistan trades · What's growing and shrinking · Trading partners
- Prices & money: The rupee · Inflation · Interest rates · Money & banks
- External balance: Reserves & the current account · Remittances
Main: topic title + one-line dek; toolbar Share · CSV · Notes; ONE primary chart card with its
variants as a segmented control, year slider inside the card; "Also in this topic" = small cards
that swap into the primary slot. No long stacked scrolls. Phone: tree collapses to one
"theme › topic ▾" button; same card below.

## State (new explorer; same template as Economy)

- Public money: The federal budget · What the state collects (FBR tax by head, 1992-2024)
- Justice: Case flows · Pendency & disposal · What the courts hear · Judges & staffing (LJCP)
- Crime & policing: Reported offences (by province/range) · FIRs registered
- Energy: Power plants · Distribution companies (NEPRA)
- Public services: Schools · Travel time to care · Weather disasters

The budget moved here from Economy (27 Sep, agreed). It reads better as the state's own accounts
than as a sector of the economy, and it has a second effect worth naming: it is the only State theme
whose data is already published, so State stops being blocked entirely on ingestion. `budget_lines`
is in the warehouse today; FBR tax collection arrived in the 27 Sep pull.
Adds a coverage strip under the title (which years exist, what is scanned/unread, partial periods),
because these series are patchy; interpolated stretches are drawn dashed. "By district, on the map"
card hands off to Places.

## Catalogue (analyst shelf)

Index gets a kind facet — Boundaries · Crosswalks · Census panels · Place indicators · Economic
series · State — and a Geography section pinned first (everything joins on its keys).
Geography datasets: PBS Digital Census 2023 boundaries (8 provinces/territories, 38 divisions,
157 districts, 649 tehsils, nested by code; GeoJSON, TopoJSON, GeoPackage, Parquet/WKB, spatial SQL)
plus crosswalks: Census 2023 units → PBS polygons (591); Census 2017 → 2023 districts + consistent
-unit frame; legacy frames (geoBoundaries ADM2/ADM3, COD-AB) → PBS 2023; Mouza tehsils → polygons;
survey p-codes → districts. Every crosswalk row carries method, evidence, status (reviewed /
unreviewed). Boundaries page centres on a "which key joins what" table: pbs_code, dd_id, dds_id,
dk, adm3_pcode.
Census panel page: coverage matrix (indicator × census × comparability state), boundary-handling
rule, crosswalk link, dictionary with span / numerator / denominator / comparability fields.

## Geography decisions

- Tehsils already draw on PBS Census 2023 polygons (Sep 2026). Move districts onto the same PBS
  layer (157) so province › division › district › tehsil nest by code; re-key districts.json, DHS,
  PSLM, HIES, LJCP, police once via crosswalk. Divisions kept in reserve, not in the toggle.
- Census panel: consistent-boundary units (IPUMS-style), computed from the crosswalk as the
  coarsest common partition for the chosen span; counts summed, rates recomputed from summed
  numerator/denominator. 2023 PBS is the anchor frame.
- Open: does `dk` survive as a stable alias or retire after one release with the crosswalk?

## Visual system

Keep the existing vocabulary: green #0c3a1e header with gold #d4a017 rule, Inter, cream ground.
ONE background for the whole site (#faf7ef), white only for panels and cards; explorers told apart
by illustration colour only (Places green, Economy gold, State teal #0f6e78). One shared stylesheet
with the ground colour as a variable replaces the two CSS worlds.

## Build order (proposed)

1. Shared shell: one stylesheet, header (nav.js VIEWS → Places/Economy/State + shelf), footer, Methods page.
2. Economy: merge finance/money/trade into one page on the topic-tree + primary-card template.
3. Places: indicator picker, geography toggle, facilities overlay; fold poverty.html in.
4. Districts onto PBS 2023 layer; publish boundaries + crosswalks in the catalogue with the kind facet.
   **Step 4's boundary half is done** (27 Sep) — see below. The catalogue half is not.
5. Census panel mode on the map + its catalogue page, once the harmonised panel certifies indicators.
6. State explorer, wiring LJCP / police / NEPRA / schools / health access / disasters.

## Open questions

- Fold External balance into Trade? (small theme on its own)
- Are "Also in this topic" cards enough, or do some topics need two charts on screen?
- Does Public services belong in State, or are those purely Places topics?
- Name for the case-category layer ("What the courts hear").

## Built so far

### The single frame (27 Sep 2026)

Step 4's boundary work landed ahead of the rest, because every other part of
Places rests on it: a year control offering 2017 · 2023 · Change is a promise
that the two years describe the same places, and until now they did not.

Both censuses are now drawn on PBS's Digital Census 2023 boundaries, with the
per-unit decisions recorded in `etl/census2017/census_unit_map.csv` and
explained in `docs/stage2/census2017_crosswalk.md`. What changed:

| | before | after |
|---|---|---|
| District shapes, 2023 | 128 of 136 | **136** |
| District shapes, 2017 | 123 | **136** |
| Sub-district units drawable, 2017 | **0** | 489, on 518 shapes |
| Sub-district shapes, 2023 | 591 | 591 |
| Mappable census series | 34,008 | **37,971** |

The 2017 sub-district data had never been drawable at all — no map key of any
kind — so that layer is new rather than improved. `districts_2023_geo.js` is
the district equivalent of the tehsil layer, 157 shapes at 0.35 MB: the 136 in
the census, plus Gilgit-Baltistan, AJK and Occupied Kashmir drawn uncoloured,
because the country does not stop at the census frame.

Four census layers are live on map.html (2017 and 2023 × districts and
tehsils), each reading its series on demand in 20–90 ms. Totals check out
against PBS: 207.7M for 2017, 241.5M for 2023.

### What Places still needs

The map now has the data and the frame. The page in the mockup does not exist
yet:

- **The picker.** Still Topic → Group → Indicator, three selects, with census
  tables as groups. The design calls for one search field over a browsable
  topic list. The index that would drive it is built and small (0.16 MB).
- **The year control.** 2017 · 2023 · Change as one control over a single
  indicator. Today the year is part of the topic, so comparing means switching
  layers and losing the series. **Change** should prefer PBS's own restated
  POPULATION 2017 column where it covers the indicator — exact for all 136
  districts, no crosswalk arithmetic — and fall back to the unit map elsewhere.
- **Series labels.** The panel prints `POPULATION - 2017 / ALL SEXES · ALL
  SEXES`: PBS's indicator and column heading concatenated, redundant whenever
  they agree. Fine for an audit trail, not for a picker.
- **The right panel.** Currently the selected series and any boundary note.
  The design wants headline value, change, rank, ranking list and a profile
  link.
- **Topics.** Census tables are not topics. Table 14 is "literate population
  10+ by level of educational attainment"; a reader wants "Education".

### How many indicators (settled 27 Sep)

The mockup said "Type to search 240 indicators". The real figure is **4,347**.
The gap was not an error so much as a count of the wrong thing: the mockup
counted the hand-written vocabulary as it stood, and the census had not yet
been countable at all.

| | count | what it is |
|---|---|---|
| Curated | 296 | the hand-written vocabulary, across 50 layers: PSLM, LFS, MPI, HIES, schools, health access, Mouza, satellite, and the derived census panel |
| Census | 4,051 | PBS table × indicator × column heading — the distinct cell definitions in the two censuses |
| **Searchable total** | **4,347** | what the field searches |
| Mappable series | 37,971 | the census side once locality and sex are chosen |

**Why table × indicator × column heading, and not something coarser.** PBS's
`indicator` on its own is frequently `MALE`, `ALL SEXES` or `ALL AGES` — 789
table×indicator pairs collapse to 482 distinct labels, so a list of labels
would be mostly repeats with no subject in them. The column heading is what
carries the definition. Coarser still would be the 41 tables, which is a good
browsing unit but not a searchable one.

**Why not 37,971.** 33,147 of those series are rural/urban and sex variants of
something already in the list. Searching series would return the same
indicator forty times and bury everything else. The facets belong on the
chosen indicator, as controls.

**Built 27 Sep, and one of the design's assumptions did not survive it.**
`place_indicator_index` now holds one row per indicator per level - 5,431 rows,
4,349 distinct indicators, which is the 4,347 above plus the two Migration
series. Locality and sex are collected into lists on each row, as the design
asks.

The year could not be. The design says "the facets of the one you pick (census
year, rural/urban, sex) are chosen after" - but of 4,039 district cell
definitions, **34 exist in both censuses**. The two censuses did not publish the
same tables, so for 99 per cent of census indicators the year is not a choice
about the indicator; it is part of what the indicator is. The index records the
years each definition actually has, and the picker must offer those rather than
assume two. The 2017 . 2023 . Change control still works as designed for the
curated survey indicators, which are built as pairs, and for population, which
PBS restates itself.

**Census is not a topic.** It was one in the first build, which filed 37,971
series under a single heading and reproduced the source-oriented structure this
redesign exists to remove. Census tables are now assigned by what they measure,
in `etl/places/census_topics.py`, and both halves of the index read one topic
vocabulary from `etl/places/topics.py` - the curated side had been carrying
app.js's twelve topics, whose labels nearly but not quite matched the design's
("Health" and "Children" against "Health & children"), so the picker showed
both. Topic counts, with census mixed in:

| topic | curated | census | total |
|---|---:|---:|---:|
| Demographics | 20 | 1,586 | 1,606 |
| Housing & infrastructure | 18 | 1,373 | 1,391 |
| Education | 44 | 1,281 | 1,325 |
| Health & children | 41 | 713 | 754 |
| Employment | 53 | 124 | 177 |
| Household welfare | 18 | 56 | 74 |
| Rural facilities (Mouza 2020) | 54 | - | 54 |
| Schools & access to care | 32 | - | 32 |
| Poverty & wealth | 10 | - | 10 |
| Satellite & environment | 6 | - | 6 |
| Migration | 2 | - | 2 |

Topic counts, on the same basis, summing to 4,347:

| topic | curated | census | total |
|---|---:|---:|---:|
| Demographics | 20 | 1,362 | 1,382 |
| Education | 44 | 746 | 790 |
| Employment | 53 | 115 | 168 |
| Household welfare | 18 | 29 | 47 |
| Poverty & wealth | 14 | – | 14 |
| Housing & infrastructure | 18 | 1,318 | 1,336 |
| Health & children | 41 | 481 | 522 |
| Schools & access to care | 32 | – | 32 |
| Rural facilities (Mouza 2020) | 54 | – | 54 |
| Satellite & environment | 2 | – | 2 |
| **Total** | **296** | **4,051** | **4,347** |

Housing is the surprise: 16 of PBS's 41 tables are housing tables, almost all
2017-only, and they carry more distinct cell definitions than education does.
Worth knowing before the topic list is drawn — "Housing & infrastructure" is
not a small topic.

Also corrected on the canvas: tehsils are **650** in PBS's layer, not 649, of
which 591 are inside the census frame; and a census rank is out of the 136
districts with census data, not the 157 drawn.

## One data path for Places (decided 27 Sep)

**Decision: Places reads everything from the warehouse.** Today it has two paths, and the curated
half is the expensive one.

Measured on map.html, first paint, own files only:

| | today | warehouse |
|---|---|---|
| `census_data.js` (data + district geometry, blocking `<script>`) | 1,826 KB | — |
| `district_indicators.parquet` (the same numbers) | — | 170 KB |
| everything else (css, js, logo) | 94 KB | 94 KB |
| **first paint** | **1,920 KB** | **~264 KB + engine** |

The engine is the only thing that moves *earlier*, and it is not a new dependency: the census
layers already load it, and the redesign puts them at the centre of Places. Its JS shim and Arrow
come to ~220 KB decoded; the WASM module is instantiated inside a Worker, so it does not appear in
the page's resource timing and was not measured here. That is the number to check before shipping.

**Byte ranges work on the live host.** A `Range: bytes=0-99` on
`darbar.adaad.org/data/warehouse/trade_hs8.parquet` returns **206** with `content-length: 100`, so
the 8 and 9 MB census panels are never fetched whole in production — only the row groups a query
touches. Locally they *are* fetched whole, because `python3 -m http.server` answers a range request
with `HTTP/1.0 200` and the full length; `probeRanges()` sees that and correctly falls back. Any
local timing of the panels is therefore meaningless — measure on the deployed site.

### What the migration actually involves

`district_indicators.parquet` is not a drop-in for `census_data.js`:

- it holds 231 indicators × 147 districts against the map's 296 curated indicators;
- it is district-only, so the tehsil layers (Mouza, poverty, satellite, school access) have no
  equivalent and need building;
- it is keyed on the 147-district 2015 frame and has to be re-keyed to the PBS 2023 district codes
  the census layers now use — the unit map already does this for the census, and the same treatment
  applies here;
- the district geometry rides inside `census_data.js` today and becomes `districts_2023_geo.js`
  (350 KB), which stays a plain file: geometry is not something to query.

**Hold it to this test:** cold first paint on the deployed site, before and after. If the WASM boot
is material, the map should draw its geometry uncoloured and fill values in when the engine is
ready, rather than waiting.

## New data, pull of 27 September

~79 MB under `raw_data/pbs_insight_explorer/`, **none of it in the warehouse yet**. Four portals,
each with a README recording what reconciles and what does not.

**Places** — all three are district-keyed and join the PBS 2023 frame directly:

| source | grain | size |
|---|---|---|
| `diaspora_…/diaspora_emigrants_by_district_2011_2024.csv` | district × year 2011–2024 + overall; 146 districts, 9.86M registered emigrants | 2,190 rows |
| `national_accounts_…/crops_district_fy_long.parquet` | crop × FY × district: area, production, yield. 108 crops with data, 123 districts, majors from 1981-82 | 78,425 rows |
| `economic_…/entities_ds.json`, `entities_th.json` | 24 enumerated unit types (school, hospital, factory, mosque, police station…) by district and by tehsil | 3,298 + 12,089 rows |

The emigrant file carries `district_code` — the same PBS code the new district layer uses — so it
needs no crosswalk. Crops are on a pre-2023 frame (Karachi as one district; no Duki, Surab, Chaman,
Korangi, Keamari, the Kohistans or Upper Chitral), so it needs the unit map's treatment.

**Economy**: GDP growth 1952–2025 with the government of the day labelled (74 rows); GVA by
sector/subsector annual 2000–2026 (594) and quarterly 2016–2025 (880); GDP, NPI, per-capita income
and the exchange rate (27); trade by partner country × FY × period (71,795 rows, 231 partners), by
commodity group (3,680) and monthly totals 2003–2026 (275). The trade README is explicit that the
portal's own whole-year rows overstate — 38.3 against a published 32.1 USD bn of exports for
2024-25 — and ships `trade_reconciliation_fy_period.csv` to check any year total against. Read it
before wiring the charts. This is FY/period aggregate trade and does not replace `trade_hs8`.

**State**: `fbr_tax_collection_by_head_1992_2024.csv`, tax type × head × subhead, 264 rows.

Also in the economic pull: `derived/census2023_units_to_pbs_geojson.csv`, which is what the
sub-district frame was built from, and `cod_vs_pbs_district_iou.csv` comparing PBS's polygons with
the COD-AB digitisation — median IoU 0.90, 5th percentile 0.62. They are different digitisations,
not the same lines.

