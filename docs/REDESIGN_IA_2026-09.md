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
- Public finances: The federal budget
- Trade: What Pakistan trades · What's growing and shrinking · Trading partners
- Prices & money: The rupee · Inflation · Interest rates · Money & banks
- External balance: Reserves & the current account · Remittances
Main: topic title + one-line dek; toolbar Share · CSV · Notes; ONE primary chart card with its
variants as a segmented control, year slider inside the card; "Also in this topic" = small cards
that swap into the primary slot. No long stacked scrolls. Phone: tree collapses to one
"theme › topic ▾" button; same card below.

## State (new explorer; same template as Economy)

- Justice: Case flows · Pendency & disposal · What the courts hear · Judges & staffing (LJCP)
- Crime & policing: Reported offences (by province/range) · FIRs registered
- Energy: Power plants · Distribution companies (NEPRA)
- Public services: Schools · Travel time to care · Weather disasters
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
5. Census panel mode on the map + its catalogue page, once the harmonised panel certifies indicators.
6. State explorer, wiring LJCP / police / NEPRA / schools / health access / disasters.

## Open questions

- Fold External balance into Trade? (small theme on its own)
- Are "Also in this topic" cards enough, or do some topics need two charts on screen?
- Does Public services belong in State, or are those purely Places topics?
- Name for the case-category layer ("What the courts hear").
