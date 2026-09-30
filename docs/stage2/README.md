# Stage 2: the full Census 2023 table set at district and tehsil level

> **Status, 26 September 2026 — built locally, nothing deployed.** All 28 cross-tabulated
> tables are extracted at district and tehsil level: **4,599,521 observations**, 136
> districts and 591 sub-district units, 629 named urban localities, **423,345 of 423,345
> closure checks passing**, and a byte-identical repeat build. The public site is unchanged.
>
> - [**build_report.md**](build_report.md) — what was produced, what was verified, and the
>   defects found in the published census
> - [**STAGES.md**](STAGES.md) — the stage-by-stage plan and where each one stands
> - [**localities_report.md**](localities_report.md) — tables 31–34: 71,599 places, 47,316 mauzas
> - [**pbs_queries.md**](pbs_queries.md) — eleven questions for PBS, drafted not sent
> - [verification.json](verification.json) — release identity and verification record
>
> The estimates in §5 below were written before any code existed. They were wrong: Option A
> was costed at 24–37 person-days and took an afternoon, and the full set followed the same
> day. They are left as written, with the revisions marked, because the size of the error is
> itself worth recording — the parsing was never the cost, and what the closure checks
> *found* was.

---

Drafted 26 September 2026. This plan covers ingesting every statistical table PBS publishes
for Census 2023, at both district and sub-district level, and turning them into a
documented research panel in the Data Darbar warehouse.

It builds on [Stage 1](../stage1/README.md), which registered the project's sources and
rebuilt population and educational attainment from the original PDFs, and on the
[geography register](../stage1/geography/README.md), which defined 129 cross-year district
comparison areas. Stage 1 deliberately stopped short of a longitudinal panel; this is the
next increment.

**Two source pages matter, not one.** The census page,
<https://www.pbs.gov.pk/census/>, publishes the tables as PDFs. A second page,
<https://www.pbs.gov.pk/result-excel/>, publishes most of the same tables as **Excel
workbooks**. The Excel release is roughly a tenth the size, parses without any text-layout
work, and is arithmetically consistent — but it destroys the printed missing-value symbol
in some tables. The plan below uses Excel as the primary input and the PDFs as a
missingness oracle. Evidence for all of this is in §3.

---

## 1. Where the project stands against SHRUG

SHRUG's value is not that it holds a lot of Indian data. It is that it holds a *persistent
location identifier* — the `shrid` — and that every dataset in it is keyed to that
identifier, documented, and downloadable in bulk. Researchers adopt it because the joining
problem is solved once, publicly, with the seams visible.

Measured against that, here is the honest position.

| SHRUG property | Data Darbar today | Gap |
|---|---|---|
| Persistent location ID across censuses | `DDG` comparison areas exist for 125 of 136 districts (Stage 1) | No sub-district identifier at all |
| Finest geography | District for census; tehsil only for satellite, Mouza 2020 and school/health access layers | No census data below district |
| Breadth of the core census | 5 of 33 published 2023 tables (1, 5, 12, 13, 14) | 28 tables unused |
| Cell-level provenance | Yes, for the Stage 1 modules — page, line, URL, checksum | Not yet for the rest |
| Reproducible build | Yes, byte-identical rebuild verified | Only covers 2 modules |
| Bulk download + machine-readable schema | Yes — Parquet + `catalog.json` + data dictionary | — |
| Query without downloading | **Better than SHRUG** — DuckDB-WASM console, no backend | — |
| Documented crosswalks and known defects | Yes, and unusually candid | — |

Two conclusions follow.

First, the *methodological* foundation is already stronger than most open data portals and,
in the query console and the published defect log, stronger than SHRUG in places. The
scarce thing is not rigour; it is coverage.

Second, **the sub-district identifier is the load-bearing piece**. Thirty-three tables of
tehsil data with no stable tehsil ID is a pile of spreadsheets, not a research resource.
The register must be built alongside the parsing, not after it. With the Excel release
removing most of the parsing cost, the register is now the dominant item in this plan — it
is over a quarter of the remaining work and cannot be delegated or automated.

---

## 2. What PBS publishes

Both source pages were fetched on 26 September 2026. The census page is byte-comparable to
the archived 25 September snapshot in `raw_data/pbs/stage1_sources/2026-09-25-official/` —
387 file links, no additions or removals — so the Stage 1 capture is current.

**33 distinct tables**, numbered 1–26 (including 13(a) and 13(b)) and 31–35. Tables 27–30
are not published. Each is issued at national, provincial and district level.

| Release | District-wise files | Total size |
|---|---:|---:|
| PDF (`/census/`) | 163 | **582.4 MB** |
| Excel (`/result-excel/`) | 159 available of 164 listed | **60.5 MB** |

Sizes measured by HTTP `HEAD` against every URL.

### Availability gaps

| Gap | Detail |
|---|---|
| Table 10, KP, district-wise | **Published but unlinked.** Both pages put `table_1_kp_districts` in Table 10's "KP — District Wise" slot — a copy-paste error repeated identically on the PDF and Excel pages. The real files exist at the predictable URLs (`table_10_kp_districts.pdf`, `.xlsx`) and were retrieved and verified on 26 September 2026. |
| Table 35, all five regions | Listed on the Excel page but every URL returns **404**, and no filename variant resolves. PDF only. |
| Table 18, Islamabad | PDF is published under a `_province`-suffixed filename; the Excel district file is present and correctly named. |

The Table 10 case is a warning about method: the published link tables are not a reliable
index of what exists. Phase 0 should probe the predictable URL for every table × region
combination rather than trusting the page.

So the Excel release covers 32 of the 33 tables completely once the unlinked Table 10 KP
file is recovered, and Table 35 not at all.

### Tehsil coverage is real, but not universal

**Most "District Wise" files contain tehsil rows** — in both releases. They are not
district-only. They carry the full district → sub-division → tehsil/taluka hierarchy, with
rural and urban splits at each level.

All 29 available KP district workbooks were opened and counted; the per-table audit is
`raw_data/pbs/stage2_sources/2026-09-26-excel-probe/kp_excel_audit.csv`.

| | Tables |
|---|---|
| Tehsil-level (123 tehsils + 25 sub-divisions) | 1, 3, 4, 5, 9, 11, 12, 13(a), 13(b), 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 31 |
| Tehsil-level, partial roster | 2 — urban localities by size, so only the 22 districts and 64 tehsils that have one |
| **District only** | **6, 7, 8, 10, 13** |

**Five tables stop at the district.** Tables 6 and 7 (marital status and relationship to
head, 15+), 8 (relationship by age), 10 (nationality) and 13 (literacy for special age
groups) have no sub-district breakdown at all, despite being published under a "District
Wise" heading.

Table 13 matters for the current site, which publishes it. Its tehsil-level equivalents
are **13(a) and 13(b)**, which do carry the full roster — any tehsil literacy layer must be
built from those, not from 13.

This was checked for KP only. Coverage should be confirmed province by province in Phase 0;
the audit script that produced the table above runs over a whole region in about a minute.

One layout wrinkle found in the same pass: **the unit label is not always in the first
column.** Table 10 puts its district banners in column 5, Table 2 in column 2. A reader
keyed to column A finds zero units in those files and silently returns an empty panel. Scan
the whole row for a lone text cell.

So the tehsil data the project wants is inside files that are one `curl` away, for 24 of the
29 tables tested. The work is geography and reconciliation, not acquisition.

### The sub-district roster

Counted from the Table 1 stub labels, Census 2023, and cross-checked against the Excel
files:

| Province | Districts | Tehsil / taluka / town | Sub-divisions |
|---|---:|---:|---:|
| Khyber Pakhtunkhwa | 35 | 123 | 25 |
| Punjab | 36 | 145 (incl. 1 town) | — |
| Sindh | 30 | 107 talukas | 31 |
| Balochistan | 34 | 81 | 78 |
| Islamabad | 1 | 1 | — |
| **Total** | **136** | **457** | **134** |

The district total of 136 matches Stage 1's 2023 unit count exactly, which is a useful
check on the extraction.

---

## 3. The Excel release: what it fixes and what it breaks

Seven district-wise workbooks were downloaded and inspected. The probe files, their
checksums and a retrieval manifest are in
`raw_data/pbs/stage2_sources/2026-09-26-excel-probe/`. They are a feasibility probe, not a
build input.

### What it fixes

Every file is a genuine spreadsheet — a real cell grid, not an exported picture of one.
Numbers are stored as numbers.

| File | Sheet | Rows × cols | Districts | Tehsils | Sub-divs |
|---|---|---:|---:|---:|---:|
| `table_1_kp_districts` | `KP Teh` | 1,243 × 12 | 35 | 123 | 25 |
| `table_4_kp_districts` | `District wise` | 17,024 × 13 | 34 | 123 | 25 |
| `table_11_kp_districts` | `District` | 2,949 × 17 | 34 | 123 | 25 |
| `table_22_kp_districts` | `District` | 741 × 11 | 34 | 123 | 25 |
| `table_31_kp_districts` | `KP` | 12,066 × 26 | 34 | 148 | 25 |
| `table_1_sindh_districts` | `sindh dist` | 1,243 × 12 | 30 | 107 | 31 |
| `table_1_balochistan_districts` | `Districts` | 586 × 12 | 34 | 81 | 78 |

This removes, at a stroke, almost everything that made the PDF route expensive: no column
positions inferred from whitespace, no headers wrapped across six physical lines, no page
continuation, no blank rows injected mid-table, no OCR. The layout families still exist —
Table 4 still puts the unit in a banner row and age in the stub — but a banner is now
trivially detectable as a labelled row whose numeric columns are all empty, rather than a
geometric inference.

The arithmetic also holds. Both hierarchy traps found in the PDFs survive intact in Excel:

- **Awaran District (Balochistan), 178,958.** Contains `AWARAN SUB-DIVISION` (45,774) *and*
  four tehsils summing to 133,184. The sub-division is a **sibling** of the tehsils, not
  their parent. The five sum exactly to the district total.
- **Badin District (Sindh), 1,947,081.** Five talukas, no sub-division, closes exactly.

Treat an Awaran-style sub-division as a parent and you lose a quarter of the district;
treat it as a duplicate and you double-count. The hierarchy differs by province and must be
modelled per province, with the closure check enforced per district. That requirement is
unchanged by the format.

Note also that the stub vocabulary differs by province — KP and Balochistan use TEHSIL and
SUB-DIVISION, Punjab uses TEHSIL and one TOWN, Sindh uses TALUKA and SUB-DIVISION. Any rule
keyed to the word "TEHSIL" silently returns nothing for Sindh.

### What it breaks

**Some Excel tables silently convert the printed dash to zero, destroying the distinction
between "missing" and "none".** This is the exact defect Stage 1 identified and corrected,
and the Excel release appears to be its origin.

Stage 1 recorded that Lower and Upper Chitral's 2023 transgender counts "are printed as
dashes and appeared as zero in local CSVs". Checking the official Excel:

| Unit | PDF Table 1 | Excel Table 1 |
|---|---|---|
| Lower Chitral District, transgender | `-` | `0` |
| Upper Chitral District, transgender | `-` | `0` |
| Upper Chitral URBAN, all columns | `-` | `0` |

The behaviour is **inconsistent between tables**. Across all 29 available KP workbooks:

| Missingness handling | Tables | Count |
|---|---|---:|
| **Preserved** — dashes kept, no zeros | 4, 5, 8, 11, 20, 24, 25, 26, 31 (+3 mostly) | 10 |
| **Coerced** — no dashes, zeros only | 1, 7, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 21, 22, 23 | 15 |
| **Mixed** — both present in the same file | 2, 6, 13(a), 13(b) | 4 |

`table_4_kp` holds 78,010 dashes and no zeros; `table_1_kp` holds 956 zeros and no dashes.
Those are clean cases. But `table_13b_kp` holds **16,740 dashes *and* 103,053 zeros**, which
means the coercion happened cell by cell, not file by file — and a file-level triage cannot
sort it out.

This is worse than the seven-file sample suggested. Roughly **19 of the 28 cross-tabulated
tables need cell-level reconciliation against the PDF**, not the "about half" first
estimated. The decision point written into §7 has already triggered: Phase 3b is a real
phase, not a checkbox, and the PDF corpus is a co-equal input rather than a spot check.

One further quirk: derived rates are inconsistently rounded *within a single file*. In
`table_1_kp`, Abbottabad District's sex ratio is stored as `100.77368234877964` while
Abbottabad Tehsil's is `101.76`. The Excel is sometimes more precise than the PDF and
sometimes not. Published rates should be recomputed from counts rather than taken from
either source.

### The resulting rule for Stage 2

> Excel is the value source. The PDF is the authority for missingness. Rates are recomputed
> from counts, never copied.

Both releases get downloaded and locked. That is 643 MB, which is not a constraint.

---

## 4. Scope options

### Option A — Core panel (recommended first increment)

Ten tables carrying most of the research demand, at district **and** tehsil level:
1 (population, area, density, growth), 5 (age groups), 9 (religion), 11 (mother tongue),
12 (literacy, enrolment, out-of-school), 13(a) (literacy by age group), 14 (employment),
16 (disability), 18 (migration), 23 (drinking water). All available in Excel and all
confirmed tehsil-level.

Note the substitution: the original list had Table 13, which the audit shows is
district-only. **13(a)** carries the same indicators down to the tehsil.

### Option B — All 28 cross-tabulated tables

Tables 1–26 including 13(a) and 13(b). Adds housing and structures (20–26), single-year age
(4), household relationships (7, 8), and the special-age-group variants. All in Excel,
including the unlinked Table 10 KP file.

Five of these — 6, 7, 8, 10 and 13 — are **district-only**, so Option B is 23 tables at
tehsil level and 28 at district level, not 28 at both.

### Option C — Add the locality tables (31–35)

Individual rural mauzas and urban localities: the true SHRUG analogue, since SHRUG's unit
*is* the village. This tier is detailed in §4a below — it is larger, better-formed and
better-supported than the first pass suggested, and it is also the only tier where the
project would be publishing something that does not currently exist anywhere in accessible
form.

---

## 4a. The locality tables in detail

All 20 available locality workbooks (Tables 31–34 × 5 regions) were downloaded and counted
on 26 September 2026. Table 35's Excel files 404 in every region and under every filename
variant tried; it is PDF-only.

### What they contain

| Table | Universe | Cols | Content |
|---|---|---:|---|
| **31** | Individual **rural** localities | 26 | Population by sex; literacy 10+ by sex; educational attainment in three bands (primary–below matric, matric–below degree, degree and above) by sex; religion (Muslim / others); population 10+, 18+, 60+; area in acres |
| **32** | Individual **rural** localities | 13 | Housing: structures total / pacca / semi-pacca / kacha; potable water, electricity, gas, kitchen, bathroom, latrine; average household size |
| **33** | **Urban** localities | 23 | As Table 31, minus area and hadbast |
| **34** | **Urban** localities | 12 | As Table 32 |
| **35** | Urban localities | — | PDF only; contents not yet established |

Tables 31 and 32 share an identical row universe, as do 33 and 34, so each pair joins
one-to-one on the locality key.

### How many places

| Province | Rural localities (T31/32) | Urban localities (T33/34) |
|---|---:|---:|
| Punjab | 24,728 | 5,276 |
| KP | 10,432 | 600 |
| Sindh | 5,695 | 4,402 |
| Balochistan | 6,465 | 430 |
| Islamabad | 129 | — |
| **Total** | **47,449** | **10,708** |

**About 58,000 places**, each with roughly 35 published indicators across the two pairs.
For comparison, the project's entire current census layer covers 147 district rows. This is
a change of kind, not degree.

### The hierarchy, which differs by province

Localities do not sit directly under tehsils. They nest inside revenue-administration
groupings that the tables print as subtotal rows:

| Province | Chain | Grouping rows |
|---|---|---:|
| Punjab | district → tehsil → **QH** (qanungo halqa) → **PC** (patwar circle) → mauza | 8,211 |
| KP | district → tehsil → QH → PC → mauza | 1,418 |
| Balochistan | district → tehsil/sub-division → QH → PC → mauza | 570 |
| Sindh | district → taluka → **STC** (supervisory tapedar circle) → **TC** (tapedar circle) → deh | 1,722 |
| Islamabad | district → tehsil → QH → PC → mauza | 38 |

Every QH, PC, STC and TC row is a subtotal of its children. Summing a column without
excluding them roughly triples the national population. Sindh's vocabulary is entirely
different from everyone else's, which is the third time in this audit that a Sindh-specific
naming convention has broken a rule that held for the other four regions.

### The identifier problem

This is the blocker, and it is worse than the parsing.

| Province | Localities | With a hadbast / deh number | Coverage |
|---|---:|---:|---:|
| Punjab | 24,728 | 23,147 | **94%** |
| Islamabad | 129 | 109 | 84% |
| Balochistan | 6,465 | 4,111 | 64% |
| KP | 10,432 | 3,433 | **33%** |
| **Sindh** | **5,695** | **0** | **0%** |
| Rural total | 47,449 | 30,800 | 65% |

Urban localities (Tables 33/34) carry **no identifier at all** — name only.

So for 35% of rural places and 100% of urban ones, the only key is the printed name plus
its position in the hierarchy. Names repeat: "BAGH" appears as both a PC and a mauza within
one Abbottabad tehsil. Any locality key has to be composite — province, district, tehsil,
grouping, name, and hadbast where present — and it will not be stable across censuses.

### What the project can already join it to

The first draft of this plan assumed the Mouza Census 2020 tables would provide a
mouza-level bridge. **They do not.** `mouza_tehsil` in the warehouse has 595 rows, one per
PBS tehsil; every column is a *count of mouzas*, and no mouza is named. There is no
mouza-level register in the project, and no public polygon set for Pakistani mauzas exists.

Two things are nonetheless directly reusable:

**1. The tehsil crosswalk is already built.** `etl/mouza2020/mouza2020_tehsil_crosswalk.csv`
maps 595 PBS tehsils onto the 553 ADM3 polygons, with each match classified as
`exact_name`, `variant`, `parent` or `approx` and the judgement calls recorded one line at
a time in `manual_map.py`. The locality tables nest inside exactly those tehsils, so the
parent geography is solved and documented. Localities inherit their tehsil's polygon; they
do not get one of their own.

**2. Mouza Census 2020 gives an independent count to validate against.** Matching Table 31's
per-tehsil locality counts to the 2020 `RuralMouzaCount` on a naive normalised-name join,
with no manual work at all:

| Province | Tehsils in T31 | Auto-matched | Median T31 ÷ Mouza 2020 | Within ±25% |
|---|---:|---:|---:|---:|
| Sindh | 109 | 98% | 1.06 | 88% |
| Punjab | 140 | 93% | 1.07 | 86% |
| KP | 153 | 76% | 1.10 | 64% |
| Balochistan | 157 | 56% | 1.04 | 72% |

Two independently produced enumerations, three years apart, agree on the number of rural
localities per tehsil to within about 6–10% at the median. That is a genuine coherence
result, and it arrives before any manual matching — the existing crosswalk's variant and
parent mappings should lift the KP and Balochistan match rates considerably. Balochistan's
56% is the expected consequence of sub-tehsils created after the polygons were drawn, which
is the same problem the crosswalk already documents.

### Can mauza polygons be constructed? What the Sindh panel did

Someone has already done it for one province. The **Pakistan Census Panel**
(<https://muhammad-binkhalid.github.io/pakcensus-panel/>) publishes a mauza-level
longitudinal panel for **Sindh only** — 173 fields per mauza across 11 census and
infrastructure years from 1951 to 2023 — and states explicitly that it is modelled on
SHRUG. It is a byproduct of Bin Khalid and Mattsson, *The Effect of Disaster Relief on
Climate Adaptation: Evidence from Floods in Pakistan*
([SSRN 5537178](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=5537178)), with a
replication package published on Dropbox. The site is a MapLibre front end over a hosted
MapTiler vector tileset whose bounds — 66.65–71.13 E, 23.70–28.51 N — are Sindh's.

Their method, in their own description: the boundaries were made by **digitising hand-drawn
patwari maps for every mauza in Sindh**, geo-referencing each sheet and digitising it
manually, "producing—for the first time—a complete province-wide mauza shapefile".
Population came from six digitised census rounds back to 1961, linked across waves by fuzzy
string matching with several rounds of manual verification; infrastructure came from the
mauza infrastructure censuses of 1993, 2003, 2008 and 2020.

That is the honest answer to whether polygons can be constructed: **yes, by hand, one
patwari sheet at a time.** There is no shortcut that produces real mauza boundaries. Four
routes exist and only the first gives true geometry.

| Route | What you get | Cost | Verdict |
|---|---|---|---|
| **1. Digitise patwari maps** | True mauza boundaries | Research-programme scale — Sindh alone was a multi-year academic effort with funding behind it | Out of scope for this project alone |
| **2. Reuse or collaborate** | Sindh boundaries, immediately | One email | **Do this first** |
| **3. Geocode to points, tessellate within tehsil** | Approximate points and Thiessen areas | Weeks, plus unbounded manual matching | Weak; see below |
| **4. Don't map them** | Tehsil-nested tabular register | Already costed in §5 | Recommended |

**Why route 2 comes first.** Sindh is exactly where the census gives no identifier at all —
0 of 5,695 rural localities carry a hadbast or deh number (§4a). It is the province where
Data Darbar's own key is weakest and where a ready-made, research-grade mauza geometry
already exists. The replication package is described as containing "mauza level population
and infrastructure data", which may or may not include the shapefile; that is a question to
ask rather than assume. The authors also write that they hope to extend to other provinces,
which makes this a collaboration rather than a competition.

**Why route 3 is weaker than it sounds.** Gazetteer coverage is not the constraint —
GeoNames holds 150,401 populated places for Pakistan, 188,587 distinct normalised name
forms, against 47,449 census rural localities, and OpenStreetMap adds 27,085
`place=village|hamlet` nodes. Matching is the constraint. On strict normalised exact-name
matching against the full GeoNames name and alternate-name set, with no tehsil restriction
at all — a deliberately generous test:

| Province | Localities | Exact name present in GeoNames |
|---|---:|---:|
| KP | 10,432 | 40% |
| Punjab | 24,730 | 35% |
| Sindh | 5,695 | 34% |
| Balochistan | 6,465 | 28% |
| **Total** | **47,322** | **35%** |

Fuzzy matching and a tehsil constraint would raise that — the Sindh panel's authors used
fuzzy matching plus repeated manual verification for exactly this reason — but the ceiling
is somewhere well short of complete, and the output is still a point with a Thiessen polygon
around it, not a boundary. For a project whose credibility rests on saying what it does and
does not know, publishing tessellated pseudo-boundaries for 47,000 villages at a 35% naive
match rate is the wrong trade.

**Where this leaves the strategy.** The Sindh panel has depth — one province, seven decades,
real geometry. Data Darbar's advantage is breadth: all five regions, 2023, 33 tables, plus a
query console the Sindh panel does not have. Those are complementary, and the natural
division is that Data Darbar publishes the national tabular locality register and links to
or ingests their geometry for Sindh, rather than starting its own digitisation programme.

### Where the patwari maps actually are

The primary cadastral document is the **shajra kishtwar**, the village field map, and its
survey sheet the **musavi**. Custody is the patwar circle — the same PC that appears as a
subtotal row in Table 31 — with copies in the tehsil revenue record room and the district
record room, under each provincial Board of Revenue. There is no single archive; the
originals sit across several hundred tehsil record rooms.

The useful question is not where the paper is, but who has already scanned it.

**PBS has.** Its Geography/GIS Section runs eight GIS labs with about a hundred staff and
states that it has prepared ArcGIS layers for "National, Provincial, Divisional, District,
Tehsil, QH/STC, PC/TC, **Mauza**, Urban Area and Blocks", that it "procured Mauza/urban
area maps from relevant Boards of Revenue/Local Government Departments", and that it
carried out "Scanning of Mussavis/revenue record for accurate Mauza boundaries of Khyber
Pakhtunkhwa and Punjab provinces" ([PBS GIS](https://www.pbs.gov.pk/gis/)).

That page is also independent confirmation of the hierarchy reverse-engineered in §4a: PBS
describes the rural chain as Division → District → Sub-division → QC/STC → PC/TC/UC →
Mauza/Deh/Village, exactly the structure the locality tables print.

**Sindh's Board of Revenue has, at province scale.** LARMIS reports that "Digital District
and Deh maps for all 5,979 Dehs of the Province have been developed", with survey-number
maps finished for 4,000 of them
([BOR Sindh](https://bor.sindh.gov.pk/land-administration-revenue-management-information-system-larmis)).
That 5,979 sits within a few dozen of the Mouza Census 2020 Sindh total of 5,976, so the
two enumerations are describing the same universe.

Punjab (PLRA), KP (Directorate of Land Records, 20 districts across two phases) and
Balochistan have all computerised land records, but those programmes are centred on the
textual record of rights; the cadastral mapping component is less complete and much less
documented.

### Can they be obtained?

**Not as GIS.** The PBS Data Dissemination Policy
([policy.pdf](https://www.pbs.gov.pk/wp-content/uploads/2020/07/policy.pdf)) places "Block
maps, geo-coordinates and shapefiles of block boundaries" in **Tier 4 — Non-Disseminable**:
"No external dissemination; only authorized internal statistical use or lawful inter-agency
use." The policy adds, pointedly, that "the classification applies independently of whether
a user offers to pay a fee."

**But maps, as documents, are for sale.** The same policy carries a published schedule of
census-map charges:

| Product | Size | Private hard / soft | Government hard / soft |
|---|---|---|---|
| **Mouza/Deh map** | A3 | **PKR 750 / 900** | PKR 500 / 600 |
| Tehsil/Taluka map showing revenue estate | Arch E | 3,750 / 4,500 | 2,500 / 3,000 |
| District map showing tehsil, QH, PC | Arch E | 3,750 / 4,500 | 2,500 / 3,000 |
| Rural/urban census circle (PC) map | A3 | 1,250 / 1,500 | 1,000 / 1,200 |

All map products are supplied "exclusively through the PBS Headquarters, Islamabad after
approval", paid into the Government Treasury under head C 02401.

So a mauza map is a purchasable A3 soft-copy product at PKR 900. Buying the country is not
an option — 47,449 mauzas at PKR 900 is about **PKR 43 million**, on the order of a hundred
thousand pounds, for raster images that would still need georeferencing and digitising. One
district is a different matter: roughly 400–600 mauzas, PKR 360,000–540,000, and it would
establish the real per-mauza cost of the whole pipeline on known ground.

**The argument worth making to PBS.** Tier 4 is written about *enumeration blocks*, and its
stated reason is that blocks "are operational sampling units rather than stable
administrative boundaries". The policy then says in the same table that public dissemination
"should normally use stable administrative areas such as **Mouza/Deh, Patwar Circle,
Tehsil/Taluka and District**". By PBS's own reasoning, mauza boundaries are the geography it
considers *suitable* for public release — the Tier 4 restriction is aimed at something else.
A request for the mauza layer, framed that way and separated explicitly from any request for
block boundaries or coordinates, is not obviously refused by the policy as written.

### Routes, in the order worth trying

| # | Route | Cost | What it gets |
|---|---|---|---|
| 1 | Ask PBS for the **mauza layer only**, citing §5.3's own stable-administrative-areas language, explicitly excluding blocks and coordinates | An email | Everything, if it lands |
| 2 | Ask **BOR Sindh** for the 5,979 digital deh maps | An email | Sindh — the province where the census gives no identifier at all |
| 3 | Ask the **Sindh panel authors** whether the replication package includes their shapefile | An email | Sindh, research-grade, already validated |
| 4 | **Buy one district's** mouza maps at PKR 900 each, georeference and digitise | ~PKR 0.4–0.5m | A costed, evidenced pipeline and a pilot layer |
| 5 | Provincial Boards of Revenue and tehsil record rooms for raw shajra kishtwar | Research-programme scale | The pakcensus route; full control, full cost |

Routes 1–3 cost three emails between them and should all be sent before any of routes 4 or
5 is contemplated. Nothing in the plan's critical path depends on the answers: the
tehsil-nested tabular register in §5 ships without any of them.

### What this tier can and cannot be

It **can** be: a documented, queryable, downloadable register of ~58,000 Pakistani
localities with population, literacy, education, religion, age structure and housing,
nested inside tehsils that already have polygons, validated against an independent 2020
enumeration. Nothing like it is currently available outside these PDFs and spreadsheets.

It **cannot** be, on this project's own resources: a mapped village layer outside Sindh, or a
cross-census panel. Real mauza boundaries exist only where someone has digitised patwari
maps, and with a third of rural places and all urban ones lacking a stable identifier,
linking to Census 2017's locality tables would be name-matching at a scale where the error
rate cannot be characterised honestly.

The correct framing is that this is Data Darbar's `shrid` moment in reverse: SHRUG could
build a persistent village ID because India's census carries one. Pakistan's does not,
consistently. Publishing the locality tables with an explicit, composite, **census-specific**
key — and saying plainly that it is not longitudinal — is the honest version, and it is
still the most valuable thing in this plan.


---

## 4b. Building at tehsil level now

**Yes — and it is closer to ready than any other part of this plan.** Tehsil is the one
level where the data, the geometry and the crosswalk all already exist. This section tests
that claim rather than asserting it.

### The three things that have to be true

**1. The tables carry tehsil rows.** 24 of the 29 available KP workbooks do (§2). The five
that stop at the district — 6, 7, 8, 10 and 13 — are known and named, and 13's tehsil-level
substitutes, 13(a) and 13(b), are both available.

**2. The polygons exist.** `app/data/tehsils_geo.js` already ships 553 ADM3 tehsil
geometries with `dd_id`, `dk` and `prov`, and the map already switches to them for the
satellite and Mouza layers. Nothing new has to be drawn.

**3. The crosswalk exists.** `etl/mouza2020/mouza2020_tehsil_crosswalk.csv` maps 595 PBS
tehsils onto 512 distinct `dd_id` polygons, with each match classified `exact_name`,
`variant`, `parent`, `approx`, `fuzzy` or `fuzzy_squashed`, and judgement calls recorded one
line at a time in `manual_map.py`.

### How much of the census roster that crosswalk already covers

Matching the 590 Census 2023 sub-district units from Table 1 against it, by normalised
name within province:

| Match | Units | Population | Share |
|---|---:|---:|---:|
| Exact name | 457 | 204,451,172 | 84.7% |
| Plus fuzzy (≥0.82) | 41 | 7,433,855 | 3.1% |
| **Auto-matched** | **498** | **211,885,027** | **87.8%** |
| Karachi urban sub-divisions | 24 | 16,904,317 | 7.0% |
| Everything else | 68 | 12,668,346 | 5.2% |
| **Total** | **590** | **241,457,690** | |

**Nearly 88% of Pakistan's population maps to an existing polygon with no manual work at
all.** The remaining 12% is not a diffuse tail; it is two named, bounded jobs.

**Karachi (24 units, 7.0%).** Korangi, Ferozabad, New Karachi, Gulzar-e-Hijri, Mominabad,
Gulshan-e-Iqbal, Lyari, Baldia, North Nazimabad and the rest are simply absent from the
crosswalk, because the Mouza Census frame it was built from is a *rural* revenue frame.
This is structural, not a spelling problem, and it is confined to four districts. It needs
its own mapping of Karachi's sub-divisions onto the city's ADM3 polygons — a day or two of
careful work, and the single highest-value unit of effort in this whole plan, since it is
7% of the country.

**Sixty-eight others (5.2%).** Overwhelmingly transliteration variants and post-polygon
splits, and the residual list reads as a to-do rather than a research problem: Quetta's
`SUB-DIVISION CITY` and `SUB-DIVISION SARIAB`; Peshawar's newer rural tehsils (Cham Kani,
Mathra, Pishta Khara, Badhber) carved out of what the crosswalk still calls `PESHAWAR CITY`
and `PESHAWAR CANTT`; Turbat; Swat Ranizai. Several are pure spelling: the census writes
**PASRUR** where the crosswalk has **PASROOR**, and **MIANWALI** where the crosswalk has the
transposed **MAINWALI**.

### The one methodological catch

The existing crosswalk is deliberately **many-to-one**: 595 PBS tehsils collapse onto 512
polygons, and its README says plainly that this is right *because every Mouza Census figure
is a count of mouzas*, so summing is exact.

Census indicators are not all counts. Summing populations across merged tehsils is fine;
**averaging rates is not**. Every derived figure must be recomputed from summed numerators
and denominators after the merge, never averaged across the units being merged — which is
the same rule Stage 1 already applies to split districts. The crosswalk is reusable, but its
`match` column has to be read as a *type* and not just a flag: `parent` and `approx` matches
carry different comparability claims from `exact_name` ones, and the published data
dictionary should say so per unit.

### What this changes in the plan

Phase 1 was costed at 8–12 days assuming a sub-district register built from nothing. With
87.8% already matched and the residual enumerable, it is closer to **5–8 days**: reviewing
and extending an existing reviewed artifact rather than creating one. Option A therefore
lands at roughly **37–57 person-days**, Option B at **55–79**.

Everything in §4a — the 58,000 localities, the hadbast problem, the patwari maps, the Sindh
panel — becomes a later stage that nothing here waits on. The locality tables nest *inside*
these tehsils, so the work done now is the foundation that tier would eventually sit on
rather than a detour away from it.

### Recommended shape

Ship **Option A at district and tehsil level**, with the sub-district register as the
headline deliverable and the locality tier explicitly deferred. Concretely that is:

1. Karachi's 24 sub-divisions mapped (highest value per day in the plan).
2. The 68 residual units resolved or explicitly withheld, in the Stage 1 house style.
3. Ten tables — 1, 5, 9, 11, 12, 13(a), 14, 16, 18, 23 — read from Excel, reconciled against
   the PDFs for missingness, published across 136 districts and 457 tehsils.
4. Rates recomputed from counts after any merge; match type published per unit.

That is a complete, honest, mapped tehsil panel, and it does not depend on a single thing
PBS has not already published.

---

## 5. Work plan

Phases 1 and 2 run in parallel; nothing downstream starts until both land.

| # | Phase | Output | Days (A) | Days (B) | Days (C) |
|---|---|---|---|---:|---:|---:|
| 0 | Acquisition and source registry — fetch both releases, extend `fetch_census_sources.py`, checksums, retrieval manifests, archive both pages, extend `source_registry.py` and the input lock | Locked, dated corpus | 1–2 | 2–3 | +1 |
| 1 | **Sub-district geography register** — `DDS` identifiers for all 591 published sub-district units; province hierarchy rules; closure checks; extend the existing crosswalk over Karachi's 24 sub-divisions and 68 residual units; 2017 linkage where supportable; explicit withholds | `sub_district_register.csv`, `sub_district_crosswalk.csv`, evidence and issues logs | 4–6 | 4–6 | +6–10 |
| 2 | **Workbook reader** — layout families, merged-header reconstruction, banner detection, nested stubs, dash preservation, per-cell provenance, regression suite | `etl/stage2/` + tests | 2–4 | 3–5 | +2–3 |
| 3 | Per-table column specs and validation | 10 / 31 specs | 2–3 | 5–8 | +3–5 |
| 3b | **Missingness reconciliation** — PDF↔Excel cell-level dash mask for the ~19 tables that coerce or mix | `missingness_mask.parquet`, `issues.csv` | 3–5 | 5–8 | +2–3 |
| 4 | Panel assembly — long-format observations, indicator dictionary, aggregation rules, rates recomputed from counts | `observations.parquet`, `indicator_dictionary.csv` | 3–4 | 4–6 | +4–6 |
| 5 | Warehouse and catalog — Parquet, `catalog.json` with real `notes`, wide slice for the map | Warehouse tables + dictionary | 2–3 | 3–4 | +3–5 |
| 6 | Site integration — indicator groups in `app.js`, tehsil geometry switching, deep links, CSV export, methodology copy | Map and console live | 4–6 | 6–9 | +3–5 |
| 7 | Release QA — byte-reproducible rebuild, verification record, build report | Signed release | 3–4 | 4–5 | +3–4 |
| | **Total person-days** | | **24–37** | **36–54** | **+27–42** |

### These numbers were revised down after measuring

The first version of this plan costed Option A at 43–65 days against the PDFs and 37–57
against Excel. Those were guesses. On 26 September a working prototype was built to test
them: `etl/stage2/read_workbook.py` and `build_table1_panel.py`, about 150 lines, produced a
complete Table 1 panel across all five regions — 2,181 rows, 136 districts and 591
sub-district units — joined to map polygons, in **roughly fifteen minutes of work and 0.36
seconds of runtime**.

It found and closed a real defect on the way. Punjab publishes **DE-EXCLUDED AREA
RAJANPUR**, a sub-district unit of 41,741 people and 5,013 km² carrying none of the words
DISTRICT, TEHSIL, TALUKA, SUB-DIVISION or TOWN. It is the only unit of its kind in the
country, and Rajanpur does not close without it. With it, **all 136 districts close exactly**
— every district's children sum to its published total, with no tolerance.

The lesson is that the parsing was over-costed and the phases above are corrected
accordingly. What has *not* been revised down is Phase 1, Phase 6 and Phase 7, because the
prototype is exactly what those phases are not: it has no tests, no input lock, no
reproducibility guarantee, no per-cell provenance beyond a row number, no review, and it
resolves nothing about the 92 unmatched units. Stage 1 shipped two modules with a 294-file
input lock, 18 regression tests and a verified byte-identical rebuild. The distance between
"it runs" and "it meets that standard" is most of what remains.

For comparison, the PDF-only route costed at 43–65 (A) and 66–93 (B). The Excel release
now saves a great deal, because measurement showed the reading is close to free. What it
does not touch are the things that now dominate the estimate: the geography register, site
integration, and the evidence apparatus that makes a Data Darbar release a Data Darbar
release.

The case for using it is not that it is cheaper. It is that the reconciliation leaves a
better audit trail than parsing alone, because every value gets checked against two
independently produced renderings of the same table.

### Calendar

| Working pattern | Option A | Option B | Option C on top |
|---|---|---|---|
| Full time (5 days/week) | 5–8 weeks | 7–11 weeks | +6–9 weeks |
| Three days a week | 8–12 weeks | 12–18 weeks | +9–14 weeks |
| Two days a week | 12–19 weeks | 18–27 weeks | +14–22 weeks |

These assume one person with the Stage 1 context already in their head. A second person
helps on Phase 3 (specs parallelise cleanly, one table at a time) and barely helps anywhere
else.

### Where an LLM assistant genuinely compresses the estimate

Phase 3 remains the best target — drafting 31 column specs and fixture tests from a sample
sheet each, with mechanical verification (does the reconciliation close?). Expect 30–40%
off, though the phase is now small enough that this saves about 4 days on Option B rather
than 8.

Phase 2 compresses maybe 20% now that the hard geometric cases are gone.

**Phase 1 compresses by roughly nothing, and should not be delegated.** Deciding whether
Awaran Sub-Division is a leaf, whether a 2017 tehsil is the same place as a 2023 one, and
when to withhold rather than guess is exactly the work that gives the register its
authority. Stage 1 withheld four districts rather than merge them to make totals cancel;
that judgement is the asset.

Net: Option B lands nearer **32–48 person-days** with assistant support.

---

## 6. Resources beyond time

| Resource | Requirement | Note |
|---|---|---|
| Source storage | 643 MB for both 2023 releases, plus ~1.2 GB if the 2017 per-district set is completed | `raw_data/` is not in git and iCloud is its only backup. Fix before adding to it. |
| Build compute | Single laptop; reading 159 workbooks is minutes | No OCR, no GPU, no cloud |
| Published warehouse | Estimated 4–8 million observations for Option B → roughly 100–200 MB Parquet | The DuckDB-WASM design already handles this via range reads; only tables under 2 MB load eagerly. The **map** needs a separate slim wide table — do not inline the panel into `census_data.js`. |
| Hosting | GitHub Pages, unchanged | Watch repo size; consider R2 or a release-asset split past ~250 MB |
| Money | Effectively nil beyond person-time and LLM usage | The architecture's main virtue |

---

## 7. Sequencing and decision points

```
Phase 0 ──┬── Phase 1 (register) ─────────┐
          └── Phase 2 (reader) ── Phase 3 ─┴── 3b ── 4 ── 5 ── 6 ── 7
```

Three points where the plan should stop and be re-decided:

**~~After the triage run in Phase 2~~ — already triggered.** The KP audit of 26 September
found 19 of 28 tables coercing or mixing their missing-value symbols, against the "about
half" first assumed. Phase 3b has been resized accordingly. The remaining question is
whether the other four provinces behave like KP; that is one more audit run and should be
the first thing Phase 0 does.

**After Phase 1.** If more than about 15% of sub-district units cannot be placed in the
register with evidence, the tehsil panel should ship as a 2023 cross-section only, with no
cross-year comparison. Publishing an unsupported 2017↔2023 tehsil link would undo the
credibility Stage 1 bought.

**Before Option C.** The original test — do hadbast numbers match the Mouza 2020 crosswalk —
is void: that source has no mouza-level rows (§4a). The replacement go/no-go is narrower and
already half-answered. Proceed if a composite locality key (province, district, tehsil,
grouping, name, hadbast where present) is unique within tehsil for at least 98% of rows, and
if the per-tehsil count validation in §4a holds after the existing crosswalk's variant and
parent mappings are applied. Both are a day's work to establish. Do **not** make a
cross-census locality panel a condition of shipping — §4a explains why that is not
available.

---

## 8. Recommended course

Do **Option A at district and tehsil level**, treat Phase 1 as the real deliverable, and
defer the locality tier to a later stage. §4b sets out the concrete shape and why tehsil is
the level where everything needed already exists.

Ten tables at tehsil level takes the project from 5 census tables across 147 districts to
10 tables across 136 districts and 457 tehsils — roughly a ninefold increase in published
census observations — and produces the sub-district identifier that every later table, and
every future dataset, joins to. That identifier is what makes Data Darbar a SHRUG-style
resource rather than a very good dashboard. Tables 20–26 are additive once it exists and
are not additive before.

At three days a week that is a release around **late November 2026**. Option B on the same
pattern runs to roughly January–February 2027, and is better taken as a second increment once the
per-table cost is known.

The Excel release changes the cost of Option B more than Option A, because Option B is the
one with 28 tables to read. But the saving is smaller than the first look suggested, and
Option B buys 23 tehsil-level tables rather than 28 — five of them stop at the district. If
the register goes smoothly, rolling from A into B remains sensible; it is not the bargain
the Excel page initially made it look.

---

## 9. Open questions for PBS

1. Table 10's "KP — District Wise" link points at Table 1's file on both pages. The real
   Table 10 KP files exist at the expected URLs — can the links be fixed? And are there
   other slots with the same copy-paste error that we have not found?
2. All five Table 35 Excel links return 404. Are the workbooks recoverable?
3. Are tables 27–30 planned for release?
4. The Excel files record `0` where the PDF prints `-` in 15 of 29 KP tables, and mix the
   two conventions within a single file in another four. Which rendering is authoritative —
   and is the dash "not applicable", "nil", or "not available"? The three have different
   consequences for every rate computed from them.
5. Tables 6, 7, 8, 10 and 13 are published without any sub-district breakdown. Is that
   deliberate — a disclosure-control or data-quality decision — or an oversight? If the
   tehsil figures exist, they would close most of the remaining gap in one step.

Separately, one question for the Sindh panel's authors rather than PBS: does the replication
package include the mauza shapefile, and would they be open to Data Darbar hosting or linking
it as the geometry layer for Sindh? See §4a.

Question 4 is the one that matters most. It is a genuine ambiguity in the published record,
it affects the Chitral figures Stage 1 already flagged, and no amount of careful parsing
resolves it without PBS saying what the symbol means.
