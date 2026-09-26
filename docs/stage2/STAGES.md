# Stage 2 — staged build plan

Approved 26 September 2026. Built first as Option A (ten tables), then extended to the full
cross-tabulated set (28 tables) at district and tehsil level.
Scope, evidence and estimates are in [README.md](README.md); this is the build sequence.

**Tables:** all 28 that PBS publishes — 1 to 26 including 13(a) and 13(b). Tables 27–30 do
not exist; 31–35 are the locality tables and remain a separate stage.

Five — 6, 7, 8, 10 and 13 — are published without any sub-district breakdown and contribute
district rows only. Table 2 lists named urban localities rather than administrative units,
so it is built into its own table instead of the unit panel.

**Standing rules,** carried from Stage 1 and not renegotiable per stage:

- The printed dash is missing, and is never converted to zero.
- Rates are recomputed from counts. Published rates are checked, never copied.
- Nothing is silently rewritten: every corrected label keeps its `unit_source`.
- A unit that cannot be placed with evidence is withheld, not guessed.
- Excel is the value source; the PDF is the authority for missingness.

---

## Stage 0 — Acquisition ✅ complete

Both releases captured to `raw_data/pbs/stage2_sources/2026-09-26-optionA/` with a
retrieval manifest recording URL, timestamp, byte count and SHA-256 per file.

| | Files | Size |
|---|---:|---:|
| Excel (`xlsx/`) | 50 | 14.2 MB |
| PDF (`pdf/`) | 50 | captured for Stage 3 |

`etl/stage2/fetch_sources.py` handles PBS's irregular filenames (Islamabad drops the
`_districts` suffix; some tables carry typos) and is never called by a build.

---

## Stage 1 — Extraction ✅ complete

`etl/stage2/read_workbook.py` + `table_spec.py` + `build_panel.py`.

Four layout shapes appear across the ten tables, declared per table in `table_spec.py`
rather than inferred, so a layout change fails a check instead of producing wrong data:

| Shape | Tables | Unit | Stub levels | Column header |
|---|---|---|---|---|
| `stub_unit` | 1 | row with values | locality | measure |
| `banner_unit` | 23 | banner | locality | measure |
| `banner_unit` | 9, 11 | banner | locality → sex | category |
| `banner_unit` | 13(a) | banner | locality → sex → indicator | age bracket |
| `banner_unit` | 5, 12, 14, 16, 18 | banner | indicator | locality × sex |

**Result:**

```
observations   2,596,564
units          725  (136 districts, 589 sub-district)
dash cells     123,382 preserved as missing
spec checks    50/50 pass
runtime        ~13 seconds
```

Unit types: 412 tehsil, 133 sub-division, 43 sub-tehsil, 1 other. **Every one of the 589
sub-district units appears in all ten tables** — the roster is identical across tables and
regions, which is the strongest internal check available at this stage.

### Defects found and recorded

**A systematic error in Table 1.** The literal string `ALL` has been deleted from six place
names, and only in Table 1: `ALLAI → AI`, `KALLAR KAHAR → KAR KAHAR`, `KALLAR SAYADDAN →
KAR SAYADDAN`, `TANDO ALLAHYAR → TANDO AHYAR` (district and taluka), `KALLAG → KAG`. The
deletion is exact in every case, which is what a find-and-replace stripping the word "ALL"
from locality stubs does to any place name containing those three letters. The other nine
tables agree on the correct spellings. Stage 1 had recorded the Tando Allahyar case on its
own; this generalises it to a single reproducible defect. Corrections are in
`etl/stage2/unit_aliases.py` with the reasoning, and every observation retains
`unit_source`.

**Malakand.** Table 1 calls it `MALAKAND DISTRICT`; tables 5–23 call it `MALAKAND PROTECTED
AREA`. One unit either way. Resolved to the Table 1 label, which is the administratively
correct one.

**De-excluded area.** Punjab publishes `DE-EXCLUDED AREA RAJANPUR` — 41,741 people,
5,013 km² — carrying none of the words district, tehsil, taluka, sub-division or town. It
is the only unit of its kind in the country and Rajanpur does not close without it.

---

## Stage 2 — Geography register 🔶 mostly complete

`etl/stage2/build_crosswalk.py` + `manual_map.py`. Output in
`data_darbar_warehouse/stage2/optionA-2026-09-26-geography/`.

**535 of 589 sub-district units resolved — 94.6% of national population.**

| Method | Units | What it means |
|---|---:|---|
| `mouza` | 459 | The existing PBS-tehsil crosswalk, which carries prior manual work |
| `boundary` | 31 | geoBoundaries ADM3 name match within the same district |
| `cod` | 22 | OCHA COD-AB p-code; the unit postdates the geoBoundaries layer |
| `manual` | 15 | Reviewed Karachi decisions in `manual_map.py` |
| `fuzzy` | 8 | Approximate name match within the same district |
| **withheld** | **54** | 5.4% of population, listed with candidates in `withheld.csv` |

Every row records its method and evidence. 427 units also carry a COD-AB p-code.

### A newer boundary source

The project's geoBoundaries ADM3 layer is **2017-vintage** — 554 units, source last updated
January 2023 — and the geoBoundaries API confirms no newer release exists. OCHA's
[COD-AB for Pakistan](https://data.humdata.org/dataset/cod-ab-pak) has **577 ADM3 units
across 160 districts, reviewed September 2024**, and carries stable p-codes (`PK50302`)
rather than opaque hashes.

It resolved 22 units geoBoundaries cannot, because they were created after 2017: Baka Khel,
Kakki and Wazir in Bannu; Darra Adam Khel and Gumbat in Kohat; Chagai, Nokkundi and Chaman
in Balochistan. The gazetteer is captured with a manifest at
`raw_data/geospatial/boundaries/cod-ab-pak-2026-09-26/`; its geometry has not been
downloaded yet.

**Open decision.** Those 22 units are resolved but not yet *renderable*, because the map
draws on `dd_id` polygons: 94.6% of population is resolved, 93.9% can be drawn. Adopting
COD-AB geometry would close that gap and give the register a real identifier, but the
poverty, satellite, Mouza and school layers are all keyed to `dd_id` and would need
re-crosswalking. That is a Stage 5/6 decision, not a Stage 2 one.

### The 54 still withheld

| Group | Units | Population | Why |
|---|---:|---:|---|
| Karachi sub-divisions | 9 | 5.8M | The polygon layer holds Karachi's old *town* system plus six cantonments; the census publishes 31 sub-divisions across seven districts. Fifteen share a town's name and are matched. The other nine were carved out of those towns and no source to hand gives the boundary. |
| KP and Balochistan new units | 43 | 6.5M | Tehsils and sub-tehsils created after both boundary layers. Some have a parent polygon available; placing them there would mean several census units sharing one shape. |
| Punjab | 2 | 0.5M | Chowk Sarwar Shaheed and one other, carved from existing tehsils |

**Open decision.** For the 43 KP/Balochistan units, either map each to its parent polygon
and aggregate — which is what the Mouza crosswalk already does with its `parent` type, and
is exact for counts — or leave them withheld and blank on the map. Aggregating recovers
about 2.7% of population; withholding keeps each published unit distinct.

### Defect worth recording

PBS inverts some Quetta labels: the census prints `SUB-DIVISION CITY` where every boundary
source has `QUETTA CITY`. Handled by trying the district-qualified form. `DERA ISMAIL KHAN
TEHSIL` against `D.I.KHAN` is handled by an explicit abbreviation list rather than fuzzy
matching, so it cannot silently mis-fire.

---

## Stage 3 — Missingness reconciliation ✅ complete

`etl/stage2/reconcile_missing.py`. Output in
`data_darbar_warehouse/stage2/optionA-2026-09-26-missingness/`.

PBS publishes each table twice. Both renderings are generated from the same workbook and
run in the same row order, so a sequential walk keyed on the row label aligns them without
any geometric parsing of the PDF.

**Alignment is self-verifying.** A row's dashes are only trusted once its unambiguous
values — the non-zero numbers — agree between the two renderings. Rows where they do not
are reported in `unaligned_rows.csv` rather than masked. An early version without this
check produced about 11,000 masks that could not be justified.

| | |
|---|---:|
| Printed dashes recovered from the PDFs | **421,883** |
| Cells where the two renderings disagree on a value | 10,480 |
| Rows not masked, reported instead | 2,418 |

Recovering 421,883 dashes means the Excel release was reporting nearly half a million
"not reported" cells as zero. Any rate computed from those columns without this step is
wrong.

### PBS's two renderings disagree on 8,380 values

Most are small and concentrated in Table 13(a)'s deepest age columns, where the alignment
is least certain; those are reported, not resolved. One is unambiguous.

**Topi Tehsil, Swabi, Table 1.** The Excel gives rural population as 382,562; the PDF gives
307,695. The difference, 74,867, is exactly Topi's urban figure — the Excel has overwritten
the rural cell with the tehsil total. Rural plus urban reconciles to the published total
only with the PDF value.

This is the only disagreement in the corpus where adopting the PDF makes a district close,
and it is corrected on that basis and recorded in `value_corrections.csv`. Every other
disagreement is left exactly as published.

---

## Stage 4 — Panel assembly ✅ complete

`etl/stage2/build_final_panel.py` and `build_dictionary.py`. Output in
`data_darbar_warehouse/stage2/optionA-2026-09-26-panel/`.

```
panel rows          2,596,564
missing cells       548,509  (425,127 recovered from the PDFs)
values corrected    1        (PDF adopted where it makes the district close)
rows with an id     2,596,564 / 2,596,564  (100%)
rate cells flagged  252,378
```

### Closure

Every count indicator, for every district, on every table: do the sub-district units sum to
the published district figure?

| Table | Comparisons | Closing |
|---|---:|---:|
| 1, 5, 9, 11, 12, 13(a), 14, 16, 18, 23 | **184,253** | **184,253 — 100%** |

No tolerance is allowed; the comparison is exact. Rates are excluded from the check and
flagged with `is_rate`, because a rate is not additive: the dictionary records that they
must be recomputed from summed numerators and denominators after any aggregation, never
averaged.

Getting this to 100% took three corrections to the check itself, each of which was a bug in
the checking rather than the data: rates had to be excluded, the column dimension had to
enter the grouping key (without it Table 13(a)'s age brackets formed a cross product), and
the Topi correction had to be applied before the mask rather than after.

### Indicator dictionary

203 entries across the ten tables, each with its universe, measure type, observation count,
completeness and range. Universes are stated per table rather than inferred, because they
differ: Table 1 is a headcount, Table 12 uses age 5+ for attendance and 10+ for literacy,
Table 14 uses 10+, and Table 23 counts households rather than people.

### A note on column naming

`table`, `row`, `col` and `column` were all used as column names and all had to be renamed
— each one broke a DuckDB query. Since the panel is published to a public SQL console, a
reserved word is a bug for every user. `test_schema.py` now fails the build if any output
column is a reserved word.

---

## Stage 5 — Warehouse and catalogue ✅ complete

`etl/stage2/build_warehouse.py`. Five Parquet tables, 5.2 MB total, sized for the
DuckDB-WASM range-read design. Catalogue entries are generated into
`warehouse/catalog_entries.json` ready to merge at Stage 6; the live
`app/data/warehouse/catalog.json` is untouched.

| Table | Rows | Size |
|---|---:|---:|
| `census2023_observations` | 2,596,564 | 5.2 MB |
| `census2023_tehsil_wide` | 727 | 57 KB |
| `census2023_units` | 591 | 21 KB |
| `census2023_indicators` | 1,409 | 12 KB |
| `census2023_withheld` | 54 | 3 KB |

The wide slice is derived from the long table in the same build, so the two cannot drift,
and its rates are recomputed from counts rather than copied — all 727 recomputed densities
agree with PBS's published figure. The withheld register is published deliberately: a gap
you can see is better than one you cannot.

The `notes` field on each catalogue entry carries the traps, because the console sidebar is
the only place a user is warned before they publish a number. The observations table
contains district *and* sub-district rows, so summing without filtering `unit_type`
double-counts; `missing` is not zero; `is_rate` must never be averaged.

---

## Stage 6 — Site integration ⬜ not started

Indicator groups in `app.js`, tehsil geometry switching, `?topic=`/`?indicator=` deep links,
CSV export, methodology copy, and the match-type caveat surfaced for `parent` and `approx`
units. Two decisions from §4b of the README are still open and both land here: whether to
adopt COD-AB geometry, and whether to aggregate the 43 new KP/Balochistan tehsils onto
parent polygons.

---

## Stage 7 — Release 🔶 machinery complete, release not cut

The apparatus is built and passing; what remains is the decision to publish.

| | |
|---|---|
| Input lock | 105 files, 193.5 MB, checksummed — `input_lock.json` |
| Build manifest | 22 artifacts, `stage2-optionA-7fef1c16a69cfd25` |
| Repeat build | **all 22 artifacts byte-identical in a fresh directory** |
| Regression tests | 18, covering every defect found in the corpus |
| Build report | [build_report.md](build_report.md) |
| Verification record | [verification.json](verification.json) |

`build_all.py` runs the whole chain in about 45 seconds and refuses to start if the tests
fail. `verify_release.py` checks a release against its manifest and against a repeat build.

---

## Status

| Stage | State |
|---|---|
| 0 Acquisition | ✅ 280 files, 592 MB, both releases, manifest and checksums |
| 1 Extraction | ✅ 4,599,521 observations, 135/135 spec checks |
| 2 Geography register | ✅ 537/591 placed, 591 DDS ids minted, 54 withheld with evidence |
| 3 Missingness | ✅ 1,120,488 dashes recovered; 25,525 cell and 1,840 row disagreements logged |
| 4 Panel assembly | ✅ 423,345 / 423,345 closure checks pass |
| 5 Warehouse | ✅ six Parquet tables, catalogue entries generated |
| Locality tier | ✅ 2,426,794 observations, 71,599 places, 99.6% hierarchy closure — see [localities_report.md](localities_report.md) |
| 6 Site | ⬜ not started — deferred; map presentation to be decided |
| 7 Release | 🔶 machinery complete and verified; release not cut |

**Nothing published. The public site is unchanged.**

See [build_report.md](build_report.md) for what was produced, what was verified, and the
defects found in the published census.

### Rebuild

```sh
python3 datadarbar/etl/stage2/build_all.py --workspace . --release <name>
python3 datadarbar/etl/stage2/verify_release.py \
  --release data_darbar_warehouse/stage2/<name> \
  --repeat  data_darbar_warehouse/stage2/<name>-repeat
```

### What the expansion changed in the code

Going from ten tables to 28 surfaced four structural problems the smaller set had hidden,
each fixed generically rather than special-cased:

- **Columns are now read from the row in which PBS numbers them**, not inferred from the
  sheet. Table 7's Punjab workbook declares 256 columns of which 249 are empty, and
  inferring width from it produced 33 times too many observations.
- **A unit banner is looked for anywhere in the row.** Table 10 centres it at column 5,
  inside the data region, and a stub-column-only reader found no units at all.
- **A row of nothing but dashes is data.** Treating it as empty dropped 186 all-dash
  transgender rows from table 11 alone — exactly the missingness the stage exists to
  preserve.
- **One emission rule replaced four layout families.** A row emits whenever it carries
  numbers, tagged with whatever stub levels are set; what differs per table is declared in
  `table_spec.py`. That covers a unit row carrying its own totals, a locality row that is
  both subtotal and level, and an indicator nested three deep.

## Census 2017 (in progress)

A second census year, read by the same reader as 2023.

- [census2017_scoping.md](census2017_scoping.md) — what 2017 publishes and how its
  40 tables map onto 2023's 33.
- [census2017_acquisition.md](census2017_acquisition.md) — 5,746 files, 643 MB,
  verified; capture `b14ca405905f8485`.
- [census2017_extraction.md](census2017_extraction.md) — 3,376,869 observations,
  135 districts, 669 units. Closure 339,011 of 339,097; all six areas
  reconcile exactly; 0.019% of rows flagged `series_ambiguous`.
- [census2017_plan.md](census2017_plan.md) — **the plan to publication**: what
  remains, in what order, and the five decisions that block it.
- [census2017_verification.md](census2017_verification.md) — checks against the
  second rendering and against independently published totals, plus
  reproducibility.

The dash-as-zero mask is **applied**: 899,800 cells that the spreadsheets record
as 0 are printed as a dash in the PDFs and are now marked missing, the zero left
in place so nothing is destroyed. Missingness is 26.8% of rows. Closure runs over 335,840 comparisons rather than
427,873, because a genuinely missing cell cannot take part in a sum. Every table
now carries every district it should: 43 missing districts went to none.

Outstanding: 17,869 of the 67,318 independent comparisons find no matching series,
mostly Pakistan's and Islamabad's; Khyber Pakhtunkhwa still shows 3,125
differences, traced to Kohistan's table 4; indicator labels are not canonicalised
for case; 46 district×table pairs do not align against their PDF; 49,835 rows were
never compared against the second rendering; Islamabad and the four locality
tables are not extracted; and there is no 2017↔2023 geography register, so the two
years cannot yet be joined.
