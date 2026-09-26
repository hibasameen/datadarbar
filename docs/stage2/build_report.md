# Stage 2 — build report

Built 26 September 2026, in the workspace at
`data_darbar_warehouse/stage2/optionB-2026-09-26/`, release `stage2-d1cd31fa08133667`. **Nothing is published; the public
website is unchanged.**

All 28 cross-tabulated tables PBS publishes for Census 2023, at district and sub-district
level, built from the Excel release and cross-checked cell by cell against the PDF release.
Scope and reasoning are in [README.md](README.md); the stage sequence is in
[STAGES.md](STAGES.md).

The first build covered ten tables; this extends it to the full set. Tables 27-30 do not
exist. Tables 31-35 are the locality tables and remain a separate stage.

## What was produced

| | |
|---|---:|
| Observations | **4,599,521** |
| Tables | 27 in the unit panel, plus table 2 as named urban localities |
| Units | 727 - 136 districts and **591 sub-district** |
| Named urban localities | 629, across 121 districts |
| Indicator series | 1,425 |
| Printed dashes preserved as missing | 1,565,334 |
| ...of which recovered from the PDFs | **1,120,488** |
| Cells on rows the two PBS renderings dispute | 27,033 |
| Published warehouse | ~12 MB across six Parquet tables |

Five tables - 6, 7, 8, 10 and 13 - are published without any sub-district breakdown, so
they contribute district rows only.

## Verification

| Check | Result |
|---|---|
| Layout specification | 135 of 135 workbooks match their declared shape |
| Unit roster | 591 sub-district units, consistent across tables |
| **District closure** | **423,345 of 423,345 comparisons pass, exactly, no tolerance** |
| Recomputed rates | 727 of 727 population densities agree with the published figure |
| National total | 241,499,431 - exactly the published figure, as are all five area totals |
| Regression tests | 19 pass |
| Repeat build | **all 24 artifacts byte-identical in a fresh directory** |
| Area column | `province_area`, not `province` — two of its values, FATA and Islamabad, are not provinces |
| Input lock | 285 files, 601.5 MB, checksummed |

Closure is the strongest check available: for every count indicator, in every district, on
every table, the sub-district units must sum to the published district figure. Rates are
excluded and flagged with `is_rate`, because a rate is not additive.

## Defects found in the published census

These are errors in PBS's own release, not in the extraction. All are recorded with
evidence.

**Table 6's "KP district-wise" file is the national file.** `table_6_kp_districts.xlsx`
contains all 135 districts of Pakistan, ending in Balochistan and Islamabad - and there is
no KP-only table 6, so KP's data exists nowhere else. PBS links it as "KP - District Wise".
It is the only file in the corpus spanning more than one province. Each district's province
is therefore taken from the Table 1 roster rather than from the filename, and 176,386
duplicate observations are dropped where a district appears both in its own province's file
and in this one.

**A find-and-replace destroyed six place names in Table 1.** The literal string `ALL` is
deleted from six names, and only in Table 1: `ALLAI` to `AI`, `KALLAR KAHAR` to
`KAR KAHAR`, `KALLAR SAYADDAN` to `KAR SAYADDAN`, `TANDO ALLAHYAR` to `TANDO AHYAR`
(district and taluka), `KALLAG` to `KAG`. The other 27 tables agree on the correct
spellings. Stage 1 had recorded the Tando Allahyar case alone; this generalises it to one
reproducible defect.

**Topi Tehsil's rural population is wrong in the Excel.** `table_1_kp.xlsx` gives 382,562;
the PDF gives 307,695. The difference, 74,867, is exactly Topi's urban figure - the Excel
overwrote the rural cell with the tehsil total. Swabi does not close without the PDF value.
It is the only cell in the corpus where adopting the PDF makes a district close, and the
only value changed.

**The Excel release turns printed dashes into zeros, inconsistently** - table by table, and
in some files cell by cell. 1,120,488 affected cells were recovered by aligning every row
against the PDF. Any rate computed from the Excel alone, in those columns, is wrong.

**One whole series in Table 14 differs between the renderings, everywhere.** For Abbottabad,
"Not L.F & Stud (15 to 24)" reads 97,801 / 27,817 / 69,959 in the Excel and
112,505 / 33,294 / 79,186 in the PDF, while Population, Employed, Paid Employee, Unemployed
and every other row in the block match exactly. This holds for **all 727 rows of that
indicator, in every province**, and it is the only Table 14 indicator affected. A further
25,525 individual cells disagree elsewhere. Nothing is corrected on this basis - there is
no ground for preferring one rendering - but the affected cells carry `renderings_disagree`
so a researcher can see a figure is disputed by its own publisher.

**Punjab's Table 1 PDF clips its last column** off the page, and prints two columns with a
single space between them (`13,957 105.18`), which a naive whitespace split merges.

**Table 7's Punjab workbook declares 256 columns**, 249 of them empty. Columns are therefore
read from the row in which PBS numbers them, not inferred from the sheet.

**Table 10 centres its unit banner in the middle of the data region**, at column 5 rather
than in the stub column.

**Table 10's KP link is broken on both PBS pages** - the "KP - District Wise" slot points at
Table 1's file. The real file exists at the predictable URL. The published link tables are
not a reliable index of what exists.

## Geography

591 sub-district units, each with a stable `dds_id`, and external keys where evidenced:

| | Units |
|---|---:|
| OCHA COD-AB p-code (reviewed September 2024) | 493 |
| geoBoundaries ADM3 polygon (2017 vintage) | 537 |
| Withheld | 54 |

Neither external source covers every published unit, which is why the panel carries its own
identifier. The 54 withheld units keep their data; they are absent only from a map. Nine are
Karachi sub-divisions carved out of the old town system, which neither boundary source
reproduces.

## Known limits

- **54 units cannot be drawn on a map** - 5.4% of population, concentrated in Karachi and
  Peshawar. All district figures are complete.
- **3,495 rows are not masked** for missingness: where the renderings disagree, or the
  columns could not be lined up. Those rows may still carry a zero that was printed as a
  dash.
- **This is a 2023 cross-section.** No 2017 comparison at sub-district level; the boundary
  work that would justify one has not been done.
- **Table 2 does not sum to anything.** Its 629 rows are named places, not administrative
  units, and are published as their own table.
- **Tables 13(a), 13(b) and 14 are the least certain** - they nest deepest and account for
  most of the rows that could not be aligned.

## Rebuild

```sh
python3 datadarbar/etl/stage2/build_all.py --workspace . --release <name>
python3 datadarbar/etl/stage2/verify_release.py \
  --release data_darbar_warehouse/stage2/<name> \
  --repeat  data_darbar_warehouse/stage2/<name>-repeat
```

Verified runtime: Python 3.11.5, DuckDB 1.5.5, openpyxl 3.1.5, Poppler 21.11.0 on macOS
arm64. A different Poppler may change the extracted PDF text and therefore the missingness
mask; compare before accepting it. The build takes about six minutes, most of it text
extraction from 592 MB of PDF, and refuses to start if the tests fail.
