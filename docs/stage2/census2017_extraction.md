# Census 2017 — first extraction

Built 26 September 2026 from capture `b14ca405905f8485`. **Nothing is published,
and this is not yet a release**: nine of nineteen tables are fully keyed and ten
are not. Sources and their defects are in
[census2017_acquisition.md](census2017_acquisition.md).

The 2017 tables are read by the *same* reader as 2023
([read_workbook.py](../../etl/stage2/read_workbook.py)), with 2017's own layout
declarations and three additions described below. The 2023 release is byte-for-byte
unchanged by all of this — verified by rebuilding it and comparing all 18 data
artifacts after every change.

## What was produced

| | |
|---|---:|
| Observations | **3,362,716** |
| Cells marked missing | **26.8% of rows**, of which 899,800 recovered from the PDFs |
| Tables | 19 of the 24 declared (locality tables not yet run) |
| Districts | **134** — all of them, including 13 FATA agencies and Frontier Regions |
| District rows supplied where the workbook named none | 40 |
| Sub-district units | 533 |
| Series | 1,633 |
| Flagged `series_ambiguous` | **636 (0.019%)**, in 8 of 19 tables |
| Tables entirely clean | **12** — tables 1, 3, 4, 5, 6, 7, 8, 10, 12, 14, 15, 20 |

## Verification

| Check | Result |
|---|---|
| Observations in = observations out | 3,346,270 = 3,346,270, no fan-out |
| Workbooks read | 2,532 read, 12 header-recovered, 2 correctly skipped as empty |
| Observations with no district | **0** |
| **District closure** | **335,754 of 335,840 pass exactly, no tolerance** |
| Each area's total vs PBS's separately published area table | **5 of 5 exact** |
| Regression tests | **64 pass** (22 shared reader, 19 normalisation, 14 unit recovery, 9 header recovery) |
| Repeat build | **byte-identical**, extract and panel |

Balochistan 12,335,129 · FATA 4,993,044 · KP 30,508,920 · Punjab 109,989,655 ·
Sindh 47,854,510 — each exactly the figure in that area's own table, published
through a different part of the archive page and so genuinely independent
evidence. Four of these are provinces; FATA was the Federally Administered Tribal
Areas, a federal territory merged into Khyber Pakhtunkhwa in 2018. Islamabad reads 0 because its 39 spreadsheets are the
anchor-listed set, not yet extracted.

## What had to be added for 2017

**FATA's unit vocabulary, and one trap inside it.** FATA existed in 2017 and was
merged into KP in 2018, so `BAJAUR AGENCY` and `FR BANNU` appear in these tables
and in no 2023 table. Without them, the seven agencies lost their district row
silently. Worse, each Frontier Region's row is followed by a sub-unit called
`TRIBAL AREA ADJ. BANNU DISTRICT` — which contains the word DISTRICT, so the
reader promoted that sub-unit to district level and discarded the FR itself. Six
districts were wrongly named before this was caught.

**Merged cell ranges.** xlrd only reports them when a workbook is opened with
`formatting_info`, and the first extract did not. Table 10 arranges nine columns
as three locality blocks — TOTAL POPULATION, RURAL, URBAN — each over TOTAL,
PAKISTANI and NON-PAKISTANI. Without the merges every nationality figure was
reported as locality `all`: the table looked complete while two thirds of it was
mislabelled. PBS's own merge is then sometimes a column short of the block it
heads, so a banner is widened to the next labelled column, or to the last numbered
column when it is the rightmost. Only existing merges are widened, never
forward-filled — a general fill smears table 1's `POPULATION - 2017` across the
sex ratio, density and household size columns, inventing three names for one
column.

**Header-block recovery.** 21 workbooks lost their header in PBS's PDF-to-Excel
conversion. Column positions are recovered from the widest data row and labels
borrowed in order from a sibling; 36 further headerless files are genuinely empty
and are left so. This is validated by the province totals: before recovery, KP
and Punjab were short by exactly 1,625,477 and 3,040,826 — Swabi's and Okara's
populations.

## Defects found in the published 2017 census

**Two districts are misspelled, each in exactly one table.** `SHEIKUPURA DISTRICT`
for Sheikhupura in table 6, and `KILLA ABDULLAB DISTRICT` — B for H — in table 17.
Both would otherwise appear as extra districts.

**Kohistan folds the locality into the unit name.** Its workbooks label the unit
`KOHISTAN DISTRICT - RURAL`, `KOHISTAN DISTRICT-RURAL` and
`KOHISTAN DISTRICT - URBAN` instead of using separate RURAL and URBAN stub rows as
every other district does — three spellings, which become three extra districts if
left alone.

**Twelve workbooks omit the district total row entirely.** Eight of them are
Musakhel's; the others are single tables in Mansehra, Jaffarabad, Nushki and FR
Kohat. 11,934 observations had no district. The district is recovered from the
tehsil, using the register table 1 establishes — and the register is restricted to
tehsil names that resolve to exactly one district, because a bare name is not
unique in Pakistan: `SAHIWAL TEHSIL` exists in both Sahiwal and Sargodha. Ignoring
that produced 25,500 observations more than were read, and dropped closure from
99.98% to 96.9%.

**One measure is spelled differently by a single file against 132 or 133.**
`INTER-MEDIATE`, `MASTER & ABOVE`, `AREA (SQ KM)`, `1998- 2017`. Resolved by
majority within the table. Note that PBS's *majority* spelling is sometimes itself
a typo — `NEVER ATTAINDED` in 132 files against `NEVER ATTENDED` in one — and the
majority is still adopted, because renaming the series would be a different
decision.

**Half the corpus drops the final I from NON-PAKISTANI.** 64 files against 66.

## Series identification

An observation is addressed by
`(table_id, unit, district, locality, sex, indicator, col_label)`. Where two rows
share that address and disagree on the value, a distinction present in the
workbook has been lost. **606 of 3,346,270 rows (0.018%) are still in that
state**, down from 33,818 when the defect was first measured.

| Table | Rows | Flagged |
|---|---:|---:|
| 11 mother tongue | 87,769 | 178 |
| 17 disabled population | 27,056 | 118 |
| 13 literacy | 47,856 | 122 |
| 37 tenure and facilities | 24,582 | 84 |
| 9 marital status | 55,972 | 54 |
| 16 usual activity | 143,352 | 40 |
| 27 types of housing unit | 53,508 | 10 |

Twelve tables are entirely clean: **1, 3, 4, 5, 6, 7, 8, 10, 12, 14, 15, 20.**

### The five defects this fixes

**A stub nesting deeper than the reader's three slots.** The reader carries
`locality`, `sex` and `indicator`; anything outside the first two vocabularies
sets `indicator`, overwriting it. Table 37 nests four deep counting the unit -
locality, then facility (KITCHEN, BATHROOM, LATRINE), then state (SEPARATE,
SHARED, NONE) - so all three NONE rows, 22,852 / 19,154 / 13,593 for Abbottabad,
landed on one address. The workbook itself says which rows are headings: a
heading carries no figures. Under `group_indicator` the indicator becomes a path,
`KITCHEN / NONE`, and `group_exits` names the leaves that sit at the heading's own
level - table 37's `TOTAL :` totals the locality, not the latrine.

**A vocabulary collision.** `TOTAL` is in the locality vocabulary, because in most
tables it means all localities. Table 27 uses it for a sum over housing types
inside a locality block, so meeting it in the Rural block reset the locality to
`all` and filed Rural's total as the district total. `not_locality` excludes it
for that table.

**Unit names PBS omitted mid-file.** Sargodha's table 12 has eight unit blocks and
five names; the sixth, seventh and eighth are introduced by an empty cell, so they
were read as continuations of SAHIWAL TEHSIL and three tehsils vanished. This is
the most damaging of the four, because unlike a mislabelled column it makes
specific numbers wrong for specific places. Each block opens with the same label,
so an opener with no unit name before it is an unnamed block, and the names come
from the district's roster in table 1, aligned alphabetically.

  **8 labels are recovered and 13 blocks refused.** The refusal matters more than
  the recovery: the opener is a heuristic - the label that usually follows a unit
  name - and in some layouts it recurs WITHIN a unit rather than once per unit.
  Kohistan's table 9 opens each block with ALL SEXES, which appears once per
  locality, so eight of its ten openers looked unnamed and the first version of
  this christened the district's own rural and urban sub-blocks DASSU, KANDIA,
  PALAS and PATTAN. That was worse than the defect it replaced: the figures had
  merely been misattributed before, and afterwards they were confidently wrong
  under real place names, with the ambiguity flag that had been signalling the
  problem switched off. A file is now refused outright unless its unnamed blocks
  account for exactly the roster names that are missing.

  What is applied is checked two ways. Sargodha's three are confirmed by value:
  table 13 labels all eight of its units and reports SARGODHA, SHAHPUR and
  SILLANWALI's 10+ population as 1,160,533, 266,223 and 251,551 - exactly the
  three unlabelled blocks' totals, which with the other four also sum to 2,768,046,
  the district's own figure. And every recovered unit is held to a bound that
  needs no judgement: it cannot contain more people than table 1 says it contains.
  That bound is what caught the Kohistan error - the invented KANDIA held 228,350
  where Kandia has 77,101 - and it now runs on every build. **8 of 8 pass.**

**A unit sharing a row with another label.** Attock's table 14 puts FATEH JANG
TEHSIL in column 4 of the very row whose stub reads OVERALL. The reader only
looked elsewhere in the row when the stub was *empty*, so that block and its
successors were filed under the previous tehsil. It now checks for a unit
alongside a non-unit stub label.

**A banner present as text but never merged.** Sanghar's table 5 has the same
layout as Abbottabad's - TOTAL, RURAL and URBAN over four sex columns each - but
where Abbottabad merges columns 1-4, 5-8 and 9-12, Sanghar merges nothing. The
banner text then applies to its own column only, nine of twelve columns get no
locality, and the district's rural and urban figures are read as if they were its
total. Three files did this, and they were three quarters of the remaining
ambiguity.

  The missing spans are synthesised, gated on the row immediately below being a
  complete sub-header - a label for every numbered column. That gate is what makes
  it safe rather than a forward fill: table 1's header has no complete sub-header
  row, because area, 1998 population and the growth rate are labelled on the upper
  row only, and filling there smears POPULATION - 2017 across the sex ratio,
  density and household size columns. Table 1 is untouched, and table 5 went from
  1,803 flagged rows to none.

### Why closure did not catch any of it

Closure groups by the same key, and the stub structure repeats identically for the
district row and every sub-district row, so a collapse happens on both sides of
the comparison and the sums still agree. Closure passed at 426,272 of 426,362 with
all four defects present.

**Closure tests arithmetic consistency, not identification.** A systematic
labelling error is invisible to it. This surfaced only by asking a different
question - is the key unique? - which is worth carrying into any future table set.

### Five bugs in the fix itself, found and corrected

Each made the panel look better or worse than it was, and each is a caution about
duplicating logic rather than reusing it.

The ambiguity flag was first written **per table**, marking every row of any table
holding one bad key - 2,459,883 observations, seventy times the real figure. Made
per key, it then **silently skipped tables 4, 5 and 20**, whose `col_label` is NULL
because locality and sex consume the whole header, and NULL never equals NULL in a
join; table 5 was reported clean with 1,803 affected rows.

A **variable name collision** made the build print `closure: 1 of 427,621`. The
bound check reused the name `ok`, which already held the closure pass count. The
stored figures were never affected - the statistics are written before that point -
but the headline number in the build log was alarming and wrong.

The recovery began with a **private copy of the reader's unit vocabulary**, which
drifted from it immediately: it omitted DE-EXCLUDED AREA RAJANPUR, the one unit in
the country carrying none of the usual keywords, and it looked only in the stub
column. It now calls the reader's own `unit_type` and `row_unit`. Even so it first
treated a row that both names a unit and opens a block as only the former, which
dropped that row from the opener list and assigned HAZRO TEHSIL to Hasan Abdal's
block - corrupting a file the reader already handled correctly.

### What is left

606 rows across seven tables, none over 180. Kohistan's refused blocks are part of
it, and correctly so - they are flagged rather than named wrongly. None of the
remainder has been diagnosed.

## Verified against the second rendering and the published totals

Since this report was first written the panel has been checked against the 135
combined PDFs and the 216 national and area workbooks, and rebuilt twice to
confirm it is reproducible. See
[census2017_verification.md](census2017_verification.md). The headline result
belongs here too, because it bears on how the panel may be used:

**1,146,340 of 3,888,682 compared cells (29.5%) are printed as a dash in the PDF
and stored as 0 in the spreadsheet**, and **899,800 of them have been applied to
this panel** — reclassified from zero to missing, with the zero left in `value`
so nothing is destroyed. Missingness is now 26.8% of rows, from 1,413 before. A `0` in this panel can now be read as a reported zero in the columns
that were checked.

## Also outstanding

- **Islamabad is not in the panel.** Its 39 anchor-listed workbooks, and the 36
  national and 180 provincial ones, are captured but not extracted.
- **The four locality tables (23–26) are not run.** They go through the locality
  reader, not this spec.
- **Five tables are not profiled at all**, and 16 of the 40 have no declared layout.
- **No 2017↔2023 geography register yet**, so the two years cannot be joined. The
  2017 frame is the pre-merger one: FATA separate, KP with 25 districts.
- **District names differ between renderings** (`ABBOTTABAD` / `ABBOTTABAD
  DISTRICT`, `CHAGHI` / `CHAGAI`, `MILIR` / `MALIR`), so the PDFs cannot yet be
  used to cross-check the spreadsheets as they were for 2023.

## Reproduce

```bash
python3 datadarbar/etl/census2017/extract_2017.py --dir raw_data/pbs/census2017_sources/2026-09-26 --out <extract>
```

```bash
python3 datadarbar/etl/census2017/build_panel_2017.py --extract <extract> --capture raw_data/pbs/census2017_sources/2026-09-26 --out <panel>
```
