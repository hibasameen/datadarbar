# Census 2017 — verification against the second rendering and the published totals

Run 26 September 2026 against capture `b14ca405905f8485` and the panel in
`data_darbar_warehouse/census2017/draft-2026-09-26/`. **Nothing is published.**

Until this, the 2017 panel rested almost entirely on internal consistency: closure
showed the parts summed to the whole as PBS printed it, and exactly one series —
total population in table 1 — had been checked against a figure published
separately. That is the configuration that has misled this project before, so
three things were added.

| | |
|---|---|
| **Reconciliation** against the 135 combined district PDFs | [reconcile_pdf.py](../../etl/census2017/reconcile_pdf.py) |
| **Verification** against the 216 national and area workbooks | [verify_area_2017.py](../../etl/census2017/verify_area_2017.py) |
| **Reproducibility** — repeat build, input lock, code fingerprint | [verify_2017.py](../../etl/census2017/verify_2017.py) |

## 1. The second rendering: 29.5% of cells are a dash stored as zero

PBS publishes 2017 twice — 5,356 per-district spreadsheets, and 135 combined PDFs
carrying all 40 tables for a district. The spreadsheets are a PDF-to-Excel
conversion of the same material, and the 2023 release showed what that costs.

**3,888,682 cells compared, across all 134 districts that have spreadsheets.**

| | Cells | Share |
|---|---:|---:|
| **Printed as a dash, stored as 0** | **1,146,340** | **29.5%** |
| Value disagreements | 30,768 | 0.791% |
| Rows that could not be aligned | 49,835 | — |

**Nearly a third of the compared cells are a printed missing-value dash that the
spreadsheet stores as zero.** The 2017 release has the same defect as 2023, at
comparable scale. The consequence is not subtle: in those columns a `0` in the
current panel cannot be read as "none" rather than "not reported", and any rate
computed from the spreadsheets alone is wrong. It is present in every table, from
5.4% of table 37's cells to 37.9% of table 14's.

| Table | Compared | Dash-as-zero | | Table | Compared | Dash-as-zero |
|---|---:|---:|---|---|---:|---:|
| 1 | 14,659 | 7.6% | | 13 | 47,496 | 16.5% |
| 3 | 20,110 | 21.8% | | 14 | 1,180,540 | **37.9%** |
| 4 | 722,338 | 26.8% | | 15 | 156,519 | 31.8% |
| 5 | 110,232 | 19.7% | | 16 | 142,759 | 24.2% |
| 6 | 112,275 | 23.1% | | 17 | 27,260 | 22.0% |
| 7 | 62,812 | 28.7% | | 20 | 4,860 | 10.3% |
| 8 | 270,070 | 32.3% | | 27 | 48,471 | 11.5% |
| 9 | 55,453 | **37.2%** | | 37 | 25,311 | 5.4% |
| 10 | 86,508 | 21.3% | | | | |
| 11 | 86,625 | 27.1% | | | | |
| 12 | 714,384 | 25.0% | | | | |

### The mask is now applied

**899,800 cells in the panel have been reclassified from zero to missing.**

The mask joins the panel on `(district, table, src_row, src_col)` — the cell's
position in the workbook — rather than on the row's printed label, because a
label like `10 -- 14` recurs once per unit, locality and sex. The correction is
deliberately conservative: a masked cell **keeps its 0 in `value`** and gains
`missing = true`, so nothing is destroyed and the change can be undone by
ignoring the flag.

Checked after applying:

| | |
|---|---|
| Newly masked cells that held a zero | **895,505 of 895,505** (on the build where this was checked) |
| Newly masked cells that held a real value | **0** |
| Values altered | **0** — only the flag changed |
| Rows added or lost | 0 |
| Reconciliation against each area's own table | still 5 of 5 exact |

Missingness now stands at **26.8% of rows**, from 1,413 before — ranging from
4.7% of table 37 to 33.9% of table 14.

Closure now runs over 335,840 comparisons rather than 427,873, because a cell
that is genuinely missing cannot take part in a sum; it passes 335,754, the same
rate as before. The 1,146,340 cells in the mask exceed the 895,505 applied because
the mask also covers rate columns and rows the panel does not carry.

### The 30,768 disagreements are not findings

They fall in **46 of a possible 2,546 district×table pairs — 1.8%** — with the
worst five holding 70% and Lahore's table 14 alone holding 21%. A defect in PBS's
own data would spread across districts; a concentration like this is the signature
of the alignment failing in particular files. In the three-district trial, every
case inspected was a one-row shift in which the PDF value equalled the *previous*
row's spreadsheet value.

They are kept in `value_disagreements_2017.csv.gz` as a work list, not as defects,
and no correction is made on their basis.

### What the alignment took

Three fixes, the third decisive:

**Row labels containing digits were read as data.** Tables 6, 12, 14, 15 and 16
are stubbed with age ranges, so `10 -- 14` yields the numeric tokens "10" and
"14"; scanning a line for its first number then leaves no label at all and the row
is dropped. Not one row of tables 6, 12 or 16 aligned. Splitting on the character
column where the data begins — read off PBS's own column-number row — took
compared cells from 5,195 to 45,036 in the trial and disagreements from 358 to 28.

**A key-indexed lookup consumed the wrong occurrences.** Replaced by a
forward-only walk, so a label recurring once per unit, locality and sex resolves
in order.

**Nine districts never paired**, because the two renderings spell them
differently: CHAGAI/CHAGHI, KHUZDAR/KHUZADAR, MALIR/MILIR, MUZAFFARGARH/
MUZAFARGARH, DERA BUGTI/DERABUGHTI, KILLA SAIFULLAH/KILLASAIFULLA, MANDI
BAHAUDDIN/Mandi Bahuddin, SHAHEED BENAZIRABAD/SHAHEEDBANAZIRABAD. Nearest-name
matching at a 0.82 cutoff recovers seven; MALIR/MILIR scores 0.80 and is an
explicit alias, because lowering the cutoff to reach it would start admitting
pairs that are merely similar — and a wrong pairing would compare one district's
spreadsheet against another's PDF and report the difference as a defect. Only
Islamabad is now unpaired, correctly: it publishes no per-district spreadsheets.

## 2. Independent totals: from one series to 67,318 comparisons

PBS publishes each table for Pakistan and for each of the six areas below it —
216 workbooks,
reached through a different part of the archive page and absent from the index the
district files come from. Comparing each area's published figure against the sum
of that area's districts in the panel extends independent verification from one
series to every table and column.

| | |
|---|---:|
| Comparisons | **67,318** |
| Exact | **42,455** |
| Differ | 6,994 |
| No matching series | 17,869 |

"Area" rather than "province" throughout: four of these six are provinces, FATA
was a federal territory merged into Khyber Pakhtunkhwa in 2018, and Islamabad is
the federal capital territory.

| Area | Exact | Differ | No match |
|---|---:|---:|---:|
| Sindh | 9,308 | 488 | 336 |
| Punjab | 9,083 | 509 | 540 |
| FATA | 8,001 | **0** | 2,135 |
| **Balochistan** | **7,470** | **138** | 233 |
| Khyber Pakhtunkhwa | 5,474 | 3,125 | 1,537 |
| Pakistan | 3,119 | 2,734 | 7,697 |
| Islamabad | 0 | 0 | 5,391 |

The check runs as one join rather than a query per row — 7 seconds instead of
about a quarter of an hour, which matters because it is re-run after every
change.

Two defects in the check itself had to be fixed before these numbers meant
anything. **An empty column label and a null one are the same thing, but not to
SQL**: the panel reaches DuckDB through a CSV, where `''` is read as NULL, while
the area side kept Python's empty string, and `'' IS NOT DISTINCT FROM NULL` is
false — so every series whose whole header is consumed by locality and sex, all
of tables 4 and 5, matched nothing at all. And the national row had to be
compared against every district *plus Islamabad's own published figure*, since
Islamabad has no per-district spreadsheets.

Islamabad's 5,391 are expected — it is not in the panel.

### Balochistan's mismatches are coverage, not error

Balochistan first reported 7,211 differences against 2,257 exact, far out of line
with Punjab and Sindh. Two causes, and neither is a wrong value.

**Division bleed.** An area workbook gives the area total, then works down
through divisions and districts. `unit_type` does not recognise a bare DIVISION —
correctly, since 2023 has no such tier — so the reader never closed the area's
block and `KALAT DIVISION`'s rows were compared as though they were Balochistan's.
Cutting the sheet at the first division took Balochistan to 5,064 and removed
16,033 spurious comparisons corpus-wide.

**Incomplete tables — since fixed.** The remainder had a signature that left no
room for doubt: the panel was lower in every case and higher in none, because it
held fewer districts than the area total covered. Table 37 was short 21
districts nationally; 25 (table, area) pairs were short, 43 districts in all.

The cause was that **40 workbooks carry no unit row at all** — they open straight
at ALL LOCALITIES, so the reader never opened a block and the file yielded
nothing. The district is now supplied from that district's own table 1, reached
through the directory the capture recorded rather than from the filename, and
held to the same bound as a recovered label: it cannot contain more people than
table 1 gives it. **48 of 48 pass.**

Coverage went from 43 missing districts to **none**. Three apparent gaps remain
and are correct: Karachi East, Karachi South and Korangi are absent from table 3,
which counts *rural* localities, and all three have a rural population of zero.

Balochistan fell from 5,064 differences to **138**, which confirms the diagnosis.

### Khyber Pakhtunkhwa is now the outlier

KP barely moved (3,132 to 3,125) and is the remaining anomaly. It is short by
784,711 on tables 4 and 27 — exactly Kohistan's population — and the cause is
that **Kohistan's table 4 has no `All Ages` row under any spelling**, where the
other 133 districts do. Kohistan has caused more trouble than any other district
in this release; this is not yet diagnosed further.

A smaller, general issue surfaced alongside it: **indicator labels vary in case
between files** — `Below 1`/`BELOW 1`, `All Ages`/`ALL AGES`, and six such pairs
in table 7. Column labels are canonicalised; indicators are not.

### The national comparison

Pakistan's row is compared against every district in the panel *plus Islamabad's
own published figure* for the same series, because Islamabad has no per-district
spreadsheets and without it the two sides do not cover the same ground. It remains
the weakest area at 3,119 exact against 2,734 differing and 7,697 unmatched.

## 3. Reproducibility

```
extract:  2 of 2 artifacts byte-identical
panel:    6 of 6 artifacts byte-identical
input capture b14ca405905f8485 matches its lock
reproducible: True
```

Two independent runs of the whole pipeline produce identical bytes, and the build
re-verifies the capture against its lock before trusting it.

## What this changes, and what it does not

Established: the panel is reproducible; 35,937 series match figures PBS published
separately; FATA matches on every one of 9,386; and 895,505 cells that the
spreadsheets record as zero are now correctly marked as not reported.

Not established, and the reason 2017 is still not a finished layer:

- **17,869 comparisons find no matching series**, so the denominator of the
  verification is still not what it should be — most of them Pakistan's and
  Islamabad's.
- **Khyber Pakhtunkhwa still has 3,125 differences**, traced to Kohistan's
  table 4 but not resolved.
- **Indicator labels are not canonicalised for case**, unlike column labels.
- **46 district×table pairs do not align** against their PDF.
- 49,835 rows were never compared against the second rendering at all.

## Reproduce

```bash
python3 datadarbar/etl/census2017/reconcile_pdf.py --dir raw_data/pbs/census2017_sources/2026-09-26 --out <dir>
```

```bash
python3 datadarbar/etl/census2017/verify_area_2017.py --panel <panel> --capture raw_data/pbs/census2017_sources/2026-09-26 --out <dir>
```

```bash
python3 datadarbar/etl/census2017/verify_2017.py --build <a> --repeat <b> --capture raw_data/pbs/census2017_sources/2026-09-26
```
