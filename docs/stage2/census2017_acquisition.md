# Census 2017 — acquisition report

Captured 26 September 2026 from <https://www.pbs.gov.pk/censusarchive/> into
`raw_data/pbs/census2017_sources/2026-09-26/`, capture `b14ca405905f8485`.
**Nothing is published and nothing is extracted yet; this records the sources and
what is wrong with them.** Scope and the 2017→2023 table mapping are in
[census2017_scoping.md](census2017_scoping.md).

## What was captured

| | Files | Size |
|---|---:|---:|
| Per-district spreadsheets (`.xls`) | 5,356 | 240 MB |
| Combined per-district PDFs | 135 | 364 MB |
| National, provincial and Islamabad spreadsheets | **255** | 10 MB |
| **Total** | **5,746** | **643 MB** |

40 tables × 134 districts, plus 135 combined PDFs covering all 135 districts.
Zero download failures; every file's size matches the manifest, and sha256 was
re-verified on all 135 PDFs plus 200 randomly chosen spreadsheets — no mismatches.

## The 255 files that are not in the index

The archive page carries its per-district index inside `<script>` blocks
(`var PREFIX`, `var DATA`). A second set of tables is published only as ordinary
`<a href>` markup in the page's HTML tables, and is therefore invisible to an
index reader:

| Area | Tables |
|---|---:|
| Pakistan | 36 |
| Punjab, Sindh, KPK, Balochistan, FATA | 36 each |
| Islamabad | 39 |

These matter more than their 10 MB suggests. **Islamabad has no per-district
spreadsheets at all** — the script index covers 134 districts, not 135 — so these
39 files are its only spreadsheet rendering. And the national and provincial
tables are *independently published totals*, which is the only kind of check that
has ever caught a real error in this project.

The lesson generalises the one already learned here: PBS publishes the same
material through more than one mechanism, and neither the anchors alone nor the
script index alone is a complete index of what exists.

## Verification against independently published figures

The area tables are labelled by anchor text, not by filename — `Table01p-1.xls`
and `Table01p-3.xls` are different provinces, WordPress having appended its
duplicate-upload suffix. Every one was therefore checked against the unit label
inside the workbook: **254 of 255 confirmed, none contradicted.** The remaining
file (Balochistan table 14) puts its banner at column 5, mid-data-region, which
the shared reader handles but the check's 4-column scan did not.

Then the two closure checks that actually test the capture:

| Check | Result |
|---|---|
| Pakistan total, 2017 | **207,684,626** — the published figure |
| Six sub-areas sum to the national total | **exact**, on area, all sexes, male, female and transgender |
| | the four provinces, FATA and the federal capital territory |
| Each area's districts sum to its published area total | **exact, all five** |
| All 134 districts sum to Pakistan minus Islamabad | **205,681,258 — exact** |

## Defects found in the published 2017 census

**Filenames do not identify their contents.** Ghotki's directory holds files
named for Dadu — `Table-02-DAD.xls`, `Table-04-DAD.xls` — and its table 1 is
`Table--0-GHO.xls`, with a doubled hyphen and an unpadded number. The *contents*
are correct Ghotki data in every case; only the names are wrong. Kohistan's
table 1 is named `Table01p.xls`, the suffix the province-level files use. This is
the 2017 form of the 2023 defect in which table 6's "KP district-wise" file is
the national file, and it forces the same rule: **the unit is read from the
workbook's own label, never from the path.**

**Lahore is reported as having 1,131,026 rural residents and no rural
localities.** Table 1 gives Lahore a rural population of 1,131,026 — 9.2% of the
district. Tables 3, 23 and 24, which count and list rural localities, are empty
for Lahore, and table 23 contains a single cell of explanation:
`LAHORE IS URBANIZED.` The published census therefore contradicts itself: over a
million people are rural in one table and live in no rural locality in another.
Nothing is corrected on this basis; it is recorded so that a researcher summing
2017 rural localities knows Lahore's million are missing from that total.

**57 of 5,611 spreadsheets have no column-number row.** PBS's 2017 spreadsheets
are a PDF-to-Excel conversion, and for these the conversion dropped the header
block — no title, no banner, no row in which PBS numbers its columns. None are
corrupt. Two different problems hide behind the one symptom, and they need
opposite treatment (see below).

**Kohistan is missing four tables, three of them explicably.** Kohistan's own
table 1 reports an urban population of exactly 0, so tables 25 and 26 (urban
localities) have nothing to report. Table 23 is absent from the spreadsheets but
present in the combined PDF; table 24 is absent from both.

## The 57 headerless files

| | Files | Treatment |
|---|---:|---|
| Effectively empty | 36 | left empty — the absence is the fact |
| Real data, no header | **21** | column positions recovered |

**The 36 empty files are explicably empty.** Every one is a locality table for a
district with no localities of that kind: tables 25 and 26 for fourteen districts
whose own table 1 reports zero urban population (Batagram, Buner, Shangla,
Sherani and nine FATA agencies), and tables 3, 23 and 24 for Karachi East and
Karachi South, which are 100% urban. Repairing these into existence would invent
data. Lahore is the sole case where an empty locality table contradicts a
non-zero population, and it is recorded as a defect above rather than repaired.

**The 21 files with real data are recoverable, by order rather than position.**
The converted grid is sparse — Swabi's table 1 spreads 12 measures across 26
columns with spacer columns between them, where Mardan's compact copy uses 12
columns exactly — so column positions cannot be borrowed from a sibling file.
The *sequence* of measures is identical, though, and the widest data row in the
file is the unit's own total row, carrying every measure the table publishes.
Reading positions off that row reconstructs exactly what PBS's numbered row
would have said.

This was checked, not assumed. Before recovery, KP and Punjab failed to close
against their own published area totals by 1,625,477 and 3,040,826 — precisely
Swabi's and Okara's district populations, the two table-1 files among the 21.
After recovery, **all five areas close exactly** — the four provinces and FATA. The recovery is in
[headerless.py](../../etl/census2017/headerless.py); `synth_columns` returns
nothing when no row is wide enough to be a total row, which is how the empty
files are distinguished from the recoverable ones rather than being repaired.

## Known limits

- **District names differ between the two renderings** and within the
  spreadsheet index: `ABBOTTABAD` vs `ABBOTTABAD DISTRICT`, `Rajan Pur` vs
  `RAJANPUR DISTRICT`, `KARACHICENTRAL` vs `KARACHI CENTRAL DISTRICT`,
  `CHAGHI` vs `CHAGAI`, `MILIR` vs `MALIR`, `KHUZADAR` vs `KHUZDAR`. A
  name register is needed before the two renderings can be reconciled, and
  before 2017 can be joined to 2023.
- **Sindh's provincial table carries a DIVISION tier** between province and
  district that the other provinces' files do not. The 2023 panel has no such
  tier.
- **Nothing is extracted yet.** The layouts are declared for 24 tables in
  [table_spec_2017.py](../../etl/census2017/table_spec_2017.py); the remaining
  16 are not profiled.
- **The 2017 geography is the pre-merger one** — FATA is a separate unit with 13
  agencies and FRs, and KP has 25 districts, not the post-2018 arrangement. Any
  join to 2023 has to cross that boundary change.

## Reproduce

```bash
python3 datadarbar/etl/census2017/fetch_sources.py --dir raw_data/pbs/census2017_sources/2026-09-26
```

```bash
python3 datadarbar/etl/census2017/fetch_anchor_tables.py --dir raw_data/pbs/census2017_sources/2026-09-26
```

```bash
python3 datadarbar/etl/census2017/lock_acquisition.py --dir raw_data/pbs/census2017_sources/2026-09-26
```

Both fetchers are resume-safe and validate magic bytes before writing. Verified
on Python 3.11.5, xlrd 2.0.2, Poppler 21.11.0, macOS arm64.
