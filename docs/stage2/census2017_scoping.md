# A 2017 layer — what the archive holds

Investigated 26 September 2026 against <https://www.pbs.gov.pk/censusarchive/>.

> **Two corrections.** A first pass concluded that 2017 had tehsil-level data for one table
> only, because the top-level `.xls` files stop at districts and the five "Tehsil Tables"
> PDFs carry just the basic demographic table. Wrong. A second pass found the full table set
> in 135 combined per-district PDFs and concluded that PDF parsing was therefore required.
> Also wrong.
>
> **The whole 2017 corpus is machine-readable: 5,356 Excel files, 40 tables × 134 districts,
> about 225 MB.** The archive page indexes them in an embedded JavaScript object, not in
> `href` links, under `var PREFIX="/wp-content/uploads/2026/08/DCR2017/"` — so a scrape that
> follows anchors finds none of them, which is how both earlier passes went wrong.

## The real source: 5,356 per-district Excel files

| | |
|---|---:|
| Files | **5,356** — 40 tables × 134 districts |
| Base URL | `https://www.pbs.gov.pk/wp-content/uploads/2026/08/DCR2017/` |
| Projected size | **~225 MB** (mean 43 KB, median 32 KB, largest tables 92–144 KB) |
| Reachability | 80 of 80 sampled returned 200 |
| Roster | KP 25, FATA 13, Punjab 36, Sindh 29, Balochistan 31 |

The index lives in the page's `var DATA={...}` object, keyed by province, each entry giving a
district name and its 40 table paths — so the district roster and the table mapping come free.

Paths follow `<Province>Dist(t)/<DISTRICT><date>/Table<NN>d.xls`, with dates varying by
district (`ABBOTTABAD25-06-2018`, `ATTOCK04-06-2018new`). **Sindh uses a different convention
entirely** — `SindhDist/BADIN08-07-2018/Table-23-BAD.xls`, with a district abbreviation
instead of the `d` suffix — so a fetcher has to read the index rather than construct paths.

Every file carries the district broken down by tehsil. `PUNJABDist/ATTOCK04-06-2018new/Table23d.xls`
is 649 rows × 27 columns: district → tehsil → QH → PC → mauza, hadbast numbers in column 2 —
structurally identical to 2023's Table 31.

## The same content also exists as PDF

| | |
|---|---:|
| Files | **135**, all reachable |
| Use | the second rendering, for reconciliation |
| Total size | **364 MB** (mean 2.7 MB, max 4.3 MB) |
| Pages each | ~101, so roughly **13,600 pages** |
| Naming | `District001_Combined.pdf` … `District135_Combined.pdf` |
| Roster | KP 25, FATA 13, Punjab 36, Sindh 29, Balochistan 31, ICT 1 |

`District039_Combined.pdf` is Attock: tables 1 through 40 across its six tehsils — Attock,
Fateh Jang, Hasan Abdal, Hazro, Jand and Pindi Gheb.

Having both renderings matters more than it sounds. The 2023 build's two-rendering
reconciliation recovered 1,120,488 printed dashes the Excel had turned into zeros, and caught
the one Topi Tehsil value where the Excel was simply wrong. **The same apparatus applies
unchanged to 2017**, because the same two renderings exist.

## It includes a village layer, with hadbast numbers

Tables 23 to 26 are the 2017 counterparts of 2023's tables 31 to 34: individual rural
mauzas and urban localities. Table 23 prints the same hierarchy the 2023 file does —
district → tehsil → QH → PC → mauza — with a **hadbast number** against each village.

That is the join a cross-year village panel needs, and it works. For Attock:

| | |
|---|---:|
| 2017 mauzas with a hadbast number | 181 |
| 2023 mauzas with a hadbast number | 182 |
| **Hadbast numbers present in both years** | **180 — 99%** |
| …of those, name also identical | 169 — 94% |

Hadbast numbers restart at 1 in each district, so the key is district + hadbast, not hadbast
alone. The 6% of matched villages whose names differ (`NAMAL(NAMBAL)` in 2017 against
`KHURA KHEL` in 2023 for hadbast 0000004) need checking rather than assuming; a changed name
on a stable number is plausible, but so is a renumbering.

**The constraint is hadbast coverage, and it is province-dependent.** In the 2023 data,
94% of Punjab's rural localities carry a hadbast number, 64% of Balochistan's, 33% of KP's
and **none** of Sindh's. Where the number is absent, linkage falls back to name-within-deh,
which is a much weaker key. A hadbast-keyed village panel is therefore strong in Punjab,
partial in Balochistan and KP, and unavailable in Sindh on this route.

## The tehsil crosswalk is largely automatic

Matching the 469 sub-district units parsed from the five 2017 tehsil PDFs against the 591 in
the 2023 register, on exact normalised name:

| | Units | Share |
|---|---:|---:|
| Exact name match **within the same district** | 444 | 95% |
| Exact match somewhere in 2023 | 457 | 97% |
| No match at all | 12 | 3% |

The residual work runs the other way: **122 of the 2023 units have no 2017 predecessor**,
because they were created since — the Chitral, Kohistan, Kalat/Surab, Killa Abdullah/Chaman,
Loralai/Duki and Karachi West/Keamari splits, plus the new ex-FATA tehsils. Stage 1's rule
applies unchanged: a 2017 value for a combined area is never assigned to a newly split child.

FATA is the other structural difference. 2017 publishes 13 FATA units — 7 agencies and 6
frontier regions — which 2023 has merged into KP districts. Stage 1's district register
already models this and keeps FATA as the historical reporting area rather than rewriting it.

## What else the archive has, and what it does not

| Source | Format | Geography | Verdict |
|---|---|---|---|
| `<Province>Dist/<DISTRICT>/Table<NN>d.xls` | **5,356 `.xls`, ~225 MB** | district **and tehsil**, plus villages in tables 23–26 | **the source to use** |
| `District<NNN>_Combined.pdf` | PDF, 135 files, 364 MB | the same content | the second rendering, for reconciliation |
| `Table<NN>p*.xls` | 200 `.xls` | province; Table 1's also list districts | useful for cross-checking district figures |
| `Table<NN>n.xls` | ~40 `.xls` | national | cross-check only |
| `Table<NN>d.xls` / `.pdf` | ~80 files | **Islamabad only**, despite the `d` | misleading; ignore |
| `*_tehsil.pdf` | 5 PDFs, 30 pages | tehsil, basic demographics only | a cheap subset of the combined reports |
| "District Census Reports" | 102 PDFs | **1998**, one page each | mislabelled; not 2017 |

Already in the workspace: `raw_data/pbs/Census 2017/` holds per-district PDF sets for tables
01, 05, 12, 14, 15 and 16 — about 816 files. Stage 1 built 2017 population and educational
attainment from 270 of them. The combined reports supersede these as a single coherent source.

## Two more defects for the PBS list

**The 102 "District Census Report" PDFs are 1998, not 2017.** They sit under the headings
"Pakistan District Census Reports" and "Pakistan Census 2017 – DCR District Tables", but each
is a one-page summary opening "Population - 1998". Swat, Lahore, Quetta and Faisalabad all
checked; all 1998, all about 9 KB.

**`Table<NN>d` means Islamabad, not district.** Both the `.xls` and `.pdf` under that suffix
contain a single district. A reader taking the suffix at face value gets 1 unit where they
expect 130.

## Scope

| Tier | Source | Gets you |
|---|---|---|
| **2017 district + tehsil, all 40 tables** | 5,356 `.xls` | The cross-year panel. ~95% of tehsils link automatically |
| **2017 village layer** | tables 23–26 in the same files | A cross-year village panel keyed on hadbast — strong in Punjab, absent in Sindh |
| Reconciliation | the 135 combined PDFs | Dash recovery and value cross-checks, exactly as for 2023 |

The extraction is one corpus in the format the pipeline already reads, checked against a
second rendering the pipeline already knows how to use. The genuinely new work is the
**geography register across years** — and the 95% tehsil and 99% hadbast match rates say that
is a review job, not a research project.

## What the two wrong turns say about the method

Both earlier conclusions were confidently wrong, and both failed the same way: they inferred
what exists from what the page links, rather than from what the page *contains*. The
per-district Excel index is in a script block; the combined PDFs are in a JSON array. An
anchor-following scrape sees neither.

The same error is already on record for the 2023 set, where Table 10's KP file exists at its
predictable URL while the page's own link points at Table 1. The rule that follows: **probe
for what should exist, and read the page's scripts, rather than trusting its links.**
