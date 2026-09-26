# Census 2017 — the locality tier

Built 26 September 2026 into
`data_darbar_warehouse/census2017/draft-2026-09-26/localities/`. **Nothing is
published.** This is a separate release from the unit panel, as 2023's is: a row
here is a *place* — a mauza, deh or urban locality — not a published
administrative unit.

Tables 23–26, which are 2023's tables 31–34 under different numbers.

| | |
|---|---:|
| Observations | **2,505,703** |
| Places | **71,177** |
| **Mauzas and dehs** | **46,692** |
| Places carrying a hadbast or deh number | 46,645 |
| Missing cells | 6,609 |

| Level | Places | | Level | Places |
|---|---:|---|---|---:|
| mauza / deh | **46,692** | | supervisory tapedar circle | 274 |
| patwar circle | 9,412 | | union council | 175 |
| circle (urban) | 8,308 | | district | 273 |
| charge (urban) | 1,585 | | section | 312 |
| tapedar circle | 1,453 | | tribe | 60 |
| qanungo halqa | 1,104 | | unnamed grouping | 38 |
| sub-district | 920 | | locality (named urban) | 571 |

## The reader is 2023's, with one parameter added

`etl/stage2/read_localities.py` worked on these tables essentially unchanged. It
had decided which tables were urban from their 2023 numbers, so that became a
parameter; everything else carried over, including the revenue hierarchies that
differ by area — QH and PC in Punjab, KP and Balochistan; STC and TC in Sindh; and
the TRIBE and SECTION levels that KP's ex-FATA districts nest between the tehsil
and the village.

What differs is the shape of the input. 2023 publishes one workbook per table per
region, 20 in all; 2017 publishes one per table per district, about 540.

> The 2017 draft carries no release manifest or input lock yet — `build_all_2017.py`
> and the release manifest are still open in workstream D, so this directory's identity
> rests on the code fingerprint in git rather than on a hash of its own artifacts. The
> 2023 locality release does carry both.

## Checks

| Check | Result |
|---|---|
| Patwar-circle closure — mauzas sum to their circle | **167,776 of 167,899 (99.9%)** |
| Table 23 ↔ 24 place join — the same villages in both | **44,618 of 45,769 (97.5%)** |
| **Internal closure** — villages sum to the same table's district row | **123 of 129 exactly, 129 of 129 within 1%** |
| **Mauza sum vs published rural population** | **129 of 129 districts reconcile** |
| Districts flagged `hierarchy_unreliable` | **none** |

## The reconciliation: 129 of 129 districts

A district's mauzas should sum to the rural population its own table 1 publishes.

| Area | Districts reconciling |
|---|---|
| Khyber Pakhtunkhwa | **24 of 24** |
| FATA | **13 of 13** |
| Islamabad | **1 of 1** |
| Punjab | **35 of 35** |
| Balochistan | **31 of 31** |
| Sindh | **25 of 25** |
| **Total** | **129 of 129** |

123 of the 129 reconcile *exactly*, to the person. The other six are **under** by
between 0.01% and 0.89% — Sahiwal −12,374, South Waziristan −5,987, Bahawalnagar
−1,484, Khuzdar −1,398, Gujrat −782, Pakpattan −199. These are villages PBS did not
print, not a hierarchy we misread. Sahiwal is the clean case and accounts for its
whole shortfall in one row: `JINNAH TOWN PC` reports 12,374 with **no village rows
beneath it at all**. Nothing in our reading can recover a village the source omits,
so these stay inside the 1% tolerance and are listed per district.

### Two checks, because they answer different questions

`internal_closure_2017.csv` compares a district's villages to the district row of
the **same table**. That is the check on our reading: it asks only whether the rows
inside one table add up, so a failure cannot be blamed on another table. In 2017 it
gives the same verdict as the table 1 comparison, because table 23's district row
agrees with table 1's rural population for all 129 districts, exactly.

That agreement is a property of the 2017 release and not something to rely on. The
2023 equivalents differ for 82 of 130 districts, by 346,346 people, so there the two
checks genuinely diverge and only the internal one measures extraction quality. Both
are now reported for both years, and a district that satisfies the internal check but
not table 1 is labelled `source_tables_disagree` rather than `hierarchy_unreliable` —
2017 has no districts in either category.

The district count is 129, not 128, because Malakand now joins — see below. Five of
the 135 districts in the panel are wholly urban and have no rural table to compare
against; Kohistan's tables 23 and 24 are absent from the spreadsheet release, which
`KNOWN_ABSENT` records.

For comparison, 2023's equivalent reconciles 124 of 130, with the other six being
cases where PBS's own two tables disagree rather than failures of extraction.

### A correction: the first version of this check was wrong

This page previously reported **60 of 128** and attributed the gap to subtotal
rows being read as places — the defect 2023 solved by naming the levels PBS
leaves unsuffixed. **That diagnosis was wrong.**

The check summed every indicator matching `%ALL SEXES%`, and table 23 has two:
`POPULATION CHARACTERISTICS / POPULATION / ALL SEXES` and
`POPULATION CHARACTERISTICS / LITERACY % (10+ YEARS) / ALL SEXES`. It was adding a
literacy **percentage** to a population and reporting the total as an over-count.
Okara appeared 8.98% over when the true figure is 7.1%, and the mauza count read
1,810 where there are 905 places each carrying two indicators.

This is the same mistake as 2023's rate detector missing `PERCENT`, which produced
750 false failures in table 21. A check that sums across indicators has to exclude
rates explicitly; matching on a sex label does not do that.

### The ten that did not: unsuffixed groupings, in three shapes

Ten districts were **over, never under** — the signature of double-counting rather
than missing data. In each, rows that are really intermediate groupings had been
read as villages, so they were summed alongside the very rows they are the total
of. Twenty-one rows are responsible, and their sum is the excess *exactly* in all
ten districts:

| District | Excess | Rows responsible |
|---|---:|---|
| Panjgur | 218,296 | `PANJGUR`, `GOWARGO`, `PAROME` |
| Okara | 155,259 | `OKARA CANTONMENT`, `MANDI AHMEDABAD(HERA SINGH)QH` |
| Sherani | 152,952 | `SHERANI` |
| Multan | 138,578 | `MULTAN CANTONMENT` |
| Washuk | 113,611 | `WASHUK`, `MASHKHEL`, `NAG`, `SHAHOO GARHI` |
| Kharan | 82,063 | `SAR KHARAN`, `TOHMULK` |
| Jhang | 79,010 | `SHORKOT CANTONMENT` |
| Gwadar | 58,380 | `GWADAR`, `SUNTSER` |
| Karachi West | 56,407 | `MANGOPIR TC II` |
| Bahawalpur | 25,730 | `ABLANI-QH` |

Three shapes:

- **A bare restatement of the unit above.** Balochistan prints the sub-division
  again at village depth before listing its union councils: `PANJGUR TEHSIL`
  178,752, then a bare `PANJGUR` 178,752, then the UCs that add to it. Sherani is
  the pure case — district, sub-division and bare row all 152,952, hence exactly
  100% over.
- **Cantonments.** Okara, Multan and Shorkot — whole urban administrative areas
  printed inside the rural table's tree with no suffix.
- **A suffix at the wrong depth.** `ABLANI-QH` and `MANDI AHMEDABAD(HERA SINGH)QH`
  are qanungo halqas printed *inside* a patwar circle; `MANGOPIR TC II` is a town
  committee inside a sub-tehsil council. The suffix is there, but not where the
  reader looks for it.

Two further tiers were being missed by name, and both are fixed in the vocabulary
rather than by arithmetic:

- **The tier word is not always last on the line.** Bajaur prints its sections as
  `MAMUND SECTION-I (PART)` and Khyber as `SECTION NO 1`; Karachi West numbers its
  tapedar circles `MANGOPIR TC II`. The suffix pattern was anchored at end-of-line,
  so none matched. 2017 got away with it — its hadbast numbers let the arithmetic
  rule catch the same 45 rows — but in 2023 the same rows are Bajaur's entire
  595,702 over-count and Khyber's entire 26,429.
- **Gwadar nests a revenue circle** between the union council and the village, alone
  in the country: `NILNAT UC` 16,172 contains `CHAKLI CIRCLE`, `KAPUR CIRCLE` and
  `NILNAT CIRCLE`, which add to exactly that, each above its own villages. Read as
  villages they double the union council. The level is named `revenue_circle`, kept
  distinct from the urban tables' `CIRCLE NO nn`, which is a census operational unit.

`relabel_unsuffixed_groupings` recognised two shapes and missed all three, because
both its tests walked downward looking for **numbered** villages. Here the children
are either groupings themselves (union councils under `SHERANI`, patwar circles
under `MULTAN CANTONMENT`) or villages whose hadbast number PBS left blank —
`MANGOPIR TC II` is exactly the sum of five dehs, two of which carry no number, and
the old run stopped at the first of them. It now tries the numbered run first, falls
back to the unnumbered one, and finally tries a run of groupings one tier up.

**A missing hadbast number is not the discriminator.** 23 districts contain
hadbast-less rows at village level and only ten failed; in the other thirteen those
rows are ordinary villages whose number was simply not printed — `PIPRI`, `MIANO`,
`301/1-L`. Karachi West contains both kinds: of its three hadbast-less rows only
`MANGOPIR TC II` is a grouping, and `HUB` and `MAIGARHI` are real dehs. Relabelling
on the missing number alone would have corrupted thirteen districts that already
reconciled. The arithmetic is what authorises the change; the name is only a hint.

The order of the tests matters as much as the tests. Reading the unnumbered run
*first* overshoots the subtotal in districts where villages are numbered, which
silently undid relabels that already worked — it cost Bajaur, Khyber, Rahim Yar
Khan and Bahawalnagar their reconciliation before the numbered run was given
priority.

### Malakand was missing entirely

Tables 5–23 print Malakand as `MALAKAND PROTECTED AREA`, a former Provincially
Administered Tribal Area whose name carries none of the words that mark a unit. The
locality reader never called the corpus-wide alias table, so the district's own row
read as a village and all 650,120 of its rural residents were orphaned with a null
district — silently dropped from the reconciliation by its inner join, which is why
this check reported 128 districts rather than 129.

The alias `MALAKAND PROTECTED AREA → MALAKAND DISTRICT` already existed, reviewed,
in `unit_aliases.py`; the unit reader applied it and the locality reader did not.
The same gap did visible damage in 2023, where it attached Malakand's tehsils to
Lower Kohistan and made it over-count by 445%.

Per-district status is in `rural_reconciliation_2017.csv`. Every row carries
`relabelled` and `relabel_rule`, naming which test reclassified it.

## What the 42 apparent failures turned out to be

The first run reported 42 problem files. **40 are empty for a reason the census
itself gives**: a district with no urban population has no urban localities to
list, and a wholly urban one has no rural localities. PBS sometimes writes the
reason into the sheet — Lahore's table 23 contains the single cell
`LAHORE IS URBANIZED.`

These are now read off the sheet rather than matched against a hard-coded list,
which would go stale and would not distinguish an explicable absence from a
parsing failure. Two genuine problems remain: Dera Bugti's table 24 and Killa
Saifullah's table 26 have no header block but do hold data, so their columns need
recovering as the unit tables' did.

## Table 2: named urban localities

Built alongside, into the same directory. A row is a named town, cantonment or
municipal committee with its parent tehsil — a place, not an administrative unit.

| | |
|---|---:|
| Named urban localities | **589** |
| With a parent tehsil | 588 |
| Districts covered | 112 |
| Empty workbooks (no urban localities to list) | 17 |
| Problems | **0** |

The row walk is 2023's `build_urban_localities.read_sheet` rather than a copy of
it: the sheet has the same shape in both years, and a duplicated walk would drift
as a duplicated vocabulary already did elsewhere in this pipeline.

**220 of the 589 arrived with no district**, because table 2 sometimes omits its
district row — the same defect the unit tables have. The name now comes from that
district's own table 1, reached through the directory the capture recorded rather
than from the filename, and districts covered went from 65 to 112. One locality
remains unattributed: Malakand's BATKHELA MC.

### Table 2 does not sum to the urban population, and should not be expected to

Four areas' localities sum **exactly** to their published urban population —
Balochistan, FATA, Islamabad and Khyber Pakhtunkhwa. Punjab is 2.08% short, which
is precisely Okara's urban population of 842,564, whose table 2 has no header
block. Sindh is 56% short.

Sindh is not a defect. Karachi East lists three named localities against an urban
population of 2,875,315, because table 2 lists *named* localities and Karachi's
population is not organised into them — the district is itself the urban area.
2023's build report says the same of its table 2: its rows "do not sum to
anything". The four exact matches are the surprise here, not the two shortfalls.

## Reproduce

```bash
python3 datadarbar/etl/census2017/build_localities_2017.py --dir raw_data/pbs/census2017_sources/2026-09-26 --out <dir> --unit-panel <panel>/panel_2017.parquet
```

```bash
python3 datadarbar/etl/census2017/build_urban_localities_2017.py --dir raw_data/pbs/census2017_sources/2026-09-26 --out <dir>
```
