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

## Checks

| Check | Result |
|---|---|
| Patwar-circle closure — mauzas sum to their circle | **168,551 of 168,754 (99.9%)** |
| Table 23 ↔ 24 place join — the same villages in both | **44,637 of 45,789 (97.5%)** |
| **Mauza sum vs published rural population** | **118 of 128 districts reconcile** |

## The reconciliation: 118 of 128 districts

A district's mauzas should sum to the rural population its own table 1 publishes.

| Area | Districts reconciling |
|---|---|
| Khyber Pakhtunkhwa | **23 of 23** |
| FATA | **13 of 13** |
| Islamabad | **1 of 1** |
| Punjab | 31 of 35 |
| Balochistan | 26 of 31 |
| Sindh | 24 of 25 |
| **Total** | **118 of 128** |

For comparison, 2023's equivalent reconciles 115 of 129.

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

Ten districts remain unreconciled and are genuine residue — five in Balochistan,
four in Punjab, one in Sindh. Their per-district status is in
`rural_reconciliation_2017.csv`, flagged `hierarchy_unreliable`, so a user can see
which are affected before relying on one.

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
