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
| **Mauza sum vs published rural population** | **60 of 128 districts reconcile** |

The first two say the hierarchy and the place identifier are working. The third is
the decisive one, and it does not yet pass.

## The known limitation: the hierarchy over-counts in 68 districts

A district's mauzas should sum to the rural population its own table 1 publishes.
In 68 of 128 they do not, and **every discrepancy runs one way — over, never
under**:

| | Districts |
|---|---:|
| Within 1% | 60 |
| Over by 1–20% | 62 |
| Over by more than 20% | 6 |

Over-counting in one direction means subtotal rows are being read as places. This
is the same defect 2023 had and solved by naming the levels PBS leaves unsuffixed
— there, adding TRIBE, SECTION and UC took the reconciliation from 96 districts to
115 of 129. For 2017 that work is not done: 38 unnamed groupings are detected and
relabelled, which is evidently not all of them.

| Area | Districts reconciling |
|---|---|
| Sindh | 23 of 25 |
| Khyber Pakhtunkhwa | 15 of 23 |
| Balochistan | 9 of 31 |
| FATA | 6 of 13 |
| Punjab | **6 of 35** |
| Islamabad | 1 of 1 |

Punjab is the worst and its excesses are modest — Okara 8.98%, Narowal 6.04%,
Multan 6.02% — which points at a small number of subtotal rows per district rather
than a whole missing tier. Okara's mauza rows nearly all carry hadbast numbers
(900 of 902), so they are genuine places; whatever is being double-counted is
elsewhere in its hierarchy.

**Until this is resolved, the mauza level must not be summed to a district total.**
The per-district status is in `rural_reconciliation_2017.csv` so a user can see
which districts are affected before relying on one.

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

## Reproduce

```bash
python3 datadarbar/etl/census2017/build_localities_2017.py --dir raw_data/pbs/census2017_sources/2026-09-26 --out <dir> --unit-panel <panel>/panel_2017.parquet
```
