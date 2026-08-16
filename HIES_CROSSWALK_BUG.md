# Data Darbar bug: every HIES 2024-25 district figure is attributed to the wrong district

Found 16 August 2026 while picking districts for Adaad Issue 2.

## What is wrong

`etl/build_dataset.py` builds its HIES district crosswalk by hand, in
`_HIES_DIVISION_DISTRICTS` and `_build_hies_crosswalk()` (around line 1799).
It assumes the four-digit prefix of `prcode` runs `div*100 + [02, 11, 21, 31, …]`
across the districts of a division in administrative order.

The official coding scheme, which sits on disk at
`PBS data/Microdata/HEIS/Coding-HIES-2024-25.xls`, says something different.

Position IV of the P-code is the **stratum**, not a district serial. `1` means
rural and `2` means urban. So within a division the codes run

| P-code | What it actually is |
|---|---|
| `2702` | **all urban households in Multan division** (Vehari, Multan, Lodhran, Khanewal together) |
| `2711` | rural Vehari |
| `2721` | rural Multan |
| `2731` | rural Lodhran |
| `2741` | rural Khanewal |

Darbar reads the same five codes as Vehari, Multan, Lodhran, Khanewal and
nothing. Two errors follow at once.

1. **An off-by-one on every rural district.** Darbar's `multan` is rural
   Vehari, its `lodhran` is rural Multan, its `khanewal` is rural Lodhran,
   and rural Khanewal is dropped. The same shift runs through every division
   in the country.
2. **The urban block is dumped onto one district.** Every urban household in
   Multan division is filed under Vehari. Every urban household in Lahore
   division is filed under Lahore, which happens to be nearly right by luck,
   but every urban household in Malakand division is filed under Chitral and
   every urban household in Gujranwala division under Gujranwala.

## How big it is

Audit of the 158 P-code prefixes present in `weight.dta`, in
`hies2425_darbar_crosswalk_audit.csv`:

| | codes | households |
|---|---:|---:|
| Correctly assigned | 5 | 818 |
| Rural, assigned to the wrong district | 85 | 16,285 |
| Urban division block collapsed onto one district | 29 | 10,877 |
| Code not in the coding scheme at all | 39 | 2,143 |

About 97 per cent of HIES households are attributed to a district they are
not in.

## What it touches

Everything reached through `_build_hies_crosswalk()`, so every field in
`app/data/districts.json` with a `hies_` prefix. That is `hies_*` welfare,
`hies_ict_*`, `hies_hq_*`, `hies_waste_*` and `hies_wdm_*`. None of these
should be quoted until the crosswalk is rebuilt.

Unaffected: Census 2017 and 2023 fields (`t1_`, `t5_`, `t12_`, `t_edu_`,
`t_emp_`), which are keyed by district name from published tables; PSLM
2019-20 fields and the MPI, which use PSLM's own district codes; and
`lfs21_*`, which uses a text district variable. The separate `lfs25_*` problem
is a different bug and still open.

## The fix

`0004-hies-crosswalk.patch` in this folder. Apply from the `datadarbar` repo:

```bash
cd datadarbar
git am ../0004-hies-crosswalk.patch
python3 etl/build_dataset.py
```

It parses the coding-scheme XLS into division rosters and derives the mapping
from the P-code structure, so there is no hand-written table left to drift.
`hies2425_pcode_district_crosswalk.csv` and
`hies2425_darbar_crosswalk_audit.csv` are the working files behind it.

The consequence is that **every HIES district estimate is now rural-only**, and
carries `hies_coverage: "rural_only"` so nothing downstream can mistake it for a
whole-district figure. Wholly urban districts return nothing at all. Lahore,
the Karachi cores and urban Islamabad now have no HIES fields. 114 districts
come back instead of the previous 147. That is a limit of the survey, not of
the code.

## Balochistan, Gilgit-Baltistan and Azad Kashmir

These are coded at division level in the **rural** stratum too, so the scheme
alone identifies no districts there. The microdata does carry non-zero district
serials for them, and in the four provinces where the scheme prints serials
explicitly the serial is the district's position in the division listing, so the
patch extends that rule and reports the count separately. If you would rather
not carry the inference, 52 strata are flagged.

Rakhshan division (Kharan, Washuk, Chagai, Nushki, created 2017) and Loralai
division (Loralai, Barkhan, Musakhail, Duki, created 2021) post-date the coding
scheme entirely and are left unmapped rather than guessed at. About 730
households, 2.4 per cent of the sample.

Note for Adaad: rural Quetta is 143 households in a district that is 57 per cent
urban, so HIES 2024-25 has nothing usable to say about Quetta.

## Section 6 consumption

Two separate problems, both fixed in the same patch.

The loader read value column `v1` only. Section 6 has four, and the
questionnaire defines all four as consumed: paid, received in kind as wages,
own produced, and received as gift, dowry or assistance. Dropping three of them
removes consumption that is concentrated in rural and farming households. Now
summed.

`hies_mean_monthly_percapita`, `hies_median_monthly_percapita` and
`hies_food_share` are **withheld** rather than recomputed. The five section
totals (item codes 1000, 2000, 4000, 5000, 6000) partition the item rows
exactly — the household-level ratio of their sum to the item sum has median
1.000 — so treating 1000 and 2000 as monthly, multiplying by twelve and adding
5000 both double-counted and dropped 4000 and 6000. The per-capita figure was
built from 6.6 per cent of recorded consumption, and item code 1000 is 2.2 per
cent of the total rather than food.

Rebuilding it needs one more step. Section 6 mixes recall periods (Part C is
"last 1 month", Parts D and E are "last 1 year") and which section total covers
which part is not established. Until it is, the raw block totals ship as fields
and the derived levels stay empty. The TODO in the code says what to check.

## Correction

An earlier version of this note said `hies_food_share` and
`hies_food_insecurity_pct` carried identical values, which looked like an
assignment slip. They are identical in 2 of 113 districts. That was a
coincidence in the district I happened to check, not a bug.
