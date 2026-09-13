# Census 2023: split districts carried one successor's schooling figures

Found 13 September 2026 while reconciling the district map's enrolment-by-sex
layer against an independent read of the same PBS tables. Fixed in the commit
that adds this file.

## What was wrong

The boundary file predates five district splits, and the name crosswalk folds
the successors back into their parents:

| boundary district | census districts summed |
|---|---|
| chitral | Lower Chitral, Upper Chitral |
| kohistan | Lower Kohistan, Upper Kohistan, Kolai Palas Kohistan |
| kalat | Kalat, Surab |
| killa abdullah | Killa Abdullah, Chaman |
| loralai | Loralai, Duki |

Two loaders did not sum. `etl/census2023_schooling_by_sex.py` (the
`t12_2023_in_school_5_16_*`, `out_of_school_5_16_*`, `never_school_5_16_*`
and `schooling_gender_gap` fields) pivoted with `aggfunc="first"`, so each of
the five carried whichever successor the source file listed first: **Killa
Abdullah showed Chaman's figures and Loralai showed Duki's**; Chitral was
Lower Chitral only, Kohistan was Kolai Palas only, Kalat omitted Surab. The
Table 13 parser in `etl/build_dataset.py` (`t_edu_2023_*`) kept its
"already read" set on the crosswalked key, so the second and third successor
blocks were skipped: one successor's attainment counts over a population
(`t1_2023_pop_total`) that is the combined district, which is why
`t_edu_2023_total / t1_2023_pop_total` was 0.17–0.53 for these five against
0.84 at the median district.

Every other census loader already accumulated across successors (Table 1,
Table 5, the Table 12 totals and literacy), so `t1_*`, `t5_*`,
`t12_2023_literacy_*`, `t12_2023_out_of_school_5_16` (total) and the
employment block were right; a single record mixed the two geographies.

Corrected in-school rates (girls / boys, %), before → after:
Chitral 77.3 / 82.1 → 80.6 / 84.9; Kohistan 12.6 / 20.4 → 12.0 / 26.3;
Kalat 36.3 / 56.0 → 29.8 / 45.0; Killa Abdullah 20.4 / 43.2 → 17.8 / 38.3;
Loralai 17.3 / 41.0 → 26.4 / 53.6. The same pass restored Malakand, whose
census block is headed "MALAKAND PROTECTED AREA" rather than "… DISTRICT" and
had been dropped by the sex-split loader (girls 75.3, boys 81.1).

## What changed

* `etl/census2023_schooling_by_sex.py`: successors summed before the rate;
  the sex columns coerced to numeric so the sum cannot silently drop a column;
  a district block is headed by a row ending DISTRICT or PROTECTED AREA, and an
  unknown indicator row can no longer swallow the rest of a block.
* `etl/build_dataset.py`: `_parse_edu_hierarchical_format` tracks the raw
  census district and accumulates into the crosswalked key; the percentage
  derivation is factored into `compute_education_pcts()`.
* `etl/rebuild_education_2023.py` (new): re-parses Table 13 alone and merges
  it, with percentages and 2017–2023 diffs, so the fix does not need a full
  build.
* `app/data/districts.json` and `census_data.js`: rebuilt from the raw PBS
  files (`raw_data/pbs/Census 2023/census2023_all_tables/`) with the fixed
  loaders. The `t12_2023_*` sex-split fields change for the five districts and
  Malakand only; the `t_edu_2023_*` counts, percentages and 2017–2023 diffs
  change for the five only. Every other district reproduces its existing
  values to the digit, which is the check that the loaders are otherwise
  untouched. An independent conversion of the same PBS PDFs
  (fahad-mirza/pakistan_census_2023_tables) gives the same Table 12 figures
  for 129 of 130 districts; Orakzai differs by a few children in one cell,
  a PDF-reading difference, and is left as it was.
* `app/data/warehouse/district_indicators.parquet` is not in this commit:
  rebuild it with `etl/build_web_warehouse.py --src <desktop warehouse>`.

Downstream: adaad.org's "How far is the girls' school?" read this layer for
its Figures 4–7 in August; the September rebuild took its enrolment rates from
Table 13(b) directly, aggregated to the same boundaries, and is unaffected.
