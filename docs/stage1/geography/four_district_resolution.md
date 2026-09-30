# Four-district resolution: full coverage using two joint areas

25 September 2026 — geography release v0.2.

Verified: 19 tests pass, 635 data checks pass, and an isolated rebuild reproduced all 21 deterministic artifacts byte for byte.

**All four source districts now enter the population and education panel.** They enter as **Jhang + Toba Tek Singh** and **Kachhi + Nasirabad**, giving 127 common comparison areas and 4,572 observations. Every 2017 and 2023 source unit is included. The original 4,500 panel rows are unchanged.

This resolves their exclusion from the panel. It does **not** establish four separate historical education allocations or explain every conflicting PBS figure. Those distinctions are preserved in the data and evidence log.

## The resulting comparison areas

| Area | 2017 population | 2023 population | Resolution |
|---|---:|---:|---|
| Jhang + Toba Tek Singh | 4,934,128 | 5,589,683 | Combine both districts in both years; preserve the unexplained 893-person retrospective adjustment |
| Kachhi + Nasirabad | 797,779 | 1,005,989 | Combine both districts in both years; documented locality transfers remain inside the joint area |

Both joint areas reconcile exactly between the original 2017 population and the summed retrospective 2017 populations in 2023 Table 1. Their summed **published district areas** also match. These are statistical comparison areas, not certified polygon overlays.

The full release includes 135 source units in 2017 and 136 in 2023. Their population totals are 207,684,626 and 241,499,431 respectively, with no source population excluded. AJK, Gilgit-Baltistan and later administrative creations remain outside this census source coverage.

## Jhang and Toba Tek Singh

The retrospective difference is confined to two tehsils. The other six tehsils' 2017 populations are unchanged in 2023 Table 1.

| Tehsil | Original 2017 | Retrospective 2017 in 2023 Table 1 | Difference |
|---|---:|---:|---:|
| Shorkot, Jhang | 548,567 | 549,460 | +893 |
| Pir Mahal, Toba Tek Singh | 422,246 | 421,353 | −893 |

A fresh comparison of the official locality tables finds **126 Shorkot rural localities and 132 Pir Mahal rural localities** with matching names and acreages in the same district in both years. Available locality codes agree; two 2023 code cells are blank and remain blank in the evidence output. Their numerical population subtotals reconcile to the published rural tehsil totals.

This is additional evidence for using the joint area, beyond two differences merely cancelling. It does **not** show a whole rural village moving from one district to the other, and it cannot distinguish a partial census-block reassignment from a retrospective data revision. The exact cause of the 893-person adjustment remains unverified.

Sources: [2017 Jhang combined tables, Table 23 pages 63–64](https://www.pbs.gov.pk/wp-content/uploads/2020/07/District059_Combined.pdf), [2017 Toba Tek Singh combined tables, Table 23 pages 52–53](https://www.pbs.gov.pk/wp-content/uploads/2020/07/District060_Combined.pdf), and [2023 Punjab Table 31, Shorkot pages 270–274 and Pir Mahal pages 755–759](https://www.pbs.gov.pk/wp-content/uploads/census_tables/tables/table_31_punjab_districts.pdf). The tehsil baselines are in [2023 Punjab Table 1](https://www.pbs.gov.pk/wp-content/uploads/census_tables/tables/table_1_punjab_districts.pdf).

## Kachhi and Nasirabad

The official administrative-change report identifies twelve localities transferred from Bhag, Kachhi into the later Landhi Tehsil, Nasirabad. All twelve can now be traced between the two locality tables by their names and **45,917 acres of matching area**:

Admani Kohna, Gahi, Garhi Karam, Kamal, Khanwah Nisaf Anbari, Kot Sultan, Landhi Khair Pur, Maror Pur, Mat Qabool, Mir Pur Manjhu, Nawara and Sanjrani.

The twelve have a combined 2023 population of 2,576. Nine numerical 2017 population cells sum to 1,763; three source dashes remain missing. The report also gives a 1,763-person adjustment, while Table 1's retrospective district adjustment is 1,932—a difference of 169. The report's adjusted district areas additionally fail conservation by four square kilometres. Those source inconsistencies are retained, and no missing cell or district allocation is guessed.

The documented transfers are internal to the joint Kachhi–Nasirabad area. Summing the two original districts in each year therefore avoids needing an unsupported allocation of education counts between them.

Sources: [2017 Kachhi combined tables, Table 23 page 69](https://www.pbs.gov.pk/wp-content/uploads/2020/07/District116_Combined.pdf), [2023 Balochistan Table 31, pages 125–126](https://www.pbs.gov.pk/wp-content/uploads/census_tables/tables/table_31_balochistan_districts.pdf), and [Balochistan census report, Table 2.3 pages 69–70](https://www.pbs.gov.pk/wp-content/uploads/2020/07/Provincial-Census-Report-2023-Balochistan.pdf). The separate retrospective district baselines are in [2023 Balochistan Table 1](https://www.pbs.gov.pk/wp-content/uploads/census_tables/tables/table_1_balochistan_districts.pdf).

## Individual-district population baselines

`district_population_bridge.csv` preserves the four districts separately, with both versions of the 2017 population and the published 2023 count:

| District | Original 2017 | PBS retrospective 2017 | 2023 |
|---|---:|---:|---:|
| Jhang | 2,742,633 | 2,743,526 | 3,065,639 |
| Toba Tek Singh | 2,191,495 | 2,190,602 | 2,524,044 |
| Kachhi | 309,932 | 308,000 | 442,674 |
| Nasirabad | 487,847 | 489,779 | 563,315 |

This file reports official population baselines as published. It is not a full indicator crosswalk: the adjusted all-ages population totals cannot be used to redistribute sex-specific or educational-attainment counts. Education rates in the main panel still use the summed education tables' own age-5+ denominators.

## Release and reproducibility

Use `data_darbar_warehouse/stage1/geography-2026-09-25-v0.2/` in the Data Darbar workspace. The accompanying `geography-2026-09-25-v0.2-reproduction.tar.gz` contains all locked inputs, code, reference output and instructions for an offline rebuild.

New evidence outputs are:

- `locality_correspondences.csv`: 270 reviewed correspondences, original names, missing-code status, population cells, acreage and PDF locations.
- `subdistrict_source_rows.csv`: original and retrospective administrative summaries for the four districts.
- `district_population_bridge.csv`: the separate population baselines above.
- `retired_comparison_geographies.csv`: the four historical individual comparison IDs and their replacements.

The replacements are new IDs `DDG-0130` and `DDG-0131`; existing identifiers were not reused. All 271 source-unit version IDs and 542 publication aliases remain intact. The previous release and its reproduction archive remain available.

The [verification record](verification-v0.2.json) records the release identity, checks, archive checksum and isolated rebuild. The build checks all 162 locked inputs, 19 regression tests and 635 data checks. Five source-missing cells remain flagged. A fresh rebuild must reproduce all deterministic artifacts byte for byte; execution-specific `run.json` is excluded.

From the workspace root, with the recorded Python 3.11.5, DuckDB 1.5.5 and Poppler 21.11.0 environment:

```sh
python3 -m unittest discover -s datadarbar/etl/geography -p 'test_*.py'
python3 datadarbar/etl/geography/build_geography.py --out data_darbar_warehouse/stage1/geography-v02-repeat
python3 datadarbar/etl/geography/verify_release.py --release data_darbar_warehouse/stage1/geography-2026-09-25-v0.2 --repeat data_darbar_warehouse/stage1/geography-v02-repeat
```

The source census release and website were not changed. Questionnaire-definition equivalence and any join to map polygons still require their own reviews.
