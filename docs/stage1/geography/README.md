# Census geography register and first comparison panel

**Superseded by v0.2:** all four previously withheld source districts are now included through two joint comparison areas. See the [four-district resolution report](four_district_resolution.md) for the evidence, remaining individual-allocation limits and current release. This page documents the initial v0.1 release.

Built 25 September 2026. The register covers **271 published census units**: 135 in 2017 and 136 in 2023. It defines **129 comparison areas**, of which **125 pass the geographic checks** and enter the first population and education panel. Four remain explicitly withheld.

**Verification:** 14 tests pass; a fresh-folder rebuild reproduced all 17 deterministic artifacts byte for byte. There are 620 passing data checks, four expected geography holds, and five retained missing-source flags.

The panel has **4,500 rows**: 125 areas × 2 years × 18 indicators. Five cells remain missing because a source uses a dash; none has been converted to zero. All six frontier-region combinations and the six major district-split groupings are included with evidence.

This is a crosswalk of **published statistical units**. It does not certify polygon boundaries, today's administrative districts, or legal creation dates. The geography dates refer to census snapshots: 2017 at year precision, and the official 1 March 2023 census frame. Education categories are harmonized at the published-label level; questionnaire equivalence and identical enumeration coverage remain provisional. The panel contains no automatic change rankings.

## Files to use

The release is in the Data Darbar workspace, alongside the application repository:

- `data_darbar_warehouse/stage1/geography-2026-09-25-release/panel.csv` — the supported population and education panel; also supplied as Parquet.
- `geography_register.csv` — every source-unit version, reporting area, type, snapshot date and project identifier.
- `comparison_geographies.csv` — approved and withheld groupings, original and retrospective 2017 totals, area checks, evidence and decisions.
- `crosswalk.csv` — each source unit's comparison area and eligibility. Approved count weights are 1; withheld weights are blank.
- `source_unit_aliases.csv` — all 542 population/education publication aliases, including the explicit Tando Allahyar spelling correction.
- `source_table1.csv` — the 271 population/area summaries re-extracted from original Table 1 PDFs, with page, URL and checksum.
- `observation_lineage.csv` — all 5,422 source observations, including withheld records and raw degree-category components.
- `indicator_dictionary.csv` — formulas, aggregation rules, population universes and definition limitations.
- `evidence.csv`, `change_events.csv`, `issues.csv` — official sources, observed changes, conflicting figures and unresolved questions.
- `coverage.csv`, `validation_checks.csv`, `validation_report.json` — coverage and verification results.
- `input_lock.json`, `build_manifest.json` — exact source, code, runtime and output identities.

The adjacent `geography-2026-09-25-reproduction.tar.gz` includes locked inputs, code, the reference release and rebuild instructions. The [verification record](verification.json) identifies the delivered release and archive.

Project identifiers are allocated once in the reviewed register. `DDV` identifies one census publication version; `DDG` identifies one comparison grouping. They are not official district codes. A 2017 value for a combined area is never assigned to a newly split child district.

## What changes between census frames

| Comparison area | 2017 inputs | 2023 inputs |
|---|---|---|
| Bannu, Dera Ismail Khan, Kohat, Lakki Marwat, Peshawar, Tank | Each district plus its separately published frontier region | The corresponding district, including the former frontier-region subdivision |
| Chitral | Chitral | Lower Chitral + Upper Chitral |
| Kohistan | Kohistan | Upper Kohistan + Lower Kohistan + Kolai Palas Kohistan |
| Kalat + Surab | Kalat | Kalat + Surab |
| Killa Abdullah + Chaman | Killa Abdullah | Killa Abdullah + Chaman |
| Loralai + Duki | Loralai | Loralai + Duki |
| Karachi West + Keamari | Karachi West | Karachi West + Keamari |

The seven 2017 agencies retain FATA as their historical reporting area and link to their 2023 district versions in KP. The six 2017 frontier regions also retain FATA in the source register. This avoids rewriting historical geography as if it had always been part of KP.

The main documentary evidence is the [PBS KP report, Table 2.4](https://www.pbs.gov.pk/wp-content/uploads/2020/07/Provincial-Census-Report-2023-KPK.pdf), [Balochistan report, Table 2.3](https://www.pbs.gov.pk/wp-content/uploads/2020/07/Provincial-Census-Report-2023-Balochistan.pdf), and [Sindh report, Table 2.3](https://www.pbs.gov.pk/wp-content/uploads/2020/07/Provincial-Census-Report-2023-Sindh.pdf). Exact PDF page references and local checksums are in `evidence.csv`.

Every supported grouping also passes two numerical checks: its original 2017 population equals the sum of the retrospective 2017 populations printed in 2023 Table 1, and its published areas sum equally in both years. Numerical equality corroborates the reviewed memberships; it does not prove identical polygons.

## Four comparisons withheld

| Area | 2017 original | 2017 retrospective in 2023 Table 1 | Difference | Decision |
|---|---:|---:|---:|---|
| Jhang | 2,742,633 | 2,743,526 | +893 | Cause unresolved; no approved cross-year comparison |
| Toba Tek Singh | 2,191,495 | 2,190,602 | −893 | Cause unresolved; no approved cross-year comparison |
| Kachhi | 309,932 | 308,000 | −1,932 | Locality transfer documented, but allocation figures conflict |
| Nasirabad | 487,847 | 489,779 | +1,932 | Locality transfer documented, but allocation figures conflict |

Jhang and Toba Tek Singh have offsetting discrepancies, but the [Punjab report's administrative-change section](https://www.pbs.gov.pk/wp-content/uploads/2020/07/Provincial-Census-Report-2023-Punjab.pdf) does not establish their cause. They have not been merged merely to make the totals cancel.

For Kachhi/Nasirabad, Balochistan report pages 69–70 document the movement of localities. However, the report adjusts 1,763 people while Table 1 adjusts 1,932, and its two adjusted areas sum to four square kilometres more than their originals. The release preserves these disagreements and supplies no guessed allocation weights.

Additional report inconsistencies are logged for Kohistan and Kalat/Surab. Their whole-unit combinations are supported by the reported memberships and final Table 1 closure; conflicting report component counts are not used. Sindh's specific Keamari change table is used despite a contradictory generic paragraph immediately before it.

## Coverage and missingness

| Census | All source units | Included source units | Included population | Withheld population |
|---|---:|---:|---:|---:|
| 2017 | 135 | 131 | 201,952,719 | 5,731,907 |
| 2023 | 136 | 132 | 234,903,759 | 6,595,672 |

The supported panel covers more than 97% of the source population in each year. It is not a complete national district panel. AJK and Gilgit-Baltistan are outside these census modules, and post-census district creations are outside this frame.

The five missing panel cells are the transgender counts for Bannu, Lakki Marwat, Kharan and Kohlu in 2017, and combined Chitral in 2023. Their total population and other indicators remain available. Missing component counts propagate to the aggregate even where a residual might appear to imply zero.

Education totals and rates use each education table's own age-5+ population. Graduate combines the two 2023 graduate categories; masters-and-above combines masters and MPhil/PhD. Matric-plus adds matric, intermediate, graduate, and masters-and-above. Rates are recomputed after adding counts and denominators. Census 2023 population includes headcount-only records for which detailed education responses may be unavailable, so its population total cannot substitute for the education denominator.

## Reproduce the build

Run from the Data Darbar workspace, one level above `datadarbar`. Requirements are Python 3.11.5, DuckDB 1.5.5 and Poppler `pdftotext` 21.11.0 for the verified byte-identical release. This machine's runtime is `/Users/hibasameen/anaconda3/bin`; ensure that directory comes first on PATH because another Poppler version is also installed.

```sh
python3 -m unittest discover -s datadarbar/etl/geography -p 'test_*.py'
python3 datadarbar/etl/geography/build_geography.py --out data_darbar_warehouse/stage1/geography-repeat
python3 datadarbar/etl/geography/verify_release.py --release data_darbar_warehouse/stage1/geography-2026-09-25-release --repeat data_darbar_warehouse/stage1/geography-repeat
```

The output directory must not exist. The build is offline, verifies all 154 locked inputs, refuses writes into the application/source trees, and publishes its output only after checks pass. A different input, registry, code or runtime receives a different release identity. Only `run.json`, containing execution time and machine paths, is excluded from byte comparison.

The input census release is `census-audit-07f818302704d872`. This geography build consumes its frozen observations; it does not invoke the earlier build's now-historical website baseline check. The earlier census extraction has its own separate reproduction archive. Neither its source files nor the website were changed by this geography step.

The next review is narrowly defined: resolve the four withheld areas against detailed locality tables or PBS revision notices, and complete the questionnaire/indicator-definition comparison before publishing cross-year change measures. Joining this panel to a map additionally requires a documented boundary vintage and a checked geometry crosswalk.
