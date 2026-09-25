# Website census corrections — 25 September 2026

The source audit identified shifted or missing education cells in eight Sindh districts, a wrong 2017 South Waziristan summary, and source dashes converted to zero in Upper/Lower Chitral. This patch applies the verified corrections to the existing website geography and updates dependent percentages and changes.

## Scope and evidence

- **2023 education:** Ghotki, Hyderabad, Keamari, Khairpur, Mirpur Khas, Naushahro Feroze, Thatta and Umer Kot. The 54 faulty/missing source cells are replaced using PBS Table 13. Ghotki's never-attended count is 908,928; the previous 77,551 is its matric count.
- **2017 education:** South Waziristan's age-5+ total is 554,377, replacing 50. The original Table 15 wraps the age label above its values; the old extract selected a later sex-specific row. All education counts and dependent rates/changes are recalculated.
- **2023 population:** the Chitral map unit's transgender count becomes null because both source districts print dashes. The false −22 change is removed. Kharan and Kohlu's 2017 dashes were already null. Frontier-region aggregations are outside this correction because their territorial coverage remains under review.
- **Shared-area education comparison:** Keamari and Karachi West retain the website's existing boundary labels and 2017 shared-area treatment. Their education changes now use the sum of 2023 education counts and age-5+ denominators, compared with the original 2017 education denominator. All-age population weights are not appropriate for these education rates. Missing/stale shared change fields are replaced consistently on both districts.

The result changes 264 fields across 11 map districts, including Karachi West's dependent comparison and Chitral's missing count. No unrelated indicators or district mappings change. The 147 map units and geometry remain unchanged.

`etl/census_corrections/2026-09-25.json` contains the complete reviewed source rows, PDF locations, URLs and SHA-256 identities. It was extracted from source-native release `census-audit-07f818302704d872`, with the source-observation checksum retained. The earlier Stage 1 reports and reproduction archive describe the pre-correction public baseline and remain historical snapshots.

`docs/census-corrections-2026-09-25-changes.json` records every changed website field and its before/after value. The website's methodology note discloses the correction. The source-native audit is separate from the website's existing geographic comparisons; this patch does not certify the whole map as a longitudinal research panel.

## Rebuild and safeguards

From the application repository, with Python, DuckDB and PyArrow available:

```sh
python3 etl/census_corrections.py --write
python3 etl/inline_districts.py
python3 etl/build_web_warehouse.py --only district_indicators
python3 scripts/build_seo.py
python3 scripts/check_seo.py
python3 -m unittest discover -s tests -p 'test_census_corrections.py'
```

`census_corrections.py` without `--write` is a dry run; applying it twice makes no additional changes. It validates the complete source-category sums and rejects absent destination rows before mutating data. Both the full legacy builder and the education-only rebuild reapply the reviewed corrections, so older local extracts cannot silently restore these errors.

The district-only warehouse option regenerates the long indicator panel and updates its catalogue entry/cache identifier. The other 24 warehouse tables are retained. Map data is re-inlined into `census_data.js`, which the website reads in preference to JSON. The map's data URL receives a new cache version. Browser CSV exports use the corrected in-memory panel; SQL queries and Parquet downloads use the rebuilt table.

Regression checks cover the shifted Ghotki row, South Waziristan's overall summary, missing Chitral values, recalculated differences, education denominators for the shared Karachi comparison, repeat application, unchanged unrelated fields and failure before mutation.

Validation passed: six correction tests, all seven district-provenance notice cases, inline/JSON equality, and all 45,829 non-null scalar warehouse rows matching JSON. All 24 unrelated Parquet tables and their catalogue entries remain unchanged. Search-page validation passed for 55 pages and 44 dataset downloads. Re-running the education-only legacy parser followed by the correction step produces zero further field changes.

Browser checks confirmed Ghotki's 908,928 never-attended count and 61.4% displayed share, and South Waziristan's 554,377 age-5+ total and 65.4% never-attended share in 2017. During this check, search-opened tooltips were found to retain a previous year/dataset value; they now close on selection changes so the next hover agrees with the current sidebar and legend. The existing provenance-test harness was updated to load the app's current geography helpers.
