# Census build findings — 25 September 2026

The census prototype now builds directly from 280 original PBS PDFs: 270 archived 2017 files and ten newly captured 2023 files. Historical 2023 CSVs are comparison inputs only. The build produces 5,422 observations on each year's source geography, with cell provenance and explicit denominators. The public website's data has not been changed.

## Verification

| Check | Result |
|---|---|
| Input identities | 294 locked files, 22,373,270 bytes, plus five baseline assets and the legacy mapping script |
| Coverage | 135 population and 135 education summaries in 2017; 136 of each in 2023 |
| Unique observation keys | Pass; zero duplicates |
| Category totals | 536 complete reconciliations; six source-symbol flags with zero arithmetic residual; zero count discrepancies |
| Missing-value handling | Six printed transgender-count dashes preserved as missing; none replaced by guessed zero |
| Parquet read-back | Exact row/value equality with generated observations and candidates |
| Regression tests | 18 passed, including wrapped rows, locality/sex context, duplicates, truncated rows, missing inputs, changed checksums, output protection and failed-write cleanup |
| Repeat build | All 14 reproducible files agree byte for byte after extracting the 325-file package into a fresh directory; only `run.json` is excluded; see `reproducibility_check.json` |
| Public baseline | All 40 published data assets have the same SHA-256 identity as Git baseline `df174cf0e4c56db1162bb6c942db4c649dbd55c4` |

Arithmetic reconciliation is a consistency check, not an independent certification of every PBS value. The Ghotki 2023 and a 2017 education page were also inspected visually to check column alignment. The parser retains page/line provenance for subsequent review.

## Corrections exposed by the originals

**Eight Sindh education extracts contain 54 incorrect or missing cells.** The affected districts are Ghotki, Hyderabad, Keamari, Khairpur, Mirpur Khas, Naushahro Feroze, Thatta and Umer Kot. Some extracted values shifted into the wrong columns. For Ghotki, the old extract gives never-attended population as 77,551; the original gives **908,928**. The original's 77,551 belongs to the matric column. All 13 original education counts are retained, including the two graduate categories and separate masters and MPhil/PhD counts. See `legacy_extract_comparison.csv` for all affected cells and their precise locations in the [original Sindh Table 13](https://www.pbs.gov.pk/wp-content/uploads/census_tables/tables/table_13_sindh_districts.pdf).

**South Waziristan's 2017 education summary was mis-extracted in the public baseline.** Its published age-5+ total in the original Table 15 is **554,377**, while the current baseline has **50**. The original wraps “5 AND ABOVE” onto a separate line from its values. The new parser reads that first overall/all-sexes row and has a regression test for this layout. Never-attended share is 65.368693%, compared with 86% in the baseline. This correction is included in the separate source-native release.

**Two Chitral population cells lost their printed missing-value symbol in the old extracts.** Lower and Upper Chitral's 2023 transgender counts are printed as dashes and appeared as zero in local CSVs. The build preserves dashes as missing. Four 2017 population summaries also contain transgender-count dashes. All six are listed in `issues.csv`; male plus female counts equal the printed totals, but the build does not infer a missing-value convention from that alone.

Across all selected 2023 raw cells, 2,256 match the old extracts and 56 differ: the 54 Sindh education cells and two Chitral missing-value cells. All source labels matched for this comparison.

## Why a common district panel is still a separate step

The legacy map crosswalk finds a candidate for 540 of the 542 unit/module entries; both unresolved entries are **FR D.I.KHAN**, once for population and once for education. They remain in the source-native data. Adding a spelling alias would resolve a name match but would not establish boundary equivalence, so this release leaves it explicit.

Other frontier-region mappings also need geographic review. For example, adding FR Bannu to Bannu produces a 2017 population of 1,210,183, while the public baseline's population field is 1,167,071. The legacy population extractor skipped some frontier-region labels while education used different mappings. These are different territorial totals, not automatically source errors. Bannu, Kohat, Lakki Marwat, Peshawar and Tank are visible examples in the diagnostic comparison. Dera Ismail Khan also differs because FR D.I.KHAN remains unresolved here.

The diagnostic comparison has 3,906 matching fields, 83 numerical differences, 25 missingness differences and 1,168 candidate fields absent from the baseline. It is a **candidate-to-baseline** comparison of these selected modules, not a full outer join or a test of every public indicator. Differences include source corrections, deliberate missing-value preservation and unresolved territorial aggregation. Rate matches allow 0.005001 percentage points for the baseline's two-decimal rounding.

The [2023 Table 1 footnote](https://www.pbs.gov.pk/wp-content/uploads/census_tables/tables/table_1_islamabad.pdf) distinguishes headcount-only population from the detailed enumeration used from Table 4 onwards. Education percentages therefore use Table 13's own age-5+ denominator. They must not use Table 1 population. Cross-year education definitions and administrative histories remain uncertified.

## Inventory boundaries and next decisions

The source inventory covers all 25 public warehouse tables, all public data assets, the local census originals, wider raw-data families and declared dependencies of local extensions. It records gaps instead of treating cached outputs as completed raw-data rebuilds. The absent DHS directory and four documented NEPRA paths are recorded individually. School, health and MICS pipelines depend partly on the separate Adaad project. Survey weighting, rights/redistribution terms, geography histories and the producer of the legacy PBS payload need source-specific work.

The next deliverable should define geographic entities and valid dates, document splits/mergers and frontier-region treatment, resolve FR D.I.KHAN, and classify each crosswalk as exact, aggregated, partial or unsupported. Only then should the corrected counts be aggregated into a common research panel and considered for a public data update. The current release is a reproducible source-native audit package, not a completed SHRUG-style longitudinal database.
