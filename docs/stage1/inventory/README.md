# Data Darbar source inventory

Generated from the registry and local file availability. Census inputs are checksum-locked; other sources retain explicit blockers.

Coverage: 35 dataset families, 25 public warehouse tables, 40 published data assets, 4169 dependency files.

| Dataset | Period | Build status | Outstanding work |
|---|---|---|---|
| boundaries | Reference dates require asset-specific verification | assets_available_history_unverified | Modified district boundaries and reference-year provenance need registry; file dates are not boundary dates |
| budget | 2009–10 through 2026–27; older archive inventoried | local_raw_and_generators_present_not_rebuilt | Budget estimate/revised/actual columns must remain distinct; script portability untested |
| census2017_education | 2017 | stage1_offline_pdf_build | Cross-year definition and geographic comparability not certified |
| census2017_other | 2017 | inventoried_not_rebuilt | Legacy source root is stale; multiple historical extractors; full extraction lineage not executed |
| census2017_population | 2017 | stage1_offline_pdf_build | Source-native build is separate from unreviewed legacy geographic aggregation |
| census2023_education | 2023 | stage1_offline_pdf_build | Historical Sindh extracts contain errors; source-native corrections are separate from public data; comparability review pending |
| census2023_other | 2023 | inventoried_not_rebuilt | Legacy path portability and upstream extraction require audit |
| census2023_population | 2023 | stage1_offline_pdf_build | Source geography is not yet a certified longitudinal geography |
| climate_events | Source-event dates | untracked_local_extension_outside_baseline | Not part of pinned public baseline or census prototype; retain separate provenance and tests |
| economic_census | 2023 | inventoried_not_rebuilt | Legacy input path and geographic aggregation need audit |
| file_catalog | File-specific | published_metadata_available | Older catalogue is not a complete project source inventory |
| health_access | Motorised 2019; walking and population 2020; release 2026–09 | external_project_dependency | Grid aggregation is upstream in Adaad; geometry rasterisation differs by level |
| hies2024 | 2024–25 | excluded_from_census_core | Do not infer whole-district coverage; review inferred mappings and recall periods |
| industry | Base/year dependent; see source tables | local_raw_and_generators_present_not_rebuilt | Legacy repo script points to obsolete raw root and output location |
| legacy_pbs_payload | Not yet established | legacy_asset_lineage_unresolved | Producer and active consumer not identified; do not treat as authoritative census input |
| lfs2020 | 2020–21 | excluded_from_census_core | District identification, survey design and weighting need audit |
| lfs2024 | 2024–25 | excluded_from_census_core | District identification, survey design and weighting need audit |
| ljcp | Source-report years | untracked_local_extension_outside_baseline | Not part of pinned public baseline or census prototype; retain separate provenance and tests |
| mics | 2016–17 through 2020–21; varies by region | external_project_dependency | Upstream Adaad pipelines and source permissions; mixed survey years |
| mouza2020 | 2020 | tehsil_extract_available | Upstream scraper outside repo; approximate mapping and denominator rules; mouza-level feasibility is separate |
| mpi | 2019–20 | published_payload_input | Complete raw-microdata-to-MPI bundler not identified in this repository |
| national_accounts | 1951–52 through 2025–26 by series | local_raw_and_generators_present_not_rebuilt | Base years, levels, growth and backcasts differ; input-output bundling path needs audit |
| nepra | 2006–07 to 2025 by series | local_warehouse_not_public_catalogue | Raw files are in Adaad; full source manifest and external inputs require separate capture |
| pdhs | 2017–18 | cached_derived_input | Raw DHS directory absent in this workspace; authorised-access source; cached output is not a raw-data rebuild |
| pslm2019 | 2019–20 | inventoried_not_rebuilt | Conditional Stage 1 extension; definitions, weighting and geography require review |
| regional_police | Source-report years | untracked_local_extension_outside_baseline | Not part of pinned public baseline or census prototype; retain separate provenance and tests |
| rwi | 2021 | raw_asset_and_payload_available | Full source-to-poverty-payload generator not identified in repository |
| sbp | Series-specific; selected observations 1947–2026 | checkpointed_local_warehouse | Original API-response directory and credentials are not a public reproducibility package; no API calls made |
| school_access | 2020 grid; 2023 enrolment; release 2026–09 | frozen_derived_tables_available | Dependencies include school_registers, worldpop, boundaries, mouza2020 and separate Census 13(b) extraction |
| school_registers | Release 2026–09; source vintages vary | external_project_dependency | Original regional harvests and geocoding live in Adaad; preparation steps not rerun |
| sindh_fir | Report dates; year-to-date totals | untracked_local_extension_outside_baseline | Not part of pinned public baseline or census prototype; retain separate provenance and tests |
| sindh_police | Source-report years | untracked_local_extension_outside_baseline | Not part of pinned public baseline or census prototype; retain separate provenance and tests |
| trade | Published FY coverage varies; 2015–2025 | local_raw_and_generators_present_not_rebuilt | Commodity totals and partner rows overlap; historical parsers include temporary paths |
| viirs | June 2020–2026 | raw_asset_and_payload_available | Full source-to-poverty-payload generator not identified in repository |
| worldpop | 2020 | raw_asset_and_payload_available | Full source-to-poverty-payload generator not identified in repository |

The detailed CSVs retain source geography, covered population, URLs, inputs, scripts, outputs, reuse conditions, evidence and availability. Both census years build directly from archived official PDFs. Historical CSVs are comparison inputs only. Inventory coverage is complete for the declared scope; unresolved lineage and missing dependencies remain explicit.
