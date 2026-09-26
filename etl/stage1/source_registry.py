"""Reviewed dataset-level inventory; availability is measured by inventory.py.

Paths are workspace-relative. This is lineage evidence, not a claim that every
historical script has been executed successfully. Untracked local work is separate.
"""
from __future__ import annotations

SOURCES = []


def source(id, period, grain, universe, source_url, inputs, pipeline, outputs,
           status, blocker, evidence, licence="Original source terms require source-specific review; no blanket redistribution assumption"):
    SOURCES.append(dict(dataset_id=id, observation_period=period, source_geography=grain,
                        population_covered=universe, source_url=source_url, inputs=inputs,
                        pipeline=pipeline, outputs=outputs, reproducibility_status=status,
                        blocker=blocker, evidence=evidence, reuse_conditions=licence))


PANEL = ["datadarbar/app/data/districts.json", "datadarbar/app/data/census_data.js",
         "datadarbar/app/data/warehouse/district_indicators.parquet"]
POV = "datadarbar/app/data/poverty_data.js"
WH = "datadarbar/app/data/warehouse/"
BUILD = ["datadarbar/etl/build_dataset.py", "datadarbar/etl/build_web_warehouse.py"]
BASE = "raw_data/pbs/"
OFFICIAL23 = BASE+"stage1_sources/2026-09-25-official"
source("census2017_population", "2017", "Published districts/agencies/frontier regions", "All ages; all sexes", "https://www.pbs.gov.pk/censusarchive/",
       [BASE+"Census 2017/pbs_2017_table01", BASE+"Census 2017/final tables/table1_combined_2017.csv"],
       ["datadarbar/etl/stage1/build_census.py"]+BUILD, PANEL, "stage1_offline_pdf_build",
       "Source-native build is separate from unreviewed legacy geographic aggregation", "2017 table01 manifest, PDFs and build_dataset.py:load_table1")
source("census2017_education", "2017", "Published districts/agencies/frontier regions", "Population age 5+; all sexes", "https://www.pbs.gov.pk/censusarchive/",
       [BASE+"Census 2017/pbs_2017_table15", BASE+"Census 2017/final tables/table15_combined_2017.csv"],
       ["datadarbar/etl/stage1/build_census.py"]+BUILD, PANEL, "stage1_offline_pdf_build",
       "Cross-year definition and geographic comparability not certified", "2017 table15 manifest and PDFs; original extraction candidates in Census 2017 directory")
source("census2017_other", "2017", "Published districts/agencies/frontier regions", "Table-specific: age, literacy, residence and employment universes", "https://www.pbs.gov.pk/censusarchive/",
       [BASE+"Census 2017"], BUILD, PANEL, "inventoried_not_rebuilt",
       "Legacy source root is stale; multiple historical extractors; full extraction lineage not executed", "build_dataset.py and archived parsers/manifests; tables 5/12/14/16")
source("census2023_population", "2023", "Source districts; provinces and ICT", "All ages, including headcount-only population", "https://www.pbs.gov.pk/census/",
       [OFFICIAL23, BASE+"Census 2023/census2023_all_tables/table_1", BASE+"Census 2023/census2023_all_tables/xlsx", BASE+"Census 2023/final tables/table1_combined_2023.csv"],
       ["datadarbar/etl/stage1/build_census.py", BASE+"Census 2023/parse and clean 2023 tables.py"]+BUILD, PANEL,
       "stage1_offline_pdf_build", "Source geography is not yet a certified longitudinal geography", "Official PDFs and retrieval manifest; Table 1 headcount footnote; historical extracts compared")
source("census2023_education", "2023", "Source districts; provinces and ICT", "Age 5+; detailed enumeration; all sexes", "https://www.pbs.gov.pk/census/",
       [OFFICIAL23, BASE+"Census 2023/census2023_all_tables/table_13", BASE+"Census 2023/census2023_all_tables/xlsx", BASE+"Census 2023/final tables/table13_combined_2023.csv"],
       ["datadarbar/etl/stage1/build_census.py", "datadarbar/etl/rebuild_education_2023.py"]+BUILD, PANEL,
       "stage1_offline_pdf_build", "Historical Sindh extracts contain errors; source-native corrections are separate from public data; comparability review pending", "Official PDFs and retrieval manifest; legacy_extract_comparison.csv; CENSUS_2023_SPLIT_DISTRICTS.md")
source("census2023_other", "2023", "District/tehsil as published; current-map aggregation", "Table-specific; school attendance age 5–16 and literacy age 10+", "https://www.pbs.gov.pk/census/",
       [BASE+"Census 2023"], BUILD+["datadarbar/etl/census2023_schooling_by_sex.py", "datadarbar/etl/inline_districts.py"], PANEL,
       "inventoried_not_rebuilt", "Legacy path portability and upstream extraction require audit", "Census 2023 local tables, parser source, correction note")
source("pslm2019", "2019–20", "Source survey districts", "Household/person/age-specific denominators", "https://www.pbs.gov.pk/pslm-3/",
       [BASE+"Microdata/PSLM 2019-20", "datadarbar/etl/pslm2019_district_fies.csv"], BUILD+["datadarbar/etl/pslm_sex_education.py"], PANEL,
       "inventoried_not_rebuilt", "Conditional Stage 1 extension; definitions, weighting and geography require review", "README methodology; build_dataset.py PSLM loaders")
source("hies2024", "2024–25", "Rural district strata; urban pooled divisions", "Published district outputs are rural-only; some inferred codes; consumption measures withheld", "https://www.pbs.gov.pk/hies/",
       [BASE+"Microdata/HEIS", "raw_data/crosswalks/hies2425_pcode_district_crosswalk.csv", "datadarbar/etl/hies2425_pcode_district_crosswalk.csv"], BUILD, PANEL,
       "excluded_from_census_core", "Do not infer whole-district coverage; review inferred mappings and recall periods", "HIES_CROSSWALK_BUG.md; HIES_DISTRICT_COVERAGE.md")
for id, period, pattern in [("lfs2020", "2020–21", "LFS2020-21*"), ("lfs2024", "2024–25", "*2024-25*")]:
    source(id, period, "Source survey units mapped to districts", "Age/sex-specific labour force; district estimates indicative", "https://www.pbs.gov.pk/",
           [BASE+"Microdata/LFS/"+pattern], BUILD, PANEL, "excluded_from_census_core", "District identification, survey design and weighting need audit", "build_dataset.py LFS loaders; README methodology")
source("economic_census", "2023", "District", "Establishments and workers by activity/type", "https://www.pbs.gov.pk/",
       [BASE+"Economic Census"], BUILD, PANEL, "inventoried_not_rebuilt", "Legacy input path and geographic aggregation need audit", "build_dataset.py:load_economic_census")
source("pdhs", "2017–18", "Survey districts; reporting domains differ", "Women/children; denominator varies by measure; district cuts indicative", "https://www.nips.org.pk/viewpublicdata",
       [BASE+"Microdata/DHS 2017-18", "datadarbar/etl/dhs_district_indicators.json"], ["datadarbar/etl/dhs_district.py"]+BUILD, PANEL,
       "cached_derived_input", "Raw DHS directory absent in this workspace; authorised-access source; cached output is not a raw-data rebuild", "dhs_district.py and README", "DHS microdata access/redistribution restrictions; derived releases require appropriate terms")
source("mics", "2016–17 through 2020–21; varies by region", "Survey districts", "Module-specific women/children/households; AJK published tables", "https://mics.unicef.org/",
       ["datadarbar/etl/mics_district_indicators.json", "../Adaad/data/pk-mics-districts"], ["datadarbar/etl/mics_district.py"]+BUILD, PANEL,
       "external_project_dependency", "Upstream Adaad pipelines and source permissions; mixed survey years", "mics_district.py source notes")
source("mpi", "2019–20", "Survey district", "Population-weighted deprivation measures; seven-indicator custom MPI", "https://www.pbs.gov.pk/pslm-3/",
       [BASE+"Microdata/PSLM 2019-20", POV], ["datadarbar/etl/build_web_warehouse.py"], [POV, WH+"mpi_districts.parquet"],
       "published_payload_input", "Complete raw-microdata-to-MPI bundler not identified in this repository", "README poverty methodology and WAREHOUSE.md")
source("mouza2020", "2020", "PBS tehsil counts of mouzas; mapping to ADM3", "Revenue villages; counts are not persons or households", "https://mc2020.pbos.gov.pk/",
       ["datadarbar/etl/mouza2020"], ["datadarbar/etl/mouza2020/build_crosswalk.py", "datadarbar/etl/mouza2020/build_payload.py", "datadarbar/etl/build_web_warehouse.py"],
       ["datadarbar/app/data/mouza_data.js", WH+"mouza_crosswalk.parquet", WH+"mouza_tehsil.parquet"],
       "tehsil_extract_available", "Upstream scraper outside repo; approximate mapping and denominator rules; mouza-level feasibility is separate", "etl/mouza2020/README.md and manual_map.py")
source("boundaries", "Reference dates require asset-specific verification", "District/tehsil polygons; source units differ", "Geometry, not population", "https://www.geoboundaries.org/",
       ["raw_data/geospatial/boundaries", "datadarbar/app/data/pakistan_districts_province_boundries.geojson", "datadarbar/app/data/tehsils_geo.js"],
       ["raw_data/geospatial/boundaries/Mapping Pakistan Districts.ipynb"], ["datadarbar/app/data/pakistan_districts_province_boundries.geojson", "datadarbar/app/data/tehsils_geo.js"],
       "assets_available_history_unverified", "Modified district boundaries and reference-year provenance need registry; file dates are not boundary dates", "workspace boundary attribution files; existing map and crosswalk notes")
for id, period, root, url, universe in [
    ("rwi", "2021", "raw_data/geospatial/ind_pak_relative_wealth_index.csv", "https://dataforgood.facebook.com/", "Modelled relative wealth; nightlights are an input"),
    ("worldpop", "2020", "raw_data/geospatial/pak_ppp_2020_1km_Aggregated_UNadj.tif", "https://www.worldpop.org/", "UN-adjusted modelled gridded population"),
    ("viirs", "June 2020–2026", "raw_data/geospatial/viirs_pak_clips", "https://eogdata.mines.edu/products/vnl/", "Monthly June radiance, not an annual mean")]:
    source(id, period, "Grid/points aggregated to tehsil polygons", universe, url, [root], ["datadarbar/etl/build_web_warehouse.py"],
           [POV, WH+"tehsil_satellite.parquet"]+([WH+"tehsil_nightlights.parquet"] if id=="viirs" else []),
           "raw_asset_and_payload_available", "Full source-to-poverty-payload generator not identified in repository", "README and warehouse catalogue; VIIRS clip README")
source("school_registers", "Release 2026–09; source vintages vary", "School points/register rows", "Government-school networks with region and positional-quality exclusions", "https://adaad.org/datasets/",
       ["datadarbar/etl/schools", "../Adaad/data/pk-school-access"], ["datadarbar/etl/schools/build_schools_pk.py", "datadarbar/etl/schools/prepare_release.py", "datadarbar/etl/build_web_warehouse.py"],
       [WH+"schools_pk.parquet", WH+"school_layer_coverage.parquet"], "external_project_dependency", "Original regional harvests and geocoding live in Adaad; preparation steps not rerun", "etl/schools/README.md")
school_tables = ["school_access_district", "school_access_tehsil", "school_distance_stats", "school_validation_district", "school_validation_tehsil", "school_validation_summary", "census_enrolment_5_16_by_sex"]
source("school_access", "2020 grid; 2023 enrolment; release 2026–09", "District/tehsil/region", "Covered populations; coverage thresholds; census age 5–16; mouza counts for validation", "https://adaad.org/datasets/",
       ["datadarbar/etl/schools", "../Adaad/data/pk-school-access"], ["datadarbar/etl/schools/build_tehsil_access.py", "datadarbar/etl/schools/validate_released_layer.py", "datadarbar/etl/schools/build_map_payload.py", "datadarbar/etl/build_web_warehouse.py"],
       [POV]+[WH+t+".parquet" for t in school_tables], "frozen_derived_tables_available", "Dependencies include school_registers, worldpop, boundaries, mouza2020 and separate Census 13(b) extraction", "etl/schools/README.md; catalogue source metadata")
source("health_access", "Motorised 2019; walking and population 2020; release 2026–09", "Grid aggregated to district/tehsil", "Modelled access to mapped facilities, not service quality", "https://malariaatlas.org/",
       ["raw_data/geospatial/map_atlas", "datadarbar/etl/health_access", "../Adaad/data/pk-health-access"], ["../Adaad/data/pk-health-access/build.py", "../Adaad/data/pk-health-access/build_tehsils.py", "datadarbar/etl/health_access/build_map_payload.py", "datadarbar/etl/build_web_warehouse.py"],
       [POV, WH+"health_access_district.parquet", WH+"health_access_tehsil.parquet"], "external_project_dependency", "Grid aggregation is upstream in Adaad; geometry rasterisation differs by level", "etl/health_access/README.md")
for id, period, grain, root, scripts, tables, extras, url, note in [
    ("trade", "Published FY coverage varies; 2015–2025", "HS8 × partner × direction × fiscal year", "trade_data_8digit", ["build_trade.py", "build_trade_pdf.py", "build_trade_extra.py", "build_econ.py", "build_econ2.py"], ["trade_hs8"], ["econ_trade_extra.json"], "https://www.pbs.gov.pk/external-trade-statistics/", "Commodity totals and partner rows overlap; historical parsers include temporary paths"),
    ("national_accounts", "1951–52 through 2025–26 by series", "National sector × year × basis", "national_accounts", ["build_na.py", "build_structure.py", "build_io.py"], ["national_accounts"], ["econ_structure.json"], "https://www.pbs.gov.pk/", "Base years, levels, growth and backcasts differ; input-output bundling path needs audit"),
    ("budget", "2009–10 through 2026–27; older archive inventoried", "Document year × printed row × estimate column", "budget_documents", ["build_budget.py", "build_budget_receipts.py", "build_budget_expenditure.py"], ["budget_lines"], ["budget_receipts_detailed.json", "budget_expenditure_detailed.json"], "https://www.finance.gov.pk/", "Budget estimate/revised/actual columns must remain distinct; script portability untested"),
    ("industry", "Base/year dependent; see source tables", "National industry × month/year × base", "industry_statistics", ["build_ind.py", "build_industry.py"], ["lsm_qim", "lsm_sector_indices"], ["econ_industry.json"], "https://www.pbs.gov.pk/industry-2/", "Legacy repo script points to obsolete raw root and output location")]:
    source(id, period, grain, "Source-specific national economic statistics", url, ["raw_data/"+root]+["data_darbar_warehouse/"+t+".parquet" for t in tables],
           ["data_darbar_warehouse/build/"+s for s in scripts]+["datadarbar/etl/build_web_warehouse.py"],
           [WH+t+".parquet" for t in tables]+["datadarbar/app/data/"+s for s in extras]+["datadarbar/app/assets/js/econ_data.js"],
           "local_raw_and_generators_present_not_rebuilt", note, "raw manifest; warehouse README; script source")
source("sbp", "Series-specific; selected observations 1947–2026", "National series × observation date", "Series-specific units/frequencies", "https://easydata.sbp.org.pk/",
       ["data_darbar_warehouse/sbp_state", "data_darbar_warehouse/sbp_observations.parquet", "data_darbar_warehouse/sbp_series_catalog.parquet"],
       ["data_darbar_warehouse/build/build_sbp.py", "datadarbar/etl/build_money.py", "datadarbar/etl/build_web_warehouse.py"],
       [WH+"sbp_observations.parquet", WH+"sbp_series_catalog.parquet", "datadarbar/app/data/money_data.js"],
       "checkpointed_local_warehouse", "Original API-response directory and credentials are not a public reproducibility package; no API calls made", "data_darbar_warehouse/SBP_EASYDATA.md")
source("file_catalog", "File-specific", "One downloaded/catalogued file", "Metadata only; presence does not imply parsed data", "https://www.pbs.gov.pk/",
       ["raw_data/budget_documents/manifest.csv", "raw_data/national_accounts/manifest.csv", "raw_data/trade_data_8digit/manifest.csv", "data_darbar_warehouse/file_catalog.parquet"],
       ["data_darbar_warehouse/build/assemble.py", "data_darbar_warehouse/build/finalize.py", "datadarbar/etl/build_web_warehouse.py"], [WH+"file_catalog.parquet"],
       "published_metadata_available", "Older catalogue is not a complete project source inventory", "WAREHOUSE.md and local manifests")
source("nepra", "2006–07 to 2025 by series", "Plant/day/month; DISCO/year; report cells", "Published power-system coverage; some networks excluded", "https://nepra.org.pk/",
       ["data_darbar_warehouse/nepra*.parquet", "../Adaad/SOI Reports", "../Adaad/PER", "../Adaad/Detail of Generation", "../Adaad/SOI Data"],
       ["data_darbar_warehouse/build/build_nepra_*.py"], ["data_darbar_warehouse/nepra*.parquet"], "local_warehouse_not_public_catalogue", "Raw files are in Adaad; full source manifest and external inputs require separate capture", "data_darbar_warehouse/NEPRA.md")
for id, period, grain, url in [
    ("ljcp", "Source-report years", "Court/jurisdiction × period × category", "https://ljcp.gov.pk/"),
    ("regional_police", "Source-report years", "District/region × year × offence", "See etl/regional_police/sources.json"),
    ("sindh_police", "Source-report years", "District × year × category", "https://sindhpolice.gov.pk/"),
    ("sindh_fir", "Report dates; year-to-date totals", "District × reporting date × offence", "https://sindhpolice.gov.pk/"),
    ("climate_events", "Source-event dates", "Event/exposure geography; source-specific", "See etl/climate_events/README.md")]:
    source(id, period, grain, "Source-specific administrative/event coverage", url, ["raw_data/"+id, "datadarbar/etl/"+id],
           ["datadarbar/etl/"+id+"/*.py"], ["data_darbar_warehouse/"+id], "untracked_local_extension_outside_baseline",
           "Not part of pinned public baseline or census prototype; retain separate provenance and tests", "Local pipeline README, source manifests and quality reports")
source("legacy_pbs_payload", "Not yet established", "District-like keys", "Not certified", "",
       ["datadarbar/app/data/pbs_district_indicators.json"], [], ["datadarbar/app/data/pbs_district_indicators.json"],
       "legacy_asset_lineage_unresolved", "Producer and active consumer not identified; do not treat as authoritative census input", "Tracked app/data asset; requires manual lineage review")
