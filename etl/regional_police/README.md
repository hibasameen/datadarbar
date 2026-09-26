# Regional police and reported-crime pipeline

Built 6 September 2026. This extends the Sindh work with official annual statistics for Balochistan, KP, ICT, GB and AJK. It is a separate dataset: these annual reported-case statistics do not establish a daily or year-to-date FIR series.

## Recovered coverage

| Region | Annual regional total | District information | Reference years |
| --- | --- | --- | --- |
| Balochistan | PBS recorded-crime total | District FIR/crime totals not recovered. Separate province-wide police/Levies tables contain selected offences and accidents for 2019–2023. | 2019–2024 regional |
| KP | PBS recorded-crime total | Seven offence categories; 32 district units in 2019–2020, 35 in 2021–2024. No district all-FIR total. | 2019–2024 |
| ICT | PBS recorded-crime total for the entire territory | Territory aggregate; no station/zone breakdown recovered. | 2019–2024 |
| GB | PBS recorded-crime total | District crime table not recovered. | 2019–2024 regional |
| AJK | PBS total and a separate AJK Police total | Ten districts, reported-crime totals and offence categories. | 2019–2024 |

No daily/YTD archive or 2025 **reference-year** observations were recovered for these five regions. A publication labelled 2025 can contain 2024 data. `not_recovered` is a discovery result, not proof the data does not exist.

## Sources and access

* **PBS / National Police Bureau:** [Social Statistics](https://www.pbs.gov.pk/social-statistics-2/). Original 2022 Statistical Year Book table 19.3 supplies 2019; the standalone provincial crime PDF supplies 2020–2021; the current yearly XLSX supplies 2022–2024. The separately downloaded monthly spreadsheet is **national**, so it is not used for regional monthly series. The other province XLSX is retained as a reference copy.
* **KP Bureau of Statistics:** [public statistics portal](https://kpbos.gov.pk/). Development Statistics editions 2020–2025 are downloaded through the website's public publication metadata and attachment APIs. The 2023 and 2024 district-table headings retain old year text: actual reference years come from the column headers. The seven categories are murder, kidnapping for ransom, child lifting, abduction, car theft, car snatching and motorcycle theft. Their sum is not an all-crime or all-FIR count.
* **AJK Bureau of Statistics / Central Police Office:** [Statistical Year Book archive](https://pndajk.gov.pk/statyearbook.php), table 17.3 in editions 2020, 2022, 2024 and 2025. Latest editions win within an overlapping reference year. The 2025 PDF's physical pages 241–242 cover 2022–2024. The following prisoner table is excluded.
* **Balochistan Bureau of Statistics / Home and Tribal Affairs:** [current publications page](https://www.bospnd.balochistan.gov.pk/resources). The current download endpoint returns `403 Unauthorized access`. Public archived copies of editions 2019–20 through 2022–23 were recovered from the Internet Archive. Edition 2022–23, PDF page 138 (printed 111), supplies 2019–2023 selected offences and accidents in police and Levies areas. Its “Grand Total” is stored as a **selected-category total**, not an all-FIR total. The neighbouring district table counts police stations/Levies thanas, not crime.
* **GB:** the official statistical PDF host failed DNS resolution. GB Police service pages did not yield a public aggregate crime archive. PBS therefore provides the usable regional series.

All original URLs, archived URLs, content hashes, source pages and retrieval date are in `sources.json`. Access outcomes are in `discovery_audit.json`. Public KP API responses, website pages and the Balochistan archive index are retained in the raw discovery directory. No private case records or personal identifiers were collected.

## Outputs

Outputs live under `data_darbar_warehouse/regional_police/` in the workspace. Every nonempty table is supplied as CSV, JSON and Parquet.

| Table | Purpose |
| --- | --- |
| `regional_totals` | 30 PBS totals: five regions × six years. |
| `regional_crime_annual` | PBS offence categories and totals for the five requested regions. |
| `district_totals` | 60 AJK totals: ten districts × six years. |
| `district_crime_annual` | 2,358 district records: 1,428 KP offence observations and 930 AJK category/total observations. |
| `baloch_police_levies_annual` | 150 observations for 2019–2023, separated by police/Levies coverage. |
| `crime_annual` | Selected records from each source family; includes other PBS regions for reconciliation. Do not sum different source families. |
| `observations` | All 5,188 extracted observations, including superseded editions and PBS reconciliation regions. |
| `source_revisions` | 146 changed values/statuses or units absent from a newer edition. |
| `total_checks` | Original-edition arithmetic checks: 265 pass, 222 cannot be fully checked, two source mismatches. |
| `cross_source_differences` | Six AJK provincial/PBS annual total differences. |
| `year_coverage` | Explicit 2019–2025 availability, including missing daily/YTD series. |
| `geography_crosswalk` | Normalized source district names and historical coverage; census ADM2 IDs remain unassigned. |
| `quality_report.json` | Build counts and validation summary. |

The two arithmetic mismatches are in the PBS 2019 table: AJK's listed categories sum to 7,680 while its printed total is 7,682; the regional “Others” values sum to 648,116 while the national cell prints 648,118. Preserve those printed values. AJK's own 2024 total is 9,962 versus PBS's 9,871. This is a cross-source disagreement; the AJK district totals themselves sum to their printed regional total. All six years have a PBS/AJK difference and carry an explicit flag in the selected data.

## Interpretation and geography

* `value` is an integer or null. `raw_value` and `value_status` distinguish blanks, dashes and reported zeros. The selected district data have 535 null cells. Missing values are not imputed, including when a total might make zero seem plausible.
* The source's latest **whole table** is selected for each family/year/scope. Selecting each district independently would retain superseded combined districts alongside their replacements, so this is deliberately avoided. Older editions stay in `observations`.
* AJK and PBS are separate source families. Neither silently overwrites the other. KP/PBS offence differences may also reflect scope/reporting differences; do not join them into one supposedly identical series.
* `geography_id` is a local source-name identifier, **not** a census code. Chitral and Kohistan boundaries change during the series; South Waziristan can represent a combined historical unit. A spatial join needs a reviewed ADM2 crosswalk. Counts are not split or duplicated across successor districts.
* Malakand and early merged-district KP cells carry reporting-coverage cautions. Reported zeros and dashes do not establish that no crime occurred.
* Category definitions differ between publications. PBS adds a separate motor-vehicle theft/snatching category from 2022; retain source categories when analysing trends.
* Blank/dash children make an arithmetic check uncheckable unless the known subtotal already exceeds the total. `uncheckable` does not mean failed. Published discrepancies are flagged, never repaired by adjusting data.

## Run

From the workspace root, with Python, Poppler (`pdftotext`) and a `tar` implementation supporting RAR archives on PATH:

```sh
python -m pip install -r datadarbar/etl/regional_police/requirements.txt
python datadarbar/etl/regional_police/pipeline.py fetch
python datadarbar/etl/regional_police/pipeline.py build
python -m unittest discover -s datadarbar/etl/regional_police -p 'test_*.py' -v
```

The tested local Python is `/Users/hibasameen/anaconda3/bin/python3`. `--raw` and `--out` override directories. Existing raw files allow an offline build. Fetch uses pinned public URLs and validates hashes; a changed publisher file requires review and a new manifest version instead of silently replacing a source. KP's public endpoint wraps downloads as base64; the fetcher decodes that wrapper and unpacks the published RAR when needed. `edition_priority` is a selection rank; it is not evidence of a publication date for undated PBS files.

To add a year, inspect the new original table, add its URL/hash/page configuration to `sources.json`, and update coverage assertions after confirming actual reference years. Do not infer an entire 2025 series from a midyear report. The pipeline does not install a scheduler or modify the app's shared DuckDB database.

Verification covers original PDF table layouts, cell/column alignment, stale title years, null/zero preservation, historical district replacement, source disagreements, output types, and deterministic rebuilds. Eleven automated tests exercise those risks. Original AJK 2024, KP 2023–2024, PBS 2020–2022 and Balochistan 2019–2023 tables were visually inspected.
