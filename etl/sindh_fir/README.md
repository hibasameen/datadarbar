# Sindh district daily and year-to-date FIR totals

Extracts section **2. FIRs REGISTERED IN SINDH PROVINCE (DAILY BASIS)** from Sindh Police's public daily statistical PDFs. These are all-FIR registration counts. Section 3 (FIRs arising from Emergency-15 complaints) and the range-level daily crime categories are separate measures and are excluded.

## Recovered coverage

11 reports, 31 police districts per report, **341 district-date records**:

- 15, 16, 17, 19, 20, 21, 22, 23 and 24 September 2025.
- 6 and 7 November 2025.

Each record contains the published daily FIR count and cumulative count from 1 January through that report date. There are 294 numeric daily counts, 47 missing daily values, and 341 numeric YTD counts. The latest recovered snapshot is **7 November 2025**, when the published province totals are 311 daily and 106,723 YTD FIRs.

**This is a partial series.** No district daily/YTD reports for 2019–2024 were recovered; 354 dates in 2025 have no recovered report. Missing reports are not evidence of zero FIRs or proof that reports never existed. No year-end 2025 district total is available in this extraction. No 2026 district FIR report was recovered either.

## Rebuild

Run from the `datadarbar` repository using Python with the packages in `requirements.txt`:

```sh
python3 etl/sindh_fir/pipeline.py rebuild
python3 -m unittest discover -s etl/sindh_fir -p 'test_*.py'
```

`rebuild` recovers complete tables from saved indexed responses, deduplicates identical copies, verifies source hashes, and writes CSV, JSON and Parquet. `build` uses the existing source manifests without rerunning recovery. All paths default to the surrounding Data Darbar workspace, regardless of the current directory. Optional `--raw` and `--out` arguments must precede the command.

To append an original PDF once accessible:

```sh
python3 etl/sindh_fir/pipeline.py fetch 'OFFICIAL_REPORT_URL'
python3 etl/sindh_fir/pipeline.py import-pdf '/path/to/report.pdf' --source-url 'OFFICIAL_REPORT_URL'
```

The original-PDF importer uses pdfplumber and accepts the complete known four-column, 31-district table. It rejects incomplete tables and unfamiliar layouts for review. It does not OCR scanned PDFs. Direct fetching currently receives HTTP 403, so the original-PDF path has not been verified against live Sindh files. Indexed-table parsing is tested against saved official-source fixtures.

## Outputs

Files are in `data_darbar_warehouse/sindh_fir/`, each dataset in CSV, JSON and Parquet:

| Dataset | Contents |
| --- | --- |
| `sindh_fir_district_daily_ytd` | 341 district-date rows; main analysis table |
| `sindh_fir_latest_available` | 31 district rows for latest recovered date |
| `sindh_fir_reported_totals` | 77 separate range/province totals; do not add to district rows |
| `sindh_fir_observations` | All source observations, including any imported duplicate representations |
| `sindh_fir_sources` | Source URLs, acquisition method, local evidence paths, dates, page and SHA-256 |
| `sindh_fir_total_checks` | 176 within-report reconciliation checks |
| `sindh_fir_temporal_flags` | 88 inter-report anomalies: 55 district and 33 aggregate |
| `sindh_fir_year_coverage` | Explicit recovery gaps for 2019–2025 |
| `quality_report.json` | Coverage, validation counts and limitations |

Main fields: `report_date`, `ytd_start_date`, `ytd_end_date`, `geography_id`, `geography_name`, `police_range`, `daily_firs`, `ytd_firs`, original cell strings/status, source URL/page/hash, and discrepancy flags. Police IDs derive from the original names and are **not census ADM2 IDs**.

## Interpretation and quality

- Dashes and blanks remain null. Explicit zeros remain zero. A printed range total of zero does not establish that every district dash means zero.
- YTD values are snapshots. Never sum them across dates or treat the last recovered report as a full-year total. Daily FIRs come only from the daily column, not differenced YTD values.
- All 163 fully checkable range/province reconciliations pass. The other 13 cannot be checked because district daily values are missing.
- 55 district temporal discrepancies are retained. For example, Tando Allah Yar YTD is printed as 154 on 19–21 September, between values above 1,200. Causes may include reporting revisions or source/index errors; no correction is inferred.
- Rows cover 31 police districts, including eight in Karachi: South, City, Keamari, East, Malir, Korangi, West and Central. Do not join them to census district boundaries solely by name. Original spellings, including SHAHEED BEANZIRABAD and NAUSHERO FEROZ, are preserved.
- Matching original PDF and indexed representations deduplicate, preferring original PDFs. Conflicting values for the same date and geography stop the build for explicit review.

## Provenance and access limits

Evidence is under `raw_data/sindh_fir/`. Every complete indexed table is archived as a hash-addressed text object, with manifest `indexed_sources.json`; `sources.json` in this ETL folder pins the initial extraction. Saved discovery responses retain the source URL and exact indexed table. No fabricated PDF is stored.

On 6 September 2026, direct PDF access returned HTTP 403 and Chrome displayed a Cloudflare block despite the user's VPN. Public search-index representations of the official PDFs were readable. Indexed page text separately confirms 93 district rows / 186 numeric cells for 15, 22 and 24 September; see `indexed_page_crosschecks.json`. Original PDF rendering was blocked, so this is **indexed-source extraction**, not visual verification of the PDFs.

Discovery included the official site, year-specific searches for 2019–2026 and the Internet Archive PDF URL index. The archive lookup returned no matching `/storage/dsr/` PDFs. This describes recovery coverage, not an exhaustive inventory of unpublished or inaccessible records. No police case-level records are part of this dataset.

This pipeline writes only its own raw/output folders. It does not modify the website, census data, annual range-level crime pipeline or shared database.
