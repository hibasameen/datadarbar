# Pakistan weather-related event pipeline

Collects source-specific disaster records, report metadata, PDFs, GIS downloads and
UNOSAT exposure observations. Outputs are local CSV and Parquet; nothing is deployed
to the public website and no EM-DAT data is redistributed by this pipeline.

## Run

Requires Python 3.10+ on macOS/Linux. From the repository root:

```sh
python3 -m pip install -r etl/climate_events/requirements.txt

# Events: annual, paginated queries; checkpoints are immutable cached responses.
python3 etl/climate_events/pipeline.py gdacs --start 2000-01-01 --end 2026-09-06

# The Pakistan July 2022 flood product discussed in the project review.
python3 etl/climate_events/pipeline.py unosat --ids 3343 --geometry

# All pages in the current monsoon listing; download the three latest PDFs.
python3 etl/climate_events/pipeline.py ndma --max-pages 20 --pdf-limit 3

# Import your authorised EM-DAT XLSX download; no account credentials are stored.
python3 etl/climate_events/pipeline.py emdat /path/to/emdat-export.xlsx

# Rebuild without network, or repeat extraction using only existing downloads.
python3 etl/climate_events/pipeline.py build
python3 etl/climate_events/pipeline.py --offline unosat --ids 3343 --geometry

# Refresh upstream data, preserving earlier source versions.
python3 etl/climate_events/pipeline.py --refresh ndma --pdf-limit 3

python3 -m unittest discover -s etl/climate_events -p 'test_*.py' -v
```

If your shell's `python3` lacks the packages, activate a Python environment with the
requirements installed. The initial live run used the existing Anaconda environment.
Global options `--raw`, `--out`, `--refresh`, `--offline` go before the subcommand.
Increase `--pdf-limit` to extract more reports, and `--max-pages` if the source grows.
Rerunning without `--refresh` uses the saved snapshots, including saved listing pages.

## Files and schemas

Defaults are resolved relative to the repository, not to a particular user's home:

- `../raw_data/climate_events/objects/`: original files, named by SHA-256.
- `../raw_data/climate_events/manifest.jsonl`: URL, source, UTC retrieval time,
  content hash, bytes, local path, content type and HTTP status.
- `../raw_data/climate_events/runs/`: extracted source snapshots. Only a completed
  extraction replaces a run file; a failed fetch leaves earlier successful runs intact.
- `../data_darbar_warehouse/climate_events/`: CSV/Parquet tables, `catalog.json`
  and `quality_report.json`.

| Table | Grain | Meaning |
|---|---|---|
| `climate_events` | source event × Pakistan | GDACS alerts or imported EM-DAT events. `record_id` is unique. |
| `climate_reports` | source publication | A report may cover many events or update earlier cumulative totals. |
| `climate_assets` | report × linked file | Download status, raw path/hash and original URL for PDFs, spreadsheets and GIS files. |
| `climate_exposures` | UNOSAT product × geographic row | Satellite/modelled exposure; country, province and district rows overlap. |
| `climate_pages` | report × PDF page | Extracted text with page number and source hash. Blank pages may require OCR. |
| `climate_impacts` | NDMA report × region × metric | Cumulative casualty/infrastructure snapshots with explicit reporting periods. |

The catalogue gives explicit column types, including typed empty tables. Dates are
ISO strings. Event dates are nullable where only a year/month is known; `start_year`
and `date_precision` retain that distinction. Original date components remain in
the EM-DAT raw export. Damage values are nominal US dollars, converted from EM-DAT's
thousand-dollar column; missing values remain null. No inflation adjustment is applied.

## Sources and exact coverage

**GDACS:** [API documentation](https://www.gdacs.org/gdacsapi/swagger/index.html).
Uses `/api/Events/geteventlist/search`, `country=Pakistan`, with annual date windows
and explicit pagination. Filters for floods, tropical cyclones, droughts and wildfires;
also verifies Pakistan in the returned country/affected-country fields. HTTP 204
means no records returned for that query, not proof that no disasters happened.
The API's period filter determines which overlapping events are returned. The initial
collection requests 2000 onward; coverage varies by hazard and period. No heatwave
history is supplied by this adapter. GDACS centroids are not assigned to districts.
Source event IDs retain identity across alert episodes; newest source revision wins.
The alert catalogue does not populate deaths, affected people or economic losses.

**UNOSAT:** [product 3343](https://unosat.org/products/3343), using the public JSON
endpoint used by its website (`/our_products/3343`). The exact XLSX layout has been
verified for product 3343. It has two overlapping worksheets; only `Statistics the
whole country` is imported. Unknown layouts fail explicitly while preserving the
download for inspection. Dates 12–21 July 2022 are the satellite observation period,
not the August publication date or a GLIDE-derived event date. This adapter records
other explicitly supplied Pakistan product IDs but only imports matching XLSX layouts.
It does not automatically discover the entire UNOSAT catalogue. `--geometry` downloads
the source GIS archives without extracting ZIP contents or calculating intersections.

**NDMA:** [monsoon listing](https://ndma.gov.pk/sitreps?cat_id=3).
Follows listing pagination up to a configured ceiling and reports whether it reached
the end. This is the current monsoon category, not all historical NDMA reports. PDFs
are retained and text is extracted per page. A strict adapter extracts the observed
2026 cumulative provincial casualty and infrastructure tables, including tables whose
heading falls on the preceding page. It requires all seven regions plus a national
total, checks demographic/housing subtotals, and checks national totals when every
component is numeric. Missing dashes remain null. A changed recognised layout fails
explicitly. Daily district tables, relief tables, scanned pages and other historical
formats remain text-only. Never sum successive sitreps.

**EM-DAT:** [access and terms](https://doc.emdat.be/docs/data-accessibility/).
Import a registered user's authorised XLSX export. Recognises header rows even after
introductory rows, filters ISO `PAK`, and excludes geological/technological/biological
events. Landslides are not included automatically because their trigger needs review.
Public access does not grant unrestricted redistribution; outputs stay outside `app/`.
No live EM-DAT import was possible without an authorised export.

GDIS, Global Flood Database, PMD drought monitoring and heatwave derivation are not
implemented here. This first pipeline covers the three live sources above and an
EM-DAT export importer. It does not attribute individual events to climate change.

## Geography and quality controls

- District matching uses exact normalised names against `app/data/districts.json`.
  A match is labelled `exact_name_boundary_unverified`, since identical names alone
  do not prove boundaries match. Variants/splits remain unmatched for review.
- Province/country summaries are kept for source reconciliation, not added to districts.
  Federal Capital Territory and its Islamabad district are separately identified.
- `population_exposed` is a WorldPop-derived estimate of potential exposure. It is
  not equivalent to reported affected people, displaced people or casualties.
- Spreadsheet `-` values stay null. Implausible source fractions above 1 are retained
  and flagged, never silently clipped. For example, some analysed-area ratios can
  exceed the source's stated administrative area.
- Product 3343's descriptive text says approximately 33,200 km², whereas its XLSX
  national maximum-water-extent cell contains 35,242.248737 km². The pipeline preserves
  both the description and spreadsheet value; it does not reconcile them by guessing.
- Source events are deduplicated within a provider only. Cross-source reconciliation
  needs an explicit reviewed crosswalk. Similar dates/GLIDE labels alone do not justify
  merging a UNOSAT product with a GDACS/EM-DAT event.
- Tests cover country/hazard filtering, revisions, missing values, partial dates,
  paging loops, immutable cache checksums, reproducible builds, typed empty outputs,
  EM-DAT unit conversion, duplicate UNOSAT sheets, and split-page NDMA reconciliation.
- `quality_report.json` records source scope/errors, unmatched districts, source
  anomalies and pages with no extractable text. Asset failures return a nonzero exit.

## Query examples

Run with DuckDB from the output directory:

```sql
-- Source records, not a cross-source deduplicated count of disasters.
SELECT source, hazard, start_year, count(*) AS source_records
FROM 'climate_events.parquet'
GROUP BY ALL ORDER BY start_year, source, hazard;

-- Potential exposure by source district, for one observation product only.
SELECT location_name, district_key, population_exposed, flooded_area_km2
FROM 'climate_exposures.parquet'
WHERE report_id = 'unosat:3343' AND admin_level = 'district'
ORDER BY population_exposed DESC NULLS LAST;

-- Retrieve report pages for a subsequent structured impact-table adapter.
SELECT report_id, page, text
FROM 'climate_pages.parquet'
WHERE source_url LIKE '%ndma.gov.pk%' AND text ILIKE '%cumulative%';

-- Cumulative national casualties: one snapshot per report, never sum over time.
SELECT period_start, period_end, value AS cumulative_deaths, source_url, source_page
FROM 'climate_impacts.parquet'
WHERE admin_level = 'country' AND metric = 'deaths_total'
ORDER BY period_end;
```

The pipeline deliberately builds a separate local catalogue. Adding approved datasets
to `build_web_warehouse.py` and the public SQL console is a separate publication step.
