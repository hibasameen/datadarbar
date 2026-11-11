# NEPRA power-sector layer (built 2026-09-06)

Everything NEPRA publishes about the power sector, at the most granular level it publishes it:
plant × **day** generation (2017-18 → 2024-25), plant × month, every ruled table of every
State of Industry Report (2010 → 2025), the DISCO Performance Evaluation Reports, and a curated
DISCO × fiscal-year panel (2006-07 → 2024-25) de-duplicated across the reports' overlapping windows.

Raw files (5.9 GB, 12,198 files) are in the iCloud folder `Adaad/` (chosen at download time):
`SOI Reports/`, `PER/`, `html/` (the Excel-as-HTML sheets), `Detail of Generation/` and
`SOI Data/` (the daily PDFs). `Adaad/nepra_download_manifest.json` has every URL, local path, status
and size (45 links were 404 on nepra.org.pk). nepra.org.pk is blocked from the sandbox: files were
streamed to disk from Chrome with the File System Access API (see the script docstrings).

## Tables / views (all Parquet, registered as views in `data_darbar.duckdb`)

| name | rows | grain | what |
|---|---|---|---|
| `nepra_plant_day` | 321,400 | plant × calendar day | installed/dependable MW, max/min load MW, generation GWh, utilisation % — from 10,562 one-page PDFs, FY2017-18→2024-25, CPPA-G system plants only (no K-Electric) |
| `nepra_plant_day_files` | 10,650 | one per daily PDF | parse status + `sum_vs_sheet_relerr` (daily sum vs the monthly sheet: 95% within 1%, 98.5% within 5%) |
| `nepra_plant_month` | 11,817 | plant × month (+ `month='FY'` annual row) | installed/dependable MW, monthly GWh, utilisation %, `daily_pdf` link. Sheet totals match SOI "CPPA-G system" generation within 0.01–1.3% |
| `nepra_plant_annual` | 913 | plant × FY | view = the `FY` rows above |
| `nepra_plants` | 133 | plant | crosswalk: `plant_id` ← 161 published name variants (`name_variants`), technology, fuel, MW, first/last FY |
| `nepra_soi_long` | 437,162 | one numeric cell | every table in SOI 2010–2025: `report_year, table_no, title, section, group, row_label, col_label, value_raw, value, page, continued` |
| `nepra_soi_cells` | 660,703 | raw grid cell | the untidied grids (row/col/owner of merged cells) for anything the tidy layer got wrong |
| `nepra_soi_pages` | 3,464 | page | chars + tables per page (shows the scanned years) |
| `nepra_per_long` / `nepra_per_cells` | 228,215 / 377,128 | cell | same method over 34 Performance Evaluation Reports (DISCOs 2012-13→2024-25, GENCOs/plants, NTDC/KE). `report` = file name; titles are best-effort (PER captions are prose) |
| `nepra_disco_annual` | 20,689 | DISCO × FY × column | 14 curated series pulled from SOI tables by title, latest report wins for each (series, disco, fy, column) |
| `nepra_disco_annual_all` | 66,666 | same, all reports | every source row, for audit / revision history |
| `nepra_disco_losses` | DISCO × FY | view | purchased, sold, losses GWh, losses %, 2006-07→2024-25 |
| `nepra_disco_consumers` | DISCO × FY × category | view | number of consumers by category, 2006-07→2023-24 |
| `nepra_disco_sales` | DISCO × FY × category | view | GWh sold by category, 2010-11→2023-24 |

Curated `series` in `nepra_disco_annual`: consumers_by_category, sanctioned_load_by_category,
units_purchased_sold_losses, units_billed_amount_realized, billing_collection_recovery,
electricity_sold_by_category, consumption_per_connection, distribution_losses_by_voltage,
peak_demand, feeders_and_loading, distribution_lines, grid_stations, power_transformers,
distribution_transformers, feeder_outages, safety_fatal_accidents, new_connections,
village_electrification, receivables, technical_commercial_losses, growth_rate_consumption
(a few match no table in some years — check `SELECT series, min(fy), max(fy)`).

## How the SOI tables were read
`build_nepra_soi.py`: PyMuPDF `find_tables(strategy='lines_strict')` on every page (pages whose text
runs vertically are rotated first — the landscape monthly tables), the "TABLE N <caption>" label is
matched to the table below it by position, continuation pages inherit the label when the header
repeats / is absent, merged cells are resolved to their owning cell (both directions) so multi-row
headers become `col_label = 'Unit Purchased (GWh)|Through NTDC'`, section rows inside a grid
(`A1: Hydel (WAPDA)`) go to `section`, the first label column (forward-filled) to `group`, the
second (or a year column) to `row_label`.

## Coverage & caveats — read before citing
* **Scanned reports**: SOI 2012, 2013, 2014 have no usable text layer (0–4 tables); SOI 2019 has a
  broken text layer (19 tables). Their fiscal years are covered by the 5-year windows of the
  neighbouring reports (e.g. 2015 report → 2010-11…2014-15; 2020–2023 → 2018-19). SOI 2025 is a
  scan with an OCR layer: numbers are fine, **vertical DISCO labels are garbage** — `build_nepra_finalize.py`
  re-labels those groups by matching their overlapping-year numbers to the 2024 report (flag
  `relabelled`); one residual garbage group is the 2025 grid-stations "Total" row, left unlabelled. Column labels in 2025 are
  borrowed from 2024 by column position.
* **Relabelling fixed 17 Oct 2026.** The matcher had let zero cells vote, so zero-heavy rows were given a
  DISCO's name: the "Total in PEPCO/CPPA-G System" grid-station rows in every report 2015–2025 became
  IESCO or PESCO, the CPPA-G Total of SOI 2024 table 65 became PESCO (so `nepra_disco_annual_all` gave
  PESCO the system's net-metering GWh), and three SOI 2011 KESC tables became HESCO or GEPCO. Now only
  non-zero values vote, a group cannot take a DISCO already labelled on the same page, the winner needs
  half the comparable cells and must beat the runner-up, and KE/K-ELECTRIC/KEL vote as one company.
  "K-Electric Limited" (SOI 2015) and "KESC" (SOI 2011) are read as K-ELECTRIC; "Supply to KESC"-type
  rows keep their label. Net effect: 29 groups relabelled (was 53); 31 groups / 1,011 rows changed
  label, listed in `data_darbar_warehouse/build/_pre_relabel_fix_2026-10-17/changed_groups.csv` beside the previous parquets;
  newly recovered are PESCO's feeder tables in SOI 2016–2018 and QESCO's consumption per connection in
  SOI 2018. KE still appears as KE, K-ELECTRIC or KEL depending on the report — filter on all three.
* **Mid-table headers read since 17 Oct 2026.** Some grids restart their header partway down — most
  often K-Electric's block, which has its own columns (in the units-purchased table: KE Own | Through
  NTDC | Through Net-Metering | Through CPPs + Others | Total Unit Purchased, against the DISCOs'
  Through NTDC | Through CPPs, SPPs | Through Net-Metering | Unit Purchased). The tidy step used to read
  only the top header, so KE's columns took the DISCO label of their position (KE's "Through NTDC" was
  its own generation; `nepra_disco_losses` gave KE 2023-24 1,691.9 GWh purchased instead of 17,896).
  `tidy()` now treats a short all-text body row (≥ 3 cells, no numbers or placeholders, numbers soon
  after) as a new header for the rows below; values, groups and row labels are unchanged, only
  `col_label` moves (9,558 cells in 151 grids: KE statistics tables, the 2022 daily-generation
  month blocks, IPP gas vs furnace-oil blocks, the 2022 subsidy table's KE columns, KE feeder counts),
  and 1,914 header-text cells are no longer stored as values. Lists in
  `data_darbar_warehouse/build/_pre_ke_header_fix_2026-10-17/changed_tables.csv` and `changed_labels.csv`. For the scanned
  2025 report, labels borrowed from 2024 now come from the same company's rows first. Re-tidy without
  the PDFs: `python build_nepra_soi.py --from-cells`.
* Figures are **as published, revisions included**: the same DISCO-year appears in up to five reports
  and values differ (e.g. PESCO 2022-23 purchased 15,254.5 in SOI 2023 vs 15,279.6 in SOI 2025).
  `nepra_disco_annual` keeps the latest report; `_all` keeps every one. The 2011 report's PESCO loss
  percentages (53–64%, footnoted `PESCO*`) are on a different basis from later reports (34–38%).
* Plant-day: `load_min_mw` is NULL where the PDF has 5 numeric columns; 2 rows with day > days-in-month
  dropped; 3 PDFs had no day rows; the 2024-25 bagasse/small-hydel files show 5–20% daily-sum vs sheet
  gaps (source inconsistency, both kept). Monthly sheets link a few wrong PDFs (e.g. Engro Dec 2020 →
  Feb 2021 file) — `nepra_plant_day_files.month_txt` holds the month printed inside the PDF.
* Plant sheets and daily PDFs cover the CPPA-G system only (K-Electric's plants are in SOI tables
  25–34 only). `plant_id` is a name-based crosswalk with manual aliases (`ALIASES`/`FULLNAME` in
  `build_nepra_finalize.py`); NPPCL/NPPMCL, Zhenfa→Atlas Solar, Tricom→Lucky Renewables, Karot,
  Laraib/New Bong, Gul Ahmed I/II are merged deliberately.
* Landscape (rotated) tables are flagged `near LIKE '%[rotated]%'` in `nepra_soi_cells`.
* Not parsed (raw only, under `Adaad/Detail of Generation/`): FCA decision PDFs 2018-22, DISCO SRO
  tariff notifications 2011-22, hydel plant sheets, XWD/KE quarterly-adjustment PDFs. The SOI tables
  already carry the monthly FPA and quarterly-adjustment series.

## Rebuild
```
export NEPRA_RAW="<path to Adaad>"     # default ~/Library/Mobile Documents/com~apple~CloudDocs/Adaad
cd etl/warehouse
python build_nepra_month.py            # sheets -> nepra_plant_month
python build_nepra_daily.py            # daily PDFs -> nepra_plant_day (+ QA file table), ~1 min on 4 cores
python build_nepra_soi.py [years]      # SOI PDFs -> soi_cells/long (30-90 s per report; per-year cache in $NEPRA_CACHE)
python build_nepra_soi.py --from-cells # re-tidy soi_long from the stored soi_cells (seconds; no PDFs needed)
python build_nepra_per.py              # PER PDFs -> per_cells/long
python build_nepra_finalize.py         # crosswalk, disco series, DuckDB views, file_catalog
```
Needs pymupdf, pandas, pyarrow, duckdb, beautifulsoup4, lxml.

## Example queries
```sql
-- daily generation of Tarbela, FY2024-25
SELECT date, load_max_mw, generation_gwh, utilization_pct FROM nepra_plant_day
WHERE plant_id='tarbela_hydropower_project_wapda' AND fiscal_year='2024-25' ORDER BY date;

-- monthly fuel mix (sum over plants by fuel)
SELECT fiscal_year, month_date, fuel, round(sum(generation_gwh)) gwh FROM nepra_plant_month
WHERE month<>'FY' GROUP BY 1,2,3 ORDER BY 2,4 DESC;

-- DISCO losses panel
SELECT * FROM nepra_disco_losses WHERE disco IN ('PESCO','LESCO','KE') ORDER BY disco, fy;

-- any SOI table by title, e.g. subsidies
SELECT report_year, "group", row_label, col_label, value FROM nepra_soi_long
WHERE title ILIKE '%subsid%' ORDER BY report_year, page, row, col;

-- which tables exist in a given report?
SELECT table_no, any_value(title) title, count(*) cells FROM nepra_soi_long
WHERE report_year=2024 GROUP BY 1 ORDER BY 1;
```
