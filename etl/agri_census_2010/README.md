# Agricultural Census 2010 (6th) — district tables

Source: PBS / Agricultural Census Organization, https://www.pbs.gov.pk/agriculture-census/ ("Agricultural
Census 2010 - Pakistan Report"). PDFs saved 2026-11-01 in `raw_data/pbs/agricultural_census_2010/`; parsed
2026-11-23 from the PDF text layer (poppler `pdftotext -layout`), no OCR.

```
python3 parse_tables.py <raw_data/pbs/agricultural_census_2010> <.../derived>   # ~7 s
python3 build_district_headline.py <.../derived>
python3 build_warehouse.py <.../derived> <app/data/warehouse>                  # three Parquet tables
python3 ../catalog_merge.py agri_census_2010 && python3 ../../scripts/build_seo.py   # catalogue entries, dataset pages
```

In the catalogue (2026-11-23): `agri_census_2010_district` (the headline table), `agri_census_2010_long`
(every cell, with table titles) and `agri_census_2010_tables` (the column index). Not in Places: the units
are the 2010 district frame and nothing is keyed to the 2023 districts yet.

Outputs in `raw_data/pbs/agricultural_census_2010/derived/`:
- `agri_census_2010_long.csv.gz` — 1,085,364 cells, one row per printed cell: scope, table, unit, group,
  row_label, col (PBS's printed column number), value_raw, value, flag, qc, pdf_page.
- `tables_catalog.csv` — table × file × column: title, area note, and `header_guess` (header text assigned to
  the nearest column by position — good for single-level headers, unreliable under spanning headers; check
  the PDF page in `first_page` before relying on it).
- `agri_census_2010_district_headline.csv` — 100 units × farms, farm area, cultivated area, average farm
  size, size-class and tenure shares, cropped/kharif/rabi/orchard area, cropping intensity (Tables 1.1, 1.2, 6.1).
- `parse_log.txt` — every row the parser could not place cleanly.

## Coverage
- **Punjab, Sindh, Khyber Pakhtunkhwa: all 90 tables (1.1–13.3) for every unit**, areas in acres.
  Punjab 37 districts (incl. Islamabad) + De-excluded Areas of D.G. Khan and of Rajanpur + Cholistan;
  Sindh 23 districts (Karachi is one unit); KP 24 districts + Malakand, Bajaur, Khyber, Mohmand agencies +
  six Frontier Regions. This is the 2010 district frame, not the 2023 one used for the 2024 census.
- **Balochistan districts are missing.** On the PBS page the "Balochistan Province Tabulation" link points
  to the Sindh file, and no other copy was found. Balochistan appears only as a province total in
  `Tables-Pakistan-In-Hectares.pdf` (scope `PAKISTAN (hectares file)`, 86 tables, areas in hectares).
- "All Pakistan Tables (In Acres)" is still on the old PBS path but was not downloaded (pbs.gov.pk is
  blocked from the shell); it duplicates the hectares file.

## Checks
- Table 1.1 farms: districts sum exactly to the province in all three provinces, and the four provinces sum
  to Pakistan's 8,264,480 (Punjab 5,249,804; Sindh 1,115,285; KP 1,539,829; Balochistan 359,562).
- Every headline column (farms, farm area, cultivated area, owner/tenant farms, cropped area) sums exactly
  from districts to province.
- Across all tables, 16,781 of 17,396 province cells of 1,000 or more (96.5%) equal the sum of their
  districts. The rest include tables PBS prints in hundreds/thousands (8.3, 12.3, 12.4), rows with a blank
  cell, and source typos; a handful of tables (Sindh 8.5 and 6.20, KP 11.4, Punjab and Sindh 12.1) have more mismatches than
  that explains and have not been traced cell by cell.

## Conventions and source quirks
- `-` = nil (value 0, flag `nil`); `*` and `+` = less than half a unit (value empty, flag `lt_half`);
  `X` = not applicable (flag `na`).
- `qc = count_mismatch` (72 rows): the row has fewer figures than columns (a blank cell in the PDF), so the
  column numbers of that row are positional guesses. `qc = dup_label` (1 row): Mohmand Agency, Table 1.1,
  prints "25.0 TO UNDER 50.0" twice; the second row is the 50–100 class.
- Table 11.3 prints column numbers 1 2 4 5 6 7 9 10 11; those printed numbers are kept as `col`.
- 11 pages have no column-number row; columns are taken from the table's first page (logged).
- Flat tables (one row per unit, e.g. 4.4) have an empty `row_label`.
- Unit spellings are PBS's ("JACCOBABAD", "SHANGLAPAR", "UMER KOT"); variants within a file are merged.
- The Pakistan hectares file prints three Table 11.7 figures in scientific notation (2.4E+07), flag `sci_rounded`.
- One block breaks across a page (Khyber Pakhtunkhwa Table 6.12, Bajaur Agency's "150.0 AND ABOVE" row):
  its 14 cells have no unit in the long CSV. `build_warehouse.py` gives them the unit of the row above and
  marks them `qc = unit_carried`; the CSV is unchanged.
