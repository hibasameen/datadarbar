# LJCP judicial statistics pipeline

Tier 1 judicial-data source for Data Darbar. This pipeline archives LJCP's annual and half-year PDFs, extracts case flows and judicial strength, retains the reported sessions geography, and builds a separate map feed using the existing census dataset.

The release contains **2,043 annual case records** for 2020–2025, **72 half-year provincial records** through June 2025, **6,996 case-category records** (family, narcotics, murder, bail, rent, appeals, revisions, seven-years-plus, hudood) for 2020 and 2022–2025, and **74 national-overview rows** (every superior court and provincial district judiciary, 2020 and 2022–2025). It does not fill missing years with zero or treat half-year counts as annual counts.

## Coverage

Counts below are judicial reporting units in the all-cases tables, not necessarily census districts. Each has all/civil/criminal records, with court-coverage qualifications below.

| Annual year | Punjab | Sindh | KP | Balochistan | ICT |
|---|---:|---:|---:|---:|---:|
| 2019 | unavailable | unavailable | unavailable | unavailable | unavailable |
| 2020 | 36 | 27 | 35 | 25 | 2 |
| 2021 | 36 | 27 | 35 | 26 | 2 |
| 2022 | 36 | 27 | 35 | 29 | 2 |
| 2023 | 36 | 28 | 35 | province only | 2 |
| 2024 | 36 | 28 | 35 | 34 | 2 |
| 2025 | province only (court-tier category totals) | 28 | 35 | province only (national overview) | 2 |

Edition search (2026-09-06, Wayback CDX over all 826 archived ljcp.gov.pk PDFs plus the old publications pages): annual 2015, 2016, 2018 and 2019 were never listed and are treated as unpublished; an **annual 2017** edition exists (Wayback capture 2021-04-25 of `5a838-jsp_14.pdf`, the same filename LJCP later reused for the 2014 edition — earlier captures of that URL are 2014) and the **July–December 2023** bi-annual (`bar.pdf`, province level) is live; both are catalogued in `sources.json` but not yet retrieved (`run.py --fetch --archive-only` with `--ids annual_2017 halfyear_2023_h2`), and 2017 needs its own table specifications. No 2019 annual edition was located in the inspected current/archived catalogues. The 2023 Balochistan chapter supplies provincial civil/criminal totals and district tables for selected categories, but no consolidated district all/civil/criminal table. The annual 2025 edition (`809 (26) LJCP Final Judicial Statistics 2025 (11-8-26).pdf`, 286 pages) was retrieved from the live LJCP site on 2026-10-24; no Wayback capture existed yet, so its catalogue entry has no archive URL. These are release gaps, not claims that the institutions have no underlying data.

The recovered 2014 edition and an alternate 2024 edition are retained as optional archive sources. Their district tables are not part of this 2019–2025 target release. The January–June 2023, January–June 2024, July–December 2024 and January–June 2025 provincial tables are extracted separately.

## Rebuild

From the Data Darbar workspace root, with Python 3.11 and the dependencies in `requirements.txt`:

```sh
python datadarbar/etl/ljcp/run.py        # extract.py → categories.py → build.py
python -m unittest discover -s datadarbar/etl/ljcp -p 'test_*.py' -v
```

## Outside LJCP: AJK and GB

LJCP does not cover Azad Jammu & Kashmir or Gilgit-Baltistan (20 map units). AJK's Bureau of Statistics publishes district-level court tables (District & Sessions, Senior Civil and Civil Courts by district, three-year windows) in chapter 11 of the *AJ&K Statistical Year Book* — editions 2017–2025 at `https://pndajk.gov.pk/statyearbook.php` (e.g. `uploadfiles/downloads/Statistical Year Book 2025.pdf` covers 2022–2024), giving continuous 2015–2024 coverage; column definitions have not yet been inspected. No GB source was found: the GB Chief Court and Supreme Appellate Court publish no statistics, and the P&DD "Gilgit-Baltistan at a Glance" booklets could not be read from here.

To download the selected, checksum-pinned editions before rebuilding:

```sh
python datadarbar/etl/ljcp/run.py --fetch --archive-only
```

Without `--archive-only`, the downloader tries LJCP first and then its archived original. It validates PDF signatures and edition checksums, retains content-addressed objects, and records retrieval URLs and timestamps. An unexpected replacement PDF or damaged cache fails validation; it is not silently substituted. The live LJCP host was inaccessible from this environment, so this release uses archived copies of the original LJCP PDFs.

The validated local interpreter is `/Users/hibasameen/anaconda3/bin/python3`. Routine rebuilds need only pdfplumber and DuckDB; the reviewed 2021 transcription is bundled, so they do not need macOS OCR or Chrome. `run.py` accepts `--raw`, `--out`, and `--population` overrides.

Defaults, relative to the workspace:

| Location | Contents |
|---|---|
| `raw_data/ljcp/pdfs/` | Named PDF copies and retrieval metadata |
| `raw_data/ljcp/objects/` | Immutable content-addressed PDF originals |
| `raw_data/ljcp/ocr/annual_2021/` | Rendered pages, column/cell crops, OCR evidence |
| `raw_data/ljcp/manifest.jsonl` | Download/cache attempt history |
| `data_darbar_warehouse/ljcp/` | Independent CSV, JSON and Parquet outputs |

The pipeline does not write to the application's district data, the shared DuckDB database, or the public site. No recurring task is configured.

## Outputs and intended use

Every nonempty dataset below is written as CSV, JSON and Parquet. Empty issue reports are JSON arrays. CSV list/object cells are JSON strings; JSON/Parquet preserve their structure. JSON null and blank CSV numeric cells mean missing, not zero.

| Dataset | Grain and purpose |
|---|---|
| `source_observations` | Source table rows including printed totals, original spellings/cells, source URLs, hashes, PDF page, table, row and stable record ID |
| `court_tier_cases` | Extracted cases before combining Punjab civil and sessions courts |
| `annual_sessions` | Year × province × sessions reporting unit × case category; retains all source geographies |
| `annual_provinces` | Annual provincial case tables/totals, including Balochistan 2023 |
| `halfyear_provinces` | Explicit January–June or July–December provincial/national district-judiciary totals; no annualization |
| `judicial_strength` | Reported consolidated or explicitly summed four-rank staffing, with reference date and rank coverage |
| `judicial_strength_by_rank` | Separate Balochistan rank tables for 2023–2025; not mistaken for consolidated staffing |
| `sessions_crosswalk` | Year-specific reporting unit, candidate map key, eligibility, review status and explanation |
| `hosted_districts` | Map districts with no court seat in a year, the host division that carries their cases, and the population added to the host denominator |
| `district_indicators` | Eligible map unit × year × category, after complete geography aggregation |
| `district_policy_indicators` | One all-cases observation per eligible map unit and year; default map/scatterplot feed |
| `category_sessions` | Year × province × sessions unit × case category (family, narcotics, murder, bail, rent, civil/criminal appeals and revisions, seven-years-plus, hudood, miscellaneous) from `categories.py`; `curated_overlap` marks the all/civil/criminal rows kept only for cross-checking |
| `district_category_indicators` | Category rows keyed to map units with per-100k rates and `share_of_all_pending_pct`; the category map/year-slider feed |
| `category_crosscheck` | Automatic reader versus curated manual specifications on the all/civil/criminal tables both read |
| `coverage` | Explicit annual coverage and gaps |
| `national_overview` | Each edition's front-of-report table: Supreme Court, Federal Shariat Court, five High Courts, five district judiciaries and printed totals (2020, 2022–2025; 2021 is scanned). 2025 Supreme Court row carries `transferred_to_fcc` and the chapter table 1.5 |
| `overview_validation` | Overview court rows summed against the overview's printed totals |
| `overview_reconciliation` | Overview district rows against the chapter-derived `annual_provinces` totals |
| `table_validation` | Detail-row sums versus printed totals, with missingness and discrepancy status |
| `category_partition_checks` | All minus civil minus criminal; detects changing/non-exhaustive category coverage |
| `halfyear_partition_checks` | The same check for the separate half-year tables |
| `continuity_checks` | Prior year's closing stock versus current year's opening stock; never used to overwrite either value |
| `geography_issues` | Incomplete multi-unit groups excluded from mapping |

`quality_report.json`, `table_specs.json`, and the generated warehouse `README.md` describe the release. `dataset.json` registers this candidate as Tier 1. `sources.json` is the source/version catalogue.

## Variables and formulas

Case counts are **cases**, not persons, crimes, convictions, or individual court events. Case institution is a flow during the period; pendency is a stock at the specified endpoint. Transfers are preserved independently.

| Field | Meaning |
|---|---|
| `pending_start`, `pending_end` | Reported pending cases at period start/end |
| `instituted`, `disposed` | Cases instituted/disposed during the stated period |
| `transfers_in`, `transfers_out` | Reported case transfers; null when absent or marked missing |
| `clearance_rate_pct` | `disposed / instituted × 100`; null for a zero/missing institution denominator |
| `backlog_change` | `pending_end − pending_start` |
| `backlog_growth_pct` | `backlog_change / pending_start × 100`; null at zero/missing opening stock |
| `stock_flow_residual` | `end − (start + instituted + transfers_in − transfers_out − disposed)`; only calculated with all six reported values |
| `unadjusted_flow_residual` | `end − start − instituted + disposed`; an unexplained difference can include transfers or adjustments |
| `sanctioned_judges` | Reported authorized posts, with reference date/rank coverage retained |
| `working_judges` | Field male + female where distinguished; 2020 reports sometimes supply only a working total |
| `pending_per_100k_population` | `pending_end / 2023 census population × 100,000` |
| `population_interpolated`, `pending_per_100k_interpolated`, `working_judges_per_million_interpolated` | Same rates on a year-end population interpolated geometrically between the 2017 (15 Mar) and 2023 (1 Mar) census counts of the unit's `population_keys`; 2024 is a ten-month extrapolation. Units whose implied growth falls outside 0–6 %/yr (boundary changes: Karachi West/Keamari, Awaran, Kachhi, Kharan, Orakzai, Panjgur, Washuk) keep the fixed 2023 count; `population_interpolation_basis` says which |
| `share_of_all_pending_pct` | Category `pending_end` / all-cases `pending_end` of the same map unit and year × 100 (category feed only) |
| `working_judges_per_million` | `working_judges / 2023 census population × 1,000,000` |
| `pending_per_working_judge` | All-cases year-end pendency / matched working judges |
| `instituted_per_working_judge` | All-cases annual institution / matched working judges |

Population comes from `datadarbar/app/data/districts.json`, specifically `t1_2023_pop_total`; its hash is saved in the quality report. Rates use a **fixed 2023 census benchmark**, not interpolated annual populations. They should be labelled accordingly. Staffing and population values repeated across category rows must not be summed across those categories. The policy dataset removes this duplication by selecting all cases once.

## Geography safeguards

The source's sessions division is the preserved primary unit. Normalization resolves spelling variants and abbreviations, not judicial boundaries. Ordinary district-name matches are explicitly **provisional**.

- ICT East and West must both be present and are summed to one Islamabad observation.
- Lower/Upper Chitral and the three Kohistan units are summed to the corresponding existing map/census units. Incomplete groups receive no rate.
- Balochistan town seats are keyed to the map district containing them (`SEAT_TO_DISTRICT` in `build.py`: Dalbandin→Chaghi, Turbat→Kech, Dhadar→Kachhi, Gandawah→Jhal Magsi, Dera Murad Jamali→Nasirabad, Basima→Washuk). Districts reported through two seats in a year are summed (Quetta+Sariab; Lasbela = Uthal+Hub; Jaffarabad = Dera Allah Yar+Usta Muhammad; Kalat+Surab; Killa Abdullah+Chaman); a single reported seat is the whole district for that year. The 2021–22 Balochistan splits (Chaman, Usta Muhammad, Surab) fold into their parent map units because the map geometry predates them.
- Map districts with no court seat of their own in a year are **hosted**: Korangi in Karachi East and Keamari in Karachi West (all years); Sujawal in Thatta before a separate Sujawal unit appears (2023); Sherani in Zhob; Sohbatpur in Jaffarabad; Washuk in Kharan until Basima reports (2022); Harnai in Sibi (2020 only). The hosted district receives no observation and its 2023 population is added to the host's denominator for that year (`population_keys`, `hosted_districts`, and the `hosted_districts.*` dataset). The rule is dynamic: once the district reports its own unit the host denominator shrinks back.
- Sindh staffing tables from 2022 name the Tharparkar division by its seat, Mithi; it is aliased to `tharparkar` (added 2026-10, so Tharparkar staffing now matches in 2022–2025).
- KP staffing tables spell the third Kohistan unit "Kolai Pallas Kohistan" (2022, 2023, 2025); it is aliased to `kolai pallas` so the Kohistan group is complete (added 2026-10 with the 2025 edition; this also completed Kohistan's 2020–2024 category groups and 2022–2023 staffing match).
- Punjab's 36 sessions divisions match the 36-district map geometry one-to-one; the 2022–23 splits (Talagang, Murree, Wazirabad, Taunsa, Kot Addu) are not map units. South Waziristan is one reported division on the undivided agency polygon.

The current release has 646 eligible map-unit/year observations (126 map units in 2024; 61 in 2025, when only Sindh, KP and ICT print district tables) and no withheld correspondences. Repeated crosswalk rows are intentional: the mapping must be reviewed for each judicial year. No counts are split across polygons or duplicated across their populations; every district population enters at most one denominator per year (tested).

Hosting assignments are jurisdictional judgements, not read from the reports: they rest on the seat-less district having been carved from the host and on the absence of any separately reported unit. If a district in `HOSTED_DISTRICTS` turns out to file elsewhere, edit the dictionary and rebuild.

## Edition and extraction decisions

Table specifications use **one-based PDF pages**, rather than ToC/printed page numbers, and zero-based table indices after removing narrow spurious fragments. The selected 2024 PDF is the archived, dated replacement `6880(25)Judicial Statistics 2024 (22-9-25).pdf` (281 pages). The user's original-name PDF (275 pages) remains archived separately; its values are not pooled with the replacement.

The 2023 archived filename begins `F1669(24)`. The 2022 source is `rep2.pdf`; some running headers incorrectly mention 2021, so the edition identity follows the report's dated tables and cover rather than those copied headers.

Key source qualifications:

- Punjab 2020–2023 splits total cases across civil and sessions courts. These tiers are added once for all cases. The 2020 civil/criminal breakdown is available for civil courts only and retains that narrower tier.
- Punjab 2022 table 3.24 reverses the Disposal/Transferred header labels relative to the numeric value order and the parallel sessions tables. The extractor uses the demonstrated numeric order and preserves the original cells and note.
- The 2020 Balochistan printed criminal table repeats the all-cases table. Curated criminal counts are derived as all minus civil, consistent with the report's exhaustive provincial category partition. The source duplication is retained. The same subtraction supplies 2024's missing separate criminal district table.
- Civil/criminal tables can omit miscellaneous matters, appeals or other categories; they are not forced to sum to the all-cases table. Use all cases for the default year slider.
- Punjab staffing in the 2022 edition is labelled 31 December 2021. Its date conflict is retained and staffing is not joined to 2022 case indicators.
- KP district staffing was not found in the selected 2024 chapter, nor in the alternate 2024 edition (both print only High Court staff for KP); the 2025 edition prints it. Balochistan 2023–2025 print rank tables only; `judicial_strength` carries a consolidated Balochistan row per map district that sums the four standard ranks (`rank_coverage = sum_of_four_reported_ranks`, `BALOCHISTAN_STAFF_UNITS` maps seat names such as "Chagai at Dalbandin", "Kuchlak", "Lasbella at Uthal"), with Majlis-e-Shoora members and Qazis carried separately as `other_judicial_officers_*`; the 2020 and 2022 printed Balochistan rows are summed to the same units. The rank tables themselves stay in `judicial_strength_by_rank`. Missing staffing is not borrowed from another year. 2021 staffing tables were not OCR'd (only the case tables were), so 2021 has no staffing anywhere.

## 2025 edition

The selected 2025 PDF is `809 (26) LJCP Final Judicial Statistics 2025 (11-8-26).pdf` (286 pages, sha256 `d7689ba5…`). All its district tables are four-column (no transfer columns), so stock-flow residuals are not computed for 2025.

- **Punjab** prints no district all/civil/criminal tables. `annual_provinces` carries provincial totals summed from the printed totals of the court-tier category tables 3.12 (sessions courts: criminal 129,163 → 113,573; civil 71,654 → 73,165) and 3.13 (civil courts: civil 825,192 → 814,207; criminal 472,662 → 441,668), `method = sum_court_tier_category_totals`. Opening pendency (1,498,671) equals the 2024 edition's closing. 3.13 civil rows sum 70 above its printed opening and 517 below its printed disposals (retained in `table_validation`). District category tables (murder, narcotics, bail, rent, family) are read; narcotics, bail, rent and family are printed per court tier and summed once, so they are broader than 2024's single tables. 3.19 (rent) names no tier; its total equals the sessions "(Rent) Appeals" row, so it is keyed to sessions courts (`TIER_OVERRIDES`).
- **Sindh** 4.17–4.19 (28 units) and **KP** 5.13, 5.15 and the unnumbered civil table (35 units) reconcile exactly to their printed totals. KP 5.14 reprints 5.13 and is skipped as an identical reprint. KP's civil total balance (157,821) is 45 below the KP province category table 5.12 (157,866).
- **ICT** 7.14 stacks all/civil/criminal/murder/narcotics/bail/rent/family blocks in one grid with a merged label column; each block is read separately (`skip_cols`, `STACKED`). The civil block's printed opening total (25,911) is 60 above its two rows, which sum to 2024's closing (25,851).
- **Balochistan**'s district chapter prints only rank staffing tables (6.17–6.22). Its 2025 provincial total exists only in the national overview (18,685 → 18,424), used in `annual_provinces` with `method = overview_table_only`. 6.18 (additional district & sessions judges) rows sum to 51 sanctioned against a printed 54 (three units blank); 6.22's total row repeats 6.21's.
- **Staffing**: Punjab 3.11.1, Sindh 4.15, KP (first edition with district staffing; "South Waziristan Lower" is printed with 4 sanctioned posts and blank working cells and is kept as an unmatched row), Balochistan rank tables, and ICT East/West working strength read from the notes inside 7.12's merged consolidated cells (West 29 male + 8 female, East 21 + 7; reconciled to the printed 59 and 17 with the special-court postings) by `transcribed_ict_2025`.
- **National overview and the Federal Constitutional Court.** The overview (p. 8) prints superior courts 406,379 → 317,999, district judiciary 1,964,848 → 1,904,043, grand total 2,371,227 → 2,222,042, institutions 5,300,597 and disposals 5,509,977. Its district rows sum to 5,079,843 institutions and 5,219,364 disposals, 35,903 and 34,200 above the printed district subtotal (`overview_validation`). The overview's district rows also differ from the district chapters (as in 2024; 2020 and 2023 match exactly) — see `overview_reconciliation`. Under the 27th Amendment the Supreme Court transferred **21,570** pending cases to the new Federal Constitutional Court in 2025 (table 1.5: 14,660 petitions, 3,985 appeals, 2,925 other). The FCC is not in the report, so the 2025 Supreme Court balance (33,787 in the overview; 34,904 in table 1.5, whose own rows balance exactly) and the superior-court total are a series break: about 21,600 of the fall is a transfer out of scope, not disposals. `national_overview` carries the transfer in `transferred_to_fcc` and flags `series_break`; it is never added to disposals.

## Case-category tables

`categories.py` reads every "District-wise …" table in the district-judiciary chapter of each text-layer edition (2020, 2022–2025; 2021 is scanned). Tables are located by their printed headings (with a fallback for numbered titles whose first line is missing from the text layer), classified by keyword, and read with `extract.read_table`; continuation pages are followed until a Total row. Six-column tables have two printed orders — transfers-out before disposal, or (older Punjab layout) received/disposal/transfer — which the stock-flow identity cannot separate, so the order is taken from the printed header when explicit and otherwise from column magnitudes; `column_order_evidence` records the decision on every row. Four-column (Balochistan) and five-column tables are read as printed.

Two printed titles are overridden with recorded evidence (`OVERRIDES`): Sindh 2024 table 4.28 repeats the 4.27 title but is the civil revisions table (every district's opening pendency equals the 2023 civil revisions closing pendency), and KP 2024 table 5.22's first title line is missing (it is the criminal appeals table). Three ICT 2022 tables (bail, rent, family) are not read because their narrow grid merges two numbers per cell. Punjab prints several categories per court tier; a category reported in both tiers is summed once, and same-tier duplicates (ICT 2020 "miscellaneous") are kept apart by section number. Categories are edition-specific: family and bail appear from 2022, hudood only to 2022, and a few tables list fewer districts (e.g. KP rent 2024: 29 of 35) — absent districts are absent, not zero.

Because the reader also reads the all/civil/criminal tables, `category_crosscheck` compares it with the curated manual specifications: 1,606 rows match exactly (all 195 of 2025's) and the 25 that differ are the 2020 Balochistan printed criminal table that repeats the all-cases table. Map keying, seat folding and hosted-district denominators are the same code path as the policy feed.
- ICT's sanctioned/vacant cells span multiple institutions; the extractor does not assign the same sanctioned total to both divisions. Their specific field working counts can be combined.
- Blank gender cells stay null unless known values reconcile exactly to a printed column total. Any resulting zero inference is logged in `inferred_zero_fields`. Other blanks and dashes are not universally converted to zero.

The release retains 175 annual stock-flow residuals. Nine comparable table-sum discrepancies remain: KP 2021 incoming/outgoing transfers are each nine greater in district rows than the printed totals; Balochistan 2024 additional district/sessions judge sanctioned posts sum to 53 versus a printed 52; and six in 2025 (ICT civil opening total; Punjab 3.13 civil opening and disposals; Balochistan Majlis-e-Shoora sanctioned; Qazi sanctioned and vacant). Their reported values are preserved. Half-year reports also contain inconsistencies, including ICT's 2024 H2 all-cases total versus its civil/criminal split, and the 2025 heading's impossible “31 June” date (normalized to June 30 from the Jan–Jun title).

## Scanned 2021 report

The 216-page 2021 report is scanned. Its checksum-pinned reviewed table transcription is versioned in `ocr_2021_reviewed_tables.json`, and explicit visual cell corrections are in `ocr_reviewed_cells.json`. Routine extraction verifies the original PDF hash before using that transcription. The large original images and OCR evidence remain in the raw archive.

To independently repeat OCR on macOS, install the optional preparation tools and put Poppler's `pdftoppm` on PATH:

```sh
python datadarbar/etl/ljcp/ocr_tables.py prepare
swiftc -module-cache-path /tmp/ljcp-swift-cache datadarbar/etl/ljcp/ocr.swift -o /tmp/ljcp-ocr
/tmp/ljcp-ocr raw_data/ljcp/ocr/annual_2021
python datadarbar/etl/ljcp/ocr_tables.py reconstruct
python datadarbar/etl/ljcp/ocr_tables.py prepare-cells
/tmp/ljcp-ocr raw_data/ljcp/ocr/annual_2021
```

`reviewed_tables(raw, from_cache=True)` reconstructs the reviewed transcription from that evidence and the explicit correction log. Inspect missing/low-confidence cells against the original image and reconcile totals before changing the versioned snapshot. OCR numbers are never adjusted merely to make an equation balance.

## Validation and extensions

Eighteen release tests cover the 2025 edition's printed totals (chapter, Punjab court-tier and overview, and the Federal Constitutional Court transfer), the category reader's agreement with the curated series (and a verified PDF cell, and the 4.28 continuity evidence), Balochistan rank sums, interpolated-population bounds, and the earlier checks: known PDF cells, merged-cell recovery, reviewed OCR, retained source discrepancies, court-tier aggregation, the duplicated criminal table, population aggregation, Balochistan seat folding, hosted-district denominators (and that no population is counted twice in a year), period separation, date conflicts, unique keys/provenance, export row counts and tampered caches. The current suite passes.

For a new edition: add its original/archive URLs and checksum to the catalogue, inspect the actual district and staffing tables, add explicit table specifications, review unit names and boundary changes, run extraction/validation, and document new coverage or scope differences. Source downloads and raw archive indexes do not establish district coverage by themselves. Keep annual and half-year datasets separate.

The judges-versus-pendency cross-section is useful for describing workload and staffing. It is not a causal estimate of the effect of appointing judges: case mix, incoming workload, jurisdiction size and reporting practices also differ.
