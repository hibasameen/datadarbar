# Data Darbar — Pakistan in Numbers

An open explorer of Pakistan's official statistics — the population census, 8-digit external trade, the national accounts, and the federal budget — turned into interactive maps and Atlas-style visualisations, now with poverty and satellite measures down to the tehsil. Built on data published by the [Pakistan Bureau of Statistics](https://www.pbs.gov.pk/), the Finance Division, and the [National Institute of Population Studies](https://www.nips.org.pk/viewpublicdata).

## Live demo

<https://hibasameen.github.io/datadarbar/>

## The four products

Data Darbar is a landing page (`index.html`) plus four self-contained products, sharing one header (with a **Data ▾** dropdown), footer, and green/gold/slate design system.

**District Map** (`map.html`) — a Leaflet choropleth of Pakistan across all 141 districts, with 17 indicator groups covering demographics, urban/rural splits, literacy, education attainment, employment, school attendance, PSLM welfare indicators, the Economic Census, the Labour Force Survey, HIES household income and expenditure, and PDHS family planning, fertility, maternal health, child immunisation and nutrition. Includes a 2017-vs-2023 census "Change" toggle, a sample-size confidence UI, province filter, district search, and CSV export.

**Trade Atlas** (`trade.html`) — every 8-digit product Pakistan buys and sells, rendered as a nested treemap grouped and coloured by HS section (~1,050 tiles). Drill from a sector down to individual commodities, track trade over time (2015–2024), see top partners and products, a "what's growing" movers view with year-on-year timelines, and per-country breakdowns. Every chart exports to CSV and its state is shareable via URL hash.

**GDP & Budget** (`finance.html`) — an interactive "structure of the economy" dashboard: sector-share of GDP from 1951-52 to today (with a pre-1999 backcast), contributions to real growth, the Large-Scale Manufacturing and Quantum indices, the CMI manufacturing censuses, a 12-sector input-output flow (as a heatmap, focus view, or chord), and the federal budget — receipts and current expenditure broken down as a treemap or trend. Shareable chart states and CSV export throughout.

**Macro & Finance** (`money.html`) — State Bank of Pakistan series, some running back to 1947: the exchange rate and effective exchange rate indices, inflation and its components, the policy rate and the KIBOR curve, lending and deposit rates, reserves and import cover, the current account as a two-sided treemap with click-through breakdowns (goods to individual commodities, services by type, remittances by source, income paid by sector), the money supply and non-performing loans. Built by `etl/build_money.py` from the SBP EasyData warehouse.

**Poverty & Wealth** (`poverty.html`) — a multidimensional poverty index built from PSLM household microdata, plus satellite measures of relative wealth, population, and night-time lights. The map is mixed-geometry: the MPI is district-level (ADM2), while relative wealth, population, and night-lights are tehsil-level (ADM3), and the "Measure" selector switches the geometry automatically. Night-lights carry a year slider (June 2020–2026) and a growth view.

## Project structure

```
datadarbar/
├── .github/workflows/       ← GitHub Actions deployment
│   └── deploy.yml
├── app/                     ← static site (deployed to GitHub Pages)
│   ├── index.html           ← landing (four product cards)
│   ├── map.html             ← District Map
│   ├── trade.html           ← Trade Atlas
│   ├── finance.html         ← GDP & Budget
│   ├── money.html           ← Macro & Finance
│   ├── poverty.html         ← Poverty & Wealth
│   ├── economy.html         ← redirect → finance.html
│   ├── assets/
│   │   ├── css/styles.css   ← shared app-shell CSS (map + poverty)
│   │   ├── js/
│   │   │   ├── app.js       ← District Map
│   │   │   ├── trade.js     ← Trade Atlas
│   │   │   ├── finance.js   ← GDP & Budget
│   │   │   ├── money.js     ← Macro & Finance
│   │   │   ├── poverty.js   ← Poverty & Wealth
│   │   │   ├── nav.js       ← shared "Data" nav dropdown
│   │   │   ├── modals.js    ← About / Methodology / Contact
│   │   │   ├── econ_data.js ← trade + economy data (window.ECON)
│   │   │   └── d3.v7.min.js ← vendored D3
│   │   └── img/             ← logo, favicons, per-page social cards
│   └── data/
│       ├── districts.json               ← District Map indicators
│       ├── census_data.js               ← inlined map data (file:// safe)
│       ├── poverty_data.js              ← MPI / RWI / pop / night-lights + geometry
│       ├── econ_industry.json           ← QIM / LSM / CMI
│       ├── econ_structure.json          ← long-arc sector series
│       ├── econ_trade_extra.json        ← country detail + movers
│       ├── budget_receipts_detailed.json
│       ├── budget_expenditure_detailed.json
│       └── pakistan_districts_province_boundries.geojson
├── etl/                     ← Python data pipeline
│   ├── build_dataset.py     ← builds districts.json (census, PSLM, LFS, HIES, EconCensus)
│   ├── dhs_district.py      ← PDHS 2017-18 district indicators (NIPS microdata)
│   ├── build_industry.py    ← QIM / LSM / CMI → econ_industry.json
│   ├── build_structure.py   ← long-arc growth/shares/backcast → econ_structure.json
│   ├── parse_pdfs.py, scrape_pbs.py
│   └── README_industry.md   ← industry/structure build notes
├── LICENSE, SECURITY.md
└── README.md
```

Note: the economy/trade/poverty JSON is published in the repo, but some of the generators that produce it (trade parsing, the detailed budget-from-PDF parser, the input-output builder, and the poverty-data bundler) run against a local DuckDB/Parquet warehouse of raw PBS downloads that is not committed. The `etl/` scripts that only need public sources — `build_dataset.py`, `dhs_district.py`, `build_industry.py`, `build_structure.py` — are included for reproducibility.

## Run locally

```bash
cd app
python3 -m http.server 8000
# open http://localhost:8000
```

The pages also open directly over `file://`: data is inlined as `window.DD_*` / `window.ECON` globals (with `fetch()` as an HTTP fallback) precisely so no server is required.

## Deployment

The site deploys automatically to GitHub Pages via GitHub Actions on every push to `main`; the workflow publishes the `app/` folder as the site root (so live URLs drop the `/app/` prefix — e.g. `/datadarbar/finance.html`).

To set up Pages for the first time: push the repo, go to **Settings → Pages**, and set the source to **GitHub Actions**. The site will be live at `https://<user>.github.io/datadarbar/`.

## Rebuild the data

```bash
cd etl
python3 build_dataset.py      # → app/data/districts.json (District Map)
python3 build_industry.py     # → app/data/econ_industry.json (QIM/LSM/CMI)
python3 build_structure.py    # → app/data/econ_structure.json (long-arc series)
```

To regenerate the PDHS layer, download the PDHS 2017-18 STATA microdata from [NIPS](https://www.nips.org.pk/viewpublicdata), extract the recode folders, and run `python etl/dhs_district.py /path/to/DHS_DIR`. See `etl/README_industry.md` for the industry/structure build details.

## Methodology

### District survey estimates (LFS, HIES)

The Labour Force Survey and HIES are designed to be representative at the provincial, not the district, level. To produce district-level estimates, the pipeline applies two adjustments.

**Minimum sample-size filter.** Districts with fewer than 30 survey observations have all derived indicators suppressed (set to null) and flagged with a `low_n` marker; on the map they appear with a distinct dashed border and a tooltip warning. This reflects the standard convention that small samples produce unreliable estimates, particularly for ratios and proportions where a handful of observations can swing values wildly.

**Post-stratification to 2023 census totals.** Survey weights are recalibrated so that weighted district totals align with Census 2023 population counts. For LFS (individual microdata) this is a sex-ratio adjustment; for HIES (household data) a district-level population calibration factor scales all household weights. This corrects for sampling frames that may not reflect post-census population shifts and reduces bias from differential non-response by sex. District-level survey indicators remain approximate — cross-district rankings should be read with caution, and the sample size (n) is shown in tooltips.

### PDHS 2017-18 (health & demographics)

The PDHS layers are computed from NIPS microdata by `etl/dhs_district.py` using the survey's sampling weights, then merged into `districts.json`. Like LFS/HIES the PDHS is representative at the national/provincial/region level, not the district level (~15,000 women across ~130 districts), so district figures are indicative and the same n<30 suppression applies. Each of the five DHS groups has its own denominator (currently-married women; all women; women with a recent birth; children 12–23 months; children under 5), so reliable coverage varies: family planning/fertility ~122 districts, maternal health ~98, nutrition ~47, immunisation ~16. National weighted estimates reproduce published PDHS figures (e.g. contraceptive prevalence 33.7% vs 34.2%; stunting 37.2% vs 37.6%). Gilgit-Baltistan and AJK — excluded from the national weight `v005` — are weighted with the combined weight `sv005`, which also populates 17 GB/AJK districts the PBS census tables leave blank.

### Poverty & satellite measures

The **multidimensional poverty index** is an Alkire-Foster M₀ (headcount H × intensity A) computed from PSLM 2019-20 household microdata across seven censored indicators; it is reported at the district level because the PSLM is only representative at that geography — going finer would be false precision. **Relative wealth** is Meta's Relative Wealth Index, population-weighted to tehsils and re-percentiled within Pakistan; note that the RWI uses night-time lights as one of its inputs, so it is not fully independent of the night-lights layer (stated in the Methodology modal). **Population** is WorldPop 2020 UN-adjusted density and count. **Night-lights** are VIIRS monthly composites (June 2020–2026), lights per km², with a growth view; low-population tehsils (<1 person/km²) are flagged to avoid snow/sand albedo artefacts. Sequential layers use classed quantile breaks and diverging layers clip to the 95th percentile of magnitude, so that outliers like Karachi do not wash the rest of the map out.

### Trade, national accounts & budget

The **Trade Atlas** aggregates PBS 8-digit external-trade records (~1.1M rows, 2015–2024) parsed from the D-10 "by commodities and countries" releases into an HS chapter→section drill-down tree. The **Economy** dashboard is built from the PBS 2015-16-base national accounts: GVA shares and sub-sector detail (with a pre-1999 backcast anchored to official real growth rates, shown hatched as indicative only), contributions to real growth, spliced LSM sector indices, the monthly Quantum Index of Manufacturing, and the CMI manufacturing censuses (the hard observations between which small-scale manufacturing is imputed rather than surveyed — treat the annual SSM series accordingly). The **budget** treemap re-parses the Finance Division's Budget-in-Brief documents; it covers current expenditure (no PSDP/development), expenditure years 2021-22→2026-27 and receipts 2009-10→2026-27.

## Data sources

| Source | Years | Coverage |
|--------|-------|----------|
| [Population Census](https://www.pbs.gov.pk/content/population-census) | 2017, 2023 | Demographics, literacy, education, employment |
| [PSLM](https://www.pbs.gov.pk/) | 2019-20 | Water, sanitation, ICT, work participation; MPI microdata |
| [Economic Census](https://www.pbs.gov.pk/) | 2023 | Establishments, workforce by sector |
| [Labour Force Survey](https://www.pbs.gov.pk/) | 2020-21, 2024-25 | LFPR, unemployment, industry |
| [HIES](https://www.pbs.gov.pk/) | 2024-25 | Household income & expenditure |
| [PDHS](https://www.nips.org.pk/viewpublicdata) (NIPS) | 2017-18 | Family planning, fertility, maternal & child health, nutrition |
| [External Trade Statistics](https://www.pbs.gov.pk/external-trade-statistics/) | 2015–2024 | 8-digit imports & exports by commodity and country |
| [National Accounts](https://www.pbs.gov.pk/) | 1951–2026 | GDP by sector, real growth, input-output, LSM/QIM/CMI |
| [Federal Budget](https://www.finance.gov.pk/) (Finance Division) | 2009–2027 | Receipts and current expenditure |
| Relative Wealth Index (Meta / Data for Good) | 2021 | Tehsil-level relative wealth |
| WorldPop | 2020 | Gridded population, UN-adjusted |
| VIIRS night-time lights (NASA/NOAA) | 2020–2026 | Monthly radiance composites |
| District & tehsil boundaries | — | GeoJSON from PBS / geoBoundaries |

## License

Source data is from the Pakistan Bureau of Statistics and other public agencies. The **code** is licensed under the [MIT License](LICENSE); **derived data** published in this repo is released under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/).
