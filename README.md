# Data Darbar — Pakistan Census Explorer

Interactive district-level map of Pakistan, visualising indicators from the 2017 and 2023 Population & Housing Censuses, the PSLM 2019-20, the Economic Census 2023, the Labour Force Survey, and the HIES 2024-25 (all published by the [Pakistan Bureau of Statistics](https://www.pbs.gov.pk/)), plus health & demographic indicators from the Pakistan Demographic and Health Survey (PDHS) 2017-18, published by the [National Institute of Population Studies (NIPS)](https://www.nips.org.pk/viewpublicdata).

## Live demo

<https://hibasameen.github.io/datadarbar/>

## Features

- **17 indicator groups** covering demographics, urban/rural splits, literacy, education attainment, employment status, school attendance, PSLM welfare indicators, Economic Census, Labour Force Survey, HIES household income/expenditure, and — new — PDHS family planning, fertility, maternal health, child immunisation, and child nutrition
- **2017 vs 2023 comparison** with a "Change" toggle showing inter-censal differences
- **Sample-size aware** — survey-based estimates (including DHS) carry per-district sample sizes and suppress/flag small-n districts on the map
- Province filter, district search, and CSV export
- Responsive layout (desktop + mobile)

## Project structure

```
datadarbar/
├── .github/workflows/    ← GitHub Actions deployment
│   └── deploy.yml
├── app/                  ← static site (deployed to GitHub Pages)
│   ├── index.html
│   ├── assets/
│   │   ├── css/styles.css
│   │   ├── js/app.js
│   │   └── img/logo.svg
│   └── data/
│       ├── districts.json
│       └── pakistan_districts_province_boundries.geojson
├── etl/                  ← Python data pipeline
│   ├── build_dataset.py
│   ├── dhs_district.py   ← PDHS 2017-18 district indicators (NIPS microdata)
│   └── dhs_district_indicators.json  ← generated DHS output (provenance)
├── .gitignore
└── README.md
```

## Run locally

```bash
cd app
python3 -m http.server 8000
# open http://localhost:8000
```

## Deployment

The site deploys automatically to GitHub Pages via GitHub Actions on every push to `main`. The workflow publishes the `app/` folder.

To set up Pages for the first time:

1. Push this repo to GitHub
2. Go to **Settings → Pages**
3. Set source to **GitHub Actions**
4. The site will be live at <https://user.github.io/datadarbar/>

## Rebuild the dataset

If you update source data, regenerate the JSON:

```bash
cd etl
python3 build_dataset.py
```

## Statistical methodology for survey data

The Labour Force Survey (LFS) and Household Integrated Economic Survey (HIES) are designed to be representative at the provincial level, not the district level. To produce district-level estimates from these surveys, the pipeline applies two adjustments:

**Minimum sample-size filter.** Districts with fewer than 30 survey observations have all derived indicators suppressed (set to null) and are flagged with a `low_n` marker. On the map, these districts appear with a distinct gold dashed border and a warning in the tooltip. This threshold reflects the standard convention that small samples produce unreliable estimates — particularly for ratio and proportion indicators where a handful of observations can swing values wildly. In the current data, this affects 6 HIES districts (Dera Bugti, Khuzdar, Mastung, Orakzai Agency, Panjgur, and Ziarat), all in remote areas with limited survey coverage.

**Post-stratification to 2023 census totals.** Survey weights are recalibrated so that weighted district-level totals align with known population counts from Census 2023 (Table 1). For LFS (individual-level microdata), this takes the form of a sex-ratio adjustment: within each district, male and female observations are reweighted so that the weighted sex composition matches the census male/female population shares. For HIES (household-level data), a simpler population calibration factor scales all household weights in a district so that the weighted population total matches the census figure. This corrects for the fact that PBS survey sampling frames may not reflect post-census population shifts across districts, and reduces bias from differential non-response by sex.

These adjustments improve the plausibility of district-level estimates but do not eliminate the fundamental limitation that provincial-level surveys have limited statistical power at finer geographies. Users should interpret district-level survey indicators as approximate and treat cross-district rankings with appropriate caution. The sample size (n) is shown in tooltips for all survey-based indicator groups.

### PDHS 2017-18 (health & demographics)

The PDHS 2017-18 layers are computed from NIPS microdata by `etl/dhs_district.py` using the survey's sampling weights, then merged into `districts.json`. Like LFS/HIES, the PDHS is representative at the national/provincial/region level, **not** the district level (~15,000 women across ~130 districts), so district figures are indicative and the same n<30 suppression applies. Each of the five DHS groups has its own denominator (currently married women; all women; women with a recent birth; children 12–23 months; children under 5), so reliable coverage varies: family planning/fertility ~122 districts, maternal health ~98, nutrition ~47, immunisation ~16. National weighted estimates reproduce published PDHS figures (e.g. contraceptive prevalence 33.7% vs 34.2%, stunting 37.2% vs 37.6%). Gilgit-Baltistan and AJK — excluded from the national weight `v005` — are weighted with the combined weight `sv005`, which also populates 17 GB/AJK districts that the PBS census tables leave blank.

To regenerate the DHS layer, download the PDHS 2017-18 STATA microdata from [NIPS](https://www.nips.org.pk/viewpublicdata), extract the recode folders, and run `python etl/dhs_district.py /path/to/DHS_DIR`.

## Data sources

| Source | Years | Coverage |
|--------|-------|----------|
| [Population Census](https://www.pbs.gov.pk/content/population-census) | 2017, 2023 | Demographics, literacy, education, employment |
| [PSLM](https://www.pbs.gov.pk/) | 2019-20 | Water, sanitation, ICT, work participation |
| [Economic Census](https://www.pbs.gov.pk/) | 2023 | Establishments, workforce by sector |
| [Labour Force Survey](https://www.pbs.gov.pk/) | 2020-21, 2024-25 | LFPR, unemployment, industry |
| [HIES](https://www.pbs.gov.pk/) | 2024-25 | Household income & expenditure |
| [PDHS](https://www.nips.org.pk/viewpublicdata) (NIPS) | 2017-18 | Family planning, fertility, maternal & child health, child nutrition |
| District boundaries | — | GeoJSON from PBS / geoBoundaries |

## License

Data is from the Pakistan Bureau of Statistics (public domain). This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
