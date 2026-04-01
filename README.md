# Data Darbar — Pakistan Census Explorer

Interactive district-level map of Pakistan, visualising indicators from the 2017 and 2023 Population & Housing Censuses, the PSLM 2019-20, the Economic Census 2023, the Labour Force Survey, and the HIES 2024-25 — all published by the [Pakistan Bureau of Statistics](https://www.pbs.gov.pk/).

## Live demo

<https://hibasameen.github.io/datadarbar/>

## Features

- **12 indicator groups** covering demographics, urban/rural splits, literacy, education attainment, employment status, school attendance, PSLM welfare indicators, Economic Census, Labour Force Survey, and HIES household income/expenditure
- **2017 vs 2023 comparison** with a "Change" toggle showing inter-censal differences
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
│   └── build_dataset.py
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

## Data sources

| Source | Years | Coverage |
|--------|-------|----------|
| [Population Census](https://www.pbs.gov.pk/content/population-census) | 2017, 2023 | Demographics, literacy, education, employment |
| [PSLM](https://www.pbs.gov.pk/) | 2019-20 | Water, sanitation, ICT, work participation |
| [Economic Census](https://www.pbs.gov.pk/) | 2023 | Establishments, workforce by sector |
| [Labour Force Survey](https://www.pbs.gov.pk/) | 2020-21, 2024-25 | LFPR, unemployment, industry |
| [HIES](https://www.pbs.gov.pk/) | 2024-25 | Household income & expenditure |
| District boundaries | — | GeoJSON from PBS / geoBoundaries |

## License

Data is from the Pakistan Bureau of Statistics (public domain). Code is MIT.
