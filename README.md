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
4. The site will be live at <https://hibasameen.github.io/datadarbar/>

## Rebuild the dataset

If you update source data, regenerate the JSON:

```bash
cd etl
python3 build_dataset.py
```

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
