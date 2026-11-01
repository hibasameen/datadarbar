# Agricultural Census 2024 (7th, "Integrated Digital Count") — district tables

Source: PBS, https://www.pbs.gov.pk/agriculture-census/ — Excel + PDF tables for National, Punjab, Sindh,
KPK, Balochistan (71 tables each; national 73 incl. Table 1.0), five reports, pulled 2026-11-01 via Chrome
(File System Access API; pbs.gov.pk is blocked from the sandbox and the Mac shell). Raw files and
`manifest.json` in `raw_data/pbs/agricultural_census_2024/`. Fieldwork Sep 2024 – Feb 2025.

```
python3 parse_tables.py            # 357 xlsx -> derived/agri_census_2024_long.csv.gz (992k cells) + tables_catalog.csv
python3 build_district_headline.py # -> derived/agri_census_2024_district_headline.csv (172 units × ~55 indicators)
```

## What to know

- **Values are weighted estimates, not counts** — PBS publishes non-integer figures (Punjab 5,050,235.79
  farms). The census enumerated a sample of mouzas/blocks (11,083 units) and is "representative at district
  level". Treat district figures as estimates with unpublished sampling error.
- **Geography = the PBS Census-2023 district frame**, the same `pbs_district` names as
  `etl/geography/district_crosswalk_cod_2023.csv`: 134 districts match 1:1 with no renaming. Extra units:
  CHOLISTAN AREA (inside Bahawalpur division, not in any district's figures), MALAKAND PROTECTED AREA (KP),
  ISLAMABAD CAPITAL TERRITORY (a block at the end of the Punjab file). GB and AJK were enumerated but are in
  none of the published tables; PAKISTAN = four provinces + ICT.
- **Totals reconcile exactly**: districts + Cholistan = Punjab; districts + MPA = KP; Sindh and Balochistan
  districts = province; Pakistan 11,103,697 farms / 59,300,696 ac / 52,787,664 ac cultivated; cattle
  55,862,560, buffaloes 47,738,368, sheep 44,584,926, goats 95,827,177, tractors 986,998 — all equal to the
  country report's "At a Glance".
- **Rows with zero are omitted** by PBS (e.g. no tenant farms in Gwadar, no camels in Swat), so a missing
  (district, table) is a zero, not a gap. Exception: WASHUK is missing from Balochistan Table 1.1 (acres)
  only; the headline builder fills farms/area from Table 1.2 (hectares × 2.47105), flagged `filled_from_T1_2`.
- **Source anomalies to carry into any map note** (parse verified against the raw sheets):
  - Umerkot (Sindh): 2,098 farms on 209,920 ac (avg 100 ac), 198,743 cattle, 1,435 tractors — the farm
    count is implausibly low for the district (2010 census: tens of thousands); area and livestock look
    normal. Likely a weighting/coverage problem in the source; do not rank on farms or farm size without a flag.
    Other very small farm counts are the Karachi districts and Hyderabad (urban), and Awaran/Duki/Chagai
    (very large average holdings, Balochistan).
  - **Crop areas are far above the Crop Reporting Service series** on the site (`crops_district_fy_long`
    from na.data.gov.pk): census wheat 14.4 mn ha (Punjab 8.4, KP 2.1, Balochistan 1.8) vs CRS 2024-25
    9.06 mn ha; cotton 2.57 vs 2.04; paddy 4.33 vs 3.90. The two are different instruments (respondent-
    reported farm area under crop vs. CRS plot estimates) — present census crop figures as farm-reported
    shares (farms reporting wheat, wheat share of cropped area), not as production-series substitutes.
  - Header typos in the source are normalised in the headline builder only (`TANENT`, `HOPUSEHOLD`,
    `FREGMENT`); the long table keeps PBS's spellings. Five tables have extra columns in some provinces
    (1.3 Balochistan, 7.10/8.15 Punjab, 8.14, 8.16) — match on header text, never on column position.
- Layouts: `block` (unit header row, then farm-size / herd-size / tenure classes) and `flat` (one row per
  unit: 7.1 livestock, 8.x machinery). `group` carries nested sub-headers (e.g. OWNER FARMS). Table 8.16
  stacks "Owned/Rented" in one cell and is split into two category rows.
- Comparison point: Agricultural Census 2010 — most of PBS's 2010 links are dead (404); 6 PDFs saved in
  `raw_data/pbs/agricultural_census_2010/`. District tables for 2010 would need another source.

## Headline indicators (district rows)
farms, farm_area_ac, cultivated_area_ac, avg_farm_size_ac, share_farms_under_2_5ac, share_area_25ac_plus,
share_farms_owner / tenant / female / fragmented, share_cultivated, irrigated_area_ac + share_irrigated,
share_irrig_tubewell_only, cropping_intensity_pct, cropped/kharif/rabi/orchard area, wheat/paddy/cotton/
maize/sugarcane area, tunnel_farms, cattle/buffaloes/sheep/goats/camels/asses, cattle_buffalo_per_farm,
tractors (+ per 1,000 farms, cultivated ac per tractor), tubewells by power (electric/solar/diesel) +
share_tubewells_solar. Column-to-source mapping in `derived/headline_column_matches.csv`.

Not yet done: warehouse registration (`build_web_warehouse.py`), map layer under Places, and the
per-crop / per-size-class long views. Population denominators for per-capita rates: Census 2023.
