"""The Issue 4 tables: what each is, where it comes from, and on what terms.

Adaad's November 2026 issue (climate, energy and resources) pulled these into
Adaad/data/pk-*/, each with its own README-adaad.md. They are published here
from that folder as Adaad left them, with one addition for every district
table: PBS's district_code, from district_crosswalk_cod_2023, beside the COD
frame's own key (renamed cod_district_key so the catalogue never offers it as
Data Darbar's district_key).

One list, read by three things: pipeline.py (which writes the Parquet into the
desktop warehouse), register.py (which publishes it) and catalog_meta.py (which
files, licenses and sources it). A table is added here or not at all.

  src       path under Adaad/data
  district  True where rows are COD-frame districts: district_code is added
  codes     PBS districts the table must reach, if not all 156
  sql       optional SELECT over the CSV, as {src}; the default takes it whole
  kind, licences, licence_note, provenance  as catalog_meta defines them
  title     the dataset page's title (build_seo DATASET_NAMES)
"""

# ── shared column descriptions ──────────────────────────────────────────────
DISTRICT = {
    'district_code': 'PBS 2023 district code, from district_crosswalk_cod_2023; several '
                     'rows share one where COD draws a Gilgit-Baltistan district PBS '
                     'does not (sum counts across them, weight means by area)',
    'cod_district_key': 'the district’s key on the 160-district COD frame, an '
                        'upper-case name; not Data Darbar’s district_key',
    'cod_adm2_pcode': 'COD-AB district p-code; blank for Keamari and Surab',
    'province': 'province or area, as the COD frame names it',
    'year': 'calendar year',
}
FY = {'fy': 'fiscal year, July to June, e.g. 2024-25'}
DISCO = {'disco': 'distribution company, K-Electric spelled as the State page spells it; '
                  'other names as NEPRA prints them, TOTAL rows being NEPRA’s own totals'}

ADAAD = 'Adaad, Issue 4 (November 2026)'

TABLES = [
    # ── climate ──────────────────────────────────────────────────────────────
    dict(name='climate_district_annual', src='pk-climate/annual-dayweighted-2026-10-18/climate_district_annual.csv',
         district=True, kind='places', licences=['cc-by-4.0', 'public-domain'],
         licence_note='Temperature: ERA5-Land, Copernicus C3S, via Open-Meteo (CC BY 4.0). '
                      'Rainfall: CHIRPS, UCSB Climate Hazards Center (public domain).',
         provenance=('derived', 'District means of gridded reanalysis and satellite '
                     'rainfall, computed by Adaad.'),
         title='Pakistan district temperature and rainfall by year since 1950',
         desc='Annual mean temperature (and mean daily maximum and minimum) and rainfall '
              'for 160 districts, 1950 to 2025, with 1981–2010 normals and anomalies.',
         notes='Temperature is ERA5-Land averaged over every 0.1° cell in the district; '
               'rainfall is CHIRPS from 1981 (a true polygon mean) with ERA5 as a cross-check. '
               'The editor’s rule: rainfall from CHIRPS, temperature from ERA5-Land. Annual '
               'and June–September means weight each month by its days. Karachi South and '
               'Korangi use a 0.2° ring because the 0.1° grid is mostly sea. A year is '
               'kept only when every day is present, so 2026 is absent. District means of a '
               'reanalysis, not station readings; trends are safest from 1981.',
         source='ERA5-Land (Copernicus C3S) via Open-Meteo; CHIRPS (UCSB CHC); district means by ' + ADAAD,
         unit='°C; mm',
         cols={**DISTRICT, 'tmean_c': 'annual mean temperature, °C',
               'tmax_mean_c': 'mean daily maximum, °C', 'tmin_mean_c': 'mean daily minimum, °C',
               'precip_mm_chirps': 'annual rainfall, CHIRPS, mm (from 1981)',
               'precip_mm_era5': 'annual rainfall, ERA5, mm (cross-check)',
               'jjas_precip_mm_chirps': 'June–September rainfall, CHIRPS, mm',
               'tmean_anom_c': 'difference from the 1981–2010 normal, °C',
               'n_points_era5': 'ERA5-Land cells averaged'}),
    dict(name='climate_district_monthly', src='pk-climate/climate_district_monthly.csv',
         district=True, kind='places', licences=['cc-by-4.0', 'public-domain'],
         licence_note='As climate_district_annual.',
         provenance=('derived', 'District means of gridded reanalysis and satellite rainfall, computed by Adaad.'),
         title='Pakistan district temperature and rainfall by month since 1950',
         desc='Monthly temperature and rainfall for 160 districts, January 1950 to September 2026.',
         notes='The monthly series behind climate_district_annual, on the same sources and the '
               'same rule: a month is kept only when every day is present.',
         source='ERA5-Land (Copernicus C3S) via Open-Meteo; CHIRPS (UCSB CHC); district means by ' + ADAAD,
         unit='°C; mm',
         cols={**DISTRICT, 'month': 'calendar month, 1–12', 'tmean_c': 'monthly mean temperature, °C',
               'tmax_mean_c': 'mean daily maximum, °C', 'tmin_mean_c': 'mean daily minimum, °C',
               'precip_mm_chirps': 'rainfall, CHIRPS, mm (from 1981)', 'precip_mm_era5': 'rainfall, ERA5, mm',
               'n_points_era5': 'ERA5-Land cells averaged'}),
    dict(name='heat_district_annual', src='pk-climate/heat-extremes-2026-10-19/heat_district_annual.csv',
         district=True, kind='places', licences=['cc-by-4.0'],
         licence_note='ERA5-Land, Copernicus C3S, via Open-Meteo.',
         provenance=('derived', 'Heat indices computed by Adaad from district-mean daily ERA5-Land.'),
         title='Pakistan district heat extremes by year since 1950',
         desc='Hot days, warm nights, warm spells and the hottest day and night, by district and '
              'year, 1950 to 2025.',
         notes='Computed from the district’s mean daily maximum and minimum, so a district '
               'reading of 40°C is an average over its cells, not a station or a person’s '
               'exposure. Percentile indices (TX90p, TN90p, warm-spell days) use a 1981–2010 '
               'base; use trends from 1981, because the reanalysis is less constrained before 1979. '
               'Days of 40°C are flat since 1981; warm nights and warm spells are not. No '
               'humidity: these are temperature alone.',
         source='ERA5-Land (Copernicus C3S) via Open-Meteo; indices by ' + ADAAD,
         unit='days; °C',
         cols={**DISTRICT, 'days_tmax_ge40': 'days with a district-mean maximum of 40°C or more',
               'days_tmax_ge45': 'days of 45°C or more', 'nights_tmin_ge25': 'nights with a minimum of 25°C or more',
               'nights_tmin_ge30': 'nights of 30°C or more', 'tx90p_days': 'days hotter than the 1981–2010 90th percentile for the date',
               'tn90p_days': 'nights warmer than the 90th percentile', 'wsdi_days': 'warm-spell days (six or more TX90p days in a row)',
               'longest_run_tmax_ge40': 'longest run of 40°C days', 'txx': 'hottest day, °C', 'tnx': 'warmest night, °C',
               'tnn': 'coldest night, °C', 'tmean_annual': 'annual mean, °C',
               'ring_fallback': 'true where a coastal district uses the 0.2° ring'}),
    # ── air ──────────────────────────────────────────────────────────────────
    dict(name='pm25_district_annual', src='pk-air-quality/pak-quality-estimation/pm25_district_annual_2024_2025.csv',
         district=True, kind='places', licences=['cc-by-4.0'],
         licence_note='Pakistan Air Quality Initiative model fields (CC BY 4.0); WorldPop 2020 weights (CC BY 4.0).',
         provenance=('derived', 'District aggregates of a modelled PM2.5 surface, computed by Adaad.'),
         title='Pakistan district PM2.5 from satellites, 2024 and 2025',
         desc='Annual mean fine particulate matter by district, area- and population-weighted, '
              '2024 and 2025.',
         notes='A model (gradient boosting on satellite aerosol, TROPOMI, ERA5 and GEOS-CF) at 0.1°, '
               'mean absolute error 14–23 µg/m³ against 2025 ground monitors. Two years: a map, '
               'not a trend, and not an independent check on the monitors it was trained against. '
               'Population weights are WorldPop 2020.',
         source='Pakistan Air Quality Initiative, pak-quality-estimation; district aggregates by ' + ADAAD,
         unit='µg/m³',
         cols={**DISTRICT, 'pm25_pop_mean': 'population-weighted mean, µg/m³',
               'pm25_area_mean': 'area-weighted mean, µg/m³', 'days_covered': 'days with a model value',
               'share_pop_in_model_mask': 'share of the district’s people inside the model’s mask'}),
    # ── forest, fire, land ───────────────────────────────────────────────────
    dict(name='forest_loss_district_year', src='pk-forest/district-2026-10-17/forest_loss_district_year.csv',
         district=True, kind='places', licences=['cc-by-4.0'],
         licence_note='Hansen/UMD/Google/USGS/NASA tree cover loss; WRI drivers; via Global Forest Watch.',
         provenance=('derived', 'Raster queries on the district polygons, run by Adaad.'),
         title='Pakistan district tree-cover loss by year since 2001',
         desc='Hectares of tree cover lost by district and year, 2001–2025, at 10 and 30 per cent '
              'canopy, with the driver of loss and the share associated with fire.',
         notes='Tree cover is 1 per cent of Pakistan’s land, so these are small numbers: under 200 '
               'hectares a year nationally since 2013 at 30 per cent canopy. Use one threshold at a '
               'time; never add them. Loss is not net change: gain is separate and tree cover 2010 is '
               'a different product.',
         source='Global Forest Watch (Hansen et al., UMD; WRI drivers); district queries by ' + ADAAD,
         unit='hectares; Mg CO2e',
         cols={**DISTRICT, 'threshold': 'canopy threshold, per cent (10 or 30)', 'loss_ha': 'tree cover lost, ha',
               'loss_fire_ha': 'of which associated with fire, ha', 'emissions_mg_co2e': 'gross emissions, Mg CO2e',
               'district_2023': 'district name on the frame'}),
    dict(name='forest_extent_district', src='pk-forest/district-2026-10-17/forest_extent_district.csv',
         district=True, kind='places', licences=['cc-by-4.0'],
         licence_note='Hansen/UMD/Google/USGS/NASA tree cover; via Global Forest Watch.',
         provenance=('derived', 'Raster queries on the district polygons, run by Adaad.'),
         title='Pakistan district tree cover in 2000 and 2010',
         desc='Tree cover in 2000 and 2010, gain 2000–2020 and loss 2001–2025, by district, at 10 and 30 per cent canopy.',
         notes='With Gilgit-Baltistan and Azad Kashmir, 981,653 ha at 30 per cent canopy in 2000, 1.12 per '
               'cent of land. Tree cover 2010 is a separate product, not 2000 minus loss.',
         source='Global Forest Watch (Hansen et al., UMD); district queries by ' + ADAAD,
         unit='hectares',
         cols={**DISTRICT, 'threshold': 'canopy threshold, per cent', 'tc2000_ha': 'tree cover in 2000, ha',
               'tc2010_ha': 'tree cover in 2010, ha', 'tc2000_pct_of_area': 'tree cover 2000, per cent of district area',
               'gain_2000_2020_ha': 'tree cover gain 2000–2020, ha', 'district_2023': 'district name on the frame'}),
    dict(name='fires_district_month', src='pk-fires/fires_district_month.csv',
         district=True, kind='places', licences=['nasa-open'],
         licence_note='NASA FIRMS VIIRS 375 m, via Global Forest Watch; credit both.',
         provenance=('derived', 'Detections assigned to districts and counted by Adaad.'),
         title='Pakistan active fires by district and month since 2012',
         desc='VIIRS active-fire detections by district and month, January 2012 to October 2026.',
         notes='Detections, not fires or emissions: one burning field can give several. Nominal and '
               'high confidence in n_detections, all in n_detections_all. Global Forest Watch’s copy '
               'misses 82 days, the whole of 27 July–23 August 2022 among them; days_missing_in_feed '
               'says how many fall in each month, so treat those months, and 2024–25, as floors.',
         source='NASA FIRMS VIIRS via Global Forest Watch; district counts by ' + ADAAD,
         unit='detections',
         cols={**DISTRICT, 'month': 'calendar month', 'n_detections': 'nominal and high-confidence detections',
               'n_detections_all': 'all detections', 'days_missing_in_feed': 'days in the month absent from the feed',
               'partial_month': '1 for months the series only partly covers'}),
    dict(name='fires_district_season', src='pk-fires/fires_district_season.csv',
         district=True, kind='places', licences=['nasa-open'],
         licence_note='NASA FIRMS VIIRS 375 m, via Global Forest Watch.',
         provenance=('derived', 'Detections assigned to districts and counted by Adaad.'),
         title='Pakistan crop-burning season fires by district',
         desc='VIIRS detections in the rice-residue season (October–November) and the wheat season '
              '(April–May), by district and year.',
         notes='Punjab is 70–89 per cent of October–November detections. The incomplete flags mark '
               'seasons with days missing from the feed; those are floors.',
         source='NASA FIRMS VIIRS via Global Forest Watch; district counts by ' + ADAAD,
         unit='detections',
         cols={**DISTRICT, 'rice_oct_nov_n': 'October–November detections', 'wheat_apr_may_n': 'April–May detections',
               'rice_incomplete': '1 where the season has days missing', 'wheat_incomplete': '1 where the season has days missing'}),
    dict(name='landcover_district_epoch', src='pk-land-cover/change-2000-2020/landcover_district_epoch_2000_2020.csv',
         district=True, kind='places', licences=['cc-by-4.0'],
         licence_note='GLAD/UMD Global Land Cover and Land Use 2000–2020, v2 (Potapov et al. 2022).',
         provenance=('derived', 'Zonal statistics of the GLAD v2 rasters, computed by Adaad.'),
         title='Pakistan district land cover in 2000, 2005, 2010, 2015 and 2020',
         desc='Area and share of each land-cover class by district at five epochs, 2000 to 2020.',
         notes='GLAD v2 throughout: its built-up layer is about half of the v1 figure Global Forest Watch '
               'serves, so never mix the two. Built-up growth is cleaner than rural built-up levels.',
         source='GLAD/UMD GLCLU 2000–2020 v2; district statistics by ' + ADAAD,
         unit='hectares; share of land',
         cols={**DISTRICT, 'epoch': 'year of the map', 'class_code': 'GLAD class value',
               'group': 'class group (e.g. cropland, built-up, trees, water)', 'subgroup': 'class detail',
               'area_ha': 'area, ha', 'share_of_land': 'share of the district’s land'}),
    dict(name='landcover_change_district', src='pk-land-cover/change-2000-2020/landcover_change_district.csv',
         district=True, kind='places', licences=['cc-by-4.0'],
         licence_note='GLAD/UMD GLCLU 2000–2020 v2.',
         provenance=('derived', 'Zonal statistics of the GLAD change layer, computed by Adaad.'),
         title='Pakistan district land-cover change 2000 to 2020',
         desc='Cropland, built-up, tree and water gains and losses by district, 2000 to 2020, from GLAD’s change map.',
         notes='From the 2000–2020 change layer, which labels the 2020 state with its transition. '
               'It has no built-up loss class.',
         source='GLAD/UMD GLCLU 2000–2020 v2; district statistics by ' + ADAAD,
         unit='hectares', cols=DISTRICT),
    dict(name='grassland_district_year', src='pk-land-cover/grassland/grassland_district_year.csv',
         district=True, kind='places', licences=['cc-by-4.0'],
         licence_note='Global Pasture Watch GGC-30m v2-beta (Land & Carbon Lab / OpenGeoHub).',
         provenance=('derived', 'Zonal statistics of the Global Pasture Watch rasters, computed by Adaad.'),
         title='Pakistan district grassland by year since 2000',
         desc='Cultivated and natural grassland and open shrubland by district, 2000–2024.',
         notes='56.5 per cent of Pakistan (89 per cent of Balochistan) is never modelled and is counted '
               'as not_modelled_km2, so grassland is a floor. Use grassland_incl_shrub for rangeland; the '
               'class split between versions is not stable enough to quote.',
         source='Global Pasture Watch GGC-30m v2-beta; district statistics by ' + ADAAD,
         unit='km²; share', cols={**DISTRICT, 'grassland_incl_shrub_km2': 'grassland and open shrubland, km²',
                                       'not_modelled_km2': 'area the model never predicts, km²'}),
    # ── water ────────────────────────────────────────────────────────────────
    dict(name='water_stress_district', src='pk-water/water_stress_district.csv',
         district=True, kind='places', licences=['cc-by-4.0'],
         licence_note='WRI Aqueduct 2.1.',
         provenance=('derived', 'Basin scores averaged over districts by Adaad.'),
         title='Pakistan district baseline and projected water stress',
         desc='Baseline water stress and 2020–2040 projections under three scenarios, by district.',
         notes='Aqueduct 2.1 scores basins, not districts; a district’s figure is the mean over the '
               'basins it covers. The stress measure is withdrawals as a share of renewable supply, '
               'capped at 1 (raw ratios reach 190 in hyper-arid basins). The baseline’s '
               '“extremely high” class is a different scale from the projections’, so compare '
               'capped means across them, not shares. Projections are scenario outputs, not forecasts.',
         source='WRI Aqueduct 2.1; district means by ' + ADAAD, unit='ratio; share',
         cols={**DISTRICT, 'ws_baseline_capped_mean': 'baseline withdrawals as a share of supply, capped at 1',
               'ws_baseline_share_extremely_high': 'share of the district in extremely high stress'}),
    dict(name='surface_water_district', src='pk-water/surface_water_transitions_district.csv',
         district=True, kind='places', licences=['jrc-open'],
         licence_note='EC JRC / Google Global Surface Water (Pekel et al. 2016).',
         provenance=('derived', 'Raster queries on the district polygons, run by Adaad.'),
         title='Pakistan district surface water change 1984 to 2020',
         desc='Permanent and seasonal surface water gained and lost by district, 1984–1999 against 2000–2020.',
         notes='From JRC’s transitions layer at 30 m. Seasonal water is water present for part of the year.',
         source='EC JRC / Google Global Surface Water; district statistics by ' + ADAAD, unit='km²', cols=DISTRICT),
    dict(name='reservoir_monthly_observed', src='pk-water/observed-2026-10-18/reservoir_monthly_observed.csv',
         kind='state', licences=['cc-by-4.0'],
         licence_note='Global Water Watch (Deltares, WRI), as stated on Global Forest Watch’s copy.',
         provenance=('mixed', 'Satellite observations of each reservoir’s surface, summarised by month by Adaad.',
                     ['obs_mean_km2', 'obs_median_km2', 'clim_mean_km2', 'anomaly_obs_vs_clim_km2', 'anomaly_obs_vs_clim_pct']),
         title='Pakistan reservoir surface area by month since 1987',
         desc='Water surface area of 96 Pakistani reservoirs, by month, from satellite observations, 1986/87 to September 2026.',
         notes='Months with an observation only. Global Water Watch’s own monthly series carries values into '
               'unobserved months and disagrees with the observations in 1,074 reservoir-months; '
               'gww_disagrees_20pct flags them. Surface area, not storage. Validated against Tarbela’s '
               'hydel generation, r of about 0.6.',
         source='Global Water Watch (Deltares, WRI) via Global Forest Watch; monthly summary by ' + ADAAD,
         unit='km²',
         cols={'reservoir': 'reservoir', 'ym': 'year and month', 'month': 'calendar month', 'year': 'calendar year',
               'n_obs': 'satellite observations in the month', 'obs_mean_km2': 'mean observed surface, km²',
               'clim_mean_km2': 'mean for the calendar month across years, km²',
               'anomaly_obs_vs_clim_pct': 'difference from that month’s mean, per cent'}),
    # ── 2022 floods ─────────────────────────────────────────────────────────
    dict(name='flood2022_district', src='pk-floods-2022/rebuild-2026-10-18/flood_district_max_7day_series.csv',
         district=True, codes=155, kind='places', licences=['cc-by-sa-4.0'],
         licence_note='UNOSAT analysis, published on HDX under CC BY-SA; share-alike carries to this table. '
                      '“Analysis: United Nations Satellite Centre (UNOSAT)”; contains modified '
                      'Copernicus Sentinel data [2022]; NOAA/Suomi NPP.',
         provenance=('derived', 'Persistence and peaks computed by Adaad from UNOSAT’s weekly flood extents.'),
         title='Pakistan 2022 floods: peak and persistence by district',
         desc='Peak flooded area and population exposed, and the number of weekly windows a district was at '
              'least 5 per cent under water, 25 August 2022 to 15 January 2023.',
         notes='From UNOSAT’s seven-day NOAA-20 composites, 11 windows on one frame. The windows are not '
               'consecutive: nothing was observed 22–25 September, 18 October–13 November or '
               '21 November–24 December 2022, so say “in every observed window”. Satellite '
               'detection under cloud cover is incomplete; low-coverage weeks are counted. Preliminary '
               'analyses, not field-validated. The rows are COD’s 2022 districts, not the 2023 frame: '
               'Keamari has no row (its area is in Karachi South and West), and Lehri (PK218), since split '
               'between Kachhi and Sibi, has a row but no district_code.',
         source='UNOSAT Pakistan 2022 flood products (HDX); persistence by ' + ADAAD,
         unit='km²; people; weeks',
         cols={**DISTRICT, 'cod_adm2_name': 'district name as COD spells it',
               'peak_share_of_district_area': 'largest weekly share of the district under water',
               'weeks_at_or_above_5pct': 'observed weeks with 5 per cent or more of the district flooded',
               'peak_population_exposed': 'largest weekly population in flooded cells (WorldPop 2020)'}),
    dict(name='flood2022_weekly', src='pk-floods-2022/rebuild-2026-10-18/flood_weekly_series_7day_cod.csv',
         district=True, codes=155, kind='state', licences=['cc-by-sa-4.0'],
         licence_note='UNOSAT analysis, CC BY-SA; share-alike carries to this table.',
         provenance=('derived', 'UNOSAT extents matched to districts by Adaad.'),
         title='Pakistan 2022 floods by district and week',
         desc='Flooded area, analysed area and population exposed by district for each of 11 seven-day windows, '
              '25 August 2022 to 15 January 2023.',
         notes='One product per window, one frame. coverage is the share of the district the satellite '
               'saw cloud-free; low_coverage flags weeks under half. 22 source cells were blank and are '
               'counted as zero, flagged in flooded_km2_blank_in_source. On COD’s 2022 districts, as '
               'flood2022_district.',
         source='UNOSAT Pakistan 2022 flood products (HDX); matched to districts by ' + ADAAD,
         unit='km²; people',
         cols={**DISTRICT, 'week': 'the seven-day window', 'period_start': 'first day of the window',
               'period_end': 'last day', 'flooded_km2': 'flooded area, km²',
               'analysed_km2': 'area seen cloud-free, km²', 'coverage': 'share of the district analysed',
               'share_of_district_area': 'flooded share of the whole district'}),
    # ── mangroves ───────────────────────────────────────────────────────────
    dict(name='mangroves_national', src='pk-mangroves/mangrove_national_comparison.csv',
         kind='places', licences=['cc-by-4.0'],
         licence_note='Gilani et al. (2024), Zenodo 10.5281/zenodo.10732690; Global Mangrove Watch v4.1 (Bunting et al. 2026).',
         provenance=('mixed', 'Gilani areas recomputed by Adaad from the rasters’ true pixel size; GMW as published.',
                     ['gilani_total_km2', 'gilani_sindh_km2', 'gilani_balochistan_km2']),
         title='Pakistan mangrove area, two satellite series, 1990 to 2020',
         desc='Mangrove area every five years from two independent satellite classifications, side by side.',
         notes='They disagree by nearly a factor of two by 2020 (Gilani 1,337 km², GMW 738 km²), and on how '
               'much the area grew. Both are model classifications. Report both; never average them. The '
               'published Gilani totals overstate the Indus delta by about 9 per cent because its raster '
               'treats each degree-pixel as 30 m square; gilani_total_km2 corrects that, '
               'gilani_total_km2_as_published does not.',
         source='Gilani et al. (2024); Global Mangrove Watch v4.1.12; compiled by ' + ADAAD, unit='km²',
         cols={'year': 'year', 'gilani_total_km2': 'Gilani et al., area corrected for pixel size, km²',
               'gilani_total_km2_as_published': 'Gilani et al., as published', 'gmw_km2': 'Global Mangrove Watch v4.1.12, km²'}),
    # ── the district-to-DISCO crosswalk ────────────────────────────────────
    dict(name='district_disco_crosswalk', src='pk-disco-crosswalk/district_to_disco.csv',
         district=True, kind='geography', licences=['cc-by-4.0', 'pbs-open'],
         licence_note='The assignment is Adaad’s; household counts are Census 2023 Table 22 (PBS).',
         provenance=('mixed', 'District-to-company assignment by Adaad; census households as PBS published.',
                     ['disco', 'confidence', 'split_with']),
         title='Pakistan districts matched to electricity distribution companies',
         desc='The electricity distribution company serving most of each district, with Census 2023 households.',
         notes='One district, one company: districts with a known minority served by another company name it '
               'in split_with. Five rows are medium confidence (Naushahro Feroze, Dadu, Shaheed Benazirabad, '
               'Lasbela, Upper Chitral). AJK and GB are AJK-ED and GB-WPD, outside NEPRA’s tables. Do not '
               'paint a company’s figure onto its districts as if each district had it.',
         source='Adaad, from NEPRA distribution licences; households from PBS Census 2023 Table 22',
         unit='households',
         cols={**DISTRICT, 'disco': 'distribution company', 'confidence': 'high or medium',
               'split_with': 'another company serving part of the district',
               'households': 'households, Census 2023', 'light_electricity': 'households lighting by electricity',
               'light_solar_panel': 'households lighting by solar panel'}),
    # ── power ───────────────────────────────────────────────────────────────
    dict(name='netmetering_disco_fy', src='pk-nepra-netmetering/netmetering_disco_fy.csv',
         sql="SELECT * EXCLUDE (latest) FROM {src} WHERE latest", disco=True,
         kind='state', licences=['gov-unlicensed'], licence_note='NEPRA, State of Industry Reports.',
         provenance=('published', 'NEPRA’s figures; DISCO names normalised.'),
         title='Pakistan net metering by distribution company',
         desc='Net-metering licences, capacity, connections, pending applications and units bought through '
              'net metering, by distribution company and fiscal year, FY2019-20 to FY2024-25.',
         notes='No company-level net-metering figure exists before NEPRA’s 2022 report. Licences and '
               'concurrences are in-year flows, not connections; never add them to connections. '
               'cumulative_or_flow says which each measure is. The latest report’s figure is kept.',
         source='NEPRA State of Industry Reports 2022–2025; extracted by ' + ADAAD, unit='see unit',
         cols={**DISCO, **FY, 'measure': 'what is counted', 'value': 'value', 'unit': 'unit of value',
               'cumulative_or_flow': 'stock at a date, or flow in the year',
               'report_year': 'the State of Industry report the figure is from', 'page': 'page in that report'}),
    dict(name='loadshedding_disco_fy', src='pk-nepra-loadshedding/loadshedding_reported_disco_fy.csv',
         sql="SELECT * EXCLUDE (latest) FROM {src} WHERE latest", disco=True,
         kind='state', licences=['gov-unlicensed'],
         licence_note='NEPRA, Performance Evaluation Reports of Distribution Companies.',
         provenance=('published', 'The hours each company reported to NEPRA, as NEPRA printed them.'),
         title='Pakistan load shedding hours reported by distribution companies',
         desc='Average daily hours of load shedding each distribution company reported to NEPRA, '
              'FY2010-11 to FY2024-25.',
         notes='These are the companies’ own claims, which NEPRA calls “fudged” (FY16, FY20), '
               '“not based on ground realities” (FY21, FY22) and “significant underreporting” '
               '(FY25). What NEPRA says it observed is in loadshedding_nepra_monitored, a different quantity. '
               'The ranking is consistent: QESCO, HESCO and PESCO report the most. FY24 comes from the FY25 '
               'report’s comparison table only.',
         source='NEPRA Performance Evaluation Reports FY2012-13 to FY2024-25; transcribed by ' + ADAAD,
         unit='hours a day',
         cols={**DISCO, **FY, 'hours_reported': 'average daily hours of load shedding the company reported',
               'report': 'report the figure is from', 'pdf_page': 'page in that report'}),
    dict(name='loadshedding_nepra_monitored', src='pk-nepra-loadshedding/loadshedding_nepra_monitoring.csv',
         disco=True, kind='state', licences=['gov-unlicensed'],
         licence_note='NEPRA, Performance Evaluation Reports of Distribution Companies.',
         provenance=('published', 'NEPRA’s own statements, with the hours they give.'),
         title='Pakistan load shedding hours NEPRA observed',
         desc='The hours of load shedding NEPRA says it observed, against what the companies reported, FY2012-13 to FY2024-25.',
         notes='A statement in a report, not a measured series: from FY15 to FY18 the same sentence, 8 to 10 '
               'hours in towns and 10 to 12 in villages on certain feeders. FY2022-23 is the only year with '
               'a figure per company. hours_low without hours_high means “more than”. ALL marks a '
               'statement about every company.',
         source='NEPRA Performance Evaluation Reports; transcribed by ' + ADAAD, unit='hours a day',
         cols={**DISCO, **FY, 'hours_low': 'lower end of the hours NEPRA gives', 'hours_high': 'upper end',
               'statement': 'what NEPRA wrote'}),
    dict(name='feeder_outages_disco_fy', src='pk-nepra-feeder-outages/feeder_outages_disco_fy.csv', disco=True,
         kind='state', licences=['gov-unlicensed'], licence_note='NEPRA, State of Industry Reports.',
         provenance=('published', 'NEPRA’s tables; DISCO names normalised.'),
         title='Pakistan feeder trippings and outage minutes by distribution company',
         desc='Planned and forced feeder trippings and outage minutes by voltage, by distribution company, '
              'FY2010-11 to FY2024-25.',
         notes='Trippings and minutes by feeder voltage, not a load-shedding series: there is no denominator '
               'and no definition of scheduled shedding. Missing FY2011-12 to FY2013-14 and FY2018-19 '
               '(scanned reports). GEPCO’s FY2010-11 11 kV row is implausible. No grand-total row in any year.',
         source='NEPRA State of Industry Reports; extracted by ' + ADAAD, unit='trippings; minutes; hours',
         cols={**DISCO, **FY, 'outage_type': 'planned, forced or total', 'voltage': 'feeder voltage',
               'measure': 'what is counted', 'value': 'value', 'unit': 'unit'}),
    # ── gas, minerals, royalties ────────────────────────────────────────────
    dict(name='gas_supply_province', src='pk-extractives/ogra/gas_supply_by_province.csv',
         kind='state', licences=['gov-unlicensed'],
         licence_note='OGRA, State of the Regulated Petroleum Industry.',
         provenance=('mixed', 'Field supply as OGRA printed it, summed to provinces by Adaad.',
                     ['supply_mmcfd', 'share_of_indigenous_supply_pct']),
         title='Pakistan natural gas supply by province',
         desc='Indigenous natural gas supply by producing province, FY2018-19 to FY2024-25.',
         notes='Field supplies summed to the province the field lies in; they reconcile to OGRA’s printed '
               'subtotals within 1.1 MMCFD. Sindh is about two thirds every year. Qadirpur LPL/EPQL is listed '
               'twice in FY21–23; the double listing is a column of its own.',
         source='OGRA State of the Regulated Petroleum Industry; compiled by ' + ADAAD, unit='MMCFD',
         cols={'fiscal_year': 'fiscal year', 'province': 'producing province',
               'supply_mmcfd': 'gas supplied, million cubic feet a day',
               'share_of_indigenous_supply_pct': 'share of all indigenous supply, per cent'}),
    dict(name='gas_supply_field', src='pk-extractives/ogra/gas_supply_by_field.csv',
         kind='state', licences=['gov-unlicensed'], licence_note='OGRA, State of the Regulated Petroleum Industry.',
         provenance=('published', 'OGRA’s field tables as printed.'),
         title='Pakistan natural gas supply by field',
         desc='Gas supplied by each field to each system, FY2018-19 to FY2024-25.',
         notes='As OGRA printed it, report by report. Use gas_supply_province for province totals.',
         source='OGRA State of the Regulated Petroleum Industry; extracted by ' + ADAAD, unit='MMCFD',
         cols={'fiscal_year': 'fiscal year', 'field': 'gas field', 'system': 'utility or independent system',
               'province': 'province the field lies in', 'mmcfd': 'million cubic feet a day'}),
    dict(name='gas_consumption_sector', src='pk-extractives/ogra/gas_consumption_by_sector.csv',
         kind='state', licences=['gov-unlicensed'], licence_note='OGRA, State of the Regulated Petroleum Industry.',
         provenance=('published', 'OGRA’s sector tables as printed.'),
         title='Pakistan natural gas consumption by sector',
         desc='Gas consumed by sector and system for four fiscal years.',
         notes='As printed. Sub-total and grand-total rows are OGRA’s own; do not add them to the sectors.',
         source='OGRA State of the Regulated Petroleum Industry; extracted by ' + ADAAD, unit='MMCFD',
         cols={'fiscal_year': 'fiscal year', 'sector': 'consuming sector', 'total_mmcfd': 'all systems, MMCFD'}),
    dict(name='gas_consumption_province', src='pk-extractives/ogra/gas_consumption_by_province.csv',
         kind='state', licences=['gov-unlicensed'], licence_note='OGRA, State of the Regulated Petroleum Industry.',
         provenance=('published', 'OGRA’s province tables as printed.'),
         title='Pakistan natural gas consumption by province',
         desc='Gas consumed by province, FY2018-19 to FY2024-25.',
         notes='The basis changes between editions (utility systems only, or with fertilizer and power plants '
               'fed from fields): Balochistan’s FY2022-23 is 115 or 408 MMCFD depending on the edition. '
               'Compare within one basis only.',
         source='OGRA State of the Regulated Petroleum Industry; extracted by ' + ADAAD, unit='MMCFD',
         cols={'fiscal_year': 'fiscal year', 'province': 'consuming province', 'basis': 'what the table covers',
               'consumption_mmcfd': 'million cubic feet a day'}),
    dict(name='mineral_extraction', src='pk-extractives/economic-survey/mineral_extraction_fy1961_fy2025.csv',
         kind='economy', licences=['pbs-open', 'gov-unlicensed'],
         licence_note='PBS figures, as printed in the Finance Division’s Economic Survey statistical supplement.',
         provenance=('published', 'Spliced from two editions of the statistical supplement; overlap years agree '
                     'bar two one-unit revisions.'),
         title='Pakistan mineral extraction by year since 1960-61',
         desc='Extraction of 22 principal minerals, coal, crude oil and natural gas, FY1960-61 to FY2024-25.',
         notes='Units differ by mineral and are kept as printed; never add across minerals. Natural gas, '
               '“000 m.cu.mtr.” in the source, is billion cubic metres. National only: no province split.',
         source='PBS, via the Pakistan Economic Survey Statistical Supplement 2008-09 and 2024-25; compiled by ' + ADAAD,
         unit='see unit',
         cols={'fiscal_year': 'fiscal year', 'mineral': 'mineral', 'unit': 'unit as printed',
               'value': 'quantity extracted', 'status': 'provisional or final'}),
    dict(name='oil_gas_transfers_province', src='pk-extractives/transfers/straight_transfers_by_province_long.csv',
         kind='state', licences=['gov-unlicensed'],
         licence_note='Finance Division, Explanatory Memorandum on Federal Receipts.',
         provenance=('published', 'Budget and revised estimates as printed.'),
         title='Pakistan oil and gas royalties transferred to provinces',
         desc='Royalty on crude oil and on natural gas, gas development surcharge and excise duty on gas paid '
              'to each province, FY2008-09 to FY2026-27, budget and revised estimates.',
         notes='Every estimate as printed. For one figure a year, take the revised estimate from the following '
               'year’s budget, as Adaad’s best estimate does. Before FY2011-12 the straight-transfer '
               'total includes provincial GST; use the four oil and gas items for a consistent series. Sindh '
               'receives about half, Punjab under a tenth.',
         source='Finance Division, Explanatory Memorandum on Federal Receipts; extracted by ' + ADAAD, unit='Rs million',
         cols={'fiscal_year': 'fiscal year', 'item': 'what is transferred', 'province': 'province',
               'estimate': 'budget or revised estimate', 'budget_document': 'the budget the figure is from',
               'rs_million': 'Rs million'}),
    # ── crop prices ─────────────────────────────────────────────────────────
    dict(name='crop_prices_wholesale', src='pk-crops/mnfsr-asp-2024-25-prices/wholesale_prices_monthly.csv',
         sql="SELECT * EXCLUDE (source_url, raw_value) FROM {src}",
         kind='economy', licences=['gov-unlicensed'],
         licence_note='Ministry of National Food Security and Research, Agricultural Statistics of Pakistan 2024-25.',
         provenance=('published', 'As printed; source dashes kept as gaps.'),
         title='Pakistan wheat, rice and maize wholesale prices by market and month',
         desc='Monthly wholesale prices of wheat, Basmati and Irri-6 rice and maize in up to seven markets, '
              'July 2006 to June 2025.',
         notes='Nominal rupees per 40 kg as printed; no inflation adjustment. 1,187 of 5,400 monthly slots '
               'are dashes in the source and stay empty.',
         source='MNFSR, Agricultural Statistics of Pakistan 2024-25, tables 145–148; extracted by ' + ADAAD,
         unit='Rs per 40 kg',
         cols={'crop': 'crop', 'variety': 'variety', 'market': 'market', 'province': 'province',
               'fiscal_year': 'fiscal year', 'date': 'first day of the month',
               'price_pkr_per_40kg': 'wholesale price, rupees per 40 kg, nominal',
               'value_status': 'reported, or a gap in the source'}),
    dict(name='crop_prices_policy', src='pk-crops/mnfsr-asp-2024-25-prices/policy_prices_annual.csv',
         sql="SELECT * EXCLUDE (raw_value) FROM {src}",
         kind='economy', licences=['gov-unlicensed'],
         licence_note='Ministry of National Food Security and Research, Agricultural Statistics of Pakistan 2024-25.',
         provenance=('published', 'As printed, footnoted exceptions kept.'),
         title='Pakistan wheat, rice, cotton and sugarcane support prices',
         desc='Government procurement, support, intervention and indicative prices, 2004-05 to 2024-25.',
         notes='Nominal rupees per 40 kg. Every 2024-25 policy cell is a dash in the source, so the latest '
               'prices are for 2023-24.',
         source='MNFSR, Agricultural Statistics of Pakistan 2024-25, tables 142–144; extracted by ' + ADAAD,
         unit='Rs per 40 kg',
         cols={'fiscal_year': 'fiscal year', 'series_label': 'which price', 'geography': 'where it applies',
               'price_pkr_per_40kg': 'rupees per 40 kg, nominal'}),
    # ── emissions ───────────────────────────────────────────────────────────
    dict(name='emissions_national_sector', src='pk-emissions/pakemis-rc68/pakemis_national_series.csv',
         kind='economy', licences=['cc-by-4.0'],
         licence_note='PakEmis release 1.0.0rc68 (data CC BY 4.0); ODbL components excluded.',
         provenance=('published', 'PakEmis’s national series as released.'),
         title='Pakistan greenhouse gas emissions by sector since FY2015-16',
         desc='Greenhouse gas emissions by sector, FY2015-16 to FY2024-25, in CO2-equivalent on two warming metrics.',
         notes='A research inventory, not Pakistan’s official submission. Agriculture is half of 445 Mt '
               'CO2e in FY2023-24 under the reference configuration. AR5 and AR6 are two sets of '
               'global-warming potentials; use one at a time.',
         source='PakEmis (Pakistan Air Quality Initiative), release 1.0.0rc68', unit='Mt CO2e',
         cols={'sector': 'sector', 'period_label': 'fiscal year', 'fiscal_end_year': 'year the fiscal year ends',
               'co2e_Mt_AR5': 'Mt CO2e, AR5 warming potentials', 'co2e_Mt_AR6': 'Mt CO2e, AR6 warming potentials'}),
    dict(name='emissions_province_year', src='pk-emissions/pakemis-rc68/pakemis_adm1_by_year.csv',
         kind='economy', licences=['cc-by-4.0'], licence_note='PakEmis release 1.0.0rc68.',
         provenance=('published', 'PakEmis’s province tables as released.'),
         title='Pakistan greenhouse gas emissions by province and sector',
         desc='Emissions by province, sector and gas, FY2015-16 to FY2024-25.',
         notes='Three spatial views: physical (where emitted), origin and accounting. Use one at a time; they '
               'answer different questions and do not add.',
         source='PakEmis (Pakistan Air Quality Initiative), release 1.0.0rc68', unit='Gg; Mt CO2e',
         cols={'period_id': 'fiscal year', 'province': 'province', 'spatial_view': 'physical, origin or accounting',
               'gas': 'gas', 'emissions_Gg': 'gigagrams of the gas', 'co2e_Mt_AR5': 'Mt CO2e, AR5'}),
]

NAMES = [t['name'] for t in TABLES]
assert len(NAMES) == len(set(NAMES))
