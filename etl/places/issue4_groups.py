"""The Issue 4 measures on the Places map: climate, heat, air, land, fire, water.

One list, read by four things so none can drift from the others:
extra_groups.py (the picker's vocabulary), topics.py (which topic files them),
build_hierarchy.py (where each sits in Environment, and whether it can be
totalled) and build_issue4.py (which writes the values).

Each group is one table in the warehouse (etl/issue4/tables.py). Its values
are on the COD frame and reach the map's PBS districts through
district_crosswalk_cod_2023, which folds four Gilgit-Baltistan districts into
three PBS ones. How a folded district is combined is the indicator's agg:

  sum   counts and hectares: Darel, Diamir and Tangir's fires add up
  mean  temperatures, rainfall, shares and scores: weighted by each part's area
  max   a count of weeks, which cannot be added across the parts

One group per long series, so no group file outgrows the largest the map
already loads (about 15,000 values): 76 years of one temperature measure over
156 districts is 11,856.

  form  the metric form build_hierarchy classifies (SUM_OK / POP_OK)
  expr  the value, as SQL over the table; where filters it; year gives the
        year column (NULL for a single snapshot)
"""

DEFAULT_REVIEW = 'Retain source definition; validate dimensions during implementation'
CLIMATE_DS = 'ERA5-Land (Copernicus C3S) and CHIRPS (UCSB), district means by Adaad'

GROUPS = [
    # ── temperature & heat ──────────────────────────────────────────────────
    dict(group_key='climateTmean', group_label='Mean temperature — ERA5-Land 1950–2025',
         dataset=CLIMATE_DS, sub='Temperature & heat', family='Annual temperature',
         table='climate_district_annual', year='year',
         indicators=[dict(ind='tmean_c', label='Mean temperature (°C)', dp=1,
                          form='Temperature (°C)', agg='mean', expr='tmean_c')]),
    dict(group_key='climateTmax', group_label='Mean daily maximum — ERA5-Land 1950–2025',
         dataset=CLIMATE_DS, sub='Temperature & heat', family='Annual temperature',
         table='climate_district_annual', year='year',
         indicators=[dict(ind='tmax_mean_c', label='Mean daily maximum temperature (°C)', dp=1,
                          form='Temperature (°C)', agg='mean', expr='tmax_mean_c')]),
    dict(group_key='climateTmin', group_label='Mean daily minimum — ERA5-Land 1950–2025',
         dataset=CLIMATE_DS, sub='Temperature & heat', family='Annual temperature',
         table='climate_district_annual', year='year',
         indicators=[dict(ind='tmin_mean_c', label='Mean daily minimum temperature (°C)', dp=1,
                          form='Temperature (°C)', agg='mean', expr='tmin_mean_c')]),
    dict(group_key='climateTanom', group_label='Temperature against the 1981–2010 normal',
         dataset=CLIMATE_DS, sub='Temperature & heat', family='Annual temperature',
         table='climate_district_annual', year='year',
         indicators=[dict(ind='tmean_anom_c', label='Temperature above or below the 1981–2010 normal (°C)',
                          dp=2, form='Temperature difference (°C)', agg='mean', expr='tmean_anom_c')]),
    dict(group_key='heatHotDays', group_label='Hot days — ERA5-Land 1981–2025',
         dataset=CLIMATE_DS, sub='Temperature & heat', family='Heat extremes',
         table='heat_district_annual', year='year', where='year >= 1981',
         indicators=[dict(ind='days_tmax_ge40', label='Days reaching 40°C or hotter', dp=0,
                          form='Day count', agg='mean', expr='days_tmax_ge40')]),
    dict(group_key='heatWarmNights', group_label='Warm nights — ERA5-Land 1981–2025',
         dataset=CLIMATE_DS, sub='Temperature & heat', family='Heat extremes',
         table='heat_district_annual', year='year', where='year >= 1981',
         indicators=[dict(ind='nights_tmin_ge25', label='Nights staying at 25°C or warmer', dp=0,
                          form='Day count', agg='mean', expr='nights_tmin_ge25')]),
    dict(group_key='heatSpells', group_label='Warm spells — ERA5-Land 1981–2025',
         dataset=CLIMATE_DS, sub='Temperature & heat', family='Heat extremes',
         table='heat_district_annual', year='year', where='year >= 1981',
         indicators=[dict(ind='wsdi_days', label='Warm-spell days (six or more unusually hot days in a row)',
                          dp=0, form='Day count', agg='mean', expr='wsdi_days')]),
    # ── rainfall ────────────────────────────────────────────────────────────
    dict(group_key='climateRain', group_label='Rainfall — CHIRPS 1981–2025',
         dataset=CLIMATE_DS, sub='Rainfall', family='Annual and monsoon rainfall',
         table='climate_district_annual', year='year', where='year >= 1981',
         indicators=[dict(ind='precip_mm_chirps', label='Rainfall in the year (mm)', dp=0,
                          form='Rainfall (mm)', agg='mean', expr='precip_mm_chirps'),
                     dict(ind='jjas_precip_mm_chirps', label='Monsoon rainfall, June–September (mm)', dp=0,
                          form='Rainfall (mm)', agg='mean', expr='jjas_precip_mm_chirps')]),
    # ── air ─────────────────────────────────────────────────────────────────
    dict(group_key='pm25', group_label='Fine particulate matter — satellite model 2024–2025',
         dataset='Pakistan Air Quality Initiative satellite PM2.5, district means by Adaad',
         sub='Air quality', family='Fine particulate matter',
         table='pm25_district_annual', year='year',
         indicators=[dict(ind='pm25_pop_mean', label='PM2.5 where people live (µg/m³)', dp=0,
                          form='Concentration (µg/m³)', agg='mean', expr='pm25_pop_mean')]),
    # ── land cover & forests ────────────────────────────────────────────────
    dict(group_key='landCover', group_label='Land cover — GLAD v2, 2000–2020',
         dataset='GLAD/UMD Global Land Cover and Land Use v2, district shares by Adaad',
         sub='Land cover & forests', family='Land cover by class',
         table='landcover_district_epoch', year='epoch',
         indicators=[dict(ind='lc_' + g, label=lab, dp=1, form='Share of area (%)', agg='mean',
                          expr=f"100 * coalesce(sum(share_of_land) FILTER (WHERE \"group\" = '{g}'), 0)")
                     for g, lab in [('cropland', 'Cropland (% of land)'),
                                    ('built_up', 'Built-up land (% of land)'),
                                    ('tree_cover', 'Tree cover (% of land)'),
                                    ('short_vegetation', 'Short vegetation (% of land)'),
                                    ('bare', 'Bare ground (% of land)'),
                                    ('water', 'Open water (% of land)')]],
         grouped=True),
    dict(group_key='grassland', group_label='Grassland — Global Pasture Watch 2000–2024',
         dataset='Global Pasture Watch GGC-30m v2-beta, district shares by Adaad',
         sub='Land cover & forests', family='Grassland',
         table='grassland_district_year', year='year',
         indicators=[dict(ind='grassland_incl_shrub_pct', label='Grassland and open shrubland (% of district)',
                          dp=1, form='Share of area (%)', agg='mean', expr='100 * grassland_incl_shrub_share'),
                     dict(ind='not_modelled_pct', label='Area the grassland model does not cover (% of district)',
                          dp=1, form='Share of area (%)', agg='mean', expr='100 * not_modelled_share')]),
    dict(group_key='forestLoss', group_label='Tree-cover loss — Global Forest Watch 2001–2025',
         dataset='Global Forest Watch tree cover loss (Hansen/UMD), district totals by Adaad',
         sub='Land cover & forests', family='Tree-cover loss',
         table='forest_loss_district_year', year='year', where='threshold = 30',
         indicators=[dict(ind='loss_ha', label='Tree cover lost (hectares, 30% canopy)', dp=0,
                          form='Area (hectares)', agg='sum', expr='loss_ha')]),
    # ── fires ───────────────────────────────────────────────────────────────
    dict(group_key='fires', group_label='Crop-burning seasons — VIIRS 2012–2025',
         dataset='NASA VIIRS active fires via Global Forest Watch, district counts by Adaad',
         sub='Fires', family='Active-fire detections',
         table='fires_district_season', year='year', where='year <= 2025',
         note_expr="CASE WHEN {incomplete} = 1 THEN 'The satellite feed misses some days of this "
                   "season here, so the count is a floor' END",
         indicators=[dict(ind='rice_oct_nov_n', label='Fire detections in October–November (rice residue)',
                          dp=0, form='Detection count', agg='sum', expr='rice_oct_nov_n',
                          incomplete='rice_incomplete'),
                     dict(ind='wheat_apr_may_n', label='Fire detections in April–May (wheat)',
                          dp=0, form='Detection count', agg='sum', expr='wheat_apr_may_n',
                          incomplete='wheat_incomplete')]),
    # ── floods & water ──────────────────────────────────────────────────────
    dict(group_key='flood2022', group_label='The 2022 floods — UNOSAT, Aug 2022–Jan 2023',
         dataset='UNOSAT seven-day flood extents (CC BY-SA), persistence by Adaad',
         sub='Floods & water', family='2022 floods', table='flood2022_district', year=None,
         indicators=[dict(ind='peak_flooded_pct', label='2022 floods: most of the district under water in any week (%)',
                          dp=1, form='Share of area (%)', agg='mean', expr='100 * peak_share_of_district_area'),
                     dict(ind='weeks_5pct', label='2022 floods: observed weeks with 5% or more under water',
                          dp=0, form='Week count', agg='max', expr='weeks_at_or_above_5pct')]),
    dict(group_key='waterStress', group_label='Water stress — WRI Aqueduct 2.1',
         dataset='WRI Aqueduct 2.1 baseline water stress, district means by Adaad',
         sub='Floods & water', family='Water stress', table='water_stress_district', year=None,
         indicators=[dict(ind='ws_baseline', label='Water withdrawn as a share of supply (baseline, capped at 1)',
                          dp=2, form='Published rate / ratio', agg='mean', expr='ws_baseline_capped_mean'),
                     dict(ind='ws_extreme_pct', label='District in Aqueduct’s “extremely high” stress class (%)', dp=1,
                          form='Share of area (%)', agg='mean', expr='100 * ws_baseline_share_extremely_high')]),
]

# New metric forms: may each be added across districts, and may it be read per
# head? Temperatures, rainfall, concentrations, shares and counts of days are
# properties of a place and add to nothing; fires and hectares add; none of
# them is a count of people.
SUM_OK = {'Temperature (°C)': False, 'Temperature difference (°C)': False,
          'Day count': False, 'Week count': False, 'Rainfall (mm)': False,
          'Concentration (µg/m³)': False, 'Share of area (%)': False,
          'Area (hectares)': True, 'Detection count': True}

# Groups whose province and national readout is a plain average of districts
# rather than a population-weighted one: a temperature or a share of land is
# about the ground, not the people on it. PM2.5 is already weighted by where
# people live, so it is averaged by population like the survey rates.
PLAIN_AVERAGE = [g['group_key'] for g in GROUPS
                 if g['group_key'] != 'pm25'
                 and not all(SUM_OK.get(i['form'], False) for i in g['indicators'])]

FEATURED = ['climateTmean|tmean_c', 'heatWarmNights|nights_tmin_ge25',
            'climateRain|jjas_precip_mm_chirps', 'pm25|pm25_pop_mean',
            'landCover|lc_built_up', 'fires|rice_oct_nov_n', 'flood2022|weeks_5pct']


def vocab():
    """The groups as extra_groups.GROUPS declares them."""
    return [dict(group_key=g['group_key'], topic='satellite',
                 topic_label='Satellite & environment', group_label=g['group_label'],
                 dataset=g['dataset'], dp={i['ind']: i['dp'] for i in g['indicators']},
                 indicators={i['ind']: i['label'] for i in g['indicators']})
            for g in GROUPS]


def mapping(countable):
    """Entries for build_hierarchy, in the crosswalk's own shape."""
    out = []
    for g in GROUPS:
        for i in g['indicators']:
            o = dict(level='district', group_key=g['group_key'], indicator=i['ind'],
                     label=i['label'], measure=i['label'], metric='', dp=i['dp'], source='place')
            out.append(dict(
                proposed_topic='Environment', proposed_subtopic=g['sub'],
                indicator_family=g['family'], metric_form=i['form'],
                suggested_controls='Year; district' if g['year'] else 'District',
                definition_review=DEFAULT_REVIEW,
                legacy_modes=(['Source value', '% of total population', 'Per 1,000 total population']
                              if countable(o) else ['Source value']),
                original=o))
    return out
