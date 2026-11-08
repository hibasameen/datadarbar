"""One record per district for the profile pages, from the published warehouse.

The profiles used to be nineteen pages of census table 1 and table 12, built
straight from districts.json. The site has since put 5,400 indicators for 156
districts on the 2023 frame behind the Places map, and a reader who arrives
from a search for "Skardu district population" or "Tharparkar literacy rate"
should land on a page that says what the site knows, not on the map's picker.

This script chooses which of those indicators a profile shows and writes them,
with the context a static page needs and the map computes on the fly:

  the value       at one fixed year per indicator, so every district's figure
                  is comparable with every other's and the rank means something
  the rank        1 = the highest value among the districts that have one
  the medians     the median district in Pakistan and in the province, so a
                  figure can be read against something without a chart
  the notes       what the warehouse recorded about the shape that published
                  the figure (a split, a merge, a renaming) and whether the
                  survey sample behind it was small
  the series      population since 1951, temperature since 1950, rainfall and
                  hot days since 1981, emigrants since 2011 - for sparklines
  the lists       tehsils with their 2023 population, the main crops, mother
                  tongues and religions as the census counted them

Everything is read from app/data/warehouse, so a profile can never say
something the catalogue and the map do not. The output is one JSON file that
scripts/build_districts.py renders; the deploy re-renders it without DuckDB.

Usage:
  build_profiles.py [--warehouse app/data/warehouse] [--out app/data/district_profiles.json]
"""
import argparse, datetime, json, pathlib, re, statistics

import duckdb

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent.parent
APP = REPO / 'app'

# ── the indicators a profile shows ─────────────────────────────────────────
# (id, group_key, indicator, year, label, fmt). `year` is the row chosen from
# place_indicators: None for groups that carry no year, a string otherwise.
# A fixed year per indicator is what makes the ranks and medians honest; the
# latest year a district happens to have is not comparable with its neighbour's.
#
# fmt: int   a count, thousands separated
#      pct   a percentage to one decimal      pct0  to none
#      n1 n2 n3  a number to that many decimals
#      c     degrees Celsius, one decimal     mm    millimetres, none
#      min   minutes, none                    ha    hectares, none
Y_CENSUS, Y_CLIMATE, Y_LAND, Y_EMIS, Y_MIGR, Y_GRASS, Y_DIEM = \
    '2023', '2025', '2020', '2024-25', '2024', '2024', '2024 (round 6)'

SPEC = [
    # people ─ Census 2023, with 2017 beside it where the table carries both
    ('pop', 'demographics', 'pop_total', Y_CENSUS, 'Population', 'int'),
    ('pop_m', 'demographics', 'pop_male', Y_CENSUS, 'Male population', 'int'),
    ('pop_f', 'demographics', 'pop_female', Y_CENSUS, 'Female population', 'int'),
    ('growth', 'demographics', 'annual_growth_rate', Y_CENSUS, 'Annual growth rate since the previous census (%)', 'n2'),
    ('area', 'demographics', 'area_sq_km', Y_CENSUS, 'Area (km²)', 'int'),
    ('density', 'demographics', 'density_per_sq_km', Y_CENSUS, 'Population density (people per km²)', 'int'),
    ('urban', 'demographics', 'urban_proportion', Y_CENSUS, 'Urban population (%)', 'pct'),
    ('hh_size', 'demographics', 'avg_household_size', Y_CENSUS, 'Average household size', 'n1'),
    ('sex_ratio', 'demographics', 'sex_ratio', Y_CENSUS, 'Sex ratio (males per 100 females)', 'n1'),
    ('rural_pop', 'urbanRural', 'rural_all', Y_CENSUS, 'Rural population', 'int'),
    ('urban_pop', 'urbanRural', 'urban_all', Y_CENSUS, 'Urban population', 'int'),
    # education
    ('lit', 'literacy', 'literacy_ratio_all', Y_CENSUS, 'Literacy rate, ages 10 and over (%)', 'pct'),
    ('lit_f', 'literacy', 'literacy_ratio_female', Y_CENSUS, 'Female literacy rate, ages 10 and over (%)', 'pct'),
    ('lit_m', 'literacy', 'literacy_ratio_male', Y_CENSUS, 'Male literacy rate, ages 10 and over (%)', 'pct'),
    ('literate', 'literacy', 'literate_all', Y_CENSUS, 'Literate people, ages 10 and over', 'int'),
    ('in_school_g', 'censusSchooling', 'in_school_5_16_female', None, 'Girls aged 5–16 in school (%)', 'pct'),
    ('in_school_b', 'censusSchooling', 'in_school_5_16_male', None, 'Boys aged 5–16 in school (%)', 'pct'),
    ('oos', 'censusSchooling', 'out_of_school_5_16', None, 'Out-of-school children, ages 5–16', 'int'),
    ('oos_g', 'censusSchooling', 'out_of_school_5_16_female', None, 'Out-of-school girls, ages 5–16', 'int'),
    ('oos_b', 'censusSchooling', 'out_of_school_5_16_male', None, 'Out-of-school boys, ages 5–16', 'int'),
    ('never_school', 'education', 'pct_never_attended', Y_CENSUS, 'Never attended school, ages 5 and over (%)', 'pct'),
    ('matric_plus', 'education', 'pct_matric_plus', Y_CENSUS, 'Matric or above, ages 5 and over (%)', 'pct'),
    ('graduate', 'education', 'pct_graduate', Y_CENSUS, 'Graduates, ages 5 and over (%)', 'pct'),
    ('ner', 'pslmEducation', 'net_enrolment_rate', None, 'Net enrolment rate, ages 5–16 (%)', 'pct'),
    ('ner_g', 'pslmEducation', 'net_enrolment_rate_female', None, 'Net enrolment rate, girls (%)', 'pct'),
    ('ner_b', 'pslmEducation', 'net_enrolment_rate_male', None, 'Net enrolment rate, boys (%)', 'pct'),
    ('pslm_lit', 'pslmEducation', 'literacy_rate', None, 'Literacy rate, survey measure (%)', 'pct'),
    ('numeracy', 'pslmEducation', 'numeracy_rate', None, 'Numeracy rate (%)', 'pct'),
    ('private_school', 'pslmEducation', 'pct_private_school', None, 'Pupils in private schools (%)', 'pct'),
    # work
    ('lfpr', 'employment', 'lfpr', Y_CENSUS, 'Labour force participation, ages 10 and over (%)', 'pct'),
    ('lfpr_f', 'employment', 'lfpr_female', Y_CENSUS, 'Female labour force participation (%)', 'pct'),
    ('lfpr_m', 'employment', 'lfpr_male', Y_CENSUS, 'Male labour force participation (%)', 'pct'),
    ('unemp', 'employment', 'unemployment_rate', Y_CENSUS, 'Unemployment rate (%)', 'pct'),
    ('unemp_f', 'employment', 'unemployment_rate_female', Y_CENSUS, 'Female unemployment rate (%)', 'pct'),
    ('unemp_m', 'employment', 'unemployment_rate_male', Y_CENSUS, 'Male unemployment rate (%)', 'pct'),
    ('employed', 'employment', 'worked', Y_CENSUS, 'People in work', 'int'),
    ('paid_emp', 'employment', 'pct_paid_employee', Y_CENSUS, 'Workers who are paid employees (%)', 'pct'),
    ('self_emp', 'employment', 'pct_self_employed', Y_CENSUS, 'Workers who are self-employed (%)', 'pct'),
    ('unpaid_fam', 'employment', 'pct_unpaid_family', Y_CENSUS, 'Unpaid family workers (%)', 'pct'),
    ('lfs_lfpr', 'lfs25', 'lfpr', None, 'Labour force participation (%)', 'pct'),
    ('lfs_unemp', 'lfs25', 'unemployment_rate', None, 'Unemployment rate (%)', 'pct'),
    ('lfs_agri', 'lfs25', 'pct_agriculture', None, 'Employed in agriculture (%)', 'pct'),
    ('lfs_manu', 'lfs25', 'pct_manufacturing', None, 'Employed in manufacturing (%)', 'pct'),
    ('lfs_trade', 'lfs25', 'pct_trade', None, 'Employed in trade (%)', 'pct'),
    ('lfs_youth', 'lfs25', 'youth_employment_ratio', None, 'Youth aged 15–24 in work (%)', 'pct'),
    ('estab', 'econCensus', 'total_establishments', None, 'Business establishments', 'int'),
    ('workforce', 'econCensus', 'total_workforce', None, 'People working in establishments', 'int'),
    ('workers_per_est', 'econCensus', 'avg_workers_per_est', None, 'Workers per establishment', 'n1'),
    ('est_manu', 'econCensus', 'pct_manufacturing', None, 'Establishments in manufacturing (%)', 'pct'),
    ('est_trade', 'econCensus', 'pct_trade', None, 'Establishments in wholesale and retail trade (%)', 'pct'),
    ('est_services', 'econCensus', 'pct_services', None, 'Establishments in services (%)', 'pct'),
    # health and children
    ('stunting', 'micsNutrition', 'stunting', None, 'Children under 5 stunted (%)', 'pct'),
    ('underweight', 'micsNutrition', 'underweight', None, 'Children under 5 underweight (%)', 'pct'),
    ('wasting', 'micsNutrition', 'wasting', None, 'Children under 5 wasted (%)', 'pct'),
    ('inst_delivery', 'micsMaternal', 'inst_delivery', None, 'Births in a health facility (%)', 'pct'),
    ('anc4', 'micsMaternal', 'anc4', None, 'Mothers with four or more antenatal visits (%)', 'pct'),
    ('sba', 'micsMaternal', 'skilled_attendance', None, 'Births attended by skilled staff (%)', 'pct'),
    ('birth_reg', 'micsProtection', 'birth_registered', None, 'Children under 5 with a registered birth (%)', 'pct'),
    ('violent_disc', 'micsProtection', 'violent_discipline', None, 'Children aged 1–14 facing violent discipline (%)', 'pct'),
    ('careseek', 'micsChildHealth', 'careseek_fever', None, 'Care sought for a child with fever (%)', 'pct'),
    ('child_func', 'micsEquity', 'child_func_difficulty', None, 'Children aged 5–17 with a functional difficulty (%)', 'pct'),
    ('ceb', 'dhsFertility', 'ceb_mean', None, 'Children ever born per woman aged 15–49', 'n2'),
    ('child_loss', 'dhsFertility', 'child_loss', None, 'Children who died, share of births (%)', 'pct'),
    ('morbidity', 'pslmHealth', 'morbidity_rate', None, 'Ill or injured in the last two weeks (%)', 'pct'),
    ('sought_tx', 'pslmHealth', 'pct_sought_treatment', None, 'Sought treatment when ill (%)', 'pct'),
    ('govt_facility', 'pslmHealth', 'pct_govt_facility', None, 'Treated at a government facility (%)', 'pct'),
    ('tt_mot', 'healthAccessDistrict', 'mot_popw_median', None, 'Travel time to the nearest health facility, motorised, median (minutes)', 'min'),
    ('tt_walk', 'healthAccessDistrict', 'wal_popw_median', None, 'Travel time to the nearest health facility, walking, median (minutes)', 'min'),
    ('tt_walk_60', 'healthAccessDistrict', 'wal_pct_pop_gt60', None, 'People more than an hour’s walk from care (%)', 'pct0'),
    ('tt_mot_60', 'healthAccessDistrict', 'mot_pct_pop_gt60', None, 'People more than an hour’s drive from care (%)', 'pct0'),
    # women
    ('cpr', 'dhsFamilyPlanning', 'cpr_any', None, 'Contraceptive use, any method (%)', 'pct'),
    ('cpr_modern', 'dhsFamilyPlanning', 'cpr_modern', None, 'Contraceptive use, modern method (%)', 'pct'),
    ('unmet_need', 'dhsFamilyPlanning', 'unmet_need', None, 'Unmet need for family planning (%)', 'pct'),
    ('married_u18', 'micsWomen', 'married_before_18', None, 'Women aged 20–49 married before 18 (%)', 'pct'),
    ('women_lit', 'micsWomen', 'literate', None, 'Women aged 15–49 literate, tested (%)', 'pct'),
    ('dv_justified', 'micsWomen', 'dv_justified', None, 'Women who say wife-beating can be justified (%)', 'pct'),
    ('not_permitted_work', 'hiesDecisions', 'pct_not_permitted_work', None, 'Rural women not permitted to work (%)', 'pct'),
    ('health_consulted', 'hiesDecisions', 'pct_health_consulted', None, 'Rural women consulted on their own healthcare (%)', 'pct'),
    # poverty and living standards
    ('mpi', 'mpi', 'mpi', None, 'Multidimensional poverty index (MPI)', 'n3'),
    ('mpi_h', 'mpi', 'H', None, 'Multidimensionally poor (headcount, %)', 'pct'),
    ('mpi_a', 'mpi', 'A', None, 'Intensity of deprivation among the poor (%)', 'pct'),
    ('dep_electricity', 'mpi', 'c_electricity', None, 'Poor and without electricity (%)', 'pct'),
    ('dep_water', 'mpi', 'c_water', None, 'Poor and without safe drinking water (%)', 'pct'),
    ('dep_sanitation', 'mpi', 'c_sanitation', None, 'Poor and without adequate sanitation (%)', 'pct'),
    ('dep_fuel', 'mpi', 'c_cooking_fuel', None, 'Poor and cooking with solid fuel (%)', 'pct'),
    ('dep_housing', 'mpi', 'c_housing', None, 'Poor and in inadequate housing (%)', 'pct'),
    ('dep_schooling', 'mpi', 'c_schooling', None, 'Poor and no member with five years of schooling (%)', 'pct'),
    ('dep_attendance', 'mpi', 'c_attendance', None, 'Poor and with a school-age child out of school (%)', 'pct'),
    ('piped_water', 'pslmWash', 'pct_piped_water', None, 'Households with piped water (%)', 'pct'),
    ('flush_toilet', 'pslmWash', 'pct_flush_toilet', None, 'Households with a flush toilet (%)', 'pct'),
    ('no_toilet', 'pslmWash', 'pct_no_toilet', None, 'Households with no toilet (%)', 'pct'),
    ('open_def', 'micsWash', 'open_defecation', None, 'Households practising open defecation (%)', 'pct'),
    ('ecoli_source', 'micsWash', 'ecoli_source', None, 'E. coli in water at the source (%)', 'pct'),
    ('internet', 'pslmDigital', 'pct_internet_user', None, 'Internet users, ages 10 and over (%)', 'pct'),
    ('internet_f', 'pslmDigital', 'pct_internet_female', None, 'Female internet users, ages 10 and over (%)', 'pct'),
    ('internet_m', 'pslmDigital', 'pct_internet_male', None, 'Male internet users, ages 10 and over (%)', 'pct'),
    ('smartphone', 'pslmDigital', 'pct_smartphone', None, 'Smartphone owners, ages 10 and over (%)', 'pct'),
    ('hh_mobile', 'pslmDigital', 'pct_mobile', None, 'Households with a mobile phone (%)', 'pct'),
    ('food_insecure', 'pslmFies', 'food_insecurity_pct', None, 'Households moderately or severely food insecure (%)', 'pct'),
    ('r_electricity', 'hiesHousing', 'pct_electricity', None, 'Rural households with electricity (%)', 'pct'),
    ('r_pucca', 'hiesHousing', 'pct_pucca_house', None, 'Rural households in a fully pucca house (%)', 'pct'),
    ('r_katcha', 'hiesHousing', 'pct_katcha_house', None, 'Rural households in a katcha house (%)', 'pct'),
    ('r_owner', 'hiesHousing', 'pct_owner_occupied', None, 'Rural households owning their home (%)', 'pct'),
    ('r_internet', 'hiesIct', 'pct_internet_user', None, 'Rural internet users (%)', 'pct'),
    ('r_smartphone', 'hiesIct', 'pct_smartphone', None, 'Rural smartphone owners (%)', 'pct'),
    ('r_food_insecure', 'hies', 'food_insecurity_pct', None, 'Rural households food insecure (%)', 'pct'),
    ('diem_poor_fc', 'diemFood', 'poor_food_consumption_pct', Y_DIEM, 'Households with poor food consumption (%)', 'pct0'),
    ('diem_skipped', 'diemFood', 'skipped_meal_pct', Y_DIEM, 'Households where someone skipped a meal in the last month (%)', 'pct0'),
    ('diem_shock', 'diemShocks', 'shock_any_pct', Y_DIEM, 'Households hit by a shock (%)', 'pct0'),
    # agriculture
    ('farms', 'agriFarms', 'farms', None, 'Farms', 'int'),
    ('farm_area', 'agriFarms', 'farm_area_ac', None, 'Farm area (acres)', 'int'),
    ('cultivated', 'agriFarms', 'cultivated_area_ac', None, 'Cultivated area (acres)', 'int'),
    ('farm_size', 'agriFarms', 'avg_farm_size_ac', None, 'Average farm size (acres)', 'n1'),
    ('small_farms', 'agriFarms', 'share_farms_under_2_5ac', None, 'Farms under 2.5 acres (%)', 'pct'),
    ('large_area', 'agriFarms', 'share_area_25ac_plus', None, 'Farm area in holdings of 25 acres or more (%)', 'pct'),
    ('tenant', 'agriFarms', 'share_farms_tenant', None, 'Tenant farms (%)', 'pct'),
    ('owner_farms', 'agriFarms', 'share_farms_owner', None, 'Owner-operated farms (%)', 'pct'),
    ('female_farms', 'agriFarms', 'share_farms_female', None, 'Farms operated by women (%)', 'pct'),
    ('irrigated', 'agriFarms', 'share_irrigated', None, 'Irrigated share of cultivated area (%)', 'pct'),
    ('crop_intensity', 'agriFarms', 'cropping_intensity_pct', None, 'Cropping intensity (%)', 'pct0'),
    ('wheat_share', 'agriFarms', 'share_wheat_of_cropped', None, 'Wheat share of cropped area (%)', 'pct'),
    ('cattle', 'agriLivestock', 'cattle', None, 'Cattle', 'int'),
    ('buffaloes', 'agriLivestock', 'buffaloes', None, 'Buffaloes', 'int'),
    ('sheep', 'agriLivestock', 'sheep', None, 'Sheep', 'int'),
    ('goats', 'agriLivestock', 'goats', None, 'Goats', 'int'),
    ('tractors', 'agriLivestock', 'tractors', None, 'Tractors', 'int'),
    ('tractors_per_farm', 'agriLivestock', 'tractors_per_1000_farms', None, 'Tractors per 1,000 farms', 'n1'),
    ('solar_tubewells', 'agriLivestock', 'share_tubewells_solar', None, 'Solar-powered tubewells (%)', 'pct'),
    ('tubewells', 'agriLivestock', 'tubewells_per_1000_cult_ac', None, 'Tubewells per 1,000 cultivated acres', 'n1'),
    # climate and environment
    ('tmean', 'climateTmean', 'tmean_c', Y_CLIMATE, 'Mean temperature (°C)', 'c'),
    ('tanom', 'climateTanom', 'tmean_anom_c', Y_CLIMATE, 'Temperature against the 1981–2010 normal (°C)', 'c2'),
    ('tmax', 'climateTmax', 'tmax_mean_c', Y_CLIMATE, 'Mean daily maximum (°C)', 'c'),
    ('tmin', 'climateTmin', 'tmin_mean_c', Y_CLIMATE, 'Mean daily minimum (°C)', 'c'),
    ('rain', 'climateRain', 'precip_mm_chirps', Y_CLIMATE, 'Rainfall in the year (mm)', 'mm'),
    ('monsoon', 'climateRain', 'jjas_precip_mm_chirps', Y_CLIMATE, 'Monsoon rainfall, June–September (mm)', 'mm'),
    ('hot_days', 'heatHotDays', 'days_tmax_ge40', Y_CLIMATE, 'Days reaching 40°C', 'int'),
    ('warm_nights', 'heatWarmNights', 'nights_tmin_ge25', Y_CLIMATE, 'Nights staying at 25°C or warmer', 'int'),
    ('warm_spells', 'heatSpells', 'wsdi_days', Y_CLIMATE, 'Warm-spell days', 'int'),
    ('pm25', 'pm25', 'pm25_pop_mean', Y_CLIMATE, 'PM2.5 where people live (µg/m³)', 'int'),
    ('lc_crop', 'landCover', 'lc_cropland', Y_LAND, 'Cropland (% of land)', 'pct'),
    ('lc_built', 'landCover', 'lc_built_up', Y_LAND, 'Built-up land (% of land)', 'pct'),
    ('lc_tree', 'landCover', 'lc_tree_cover', Y_LAND, 'Tree cover (% of land)', 'pct'),
    ('lc_short', 'landCover', 'lc_short_vegetation', Y_LAND, 'Short vegetation (% of land)', 'pct'),
    ('lc_bare', 'landCover', 'lc_bare', Y_LAND, 'Bare ground (% of land)', 'pct'),
    ('lc_water', 'landCover', 'lc_water', Y_LAND, 'Open water (% of land)', 'pct'),
    ('grassland', 'grassland', 'grassland_incl_shrub_pct', Y_GRASS, 'Grassland and open shrubland (% of district)', 'pct'),
    ('water_stress', 'waterStress', 'ws_baseline', None, 'Water withdrawn as a share of supply (0 to 1)', 'n2'),
    ('water_extreme', 'waterStress', 'ws_extreme_pct', None, 'District under extremely high water stress (%)', 'pct'),
    ('flood_peak', 'flood2022', 'peak_flooded_pct', None, '2022 floods: most of the district under water in any week (%)', 'pct'),
    ('flood_weeks', 'flood2022', 'weeks_5pct', None, '2022 floods: weeks with 5% or more under water', 'int'),
    ('ghg_km2', 'emissions', 'ghg_t_per_km2', Y_EMIS, 'Greenhouse gas emissions per km² (t CO₂e)', 'int'),
    ('ghg_agri', 'emissions', 'agriculture_pct', Y_EMIS, 'Share of emissions from agriculture (%)', 'pct'),
    ('ghg', 'emissions', 'ghg_kt_co2e', Y_EMIS, 'Greenhouse gas emissions (kt CO₂e)', 'int'),
    ('fires_rice', 'fires', 'rice_oct_nov_n', Y_CLIMATE, 'Fire detections, October–November (rice residue)', 'int'),
    ('fires_wheat', 'fires', 'wheat_apr_may_n', Y_CLIMATE, 'Fire detections, April–May (wheat)', 'int'),
    # buildings and facilities counted by Census 2023
    ('f_school', 'facilities', 'entity_8', None, 'Schools', 'int'),
    ('f_college', 'facilities', 'entity_9', None, 'Colleges', 'int'),
    ('f_university', 'facilities', 'entity_10', None, 'Universities and campuses', 'int'),
    ('f_madrisah', 'facilities', 'entity_7', None, 'Madrassas', 'int'),
    ('f_hospital', 'facilities', 'entity_6', None, 'Hospitals', 'int'),
    ('f_mosque', 'facilities', 'entity_11', None, 'Mosques', 'int'),
    ('f_bank', 'facilities', 'entity_20', None, 'Banks', 'int'),
    ('f_post', 'facilities', 'entity_19', None, 'Post offices', 'int'),
    ('f_police', 'facilities', 'entity_21', None, 'Police stations', 'int'),
    ('f_govt', 'facilities', 'entity_18', None, 'Government offices', 'int'),
    ('f_factory', 'facilities', 'entity_16', None, 'Factories', 'int'),
    ('f_shop', 'facilities', 'entity_12', None, 'Retail shops', 'int'),
    ('f_hotel', 'facilities', 'entity_4', None, 'Hotels', 'int'),
    # migration
    ('emig', 'migration', 'emigrants_registered', Y_MIGR, 'Emigrants registered in the year', 'int'),
    ('emig_total', 'migration', 'emigrants_cumulative', None, 'Emigrants registered, all years on record', 'int'),
]

# Derived from series rather than read: the same id space, built in code.
DERIVED = [
    ('hot_days_normal', 'Days reaching 40°C, 1981–2010 average', 'n1'),
    ('rain_normal', 'Rainfall, 1981–2010 average (mm)', 'mm'),
    ('forest_loss_total', 'Tree cover lost, 2001–2025 (hectares)', 'ha'),
    ('trans', 'Transgender population', 'int'),
    ('pop_2017', 'Population, Census 2017', 'int'),
    ('under_15', 'Under 15 (% of population)', 'pct'),
    ('age_15_64', 'Aged 15–64 (% of population)', 'pct'),
    ('age_60_plus', 'Aged 60 and over (% of population)', 'pct'),
    ('pending', 'Cases pending in the district courts at year end', 'int'),
    ('instituted', 'Cases instituted in the year', 'int'),
    ('disposed', 'Cases disposed in the year', 'int'),
    ('clearance', 'Clearance rate (disposed as % of instituted)', 'pct'),
    ('pending_100k', 'Pending cases per 100,000 people', 'n1'),
    ('judges', 'Judges working', 'int'),
    ('crime', 'Offences reported to the police', 'int'),
]

# Survey groups publish a small-sample flag beside their estimates. The flag
# sits in a sibling group; this says which, and what the flag is called.
LOW_N = {
    'mpi': ('mpi', 'low_n'),
    'dhsFamilyPlanning': ('dhs_fp', 'dhs_fp_low_n'), 'dhsFertility': ('dhs_fert', 'dhs_fert_low_n'),
    'dhsMaternal': ('dhs_mat', 'dhs_mat_low_n'), 'dhsNutrition': ('dhs_nut', 'dhs_nut_low_n'),
    'pslmEducation': ('pslm', 'pslm_low_n'), 'pslmEmployment': ('pslm', 'pslm_low_n'),
    'pslmWash': ('pslm', 'pslm_low_n'), 'pslmDigital': ('pslm_digital', 'pslm_digital_low_n'),
    'pslmHealth': ('pslm_health', 'pslm_health_low_n'),
    'hies': ('hies', 'hies_low_n'), 'hiesHousing': ('hies_hq', 'hies_hq_low_n'),
    'hiesIct': ('hies_ict', 'hies_ict_low_n'), 'hiesWaste': ('hies_waste', 'hies_waste_low_n'),
    'hiesDecisions': ('hies_wdm', 'hies_wdm_low_n'),
    'lfs25': ('lfs25', 'lfs25_low_n'), 'lfs': ('lfs21', 'lfs21_low_n'),
}

# Court districts are keyed by LJCP's spelling of the name. Matched to PBS's
# 2023 names after the obvious normalisation; what does not match is left out
# rather than guessed, and the build prints what it left out.
COURT_ALIASES = {
    'tor ghar': 'torghar', 'mirpurkhas': 'mirpur khas', 'umerkot': 'umer kot',
    'kambar shahdadkot': 'kambar shahdad kot', 'naushehro feroze': 'naushahro feroze',
    'sajawal': 'sujawal', 'tando allah yar': 'tando allahyar', 'malakand': 'malakand protected area',
    'qilla abdullah': 'killa abdullah', 'qilla saifullah': 'killa saifullah',
    'kech (turbat)': 'kech', 'd.g. khan': 'dera ghazi khan', 'd.i. khan': 'dera ismail khan',
    'nawabshah': 'shaheed benazirabad', 'chaghi': 'chagai', 'musakhail': 'musakhel',
    'jafarabad': 'jaffarabad', 'kachhi (bolan)': 'kachhi', 'bolan': 'kachhi',
    'shikarpur': 'shikarpur', 'lasbella': 'lasbela', 'killa abdullah': 'killa abdullah',
    'ketch': 'kech', 'gilgit': 'gilgit', 'hunza nagar': 'hunza',
}


def pretty(name):
    """'DERA GHAZI KHAN DISTRICT' -> 'Dera Ghazi Khan'. Census names are
    shouted and carry the tier; the page says both already."""
    n = re.sub(r'\s+DISTRICT$', '', name.strip())
    small = {'and', 'of', 'the'}
    out = []
    for w in n.split():
        if w == '&':
            out.append('&')
        elif w.lower() in small and out:
            out.append(w.lower())
        elif '-' in w:
            out.append('-'.join(p[:1].upper() + p[1:].lower() for p in w.split('-')))
        else:
            out.append(w[:1].upper() + w[1:].lower())
    return ' '.join(out)


def slug(name):
    """The address: 'KARACHI CENTRAL DISTRICT' -> 'karachi-central'. The same
    rule is in places.js, which links the map to the profile."""
    return re.sub(r'[^a-z0-9]+', '-', re.sub(r'\s+district$', '', name.lower())).strip('-')


def median(xs):
    xs = [x for x in xs if x is not None]
    return round(statistics.median(xs), 4) if xs else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', default=str(APP / 'data' / 'warehouse'))
    ap.add_argument('--out', default=str(APP / 'data' / 'district_profiles.json'))
    a = ap.parse_args()
    W = pathlib.Path(a.warehouse)
    con = duckdb.connect()

    def q(sql, *params):
        return con.execute(sql, params).fetchall()

    def tbl(name):
        return f"read_parquet('{(W / (name + '.parquet')).as_posix()}')"

    # ── the frame: PBS's 156 districts, as the map draws them ──────────────
    js = (APP / 'data' / 'districts_2023_geo.js').read_text()
    geo = json.loads(js[js.index('{'):js.rindex('}') + 1])
    frame = []
    for f in geo['features']:
        p = f['properties']
        frame.append({'code': p['code'], 'census_name': p['n'], 'name': pretty(p['n']),
                      'slug': slug(p['n']), 'province': pretty(p['p']),
                      'outside_census': bool(p.get('nc'))})
    assert len({d['slug'] for d in frame}) == len(frame), 'slugs collide'
    by_code = {d['code']: d for d in frame}
    codes = sorted(by_code)

    # tehsils, from the same source as the tehsil map
    js = (APP / 'data' / 'tehsils_2023_geo.js').read_text()
    tgeo = json.loads(js[js.index('{'):js.rindex('}') + 1])
    tehsil_pop = dict(q(f"""SELECT map_key, value FROM {tbl('census_panel_2023')}
        WHERE table_id='1' AND indicator='POPULATION-2023 / ALL SEXES' AND locality='all'
          AND sex='all' AND unit_type<>'district'"""))
    tehsils = {}
    for f in tgeo['features']:
        p = f['properties']
        code = next((c for c in codes if by_code[c]['census_name'] == p['d']), None)
        if code is None:
            continue
        kind = 'sub-division' if 'SUB-DIVISION' in p['n'] else ('sub-tehsil' if 'SUB-TEHSIL' in p['n'] else 'tehsil')
        tehsils.setdefault(code, []).append({
            'id': p['dds_id'],
            'name': pretty(re.sub(r'\s+(TEHSIL|SUB-DIVISION|SUB-TEHSIL)$', '', p['n'])),
            'kind': kind, 'pop': tehsil_pop.get(p['dds_id'])})
    for lst in tehsils.values():
        lst.sort(key=lambda t: (-(t['pop'] or 0), t['name']))

    # ── values ─────────────────────────────────────────────────────────────
    rows = q(f"""SELECT map_key, group_key, indicator, year, value, relation, note
        FROM {tbl('place_indicators')} WHERE level='district'""")
    cell = {}        # (group, indicator, year) -> {code: value}
    meta = {}        # (group, code) -> (relation, note)
    for code, g, ind, y, v, rel, note in rows:
        if code not in by_code or v is None:
            continue
        cell.setdefault((g, ind, y), {})[code] = v
        if rel != 'exact' and note:
            meta[(g, code)] = note

    low_n = {}       # (group, code) -> True
    for g, (fg, fi) in LOW_N.items():
        for code, v in cell.get((fg, fi, None), {}).items():
            if v:
                low_n[(g, code)] = True

    indicators = {}
    values = {c: {} for c in codes}
    notes = {c: {} for c in codes}

    def put(iid, label, fmt, by_code_values, source_group=None, year=None, unit_note=None):
        vals = {c: v for c, v in by_code_values.items() if c in by_code and v is not None}
        if not vals:
            print(f'  no values for {iid}')
            return
        order = sorted(vals, key=lambda c: -vals[c])
        rank = {c: i + 1 for i, c in enumerate(order)}
        prov_med = {}
        for prov in {d['province'] for d in frame}:
            xs = [vals[c] for c in vals if by_code[c]['province'] == prov]
            if len(xs) >= 3:
                prov_med[prov] = median(xs)
        indicators[iid] = {'label': label, 'fmt': fmt, 'n': len(vals), 'median': median(vals.values()),
                           'prov_median': prov_med, 'year': year, 'group': source_group}
        for c, v in vals.items():
            rec = {'v': v, 'r': rank[c]}
            if source_group and low_n.get((source_group, c)):
                rec['low_n'] = True
            values[c][iid] = rec
            if source_group and (source_group, c) in meta:
                notes[c][meta[(source_group, c)]] = True

    for iid, g, ind, y, label, fmt in SPEC:
        put(iid, label, fmt, cell.get((g, ind, y), {}), g, y)
        if y == Y_CENSUS and (g, ind, '2017') in cell:
            # the earlier census beside the latest, for the same shape
            prev = cell[(g, ind, '2017')]
            for c in values:
                if iid in values[c] and c in prev:
                    values[c][iid]['prev'] = prev[c]

    # census table 1 and 5: transgender count, 2017 population, age structure
    t1 = q(f"""SELECT map_key, indicator, value FROM {tbl('census_panel_2023')}
        WHERE table_id='1' AND unit_type='district' AND locality='all' AND sex='all'""")
    trans, pop17 = {}, {}
    for c, ind, v in t1:
        if ind == 'POPULATION-2023 / TRANS GENDER':
            trans[c] = v
        elif ind == 'POPULATION 2017':
            pop17[c] = v
    put('trans', 'Transgender population', 'int', trans, None, '2023')
    put('pop_2017', 'Population, Census 2017', 'int', pop17, None, '2017')
    t5 = q(f"""SELECT map_key, indicator, value FROM {tbl('census_panel_2023')}
        WHERE table_id='5' AND unit_type='district' AND locality='all' AND sex='all'""")
    ages = {}
    for c, ind, v in t5:
        ages.setdefault(c, {})[ind] = v
    def share(band):
        out = {}
        for c, d in ages.items():
            if d.get('ALL AGES') and d.get(band) is not None:
                out[c] = round(100 * d[band] / d['ALL AGES'], 2)
        return out
    put('under_15', 'Under 15 (% of population)', 'pct', share('UNDER 15'), None, '2023')
    put('age_15_64', 'Aged 15–64 (% of population)', 'pct', share('15 -- 64'), None, '2023')
    put('age_60_plus', 'Aged 60 and over (% of population)', 'pct', share('60 & ABOVE'), None, '2023')

    # mother tongue and religion, as shares of the table's own total
    def composition(table, total_label, strip):
        out = {}
        for c, ind, v in q(f"""SELECT map_key, indicator, value FROM {tbl('census_panel_2023')}
                WHERE table_id='{table}' AND unit_type='district' AND locality='all' AND sex='all'"""):
            out.setdefault(c, {})[ind] = v
        comp = {}
        for c, d in out.items():
            tot = d.get(total_label)
            if not tot:
                continue
            items = []
            for ind, v in d.items():
                if ind == total_label or v is None:
                    continue
                name = pretty(ind.replace(strip, '').strip(' /'))
                items.append([name, round(100 * v / tot, 2)])
            items.sort(key=lambda x: -x[1])
            comp[c] = items
        return comp
    tongues = composition('11', 'POPULATION BY MOTHER TONGUE / TOTAL', 'POPULATION BY MOTHER TONGUE')
    religions = composition('9', 'TOTAL POPULATION', '')
    FIX = {'Pushto': 'Pashto', 'Brahvi': 'Brahui', 'Kohiostani': 'Kohistani', 'Qadiani / Ahmadi': 'Ahmadi',
           'Hindu Jati': 'Hindu', 'Scheduled Castes': 'Scheduled castes'}
    for comp in (tongues, religions):
        for items in comp.values():
            for it in items:
                it[0] = FIX.get(it[0], it[0])

    # ── series ─────────────────────────────────────────────────────────────
    def series(g, ind, lo=None, hi=None):
        out = {c: [] for c in codes}
        for (gg, ii, y), vals in cell.items():
            if gg != g or ii != ind or y is None or not re.fullmatch(r'\d{4}', y):
                continue
            yi = int(y)
            if (lo and yi < lo) or (hi and yi > hi):
                continue
            for c, v in vals.items():
                out[c].append([yi, round(v, 2)])
        for c in out:
            out[c].sort()
        return out
    tanom = series('climateTanom', 'tmean_anom_c', 1950, 2025)
    rain = series('climateRain', 'precip_mm_chirps', 1981, 2025)
    hot = series('heatHotDays', 'days_tmax_ge40', 1981, 2025)
    loss = series('forestLoss', 'loss_ha', 2001, 2025)
    emig = series('migration', 'emigrants_registered', 2011, 2024)

    def normal(s):
        return {c: round(statistics.mean(v for y, v in pts if 1981 <= y <= 2010), 1)
                for c, pts in s.items() if sum(1 for y, v in pts if 1981 <= y <= 2010) >= 20}
    put('hot_days_normal', 'Days reaching 40°C, 1981–2010 average', 'n1', normal(hot), None, '1981–2010')
    put('rain_normal', 'Rainfall, 1981–2010 average (mm)', 'mm', normal(rain), None, '1981–2010')
    put('forest_loss_total', 'Tree cover lost, 2001–2025 (hectares)', 'ha',
        {c: round(sum(v for y, v in pts)) for c, pts in loss.items() if pts}, None, '2001–2025')

    # population at every census, on the footprint PBS can separate
    hist = {}
    for mk, place, today, prov, y, pop, rel in q(f"""SELECT map_key, place, today, province,
            census_year, population, relation FROM {tbl('census_population_history')}
            WHERE level='district' ORDER BY census_year"""):
        for c in mk.split():
            h = hist.setdefault(c, {'label': place, 'relation': rel, 'pts': []})
            h['pts'].append([int(y), pop])

    # ── lists ──────────────────────────────────────────────────────────────
    fy = q(f"SELECT max(fy) FROM {tbl('crops_district_fy')}")[0][0]
    crops = {}
    for c, crop, area, prod, yld in q(f"""SELECT district_code, crop, area_000ha, production_000t,
            yield_t_per_ha FROM {tbl('crops_district_fy')} WHERE fy=? AND area_000ha IS NOT NULL
            ORDER BY district_code, area_000ha DESC""", fy):
        if c in by_code and len(crops.setdefault(c, [])) < 6 and area and area > 0:
            crops[c].append({'crop': crop.title() if crop.isupper() else crop,
                             'area_000ha': area, 'production_000t': prod, 'yield_t_ha': yld})

    # ── courts (LJCP) and policing (AJK yearbooks), matched by name ────────
    court_year = 2024
    courts = {}
    by_name = {re.sub(r'\s+district$', '', d['census_name'].lower()): d['code'] for d in frame}
    unmatched = []
    for prov, dist, pend, inst, disp, clr, p100k, judges in q(f"""SELECT province, district,
            pending_end, instituted, disposed, clearance_rate_pct, pending_per_100k, working_judges
            FROM {tbl('ljcp_court_districts')} WHERE year=?""", court_year):
        key = re.sub(r'\s+agency$', '', dist.lower().strip())
        key = COURT_ALIASES.get(key, key)
        code = by_name.get(key)
        if code is None:
            unmatched.append(dist)
            continue
        courts[code] = (pend, inst, disp, clr, p100k, judges)
    if unmatched:
        print('  courts not matched to a 2023 district:', ', '.join(sorted(unmatched)))
    for i, (iid, label, fmt) in enumerate([('pending', 'Cases pending in the district courts at year end', 'int'),
                                           ('instituted', 'Cases instituted in the year', 'int'),
                                           ('disposed', 'Cases disposed in the year', 'int'),
                                           ('clearance', 'Clearance rate (disposed as % of instituted)', 'pct'),
                                           ('pending_100k', 'Pending cases per 100,000 people', 'n1'),
                                           ('judges', 'Judges working', 'int')]):
        put(iid, label, fmt, {c: t[i] for c, t in courts.items()}, None, str(court_year))
    crime_year = q(f"SELECT max(year) FROM {tbl('police_crime_district')} WHERE measure='reported_crime_total'")[0][0]
    crime = {}
    for geog, v in q(f"""SELECT geography, value FROM {tbl('police_crime_district')}
            WHERE measure='reported_crime_total' AND geography_level='district' AND year=?""", crime_year):
        code = by_name.get(geog.lower().strip())
        if code:
            crime[code] = v
    put('crime', 'Offences reported to the police', 'int', crime, None, str(crime_year))

    # ── sources, from the index the picker uses ────────────────────────────
    sources = {}
    for g, ds, src in q(f"""SELECT DISTINCT group_key, dataset, source FROM {tbl('place_indicator_index')}
            WHERE level='district'"""):
        sources[g] = {'dataset': ds, 'source': src}

    # ── assemble ───────────────────────────────────────────────────────────
    out = {
        'generated': datetime.date.today().isoformat(),
        'frame': 'PBS Digital Census 2023 districts',
        'census_year': 2023, 'climate_year': int(Y_CLIMATE), 'crops_fy': fy,
        'court_year': court_year, 'crime_year': crime_year,
        'indicators': indicators,
        'sources': sources,
        'districts': [],
    }
    for c in codes:
        d = dict(by_code[c])
        d['values'] = values[c]
        d['notes'] = sorted(notes[c])
        d['series'] = {
            'population': hist.get(c),
            'tanom': tanom.get(c) or None, 'rain': rain.get(c) or None,
            'hot_days': hot.get(c) or None, 'emigrants': emig.get(c) or None,
        }
        d['tehsils'] = tehsils.get(c, [])
        d['crops'] = crops.get(c, [])
        d['tongues'] = tongues.get(c, [])
        d['religions'] = religions.get(c, [])
        out['districts'].append(d)

    path = pathlib.Path(a.out)
    path.write_text(json.dumps(out, ensure_ascii=False, separators=(',', ':')) + '\n')
    n_vals = sum(len(d['values']) for d in out['districts'])
    print(f'{len(out["districts"])} districts, {len(indicators)} indicators, {n_vals:,} values '
          f'-> {path} ({path.stat().st_size/1e6:.1f} MB)')


if __name__ == '__main__':
    main()
