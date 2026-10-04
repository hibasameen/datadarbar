"""What the catalogue says about each table beyond its own schema.

build_web_warehouse.py stamps these into catalog.json, so the catalogue pages
and the query console read one description rather than each keeping a copy:

  kind      which shelf the table sits on. The design's facet, in the site's
            own vocabulary (Places, Economy, State) rather than by source.
  span      the first and last period in the table, measured, from the first
            column in TIME_COLS it has.
  keys      the place identifiers it carries, in KEY_COLS order, so an analyst
            can see what it joins on before opening it.

A table with no KIND entry fails the build: a table the catalogue cannot file
is a table nobody finds.
"""

KINDS = [
    ('geography', 'Geography',
     'Boundaries, the identifiers that name places, and the crosswalks between '
     'every frame the site has used. Every other table joins on one of these keys.'),
    ('census', 'Census tables',
     'The 2017 and 2023 census tables as PBS published them, what survives of '
     '1998, population back to 1951, and the index of what can be mapped.'),
    ('places', 'Place indicators',
     'District and tehsil figures from surveys, satellites, the Mouza Census and '
     'administrative registers.'),
    ('economy', 'Economic series',
     'National accounts, industry, trade, prices, money and the external balance.'),
    ('state', 'State',
     'The budget, tax, courts, police, power and disasters: what institutions '
     'report about themselves.'),
    ('reference', 'Reference',
     'Where the source files came from.'),
]

KIND = {
    'geography_keys': 'geography',
    'census_unit_map': 'geography',
    'district_crosswalk_2017_2023': 'geography',
    'subdistrict_crosswalk_2017_2023': 'geography',
    'mouza_crosswalk': 'geography',

    'census_panel_2017': 'census',
    'census_panel_2023': 'census',
    'census_admin_units_1951_1998': 'census',
    'census1998_district_glance': 'census',
    'census_series_index': 'census',
    'census_entities': 'census',
    'census_enrolment_5_16_by_sex': 'census',

    'place_indicators': 'places',
    'place_indicator_index': 'places',
    'district_indicators': 'places',
    'mpi_districts': 'places',
    'tehsil_satellite': 'places',
    'tehsil_nightlights': 'places',
    'health_access_district': 'places',
    'health_access_tehsil': 'places',
    'school_access_district': 'places',
    'school_access_tehsil': 'places',
    'school_distance_stats': 'places',
    'schools_pk': 'places',
    'school_layer_coverage': 'places',
    'school_validation_district': 'places',
    'school_validation_tehsil': 'places',
    'school_validation_summary': 'places',
    'mouza_tehsil': 'places',
    'health_facilities_pk': 'places',
    'healthsites_osm_2019': 'places',
    'crops_district_fy': 'places',
    'diaspora_emigrants_district': 'places',

    'national_accounts': 'economy',
    'gdp_growth': 'economy',
    'gdp_indicators': 'economy',
    'gva_by_activity_annual': 'economy',
    'gva_by_activity_quarterly': 'economy',
    'lsm_qim': 'economy',
    'lsm_sector_indices': 'economy',
    'trade_hs8': 'economy',
    'trade_by_country': 'economy',
    'trade_by_group': 'economy',
    'trade_monthly_totals': 'economy',
    'trade_reconciliation': 'economy',
    'sbp_observations': 'economy',
    'sbp_series_catalog': 'economy',
    'sbp_handbook_series': 'economy',
    'imf_pakistan_fiscal': 'economy',
    'wdi_comparators': 'economy',
    'diaspora_remittances_monthly': 'economy',
    'diaspora_emigrants_by_skill': 'economy',
    'diaspora_destinations': 'economy',
    'diaspora_occupations': 'economy',

    'budget_lines': 'state',
    'fbr_tax_collection': 'state',
    'ljcp_case_flows': 'state',
    'ljcp_court_districts': 'state',
    'ljcp_judges_province': 'state',
    'ljcp_judicial_strength': 'state',
    'police_crime_annual': 'state',
    'police_crime_district': 'state',
    'sindh_crime_annual': 'state',
    'sindh_fir_daily': 'state',
    'nepra_plants': 'state',
    'nepra_plant_years': 'state',
    'nepra_disco_annual': 'state',
    'climate_events': 'state',
    'climate_impacts': 'state',

    'file_catalog': 'reference',
}

# The period a table covers, from the first of these it has. Order matters:
# a table with both fy and fy_end reads better as fy.
TIME_COLS = ['census_year', 'year', 'reporting_year', 'fy', 'fiscal_year',
             'doc_fy', 'report_year', 'obs_date', 'report_date', 'month',
             'period_start', 'start_date', 'as_of_date']

# Place identifiers, most specific first. The ones geography_keys measures
# carry its verdict on whether they are safe to join on.
KEY_COLS = ['dds_id', 'tehsil_code', 'dd_id', 'tehsil_id', 'adm3_pcode',
            'district_code', 'map_key', 'district_key', 'dk', 'district', 'province']
# Named in the catalogue beside the key, from geography_keys: safe to join on
# alone, or not. A name is never safe, so it is not listed as either.

