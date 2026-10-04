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
    'census_panel_1998': 'census',
    'census_panel_1951_1981': 'census',
    'census_population_history': 'census',
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



# ── licences ────────────────────────────────────────────────────────────────
# Data Darbar's own work - the cleaning, joins, crosswalks and derived columns -
# is CC BY 4.0. The figures underneath stay under their publisher's terms, and
# those are not all the same: a table cannot be offered on freer terms than
# its source allows. Each table names the terms that govern it here, and the
# catalogue, the dataset page and catalog.json all read from this.
#
# reuse is what a reader may do without asking:
#   open            any use, commercial included, with attribution
#   share-alike     open, but a database made from it must carry the same licence
#   non-commercial  not for commercial use
#   unstated        the publisher states no licence: cite it, and ask it before
#                   commercial reuse
# Checked 4 October 2026 against each publisher's own terms.
LICENCES = {
    'pbs-open': dict(
        name='PBS open licence', reuse='open',
        url='https://www.pbs.gov.pk/wp-content/uploads/2020/07/policy.pdf',
        summary='PBS Data Dissemination Policy (August 2026), section 9.1: copy, adapt and '
                'redistribute, commercially too, acknowledging PBS as the source, marking '
                'changes, and not presenting derived figures as official PBS statistics.'),
    'cc-by-4.0': dict(
        name='CC BY 4.0', reuse='open',
        url='https://creativecommons.org/licenses/by/4.0/',
        summary='Any use, commercial included, with attribution.'),
    'cc0': dict(
        name='CC0 1.0', reuse='open',
        url='https://creativecommons.org/publicdomain/zero/1.0/',
        summary='Public domain dedication: no conditions, though credit is courteous.'),
    'imf': dict(
        name='IMF terms of use', reuse='open',
        url='https://www.imf.org/en/About/copyright-and-terms',
        summary='Reuse, derived works and redistribution allowed with attribution to the IMF; '
                'a materially transformed figure must say so.'),
    'ec-reuse': dict(
        name='European Commission reuse policy (CC BY 4.0)', reuse='open',
        url='https://www.gdacs.org/About/termofuse.aspx',
        summary='GDACS is run by the European Commission\u2019s Joint Research Centre; '
                'Commission content is reusable with acknowledgement under its reuse decision '
                '(2011/833/EU, implemented through CC BY 4.0). GDACS\u2019s disclaimer applies '
                'alongside.'),
    'odbl': dict(
        name='ODbL 1.0', reuse='share-alike',
        url='https://opendatacommons.org/licenses/odbl/1-0/',
        summary='© OpenStreetMap contributors. Reuse with attribution; a database made '
                'from it must be shared under the ODbL too.'),
    'cc-by-nc-4.0': dict(
        name='CC BY-NC 4.0', reuse='non-commercial',
        url='https://creativecommons.org/licenses/by-nc/4.0/',
        summary='Attribution, non-commercial use only. Applies to Meta’s Relative Wealth '
                'Index.'),
    'sbp': dict(
        name='State Bank of Pakistan terms', reuse='non-commercial',
        url='https://archive.sbp.org.pk/Museum/dsclmr.htm',
        summary='SBP information may be copied and used with reference to the source, for '
                'non-commercial use, without being changed.'),
    'survey-programmes': dict(
        name='Survey programme terms (DHS Program, UNICEF MICS)', reuse='unstated',
        url='https://dhsprogram.com/data/Terms-of-Use.cfm',
        summary='District estimates computed from DHS and MICS survey microdata. Cite the '
                'survey; the programmes’ terms govern the microdata, which is not '
                'redistributed here. No licence is stated for estimates like these.'),
    'gov-unlicensed': dict(
        name='No licence stated', reuse='unstated',
        url='',
        summary='Published by a Pakistani public body without an open licence. Reproduced '
                'here unchanged and with attribution; ask the publisher before commercial '
                'reuse.'),
}

# Table -> (licences, note). The first licence is the one that governs most of
# the table; a note says where it does not.
LICENCE = {
    'budget_lines': (['gov-unlicensed'], 'Finance Division, Budget in Brief.'),
    'census1998_district_glance': (['pbs-open'], ''),
    'census_admin_units_1951_1998': (['pbs-open'], ''),
    'census_enrolment_5_16_by_sex': (['pbs-open'], ''),
    'census_entities': (['pbs-open'], ''),
    'census_panel_2017': (['pbs-open'], ''),
    'census_panel_2023': (['pbs-open'], ''),
    'census_panel_1998': (['pbs-open', 'cc-by-4.0'], 'The link to the 2023 frame is Data Darbar\u2019s work.'),
    'census_panel_1951_1981': (['pbs-open', 'cc-by-4.0'], 'The link to the 2023 frame is Data Darbar\u2019s work.'),
    'census_population_history': (['pbs-open', 'cc-by-4.0'], 'The footprints that hold still across censuses are Data Darbar\u2019s work.'),
    'census_series_index': (['pbs-open'], ''),
    'census_unit_map': (['pbs-open', 'cc-by-4.0'], 'The mapping itself is Data Darbar’s work.'),
    'climate_events': (['ec-reuse'], ''),
    'climate_impacts': (['gov-unlicensed'], 'NDMA situation reports.'),
    'crops_district_fy': (['pbs-open'], ''),
    'diaspora_destinations': (['pbs-open'], 'Bureau of Emigration figures as disseminated by PBS.'),
    'diaspora_emigrants_by_skill': (['pbs-open'], 'Bureau of Emigration figures as disseminated by PBS.'),
    'diaspora_emigrants_district': (['pbs-open'], 'Bureau of Emigration figures as disseminated by PBS.'),
    'diaspora_occupations': (['pbs-open'], 'Bureau of Emigration figures as disseminated by PBS.'),
    'diaspora_remittances_monthly': (['pbs-open'], 'Bureau of Emigration figures as disseminated by PBS.'),
    'district_crosswalk_2017_2023': (['pbs-open', 'cc-by-4.0'], 'The crosswalk is Data Darbar’s work.'),
    'district_indicators': (['pbs-open', 'survey-programmes'],
                            'PBS figures are open; the DHS and MICS estimates follow the survey '
                            'programmes’ terms.'),
    'fbr_tax_collection': (['pbs-open'], 'FBR figures as disseminated by PBS.'),
    'file_catalog': (['pbs-open', 'gov-unlicensed'], 'Lists PBS and Finance Division files.'),
    'gdp_growth': (['pbs-open'], ''),
    'gdp_indicators': (['pbs-open'], ''),
    'geography_keys': (['cc-by-4.0'], 'Data Darbar’s measurements of the PBS layers.'),
    'gva_by_activity_annual': (['pbs-open'], ''),
    'gva_by_activity_quarterly': (['pbs-open'], ''),
    'health_access_district': (['cc-by-4.0'], 'Malaria Atlas Project and WorldPop, both CC BY 4.0.'),
    'health_access_tehsil': (['cc-by-4.0'], 'Malaria Atlas Project and WorldPop, both CC BY 4.0.'),
    'health_facilities_pk': (['cc0'], 'ALHASAN Systems via the Humanitarian Data Exchange.'),
    'healthsites_osm_2019': (['odbl'], ''),
    'imf_pakistan_fiscal': (['imf'], ''),
    'ljcp_case_flows': (['gov-unlicensed'], 'Law & Justice Commission of Pakistan.'),
    'ljcp_court_districts': (['gov-unlicensed'], 'Law & Justice Commission of Pakistan.'),
    'ljcp_judges_province': (['gov-unlicensed'], 'Law & Justice Commission of Pakistan.'),
    'ljcp_judicial_strength': (['gov-unlicensed'], 'Law & Justice Commission of Pakistan.'),
    'lsm_qim': (['pbs-open'], ''),
    'lsm_sector_indices': (['pbs-open'], ''),
    'mouza_crosswalk': (['pbs-open', 'cc-by-4.0'], 'geoBoundaries ADM3 is CC BY 4.0.'),
    'mouza_tehsil': (['pbs-open'], ''),
    'mpi_districts': (['pbs-open', 'cc-by-4.0'],
                      'Computed by Data Darbar from PBS PSLM/HIES microdata.'),
    'national_accounts': (['pbs-open'], ''),
    'nepra_disco_annual': (['gov-unlicensed'], 'NEPRA reports.'),
    'nepra_plant_years': (['gov-unlicensed'], 'NEPRA reports.'),
    'nepra_plants': (['gov-unlicensed'], 'NEPRA reports.'),
    'place_indicator_index': (['pbs-open', 'cc-by-4.0', 'survey-programmes', 'cc-by-nc-4.0'],
                              'Each row keeps its source’s terms: rows in group rwi are '
                              'Meta’s Relative Wealth Index, non-commercial; DHS and MICS '
                              'rows follow the survey programmes.'),
    'place_indicators': (['pbs-open', 'cc-by-4.0', 'survey-programmes', 'cc-by-nc-4.0'],
                         'Each row keeps its source’s terms: rows in group rwi are '
                         'Meta’s Relative Wealth Index, non-commercial; DHS and MICS rows '
                         'follow the survey programmes.'),
    'police_crime_annual': (['gov-unlicensed'], 'Provincial police returns.'),
    'police_crime_district': (['gov-unlicensed'], 'Provincial police returns.'),
    'sbp_handbook_series': (['sbp'], ''),
    'sbp_observations': (['sbp'], ''),
    'sbp_series_catalog': (['sbp'], ''),
    'school_access_district': (['cc-by-4.0', 'pbs-open'],
                               'Derived by Data Darbar from schools_pk, WorldPop and the Malaria '
                               'Atlas friction surface.'),
    'school_access_tehsil': (['cc-by-4.0'], 'Derived by Data Darbar from schools_pk and WorldPop.'),
    'school_distance_stats': (['cc-by-4.0'], 'Derived by Data Darbar from schools_pk and WorldPop.'),
    'school_layer_coverage': (['cc-by-4.0'], 'Data Darbar’s ledger of the school layer.'),
    'school_validation_district': (['pbs-open', 'cc-by-4.0'], ''),
    'school_validation_summary': (['pbs-open', 'cc-by-4.0'], ''),
    'school_validation_tehsil': (['pbs-open', 'cc-by-4.0'], ''),
    'schools_pk': (['gov-unlicensed', 'odbl'],
                   'Compiled by Adaad from provincial school registers, which state no licence; '
                   'the Islamabad rows come from OpenStreetMap and stay under the ODbL.'),
    'sindh_crime_annual': (['gov-unlicensed'], 'Sindh Police.'),
    'sindh_fir_daily': (['gov-unlicensed'], 'Sindh Police.'),
    'subdistrict_crosswalk_2017_2023': (['pbs-open', 'cc-by-4.0'], 'The crosswalk is Data Darbar’s work.'),
    'tehsil_nightlights': (['cc-by-4.0'], 'Earth Observation Group, CC BY 4.0.'),
    'tehsil_satellite': (['cc-by-nc-4.0', 'cc-by-4.0'],
                         'The Relative Wealth Index is Meta’s, non-commercial; the WorldPop '
                         'columns are CC BY 4.0.'),
    'trade_by_country': (['pbs-open'], ''),
    'trade_by_group': (['pbs-open'], ''),
    'trade_hs8': (['pbs-open'], ''),
    'trade_monthly_totals': (['pbs-open'], ''),
    'trade_reconciliation': (['pbs-open'], ''),
    'wdi_comparators': (['cc-by-4.0'], 'World Bank.'),
}

REUSE_ORDER = ['open', 'unstated', 'share-alike', 'non-commercial']
REUSE_LABEL = {'open': 'Open', 'unstated': 'No licence stated',
               'share-alike': 'Share-alike', 'non-commercial': 'Non-commercial'}


def licence_of(table):
    """The licence record catalog.json carries for a table."""
    keys, note = LICENCE[table]
    reuse = max((LICENCES[k]['reuse'] for k in keys), key=REUSE_ORDER.index)
    return {'reuse': reuse, 'label': REUSE_LABEL[reuse], 'note': note,
            'terms': [{'key': k, 'name': LICENCES[k]['name'], 'url': LICENCES[k]['url'],
                       'summary': LICENCES[k]['summary']} for k in keys]}


# ── provenance: published, derived, or published with derived columns ───────
# PBS's open licence asks reusers to mark changes and not to present derived
# figures as official statistics; the same honesty is owed for every source.
# So every table says which of its figures are the publisher's and which are
# Data Darbar's.
#   published  the publisher's figures, unchanged (we may add join keys,
#              labels and quality flags, which are not figures)
#   mixed      the publisher's figures plus columns we constructed
#   derived    the figures themselves are Data Darbar's: computed, estimated
#              from microdata, aggregated from rasters or points, modelled,
#              or reallocated across boundaries
# Audited 4 October 2026 against the ETL that builds each table.
PROVENANCE = {
    # ── published as the source printed it ──
    **{t: ('published', '', []) for t in [
        'census1998_district_glance', 'census_entities', 'climate_events',
        'climate_impacts', 'crops_district_fy', 'diaspora_destinations',
        'diaspora_emigrants_by_skill', 'diaspora_emigrants_district',
        'diaspora_occupations', 'diaspora_remittances_monthly', 'fbr_tax_collection',
        'file_catalog', 'gdp_growth', 'gdp_indicators', 'gva_by_activity_annual',
        'gva_by_activity_quarterly', 'ljcp_judges_province', 'lsm_qim',
        'lsm_sector_indices', 'mouza_tehsil', 'national_accounts', 'nepra_disco_annual',
        'nepra_plant_years', 'police_crime_annual', 'police_crime_district',
        'sbp_handbook_series', 'sbp_observations', 'sbp_series_catalog',
        'sindh_crime_annual', 'sindh_fir_daily', 'trade_by_group', 'trade_hs8',
        'trade_monthly_totals', 'wdi_comparators']},
    # ── the publisher's figures plus columns we constructed ──
    'budget_lines': ('mixed', 'What each printed column means is inferred from its position.',
                     ['col_label', 'is_own_year_be']),
    'census_admin_units_1951_1998': ('mixed', 'Populations as printed; each unit’s level is '
                                     'read from the layout.', ['unit_type']),
    'census_enrolment_5_16_by_sex': ('mixed', 'Counts are PBS’s, but successor districts are '
                                     'summed into their 2017 parents; the shares and the gap are '
                                     'computed.', [f'{s}_in_school_pct' for s in (
                                         'girls', 'boys', 'girls_rural', 'boys_rural',
                                         'girls_urban', 'boys_urban')] + ['enrol_gap_pp']),
    'census_panel_2017': ('mixed', 'Figures as published, with labels harmonised across '
                          'workbooks. Two exceptions are constructed: a wholly rural unit’s '
                          'urban proportion, printed as a dash, is set to 0; and the map columns '
                          'link each unit to the 2023 frame.',
                          ['map_comparable', 'map_weight', 'is_rate', 'series_ambiguous']),
    'census_panel_1998': ('mixed', 'Figures as PBS published them; the link of each unit to the '
                          '2023 frame, and of each glance district to the 2017 districts it became, '
                          'is Data Darbar\u2019s.', ['map_comparable', 'map_weight']),
    'census_panel_1951_1981': ('mixed', 'Figures as PBS published them on the districts of 1998; '
                               'footprints that join districts PBS counted together, and the link '
                               'to the 2023 frame, are Data Darbar\u2019s.',
                               ['map_key', 'map_comparable', 'map_weight', 'value']),
    'census_population_history': ('mixed', 'Pakistan and provinces as PBS printed them; district '
                                  'series are sums over footprints that hold still, built by '
                                  'Data Darbar.', ['population', 'map_key']),
    'census_panel_2023': ('mixed', 'Figures as published or as corrected against PBS’s other '
                          'rendering; a wholly rural unit’s urban proportion, printed as a '
                          'dash, is set to 0.',
                          ['map_comparable', 'map_weight', 'value_corrected', 'renderings_disagree']),
    'district_indicators': ('mixed', 'Some figures are read as published; most are Data '
                            'Darbar’s: ratios computed from census counts, and estimates from '
                            'PSLM, HIES, LFS, DHS and MICS microdata. place_indicator_index.derived '
                            'says which.', []),
    'health_facilities_pk': ('mixed', 'Facilities as published; the care grouping and the 2023 '
                             'district (point in polygon) are assigned by Data Darbar.',
                             ['care_group', 'in_care_layer', 'district_code', 'district_2023',
                              'province_2023', 'district_key']),
    'healthsites_osm_2019': ('mixed', 'Amenities as mapped; the 2023 district is assigned by point '
                             'in polygon.', ['district_code', 'district_2023', 'province_2023',
                                             'district_key']),
    'imf_pakistan_fiscal': ('mixed', 'The IMF’s figures; which are projections is set by '
                            'Data Darbar from the edition date.', ['is_projection']),
    'ljcp_case_flows': ('mixed', 'Case counts as printed; the rates and reconciliation checks are '
                        'computed.', ['clearance_rate_pct', 'backlog_change', 'backlog_growth_pct',
                                      'stock_flow_residual', 'stock_flow_check',
                                      'unadjusted_flow_residual']),
    'ljcp_court_districts': ('mixed', 'Sessions divisions are summed into map districts; the '
                             'population, rates and per-judge figures are computed.',
                             ['clearance_rate_pct', 'population_2023', 'pending_per_100k',
                              'pending_per_100k_interpolated', 'pending_per_working_judge',
                              'stock_flow_check']),
    'ljcp_judicial_strength': ('mixed', 'Posts as printed; where a report leaves a cell blank '
                               'that the table implies is zero, it is set to 0 and listed in '
                               'inferred_zero_fields.', ['inferred_zero_fields']),
    'nepra_plants': ('mixed', 'A register compiled from every report: spellings, first and last '
                     'years are Data Darbar’s.', ['name_variants', 'first_fy', 'last_fy']),
    'place_indicators': ('mixed', 'Values are the source’s or Data Darbar’s by indicator '
                         '— place_indicator_index.derived says which. A value drawn on a '
                         'later-split unit is repeated on each successor shape.', []),
    'schools_pk': ('mixed', 'Schools as the registers list them. Standardised levels, the gender '
                   'read from the name, the district by polygon and the analysis fields are Data '
                   'Darbar’s; so are the positions themselves where coord_method is not '
                   'gps_at_source (multilaterated or geocoded).',
                   ['level_std', 'primary_plus', 'middle_plus', 'high_plus', 'gender',
                    'functional', 'district_key_boundary', 'coord_method', 'coord_precision',
                    'coord_tier', 'coord_resid_m', 'pin_vs_solved_m', 'in_analysis',
                    'analysis_sex', 'analysis_note', 'analysis_district_key_boundary']),
    'trade_by_country': ('mixed', 'Rows with period FY_from_quarters are the quarters summed by '
                         'Data Darbar; every other row is as published.', []),
    # ── constructed by Data Darbar ──
    **{t: ('derived', n, []) for t, n in [
        ('census_series_index', 'A coverage index computed from the census panels.'),
        ('census_unit_map', 'Data Darbar’s mapping of census units onto the 2023 frame.'),
        ('district_crosswalk_2017_2023', 'Constructed crosswalk; the populations in it are PBS’s.'),
        ('subdistrict_crosswalk_2017_2023', 'Constructed crosswalk; the populations in it are PBS’s.'),
        ('geography_keys', 'Measured from the boundary layers and the tables.'),
        ('health_access_district', 'Population-weighted statistics of modelled travel-time surfaces.'),
        ('health_access_tehsil', 'Population-weighted statistics of modelled travel-time surfaces.'),
        ('mouza_crosswalk', 'Data Darbar’s match of PBS tehsils to boundary polygons.'),
        ('mpi_districts', 'Alkire-Foster index computed from PSLM 2019-20 microdata.'),
        ('place_indicator_index', 'Metadata about the indicators, built by Data Darbar.'),
        ('school_access_district', 'Distances and counts computed from the school layer and WorldPop.'),
        ('school_access_tehsil', 'Distances and counts computed from the school layer and WorldPop.'),
        ('school_distance_stats', 'Distance distributions computed from the school layer and WorldPop.'),
        ('school_layer_coverage', 'Data Darbar’s coverage ledger.'),
        ('school_validation_district', 'A validation test computed against the Mouza Census.'),
        ('school_validation_summary', 'Rank correlations computed by Data Darbar.'),
        ('school_validation_tehsil', 'A validation test computed against the Mouza Census.'),
        ('tehsil_nightlights', 'Population-weighted radiance computed from VIIRS composites.'),
        ('tehsil_satellite', 'Zonal statistics of Meta’s wealth grid, WorldPop and VIIRS.'),
        ('trade_reconciliation', 'A comparison of PBS’s totals computed by Data Darbar.'),
    ]},
}

PROVENANCE_LABEL = {'published': 'As published', 'mixed': 'Includes derived',
                    'derived': 'Derived'}


def provenance_of(table, columns):
    """The provenance record catalog.json carries; fails on a derived column the
    table does not have, so a renamed column cannot silently lose its label."""
    kind, note, cols = PROVENANCE[table]
    missing = [c for c in cols if c not in columns]
    assert not missing, f'{table}: PROVENANCE names columns it lacks: {missing}'
    return {'type': kind, 'label': PROVENANCE_LABEL[kind], 'note': note,
            'derived_columns': cols}


# ── Places indicator groups ──────────────────────────────────────────────────
# Whether a curated group's VALUES are the publisher's or Data Darbar's.
# 'mixed' groups list the indicator ids that are constructed; the rest of the
# group is as published. Every census-panel series is as published.
PLACES_DERIVED = {
    'published': {'facilities', 'crops', 'migration', 'pslmFies'},
    'mixed': {
        'demographics': {'sex_ratio', 'density_per_sq_km'},
        'urbanRural': {'pct_urban', 'pct_rural'},
        'literacy': {'literacy_ratio_all', 'literacy_ratio_male', 'literacy_ratio_female',
                     'illiterate_all'},
        'censusSchooling': {'in_school_5_16_female', 'in_school_5_16_male',
                            'schooling_gender_gap'},
        'education': 'pct_*',
        'employment': {'lfpr*', 'employment_ratio*', 'unemployment_rate*', 'pct_paid_employee',
                       'pct_self_employed', 'pct_unpaid_family', 'not_in_lf'},
        'econCensus': {'avg_workers_per_est', 'pct_*'},
    },
    # every other group is constructed: microdata estimates (PSLM, HIES, LFS,
    # DHS, MICS), Mouza shares, MPI, satellite and access statistics
}


def place_value_derived(group_key, indicator):
    """True where Data Darbar constructed the figure, for a curated indicator."""
    import fnmatch
    if group_key in PLACES_DERIVED['published']:
        return False
    pats = PLACES_DERIVED['mixed'].get(group_key)
    if pats is None:
        return True
    pats = [pats] if isinstance(pats, str) else pats
    return any(fnmatch.fnmatch(indicator, p) for p in pats)
