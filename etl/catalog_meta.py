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
