"""The State explorer's payload: what the state collects, and what it does not.

All five of State's themes have data. Four of them had been built years ago and
never published: they sit in the desktop warehouse under ljcp/, regional_police/,
sindh_police/, sindh_fir/ and climate_events/, plus the nepra tables at its top
level, which is why a search of app/data alone found nothing.

  Public money   FBR's collection by head, 1991-92 to 2023-24, totalling
                 9.3 trillion rupees in the last year - FBR's own figure.
  Justice        case flows for five provinces, 2020 to 2024, and judicial
                 posts for Balochistan.
  Crime          reported offences by force and year, 2019 to 2024, with the
                 eleven-offence breakdown PBS publishes for every force, and
                 district figures where a force publishes them.
  Energy         133 power plants with fuel and installed capacity, and
                 nineteen years of distribution losses by company.
  Disasters      31 GDACS alerts since 2001, and NDMA's monsoon impacts.

Four things the shape of these tables forces on the queries below:

1. Every police figure exists twice for Azad Jammu & Kashmir - once in its own
   yearbooks and once in PBS's national compilation - and the two disagree by a
   few dozen cases a year (quality flag pbs_and_ajk_annual_totals_differ). The
   regional trend therefore takes PBS only, because that is the one series that
   covers all nine forces on one definition. AJK's own figure is carried
   alongside it so the gap is visible rather than resolved silently.

2. police_crime_annual mixes grains: a region row and a district row look alike
   apart from geography_level, so the trend must filter on it or it double
   counts. reported_crime_total is the force's own total; the per-offence rows
   are a partial breakdown and do not add up to it.

3. climate_events and climate_impacts are two different universes and do not
   join. Events are 31 GDACS alerts, 2001 to 2025, and GDACS alerts carry no
   casualty figures at all - deaths, affected and damage_usd are null in all 31
   rows. Impacts are NDMA monsoon situation reports for one season, 2026, by
   province. An earlier version of this file joined impacts.report_id to
   events.record_id; those keys are 'ndma:5f744f...' and 'gdacs:DR:1012498:PAK'
   and never match, so the join produced a column of nulls that read as "no
   casualties" rather than "no such measurement". The two are separate blocks.

4. The NDMA figures are cumulative snapshots, and their own measurement note
   forbids summing across reports or across the country and province rows. Only
   the latest report is carried, and the country row is kept apart from the
   provinces rather than added to them.

The federal budget is in the warehouse as budget_lines and is drawn on this
page (treemap, share of GDP, nominal trend) from app/data/budget_data.js, the
grouped extract that scripts/extract_budget_payload.py writes. What stays true
is the caveat: the item labels drift between documents, so the crosswalk
behind that chart is a reading of them, not a fact about them.
It is 18 years of budget documents whose item labels drift between them -
"EXPENDITURE (I + II)" one year, "Current Exp. on Revenue Receipts" another,
some carrying figures inside the label - so no item name spans more than five of
the eighteen years. Charting it needs an item crosswalk across the documents,
the same kind of work the census districts needed, and inventing one here would
produce a line that looks continuous and is not.

Usage:
  build_state_payload.py --warehouse <dir> --out <js>
"""
import argparse, json, pathlib, sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import duckdb

from build_disco_payload import AREA as DISCO_AREA, COLS as DISCO_COLS, losses, \
    recovery, RECOVERY_COLS

# NEPRA's technology strings distinguish things that are the same fuel and spell
# the same fuel two ways ('Coal' and 'THERMAL- COAL'). Group for the chart; the
# raw technology and fuel travel with each plant so the detail is not lost.
FUEL_FAMILY = """
    CASE
      WHEN technology ILIKE '%COAL%' OR fuel ILIKE '%COAL%'    THEN 'Coal'
      WHEN technology ILIKE '%HYDEL%'                          THEN 'Hydro'
      WHEN technology ILIKE '%NUCLEAR%'                        THEN 'Nuclear'
      WHEN technology ILIKE '%WIND%'                           THEN 'Wind'
      WHEN technology ILIKE '%SOLAR%'                          THEN 'Solar'
      WHEN technology ILIKE '%BIOGAS%' OR fuel ILIKE '%BAGASSE%' THEN 'Bagasse and biogas'
      WHEN fuel ILIKE '%GAS%'                                  THEN 'Gas'
      ELSE 'Oil and mixed thermal'
    END"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    W = pathlib.Path(a.warehouse)
    W_ = W.as_posix()
    cat_file = W / 'catalog.json'
    catalog = json.loads(cat_file.read_text()) if cat_file.exists() else None

    def rows(sql):
        return [[str(x) if hasattr(x, 'isoformat') else x for x in r]
                for r in con.sql(sql).fetchall()]

    tax = rows(f"""
        SELECT fy, fy_end, tax_type, head, round(sum(collection_pkr_mn), 1)
        FROM '{W_}/fbr_tax_collection.parquet'
        WHERE collection_pkr_mn IS NOT NULL
        GROUP BY 1, 2, 3, 4 ORDER BY fy_end, tax_type, head""")

    # Courts: the stock and the flow. Categories nest, so they travel labelled
    # rather than added; Balochistan is missing one of the five years.
    courts = rows(f"""
        SELECT year, province, category, pending_start, instituted, disposed,
               pending_end, round(clearance_rate_pct, 1)
        FROM '{W_}/ljcp_case_flows.parquet'
        WHERE court_tier = 'all_courts'
        ORDER BY year, province, category""")

    # Judicial strength by rank. Working and vacant do not add to sanctioned:
    # the table's own working_definition is
    # working_in_field_excludes_excadre, so a post filled by an officer on
    # deputation counts as neither. The residual is carried as its own column
    # rather than folded into either side.
    judges = rows(f"""
        SELECT year, province, court_tier, sum(sanctioned_judges),
               sum(working_judges), sum(vacant_judges),
               sum(sanctioned_judges) - sum(working_judges) - sum(vacant_judges)
        FROM '{W_}/ljcp_judicial_strength.parquet'
        GROUP BY 1, 2, 3 ORDER BY 1, 2, 4 DESC""")

    # Crime, the comparable series: PBS's total for each of the nine forces,
    # with AJK's own yearbook total beside it where the two disagree.
    crime = rows(f"""
        WITH pbs AS (
          SELECT year, region, value
          FROM '{W_}/police_crime_annual.parquet'
          WHERE measure = 'reported_crime_total'
            AND geography_level = 'region'
            AND source_family = 'pbs_national_police_bureau'),
        own AS (
          SELECT year, region, max(value) AS value
          FROM '{W_}/police_crime_annual.parquet'
          WHERE measure = 'reported_crime_total'
            AND geography_level = 'region'
            AND source_family <> 'pbs_national_police_bureau'
          GROUP BY 1, 2)
        SELECT p.year, p.region, p.value, o.value
        FROM pbs p LEFT JOIN own o USING (year, region)
        ORDER BY p.region, p.year""")

    # The offence breakdown PBS publishes on one definition for every force.
    # It is a partial breakdown: these eleven do not add to the force total.
    offences = rows(f"""
        SELECT year, region, offence, value
        FROM '{W_}/police_crime_annual.parquet'
        WHERE measure = 'reported_offence_count'
          AND geography_level = 'region'
          AND source_family = 'pbs_national_police_bureau'
          AND value IS NOT NULL
        ORDER BY region, year, value DESC""")

    # District crime, where a force publishes it - and the two forces that do
    # publish it do not publish the same thing. Azad Jammu & Kashmir gives a
    # district total for all reported crime across its 10 districts. Khyber
    # Pakhtunkhwa gives seven named serious offences across 37 districts and no
    # total: those seven come to 5,971 cases in 2024 against a provincial total
    # of 216,872, so adding them would produce a "district total" 36 times too
    # small. The measure travels with every row and the view keeps them apart.
    districts = rows(f"""
        SELECT year, region, geography, measure, offence, value
        FROM '{W_}/police_crime_annual.parquet'
        WHERE geography_level = 'district' AND value IS NOT NULL
          AND (measure = 'reported_crime_total' OR region = 'KP')
        ORDER BY region, geography, year, offence""")

    # Sindh publishes its own crime tables on a different schema from the
    # national compilation - 44 categories in 6 groups, by police range - so
    # it is its own dataset rather than being forced into police_crime.
    #
    # GROUPED ON THE CANONICAL KEYS, not the printed names. The reports change
    # capitalisation partway through: KARACHI RANGE becomes Karachi Range, and
    # BLASPHEMY (Offences relating to religion) loses its brackets. Sent to the
    # browser as raw strings those became 14 places and 54 categories instead
    # of 7 and 44, and every chart split one series into two that stopped and
    # started at the year the typography changed. The warehouse has carried
    # geography_key and crime_category_key all along.
    #
    # The display name is the variant the source printed in mixed case where
    # there is one, which is the later and more readable form; the raw labels
    # stay in the warehouse as the audit trail.
    #
    # Prior-year comparison columns ARE included, and that is deliberate: they
    # carry 2019 from the 2020 report, and no cell appears both as a
    # comparison column and as its own year - 0 of 1,232 (year, place,
    # category) cells have both - so nothing is counted twice. Dropping them
    # would delete the only observations those years have.
    sindh = rows(f"""
        WITH named AS (
          SELECT geography_key,
                 any_value(geography_name ORDER BY
                   CASE WHEN geography_name = upper(geography_name) THEN 1 ELSE 0 END,
                   geography_name) AS geo_name
          FROM '{W_}/sindh_crime_annual.parquet' GROUP BY 1),
        cats AS (
          SELECT crime_category_key,
                 any_value(crime_category_raw ORDER BY
                   CASE WHEN crime_category_raw = upper(crime_category_raw) THEN 1 ELSE 0 END,
                   crime_category_raw) AS cat_name
          FROM '{W_}/sindh_crime_annual.parquet' GROUP BY 1)
        SELECT s.year, n.geo_name, s.geography_level,
               CASE WHEN upper(s.category_group) = 'MISCELLANEOUS' THEN 'Miscellaneous'
                    ELSE s.category_group END AS grp,
               c.cat_name, sum(s.cases_reported)
        FROM '{W_}/sindh_crime_annual.parquet' s
        JOIN named n USING (geography_key)
        JOIN cats  c USING (crime_category_key)
        WHERE s.selection_status = 'selected' AND s.row_type = 'detail'
          AND s.cases_reported IS NOT NULL
        GROUP BY 1, 2, 3, 4, 5 ORDER BY 1, 2, 4, 5""")

    # First information reports, daily. Eight weeks of 2025, not a year: the
    # ytd column is a running total and must never be summed.
    firs = rows(f"""
        SELECT report_date, geography_name, geography_level, daily_firs, ytd_firs
        FROM '{W_}/sindh_fir_daily.parquet'
        WHERE daily_firs IS NOT NULL
        ORDER BY report_date, geography_name""")

    # Energy. The plant-YEAR observations, not the collapsed one-row-per-plant
    # table: nepra_plants carries installed_mw as the maximum across every
    # report a plant appears in, so anything summed from it is a union of
    # eight years rather than a year. 133 plants and 45,405 MW that way,
    # against 118 and 41,440 MW actually reported for 2024-25.
    # The canonical name, not the one that report happened to print: NEPRA
    # spells 133 plants 161 ways across eight editions, so counting the
    # printed name makes 161 plants out of 133. plant_id is what resolves
    # them and it is unique within every year.
    plants = rows(f"""
        SELECT y.plant_id, coalesce(p.plant_name, y.plant) AS plant,
               y.fiscal_year, {FUEL_FAMILY.replace('fuel', 'y.fuel')} AS family,
               y.technology, y.fuel, y.installed_mw, y.generation_gwh
        FROM '{W_}/nepra_plant_years.parquet' y
        LEFT JOIN (SELECT plant_id, plant_name
                   FROM '{W_}/nepra_plants.parquet') p USING (plant_id)
        ORDER BY y.fiscal_year DESC, y.installed_mw DESC NULLS LAST""")
    assert len({r[0] for r in plants}) == 133, 'plant_id count moved'
    plant_fys = sorted({r[2] for r in plants})
    assert len(plant_fys) >= 6, f'only {len(plant_fys)} plant years'
    # A plant listed without a capacity is still a plant that was listed, so
    # the rows are kept and the two counts are carried separately. A chart
    # that sums megawatts is drawing the second; one that counts plants is
    # drawing the first, and they are not the same number before 2021-22.
    plant_cover = {}
    for fy in plant_fys:
        yr = [r for r in plants if r[2] == fy]
        assert len(yr) > 50, f'{fy} has only {len(yr)} plants'
        plant_cover[fy] = {'plants': len(yr),
                           'with_mw': sum(1 for r in yr if r[6] is not None)}

    # Disasters: the alerts. GDACS carries no casualty figures, so none are
    # claimed here - alert_level is what the source actually grades.
    events = rows(f"""
        SELECT record_id, hazard, title, start_date, end_date,
               upper(alert_level[1]) || lower(alert_level[2:]), source_url
        FROM '{W_}/climate_events.parquet'
        ORDER BY start_date""")

    # Monsoon impacts: the latest NDMA report only, deduplicated across the
    # pages it spans, with country and province rows kept apart.
    impacts = rows(f"""
        WITH latest AS (
          SELECT * FROM '{W_}/climate_impacts.parquet'
          WHERE period_end = (SELECT max(period_end)
                              FROM '{W_}/climate_impacts.parquet'))
        SELECT location_name, admin_level, metric, max(value)
        FROM latest GROUP BY 1, 2, 3
        ORDER BY metric, 4 DESC""")

    window = con.sql(f"""
        SELECT max(period_start), max(period_end) FROM '{W_}/climate_impacts.parquet'
        WHERE period_end = (SELECT max(period_end)
                            FROM '{W_}/climate_impacts.parquet')""").fetchone()

    budget = con.sql(f"""
        SELECT count(*), count(DISTINCT doc_fy), min(doc_fy), max(doc_fy),
               count(DISTINCT item)
        FROM '{W_}/budget_lines.parquet'""").fetchone()

    # ── electricity distribution losses ────────────────────────────────────
    # The crosswalk lives in build_disco_payload so this page and the exporter
    # cannot drift apart. What comes back is one row per company-year, with
    # the national total printed under PESCO's name in 2019-20 already thrown
    # out and the loss rate computed from the GWh rather than taken from a
    # percentage column that contradicts them.
    disco_rows, disco_dropped, disco_disagree = losses(a.warehouse)
    assert disco_rows, 'the distribution crosswalk produced nothing'
    recovery_rows = recovery(a.warehouse)
    assert recovery_rows, 'no bill recovery rows'


    # ── derived denominators ─────────────────────────────────────────────
    # Two numbers the State charts divide by, carried with the payload and
    # marked as derived wherever they are used.
    #
    # GDP at current market prices, by fiscal year. PBS's Table 4 runs on the
    # 2015-16 base from 1999-00; TABLE-2 runs on the old base back to 1960-61
    # and prints 1999-00 on both bases in the same column. Earlier years are
    # spliced onto the new base by that overlap ratio (about 1.64), which is
    # a reading rather than a published figure, so each row says whether it
    # was spliced and the chart draws those years dashed.
    gdp_new = dict(rows(f"""
        SELECT year, value FROM '{W_}/national_accounts.parquet'
        WHERE table_sheet = 'Table 4' AND item LIKE 'G GDP at mp%'"""))
    gdp_old = dict(rows(f"""
        SELECT year, min(value) FROM '{W_}/national_accounts.parquet'
        WHERE table_sheet = 'TABLE-2' AND item = 'GDP (MP)' GROUP BY 1"""))
    splice = gdp_new['1999-00'] / gdp_old['1999-00']
    gdp = []
    for fy in sorted(set(gdp_old) | set(gdp_new)):
        end = int(fy[:4]) + 1
        if end < 1990:
            continue
        if fy in gdp_new:
            gdp.append([fy, end, round(gdp_new[fy], 1), 0])
        else:
            gdp.append([fy, end, round(gdp_old[fy] * splice, 1), 1])

    # Census 2023 population by province, summed from the district panel.
    # Azad Jammu & Kashmir and Gilgit-Baltistan are outside the census frame
    # and Railways polices no territory, so those three forces have no
    # denominator here and the per-capita chart says so rather than guessing.
    pop_name = {'PUNJAB': 'Punjab', 'SINDH': 'Sindh',
                'KHYBER PAKHTUNKHWA': 'KP', 'BALOCHISTAN': 'Balochistan',
                'ISLAMABAD': 'ICT'}
    pop = rows(f"""
        SELECT province_area, sum(value)
        FROM '{W_}/census_panel_2023.parquet'
        WHERE unit_type = 'district' AND table_id = '1'
          AND locality = 'all' AND sex = 'all'
          AND indicator = 'POPULATION-2023 / ALL SEXES'
        GROUP BY 1""")
    population = [[pop_name[p], 2023, int(v)] for p, v in pop]
    population.append(['Pakistan', 2023, sum(int(v) for _, v in pop)])
    assert abs(population[-1][2] - 241_499_431) < 1000, population[-1]

    # ── the indicator index ────────────────────────────────────────────────
    # One row per dataset x topic x indicator: what the three dropdowns offer
    # and what each one draws. Nothing here is a claim about the data - the
    # year span on every row is computed from the block below, so an index
    # entry cannot outlive the series it describes.
    INDEX = [
        # ds, topic, indicator, label, chart, block, year-field
        # Public money. The share of GDP leads because nominal rupees over
        # thirty-three years are mostly inflation; the ratio is the number
        # the reader wants and the one the story is about (7.1 to 8.8).
        ('fbr_tax_collection', 'tax', 'gdp',
         'Collection as a share of GDP, by head', 'taxGdp', 'tax', 'fy_end'),
        ('fbr_tax_collection', 'tax', 'share',
         'Each head as a share of the total', 'taxShare', 'tax', 'fy_end'),
        ('fbr_tax_collection', 'tax', 'stack',
         'Collection by head, nominal rupees', 'taxStack', 'tax', 'fy_end'),
        ('fbr_tax_collection', 'tax', 'shift',
         'Then and now: each head in the first and last year', 'taxShift',
         'tax', 'fy_end'),
        ('budget_lines', 'budget', 'tree',
         'One budget year as a treemap', 'budgetTree', None, None),
        ('budget_lines', 'budget', 'gdp',
         'Eighteen budgets as a share of GDP', 'budgetGdp', None, None),
        ('budget_lines', 'budget', 'trend',
         'Eighteen budgets in nominal rupees', 'budgetTrend', None, None),
        # Justice. One panel per court with its own scale, because Punjab is
        # thirty times Balochistan and one axis flattened the other four.
        ('ljcp_case_flows', 'courts', 'pending',
         'Cases pending at year end, court by court', 'courtsPending',
         'courts', 'year'),
        ('ljcp_case_flows', 'courts', 'net',
         'Net flow: instituted less disposed', 'courtsNet', 'courts', 'year'),
        ('ljcp_case_flows', 'courts', 'clearance',
         'Clearance rate against 100 per cent', 'courtsClearance', 'courts',
         'year'),
        ('ljcp_case_flows', 'courts', 'category',
         'Civil and criminal shares of the backlog', 'courtsCategory',
         'courts', 'year'),
        ('ljcp_judicial_strength', 'judges', 'composition',
         'Posts by rank: working, vacant, neither', 'judgesComposition',
         'judges', 'year'),
        ('ljcp_judicial_strength', 'judges', 'vacancy',
         'Vacancy rate by rank, first year to last', 'judgesVacancy',
         'judges', 'year'),
        # Crime. Per head of population where a census denominator exists;
        # ranked with the change since the first year everywhere else.
        ('police_crime_annual', 'crime', 'rate',
         'Reported cases per 100,000 people, by force', 'crimeRate', 'crime',
         'year'),
        ('police_crime_annual', 'crime', 'indexed',
         'Growth since the first year, by force', 'crimeIndexed', 'crime',
         'year'),
        ('police_crime_annual', 'crime', 'force',
         'Reported cases by force, stacked', 'crimeForce', 'crime', 'year'),
        ('police_crime_annual', 'crime', 'offence',
         'By offence: latest year against the first', 'crimeOffence',
         'offences', 'year'),
        ('police_crime_annual', 'crime', 'ajk',
         'Azad Jammu & Kashmir, by district: latest against the first',
         'crimeAjk', 'crimeDistricts', 'year'),
        ('police_crime_annual', 'crime', 'kp',
         'Khyber Pakhtunkhwa, seven serious offences: latest against the first',
         'crimeKp', 'crimeDistricts', 'year'),
        ('sindh_crime_annual', 'sindhCrime', 'group',
         'Sindh, by category group, stacked', 'sindhGroup', 'sindhCrime',
         'year'),
        ('sindh_crime_annual', 'sindhCrime', 'category',
         'Sindh, the twelve largest categories: latest against the first',
         'sindhCategory', 'sindhCrime', 'year'),
        ('sindh_crime_annual', 'sindhCrime', 'range',
         'Sindh, by police range: latest against the first', 'sindhRange',
         'sindhCrime', 'year'),
        ('sindh_fir_daily', 'firs', 'daily',
         'First information reports, the days observed', 'firsDaily', 'firs',
         None),
        # Energy. What the capacity actually produced leads: nameplate
        # capacity alone shows what could run, and the system ran at 34 per
        # cent of it in 2024-25. The fuel mix and single years follow.
        ('nepra_plant_years', 'plants', 'used',
         'How much of the capacity ran, by fuel', 'plantsUsed', 'plants', 'fy'),
        ('nepra_plant_years', 'plants', 'usetrend',
         'Share of capacity used, year by year', 'plantsUseTrend', 'plants', 'fy'),
        ('nepra_plant_years', 'plants', 'mix',
         'Reported capacity by fuel, every report year', 'plantsMix',
         'plants', 'fy'),
        ('nepra_plant_years', 'plants', 'fuel',
         'Reported capacity by fuel, one year at a time', 'plantsFuel',
         'plants', 'fy'),
        ('nepra_plant_years', 'plants', 'largest',
         'The eighteen largest plants in the selected year', 'plantsLargest',
         'plants', 'fy'),
        ('nepra_plant_years', 'plants', 'reports',
         'What NEPRA’s reports covered, year by year', 'plantsReports',
         'plants', 'fy'),
        ('nepra_disco_annual', 'discos', 'change',
         'Losses then and now, company by company', 'discoChange', 'discos',
         'fy_end'),
        ('nepra_disco_annual', 'discos', 'recovery',
         'Bills paid: rupees collected for every rupee billed', 'discoRecovery',
         'recovery', 'fy_end'),
        ('nepra_disco_annual', 'discos', 'losses',
         'T&D losses year by year', 'discoLosses', 'discos', 'fy_end'),
        ('nepra_disco_annual', 'discos', 'latest',
         'The latest year, companies ranked', 'discoLatest', 'discos',
         'fy_end'),
        ('nepra_disco_annual', 'discos', 'units',
         'Units in, units billed, units never billed', 'discoUnits', 'discos',
         'fy_end'),
        ('climate_events', 'events', 'timeline', 'Alerts by hazard and year',
         'eventsTimeline', 'events', None),
        ('climate_events', 'events', 'count', 'Alerts per year, by hazard',
         'eventsCount', 'events', None),
        ('climate_impacts', 'impacts', 'overview',
         'Deaths, injuries, houses and livestock by province', 'impactsPanel',
         None, None),
        ('climate_impacts', 'impacts', 'deaths_total', 'Deaths by province',
         'impactsMetric', None, None),
        ('climate_impacts', 'impacts', 'injured_total', 'Injured by province',
         'impactsMetric', None, None),
        ('climate_impacts', 'impacts', 'houses_damaged_total',
         'Houses damaged by province', 'impactsMetric', None, None),
        ('climate_impacts', 'impacts', 'livestock_perished',
         'Livestock perished by province', 'impactsMetric', None, None),
    ]
    TOPIC_LABEL = {
        'tax': ('What the state collects', 'Public money'),
        'budget': ('The federal budget', 'Public money'),
        'courts': ('Case flows and pendency', 'Justice'),
        'judges': ('Judges and vacancies', 'Justice'),
        'crime': ('Reported offences', 'Crime & policing'),
        'sindhCrime': ('Sindh crime tables', 'Crime & policing'),
        'firs': ('Sindh first information reports', 'Crime & policing'),
        'plants': ('Power plants and capacity', 'Energy'),
        'discos': ('Electricity distribution', 'Energy'),
        'events': ('Disaster alerts', 'Public services'),
        'impacts': ('Monsoon impacts', 'Public services'),
    }

    payload = {
        'tax': {'rows': tax,
                'cols': ['fy', 'fy_end', 'tax_type', 'head', 'pkr_mn']},
        'courts': {'rows': courts,
                   'cols': ['year', 'province', 'category', 'pending_start',
                            'instituted', 'disposed', 'pending_end',
                            'clearance_pct']},
        'judges': {'rows': judges,
                   'cols': ['year', 'province', 'tier', 'sanctioned', 'working',
                            'vacant', 'unaccounted']},
        'crime': {'rows': crime,
                  'cols': ['year', 'region', 'value', 'own_value']},
        'offences': {'rows': offences,
                     'cols': ['year', 'region', 'offence', 'value']},
        'crimeDistricts': {'rows': districts,
                           'cols': ['year', 'region', 'district', 'measure',
                                    'offence', 'value']},
        'sindhCrime': {'rows': sindh,
                       'cols': ['year', 'place', 'level', 'group', 'category',
                                'value']},
        'firs': {'rows': firs,
                 'cols': ['date', 'place', 'level', 'daily', 'ytd']},
        'plants': {'rows': plants,
                   'cols': ['id', 'plant', 'fy', 'family', 'technology',
                            'fuel', 'mw', 'gwh'],
                   'years': plant_fys, 'cover': plant_cover},
        'events': {'rows': events,
                   'cols': ['id', 'hazard', 'title', 'start', 'end', 'alert',
                            'url']},
        'impacts': {'rows': impacts,
                    'cols': ['place', 'level', 'metric', 'value'],
                    'start': str(window[0]), 'end': str(window[1])},
        'budget': {
            'rows': budget[0], 'docs': budget[1],
            'first': budget[2], 'last': budget[3], 'items': budget[4],
        },
        'recovery': {'rows': recovery_rows, 'cols': RECOVERY_COLS},
        'discos': {'rows': disco_rows, 'cols': DISCO_COLS, 'areas': DISCO_AREA,
                   'rejected': disco_dropped, 'disagreements': disco_disagree},
        'derived': {
            'gdp': {'cols': ['fy', 'fy_end', 'gdp_mp_pkr_mn', 'spliced'],
                    'rows': gdp, 'splice_factor': round(splice, 4)},
            'population': {'cols': ['region', 'census_year', 'population'],
                           'rows': population},
        },
    }
    def span(block, field):
        """The years a block actually covers, read off the block itself."""
        if not block or not field:
            return ''
        b = payload[block]
        i = b['cols'].index(field)
        ys = sorted({r[i] for r in b['rows'] if r[i] is not None})
        if not ys:
            return ''
        return str(ys[0]) if len(ys) == 1 else f'{ys[0]} to {ys[-1]}'

    ds_rows = {t['name']: t['rows'] for t in catalog['tables']} if catalog else {}
    payload['index'] = [
        {
            'ds': ds, 'dsLabel': ds,
            'topic': topic, 'topicLabel': TOPIC_LABEL[topic][0],
            'theme': TOPIC_LABEL[topic][1],
            'ind': ind, 'label': label, 'chart': chart,
            'years': span(block, yf),
            'rows': (f"{ds_rows[ds]:,} rows" if ds in ds_rows else ''),
        }
        for ds, topic, ind, label, chart, block, yf in INDEX
    ]
    missing = {r['topic'] for r in payload['index']} - set(TOPIC_LABEL)
    assert not missing, f'index topics with no label: {missing}'

    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        '/* Data Darbar - State explorer payload. Generated by\n'
        '   etl/economy/build_state_payload.py; do not edit by hand. */\n'
        'window.DD_STATE=' + json.dumps(payload, separators=(',', ':')) + ';\n')

    for k in ('tax', 'courts', 'judges', 'crime', 'offences', 'crimeDistricts',
              'sindhCrime', 'firs', 'plants', 'events', 'impacts'):
        print(f'  {k:15s} {len(payload[k]["rows"]):>6,} rows')
    print(f'  -> {out} ({out.stat().st_size/1e3:.1f} KB)')

    # Two checks that would have caught the join that was not a join.
    assert all(r[5] for r in events), 'an event lost its alert level'
    assert all(r[3] == r[4] + r[5] + r[6] for r in judges), \
        'a judicial rank does not reconcile to its sanctioned strength'
    assert not any(r[1] == 'KP' and r[3] == 'reported_crime_total'
                   for r in districts), \
        'KP has no district total; its seven offences must not be labelled one'
    national, provincial = {}, {}
    for place, level, metric, value in impacts:
        if level == 'country':
            national[metric] = value
        else:
            provincial[metric] = provincial.get(metric, 0) + (value or 0)
    for metric, total in sorted(national.items()):
        got = provincial.get(metric)
        flag = 'provinces agree' if got == total else f'provinces sum to {got:g}'
        print(f'  impacts {metric:26s} country {total:>9,.0f}  {flag}')


if __name__ == '__main__':
    main()
