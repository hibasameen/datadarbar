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
  Energy         133 power plants with fuel and installed capacity.
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

The federal budget is in the warehouse as budget_lines and is not a series yet.
It is 18 years of budget documents whose item labels drift between them -
"EXPENDITURE (I + II)" one year, "Current Exp. on Revenue Receipts" another,
some carrying figures inside the label - so no item name spans more than five of
the eighteen years. Charting it needs an item crosswalk across the documents,
the same kind of work the census districts needed, and inventing one here would
produce a line that looks continuous and is not.

Usage:
  build_state_payload.py --warehouse <dir> --out <js>
"""
import argparse, json, pathlib

import duckdb

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
    W_ = pathlib.Path(a.warehouse).as_posix()

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

    # Energy: every plant, with a grouped fuel family for the chart.
    plants = rows(f"""
        SELECT plant_name, {FUEL_FAMILY} AS family, technology, fuel,
               installed_mw, first_fy, last_fy
        FROM '{W_}/nepra_plants.parquet'
        WHERE installed_mw IS NOT NULL
        ORDER BY installed_mw DESC""")

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
        'plants': {'rows': plants,
                   'cols': ['plant', 'family', 'technology', 'fuel', 'mw',
                            'first_fy', 'last_fy']},
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
    }
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        '/* Data Darbar - State explorer payload. Generated by\n'
        '   etl/economy/build_state_payload.py; do not edit by hand. */\n'
        'window.DD_STATE=' + json.dumps(payload, separators=(',', ':')) + ';\n')

    for k in ('tax', 'courts', 'judges', 'crime', 'offences', 'crimeDistricts',
              'plants', 'events', 'impacts'):
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
