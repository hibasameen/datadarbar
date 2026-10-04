"""Every 2017 census unit reaches the 2023 map, and the weights are the census.

The unit map links the 2017 census to PBS's 2023 boundaries. Two things must
hold, and both are checked against the publisher rather than our own sums:

- Every 2017 district and tehsil has at least one 2023 shape to be drawn on.
  The many-to-many tehsil groups were left blank until 1 October 2026, which
  cost the 2017 map 73 tehsils; a regression there would look like missing
  data, not an error.
- The 2017 population carried as each unit's weight - used to combine rates
  across units - adds up to the published 2017 total, 207,684,626, at both
  tiers. A weight attached to the wrong unit would still produce a plausible
  map, so the total is the test.
"""
import csv
import pathlib

ROOT = pathlib.Path(__file__).resolve().parent.parent
UNIT_MAP = ROOT / 'etl' / 'census2017' / 'census_unit_map.csv'
PUBLISHED_2017 = 207_684_626
PUBLISHED_2023 = 241_499_431


def rows(year, tier):
    return [r for r in csv.DictReader(open(UNIT_MAP))
            if r['census_year'] == str(year) and r['unit_type'] == tier]


def test_every_2017_unit_reaches_the_2023_map():
    for tier in ('district', 'tehsil'):
        undrawn = [r['unit'] for r in rows(2017, tier) if not r['map_key']]
        assert not undrawn, f'{len(undrawn)} 2017 {tier}s have no 2023 shape, e.g. {undrawn[:3]}'


def test_every_2023_tehsil_has_a_2017_figure():
    shapes23 = {r['map_key'] for r in rows(2023, 'tehsil')}
    reached = {k for r in rows(2017, 'tehsil') for k in r['map_key'].split()}
    assert shapes23 <= reached, f'{len(shapes23 - reached)} 2023 tehsils get no 2017 figure'


def test_weights_sum_to_the_published_totals():
    # 2023 weights average the 2023 side of a footprint for a rate's change;
    # without them every split unit dropped off the change map.
    for year, published in ((2017, PUBLISHED_2017), (2023, PUBLISHED_2023)):
        for tier in ('district', 'tehsil'):
            total = sum(float(r['weight']) for r in rows(year, tier) if r['weight'])
            assert round(total) == published, f'{year} {tier} weights sum to {total:,.0f}'
            assert all(r['weight'] for r in rows(year, tier)), f'a {year} {tier} has no weight'


# A district whose workbook spelled a heading differently from the other 134
# lands in a series of its own and is blank on every map of the real one. The
# 3 October 2026 sweep folded 47 such district tables back; these are the ones
# a reader is most likely to open, each checked on the published panel.
REFOLDED = [
    ('11', 'HARIPUR DISTRICT', 'POPULATION BY MOTHER TONGUE / HINDKO', 'HINDKO'),
    ('22', 'LAHORE DISTRICT', '15 -- 24', 'LITERATE ( 10 YEARS & ABOVE )'),
    ('22', 'KOHISTAN DISTRICT', '15 -- 24', 'WORKED'),
    ('28', 'ZHOB DISTRICT', 'HOUSEHOLD BY NUMBER OF PERSONS / 5 PERSONS', '5 PERSONS'),
    ('6', 'KOHISTAN DISTRICT', '15 - 19', 'MARRIED'),
    ('1', 'KOHISTAN DISTRICT', 'POPULATION - 2017 / SEX RATIO', 'SEX RATIO'),
    ('38', 'TORGHAR DISTRICT', 'KITCHEN / NONE', 'TOTAL'),
    ('34', 'UMER KOT DISTRICT', 'OUTER WALLS / UNBAKED BRICKS / MUD', 'TOTAL'),
]


def test_misspelt_2017_headings_rejoin_their_series():
    import duckdb
    panel = (ROOT / 'app' / 'data' / 'warehouse' / 'census_panel_2017.parquet').as_posix()
    lost = [r for r in REFOLDED if not duckdb.execute(
        f"SELECT count(*) FROM '{panel}' WHERE table_id=? AND unit=? AND indicator=?"
        " AND col_label=? AND locality='all' AND sex='all' AND value IS NOT NULL",
        list(r)).fetchone()[0]]
    assert not lost, f'{len(lost)} district tables split off again, e.g. {lost[0]}'


def test_1998_admin_units_add_up():
    """Pakistan equals its provinces at every census 1951-1998, and in 1998
    every district equals its sub-units - the check that caught Balochistan's
    two nested levels and Peshawar's double breakdown in the parse."""
    import duckdb
    t = (ROOT / 'app' / 'data' / 'warehouse' / 'census_admin_units_1951_1998.parquet').as_posix()
    gap = duckdb.execute(f"""
        SELECT census_year,
               sum(population) FILTER (WHERE unit_type='country')
             - sum(population) FILTER (WHERE unit_type='province') AS d
        FROM '{t}' WHERE table_no=1 AND locality='all' GROUP BY 1 HAVING d <> 0""").fetchall()
    assert not gap, f'Pakistan and its provinces disagree: {gap}'
    assert duckdb.execute(f"SELECT population FROM '{t}' WHERE unit_type='country' "
                          "AND census_year=1998 AND locality='all'").fetchone()[0] == 132_352_279
    off = duckdb.execute(f"""
        WITH d AS (SELECT table_no, unit AS district, population p FROM '{t}'
                   WHERE unit_type='district' AND census_year=1998 AND locality='all'),
             s AS (SELECT table_no, district, sum(population) p FROM '{t}'
                   WHERE census_year=1998 AND locality='all' AND (unit_type='sub_division'
                     OR (unit_type='tehsil' AND sub_division IS NULL)) GROUP BY 1, 2)
        SELECT d.district FROM d JOIN s USING (table_no, district) WHERE d.p <> s.p""").fetchall()
    assert not off, f'districts that do not equal their sub-units in 1998: {off[:5]}'


PANEL_1998 = (ROOT / 'app' / 'data' / 'warehouse' / 'census_panel_1998.parquet').as_posix()


def test_1998_panel_population_is_the_published_total_on_the_2023_frame():
    """The 1998 panel's population is PBS's own restatement on the 2017
    units, so both tiers must add to 132,352,279 - including the de-excluded
    area of Rajanpur (14,051 people, unit_type 'other'), whose loss once left
    the sub-district tier short - and every 2023 district and tehsil must get
    a 1998 figure through the 2017 unit map."""
    import duckdb
    for tier in ("unit_type = 'district'", "unit_type <> 'district'"):
        total = duckdb.execute(f"SELECT sum(value) FROM '{PANEL_1998}' WHERE table_id = '1' "
                               f"AND locality = 'all' AND {tier}").fetchone()[0]
        assert round(total) == 132_352_279, f'1998 {tier} sums to {total:,.0f}'
    for tier in ('district', 'tehsil'):
        shapes23 = {r['map_key'] for r in rows(2023, tier)}
        side = "= 'district'" if tier == 'district' else "<> 'district'"
        reached = {k for (mk,) in duckdb.execute(
            f"SELECT DISTINCT map_key FROM '{PANEL_1998}' WHERE table_id = '1' "
            f"AND unit_type {side} AND map_key IS NOT NULL").fetchall() for k in mk.split()}
        assert shapes23 <= reached, f'{len(shapes23 - reached)} 2023 {tier}s get no 1998 figure'


def test_1998_glance_counts_are_drawn_only_on_whole_shapes():
    """A District at a Glance figure reaches the map only through a group of
    2017 districts whose restated 1998 population equals the glance's own, to
    the person; the three that do not balance stay off the map, not guessed.
    A count is drawn only where it is the whole of its shapes: Peshawar's 2023
    shape also took in FR Peshawar, which no glance includes, so its 1998
    count there would undercount the shape and inflate the change since."""
    import duckdb
    drawn, undrawn = duckdb.execute(f"""
        SELECT count(DISTINCT district) FILTER (WHERE map_key IS NOT NULL),
               count(DISTINCT district) FILTER (WHERE map_key IS NULL)
        FROM '{PANEL_1998}' WHERE table_id = 'glance' AND is_rate""").fetchone()
    assert drawn >= 97, f'only {drawn} glance districts linked'
    assert undrawn <= 3, f'{undrawn} glance districts unlinked'
    # every 1998 person on the shapes a count is drawn on is in that count
    bad = duckdb.execute(f"""
        WITH g AS (SELECT DISTINCT district, map_key, map_weight FROM '{PANEL_1998}'
                   WHERE table_id = 'glance' AND NOT is_rate AND map_key IS NOT NULL),
             grp AS (SELECT map_key, sum(map_weight) w FROM g GROUP BY 1),
             t1 AS (SELECT map_key, value FROM '{PANEL_1998}' WHERE table_id = '1'
                    AND unit_type = 'district' AND locality = 'all')
        SELECT map_key, w, (SELECT sum(value) FROM t1 WHERE list_has_any(
                   string_split(t1.map_key, ' '), string_split(grp.map_key, ' '))) AS on_shapes
        FROM grp WHERE round(w) <> round(on_shapes)""").fetchall()
    assert not bad, f'glance counts drawn on shapes they do not fill: {bad[:3]}'
