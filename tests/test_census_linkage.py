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


def test_weights_sum_to_the_published_2017_total():
    for tier in ('district', 'tehsil'):
        total = sum(float(r['weight']) for r in rows(2017, tier) if r['weight'])
        assert round(total) == PUBLISHED_2017, f'{tier} weights sum to {total:,.0f}'
        assert all(r['weight'] for r in rows(2017, tier)), f'a 2017 {tier} has no weight'
