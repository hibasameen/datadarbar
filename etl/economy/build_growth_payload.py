"""Where each year's growth came from, decomposed so that it adds up.

WHAT WAS THERE BEFORE did not add up, and the page explained the gap with a
reason that could not produce it. Contributions were built by multiplying
Table 6's REAL growth rates by Table 7b's CURRENT-price shares - two different
price bases - and the component list carried both "Crops" and "Cotton
Ginning", although ginning is the third thing inside Crops. In FY2022-23 the
displayed components summed to -0.934 percentage points against a headline of
-0.210, and the note beside the chart blamed rounding and the tax bridge
between GVA and GDP. Neither explains 0.72 points, and the compared aggregate
is on the basic-price basis anyway, so there is no bridge to blame.

THIS NEEDS NO WEIGHTS AT ALL. Table 5 is the national accounts at constant
2015-16 prices, in levels, and its eighteen leaf activities sum to GVA exactly
in all 27 years - gap 0.0, not 0.0 after rounding. So each activity's
contribution to growth is just its own change over last year's total:

    contribution_i(t) = (V_i(t) - V_i(t-1)) / GVA(t-1) x 100

and the parts sum to (GVA(t) - GVA(t-1)) / GVA(t-1) x 100, which is GVA
growth, by construction rather than by luck. The build asserts it to 1e-9
percentage points and fails otherwise.

The eighteen are mutually exclusive: the aggregates (A, B, C, D, Commodity
Producing) and the sub-items inside Crops and Manufacturing are excluded, so
nothing is counted twice at the level shown. The three-sector view sums them
by parent rather than reading a separate series, so the two views cannot
disagree.

ONE THING THIS DOES NOT DO. econ_data.js, which holds the rest of this page -
sector shares, industry, budget, input-output, trade - has no builder in the
repo at all: build_structure.py reads a DuckDB file and writes a directory,
and neither exists. This replaces the one block that was demonstrably wrong.
The rest is still a committed artefact nobody can regenerate.

Usage: build_growth_payload.py --warehouse <dir> --out <js>
"""
import argparse, json, pathlib

import duckdb

# The leaf activities of the 2015-16-base accounts, with the aggregate each
# belongs to. The aggregates themselves - A, B, C, D, Commodity Producing -
# and the sub-items inside Crops and Manufacturing are deliberately absent:
# including a parent beside its children is how the old list double counted.
LEAVES = [
    ('crops',      'agri', 'Crops',                  '1. Crops ( i+ii+iii)'),
    ('livestock',  'agri', 'Livestock',              '2.  Livestock'),
    ('forestry',   'agri', 'Forestry',               '3.  Forestry'),
    ('fishing',    'agri', 'Fishing',                '4.  Fishing'),
    ('mining',     'ind',  'Mining & quarrying',     '1.  Mining and Quarrying'),
    ('mfg',        'ind',  'Manufacturing',          '2.  Manufacturing ( i+ii+iii)'),
    ('utilities',  'ind',  'Electricity, gas & water',
     '3   Electricity, Gas and Water supply'),
    ('constr',     'ind',  'Construction',           '4.  Construction'),
    ('trade',      'serv', 'Wholesale & retail trade',
     '1.  Wholesale & Retail trade'),
    ('transport',  'serv', 'Transport & storage',    '2. Transportation & Storage'),
    ('hotels',     'serv', 'Accommodation & food',
     '3. Accommodation and Food Services Activities (Hotels & Restaurants)'),
    ('ict',        'serv', 'Information & communication',
     '4. Information and Communication'),
    ('finance',    'serv', 'Finance & insurance',
     '5.  Financial and Insurance Activities'),
    ('realestate', 'serv', 'Real estate',            '6.  Real Estate Activities (OD)'),
    ('public',     'serv', 'Public administration',
     '7.  Public Administration and Social Security (General Government)'),
    ('education',  'serv', 'Education',              '8. Education'),
    ('health',     'serv', 'Health & social work',
     '9. Human Health and Social Work Activities'),
    ('otherserv',  'serv', 'Other private services',  '10.  Other Private Services'),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    W = pathlib.Path(a.warehouse).as_posix()
    NA = f"'{W}/national_accounts.parquet'"

    def series(item):
        rows = con.execute(
            f"SELECT year, value FROM {NA} "
            "WHERE table_sheet = 'Table 5' AND item = ? AND value IS NOT NULL",
            [item]).fetchall()
        return {y: v for y, v in rows}

    gva = series('D GDP {Total of GVA at bp (A+B+C)')
    years = sorted(gva, key=lambda y: int(y[:4]))
    assert len(years) > 20, f'only {len(years)} years of GVA'

    vals = {key: series(item) for key, _, _, item in LEAVES}
    for key, _, _, item in LEAVES:
        missing = [y for y in years if y not in vals[key]]
        assert not missing, f'{item!r} has no value for {missing}'

    # The leaves must BE the total, or a decomposition of it means nothing.
    for y in years:
        gap = sum(vals[k][y] for k, _, _, _ in LEAVES) - gva[y]
        assert abs(gap) < 1.0, f'{y}: leaves miss GVA by {gap:,.1f} million'

    contrib, total = {}, []
    for key, parent, label, _ in LEAVES:
        contrib[key] = {'label': label, 'parent': parent, 'points': []}
    for prev, y in zip(years, years[1:]):
        base = gva[prev]
        got = 0.0
        for key, _, _, _ in LEAVES:
            pp = (vals[key][y] - vals[key][prev]) / base * 100.0
            contrib[key]['points'].append({'year': y, 'value': round(pp, 3)})
            got += pp
        headline = (gva[y] - gva[prev]) / base * 100.0
        assert abs(got - headline) < 1e-9, \
            f'{y}: parts sum to {got:.12f} against {headline:.12f}'
        total.append({'year': y, 'value': round(headline, 3)})

    # And against what PBS itself publishes as the growth of that aggregate.
    pub = series('D GDP {Total of GVA at bp (A+B+C)')
    t6 = {y: v for y, v in con.execute(
        f"SELECT year, value FROM {NA} WHERE table_sheet = 'Table 6' "
        "AND item = ?", ['D GDP {Total of GVA at bp (A+B+C)']).fetchall()}
    worst, worst_y = 0.0, None
    for p in total:
        if p['year'] in t6 and t6[p['year']] is not None:
            d = abs(p['value'] - t6[p['year']])
            if d > worst:
                worst, worst_y = d, p['year']

    payload = {
        'contrib': contrib,
        'total': total,
        'meta': {
            'basis': 'constant 2015-16 prices, GVA at basic prices',
            'source': 'PBS National Accounts Table 5',
            'method': ('contribution = change in the activity over last '
                       "year's total GVA; the parts sum to GVA growth exactly"),
            'components': len(LEAVES),
        },
    }
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        '/* Data Darbar - growth decomposition. Generated by\n'
        '   etl/economy/build_growth_payload.py; do not edit by hand. */\n'
        'window.DD_GROWTH=' + json.dumps(payload, separators=(',', ':')) + ';\n')

    print(f'  {len(LEAVES)} mutually exclusive activities, '
          f'{len(total)} years of growth')
    print('  leaves sum to GVA in every year, and the parts sum to headline '
          'growth to within 1e-9 pp')
    print(f"  worst gap against PBS's own published growth rate: "
          f'{worst:.3f} pp ({worst_y})')
    bad = [p for p in total if p['year'] == '2022-23']
    if bad:
        print(f"  2022-23 now reads {bad[0]['value']:+.3f} pp "
              f'(the old chart summed its parts to -0.934)')
    print(f'  -> {out} ({out.stat().st_size/1e3:.1f} KB)')


if __name__ == '__main__':
    main()
