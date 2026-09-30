"""Sector shares and real growth, rebuilt from the warehouse.

This block - the seven-decade share chart, the year's sector mix and the
sub-sector growth series - lived only in econ_data.js, a 2.87 MB file that
nothing in the repo produced. build_structure.py, the script it was named
after, reads a DuckDB file at the repo root and writes an econ_data/
directory, and neither has existed for some time. So the page's opening chart
rested on an artefact that could not be checked against its source, refreshed
when PBS published, or corrected when it was wrong.

It is all in national_accounts:

  Table-1   real growth for GDP, agriculture, manufacturing and services,
            1951-52 onward - the long arc
  Table 6   real growth by activity, 2000-01 onward, including the industrial
            aggregate the long series has no row for
  Table 7b  sectoral shares of GVA at current prices, 1999-00 onward, where
            agriculture plus industry plus services is exactly 100

THE BACKCAST IS STILL A BACKCAST. Shares before 1999-00 are not published.
They are carried back from the 1999-00 shares using PBS's own real growth
rates, which ignores every relative-price shift and rebasing in between, so
they are indicative and the chart draws them dashed. Rebuilding them here
does not make them observations; it makes them reproducible.

Usage: build_structure_payload.py --warehouse <dir> --out <js>
"""
import argparse, json, pathlib

import duckdb

# Table-1: the long real-growth series, one row per sector per year.
LONG = {'gdp': 'GDP', 'agri': 'Agriculture',
        'mfg': 'Manufacturing', 'serv': 'Services Sector'}

# Table 7b: shares of GVA at current prices. The three aggregates plus the
# leaves the year-mix chart breaks them into.
SHARES = [
    ('agri', 'Agriculture', 'A. Agriculture, Forestry and Fishing     ( 1 to 4 )'),
    ('ind', 'Industry', 'B. Industrial Activities ( 1 to 4 )'),
    ('serv', 'Services', 'C. Services ( 1 to 10)'),
    ('crops', 'Crops', '1. Crops ( i+ii+iii)'),
    ('livestock', 'Livestock', '2.  Livestock'),
    ('mfg', 'Manufacturing', '2.  Manufacturing ( i+ii+iii)'),
    ('lsm', 'Large-scale manufacturing', 'i)    Large Scale'),
    ('ssm', 'Small-scale manufacturing', 'ii)   Small Scale'),
    ('constr', 'Construction', '4.  Construction'),
    ('mining', 'Mining & quarrying', '1.  Mining and Quarrying'),
    ('utilities', 'Electricity, gas & water', '3   Electricity, Gas and Water supply'),
    ('trade', 'Wholesale & retail trade', '1.  Wholesale & Retail trade'),
    ('transport', 'Transport & storage', '2. Transportation & Storage'),
    ('ict', 'Information & communication', '4. Information and Communication'),
    ('finance', 'Finance & insurance', '5.  Financial and Insurance Activities'),
    ('realestate', 'Real estate', '6.  Real Estate Activities (OD)'),
    ('public', 'Public administration',
     '7.  Public Administration and Social Security (General Government)'),
    ('education', 'Education', '8. Education'),
    ('health', 'Health', '9. Human Health and Social Work Activities'),
]

# Table 6: real growth by activity. Cotton ginning is filed under agriculture
# here because that is where PBS's own commentary discusses it, even though
# the accounts nest it inside Crops; the growth chart shows a rate, not a
# contribution, so nothing is double counted by listing it.
SUB = [
    ('crops', 'agri', 'Crops', '1. Crops ( i+ii+iii)'),
    ('livestock', 'agri', 'Livestock', '2.  Livestock'),
    ('forestry', 'agri', 'Forestry', '3.  Forestry'),
    ('fishing', 'agri', 'Fishing', '4.  Fishing'),
    ('ginning', 'agri', 'Cotton ginning', 'iii) Cotton Ginning'),
    ('mining', 'ind', 'Mining & quarrying', '1.  Mining and Quarrying'),
    ('lsm', 'ind', 'Large-scale manufacturing', 'i)    Large Scale'),
    ('ssm', 'ind', 'Small-scale manufacturing', 'ii)   Small Scale'),
    ('slaughter', 'ind', 'Slaughtering', 'iii)  Slaughtering'),
    ('constr', 'ind', 'Construction', '4.  Construction'),
    ('utilities', 'ind', 'Electricity, gas & water',
     '3   Electricity, Gas and Water supply'),
    ('trade', 'serv', 'Wholesale & retail trade', '1.  Wholesale & Retail trade'),
    ('transport', 'serv', 'Transport & storage', '2. Transportation & Storage'),
    ('hotels', 'serv', 'Hotels & restaurants',
     '3. Accommodation and Food Services Activities (Hotels & Restaurants)'),
    ('ict', 'serv', 'Information & communication', '4. Information and Communication'),
    ('finance', 'serv', 'Finance & insurance', '5.  Financial and Insurance Activities'),
    ('realestate', 'serv', 'Real estate', '6.  Real Estate Activities (OD)'),
    ('public', 'serv', 'Public administration',
     '7.  Public Administration and Social Security (General Government)'),
    ('education', 'serv', 'Education', '8. Education'),
    ('health', 'serv', 'Health & social work',
     '9. Human Health and Social Work Activities'),
    ('otherserv', 'serv', 'Other private services', '10.  Other Private Services'),
]
IND_GROWTH = 'B Industrial Activities ( 1 to 4 )'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    W = pathlib.Path(a.warehouse).as_posix()
    NA = f"'{W}/national_accounts.parquet'"

    def pts(sheet, item):
        rows = con.execute(
            f"SELECT year, value FROM {NA} WHERE table_sheet = ? AND item = ? "
            "AND value IS NOT NULL", [sheet, item]).fetchall()
        rows.sort(key=lambda r: int(r[0][:4]))
        return [{'year': y, 'value': round(v, 3)} for y, v in rows]

    growth = {k: pts('Table-1', item) for k, item in LONG.items()}
    for k, item in LONG.items():
        assert len(growth[k]) > 60, f'{item!r} has only {len(growth[k])} years'
    growth['ind'] = pts('Table 6', IND_GROWTH)
    assert growth['ind'], 'no industrial growth series'

    shares, labels = {}, {}
    for key, label, item in SHARES:
        shares[key] = pts('Table 7b', item)
        labels[key] = label
        assert shares[key], f'no share series for {item!r}'

    # The three aggregates are the whole of GVA, so they must come to 100.
    tot = {}
    for key in ('agri', 'ind', 'serv'):
        for p in shares[key]:
            tot[p['year']] = tot.get(p['year'], 0.0) + p['value']
    off = {y: v for y, v in tot.items() if abs(v - 100) > 0.05}
    assert not off, f'shares do not sum to 100 in {off}'

    sub, sub_labels, sub_parent = {}, {}, {}
    for key, parent, label, item in SUB:
        sub[key] = pts('Table 6', item)
        sub_labels[key], sub_parent[key] = label, parent
        assert sub[key], f'no growth series for {item!r}'

    # ── the backcast, and it is still a backcast ──────────────────────────
    # Walk the published 1999-00 shares backwards with the published real
    # growth rates. Relative prices and rebasings are ignored, which is why
    # the chart draws these dashed and the note calls them indicative.
    anchor = {k: shares[k][0] for k in ('agri', 'serv', 'mfg')}
    anchor_year = anchor['agri']['year']
    gyears = [p['year'] for p in growth['gdp']]
    back_years = [y for y in gyears if int(y[:4]) < int(anchor_year[:4])]
    rate = {k: {p['year']: p['value'] for p in growth[k]}
            for k in ('agri', 'serv', 'mfg', 'gdp')}
    backcast = {k: [] for k in ('agri', 'serv', 'mfg')}
    cur = {k: anchor[k]['value'] for k in backcast}
    for y in reversed(back_years):
        nxt = gyears[gyears.index(y) + 1]
        for k in backcast:
            g, gg = rate[k].get(nxt), rate['gdp'].get(nxt)
            if g is None or gg is None:
                continue
            # share(t-1) = share(t) * (1+gdp growth) / (1+sector growth)
            cur[k] = cur[k] * (1 + gg / 100.0) / (1 + g / 100.0)
            backcast[k].append({'year': y, 'value': round(cur[k], 3)})
    for k in backcast:
        backcast[k].reverse()

    payload = {
        'growth': growth, 'shares': shares, 'labels': labels,
        'backcast': backcast, 'growth_sub': sub,
        'growth_sub_labels': sub_labels, 'growth_sub_parent': sub_parent,
        'meta': {
            'growth_src': 'PBS National Accounts, Macro Economic Indicators '
                          '(Table-1), real growth %',
            'shares_src': 'PBS National Accounts 2015-16 base, Sectoral Shares '
                          '(Table 7b), % of GVA at current prices',
            'sub_src': 'PBS National Accounts, real growth by activity (Table 6)',
            'backcast_note': 'Before 1999-00, carried back from the published '
                             '1999-00 shares using PBS real growth rates. '
                             'Relative-price shifts and rebasings are ignored: '
                             'indicative, not observed.',
        },
    }
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        '/* Data Darbar - sector shares and real growth. Generated by\n'
        '   etl/economy/build_structure_payload.py; do not edit by hand. */\n'
        'window.DD_STRUCTURE=' + json.dumps(payload, separators=(',', ':')) + ';\n')

    print(f"  long growth {growth['gdp'][0]['year']} to "
          f"{growth['gdp'][-1]['year']} ({len(growth['gdp'])} years)")
    print(f"  {len(shares)} share series from {shares['agri'][0]['year']}; "
          f'agriculture + industry + services = 100 in every year')
    print(f'  {len(sub)} activity growth series; '
          f"{len(backcast['agri'])} backcast years, marked indicative")
    print(f'  -> {out} ({out.stat().st_size/1e3:.1f} KB)')


if __name__ == '__main__':
    main()
