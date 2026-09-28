"""Electricity distribution losses, by company, 2006-07 to 2024-25.

nepra_disco_annual has been in the warehouse and in the catalogue for as long
as the State page has existed, and the State page said of it: "published and
queryable but not yet a series". The obstacle was real - the table is 20,689
rows of OCR from nineteen editions of NEPRA's State of Industry report, with a
company called `.5 9 E '!f~ 0 0. > I- V"'` and a column headed
`Unit Purchllsed (G111111` - but it is a label problem, not a data problem, and
the label problem is confined to a handful of strings.

What comes out is the single most-quoted number in Pakistani energy policy:
the share of the electricity a distribution company buys that it never bills
anyone for. In 2024-25 that is 8.4 per cent for IESCO, which supplies
Islamabad, and 38.8, 38.4 and 39.0 per cent for PESCO, QESCO and SEPCO. Two of
every five units entering the Peshawar, Quetta and Sukkur networks are lost or
unbilled.

The crosswalk is small. The traps were not, and three would each have put a
wrong line on the chart:

1. THE NATIONAL TOTAL WEARING PESCO'S NAME. The 2024 edition gives PESCO
   2019-20 as 114,359.73 GWh purchased and 92,790.76 sold. Those are the whole
   country's figures - the CPPA-G system total row for that year is
   114,359.48 / 92,790.75 - and PESCO's own, from the 2023 edition, are
   14,750.30 and 9,043.05. Left in, PESCO's loss rate reads 18.9 per cent in
   2019-20, better than LESCO, when it was in fact 37 or 38. Any DISCO row
   within two per cent of the system total is rejected.

2. A PERCENTAGE COLUMN THAT CONTRADICTS ITS OWN ROW. The 2011 edition puts
   PESCO's losses at 56.37, 53.90, 63.65 and 60.97 per cent for 2006-07 to
   2009-10. The GWh figures in the same rows give 32.2, 32.4, 35.2 and 34.7,
   and the 2015 edition restates 2010-11 as 37.96 where 2011 printed 62.35.
   HESCO's published and computed percentages agree to two decimals in exactly
   those years, so this is PESCO's column, not the format. The percentage is
   therefore computed from the two GWh columns in the same row, which can be
   checked, and the published one is only reported as a disagreement.

3. A COMPANY WITH NO LOSS COLUMN AT ALL. Rejecting the fake PESCO 2019-20 left
   the real one unusable, because the 2023 edition prints its purchased and
   sold figures but not the losses between them. Since purchased = sold +
   losses holds in all 178 company-years where all three are printed - to
   within a thousandth - the loss is derived from the other two rather than
   the year being dropped. PESCO 2019-20 is then 38.7 per cent, which sits
   where its neighbours do (36.6 in 2018-19, 38.2 in 2020-21) and not at the
   18.9 the system total was claiming. Derived years are flagged in the
   payload.

4. K-ELECTRIC DOES NOT BUY MOST OF ITS ELECTRICITY. It generates it. Its
   "purchased" figure is grid imports alone - 1,083 GWh in 2024-25 against
   15,249 GWh sold - so purchased minus sold is not its loss and the identity
   every DISCO satisfies is meaningless for it. Its published loss rate is
   kept, its decomposition is not, and the chart marks it.

Usage: build_disco_payload.py --warehouse <dir> --out <js>
"""
import argparse, json, pathlib

import duckdb

DISCOS = ['PESCO', 'TESCO', 'IESCO', 'GEPCO', 'LESCO',
          'FESCO', 'MEPCO', 'HESCO', 'SEPCO', 'QESCO']
KE = {'KE', 'KEL', 'K-ELECTRIC', 'K-Electric Limited', 'KESC'}
SYSTEM = {'Total in CPPA-G System', 'Total in PEPCO System',
          'Total in PEP- CO System'}

# Where each company distributes, because 'QESCO' means nothing to most readers.
AREA = {
    'PESCO': 'Khyber Pakhtunkhwa', 'TESCO': 'the tribal districts',
    'IESCO': 'Islamabad & Rawalpindi', 'GEPCO': 'Gujranwala',
    'LESCO': 'Lahore', 'FESCO': 'Faisalabad', 'MEPCO': 'southern Punjab',
    'HESCO': 'Hyderabad', 'SEPCO': 'Sukkur', 'QESCO': 'Balochistan',
    'K-Electric': 'Karachi',
}

# The OCR spellings of four columns. Anything not here is a breakdown of where
# the units were bought (NTDC, captive plants, net metering), which is a
# different question from how many were lost.
MEASURE = {
    'Unit Purchased (GWh)|Total Unit Purchased': 'purchased',
    'Unit Purchased (GWh)|Unit Purchased': 'purchased',
    'Unit Purchllsed (G111111|Unit Pun:haNd': 'purchased',
    'Unit Sold (GWh)': 'sold',
    'Losses|Unit Sold': 'sold',
    '......|UnltSold': 'sold',
    'Losses|GWh': 'loss_gwh',
    '......|GWh': 'loss_gwh',
    'Losses|%age': 'loss_pct',
    'Losses|Percentage': 'loss_pct',
}


def unit(d):
    if not isinstance(d, str):
        return None
    d = d.strip()
    if d in DISCOS:
        return d
    if d in KE:
        return 'K-Electric'
    if d in SYSTEM:
        return 'ALL'
    return None                      # OCR wreckage and stray year strings


COLS = ['unit', 'fy', 'fy_end', 'edition', 'purchased', 'sold', 'loss_gwh',
        'loss_pct', 'derived']


def losses(warehouse):
    """The crosswalked loss panel, plus what was rejected on the way.

    Returned rather than only written, because the State page draws the same
    series and a second copy of the crosswalk is a second thing to get wrong.
    """
    con = duckdb.connect()
    con.execute('SET threads TO 1')
    W = pathlib.Path(warehouse).as_posix()
    raw = con.sql(f"""
        SELECT disco, fy, report_year, col_label, value
        FROM '{W}/nepra_disco_annual.parquet'
        WHERE series = 'units_purchased_sold_losses' AND value IS NOT NULL
    """).fetchall()

    # (unit, fy, edition) -> {measure: value}
    cells = {}
    for disco, fy, ry, col, val in raw:
        u, m = unit(disco), MEASURE.get(col)
        if u is None or m is None or not fy:
            continue
        cells.setdefault((u, fy, ry), {})[m] = float(val)

    # The system total first, so a DISCO row can be tested against it.
    total = {}
    for (u, fy, ry), c in cells.items():
        if u == 'ALL' and c.get('purchased'):
            total[fy] = max(total.get(fy, 0), c['purchased'])

    def complete(c):
        return all(c.get(k) for k in ('purchased', 'sold', 'loss_gwh'))

    def balances(c):
        """purchased = sold + losses, to within a tenth of a per cent."""
        return abs(c['purchased'] - c['sold'] - c['loss_gwh']) \
            <= 0.001 * c['purchased']

    rows, dropped, disagree = [], [], []
    for u in DISCOS + ['K-Electric', 'ALL']:
        for fy in sorted({k[1] for k in cells if k[0] == u}):
            eds = sorted([k[2] for k in cells if k[0] == u and k[1] == fy],
                         reverse=True)
            pick = None
            for ry in eds:                      # newest edition that holds up
                c = cells[(u, fy, ry)]
                # Trap 1: the national total printed under a company's name.
                if u not in ('ALL',) and c.get('purchased') and total.get(fy) \
                        and abs(c['purchased'] - total[fy]) <= 0.02 * total[fy]:
                    dropped.append((u, fy, ry, 'is the system total'))
                    continue
                if u == 'K-Electric':           # Trap 4: no identity to check
                    if c.get('loss_pct') or complete(c):
                        pick = (ry, c, False)
                        break
                    continue
                if complete(c) and balances(c):
                    pick = (ry, c, False)
                    break
                if complete(c):
                    dropped.append((u, fy, ry, 'purchased != sold + losses'))
                    continue
                # Trap 4: no loss column, but the two columns that bound it.
                if c.get('purchased') and c.get('sold') \
                        and c['sold'] < c['purchased']:
                    c = dict(c, loss_gwh=c['purchased'] - c['sold'])
                    pick = (ry, c, True)
                    break
            if pick is None:
                continue
            ry, c, derived = pick
            # Trap 2: derive the rate, do not take the printed one on trust.
            if u == 'K-Electric':
                pct = c.get('loss_pct')
            else:
                pct = round(100.0 * c['loss_gwh'] / c['purchased'], 2)
                if c.get('loss_pct') is not None \
                        and abs(c['loss_pct'] - pct) > 0.5:
                    disagree.append((u, fy, ry, c['loss_pct'], pct))
            rows.append([
                u, fy, int(fy[:4]) + 1, ry,
                round(c['purchased'], 1) if c.get('purchased') else None,
                round(c['sold'], 1) if c.get('sold') else None,
                round(c['loss_gwh'], 1) if c.get('loss_gwh') else None,
                pct, 1 if derived else 0,
            ])

    return rows, [list(d) for d in dropped], [list(d) for d in disagree]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    rows, dropped, disagree = losses(a.warehouse)
    payload = {
        'losses': {'rows': rows, 'cols': COLS},
        'areas': AREA,
        'rejected': dropped,
        'disagreements': disagree,
    }
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        '/* Data Darbar - electricity distribution losses. Generated by\n'
        '   etl/economy/build_disco_payload.py; do not edit by hand. */\n'
        'window.DD_DISCO=' + json.dumps(payload, separators=(',', ':')) + ';\n')

    got = {r[0] for r in rows}
    assert not (set(DISCOS) - got), f'lost a company: {set(DISCOS) - got}'

    # The check that matters, because it is external to the crosswalk: NEPRA
    # prints its own system total for thirteen of these years, and the ten
    # companies as crosswalked here must add up to it. They do, to within
    # 0.13 per cent in the worst year and exactly in eight of the thirteen.
    # If a company were dropped, duplicated or misnamed, this is where it
    # would show - the identity check inside a row could not see it.
    sys_total = {r[1]: r[4] for r in rows if r[0] == 'ALL' and r[4]}
    worst_fy, worst = None, 0.0
    for fy, published in sys_total.items():
        summed = sum(r[4] for r in rows
                     if r[1] == fy and r[4] and r[0] in DISCOS)
        off = abs(summed - published) / published
        if off > worst:
            worst_fy, worst = fy, off
    assert sys_total, 'no system total to check the companies against'
    assert worst < 0.005, \
        f'companies miss the published system total by {worst:.1%} in {worst_fy}'
    pesco = {r[1]: r[7] for r in rows if r[0] == 'PESCO'}
    assert pesco['2019-20'] > 30, \
        f"the system total is still wearing PESCO's name: {pesco['2019-20']}%"
    assert all(0 < r[7] <= 60 for r in rows if r[7] is not None), \
        'a loss rate outside 0-60% survived'

    print(f'  {len(rows):,} company-years, {len(got)} units, '
          f'{len({r[1] for r in rows})} fiscal years')
    print(f'  companies sum to NEPRA\'s own system total across '
          f'{len(sys_total)} years, worst gap {worst:.2%} ({worst_fy})')
    print(f'  rejected {len(dropped)} rows; '
          f'{len(disagree)} printed percentages contradicted their own GWh; '
          f'{sum(r[8] for r in rows)} losses derived from purchased - sold')
    last = max(r[1] for r in rows)
    worst = sorted([r for r in rows if r[1] == last and r[7]],
                   key=lambda r: -r[7])
    print(f'  {last}: ' + ', '.join(f'{r[0]} {r[7]:.1f}%' for r in worst[:3])
          + ' ... ' + ', '.join(f'{r[0]} {r[7]:.1f}%' for r in worst[-2:]))
    print(f'  -> {out} ({out.stat().st_size/1e3:.1f} KB)')


if __name__ == '__main__':
    main()
