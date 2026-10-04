"""The long-run Economy charts: money and prices since 1950, real GDP since FY50,
public finances since 1950, and Pakistan beside its peers.

Reads the three warehouse tables built from the sources the Global Macro
Database compiles (etl/macro_history): sbp_handbook_series, imf_pakistan_fiscal
and wdi_comparators. Writes app/data/history_data.js (window.DD_HISTORY) in
money.js's shape: series as [[date, value], ...], dated mid-year because every
one of them is annual.

What is the publisher's and what is ours is kept apart, because the chart
says so:
  published  SBP's real GDP and its growth rate (Handbook 1.5), SBP's
             consolidated budget ratios (3.7), every IMF and World Bank figure
  derived    broad and reserve money growth and CPI inflation, computed here
             from SBP's levels and indices (4.1, 2.8) - and only WITHIN a
             printed block and a single index base, so a change of definition
             or base is a gap in the line, never a jump in it.

    python3 etl/economy/build_history_payload.py --warehouse app/data/warehouse \
        --out app/data/history_data.js
"""
import argparse, json, pathlib, re

import duckdb

PEER_INDICATORS = [
    # code, short label, unit, decimals
    ('NY.GDP.PCAP.PP.KD', 'GDP per person, PPP', 'constant 2021 international $', 0),
    ('NY.GDP.MKTP.KD.ZG', 'GDP growth', '% a year', 1),
    ('FP.CPI.TOTL.ZG', 'Inflation', '% a year, consumer prices', 1),
    ('GC.TAX.TOTL.GD.ZS', 'Tax revenue', '% of GDP, central government', 1),
    ('NE.EXP.GNFS.ZS', 'Exports', '% of GDP, goods and services', 1),
    ('BX.TRF.PWKR.DT.GD.ZS', 'Remittances received', '% of GDP', 1),
    ('NE.GDI.TOTL.ZS', 'Investment', '% of GDP, gross capital formation', 1),
    ('SL.TLF.CACT.FE.ZS', 'Women in the labour force', '% of women 15+, ILO model', 1),
    ('SP.DYN.LE00.IN', 'Life expectancy', 'years at birth', 1),
    ('SH.DYN.MORT', 'Under-5 mortality', 'per 1,000 live births', 1),
    ('SP.DYN.TFRT.IN', 'Fertility', 'births per woman', 2),
    ('EG.ELC.ACCS.ZS', 'Access to electricity', '% of population', 1),
]
PEERS = ['PAK', 'IND', 'BGD', 'LKA', 'NPL', 'AFG', 'IRN', 'EGY', 'IDN', 'NGA', 'TUR',
         'SAS', 'LMC', 'WLD']


def mid(y):
    return f'{int(y)}-06-30'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    W = pathlib.Path(a.warehouse)
    con = duckdb.connect()
    hb = (W / 'sbp_handbook_series.parquet').as_posix()
    imf = (W / 'imf_pakistan_fiscal.parquet').as_posix()
    wdi = (W / 'wdi_comparators.parquet').as_posix()
    S, M = {}, {}

    def put(key, pts, **meta):
        assert pts, f'{key}: no points'
        S[key] = pts
        M[key] = {'freq': 'Annual', **meta}

    # ── real GDP since FY50 (SBP Handbook 1.5, as published) ──────────────
    rows = con.sql(f"""SELECT year, series, value FROM '{hb}'
        WHERE table_id = '1.5' AND value IS NOT NULL ORDER BY year""").fetchall()
    lvl = [(y, v) for y, s, v in rows if s.startswith('At Constant')]
    gro = [(y, v) for y, s, v in rows if s.startswith('Growth')]
    put('gdp_level', [[mid(y), v] for y, v in lvl], unit='million rupees, 2005-06 prices',
        derived=False)
    put('gdp_growth', [[mid(y), round(v, 2)] for y, v in gro], unit='% a year', derived=False)

    # ── money and prices since 1950 (SBP Handbook 4.1, 2.8): growth, ours ─
    def money(prefix):
        """Year-on-year growth of an aggregate, computed within each printed
        block from its annual rows, or its June rows once SBP prints half
        years. A NULL closes the line at every break between blocks."""
        pts = []
        for block in (1, 2, 3):
            got = con.execute(f"""SELECT year, month, value FROM '{hb}'
                WHERE table_id = '4.1' AND block = ? AND series LIKE ? AND value IS NOT NULL
                ORDER BY year, month""", [block, prefix + '%']).fetchall()
            yearly = {}
            for y, m, v in got:
                if m in (None, 6):                      # annual rows, or end-June
                    yearly[(y, m)] = v
            keys = sorted(yearly)
            seg = []
            for (y0, m0), (y1, m1) in zip(keys, keys[1:]):
                # same footing and one year apart: 1990 (month unstated) to
                # 1991 June is not a year-on-year change, so it is skipped
                # ...and 1971 to 1972 is not one either: 1971 is Pakistan with
                # its east wing and 1972 is not, so the "growth" is a border.
                if y1 - y0 == 1 and m0 == m1 and y1 != 1972:
                    seg.append([mid(y1), round((yearly[(y1, m1)] / yearly[(y0, m0)] - 1) * 100, 2)])
                elif seg:
                    seg.append([mid(y1), None])
            if block == 3 and pts:
                # blocks 2 and 3 overlap from 2003; the later definition takes
                # over where it starts, so the earlier one stops there
                start = seg[0][0] if seg else None
                if start:
                    pts = [p for p in pts if p[0] < start]
            if pts and seg:
                pts.append([seg[0][0][:4] + '-01-01', None])     # a visible break
            pts += seg
        return pts

    put('m2_growth', money('Broad Money'), unit='% a year', derived=True)
    put('m0_growth', money('Reserve Money'), unit='% a year', derived=True)

    cpi = con.sql(f"""SELECT year, base, value FROM '{hb}'
        WHERE table_id = '2.8' AND series = 'Consumer Price Index' AND value IS NOT NULL
        ORDER BY year""").fetchall()
    infl = []
    for (y0, b0, v0), (y1, b1, v1) in zip(cpi, cpi[1:]):
        # inflation is read only between two years on the same base: across a
        # rebasing the index jumps by the change of base, not of prices
        if y1 - y0 == 1 and b0 == b1 and b0:
            infl.append([mid(y1), round((v1 / v0 - 1) * 100, 2)])
        else:
            infl.append([mid(y1), None])
    put('cpi_infl', infl, unit='% a year', derived=True)

    # ── public finances since 1950 (IMF; SBP 3.7), as published ──────────
    for key, code in [('imf_rev', 'rev'), ('imf_exp', 'exp'), ('imf_pb', 'pb'),
                      ('imf_ie', 'ie'), ('imf_debt', 'd')]:
        pts = con.execute(f"""SELECT year, value FROM '{imf}' WHERE indicator = ?
            AND NOT coalesce(is_projection, false) ORDER BY year""", [code]).fetchall()
        put(key, [[mid(y), round(v, 2)] for y, v in pts], unit='% of GDP', derived=False)
    for key, name in [('sbp_rev', 'Total Revenue (% of GDP)'),
                      ('sbp_exp', 'Total Expenditure (% of GDP)'),
                      ('sbp_fb', 'Fiscal Balance (% of GDP)')]:
        pts = con.execute(f"""SELECT year, value FROM '{hb}' WHERE table_id = '3.7'
            AND series = ? AND value IS NOT NULL ORDER BY year""", [name]).fetchall()
        put(key, [[mid(y), round(v, 2)] for y, v in pts], unit='% of GDP', derived=False)

    # ── Pakistan and its peers (World Bank WDI), as published ───────────
    names = dict(con.sql(f"SELECT DISTINCT country_code, country FROM '{wdi}'").fetchall())
    # the World Bank's code for lower-middle income comes back as XN
    peers = [c if c in names else {'LMC': 'XN'}.get(c, c) for c in PEERS]
    peers = [c for c in peers if c in names]
    pdata = {}
    for code, _, _, _ in PEER_INDICATORS:
        by = {}
        for c, y, v in con.execute(f"""SELECT country_code, year, value FROM '{wdi}'
                WHERE indicator = ? ORDER BY year""", [code]).fetchall():
            by.setdefault(c, []).append([mid(y), round(v, 3)])
        if 'PAK' not in by:
            # A comparison with no Pakistan line is not one. The World Bank has
            # no central-government tax series for Pakistan; the IMF and SBP
            # fiscal charts carry tax and revenue instead.
            print(f'  {code}: no Pakistan series in WDI, left out')
            continue
        pdata[code] = by
    out = {
        'generated_from': ['sbp_handbook_series', 'imf_pakistan_fiscal', 'wdi_comparators'],
        'series': S, 'meta': M,
        'peers': {
            'indicators': [{'code': c, 'label': l, 'unit': u, 'dp': d}
                           for c, l, u, d in PEER_INDICATORS if c in pdata],
            'countries': [{'code': c, 'name': names[c]} for c in peers],
            'data': pdata,
        },
    }
    dest = pathlib.Path(a.out)
    dest.write_text('/* Data Darbar - the long-run Economy charts. Generated by\n'
                    '   etl/economy/build_history_payload.py; do not edit by hand. */\n'
                    'window.DD_HISTORY=' + json.dumps(out, separators=(',', ':')) + ';\n')
    print(f'{dest}: {len(S)} series, {len(pdata)} peer indicators x {len(peers)} countries, '
          f'{dest.stat().st_size / 1e3:.0f} KB')


if __name__ == '__main__':
    main()
