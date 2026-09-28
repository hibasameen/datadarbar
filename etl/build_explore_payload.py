"""One index of comparable series, across the Economy and State pages.

A READER WHO WANTS TWO NUMBERS ON ONE AXIS COULD NOT GET THEM. Every chart
here is a fixed selection on a fixed page: inflation lives on one, tax heads
on another, and nothing lets you put the policy rate beside manufacturing
output, or one DISCO's losses beside another's, or exports beside imports on
a basis you chose. The pages answer the questions they were built to answer.

This is the index behind an explorer that answers the rest. It is built FROM
THE PAGE PAYLOADS rather than re-derived from the warehouse, which is
deliberate: re-deriving is how the trade totals came to disagree with
themselves, and a series that says something different from the chart it came
from would be a worse defect than not having it. Where a page payload carries
a figure, that figure is what appears here.

Every series declares its own frequency, its basis (fiscal or calendar) and
its unit, because those are the things that make a comparison invalid and the
explorer has to refuse to do arithmetic across them. Fiscal years are plotted
at the year they end, and say so; a fiscal year and a calendar year are never
silently treated as the same point.

Usage: build_explore_payload.py --app <dir> --out <js>
"""
import argparse, json, pathlib, re

# A comparison is only as honest as the unit it is drawn in. kind decides
# which transformations the explorer will offer: an index cannot be indexed
# again, a percentage cannot be summed into a share, and a rate change is in
# points, not per cent.
LEVEL, RATE, INDEX, PCT = 'level', 'rate', 'index', 'pct'


def payload(path, var):
    s = pathlib.Path(path).read_text()
    i = s.index('{')
    j = s.rstrip().rfind('}') + 1
    return json.loads(s[i:j])


def fy_end(fy):
    """'2023-24' -> the date its fiscal year ends, 30 June 2024."""
    m = re.match(r'^(\d{4})-(\d{2})$', str(fy))
    if not m:
        return None
    return f'{int(m.group(1)) + 1}-06-30'


def r(v, dp=3):
    return None if v is None else round(float(v), dp)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--app', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    app = pathlib.Path(a.app)

    series, index = {}, []

    def add(key, label, topic, page, unit, kind, freq, basis, source, pts,
            note=None):
        pts = [(t, r(v)) for t, v in pts if t and v is not None]
        if len(pts) < 3:
            return
        series[key] = [[t, v] for t, v in pts]
        rec = {'key': key, 'label': label, 'topic': topic, 'page': page,
               'unit': unit, 'kind': kind, 'freq': freq, 'basis': basis,
               'source': source, 'n': len(pts),
               'from': pts[0][0], 'to': pts[-1][0]}
        if note:
            rec['note'] = note
        index.append(rec)

    # ── money and prices: the curated SBP series the Economy page draws ──
    # A LABEL THAT ONLY WORKS ON ITS OWN CHART IS NOT A LABEL HERE. The money
    # page names its series for the chart they sit on, where "1 month" is
    # obviously a KIBOR tenor and "National" is obviously CPI. In a list of
    # 106 series from both pages they are ambiguous, so each gets the family
    # it belongs to. SBP's own full name is kept as the note underneath.
    FAMILY = [('kib_', 'KIBOR'), ('cpi_', 'CPI'), ('res_', 'Reserves'),
              ('pol_', 'Policy rates'), ('npl_', 'Banking')]
    ONE_OFF = {'m1': 'Money supply', 'm2': 'Money supply', 'm3': 'Money supply',
               'notes': 'Money supply', 'lend': 'Bank rates',
               'depo': 'Bank rates', 'spi': 'Prices', 'wpi': 'Prices',
               'gx': 'Balance of payments', 'gm': 'Balance of payments',
               'sx': 'Balance of payments', 'sm': 'Balance of payments',
               'ca': 'Balance of payments', 'remit_bop': 'Balance of payments',
               'pic': 'Balance of payments', 'pid': 'Balance of payments'}

    def qualify(k, label):
        fam = ONE_OFF.get(k)
        if not fam:
            for pre, f in FAMILY:
                if k.startswith(pre):
                    fam = f
                    break
        if not fam or label.lower().startswith(fam.lower()):
            return label
        # M2 and NPL are not sentences: only lower the first letter when the
        # second one is already lower, which leaves acronyms alone.
        rest = label if (len(label) > 1 and not label[1].islower()) \
            else label[0].lower() + label[1:]
        return f'{fam}, {rest}'

    M = payload(app / 'data/money_data.js', 'DD_MONEY')
    for k, pts in M['series'].items():
        meta = M['meta'].get(k, {})
        unit = meta.get('unit') or ''
        kind = (INDEX if unit == 'Index' else
                RATE if unit in ('Percent', '%') or k.startswith(('kib_', 'pol_'))
                else LEVEL)
        add('sbp:' + k, qualify(k, meta.get('label') or k),
            'Money & prices', 'economy', unit or 'level', kind,
            meta.get('freq') or 'Monthly', 'calendar',
            'sbp_observations', pts,
            note=meta.get('name'))

    # ── trade: the published totals ──────────────────────────────────────
    T = payload(app / 'data/trade_data.js', 'DD_TRADE')['totals']
    for k, lab in (('export', 'Exports'), ('import', 'Imports'),
                   ('balance', 'Trade balance')):
        add('trade:' + k, lab + ' (goods)', 'Trade', 'economy',
            'Rs billion', LEVEL, 'Annual', 'fiscal', 'trade_monthly_totals',
            [(fy_end(y), T[k][y]) for y in T['years']],
            note='Published grand total, not the sum of product detail')

    # ── national accounts ────────────────────────────────────────────────
    MA = payload(app / 'data/macro_data.js', 'DD_MACRO')['headline']
    ci = {c: i for i, c in enumerate(MA['cols'])}
    for f, lab, unit in (('gva_tn', 'GVA at basic prices', 'Rs trillion'),
                         ('gdp_tn', 'GDP at market prices', 'Rs trillion'),
                         ('gni_tn', 'GNI', 'Rs trillion'),
                         ('pci17', 'Income per person (2017 census base)', 'Rs'),
                         ('pci23', 'Income per person (2023 census base)', 'Rs'),
                         ('usd', 'Rupees per US dollar (annual average)', 'PKR')):
        add('na:' + f, lab, 'National accounts', 'economy', unit, LEVEL,
            'Annual', 'fiscal', 'national_accounts',
            [(fy_end(x[ci['fy']]), x[ci[f]]) for x in MA['rows']],
            note='Constant 2015-16 prices' if f.endswith('_tn') or
                 f.startswith('pci') else None)

    # ── industry ─────────────────────────────────────────────────────────
    E = payload(app / 'assets/js/econ_data.js', 'ECON')
    trend = (E.get('industry') or {}).get('trend') or []
    add('qim:overall', 'Manufacturing output (QIM)', 'Industry', 'economy',
        'Index 2015-16 = 100', INDEX, 'Monthly', 'calendar', 'lsm_qim',
        [((p['month'] + '-01') if len(str(p.get('month', ''))) == 7 else None,
          p.get('qim')) for p in trend])

    # ── the State page ───────────────────────────────────────────────────
    S = payload(app / 'data/state_data.js', 'DD_STATE')

    def block(name):
        b = S.get(name) or {}
        cols = b.get('cols') or []
        return cols, (b.get('rows') or [])

    cols, rows_ = block('tax')
    if cols:
        ci = {c: i for i, c in enumerate(cols)}
        heads = sorted({x[ci['head']] for x in rows_})
        for h in heads:
            add('tax:' + h, h.title() + ' tax collected', 'Public money',
                'state', 'Rs million', LEVEL, 'Annual', 'fiscal',
                'fbr_tax_collection',
                sorted((fy_end(x[ci['fy']]), x[ci['pkr_mn']])
                       for x in rows_ if x[ci['head']] == h),
                note='Nominal rupees. Absent years are absent, not zero.')

    cols, rows_ = block('discos')
    if cols:
        ci = {c: i for i, c in enumerate(cols)}
        units = sorted({x[ci['unit']] for x in rows_ if x[ci['unit']] != 'ALL'})
        for u in units:
            add('disco:' + u, u + ' T&D loss rate', 'Energy', 'state',
                'per cent of units entering the system', RATE, 'Annual',
                'fiscal', 'nepra_disco_annual',
                sorted((fy_end(x[ci['fy']]), x[ci['loss_pct']])
                       for x in rows_
                       if x[ci['unit']] == u and x[ci['loss_pct']] is not None),
                note='K-Electric is on its own available-energy base'
                     if u == 'K-Electric' else None)

    cols, rows_ = block('courts')
    if cols:
        ci = {c: i for i, c in enumerate(cols)}
        provs = sorted({x[ci['province']] for x in rows_})
        for p in provs:
            for f, lab, unit, kind in (('pending_end', 'cases pending', 'cases', LEVEL),
                                       ('instituted', 'cases instituted', 'cases', LEVEL),
                                       ('disposed', 'cases disposed', 'cases', LEVEL),
                                       ('clearance_pct', 'clearance rate', 'per cent', RATE)):
                if f not in ci:
                    continue
                add(f'courts:{p}:{f}', f'{p} — {lab}', 'Justice', 'state',
                    unit, kind, 'Annual', 'calendar', 'ljcp_case_flows',
                    sorted((f'{x[ci["year"]]}-12-31', x[ci[f]]) for x in rows_
                           if x[ci['province']] == p
                           and x[ci['category']] == 'all'
                           and x[ci[f]] is not None))

    cols, rows_ = block('crime')
    if cols:
        ci = {c: i for i, c in enumerate(cols)}
        regs = sorted({x[ci['region']] for x in rows_})
        for g in regs:
            add('crime:' + g, g + ' — reported cases', 'Crime & policing',
                'state', 'cases', LEVEL, 'Annual', 'calendar',
                'police_crime_annual',
                sorted((f'{x[ci["year"]]}-12-31', x[ci['value']])
                       for x in rows_ if x[ci['region']] == g
                       and x[ci['value']] is not None),
                note='Offences reported to police, not offences committed')

    index.sort(key=lambda x: (x['page'], x['topic'], x['label']))
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        '/* Data Darbar - the series index behind the explorer. Generated by\n'
        '   etl/build_explore_payload.py; do not edit by hand. */\n'
        'window.DD_EXPLORE=' + json.dumps({'index': index, 'series': series},
                                          separators=(',', ':')) + ';\n')

    by_page = {}
    for s in index:
        by_page.setdefault(s['page'], []).append(s)
    assert len(index) > 60, f'only {len(index)} series'
    for s in index:
        assert s['unit'] and s['kind'] and s['freq'], s['key']
    print(f'  {len(index)} series, {sum(s["n"] for s in index):,} observations')
    for p, v in sorted(by_page.items()):
        topics = {}
        for s in v:
            topics[s['topic']] = topics.get(s['topic'], 0) + 1
        print(f'  {p:8} {len(v):3}  ' +
              ', '.join(f'{t} {n}' for t, n in sorted(topics.items())))
    print(f'  -> {out} ({out.stat().st_size/1e3:.0f} KB)')


if __name__ == '__main__':
    main()
