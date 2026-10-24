"""Quarterly GVA by activity from PBS's QNA Excel tables, in the portal pull's shape.

The 2026-09-27 pull took the quarterly national accounts from the
na.data.gov.pk dashboard (/GDP/getGDPbreakdown/?months=3...). On 2026-10-24
the dashboard still stopped at FY2024-25 Q4 on the pre-118th-NAC vintage, so
the FY2025-26 quarters and the Sep-2026 revisions are read here from the
workbook PBS published with the 118th NAC instead:

  https://www.pbs.gov.pk/wp-content/uploads/2020/07/QNA-Excel-Tables-Q4-2025-26-for-web-Sep-2026.xlsx

Sheet 'QNA Table 2 Constant' (constant basic prices of 2015-16, Rs million,
seasonally unadjusted) has one column per quarter, FY2015-16 Q1 onwards, and
one row per activity with the sector and sub-sector subtotals between them.
Only the 22 leaf activities are kept, mapped onto the sector / subsector /
category labels the portal serves, so the CSV is a drop-in replacement for
gdp_by_activity_quarterly_constant_2016_2025.csv (same name and columns; the
web build only needs its folder constant changed). The GVA total row goes to
gdp_quarterly_constant_2016_2025.csv, which on the portal is labelled GDP but
is GVA at basic prices.

status is 1 on every row, as it was on every portal row; the workbook carries
no flag of its own.

Checks (asserted): every leaf found exactly once per quarter; the leaves of
every quarter add up to the workbook's own 'Total of GVA (A+B+C)' row;
the sector subtotals match their leaves.

Usage: python3 -I build_qna_from_xlsx.py --xlsx QNA.xlsx --out DIR
"""
import argparse, csv, pathlib, re

import openpyxl

SHEET = 'QNA Table 2 Constant'

# normalised workbook label -> (sector, subsector, category) as the portal serves them
LEAVES = {
    'important crops': ('Agriculture', 'Crops', 'Important Crops'),
    'other crops': ('Agriculture', 'Crops', 'Other Crops'),
    'cotton ginning': ('Agriculture', 'Crops', 'Cotton Ginning'),
    'livestock': ('Agriculture', 'Livestock', 'Livestock'),
    'forestry': ('Agriculture', 'Forestry', 'Forestry'),
    'fishing': ('Agriculture', 'Fishing', 'Fishing'),
    'mining & quarrying': ('Industry', 'Mining & Quarrying', 'Mining & Quarrying'),
    'large scale': ('Industry', 'Manufacturing', 'Large Scale'),
    'small scale': ('Industry', 'Manufacturing', 'Small Scale'),
    'slaughtering': ('Industry', 'Manufacturing', 'Slaughtering'),
    'electricity, gas and water supply': ('Industry', 'Electricity, gas and water supply',
                                          'Electricity, gas and water supply'),
    'construction': ('Industry', 'Construction', 'Construction'),
    'wholesale & retail trade': ('Service', 'Wholesale & Retail Trade', 'Wholesale & Retail Trade'),
    'transport & storage': ('Service', 'Transport& Storage', 'Transport& Storage'),
    'accomodation and food services activities (hotels & restaurants)':
        ('Service', 'Accomodation and Food Services Activities (Hotels & Restaurants)',
         'Hotels & Restaurants'),
    'information and communication': ('Service', 'Information and Communication',
                                      'Information and Communication'),
    'finance & insurance activities': ('Service', 'Finance & Insurance Activities',
                                       'Finance & Insurance Activities'),
    'real estate activities (ownership of dwellings)':
        ('Service', 'Real Estate Activities (Ownership of Dwellings)', 'Real Estate Activities'),
    'public administration and social security (general government)':
        ('Service', 'Public Administration and Social Security (General Government)',
         'General Government'),
    'education': ('Service', 'Education', 'Education'),
    'human health and social work activities': ('Service', 'Human Health and Social Work Activities',
                                                'Human Health and Social Work Activities'),
    'other private services': ('Service', 'Other Private Services', 'Other Private Services'),
}
SUBTOTALS = {'agriculture sector (1 to 4)': 'Agriculture', 'industrial sector (1 to 4)': 'Industry',
             'services sector (1 to 10)': 'Service'}
TOTAL = 'total of gva (a+b+c)'


def norm(s):
    s = re.sub(r'^\s*(\d+\.|[ivx]+\.)\s*', '', str(s))       # '1. ', 'iii. '
    s = re.sub(r'\s*\([ivx+]+\)\s*$', '', s)                   # 'Crops (i+ii+iii)'
    s = re.sub(r'\s*&\s*', ' & ', s)                           # 'Transport& Storage'
    return re.sub(r'\s+', ' ', s).strip().lower()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--xlsx', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    wb = openpyxl.load_workbook(a.xlsx, read_only=True, data_only=True)
    g = [list(r) for r in wb[SHEET].iter_rows(values_only=True)]
    wb.close()

    # header rows: 'FY 2015-16' (forward-filled over its four columns) and Q1..Q4
    # (the title row also names '2015-16', as the price base, so ask for several)
    hi = next(i for i, r in enumerate(g)
              if sum(bool(re.search(r'\d{4}-\d{2}', str(c))) for c in r if c) >= 3)
    cols, fy = [], None
    for j, (h, q) in enumerate(zip(g[hi], g[hi + 1])):
        m = re.search(r'(\d{4})-(\d{2})', str(h)) if h else None
        if m:
            fy = f'{m.group(1)}-{m.group(2)}'
        qm = re.fullmatch(r'Q([1-4])', str(q).strip()) if q else None
        if fy and qm:
            cols.append((j, fy, int(qm.group(1))))
    assert len(cols) % 4 == 0 and cols[0][1:] == ('2015-16', 1), cols[:2]

    leaves, subt, total = {}, {}, None
    for r in g[hi + 2:]:
        lab = norm(r[1]) if r[1] else ''
        vals = [r[j] for j, _, _ in cols]
        if lab in LEAVES:
            assert lab not in leaves, f'{lab!r} twice'
            leaves[lab] = vals
        elif lab in SUBTOTALS:
            subt[SUBTOTALS[lab]] = vals
        elif lab == TOTAL:
            total = vals
    missing = set(LEAVES) - set(leaves)
    assert not missing, f'not found in the workbook: {missing}'
    assert total and len(subt) == 3

    for k, (j, fy, q) in enumerate(cols):
        s = sum(leaves[l][k] for l in LEAVES)
        assert abs(s - total[k]) < 1.0, f'{fy} Q{q}: leaves {s:,.1f} vs total {total[k]:,.1f}'
        for sec, v in subt.items():
            ss = sum(leaves[l][k] for l in LEAVES if LEAVES[l][0] == sec)
            assert abs(ss - v[k]) < 1.0, f'{fy} Q{q} {sec}: {ss:,.1f} vs {v[k]:,.1f}'

    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    order = sorted(LEAVES, key=lambda l: LEAVES[l])           # sector, subsector, category
    with open(out / 'gdp_by_activity_quarterly_constant_2016_2025.csv', 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['fy', 'fy_end', 'quarter', 'sector', 'subsector', 'category',
                    'gva_constant_pkr_mn', 'status'])
        for k, (j, fy, q) in enumerate(cols):
            for l in order:
                w.writerow([fy, int(fy[:4]) + 1, q, *LEAVES[l], repr(float(leaves[l][k])), 1])
    with open(out / 'gdp_quarterly_constant_2016_2025.csv', 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['fy', 'fy_end', 'quarter', 'gdp_constant_pkr_mn', 'status'])
        for k, (j, fy, q) in enumerate(cols):
            w.writerow([fy, int(fy[:4]) + 1, q, repr(float(total[k])), 1])
    print(f'  {len(cols)} quarters, {cols[0][1]} Q1 to {cols[-1][1]} Q{cols[-1][2]}; '
          f'{len(cols) * len(LEAVES)} activity rows; leaves = GVA total in every quarter')
    print(f'  -> {out}')


if __name__ == '__main__':
    main()
