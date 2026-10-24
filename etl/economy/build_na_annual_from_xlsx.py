"""Annual GVA by activity and the long growth series, on the 118th NAC.

The na.data.gov.pk portal had not taken the 118th NAC release (19 October
2026) by the 2026-10-24 refresh, so its annual CSVs were still the May-2026
vintage. This rebuilds the two that the release changes from the annual
tables PBS published with it (National Accounts Annual Tables 2025-26,
updated Sep 2026), as parsed into the desktop warehouse's national_accounts
table, keeping the portal files' names, columns and labels so the web build
only reads a different folder:

  gdp_by_activity_annual_constant_2000_2026.csv
      Table 5 (constant basic prices of 2015-16, Rs million), the 22 leaf
      activities 1999-00 to 2025-26. Each portal category is matched to the
      one Table 5 row equal to it in every year to 2019-20, which the 118th
      NAC did not revise. (One year is not enough: in 2010-11 Mining and
      Quarrying and Subsidies are both Rs581,713mn.)

  gdp_growth_1952_2026.csv
      Table 1 (GDP, Agriculture, Manufacturing, Services, 1951-52 onward)
      and Table 6 (Industry; commodity-producing sectors) of the same
      release. The portal's industry_growth_pct is NOT industry: it equals
      Table 1's Manufacturing in every one of its 74 years and differs from
      the Industrial Activities total in most of them. It is carried here
      as manufacturing_growth_pct, and industry_growth_pct is the real
      industry total from Table 6, which starts in 2000-01 (NULL before).
      Commodity-producing growth before 2000-01 is not in the workbook and
      is kept from the portal; those years are not touched by the release.

Checks (asserted): every leaf matched once; leaves add to Table 5's GVA
total in every year; leaves and growth agree with the portal through
2019-20, so whatever differs is the revision and nothing else.

Usage: python3 -I build_na_annual_from_xlsx.py --na national_accounts.parquet
           --portal <2026-09-27 pull dir> --out <2026-10-24 pull dir>
"""
import argparse, csv, pathlib

import duckdb

UNREVISED_TO = '2019-20'      # the 118th NAC revised 2020-21 onward
GOVERNMENT_2025_26 = 'PMLN'   # same government as 2024-25 (portal label)


def fy_end(fy):
    return int(fy[:4]) + 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--na', required=True)
    ap.add_argument('--portal', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    out = pathlib.Path(a.out)
    portal = pathlib.Path(a.portal)
    con = duckdb.connect()
    con.sql(f"CREATE VIEW na AS SELECT * FROM '{a.na}'")

    # ── gva_by_activity_annual ────────────────────────────────────────────
    t5 = {}
    for item, fy, v in con.sql("""SELECT item, year, value FROM na
            WHERE table_sheet = 'Table 5' AND value IS NOT NULL""").fetchall():
        t5.setdefault(item.strip(), {})[fy] = v
    old = list(csv.DictReader(open(portal / 'gdp_by_activity_annual_constant_2000_2026.csv')))
    cats = sorted({(r['sector'], r['subsector'], r['category']) for r in old})
    oldv = {(r['sector'], r['subsector'], r['category'], r['fy']): float(r['gva_constant_pkr_mn'])
            for r in old}

    item_of = {}
    base = sorted(fy for (*_, fy) in oldv if fy <= UNREVISED_TO)
    for c in cats:
        hits = [i for i, s in t5.items()
                if all(abs(s.get(fy, -1e18) - oldv[c + (fy,)]) < 0.5 for fy in set(base))]
        assert len(hits) == 1, (c, hits)
        item_of[c] = hits[0]
    assert len(set(item_of.values())) == 22, 'two categories matched one row'

    years = sorted(t5[item_of[cats[0]]])
    total = next(s for i, s in t5.items() if i.startswith('D GDP'))
    rows = []
    for fy in years:
        leaf = 0.0
        for c in cats:
            v = t5[item_of[c]][fy]
            leaf += v
            if fy <= UNREVISED_TO:
                assert abs(v - oldv[c + (fy,)]) < 0.5, (c, fy, v, oldv[c + (fy,)])
            rows.append([fy, fy_end(fy), *c, v, 1])
        assert abs(leaf - total[fy]) < 1.0, (fy, leaf, total[fy])
    with open(out / 'gdp_by_activity_annual_constant_2000_2026.csv', 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['fy', 'fy_end', 'sector', 'subsector', 'category',
                    'gva_constant_pkr_mn', 'status'])
        w.writerows(sorted(rows, key=lambda r: (r[0], r[2], r[3], r[4])))
    moved = {r[0]: abs(r[5] - oldv.get(tuple(r[2:5]) + (r[0],), r[5])) for r in rows}
    print(f'  gva_by_activity_annual: {len(rows)} rows, {years[0]} to {years[-1]}; '
          f'largest leaf revision {max(moved.values()):,.0f} Rs mn')

    # ── gdp_growth ───────────────────────────────────────────────────────
    def series(sheet, prefix):
        got = dict(con.sql(f"""SELECT year, value FROM na WHERE table_sheet = '{sheet}'
                   AND trim(item) LIKE '{prefix}%' AND value IS NOT NULL""").fetchall())
        assert got, (sheet, prefix)
        return got
    gdp = series('Table-1', 'GDP')
    agr = series('Table-1', 'Agriculture')
    man = series('Table-1', 'Manufacturing')
    srv = series('Table-1', 'Services')
    ind = series('Table 6', 'B Industrial')
    cps = series('Table 6', 'Commodity Producing')
    pold = {r['fy']: r for r in csv.DictReader(open(portal / 'gdp_growth_1952_2025.csv'))}

    def same(a, b):
        return abs(a - float(b)) < 0.005

    grow = []
    for fy in sorted(gdp):
        p = pold.get(fy)
        cp = cps.get(fy)
        if cp is None:
            assert fy < '2000-01' and p, fy
            cp = float(p['commodity_producing_sector_growth_pct'])
        if p and fy <= UNREVISED_TO:
            assert same(gdp[fy], p['gdp_growth_pct']), (fy, 'gdp')
            assert same(agr[fy], p['agriculture_growth_pct']), (fy, 'agriculture')
            assert same(man[fy], p['industry_growth_pct']), (fy, 'manufacturing')
            assert same(srv[fy], p['services_growth_pct']), (fy, 'services')
            assert same(cp, p['commodity_producing_sector_growth_pct']), (fy, 'cps')
        label = p['government_label'] if p else GOVERNMENT_2025_26
        grow.append([fy, fy_end(fy), gdp[fy], agr[fy], ind.get(fy), man[fy], srv[fy], cp, label])
    with open(out / 'gdp_growth_1952_2026.csv', 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(['fy', 'fy_end', 'gdp_growth_pct', 'agriculture_growth_pct',
                    'industry_growth_pct', 'manufacturing_growth_pct', 'services_growth_pct',
                    'commodity_producing_sector_growth_pct', 'government_label'])
        w.writerows(['' if v is None else v for v in r] for r in grow)
    last = grow[-1]
    print(f'  gdp_growth: {len(grow)} years, {grow[0][0]} to {last[0]}; '
          f'{last[0]} GDP {last[2]:.2f}, agriculture {last[3]:.2f}, industry {last[4]:.2f}, '
          f'manufacturing {last[5]:.2f}, services {last[6]:.2f}')


if __name__ == '__main__':
    main()
