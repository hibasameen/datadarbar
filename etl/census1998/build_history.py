"""Pakistan's population at every census, 1951-2023, on PBS's 2023 districts.

PBS's 'Area & Population of Administrative Units 1951-1998' restates the
1951, 1961, 1972 and 1981 counts on the 115 districts and agencies of 1998.
This puts them on the 2023 frame the rest of the census sits on:

  1. each 1998 district reaches the 2017 districts it became through
     glance_crosswalk.build, accepted only where its 1998 population equals
     PBS's restated 1998 population of those 2017 districts to the person
     (all 115 do), and through them the 2023 shapes;
  2. where a district had no figure of its own at an earlier census, PBS's
     footnote names the district that counted it, and the two are drawn as
     one for that year (MERGES). Three Balochistan districts are blank in 1951
     with no footnote; they are drawn with the two districts that could hold
     them, Sibi and Kalat, and say so;
  3. every year must add up to PBS's published national and provincial
     totals before anything is written.

Two outputs:
  panel_1951_1981.parquet   the census-panel shape, one row per footprint,
                            year and locality, read by the Places map exactly
                            as census_panel_1998 is
  population_history.parquet  one series per place for the chart: Pakistan,
                            each province, and each district footprint on a
                            boundary that holds still from 1972 to 2023, with
                            1951 and 1961 only where the footprint is the same

    python3 etl/census1998/build_history.py --warehouse app/data/warehouse \
        --out ../data_darbar_warehouse/census1998/<date>
"""
import argparse, collections, pathlib, sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import glance_crosswalk as gx

YEARS = [1951, 1961, 1972, 1981]
PUBLISHED = {1951: 33_740_167, 1961: 42_880_378, 1972: 65_309_340, 1981: 84_253_644,
             1998: 132_352_279, 2017: 207_684_626, 2023: 241_499_431}

# Districts with no figure of their own at a census, and the one PBS says
# counted them. Read from the table's footnotes; checked below against the
# national totals.
TA = 'Tribal Area Adjoining {} District'
MERGES = {
    1951: [
        ('UPPER DIR DISTRICT', ['LOWER DIR DISTRICT'], 'footnote'),
        ('MANSEHRA DISTRICT', ['KOHISTAN DISTRICT', 'BATAGRAM DISTRICT'], 'footnote'),
        (TA.format('Kohat'), ['ORAKZAI AGENCY'], 'footnote'),
        (TA.format('Bannu'), [TA.format('Lakki Marwat')], 'footnote'),
        (TA.format('D.I.Khan'), [TA.format('Tank')], 'footnote'),
        ('QUETTA DISTRICT', ['PISHIN DISTRICT', 'KILLA ABDULLAH DISTRICT'], 'footnote'),
        ('CHAGAI DISTRICT', ['NUSHKI DISTRICT'], 'footnote'),
        ('LORALAI DISTRICT', ['ZIARAT DISTRICT', 'BARKHAN DISTRICT', 'MUSAKHEL DISTRICT'], 'footnote'),
        ('ZHOB DISTRICT', ['KILLA SAIFULLAH DISTRICT'], 'footnote'),
        ('SIBI DISTRICT', ['ZIARAT DISTRICT', 'KOHLU DISTRICT', 'DERA BUGTI DISTRICT',
                           'NASIRABAD DISTRICT'], 'footnote'),
        ('KALAT DISTRICT', ['MASTUNG DISTRICT', 'KHUZDAR DISTRICT', 'AWARAN DISTRICT'], 'footnote'),
        # blank in 1951 and footnoted nowhere: held by Sibi or Kalat, the two
        # districts around them, so the three are drawn with both
        ('SIBI DISTRICT', ['KALAT DISTRICT', 'BOLAN DISTRICT', 'JAFARABAD DISTRICT',
                           'JHAL MAGSI DISTRICT'], 'unstated'),
    ],
    1961: [
        ('UPPER DIR DISTRICT', ['LOWER DIR DISTRICT'], 'footnote'),
        ('MANSEHRA DISTRICT', ['KOHISTAN DISTRICT'], 'footnote'),
        (TA.format('Bannu'), [TA.format('Lakki Marwat')], 'footnote'),
        (TA.format('D.I.Khan'), [TA.format('Tank')], 'footnote'),
    ],
}
# 2017 province names; NWFP is Khyber Pakhtunkhwa, and FATA joined it in 2018
PROVINCE = {2: 'KHYBER PAKHTUNKHWA', 3: 'FATA', 4: 'PUNJAB', 5: 'SINDH', 6: 'BALOCHISTAN',
            7: 'ISLAMABAD'}


class DSU:
    def __init__(self):
        self.p = {}

    def find(self, x):
        self.p.setdefault(x, x)
        while self.p[x] != x:
            self.p[x] = self.p[self.p[x]]
            x = self.p[x]
        return x

    def union(self, a, b):
        self.p[self.find(a)] = self.find(b)


def nice(name):
    n = name.replace('Tribal Area Adjoining', 'FR').replace(' District', '')
    return gx.norm(n).title().replace('Fr ', 'FR ').replace('D.I.Khan', 'D.I. Khan') \
        .replace('D.G.Khan', 'D.G. Khan')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    W, out = pathlib.Path(a.warehouse), pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    au = (W / 'census_admin_units_1951_1998.parquet').as_posix()
    p98 = (W / 'census_panel_1998.parquet').as_posix()
    p23 = (W / 'census_panel_2023.parquet').as_posix()

    # ── the link: 1998 districts -> 2017 districts -> 2023 shapes ───────────
    apop = dict(con.sql(f"""SELECT unit, population FROM '{au}' WHERE unit_type = 'district'
        AND census_year = 1998 AND locality = 'all'""").fetchall())
    tab = dict(con.sql(f"""SELECT DISTINCT unit, table_no FROM '{au}'
        WHERE unit_type = 'district'""").fetchall())
    d17 = {(p, u): v for p, u, v in con.sql(f"""SELECT province_area, unit, value FROM '{p98}'
        WHERE table_id = '1' AND unit_type = 'district' AND locality = 'all'""").fetchall()}
    key17 = dict(con.sql(f"""SELECT unit, any_value(map_key) FROM '{p98}'
        WHERE table_id = '1' AND unit_type = 'district' AND locality = 'all'
        GROUP BY 1""").fetchall())
    groups, orphans = gx.build(apop, d17)
    bad = [g for g in groups if g[3] != 0 or not g[1]]
    assert not bad and not orphans, f'1998 districts that do not balance: {bad} {orphans}'
    keys_of = {}            # 1998 district -> the 2023 shapes of its group
    for gs, ds, _, _ in groups:
        ks = {k for d in ds for k in key17[d].split()}
        for g in gs:
            keys_of[g] = ks
    print(f'  {len(apop)} districts of 1998 in {len(groups)} balanced groups')

    # ── per year: printed figures, footprints, and the national check ──────
    vals = collections.defaultdict(dict)    # (year, locality) -> {district: value}
    for u, y, loc, v in con.sql(f"""SELECT unit, census_year, locality, population FROM '{au}'
            WHERE unit_type = 'district' AND census_year < 1998""").fetchall():
        vals[(y, loc)][u] = v

    def contribution(y, loc, u):
        v = vals[(y, loc)].get(u)
        if v is not None or loc != 'all':
            return v or 0
        # no total printed: whatever part is printed is this district's own,
        # the rest is in the district that counted it
        return (vals[(y, 'urban')].get(u) or 0) + (vals[(y, 'rural')].get(u) or 0)

    def footprints(year_merges):
        """1998 districts -> footprint id, joining districts that share a 2023
        shape (a Frontier Region and its host district) and those merged by a
        footnote for the year."""
        dsu = DSU()
        for g, ks in keys_of.items():
            dsu.find(('d', g))
            for k in ks:
                dsu.union(('d', g), ('k', k))
        for host, absorbed, _ in year_merges:
            for x in absorbed:
                dsu.union(('d', host), ('d', x))
        fp = collections.defaultdict(list)
        for g in keys_of:
            fp[dsu.find(('d', g))].append(g)
        return list(fp.values())

    rows = []
    for y in YEARS:
        merged_away = {x for _, xs, _ in MERGES.get(y, []) for x in xs}
        blank = [u for u in apop if vals[(y, 'all')].get(u) is None]
        unexplained = sorted(set(blank) - merged_away)
        assert not unexplained, f'{y}: no figure and no footnote for {unexplained}'
        total = sum(contribution(y, 'all', u) for u in apop)
        assert round(total) == PUBLISHED[y], f'{y}: districts sum to {total:,.0f}'
        unstated = {x for _, xs, how in MERGES.get(y, []) if how == 'unstated' for x in xs}
        for members in footprints(MERGES.get(y, [])):
            members = sorted(members, key=lambda g: -apop[g])
            ks = sorted({k for g in members for k in keys_of[g]})
            # the province of the footprint is that of its largest district
            prov = PROVINCE[tab[members[0]]]
            merged = [g for g in members if g in merged_away]
            relation = 'exact' if len(members) == 1 and len(keys_of[members[0]]) == 1 else 'combined'
            note = None
            if len(members) > 1 or relation == 'combined':
                note = (f'{" + ".join(nice(g) for g in members)} in {y}, drawn across '
                        f'all {len(ks)} of today’s districts they cover.')
            if merged:
                note = (f'In {y} PBS counted {", ".join(nice(g) for g in merged)} within '
                        f'a neighbouring district, so they are drawn together. ' + (note or ''))
            if unstated & set(members):
                note += (' PBS does not say which district counted Bolan, Jafarabad and '
                         'Jhal Magsi in 1951; Sibi and Kalat, the two around them, are '
                         'drawn with them.')
            allv = sum(contribution(y, 'all', g) for g in members)
            for loc in ('all', 'rural', 'urban'):
                v = sum(contribution(y, loc, g) for g in members)
                if loc != 'all' and not any(vals[(y, loc)].get(g) is not None for g in members):
                    v = None
                rows.append({'census_year': y, 'province_area': prov, 'table_id': '1',
                             'district': ' + '.join(members), 'unit': ' + '.join(members),
                             'unit_type': 'district', 'map_key': ' '.join(ks),
                             'map_relation': relation,
                             'map_comparable': 'yes' if relation == 'exact' else 'combined',
                             'map_note': note, 'map_weight': allv, 'locality': loc,
                             'sex': 'all', 'indicator': 'POPULATION',
                             'col_label': 'POPULATION / ALL SEXES', 'value': v,
                             'missing': v is None, 'is_rate': False,
                             'series_ambiguous': False,
                             'published_in': 'Area & Population of Administrative Units '
                                             '1951-1998 (PBS), restated on 1998 districts'})
        print(f'  {y}: {len(footprints(MERGES.get(y, [])))} footprints, '
              f'{round(total):,} = published total')
    con.execute('CREATE TABLE panel AS SELECT * FROM (SELECT unnest(?, recursive := true))',
                [rows])
    con.execute(f"""COPY (SELECT * FROM panel ORDER BY census_year, map_key, locality)
                    TO '{(out / 'panel_1951_1981.parquet').as_posix()}' (FORMAT PARQUET)""")

    # ── the chart: one series per place on a boundary that holds still ──────
    base = footprints([])               # 1972 onwards: no footnoted merges
    p17 = dict(con.sql(f"""SELECT unit, value FROM '{(W / 'census_panel_2017.parquet').as_posix()}'
        WHERE table_id = '1' AND unit_type = 'district' AND locality = 'all' AND sex = 'all'
          AND indicator = 'POPULATION - 2017 / ALL SEXES'""").fetchall())
    p23k = collections.defaultdict(float)
    for k, v in con.sql(f"""SELECT map_key, value FROM '{p23}' WHERE table_id = '1'
            AND unit_type = 'district' AND locality = 'all' AND sex = 'all'
            AND indicator = 'POPULATION-2023 / ALL SEXES' AND map_key IS NOT NULL""").fetchall():
        ks = k.split()
        assert len(ks) == 1, f'a 2023 district on {len(ks)} shapes: {k}'
        p23k[ks[0]] += v
    assert round(sum(p23k.values())) == PUBLISHED[2023], sum(p23k.values())
    by_year_fp = {y: {g: tuple(sorted(m)) for m in footprints(MERGES.get(y, [])) for g in m}
                  for y in YEARS}
    ds_of = {g: ds for gs, ds, _, _ in groups for g in gs}
    hist = []
    for members in base:
        members = sorted(members, key=lambda g: -apop[g])
        ks = sorted({k for g in members for k in keys_of[g]})
        series = {}
        for y in YEARS:
            fp = by_year_fp[y][members[0]]
            if set(fp) == set(members):        # same ground as today's footprint
                series[y] = sum(contribution(y, 'all', g) for g in members)
        series[1998] = sum(apop[g] for g in members)
        series[2017] = sum(p17[d] for d in {d for g in members for d in ds_of[g]})
        series[2023] = sum(p23k[k] for k in ks)
        for y, v in series.items():
            today = sorted({nice(d) for g in members for d in ds_of[g]})
            hist.append({'level': 'district', 'map_key': ' '.join(ks),
                         'place': ' + '.join(nice(g) for g in members),
                         'today': ', '.join(today),
                         'province': PROVINCE[tab[members[0]]],
                         'census_year': y, 'population': v,
                         'relation': 'exact' if len(members) == 1 and len(ks) == 1 else 'combined'})
    # Pakistan and provinces, as PBS prints them; 2017 and 2023 from the panels
    for tno, unit, y, v in con.sql(f"""SELECT table_no, unit, census_year, population FROM '{au}'
            WHERE table_no = 1 AND unit_type IN ('country', 'province')
              AND locality = 'all'""").fetchall():
        hist.append({'level': 'country' if 'PAKISTAN' in unit.upper() else 'province',
                     'map_key': None, 'place': unit, 'today': None, 'province': None, 'census_year': y,
                     'population': v, 'relation': 'exact'})
    con.execute('CREATE TABLE hist AS SELECT * FROM (SELECT unnest(?, recursive := true))', [hist])
    for y in (1998, 2017, 2023):
        t = con.sql(f"SELECT sum(population) FROM hist WHERE level = 'district' AND census_year = {y}").fetchone()[0]
        assert round(t) == PUBLISHED[y], f'history {y} sums to {t:,.0f}'
    con.execute(f"""COPY (SELECT * FROM hist ORDER BY level, place, census_year)
                    TO '{(out / 'population_history.parquet').as_posix()}' (FORMAT PARQUET)""")
    n = con.sql("SELECT count(DISTINCT map_key) FROM hist WHERE level = 'district'").fetchone()[0]
    print(f'  population_history: {n} district footprints, {len(hist)} rows')


if __name__ == '__main__':
    main()
