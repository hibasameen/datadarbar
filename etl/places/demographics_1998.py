"""1998 for the curated Population & households measures, and a fix to 2017.

The curated measures (Total Population, Density, Growth, Household Size, Urban
Proportion, Sex Ratio, and population by sex) are one row per district of
2017 on the 2023 map, which is how the census published them. 1998 joins them
on the same footing:

  population and urban share  PBS's restatement of 1998 on each 2017 district
                              (census 2017 table 1, via census_panel_1998), so
                              every district gets its own exact figure;
  sex, sex ratio, density,    the District at a Glance (1998). A glance district
  household size, growth      that became several 2017 districts - Jhang into
                              Jhang and Chiniot - is one figure drawn across
                              all of them; counts are summed, and a rate is the
                              printed one where one glance district covers the
                              ground, or derived from summed counts where
                              several do (never growth, whose 1981 base is not
                              reliable across glance districts).

Each 1998 row gets a change to 2017 on the same ground: the 2017 counts are
summed over the same districts, and a rate is re-derived from them.

And Karachi West: the source repeats its 2017 figure on Keamari, carved out of
it in 2020, as a second row, so the 2017 map counted 3.9 million people twice
(211.2m against PBS's 207.7m). The repeat is folded into one row drawn across
both shapes, as every other split district already is.
"""
import collections, math

YEARS_SEP = ' + '


def nice(s):
    s = s.replace('CITY DISTRICT', '').replace(' DISTRICT', '').replace(' AGENCY', ' Agency')
    return ' '.join(s.split()).title()


def fold_keamari(con, note):
    """Karachi West's figure repeated on Keamari becomes one row on both."""
    n = con.execute("""
        WITH pair AS (
          SELECT k.group_key, k.indicator, k.year FROM place_indicators k
          JOIN place_indicators w ON w.group_key = k.group_key AND w.indicator = k.indicator
            AND w.year IS NOT DISTINCT FROM k.year AND w.source_key = 'karachi west'
            AND w.value = k.value
          WHERE k.source_key = 'keamari')
        SELECT count(*) FROM pair""").fetchone()[0]
    con.execute("""
        CREATE TEMP TABLE _pair AS
          SELECT k.group_key, k.indicator, k.year FROM place_indicators k
          JOIN place_indicators w ON w.group_key = k.group_key AND w.indicator = k.indicator
            AND w.year IS NOT DISTINCT FROM k.year AND w.source_key = 'karachi west'
            AND w.value = k.value
          WHERE k.source_key = 'keamari'""")
    con.execute("""UPDATE place_indicators p SET map_key = '147 166', relation = 'split', note = ?
        WHERE source_key = 'karachi west' AND EXISTS (SELECT 1 FROM _pair x
          WHERE x.group_key = p.group_key AND x.indicator = p.indicator
            AND x.year IS NOT DISTINCT FROM p.year)""", [note])
    con.execute("""DELETE FROM place_indicators p WHERE source_key = 'keamari' AND EXISTS (
        SELECT 1 FROM _pair x WHERE x.group_key = p.group_key AND x.indicator = p.indicator
          AND x.year IS NOT DISTINCT FROM p.year)""")
    con.execute('DROP TABLE _pair')
    print(f'  Karachi West repeated on Keamari: {n} rows folded into one drawn across both')


def add_1998(con, src):
    p98 = f'{src}/census_panel_1998.parquet'
    cur = con.sql("""SELECT source_key, map_key, relation, note, place, province, dataset,
                            group_label
                     FROM place_indicators WHERE group_key = 'demographics' AND year = '2017'
                     QUALIFY row_number() OVER (PARTITION BY source_key ORDER BY indicator) = 1
                  """).fetchall()
    cur = {r[0]: r for r in cur}
    v17 = {(s, i): v for s, i, v in con.sql("""SELECT source_key, indicator, value
        FROM place_indicators WHERE group_key = 'demographics' AND year = '2017'""").fetchall()}
    label = dict(con.sql("""SELECT indicator, any_value(label) FROM place_indicators
        WHERE group_key = 'demographics' AND label NOT LIKE '%change%' GROUP BY 1""").fetchall())

    ks = lambda k: frozenset(str(k).split())
    t1 = collections.defaultdict(dict)
    unit_keys = {}
    for unit, k, loc, v in con.sql(f"""SELECT unit, map_key, locality, value FROM '{p98}'
            WHERE table_id = '1' AND unit_type = 'district' AND unit NOT LIKE 'FR %'""").fetchall():
        t1[unit][loc] = v
        unit_keys[unit] = ks(k)
    slug_of_unit = {}
    for s, r in cur.items():
        hit = [u for u, k in unit_keys.items() if k == ks(r[1])]
        if len(hit) == 1:
            slug_of_unit[hit[0]] = s
    missing = sorted(set(cur) - set(slug_of_unit.values()))
    print(f'  1998: {len(slug_of_unit)} of {len(cur)} curated districts matched to a 2017 '
          f'district; none for {missing}')

    rows = []

    def put(slugs, ind, year, value, relation=None, note=None):
        if value is None or (isinstance(value, float) and math.isnan(value)):
            return
        r = cur[slugs[0]]
        keys = sorted(set().union(*(ks(cur[s][1]) for s in slugs)))
        one = len(slugs) == 1
        lab = label[ind] + ('' if year == '1998' else ' — change 1998→2017')
        rows.append({'level': 'district', 'map_key': ' '.join(keys),
                     'relation': relation or (r[2] if one else 'combined'),
                     'note': note if note is not None else (r[3] if one else None),
                     'source_key': YEARS_SEP.join(slugs), 'place': r[4] if one else
                     YEARS_SEP.join(cur[s][4] for s in slugs), 'province': r[5],
                     'dataset': r[6], 'group_key': 'demographics', 'group_label': r[7],
                     'indicator': ind, 'label': lab, 'year': year, 'kind': 'indicator',
                     'field': f't1_{"1998" if year == "1998" else "diff9817"}_{ind}',
                     'value': float(value), 'value_text': None})

    # population and urban share: every 2017 district, exact
    for unit, s in slug_of_unit.items():
        p, u = t1[unit].get('all'), t1[unit].get('urban')
        put([s], 'pop_total', '1998', p)
        if (s, 'pop_total') in v17 and p is not None:
            put([s], 'pop_total', 'Δ1998-2017', v17[(s, 'pop_total')] - p)
        if p and u is not None:
            put([s], 'urban_proportion', '1998', u / p * 100)
            if (s, 'urban_proportion') in v17:
                put([s], 'urban_proportion', 'Δ1998-2017',
                    v17[(s, 'urban_proportion')] - u / p * 100)

    # the glance measures, one row per glance group
    g = collections.defaultdict(dict)
    meta = {}
    for d, ind, col, sex, v, w, ds, gs in con.sql(f"""SELECT district, indicator, col_label, sex,
            value, map_weight, districts_2017, glance_group FROM '{p98}'
            WHERE table_id = 'glance' AND districts_2017 IS NOT NULL""").fetchall():
        g[d][(ind, col, sex)] = v
        g[d]['pop'] = w
        meta[gs] = ds.split(YEARS_SEP)
    for gs, ds in meta.items():
        members = gs.split(YEARS_SEP)
        slugs = [slug_of_unit.get(d) for d in ds]
        if not all(slugs):
            continue
        single = len(members) == 1
        note = None
        if len(slugs) > 1 or not single:
            note = (f'1998 {" + ".join(nice(m) for m in members)} covers today’s '
                    f'{", ".join(nice(d) for d in ds)}; one figure is drawn across all of them'
                    + ('' if single else ', derived from the districts’ own counts') + '.')
        val = lambda m, k: g[m].get(k)
        male = sum(val(m, ('POPULATION - 1998 BY SEX', 'MALE', 'all')) or 0 for m in members)
        female = sum(val(m, ('POPULATION - 1998 BY SEX', 'FEMALE', 'all')) or 0 for m in members)
        pop = sum(g[m]['pop'] for m in members)
        area = sum(val(m, ('AREA (SQ. KM.)', 'AREA (SQ. KM.)', 'all')) or 0 for m in members)
        hh = [val(m, ('AVERAGE HOUSEHOLD SIZE', 'AVERAGE HOUSEHOLD SIZE', 'all')) for m in members]
        printed = lambda k: val(members[0], k) if single else None
        v98 = {
            'pop_male': male or None, 'pop_female': female or None,
            'sex_ratio': printed(('SEX RATIO', 'SEX RATIO', 'all'))
                         or (male / female * 100 if female else None),
            'density_per_sq_km': printed(('POPULATION DENSITY PER SQ. KM.',
                                          'POPULATION DENSITY PER SQ. KM.', 'all'))
                                 or (pop / area if area else None),
            'avg_household_size': printed(('AVERAGE HOUSEHOLD SIZE', 'AVERAGE HOUSEHOLD SIZE', 'all'))
                                  or (pop / sum(g[m]['pop'] / h for m, h in zip(members, hh))
                                      if all(hh) else None),
            'annual_growth_rate': printed(('1981-1998 AVERAGE ANNUAL GROWTH RATE',
                                           '1981-1998 AVERAGE ANNUAL GROWTH RATE', 'all')),
        }
        # 2017 on the same ground: counts summed, rates re-derived from them
        sv = lambda i: [v17.get((s, i)) for s in slugs]
        one17 = lambda i: sv(i)[0] if len(slugs) == 1 else None
        m17, f17, p17 = sum(x or 0 for x in sv('pop_male')), sum(x or 0 for x in sv('pop_female')), \
            sum(x or 0 for x in sv('pop_total'))
        a17, h17 = sv('area_sq_km'), sv('avg_household_size')
        r17 = {
            'pop_male': m17 if all(sv('pop_male')) else None,
            'pop_female': f17 if all(sv('pop_female')) else None,
            'sex_ratio': one17('sex_ratio') or (m17 / f17 * 100 if f17 and all(sv('pop_male')) else None),
            'density_per_sq_km': one17('density_per_sq_km')
                                 or (p17 / sum(a17) if all(a17) and all(sv('pop_total')) else None),
            'avg_household_size': one17('avg_household_size')
                                  or (p17 / sum(p / h for p, h in zip(sv('pop_total'), h17))
                                      if all(h17) and all(sv('pop_total')) else None),
            'annual_growth_rate': one17('annual_growth_rate'),
        }
        for ind, v in v98.items():
            if ind == 'annual_growth_rate' and v is not None:
                gnote = 'Average annual growth 1981–1998.' + (' ' + note if note else '')
            else:
                gnote = note
            put(slugs, ind, '1998', v, note=gnote)
            if v is not None and r17.get(ind) is not None:
                put(slugs, ind, 'Δ1998-2017', r17[ind] - v, note=gnote)

    con.execute('CREATE TEMP TABLE _d98 AS SELECT * FROM (SELECT unnest(?, recursive := true))', [rows])
    con.execute('INSERT INTO place_indicators BY NAME SELECT * FROM _d98')
    con.execute('DROP TABLE _d98')
    got = collections.Counter((r['indicator'], r['year']) for r in rows)
    print('  1998 curated rows: ' + ', '.join(f'{i} {y} {n}' for (i, y), n in sorted(got.items())))
