"""The 1998 census as a panel on PBS's 2023 boundaries, like census_panel_2017.

Two layers, because that is what survives of 1998 at each level:

  table 1 - POPULATION 1998, all / rural / urban, for every district AND every
            sub-district (tehsil, taluka, sub-division, sub-tehsil). This is
            PBS's own restatement: the 2017 census reprints each 2017 unit's
            1998 population. It sums to 132,352,279, the published 1998 total,
            and reaches the 2023 frame through the same unit map as 2017.
  glance  - District at a Glance (1998): sex, sex ratio, density, household
            size, literacy by sex, 1981 population and growth since, housing
            and its amenities. Published for districts only. Each glance
            district reaches the 2017 districts it became through
            glance_crosswalk.py, accepted only where PBS's restated 1998
            populations balance to the person, and through them the 2023 frame.

Nothing is published for 1998 below the district beyond population: the
District Census Reports that carried the rest are print-only.

    python3 etl/census1998/build_panel_1998.py --warehouse app/data/warehouse \
        --out ../data_darbar_warehouse/census1998/<date>
"""
import argparse, csv, json, pathlib, sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import glance_crosswalk as gx

PUBLISHED_1998 = 132_352_279

# glance indicator -> (indicator, col_label, sex, is_rate); shares printed
# beside a count become their own rate rows
GLANCE = {
    'area': ('AREA (SQ. KM.)', 'AREA (SQ. KM.)', 'all', False),
    # the sex is in the column, as in census table 1, so these rows sit under
    # sex = 'all' - the same convention the map reads table 1 with
    'population_male': ('POPULATION - 1998 BY SEX', 'MALE', 'all', False),
    'population_female': ('POPULATION - 1998 BY SEX', 'FEMALE', 'all', False),
    'sex_ratio': ('SEX RATIO', 'SEX RATIO', 'all', True),
    'population_density': ('POPULATION DENSITY PER SQ. KM.', 'POPULATION DENSITY PER SQ. KM.', 'all', True),
    'avg_household_size': ('AVERAGE HOUSEHOLD SIZE', 'AVERAGE HOUSEHOLD SIZE', 'all', True),
    'literacy_ratio': ('LITERACY RATIO (10+)', 'LITERACY RATIO', 'all', True),
    'literacy_ratio_male': ('LITERACY RATIO (10+)', 'LITERACY RATIO', 'male', True),
    'literacy_ratio_female': ('LITERACY RATIO (10+)', 'LITERACY RATIO', 'female', True),
    'population_1981': ('POPULATION 1981', 'POPULATION 1981', 'all', False),
    'growth_rate_1981_98': ('1981-1998 AVERAGE ANNUAL GROWTH RATE', '1981-1998 AVERAGE ANNUAL GROWTH RATE', 'all', True),
    'housing_units': ('HOUSING UNITS', 'TOTAL', 'all', False),
    'housing_units_pacca': ('HOUSING UNITS', 'PACCA', 'all', False),
    'housing_units_electricity': ('HOUSING UNITS', 'WITH ELECTRICITY', 'all', False),
    'housing_units_piped_water': ('HOUSING UNITS', 'WITH PIPED WATER', 'all', False),
    'housing_units_gas_cooking': ('HOUSING UNITS', 'USING GAS FOR COOKING', 'all', False),
}
SHARE_OF = {   # count indicator -> the printed share's own row
    'housing_units_pacca': 'PACCA',
    'housing_units_electricity': 'WITH ELECTRICITY',
    'housing_units_piped_water': 'WITH PIPED WATER',
    'housing_units_gas_cooking': 'USING GAS FOR COOKING',
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    W, out = pathlib.Path(a.warehouse), pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    p17 = (W / 'census_panel_2017.parquet').as_posix()
    gl = (W / 'census1998_district_glance.parquet').as_posix()

    # ── table 1: PBS's 1998 population on the 2017 units, every tier ──────
    con.execute(f"""CREATE TABLE t1 AS
        SELECT 1998 AS census_year, province_area, '1' AS table_id, district, unit, unit_type,
               map_key, map_relation, map_comparable, map_note,
               locality, 'all' AS sex, 'POPULATION - 1998' AS indicator,
               'POPULATION - 1998 / ALL SEXES' AS col_label, value, missing,
               FALSE AS is_rate, FALSE AS series_ambiguous,
               'Census 2017 table 1, POPULATION 1998 (PBS restatement on 2017 units)' AS published_in
        FROM '{p17}'
        WHERE table_id = '1' AND indicator = 'POPULATION 1998'""")
    # each unit's own 1998 population is its weight for averaging a rate
    con.execute("""CREATE TABLE t1w AS SELECT t1.*, w.value AS map_weight FROM t1
        LEFT JOIN (SELECT district, unit, unit_type, value FROM t1 WHERE locality = 'all') w
          USING (district, unit, unit_type)""")
    for tier, where in (('district', "unit_type = 'district'"),
                        ('sub-district', "unit_type <> 'district'")):
        tot = con.sql(f"SELECT sum(value) FROM t1 WHERE locality = 'all' AND {where}").fetchone()[0]
        assert round(tot) == PUBLISHED_1998, f'{tier} 1998 population sums to {tot:,.0f}'
        print(f'  table 1, {tier}: {round(tot):,} = published 1998 total')

    # ── glance: district indicators, linked through balanced groups ───────
    d17 = {(p, u): v for p, u, v in con.sql(
        "SELECT province_area, unit, value FROM t1 WHERE unit_type = 'district' AND locality = 'all'").fetchall()}
    gpop = dict(con.sql(f"SELECT district, value FROM '{gl}' WHERE indicator = 'population_1998'").fetchall())
    groups, orphans = gx.build(gpop, d17)
    key17 = dict(con.sql("""SELECT unit, any_value(map_key) FROM t1
        WHERE unit_type = 'district' AND locality = 'all' GROUP BY 1""").fetchall())
    link, xw = {}, []
    for gs, ds, prov, gap in groups:
        ok = gap == 0 and ds
        keys = sorted({k for d in ds for k in (key17.get(d) or '').split()}) if ok else []
        for gname in gs:
            link[gname] = (prov, keys, gs, ds) if ok else None
        xw.append({'glance_districts': ' + '.join(gs), 'districts_2017': ' + '.join(ds),
                   'province_area': prov or '', 'map_key': ' '.join(keys),
                   'population_1998_glance': sum(gpop[g] for g in gs),
                   'population_1998_restated': sum(d17.get((prov, d), 0) for d in ds),
                   'balances': 'yes' if ok else 'no'})
    with open(out / 'glance_crosswalk_1998.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(xw[0]))
        w.writeheader(); w.writerows(xw)

    # A shape is whole only if every 2017 district drawn on it is covered by a
    # linked glance group. Peshawar, D.I. Khan and Tank balance against their
    # 2017 namesakes, but their 2023 shapes also took in FR Peshawar, FR D.I.
    # Khan and FR Tank, which had no glance sheet: a rate still describes the
    # shape, a count would leave those people out of it - and out of the
    # 1998-2017 change, inflating it.
    covered = {d for v in link.values() if v for d in v[3]}
    on_shape = {}
    for (prov, unit), _ in d17.items():
        for k in (key17.get(unit) or '').split():
            on_shape.setdefault(k, set()).add(unit)
    def left_out(keys, ds):
        return sorted({u for k in keys for u in on_shape.get(k, ())} - covered)

    rows = []
    for district, ind, value, share in con.sql(f"""SELECT district, indicator, value, share_pct
            FROM '{gl}' WHERE indicator NOT LIKE 'admin_%' AND indicator <> 'population_1998'
              AND indicator <> 'population_urban' AND indicator <> 'population_rural'""").fetchall():
        if ind not in GLANCE:
            continue
        L = link.get(district)
        indicator, col, sex, rate = GLANCE[ind]
        prov, keys, gs, ds = L if L else (None, [], [district], [])
        one = len(gs) == 1 and len(ds) == 1
        if not L:
            rel, comp, note = None, 'no', ('No 2023 shape: this district’s 1998 population does '
                                           'not balance against the 2017 districts it became, so '
                                           'its figures are not drawn.')
        elif one:
            rel, comp, note = 'exact', 'yes', None
        else:
            rel, comp = 'combined', 'combined'
            note = (f'1998 district {" + ".join(gs).title()} covers the ground of 2017 '
                    f'{" + ".join(ds).title()}; drawn across all of it.')
        base = {'census_year': 1998, 'province_area': prov, 'table_id': 'glance',
                'district': district, 'unit': district, 'unit_type': 'district',
                'map_key': ' '.join(keys) or None, 'map_relation': rel, 'map_comparable': comp,
                'map_note': note, 'locality': 'all', 'sex': sex, 'missing': value is None,
                'series_ambiguous': False, 'map_weight': gpop.get(district),
                'published_in': 'District at a Glance (1998)'}
        out_ = left_out(keys, ds) if L else []

        def placed(is_rate):
            if not out_:
                return base
            also = ', '.join(o.title().replace('Fr ', 'FR ') for o in out_)
            if not is_rate:
                return {**base, 'map_key': None, 'map_comparable': 'no',
                        'map_note': (f'Not drawn: the 2023 shape also covers {also}, which no '
                                     '1998 District at a Glance includes, so a count for the '
                                     'shape would leave its people out.')}
            return {**base, 'map_note': ((base['map_note'] + ' ') if base['map_note'] else '')
                    + f'The 2023 shape also covers {also}, not in this rate.'}
        rows.append({**placed(rate), 'indicator': indicator, 'col_label': col, 'value': value,
                     'is_rate': rate})
        if ind in SHARE_OF and share is not None:
            rows.append({**placed(True), 'indicator': 'HOUSING UNITS, % OF ALL',
                         'col_label': SHARE_OF[ind], 'value': share, 'is_rate': True})
    tmp = out / '_glance.json'
    tmp.write_text(json.dumps(rows))
    con.execute(f"CREATE TABLE g AS SELECT * FROM read_json_auto('{tmp}')")
    tmp.unlink()
    cols = [r[0] for r in con.sql("DESCRIBE t1w").fetchall()]
    sel = ', '.join(f'CAST({c} AS {t})' if False else c for c, t in
                    [(r[0], r[1]) for r in con.sql('DESCRIBE t1w').fetchall()])
    con.execute(f"""COPY (
        SELECT {', '.join(cols)} FROM t1w
        UNION ALL BY NAME
        SELECT * FROM g
        ORDER BY table_id, unit_type, district, unit, locality, sex, indicator, col_label)
        TO '{(out / 'panel_1998.parquet').as_posix()}' (FORMAT PARQUET)""")
    n = con.sql(f"SELECT count(*) FROM '{(out / 'panel_1998.parquet').as_posix()}'").fetchone()[0]
    linked = sum(1 for v in link.values() if v)
    print(f'  glance: {linked} of {len(link)} districts linked through '
          f'{sum(1 for x in xw if x["balances"] == "yes")} balanced groups; unlinked: '
          f'{", ".join(sorted(k for k, v in link.items() if not v))}')
    print(f'  panel_1998.parquet: {n:,} rows')


if __name__ == '__main__':
    main()
