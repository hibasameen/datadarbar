#!/usr/bin/env python3
"""Two Issue 4 district tables that Data Darbar builds rather than copies.

    python3 -I etl/issue4/derive.py [--adaad <Adaad/data>] [--out <warehouse>/issue4/derived]

Both end on the COD frame's district_key, the key every other Issue 4 district
table carries, so pipeline.py adds district_code to them the same way.

diem_district_round.csv
    FAO's Data in Emergencies household surveys, rounds 2–6 (2021–2024), from
    the FAM microdata files in pk-diem-microdata. Weighted shares of households
    per surveyed district and round. Only these aggregates leave this script:
    FAM's terms allow aggregate reporting and forbid passing on household
    records. Districts are matched by name within province, because rounds 2–3
    use an older p-code scheme (their PK232 is Bajaur; COD's is Ziarat) and
    round 6 gives one Shikarpur household Sanghar's code. A share resting on
    fewer than MIN_N answers is withheld.

emissions_district_year.csv
    PakEmis release 1.0.0rc68, district (adm2) emissions by sector, physical
    view (where the gas is released), CO2e on AR5 potentials, FY2015-16 to
    FY2024-25. PakEmis draws districts on geoBoundaries 2019 shapes; fourteen of
    them hold two or more of today's districts (Sujawal with Thatta, Karachi's
    seven, all of Azad Kashmir). A shape's total is not divided between its
    districts: each carries the shape's emissions per km² and agriculture's
    share, which hold for every part, and a district total only where the shape
    is that district alone.
"""
import argparse, csv, os, pathlib, re

import duckdb

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent.parent
ADAAD = pathlib.Path(os.environ.get(
    'ADAAD_DATA', pathlib.Path.home() / 'Library/Mobile Documents/com~apple~CloudDocs/Adaad/data'))
OUT = REPO.parent / 'data_darbar_warehouse' / 'issue4' / 'derived'
XW = REPO / 'etl' / 'geography' / 'district_crosswalk_cod_2023.csv'

MIN_N = 30

# survey folder -> (round, year the fieldwork was in)
ROUNDS = {'PAK_2021_DEMHS-R2': (2, 2021), 'PAK_2022_DEMHS-R3': (3, 2022),
          'PAK_2023_DIEM-HS-R4': (4, 2023), 'PAK_2023_DIEM_HS_R5': (5, 2023),
          'PAK_2024_DIEM_HS_R6': (6, 2024)}

# Names that differ by more than spacing, word order or the word "district".
ALIAS = {'dikhan': 'DERA ISMAIL KHAN', 'kolaipalas': 'KOLAI PALAS KOHISTAN',
         'ustamuhammad': 'JAFFARABAD', 'diamer': 'DIAMIR',
         'malakandprotectedarea': 'MALAKAND'}
NOTE = {'ustamuhammad': 'Surveyed in Usta Muhammad, which the 2023 census counts within Jaffarabad'}

# Each DIEM indicator: the answers that count, the answers that are valid (the
# denominator; don't-know, refused and not-asked are left out), and the column.
# V1 files (rounds 2–3) store coping and consumption groups as codes, V2 as
# labels; both are listed. Codes are FAO's (coded_value_labels.csv).
YES = ("'Yes'",)
YN = ("'Yes'", "'No'")
DIEM = [
    ('shock_any_pct', 'shock_noshock', ("'No'",), YN),
    ('shock_flood_pct', 'shock_flood', YES, YN),
    ('shock_food_prices_pct', 'shock_higherfoodprices', YES, YN),
    # round 3 answers Less / More where the others say Slightly less / more
    ('income_lower_pct', 'income_main_comp',
     ("'Slightly less'", "'Less'", "'A lot less (less than half as much)'"),
     ("'Slightly less'", "'Less'", "'A lot less (less than half as much)'", "'Same'",
      "'Slightly more'", "'More'", "'A lot more (increased by more than 50%)'")),
    ('skipped_meal_pct', 'fies_skipped', YES, YN),
    ('whole_day_no_food_pct', 'fies_whlday', YES, YN),
    # rounds 5–6 call the middle group Moderate, FAO's codebook Borderline
    ('poor_food_consumption_pct', 'fcg', ("'1'", "'Poor'"),
     ("'1'", "'2'", "'3'", "'Poor'", "'Borderline'", "'Moderate'", "'Acceptable'")),
    ('crisis_coping_pct', 'lcsi', ("'2'", "'3'", "'Crisis'", "'Emergency'"),
     ("'0'", "'1'", "'2'", "'3'", "'None'", "'stress'", "'Stress'", "'Crisis'", "'Emergency'")),
]
# Answers that are no answer: left out of every denominator. Any other answer
# not listed above stops the build, so a relabelled round cannot drop out quietly.
NO_ANSWER = ("'NA'", "'Don''t know'", "'Do not know'", "'Refused'")


def norm(s):
    """Two spellings of one district name: letters only, and the same with its
    words sorted, so 'Chitral Upper' meets 'UPPER CHITRAL'."""
    w = [x for x in re.findall(r'[a-z]+', (s or '').lower()) if x != 'district']
    return ''.join(w), ''.join(sorted(w))


def prov(s):
    s = (s or '').upper()
    return {'KP': 'KHYBER PAKHTUNKHWA', 'ICT': 'ISLAMABAD', 'OTHER': 'OTHER'}.get(s, s)


class Districts:
    """COD district keys by province and normalised name."""

    def __init__(self):
        self.rows = list(csv.DictReader(open(XW)))
        self.by = {}
        for r in self.rows:
            for k in norm(r['cod_district_key']):
                self.by[(r['province'], k)] = r['cod_district_key']
        # PBS districts: a shape is a set of these. COD splits four Gilgit-Baltistan
        # districts PBS keeps whole; each PBS district is written once, on the COD
        # row that matched, with the area of all its COD parts.
        self.code = {r['cod_district_key']: r['district_code'] for r in self.rows}
        self.area = {}
        for r in self.rows:
            self.area[r['district_code']] = self.area.get(r['district_code'], 0) + float(r['frame_area_km2'])
        self.province_of = {r['cod_district_key']: r['province'] for r in self.rows}

    def find(self, province, name):
        a, b = norm(name)
        if a in ALIAS:
            return ALIAS[a]
        for k in (a, b):
            hit = self.by.get((prov(province), k))
            if hit:
                return hit
        return None

    def in_province(self, p):
        return sorted({r['cod_district_key'] for r in self.rows if r['province'] == p})


def diem(con, D, src, out):
    folders = sorted(p for p in (src / 'pk-diem-microdata').iterdir() if p.is_dir())
    parts = []
    for f in folders:
        key = next(k for k in ROUNDS if f.name.startswith(k))
        rnd, yr = ROUNDS[key]
        csvf = next((f / 'microdata').glob('*.csv'))
        con.execute(f"""CREATE OR REPLACE TABLE h AS SELECT * FROM read_csv('{csvf.as_posix()}',
                        all_varchar=true, sample_size=-1)""")
        n = con.sql('SELECT count(*) FROM h').fetchone()[0]
        # district by name within province; rows with no district are province-only
        names = con.sql("""SELECT DISTINCT adm1_name, adm2_name FROM h
                           WHERE adm2_name IS NOT NULL AND adm2_name <> 'NA'""").fetchall()
        m = {}
        for p, nm in names:
            hit = D.find(p, nm)
            assert hit, f'DIEM round {rnd}: no COD district for {nm}, {p}'
            m[(p, nm)] = (hit, NOTE.get(norm(nm)[0]))
        con.execute('CREATE OR REPLACE TABLE m(adm1_name TEXT, adm2_name TEXT, district_key TEXT, note TEXT)')
        con.executemany('INSERT INTO m VALUES (?, ?, ?, ?)', [(p, nm, k, nt) for (p, nm), (k, nt) in m.items()])
        bad_w = con.sql("""SELECT count(*) FROM h WHERE TRY_CAST(weight_final AS DOUBLE) IS NULL
                           OR TRY_CAST(weight_final AS DOUBLE) <= 0""").fetchone()[0]
        assert not bad_w, f'DIEM round {rnd}: {bad_w} households without a positive weight'
        dropped = con.sql("SELECT count(*) FROM h WHERE adm2_name IS NULL OR adm2_name = 'NA'").fetchone()[0]

        cols = []
        for ind, col, num, den in DIEM:
            known = ', '.join(den + NO_ANSWER)
            odd = con.sql(f'SELECT list(DISTINCT "{col}") FROM h WHERE "{col}" IS NOT NULL '
                          f'AND "{col}" NOT IN ({known})').fetchone()[0]
            assert not odd, f'DIEM round {rnd}, {col}: answers not in the definition: {odd}'
            d = ', '.join(den)
            u = ', '.join(num)
            cols.append(f"""count(*) FILTER (WHERE h."{col}" IN ({d})) AS n_{ind},
                CASE WHEN count(*) FILTER (WHERE h."{col}" IN ({d})) >= {MIN_N} THEN
                  round(100 * coalesce(sum(w) FILTER (WHERE h."{col}" IN ({u})), 0)
                        / sum(w) FILTER (WHERE h."{col}" IN ({d})), 2) END AS {ind}""")
        parts.append(f"""
            SELECT m.district_key, any_value(upper(h.adm1_name)) AS province,
                   {rnd} AS survey_round, {yr} AS survey_year,
                   '{yr} (round {rnd})' AS round_label,
                   count(*) AS households,
                   {', '.join(cols)},
                   string_agg(DISTINCT m.adm2_name, ' + ' ORDER BY m.adm2_name) AS diem_district,
                   any_value(m.note) AS note
            FROM (SELECT *, CAST(weight_final AS DOUBLE) AS w FROM h) h
            JOIN m USING (adm1_name, adm2_name)
            GROUP BY m.district_key""")
        con.execute(f"CREATE OR REPLACE TABLE r{rnd} AS {parts[-1]}")
        kept = con.sql(f'SELECT sum(households) FROM r{rnd}').fetchone()[0]
        assert kept + dropped == n, f'DIEM round {rnd}: {n} households in, {kept} + {dropped} out'
        print(f'  DIEM round {rnd} ({yr}): {n:,} households, {kept:,} in '
              f'{con.sql(f"SELECT count(*) FROM r{rnd}").fetchone()[0]} districts, '
              f'{dropped} with no district left out')
    con.execute('CREATE OR REPLACE TABLE diem AS ' + ' UNION ALL '.join(
        f'SELECT * FROM r{ROUNDS[k][0]}' for k in ROUNDS))
    for ind, *_ in DIEM:
        bad = con.sql(f'SELECT count(*) FROM diem WHERE {ind} < 0 OR {ind} > 100').fetchone()[0]
        assert not bad, f'{ind}: {bad} shares outside 0–100'
    dup = con.sql('SELECT district_key, survey_round, count(*) n FROM diem GROUP BY ALL HAVING n > 1').fetchall()
    assert not dup, f'two DIEM rows for one district and round: {dup}'
    p = out / 'diem_district_round.csv'
    con.execute(f"COPY (SELECT * FROM diem ORDER BY survey_round, province, district_key) TO '{p.as_posix()}' (HEADER)")
    print(f'  -> {p.name}: {con.sql("SELECT count(*) FROM diem").fetchone()[0]} rows')


def emissions(con, D, src, out):
    base = src / 'pk-emissions' / 'pakemis-rc68'
    cw = {r['adm2_id']: r for r in csv.DictReader(open(base / 'pakemis_adm2_crosswalk.csv'))}
    # each PakEmis unit to COD districts; legacy aggregates are named by their parts
    cod_of = {}
    for a, r in cw.items():
        if r['status'] in ('superseded', 'residual', 'statistical_area'):
            continue
        if a == 'ajk_azad_kashmir':
            cod_of[a] = D.in_province('AZAD KASHMIR')
            continue
        hit = D.find(r['province_id'] if r['province_id'] != 'Other' else
                     ('GILGIT BALTISTAN' if a.startswith('gb_') else 'ISLAMABAD'), r['canonical_name'])
        assert hit, f'PakEmis {a} ({r["canonical_name"]}): no COD district'
        cod_of[a] = [hit]

    s = f"read_csv('{(base / 'pakemis_adm2_sector_by_year.csv').as_posix()}', header=true, sample_size=-1)"
    rows = con.sql(f"""SELECT period_id, shape_id, adm2_ids, allocation_status, province,
                              sum(co2e_Mt_AR5) AS mt, sum(co2e_Mt_AR5) FILTER (WHERE module = 'agriculture') AS agri
                       FROM {s} WHERE spatial_view = 'physical' AND shape_id IS NOT NULL
                       GROUP BY ALL""").fetchall()
    unit_shapes = {}
    out_rows = []
    for fy, shape, ids, status, province, mt, agri in rows:
        units = [u for u in ids.split(';') if u in cod_of]
        assert units, f'PakEmis shape {shape} ({ids}) reaches no district'
        rep = {}                                   # PBS district -> the COD row that names it
        for u in units:
            for c in cod_of[u]:
                rep.setdefault(D.code[c], c)
        for u in units:
            unit_shapes.setdefault(u, set()).add(shape)
        area = sum(D.area[k] for k in rep)
        alone = len(rep) == 1
        names = ', '.join(cw[u]['canonical_name'].replace(' District', '') for u in units)
        for c in sorted(rep.values()):
            out_rows.append(dict(
                district_key=c, province=D.province_of[c],
                fy=fy.replace('FY', ''), pakemis_shape_id=shape, pakemis_units=names,
                shape_districts=len(rep), shape_area_km2=round(area, 1),
                shape_ghg_kt_co2e=round(1000 * mt, 3),
                ghg_kt_co2e=round(1000 * mt, 3) if alone else None,
                ghg_t_per_km2=round(1e6 * mt / area, 2),
                agriculture_pct=round(100 * (agri or 0) / mt, 2) if mt else None,
                note=(None if alone else
                      f'PakEmis draws {names} as one area of {len(rep)} districts; its emissions per km² '
                      f'and agriculture’s share are shown for each, its total is not divided between them')))
    two = {u: s for u, s in unit_shapes.items() if len(s) > 1}
    assert not two, f'a PakEmis unit in two shapes: {two}'
    seen = {}
    for r in out_rows:
        k = (r['district_key'], r['fy'])
        assert k not in seen, f'two PakEmis shapes for {k}'
        seen[k] = 1
    p = out / 'emissions_district_year.csv'
    with open(p, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0]))
        w.writeheader()
        w.writerows(sorted(out_rows, key=lambda r: (r['fy'], r['province'], r['district_key'])))
    # the districts' shape totals, each shape once, against the published sum
    con.execute(f"CREATE OR REPLACE TABLE e AS SELECT * FROM read_csv('{p.as_posix()}', header=true)")
    got = con.sql("""SELECT round(sum(t), 1) FROM (SELECT DISTINCT fy, pakemis_shape_id, shape_ghg_kt_co2e / 1000 AS t
                     FROM e WHERE fy = '2023-24')""").fetchone()[0]
    want = con.sql(f"""SELECT round(sum(co2e_Mt_AR5), 1) FROM {s} WHERE spatial_view = 'physical'
                       AND period_id = 'FY2023-24' AND shape_id IS NOT NULL""").fetchone()[0]
    assert got == want, f'PakEmis 2023-24: {got} Mt on the map, {want} published'
    print(f'  -> {p.name}: {len(out_rows)} rows, {len({r["district_key"] for r in out_rows})} COD districts; '
          f'2023-24 mapped {got} Mt CO2e')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--adaad', type=pathlib.Path, default=ADAAD)
    ap.add_argument('--out', type=pathlib.Path, default=OUT)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute('SET threads TO 1')
    D = Districts()
    diem(con, D, a.adaad, a.out)
    emissions(con, D, a.adaad, a.out)


if __name__ == '__main__':
    main()
