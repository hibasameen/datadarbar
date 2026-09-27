"""Put the curated district indicators on PBS's 2023 frame, in one long table.

Places is meant to have one picker over everything it can draw. The census half
already works that way - a source-faithful panel plus a small derived index -
and this is the other half: the 231 indicators that arrive from PSLM, LFS, HIES,
MPI, the school and health-access layers and the derived census panel, which
today ship as a 1.8 MB JavaScript blob keyed on a 2015 boundary set.

What comes out is `place_indicators`: one row per (place, indicator, year), keyed
on the PBS district code the map draws, with the same map_key convention the
census panels use - a key can name several shapes, where a district that was
later split is drawn across its successors.

Usage:
  build_place_indicators.py --src <warehouse dir> --geo <districts_2023_geo.js>
                            --out <parquet>
"""
import argparse, json, pathlib, re, sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from place_map import ALIAS, SPLIT, NOTE


def norm(s):
    s = (s or '').upper()
    for w in (' DISTRICT', ' AGENCY', ' PROTECTED AREA'):
        s = s.replace(w, '')
    return re.sub(r'[^A-Z0-9]', '', s)


def pbs_districts(geo):
    """[(code, name, province)] from the layer the map actually draws."""
    js = pathlib.Path(geo).read_text()
    gj = json.loads(js[js.index('=') + 1:].rstrip().rstrip(';'))
    return [(f['properties']['code'], f['properties']['n'], f['properties']['p'])
            for f in gj['features']]


def nice(name):
    return ' '.join(w if w.isupper() and len(w) <= 3 else w.title()
                    for w in name.replace(' DISTRICT', '').split())


def build_map(con, src, geo):
    """slug -> (map_key, relation, note, pbs name, province)."""
    pbs = pbs_districts(geo)
    by_norm, by_code = {}, {}
    for code, name, prov in pbs:
        by_norm.setdefault(norm(name), []).append(code)
        by_code[code] = (name, prov)

    rows = con.sql(f"""SELECT DISTINCT district_key, district, province
                       FROM '{src}/district_indicators.parquet'""").fetchall()
    out, unmatched = {}, []
    for slug, name, prov in rows:
        if slug in SPLIT:
            codes = SPLIT[slug]
            note = NOTE['split'].format(
                old=nice(name),
                new=', '.join(nice(by_code[c][0]) for c in codes))
            out[slug] = (' '.join(codes), 'split', note, name, prov)
            continue
        if slug in ALIAS:
            code = ALIAS[slug]
            note = NOTE['alias'].format(old=nice(name), new=nice(by_code[code][0]))
            out[slug] = (code, 'alias', note, by_code[code][0], by_code[code][1])
            continue
        cand = by_norm.get(norm(name)) or by_norm.get(norm(slug))
        if cand and len(cand) == 1:
            code = cand[0]
            out[slug] = (code, 'exact', '', by_code[code][0], by_code[code][1])
        else:
            unmatched.append((slug, name, prov))
    return out, unmatched


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--geo', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    mp, unmatched = build_map(con, a.src, a.geo)
    if unmatched:
        print(f'{len(unmatched)} district(s) with no PBS code - not drawn:')
        for slug, name, prov in unmatched:
            print(f'   {slug:26s} {name} ({prov})')

    vals = ', '.join(
        "('" + s.replace("'", "''") + "', '" + k + "', '" + rel + "', '"
        + note.replace("'", "''") + "', '" + pname.replace("'", "''") + "', '"
        + prov.replace("'", "''") + "')"
        for s, (k, rel, note, pname, prov) in sorted(mp.items()))

    con.execute(f"""
        CREATE TABLE place_indicators AS
        WITH m(slug, map_key, relation, note, pbs_name, pbs_prov) AS (VALUES {vals})
        SELECT 'district'          AS level,
               m.map_key           AS map_key,
               m.relation          AS relation,
               nullif(m.note, '')  AS note,
               d.district_key      AS source_key,
               m.pbs_name          AS place,
               m.pbs_prov          AS province,
               d.dataset, d.group_key, d.group_label,
               d.indicator, d.label, d.year, d.kind, d.field,
               d.value, d.value_text
        FROM '{a.src}/district_indicators.parquet' AS d
        JOIN m ON m.slug = d.district_key
        ORDER BY d.group_key, d.indicator, d.year, m.map_key""")

    # Two curated district groups keep their values in tables of their own
    # rather than in district_indicators, so they arrive column-shaped and have
    # to be melted. They key on the same slug, so the same map applies.
    EXTRA = {
        'mpi': ('mpi_districts', 'Poverty & Wealth \u2014 PSLM 2019-20',
                'PSLM 2019-20 \u00b7 Alkire\u2013Foster',
                ['mpi', 'H', 'A', 'c_schooling', 'c_attendance', 'c_electricity',
                 'c_cooking_fuel', 'c_sanitation', 'c_water', 'c_housing']),
        'healthAccessDistrict': ('health_access_district',
                'Travel Time to Care \u2014 district',
                'Malaria Atlas accessibility surfaces 2019',
                ['mot_popw_mean', 'mot_popw_median', 'wal_popw_mean', 'wal_popw_median',
                 'mot_pct_pop_gt30', 'mot_pct_pop_gt60', 'mot_pct_pop_gt120',
                 'wal_pct_pop_gt60', 'wal_pct_pop_gt120']),
    }
    for gk, (tbl, glabel, dataset, cols) in EXTRA.items():
        have = {c[0] for c in con.sql(
            f"DESCRIBE SELECT * FROM '{a.src}/{tbl}.parquet'").fetchall()}
        for c in cols:
            if c not in have:
                print(f'  {gk}: no column {c} in {tbl}')
                continue
            con.execute(f"""INSERT INTO place_indicators
                WITH m(slug, map_key, relation, note, pbs_name, pbs_prov) AS (VALUES {vals})
                SELECT 'district', m.map_key, m.relation, nullif(m.note,''),
                       t.district_key, m.pbs_name, m.pbs_prov,
                       ?, ?, ?, ?, ?, NULL, 'indicator', ?, t.{c}, NULL
                FROM '{a.src}/{tbl}.parquet' t JOIN m ON m.slug = t.district_key
                WHERE t.{c} IS NOT NULL""",
                [dataset, gk, glabel, c, c, c])

    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY place_indicators TO '{out.as_posix()}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")

    n, places, inds = con.sql("""SELECT count(*), count(DISTINCT map_key),
                                        count(DISTINCT group_key||'|'||indicator)
                                 FROM place_indicators""").fetchone()
    print(f'\n{n:,} rows -> {out}  ({out.stat().st_size/1e6:.2f} MB)')
    print(f'   {places} map keys, {inds} indicators')
    print(con.sql("""SELECT relation, count(DISTINCT source_key) AS districts,
                            count(*) AS rows FROM place_indicators
                     GROUP BY 1 ORDER BY 2 DESC""").df().to_string(index=False))


if __name__ == '__main__':
    main()
