"""Places rows for the Issue 4 measures: climate, heat, air, land, fire, water.

Reads each group's table from the web warehouse (etl/issue4 put it there),
carries its COD-frame districts onto PBS district codes through
district_crosswalk_cod_2023, and writes value rows in the shape
build_place_index.py --extra takes. The groups themselves, and how a folded
Gilgit-Baltistan district is combined, are declared in issue4_groups.py.

Usage:
  build_issue4.py --warehouse <app/data/warehouse> --out <parquet>
"""
import argparse, pathlib

import duckdb

from issue4_groups import GROUPS

HOW = {'sum': 'added together', 'mean': 'averaged by area', 'max': 'taking the larger'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    W = pathlib.Path(a.warehouse)

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    con.execute("CREATE MACRO title(s) AS array_to_string(list_transform("
                "string_split(lower(s), ' '), w -> upper(w[1]) || w[2:]), ' ')")
    con.execute(f"""CREATE TABLE xw AS SELECT cod_district_key, district_code, relation AS xrel,
                           area_weight AS w, district_2023
                    FROM '{(W / 'district_crosswalk_cod_2023.parquet').as_posix()}'""")
    parts = []
    for g in GROUPS:
        t = (W / f"{g['table']}.parquet").as_posix()
        where = f"WHERE {g['where']}" if g.get('where') else ''
        yr = f"CAST({g['year']} AS VARCHAR)" if g['year'] else 'NULL'
        for i in g['indicators']:
            note = (g['note_expr'].format(**i) if g.get('note_expr') else 'NULL')
            if g.get('grouped'):
                base = (f"SELECT cod_district_key, {yr} AS yr, {i['expr']} AS v, NULL AS note "
                        f"FROM '{t}' {where} GROUP BY ALL")
            else:
                base = (f"SELECT cod_district_key, {yr} AS yr, {i['expr']} AS v, {note} AS note "
                        f"FROM '{t}' {where}")
            agg = {'sum': 'sum(b.v)',
                   'mean': 'sum(b.v * x.w) FILTER (WHERE b.v IS NOT NULL) '
                           '/ nullif(sum(x.w) FILTER (WHERE b.v IS NOT NULL), 0)',
                   'max': 'max(b.v)'}[i['agg']]
            parts.append(f"""
                SELECT 'district' AS level, x.district_code AS map_key,
                       string_agg(DISTINCT b.cod_district_key, ' + ' ORDER BY b.cod_district_key) AS source_key,
                       CASE WHEN count(DISTINCT b.cod_district_key) > 1 THEN 'combined'
                            WHEN any_value(x.xrel) = 'renamed' THEN 'alias' ELSE 'exact' END AS relation,
                       concat_ws('. ',
                         CASE WHEN count(DISTINCT b.cod_district_key) > 1 THEN
                           'COD draws this district as ' || string_agg(DISTINCT title(b.cod_district_key), ', '
                             ORDER BY title(b.cod_district_key)) || '; their figures are {HOW[i['agg']]} here' END,
                         string_agg(DISTINCT b.note, '. ')) AS note,
                       '{g['group_key']}' AS group_key, '{i['ind']}' AS indicator, b.yr AS year,
                       CAST({agg} AS DOUBLE) AS value
                FROM ({base}) b JOIN xw x USING (cod_district_key)
                GROUP BY x.district_code, b.yr""")
    con.execute('CREATE TABLE places AS ' + ' UNION ALL '.join(parts))
    con.execute("UPDATE places SET note = NULL WHERE note = ''")
    con.execute('DELETE FROM places WHERE value IS NULL')

    # one value per place, indicator and year, or the map shows whichever it reads last
    dup = con.sql("""SELECT group_key, indicator, year, map_key, count(*) n FROM places
                     GROUP BY ALL HAVING n > 1 LIMIT 3""").fetchall()
    assert not dup, f'duplicate place values: {dup}'
    sizes = con.sql("""SELECT group_key, count(*) n FROM places GROUP BY 1 ORDER BY n DESC""").fetchall()
    assert sizes[0][1] < 16_000, f'{sizes[0][0]} has {sizes[0][1]:,} values: split it'

    p = pathlib.Path(a.out)
    p.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * FROM places ORDER BY group_key, indicator, year, map_key) "
                f"TO '{p.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)")
    print(con.sql("""SELECT group_key, indicator, count(DISTINCT year) AS years,
                            count(DISTINCT map_key) AS districts, count(*) AS n,
                            round(min(value), 2) AS lo, round(max(value), 2) AS hi
                     FROM places GROUP BY ALL ORDER BY 1, 2""").df().to_string(index=False))
    print(f'  -> {p} ({p.stat().st_size/1e3:.0f} KB)')


if __name__ == '__main__':
    main()
