"""Crops by district and fiscal year, from PBS's Agriculture Statistics.

108 crops across 123 districts and 44 fiscal years back to 1981-82, with area,
production and yield. 78,425 rows - which is why the values do not join the
Places payload: three measures over that many rows would more than triple it,
past the point where shipping beats querying. Crops follow the census pattern
instead: a source-faithful table, and index rows that point at it. The picker
lists them; the values are read when one is chosen.

What the frame does and does not cover, checked rather than assumed:

  All 123 district codes join PBS's 2023 layer exactly. 33 of the 156 districts
  Data Darbar draws have no crop rows at all - every district of Azad Jammu &
  Kashmir and Gilgit-Baltistan, which have their own agricultural authorities,
  six of Karachi's seven, and the districts created most recently (Chaman,
  Duki, Surab, Sohbatpur, Upper Chitral, Upper and Lower Kohistan, Keamari).

  Karachi is one district in this frame, filed under Karachi Central's code.
  It is a vestigial entry rather than a city-wide figure: six crops, nothing at
  all since well before 2024-25.

  Crop ids 124 and 125 are unlabelled on the portal - "newCrop" and "crop2" -
  and 125 is not small: 85,000 hectares in 2024-25. They are kept under those
  names rather than dropped, because dropping a sizeable crop silently is worse
  than carrying one nobody has named.

Usage:
  build_crops.py --src <parquet> --geo <districts_2023_geo.js>
                 --out-table <parquet> --out-index <parquet>
"""
import argparse, json, pathlib

import duckdb

MEASURES = [
    ('area_000ha', 'Area', 'thousand hectares', 1),
    ('production_000t', 'Production', 'thousand tonnes', 1),
    ('yield_t_per_ha', 'Yield', 'tonnes per hectare', 2),
]
UNNAMED = {'newCrop': 'Unnamed crop (portal id 124)',
           'crop2': 'Unnamed crop (portal id 125)'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--geo', required=True)
    ap.add_argument('--out-table', required=True)
    ap.add_argument('--out-index', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    js = pathlib.Path(a.geo).read_text()
    gj = json.loads(js[js.index('=') + 1:].rstrip().rstrip(';'))
    con.execute('CREATE TABLE geo(code TEXT, name TEXT, prov TEXT)')
    con.executemany('INSERT INTO geo VALUES (?,?,?)',
                    [(f['properties']['code'], f['properties']['n'],
                      f['properties']['p']) for f in gj['features']])

    ren = ', '.join(f"('{k}', '{v}')" for k, v in UNNAMED.items())
    con.execute(f"""CREATE TABLE crops_district_fy AS
        WITH ren(raw, nice) AS (VALUES {ren})
        SELECT lpad(CAST(c.dsid AS VARCHAR), 3, '0') AS district_code,
               g.name AS district, g.prov AS province,
               c.crop_id, coalesce(ren.nice, c.crop) AS crop, c.fy,
               c.area_000ha, c.production_000t, c.yield_t_per_ha
        FROM '{a.src}' c
        JOIN geo g ON g.code = lpad(CAST(c.dsid AS VARCHAR), 3, '0')
        LEFT JOIN ren ON ren.raw = c.crop
        ORDER BY c.crop_id, c.fy, district_code""")

    unmatched = con.sql(f"""SELECT count(DISTINCT dsid) FROM '{a.src}'
        WHERE lpad(CAST(dsid AS VARCHAR),3,'0') NOT IN (SELECT code FROM geo)""").fetchone()[0]
    if unmatched:
        print(f'{unmatched} crop district code(s) do not join the PBS layer')

    # index rows: one per crop and measure, with the years each actually has
    parts = []
    for col, mlabel, unit, dp in MEASURES:
        parts.append(f"""
        SELECT 'district' AS level, 'agriculture' AS topic,
               'Agriculture' AS topic_label,
               'crops' AS group_key,
               'Crops — PBS Agriculture Statistics' AS group_label,
               'PBS Agriculture Statistics, pull of 2026-09-27' AS dataset,
               'crop|' || crop_id || '|{col}' AS indicator,
               crop || ' · {mlabel} ({unit})' AS label,
               {dp} AS dp, 'crops' AS source,
               list_sort(list_distinct(list(fy))) AS years,
               ['all'] AS localities, ['all'] AS sexes,
               count(DISTINCT district_code) AS shapes,
               count(DISTINCT district_code) AS units,
               min({col}) AS min_value, max({col}) AS max_value
        FROM crops_district_fy WHERE {col} IS NOT NULL
        GROUP BY crop_id, crop""")
    con.execute('CREATE TABLE crops_index AS ' + ' UNION ALL '.join(parts))

    for tbl, out in (('crops_district_fy', a.out_table), ('crops_index', a.out_index)):
        p = pathlib.Path(out)
        p.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""COPY {tbl} TO '{p.as_posix()}'
                        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")
        n = con.sql(f'SELECT count(*) FROM {tbl}').fetchone()[0]
        print(f'  {tbl:22s} {n:>7,} rows  {p.stat().st_size/1e6:5.2f} MB')

    print()
    print(con.sql("""SELECT count(DISTINCT crop) AS crops,
                            count(DISTINCT district_code) AS districts,
                            count(DISTINCT fy) AS years, min(fy) AS first, max(fy) AS last
                     FROM crops_district_fy""").df().to_string(index=False))
    print('\n  largest by area in 2024-25:')
    print(con.sql("""SELECT crop, round(sum(area_000ha),0) AS area_000ha
                     FROM crops_district_fy WHERE fy='2024-25'
                     GROUP BY 1 ORDER BY 2 DESC NULLS LAST LIMIT 5""").df().to_string(index=False))


if __name__ == '__main__':
    main()
