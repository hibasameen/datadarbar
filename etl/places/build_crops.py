"""Crops by district and fiscal year, from PBS's Agriculture Statistics.

108 crops across 122 districts and 44 fiscal years back to 1981-82, with area,
production and yield. 78,023 rows - which is why the values do not join the
Places payload: three measures over that many rows would more than triple it,
past the point where shipping beats querying. Crops follow the census pattern
instead: a source-faithful table, and index rows that point at it. The picker
lists them; the values are read when one is chosen.

What the frame does and does not cover, checked rather than assumed:

  All 122 district codes join PBS's 2023 layer exactly. 34 of the 156 districts
  Data Darbar draws have no crop rows at all - every district of Azad Jammu &
  Kashmir and Gilgit-Baltistan, which have their own agricultural authorities,
  six of Karachi's seven, the districts created most recently (Chaman, Duki,
  Surab, Sohbatpur, Upper Chitral, Upper and Lower Kohistan, Keamari), and
  Sujawal, whose only rows were the Sindh rice total (below).

  Karachi is one district in this frame, filed under Karachi Central's code.
  It is a vestigial entry rather than a city-wide figure: six crops, nothing at
  all since well before 2024-25.

  Crop ids 124 and 125 are unlabelled on the portal - "newCrop" and "crop2" -
  and 125 is not small: 85,000 hectares in 2024-25. They are kept under those
  names rather than dropped, because dropping a sizeable crop silently is worse
  than carrying one nobody has named.

  Sujawal's rice is the Sindh total. In every year it has, 2004-05 to 2022-23
  (nine years before the district existed), it equals the sum of Sindh's other
  districts to the third decimal, so the map drew Sujawal as 700,000-odd
  hectares of rice and any Sindh or national sum counted the province twice.
  Those rows are dropped, not kept with a flag: the map and the query console
  read this table directly. A row goes only where it still equals the rest of
  its province, so a genuine Sujawal figure in a later pull survives. A scan of
  every crop found no other district carrying its province's total; Adaad's
  crop audit of 19 Oct 2026 found the same.

  Balochistan's Kinno (all eleven years it has, 2010-11 to 2022-23) and its
  sugar beet in 2013-14 (under both of the portal's sugar beet ids) are not
  figures at all. Taken in PBS's district order, each district's area is twice
  the one before - Quetta 0.15, Pishin 0.30, ... Gwadar 80 million thousand
  hectares, a thousand times the country - the mark of a running total dragged
  down a column. No row in those groups can be read back to a value, so the
  whole province-year goes; one Lasbela sugar beet production figure (5,885 t)
  goes with it. The test is the shape, not the size: a province-year whose
  areas, sorted, double at nine steps in ten or more, over at least ten steps.
  Nothing else in the 108 crops comes close.

  Cotton production is in thousand bales of 170 kg, not thousand tonnes, though
  the portal labels every crop "1000 MT": the district rows sum to the official
  bales series (10,218 for 2023-24, 4,908 for the 2022-23 flood year). The
  figures are kept as PBS published them and labelled in bales, so its yield is
  bales per hectare.

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
# (district code, crop) pairs the portal files the provincial total under
PROVINCE_TOTALS = [('152', 'Rice')]   # Sujawal
# crops whose figures are not in the portal's stated units: crop_id -> measure -> unit
UNITS = {5: {'production_000t': 'thousand bales of 170 kg',   # cotton
             'yield_t_per_ha': 'bales per hectare'}}


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

    con.execute(f"""CREATE TABLE src AS
        SELECT *, lpad(CAST(dsid AS VARCHAR), 3, '0') AS code FROM '{a.src}'""")

    # a listed row goes only where it equals the rest of its province, on
    # either measure (Sujawal 2022-23 has production but no area)
    keys = ', '.join(f"('{d}', '{c}')" for d, c in PROVINCE_TOTALS)
    con.execute(f"""CREATE TABLE misfiled AS
        WITH k(code, crop) AS (VALUES {keys}),
        r AS (SELECT s.code, s.crop, s.fy, s.area_000ha AS a, s.production_000t AS p,
                     sum(s.area_000ha) OVER w - coalesce(s.area_000ha, 0) AS rest_a,
                     sum(s.production_000t) OVER w - coalesce(s.production_000t, 0) AS rest_p
              FROM src s JOIN geo g ON g.code = s.code
              WINDOW w AS (PARTITION BY s.crop, s.fy, g.prov))
        SELECT r.code, r.crop, r.fy FROM r JOIN k USING (code, crop)
        WHERE (r.a > 0 AND abs(r.a - r.rest_a) <= 0.001 * r.a)
           OR (r.p > 0 AND abs(r.p - r.rest_p) <= 0.001 * r.p)""")

    # province-years whose areas are a doubling series rather than figures
    con.execute("""CREATE TABLE doubled AS
        WITH r AS (SELECT s.crop_id, s.fy, g.prov,
                          s.area_000ha / lag(s.area_000ha) OVER (
                              PARTITION BY s.crop_id, s.fy, g.prov
                              ORDER BY s.area_000ha) AS ratio
                   FROM src s JOIN geo g ON g.code = s.code
                   WHERE s.area_000ha > 0)
        SELECT crop_id, fy, prov FROM r GROUP BY ALL
        HAVING count(ratio) >= 10
           AND count(*) FILTER (WHERE abs(ratio - 2) < 0.05) >= 0.9 * count(ratio)""")
    con.execute("""CREATE TABLE dropped AS
        SELECT s.code, s.crop_id, s.fy, 'provincial total' AS why
        FROM src s JOIN misfiled m ON m.code = s.code AND m.crop = s.crop AND m.fy = s.fy
        UNION ALL
        SELECT s.code, s.crop_id, s.fy, 'doubling series' AS why
        FROM src s JOIN geo g ON g.code = s.code
        JOIN doubled d ON d.crop_id = s.crop_id AND d.fy = s.fy AND d.prov = g.prov""")
    print(con.sql("""SELECT d.why, s.crop, g.prov AS province, count(*) AS rows_dropped,
                            min(d.fy) AS first, max(d.fy) AS last
                     FROM dropped d
                     JOIN src s ON s.code = d.code AND s.crop_id = d.crop_id AND s.fy = d.fy
                     JOIN geo g ON g.code = d.code
                     GROUP BY ALL ORDER BY 1, 2""").df().to_string(index=False))

    ren = ', '.join(f"('{k}', '{v}')" for k, v in UNNAMED.items())
    con.execute(f"""CREATE TABLE crops_district_fy AS
        WITH ren(raw, nice) AS (VALUES {ren})
        SELECT c.code AS district_code,
               g.name AS district, g.prov AS province,
               c.crop_id, coalesce(ren.nice, c.crop) AS crop, c.fy,
               c.area_000ha, c.production_000t, c.yield_t_per_ha
        FROM src c
        JOIN geo g ON g.code = c.code
        ANTI JOIN dropped d ON d.code = c.code AND d.crop_id = c.crop_id AND d.fy = c.fy
        LEFT JOIN ren ON ren.raw = c.crop
        ORDER BY c.crop_id, c.fy, district_code""")

    unmatched = con.sql(f"""SELECT count(DISTINCT dsid) FROM '{a.src}'
        WHERE lpad(CAST(dsid AS VARCHAR),3,'0') NOT IN (SELECT code FROM geo)""").fetchone()[0]
    if unmatched:
        print(f'{unmatched} crop district code(s) do not join the PBS layer')

    # index rows: one per crop and measure, with the years each actually has
    parts = []
    for col, mlabel, unit, dp in MEASURES:
        odd = ' '.join(f"WHEN {cid} THEN '{u[col]}'"
                       for cid, u in UNITS.items() if col in u)
        unit_sql = f"CASE crop_id {odd} ELSE '{unit}' END" if odd else f"'{unit}'"
        parts.append(f"""
        SELECT 'district' AS level, 'agriculture' AS topic,
               'Agriculture' AS topic_label,
               'crops' AS group_key,
               'Crops — PBS Agriculture Statistics' AS group_label,
               'PBS Agriculture Statistics, pull of 2026-09-27' AS dataset,
               'crop|' || crop_id || '|{col}' AS indicator,
               crop || ' · {mlabel} (' || {unit_sql} || ')' AS label,
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
