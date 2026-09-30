"""The census's count of enumerated structures, by district and tehsil.

Census 2023 enumerated every building it met and recorded what it was: 24 kinds,
from schools and hospitals to factories, mosques, police stations and cattle
sheds. PBS serves the counts by area through its economic portal.

This is the only dataset here that covers the whole frame Data Darbar draws -
156 districts and 649 tehsils, every district of Azad Jammu & Kashmir and
Gilgit-Baltistan included. Neither census panel has a row for either, and no
survey reaches them, so these are the first values those shapes carry.

Two things not to take at face value:

  "Home" is returned for 9 districts and 42 tehsils out of 156 and 649. The
  pull's own README says to treat it as unavailable, and it is kept out of the
  index for that reason - an indicator that colours 9 shapes of 156 reads as a
  finding rather than as an artefact of the API. It stays in the table for
  anyone who wants it.

  A missing row is not a zero. A district with no jail simply has no jail row,
  so coverage varies by kind: hostels and hotels reach all 156 districts,
  universities 74, orphanages 66. The index records what each actually covers.

Usage:
  build_entities.py --src <economic dir> --tehsil-geo <tehsils_2023_geo.js>
                    --district-geo <districts_2023_geo.js>
                    --out-table <parquet> --out-places <parquet>
                    --out-index <parquet>
"""
import argparse, json, pathlib

import duckdb

# Returned for 9 districts of 156. The portal's README says to treat it as
# unavailable; it is kept in the table and out of the picker.
TOO_SPARSE = {'1'}


from topics import TOPICS


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--pbs-t', required=True, help='PBS tehsil geojson, for the code->dds_id map')
    ap.add_argument('--out-table', required=True)
    ap.add_argument('--out-places', required=True)
    ap.add_argument('--out-index', required=True)
    a = ap.parse_args()

    src = pathlib.Path(a.src)
    ds = json.loads((src / 'entities_ds.json').read_text())
    th = json.loads((src / 'entities_th.json').read_text())

    # PBS tehsil code -> the key the tehsil layer draws on
    pbs = json.loads(pathlib.Path(a.pbs_t).read_text())
    tkey, tname = {}, {}
    for f in pbs['features']:
        p = f['properties']
        if p.get('province') == 'OCCUPIED KASHMIR':
            continue
        code = str(p['tehsil_code'])
        tkey[code] = p['dds_id'] or 'PBS-' + code
        tname[code] = p.get('census_unit') or p.get('tehsil')

    rows = []
    for level, blob in (('district', ds), ('tehsil', th)):
        for uid, block in blob.items():
            for r in block['rows']:
                code = str(r['id'])
                mk = code if level == 'district' else tkey.get(code)
                if not mk:
                    continue
                rows.append((level, code, mk, int(uid), block['unit'],
                             r.get('name'), r['value']))

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    con.execute("""CREATE TABLE census_entities(
        level TEXT, area_code TEXT, map_key TEXT, unit_id INTEGER,
        unit_type TEXT, area TEXT, count BIGINT)""")
    con.executemany('INSERT INTO census_entities VALUES (?,?,?,?,?,?,?)', rows)

    sparse = ', '.join(TOO_SPARSE)
    con.execute(f"""CREATE TABLE places AS
        SELECT level, map_key, area_code AS source_key,
               'exact' AS relation, NULL AS note,
               'facilities' AS group_key,
               'entity_' || unit_id AS indicator,
               NULL AS year, CAST(count AS DOUBLE) AS value
        FROM census_entities WHERE unit_id NOT IN ({sparse})""")

    con.execute(f"""CREATE TABLE ix AS
        SELECT level, 'facilities' AS topic,
               '{TOPICS['facilities'].replace("'", "''")}' AS topic_label,
               'facilities' AS group_key,
               'Enumerated structures — Census 2023' AS group_label,
               'Census 2023 buildings & facilities' AS dataset,
               'entity_' || unit_id AS indicator,
               unit_type AS label,
               NULL::INTEGER AS dp, 'place' AS source,
               []::VARCHAR[] AS years, ['all'] AS localities, ['all'] AS sexes,
               count(DISTINCT map_key) AS shapes,
               count(DISTINCT map_key) AS units,
               min(count)::DOUBLE AS min_value, max(count)::DOUBLE AS max_value
        FROM census_entities WHERE unit_id NOT IN ({sparse})
        GROUP BY level, unit_id, unit_type""")

    for tbl, out in (('census_entities', a.out_table), ('places', a.out_places),
                     ('ix', a.out_index)):
        p = pathlib.Path(out)
        p.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""COPY {tbl} TO '{p.as_posix()}'
                        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")
        n = con.sql(f'SELECT count(*) FROM {tbl}').fetchone()[0]
        print(f'  {tbl:18s} {n:>6,} rows  {p.stat().st_size/1e3:6.1f} KB')

    print()
    print(con.sql("""SELECT level, count(DISTINCT map_key) AS shapes,
                            count(DISTINCT unit_id) AS kinds, count(*) AS rows
                     FROM census_entities GROUP BY 1 ORDER BY 1""").df().to_string(index=False))
    print('\n  widest and narrowest coverage, districts:')
    print(con.sql("""SELECT unit_type, count(DISTINCT map_key) AS districts
                     FROM census_entities WHERE level='district'
                     GROUP BY 1 ORDER BY 2 DESC LIMIT 3""").df().to_string(index=False))
    print(con.sql("""SELECT unit_type, count(DISTINCT map_key) AS districts
                     FROM census_entities WHERE level='district'
                     GROUP BY 1 ORDER BY 2 LIMIT 3""").df().to_string(index=False))


if __name__ == '__main__':
    main()
