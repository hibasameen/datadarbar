"""The "which key joins what" table: every identifier that names a Pakistani place.

Five identifiers name sub-districts in this repository and they are not
interchangeable. Which one a table carries decides what can be joined to it,
and getting that wrong is quiet - a join on a key that is not unique returns a
plausible answer with units missing.

The counts here are measured from the files rather than asserted, so the table
cannot drift from the data it describes.

Usage:
  build_geography_keys.py --app <app dir> --pbs-d <geojson> --pbs-t <geojson>
                          --bridge <units_to_pbs.csv> --out <parquet>
"""
import argparse, collections, json, pathlib

import duckdb


def feats(path):
    return json.loads(pathlib.Path(path).read_text())['features']


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--app', required=True)
    ap.add_argument('--pbs-d', required=True)
    ap.add_argument('--pbs-t', required=True)
    ap.add_argument('--bridge', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    W = pathlib.Path(a.app) / 'data' / 'warehouse'

    d, t = feats(a.pbs_d), feats(a.pbs_t)
    br = con.sql(f"""SELECT dds_id, dd_id, adm3_pcode FROM
        read_csv('{a.bridge}', header=true, quote='"', escape='"')""").fetchall()
    per_dd = collections.Counter(x[1] for x in br if x[1])

    rows = [
        ('dds_id', 'sub-district',
         "Census 2023's own unit id, DDS-XX-NNNN.",
         len({x[0] for x in br}), True,
         'PBS Digital Census 2023 sub-district layer',
         'The only sub-district key with one polygon per census unit. Use it.'),
        ('dd_id', 'sub-district',
         'geoBoundaries ADM3, 2017 vintage. A hash.',
         len(per_dd), False,
         'geoBoundaries ADM3 (app/data/tehsils_geo.js)',
         f'NOT unique against the 2023 frame: {len({x[0] for x in br})} census units '
         f'share {len(per_dd)} of these, because units were split after the boundary '
         f'was drawn. {sum(1 for v in per_dd.values() if v > 1)} polygons carry more '
         f'than one unit and {max(per_dd.values())} is the worst. Joining on it and '
         f'taking one row per polygon silently drops the rest. Called tehsil_id in '
         f'the satellite and night-lights tables.'),
        ('adm3_pcode', 'sub-district',
         'COD-AB / OCHA p-code.',
         len({x[2] for x in br if x[2]}), False,
         'COD-AB',
         'Partial coverage of the census frame. A different digitisation of the '
         'same borders, not the same lines.'),
        ('tehsil_code', 'sub-district',
         "PBS's own numeric tehsil code.",
         len({str(f['properties']['tehsil_code']) for f in t}), True,
         'PBS Digital Census 2023 tehsil layer',
         'Unique across all 650 PBS tehsils, including the 59 outside the census '
         'frame. The route in from the Mouza Census, via '
         'etl/mouza2020/mouza2020_tehsil_crosswalk_pbs.csv.'),
        ('district_code', 'district',
         "PBS's own numeric district code.",
         len({f['properties']['district_code'] for f in d}), True,
         'PBS Digital Census 2023 district layer',
         'Unique across all 157 PBS districts. The key place_indicators and both '
         'census panels draw districts on.'),
        ('district_key', 'district',
         "Data Darbar's district slug, e.g. abbottabad.",
         con.sql(f"""SELECT count(DISTINCT district_key) FROM
             '{(W / 'district_indicators.parquet').as_posix()}'""").fetchone()[0],
         True, 'Data Darbar, on a 2015 boundary set',
         'A 2015 frame of 147 districts. Maps to district_code through '
         'etl/places/place_map.py, which handles nine spellings and seven real '
         'splits. Called dk in the tehsil payloads.'),
    ]

    con.execute("""CREATE TABLE geography_keys(
        key TEXT, level TEXT, what TEXT, distinct_values BIGINT,
        unique_per_place BOOLEAN, frame TEXT, notes TEXT)""")
    con.executemany('INSERT INTO geography_keys VALUES (?,?,?,?,?,?,?)', rows)

    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT * FROM geography_keys ORDER BY level DESC, key)
                    TO '{out.as_posix()}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")
    print(con.sql("""SELECT key, level, distinct_values AS n, unique_per_place AS uniq
                     FROM geography_keys ORDER BY level DESC, key""").df().to_string(index=False))


if __name__ == '__main__':
    main()
