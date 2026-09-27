"""Registered emigrants by district, 2011-2024, onto PBS's 2023 frame.

From the Bureau of Emigration & Overseas Employment, read through PBS's
diaspora portal. Two things about it need saying before anyone uses it.

**"overall" is not the sum of the years.** It is 9,858,937 against 8,663,198
for 2011-2024 added up, because the Bureau's register goes back well before
2011. It is kept as its own indicator rather than folded in with the yearly
series, so nobody adds it to them.

**The frame is partial.** The file keys on PBS district codes and joins all 146
of them cleanly, but eleven PBS districts have no row: Chaman, Duki, Surab,
Kharmang, Nagar, Shigar, Upper and Lower Kohistan, Upper Chitral and Keamari.
These are the districts created most recently, and the pattern is not uniform -
Kolai Palas Kohistan and Lower Chitral do appear, their siblings do not. So the
register has followed some splits and not others. Those eleven are left undrawn
rather than given a share of a parent, and the parent's figure is labelled as
covering ground that is now more than one district.

Usage:
  build_diaspora.py --src <diaspora dir> --geo <districts_2023_geo.js>
                    --out-table <parquet> --out-places <parquet>
"""
import argparse, json, pathlib

import duckdb

# PBS districts absent from the register, and the district whose figure still
# covers them. Checked against the crosswalk's split groups, not guessed.
COVERED_BY = {
    '167': '100',   # Chaman, inside Killa Abdullah
    '165': '111',   # Duki, inside Loralai
    '164': '086',   # Surab, inside Kalat
    '155': '129',   # Kharmang, inside Skardu
    '156': '129',   # Shigar, inside Skardu
    '157': '135',   # Nagar, inside Hunza
    '166': '147',   # Keamari, inside Karachi West
    '168': '008',   # Upper Kohistan, inside Kolai Palas Kohistan
    '169': '008',   # Lower Kohistan, inside Kolai Palas Kohistan
    '170': '015',   # Upper Chitral, inside Lower Chitral
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--geo', required=True)
    ap.add_argument('--out-table', required=True)
    ap.add_argument('--out-places', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    csv = pathlib.Path(a.src) / 'diaspora_emigrants_by_district_2011_2024.csv'
    con.execute(f"""CREATE VIEW raw AS SELECT * FROM read_csv('{csv.as_posix()}',
        header=true, quote='"', escape='"', types={{'district_code':'VARCHAR'}})""")

    js = pathlib.Path(a.geo).read_text()
    gj = json.loads(js[js.index('=') + 1:].rstrip().rstrip(';'))
    con.execute('CREATE TABLE geo(code TEXT, name TEXT, prov TEXT)')
    con.executemany('INSERT INTO geo VALUES (?,?,?)',
                    [(f['properties']['code'], f['properties']['n'],
                      f['properties']['p']) for f in gj['features']])

    missing = con.sql("""SELECT code FROM geo WHERE code <> '0'
                         AND code NOT IN (SELECT district_code FROM raw)""").fetchall()
    unexplained = [c for (c,) in missing if c not in COVERED_BY]
    if unexplained:
        print(f'{len(unexplained)} PBS district(s) absent from the register and not '
              f'accounted for: {", ".join(unexplained)}')

    # ── source-faithful table ───────────────────────────────────────────────
    con.execute("""CREATE TABLE diaspora_emigrants_district AS
        SELECT r.district_code, g.name AS district, r.division, r.province,
               r.year, r.emigrants_registered,
               r.year = 'overall' AS is_cumulative
        FROM raw r JOIN geo g ON g.code = r.district_code
        ORDER BY r.district_code, r.year""")

    # ── the Places rows ─────────────────────────────────────────────────────
    cov = ', '.join(f"('{k}', '{v}')" for k, v in sorted(COVERED_BY.items()))
    con.execute(f"""CREATE TABLE places AS
        WITH covered(absent, parent) AS (VALUES {cov}),
        note AS (
          SELECT parent, 'This district''s figure also covers '
                 || string_agg(g.name, ' and ' ORDER BY g.name)
                 || ', which the emigration register does not list separately' AS txt
          FROM covered JOIN geo g ON g.code = covered.absent
          GROUP BY parent)
        SELECT 'district' AS level, r.district_code AS map_key,
               r.district_code AS source_key,
               CASE WHEN note.parent IS NOT NULL THEN 'covers-successors' ELSE 'exact' END
                 AS relation,
               note.txt AS note,
               'migration' AS group_key,
               CASE WHEN r.year = 'overall' THEN 'emigrants_cumulative'
                    ELSE 'emigrants_registered' END AS indicator,
               CASE WHEN r.year = 'overall' THEN NULL ELSE r.year END AS year,
               CAST(r.emigrants_registered AS DOUBLE) AS value
        FROM raw r LEFT JOIN note ON note.parent = r.district_code
        WHERE r.emigrants_registered IS NOT NULL
        ORDER BY indicator, year, map_key""")

    for tbl, out in (('diaspora_emigrants_district', a.out_table),
                     ('places', a.out_places)):
        p = pathlib.Path(out)
        p.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""COPY {tbl} TO '{p.as_posix()}'
                        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")
        n = con.sql(f'SELECT count(*) FROM {tbl}').fetchone()[0]
        print(f'  {tbl:32s} {n:>6,} rows  {p.stat().st_size/1e3:5.1f} KB')

    print()
    print(con.sql("""SELECT indicator, count(DISTINCT year) AS years,
                            count(DISTINCT map_key) AS districts,
                            sum(value) AS total
                     FROM places GROUP BY 1 ORDER BY 1""").df().to_string(index=False))
    print(f'\n{len(COVERED_BY)} PBS districts are not listed separately and are '
          f'covered by {len(set(COVERED_BY.values()))} parents')


if __name__ == '__main__':
    main()
