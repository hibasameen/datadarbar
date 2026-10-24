#!/usr/bin/env python3
"""Copy the Issue 4 tables from Adaad's folder into the desktop warehouse.

    python3 etl/issue4/pipeline.py [--adaad <Adaad/data>] [--out <warehouse>/issue4]

Reads each table named in tables.py from Adaad/data/pk-*, adds PBS's
district_code to the district tables through district_crosswalk_cod_2023,
spells K-Electric as the State page does, and writes Parquet to
data_darbar_warehouse/issue4/. build_web_warehouse.py --only issue4 publishes
from there, the way the climate, courts and police families publish from
theirs.

The checks are the ones a reader would otherwise have to run: every COD
district reaches a PBS district; no district value sits outside what is
physically possible (the Kinno lesson, PR #27); counts are not lost on the way.
"""
import argparse, os, pathlib, sys

import duckdb

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(HERE.parent))
from issue4.tables import TABLES                       # noqa: E402
from economy.build_disco_payload import KE              # noqa: E402

ADAAD = pathlib.Path(os.environ.get(
    'ADAAD_DATA', pathlib.Path.home() / 'Library/Mobile Documents/com~apple~CloudDocs/Adaad/data'))
OUT = REPO.parent / 'data_darbar_warehouse' / 'issue4'
XW = REPO / 'etl' / 'geography' / 'district_crosswalk_cod_2023.csv'

# Physical ceilings on columns that recur. A value above one is a parse error,
# not a finding.
CEILING = {
    'tmean_c': 45, 'tmax_mean_c': 55, 'tmin_mean_c': 40, 'precip_mm_chirps': 6000,
    'share_of_land': 1.0001, 'share_of_district_area': 1.0001, 'grassland_share': 1.0001,
    'pm25_pop_mean': 400, 'days_tmax_ge40': 366, 'nights_tmin_ge25': 366,
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--adaad', type=pathlib.Path, default=ADAAD)
    ap.add_argument('--out', type=pathlib.Path, default=OUT)
    a = ap.parse_args()
    a.out.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    con.execute(f"""CREATE TABLE xw AS SELECT cod_district_key, district_code
        FROM read_csv('{XW.as_posix()}', header=true, types={{'district_code': 'VARCHAR'}})""")
    ke = ', '.join(f"'{k}'" for k in sorted(KE))

    for t in TABLES:
        f = a.adaad / t['src']
        if not f.exists():
            raise SystemExit(f'{t["name"]}: missing {f}')
        con.execute(f"""CREATE OR REPLACE VIEW raw AS SELECT * FROM read_csv('{f.as_posix()}',
            header=true, auto_detect=true, sample_size=-1)""")
        body = t.get('sql', 'SELECT * FROM {src}').format(src='raw')
        con.execute(f'CREATE OR REPLACE TABLE body AS {body}')
        cols = [r[0] for r in con.sql('DESCRIBE body').fetchall()]

        if t.get('disco'):
            con.execute(f"""UPDATE body SET disco = CASE WHEN disco IN ({ke}) THEN 'K-Electric'
                                                         ELSE trim(disco) END""")
        if t.get('district'):
            assert 'district_key' in cols, f'{t["name"]}: no district_key'
            ren = ['district_key']
            pc = ''
            if 'pcode' in cols and 'cod_adm2_pcode' not in cols:
                ren.append('pcode')
                pc = ', b.pcode AS cod_adm2_pcode'
            con.execute(f"""CREATE OR REPLACE TABLE body AS
                SELECT x.district_code, b.district_key AS cod_district_key{pc},
                       b.* EXCLUDE ({', '.join(ren)})
                FROM body b LEFT JOIN xw x ON x.cod_district_key = b.district_key""")
            lost = con.sql("""SELECT list(DISTINCT cod_district_key) FROM body
                              WHERE district_code IS NULL AND cod_district_key IS NOT NULL""").fetchone()[0]
            assert not lost, f'{t["name"]}: COD districts with no PBS district: {lost}'
            n_codes = con.sql('SELECT count(DISTINCT district_code) FROM body').fetchone()[0]
            want = t.get('codes', 156)
            assert n_codes == want, f'{t["name"]}: reaches {n_codes} PBS districts, not {want}'

        for c, cap in CEILING.items():
            if c in cols:
                bad = con.sql(f'SELECT count(*) FROM body WHERE "{c}" > {cap}').fetchone()[0]
                assert not bad, f'{t["name"]}: {bad} rows of {c} above {cap}'

        n_in = con.sql('SELECT count(*) FROM raw').fetchone()[0]
        n_out = con.sql('SELECT count(*) FROM body').fetchone()[0]
        if 'sql' not in t:
            assert n_in == n_out, f'{t["name"]}: {n_in} rows in, {n_out} out'
        out = a.out / f'{t["name"]}.parquet'
        con.execute(f"COPY body TO '{out.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        print(f'  {t["name"]:<30} {n_out:>8,} rows')


if __name__ == '__main__':
    main()
