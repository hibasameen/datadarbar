"""The diaspora portal's country-level files, two of which are not country-level.

remittances_by_country.json and skill_timeseries_by_country.json are keyed by
country and look like country series. They are not:

  remittances   198 country keys, 21 distinct series, and not one country has a
                series of its own. 141 of them - 71 per cent - return the same
                one. Whatever the portal is doing with the country parameter, it
                is not returning that country's remittances.

  skills        198 keys, 56 distinct series, 45 countries sharing the modal
                one and only four - Germany, Guatemala, Nigeria and St Helena -
                with a series to themselves. Also not trustworthy by country.

Publishing either by country would put the same number against 141 countries
and present it as variation. So the national series is taken once - it matches
the national totals in magnitude, about 34.7 in 2024 against roughly 30 billion
US dollars published, and 382,439 emigrants in 2018 - and the country dimension
is dropped with a note rather than carried as if it meant something.

destination_by_year.json is different and is kept as it comes: 199 countries in
2024 with 102 distinct values, Saudi Arabia at 452,373 against Oman's 81,590,
summing to 725,587. That varies the way real country data varies.

Usage:
  build_diaspora_country.py --src <diaspora dir> --out-dir <warehouse>
"""
import argparse, collections, json, pathlib

import duckdb


def modal_series(blobs, key):
    """The series most countries return, and how many return it."""
    sig = {cc: json.dumps(b.get(key), sort_keys=True) for cc, b in blobs.items()}
    counts = collections.Counter(sig.values())
    body, n = counts.most_common(1)[0]
    return json.loads(body), n, len(counts)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--out-dir', required=True)
    a = ap.parse_args()
    D = pathlib.Path(a.src)
    out = pathlib.Path(a.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute('SET threads TO 1')

    # ── remittances: national, monthly ──────────────────────────────────────
    rem = json.loads((D / 'remittances_by_country.json').read_text())
    months, shared, distinct = modal_series(rem, 'monthly')
    print(f'remittances: {len(rem)} country keys, {distinct} distinct series, '
          f'{shared} share the modal one — taking it as the national series')
    con.execute("""CREATE TABLE diaspora_remittances_monthly(
        year INTEGER, month INTEGER, value DOUBLE)""")
    con.executemany('INSERT INTO diaspora_remittances_monthly VALUES (?,?,?)',
                    [(m['year'], m['month'], m.get('value'))
                     for m in months if m.get('value') is not None])

    # ── emigrants by skill: national, by year ───────────────────────────────
    sk = json.loads((D / 'skill_timeseries_by_country.json').read_text())
    rows, shared, distinct = modal_series(sk, 'rows')
    print(f'skills:      {len(sk)} country keys, {distinct} distinct series, '
          f'{shared} share the modal one — taking it as the national series')
    LEVELS = ['HighlyQualified', 'HighlySkilled', 'Skilled', 'SemiSkilled',
              'UnSkilled', 'Total']
    con.execute("""CREATE TABLE diaspora_emigrants_by_skill(
        year INTEGER, mode TEXT, skill_level TEXT, emigrants BIGINT)""")
    con.executemany('INSERT INTO diaspora_emigrants_by_skill VALUES (?,?,?,?)',
                    [(r['Year'], r.get('Mode'), lv, r.get(lv))
                     for r in rows for lv in LEVELS if r.get(lv) is not None])

    # ── destinations: genuinely by country ──────────────────────────────────
    dest = json.loads((D / 'destination_by_year.json').read_text())
    con.execute("""CREATE TABLE diaspora_destinations(
        year INTEGER, country TEXT, iso2 TEXT, iso3 TEXT, continent TEXT,
        emigrants BIGINT)""")
    con.executemany('INSERT INTO diaspora_destinations VALUES (?,?,?,?,?,?)',
                    [(int(y), r.get('name'), r.get('c2'), r.get('c3'),
                      r.get('cc'), r.get('value'))
                     for y, rows_ in dest.items() for r in rows_
                     if r.get('value') is not None])

    # ── occupations, 2024 ───────────────────────────────────────────────────
    occ = json.loads((D / 'occupation_categories_2024.json').read_text())
    con.execute("""CREATE TABLE diaspora_occupations(
        year INTEGER, occupation TEXT, emigrants BIGINT)""")
    con.executemany('INSERT INTO diaspora_occupations VALUES (?,?,?)',
                    [(r.get('year'), r.get('category'), r.get('value'))
                     for r in occ if r.get('value') is not None])

    for t in ('diaspora_remittances_monthly', 'diaspora_emigrants_by_skill',
              'diaspora_destinations', 'diaspora_occupations'):
        p = out / f'{t}.parquet'
        con.execute(f"""COPY {t} TO '{p.as_posix()}'
                        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")
        n = con.sql(f'SELECT count(*) FROM {t}').fetchone()[0]
        print(f'  {t:32s} {n:>6,} rows  {p.stat().st_size/1e3:5.1f} KB')

    print()
    print(con.sql("""SELECT year, sum(value) AS remittances
                     FROM diaspora_remittances_monthly
                     GROUP BY 1 ORDER BY 1 DESC LIMIT 4""").df().to_string(index=False))
    print(con.sql("""SELECT year, sum(emigrants) AS to_all_destinations
                     FROM diaspora_destinations GROUP BY 1 ORDER BY 1 DESC LIMIT 3""")
          .df().to_string(index=False))


if __name__ == '__main__':
    main()
