"""Stage 4.2: the indicator dictionary.

One row per (table, indicator, column), with its universe, whether it is a rate,
how many observations it has, and how complete it is. This is what the data
dictionary page and the SQL console sidebar read; it is generated from the panel
so it cannot drift from the data.
"""
import argparse, csv, json, pathlib
import duckdb
from table_spec import TITLES

# Each table counts a different population. Getting this wrong is the single
# easiest way to publish a wrong rate, so it is stated per table rather than
# inferred.
UNIVERSE = {
    '2':   'Named urban localities, not administrative units.',
    '3':   'Rural localities grouped by population size; counts are of localities and of people in them.',
    '4':   'All persons, by single year of age.',
    '6':   'Population aged 15 and over, by marital status.',
    '7':   'Population aged 15 and over, by relationship to the head of household.',
    '8':   'All persons, by relationship to the head of household and age group.',
    '10':  'All persons, by nationality. District level only.',
    '13':  'Population in the stated age bracket. District level only.',
    '13b': 'Population in the stated age bracket; literacy is of those aged 10 and over within it.',
    '15':  'Population aged 10 and over, by employment status, for selected age groups.',
    '17':  'All persons, for disability and functional limitation, by selected age group.',
    '19':  'All persons, for migration, by selected age group.',
    '20':  'Housing units, not persons.',
    '21':  'Households and the population living in them.',
    '22':  'Households, not persons.',
    '24':  'Households, not persons.',
    '25':  'Households, not persons.',
    '26':  'Structures, not persons or households.',
    '1':   'All persons enumerated (headcount). Area in square kilometres.',
    '5':   'All persons, by selected age group.',
    '9':   'All persons, by religion.',
    '11':  'All persons, by mother tongue.',
    '12':  'Population aged 5 and over for attendance and enrolment; aged 10 and over for literacy.',
    '13a': 'Population in the stated age bracket; literacy is of those aged 10 and over within it.',
    '14':  'Population aged 10 and over for employment status.',
    '16':  'All persons, for disability and functional limitation.',
    '18':  'All persons, for migration status and reason.',
    '23':  'Households, not persons. Counts are of housing units.',
}
NOTE_RATE = ('A published rate. Never average it across units: recompute from the '
             'summed numerator and denominator after any aggregation.')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--panel', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    out = pathlib.Path(a.out); out.mkdir(parents=True, exist_ok=True)
    rows = duckdb.sql(f"""
      SELECT table_id, indicator, coalesce(col_label,'') AS col_label, any_value(is_rate) AS is_rate,
             count(*) AS observations,
             count(*) FILTER (WHERE value IS NOT NULL) AS with_value,
             count(*) FILTER (WHERE missing) AS missing,
             count(DISTINCT unit) AS units,
             count(DISTINCT locality) AS localities,
             count(DISTINCT sex) AS sexes,
             min(value) AS min_value, max(value) AS max_value
      FROM '{a.panel}' GROUP BY 1,2,3 ORDER BY 1,2,3""").fetchall()
    cols = ['table_id', 'table_title', 'indicator', 'col_label', 'measure', 'universe',
            'observations', 'with_value', 'missing', 'complete_pct', 'units',
            'localities', 'sexes', 'min_value', 'max_value', 'notes']
    with open(out / 'indicator_dictionary.csv', 'w', newline='') as fh:
        w = csv.writer(fh); w.writerow(cols)
        for (t, ind, col, rate, n, wv, miss, u, loc, sx, lo, hi) in rows:
            w.writerow([t, TITLES.get(t, ''), ind, col, 'rate' if rate else 'count',
                        UNIVERSE.get(t, ''), n, wv, miss, round(100 * wv / n, 1) if n else 0,
                        u, loc, sx,
                        None if lo is None else round(lo, 4), None if hi is None else round(hi, 4),
                        NOTE_RATE if rate else ''])
    print(f"indicator dictionary: {len(rows)} entries -> {out/'indicator_dictionary.csv'}")
    per = duckdb.sql(f"""SELECT table_id, count(DISTINCT indicator||'|'||coalesce(col_label,''))
                         FROM '{a.panel}' GROUP BY 1 ORDER BY 1""").fetchall()
    for t, k in per:
        print(f"   T{t:<5} {k:5d} distinct indicator/column combinations")


if __name__ == '__main__':
    main()
