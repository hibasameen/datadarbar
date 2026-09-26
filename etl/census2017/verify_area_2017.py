"""Check the 2017 panel against PBS's national and provincial tables.

Until now exactly one series had been verified against an independently published
figure: total population in table 1. Everything else rested on closure, which is
internal - it shows the parts sum to the whole as PBS printed it, and is blind to
anything that goes wrong identically on both sides.

PBS also publishes each table for Pakistan and for each province_area, as 216
workbooks reached through a different part of the archive page and absent from
the index the district files come from. Those are genuinely independent figures,
and they cover every table and every column rather than one.

The check: read an area workbook with the same spec the district files use, then
for each (table, indicator, column, locality, sex) compare its published figure
against the sum of that province_area's districts in the panel. Rates are excluded,
being non-additive.

Anything that does not match is reported rather than corrected. A mismatch can
mean an extraction error, a PBS inconsistency, or a series the two levels define
differently, and telling those apart is a judgement this script should not make.
"""
import argparse, collections, csv, json, os, pathlib, re, sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'stage2'))

import duckdb
from read_xls import load, synthesise_banner_merges
from read_workbook import anchor, numbered_columns, header, read, unit_type
from normalise_2017 import canon_map, last_segment
from table_spec_2017 import SPEC_2017

IS_RATE = re.compile(r'\b(RATE|RATIO|PERCENT|PROPORTION|AVERAGE|DENSITY|SIZE)\b')
# What ends the area's own block. A provincial workbook lists the province_area total
# first and then works down through divisions and districts; `unit_type` does not
# recognise a bare DIVISION - correctly, since 2023 has no such tier - so without
# this the reader keeps attributing KALAT DIVISION's rows to BALOCHISTAN and the
# check compares a division against a province_area. That alone produced 7,211 of
# Balochistan's mismatches.
BLOCK_END = re.compile(r'\bDIVISION\b')

# The area workbooks list the areas one after another down a single sheet -
# PAKISTAN, then each province, then FATA and FEDERAL CAPITAL - each with its own
# RURAL and URBAN rows beneath it. None of those labels is a unit the reader
# recognises, and none contains the word DIVISION, so the block opened at the
# area's own row ran to the end of the sheet and every other area's figures were
# emitted as this one's. In table 1 that turned one national series into 13 rows,
# carrying Punjab's 109,989,655 and Sindh's 47,854,510 as if they were Pakistan's,
# and it accounted for 8,519 of the 16,327 apparent differences.
AREA_LABELS = {'PAKISTAN', 'KHYBERPAKHTUNKHWA', 'PUNJAB', 'SINDH', 'BALOCHISTAN',
               'FATA', 'FEDERALCAPITAL', 'ISLAMABAD'}


def area_label(lab):
    """The area a stub label names, or None."""
    k = re.sub(r'[^A-Z]', '', (lab or '').upper())
    return k if k in AREA_LABELS else None


def area_row_index(rows, stub, a, area):
    """The row at which the area's own total block begins.

    A province_area's row is labelled PUNJAB or SINDH, which `unit_type` does not
    recognise as a unit - correctly, since a province_area is not a sub-district unit -
    so the reader never opens a block there and nothing beneath it is emitted.
    Only Islamabad came through on the first attempt, because its row happens to
    read ISLAMABAD DISTRICT.
    """
    want = re.sub(r'[^A-Z]', '', area.upper())[:6]
    for i in range(a + 1, len(rows)):
        if stub >= len(rows[i]):
            continue
        lab = re.sub(r'[^A-Z]', '', str(rows[i][stub] or '').upper())
        if lab.startswith(want):
            return i
    return None


def area_observations(capture, man, table):
    """[(area, indicator, col_label, locality, sex, value)] from the area workbooks."""
    out = []
    for f in man['files']:
        if f['kind'] != 'xls_area' or f['table'] != table:
            continue
        rows, merges = load(os.path.join(capture, f['path']))
        a = anchor(rows)
        if a is None:
            continue
        stub, dcols = numbered_columns(rows, a)
        if not dcols:
            continue
        merges = synthesise_banner_merges(rows, merges, a, dcols)
        cols = header(rows, a, stub, merges, dcols)
        i = area_row_index(rows, stub, a, f['area'])
        if i is None:
            continue
        # cut the sheet at the first division or recognised unit after the area's
        # own block, so only the area's own figures are read
        end = len(rows)
        for j in range(i + 1, len(rows)):
            lab = str(rows[j][stub] or '').strip() if stub < len(rows[j]) else ''
            here = area_label(lab)
            if lab and (BLOCK_END.search(lab.upper()) or unit_type(lab)
                        or (here and here != re.sub(r'[^A-Z]', '', f['area'].upper()))):
                end = j
                break
        rows = rows[:end]
        # The area's own row is declared a unit so the reader opens a block there.
        # Everything after the NEXT unit-like row belongs to its districts or
        # divisions and is dropped.
        for o in read(rows, f['area'], table, spec=SPEC_2017[table],
                      layout=(a, stub, dcols, cols), unit_at={i: f['area']}):
            if o['unit'] != f['area']:
                continue
            if o['value'] is None or o['missing']:
                continue
            # An empty column label and a null one are the same thing, but not
            # to SQL: the panel reaches DuckDB through a CSV, where '' is read as
            # NULL, while this side keeps the empty string Python produced.
            # `'' IS NOT DISTINCT FROM NULL` is false, so every series whose whole
            # header is consumed by locality and sex - all of tables 4 and 5, and
            # much of 12 - failed to match anything at all.
            out.append((f['area'], o['indicator'] or None, o['col_label'] or None,
                        o['locality'], o['sex'], o['value']))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--panel', required=True)
    ap.add_argument('--capture', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    man = json.load(open(os.path.join(a.capture, 'retrieval_manifest.json')))
    d = duckdb.connect()
    d.execute('SET threads TO 1')
    d.execute(f"""CREATE TABLE panel AS
                  SELECT * FROM read_parquet('{os.path.join(a.panel, 'panel_2017.parquet')}')""")

    tables = sorted({f['table'] for f in man['files'] if f['kind'] == 'xls_area'}
                    & set(SPEC_2017), key=int)
    print(f'{len(tables)} tables have an area workbook', flush=True)

    # Collected first, compared in one pass. Comparing row by row meant 67,318
    # separate queries and about a quarter of an hour; as a single join it is
    # seconds, which matters because this is re-run after every change.
    d.execute('CREATE TABLE area (table_id VARCHAR, area VARCHAR, indicator VARCHAR,'
              ' col_label VARCHAR, locality VARCHAR, sex VARCHAR, published DOUBLE)')
    for t in tables:
        obs = area_observations(a.capture, man, t)
        if not obs:
            continue
        seen = collections.defaultdict(set)
        for area, ind, lab, loc, sx, v in obs:
            if lab is not None:
                seen[(t, lab)].add((area, loc, sx))
        cmap, _, _ = canon_map(seen)
        batch = []
        for area, ind, lab, loc, sx, v in obs:
            if IS_RATE.search(f'{ind or ""} {lab or ""}'.upper()):
                continue
            canon = cmap.get((t, lab), lab)
            batch.append((t, area, ind, canon, loc, sx, v))
        d.executemany('INSERT INTO area VALUES (?,?,?,?,?,?,?)', batch)
        print(f'  table {t:3s}  {len(batch):>6,} comparable series', flush=True)

    # A province_area is compared against its own districts. PAKISTAN is compared
    # against every district in the panel PLUS Islamabad's own published figure,
    # because Islamabad has no per-district spreadsheets and without it the two
    # sides do not cover the same ground.
    d.execute("""
        CREATE TABLE result AS
        WITH prov AS (
          SELECT table_id, province_area, locality, sex, indicator, col_label,
                 sum(value) AS s
          FROM panel WHERE unit_type='district' AND NOT missing GROUP BY ALL
        ), nat AS (
          SELECT table_id, locality, sex, indicator, col_label, sum(value) AS s
          FROM panel WHERE unit_type='district' AND NOT missing GROUP BY ALL
        )
        SELECT a.*,
               CASE WHEN a.area='PAKISTAN' THEN nat.s ELSE prov.s END AS panel_sum,
               false AS islamabad_share_unknown
        FROM area a
        LEFT JOIN prov ON prov.table_id=a.table_id AND prov.province_area=a.area
             AND prov.locality=a.locality AND prov.sex=a.sex
             AND prov.indicator IS NOT DISTINCT FROM a.indicator
             AND prov.col_label IS NOT DISTINCT FROM a.col_label
        LEFT JOIN nat ON a.area='PAKISTAN' AND nat.table_id=a.table_id
             AND nat.locality=a.locality AND nat.sex=a.sex
             AND nat.indicator IS NOT DISTINCT FROM a.indicator
             AND nat.col_label IS NOT DISTINCT FROM a.col_label""")

    rows = [dict(table_id=r[0], area=r[1], indicator=r[2], col_label=r[3],
                 locality=r[4], sex=r[5], published=r[6], panel_sum=r[7],
                 status=('no matching series' if r[7] is None or r[8]
                         else 'exact' if abs(r[7] - r[6]) < 0.5 else 'differs'))
            for r in d.execute('SELECT * FROM result').fetchall()]
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    with open(out / 'area_verification_2017.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]), quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    tally = collections.Counter(r['status'] for r in rows)
    by_table = {t: dict(collections.Counter(r['status'] for r in rows if r['table_id'] == t))
                for t in tables}
    json.dump(dict(comparisons=len(rows), tally=dict(tally), by_table=by_table),
              open(out / 'area_verification_2017.json', 'w'), indent=1, sort_keys=True)
    print(f"\n{len(rows):,} comparisons against independently published figures")
    for k, v in sorted(tally.items()):
        print(f'  {k:22s} {v:>7,}')


if __name__ == '__main__':
    main()
