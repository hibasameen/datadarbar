"""Build the Census 2017 locality panel from tables 23-26.

Rows here are places - a mauza, deh or urban locality - not published
administrative units, so this is a separate release from the unit panel, exactly
as 2023's tables 31-34 are.

The reader is 2023's (`etl/stage2/read_localities.py`) unchanged but for one
parameter: it decided which tables were urban from their 2023 numbers, and 2017
publishes the same four as 23-26. Everything else carries over, including the
revenue hierarchies that differ by area - QH and PC in Punjab, KP and
Balochistan, STC and TC in Sindh, and the TRIBE and SECTION levels that KP's
ex-FATA districts nest between the tehsil and the village.

The shape of the input differs: 2023 publishes one workbook per table per region,
2017 one per table per district, so this reads about 540 files where the 2023
builder reads 20.

Subtotal rows are emitted, tagged with their level, because they are published
figures worth validating against - but they must never be summed with their
children, which roughly triples the population.
"""
import argparse, collections, csv, json, os, pathlib, sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'stage2'))

import duckdb
from read_localities import read, relabel_unsuffixed_groupings
from read_xls import load, synthesise_banner_merges
from headerless import is_empty, reason_text, synth_columns
from read_workbook import anchor, numbered_columns
from extract_2017 import area_of

TABLES = ['23', '24', '25', '26']
URBAN = {'25', '26'}
TITLES = {'23': 'Selected population statistics of individual rural localities',
          '24': 'Selected housing statistics of individual rural localities',
          '25': 'Selected population statistics of urban localities',
          '26': 'Selected housing statistics of urban localities'}
# Whether a locality table is empty is read off the sheet rather than listed
# here: a district with no urban population has no urban localities, and a wholly
# urban one has no rural localities. Hard-coding the pairs would go stale and
# would not distinguish an explicable absence from a parsing failure.


def workbooks(man, table):
    """Every district's workbook for one locality table, Islamabad included."""
    out = [f for f in man['files'] if f['kind'] == 'xlsx' and f['table'] == table]
    out += [f for f in man['files'] if f['kind'] == 'xls_area'
            and f.get('area') == 'ISLAMABAD' and f['table'] == table]
    return sorted(out, key=lambda f: f['path'])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', required=True, help='the dated capture directory')
    ap.add_argument('--out', required=True)
    ap.add_argument('--unit-panel', help='panel_2017.parquet, for the rural reconciliation')
    a = ap.parse_args()
    man = json.load(open(os.path.join(a.dir, 'retrieval_manifest.json')))
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    obs, problems, empty = [], [], []
    for t in TABLES:
        files = workbooks(man, t)
        print(f'table {t}: {len(files)} workbooks', flush=True)
        for i, f in enumerate(files, 1):
            rows, merges = load(os.path.join(a.dir, f['path']))
            ai = anchor(rows)
            if ai is None:
                # These are the workbooks PBS's PDF-to-Excel conversion left
                # without a header block. Almost all are empty for a reason the
                # census itself gives: a district with no urban population has no
                # urban localities to list, and one that is wholly urban has no
                # rural ones. PBS sometimes writes the reason into the sheet -
                # Lahore's table 23 contains the single cell "LAHORE IS
                # URBANIZED." An empty file is not a failure and must not be
                # reported as one; a headerless file that does hold data is.
                if is_empty(rows):
                    empty.append(dict(table=t, path=f['path'],
                                      note=reason_text(rows) or 'no data rows'))
                else:
                    problems.append(dict(table=t, path=f['path'],
                                         issue='header block missing, but the file holds data'))
                continue
            stub, dcols = numbered_columns(rows, ai)
            merges = synthesise_banner_merges(rows, merges, ai, dcols)
            try:
                recs = list(read(rows, area_of(f), t, merges, urban=(t in URBAN)))
            except Exception as e:
                problems.append(dict(table=t, path=f['path'],
                                     issue=f'{type(e).__name__}: {e}'))
                continue
            if t in ('23', '24') and recs:
                key = next((k for k in recs[0]
                            if 'POPULATION' in k.upper() and 'ALL SEXES' in k.upper()), None) \
                      or next((k for k in recs[0] if 'TOTAL' in k.upper()), None)
                if key:
                    recs = relabel_unsuffixed_groupings(recs, key)
            if not recs:
                # Same distinction, reached the other way: the sheet has a header
                # but no place rows beneath it.
                empty.append(dict(table=t, path=f['path'],
                                  note=reason_text(rows) or 'header present, no place rows'))
                continue
            for rec in recs:
                rec['source_file'] = f['path']
                obs.append(rec)
            if i % 40 == 0:
                print(f'  {i}/{len(files)}  {len(obs):,} rows', flush=True)

    fixed = ['own_id', 'parent_id', 'province_area', 'table_id', 'district', 'sub_district',
             'qanungo_halqa', 'patwar_circle', 'charge', 'locality', 'name', 'level',
             'hadbast', 'locality_type', 'missing', 'source_file', 'src_row', 'relabelled', 'relabel_rule']
    long = []
    for r in obs:
        r.setdefault('relabelled', False)
        r.setdefault('relabel_rule', None)
        miss = set((r['missing'] or '').split(';')) - {''}
        for k, v in r.items():
            if k in fixed:
                continue
            long.append(dict({f: r[f] for f in fixed if f != 'missing'},
                             indicator=k, value=v, missing=False))
        for k in miss:
            long.append(dict({f: r[f] for f in fixed if f != 'missing'},
                             indicator=k, value=None, missing=True))

    cols = [f for f in fixed if f != 'missing'] + ['indicator', 'value', 'missing']
    tmp = out / '_long.csv'
    with open(tmp, 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=cols, extrasaction='ignore')
        w.writeheader(); w.writerows(long)
    con = duckdb.connect()
    con.execute('SET threads TO 1'); con.execute('SET preserve_insertion_order TO true')
    types = {c: 'VARCHAR' for c in cols}
    types.update(value='DOUBLE', missing='BOOLEAN', relabelled='BOOLEAN', src_row='INTEGER')
    types.update(relabel_rule='VARCHAR')
    spec = ', '.join(f"'{k}': '{v}'" for k, v in types.items())
    con.execute(f"""COPY (SELECT * FROM read_csv('{tmp}', header=true, quote='"', escape='"',
        columns={{{spec}}}) ORDER BY province_area, district, sub_district, patwar_circle,
        locality, charge, name, table_id, indicator)
        TO '{out / 'locality_observations_2017.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    tmp.unlink()

    reg_rows, seen = [], set()
    for r in obs:
        k = (r['province_area'], r['district'], r['sub_district'], r['patwar_circle'],
             r['locality'], r['charge'], r['name'], r['hadbast'])
        if k in seen:
            continue
        seen.add(k)
        reg_rows.append({f: r[f] for f in
                         ['own_id', 'parent_id', 'province_area', 'district', 'sub_district',
                          'qanungo_halqa', 'patwar_circle', 'charge', 'locality', 'name',
                          'level', 'hadbast', 'locality_type']})
    reg_rows.sort(key=lambda r: tuple(str(r[k] or '') for k in
                                      ['province_area', 'district', 'sub_district',
                                       'patwar_circle', 'locality', 'charge', 'name']))
    with open(out / 'locality_register_2017.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(reg_rows[0])); w.writeheader(); w.writerows(reg_rows)

    P = f"'{out / 'locality_observations_2017.parquet'}'"
    closure = con.sql(f"""
      WITH v AS (SELECT own_id, parent_id, level, indicator, value
                 FROM {P} WHERE table_id='23' AND NOT missing AND value IS NOT NULL
                   AND indicator NOT LIKE '%RATIO%' AND indicator NOT LIKE '%(%)%'),
           kids AS (SELECT parent_id, indicator, sum(value) s FROM v
                    WHERE level='mauza' AND parent_id IS NOT NULL GROUP BY 1,2)
      SELECT count(*), count(*) FILTER (WHERE abs(p.value-k.s) < 0.5)
      FROM (SELECT own_id AS parent_id, indicator, value FROM v
            WHERE level='patwar_circle') p
      JOIN kids k USING (parent_id, indicator)""").fetchone()

    # The mauza sum must count population only. `%ALL SEXES%` alone also matches
    # `LITERACY % (10+ YEARS) / ALL SEXES`, so the first version of this check was
    # adding a percentage to a population and reporting the result as an
    # over-count - Okara appeared 8.98% over when the true figure is 7.1%, and the
    # mauza count read 1,810 where there are 905 places carrying two indicators
    # each. The same mistake as 2023's rate detector missing PERCENT.
    recon = []
    if a.unit_panel and pathlib.Path(a.unit_panel).exists():
        recon = con.sql(f"""
          WITH m AS (SELECT province_area, district, sum(value) mauza_sum FROM {P}
                     WHERE table_id='23' AND level='mauza' AND NOT missing
                       AND indicator LIKE '%POPULATION / ALL SEXES%'
                       AND indicator NOT LIKE '%\%%' ESCAPE '\\'
                       AND indicator NOT LIKE '%RATIO%' GROUP BY 1,2),
               p AS (SELECT province_area, district, sum(value) published_rural
                     FROM '{a.unit_panel}'
                     WHERE table_id='1' AND unit_type='district' AND locality='rural'
                       AND col_label LIKE '%ALL SEXES%' AND NOT missing GROUP BY 1,2)
          SELECT p.province_area, p.district, published_rural, mauza_sum,
                 mauza_sum - published_rural AS excess,
                 CASE WHEN published_rural > 0
                      THEN round(100.0*(mauza_sum-published_rural)/published_rural, 2) END AS pct,
                 CASE WHEN published_rural > 0
                       AND abs(mauza_sum-published_rural) <= 0.01*published_rural
                      THEN 'reconciles' ELSE 'hierarchy_unreliable' END AS status
          FROM p JOIN m USING (province_area, district) ORDER BY excess DESC""").fetchall()
        with open(out / 'rural_reconciliation_2017.csv', 'w', newline='') as fh:
            w = csv.writer(fh)
            w.writerow(['province_area', 'district', 'published_rural', 'mauza_sum',
                        'excess', 'pct', 'status'])
            w.writerows(recon)

    jn = con.sql(f"""
      WITH x AS (SELECT DISTINCT own_id FROM {P} WHERE table_id='23' AND level='mauza'),
           y AS (SELECT DISTINCT own_id FROM {P} WHERE table_id='24' AND level='mauza')
      SELECT (SELECT count(*) FROM x), (SELECT count(*) FROM y),
             (SELECT count(*) FROM x SEMI JOIN y USING (own_id))""").fetchone()

    lv = collections.Counter(r['level'] for r in reg_rows)
    byarea = collections.defaultdict(lambda: [0, 0])
    for area, dist, pub, ms, exc, pct, st in recon:
        byarea[area][0] += 1
        byarea[area][1] += (st == 'reconciles')
    rep = dict(observations=len(long), places=len(reg_rows), levels=dict(lv),
               relabelled_groupings=sum(1 for r in obs if r.get('relabelled')),
               table23_24_join=dict(in_23=jn[0], in_24=jn[1], joining=jn[2]),
               rural_reconciliation={k: dict(districts=v[0], reconciling=v[1])
                                     for k, v in sorted(byarea.items())},
               missing_cells=sum(1 for r in long if r['missing']),
               with_hadbast=sum(1 for r in reg_rows if r['hadbast']),
               patwar_circle_closure=dict(comparisons=closure[0], closing=closure[1]),
               explicably_empty=empty, problems=problems)
    (out / 'locality_report_2017.json').write_text(json.dumps(rep, indent=2, sort_keys=True))
    print(f"\nobservations      {len(long):,}")
    print(f"places            {len(reg_rows):,}   {dict(lv)}")
    print(f"with an identifier {rep['with_hadbast']:,}")
    print(f"missing cells     {rep['missing_cells']:,}")
    print(f"23<->24 place join {jn[2]:,}/{jn[0]:,} mauzas"
          + (f"  ({100*jn[2]/jn[0]:.1f}%)" if jn[0] else ""))
    print(f"patwar-circle closure {closure[1]:,}/{closure[0]:,}"
          + (f"  ({100*closure[1]/closure[0]:.1f}%)" if closure[0] else ""))
    if recon:
        print("\nmauza sum vs published rural population:")
        for area, (n, ok) in sorted(byarea.items()):
            flag = '' if ok == n else '   <-- hierarchy unreliable'
            print(f"    {area:22s} {ok:3d}/{n:3d} districts reconcile{flag}")
    print(f"explicably empty  {len(empty)}")
    print(f"problems          {len(problems)}")
    for p in problems[:8]:
        print("  PROBLEM", p)


if __name__ == '__main__':
    main()
