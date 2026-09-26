"""Build the locality panel from tables 31-34.

Rows here are places, not published administrative units, so this is a separate
release from the unit panel. The equivalent of the district closure check is
whether each level of the revenue hierarchy sums to its parent: mauzas to their
patwar circle, circles to their qanungo halqa, and those to the tehsil.
"""
import argparse, collections, csv, json, pathlib
import duckdb, openpyxl
from read_localities import read, relabel_unsuffixed_groupings
from read_workbook import merged_ranges

REGIONS = {'kp': 'KHYBER PAKHTUNKHWA', 'punjab': 'PUNJAB', 'sindh': 'SINDH',
           'balochistan': 'BALOCHISTAN', 'islamabad': 'ISLAMABAD'}
TABLES = ['31', '32', '33', '34']
TITLES = {'31': 'Selected population statistics of individual rural localities',
          '32': 'Selected housing statistics of individual rural localities',
          '33': 'Selected population statistics of urban localities',
          '34': 'Selected housing statistics of urban localities'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--unit-panel', help='census2023_observations.parquet, for the rural reconciliation')
    a = ap.parse_args()
    src, out = pathlib.Path(a.src), pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    obs, problems = [], []
    for t in TABLES:
        for reg, prov in REGIONS.items():
            f = src / 'xlsx' / f'table_{t}_{reg}.xlsx'
            if not f.exists():
                problems.append(dict(table=t, region=reg, issue='file missing')); continue
            ws = openpyxl.load_workbook(f, read_only=True, data_only=True).worksheets[0]
            g = [list(r) for r in ws.iter_rows(values_only=True)]
            recs = list(read(g, prov, t, merged_ranges(f)))
            if t in ('31', '32'):
                key = next((k for k in (recs[0] if recs else {})
                            if 'POPULATION' in k.upper() and 'ALL SEXES' in k.upper()),
                           None) or next((k for k in (recs[0] if recs else {})
                                          if 'TOTAL' in k.upper()), None)
                if key:
                    recs = relabel_unsuffixed_groupings(recs, key)
            n = 0
            for rec in recs:
                rec['source_file'] = f.name
                obs.append(rec); n += 1
            if not n:
                problems.append(dict(table=t, region=reg, issue='no rows'))

    # long format: one row per place per measure
    fixed = ['own_id', 'parent_id', 'province_area', 'table_id', 'district', 'sub_district',
             'qanungo_halqa', 'patwar_circle', 'charge', 'locality', 'name', 'level',
             'hadbast', 'locality_type', 'missing', 'source_file', 'src_row',
             'relabelled']
    long = []
    for r in obs:
        r.setdefault('relabelled', False)
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
    con.execute("SET threads TO 1"); con.execute("SET preserve_insertion_order TO true")
    types = {c: 'VARCHAR' for c in cols}
    types['value'] = 'DOUBLE'
    types['missing'] = 'BOOLEAN'
    types['relabelled'] = 'BOOLEAN'
    types['src_row'] = 'INTEGER'
    spec = ', '.join(f"'{k}': '{v}'" for k, v in types.items())
    con.execute(f"""COPY (SELECT * FROM read_csv('{tmp}', header=true, quote='"', escape='"',
        columns={{{spec}}}) ORDER BY province_area, district, sub_district, patwar_circle,
        locality, charge, name, table_id, indicator)
        TO '{out / 'locality_observations.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    tmp.unlink()

    # register of places
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
    with open(out / 'locality_register.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(reg_rows[0])); w.writeheader(); w.writerows(reg_rows)

    # closure: mauzas -> patwar circle, for table 31 population
    P = f"'{out / 'locality_observations.parquet'}'"
    closure = con.sql(f"""
      WITH v AS (SELECT own_id, parent_id, level, indicator, value
                 FROM {P} WHERE table_id='31' AND NOT missing AND value IS NOT NULL
                   AND indicator NOT LIKE '%RATIO%' AND indicator NOT LIKE '%(%)%'),
           kids AS (SELECT parent_id, indicator, sum(value) s, count(*) n FROM v
                    WHERE level='mauza' AND parent_id IS NOT NULL GROUP BY 1,2)
      SELECT count(*) comparisons,
             count(*) FILTER (WHERE abs(p.value-k.s) < 0.5) closing
      FROM (SELECT own_id AS parent_id, indicator, value FROM v
            WHERE level='patwar_circle') p
      JOIN kids k USING (parent_id, indicator)""").fetchone()

    # The decisive check: do a district's mauzas sum to its published rural
    # population? Where they do not, the district's hierarchy contains levels
    # PBS has not marked - in the ex-FATA districts these are named after tribes
    # and areas, with no suffix, and the mauza level triples or worse.
    recon = []
    if a.unit_panel and pathlib.Path(a.unit_panel).exists():
        recon = con.sql(f"""
          WITH m AS (SELECT province_area, district, sum(value) mauza_sum FROM {P}
                     WHERE table_id='31' AND level='mauza' AND NOT missing
                       AND indicator LIKE '%POPULATION / ALL SEXES%' GROUP BY 1,2),
               p AS (SELECT province_area, district, sum(value) published_rural
                     FROM '{a.unit_panel}'
                     WHERE table_id='1' AND unit_type='district' AND locality='rural'
                       AND indicator LIKE '%ALL SEXES%' AND NOT missing GROUP BY 1,2)
          SELECT p.province_area, p.district, published_rural, mauza_sum,
                 mauza_sum - published_rural AS excess,
                 CASE WHEN published_rural > 0
                      THEN round(100.0*(mauza_sum-published_rural)/published_rural, 2) END AS pct,
                 CASE WHEN published_rural > 0
                       AND abs(mauza_sum-published_rural) <= 0.01*published_rural
                      THEN 'reconciles' ELSE 'hierarchy_unreliable' END AS status
          FROM p JOIN m USING (province_area, district) ORDER BY excess DESC""").fetchall()
        with open(out / 'rural_reconciliation.csv', 'w', newline='') as fh:
            w = csv.writer(fh)
            w.writerow(['province_area', 'district', 'published_rural', 'mauza_sum',
                        'excess', 'pct', 'status'])
            w.writerows(recon)

    # Population (31) and housing (32) describe the same villages, so they have
    # to join on the place id. This is the check that the id is actually doing
    # its job.
    jn = con.sql(f"""
      WITH a AS (SELECT DISTINCT own_id FROM {P} WHERE table_id='31' AND level='mauza'),
           b AS (SELECT DISTINCT own_id FROM {P} WHERE table_id='32' AND level='mauza')
      SELECT (SELECT count(*) FROM a), (SELECT count(*) FROM b),
             (SELECT count(*) FROM a SEMI JOIN b USING (own_id))""").fetchone()

    lv = collections.Counter(r['level'] for r in reg_rows)
    dup = collections.Counter(
        (r['table_id'], r['province_area'], r['district'], r['sub_district'],
         r['patwar_circle'], r['locality'], r['charge'], r['name'], r['hadbast'])
        for r in obs)
    ndup = sum(v - 1 for v in dup.values() if v > 1)
    nrelab = sum(1 for r in obs if r.get('relabelled'))
    byprov = collections.defaultdict(lambda: [0, 0])
    for prov, dist, pub, ms, exc, pct, st in recon:
        byprov[prov][0] += 1
        byprov[prov][1] += (st == 'reconciles')
    rep = dict(observations=len(long), places=len(reg_rows), levels=dict(lv),
               relabelled_groupings=nrelab,
               table31_32_join=dict(in_31=jn[0], in_32=jn[1], joining=jn[2]),
               rural_reconciliation={k: dict(districts=v[0], reconciling=v[1])
                                     for k, v in sorted(byprov.items())},
               duplicate_keys=ndup, missing_cells=sum(1 for r in long if r['missing']),
               with_hadbast=sum(1 for r in reg_rows if r['hadbast']),
               patwar_circle_closure=dict(comparisons=closure[0], closing=closure[1]),
               problems=problems)
    (out / 'locality_report.json').write_text(json.dumps(rep, indent=2, sort_keys=True))
    print(f"observations      {len(long):,}")
    print(f"places            {len(reg_rows):,}   {dict(lv)}")
    print(f"with an identifier {rep['with_hadbast']:,}")
    print(f"missing cells     {rep['missing_cells']:,}")
    print(f"duplicate keys    {ndup}")
    print(f"relabelled rows   {nrelab}  (subtotal rows PBS published without a PC suffix)")
    print(f"31<->32 place join  {jn[2]:,}/{jn[0]:,} mauzas"
          + (f"  ({100*jn[2]/jn[0]:.1f}%)" if jn[0] else ""))
    print(f"patwar-circle closure  {closure[1]:,}/{closure[0]:,}"
          + (f"  ({100*closure[1]/closure[0]:.1f}%)" if closure[0] else ""))
    if recon:
        print("\nmauza sum vs published rural population, by province_area:")
        for prov, (n, ok) in sorted(byprov.items()):
            flag = '' if ok == n else '   <-- hierarchy unreliable'
            print(f"    {prov:22s} {ok:3d}/{n:3d} districts reconcile{flag}")
    for p in problems:
        print("  PROBLEM", p)


if __name__ == '__main__':
    main()
