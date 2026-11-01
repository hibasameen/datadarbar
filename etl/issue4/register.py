"""Publish the Issue 4 tables into the web warehouse.

Two ways in, as the health-facility tables have:

  register_tables(register, src)  called by build_web_warehouse.build(), so a
                                  full rebuild keeps these tables;
  update_warehouse(out, src)      build_web_warehouse.py --only issue4: copies
                                  these tables and patches catalog.json and
                                  catalog_samples.json without rebuilding the
                                  rest, stamping everything the full build
                                  stamps (kind, keys, span, licence,
                                  provenance, derived columns). generated is
                                  left alone: it sets the cache key on every
                                  Parquet URL, and these tables do not change
                                  the others.
"""
import json
from pathlib import Path

from issue4.tables import TABLES

PQ = "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12, ROW_GROUP_SIZE 122880)"


def _sql(src, t):
    f = Path(src) / 'issue4' / f'{t["name"]}.parquet'
    if not f.exists():
        raise SystemExit(f'{t["name"]}: run etl/issue4/pipeline.py first (no {f})')
    return f"SELECT * FROM '{f.as_posix()}'"


def register_tables(register, src):
    for t in TABLES:
        register(t['name'], t['desc'], t['notes'], t['cols'], t['source'], _sql(src, t), unit=t['unit'])


def update_warehouse(out, src):
    import duckdb
    from catalog_meta import KIND, TIME_COLS, KEY_COLS, licence_of, provenance_of
    out = Path(out)
    con = duckdb.connect()
    cat = json.loads((out / 'catalog.json').read_text())
    samples = json.loads((out / 'catalog_samples.json').read_text())

    for t in TABLES:
        name = t['name']
        target = out / f'{name}.parquet'
        con.sql(f"COPY ({_sql(src, t)}) TO '{target.as_posix()}' {PQ}")
        p = target.as_posix()
        schema = con.sql(f"DESCRIBE SELECT * FROM '{p}'").fetchall()
        cols = [c[0] for c in schema]
        prov = provenance_of(name, cols)
        entry = {'name': name, 'file': f'{name}.parquet', 'bytes': target.stat().st_size,
                 'rows': con.sql(f"SELECT count(*) FROM '{p}'").fetchone()[0],
                 'description': t['desc'], 'notes': t['notes'], 'unit': t['unit'],
                 'source': t['source'],
                 'columns': [dict({'name': c[0], 'type': c[1], 'description': t['cols'].get(c[0], '')},
                                  **({'derived': True} if c[0] in prov['derived_columns'] else {}))
                             for c in schema],
                 'kind': KIND[name], 'licence': licence_of(name), 'provenance': prov}
        tc = next((c for c in TIME_COLS if c in cols), None)
        if tc:
            lo, hi = con.sql(f'SELECT min("{tc}")::VARCHAR, max("{tc}")::VARCHAR FROM \'{p}\' '
                             f"WHERE \"{tc}\"::VARCHAR NOT IN ('overall', '-1') "
                             f"AND \"{tc}\"::VARCHAR NOT LIKE 'Δ%'").fetchone()
            entry['span'] = {'column': tc, 'from': lo, 'to': hi}
        entry['keys'] = [k for k in KEY_COLS if k in cols]
        cat['tables'] = sorted([x for x in cat['tables'] if x['name'] != name] + [entry],
                               key=lambda x: x['name'])
        samples[name] = [[None if v is None else str(v)[:80] for v in r]
                         for r in con.sql(f"SELECT * FROM '{p}' LIMIT 5").fetchall()]
        print(f'  {name:<30} {entry["rows"]:>8,} rows  {entry["bytes"]/1e6:5.2f} MB')

    (out / 'catalog_samples.json').write_text(json.dumps(samples, ensure_ascii=False, separators=(',', ':')))
    (out / 'catalog.json').write_text(json.dumps(cat, indent=1, ensure_ascii=False))
    con.close()
