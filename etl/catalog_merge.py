#!/usr/bin/env python3
"""Add or refresh a few tables in catalog.json without rebuilding the warehouse.

    python3 etl/catalog_merge.py agri_census

The full build (build_web_warehouse.py) regenerates every table from the desktop
warehouse and needs its whole dependency set; a new source whose Parquet is
already in app/data/warehouse only needs its catalogue entry. Each family here is
a function in build_web_warehouse.py that takes a `register` callback.
"""
import json, sys
from pathlib import Path

import duckdb

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import build_web_warehouse as W

FAMILIES = {"agri_census": W.register_agri_census}


def main(family):
    OUT = W.OUT
    con = duckdb.connect()
    tables = []

    def register(name, desc, notes, columns, source, sql=None, unit=None):
        path = OUT / f"{name}.parquet"
        if not path.exists():
            raise SystemExit(f"{path} is not built")
        rows = con.sql(f"SELECT count(*) FROM '{path.as_posix()}'").fetchone()[0]
        schema = con.sql(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()
        tables.append({
            "name": name, "file": f"{name}.parquet", "bytes": path.stat().st_size,
            "rows": rows, "description": desc, "notes": notes, "unit": unit, "source": source,
            "columns": [{"name": c[0], "type": c[1], "description": columns.get(c[0], "")}
                        for c in schema]})
        print(f"  {name:<28} {rows:>9,} rows  {path.stat().st_size/1e6:6.2f} MB")

    FAMILIES[family](register)
    samples = W.finalise_catalog(tables, con)

    cat = json.loads((OUT / "catalog.json").read_text())
    names = {t["name"] for t in tables}
    cat["tables"] = sorted([t for t in cat["tables"] if t["name"] not in names] + tables,
                           key=lambda t: t["name"])
    cat["generated"] = W._today()
    (OUT / "catalog.json").write_text(json.dumps(cat, indent=1, ensure_ascii=False))
    sp = OUT / "catalog_samples.json"
    allsamples = json.loads(sp.read_text())
    allsamples.update(samples)
    sp.write_text(json.dumps(allsamples, ensure_ascii=False, separators=(",", ":")))
    print(f"catalog.json: {len(cat['tables'])} tables ({', '.join(sorted(names))} merged)")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "agri_census")
