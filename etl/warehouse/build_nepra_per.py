#!/usr/bin/env python3
"""NEPRA Performance Evaluation Reports (DISCOs, GENCOs/plants, NTDC/KE) -> tables, same method as build_nepra_soi.

Input : $NEPRA_RAW/PER/*.pdf  (35 reports, FY2010-11 -> FY2024-25; DISCO reports carry 5-year comparisons)
Output: $DD_WAREHOUSE/nepra_per_cells.parquet, $DD_WAREHOUSE/nepra_per_long.parquet  (same columns as the SOI tables, but
        `report` = PDF file name instead of report_year, and `table_no` is rarely printed)
"""
import os, re, sys, pickle
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_nepra_soi as S

def main():
    cache = S.NEPRA_CACHE; cache.mkdir(parents=True, exist_ok=True)
    files = sorted((S.RAW / 'PER').glob('*.pdf'))
    only = [a for a in sys.argv[1:] if not a.startswith('--')]
    all_cells, all_pages = [], []
    for p in files:
        if p.stat().st_size == 0:
            continue
        key = p.stem
        if only and not any(o.lower() in key.lower() for o in only):
            continue
        cp = cache / ('per_' + re.sub(r'[^A-Za-z0-9]+', '_', key) + '.pkl')
        if cp.exists() and '--force' not in sys.argv:
            cells, pages = pickle.load(open(cp, 'rb'))
        else:
            cells, pages = S.extract_report(p, key)
            pickle.dump((cells, pages), open(cp, 'wb'))
        ntab = len({(c[1], c[2]) for c in cells})
        print(f'{key[:60]}: {len(pages)} pages, {ntab} tables, {len(cells)} cells', file=sys.stderr)
        all_cells += cells; all_pages += pages
    if '--assemble' in sys.argv or not only:
        # assemble from every cached PER pickle
        all_cells, all_pages = [], []
        for cp in sorted(cache.glob('per_*.pkl')):
            c, pg = pickle.load(open(cp, 'rb')); all_cells += c; all_pages += pg
        cells_df, long_df = S.tidy(all_cells)
        cells_df = cells_df.rename(columns={'report_year': 'report'})
        long_df = long_df.rename(columns={'report_year': 'report'})
        cells_df.to_parquet(S.WH / 'nepra_per_cells.parquet', index=False)
        long_df.to_parquet(S.WH / 'nepra_per_long.parquet', index=False)
        print(f'wrote PER cells={len(cells_df)} long={len(long_df)} from {len(list(cache.glob("per_*.pkl")))} reports', file=sys.stderr)

if __name__ == '__main__':
    main()
