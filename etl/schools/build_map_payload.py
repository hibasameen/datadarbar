#!/usr/bin/env python3
"""
1. Fold the tehsil distance stats and school counts into one wide table,
   etl/schools/school_access_tehsil.csv (one row per ADM3 polygon with data).
2. Inject it as the `schools` table of window.DD_POV in app/data/poverty_data.js,
   keyed by dd_id, so the map's schoolAccess group (a `pov` group) can draw it
   without new JavaScript plumbing. Idempotent: re-running replaces the table.

poverty_data.js has no generator in this repo (its MPI/RWI/lights tables come
from an older pipeline), so this is the one place the file is rewritten; only
the DD_POV line changes, the two geometry lines are copied through.

Usage: python3 build_map_payload.py --stats tehsil_distance_stats.csv --counts tehsil_school_counts.csv
         --pov app/data/poverty_data.js --out etl/schools/school_access_tehsil.csv
"""
import argparse, json
from pathlib import Path
import numpy as np, pandas as pd

RELEASE = '2026-09'
TIER = {'Balochistan': 'A', 'Sindh': 'A', 'KP (settled)': 'A', 'Punjab': 'B-', 'Gilgit-Baltistan': 'B',
        'AJK (Mirpur+Kotli)': 'C', 'Islamabad (ICT)': 'C'}

def main():
    ap = argparse.ArgumentParser()
    for k in ('stats', 'counts', 'pov', 'out'): ap.add_argument(f'--{k}', required=True, type=Path)
    a = ap.parse_args()
    t = pd.read_csv(a.stats); c = pd.read_csv(a.counts)
    # keep a tehsil only when the analysed cells hold at least half of its people
    t = t[t['coverage_pct'] >= 50].copy()
    t['level'] = t['cls'].str.replace('_plus', '', regex=False)
    piv = t.pivot_table(index='dd_id', columns=['sex', 'level'], values=['median_km', 'share_over_5km'])
    piv.columns = [f'{sex}_{lvl}_{ "km" if v == "median_km" else "over5km_pct"}' for v, sex, lvl in piv.columns]
    piv = piv.reset_index()
    for col in [c_ for c_ in piv.columns if c_.endswith('over5km_pct')]: piv[col] = (100 * piv[col]).round(1)
    for col in [c_ for c_ in piv.columns if c_.endswith('_km')]: piv[col] = piv[col].round(2)
    meta = t.drop_duplicates('dd_id')[['dd_id', 'dk', 'region', 'pop', 'coverage_pct']].copy()
    meta['pop'] = meta['pop'].round(0).astype(int)
    w = meta.merge(piv, on='dd_id', how='left').merge(c, on='dd_id', how='left')
    for col in [c_ for c_ in w.columns if c_.endswith('_schools')]: w[col] = w[col].fillna(0).astype(int)
    for lvl in ('primary', 'middle', 'high'):
        w[f'gap_{lvl}_km'] = (w[f'girls_{lvl}_km'] - w[f'boys_{lvl}_km']).round(2)
    tot = w['girls_middle_schools'] + w['boys_middle_schools']
    w['girls_share_middle_pct'] = np.where(tot > 0, (100 * w['girls_middle_schools'] / tot.replace(0, np.nan)).round(1), np.nan)
    w['coord_tier'] = w['region'].map(TIER)
    w['release'] = RELEASE
    # tehsil names come from the poverty payload's tehsil table (the ADM3 frame's own names)
    s = a.pov.read_text(encoding='utf-8')
    head = 'window.DD_POV='
    i = s.index(head) + len(head)
    d, end = json.JSONDecoder().raw_decode(s[i:])
    names = d['tehsils']
    w['tehsil'] = w['dd_id'].map(lambda k: (names.get(k) or {}).get('name', ''))
    cols = ['dd_id', 'tehsil', 'dk', 'region', 'coord_tier', 'pop', 'coverage_pct',
            'girls_primary_km', 'boys_primary_km', 'gap_primary_km',
            'girls_middle_km', 'boys_middle_km', 'gap_middle_km',
            'girls_high_km', 'boys_high_km', 'gap_high_km',
            'girls_middle_over5km_pct', 'boys_middle_over5km_pct',
            'girls_primary_schools', 'boys_primary_schools', 'girls_middle_schools', 'boys_middle_schools',
            'girls_high_schools', 'boys_high_schools', 'girls_share_middle_pct', 'release']
    w = w[cols].sort_values(['region', 'dk', 'dd_id']).reset_index(drop=True)
    w = w.rename(columns={'dk': 'district_key'})
    w.to_csv(a.out, index=False)

    # inject into poverty_data.js
    rows = {}
    for r in w.itertuples(index=False):
        rec = {'name': (names.get(r.dd_id) or {}).get('name', ''), 'dk': r.district_key, 'prov': (names.get(r.dd_id) or {}).get('prov', ''),
               'region': r.region, 'tier': r.coord_tier, 'pop': int(r.pop)}
        for col in cols[7:-1]:
            v = getattr(r, col)
            rec[col] = None if (isinstance(v, float) and np.isnan(v)) else (float(v) if isinstance(v, (float, np.floating)) else int(v))
        rows[r.dd_id] = rec
    d['schools'] = rows
    d['meta']['schools_release'] = RELEASE
    d['meta']['n_tehsils_schools'] = len(rows)
    new = s[:i] + json.dumps(d, separators=(',', ':'), ensure_ascii=False) + s[i + end:]
    a.pov.write_text(new, encoding='utf-8')
    print(f'{len(w)} tehsils; schools table injected into {a.pov} ({len(new)/1e6:.2f} MB)')

if __name__ == '__main__':
    main()
