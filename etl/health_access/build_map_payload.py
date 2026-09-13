#!/usr/bin/env python3
"""
Inject the travel-time-to-care tables as `health_tehsils` and `health_districts`
in window.DD_POV (app/data/poverty_data.js), keyed by dd_id and district key,
so the map's healthAccess / healthAccessDistrict groups (`pov` groups) can draw
them. Idempotent: re-running replaces both tables; only the DD_POV line of the
file changes (see etl/schools/build_map_payload.py for the same pattern).

Usage: python3 etl/health_access/build_map_payload.py
"""
import json
from pathlib import Path
import numpy as np, pandas as pd

HERE = Path(__file__).resolve().parent
POV = HERE.parent.parent / 'app' / 'data' / 'poverty_data.js'
RELEASE = '2026-09'

def clean(v):
    if v is None or (isinstance(v, float) and np.isnan(v)) or (hasattr(v, '__class__') and v.__class__.__name__ == 'NAType'):
        return None
    return float(v) if isinstance(v, (float, np.floating)) else int(v)

def main():
    t = pd.read_csv(HERE / f'travel_time_tehsils_{RELEASE}.csv')
    d = pd.read_csv(HERE / f'travel_time_districts_{RELEASE}.csv')
    s = POV.read_text(encoding='utf-8')
    head = 'window.DD_POV='
    i = s.index(head) + len(head)
    pov, end = json.JSONDecoder().raw_decode(s[i:])
    names = pov['tehsils']
    TCOLS = ['n_px', 'pop_2020', 'mot_mean', 'mot_popw_mean', 'wal_mean', 'wal_popw_mean',
             'mot_pct_pop_gt30', 'mot_pct_pop_gt60', 'mot_pct_pop_gt120', 'wal_pct_pop_gt30', 'wal_pct_pop_gt60', 'wal_pct_pop_gt120']
    th = {}
    for r in t.to_dict('records'):
        rec = {'name': r['tehsil'], 'dk': r['district_key'], 'prov': (names.get(r['dd_id']) or {}).get('prov', r['province'])}
        rec.update({c: clean(r[c]) for c in TCOLS})
        th[r['dd_id']] = rec
    DCOLS = ['n_px', 'pop_2020', 'mot_popw_median', 'mot_popw_mean', 'mot_mean', 'wal_popw_median', 'wal_popw_mean', 'wal_mean',
             'mot_pct_pop_gt30', 'mot_pct_pop_gt60', 'mot_pct_pop_gt120', 'wal_pct_pop_gt30', 'wal_pct_pop_gt60', 'wal_pct_pop_gt120']
    dh = {}
    for r in d.to_dict('records'):
        rec = {'name': r['district'], 'prov': r['province'], 'merged_district': int(r['merged_district'])}
        rec.update({c: clean(r[c]) for c in DCOLS})
        dh[r['district_key']] = rec
    pov['health_tehsils'] = th
    pov['health_districts'] = dh
    pov['meta']['health_release'] = RELEASE
    new = s[:i] + json.dumps(pov, separators=(',', ':'), ensure_ascii=False) + s[i + end:]
    POV.write_text(new, encoding='utf-8')
    print(f'{len(th)} tehsils, {len(dh)} districts injected into {POV.name} ({len(new)/1e6:.2f} MB)')

if __name__ == '__main__':
    main()
