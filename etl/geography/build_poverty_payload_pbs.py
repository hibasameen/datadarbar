#!/usr/bin/env python3
"""
Write the tehsil satellite table (tehsil_satellite_pbs2023.csv) into app/data/poverty_data.js as
DD_POV.tehsils, keyed by the PBS tehsil code. DD_POV.districts (the MPI) is left as it is; the embedded
DD_POV_GEO_* copies of the geometry are dropped (nothing reads them since the poverty page became a redirect). Run before
etl/schools/build_map_payload.py and etl/health_access/build_map_payload.py, which add their tables.
"""
import json, sys
from pathlib import Path
import numpy as np, pandas as pd

REPO = Path(sys.argv[1]) if len(sys.argv) > 1 else Path('.')
POV = REPO / 'app/data/poverty_data.js'
SAT = REPO / 'etl/geography/tehsil_satellite_pbs2023.csv'
IDX = REPO / 'etl/geography/pbs_tehsil_index.csv'

sat = pd.read_csv(SAT, dtype={'dd_id': str, 'district_code': str})
idx = pd.read_csv(IDX, dtype={'dd_id': str}).set_index('dd_id')
years = sorted(int(c[3:]) for c in sat.columns if c.startswith('nl_') and c[3:].isdigit())

def num(v, dp=None):
    if v is None or (isinstance(v, float) and np.isnan(v)): return None
    return round(float(v), dp) if dp is not None else float(v)

teh = {}
for r in sat.itertuples(index=False):
    m = idx.loc[r.dd_id]
    rec = {'name': r.name.title().replace(' Sub-Division', ' Sub-Division'), 'dk': m.dk, 'prov': m.prov, 'area': num(r.area_km2, 1),
           'rwi': num(r.rwi, 4), 'rwi_pct': num(r.rwi_pct, 1), 'pop': int(r.pop), 'popdens': num(r.popdens, 1),
           'nl': {str(y): num(getattr(r, f'nl_{y}'), 3) for y in years}, 'nl_growth': num(r.nl_growth, 1), 'nl_lowc': int(r.nl_lowc)}
    if isinstance(m.dds_id, str) and m.dds_id: rec['dds_id'] = m.dds_id
    teh[r.dd_id] = rec

s = POV.read_text(encoding='utf-8')
head = 'window.DD_POV='; i = s.index(head) + len(head)
pov, end = json.JSONDecoder().raw_decode(s[i:])
pov['tehsils'] = teh
pov['meta']['n_tehsils'] = len(teh)
pov['meta']['years'] = years
pov['meta']['tehsil_frame'] = 'PBS Census 2023 tehsils (economic.data.gov.pk th.geojson), dd_id = PBS tehsil code'
rest = s[i + end:]
# DD_POV_GEO_D / DD_POV_GEO_T were the standalone poverty page's geometry; that page now redirects to
# map.html, which draws the district GeoJSON and data/tehsils_geo.js, so the copies are dropped.
rest = ';\n' if 'window.DD_POV_GEO' in rest else rest
new = s[:i] + json.dumps(pov, separators=(',', ':'), ensure_ascii=False) + rest
POV.write_text(new, encoding='utf-8')
print(f'{POV.name}: {len(teh)} tehsils, {len(new)/1e6:.2f} MB')
