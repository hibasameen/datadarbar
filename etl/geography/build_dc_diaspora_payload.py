#!/usr/bin/env python3
"""
Two new map layers from PBS's Insight Explorer portals, written into window.DD_POV (app/data/poverty_data.js)
so the map's `pov` groups can draw them:

  dc_tehsils / dc_districts   Digital Census 2023 building enumeration: counts of 23 unit types by tehsil
                              (PBS code) and district (Data Darbar district key), plus per-1,000-people rates.
  diaspora_districts          Bureau of Emigration registrations by district of origin, 2011-2024, per 1,000.

Population for the rates is the 2023 census (Stage-2 warehouse) where the unit is a census unit; the 58 GB/AJK
tehsils and the 24 GB/AJK districts are not in the 2023 release and use WorldPop 2020 instead (flagged pop_src).

Inputs (paths relative to the iCloud Data Darbar folder unless given):
  --units-tehsil   raw_data/pbs_insight_explorer/economic_2026-09-27/derived/economic_units_2023_tehsil_wide.csv
  --units-district raw_data/pbs_insight_explorer/economic_2026-09-27/derived/economic_units_2023_district_wide.csv
  --census         data_darbar_warehouse/stage2/optionB-2026-09-26/warehouse/census2023_tehsil_wide.parquet
  --diaspora       raw_data/pbs_insight_explorer/diaspora_2026-09-27/diaspora_emigrants_by_district_2011_2024.csv
"""
import argparse, json, re, sys
from pathlib import Path
import numpy as np, pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument('--repo', default='.', type=Path)
for k in ('units-tehsil', 'units-district', 'census', 'diaspora'): ap.add_argument('--' + k, required=True, type=Path)
a = ap.parse_args()
REPO = a.repo
sys.path.insert(0, str(REPO / 'etl'))
from build_dataset import apply_crosswalk  # noqa: E402

UNITS = ['retail_shop', 'wholesale_shop', 'service_shop', 'production_shop', 'factory', 'hotel', 'private_office', 'govt_office', 'bank', 'post_office',
         'police_station', 'school', 'college', 'university_campus', 'madrisah', 'hospital', 'mosque', 'hostel', 'orphan_old_home', 'jail',
         'bara_cattle_shed', 'otaq_dera', 'other']
DK_ALIAS = {'astore': 'astor', 'diamer': 'diamir', 'ghanche': 'ghanchi', 'hunza': 'hunza nagar', 'nagar': 'hunza nagar', 'kharmang': 'skardu',
            'shigar': 'skardu', 'jhelum valley': 'hattian', 'sudhnoti': 'sudhnutti'}
def norm(s): return re.sub(r'\s+', ' ', re.sub(r'[^a-z0-9]+', ' ', (s or '').lower())).strip()
def dkey(district):
    k = norm(re.sub(r'\b(DISTRICT|PROTECTED AREA)\b', '', district)); return apply_crosswalk(DK_ALIAS.get(k, k))
def clean(v):
    if v is None or (isinstance(v, float) and np.isnan(v)): return None
    return int(v) if float(v).is_integer() else round(float(v), 2)

idx = pd.read_csv(REPO / 'etl/geography/pbs_tehsil_index.csv', dtype={'dd_id': str}).set_index('dd_id')
sat = pd.read_csv(REPO / 'etl/geography/tehsil_satellite_pbs2023.csv', dtype={'dd_id': str}).set_index('dd_id')
cen = pd.read_parquet(a.census)
cen_unit = cen[cen.unit_type != 'district'].set_index('dds_id').pop_total
cen_dist = cen[cen.unit_type == 'district'].copy(); cen_dist['dk'] = cen_dist.district.map(dkey); cen_dist = cen_dist.groupby('dk').pop_total.sum()
dist_keys = set(json.load(open(REPO / 'app/data/districts.json')).keys())

# ---- tehsils --------------------------------------------------------------------------
ut = pd.read_csv(a.units_tehsil, dtype={'code': str, 'district_code': str, 'dds_id': str})
dc_t = {}
for r in ut.itertuples(index=False):
    if r.code not in idx.index: continue
    m = idx.loc[r.code]
    pop, src = (cen_unit.get(m.dds_id), 'census2023') if isinstance(m.dds_id, str) and m.dds_id in cen_unit.index else (sat.loc[r.code, 'pop'], 'worldpop2020')
    rec = {'name': m['name'].title(), 'dk': m.dk, 'prov': m.prov, 'pop': int(pop) if pop else None, 'pop_src': src}
    for u in UNITS:
        v = int(getattr(r, u)); rec[u] = v
        rec[u + '_pk'] = round(v / pop * 1000, 2) if pop else None
    dc_t[r.code] = rec

# ---- districts: sum the tehsil counts onto Data Darbar district keys (several PBS districts share a polygon key in GB/AJK) ----
ud = pd.read_csv(a.units_district, dtype={'code': str})
ud['dk'] = ud.name.map(dkey)
wp_dist = sat.assign(dk=idx.dk).groupby('dk')['pop'].sum()
dc_d = {}
for dk, g in ud.groupby('dk'):
    if dk not in dist_keys: print('no district key for', g.name.tolist()); continue
    pop, src = (cen_dist.get(dk), 'census2023') if dk in cen_dist.index else (wp_dist.get(dk), 'worldpop2020')
    rec = {'name': '; '.join(n.title().replace(' District', '') for n in g.name), 'pop': int(pop) if pop else None, 'pop_src': src, 'n_pbs_districts': len(g)}
    for u in UNITS:
        v = int(g[u].sum()); rec[u] = v; rec[u + '_pk'] = round(v / pop * 1000, 2) if pop else None
    dc_d[dk] = rec

# ---- diaspora ---------------------------------------------------------------------------
di = pd.read_csv(a.diaspora, dtype={'district_code': str})
di['dk'] = di.district.map(dkey)
years = sorted(int(y) for y in di.year.unique() if str(y).isdigit())
dia = {}
for dk, g in di.groupby('dk'):
    if dk not in dist_keys: print('diaspora: no key for', g.district.iloc[0]); continue
    pop, src = (cen_dist.get(dk), 'census2023') if dk in cen_dist.index else (wp_dist.get(dk), 'worldpop2020')
    by_year = g[g.year != 'overall'].groupby(g.year.astype(str)).emigrants_registered.sum()
    total = int(g[g.year == 'overall'].emigrants_registered.sum())
    rec = {'name': '; '.join(n.title().replace(' District', '') for n in g.district.unique()), 'pop': int(pop) if pop else None, 'pop_src': src,
           'em': {str(y): int(by_year.get(str(y), 0)) for y in years}, 'em_total': total}
    for y in (years[-1], years[-2], years[-3]): rec[f'em_{y}'] = int(by_year.get(str(y), 0))
    rec['em_per1000_' + str(years[-1])] = round(rec[f'em_{years[-1]}'] / pop * 1000, 2) if pop else None
    rec['em_per1000_total'] = round(total / pop * 1000, 1) if pop else None
    rec['em_avg_2019_24'] = int(round(sum(by_year.get(str(y), 0) for y in range(2019, 2025)) / 6))
    rec['em_per1000_avg_2019_24'] = round(rec['em_avg_2019_24'] / pop * 1000, 2) if pop else None
    b0 = sum(by_year.get(str(y), 0) for y in range(2011, 2017)); b1 = sum(by_year.get(str(y), 0) for y in range(2019, 2025))
    rec['em_change_pct'] = round((b1 - b0) / b0 * 100, 1) if b0 else None
    dia[dk] = rec

POV = REPO / 'app/data/poverty_data.js'
s = POV.read_text(encoding='utf-8'); head = 'window.DD_POV='; i = s.index(head) + len(head)
pov, end = json.JSONDecoder().raw_decode(s[i:])
pov['dc_tehsils'] = dc_t; pov['dc_districts'] = dc_d; pov['diaspora_districts'] = dia
pov['meta']['dc_release'] = '2026-09-27'; pov['meta']['diaspora_years'] = years
POV.write_text(s[:i] + json.dumps(pov, separators=(',', ':'), ensure_ascii=False) + s[i + end:], encoding='utf-8')
print(f'dc_tehsils {len(dc_t)}, dc_districts {len(dc_d)}, diaspora_districts {len(dia)} -> {POV.name} ({POV.stat().st_size/1e6:.2f} MB)')
print('worldpop fallback: tehsils', sum(1 for v in dc_t.values() if v['pop_src'] != 'census2023'), 'districts', sum(1 for v in dc_d.values() if v['pop_src'] != 'census2023'))
