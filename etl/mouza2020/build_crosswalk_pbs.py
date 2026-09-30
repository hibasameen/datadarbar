#!/usr/bin/env python3
"""
Mouza Census 2020 tehsils -> PBS Census-2023 tehsil polygons (mouza2020_tehsil_crosswalk_pbs.csv).

The Mouza Census dashboard uses PBS's own tehsil codes, so most rows join by code once the leading
zero is dropped; the code is trusted only when the district agrees (PBS reuses a few codes across
frames: 613 is Mohmand Agency in one and Patkain sub-tehsil in the other). Then name + district,
then the old geoBoundaries crosswalk carried through the census-unit register. Output columns match
mouza2020_tehsil_crosswalk.csv so build_payload.py can read either file (MOUZA_XW=...).
"""
import re, sys
from pathlib import Path
import pandas as pd
HERE = Path(__file__).resolve().parent
m = pd.read_csv(HERE / 'pk-mouza-2020-tehsil.csv', dtype=str)
p = pd.read_csv(HERE / '../geography/pbs_tehsil_index.csv', dtype=str)
old = pd.read_csv(HERE / 'mouza2020_tehsil_crosswalk.csv', dtype=str)
units = pd.read_csv(HERE / '../geography/census2023_units_to_pbs_geojson.csv', dtype=str) if (HERE / '../geography/census2023_units_to_pbs_geojson.csv').exists() else None
def norm(s):
    s = str(s).upper(); s = re.sub(r'\b(DISTRICT|TEHSIL|SUB-DIVISION|SUB DIVISION|SUB-TEHSIL|SUB-|TALUKA|TOWN|PROTECTED AREA|AGENCY)\b', ' ', s)
    s = re.sub(r'[-./()]', ' ', s); return re.sub(r'\s+', ' ', s).strip()
DALIAS = {'CHOLISTAN': None, 'D I KHAN': 'DERA ISMAIL KHAN', 'MOHMAND': 'MOHMAND', 'CHITRAL LOWER': 'LOWER CHITRAL', 'CHITRAL UPPER': 'UPPER CHITRAL', 'LOWER KOHISTAN': 'LOWER KOHISTAN', 'KOHISTAN': 'UPPER KOHISTAN'}
p['tn'] = p.name.map(norm); p['dn'] = p.district.map(norm)
by_code = p.set_index('dd_id'); by_name = {(r.dn, r.tn): r.dd_id for r in p.itertuples()}
old_map = dict(zip(old.tehsil_code, old.dd_id))
via = {}
if units is not None:
    for r in units.itertuples():
        if isinstance(r.dd_id, str) and r.dd_id: via.setdefault(r.dd_id, r.pbs_tehsil_code)
# Reviewed by hand (mouza tehsil code -> PBS 2023 tehsil name within the district; 'parent' = folded into the unit it was carved from)
MANUAL = {'637': ('BAKKA KHEL TEHSIL', 'manual'), '641': ('PESHAWAR TEHSIL', 'manual'), '392': ('CHAMAN SADDAR SUB-DIVISION', 'manual'),
          '644': ('DOBANDI SUB-TEHSIL', 'manual'), '624': ('DERA BUGTI SUB-DIVISION', 'parent'), '653': ('KAKAR KHURASAN SUB-DIVISION', 'manual'),
          '632': ('GULMAT GOJAL SUB-DIVISION', 'manual'), '636': ('DARLIAH JATTAN TEHSIL', 'manual')}
name_any = {r.name: r.dd_id for r in p.itertuples()}
rows = []
for r in m.itertuples():
    code3 = r.tehsil_code.lstrip('0').zfill(3); dn = norm(r.district); dn = DALIAS.get(dn, dn); tn = norm(r.tehsil)
    dd, match, cand = '', '', ''
    if code3 in by_code.index:
        c = by_code.loc[code3]
        if dn is None or c.dn == dn or tn == c.tn: dd, match = code3, 'code' if c.tn == tn else 'code_variant'
    if not dd and (dn, tn) in by_name: dd, match = by_name[(dn, tn)], 'exact_name'
    if not dd and old_map.get(r.tehsil_code) in via: dd, match = via[old_map[r.tehsil_code]], 'via_2017_crosswalk'
    if not dd and r.tehsil_code in MANUAL: dd, match = name_any[MANUAL[r.tehsil_code][0]], MANUAL[r.tehsil_code][1]
    if not dd:
        cands = p[p.dn == dn].name.tolist() if dn else []; cand = '; '.join(cands[:6])
    rows.append(dict(tehsil_code=r.tehsil_code, tehsil=r.tehsil, district_code=r.district_code, district=r.district, province=r.province,
                     mouzas=r.TotalMauzaCount, dd_id=dd, dd_name=by_code.loc[dd, 'name'] if dd else '', dd_district=by_code.loc[dd, 'district'] if dd else '', match=match, dd_candidates=cand))
x = pd.DataFrame(rows); x.to_csv(HERE / 'mouza2020_tehsil_crosswalk_pbs.csv', index=False)
print(x.match.replace('', 'UNMATCHED').value_counts().to_string()); print('mouzas unmatched:', x[x.dd_id == ''].mouzas.astype(int).sum(), 'of', x.mouzas.astype(int).sum())
print(x[x.dd_id == ''][['tehsil_code', 'tehsil', 'district', 'mouzas', 'dd_candidates']].to_string(index=False))
