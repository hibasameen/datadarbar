#!/usr/bin/env python3
"""
Travel time to care on PBS's Census-2023 tehsils. The pk-health-access tehsil build (Adaad,
build_tehsils.py) with the zones taken from app/data/tehsils_geo.js (PBS polygons, dd_id = PBS code)
and names from etl/geography/pbs_tehsil_index.csv. Same clipped MAP rasters and WorldPop grid.

Usage: python3 build_tehsils_pbs.py --tehsils app/data/tehsils_geo.js --index etl/geography/pbs_tehsil_index.csv
         --motorized pak_motorized_tt_healthcare_2019.tif --walking pak_walking_tt_healthcare_2020.tif
         --pop pak_ppp_2020_1km_Aggregated_UNadj.tif --out travel_time_tehsils_2026-09.csv
"""
import argparse, json
from pathlib import Path
import numpy as np, pandas as pd, geopandas as gpd, rasterio
from rasterio.warp import reproject, Resampling
from rasterio.features import rasterize
THRESHOLDS = (30, 60, 120)
ap = argparse.ArgumentParser()
for k in ('tehsils', 'index', 'motorized', 'walking', 'pop', 'out'): ap.add_argument('--' + k, required=True, type=Path)
a = ap.parse_args()
raw = a.tehsils.read_text(); gj = json.loads(raw[raw.index('{'):raw.rindex('}') + 1])
gdf = gpd.GeoDataFrame.from_features(gj['features'], crs='EPSG:4326')
names = pd.read_csv(a.index, dtype={'dd_id': str}).set_index('dd_id')['name']
with rasterio.open(a.motorized) as m, rasterio.open(a.walking) as w, rasterio.open(a.pop) as p:
    mot, wal = m.read(1), w.read(1); tr, shape, crs = m.transform, m.shape, m.crs
    pop = np.zeros(shape)
    reproject(rasterio.band(p, 1), pop, dst_transform=tr, dst_crs=crs, resampling=Resampling.nearest, dst_nodata=0.0)
    pop[pop < 0] = 0.0
zones = rasterize([(g, i + 1) for i, g in enumerate(gdf.geometry)], out_shape=shape, transform=tr, fill=0, dtype='int32')
valid = (mot >= 0) & (wal >= 0) & (zones > 0)
z = zones[valid]; mo = mot[valid].astype(float); wa = wal[valid].astype(float); pw = pop[valid]
print(f'population captured on valid pixels: {pw.sum()/1e6:.1f}M; national pop-weighted motorised {(mo*pw).sum()/pw.sum():.2f} min')
rows = []
for i, r in gdf.iterrows():
    s = z == i + 1
    rec = dict(dd_id=r['dd_id'], tehsil=names.get(r['dd_id'], r.get('name', '')), district_key=r['dk'], province=r['prov'], merged_district=int(r.get('merged', 0)))
    if s.any():
        moi, wai, pwi = mo[s], wa[s], pw[s]; tot = pwi.sum()
        rec.update(n_px=int(s.sum()), pop_2020=round(tot), mot_mean=round(moi.mean(), 1), mot_popw_mean=round((moi * pwi).sum() / tot, 1) if tot > 0 else np.nan,
                   wal_mean=round(wai.mean(), 1), wal_popw_mean=round((wai * pwi).sum() / tot, 1) if tot > 0 else np.nan)
        for pref, arr in (('mot', moi), ('wal', wai)):
            for t in THRESHOLDS: rec[f'{pref}_pct_pop_gt{t}'] = round((pwi[arr > t].sum() / tot * 100) if tot > 0 else np.nan, 1)
    else:
        rec.update(n_px=0)
    rec['release'] = '2026-09'
    rows.append(rec)
df = pd.DataFrame(rows)
cols = ['dd_id', 'tehsil', 'district_key', 'province', 'merged_district', 'n_px', 'pop_2020', 'mot_mean', 'mot_popw_mean', 'wal_mean', 'wal_popw_mean',
        'mot_pct_pop_gt30', 'mot_pct_pop_gt60', 'mot_pct_pop_gt120', 'wal_pct_pop_gt30', 'wal_pct_pop_gt60', 'wal_pct_pop_gt120', 'release']
df[cols].to_csv(a.out, index=False)
print(f'{a.out}: {len(df)} tehsils, {(df.n_px > 0).sum()} with pixels')
