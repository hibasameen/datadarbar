#!/usr/bin/env python3
"""
Satellite layers on PBS's Census-2023 tehsils: Meta RWI (population-weighted), WorldPop 2020 (zonal sum,
density) and VIIRS June night-lights 2020–2026 (filtered sum of lights per km², growth, low-confidence flag).

Same method as the August 2026 build on the geoBoundaries layer (see etl/README and the poverty page's
methodology): noise floor 1.0 nW zeroed; persistent flares (>500 nW in every year, dilated 1 px) masked;
nl_lowc = 1 where population density < 1 per km² (June snow/sand albedo dominates the radiance there).

Inputs  --tehsils  full-precision PBS tehsil GeoJSON (pbs_tehsils_2023.geojson)
        --pop      WorldPop pak_ppp_2020_1km_Aggregated_UNadj.tif
        --rwi      ind_pak_relative_wealth_index.csv
        --viirs    directory of viirs_YYYY06_pak.npz (window lon 60–78E, lat 23–38N, 1/240°)
Output  tehsil_satellite_pbs2023.csv  one row per tehsil (dd_id = PBS code)
"""
import argparse, json, re
from pathlib import Path
import numpy as np, pandas as pd, rasterio
from rasterio.features import rasterize
from rasterio.transform import from_origin
from shapely.geometry import shape
from scipy.ndimage import binary_dilation, uniform_filter

ap = argparse.ArgumentParser()
for k in ('tehsils', 'pop', 'rwi', 'viirs', 'out'): ap.add_argument('--' + k, required=True, type=Path)
a = ap.parse_args()

fc = json.load(open(a.tehsils))
feats = [f for f in fc['features'] if f['properties']['tehsil_code'] != '000']
geoms = [shape(f['geometry']) for f in feats]
props = [f['properties'] for f in feats]
n = len(feats)
area = np.array([p['area_km2'] for p in props], dtype=float)

# ---- WorldPop ---------------------------------------------------------------------------
with rasterio.open(a.pop) as src:
    pop = src.read(1).astype('float64'); tr = src.transform; H, W = pop.shape
pop[(pop < 0) | ~np.isfinite(pop)] = 0
zones_pop = rasterize(((g, i + 1) for i, g in enumerate(geoms)), out_shape=(H, W), transform=tr, fill=0, dtype='int32', all_touched=False)
pop_sum = np.bincount(zones_pop.ravel(), weights=pop.ravel(), minlength=n + 1)[1:]
print(f'WorldPop: grid {H}x{W}, total {pop.sum()/1e6:.1f}M, in tehsils {pop_sum.sum()/1e6:.1f}M')
popdens = pop_sum / area

# ---- RWI --------------------------------------------------------------------------------
rwi = pd.read_csv(a.rwi)
rwi = rwi[(rwi.longitude.between(60, 78)) & (rwi.latitude.between(23, 38))]
col = ((rwi.longitude.values - tr.c) / tr.a).astype(int); row = ((rwi.latitude.values - tr.f) / tr.e).astype(int)
ok = (row >= 0) & (row < H) & (col >= 0) & (col < W)
rwi = rwi[ok]; row, col = row[ok], col[ok]
z = zones_pop[row, col]
w3 = uniform_filter(pop, size=3, mode='constant') * 9.0  # 3x3 population window (~3 km) as weight
wt = w3[row, col]
keep = z > 0
num = np.bincount(z[keep], weights=(rwi.rwi.values * wt)[keep], minlength=n + 1)[1:]
den = np.bincount(z[keep], weights=wt[keep], minlength=n + 1)[1:]
cnt = np.bincount(z[keep], minlength=n + 1)[1:]
# fall back to the unweighted mean where the 3 km window holds no population
num_u = np.bincount(z[keep], weights=rwi.rwi.values[keep], minlength=n + 1)[1:]
rwi_mean = np.where(den > 0, num / np.where(den > 0, den, 1), np.where(cnt > 0, num_u / np.where(cnt > 0, cnt, 1), np.nan))
rwi_pct = pd.Series(rwi_mean).rank(pct=True).values * 100
print(f'RWI: {keep.sum()} cells in tehsils; {int((cnt > 0).sum())} tehsils with cells')

# ---- VIIRS ------------------------------------------------------------------------------
files = sorted(a.viirs.glob('viirs_*06_pak.npz'))
years = [int(re.search(r'viirs_(\d{4})', f.name).group(1)) for f in files]
rad = {y: np.load(f)['rade'].astype('float32') for y, f in zip(years, files)}
vh, vw = next(iter(rad.values())).shape
vtr = from_origin(60.0, 38.0, 1 / 240, 1 / 240)
zones_v = rasterize(((g, i + 1) for i, g in enumerate(geoms)), out_shape=(vh, vw), transform=vtr, fill=0, dtype='int32')
flare = np.ones((vh, vw), dtype=bool)
for y in years: flare &= rad[y] > 500
flare = binary_dilation(flare, iterations=1)
print(f'VIIRS: {len(years)} years, grid {vh}x{vw}, persistent-flare pixels {int(flare.sum())}')
nl = {}
for y in years:
    r = rad[y].copy(); r[r < 1.0] = 0; r[flare] = 0
    nl[y] = np.bincount(zones_v.ravel(), weights=r.ravel(), minlength=n + 1)[1:]
    print(f'  {y}: national filtered sum of lights {nl[y].sum()/1e6:.2f}M')
y0, y1 = min(years), max(years)

rows = []
for i, p in enumerate(props):
    rec = dict(dd_id=p['tehsil_code'], name=p['tehsil'], district=p['district'], district_code=p['district_code'], province=p['province'],
               area_km2=round(area[i], 1), pop=int(round(pop_sum[i])), popdens=round(popdens[i], 1),
               rwi=round(float(rwi_mean[i]), 4) if np.isfinite(rwi_mean[i]) else None, rwi_pct=round(float(rwi_pct[i]), 1) if np.isfinite(rwi_mean[i]) else None,
               rwi_cells=int(cnt[i]), nl_lowc=int(popdens[i] < 1))
    for y in years: rec[f'nl_{y}'] = round(nl[y][i] / area[i], 3)
    rec['nl_growth'] = round((nl[y1][i] - nl[y0][i]) / nl[y0][i] * 100, 1) if nl[y0][i] > 0 else None
    rows.append(rec)
df = pd.DataFrame(rows); df.to_csv(a.out, index=False)
print(f'{a.out}: {len(df)} tehsils; pop total {df["pop"].sum()/1e6:.1f}M; rwi missing {df.rwi.isna().sum()}; nl_lowc {df.nl_lowc.sum()}')
