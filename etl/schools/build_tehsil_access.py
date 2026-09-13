#!/usr/bin/env python3
"""
Tehsil-level school access from the released layer (schools_pk), by the same
method as the piece: population-weighted straight-line distance (Lambert
conformal conic, 1 km WorldPop grid) from every populated cell to the nearest
government school of each sex in the cell's own region network, aggregated
to Data Darbar's 553 ADM3 polygons. District aggregates on the same cells are
written too, so the tehsil run can be checked against the piece's district
numbers (school_distance_stats).

Usage: python3 build_tehsil_access.py --schools schools_pk.csv.gz --pop worldpop.tif
         --districts districts.geojson --tehsils tehsils_geo.js --out <dir>
"""
import argparse, json, re
from pathlib import Path
import numpy as np, pandas as pd, geopandas as gpd, rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.features import rasterize
from scipy.spatial import cKDTree
from shapely.geometry import Point, shape

CRS = "+proj=lcc +lat_1=25 +lat_2=35 +lat_0=30 +lon_0=70 +datum=WGS84 +units=m +no_defs"
RES = 1000.0
THRESH = (2, 5, 10)
CLASSES = ['primary_plus', 'middle_plus', 'high_plus']
REGION_PROV = {'Balochistan': 'balochistan', 'Sindh': 'sindh', 'KP (settled)': 'khyber pakhtunkhwa', 'Punjab': 'punjab',
               'Gilgit-Baltistan': 'gilgit-baltistan', 'AJK (Mirpur+Kotli)': 'azad jammu', 'Islamabad (ICT)': 'islamabad'}
REGION_DISTRICTS = {'AJK (Mirpur+Kotli)': {'mirpur', 'kotli'}}

def main():
    ap = argparse.ArgumentParser()
    for k in ('schools', 'pop', 'districts', 'tehsils', 'out'): ap.add_argument(f'--{k}', required=True, type=Path)
    a = ap.parse_args(); a.out.mkdir(parents=True, exist_ok=True)

    sp = pd.read_csv(a.schools, low_memory=False)
    pts = sp[sp['in_analysis']].copy()
    pts['gender'] = pts['analysis_sex']
    gpts = gpd.GeoDataFrame(pts, geometry=[Point(xy) for xy in zip(pts['lng'], pts['lat'])], crs='EPSG:4326').to_crs(CRS)
    gpts['x'] = gpts.geometry.x; gpts['y'] = gpts.geometry.y

    bnd = gpd.read_file(a.districts)
    bnd['district_key'] = bnd['districts'].str.lower().str.strip().map(lambda s: re.sub(r'[^a-z]', '', s))
    bnd = bnd.to_crs(CRS)
    prov_of = dict(zip(bnd.index, bnd['province_territory'].str.lower()))

    js = open(a.tehsils).read()
    tg = json.loads(js[js.index('{'):js.rindex('}') + 1])
    teh = gpd.GeoDataFrame([f['properties'] for f in tg['features']],
                           geometry=[shape(f['geometry']) for f in tg['features']], crs='EPSG:4326').to_crs(CRS)

    west, south, east, north = 60.5, 23.5, 77.9, 37.3
    with rasterio.open(a.pop) as src:
        tr, w, h = calculate_default_transform(src.crs, CRS, src.width, src.height, west, south, east, north, resolution=(RES, RES))
        pop = np.zeros((h, w), dtype='float32')
        reproject(rasterio.band(src, 1), pop, src_transform=src.transform, src_crs=src.crs,
                  dst_transform=tr, dst_crs=CRS, dst_nodata=0.0, resampling=Resampling.sum)
    pop = np.where(np.isfinite(pop) & (pop > 0), pop, 0.0)
    dist_ras = rasterize(((g, i + 1) for i, g in enumerate(bnd.geometry)), out_shape=(h, w), transform=tr, fill=0, dtype='int32')
    teh_ras = rasterize(((g, i + 1) for i, g in enumerate(teh.geometry)), out_shape=(h, w), transform=tr, fill=0, dtype='int32')
    print(f'grid {pop.shape}, population {pop.sum()/1e6:.1f}M, in a tehsil polygon {pop[teh_ras>0].sum()/1e6:.1f}M')

    rows_t, rows_d = [], []
    def stat(v, ww):
        if ww.sum() == 0: return None
        o = np.argsort(v)
        rec = dict(pop=float(ww.sum()), mean_km=float(np.average(v, weights=ww)),
                   median_km=float(np.interp(0.5, np.cumsum(ww[o]) / ww.sum(), np.sort(v))))
        for t in THRESH: rec[f'share_over_{t}km'] = float(ww[v > t].sum() / ww.sum())
        return rec

    for region, grp in gpts.groupby('region'):
        prov = REGION_PROV[region]
        idx = [i + 1 for i in bnd.index if prov in prov_of[i]]
        only = REGION_DISTRICTS.get(region)
        if only: idx = [i for i in idx if bnd.loc[i - 1, 'district_key'] in only]
        sub = np.isin(dist_ras, idx) & (pop > 0)
        rr, cc = np.where(sub)
        if len(rr) == 0: continue
        X = tr.c + (cc + 0.5) * tr.a; Y = tr.f + (rr + 0.5) * tr.e
        W = pop[rr, cc]; DK = dist_ras[rr, cc]; TK = teh_ras[rr, cc]
        for cls in CLASSES:
            for sex, tag in [('G', 'girls'), ('B', 'boys')]:
                sel = grp[(grp['gender'] == sex) & (grp[cls])]
                if len(sel) < 20: continue
                d, _ = cKDTree(np.c_[sel['x'], sel['y']]).query(np.c_[X, Y]); d = d / 1000.0
                for i in np.unique(DK):
                    m = DK == i; r = stat(d[m], W[m])
                    if r: rows_d.append(dict(region=region, district_key=bnd.loc[i - 1, 'district_key'], sex=tag, cls=cls, **r))
                for j in np.unique(TK):
                    if j == 0: continue
                    m = TK == j; r = stat(d[m], W[m])
                    if r: rows_t.append(dict(region=region, dd_id=teh.loc[j - 1, 'dd_id'], dk=teh.loc[j - 1, 'dk'], sex=tag, cls=cls, **r))
        print(region, 'cells', len(rr))

    pd.DataFrame(rows_d).to_csv(a.out / 'district_check.csv', index=False)
    T = pd.DataFrame(rows_t)
    # a tehsil straddling a region boundary gets rows from both regions; keep the region holding more of its population
    T = T.sort_values('pop', ascending=False).drop_duplicates(['dd_id', 'sex', 'cls'])
    # population of the whole polygon, so a sliver of a tehsil that lies across
    # a region boundary (Bhimber cells inside the Mirpur district polygon) can be
    # recognised and dropped downstream
    tot = pd.Series({teh.loc[j - 1, 'dd_id']: float(pop[teh_ras == j].sum()) for j in np.unique(teh_ras) if j})
    T['pop_total'] = T['dd_id'].map(tot)
    T['coverage_pct'] = (100 * T['pop'] / T['pop_total']).round(1)
    T.to_csv(a.out / 'tehsil_distance_stats.csv', index=False)

    # school counts per tehsil polygon (point in polygon), in_analysis rows
    ij = gpd.sjoin(gpts[['gender', 'primary_plus', 'middle_plus', 'high_plus', 'geometry']], teh[['dd_id', 'geometry']], predicate='within', how='inner')
    cnt = {}
    for cls, lab in [('primary_plus', 'primary'), ('middle_plus', 'middle'), ('high_plus', 'high')]:
        c = ij[ij[cls]].groupby(['dd_id', 'gender']).size().unstack(fill_value=0)
        for sex, tag in [('G', 'girls'), ('B', 'boys')]:
            cnt[f'{tag}_{lab}_schools'] = c[sex] if sex in c else pd.Series(dtype=int)
    C = pd.DataFrame(cnt).fillna(0).astype(int)
    C.index.name = 'dd_id'
    C.reset_index().to_csv(a.out / 'tehsil_school_counts.csv', index=False)
    print('tehsils with distance rows:', T['dd_id'].nunique(), '| with schools:', len(C))

if __name__ == '__main__':
    main()
