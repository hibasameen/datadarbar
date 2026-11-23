#!/usr/bin/env python3
"""Rural land ownership by district from PSLM 2019-20 Section G (assets in possession).

    python3 etl/pslm_land/build_district_land.py "<raw_data>/pbs/Microdata/PSLM 2019-20/stata data"
        -> etl/pslm2019_district_land.csv

Section G asks every household whether it owns agricultural land (item 1), rents
any of it out (item 2) or rents land in (item 3), and how much. Rural households
only, weighted by the PSU weight; the household is the unit.

What to know about the acreage (see README.md):
  * Acres are sgaq03 x a unit factor (1 = acre, 2 = 0.5, 3 = kanal 0.125, 4 = 24).
    The unit codes carry no labels in the file or the questionnaire; the factors
    are read off PBS's own converted column, sgaq03_con. That column is not used
    directly: 229 rural owner rows disagree with their own factor, some by 1000x.
  * The reported quantity is capped at 200 in every unit, so the top is truncated.
  * 74 rural owners report in unit 4. They move every concentration measure
    (Sindh's Gini among owners is 0.71 with them, 0.57 without), so the Gini is
    written both ways and is NOT drawn on the map. Landlessness, the median
    holding and the renting shares do not depend on them.
Standard errors: PSU-cluster bootstrap within district, 200 draws, seed 42.
"""
import sys, pathlib, warnings
import numpy as np, pandas as pd
warnings.filterwarnings('ignore')

FAC = {1: 1.0, 2: 0.5, 3: 0.125, 4: 24.0}
MIN_N, REPS = 30, 200
PROV = {1: 'Khyber Pakhtunkhwa', 2: 'Punjab', 3: 'Sindh', 4: 'Balochistan'}


def wgini(x, w):
    x, w = np.asarray(x, float), np.asarray(w, float)
    if len(x) < 2 or (x * w).sum() <= 0:
        return np.nan
    o = np.argsort(x); x, w = x[o], w[o]
    p = np.cumsum(w) / w.sum(); L = np.cumsum(x * w) / (x * w).sum()
    return 1 - np.sum((p - np.r_[0, p[:-1]]) * (L + np.r_[0, L[:-1]]))


def wmedian(x, w):
    x, w = np.asarray(x, float), np.asarray(w, float)
    o = np.argsort(x); x, w = x[o], w[o]
    return x[min(np.searchsorted(np.cumsum(w) / w.sum(), 0.5), len(x) - 1)]


def load(src):
    g = pd.read_stata(src / 'secg.dta', convert_categoricals=False)
    labels = pd.read_stata(src / 'secg.dta', iterator=True).value_labels()['district']
    g = g[g.itc.isin([1, 2, 3])].drop_duplicates(['hhcode', 'itc'])
    g['yes'] = g.sgaq01 == 1
    b = g[g.itc == 1][['hhcode', 'province', 'region', 'psu', 'district', 'yes', 'sgaq03', 'sgaq31']].copy()
    b['acres'] = (b.sgaq03 * b.sgaq31.map(FAC)).where(b.yes, 0.0).fillna(0.0)
    b['unit4'] = b.yes & (b.sgaq31 == 4)
    for item, name in ((2, 'rent_out'), (3, 'rent_in')):
        b = b.merge(g[g.itc == item][['hhcode', 'yes']].rename(columns={'yes': name}), on='hhcode', how='left')
        b[name] = b[name].eq(True)
    w = pd.read_stata(src / 'weight_file.dta')[['psu', 'weights']]
    b = b.merge(w, on='psu', how='left')
    assert b.weights.notna().all()
    return b[b.region == 1].copy(), labels


def measures(d):
    W = d.weights; o = d[d.acres > 0]; o4 = o[~o.unit4]
    pct = lambda s: 100 * np.average(s.astype(float), weights=W)
    return dict(
        n_rural_hh=len(d), n_owners=len(o), n_psu=d.psu.nunique(),
        landless_pct=100 - pct(d.acres > 0),
        rent_in_pct=pct(d.rent_in), rent_out_pct=pct(d.rent_out),
        median_owned_ac=wmedian(o.acres, o.weights) if len(o) else np.nan,
        owners_under_2_5ac_pct=100 * np.average(o.acres < 2.5, weights=o.weights) if len(o) else np.nan,
        gini_owners=wgini(o.acres, o.weights), gini_owners_ex_unit4=wgini(o4.acres, o4.weights),
        owners_unit4=int(o.unit4.sum()))


def main(src, out):
    r, labels = load(pathlib.Path(src))
    rng = np.random.default_rng(42); rows = []
    for dc, d in r.groupby('district'):
        m = measures(d)
        psus = d.psu.unique(); by = {p: g for p, g in d.groupby('psu')}; bl = []
        for _ in range(REPS):
            x = pd.concat([by[p] for p in rng.choice(psus, len(psus))])
            bl.append(100 - 100 * np.average(x.acres > 0, weights=x.weights))
        m['landless_se'] = float(np.std(bl))
        if m['n_owners'] < MIN_N:      # too few owners to describe their holdings
            for k in ('median_owned_ac', 'owners_under_2_5ac_pct', 'gini_owners', 'gini_owners_ex_unit4'):
                m[k] = np.nan
        rows.append(dict(pslm_district_code=dc, pslm_district=labels[dc], province=PROV[d.province.iloc[0]], **m))
    df = pd.DataFrame(rows)
    df = df[df.n_rural_hh >= MIN_N]
    df.round(3).to_csv(out, index=False)
    nat = measures(r)
    print(f'{len(df)} districts -> {out}')
    print('four provinces, rural: landless %.1f%%, rent in %.1f%%, median owned %.1f ac'
          % (nat['landless_pct'], nat['rent_in_pct'], nat['median_owned_ac']))


if __name__ == '__main__':
    here = pathlib.Path(__file__).resolve().parent
    main(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else here.parent / 'pslm2019_district_land.csv')
