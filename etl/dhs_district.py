"""
Compute district-level indicators from the Pakistan Demographic & Health Survey
(PDHS) 2017-18 microdata, aligned to the app's district crosswalk.

SOURCE
------
NIPS "View Public Data" → "Micro Data Set of PDHS 2017-18 in STATA Format":
  https://www.nips.org.pk/viewpublicdata
Download the STATA zip and extract the recode folders (PKIR71DT, PKKR71DT, ...).
Point DHS_DIR at the folder that contains those sub-folders.

METHODOLOGICAL NOTE
-------------------
The PDHS 2017-18 is representative at the national, provincial and region level
(8 regions x urban/rural = 16 strata), NOT at the district level. District
estimates below are therefore INDICATIVE: each carries its unweighted sample
size (`*_n_obs`) and districts with n < 30 are suppressed (`*_low_n = true`)
and shown greyed-out in the app.

Weights: the national women's weight `v005` is zero for Gilgit-Baltistan and
Azad Jammu & Kashmir (those domains are excluded from national estimates), so
we fall back to the combined weight `sv005` there. Because every district sits
within a single region, this gives correct WITHIN-district weighting everywhere
while leaving the four provinces + ICT + FATA on the published `v005` scale.

Denominator-specific groups (each carries its own n_obs / low_n):
  dhs_fp    Family Planning            currently married women 15-49
  dhs_fert  Fertility & Child Survival all women 15-49
  dhs_mat   Maternal Health            women with a live birth in last 5 yrs
  dhs_imm   Child Immunisation         living children aged 12-23 months
  dhs_nut   Child Nutrition            children 0-59 months, valid anthropometry

Validated against published PDHS 2017-18 national figures (weighted):
  CPR any 33.7% (34.2), modern 24.7% (25.0), unmet need 18.4% (17.3),
  ANC4 49.6% (~51), SBA 69.7% (~69), facility delivery 68.0% (~66),
  full immunisation 66.0% (~66), stunting 37.2% (37.6), wasting 6.5% (7.1),
  underweight 21.9% (23.1).

Run standalone:  python dhs_district.py /path/to/DHS_DIR
"""
import sys, json
from pathlib import Path
from collections import defaultdict

MIN_N = 30

# DHS district-label -> GeoJSON-normalised name, for names/spellings the base
# crosswalk in build_dataset.py doesn't already cover (mostly GB/AJK + variants).
DHS_EXTRA_CROSSWALK = {
    'astore': 'astor', 'diamer': 'diamir', 'ghanche': 'ghanchi',
    'hattian bala': 'hattian', 'hunza': 'hunza nagar', 'nagar': 'hunza nagar',
    'jafarabad': 'jaffarabad', 'karachi malir': 'karachi',
    'kharmang': 'skardu', 'shigar': 'skardu',
    'naushahro firoze': 'naushehro feroze', 'sudhonti': 'sudhnutti',
}


def _add(acc, ind, geo_k, w, val, valid=True):
    if geo_k is None or not valid:
        return
    a = acc[ind][geo_k]
    a['den'] += w
    a['num'] += w * val
    a['n'] += 1


def compute(dhs_dir):
    """Return {geo_norm_name: {dhs_*: value, ..._n_obs, ..._low_n}}."""
    import pyreadstat, pandas as pd, numpy as np
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from build_dataset import norm, apply_crosswalk

    dhs_dir = Path(dhs_dir)
    IR = dhs_dir / 'PKIR71DT' / 'PKIR71FL.DTA'
    KR = dhs_dir / 'PKKR71DT' / 'PKKR71FL.DTA'

    def geo_name(label):
        n = apply_crosswalk(norm(str(label)))
        return DHS_EXTRA_CROSSWALK.get(n, n)

    acc = {k: defaultdict(lambda: defaultdict(lambda: {'num': 0.0, 'den': 0.0, 'n': 0}))
           for k in ('fp', 'fert', 'mat', 'imm', 'nut')}

    # ── IR: women 15-49 ──
    ir_cols = ['sdist', 'v005', 'sv005', 'v502', 'v313', 'v626a', 'v201', 'v206',
               'v207', 'v208', 'm14_1', 'm15_1', 'm3a_1', 'm3b_1', 'm3c_1', 'm3d_1']
    ir, m_ir = pyreadstat.read_dta(str(IR), usecols=ir_cols)
    ir['w'] = np.where(ir['v005'] > 0, ir['v005'], ir['sv005']) / 1e6
    lbl = m_ir.variable_value_labels['sdist']
    ir['geo'] = ir['sdist'].map(lambda c: geo_name(lbl.get(c, c)))

    for _, r in ir.iterrows():
        g, w = r['geo'], r['w']
        if r['v502'] == 1:  # currently married
            v = r['v313']
            _add(acc['fp'], 'cpr_any', g, w, 1.0 if v in (1, 2, 3) else 0.0, pd.notna(v))
            _add(acc['fp'], 'cpr_modern', g, w, 1.0 if v == 3 else 0.0, pd.notna(v))
            u = r['v626a']
            _add(acc['fp'], 'unmet_need', g, w, 1.0 if u in (1, 2) else 0.0,
                 pd.notna(u) and u != 0)
        if pd.notna(r['v201']):
            _add(acc['fert'], 'ceb_mean', g, w, float(r['v201']))
        ceb, died = r['v201'], (r['v206'] or 0) + (r['v207'] or 0)
        if pd.notna(ceb) and ceb > 0:
            _add(acc['fert'], 'child_loss', g, w, float(died) / float(ceb))
        if pd.notna(r['v208']) and r['v208'] > 0:  # birth in last 5 yrs
            m14 = r['m14_1']
            _add(acc['mat'], 'anc4', g, w, 1.0 if (pd.notna(m14) and 4 <= m14 < 98) else 0.0, pd.notna(m14))
            m15 = r['m15_1']
            _add(acc['mat'], 'facility', g, w, 1.0 if (pd.notna(m15) and 20 <= m15 < 96) else 0.0, pd.notna(m15))
            sba = any(r[c] == 1 for c in ('m3a_1', 'm3b_1', 'm3c_1', 'm3d_1'))
            _add(acc['mat'], 'sba', g, w, 1.0 if sba else 0.0)

    # ── KR: children ──
    kr_cols = ['sdist', 'v005', 'sv005', 'b5', 'b19', 'hw13', 'hw70', 'hw71', 'hw72',
               'h2', 'h3', 'h5', 'h7', 'h4', 'h6', 'h8', 'h9']
    kr, m_kr = pyreadstat.read_dta(str(KR), usecols=kr_cols)
    kr['w'] = np.where(kr['v005'] > 0, kr['v005'], kr['sv005']) / 1e6
    lblk = m_kr.variable_value_labels['sdist']
    kr['geo'] = kr['sdist'].map(lambda c: geo_name(lblk.get(c, c)))
    got = lambda v: pd.notna(v) and v in (1, 2, 3)

    for _, r in kr.iterrows():
        g, w = r['geo'], r['w']
        if r['b5'] == 1 and pd.notna(r['b19']) and 12 <= r['b19'] <= 23:
            full = (got(r['h2']) and all(got(r[c]) for c in ('h3', 'h5', 'h7'))
                    and all(got(r[c]) for c in ('h4', 'h6', 'h8')) and got(r['h9']))
            _add(acc['imm'], 'full_immun', g, w, 1.0 if full else 0.0)
            _add(acc['imm'], 'dpt3', g, w, 1.0 if got(r['h7']) else 0.0)
            _add(acc['imm'], 'measles', g, w, 1.0 if got(r['h9']) else 0.0)
        if r['hw13'] == 0:  # anthropometry measured
            for ind, col in (('stunting', 'hw70'), ('underweight', 'hw71'), ('wasting', 'hw72')):
                v = r[col]
                if pd.notna(v) and abs(v) < 900:  # exclude flagged 9996-9999
                    _add(acc['nut'], ind, g, w, 1.0 if v < -200 else 0.0)

    # ── finalise ──
    PCT = {'fp': True, 'fert': None, 'mat': True, 'imm': True, 'nut': True}
    MEAN_INDS = {'ceb_mean'}   # reported as a mean, not a percentage
    result = defaultdict(dict)
    for key, a in acc.items():
        prefix = f'dhs_{key}'
        n_by_geo = defaultdict(int)
        for ind, gd in a.items():
            for gk, d in gd.items():
                n_by_geo[gk] = max(n_by_geo[gk], d['n'])
        for ind, gd in a.items():
            for gk, d in gd.items():
                if d['den'] > 0:
                    v = d['num'] / d['den']
                    if ind in MEAN_INDS:
                        result[gk][f'{prefix}_{ind}'] = round(v, 2)
                    else:
                        result[gk][f'{prefix}_{ind}'] = round(v * 100, 1)
        for gk in set(result.keys()) | set(n_by_geo.keys()):
            n = n_by_geo[gk]
            if n == 0:
                continue
            result[gk][f'{prefix}_n_obs'] = int(n)
            low = n < MIN_N
            result[gk][f'{prefix}_low_n'] = bool(low)
            if low:
                for k in list(result[gk].keys()):
                    if k.startswith(prefix + '_') and not k.endswith(('_n_obs', '_low_n')):
                        result[gk][k] = None
    return dict(result)


if __name__ == '__main__':
    d = sys.argv[1] if len(sys.argv) > 1 else '.'
    res = compute(d)
    out = Path(__file__).resolve().parent / 'dhs_district_indicators.json'
    json.dump(res, open(out, 'w'), indent=2, ensure_ascii=False)
    print(f'Wrote {len(res)} districts -> {out}')
