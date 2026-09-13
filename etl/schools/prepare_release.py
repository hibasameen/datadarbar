#!/usr/bin/env python3
"""
Assemble the district-level, coverage and validation tables that ship beside
schools_pk in Data Darbar's etl/schools/ folder. Inputs are the rerun-2026-09
outputs of the analysis (filled layer, designation network, LCC projection,
Table 13(b) outcome) plus the validation run on the released layer.

Usage: python3 prepare_release.py --rerun <rerun-2026-09> --validation <dir> --schools <schools_pk.csv.gz> --out <dir>
"""
import argparse, json, re
from pathlib import Path
import numpy as np, pandas as pd

RELEASE = '2026-09'

def flat(k): return re.sub(r'[^a-z]', '', str(k).lower())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--rerun', type=Path, required=True)
    ap.add_argument('--validation', type=Path, required=True)
    ap.add_argument('--schools', type=Path, required=True)
    ap.add_argument('--darbar', type=Path, required=True)
    ap.add_argument('--out', type=Path, required=True)
    a = ap.parse_args()
    R = a.rerun; V = a.validation; O = a.out; O.mkdir(parents=True, exist_ok=True)
    FD = R / 'filled_designation'
    keys = json.load(open(a.darbar)).keys()
    unflat = {flat(k): k for k in keys}
    sp = pd.read_csv(a.schools, low_memory=False)

    # ── 1. school_access_district: one row per analysis district (132) ─────
    da = pd.read_csv(FD / 'district_access.csv')
    da['district_key'] = da['district'].map(unflat)
    assert da['district_key'].notna().all(), da[da['district_key'].isna()]
    gg = pd.read_csv(FD / 'gap_on_gap.csv')[['district_key', 'girls_ner', 'boys_ner', 'enrol_gap_pp', 'ls_deprivation', 'material_deprivation']]
    gg['district_key'] = gg['district_key'].map(unflat)
    gg = gg.rename(columns={'girls_ner': 'girls_in_school_pct_5_16', 'boys_ner': 'boys_in_school_pct_5_16',
                            'enrol_gap_pp': 'enrol_gap_pp_boys_minus_girls', 'ls_deprivation': 'living_standards_deprivation_pslm',
                            'material_deprivation': 'material_deprivation_pslm'})
    n = pd.read_csv(FD / 'national_distance_stats.csv')
    n = n[n['geography'].str.lower() != n['region'].str.lower()].copy()
    n['district_key'] = n['geography'].map(unflat)
    # n_schools in the district rows of national_distance_stats is the REGION's
    # network (schools across a district line count), so per-district school
    # counts come from schools_pk instead: schools located in the district.
    piv = n.pivot_table(index='district_key', columns=['sex', 'cls'], values=['median_km', 'share_over_5km'])
    piv.columns = [f'{sex}_{cls.replace("_plus", "")}_{val}' for val, sex, cls in piv.columns]
    piv = piv.reset_index()
    # Counts use district_key_boundary (the polygon the point falls in), the
    # basis of Figure 1; sums reproduce the piece's 13,754 v 18,060.
    ia = sp[sp['in_analysis'] & sp['district_key_boundary'].notna()].copy()
    ia['sex'] = ia['analysis_sex'].map({'G': 'girls', 'B': 'boys'})
    for lvl, col in [('primary', 'primary_plus'), ('middle', 'middle_plus'), ('high', 'high_plus')]:
        t = ia[ia[col]].groupby(['district_key_boundary', 'sex']).size().unstack(fill_value=0).rename_axis('district_key')
        for sex in ['girls', 'boys']:
            piv = piv.merge(t[sex].rename(f'{sex}_{lvl}_n_schools').reset_index(), on='district_key', how='left')
    sn = pd.read_csv(R / 'sindh_network_definitions_district_2026-09.csv')
    sn = sn[sn['region'] == 'Sindh'][['district_name', 'girls_km_mixed', 'boys_km_mixed', 'gap_km_mixed', 'girls_km_attend', 'boys_km_attend', 'gap_km_attend']]
    sn = sn.rename(columns={'district_name': 'district_name_s'})
    out = da.merge(gg, on='district_key', how='left').merge(piv, on='district_key', how='left')
    out = out.merge(sn, left_on='district_name', right_on='district_name_s', how='left').drop(columns=['district_name_s'])
    # listed / released school counts from schools_pk
    cnt = sp.groupby('district_key').agg(schools_listed=('row_id', 'size'), schools_with_coords=('has_coords', 'sum'),
                                          schools_in_analysis=('in_analysis', 'sum')).reset_index()   # by SOURCE district
    out = out.merge(cnt, on='district_key', how='left')
    out = out.rename(columns={'district': 'district_id', 'district_name': 'district'})
    out['release'] = RELEASE
    cols = ['region', 'district_key', 'district', 'all_km', 'boys_km', 'girls_km', 'gap_km', 'all_min', 'boys_min', 'girls_min', 'gap_min', 'tt_caveat',
            'girls_in_school_pct_5_16', 'boys_in_school_pct_5_16', 'enrol_gap_pp_boys_minus_girls',
            'living_standards_deprivation_pslm', 'material_deprivation_pslm',
            'schools_listed', 'schools_with_coords', 'schools_in_analysis']
    cols += [c for c in out.columns if re.match(r'(girls|boys)_(primary|middle|high)_(n_schools|median_km|share_over_5km)$', c)]
    cols = [c for c in cols if c.endswith('n_schools')][:0] + cols  # keep order
    cols += ['girls_km_mixed', 'boys_km_mixed', 'gap_km_mixed', 'girls_km_attend', 'boys_km_attend', 'gap_km_attend', 'release']
    out = out[cols].sort_values(['region', 'district']).reset_index(drop=True)
    for c in out.columns:
        if out[c].dtype == float and c.endswith(('_km', '_min', 'km_mixed', 'km_attend')): out[c] = out[c].round(3)
        if c.endswith('share_over_5km'): out[c] = out[c].round(4)
        if c.endswith('n_schools') or c.startswith('schools_'): out[c] = out[c].fillna(0).astype('Int64') if c.endswith('n_schools') else out[c].astype('Int64')
    out.to_csv(O / 'school_access_district.csv', index=False)

    # ── 2. school_distance_stats: the long table, region and district rows ──
    nd = pd.read_csv(FD / 'national_distance_stats.csv')
    nd['district_key'] = np.where(nd['geography'].str.lower() == nd['region'].str.lower(), None, nd['geography'].map(unflat))
    nd['level'] = nd['cls'].str.replace('_plus', '', regex=False)
    nd['geography_type'] = np.where(nd['district_key'].isna(), 'region', 'district')
    nd = nd.rename(columns={'n_schools': 'network_schools'})
    nd = nd[['region', 'geography_type', 'district_key', 'sex', 'level', 'network_schools', 'pop', 'mean_km', 'median_km', 'share_over_2km', 'share_over_5km', 'share_over_10km', 'tier', 'sector']]
    for c in ['mean_km', 'median_km']: nd[c] = nd[c].round(3)
    for c in ['share_over_2km', 'share_over_5km', 'share_over_10km']: nd[c] = nd[c].round(4)
    nd['pop'] = nd['pop'].round(0).astype('Int64')
    nd['release'] = RELEASE
    nd.to_csv(O / 'school_distance_stats.csv', index=False)

    # ── 3. coverage ledger, counts reconciled to schools_pk ─────────────────
    led = pd.read_csv(R / 'coverage_ledger_proposed.csv')
    led.loc[led['region'] == 'Sindh', 'coverage_note'] = (
        '30 districts and every taluka; functional schools only; 22 without a position; '
        'girls\' and boys\' networks by name designation (GG/GB), the official SEMIS Boys/Girls/Mixed field kept as gender_official')
    led.loc[led['region'] == 'Sindh', 'schools_in_analysis'] = 39741
    reg_map = {'Balochistan': 'Balochistan', 'Sindh': 'Sindh', 'KP (settled districts)': 'KP (settled)', 'Punjab': 'Punjab',
               'Gilgit-Baltistan': 'Gilgit-Baltistan', 'AJK (Mirpur + Kotli)': 'AJK (Mirpur+Kotli)', 'Islamabad (ICT)': 'Islamabad (ICT)'}
    rel = sp.groupby('region').agg(rows_in_schools_pk=('row_id', 'size'), rows_with_coords=('has_coords', 'sum'), rows_in_analysis=('in_analysis', 'sum'))
    led['schools_pk_region'] = led['region'].map(reg_map)
    led = led.merge(rel, left_on='schools_pk_region', right_index=True, how='left')
    for c in ['rows_in_schools_pk', 'rows_with_coords', 'rows_in_analysis']: led[c] = led[c].astype('Int64')
    led['release'] = RELEASE
    led.to_csv(O / 'school_layer_coverage.csv', index=False)

    # ── 4. validation tables ────────────────────────────────────────────────
    A = pd.read_csv(V / 'validation_A_counts_by_district.csv')
    C = pd.read_csv(V / 'validation_C_distance_by_district.csv')
    A['district_key'] = A['dkey'].map(unflat); C['district_key'] = C['dkey'].map(unflat)
    A = A.drop(columns=['dkey']); C = C.drop(columns=['dkey', 'region', 'mouza_district', 'rural_mouzas'])
    A = A.rename(columns={'mouza_name': 'mouza_district', 'harvest_name': 'layer_district'})
    A['flag'] = A['flag'].fillna('')
    AC = A.merge(C, on='district_key', how='left')
    front = ['region', 'district_key', 'mouza_district', 'layer_district', 'status', 'rural_mouzas', 'flag']
    AC = AC[front + [c for c in AC.columns if c not in front]]
    AC['release'] = RELEASE
    AC.round(3).to_csv(O / 'school_validation_district.csv', index=False)
    B = pd.read_csv(V / 'validation_B_counts_by_tehsil.csv')
    B = B[B['region'].isin(['Sindh', 'Punjab', 'Balochistan', 'Gilgit-Baltistan'])].copy()
    B['district_key'] = B['dkey'].map(unflat)
    B = B.drop(columns=['dkey']).rename(columns={'tkey': 'tehsil_key', 'mouza_name': 'mouza_tehsil', 'harvest_name': 'layer_tehsil'})
    B['flag'] = B['flag'].fillna('')
    front = ['region', 'district_key', 'tehsil_key', 'mouza_tehsil', 'layer_tehsil', 'status', 'rural_mouzas', 'flag']
    B = B[front + [c for c in B.columns if c not in front]]
    B['release'] = RELEASE
    B.to_csv(O / 'school_validation_tehsil.csv', index=False)
    S = pd.read_csv(V / 'validation_C_summary.csv')
    Ar = pd.read_csv(V / 'validation_A_summary_by_region.csv')
    S['release'] = RELEASE; Ar['release'] = RELEASE
    S.to_csv(O / 'school_validation_summary.csv', index=False)
    Ar.to_csv(O / 'school_validation_counts_by_region.csv', index=False)

    # ── 5. census enrolment 5–16 by sex (Table 13(b), boundary districts) ───
    ce = pd.read_csv(R / 'census2023_enrolment_5_16_by_sex.csv')
    ce['district_key'] = ce['district'].map(lambda x: unflat.get(flat(x)))
    ce['release'] = RELEASE
    ce.to_csv(O / 'census_enrolment_5_16_by_sex.csv', index=False)

    # ── manifest ────────────────────────────────────────────────────────────
    man = []
    for f in sorted(O.glob('*.csv')) + sorted(O.glob('*.csv.gz')):
        d = pd.read_csv(f, low_memory=False)
        man.append({'file': f.name, 'rows': len(d), 'columns': len(d.columns), 'bytes': f.stat().st_size})
    pd.DataFrame(man).to_csv(O / 'manifest.csv', index=False)
    print(pd.DataFrame(man).to_string(index=False))
    print('census districts without key:', ce[ce['district_key'].isna()]['district'].tolist())

if __name__ == '__main__':
    main()
