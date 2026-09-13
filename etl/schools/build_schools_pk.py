#!/usr/bin/env python3
"""
Build schools_pk: one row per government school in the layer behind Adaad's
"How far is the girls' school?" (September 2026), with the provenance every row
needs before anyone treats lat/lng as a GPS fix.

Reads the per-province files from the Adaad working folder (data/pk-school-access)
and writes:

  schools_pk_<release>.csv.gz      one row per listed government school
  schools_pk_build_report.json     counts that must reconcile with the piece

Every region enters with the SAME filters build_national.py applied for the
analysis (LAYER=filled, SINDH_NET=designation), so in_analysis reproduces the
school counts in national_distance_stats.csv exactly. Rows the analysis dropped
are kept with in_analysis = false and analysis_note saying why.

Usage: python3 build_schools_pk.py --src <pk-school-access> --darbar <districts.json> --out <dir>
"""
import argparse, json, re
from pathlib import Path
import numpy as np, pandas as pd

RELEASE = "2026-09"

# ── district keys ──────────────────────────────────────────────────────────
# Data Darbar's 147-district frame is the boundary the analysis used, with the
# districts carved out after those boundaries were drawn summed back into the
# parent. district_key is therefore the parent's Darbar key; district keeps the
# source's own name so nothing is lost.
ALIAS = {
    'surab': 'kalat', 'chaman': 'killa abdullah', 'duki': 'loralai',
    'lehri': 'sibi', 'usta muhammad': 'jaffarabad', 'barkhan': 'barkhan',
    'shaheed sikandarabad': 'kalat', 'sohbatpur': 'sohbatpur',
    'lower chitral': 'chitral', 'upper chitral': 'chitral', 'chitral lower': 'chitral', 'chitral upper': 'chitral',
    'lower kohistan': 'kohistan', 'upper kohistan': 'kohistan', 'kolai pallas': 'kohistan', 'kolai palas': 'kohistan',
    'd.i.khan': 'dera ismail khan', 'd i khan': 'dera ismail khan', 'di khan': 'dera ismail khan',
    'dir upper': 'upper dir', 'dir lower': 'lower dir', 'lakki': 'lakki marwat', 'torghar': 'tor ghar',
    'kambar shahdadkot': 'kambar shahdadkot', 'qambar shahdadkot': 'kambar shahdadkot',
    'kamber shahdadkot': 'kambar shahdadkot', 'shikarpur': 'shikarpur',
    'sujawal': 'sajawal', 'naushahro feroze': 'naushehro feroze', 'nausheroferoze': 'naushehro feroze',
    'naushero feroze': 'naushehro feroze', 'tando allahyar': 'tando allah yar',
    'karachi keamari': 'keamari', 'kemari': 'keamari', 'karachi korangi': 'korangi', 'karachi malir': 'malir',
    'astore': 'astor', 'diamer': 'diamir', 'ghanche': 'ghanchi', 'hunza': 'hunza nagar', 'nagar': 'hunza nagar',
    'baltistan': 'skardu', 'shigar': 'skardu', 'kharmang': 'skardu', 'roundu': 'skardu',
    'darel': 'diamir', 'tangir': 'diamir', 'gupis yasin': 'ghizer', 'gupis-yasin': 'ghizer',
    'rawalpindi': 'rawalpindi', 'murree': 'rawalpindi', 'talagang': 'chakwal', 'kot addu': 'muzaffargarh',
    'wazirabad': 'gujranwala', 'taunsa': 'dera ghazi khan', 'dg khan': 'dera ghazi khan', 'd.g. khan': 'dera ghazi khan',
    'd.g.khan': 'dera ghazi khan', 'rahimyar khan': 'rahim yar khan', 'ry khan': 'rahim yar khan',
    'toba tek singh': 'toba tek singh', 't.t.singh': 'toba tek singh', 'nankana': 'nankana sahib',
    'mandi bahauddin': 'mandi bahauddin', 'm.b.din': 'mandi bahauddin', 'jhal magsi': 'jhal magsi',
    'dera bugti': 'dera bugti', 'killa saifullah': 'killa saifullah', 'qilla saifullah': 'killa saifullah',
    'qila saifullah': 'killa saifullah', 'qilla abdullah': 'killa abdullah', 'qila abdullah': 'killa abdullah',
    'nasirabad': 'nasirabad', 'naseerabad': 'nasirabad', 'chagai': 'chaghi', 'nushki': 'nushki', 'noshki': 'nushki',
    'musakhel': 'musakhail', 'kachhi': 'kachhi', 'bolan': 'kachhi', 'kech': 'kech', 'turbat': 'kech',
    'sherani': 'sherani', 'islamabad': 'islamabad', 'ict': 'islamabad',
    'barshore': 'pishin', 'hub': 'lasbela', 'north dera bugti': 'dera bugti', 'south dera bugti': 'dera bugti', 'tump': 'kech',
    'kambar at shahdadkot': 'kambar shahdadkot', 'kashmore at kandhkot': 'kashmore', 'tharparkar at mithi': 'tharparkar',
    'khairpur mirs': 'khairpur', 'kot adu': 'muzaffargarh',
}

def _norm(s):
    s = str(s).lower().strip()
    s = re.sub(r'\b(district|agency)\b', '', s)
    s = re.sub(r'[^a-z. ]', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()

class Keyer:
    def __init__(self, darbar_keys):
        self.keys = set(darbar_keys)
        self.flat = {re.sub(r'[^a-z]', '', k): k for k in darbar_keys}
        self.miss = {}
    def __call__(self, name):
        if pd.isna(name): return None
        n = _norm(name)
        if n in ALIAS: n = ALIAS[n]
        if n in self.keys: return n
        f = re.sub(r'[^a-z]', '', n)
        if f in self.flat: return self.flat[f]
        f2 = re.sub(r'[^a-z]', '', ALIAS.get(n, n))
        if f2 in self.flat: return self.flat[f2]
        self.miss[name] = self.miss.get(name, 0) + 1
        return None

def title(s):
    return None if pd.isna(s) else ' '.join(w.capitalize() if w.isalpha() else w for w in str(s).strip().split())

COLS = ['school_id','province','region','district','district_key','tehsil','name',
        'level','level_std','primary_plus','middle_plus','high_plus',
        'gender','gender_official','boys_enrolled','girls_enrolled','enrolment_total',
        'status','functional','lat','lng','has_coords',
        'coord_method','coord_precision','coord_tier','coord_resid_m','pin_vs_solved_m','geocode_match',
        'source','source_url','source_vintage','in_analysis','analysis_sex','analysis_note','release']

def finish(df, region, province, tier, source, url, vintage):
    df = df.copy()
    df['region'] = region; df['province'] = province; df['coord_tier'] = tier
    df['source'] = source; df['source_url'] = url; df['source_vintage'] = vintage
    df['release'] = RELEASE
    df['has_coords'] = df['lat'].notna() & df['lng'].notna()
    for c in COLS:
        if c not in df.columns: df[c] = None
    df['analysis_sex'] = df['analysis_sex'].where(df['in_analysis'], None)
    return df[COLS]

def std_level(x, mapping):
    return mapping.get(x, 'other')

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True, type=Path)
    ap.add_argument('--darbar', required=True, type=Path)
    ap.add_argument('--geojson', required=True, type=Path, help="Data Darbar's 147-district boundary file")
    ap.add_argument('--out', required=True, type=Path)
    a = ap.parse_args()
    S = a.src; F = S / 'fill-2026-09'
    key = Keyer(json.load(open(a.darbar)).keys())
    parts = []

    # ── Balochistan: SED open-data portal, GPS at source ─────────────────
    b = pd.read_csv(S / 'sed_balochistan_schools.csv', dtype={'emis': str})
    LB = {'P': 'primary', 'M': 'middle', 'H': 'high', 'S': 'high'}
    b['level_std'] = b['level'].map(lambda x: std_level(x, LB))
    b['primary_plus'] = True
    b['middle_plus'] = b['level'].isin(['M', 'H', 'S']); b['high_plus'] = b['level'].isin(['H', 'S'])
    b['gender'] = b['gender'].map({'B': 'Boys', 'G': 'Girls'}).fillna('Unknown')
    b['school_id'] = b['emis']; b['enrolment_total'] = b['enrollment']
    b['district'] = b['district'].map(title); b['tehsil'] = b['tehsil'].map(title)
    b['district_key'] = b['district'].map(key)
    b['level'] = b['level'].map({'P': 'Primary', 'M': 'Middle', 'H': 'High', 'S': 'Higher Secondary'}).fillna('Unknown')
    b['status'] = None; b['functional'] = None
    b['coord_method'] = np.where(b['lat'].notna(), 'gps_at_source', None)
    b['coord_precision'] = np.where(b['lat'].notna(), 'school', 'none')
    b['in_analysis'] = b['lat'].notna() & b['gender'].isin(['Boys', 'Girls'])
    b['analysis_sex'] = b['gender'].map({'Boys': 'B', 'Girls': 'G'})
    b['analysis_note'] = np.select([b['lat'].isna(), ~b['gender'].isin(['Boys', 'Girls'])],
                                   ['no coordinates at source', 'gender not stated at source'], None)
    parts.append(finish(b, 'Balochistan', 'Balochistan', 'A',
                        'Secondary Education Department Balochistan open-data portal',
                        'https://sed.gob.pk/', 'August 2026'))

    # ── Sindh: SELD Institution Checker roster + RSU pins + multilateration ─
    s = pd.read_csv(F / 'sindh_schools_filled_2026-09.csv', dtype={'semis': str})
    s = s[s['is_school']].copy()                      # 856 'Office' rows are not schools
    LS = {'Primary': 'primary', 'Middle': 'middle', 'Elementary': 'middle', 'Secondary': 'high', 'Higher Secondary': 'high'}
    s['level_std'] = s['level'].map(lambda x: std_level(x, LS))
    s['primary_plus'] = True
    s['middle_plus'] = s['level'].isin(['Middle', 'Elementary', 'Secondary', 'Higher Secondary'])
    s['high_plus'] = s['level'].isin(['Secondary', 'Higher Secondary'])
    s['gender'] = s['gender_designation'].fillna('Unknown')
    s['gender_official'] = s['gender_semis']
    s['school_id'] = s['semis']
    s['enrolment_total'] = s['enrolment']
    s['district'] = s['district'].map(title); s['tehsil'] = s['taluka'].map(title)
    s['district_key'] = s['district'].map(key)
    s['status'] = s['status_checker']
    s['coord_method'] = np.select(
        [s['coord_source'].str.startswith('RSU KML', na=False),
         s['coord_source'].str.startswith('checker frame', na=False)],
        ['rsu_pin_confirmed', 'multilaterated'], None)
    s['coord_precision'] = np.where(s['lat'].notna(), 'school', 'none')
    s['coord_resid_m'] = s['cf_resid_m'].round(1)
    s['pin_vs_solved_m'] = s['kml_vs_cf_m'].round(0)
    s['geocode_match'] = s['coord_quality'].map({'A': 'A: pin within 300 m of solved position',
                                                 'B': 'B: solved from checker distances (pin absent or >300 m off)',
                                                 'C': 'C: solved, fair residual', 'none': None})
    s['in_analysis'] = s['functional'] & s['lat'].notna() & s['gender'].isin(['Boys', 'Girls'])
    s['analysis_sex'] = s['gender'].map({'Boys': 'B', 'Girls': 'G'})
    s['analysis_note'] = np.select([~s['functional'], s['lat'].isna(), ~s['gender'].isin(['Boys', 'Girls'])],
                                   ['not functional (checker status)', 'no position solved', 'no boys/girls designation in the name'], None)
    parts.append(finish(s, 'Sindh', 'Sindh', 'A',
                        'SELD Institution Checker roster and Distance Checker; RSU district GIS pins',
                        'https://checker.sindheducation.gov.pk/', 'September 2026'))

    # ── KP settled districts: KPEMA locator + JSiMS mirror for primaries ───
    k = pd.read_csv(F / 'kp_schools_filled_2026-09.csv', dtype={'emis': str})
    LK = {'Primary': 'primary', 'Middle': 'middle', 'High': 'high', 'Higher Secondary': 'high', 'Mosque': 'primary'}
    k['level_std'] = k['level'].map(lambda x: std_level(x, LK))
    k['primary_plus'] = True
    k['middle_plus'] = k['level'].isin(['Middle', 'High', 'Higher Secondary'])
    k['high_plus'] = k['level'].isin(['High', 'Higher Secondary'])
    k['school_id'] = k['emis']
    k['district'] = k['district'].map(title)
    k['district_key'] = k['district_2015'].map(key)
    k['tehsil'] = None
    k['status'] = k['status']
    k['functional'] = ~k['status'].isin(['Closed', 'Inaccessible'])
    k['coord_method'] = np.select([k['source'].str.startswith('KPEMA', na=False), k['source'].str.startswith('JSiMS', na=False)],
                                  ['kpema_detail', 'jsims_mirror'], None)
    k['coord_precision'] = np.where(k['lat'].notna(), 'school', 'none')
    k['in_analysis'] = k['functional'] & k['lat'].notna() & k['gender'].isin(['Boys', 'Girls'])
    k['analysis_sex'] = k['gender'].map({'Boys': 'B', 'Girls': 'G'})
    k['analysis_note'] = np.select([~k['functional'], k['lat'].isna()], ['closed or inaccessible (KPEMA status)', 'no coordinates'], None)
    parts.append(finish(k, 'KP (settled)', 'Khyber Pakhtunkhwa', 'A',
                        'KP Education Monitoring Authority school locator (roster + per-school detail); JSiMS mirror of KP EMIS for primaries',
                        'http://175.107.63.45/newimusite/', 'September 2026 (JSiMS: August 2026)'))

    # ── Punjab: SIS roster, gazetteer-geocoded ─────────────────────────────
    p = pd.read_csv(S / 'punjab_schools_geocoded.csv', dtype={'School EMIS': str})
    LP = {'Primary': 'primary', 'Middle': 'middle', 'High': 'high', 'H.Sec.': 'high', 'sMosque': 'primary'}
    p['level'] = p['School Level'].replace({'H.Sec.': 'Higher Secondary', 'sMosque': 'Mosque'})
    p['level_std'] = p['School Level'].map(lambda x: std_level(x, LP))
    p['primary_plus'] = True
    p['middle_plus'] = p['School Level'].isin(['Middle', 'High', 'H.Sec.'])
    p['high_plus'] = p['School Level'].isin(['High', 'H.Sec.'])
    p['gender'] = p['School Type'].map({'Male': 'Boys', 'Female': 'Girls'}).fillna('Unknown')
    p['school_id'] = p['School EMIS']; p['name'] = p['School Name']
    p['district'] = p['District'].map(title); p['tehsil'] = p['Tehsil'].map(title)
    p['district_key'] = p['District'].map(key)
    p['status'] = None; p['functional'] = None
    VIL = {'exact', 'exact_fold', 'chak', 'chak_num', 'chak_prov', 'fuzzy', 'fuzzy_tehsil'}
    p['coord_method'] = np.select([p['match_method'].isin(VIL), p['match_method'].isin(['markaz', 'markaz_demoted']), p['match_method'] == 'tehsil'],
                                  ['geocoded_settlement', 'geocoded_markaz', 'geocoded_tehsil'], None)
    p['coord_precision'] = np.select([p['match_method'].isin(VIL), p['match_method'].isin(['markaz', 'markaz_demoted']), p['match_method'] == 'tehsil'],
                                     ['settlement', 'markaz', 'tehsil'], 'none')
    p['geocode_match'] = p['match_method']
    p['in_analysis'] = p['lat'].notna() & p['gender'].isin(['Boys', 'Girls'])
    p['analysis_sex'] = p['gender'].map({'Boys': 'B', 'Girls': 'G'})
    p['analysis_note'] = np.select([p['lat'].isna()], ['settlement not found in gazetteer'], None)
    parts.append(finish(p, 'Punjab', 'Punjab', 'B-',
                        'Punjab School Information System roster; positions geocoded from settlement names against GeoNames/OSM',
                        'https://sis.pesrp.edu.pk/', 'August 2026'))

    # ── Gilgit-Baltistan: EMIS roster, geocoded ────────────────────────────
    g = pd.read_csv(S / 'gb_schools_geocoded.csv', dtype={'school_code': str})
    LG = {'Primary': 'primary', 'Middle': 'middle', 'High': 'high', 'Higher Secondary': 'high'}
    g['level_std'] = g['level'].map(lambda x: std_level(x, LG))
    g['primary_plus'] = True
    g['middle_plus'] = g['level'].isin(['Middle', 'High', 'Higher Secondary'])
    g['high_plus'] = g['level'].isin(['High', 'Higher Secondary'])
    g['gender'] = g['gender'].map({'B': 'Boys', 'G': 'Girls'}).fillna('Unknown')
    g['school_id'] = g['school_code']; g['name'] = g['school_name']
    g['district'] = g['district'].map(title); g['tehsil'] = g['tehsil'].map(title)
    g['district_key'] = g['district'].map(key)
    g['level'] = g['level'].fillna('Unknown')
    g['status'] = None; g['functional'] = None
    g['coord_method'] = np.select([g['match_method'].isin(['osm_exact', 'osm_fuzzy']), g['match_method'].isin(['place_exact', 'place_fuzzy']),
                                   g['match_method'].isin(['cluster', 'cluster_demoted'])],
                                  ['geocoded_school_point', 'geocoded_settlement', 'geocoded_cluster'], None)
    g['coord_precision'] = np.select([g['match_method'].isin(['osm_exact', 'osm_fuzzy']), g['match_method'].isin(['place_exact', 'place_fuzzy']),
                                      g['match_method'].isin(['cluster', 'cluster_demoted'])], ['school', 'settlement', 'cluster'], 'none')
    g['geocode_match'] = g['match_method']
    g['in_analysis'] = g['lat'].notna() & g['gender'].isin(['Boys', 'Girls'])
    g['analysis_sex'] = g['gender'].map({'Boys': 'B', 'Girls': 'G'})
    g['analysis_note'] = np.select([g['lat'].isna(), ~g['gender'].isin(['Boys', 'Girls'])],
                                   ['settlement not found in gazetteer', 'sex not inferable from the name (roster has no sex field)'], None)
    parts.append(finish(g, 'Gilgit-Baltistan', 'Gilgit-Baltistan', 'B',
                        'GB EMIS school roster; positions geocoded from settlement and cluster names',
                        'https://emis.gilgitbaltistan.gov.pk/', 'August 2026'))

    # ── AJK: Mirpur + Kotli exam-board rosters, government schools only ────
    a_ = pd.read_csv(S / 'ajk_two_districts_geocoded.csv')
    a_ = a_[a_['name'].str.upper().str.contains(r'GOVT|GOVERNMENT|\bGBHS|\bGGHS|\bGMS|\bGGMS|\bGPS|\bGGPS', regex=True)].copy()
    LA = {'primary': 'primary', 'middle': 'middle', 'high': 'high', 'higher_secondary': 'high'}
    a_['level_std'] = a_['level'].map(lambda x: std_level(x, LA))
    a_['primary_plus'] = True
    a_['middle_plus'] = a_['level'].isin(['middle', 'high', 'higher_secondary'])
    a_['high_plus'] = a_['level'].isin(['high', 'higher_secondary'])
    a_['level'] = a_['level'].map({'primary': 'Primary', 'middle': 'Middle', 'high': 'High', 'higher_secondary': 'Higher Secondary'}).fillna('Unknown')
    a_['gender'] = a_['gender'].map({'B': 'Boys', 'G': 'Girls'}).fillna('Unknown')
    a_['school_id'] = None; a_['tehsil'] = None
    a_['district'] = a_['district'].map(title); a_['district_key'] = a_['district'].map(key)
    a_['status'] = None; a_['functional'] = None
    a_['coord_method'] = np.where(a_['match_method'].isin(['exact', 'fuzzy']), 'geocoded_settlement', None)
    a_['coord_precision'] = np.where(a_['match_method'].isin(['exact', 'fuzzy']), 'settlement', 'none')
    a_['geocode_match'] = a_['match_method']
    govt = a_['name'].str.upper().str.contains('GOVT|GOVERNMENT')
    a_['in_analysis'] = govt & a_['lat'].notna() & a_['gender'].isin(['Boys', 'Girls'])
    a_['analysis_sex'] = a_['gender'].map({'Boys': 'B', 'Girls': 'G'})
    a_['analysis_note'] = np.select([a_['lat'].isna(), ~a_['gender'].isin(['Boys', 'Girls']), ~govt],
                                    ['settlement not found in gazetteer', 'sex not inferable from the name', 'name lacks Govt/Government'], None)
    parts.append(finish(a_, 'AJK (Mirpur+Kotli)', 'Azad Jammu and Kashmir', 'C',
                        'Mirpur and Kotli boards of intermediate and secondary education (school rosters); positions geocoded from settlement names',
                        'https://beemajk.com/; https://ebekotli.dsisajk.net/', 'August 2026'))

    # ── Islamabad: federal (FDE) institutions from OSM ─────────────────────
    i = pd.read_csv(S / 'ict_schools_combined.csv')
    nm0 = i['name'].str.upper().fillna('')
    i = i[nm0.str.contains(r'\bF\.?\s?G\b|IMCB|IMCG|\bICB\b|\bICG\b|ISLAMABAD MODEL|FEDERAL', regex=True)].copy()
    nm = i['name'].str.upper().fillna('')
    i['level_std'] = np.select([nm.str.contains(r'\bHIGH\b|SECONDARY|COLLEGE|IMCG|IMCB|\bICG\b|\bICB\b'), nm.str.contains('PRIMARY|JUNIOR')], ['high', 'primary'], 'middle')
    i['level'] = i['level'].map({'primary': 'Primary', 'middle': 'Middle', 'high': 'High', 'college_hs': 'Model College (school section)', 'unknown': 'Unknown'})
    i['primary_plus'] = True
    i['middle_plus'] = ~nm.str.contains('PRIMARY|JUNIOR|BECS')
    i['high_plus'] = nm.str.contains(r'\bHIGH\b|SECONDARY|COLLEGE|IMCG|IMCB|\bICG\b|\bICB\b')
    i['gender'] = i['gender'].map({'B': 'Boys', 'G': 'Girls'}).fillna('Unknown')
    i['school_id'] = None; i['district'] = 'Islamabad'; i['district_key'] = 'islamabad'; i['tehsil'] = None
    i['status'] = None; i['functional'] = None
    i['coord_method'] = 'osm_feature'; i['coord_precision'] = 'school'; i['geocode_match'] = i['source']
    i['in_analysis'] = i['gender'].isin(['Boys', 'Girls'])
    i['analysis_sex'] = i['gender'].map({'Boys': 'B', 'Girls': 'G'})
    i['analysis_note'] = np.select([~i['gender'].isin(['Boys', 'Girls'])], ['sex not inferable from the name'], None)
    parts.append(finish(i, 'Islamabad (ICT)', 'Islamabad Capital Territory', 'C',
                        'OpenStreetMap education features, Federal Directorate of Education institutions only',
                        'https://www.openstreetmap.org/', 'August 2026'))

    df = pd.concat(parts, ignore_index=True)
    df['name'] = df['name'].astype(str).str.strip()
    for c in ['primary_plus', 'middle_plus', 'high_plus', 'in_analysis', 'has_coords']:
        df[c] = df[c].astype(bool)
    df['functional'] = df['functional'].astype('boolean')
    for c in ['boys_enrolled', 'girls_enrolled', 'enrolment_total']:
        df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
    df['lat'] = df['lat'].round(6); df['lng'] = df['lng'].round(6)
    df = df.sort_values(['province', 'district', 'name'], kind='stable').reset_index(drop=True)
    df.insert(0, 'row_id', np.arange(1, len(df) + 1))

    # district_key_boundary: the Darbar polygon the point falls in. This is the
    # basis of the piece's per-district school counts (Figure 1); district_key
    # is what the source says. They differ for 3% of rows — border villages,
    # and Punjab schools placed at a tehsil centroid.
    from shapely.geometry import shape, Point
    from shapely.strtree import STRtree
    gj = json.load(open(a.geojson))
    pk = [(lambda n: n if n in key.keys else key(n))(f['properties']['districts'].lower().replace('-', ' ')) for f in gj['features']]
    tree = STRtree([shape(f['geometry']) for f in gj['features']])
    res = [None] * len(df)
    idx = [i for i, (x, y) in enumerate(zip(df['lng'], df['lat'])) if pd.notna(x) and pd.notna(y)]
    hits = tree.query([Point(df['lng'].iat[i], df['lat'].iat[i]) for i in idx], predicate='within')
    for qi, pi in zip(hits[0], hits[1]): res[idx[qi]] = pk[pi]
    df.insert(df.columns.get_loc('district_key') + 1, 'district_key_boundary', res)

    a.out.mkdir(parents=True, exist_ok=True)
    df.to_csv(a.out / f'schools_pk_{RELEASE}.csv.gz', index=False, compression='gzip')

    rep = {
        'release': RELEASE, 'rows': int(len(df)),
        'with_coords': int(df['has_coords'].sum()), 'in_analysis': int(df['in_analysis'].sum()),
        'by_region': df.groupby('region').agg(listed=('row_id', 'size'), with_coords=('has_coords', 'sum'),
                                             in_analysis=('in_analysis', 'sum')).astype(int).to_dict('index'),
        'in_analysis_by_region_sex': df[df['in_analysis']].groupby(['region', 'analysis_sex']).size().unstack().astype(int).to_dict('index'),
        'in_analysis_middle_plus_by_region_sex': df[df['in_analysis'] & df['middle_plus']].groupby(['region', 'analysis_sex']).size().unstack().astype(int).to_dict('index'),
        'district_key_missing': df[df['district_key'].isna()].groupby('region').size().astype(int).to_dict(),
        'inside_a_boundary': int(df['district_key_boundary'].notna().sum()),
        'boundary_differs_from_source': int(((df['district_key_boundary'] != df['district_key']) & df['district_key_boundary'].notna()).sum()),
        'fig1_middle_plus_in_boundary': df[df['in_analysis'] & df['middle_plus'] & df['district_key_boundary'].notna()].groupby('analysis_sex').size().astype(int).to_dict(),
        'unmatched_district_names': key.miss,
        'coord_method': df['coord_method'].fillna('none').value_counts().astype(int).to_dict(),
        'coord_precision': df['coord_precision'].value_counts().astype(int).to_dict(),
    }
    (a.out / 'schools_pk_build_report.json').write_text(json.dumps(rep, indent=1, default=str))
    print(json.dumps(rep, indent=1, default=str))

if __name__ == '__main__':
    main()
