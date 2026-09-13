"""Validate the RELEASED government-school layer (schools_pk) against the Mouza Census
2020 microdata (mouza level, rural mouzas = status 1).

Three tests, each written to a CSV beside this script:
  A. validation_A_counts_by_district.csv   collected schools by district, sex,
     level v. rural mouzas reporting an institution of that sex and level.
     Mouza counts are sector-blind and count villages, not schools, so the
     ratio is a floor test: well under 1 means schools are missing or
     misclassified; above 1 is expected.
  B. validation_B_counts_by_tehsil.csv     the same at tehsil level for the
     provinces whose harvest carries a tehsil (Sindh, Punjab, Balochistan,
     GB), to locate holes inside districts.
  C. validation_C_distance_by_district.csv the model's population-weighted
     distance to the nearest school of each sex (national_distance_stats.csv)
     v. the villages' own reported distance to the nearest institution.
     Levels are not comparable (unweighted, road, any sector); ranks, signs
     and the girls-minus-boys difference are.
"""
import os, re
import numpy as np, pandas as pd

import sys
SP = sys.argv[1]          # schools_pk_<release>.csv.gz
D = sys.argv[2]           # folder holding national_distance_stats.csv (filled_designation)
M = sys.argv[3]           # mc2020_mouza_level.csv.gz
OUT = sys.argv[4]         # output folder
os.makedirs(OUT, exist_ok=True)

ALIAS = {'kambershahdadkot':'kambar','kamberatshahdadkot':'kambar','kambarshahdadkot':'kambar','qambarshahdadkot':'kambar',
 'barshore':'pishin','chaman':'qilaabdullah','dgkhan':'deraghazikhan','diamer':'diamir','hub':'lasbela','kotadu':'muzaffargarh',
 'murree':'rawalpindi','musakhail':'musakhel','naseerabad':'nasirabad','noshki':'nushki','northderabugti':'derabugti',
 'southderabugti':'derabugti','skardu':'baltistan','surab':'kalat','ttsingh':'tobateksingh','talagang':'chakwal','tump':'kech',
 'ustamuhammad':'jaffarabad','wazirabad':'gujranwala','chitrallower':'chitral','chitralupper':'chitral','lowerkohistan':'kohistan',
 'kolaipalaskohistan':'kohistan','kolaipalas':'kohistan','upperkohistan':'kohistan','karachimalir':'malir','cholistan':'bahawalpur',
 'shaheedsikandarabad':'kalat','duki':'loralai','qillaabdullah':'qilaabdullah','killaabdullah':'qilaabdullah',
 'qillasaifullah':'qilasaifullah','killasaifullah':'qilasaifullah','khairpurmirs':'khairpur','larakana':'larkana',
 'nausheroferoze':'naushahroferoze','naushehroferoze':'naushahroferoze','kashmoreatkandhkot':'kashmore','dikhan':'deraismailkhan',
 'lehri':'sibi','hattianbala':'hattian','sudhnutti':'sudhnoti'}

# Mouza-side district keys -> Data Darbar district keys (spaces removed), which is
# what schools_pk.district_key and national_distance_stats.geography carry.
MK = {'qilaabdullah':'killaabdullah','qilasaifullah':'killasaifullah','kambar':'kambarshahdadkot',
      'naushahroferoze':'naushehroferoze','astore':'astor','ghanche':'ghanchi','hunza':'hunzanagar','nagar':'hunzanagar',
      'baltistan':'skardu','musakhel':'musakhail','chagai':'chaghi','sujawal':'sajawal','kemari':'keamari','shigar':'skardu','kharmang':'skardu','roundu':'skardu','darel':'diamir','tangir':'diamir','gupisyasin':'ghizer'}

def dkey(s):
    s = str(s).lower()
    s = re.sub(r'\b(district|agency|protected area|sub-division|subdivision)\b', '', s)
    s = re.sub(r'[^a-z]', '', s)
    s = ALIAS.get(s, s)
    return MK.get(s, s)

def tkey(s):
    s = str(s).lower()
    s = re.sub(r'\b(taluka|tehsil|sub-division|subdivision|sub division|city|saddar|cantt)\b', '', s)
    s = re.sub(r'\(.*?\)', '', s)
    s = re.sub(r'[^a-z]', '', s)
    TA = {'thull':'thul','knshah':'khairpurnathanshah','thanabulakhan':'thanobulakhan','warrah':'warah','sijawaljunejo':'sujawaljunejo',
          'naseerabad':'nasirabad','kashmore':'kashmor','bhirya':'bhiria','dalhi':'dahli','mirpurbithoro':'mirpurbathoro','shahbundar':'shahbunder',
          'ketibundar':'ketibunder','kharochann':'kharochan','tandomohdkhan':'tandomuhammadkhan','kotghulammohd':'kotghulammuhammad','jhuddo':'jhudo',
          'kambar':'kambaralikhan','khairpurmirs':'khairpur','nausheroferoze':'naushahroferoze','mitiari':'matiari','ratodero':'ratodero','panoakil':'panoakil',
          'nawabshah':'nawabshah','tandoallahyar':'tandoallahyar','umerkot':'umerkot','kotdiji':'kotdiji','mirpurkhas':'mirpurkhas',
          # SELD checker spellings (September 2026 roster)
          'shaheedfazalrahugolarchi':'golarchi','sehwansharif':'sehwan','tharimirwah':'mirwah','kotghulammohammad':'kotghulammuhammad',
          'shujabad':'shujaabad','shahbandar':'shahbunder','daulatpur':'kaziahmed','garhiyaseen':'garhiyasin','panoaqil':'panoakil',
          'ketibandar':'ketibunder','mirpurbithoro':'mirpurbathoro','kharochann':'kharochan'}
    return TA.get(s, s)

# The collected layer is the RELEASED table (schools_pk), restricted to the rows
# that entered the analysis (in_analysis), so the test describes the layer the
# distances were computed from, not the raw harvests.
sp = pd.read_csv(SP, low_memory=False)
sp = sp[sp['in_analysis'] & sp['level_std'].isin(['primary','middle','high'])].copy()
REG = {'KP (settled)':'KP','AJK (Mirpur+Kotli)':'AJK','Islamabad (ICT)':'Islamabad'}
sp['region'] = sp['region'].replace(REG)
c = sp[['region','district','tehsil','analysis_sex','level_std']].rename(columns={'analysis_sex':'sex','level_std':'level'}).reset_index(drop=True)
c['dkey'] = sp['district_key'].str.replace(' ', '', regex=False).values; c['tkey'] = c['tehsil'].map(tkey)

m = pd.read_csv(M, dtype=str, keep_default_na=False)
m = m[m['P1Q09'] == '1'].copy()
PROV = {'PUNJAB':'Punjab','SINDH':'Sindh','KHYBER PAKHTUNKHWA':'KP','BALOCHISTAN':'Balochistan','GILGIT BALTISTAN':'Gilgit-Baltistan',
        'AZAD JAMMU AND KASHMIR':'AJK','ISLAMABAD CAPITAL TERRITORY':'Islamabad'}
m['region'] = m['prv_desc'].map(PROV)
m['dkey'] = m['dist_desc'].map(dkey); m['tkey'] = m['teh_desc'].map(tkey)
V = {('B','primary'):('P4Q1111','P4Q1112'),('B','middle'):('P4Q1121','P4Q1122'),('B','high'):('P4Q1131','P4Q1132'),
     ('G','primary'):('P4Q1211','P4Q1212'),('G','middle'):('P4Q1221','P4Q1222'),('G','high'):('P4Q1231','P4Q1232')}
for (x,l),(y,km) in V.items():
    yes = m[y]; d = pd.to_numeric(m[km], errors='coerce')
    dist = pd.Series(np.where(yes=='1', 0.0, d), index=m.index)
    dist[(yes=='') | (dist > 200)] = np.nan
    m[f'has_{x}_{l}'] = (yes=='1').astype(int); m[f'km_{x}_{l}'] = dist

SL = [(x,l) for x in 'BG' for l in ('primary','middle','high')]

def build(mouza_keys, coll_keys, name_col, out_name, min_m=10):
    mm = m.groupby(mouza_keys).agg(region=('region','first'), mouza_name=(name_col, lambda v: ' + '.join(sorted(set(v)))),
                                   rural_mouzas=('dkey','size'), **{f'mouzas_with_{x}_{l}':(f'has_{x}_{l}','sum') for x,l in SL}).reset_index()
    cc = c.dropna(subset=coll_keys).groupby(coll_keys + ['sex','level']).size().unstack(['sex','level'], fill_value=0)
    cc.columns = [f'coll_{x}_{l}' for x,l in cc.columns]
    for x,l in SL:
        if f'coll_{x}_{l}' not in cc.columns: cc[f'coll_{x}_{l}'] = 0
    cc = cc.reset_index()
    hn = c.dropna(subset=coll_keys).groupby(coll_keys)['district' if len(coll_keys)==1 else 'tehsil'].first().rename('harvest_name').reset_index()
    cc = cc.merge(hn, on=coll_keys)
    t = mm.merge(cc, on=mouza_keys, how='outer', indicator=True)
    for col in [col for col in t.columns if col.startswith('coll_')]: t[col] = t[col].fillna(0).astype(int)
    t['status'] = t['_merge'].map({'both':'matched','left_only':'absent from harvest','right_only':'harvest only (no mouza rows)'})
    for x in 'BG':
        t[f'coll_midplus_{x}'] = t[f'coll_{x}_middle'] + t[f'coll_{x}_high']
        t[f'ratio_midplus_{x}'] = (t[f'coll_midplus_{x}'] / t[f'mouzas_with_{x}_middle'].replace(0,np.nan)).round(2)
        t[f'ratio_high_{x}'] = (t[f'coll_{x}_high'] / t[f'mouzas_with_{x}_high'].replace(0,np.nan)).round(2)
        t[f'ratio_primary_{x}'] = (t[f'coll_{x}_primary'] / t[f'mouzas_with_{x}_primary'].replace(0,np.nan)).round(2)
    t['girls_to_boys_ratio_midplus'] = (t['ratio_midplus_G'] / t['ratio_midplus_B']).round(2)
    def flag(r):
        if r['status'] != 'matched': return r['status']
        f = []
        for x, lab in (('B','boys'),('G','girls')):
            if r[f'mouzas_with_{x}_middle'] >= min_m and r[f'ratio_midplus_{x}'] < 0.5: f.append(f'{lab} middle-plus under half')
            if r[f'mouzas_with_{x}_primary'] >= min_m and r[f'ratio_primary_{x}'] < 0.5: f.append(f'{lab} primary under half')
        gb = r['girls_to_boys_ratio_midplus']
        if pd.notna(gb) and r['mouzas_with_G_middle'] >= min_m and gb < 0.6: f.append('girls undercounted relative to boys')
        return '; '.join(f)
    t['flag'] = t.apply(flag, axis=1)
    t = t.drop(columns=['_merge']).sort_values(['region'] + mouza_keys)
    t.to_csv(f'{OUT}/{out_name}', index=False)
    return t

A = build(['dkey'], ['dkey'], 'dist_desc', 'validation_A_counts_by_district.csv')
Am = A[A.status=='matched']
print('A. mouza districts:', (A.status!='harvest only (no mouza rows)').sum(), 'matched:', len(Am), 'flagged:', (Am.flag!='').sum(), 'absent:', (A.status=='absent from harvest').sum())
print(A.groupby('region').apply(lambda s: pd.Series({'mouza_districts':(s.status!='harvest only (no mouza rows)').sum(),'matched':(s.status=='matched').sum(),'flagged':((s.flag!='')&(s.status=='matched')).sum(),'absent':(s.status=='absent from harvest').sum()})).to_string())
prov = Am.groupby('region')[[c_ for c_ in Am.columns if c_.startswith(('coll_','mouzas_with_'))]].sum()
for x in 'BG':
    prov[f'ratio_midplus_{x}'] = (prov[f'coll_midplus_{x}']/prov[f'mouzas_with_{x}_middle']).round(2)
    prov[f'ratio_high_{x}'] = (prov[f'coll_{x}_high']/prov[f'mouzas_with_{x}_high']).round(2)
    prov[f'ratio_primary_{x}'] = (prov[f'coll_{x}_primary']/prov[f'mouzas_with_{x}_primary']).round(2)
prov[[c_ for c_ in prov.columns if c_.startswith('ratio')]].to_csv(f'{OUT}/validation_A_summary_by_region.csv')
print(prov[[c_ for c_ in prov.columns if c_.startswith('ratio')]].to_string())
print(A[(A.flag!='')&(A.status!='harvest only (no mouza rows)')][['region','mouza_name','ratio_primary_B','ratio_primary_G','ratio_midplus_B','ratio_midplus_G','flag']].to_string())

B = build(['dkey','tkey'], ['dkey','tkey'], 'teh_desc', 'validation_B_counts_by_tehsil.csv', min_m=5)
B = B[B.region.isin(['Sindh','Punjab','Balochistan','Gilgit-Baltistan'])]
print('\nB. tehsils:', (B.status!='harvest only (no mouza rows)').sum(), 'matched', (B.status=='matched').sum(), 'absent', (B.status=='absent from harvest').sum(), 'harvest-only', (B.status=='harvest only (no mouza rows)').sum())
print(B.groupby('region').apply(lambda s: pd.Series({'mouza_tehsils':(s.status!='harvest only (no mouza rows)').sum(),'matched':(s.status=='matched').sum(),'flagged':((s.flag!='')&(s.status=='matched')).sum(),'absent':(s.status=='absent from harvest').sum()})).to_string())
show = ['region','mouza_name','rural_mouzas','coll_B_primary','mouzas_with_B_primary','coll_midplus_B','mouzas_with_B_middle','coll_midplus_G','mouzas_with_G_middle','flag']
print(B[(B.status=='matched')&(B.flag!='')][show].to_string())
print(B[(B.status=='absent from harvest')&(B.rural_mouzas>=10)][['region','dkey','mouza_name','rural_mouzas','mouzas_with_B_middle','mouzas_with_G_middle']].to_string())

n = pd.read_csv(f'{D}/national_distance_stats.csv')
n = n[n.geography.str.lower() != n.region.str.lower()].copy()
n['dkey'] = n['geography'].map(dkey)
piv = n.pivot_table(index='dkey', columns=['sex','cls'], values=['median_km','share_over_5km'])
piv.columns = [f'model_{a_}_{x}_{c_}' for a_,x,c_ in piv.columns]; piv = piv.reset_index()
agg = {f'village_mean_km_{x}_{l}':(f'km_{x}_{l}','mean') for x,l in SL}
agg.update({f'village_median_km_{x}_{l}':(f'km_{x}_{l}','median') for x,l in SL})
mv = m.groupby('dkey').agg(region=('region','first'), mouza_district=('dist_desc','first'), rural_mouzas=('dkey','size'), **agg).reset_index()
for x,l in SL:
    mv[f'village_share_over_5km_{x}_{l}'] = m.groupby('dkey')[f'km_{x}_{l}'].apply(lambda v: (v>5).sum()/v.notna().sum() if v.notna().sum() else np.nan).values
C = mv.merge(piv, on='dkey', how='inner')
LV = {'primary':'primary_plus','middle':'middle_plus','high':'high_plus'}
for l, cls in LV.items():
    C[f'village_gap_km_{l}'] = C[f'village_mean_km_G_{l}'] - C[f'village_mean_km_B_{l}']
    C[f'model_gap_km_{l}'] = C[f'model_median_km_girls_{cls}'] - C[f'model_median_km_boys_{cls}']
    C[f'village_gap_share5_{l}'] = C[f'village_share_over_5km_G_{l}'] - C[f'village_share_over_5km_B_{l}']
    C[f'model_gap_share5_{l}'] = C[f'model_share_over_5km_girls_{cls}'] - C[f'model_share_over_5km_boys_{cls}']
C.round(3).to_csv(f'{OUT}/validation_C_distance_by_district.csv', index=False)
print('\nC. districts matched to model distance stats:', len(C))
def rep(s_, l, cls):
    a_ = s_.dropna(subset=[f'village_gap_km_{l}', f'model_gap_km_{l}'])
    if len(a_) < 5: return None
    return dict(n=len(a_),
        rank_corr_level_girls=round(a_[[f'village_median_km_G_{l}', f'model_median_km_girls_{cls}']].corr('spearman').iloc[0,1],2),
        rank_corr_share5_girls=round(a_[[f'village_share_over_5km_G_{l}', f'model_share_over_5km_girls_{cls}']].corr('spearman').iloc[0,1],2),
        rank_corr_gap=round(a_[[f'village_gap_km_{l}', f'model_gap_km_{l}']].corr('spearman').iloc[0,1],2),
        sign_agree_gap=round(((a_[f'village_gap_km_{l}']>0)==(a_[f'model_gap_km_{l}']>0)).mean(),2),
        rank_corr_gap_share5=round(a_[[f'village_gap_share5_{l}', f'model_gap_share5_{l}']].corr('spearman').iloc[0,1],2))
res = []
for l, cls in LV.items():
    r_ = rep(C, l, cls); r_.update(region='All', level=l); res.append(r_)
    for reg, s_ in C.groupby('region'):
        r_ = rep(s_, l, cls)
        if r_: r_.update(region=reg, level=l); res.append(r_)
R = pd.DataFrame(res)[['region','level','n','rank_corr_level_girls','rank_corr_share5_girls','rank_corr_gap','sign_agree_gap','rank_corr_gap_share5']]
R.to_csv(f'{OUT}/validation_C_summary.csv', index=False)
print(R.to_string())
