import re, glob, os, openpyxl, pandas as pd
from dd_paths import RAW, WORK
BASE=str(RAW/"trade_data_8digit")+"/"
def num(t):
    if t is None: return None
    t=str(t).strip().replace(',','')
    if t in ('--','','-','nan'): return None
    try: return float(t)
    except: return None
def parse_txt(path, direction):
    lines=open(path,encoding='latin-1').read().splitlines()
    fy=None
    for l in lines[:8]:
        m=re.search(r'(\d)\s*(\d)\s*(\d)\s*(\d)\s*-\s*(\d)\s*(\d)\s*(\d)\s*(\d)', l)
        if m: g=m.groups(); fy=f"{''.join(g[:4])}-{''.join(g[4:])[2:]}"; break
    rows=[]; hs=None; comm=None
    for l in lines:
        if len(l)<50: continue
        u=l.upper()
        if 'GRAND' in u or 'HSCODE' in l or 'QUANTITY' in l or 'PAGE' in u or 'COMMODITIES AND' in u: continue
        code=l[0:8].strip(); tail=l[44:].split()
        if len(tail)<4: continue
        v=[num(x) for x in tail[:4]]
        if code.isdigit() and len(code)>=6:
            hs=code.zfill(8); comm=l[9:38].strip(); unit=l[38:44].strip()
            rows.append([direction,fy,hs,comm,None,unit,*v])
        else:
            ctry=l[9:38].strip()
            if ctry and hs: rows.append([direction,fy,hs,comm,ctry,'',*v])
    return rows
def parse_xlsx(path, direction, fy):
    wb=openpyxl.load_workbook(path, read_only=True); ws=wb[wb.sheetnames[0]]
    rows=[]; hs=None; comm=None; started=False
    for row in ws.iter_rows(min_col=1,max_col=7,values_only=True):
        c0,c1,c2,c3,c4,c5,c6=row
        if c1 and 'GRAND TOTAL' in str(c1).upper(): started=True; continue
        if not started: continue
        code=str(c0).strip() if c0 is not None else ''
        code=re.sub(r'\.0$','',code)
        if code.isdigit():
            hs=code.zfill(8); comm=(str(c1).strip() if c1 else ''); unit=(str(c2).strip() if c2 else '')
            rows.append([direction,fy,hs,comm,None,unit,num(c3),num(c4),num(c5),num(c6)])
        elif c1 and hs:
            rows.append([direction,fy,hs,comm,str(c1).strip(),'',num(c3),num(c4),num(c5),num(c6)])
    wb.close(); return rows

jobs=[]
for p in glob.glob(BASE+"annual_by_commodity_country/Export/*.txt"): jobs.append(('txt',p,'export',None))
for p in glob.glob(BASE+"annual_by_commodity_country/Import/*.txt"): jobs.append(('txt',p,'import',None))
jobs.append(('xlsx',BASE+"monthly/Import/Import_2024-06.xlsx",'import','2024-25'))
jobs.append(('xlsx',BASE+"monthly/Export/Export_2024-06.xlsx",'export','2024-25'))
cols=['direction','fiscal_year','hs8','commodity','country','unit','month_qty','month_value_kpkr','fy_qty','fy_value_kpkr']
allrows=[]; src=[]
for kind,path,direction,fy in jobs:
    r=parse_txt(path,direction) if kind=='txt' else parse_xlsx(path,direction,fy)
    fyv=r[0][1] if r else fy
    allrows+= [row+[os.path.basename(path)] for row in r]
    print(f"{os.path.basename(path):40s} dir={direction} fy={fyv} rows={len(r)}")
df=pd.DataFrame(allrows,columns=cols+['source_file'])
# hs8 numeric strings only, drop junk
df=df[df.hs8.str.match(r'^\d{8}$')]
df.to_parquet(WORK/"trade_hs8.parquet",index=False)
print("\nTOTAL rows:",len(df))
print("by direction x fy (country rows only):")
print(df[df.country.notna()].groupby(['direction','fiscal_year']).size())
print("unique hs8:",df.hs8.nunique(),"| unique countries:",df.country.nunique())
