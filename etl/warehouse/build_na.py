import re, argparse, openpyxl, pandas as pd
from dd_paths import WORK
# usage: python3 -I build_na.py WORKBOOK.xlsx [--out na_annual.parquet]
#   WORKBOOK = $DD_RAW/national_accounts/annual_tables_2015-16_base/National_Accounts_Annual_Tables_*.xlsx
#   (2026-10-24: Sep-2026 workbook, 118th NAC; was the May-2026 one)
ap=argparse.ArgumentParser(description="walk every sheet of the PBS annual-tables workbook into long rows")
ap.add_argument("xlsx"); ap.add_argument("--out",default=str(WORK/"na_annual.parquet"))
A=ap.parse_args()
P=A.xlsx
YR=re.compile(r'^(19|20)\d\d[-/](?:19|20)?\d\d$')
def norm_year(s):
    s=str(s).strip().replace('/','-')
    m=re.match(r'^((?:19|20)\d\d)-((?:19|20)?\d\d)$',s)
    if not m: return None
    a,b=m.group(1),m.group(2); b=b[-2:]
    return f"{a}-{b}"
def is_year(v): return v is not None and YR.match(str(v).strip())
def num(v):
    try: return float(str(v).replace(',','').strip())
    except: return None
wb=openpyxl.load_workbook(P,read_only=True,data_only=True)
out=[]; report=[]
for sn in wb.sheetnames:
    ws=wb[sn]; grid=[list(r) for r in ws.iter_rows(values_only=True)]
    title=''
    for r in grid[:3]:
        for c in r:
            if c and len(str(c).strip())>4: title=str(c).strip(); break
        if title: break
    # find header row with >=3 year tokens (years-as-columns)
    hdr=None
    for i,r in enumerate(grid):
        yc=sum(1 for c in r if is_year(c))
        if yc>=3: hdr=i; break
    n0=len(out)
    if hdr is not None:
        year_cols={j:norm_year(c) for j,c in enumerate(grid[hdr]) if is_year(c)}
        textcols=[j for j in range(min(3,len(grid[hdr]))) if j not in year_cols]
        for r in grid[hdr+1:]:
            label=' '.join(str(r[j]).strip() for j in textcols if j<len(r) and r[j] not in (None,'')).strip()
            if not label: continue
            for j,yr in year_cols.items():
                if j<len(r):
                    v=num(r[j])
                    if v is not None and yr: out.append([sn,title,label,yr,v])
    else:
        # years-as-rows: find column with many years
        col_years={}
        for j in range(max(len(r) for r in grid)):
            ys=[(i,norm_year(r[j])) for i,r in enumerate(grid) if j<len(r) and is_year(r[j])]
            if len(ys)>=5: col_years[j]=ys; 
        if col_years:
            yj=min(col_years); yrows=dict(col_years[yj])
            # header labels: last non-empty row above first data year in each col
            first_data=col_years[yj][0][0]
            hdrs={}
            for j in range(max(len(r) for r in grid)):
                if j==yj: continue
                lab=''
                for i in range(first_data):
                    if j<len(grid[i]) and grid[i][j] not in (None,''):
                        t=str(grid[i][j]).strip()
                        if not YR.match(t): lab=t
                hdrs[j]=lab or f"col{j}"
            for i,r in enumerate(grid):
                yr=yrows.get(i)
                if not yr: continue
                for j,lab in hdrs.items():
                    if j<len(r):
                        v=num(r[j])
                        if v is not None: out.append([sn,title,lab,yr,v])
    report.append((sn,title[:45],'cols' if hdr is not None else 'rows',len(out)-n0))
wb.close()
df=pd.DataFrame(out,columns=['table_sheet','table_title','item','year','value'])
df.to_parquet(A.out,index=False)
for s,t,o,n in report: print(f"{s:10s} {o:4s} {n:6d}  {t}")
print("\nTOTAL na rows:",len(df),"| sheets:",df.table_sheet.nunique(),"| years:",df.year.nunique(),df.year.min(),"->",df.year.max())
