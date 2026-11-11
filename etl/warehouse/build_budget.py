import pdfplumber, re, os, glob, pandas as pd
from dd_paths import RAW, WORK
NUM=re.compile(r'\(?-?\d[\d,]*(?:\.\d+)?\)?$')
YRRE=re.compile(r'(?:19|20)\d\d[-–]\d{2,4}')
def tonum(t):
    t=t.strip()
    if t in ('-','–','','—'): return None
    neg=t.startswith('(') and t.endswith(')')
    t=t.strip('()').replace(',','')
    try: v=float(t); return -v if neg else v
    except: return None
def split_label_nums(line):
    toks=line.split(); i=len(toks); nums=[]
    while i>0 and NUM.match(toks[i-1]) and any(c.isdigit() for c in toks[i-1]):
        nums.insert(0,toks[i-1]); i-=1
    return ' '.join(toks[:i]).strip(' .:-'), nums
def extract(path):
    fy=os.path.basename(os.path.dirname(path))
    pdf=pdfplumber.open(path); rows=[]
    tbl=None; title=None; collabels=[]; ptypes=[]; pyears=[]
    for pg in pdf.pages:
        for ln in (pg.extract_text() or "").split("\n"):
            s=ln.strip()
            mt=re.match(r'(?:TABLE|Table)\s*[-–]\s*(\d+)', s)
            if mt:
                tbl=int(mt.group(1)); title=None; collabels=[]; ptypes=[]; pyears=[]; continue
            if tbl and title is None and s and not s.startswith('(Rs') and 'presents' not in s.lower() and 'shows' not in s.lower() and (not any(c.isdigit() for c in s) or YRRE.search(s)):
                title=s[:80]
            for w in s.split():
                if w=='Budget': ptypes.append('BE')
                elif w=='Revised': ptypes.append('RE')
            for y in YRRE.findall(s): pyears.append(y.replace('–','-'))
            if ptypes and pyears and len(pyears)>=len(ptypes) and not collabels:
                collabels=[f"{a} {b}" for a,b in zip(ptypes,pyears[:len(ptypes)])]
            label,nums=split_label_nums(s)
            if tbl and label and nums and len(label)>1 and not label.replace('.','').isdigit():
                n=len(nums)
                for k,tk in enumerate(nums):
                    v=tonum(tk)
                    rows.append([fy,tbl,title,label,k,n,(collabels[k] if k<len(collabels) else None),v,k==n-1])
    pdf.close(); return rows
base=str(RAW/"budget_documents")+"/"
cols=['doc_fy','table_no','table_title','item','col_index','n_cols','col_label','value_rs_mn','is_own_year_be']
allr=[]
for p in sorted(glob.glob(base+"*/Budget_in_Brief.pdf")):
    r=extract(p); allr+=r
    fy=os.path.basename(os.path.dirname(p))
    df1=pd.DataFrame(r,columns=cols)
    print(f"{fy}: rows={len(df1):5d} tables={df1.table_no.nunique():3d} own_yr_be_rows={int(df1.is_own_year_be.sum())}")
df=pd.DataFrame(allr,columns=cols)
df.to_parquet(WORK/"budget_lines.parquet",index=False)
print("\nTOTAL budget rows:",len(df)," | years:",df.doc_fy.nunique(),df.doc_fy.min(),"->",df.doc_fy.max())
# quick sanity: FBR taxes own-year BE across years
fbr=df[(df.item.str.contains("FBR",case=False,na=False))&(df.is_own_year_be)][['doc_fy','item','value_rs_mn']]
print("\nFBR line own-year BE sample:"); print(fbr.drop_duplicates('doc_fy').head(20).to_string(index=False))
