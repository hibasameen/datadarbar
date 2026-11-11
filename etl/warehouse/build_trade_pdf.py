import subprocess, re, os, glob, pandas as pd
from dd_paths import RAW, WORK
BASE=str(RAW/"trade_data_8digit"/"annual_by_commodity_country")+"/"
def num(t):
    t=t.strip()
    if t in ('-','','—'): return None
    try: return float(t.replace(',',''))
    except: return None
NUMTOK=re.compile(r'^-$|^-?[\d,]+(?:\.\d+)?$')
HS=re.compile(r'^(\d{7,8})(?=\D|$)')
def poptail(s):
    toks=s.split(); vals=[]
    while toks and NUMTOK.match(toks[-1]) and len(vals)<8:
        vals.insert(0,toks.pop())
    return ' '.join(toks).strip(), vals
def parse_pdf(path,direction):
    fy=re.search(r'(20\d\d)-(\d\d)',os.path.basename(path)); fy=f"{fy.group(1)}-{fy.group(2)}"
    txt=str(WORK/"_t.txt"); subprocess.run(["pdftotext","-layout",path,txt],check=True)
    rows=[]; hs=None; comm=None; pend=None; grand=None
    for l in open(txt,encoding="latin-1"):
        s=l.rstrip()
        if not s.strip(): continue
        U=s.upper()
        if 'GRAND TOTAL' in U:
            gn=[t for t in s.split() if NUMTOK.match(t) and t!='-']
            if len(gn)>=2: grand=num(gn[1])
            continue
        if 'HS CODE' in U or 'QUANTITY' in U or 'COMMODITES' in U or 'RUPEES IN THOUS' in U or 'CUMULATIVE' in U or U.strip().startswith('PAGE'): continue
        label,vals=poptail(s)
        m=HS.match(label)
        def emit(country,vv):
            rows.append([direction,fy,hs,comm,country,'',num(vv[0]),num(vv[1]),num(vv[2]),num(vv[3])])
        if m:
            hs=m.group(1).zfill(8); comm=label[m.end():].strip(); pend=None
            if len(vals)>=8: emit(None,vals)
            else: pend=True
        else:
            if pend and len(vals)>=8:
                emit(None,vals); pend=None      # wrapped commodity-total line (label = unit)
            elif len(vals)>=8 and hs is not None:
                emit(label,vals)                # country line
    return rows,fy,grand
cols=['direction','fiscal_year','hs8','commodity','country','unit','month_qty','month_value_kpkr','fy_qty','fy_value_kpkr']
allr=[]
for d,folder in [('export','Export'),('import','Import')]:
    for path in sorted(glob.glob(BASE+folder+"/*.pdf")):
        if not re.search(r'202[123]-2[234]',os.path.basename(path)): continue
        r,fy,grand=parse_pdf(path,d)
        df=pd.DataFrame(r,columns=cols); df=df[df.hs8.str.match(r'^\d{8}$')]
        mysum=df[df.country.isna()].fy_value_kpkr.sum()/1e6; gt=grand/1e6 if grand else 0
        allr+=[row for row in r if re.match(r'^\d{8}$',row[2])]
        print(f"{os.path.basename(path):22s} {d:6s} {fy} hs8={df.hs8.nunique():5d} mysum=Rs {mysum:6,.0f}bn grand=Rs {gt:6,.0f}bn match={100*mysum/gt:.1f}%")
pdf=pd.DataFrame(allr,columns=cols); pdf.to_parquet(WORK/"trade_pdf_2124.parquet",index=False)
print("TOTAL pdf rows:",len(pdf))
