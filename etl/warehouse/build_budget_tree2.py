import subprocess, re, os, glob, json
from dd_paths import RAW, WORK, ECON
BASE=str(RAW/"budget_documents")+"/"
OUT=str(ECON)
TXT=str(WORK/"_b.txt")
NUM=re.compile(r'^\(?-?[\d,]+(?:\.\d+)?\)?$')
def nums(line):
    toks=line.split();v=[]
    while toks and NUM.match(toks[-1]) and any(c.isdigit() for c in toks[-1]):
        v.insert(0,toks.pop())
    return ' '.join(toks).strip(), v
def val(v):  # own-year budget = last numeric col
    if not v: return None
    t=v[-1].strip().replace(',','').strip('()')
    try: return float(t)
    except: return None
def table_lines(txt, tno):
    m=re.search(rf'TABLE\s*-\s*{tno}\b', txt)
    if not m: return []
    seg=txt[m.end():]
    nxt=re.search(r'TABLE\s*-\s*\d+', seg)
    if nxt: seg=seg[:nxt.start()]
    return seg.split('\n')
def parse_year(fy):
    p=f"{BASE}{fy}/Budget_in_Brief.pdf"
    if not os.path.exists(p): return None,None
    subprocess.run(["pdftotext","-layout",p,TXT],check=True)
    txt=open(TXT,encoding="latin-1").read()
    # ---- EXPENDITURE: Table 10, heads (i)-(vii) ----
    exp=[];cur=None
    ROM=re.compile(r'^\((?:i|ii|iii|iv|v|vi|vii|viii)\)\s*(.+)',re.I)
    for ln in table_lines(txt,10):
        s=ln.strip()
        if not s or 'Classification' in s or 'Rs in Million' in s or 'Budget' in s and 'Revised' in s: continue
        lab,v=nums(s); vv=val(v)
        if vv is None or vv<=0: continue
        mh=ROM.match(lab)
        if mh:
            name=mh.group(1).strip().rstrip('*').strip()
            if 'CURRENT EXPENDITURE' in name.upper(): continue
            cur={"label":name,"children":[]}; cur["_tot"]=vv; exp.append(cur)
        elif lab.startswith('-') and cur is not None:
            cur["children"].append({"name":lab.lstrip('- ').strip(),"bn":round(vv/1000,2)})
    # fill: groups w/o children => single leaf = group total
    for g in exp:
        if not g["children"]: g["children"]=[{"name":g["label"],"bn":round(g["_tot"]/1000,2)}]
        del g["_tot"]
    # ---- RECEIPTS: Table 4 (tax) + Table 5 (non-tax) ----
    rec=[]
    # tax
    tcur=None
    TAXH=re.compile(r'^(I{1,3}\.)\s*(.+)')
    for ln in table_lines(txt,4):
        s=ln.strip()
        if not s or 'Classification' in s: continue
        lab,v=nums(s); vv=val(v)
        if vv is None or vv<=0: continue
        mh=TAXH.match(lab)
        if mh:
            nm=mh.group(2).strip(); tcur={"label":nm,"children":[]}; rec.append(tcur)
        elif lab.startswith('-') and tcur is not None:
            tcur["children"].append({"name":lab.lstrip('- ').strip(),"bn":round(vv/1000,2)})
    # non-tax sections A/B/C/D
    ncur=None
    SEC=re.compile(r'^([A-D])\.\s+(.+)')
    for ln in table_lines(txt,5):
        s=ln.strip()
        if not s or 'Classification' in s: continue
        lab,v=nums(s); vv=val(v)
        if vv is None or vv<=0: continue
        mh=SEC.match(lab)
        if mh:
            nm=mh.group(2).strip().title(); ncur={"label":nm,"children":[]}; rec.append(ncur)
        elif lab.startswith('-') and ncur is not None:
            ncur["children"].append({"name":lab.lstrip('- ').strip(),"bn":round(vv/1000,2)})
    rec=[g for g in rec if g["children"]]
    return rec,exp

LBLFIX={"Provision for Emergency and":"Provision for Emergency & Others","Income From Property &":"Income from Property & Enterprise","Receipts From Civil":"Receipts from Civil Admin.","Grants and Transfers":"Grants & Transfers","Mark-up Payments":"Debt servicing (mark-up)"}
def fixlbl(x):
    x=x.strip()
    x=re.sub(r'\s+(and|&|from|of)$','',x,flags=re.I).strip()
    return LBLFIX.get(x, x)

PAL=["#1e6b3e","#c0392b","#3d6db5","#e07b39","#9b59b6","#16a085","#5b8c5a","#d4a017","#a0522d","#5d6d7e"]
def colorize(groups):
    for i,g in enumerate(groups):
        g["label"]=fixlbl(g["label"]); g["color"]=PAL[i%len(PAL)]; g["key"]=g["label"]
    return groups
# Older Budget-in-Brief PDFs number their tables differently, so "Table 4/5"
# can land on a Current-vs-Development *expenditure* summary. Only accept a
# receipts year whose groups actually look like receipts (tax / non-tax heads).
RECEIPT_OK=re.compile(r'tax|revenue|levies|receipts|non-?tax|customs|income from property|miscellaneous|administration|fees|petroleum|surcharge|gas',re.I)
tree={"receipts":{},"expenditure":{}}
for p in sorted(glob.glob(BASE+"*/Budget_in_Brief.pdf")):
    fy=os.path.basename(os.path.dirname(p))
    rec,exp=parse_year(fy)
    if exp and sum(sum(c["bn"] for c in g["children"]) for g in exp)>1000:
        tree["expenditure"][fy]=colorize(exp)
    if rec and any(RECEIPT_OK.search(g["label"]) for g in rec) \
       and sum(sum(c["bn"] for c in g["children"]) for g in rec)>1000:
        tree["receipts"][fy]=colorize(rec)
    elif rec:
        print(f"  skipped {fy} receipts (no tax/non-tax heads — likely wrong table in old-format PDF)")
json.dump(tree,open(f"{OUT}/budget_tree.json","w"))
print("expenditure years:",sorted(tree["expenditure"].keys()))
print("receipts years:",sorted(tree["receipts"].keys()))
fy='2025-26'
if fy in tree["expenditure"]:
    print(f"\n{fy} EXPENDITURE groups:")
    for g in tree["expenditure"][fy]: print(f"  {g['label']}: {[(c['name'][:28],c['bn']) for c in g['children']]}")
    print(f"  total = Rs {sum(sum(c['bn'] for c in g['children']) for g in tree['expenditure'][fy]):,.0f} bn")
if fy in tree["receipts"]:
    print(f"\n{fy} RECEIPTS groups:")
    for g in tree["receipts"][fy]: print(f"  {g['label']}: {len(g['children'])} items, Rs {sum(c['bn'] for c in g['children']):,.0f} bn")
