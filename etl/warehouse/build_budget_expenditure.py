import re,glob,os,json
from dd_paths import BUD
BUD=str(BUD)
NUM=re.compile(r'-?[\d,]{3,}(?:\.\d+)?')
def tonum(t):
    t=t.replace(',','')
    try:return float(t)
    except:return None
def lastnum(l):
    n=NUM.findall(l); return tonum(n[-1]) if n else None
def dz(s): return re.sub(r'[^a-z]','',s.lower())
FUNC=[('General Public Services','generalpublicservice'),
 ('Defence Affairs & Services','defenceaffair'),
 ('Public Order & Safety','publicorderandsaf'),
 ('Economic Affairs','economicaffair'),
 ('Environment Protection','environmentprotect'),
 ('Housing & Community Amenities','housingandcommunityamen'),
 ('Health Affairs & Services','healthaffair'),
 ('Recreation, Culture & Religion','recreationculture'),
 ('Education Affairs & Services','educationaffair'),
 ('Social Protection','socialprotection')]
KEYS=[k for _,k in FUNC]
def find_table(L):
    """return line-index range of the functional current-expenditure data table"""
    best=None
    for i,l in enumerate(L):
        if 'generalpublicservice' in dz(l):
            if len(NUM.findall(l))<3: continue      # need 3-col table (BE prev, RE prev, BE own)
            lv=lastnum(l)
            if not lv or lv<100: continue           # skip TOC/prose
            # count distinct func keys with data-values in next 16 lines
            hits=0; seen=set()
            for j in range(i,min(i+18,len(L))):
                d=dz(L[j]); v=lastnum(L[j])
                if len(NUM.findall(L[j]))<3: continue
                for k in KEYS:
                    if k in d and v and v>=100 and k not in seen: seen.add(k)
            if len(seen)>=6:
                return i, min(i+22,len(L))
    return None
def parse(path):
    L=[l.rstrip('\n') for l in open(path,encoding='latin-1')]
    rng=find_table(L)
    if not rng: return None
    a,b=rng
    vals={}
    for j in range(a,b):
        d=dz(L[j]); 
        if len(NUM.findall(L[j]))<3: continue
        v=lastnum(L[j])
        if v is None or v<50: continue
        for name,k in FUNC:
            if k in d and name not in vals:
                vals[name]=v
    # total current: a TOTAL line in/after window
    tot=None
    for j in range(a,min(b+3,len(L))):
        if re.match(r'\s*TOTAL',L[j],re.I) or 'totalcurrent' in dz(L[j]):
            tv=lastnum(L[j])
            if tv and tv>sum(vals.values())*0.8: tot=tv; break
    return vals,tot
print(f"{'year':8}{'#fn':>4}{'sumFn bn':>10}{'totCur bn':>10}  reconcile")
allv={}
for f in sorted(glob.glob(BUD+'/bib_*.txt')):
    y=os.path.basename(f)[4:-4]
    r=parse(f)
    if not r: print(f"{y:8}  NO TABLE FOUND"); continue
    vals,tot=r
    s=sum(vals.values())
    d=(s-tot)/tot*100 if tot else None
    allv[y]={'funcs':{k:round(v/1000,2) for k,v in vals.items()},'total_bn':round((tot or s)/1000,2)}
    st='OK' if tot and abs(d)<2 else (f'Δ{d:+.0f}%' if tot else 'no total')
    print(f"{y:8}{len(vals):4d}{s/1000:10.1f}{(tot or 0)/1000:10.1f}  {st}")
json.dump(allv,open(BUD+'/exp_funcs.json','w'))

# ---- extend: grab debt servicing & pensions, build finance.js group format ----
def grab_debt(L):
    for l in L:
        d=dz(l)
        if 'markuppayment' in d and len(NUM.findall(l))>=3: return tonum(NUM.findall(l)[-1])
    dom=fo=None
    for l in L:
        if len(NUM.findall(l))<3: continue
        d=dz(l)
        if 'servicingofdomesticdebt' in d and dom is None: dom=tonum(NUM.findall(l)[-1])
        if 'servicingofforeigndebt' in d and fo is None: fo=tonum(NUM.findall(l)[-1])
    return (dom or 0)+(fo or 0) if (dom or fo) else None
def grab_pension(L):
    for l in L:
        if len(NUM.findall(l))<3: continue
        if 'superannuation' in dz(l): return tonum(NUM.findall(l)[-1])
    for l in L:
        if len(NUM.findall(l))<3: continue
        if re.match(r'\s*\(ii\)\s*pension',l,re.I): return tonum(NUM.findall(l)[-1])
    return None

COL={'General Public Services':'#c0392b','Defence Affairs & Services':'#1e6b3e','Public Order & Safety':'#5d6d7e',
 'Economic Affairs':'#d4a017','Environment Protection':'#16a085','Housing & Community Amenities':'#8e7cc3',
 'Health Affairs & Services':'#e07b39','Recreation, Culture & Religion':'#a0522d','Education Affairs & Services':'#3d6db5',
 'Social Protection':'#9b59b6'}
def bn(x): return round(x/1000.0,2)
out={}
for f in sorted(glob.glob(BUD+'/bib_*.txt')):
    y=os.path.basename(f)[4:-4]
    L=[l.rstrip('\n') for l in open(f,encoding='latin-1')]
    r=parse(f)
    if not r: continue
    vals,tot=r
    debt=grab_debt(L); pens=grab_pension(L)
    groups=[]
    for name,_ in FUNC:
        if name not in vals: continue
        gv=bn(vals[name])
        if name=='General Public Services':
            kids=[]
            db=bn(debt) if debt else 0; pn=bn(pens) if pens else 0
            db=min(db,gv); pn=min(pn,gv-db)
            if db>0.5: kids.append({'name':'Debt servicing (mark-up)','bn':db})
            if pn>0.5: kids.append({'name':'Pensions','bn':pn})
            other=round(gv-sum(k['bn'] for k in kids),2)
            if other>0.5: kids.append({'name':'Other general public services','bn':other})
            children=kids or [{'name':name,'bn':gv}]
        else:
            children=[{'name':name,'bn':gv}]
        groups.append({'label':name,'key':name,'color':COL[name],'children':children})
    out[y]=groups
json.dump(out,open(BUD+'/exp_groups.json','w'))
# validation print
print("\n--- final tree (Rs bn) ---")
for y in ['2009-10','2015-16','2024-25']:
    g=out[y]; t=sum(sum(c['bn'] for c in x['children']) for x in g)
    gps=[x for x in g if x['label']=='General Public Services'][0]
    print(f"{y}: total {t:.0f} bn | {len(g)} funcs | GPS {sum(c['bn'] for c in gps['children']):.0f} = "+", ".join(f"{c['name'].split()[0]} {c['bn']:.0f}" for c in gps['children']))
