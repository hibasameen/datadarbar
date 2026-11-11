import re,glob,os,json
from dd_paths import BUD
BUD=str(BUD)
NUM=re.compile(r'-?[\d,]+\.\d{1,3}|-?[\d,]{4,}')
def tonum(t):
    t=t.replace(',','');neg=t.startswith('(');t=t.strip('()')
    try:return (-1 if neg else 1)*float(t)
    except:return None
def own(line):
    n=NUM.findall(line)
    return tonum(n[-1]) if n else None
def code_of(line):
    m=re.match(r'^\s*([A-G]\d[\dA-Z,]*(?:-\d+)?)\b',line)
    return m.group(1).replace(',','') if m else None

def extract(path):
    L=[l.rstrip('\n') for l in open(path,encoding='latin-1')]
    # start at first TAX REVENUE control (own>=50000, not NON)
    start=0
    for i,l in enumerate(L):
        if re.search(r'\bTAX\s+REVENUE\b',l,re.I) and not re.search(r'NON',l,re.I):
            v=own(l)
            if v and v>=50000: start=i; break
    win=L[start:]   # whole doc after start; first-occurrence of each specific code/name is safe
    T={}
    def setfirst(key,pred):
        if key in T: return
        for l in win:
            if pred(l):
                v=own(l)
                if v is not None: T[key]=v; return
    c=lambda l: code_of(l)
    setfirst('tax', lambda l: re.search(r'\bTAX\s+REVENUE\b',l,re.I) and not re.search(r'NON',l,re.I) and (own(l) or 0)>=50000)
    setfirst('direct', lambda l: (c(l)=='B01') or re.search(r'\bDirect\s+Tax',l,re.I))
    setfirst('indirect', lambda l: (c(l)=='B02') or re.search(r'\bIndirect\s+Tax',l,re.I))
    setfirst('income', lambda l: (c(l)=='B011') or re.search(r'Taxes on Income|Income Tax\b',l,re.I))
    setfirst('customs', lambda l: (c(l) or '').startswith('B020') or re.search(r'\bCustoms\b',l,re.I))
    setfirst('sales', lambda l: (c(l)=='B023') or re.search(r'\bSales Tax\b',l,re.I))
    setfirst('fed', lambda l: (c(l) or '').startswith('B024') or re.search(r'Federal Excise',l,re.I))
    # non-tax
    setfirst('nontax', lambda l: re.search(r'NON.?TAX\s+REVENUE',l,re.I) and (own(l) or 0)>=20000)
    setfirst('c01', lambda l: (c(l)=='C01') or re.search(r'Income from Property',l,re.I))
    setfirst('c02', lambda l: (c(l)=='C02') or re.search(r'Receipts from Civil|Civil Administration',l,re.I))
    setfirst('c03', lambda l: (c(l)=='C03') or re.search(r'Miscellaneous Receipts',l,re.I))
    setfirst('levies', lambda l: re.search(r'Levies (and|&) Fees',l,re.I))
    return T

def bn(x): return round(x/1000.0,2)
def node(name,total,children=None):
    n={'name':name,'bn':round(total,2)}
    if children: n['children']=[c for c in children if c and c['bn']>0.01]
    return n

def build(T):
    g=lambda k: T.get(k)
    D=g('direct'); I=g('indirect'); TAX=g('tax') or ((D or 0)+(I or 0))
    # tax children
    taxkids=[]
    if D is not None:
        it=g('income'); it=min(it,D) if it else None
        dk=[node('Income Tax',bn(it))] if it else []
        oth=bn(D)-sum(x['bn'] for x in dk)
        if oth>0.05 and dk: dk.append(node('Other direct taxes',oth))
        taxkids.append(node('Direct Taxes',bn(D),dk or None))
    if I is not None:
        parts=[]
        for k,nm in [('customs','Customs Duty'),('sales','Sales Tax'),('fed','Federal Excise Duty')]:
            if g(k): parts.append(node(nm,bn(g(k))))
        used=sum(x['bn'] for x in parts)
        oth=bn(I)-used
        if oth>0.05 and parts: parts.append(node('Other indirect',oth))
        taxkids.append(node('Indirect Taxes',bn(I),parts or None))
    # other taxes/levies = TAX - D - I
    otax=bn(TAX)-(bn(D) if D else 0)-(bn(I) if I else 0)
    if otax>0.5: taxkids.append(node('Other Taxes & Levies (Petroleum Levy, GIDC, etc.)',otax))
    tax_node=node('Tax Revenue',bn(TAX),taxkids)
    # non-tax
    N=g('nontax')
    ntkids=[]
    for k,nm in [('c01','Income from Property & Enterprise'),('c02','Receipts from Civil Administration'),('c03','Miscellaneous Receipts'),('levies','Levies & Fees')]:
        if g(k): ntkids.append(node(nm,bn(g(k))))
    ntsum=sum(x['bn'] for x in ntkids)
    NT=N if N else ntsum*1000
    oth=bn(NT)-ntsum
    if oth>0.5 and ntkids: ntkids.append(node('Other non-tax receipts',oth))
    nontax_node=node('Non-Tax Revenue',bn(NT),ntkids)
    total=tax_node['bn']+nontax_node['bn']
    return node('Federal Receipts (Revenue)',total,[tax_node,nontax_node]),tax_node['bn'],nontax_node['bn']

print(f"{'year':8}{'TAX bn':>9}{'NONTAX bn':>10}{'TOTAL bn':>10}  depth/leaves")
allyears={}
for f in sorted(glob.glob(BUD+'/em_*.txt')):
    y=os.path.basename(f)[3:-4]
    T=extract(f)
    tree,tax,nt=build(T)
    # count leaves
    def leaves(n): return 1 if 'children' not in n else sum(leaves(c) for c in n['children'])
    allyears[y]=tree
    print(f"{y:8}{tax:9.0f}{nt:10.0f}{tax+nt:10.0f}   leaves={leaves(tree)}")
json.dump(allyears,open(BUD+'/receipts_tree.json','w'))
print("\nsaved receipts_tree.json")
