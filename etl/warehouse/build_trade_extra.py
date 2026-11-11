#!/usr/bin/env python3
"""Extra Trade Atlas data: per-country detail + product/chapter/section 'movers'.

Reads trade_hs8 from the warehouse. Values fy_value_kpkr are THOUSAND Rs, so
bn = kpkr / 1e6.

Outputs econ_data/trade_extra.json with:
  countries: sorted list of {code(name), exp_latest, imp_latest} for the sidebar selector
  country: { NAME: { series:{year:{exp,imp}}, level:{ section|chapter|product: {exp:[{code,name,bn}], imp:[...] } } } }
           (level breakdowns are for the latest year of each direction)
  movers:  { export|import: { section|chapter|product: [ {code,name,v0,v1,delta,pct,y0,y1} ] } }
           full-span first->last available year; product level keeps a bounded union of
           biggest absolute movers and biggest % movers (above a value floor).
  meta
"""
import json, os, re
import duckdb
import pandas as pd

from dd_paths import WAREHOUSE
WH = str(WAREHOUSE)
OUT = os.path.join(WH, 'econ_data'); os.makedirs(OUT, exist_ok=True)

SECTIONS=[("I","Animals & products",range(1,6)),("II","Vegetables",range(6,15)),("III","Fats & oils",range(15,16)),
("IV","Food, bev, tobacco",range(16,25)),("V","Minerals & fuels",range(25,28)),("VI","Chemicals",range(28,39)),
("VII","Plastics & rubber",range(39,41)),("VIII","Hides & leather",range(41,44)),("IX","Wood",range(44,47)),
("X","Paper & pulp",range(47,50)),("XI","Textiles",range(50,64)),("XII","Footwear & headgear",range(64,68)),
("XIII","Stone, cement, glass",range(68,71)),("XIV","Gems & precious metal",range(71,72)),("XV","Base metals",range(72,84)),
("XVI","Machinery & electrical",range(84,86)),("XVII","Transport equip.",range(86,90)),("XVIII","Instruments",range(90,93)),
("XIX","Arms",range(93,94)),("XX","Misc. manufactures",range(94,97)),("XXI","Art & special",range(97,100))]
ch2sec={}; secname={}
for code,name,rng in SECTIONS:
    for ch in rng: ch2sec[ch]=code
    secname[code]=name
CH_NAMES={  # full HS 2-digit chapter labels (readable, abbreviated)
 '01':'Live animals','02':'Meat','03':'Fish & seafood','04':'Dairy, eggs & honey','05':'Other animal products',
 '06':'Live plants & flowers','07':'Vegetables','08':'Fruit & nuts','09':'Coffee, tea & spices','10':'Cereals (rice, wheat)',
 '11':'Milling products (flour, starch)','12':'Oil seeds','13':'Gums & resins','14':'Vegetable plaiting materials','15':'Fats & oils',
 '16':'Meat & fish preparations','17':'Sugar & confectionery','18':'Cocoa','19':'Cereal & flour preparations','20':'Fruit & vegetable preparations',
 '21':'Misc edible preparations','22':'Beverages & spirits','23':'Animal feed & food residues','24':'Tobacco','25':'Salt, sulphur & cement',
 '26':'Ores, slag & ash','27':'Mineral fuels & oils','28':'Inorganic chemicals','29':'Organic chemicals','30':'Pharmaceuticals',
 '31':'Fertilisers','32':'Dyes, paints & tanning','33':'Perfumery & cosmetics','34':'Soap & waxes','35':'Glues & enzymes',
 '36':'Explosives & matches','37':'Photographic goods','38':'Misc chemical products','39':'Plastics','40':'Rubber',
 '41':'Raw hides & leather','42':'Leather goods','43':'Furskins','44':'Wood','45':'Cork',
 '46':'Straw & basketware','47':'Wood pulp','48':'Paper & paperboard','49':'Printed books & media','50':'Silk',
 '51':'Wool','52':'Cotton','53':'Other vegetable fibres','54':'Man-made filaments','55':'Man-made staple fibres',
 '56':'Wadding, felt & nonwovens','57':'Carpets','58':'Special woven fabrics','59':'Coated & technical textiles','60':'Knitted fabrics',
 '61':'Knitted apparel','62':'Woven apparel','63':'Made-up textiles','64':'Footwear','65':'Headgear',
 '66':'Umbrellas','67':'Feathers & artificial flowers','68':'Stone, plaster & cement articles','69':'Ceramics','70':'Glass',
 '71':'Gems & precious metals','72':'Iron & steel','73':'Iron & steel articles','74':'Copper','75':'Nickel',
 '76':'Aluminium','78':'Lead','79':'Zinc','80':'Tin','81':'Other base metals',
 '82':'Tools & cutlery','83':'Misc base-metal articles','84':'Machinery','85':'Electrical machinery','86':'Railway vehicles',
 '87':'Vehicles','88':'Aircraft','89':'Ships & boats','90':'Optical & medical instruments','91':'Clocks & watches',
 '92':'Musical instruments','93':'Arms & ammunition','94':'Furniture & bedding','95':'Toys & sports equipment','96':'Misc manufactured articles',
 '97':'Works of art','98':'Special transactions','99':'Unclassified'}

def clean(s, cap=46):
    s=re.sub(r'\s+',' ',str(s or '').strip().title())
    return s if len(s)<=cap else s[:cap-1].rstrip()+'…'

def sec_of(hs8):
    try: return ch2sec.get(int(str(hs8)[:2]),"XXI")
    except: return "XXI"

os.chdir(WH)  # trade_hs8 is a view over trade_hs8.parquet via a relative path
con = duckdb.connect('data_darbar.duckdb', read_only=True)
df = con.sql("""
  select direction, fiscal_year, hs8, commodity, country, fy_value_kpkr
  from trade_hs8 where country is not null and fy_value_kpkr is not null
""").df()
df['bn'] = df.fy_value_kpkr/1e6
df['chapter'] = df.hs8.str[:2]
df['section'] = df.hs8.map(sec_of)
df['country'] = df.country.map(clean)

eyears = sorted(df[df.direction=='export'].fiscal_year.unique())
iyears = sorted(df[df.direction=='import'].fiscal_year.unique())
elatest, ilatest = eyears[-1], iyears[-1]
print('export years', eyears, '\nimport years', iyears)

# ---------------- country selector list ----------------
def ctotal(direction, year):
    d=df[(df.direction==direction)&(df.fiscal_year==year)]
    return d.groupby('country').bn.sum()
exp_l=ctotal('export',elatest); imp_l=ctotal('import',ilatest)
countries=sorted(set(exp_l.index)|set(imp_l.index),
                 key=lambda c:-(float(exp_l.get(c,0))+float(imp_l.get(c,0))))
# keep countries with total trade >= 6 bn (drops long tail of micro-partners; keeps ~65)
keep=[c for c in countries if (float(exp_l.get(c,0))+float(imp_l.get(c,0)))>=6.0]
print(f'countries kept: {len(keep)} of {len(countries)}')

sel=[{'name':c,'exp':round(float(exp_l.get(c,0)),1),'imp':round(float(imp_l.get(c,0)),1)} for c in keep]

# ---------------- per-country detail ----------------
country={}
gc = df.groupby(['country','direction','fiscal_year']).bn.sum().reset_index()
for c in keep:
    sub=gc[gc.country==c]
    series={}
    for r in sub.itertuples():
        series.setdefault(r.fiscal_year,{})[('exp' if r.direction=='export' else 'imp')]=round(r.bn,2)
    # latest-year breakdowns at 3 levels
    level={}
    for lvl,col in [('section','section'),('chapter','chapter'),('product','hs8')]:
        level[lvl]={}
        for d,yr in [('exp',elatest),('imp',ilatest)]:
            dd=df[(df.country==c)&(df.direction=={'exp':'export','imp':'import'}[d])&(df.fiscal_year==yr)]
            if col=='hs8':
                g=dd.groupby('hs8').agg(bn=('bn','sum'),name=('commodity','first')).reset_index().sort_values('bn',ascending=False).head(8)
                level[lvl][d]=[{'code':r.hs8,'name':clean(r.name),'bn':round(r.bn,2)} for r in g.itertuples() if r.bn>0]
            else:
                g=dd.groupby(col).bn.sum().reset_index().sort_values('bn',ascending=False)
                def lab(code): return secname[code] if col=='section' else CH_NAMES.get(code,'Ch '+str(code))
                level[lvl][d]=[{'code':r[1],'name':lab(r[1]),'bn':round(r.bn,2)} for r in g.itertuples() if r.bn>0]
    country[c]=dict(series=series,level=level)

# ---------------- movers: full span + each year-on-year pair ----------------
LEVELS=[('section','section',lambda k:secname.get(k,k)),
        ('chapter','chapter',lambda k:CH_NAMES.get(k,'Ch '+str(k))),
        ('product','hs8',None)]
def movers_pair(direction,y0,y1):
    d=df[df.direction==direction]
    res={}
    for lvl,col,namer in LEVELS:
        a=d[d.fiscal_year==y0].groupby(col).agg(v0=('bn','sum'),nm=('commodity','first'))
        b=d[d.fiscal_year==y1].groupby(col).agg(v1=('bn','sum'),nm=('commodity','first'))
        m=a.join(b,how='outer',lsuffix='_0',rsuffix='_1').fillna(0.0)
        rows=[]
        for k,r in m.iterrows():
            nm = clean(r.get('nm_1') or r.get('nm_0')) if col=='hs8' else namer(k)
            v0,v1=float(r.v0),float(r.v1)
            pct = None if v0<=0 else round(100*(v1-v0)/v0,1)
            rows.append({'code':k,'name':nm,'v0':round(v0,2),'v1':round(v1,2),
                         'delta':round(v1-v0,2),'pct':pct,'y0':y0,'y1':y1})
        if col=='hs8':
            byabs=sorted(rows,key=lambda r:-abs(r['delta']))[:40]
            elig=[r for r in rows if max(r['v0'],r['v1'])>=1.0 and r['pct'] is not None]
            byrise=sorted(elig,key=lambda r:-r['pct'])[:22]
            byfall=sorted(elig,key=lambda r:r['pct'])[:22]
            seen=set(); kr=[]
            for r in byabs+byrise+byfall:
                if r['code'] in seen: continue
                seen.add(r['code']); kr.append(r)
            rows=kr
        else:
            rows=sorted(rows,key=lambda r:-abs(r['delta']))
        res[lvl]=rows
    return res

movers={}; windows={}
for direction in ['export','import']:
    yrs = eyears if direction=='export' else iyears
    lvl_out={lvl:{'span':None,'yoy':{}} for lvl,_,_ in LEVELS}
    sp=movers_pair(direction,yrs[0],yrs[-1])
    for lvl,_,_ in LEVELS: lvl_out[lvl]['span']=sp[lvl]
    for i in range(1,len(yrs)):
        pr=movers_pair(direction,yrs[i-1],yrs[i])
        for lvl,_,_ in LEVELS: lvl_out[lvl]['yoy'][yrs[i]]=pr[lvl]
    movers[direction]=lvl_out
    # window options for the frontend timeline
    windows[direction]=[{'key':'span','y0':yrs[0],'y1':yrs[-1],'label':f'{yrs[0]} → {yrs[-1]}'}] + \
        [{'key':yrs[i],'y0':yrs[i-1],'y1':yrs[i],'label':f'{yrs[i-1]} → {yrs[i]}'} for i in range(1,len(yrs))]

data=dict(sel=sel, country=country, movers=movers,
          meta=dict(export_years=eyears, import_years=iyears,
                    span={'export':(eyears[0],eyears[-1]),'import':(iyears[0],iyears[-1])},
                    windows=windows,
                    latest={'export':elatest,'import':ilatest},
                    note='Country totals and movers from PBS 8-digit trade; values Rs bn. Section/chapter/8-digit levels; full-span and year-on-year windows.',
                    chapter_names='readable labels for common HS chapters; others shown as "Ch NN"'))
p=os.path.join(OUT,'trade_extra.json')
json.dump(data,open(p,'w'),separators=(',',':'))
kb=os.path.getsize(p)//1024
print(f'wrote {p} {kb} KB · countries {len(sel)} · export windows {len(windows["export"])}')
# quick validation
top=sorted(movers['export']['section']['span'],key=lambda r:-r['delta'])[:3]
print('top export section risers (span):', [(r['name'],r['delta']) for r in top])
last_yoy=windows['export'][-1]['key']
topy=sorted(movers['export']['section']['yoy'][last_yoy],key=lambda r:-r['delta'])[:3]
print(f'top export section risers YoY {last_yoy}:', [(r['name'],r['delta']) for r in topy])
