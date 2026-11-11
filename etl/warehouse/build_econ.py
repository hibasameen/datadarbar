import duckdb, pandas as pd, json, os
from dd_paths import STAGE, ECON
con=duckdb.connect(str(STAGE/"data_darbar.duckdb"))
os.chdir(STAGE)
OUT=str(ECON); os.makedirs(OUT,exist_ok=True)
trade=con.execute("SELECT * FROM read_parquet('trade_hs8.parquet')").df()
# HS section from chapter (first 2 digits)
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
def sec(hs8):
    try: return ch2sec.get(int(hs8[:2]),"XXI")
    except: return "XXI"
trade['section']=trade.hs8.map(sec)
trade['bn']=trade.fy_value_kpkr/1e6  # thousand PKR -> billion PKR
tot=trade[trade.country.isna()]      # commodity totals
ctry=trade[trade.country.notna()]
years=sorted(trade.fiscal_year.unique())
# 1. section composition per direction x year
comp={}
for d in ['export','import']:
    comp[d]={}
    for y in years:
        sub=tot[(tot.direction==d)&(tot.fiscal_year==y)]
        if len(sub)==0: continue
        g=sub.groupby('section').bn.sum().reset_index().sort_values('bn',ascending=False)
        comp[d][y]=[{"section":r.section,"name":secname[r.section],"bn":round(r.bn,2)} for r in g.itertuples()]
json.dump(comp,open(f"{OUT}/econ_trade_sections.json","w"))
# 2. top products
prod={}
for d in ['export','import']:
    prod[d]={}
    for y in years:
        sub=tot[(tot.direction==d)&(tot.fiscal_year==y)].nlargest(25,'bn')
        prod[d][y]=[{"hs8":r.hs8,"name":str(r.commodity).title()[:48],"section":r.section,"bn":round(r.bn,2)} for r in sub.itertuples()]
json.dump(prod,open(f"{OUT}/econ_top_products.json","w"))
# 3. top partners
part={}
for d in ['export','import']:
    part[d]={}
    for y in years:
        sub=ctry[(ctry.direction==d)&(ctry.fiscal_year==y)].groupby('country').bn.sum().reset_index().nlargest(25,'bn')
        part[d][y]=[{"country":r.country,"bn":round(r.bn,2)} for r in sub.itertuples()]
json.dump(part,open(f"{OUT}/econ_top_partners.json","w"))
# 4. trade totals + balance
totals={"years":years,"export":{},"import":{},"balance":{}}
for y in years:
    e=tot[(tot.direction=='export')&(tot.fiscal_year==y)].bn.sum()
    i=tot[(tot.direction=='import')&(tot.fiscal_year==y)].bn.sum()
    totals["export"][y]=round(e,1) if e else None
    totals["import"][y]=round(i,1) if i else None
    totals["balance"][y]=round(e-i,1) if e and i else None
json.dump(totals,open(f"{OUT}/econ_trade_totals.json","w"))
print("years:",years)
print("export sections 2020-21:",comp['export'].get('2020-21',[])[:5])
print("export totals:",totals['export'])
print("import totals:",totals['import'])
con.close()
