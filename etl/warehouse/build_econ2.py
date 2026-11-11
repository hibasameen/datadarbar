import pandas as pd, json, os
from dd_paths import STAGE, ECON
comb=pd.read_parquet(STAGE/"trade_hs8.parquet")
OUT=str(ECON); os.makedirs(OUT,exist_ok=True)
SECTIONS=[("I","Animals & products",range(1,6)),("II","Vegetables",range(6,15)),("III","Fats & oils",range(15,16)),
("IV","Food, bev, tobacco",range(16,25)),("V","Minerals & fuels",range(25,28)),("VI","Chemicals",range(28,39)),
("VII","Plastics & rubber",range(39,41)),("VIII","Hides & leather",range(41,44)),("IX","Wood",range(44,47)),
("X","Paper & pulp",range(47,50)),("XI","Textiles",range(50,64)),("XII","Footwear & headgear",range(64,68)),
("XIII","Stone, cement, glass",range(68,71)),("XIV","Gems & precious metal",range(71,72)),("XV","Base metals",range(72,84)),
("XVI","Machinery & electrical",range(84,86)),("XVII","Transport equip.",range(86,90)),("XVIII","Instruments",range(90,93)),
("XIX","Arms",range(93,94)),("XX","Misc. manufactures",range(94,97)),("XXI","Art & special",range(97,100))]
ch2sec={};secname={}
for code,name,rng in SECTIONS:
    for ch in rng: ch2sec[ch]=code
    secname[code]=name
def chapter(hs8): return hs8[:2]
def sec(hs8):
    try:return ch2sec.get(int(hs8[:2]),"XXI")
    except:return "XXI"
comb['section']=comb.hs8.map(sec); comb['chapter']=comb.hs8.map(chapter); comb['bn']=comb.fy_value_kpkr/1e6
tot=comb[comb.country.isna()]; ctry=comb[comb.country.notna()]
eyears=sorted(comb[comb.direction=='export'].fiscal_year.unique())
iyears=sorted(comb[comb.direction=='import'].fiscal_year.unique())
def clean(s): return str(s).title()[:46]
# sections composition
comp={};prod={};part={}
for d in ['export','import']:
    comp[d]={};prod[d]={};part[d]={}
    yrs=sorted(comb[comb.direction==d].fiscal_year.unique())
    for y in yrs:
        s=tot[(tot.direction==d)&(tot.fiscal_year==y)]
        g=s.groupby('section').bn.sum().reset_index().sort_values('bn',ascending=False)
        comp[d][y]=[{"section":r.section,"name":secname[r.section],"bn":round(r.bn,2)} for r in g.itertuples() if r.bn>0]
        pr=s.nlargest(25,'bn'); prod[d][y]=[{"hs8":r.hs8,"name":clean(r.commodity),"section":r.section,"bn":round(r.bn,2)} for r in pr.itertuples()]
        pt=ctry[(ctry.direction==d)&(ctry.fiscal_year==y)].groupby('country').bn.sum().reset_index().nlargest(25,'bn')
        part[d][y]=[{"country":r.country,"bn":round(r.bn,2)} for r in pt.itertuples()]
json.dump(comp,open(f"{OUT}/econ_trade_sections.json","w"))
json.dump(prod,open(f"{OUT}/econ_top_products.json","w"))
json.dump(part,open(f"{OUT}/econ_top_partners.json","w"))
# totals
years=sorted(set(eyears)|set(iyears))
T={"years":years,"export":{},"import":{},"balance":{}}
for y in years:
    e=tot[(tot.direction=='export')&(tot.fiscal_year==y)].bn.sum()
    i=tot[(tot.direction=='import')&(tot.fiscal_year==y)].bn.sum()
    T["export"][y]=round(e,1) if e else None; T["import"][y]=round(i,1) if i else None
    T["balance"][y]=round(e-i,1) if (e and i) else None
json.dump(T,open(f"{OUT}/econ_trade_totals.json","w"))
# DRILL-DOWN TREE: section -> chapter -> products (top 12/chapter + Other)
tree={}
for d in ['export','import']:
    tree[d]={}
    yrs=sorted(comb[comb.direction==d].fiscal_year.unique())
    for y in yrs:
        s=tot[(tot.direction==d)&(tot.fiscal_year==y)]
        secs=[]
        for scode,sg in s.groupby('section'):
            chs=[]
            for ch,cg in sg.groupby('chapter'):
                cg=cg.sort_values('bn',ascending=False)
                top=cg.head(12); rest=cg.iloc[12:]
                prods=[{"hs8":r.hs8,"name":clean(r.commodity),"bn":round(r.bn,3)} for r in top.itertuples() if r.bn>0]
                if len(rest)>0 and rest.bn.sum()>0: prods.append({"hs8":ch+"XXXXXX","name":f"Other ({len(rest)} items)","bn":round(rest.bn.sum(),3)})
                clabel=ch+" · "+(clean(cg.iloc[0].commodity).split(',')[0][:22])
                chs.append({"code":ch,"name":clabel,"bn":round(cg.bn.sum(),3),"products":prods})
            chs.sort(key=lambda x:-x["bn"])
            secs.append({"section":scode,"name":secname[scode],"bn":round(sg.bn.sum(),2),"chapters":chs})
        secs.sort(key=lambda x:-x["bn"])
        tree[d][y]=secs
json.dump(tree,open(f"{OUT}/econ_trade_tree.json","w"))
print("export years:",eyears)
print("import years:",iyears)
print("tree size:",os.path.getsize(f"{OUT}/econ_trade_tree.json")//1024,"KB")
print("sample chapters in Textiles (export 2023-24):",[c["name"] for c in next(s for s in tree['export']['2023-24'] if s['section']=='XI')["chapters"][:4]])
