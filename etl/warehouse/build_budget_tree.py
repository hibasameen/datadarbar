import duckdb, json, re, os
from dd_paths import STAGE, ECON
con=duckdb.connect()
con.execute("CREATE VIEW budget_lines AS SELECT * FROM read_parquet('"+(STAGE/'budget_lines.parquet').as_posix()+"')")
OUT=str(ECON)
def be(fy,tbl,like):
    r=con.execute("""SELECT value_rs_mn FROM budget_lines WHERE doc_fy=? AND table_no=? AND item ILIKE ? AND is_own_year_be AND value_rs_mn>0 ORDER BY value_rs_mn DESC LIMIT 1""",[fy,tbl,like]).fetchone()
    return round(r[0]/1000,1) if r else None
def funcs(fy):
    # numbered function lines in table 11
    rows=con.execute("""SELECT item,value_rs_mn FROM budget_lines WHERE doc_fy=? AND table_no=11 AND is_own_year_be AND value_rs_mn>0 AND regexp_matches(item,'^\\(?\\d+\\)')""",[fy]).fetchall()
    out={}
    for it,v in rows:
        nm=re.sub(r'^\(?\d+\)\s*','',it).strip().title()
        out[nm]=round(v/1000,1)
    return out
years=[r[0] for r in con.execute("SELECT DISTINCT doc_fy FROM budget_lines ORDER BY doc_fy").fetchall()]
tree={"receipts":{},"expenditure":{}}
for fy in years:
    # RECEIPTS
    direct=[("Income Tax",be(fy,4,'Income Tax')),("Capital Value Tax",be(fy,4,'%Capital Value%')),("Workers Welfare Fund",be(fy,4,'%Workers Welfare%')),("Workers Profit Fund",be(fy,4,'%Workers Prof%'))]
    indirect=[("Sales Tax",be(fy,4,'Sales Tax')),("Customs Duties",be(fy,4,'Customs%')),("Federal Excise",be(fy,4,'Federal Excise%'))]
    nontax=be(fy,5,'%')  # crude: top non-tax line; fallback total below
    nontax_tot=con.execute("SELECT max(value_rs_mn) FROM budget_lines WHERE doc_fy=? AND table_no=5 AND is_own_year_be",[fy]).fetchone()[0]
    groups=[]
    dl=[{"name":n,"bn":v} for n,v in direct if v]
    il=[{"name":n,"bn":v} for n,v in indirect if v]
    if dl: groups.append({"key":"direct","label":"Direct taxes","color":"#1e6b3e","children":dl})
    if il: groups.append({"key":"indirect","label":"Indirect taxes","color":"#d4a017","children":il})
    if nontax_tot: groups.append({"key":"nontax","label":"Non-tax revenue","color":"#3d6db5","children":[{"name":"Non-tax revenue","bn":round(nontax_tot/1000,1)}]})
    if sum(len(g["children"]) for g in groups)>=3: tree["receipts"][fy]=groups
    # EXPENDITURE
    fx=funcs(fy)
    if fx:
        egroups=[]
        pal=["#1e6b3e","#c0392b","#3d6db5","#e07b39","#9b59b6","#16a085","#5b8c5a","#a0522d","#d4a017","#5d6d7e"]
        # split General Public Service
        i=0
        for nm,tot in sorted(fx.items(),key=lambda x:-x[1]):
            col=pal[i%len(pal)]; i+=1
            if 'General Public' in nm:
                dom=be(fy,11,'%Servicing of Domestic%'); frn=be(fy,11,'%Servicing of Foreign%'); pen=be(fy,11,'%Superannuation%') or be(fy,11,'%Pension%')
                subs=[]; used=0
                if dom:subs.append({"name":"Domestic debt servicing","bn":dom});used+=dom
                if frn:subs.append({"name":"Foreign debt servicing","bn":frn});used+=frn
                if pen:subs.append({"name":"Pensions","bn":pen});used+=pen
                rem=round(tot-used,1)
                if rem>0: subs.append({"name":"Other general govt","bn":rem})
                egroups.append({"key":nm,"label":nm,"color":col,"children":subs if subs else [{"name":nm,"bn":tot}]})
            else:
                egroups.append({"key":nm,"label":nm,"color":col,"children":[{"name":nm,"bn":tot}]})
        tree["expenditure"][fy]=egroups
json.dump(tree,open(f"{OUT}/budget_tree.json","w"))
print("receipts years:",sorted(tree["receipts"].keys()))
print("expenditure years:",sorted(tree["expenditure"].keys()))
fy='2025-26'
print(f"\n{fy} receipts groups:",[(g['label'],[(c['name'],c['bn']) for c in g['children']]) for g in tree['receipts'].get(fy,[])])
print(f"\n{fy} expenditure groups:",[(g['label'],sum(c['bn'] for c in g['children'])) for g in tree['expenditure'].get(fy,[])])
con.close()
