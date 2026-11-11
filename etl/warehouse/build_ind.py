import duckdb, json, os
from dd_paths import STAGE, ECON
con=duckdb.connect(str(STAGE/"data_darbar.duckdb"))
OUT=str(ECON)
def na_series(sheet,item,scale=1/1000.0):
    rows=con.execute("SELECT year,value FROM national_accounts WHERE table_sheet=? AND item=? ORDER BY year",[sheet,item]).fetchall()
    return [{"year":y,"value":round(v*scale,2)} for y,v in rows if v is not None]
ind={"series":[]}
def add(key,label,unit,source,pts,note=""):
    pts=[p for p in pts if p["value"] is not None]
    if pts: ind["series"].append({"key":key,"label":label,"unit":unit,"source":source,"note":note,"points":pts})
add("gdp_mp_const","GDP at market prices (constant)","Rs bn","PBS National Accounts (2015-16 base)",na_series("Table 5","G GDP at mp (GVA+T-S)"))
add("gdp_mp_curr","GDP at market prices (current)","Rs bn","PBS National Accounts",na_series("Table 4","G GDP at mp (GVA+T-S)"))
add("gdp_growth","Real GDP growth","%","PBS National Accounts (Table 6)",[{"year":y,"value":round(v,2)} for y,v in con.execute("SELECT year,value FROM national_accounts WHERE table_sheet='Table 6' AND item='D GDP {Total of GVA at bp (A+B+C)' ORDER BY year").fetchall() if v is not None])
add("gni_const","Gross National Income (constant)","Rs bn","PBS National Accounts",na_series("Table 5","I Gross National Income"))
add("va_agri","Agriculture value added (constant)","Rs bn","PBS National Accounts",na_series("Table 5","A Agriculture, Forestry and Fishing     ( 1 to 4 )"))
add("va_ind","Industry value added (constant)","Rs bn","PBS National Accounts",na_series("Table 5","B Industrial Activities ( 1 to 4 )"))
add("va_serv","Services value added (constant)","Rs bn","PBS National Accounts",na_series("Table 5","C Services ( 1 to 10)"))
# FBR tax revenue: table 4, the FBR total line (largest FBR value per year), own-year BE
fbr=con.execute("""WITH f AS (SELECT doc_fy, value_rs_mn, row_number() OVER (PARTITION BY doc_fy ORDER BY value_rs_mn DESC) rn
  FROM budget_lines WHERE table_no=4 AND item ILIKE '%FBR%' AND is_own_year_be AND value_rs_mn IS NOT NULL)
  SELECT doc_fy, value_rs_mn FROM f WHERE rn=1 ORDER BY doc_fy""").fetchall()
add("fbr_tax","FBR tax revenue (Budget Estimate)","Rs bn","Federal Budget in Brief (Table 4)",[{"year":d,"value":round(v/1000,1)} for d,v in fbr],"Own-year budget estimate; extracted from PDF")
# trade totals
tt=json.load(open(f"{OUT}/econ_trade_totals.json"))
for k,lbl in [("export","Total exports"),("import","Total imports"),("balance","Trade balance")]:
    add("trade_"+k,lbl,"Rs bn","PBS 8-digit trade (D-10)",[{"year":y,"value":tt[k][y]} for y in tt["years"] if tt[k].get(y) is not None])
json.dump(ind,open(f"{OUT}/econ_indicators.json","w"))
# meta
meta={"export_years":sorted(json.load(open(f"{OUT}/econ_trade_sections.json"))["export"].keys()),
      "import_years":sorted(json.load(open(f"{OUT}/econ_trade_sections.json"))["import"].keys()),
      "built":"2026-08-04","note":"Trade values are as-published (Rs, thousand-scale aggregated to billions). NA constant-price series on 2015-16 base."}
json.dump(meta,open(f"{OUT}/econ_meta.json","w"))
print("series built:",[(s["key"],len(s["points"])) for s in ind["series"]])
print("\nGDP growth pts:", [ (p["year"],p["value"]) for p in next(s for s in ind["series"] if s["key"]=="gdp_growth")["points"][-6:]])
print("FBR pts:", [(p["year"],p["value"]) for p in next(s for s in ind["series"] if s["key"]=="fbr_tax")["points"]])
print("files:", os.listdir(OUT))
con.close()
