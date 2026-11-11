import openpyxl, json, re
from dd_paths import RAW, ECON
p=RAW/"national_accounts"/"supply_use_io_2015-16"/"Supply_use_and_Input-Output_tables_of_Pakistan_2015-16_Excel_Table.xlsx"
wb=openpyxl.load_workbook(p,read_only=True,data_only=True); ws=wb['Input Output table']
rows=[list(r) for r in ws.iter_rows(values_only=True)]
# row sectors rows 8..75 (col0 code, col1 desc); col sectors cols 2..69
rsec=[(i,str(rows[i][0]).strip(),str(rows[i][1]).strip()) for i in range(8,76) if rows[i][0] not in (None,'')]
ncol=68
# group mapping by leading chapter number
def lead(code):
    m=re.match(r'(\d+)',str(code)); return int(m.group(1)) if m else 999
GROUPS=[("Agriculture",lambda n:1<=n<=3,"#5b8c5a"),("Mining & fuels",lambda n:5<=n<=9,"#5d6d7e"),
("Food, bev & tobacco",lambda n:10<=n<=12,"#c0392b"),("Textiles & apparel",lambda n:13<=n<=15,"#1e6b3e"),
("Other manufacturing",lambda n:16<=n<=33,"#e07b39"),("Electricity, gas & water",lambda n:35<=n<=39,"#3d6db5"),
("Construction",lambda n:41<=n<=43,"#a0522d"),("Wholesale & retail trade",lambda n:45<=n<=47,"#d4a017"),
("Transport & storage",lambda n:49<=n<=54,"#2c3e8f"),("Info, finance & business svcs",lambda n:(55<=n<=82),"#9b59b6"),
("Public admin & education",lambda n:(84<=n<=85),"#16a085"),("Health & other services",lambda n:86<=n<=99,"#8e44ad")]
def grp(code):
    n=lead(code)
    for gi,(nm,f,c) in enumerate(GROUPS):
        if f(n): return gi
    return len(GROUPS)-1
# position k -> group; row k = rsec[k], col k = same order
row_g=[grp(c) for _,c,_ in rsec]
NG=len(GROUPS)
M=[[0.0]*NG for _ in range(NG)]
for k,(ri,code,desc) in enumerate(rsec):
    gi=row_g[k]
    for l in range(ncol):
        gj=row_g[l] if l<len(row_g) else None
        if gj is None: continue
        v=rows[ri][2+l]
        if isinstance(v,(int,float)): M[gi][gj]+=float(v)/1000.0
# scale check
tot=sum(sum(r) for r in M); 
# values appear to be in Rs billion already (sheet 'B'); keep as bn
# zero diagonal for cleaner inter-sector flow
Mnodiag=[[ (0 if i==j else round(M[i][j],1)) for j in range(NG)] for i in range(NG)]
io={"sectors":[g[0] for g in GROUPS],"colors":[g[2] for g in GROUPS],"matrix":[[round(x,1) for x in r] for r in M],"matrix_nodiag":Mnodiag,"total":round(tot,1),"note":"2015-16 industry-by-industry IO, aggregated to 12 sectors, Rs billion (basic prices)"}
json.dump(io,open(ECON/"io_2015.json","w"))
print("total intermediate flows: Rs",round(tot),"bn")
print("sectors:",io["sectors"])
# top cross-sector flows
flows=[]
for i in range(NG):
    for j in range(NG):
        if i!=j: flows.append((Mnodiag[i][j],io["sectors"][i],io["sectors"][j]))
flows.sort(reverse=True)
print("top cross-sector flows (supplier -> user, Rs bn):")
for v,a,b in flows[:8]: print(f"  {v:8.0f}  {a}  ->  {b}")
wb.close()
