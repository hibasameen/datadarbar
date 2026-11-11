import duckdb, os, glob
from dd_paths import STAGE
OUT=str(STAGE); os.chdir(OUT)
dbp="data_darbar.duckdb"
if os.path.exists(dbp): os.remove(dbp)
con=duckdb.connect(dbp)
for t in ['national_accounts','budget_lines','file_catalog']:
    con.execute(f"CREATE TABLE {t} AS SELECT * FROM read_parquet('{t}.parquet')")
# relative path view (resolves from the warehouse folder)
con.execute("CREATE VIEW trade_hs8 AS SELECT * FROM read_parquet('trade_hs8.parquet')")
con.execute("CREATE VIEW trade_by_country AS SELECT direction,fiscal_year,hs8,commodity,country,unit,month_qty,month_value_kpkr,fy_qty,fy_value_kpkr FROM trade_hs8 WHERE country IS NOT NULL")
con.execute("CREATE VIEW trade_commodity_totals AS SELECT direction,fiscal_year,hs8,commodity,unit,fy_qty,fy_value_kpkr FROM trade_hs8 WHERE country IS NULL")
con.execute("CREATE VIEW budget_detailed_be AS SELECT doc_fy,table_no,table_title,item,value_rs_mn FROM budget_lines WHERE is_own_year_be AND table_no>=4")
# verify
print("trade rows via view:", con.execute("SELECT count(*) FROM trade_hs8").fetchone()[0])
print("tables/views:", [r[0] for r in con.execute("SELECT view_name FROM duckdb_views() WHERE NOT internal UNION ALL SELECT table_name FROM duckdb_tables()").fetchall()])
con.close()
print("db size:", os.path.getsize(dbp)//1024,"KB")
for f in sorted(glob.glob("*")): 
    if os.path.isfile(f): print(f"  {os.path.getsize(f)//1024:6d}KB  {f}")
