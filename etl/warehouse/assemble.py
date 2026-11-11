import duckdb, pandas as pd, os, re, glob, argparse
from dd_paths import RAW, WORK, STAGE
# usage: python3 assemble.py --uploads RAW_DATA_DIR --work DIR_WITH_STAGE_PARQUETS --out OUTDIR [--only national_accounts ...]
#   --work holds trade_hs8.parquet / na_annual.parquet / budget_lines.parquet from the build_* scripts
#   --only restricts the tables written (the file catalog is built only when file_catalog is asked for)
#   (2026-10-24: paths made arguments for the 118th-NAC refresh; defaults now come from dd_paths.py: DD_RAW, DD_WORK, DD_WORK/warehouse)
ALL=['trade_hs8','national_accounts','budget_lines','file_catalog']
ap=argparse.ArgumentParser()
ap.add_argument("--uploads",default=str(RAW))
ap.add_argument("--work",default=str(WORK)); ap.add_argument("--out",default=str(STAGE))
ap.add_argument("--only",nargs="+",choices=ALL,default=ALL)
A=ap.parse_args()
U=A.uploads.rstrip("/")+"/"; W=A.work.rstrip("/")
OUT=A.out; os.makedirs(OUT+"/parquet",exist_ok=True)
TABLES=[t for t in ALL if t in A.only]

# ---------- FILE CATALOG from manifests ----------
catalog=None
if 'file_catalog' in TABLES:
    def norm(dataset, df, m):
        out=pd.DataFrame()
        out['dataset']=[dataset]*len(df)
        for k,src in m.items():
            out[k]=df[src] if src in df.columns else None
        return out
    cat_parts=[]
    # budget main
    b=pd.read_csv(U+"budget_documents/manifest.csv")
    cat_parts.append(pd.DataFrame({'dataset':'budget','category':'federal_budget','period':b['fiscal_year'],
        'item':b['document'],'filename':b['filename'],'format':b['filename'].str.extract(r'\.(\w+)$')[0],
        'source_url':b['source_url'],'status':'saved','notes':None}))
    # budget pre-2009
    try:
        b2=pd.read_csv(U+"budget_documents/archive_pre2009/manifest.csv")
        cat_parts.append(pd.DataFrame({'dataset':'budget','category':'federal_budget_pre2009','period':b2['fiscal_year'],
            'item':b2['document'],'filename':b2['filename'],'format':b2['filename'].str.extract(r'\.(\w+)$')[0],
            'source_url':b2['original_source_url'],'status':'wayback','notes':'capture '+b2['wayback_capture'].astype(str)}))
    except Exception as e: print("pre2009",e)
    # trade
    t=pd.read_csv(U+"trade_data_8digit/manifest.csv")
    cat_parts.append(pd.DataFrame({'dataset':'trade_8digit','category':t['category']+'/'+t['type'],'period':t['period_label'],
        'item':t['type'],'filename':t['saved_as'],'format':t['saved_as'].str.extract(r'\.(\w+)$')[0],
        'source_url':t['source_url'],'status':t['status'],'notes':t['source_filename']}))
    # NA
    n=pd.read_csv(U+"national_accounts/manifest.csv")
    cat_parts.append(pd.DataFrame({'dataset':'national_accounts','category':n['folder'],'period':None,
        'item':n['title'],'filename':n['saved_as'],'format':n['format'],'source_url':n['source_url'],
        'status':n['status'],'notes':None}))
    catalog=pd.concat(cat_parts,ignore_index=True)
    catalog['parsed_into_db']= (
        ((catalog.dataset=='trade_8digit')&(catalog.format.isin(['txt','xlsx']))&(catalog.category.str.contains('annual',na=False))) |
        ((catalog.dataset=='national_accounts')&(catalog.item.astype(str).str.contains('Annual Tables',na=False))) |
        ((catalog.dataset=='budget')&(catalog.item.astype(str).str.contains('Budget in Brief',na=False)))
    )
    print("catalog rows:",len(catalog),"| by dataset:\n",catalog.dataset.value_counts().to_string())

# ---------- load parquets ----------
trade=pd.read_parquet(W+"/trade_hs8.parquet") if 'trade_hs8' in TABLES else None
na=pd.read_parquet(W+"/na_annual.parquet")
budget=pd.read_parquet(W+"/budget_lines.parquet") if 'budget_lines' in TABLES else None
# clean NA titles
NAT={'Table-1':('Macro Economic Indicators','mixed'),'TABLE-2':('National Accounts Main Aggregates','current prices'),
'TABLE-3':('National Accounts Main Aggregates','constant prices'),'Table 4':('GDP by Sector','current basic prices'),
'Table 5':('GDP by Sector','constant basic prices'),'Table 6':('Real Growth Rates of GDP','constant, % growth'),
'Table 7a':('Sectoral Shares in GDP','constant, % share'),'Table 7b':('Sectoral Shares in GDP','current, % share'),
'Table 8':('Expenditure on GDP','current prices'),'Table 9':('Expenditure on GDP','constant prices'),
'T10':('Gross Fixed Capital Formation (all sectors)','current market prices'),'T10a':('GFCF Private Sector','current market prices'),
'T10b':('GFCF Public & Govt Sector','current market prices'),'T11':('GFCF (all sectors)','constant prices'),
'T11a':('GFCF Private Sector','constant prices'),'T11b':('GFCF Public & Govt Sector','constant prices')}
# Sep-2026 workbook (118th NAC) renamed the GFCF sheets T10..T11b -> 'Table 10'..'Table 11b'
NAT.update({'Table '+k[1:]:v for k,v in list(NAT.items()) if re.match(r'^T1[01]',k)})
# sheet names carry stray trailing spaces ('Table 7a ', and 'Table 8 ' up to May 2026): look up stripped
na['table_name']=na.table_sheet.map(lambda s:NAT.get(s.strip(),(s,''))[0])
na['price_basis']=na.table_sheet.map(lambda s:NAT.get(s.strip(),('',''))[1])
na['base_year']='2015-16'
na=na[['table_sheet','table_name','price_basis','base_year','item','year','value']]

# ---------- DuckDB ----------
dbp=OUT+"/data_darbar.duckdb"
if os.path.exists(dbp): os.remove(dbp)
con=duckdb.connect(dbp)
for name,df in [('trade_hs8',trade),('national_accounts',na),('budget_lines',budget),('file_catalog',catalog)]:
    if name not in TABLES: continue
    con.register('t',df); con.execute(f"CREATE TABLE {name} AS SELECT * FROM t"); con.unregister('t')
    df.to_parquet(f"{OUT}/parquet/{name}.parquet",index=False)
# convenience views
if 'trade_hs8' in TABLES:
    con.execute("CREATE VIEW trade_by_country AS SELECT direction,fiscal_year,hs8,commodity,country,unit,fy_qty,fy_value_kpkr FROM trade_hs8 WHERE country IS NOT NULL")
    con.execute("CREATE VIEW trade_commodity_totals AS SELECT direction,fiscal_year,hs8,commodity,unit,fy_qty,fy_value_kpkr FROM trade_hs8 WHERE country IS NULL")
if 'budget_lines' in TABLES:
    con.execute("CREATE VIEW budget_own_year_be AS SELECT doc_fy,table_no,table_title,item,value_rs_mn FROM budget_lines WHERE is_own_year_be AND table_no>=4")
for r in con.execute("SELECT table_name,estimated_size FROM duckdb_tables()").fetchall(): print("tbl",r)
print("\nrow counts:")
for t in TABLES:
    print(" ",t,con.execute(f"SELECT count(*) FROM {t}").fetchone()[0])
con.close()
import subprocess
print("\nsizes:"); print(subprocess.run(["du","-sh",OUT,dbp]+glob.glob(OUT+"/parquet/*"),capture_output=True,text=True).stdout)
