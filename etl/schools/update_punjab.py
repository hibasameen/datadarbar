"""Integrate the public 2017 Punjab school points without changing roster membership.

Preserves the September analysis coordinates and attributes. The source extract
contains historical schools; only unique EMIS matches to the current roster enter.
"""
import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import re
import pandas as pd
from shapely.geometry import shape, Point
from shapely.strtree import STRtree

HERE = Path(__file__).resolve().parent
RELEASE = '2026-09-30'
SOURCE = HERE / 'punjab_public_schools_2017.json.gz'
URL = 'https://services3.arcgis.com/t6lYS2Pmd8iVx1fy/arcgis/rest/services/pak_punjab_edu_inst_aepam/FeatureServer/0'
PRIOR = ['lat', 'lng', 'district_key_boundary', 'coord_method', 'coord_precision',
         'coord_tier', 'geocode_match', 'source', 'source_url', 'source_vintage']

def norm(value):
    return re.sub('[^a-z0-9]', '', str(value).lower())

def integrate(df, geojson, district_keys):
    df = df.copy()
    if 'analysis_lat' in df:
        raise ValueError('Input is already integrated; rebuild from the frozen September input.')
    for c in PRIOR:
        df['analysis_' + c] = df[c]
    for c in ['coord_source_url','coord_source_id','historical_name','historical_gender','coord_review_flags','coord_validation']:
        df[c] = pd.Series(None, index=df.index, dtype=object)
    df['coord_source_year'] = pd.Series(pd.NA, index=df.index, dtype='Int64')
    with gzip.open(SOURCE, 'rt') as f:
        raw = json.load(f)['features']
    ids = [str(int(f['attributes']['Inst_ID'])) for f in raw]
    assert len(raw) == 52337 and len(set(ids)) == len(ids)
    assert all(len(x) == 9 and x.startswith('1') for x in ids)
    assert all(f['attributes']['Year'] == 2017 and f['attributes']['Sector'] == 'Public' for f in raw)
    points = {x[1:]: f for x,f in zip(ids,raw)}
    shared = Counter((round(f['geometry']['x'],6), round(f['geometry']['y'],6)) for f in raw)
    gj = json.loads(Path(geojson).read_text())
    tree = STRtree([shape(f['geometry']) for f in gj['features']])
    flat_keys = {norm(k):k for k in district_keys}
    keys = [flat_keys[norm(f['properties']['districts'])] for f in gj['features']]
    flags_count = Counter()
    changed = []
    for i,r in df[df['province'] == 'Punjab'].iterrows():
        f = points.get(str(r['school_id']))
        if f is None:
            continue
        a,g = f['attributes'],f['geometry']
        lng,lat = round(g['x'],6),round(g['y'],6)
        assert 69 <= lng <= 76 and 27 <= lat <= 35
        hits = tree.query(Point(lng,lat), predicate='within')
        boundary = keys[int(hits[0])] if len(hits) else None
        flags = ['historical_2017_building_unverified']
        if norm(r['name']) != norm(a['Name']): flags.append('name_differs')
        if {'Boys':'Male','Girls':'Female'}.get(r['gender']) != a['Schl_Gndr']: flags.append('historical_gender_conflict')
        if shared[(lng,lat)] > 1: flags.append('shared_coordinate')
        if boundary is None: flags.append('outside_district_polygons')
        elif boundary != r['district_key']: flags.append('outside_source_district')
        if len(hits) > 1: flags.append('overlapping_district_polygons')
        values = {
            'lat':lat, 'lng':lng, 'has_coords':True, 'district_key_boundary':boundary,
            'coord_method':'historical_school_point_2017', 'coord_precision':'school',
            'geocode_match':'emis_match_after_removing_leading_1_from_Inst_ID',
            'source':'Punjab SIS roster (2026); PIE-linked AEPAM public-school point layer (2017)',
            'source_url':'https://sis.pesrp.edu.pk/; ' + URL,
            'source_vintage':'Roster: August 2026; coordinates: 2017 (retrieved 28 September 2026)',
            'coord_source_year':2017,'coord_source_url':URL,'coord_source_id':str(int(a['Inst_ID'])),
            'historical_name':a['Name'],'historical_gender':a['Schl_Gndr'],
            'coord_review_flags':';'.join(flags),
            'coord_validation':'ID linked; numeric and district-polygon checks; no building verification',
        }
        for k,v in values.items(): df.at[i,k] = v
        flags_count.update(flags)
        changed.append(i)
    df['release'] = RELEASE
    report = {'release':RELEASE,'rows':len(df),'matched_punjab':len(changed),
              'punjab_roster':int((df.province == 'Punjab').sum()),
              'has_coords':int(df.has_coords.sum()),
              'punjab_has_coords':int(df.loc[df.province == 'Punjab','has_coords'].sum()),
              'newly_positioned':int(df.loc[changed,'analysis_lat'].isna().sum()),
              'in_analysis_frozen':int(df.in_analysis.sum()),
              'flags':dict(flags_count),'source_sha256':hashlib.sha256(SOURCE.read_bytes()).hexdigest(),
              'source_records':len(raw),
              'unmatched_historical_not_added':len(raw)-len(changed),
              'note':'Current roster attributes and published-analysis membership retained. Analysis coordinates preserved in analysis_* fields; derived distance tables remain September snapshots.'}
    return df,report

def register_table(register, sc_dir=HERE):
    meta = json.loads((Path(sc_dir)/'schools_pk_metadata.json').read_text())
    path = (Path(sc_dir)/f'schools_pk_{RELEASE}.csv.gz').as_posix()
    types = {'school_id':'VARCHAR','row_id':'INTEGER','boys_enrolled':'INTEGER','girls_enrolled':'INTEGER',
             'enrolment_total':'INTEGER','functional':'BOOLEAN','coord_resid_m':'DOUBLE','pin_vs_solved_m':'DOUBLE',
             'lat':'DOUBLE','lng':'DOUBLE','analysis_lat':'DOUBLE','analysis_lng':'DOUBLE',
             'coord_source_id':'VARCHAR','coord_source_year':'INTEGER'}
    sql = f"SELECT * FROM read_csv('{path}', header=true, auto_detect=true, sample_size=-1, types={types!r}) ORDER BY row_id"
    register('schools_pk',meta['description'],meta['notes'],meta['columns'],meta['source'],sql)

def update_warehouse(out):
    """Update only the school Parquet, its catalogue entry and sample rows."""
    import duckdb
    out = Path(out)
    con = duckdb.connect()
    cat = json.loads((out/'catalog.json').read_text())
    old = next(t for t in cat['tables'] if t['name']=='schools_pk')
    def register(name,desc,notes,columns,source,sql):
        target=out/(name+'.parquet')
        con.sql(f"COPY ({sql}) TO '{target.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)")
        schema=con.sql(f"DESCRIBE SELECT * FROM '{target.as_posix()}'").fetchall()
        entry=dict(old,description=desc,notes=notes,source=source,bytes=target.stat().st_size,
                   rows=con.sql(f"SELECT count(*) FROM '{target.as_posix()}'").fetchone()[0],
                   columns=[{'name':c[0],'type':c[1],'description':columns.get(c[0],'')} for c in schema])
        cat['tables']=[entry if t['name']==name else t for t in cat['tables']]
        sample=json.loads((out/'catalog_samples.json').read_text())
        sample[name]=[[None if v is None else str(v)[:80] for v in row] for row in con.sql(f"SELECT * FROM '{target.as_posix()}' LIMIT 5").fetchall()]
        (out/'catalog_samples.json').write_text(json.dumps(sample,ensure_ascii=False,separators=(',',':')))
    register_table(register)
    # Figure-1 example must continue to refer to the published snapshot geometry.
    for e in cat.get('examples',[]):
        if 'schools_pk' in e.get('sql','') and 'Figure 1' in e.get('title',''):
            e['sql']=e['sql'].replace('district_key_boundary','analysis_district_key_boundary')
    cat['generated']=RELEASE
    cat['revision']=RELEASE+'-punjab-coordinates'
    (out/'catalog.json').write_text(json.dumps(cat,indent=1)+'\n')
    con.close()

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--input',type=Path,default=HERE/'schools_pk_2026-09.csv.gz')
    p.add_argument('--geojson',type=Path,required=True)
    p.add_argument('--darbar',type=Path,required=True)
    p.add_argument('--warehouse',type=Path)
    args=p.parse_args()
    df=pd.read_csv(args.input,dtype={'school_id':str},low_memory=False)
    out,report=integrate(df,args.geojson,json.loads(args.darbar.read_text()).keys())
    out.to_csv(HERE/f'schools_pk_{RELEASE}.csv.gz',index=False,compression={'method':'gzip','mtime':0})
    (HERE/'punjab_integration_report.json').write_text(json.dumps(report,indent=2)+'\n')
    if args.warehouse: update_warehouse(args.warehouse)
    print(json.dumps(report,indent=2))

if __name__=='__main__': main()
