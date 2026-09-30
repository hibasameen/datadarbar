"""Turn saved search-index table responses into provenance-rich observations.

This recovery script deliberately consumes only explicit table numbers; it does
not fill missing figures, infer annual counts from monthly rows, or change source
values when arithmetic conflicts are found.
"""
from pathlib import Path
import argparse, json, re, html, hashlib

WORKSPACE = Path(__file__).resolve().parents[3]
ROOT = WORKSPACE / "raw_data" / "sindh_police" / "recovery_2023_2025"
OUT = WORKSPACE / "data_darbar_warehouse" / "sindh_police" / "recovered_source_extracts"
EXPECTED_SHA256 = {'query_2024_p3.json': 'cb93486e9efa743d4b2259ac6ff78702820613ed708ccdd4658799191e1b2f0b', 'query_2024_p4.json': '50ea4dcad508918c5b1fc1a6a53acd1a6decaa380500bbf9ecf43fa09d15d783', 'query_2024_province.json': '8b41629028bfdd389adfaf77200e97d71223ec8bee874ed41c6d8a6d7363ab9a', 'annual2023_2025_2025_province.json': '1c57c9ecb7b022f3f5f6904d73d45cb7869bf960f325a091821ef2396764c3fc', 'query_2025_p1.json': 'aa608434cffc897caf84d28021b3d52f7cfcfc3880a70fcfc97136c5358ed7c3', 'query_2025_p3.json': '8824ef963d7c670ae97fd4985d0a1feefdc0461ab980d1ca63bf89d4514e34ab'}
URLS = {
    2023: 'https://www.sindhpolice.gov.pk/storage/statistic/1706297616_68886fbacb665.pdf',
    2024: 'https://www.sindhpolice.gov.pk/storage/statistic/1493415403_68886fe55a194.pdf',
    2025: 'https://www.sindhpolice.gov.pk/storage/statistic/1551180790_69721c548efb2.pdf',
}
RANGES = ['KARACHI RANGE', 'SUKKUR RANGE', 'LARKANA RANGE', 'HYDERABAD RANGE', 'MIRPURKHAS RANGE', 'S.B.ABAD RANGE']
def clean(x): return re.sub(r'\s+', ' ', html.unescape(re.sub('<[^>]+>', ' ', x))).strip()
def raw_response(name):
    payload=(ROOT/name).read_bytes()
    if hashlib.sha256(payload).hexdigest() != EXPECTED_SHA256[name]:
        raise ValueError(f"Recovery evidence checksum mismatch: {ROOT/name}")
    d=json.loads(payload)
    if isinstance(d,str): return d
    if 'response' in d: d=d['response']
    return d['value']
def get_block(name, index=0): return raw_response(name).split('-'*80)[index].strip()
def rows(block):
    if '<table' in block:
        table=block[block.index('<table'):]
        table=table[:table.index('</table>')+8] if '</table>' in table else table
        for tr in re.findall(r'<tr\b[^>]*>(.*?)</tr>',table,re.S):
            cells=[clean(x) for x in re.findall(r'<td\b[^>]*>(.*?)</td>',tr,re.S)]
            if cells: yield cells
    else:
        for line in block.splitlines():
            if line.startswith('S.#') and ' | H E A D S ' in line:
                cells=[x.strip() for x in line.split('|')]
                cells[0]=cells[0].removeprefix('S.#').strip()
                cells[1]=cells[1].removeprefix('H E A D S').strip()
                for i in range(2,len(cells)):
                    cells[i]=re.sub(r'^(?:KARACHI RANGE|SUKKUR RANGE|LARKANA RANGE|HYDERABAD RANGE|MIRPURKHAS RANGE|S.B.ABAD RANGE|SINDH PROVINCE|CURRENT MONTH)\s*','',cells[i])
                yield cells

def extract(name,index,report_year,page,geos,mode):
    block=get_block(name,index)
    assert URLS[report_year].split('/storage/')[1] in block.splitlines()[0], (name,index)
    assert '31-12-'+str(report_year) in block or '31.12.'+str(report_year) in block, (name,index)
    out=[]; section=None
    for cells in rows(block):
        if len(cells)<2: continue
        is_grand_total = cells[0].replace(' ','').upper()=='GRANDTOTAL'
        is_total = cells[0].replace(' ','').upper()=='TOTAL' or is_grand_total
        if is_total:
            label=cells[0]
            vals=cells[1:] if cells[1] not in ('','None') else cells[2:]
        else:
            label=cells[1]; vals=cells[2:]
        # Headers sometimes include stray zeros: they are not data rows.
        if cells[0] in ('A','B','D','E'):
            section=label
            continue
        if cells[0]=='C': section='MISCELLANEOUS'
        if cells[0]=='F': section='BLASPHEMY'
        if not vals or any(not re.fullmatch(r'[+-]?\d+',v) for v in vals): continue
        nums=list(map(int,vals))
        if mode=='province_comparison':
            if len(nums)!=6: continue
            specs=[('SINDH PROVINCE',report_year-1,nums[3],vals[3],'previous_year'),('SINDH PROVINCE',report_year,nums[4],vals[4],'current_year')]
        elif mode=='range_comparison':
            if len(nums)!=len(geos)*3: continue
            specs=[(geo,report_year+delta,nums[i*3+j],vals[i*3+j],col) for i,geo in enumerate(geos) for delta,j,col in [(-1,0,'previous_year'),(0,1,'current_year')]]
        elif mode=='range_current':
            if len(nums)!=len(geos): continue
            specs=[(geo,report_year,nums[i],vals[i],'current_year') for i,geo in enumerate(geos)]
        else: raise ValueError(mode)
        for geo,year,n,value_raw,column in specs:
            out.append(dict(year=year,period_start=f'{year}-01-01',period_end=f'{year}-12-31',is_full_year=True,geography_name=geo,geography_level='province' if geo=='SINDH PROVINCE' else 'range',crime_category_raw=label,category_group=section,row_type=('subtotal' if mode=='range_current' and is_total and not is_grand_total else 'total' if is_total else 'detail'),value_raw=value_raw,cases_reported=n,source_page=page,comparison_column=column,recovery_evidence_file=str((ROOT/name).resolve()),recovery_block=index))
    return out

def save_report(year,configs):
    observations=[]
    for c in configs: observations.extend(extract(*c))
    # Deduplicate identical rows encountered in separate indexed excerpts.
    unique={}
    for o in observations:
        key=(o['year'],o['geography_name'],o['category_group'],o['crime_category_raw'],o['row_type'],o['cases_reported'],o['source_page'])
        unique.setdefault(key,o)
    observations=list(unique.values())
    for observation in observations:
        if observation['row_type']=='total': observation['category_group']=None
        label=observation['crime_category_raw']
        canonical=label
        if label.lower().startswith(('receiving ','reeceiving ')): canonical='Receiving Stolen Property (S 411 PPC)'
        if label.lower()=='other roberry': canonical='Other Robbery'
        if label.lower().startswith('blasphemy'): canonical='BLASPHEMY (Offences relating to religion)'
        if observation['row_type']=='total': canonical='total'
        observation['crime_category_key']=re.sub(r'[^a-z0-9]+','_',canonical.lower()).strip('_')
    source_files=sorted(set(o['recovery_evidence_file'] for o in observations))
    report=dict(report_id=f'sindh_police_annual_{year}_recovered',source_url=URLS[year],title=f'Crime figures of Sindh Province {year}: December and year-end comparative statement',reporting_year=year,period_start=f'{year}-01-01',period_end=f'{year}-12-31',is_full_year=True,source_type='search_index_extract',extraction_method='Explicit numeric cells parsed from retained official-PDF search-index text; live PDF download blocked by HTTP 403.',source_priority=year,period_verification='Explicit 01 January to 31 December headings in recovered annual tables; December-only tables excluded.',retrieved_at='2026-09-06',quality_issues=['Live PDF unavailable; values recovered from search-index text, which can omit rows or reflect an earlier snapshot.','Original offence and geography labels preserved. Comparison years are separate observations.','Numeric arithmetic and source-version conflicts must remain visible; no source counts are corrected.'],raw_evidence=[dict(path=f,sha256=hashlib.sha256((ROOT/f).read_bytes()).hexdigest()) for f in source_files],observations=observations)
    report['category_partition_complete']=(year==2025)
    report['selection_eligible']=(year==2025)
    if year==2024:
        report['quality_issues'].extend([
            'The province-page headline states UPTO DATE: 30-11-2024, while its December title, annual column dates, and range pages explicitly end 31-12-2024. Classified as full-year using table column dates; headline discrepancy retained.',
            'Province comparison table is complete (44 detail categories and total per year); original range extracts are partial. Complete 2024 range coverage is supplied separately by the 2025 report comparison columns.',
            'The later 2025 report revises some 2024 comparison values. Preserve both vintages and their source URLs.'
        ])
    if len(observations) != {2024: 234, 2025: 630}[year]:
        raise ValueError(f'Unexpected observation count for {year}: {len(observations)}')
    OUT.mkdir(parents=True, exist_ok=True)
    (OUT/f'recovered_annual_{year}.json').write_text(json.dumps(report,indent=2)+'\n')
    print(year,len(observations), sorted(set(o['geography_name'] for o in observations)))
    return report

def main(argv=None):
    global ROOT, OUT
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw-dir', type=Path, default=ROOT)
    parser.add_argument('--out-dir', type=Path, default=OUT)
    args=parser.parse_args(argv)
    ROOT=args.raw_dir.resolve()
    OUT=args.out_dir.resolve()
    if OUT == ROOT or ROOT in OUT.parents:
        parser.error('--out-dir must be outside --raw-dir to preserve source records')
    save_report(2025,[('query_2025_p1.json',0,2025,1,[], 'province_comparison'),('query_2025_p3.json',0,2025,3,RANGES[:3],'range_comparison'),('annual2023_2025_2025_province.json',1,2025,4,RANGES[3:],'range_comparison')])
    save_report(2024,[('query_2024_province.json',0,2024,1,[],'province_comparison'),('query_2024_p3.json',0,2024,3,RANGES[:3],'range_comparison'),('query_2024_p4.json',0,2024,4,RANGES[3:],'range_comparison')])


if __name__ == "__main__":
    main()
