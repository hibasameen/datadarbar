"""Extract separately dated provincial mid-year tables, never annualize them."""
import hashlib
import json
import re
import pdfplumber
from extract import HERE, FOUR, clean, integer

# Punjab 2025 prints no district all/civil/criminal tables. Its provincial totals exist only
# as the court-tier category tables 3.12 (sessions courts: criminal, then civil) and 3.13
# (civil courts: civil, then criminal). Each part's category rows are summed against its
# printed total; the four printed totals are then added once per category.
PUNJAB_2025_PARTS=[('3.12','sessions_courts','criminal',[(74,0)]),('3.12','sessions_courts','civil',[(74,1),(75,0)]),
                   ('3.13','civil_courts','civil',[(75,1),(76,0)]),('3.13','civil_courts','criminal',[(76,1)])]
PUNJAB_TOTAL_NAMES={'Grand Total','Total','Total of All Civil Categories'}

def punjab_2025(raw,catalog):
    sid='annual_2025';path=raw/'pdfs'/f'{sid}.pdf';sha=hashlib.sha256(path.read_bytes()).hexdigest()
    if sha!=json.loads(path.with_suffix('.metadata.json').read_text())['sha256']:raise ValueError(f'Checksum mismatch: {sid}')
    parts,checks=[],[]
    with pdfplumber.open(path) as pdf:
        for section,court_tier,category,refs in PUNJAB_2025_PARTS:
            detail,total,pages=[],None,[]
            for p,t in refs:
                tables=[x for x in pdf.pages[p-1].find_tables() if x.bbox[2]-x.bbox[0]>300]
                for cells in tables[t].extract():
                    row=[clean(v) for v in cells if clean(v)]
                    if len(row)!=5 or not re.search('[A-Za-z]',row[0]) or not all(re.fullmatch(r'[\d,]+',v) for v in row[1:]):continue
                    nums=[integer(v) for v in row[1:]]
                    if row[0] in PUNJAB_TOTAL_NAMES:
                        if total is not None:raise ValueError(f'Two totals in Punjab 2025 {section} {category}')
                        total=dict(zip(FOUR,nums));total_cells=cells;total_page=p
                    else:detail.append(nums)
                pages.append(p)
            if total is None:raise ValueError(f'No printed total in Punjab 2025 {section} {category}')
            for i,f in enumerate(FOUR):
                s=sum(n[i] for n in detail)
                checks.append(dict(source_id=sid,province='Punjab',section=f'{section} {court_tier} {category}',metric=f,detail_sum=s,reported_total=total[f],
                                   difference=s-total[f],status='pass' if s==total[f] else 'source_mismatch',total_source_record_id=f'{sid}:Punjab:{section}:{court_tier}:{category}:total'))
            parts.append(dict(section=section,court_tier=court_tier,category=category,**total,category_rows=len(detail),pdf_pages=pages,total_pdf_page=total_page,raw_cells=total_cells))
    rows=[]
    for category in ['civil','criminal','all']:
        ps=[x for x in parts if category=='all' or x['category']==category]
        rows.append(dict(source_id=sid,year=2025,period_type='annual',period_start='2025-01-01',period_end='2025-12-31',province='Punjab',geography_level='province',
                         category=category,court_tier='all_courts',**{f:sum(x[f] for x in ps) for f in FOUR},transfers_in=None,transfers_out=None,
                         method='sum_court_tier_category_totals',components=[{k:x[k] for k in ('section','court_tier','category',*FOUR,'total_pdf_page')} for x in ps],
                         raw_values=None,raw_cells=None,pdf_page=None,table_index=None,row_index=None,
                         source_url=catalog[sid]['source_url'],source_sha256=sha,source_record_id=f'{sid}:Punjab:3.12+3.13:{category}',
                         scope_note='Punjab 2025 prints no district all/civil/criminal tables; provincial totals are the printed totals of the court-tier category tables 3.12 (sessions courts) and 3.13 (civil courts), added once. Several 3.12/3.13 categories print zero opening pendency beside a positive balance (new categories), so these totals do not satisfy the stock-flow identity and differ from the overview row (1,501,119 opening; 1,434,397 balance).'))
    return rows,checks

def extract_aggregates(raw):
    catalog={s['id']:s for s in json.loads((HERE/'sources.json').read_text())}
    specs=[('halfyear_2023_h1',2023,'01-01','06-30',[(7,0,'all'),(9,0,'criminal'),(9,1,'civil')]),
           ('halfyear_2024_h1',2024,'01-01','06-30',[(11,0,'all'),(14,0,'criminal'),(14,1,'civil')]),
           ('halfyear_2024_h2',2024,'07-01','12-31',[(6,0,'all'),(7,1,'criminal'),(7,2,'civil')]),
           ('halfyear_2025_h1',2025,'01-01','06-30',[(2,1,'wide')]),
           ('annual_2023',2023,'01-01','12-31',[(217,0,'balochistan_categories')])]
    output=[]
    for sid,year,start,end,refs in specs:
        path=raw/'pdfs'/f'{sid}.pdf';sha=hashlib.sha256(path.read_bytes()).hexdigest()
        metadata=json.loads(path.with_suffix('.metadata.json').read_text())
        if sha!=metadata['sha256']:raise ValueError(f'Checksum mismatch: {sid}')
        with pdfplumber.open(path) as pdf:
            for p,t,cat in refs:
                tables=[x for x in pdf.pages[p-1].find_tables() if x.bbox[2]-x.bbox[0]>300]
                for ri,cells in enumerate(tables[t].extract()):
                    row=[clean(v) for v in cells if clean(v)]
                    if not row:continue
                    name=row[0]
                    if cat=='balochistan_categories':
                        if name not in ['Criminal Cases','Civil Cases','Total']:continue
                        cats=[('all' if name=='Total' else name.split()[0].lower(),row[1:])];province='Balochistan'
                    else:
                        if name not in ['Punjab','Sindh','Khyber Pakhtunkhwa','Balochistan','Islamabad','Total']:continue
                        cats=[(c,row[1+i*4:5+i*4]) for i,c in enumerate(['criminal','civil','all'])] if cat=='wide' else [(cat,row[1:])]
                        province='Pakistan' if name=='Total' else name
                    for category,values in cats:
                        if len(values)!=4:raise ValueError(f'Unexpected aggregate columns {sid}/{p}/{t}/{ri}: {row}')
                        # The asterisk in 2024 H1 refers to the source footnote;
                        # retain raw_values while parsing its numeric count.
                        nums=[integer(v.lstrip('*')) for v in values]
                        output.append(dict(source_id=sid,year=year,period_type='annual' if sid.startswith('annual') else 'half_year',period_start=f'{year}-{start}',period_end=f'{year}-{end}',
                                           province=province,geography_level='national' if province=='Pakistan' else 'province',category=category,**dict(zip(FOUR,nums)),
                                           transfers_in=None,transfers_out=None,raw_values=values,raw_cells=cells,pdf_page=p,table_index=t,row_index=ri,
                                           source_url=catalog[sid]['source_url'],source_sha256=sha,source_record_id=f'{sid}:{p}:{t}:{ri}:{category}',
                                           scope_note='Provincial district-judiciary aggregate. Half-year counts must not be mixed with annual counts. The 2025 source misprints 31 June; period_end is normalized to 30 June from the Jan–Jun title.'))
    return output
