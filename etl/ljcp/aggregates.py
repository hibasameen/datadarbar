"""Extract separately dated provincial mid-year tables, never annualize them."""
import hashlib
import json
import pdfplumber
from extract import HERE, FOUR, clean, integer

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
