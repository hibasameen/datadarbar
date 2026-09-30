"""LJCP edition-specific table extraction. Run with Python + pdfplumber.

All page numbers are one-based PDF pages, not printed page labels. Tables are
numbered from zero after filtering out narrow spurious header fragments.
"""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import re
import pdfplumber

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[2]
FIELDS = ['pending_start', 'instituted', 'transfers_in', 'transfers_out', 'disposed', 'pending_end']
STANDARD = FIELDS
RECEIVED = ['pending_start', 'instituted', 'transfers_in', 'disposed', 'transfers_out', 'pending_end']
FOUR = ['pending_start', 'instituted', 'disposed', 'pending_end']
JUDGES = ['sanctioned', 'working_male', 'working_female', 'excadre_male', 'excadre_female', 'vacant']


def clean(value):
    return re.sub(r'\s+', ' ', value or '').strip()


def integer(value):
    s = clean(value).replace(',', '')
    if s in ('', '-', '--', '—', '–', 'N/A'):
        return None
    if not re.fullmatch(r'-?\d+', s):
        raise ValueError(f'Not an integer or missing marker: {value!r}')
    return int(s)


def is_total(name):
    return bool(re.fullmatch(r'(grand\s+)?total[:\- ]*', name, re.I))


def data_name(row, serial):
    r = [clean(v) for v in row if clean(v)]
    if not r:
        return None
    if is_total(r[0]):
        return r[0]
    if serial:
        if len(r) > 1 and re.fullmatch(r'\d+[.]?', r[0]) and re.search('[A-Za-z]', r[1]):
            return r[1]
    elif re.search('[A-Za-z]', r[0]) and len(r) > 1:
        try:
            integer(r[1])
            return r[0]
        except ValueError:
            pass
    return None


def read_table(page, table_index, fields, serial=True, ignore_names=()):
    tables = [t for t in page.find_tables() if t.bbox[2] - t.bbox[0] > 300]
    table = tables[table_index]
    rows = table.extract()
    if ignore_names:
        # Header fragments such as "2024 | Out" can look like a serial + name row.
        rows = [row if data_name(row, serial) not in ignore_names else [None] * len(row) for row in rows]
    # Use actual numeric cell coordinates, not positions of None placeholders in
    # pdfplumber's ragged merged-cell arrays. This also recovers text in cells that
    # the grid detector incorrectly marks as None (e.g. Uthal 2024: 244).
    anchors = []
    for ri, row in enumerate(rows):
        name = data_name(row, serial)
        if not name or is_total(name):
            continue
        nonempty = [(clean(v), c) for v, c in zip(row, table.rows[ri].cells) if clean(v) and c]
        prefix = 2 if serial else 1
        vals = nonempty[prefix:]
        if len(vals) == len(fields):
            try:
                [integer(v) for v, c in vals]
                anchors.append([c for v, c in vals])
            except ValueError:
                pass
    if not anchors:
        for ri,row in enumerate(rows):
            name=data_name(row,serial)
            if name and is_total(name):
                vals=[(clean(v),c) for v,c in zip(row,table.rows[ri].cells) if clean(v) and c][1:]
                if len(vals)==len(fields):
                    try:
                        [integer(v) for v,c in vals];anchors.append([c for v,c in vals])
                    except ValueError:pass
    anchor = anchors[0] if anchors else None
    words = page.extract_words(x_tolerance=1, y_tolerance=2)
    results = []
    for ri, row in enumerate(rows):
        name = data_name(row, serial)
        if not name:
            continue
        compact = [clean(v) for v in row if clean(v)]
        prefix = 1 if is_total(name) or not serial else 2
        compact_values = compact[prefix:]
        if anchor:
            # A shared ex-cadre cell can span five Karachi rows. Its bbox must
            # not extend the reading band for all the other numeric columns.
            named = [(clean(v), c) for v, c in zip(row, table.rows[ri].cells) if clean(v) and c]
            bbox = named[0][1] if serial and not is_total(name) else next((c for v,c in named if v == name), table.rows[ri].bbox)
            raw = []
            for cell in anchor:
                selected = [w for w in words if cell[0] - .2 <= (w['x0'] + w['x1']) / 2 <= cell[2] + .2
                            and bbox[1] <= (w['top'] + w['bottom']) / 2 < bbox[3]]
                raw.append(' '.join(w['text'] for w in selected))
        else:
            # A continuation page can contain only the grand total.
            raw = compact_values
            if len(raw)!=len(fields):
                unmerged=[clean(v) for v in row if v is not None]
                if name in unmerged:
                    preserved=unmerged[unmerged.index(name)+1:]
                    if len(preserved)==len(fields):raw=preserved
        if len(raw) != len(fields):
            raise ValueError(f'Unexpected number of columns, page {page.page_number}, table {table_index}, {name}: {raw}')
        values = {f: integer(v) for f, v in zip(fields, raw)}
        results.append(dict(name=name, **values, raw_values=raw, raw_cells=row,
                            pdf_page=page.page_number, table_index=table_index, row_index=ri,
                            is_total=is_total(name)))
    if not results:
        raise ValueError(f'No data on page {page.page_number}, table {table_index}')
    return results


def specs():
    out = []
    def add(year, province, category, refs, section, tier='all_courts', order=STANDARD, count=None, note=''):
        out.append(dict(source_id=f'annual_{year}', year=year, province=province,
                        category=category, court_tier=tier, refs=refs, section=section,
                        fields=order, expected_rows=count, scope_note=note, kind='cases', serial=True))
    for y, a in [(2020,58),(2022,57),(2023,65)]:
        if y == 2020:
            add(y,'Punjab','all',[(58,0)],'3.19','civil_courts',count=36)
            add(y,'Punjab','all',[(60,1),(61,0)],'3.22','sessions_courts',RECEIVED,36)
            add(y,'Punjab','civil',[(58,1),(59,0)],'3.20','civil_courts',count=36)
            add(y,'Punjab','criminal',[(59,1),(60,0)],'3.21','civil_courts',count=36)
        elif y == 2022:
            add(y,'Punjab','all',[(57,0)],'3.25','civil_courts',count=36)
            add(y,'Punjab','all',[(56,0)],'3.24','sessions_courts',RECEIVED,36,
                'Source swaps Disposal/Transferred header labels; value order follows magnitudes and parallel sessions tables; raw cells retained.')
            for cat,p,t,order in [('civil',58,'civil_courts',STANDARD),('criminal',59,'civil_courts',STANDARD),('civil',60,'sessions_courts',RECEIVED),('criminal',61,'sessions_courts',RECEIVED)]:
                add(y,'Punjab',cat,[(p,0)],f'3.{p-32}',t,order,36)
        else:
            for cat,p,t in [('all',65,'civil_courts'),('all',66,'sessions_courts'),('civil',67,'civil_courts'),('criminal',68,'civil_courts'),('civil',69,'sessions_courts'),('criminal',70,'sessions_courts')]:
                add(y,'Punjab',cat,[(p,0)],f'3.{p-51}',t,RECEIVED,36)
    for cat,refs,s in [('all',[(73,2),(74,0)],'3.15'),('civil',[(74,1),(75,0)],'3.16'),('criminal',[(78,0),(79,0)],'3.18')]:
        add(2024,'Punjab',cat,refs,s,count=36,note='Civil and criminal categories do not exhaust the all-cases total; miscellaneous cases remain outside the split.')
    for y,cat,refs,s,n in [
        (2020,'all',[(95,0)],'4.23',27),(2020,'civil',[(95,1),(96,0)],'4.24',27),(2020,'criminal',[(96,1)],'4.25',27),
        (2022,'all',[(101,0)],'4.23',27),(2022,'civil',[(101,1),(102,0)],'4.24',27),(2022,'criminal',[(102,1),(103,0)],'4.25',27),
        (2023,'all',[(124,2),(125,0)],'4.14',28),(2023,'civil',[(125,1),(126,0)],'4.15',28),(2023,'criminal',[(127,1),(128,0)],'4.17',28),
        (2024,'all',[(137,0)],'4.18',28),(2024,'civil',[(138,0)],'4.19',28),(2024,'criminal',[(140,0)],'4.21',28)]:
        add(y,'Sindh',cat,refs,s,count=n)
    for y,cat,refs,s in [
        (2020,'all',[(130,0)],'5.28'),(2020,'civil',[(130,1),(131,0)],'5.29'),(2020,'criminal',[(131,1),(132,0)],'5.30'),
        (2022,'all',[(135,0)],'5.28'),(2022,'civil',[(136,0)],'5.29'),(2022,'criminal',[(136,1),(137,0)],'5.30'),
        (2023,'all',[(178,0)],'5.14'),(2023,'civil',[(180,0)],'5.16'),(2023,'criminal',[(179,0)],'5.15'),
        (2024,'all',[(188,0)],'5.18'),(2024,'civil',[(189,0)],'5.19'),(2024,'criminal',[(191,0)],'5.21')]:
        add(y,'Khyber Pakhtunkhwa',cat,refs,s,count=35)
    for y,cat,refs,s,n in [
        (2020,'all',[(157,1)],'6.22',25),(2020,'civil',[(157,2),(158,0)],'6.23',25),
        (2020,'criminal',[(158,1)],'6.24',25),
        (2022,'all',[(165,0)],'6.22',29),(2022,'civil',[(165,1),(166,0)],'6.23',29),(2022,'criminal',[(166,1),(167,0)],'6.24',29),
        (2024,'all',[(239,1),(240,0)],'unnumbered consolidated after 6.38',34),
        (2024,'civil',[(238,1),(239,0)],'unnumbered civil after 6.38',34)]:
        add(y,'Balochistan',cat,refs,s,order=FOUR,count=n,
            note='2020 criminal table duplicates the all-cases table; excluded from curated criminal series.' if y==2020 and cat=='criminal' else '')
    for y,cat,refs,s in [
        (2020,'all',[(179,0)],'7.19'),(2020,'civil',[(179,1)],'7.20'),(2020,'criminal',[(179,2)],'7.21'),
        (2022,'all',[(191,0)],'7.19'),(2022,'civil',[(191,1)],'7.20'),(2022,'criminal',[(191,2)],'7.21'),
        (2023,'all',[(267,1)],'7.15'),(2023,'civil',[(267,2)],'7.16'),(2023,'criminal',[(268,1)],'7.18'),
        (2024,'all',[(274,1)],'7.17'),(2024,'civil',[(275,0)],'7.21'),(2024,'criminal',[(275,2)],'7.23')]:
        add(y,'Islamabad',cat,refs,s,order=FOUR if y==2022 or (y==2020 and cat=='criminal') else STANDARD,count=2,
            note='Civil/criminal table scope varies by edition and may exclude appeals, bail, family or miscellaneous categories. Compare all-cases totals across years.')
    def judge(y,province,refs,section,fields=JUDGES,serial=False,note=''):
        out.append(dict(source_id=f'annual_{y}',year=y,province=province,category='judges',court_tier='all_courts',refs=refs,
                        section=section,fields=fields,serial=serial,expected_rows=None,scope_note=note,kind='judges'))
    # Older reports provide total sanctioned/working/vacant columns after ranks.
    triple = ['sanctioned','working','vacant']
    judge(2020,'Punjab',[(55,0),(56,0)],'3.17',[f'rank{i}_{f}' for i in range(4) for f in triple]+triple)
    judge(2020,'Sindh',[(93,0)],'4.21',[f'rank{i}_{f}' for i in range(4) for f in triple]+triple)
    kp2020 = ['r0_s','r0_w','r0_v','r1_s','r1_w','r1_leave','r1_v','r2_s','r2_w','r2_v','r3_s','r3_w','r3_v']+triple
    judge(2020,'Khyber Pakhtunkhwa',[(127,0),(128,0)],'5.26',kp2020,True)
    quadruple=['sanctioned','working','excadre','vacant']
    judge(2020,'Balochistan',[(153,0),(154,0),(155,0)],'6.17',[f'rank{i}_{f}' for i in range(6) for f in quadruple]+quadruple)
    judge(2023,'Punjab',[(57,0)],'3.11.2 A')
    judge(2024,'Punjab',[(66,0),(67,0)],'3.13.1')
    judge(2023,'Sindh',[(119,0)],'4.11.2.1',serial=True)
    judge(2024,'Sindh',[(131,0)],'4.16 consolidated')
    judge(2023,'Khyber Pakhtunkhwa',[(170,0)],'5.12.2.1',serial=True)
    judge(2023,'Islamabad',[(264,0)],'7.13.1',note='Sanctioned and vacant cells span several institutions. Retain only division-specific working strength for East/West indicators.')
    judge(2024,'Islamabad',[(271,0)],'7.14 consolidated',note='Sanctioned and vacant cells span several institutions. Retain only division-specific working strength for East/West indicators.')
    for province,refs,sec,serial,total in [
        ('Punjab',[(53,0),(54,0)],'3.22',False,False),('Sindh',[(98,0),(99,0)],'4.21',False,False),
        ('Khyber Pakhtunkhwa',[(132,0),(133,0)],'5.26',True,True),('Balochistan',[(162,0),(163,0)],'6.18',False,False),
        ('Islamabad',[(189,0)],'7.17',False,False)]:
        fs = [f'rank{i}_{f}' for i in range(4) for f in JUDGES]+(JUDGES if total else [])
        judge(2022,province,refs,sec,fs,serial,
              note='Printed strength reference date is 31-12-2021 although edition is 2022; not joined to 2022 case indicators.' if province=='Punjab' else '')
    for y,rank,refs,section in [
        (2023,'district_sessions_judges',[(213,0)],'6.14.2.1'),
        (2023,'additional_district_sessions_judges',[(214,0)],'6.14.2.2'),
        (2023,'senior_civil_judges',[(214,1),(215,0)],'6.14.2.3'),
        (2023,'civil_judges_magistrates_family_judges',[(215,1),(216,0)],'6.14.2.4'),
        (2023,'majlis_e_shoora',[(216,1)],'6.14.2.5'),(2023,'qazi',[(216,2)],'6.14.2.6'),
        (2024,'district_sessions_judges',[(233,1),(234,0)],'6.32'),
        (2024,'additional_district_sessions_judges',[(234,1),(235,0)],'6.33'),
        (2024,'senior_civil_judges',[(235,1)],'6.34'),
        (2024,'civil_judges_magistrates_family_judges',[(236,0)],'6.35'),
        (2024,'majlis_e_shoora',[(237,0)],'6.36'),(2024,'qazi',[(237,1)],'6.37')]:
        judge(y,'Balochistan',refs,section,note='Rank-specific table; jurisdiction names differ from the case-flow sessions tables. No unreviewed aggregation to district staffing.')
        out[-1]['court_tier']=rank
    # Keep ICT's province-wide sanctioned total separate from East/West working
    # counts: the merged sanctioned cell cannot be assigned to each division.
    return out


def scanned_2021(raw,catalog):
    from ocr_tables import reviewed_tables
    expected_sha='97a5093dc711a03022a47015bff1f94e2e3078119a056cc0044a1c76cf07b75d'
    digest=hashlib.sha256((raw/'pdfs/annual_2021.pdf').read_bytes()).hexdigest()
    if digest!=expected_sha:raise ValueError('2021 PDF differs from the visually reviewed OCR edition')
    tables=reviewed_tables(raw)
    configs=[('Punjab','all',[(65,0)],'3.23',36),('Punjab','civil',[(65,1),(66,0)],'3.24',36),('Punjab','criminal',[(66,1),(67,0)],'3.25',36),
             ('Sindh','all',[(110,0)],'4.23',27),('Sindh','civil',[(110,1),(111,0)],'4.24',27),('Sindh','criminal',[(112,0)],'4.26',27),
             ('Khyber Pakhtunkhwa','all',[(149,0)],'5.34',35),('Khyber Pakhtunkhwa','civil',[(149,1),(150,0)],'5.35',35),('Khyber Pakhtunkhwa','criminal',[(151,1),(152,0)],'5.37',35),
             ('Balochistan','all',[(184,1)],'6.22',26),('Balochistan','civil',[(184,2),(185,0)],'6.23',26),('Balochistan','criminal',[(185,1)],'6.24',26),
             ('Islamabad','all',[(213,0)],'7.19',2),('Islamabad','civil',[(213,1)],'7.20',2),('Islamabad','criminal',[(213,2)],'7.21',2)]
    output=[]
    for province,category,refs,section,count in configs:
        parsed=[]
        for p,t in refs:
            for ri,cells in enumerate(tables[p,t]['rows']):
                if ri==0 or not cells[1]:continue
                name=clean(cells[1]);fields=FOUR if province=='Balochistan' else FIELDS
                vals=[integer(v) for v in cells[2:]]
                if len(vals)!=len(fields) or any(v is None for v in vals):raise ValueError(f'Unverified OCR cell {p}/{t}/{ri}: {cells}')
                parsed.append(dict(name=name,**dict(zip(fields,vals)),raw_values=cells[2:],raw_cells=cells,pdf_page=p,table_index=t,row_index=ri,is_total=is_total(name),
                                   source_id='annual_2021',year=2021,province=province,category=category,court_tier='all_courts',section=section,kind='cases',
                                   scope_note='Scanned edition: column OCR, cell fallback and explicit visual corrections. Civil/criminal categories are edition-specific.',
                                   extraction_method='vision_ocr_with_visual_review',source_url=catalog['annual_2021']['source_url'],source_sha256=digest,
                                   record_id=f'annual_2021:{province}:{section}:{p}:{t}:{ri}'))
        if sum(not r['is_total'] for r in parsed)!=count:raise ValueError(f'OCR row count mismatch {province} {category}')
        output+=parsed
    return output


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw',type=Path,default=WORKSPACE/'raw_data/ljcp')
    ap.add_argument('--out',type=Path,default=WORKSPACE/'data_darbar_warehouse/ljcp')
    args = ap.parse_args()
    args.out.mkdir(parents=True,exist_ok=True)
    catalog = {s['id']:s for s in json.loads((HERE/'sources.json').read_text())}
    configs=specs(); (args.out/'table_specs.json').write_text(json.dumps(configs,indent=2))
    records=[]; failures=[]; handles={}
    try:
        for cfg in configs:
            sid=cfg['source_id']
            if sid not in handles:
                path=args.raw/'pdfs'/f'{sid}.pdf'
                digest=hashlib.sha256(path.read_bytes()).hexdigest()
                metadata=json.loads(path.with_suffix('.metadata.json').read_text())
                if digest != metadata['sha256']:
                    raise ValueError(f'PDF checksum mismatch: {path}')
                if catalog[sid].get('sha256') and digest!=catalog[sid]['sha256']:
                    raise ValueError(f'PDF differs from pinned source edition: {sid}')
                handles[sid]=(pdfplumber.open(path),digest)
            pdf,digest=handles[sid]
            parsed=[]
            try:
                for p,t in cfg['refs']:
                    parsed += read_table(pdf.pages[p-1],t,cfg['fields'],cfg['serial'])
                n=sum(not r['is_total'] for r in parsed)
                if cfg['expected_rows'] is not None and n != cfg['expected_rows']:
                    raise ValueError(f"Expected {cfg['expected_rows']} rows, found {n}")
                for r in parsed:
                    r.update({k:v for k,v in cfg.items() if k not in ('refs','fields','serial','expected_rows')})
                    r['source_url']=catalog[sid]['source_url'];r['source_sha256']=digest
                    r['record_id']=f"{sid}:{cfg['province']}:{cfg['section']}:{r['pdf_page']}:{r['table_index']}:{r['row_index']}"
                records+=parsed
            except Exception as exc:
                failures.append(dict(source_id=sid,province=cfg['province'],section=cfg['section'],error=str(exc)))
    finally:
        for pdf,digest in handles.values():pdf.close()
    try:records+=scanned_2021(args.raw,catalog)
    except Exception as exc:failures.append(dict(source_id='annual_2021',error=str(exc)))
    (args.out/'source_observations.json').write_text(json.dumps(records,indent=2,ensure_ascii=False))
    (args.out/'extraction_failures.json').write_text(json.dumps(failures,indent=2))
    print(json.dumps(dict(records=len(records),failures=failures),indent=2))
    if failures:raise SystemExit(1)


if __name__=='__main__':main()
