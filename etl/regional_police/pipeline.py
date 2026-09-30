#!/usr/bin/env python3
"""Official regional crime statistics, with publication provenance and explicit gaps."""
import argparse
import base64
from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[2]
RAW = WORKSPACE / 'raw_data/regional_police'
OUT = WORKSPACE / 'data_darbar_warehouse/regional_police'
REGIONS = ['Balochistan', 'KP', 'ICT', 'GB', 'AJK']
PBS_REGIONS = ['Punjab', 'Sindh', 'KP', 'Balochistan', 'ICT', 'Railways', 'GB', 'AJK', 'Pakistan']
AJK_DISTRICTS = ['Muzaffarabad', 'Neelum', 'Jhelum Valley', 'Bagh', 'Haveli',
                 'Poonch', 'Sudhnoti', 'Kotli', 'Mirpur', 'Bhimber', 'AJK']
AJK_LABELS = ['Murder', 'Attempted Murder', 'Hurt Cases', 'Rioting', 'Assault on Govt. Servants',
              'Rape', 'Kidnapping / Abduction', 'Dacoity', 'Robbery Cases', 'Burglary Cases',
              'Motor Vehicle Theft', 'Cattle Theft', 'Ordinary Theft']
KP_LABELS = ['Murder', 'Kidnapping for Ransom', 'Child Lifting', 'Abduction',
             'Car Theft', 'Car Snatching', 'Motorcycle Theft']
NUM = re.compile(r'(?<!\S)(?:\d[\d,]*|-)(?!\S)')


def numeric(value):
    if value is None or str(value).strip() in ('', '-', '—', '–'):
        return None
    s = str(value).strip().replace(',', '')
    assert re.fullmatch(r'\d+(?:\.0)?', s), f'Unexpected numeric cell: {value!r}'
    return int(float(s))


def slug(s):
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_')


def geo_name(s):
    s = ' '.join(s.split())
    return {'D.I. Khan': 'Dera Ismail Khan', 'D.I.Khan': 'Dera Ismail Khan',
            'N.Waziristan': 'North Waziristan', 'S.Waziristan': 'South Waziristan',
            'N. Waziristan': 'North Waziristan', 'S. Waziristan': 'South Waziristan',
            'Khyber Pakhtunkhwa': 'KP', 'Total': 'KP'}.get(s, s)


def record(src, family, region, geo, level, year, label, raw, page, scope, total=False):
    value = numeric(raw)
    return dict(source_family=family, region=region, geography=geo,
                geography_id=f'{region.lower()}:{slug(geo)}', geography_level=level,
                year=int(year), period_start=f'{year}-01-01', period_end=f'{year}-12-31',
                frequency='annual', measure='reported_crime_total' if total else 'reported_offence_count',
                offence='Total' if total else label, offence_id='total' if total else slug(label),
                reporting_scope=scope, unit='reported cases', value=value,
                raw_value='' if raw is None else str(raw),
                value_status=('blank' if raw is None or str(raw).strip()=='' else 'dash') if value is None else 'reported',
                source_id=src['id'], source_url=src['url'], source_locator=str(page),
                source_sha256=src['sha256'], edition_priority=src['edition_priority'],
                quality_flags='')


def pdf_pages(src, raw):
    pdf = raw / src.get('pdf_file', src['file'])
    tool = shutil.which('pdftotext')
    if not tool:
        raise RuntimeError('Install Poppler (pdftotext) to read the original PDFs.')
    text = subprocess.check_output([tool, '-layout', str(pdf), '-'], text=True)
    return text.split('\f')


def parse_ajk(src, raw):
    pages = pdf_pages(src, raw)
    output = []
    for pn in src['pages']:
        text = pages[pn-1].split('Source:')[0]
        sections = list(re.finditer(r'^\s*(20\d\d)\s*$', text, re.M))
        for ix, match in enumerate(sections):
            year = int(match[1])
            if not 2019 <= year <= 2025:
                continue
            body = text[match.end():sections[ix+1].start() if ix+1 < len(sections) else len(text)]
            labels = AJK_LABELS + (['COVID-19'] if 'COVID-19' in body else []) + ['Misc.', 'Total']
            rows = [(line, NUM.findall(line)) for line in body.splitlines() if len(NUM.findall(line)) == 11]
            assert len(rows) == len(labels), (src['id'], pn, year, len(rows), len(labels))
            for label, (line, vals) in zip(labels, rows):
                # Multiline labels place some numeric rows on an otherwise empty line.
                # Nonempty labels must match the expected category order.
                prefix = line[:NUM.search(line).start()].strip()
                assert not prefix or label.startswith(prefix), (src['id'], label, prefix)
                for geo, val in zip(AJK_DISTRICTS, vals):
                    output.append(record(src, 'ajk_police_yearbooks', 'AJK', geo,
                        'region' if geo == 'AJK' else 'district', year, label, val,
                        f'PDF page {pn}, table 17.3', 'all_reported_crimes', label == 'Total'))
    return output


def parse_kp(src, raw):
    import pdfplumber
    output = []
    with pdfplumber.open(raw / src.get('pdf_file', src['file'])) as pdf:
        for pn in src['pages']:
            tables = [t for t in pdf.pages[pn-1].extract_tables() if len(t[0]) == 15]
            assert len(tables) == 1, (src['id'], 'KP table shape changed')
            table = tables[0]
            if src['id'] != 'kp_2020':
                assert 'Murder' in str(table[0]) and 'Abduction' in str(table[0])
            years = [int(s) for s in table[1][1:]]
            assert len(years) == 14 and years == years[:2] * 7
            if src['id'] == 'kp_2020':
                assert years == [2018, 2019] * 7
            assert len(table[2:]) in (33, 36), (src['id'], len(table))
            for row in table[2:]:
                assert len(row) == 15 and row[0]
                geo = geo_name(row[0])
                for ix, (year, val) in enumerate(zip(years, row[1:])):
                    if not 2019 <= year <= 2025:
                        continue
                    output.append(record(src, 'kp_police_yearbooks', 'KP', geo,
                        'region' if geo == 'KP' else 'district', year, KP_LABELS[ix//2], val,
                        f'PDF page {pn}, district crime table', 'seven_selected_offences'))
    return output


def parse_pbs_pdf(src, raw):
    pages = pdf_pages(src, raw)
    output = []
    year = None
    for pn in src['pages']:
        for line in pages[pn-1].splitlines():
            if re.fullmatch(r'\s*20\d\d\s*', line):
                year = int(line)
                continue
            matches = list(NUM.finditer(line))
            if year not in src['years'] or len(matches) != 9:
                continue
            label = line[:matches[0].start()].strip()
            assert label, (src['id'], line)
            for geo, m in zip(PBS_REGIONS, matches):
                output.append(record(src, 'pbs_national_police_bureau', geo, geo,
                    'region', year, label, m[0], f'PDF page {pn}',
                    'all_recorded_crime', label == 'TOTAL RECORDED CRIME'))
    return output


def parse_pbs_xlsx(src, raw):
    import openpyxl
    wb = openpyxl.load_workbook(raw / src['file'], data_only=True)
    output, year = [], None
    for n, row in enumerate(wb.active.iter_rows(values_only=True), 1):
        vals = list(row)
        if any(str(x).strip() == 'Punjab' for x in vals):
            headers = [str(x).strip() for x in vals]
            assert headers[1:5] == ['Punjab', 'Sindh', 'KP', 'Balochistan']
        years = [int(x) for x in vals if str(x).strip() in ('2022', '2023', '2024')]
        if len(years) == 1 and sum(x is not None for x in vals) == 1:
            year = years[0]
            continue
        if year is None or vals[0] is None or not isinstance(vals[1], (int, float)):
            continue
        label = str(vals[0]).strip()
        for geo, v in zip(PBS_REGIONS, vals[1:]):
            output.append(record(src, 'pbs_national_police_bureau', geo, geo, 'region', year,
                label, v, f'Sheet1 row {n}', 'all_recorded_crime', 'TOTAL' in label.upper()))
    wb.close()
    return output


def parse_baloch(src, raw):
    output = []
    pages = pdf_pages(src, raw)
    for pn in src['pages']:
        text = pages[pn-1]
        header = next(l for l in text.splitlines() if len(NUM.findall(l)) == 10 and '2019' in l)
        years = [int(v) for v in NUM.findall(header)]
        assert years == list(range(2019,2024)) * 2
        for line in text.splitlines():
            matches = list(NUM.finditer(line))
            if len(matches) != 10 or line == header:
                continue
            label = line[:matches[0].start()].strip()
            assert label
            for ix, (year, m) in enumerate(zip(years, matches)):
                scope = ('police' if ix < 5 else 'levies') + '_area_selected_offences_and_accidents'
                r = record(src, 'baloch_home_department', 'Balochistan', 'Balochistan', 'region',
                    year, label, m[0], f'PDF page {pn}, table 1', scope)
                # A grand total of listed offences and accidents is not an all-FIR total.
                if label == 'Grand Total':
                    r.update(measure='selected_offences_and_accidents_total', offence_id='selected_total')
                output.append(r)
    return output


def row_key(r):
    return tuple(r[k] for k in ('source_family','region','geography','geography_level','year',
                               'reporting_scope','measure','offence_id'))


def select_latest(rows):
    groups = defaultdict(list)
    for r in rows:
        groups[(r['source_family'],r['year'],r['reporting_scope'])].append(r)
    selected, revisions = [], []
    for _, variants in sorted(groups.items()):
        latest=max(variants,key=lambda r:(r['edition_priority'],r['source_id']))['source_id']
        chosen={row_key(r):r.copy() for r in variants if r['source_id']==latest}
        selected.extend(chosen.values())
        for old in [r for r in variants if r['source_id']!=latest]:
            new=chosen.get(row_key(old))
            if new is None or (old['value'],old['value_status']) != (new['value'],new['value_status']):
                revisions.append(dict(region=old['region'], geography=old['geography'], year=old['year'],
                    offence=old['offence'], old_source=old['source_id'], new_source=latest,
                    old_value=old['value'], new_value=new['value'] if new else None,
                    old_status=old['value_status'], new_status=new['value_status'] if new else 'unit_or_category_absent_in_latest_edition'))
    return sorted(selected,key=row_key), revisions


def check_sum(kind, parent, children):
    known = sum(r['value'] for r in children if r['value'] is not None)
    missing = sum(r['value'] is None for r in children)
    total = parent['value']
    status = ('uncheckable' if total is None else
              'mismatch' if known > total or (not missing and known != total) else
              'uncheckable' if missing else 'pass')
    return dict(check=kind, source_id=parent['source_id'], region=parent['region'],
                geography=parent['geography'], year=parent['year'], offence=parent['offence'],
                reporting_scope=parent['reporting_scope'], reported_total=total,
                known_sum=known, missing_cells=missing, status=status)


def validate(rows):
    checks, flags = [], defaultdict(set)
    grouped = defaultdict(list)
    for r in rows:
        grouped[(r['source_id'],r['year'],r['reporting_scope'])].append(r)
    for _, group in grouped.items():
        family = group[0]['source_family']
        if family in ('ajk_police_yearbooks', 'kp_police_yearbooks'):
            for parent in [r for r in group if r['geography_level']=='region']:
                children = [r for r in group if r['geography_level']=='district' and r['offence_id']==parent['offence_id']]
                checks.append(check_sum('district_sum', parent, children))
        if family in ('ajk_police_yearbooks','pbs_national_police_bureau','baloch_home_department'):
            for parent in [r for r in group if r['measure'] in ('reported_crime_total','selected_offences_and_accidents_total')]:
                children = [r for r in group if r['geography']==parent['geography'] and r['measure']=='reported_offence_count']
                checks.append(check_sum('category_sum', parent, children))
        if family == 'pbs_national_police_bureau':
            for parent in [r for r in group if r['region']=='Pakistan']:
                children = [r for r in group if r['region']!='Pakistan' and r['offence_id']==parent['offence_id']]
                checks.append(check_sum('regional_sum', parent, children))
    for c in checks:
        if c['status']=='mismatch':
            flags[(c['source_id'], c['year'])].add('published_table_arithmetic_mismatch')
    for r in rows:
        f = set(flags[(r['source_id'],r['year'])])
        if r['region']=='KP' and r['geography_level']=='district':
            if r['geography'] in ('Chitral','Kohistan','South Waziristan'):
                f.add('historical_or_combined_district_boundary')
            if r['geography']=='Malakand' or (r['year']<=2020 and r['geography'] in
                    ('Bajaur','Khyber','Kurram','Mohmand','Orakzai','North Waziristan','South Waziristan')):
                f.add('reporting_coverage_requires_caution')
        r['quality_flags']=';'.join(sorted(f))
    return checks


def write_table(out, name, rows):
    (out / (name+'.json')).write_text(json.dumps(rows, indent=2, ensure_ascii=False)+'\n')
    if not rows:
        return
    with (out / (name+'.csv')).open('w', newline='') as f:
        w=csv.DictWriter(f, fieldnames=list(rows[0]));w.writeheader();w.writerows(rows)
    import duckdb
    con=duckdb.connect(':memory:')
    con.read_json(str(out/(name+'.json'))).write_parquet(str(out/(name+'.parquet')))
    con.close()


def fetch(sources, raw):
    import requests
    raw.mkdir(parents=True, exist_ok=True)
    for src in sources:
        if src['adapter']=='reference_only':
            continue
        p = raw/src['file']
        if not p.exists():
            response=requests.get(src['url'], timeout=120);response.raise_for_status()
            data=response.content
            if '/DownloadAttachment/' in src['url']:
                payload=response.json()[0]
                assert payload.startswith('data:')
                data=base64.b64decode(payload.split(',',1)[1])
            assert hashlib.sha256(data).hexdigest()==src['sha256'], f"Changed source {src['id']}; review and version the manifest"
            p.parent.mkdir(parents=True, exist_ok=True);p.write_bytes(data)
        if 'pdf_file' in src and not (raw/src['pdf_file']).exists():
            members=subprocess.check_output(['tar','-tf',str(p)],text=True).splitlines()
            member=next(n for n in members if n.endswith('.pdf'))
            with (raw/src['pdf_file']).open('wb') as f:
                subprocess.run(['tar','-xOf',str(p),member],stdout=f,check=True)


def build(raw=RAW, out=OUT):
    sources=json.loads((HERE/'sources.json').read_text())
    adapters={'ajk':parse_ajk,'kp':parse_kp,'pbs_pdf':parse_pbs_pdf,
              'pbs_xlsx':parse_pbs_xlsx,'baloch':parse_baloch}
    observations=[]
    for src in sources:
        if src['adapter'] not in adapters:
            continue
        assert hashlib.sha256((raw/src['file']).read_bytes()).hexdigest()==src['sha256'], src['id']
        if 'pdf_file' in src:
            assert hashlib.sha256((raw/src['pdf_file']).read_bytes()).hexdigest()==src['pdf_sha256'], src['id']
        parsed=adapters[src['adapter']](src,raw)
        assert parsed, f"No observations extracted: {src['id']}"
        assert len({row_key(r) for r in parsed})==len(parsed), f"Duplicate observations in {src['id']}"
        observations.extend(parsed)
    # Check each original edition, then selected tables, preserving source discrepancies.
    checks=validate(observations)
    rows,revisions=select_latest(observations)
    assert len({row_key(r) for r in rows})==len(rows)
    regional=[r for r in rows if r['region'] in REGIONS and r['source_family']=='pbs_national_police_bureau']
    totals=[r for r in regional if r['measure']=='reported_crime_total']
    assert len(totals)==30 and {r['year'] for r in totals}==set(range(2019,2025)), 'PBS coverage changed'
    districts=[r for r in rows if r['geography_level']=='district']
    district_totals=[r for r in districts if r['measure']=='reported_crime_total']
    assert len(district_totals)==60, 'AJK district total coverage changed'
    differences=[]
    for ajk in [r for r in rows if r['source_family']=='ajk_police_yearbooks' and r['geography']=='AJK' and r['measure']=='reported_crime_total']:
        pbs=next(r for r in totals if r['region']=='AJK' and r['year']==ajk['year'])
        if pbs['value']!=ajk['value']:
            differences.append(dict(region='AJK',year=ajk['year'],pbs_total=pbs['value'],
                provincial_total=ajk['value'],difference=ajk['value']-pbs['value'],
                pbs_source=pbs['source_id'],provincial_source=ajk['source_id']))
    for r in rows:
        if r['region']=='AJK' and any(d['year']==r['year'] for d in differences):
            r['quality_flags']=';'.join(filter(None,[r['quality_flags'],'pbs_and_ajk_annual_totals_differ']))
    coverage=[]
    for region in REGIONS:
        for year in range(2019,2026):
            ds=[r for r in districts if r['region']==region and r['year']==year]
            coverage.append(dict(region=region,year=year,
                regional_total_status='available' if any(r['region']==region and r['year']==year for r in totals) else 'not_recovered',
                district_total_status='available_with_source_flags' if any(r['measure']=='reported_crime_total' for r in ds) else 'not_recovered',
                district_offence_status='available' if ds else 'not_recovered',
                district_units=len({r['geography'] for r in ds}),
                district_daily_fir_status='not_recovered',district_ytd_fir_status='not_recovered'))
    geos=[]
    for region,geo in sorted({(r['region'],r['geography']) for r in districts}):
        yrs=sorted({r['year'] for r in districts if r['region']==region and r['geography']==geo})
        geos.append(dict(region=region,geography=geo,geography_id=f'{region.lower()}:{slug(geo)}',
            years=','.join(map(str,yrs)),adm2_id=None,adm2_join_status='requires_boundary_crosswalk',
            note='Historical/combined unit; do not split counts across modern districts' if geo in ('Chitral','Kohistan','South Waziristan') else 'Source district name normalized; census code not assigned'))
    out.mkdir(parents=True,exist_ok=True)
    for name,data in [('observations',observations),('crime_annual',rows),('regional_crime_annual',regional),
        ('regional_totals',totals),('district_crime_annual',districts),('district_totals',district_totals),
        ('baloch_police_levies_annual',[r for r in rows if r['source_family']=='baloch_home_department']),
        ('source_revisions',revisions),('total_checks',checks),('cross_source_differences',differences),
        ('year_coverage',coverage),('geography_crosswalk',geos)]:write_table(out,name,data)
    (out/'sources.json').write_text(json.dumps(sources,indent=2)+'\n')
    report=dict(regional_total_rows=len(totals),district_rows=len(districts),district_total_rows=len(district_totals),
        observations=len(observations),selected_rows=len(rows),source_revision_count=len(revisions),
        checks=dict(Counter(c['status'] for c in checks)),cross_source_differences=len(differences),
        daily_or_ytd_rows=0,reference_years=[2019,2020,2021,2022,2023,2024],
        missing_reference_years=[2025],null_district_cells=sum(r['value'] is None for r in districts),
        interpretation='Annual reported cases, not a verified all-FIR register; seven KP offences are not exhaustive. Dashes and blanks are null. Source arithmetic discrepancies are preserved and flagged.')
    (out/'quality_report.json').write_text(json.dumps(report,indent=2)+'\n')
    return report


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['build','fetch'])
    p.add_argument('--raw',type=Path,default=RAW)
    p.add_argument('--out',type=Path,default=OUT)
    a=p.parse_args()
    if a.command=='fetch':fetch(json.loads((HERE/'sources.json').read_text()),a.raw)
    else:print(json.dumps(build(a.raw,a.out),indent=2))


if __name__=='__main__':
    main()
