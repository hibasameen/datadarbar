#!/usr/bin/env python3
"""Recover, import and build Sindh Police district daily/YTD FIR snapshots.

Only section 2 (all registered FIRs) is accepted. Section 3 counts FIRs from
Emergency-15 complaints and must never enter this series.
"""
from __future__ import annotations
import argparse
import calendar
import csv
import hashlib
import json
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import urlopen

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
RAW = ROOT / 'raw_data/sindh_fir'
OUT = ROOT / 'data_darbar_warehouse/sindh_fir'
TITLE = '2. FIRs REGISTERED IN SINDH PROVINCE (DAILY BASIS)'
URL_RE = r'https?://(?:www\.)?sindhpolice\.gov\.pk/storage/dsr/[\w.-]+\.pdf'
GROUPS = [
    ('Karachi', ['SOUTH KARACHI', 'CITY KARACHI', 'KEAMARI KARACHI', 'EAST KARACHI',
                 'MALIR KARACHI', 'KORANGI KARACHI', 'WEST KARACHI', 'CENTRAL KARACHI']),
    ('Hyderabad', ['HYDERABAD', 'JAMSHORO', 'MATIARI', 'THATTA', 'SUJAWAL', 'BADIN',
                   'DADU', 'TANDO ALLAH YAR', 'TANDO M KHAN']),
    ('Mirpurkhas', ['MIRPURKHAS', 'UMARKOT', 'MITHI@ THARPARKAR']),
    ('Shaheed Benazirabad', ['SHAHEED BEANZIRABAD', 'SANGHAR', 'NAUSHERO FEROZ']),
    ('Sukkur', ['SUKKUR', 'GHOTKI', 'KHAIRPUR']),
    ('Larkana', ['LARKANA', 'QAMBAR', 'SHIKARPUR', 'JACOBABAD', 'KASHMORE']),
]

def compact(x):
    return re.sub(r'\s+', ' ', str(x or '')).strip()

def slug(x):
    return re.sub(r'[^a-z0-9]+', '_', x.lower()).strip('_')

def sha(data):
    return hashlib.sha256(data).hexdigest()

def dump(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False) + '\n')

def value(cell):
    cell = compact(cell)
    if cell in ('', '-', '–', '—'):
        return None
    if not re.fullmatch(r'\d+|\d{1,3}(?:,\d{3})+', cell):
        raise ValueError(f'Invalid FIR count {cell!r}')
    return int(cell.replace(',', ''))

def dates(text):
    tokens = re.findall(r'\b\d{2}-\d{2}-\d{4}\b', text)
    parsed = sorted(set(datetime.strptime(s, '%d-%m-%Y').date() for s in tokens))
    if len(parsed) != 2 or parsed[0] != date(parsed[1].year, 1, 1):
        raise ValueError(f'Expected one daily date and same-year January 1: {parsed}')
    return parsed[1].isoformat(), parsed[0].isoformat()

def parse_rows(rows, text):
    """Validate the complete known 31-district layout; refuse changed layouts."""
    report_date, ytd_start = dates(text)
    data = []
    for cells in rows:
        c = [compact(x) for x in cells]
        if len(c) != 4:
            continue
        label = c[0].replace(' ', '').upper()
        if c[0].isdigit() or label in ('TOTAL', 'GRANDTOTAL'):
            data.append(c)
    if len(data) != 38:
        raise ValueError(f'Incomplete/unrecognized FIR table: {len(data)} rows, expected 38')
    out, position = [], 0
    for range_name, districts in GROUPS:
        for seq, district in enumerate(districts, 1):
            row = data[position]
            if row[:2] != [str(seq), district]:
                raise ValueError(f'Unexpected district/sequence {row[:2]}, expected {[str(seq), district]}')
            out.append(make_row(report_date, ytd_start, 'police_district', district, range_name, row))
            position += 1
        row = data[position]
        if row[0].replace(' ', '') != 'TOTAL':
            raise ValueError('Missing range subtotal')
        out.append(make_row(report_date, ytd_start, 'police_range', range_name, range_name, row))
        position += 1
    if data[position][0].replace(' ', '') != 'GRANDTOTAL':
        raise ValueError('Missing province total')
    out.append(make_row(report_date, ytd_start, 'province', 'Sindh', '', data[position]))
    return out

def make_row(day, start, level, name, range_name, cells):
    return dict(report_date=day, ytd_start_date=start, ytd_end_date=day,
                geography_level=level, geography_id='sindh_' + level + '_' + slug(name),
                geography_name=name, police_range=range_name,
                daily_firs=value(cells[2]), ytd_firs=value(cells[3]),
                daily_raw=cells[2], ytd_raw=cells[3],
                daily_status='reported' if value(cells[2]) is not None else 'missing_marker',
                ytd_status='reported' if value(cells[3]) is not None else 'missing_marker')

def parse_indexed(text):
    """Parse HTML or Markdown tables in saved indexed official PDF responses."""
    from bs4 import BeautifulSoup
    starts = [m.start() for m in re.finditer(re.escape(TITLE), text)]
    if not starts:
        raise ValueError('Not the all-FIR section')
    text = text[starts[-1]:]
    if '<table' in text:
        table = BeautifulSoup(text, 'html.parser').find('table')
        # Recursive=False avoids double-counting the malformed nested <tr> on 7 November.
        rows = [[c.get_text(' ', strip=True) for c in row.find_all(['td', 'th'], recursive=False)]
                for row in table.find_all('tr')]
        return parse_rows(rows, table.get_text(' ', strip=True))
    text = re.sub(r'SHAHEED\s+BEANZIRABAD', 'SHAHEED BEANZIRABAD', text)
    rows = []
    for line in text.splitlines():
        if '|' not in line:
            continue
        c = line.strip().split('|')
        if not c[-1].strip():
            c.pop()
        if not c[0].strip():
            c.pop(0)
        rows.append(c)
    return parse_rows(rows, text)

def parse_pdf(path):
    import pdfplumber
    with pdfplumber.open(path) as pdf:
        matches = []
        for page_num, page in enumerate(pdf.pages, 1):
            text = page.extract_text() or ''
            if TITLE.lower() not in compact(text).lower():
                continue
            for table in page.extract_tables():
                try:
                    # Some exporters leave an empty final column.
                    while table and all(not compact(r[-1]) for r in table):
                        table = [r[:-1] for r in table]
                    rows = parse_rows(table, text)
                    matches.append((rows, page_num))
                except ValueError:
                    continue
        if len(matches) != 1:
            raise ValueError(f'Expected one complete FIR table, found {len(matches)}; review PDF layout')
        return matches[0]

def recover(raw):
    """Archive every complete indexed table; duplicates must agree exactly."""
    found, failures = {}, []
    for path in sorted((raw / 'discovery').glob('search_*.txt')):
        for block in re.split(r'-{30,}', path.read_text()):
            m = re.search(URL_RE, block)
            if not m or TITLE not in block:
                continue
            url = m.group()
            try:
                rows = parse_indexed(block)
            except ValueError as exc:
                failures.append(dict(file=path.name, source_url=url, reason=str(exc)))
                continue
            if url in found:
                if rows != found[url]['rows']:
                    raise ValueError(f'Conflicting indexed representations for {url}')
                continue
            payload = block.strip().encode()
            content_hash = sha(payload)
            obj = raw / 'objects' / (content_hash + '.txt')
            obj.parent.mkdir(parents=True, exist_ok=True)
            obj.write_bytes(payload)
            found[url] = dict(source_id=sha(url.encode())[:16], source_url=url,
                              report_date=rows[0]['report_date'], source_page=2,
                              source_section=TITLE, acquisition_method='search_index_of_official_pdf',
                              acquired_on='2026-09-06', local_file=str(obj.relative_to(raw)),
                              sha256=content_hash, discovery_file=str(path.relative_to(raw)), rows=rows)
    sources = []
    for source in found.values():
        source.pop('rows')
        sources.append(source)
    sources.sort(key=lambda x: (x['report_date'], x['source_id']))
    if not sources:
        raise ValueError('No complete indexed reports recovered')
    dump(raw / 'indexed_sources.json', sources)
    dump(raw / 'recovery_log.json', dict(complete_reports=len(sources), skipped_blocks=failures))
    return sources

def import_pdf(raw, path, url):
    if urlparse(url).hostname not in ('sindhpolice.gov.pk', 'www.sindhpolice.gov.pk'):
        raise ValueError('Source URL must identify the official Sindh Police publication')
    payload = path.read_bytes()
    if not payload.startswith(b'%PDF-'):
        raise ValueError('Not a PDF (possibly an HTML access-block response)')
    rows, page = parse_pdf(path)
    content_hash = sha(payload)
    obj = raw / 'objects' / (content_hash + '.pdf')
    obj.parent.mkdir(parents=True, exist_ok=True)
    obj.write_bytes(payload)
    source = dict(source_id=sha((url + content_hash).encode())[:16], source_url=url,
                  report_date=rows[0]['report_date'], source_page=page, source_section=TITLE,
                  acquisition_method='original_pdf', acquired_on=datetime.now(timezone.utc).date().isoformat(),
                  local_file=str(obj.relative_to(raw)), sha256=content_hash, discovery_file='')
    manifest = raw / 'pdf_sources.json'
    sources = json.loads(manifest.read_text()) if manifest.exists() else []
    if not any(s['source_id'] == source['source_id'] for s in sources):
        sources.append(source)
    dump(manifest, sources)

def qa(rows):
    checks, temporal = [], []
    by_date = defaultdict(list)
    for row in rows:
        by_date[row['report_date']].append(row)
    for day, group in sorted(by_date.items()):
        districts = [r for r in group if r['geography_level'] == 'police_district']
        ranges = [r for r in group if r['geography_level'] == 'police_range']
        province = next(r for r in group if r['geography_level'] == 'province')
        units = [(r, [d for d in districts if d['police_range'] == r['police_range']], 'district_to_range') for r in ranges]
        units += [(province, ranges, 'range_to_province'), (province, districts, 'district_to_province')]
        for parent, children, kind in units:
            for metric in ('daily_firs', 'ytd_firs'):
                missing = sum(c[metric] is None for c in children)
                known_sum = sum(c[metric] for c in children if c[metric] is not None)
                residual = None if missing or parent[metric] is None else parent[metric] - known_sum
                checks.append(dict(report_date=day, geography_id=parent['geography_id'],
                    geography_name=parent['geography_name'], check=kind, metric=metric,
                    printed_total=parent[metric], sum_reported_children=known_sum,
                    missing_children=missing, residual=residual,
                    status='not_checkable_missing_values' if residual is None else ('pass' if residual == 0 else 'mismatch')))
    previous = {}
    for r in sorted(rows, key=lambda r: (r['report_date'], r['geography_id'])):
        p = previous.get(r['geography_id'])
        if p and p['ytd_start_date'] == r['ytd_start_date'] and p['ytd_firs'] is not None and r['ytd_firs'] is not None:
            gap = (date.fromisoformat(r['report_date']) - date.fromisoformat(p['report_date'])).days
            delta = r['ytd_firs'] - p['ytd_firs']
            residual = delta - r['daily_firs'] if gap == 1 and r['daily_firs'] is not None else None
            if delta < 0 or residual not in (None, 0):
                temporal.append(dict(report_date=r['report_date'], previous_report_date=p['report_date'],
                    geography_id=r['geography_id'], geography_name=r['geography_name'],
                    geography_level=r['geography_level'], gap_days=gap, previous_ytd=p['ytd_firs'],
                    ytd_firs=r['ytd_firs'], daily_firs=r['daily_firs'], observed_ytd_change=delta,
                    change_minus_daily=residual, flag='ytd_decrease' if delta < 0 else 'ytd_change_differs_from_daily'))
        previous[r['geography_id']] = r
    return checks, temporal

def export(out, name, rows):
    if not rows:
        dump(out / (name + '.json'), [])
        return
    dump(out / (name + '.json'), rows)
    with (out / (name + '.csv')).open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    import duckdb
    con = duckdb.connect(':memory:')
    con.read_json(str(out / (name + '.json'))).write_parquet(str(out / (name + '.parquet')))
    assert con.read_parquet(str(out / (name + '.parquet'))).count('*').fetchone()[0] == len(rows)
    con.close()

def crosscheck_page_text(raw, rows):
    """Compare table extraction with separately retrieved indexed PDF page text."""
    checks = []
    for path in sorted((raw / 'discovery').glob('open_*.txt')):
        text = path.read_text()
        if 'Number of pages: 3' not in text:
            continue
        url = re.search(URL_RE, text).group()
        clean = compact(re.sub(r'L\d+@P\d+(?:-\d+)?: ?', '', text))
        for row in rows:
            if row['source_url'] != url or row['geography_level'] != 'police_district':
                continue
            match = re.search(r'(?<!\w)\d+ ' + re.escape(row['geography_name']) + r' (\d+|-) (\d+|-)\b', clean)
            if not match or list(match.groups()) != [row['daily_raw'], row['ytd_raw']]:
                raise ValueError(f'Indexed page text differs from table: {path.name}, {row["geography_name"]}')
            checks.append(dict(report_date=row['report_date'], geography_name=row['geography_name'],
                               daily_firs=row['daily_firs'], ytd_firs=row['ytd_firs'],
                               comparison_file=str(path.relative_to(raw)), status='match'))
    return checks

def build(raw, out):
    sources = []
    for file in ('indexed_sources.json', 'pdf_sources.json'):
        if (raw / file).exists():
            sources.extend(json.loads((raw / file).read_text()))
    if not sources:
        raise ValueError('No sources. Run recover or import-pdf first.')
    observations, selected = [], {}
    for source in sorted(sources, key=lambda s: (s['acquisition_method'] != 'original_pdf', s['source_id'])):
        path = raw / source['local_file']
        if sha(path.read_bytes()) != source['sha256']:
            raise ValueError(f'Source integrity failure: {path}')
        records = parse_pdf(path)[0] if path.suffix == '.pdf' else parse_indexed(path.read_text())
        if records[0]['report_date'] != source['report_date']:
            raise ValueError('Manifest date differs from table')
        for row in records:
            row.update({k: source[k] for k in ('source_id', 'source_url', 'source_page', 'acquisition_method', 'sha256')})
            key = (row['report_date'], row['geography_id'])
            if key in selected and any(row[k] != selected[key][k] for k in ('daily_firs', 'ytd_firs', 'daily_raw', 'ytd_raw')):
                raise ValueError(f'Conflicting source revisions for {key}; explicit review required')
            selected.setdefault(key, row)
            observations.append(dict(row))
    rows = sorted(selected.values(), key=lambda r: (r['report_date'], r['geography_level'], r['geography_id']))
    page_crosschecks = crosscheck_page_text(raw, rows)
    checks, temporal = qa(rows)
    temporal_keys = {(r['report_date'], r['geography_id']) for r in temporal}
    mismatches = {(r['report_date'], r['geography_id']) for r in checks if r['status'] == 'mismatch'}
    for row in rows:
        key = (row['report_date'], row['geography_id'])
        row['temporal_discrepancy_flag'] = key in temporal_keys
        row['printed_total_discrepancy_flag'] = key in mismatches
    district_rows = [r for r in rows if r['geography_level'] == 'police_district']
    totals = [r for r in rows if r['geography_level'] != 'police_district']
    covered = sorted({r['report_date'] for r in rows})
    coverage = []
    for year in range(2019, max(2025, int(covered[-1][:4])) + 1):
        available = [d for d in covered if d.startswith(str(year))]
        coverage.append(dict(year=year, reports_recovered=len(available),
            calendar_days=366 if calendar.isleap(year) else 365,
            days_without_recovered_report=(366 if calendar.isleap(year) else 365)-len(available),
            first_recovered_date=min(available) if available else None,
            last_recovered_date=max(available) if available else None,
            status='partial' if available else 'not_recovered'))
    latest = max(covered)
    out.mkdir(parents=True, exist_ok=True)
    for name, data in [('sindh_fir_district_daily_ytd', district_rows), ('sindh_fir_reported_totals', totals),
                       ('sindh_fir_observations', observations), ('sindh_fir_sources', sources),
                       ('sindh_fir_latest_available', [r for r in district_rows if r['report_date'] == latest]),
                       ('sindh_fir_total_checks', checks), ('sindh_fir_temporal_flags', temporal),
                       ('sindh_fir_year_coverage', coverage)]:
        export(out, name, data)
    summary = dict(report_count=len(covered), district_count=len(GROUPS[0][1])+sum(len(d) for _, d in GROUPS[1:]),
        district_date_rows=len(district_rows), reported_total_rows=len(totals), source_count=len(sources),
        report_dates=covered, daily_missing_values=sum(r['daily_firs'] is None for r in district_rows),
        ytd_missing_values=sum(r['ytd_firs'] is None for r in district_rows),
        total_check_statuses=dict(Counter(r['status'] for r in checks)),
        temporal_flags=len(temporal), district_temporal_flags=sum(r['geography_level']=='police_district' for r in temporal),
        complete_historical_series=False, latest_recovered_date=latest,
        direct_access_status='HTTP 403 / Chrome Cloudflare block on 2026-09-06',
        indexed_page_crosschecked_district_rows=len(page_crosschecks),
        indexed_page_crosschecked_reports=len({r['report_date'] for r in page_crosschecks}),
        source_visual_verification='Original PDF rendering unavailable for initial extraction; indexed-source evidence only.',
        caveats=['Dashes remain null, explicit zero remains zero.',
            'YTD values are cumulative snapshots; never sum them across report dates.',
            'Daily FIRs are taken from the daily column, never inferred from YTD differences.',
            '31 police districts include eight Karachi police districts; ADM2 equivalence is unverified.',
            'No 2019–2024 reports recovered. 2025 is partial; November 7 is not a year-end total.',
            'Source/index discrepancies are preserved, not silently corrected.'])
    dump(out / 'quality_report.json', summary)
    dump(out / 'indexed_page_crosschecks.json', page_crosschecks)
    return summary

def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--raw', type=Path, default=RAW)
    p.add_argument('--out', type=Path, default=OUT)
    sub = p.add_subparsers(dest='command', required=True)
    sub.add_parser('recover')
    sub.add_parser('build')
    sub.add_parser('rebuild')
    imp = sub.add_parser('import-pdf')
    imp.add_argument('path', type=Path)
    imp.add_argument('--source-url', required=True)
    fetch = sub.add_parser('fetch')
    fetch.add_argument('url')
    args = p.parse_args()
    if args.command in ('recover', 'rebuild'):
        recover(args.raw)
    if args.command == 'import-pdf':
        import_pdf(args.raw, args.path, args.source_url)
    if args.command == 'fetch':
        if urlparse(args.url).hostname not in ('sindhpolice.gov.pk', 'www.sindhpolice.gov.pk'):
            p.error('Only official Sindh Police report URLs are accepted')
        with urlopen(args.url, timeout=45) as response:
            payload = response.read()
        if not payload.startswith(b'%PDF-'):
            raise ValueError('Response is not a PDF; refusing to import an access-block page')
        import tempfile
        with tempfile.TemporaryDirectory() as temp:
            pdf = Path(temp) / 'report.pdf'
            pdf.write_bytes(payload)
            import_pdf(args.raw, pdf, args.url)
    if args.command != 'recover':
        print(json.dumps(build(args.raw, args.out), indent=2))

if __name__ == '__main__':
    main()
