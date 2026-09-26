#!/usr/bin/env python3
"""Reproduce annual 2019–2021 observations from archived official Sindh PDFs.

Requires the Poppler pdftotext executable. Original PDFs are retained untouched.
Selects only comprehensive comparative crime tables, excluding overview pages
with inconsistent totals. Counts and printed differences are both retained.
"""
from __future__ import annotations

import hashlib
import argparse
import json
import re
import subprocess
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[3]
HERE = WORKSPACE / 'raw_data' / 'sindh_police' / 'recovery_2019_2022'
ARCHIVE = HERE.parent / 'archive'
OUT = WORKSPACE / 'data_darbar_warehouse' / 'sindh_police' / 'recovered_source_extracts'
PAGE_SPECS = {
    2020: [(1, ['Karachi Range', 'Sukkur Range', 'Larkana Range']),
           (2, ['Hyderabad Range', 'Mirpurkhas Range', 'S.B.Abad Range', 'Sindh Province'])],
    2021: [(5, ['Sindh Province', 'Karachi Range']),
           (6, ['Sukkur Range', 'Larkana Range']),
           (7, ['Hyderabad Range', 'Mirpurkhas Range', 'S.B.Abad Range'])],
}


def strip_row_label(value: str) -> str:
    return re.sub(r'^(?:[A-F]\.?|[a-z](?:\.|\))|[ivx]+[.)]?)\s+', '', value.strip()).strip()


def slug(value: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', value.lower()).strip('_')


def main() -> None:
    global HERE, ARCHIVE, OUT
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw-dir', type=Path, default=HERE.parent)
    parser.add_argument('--out-dir', type=Path, default=OUT)
    args = parser.parse_args()
    HERE = args.raw_dir / 'recovery_2019_2022'
    ARCHIVE = args.raw_dir / 'archive'
    OUT = args.out_dir
    OUT.mkdir(parents=True, exist_ok=True)
    categories = json.loads((Path(__file__).resolve().parent / 'category_schema.json').read_text())
    cdx = json.loads((ARCHIVE / 'legacy_cdx_20260906.json').read_text())
    download_manifest = {item['file']: item for item in json.loads((ARCHIVE / 'download_manifest.json').read_text())}
    for report_year, specs in PAGE_SPECS.items():
        pdf = ARCHIVE / f'annual_{report_year}.pdf'
        sha256 = hashlib.sha256(pdf.read_bytes()).hexdigest()
        assert download_manifest[pdf.name]['content_sha256'] == sha256, f'Archived PDF checksum mismatch: {pdf}'
        source = next(row for row in cdx[1:]
                      if f'Year-{report_year}%20(01-01-' in row[2])
        archive_url = f'https://web.archive.org/web/{source[1]}id_/{source[2]}'
        layout = subprocess.check_output(['pdftotext', '-layout', str(pdf), '-'], text=True)
        (OUT / f'annual_{report_year}_layout.txt').write_text(layout)
        pages = layout.split('\f')
        observations = []
        difference_issues = []
        for page_number, geographies in specs:
            page = pages[page_number - 1]
            assert f'31-12-{report_year}' in page, f'Full-year end missing on page {page_number}'
            value_count = 3 * len(geographies)
            pattern = re.compile(r'(?P<values>(?:\s+[+-]?\d+){' + str(value_count) + r'})\s*$')
            lines = page.splitlines()
            rows = [(i, line, match) for i, line in enumerate(lines) if (match := pattern.search(line))]
            assert len(rows) == 45, f'Expected 44 crime rows + total, found {len(rows)}'
            for row_index, (line_index, line, match) in enumerate(rows, 1):
                canonical = categories[row_index - 1]
                label = line[:match.start()].strip()
                # Three headings place the label above their numeric row.
                # Two long labels on 2021 page 7 also wrap above and below it.
                if not label or re.fullmatch(r'[A-F]|[ivx]+', label):
                    label = lines[line_index - 1].strip()
                label = strip_row_label(label)
                if line_index + 1 < len(lines):
                    continuation = lines[line_index + 1].strip()
                    if continuation.startswith('(') or continuation in {'Snatched', 'Laws'}:
                        label += ' ' + continuation
                label = ' '.join(label.split())
                assert label, f'Missing source label {report_year}/{page_number}/{row_index}'
                # Check the recovered heading against this fixed published table schema.
                normalized = label.lower().replace('grevious', 'grievous').replace('roberry', 'robbery')
                normalized = normalized.replace('reeceiving', 'receiving').replace('andf', 'and')
                normalized = normalized.replace(' accidents', '').replace('murder )', 'murder)')
                expected = canonical['crime_category_raw'].lower()
                if row_index == 45:
                    assert normalized.replace(' ', '') == 'total'
                else:
                    assert normalized == expected, (label, expected, report_year, page_number, row_index)
                values = match.group('values').split()
                for geo_index, geography in enumerate(geographies):
                    previous, current, diff = map(int, values[geo_index * 3:geo_index * 3 + 3])
                    if current - previous != diff:
                        difference_issues.append(dict(source_page=page_number, source_row_index=row_index,
                            geography_name=geography, crime_category_raw=label,
                            source_difference=diff, calculated_difference=current - previous))
                    for year_offset, year in enumerate((report_year - 1, report_year)):
                        raw = values[geo_index * 3 + year_offset]
                        observations.append(dict(
                            year=year, period_start=f'{year}-01-01', period_end=f'{year}-12-31',
                            is_full_year=True, geography_name=geography,
                            geography_level='province' if geography == 'Sindh Province' else 'range',
                            crime_category_raw=label, crime_category_key=slug(canonical['crime_category_raw']),
                            category_group=canonical['category_group'],
                            row_type=canonical['row_type'], value_raw=raw, cases_reported=int(raw),
                            source_page=page_number, source_row_index=row_index,
                            source_text_line=line.strip(), source_difference_raw=str(diff),
                            source_pdf_sha256=sha256,
                            comparison_column='prior_year' if year_offset == 0 else 'current_year'))
        for geography in {o['geography_name'] for o in observations}:
            for year in (report_year - 1, report_year):
                selected = [o for o in observations if o['geography_name'] == geography and o['year'] == year]
                details = sum(o['cases_reported'] for o in selected if o['row_type'] == 'detail')
                totals = [o['cases_reported'] for o in selected if o['row_type'] == 'total']
                assert len(selected) == 45 and totals == [details], (year, geography, details, totals)
        for year in (report_year - 1, report_year):
            for key in {o['crime_category_key'] for o in observations}:
                selected = [o for o in observations if o['year'] == year and o['crime_category_key'] == key]
                province = next(o['cases_reported'] for o in selected if o['geography_level'] == 'province')
                ranges = sum(o['cases_reported'] for o in selected if o['geography_level'] == 'range')
                assert province == ranges, (year, key, province, ranges)
        report = dict(
            report_id=f'annual_{report_year}_archived_official_pdf', source_url=source[2],
            archive_url=archive_url, archive_timestamp=source[1], source_pdf_sha256=sha256,
            source_document_path=str(pdf), source_document_sha256=sha256,
            title=f'Crime figures of Sindh Province for the Year-{report_year} (01-01-{report_year} to 31-12-{report_year})',
            reporting_year=report_year, period_start=f'{report_year}-01-01', period_end=f'{report_year}-12-31',
            is_full_year=True, source_type='archived_official_pdf',
            extraction_method='Poppler pdftotext -layout; fixed published annual table schema with source-label assertions; counts and totals verified',
            source_priority=report_year, selection_eligible=True, category_partition_complete=True,
            notes=['Overview summaries outside the comprehensive crime tables are excluded. Printed difference columns contain source errors; preserve counts and record arithmetic discrepancies without changing values.'],
            source_difference_issues=difference_issues, observations=observations,
        )
        output = OUT / f'annual_{report_year}_archived_official_pdf.json'
        output.write_text(json.dumps(report, indent=2) + '\n')
        print(f'{output.name}: {len(observations)} observations; {len(difference_issues)} printed difference inconsistencies; all crime totals reconcile')


if __name__ == '__main__':
    main()
