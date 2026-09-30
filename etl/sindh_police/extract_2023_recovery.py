#!/usr/bin/env python3
"""Rebuild 2022–2023 annual observations from retained official PDF web excerpts.

Uses only raw text snapshots, never previously derived JSON reports. Writes to a
separate output directory and rejects attempts to overwrite the raw directory.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
from collections import defaultdict
from pathlib import Path

SOURCE_URL = 'https://www.sindhpolice.gov.pk/storage/statistic/1706297616_68886fbacb665.pdf'
WORKSPACE = Path(__file__).resolve().parents[3]
INPUT_SPECS = (
    ('crime_report_december_2023_province_raw.txt', 1, ['Sindh Province']),
    ('2023_ranges_fill_raw.txt', 3, ['Karachi Range', 'Sukkur Range', 'Larkana Range']),
    ('2023_hyd_fill_raw.txt', 4, ['Hyderabad Range', 'Mirpurkhas Range', 'S.B.Abad Range']),
)


def plain(value: str) -> str:
    value = re.sub(r'<br\s*/?>', ' ', value, flags=re.I)
    return ' '.join(html.unescape(re.sub(r'<[^>]*>', '', value)).split())


def slug(value: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', value.lower()).strip('_')


def extract_table(raw: str, page: int, geographies: list[str]) -> list[dict]:
    assert SOURCE_URL in raw, 'Expected official PDF URL missing from raw snapshot'
    assert '31-12-2023' in raw and '01-01-2023' in raw, 'Full-year dates missing'
    assert re.search(rf'Page\s*#?\s*{page}\b', raw), f'Page number {page} missing'
    # A snippet may contain isolated <tr> fragments before the complete table.
    # Select the first actual table element, not those leading preview fragments.
    table_match = re.search(r'<table\b[^>]*>(.*?)</table>', raw, flags=re.S | re.I)
    assert table_match, 'Complete table missing'
    table = table_match.group(1)
    headers = [plain(value) for value in re.findall(r'<th[^>]*>(.*?)</th>', table, flags=re.S | re.I)]
    assert '2022' in headers and '2023' in headers, 'Comparison years missing'
    for geography in geographies:
        assert any(geography.upper() in header.upper() for header in headers), geography
    province = page == 1
    assert (not province) or any('CURRENT MONTH' in value for value in headers)
    observations = []
    category_group = ''
    row_index = 0
    for row_html in re.findall(r'<tr>(.*?)</tr>', table, flags=re.S | re.I):
        cells = [plain(value) for value in re.findall(r'<td[^>]*>(.*?)</td>', row_html, flags=re.S | re.I)]
        if not cells:
            continue
        if len(cells) >= 2 and cells[0] in {'A', 'B', 'D', 'E'}:
            category_group = cells[1]
            continue
        is_total = cells[0].replace(' ', '') == 'TOTAL'
        if is_total:
            assert len(cells) == (7 if province else 10), cells
            label, row_label, values = cells[0], '', cells[1:]
            group = 'ALL CRIMES'
            row_type = 'total'
        else:
            assert len(cells) == (8 if province else 11), cells
            row_label, label, values = cells[0], cells[1], cells[2:]
            group = 'Miscellaneous' if row_label == 'C' else 'BLASPHEMY' if row_label == 'F' else category_group
            row_type = 'detail'
        assert all(re.fullmatch(r'[+-]?\d+', value) for value in values), (label, values)
        assert group, label
        row_index += 1
        # Province table carries December values before annual comparison values.
        annual_values = values[3:] if province else values
        for geo_index, geography in enumerate(geographies):
            previous, current, difference = map(int, annual_values[geo_index * 3:geo_index * 3 + 3])
            assert current - previous == difference, (geography, label, previous, current, difference)
            for offset, year in enumerate((2022, 2023)):
                raw_value = annual_values[geo_index * 3 + offset]
                observation = dict(
                    year=year, period_start=f'{year}-01-01', period_end=f'{year}-12-31',
                    is_full_year=True, geography_name=geography,
                    geography_level='province' if province else 'range',
                    crime_category_raw=label, row_type=row_type, value_raw=raw_value,
                    cases_reported=int(raw_value), source_page=page,
                    comparison_column='prior_year' if offset == 0 else 'current_year',
                    source_row_index=row_index, category_group=group,
                    crime_category_key=slug(label),
                )
                if not (province and is_total):
                    observation['source_row_label'] = row_label
                observations.append(observation)
    assert row_index == 45, f'Expected 44 crime categories plus total; found {row_index}'
    return observations


def validate(observations: list[dict]) -> None:
    groups = defaultdict(list)
    for observation in observations:
        groups[observation['year'], observation['geography_name']].append(observation)
    assert len(observations) == 630 and len(groups) == 14
    for group, values in groups.items():
        assert len(values) == 45
        assert len({value['crime_category_key'] for value in values}) == 45
        details = sum(value['cases_reported'] for value in values if value['row_type'] == 'detail')
        totals = [value['cases_reported'] for value in values if value['row_type'] == 'total']
        assert totals == [details], (group, totals, details)
    for year in (2022, 2023):
        province = {row['crime_category_key']: row['cases_reported']
                    for row in groups[year, 'Sindh Province']}
        for key, count in province.items():
            ranges = sum(row['cases_reported'] for row in observations
                         if row['year'] == year and row['geography_level'] == 'range'
                         and row['crime_category_key'] == key)
            assert count == ranges, (year, key, count, ranges)


def build_reports(raw_dir: Path) -> list[dict]:
    parsed = []
    snapshots = []
    for filename, page, geographies in INPUT_SPECS:
        path = raw_dir / filename
        data = path.read_bytes()
        page_rows = extract_table(data.decode('utf-8'), page, geographies)
        for row in page_rows:
            row['recovery_evidence_file'] = str(path.resolve())
        parsed.append(page_rows)
        snapshots.append(dict(filename=filename, path=str(path.resolve()), sha256=hashlib.sha256(data).hexdigest(), source_page=page))
    validate([row for page in parsed for row in page])
    reports = []
    for kind, observations, evidence in (
        ('province', parsed[0], snapshots[:1]),
        ('ranges', parsed[1] + parsed[2], snapshots[1:]),
    ):
        province = kind == 'province'
        report = dict(
            report_id=f'crime_report_december_2023_{kind}', source_url=SOURCE_URL,
            title=('COMPARATIVE STATEMENT SHOWING COGNIZABLE CASES REPORTED DURING THE MONTH OF DECEMBER-2023 AND UPTO DATE: 31-12-2023, IN SINDH PROVINCE.'
                   if province else 'COMPARATIVE STATEMENT SHOWING COGNIZABLE CASES REPORTED DURING THE PERIOD FROM 01-01-2023 TO 31-12-2023. RANGE TABLES'),
            reporting_year=2023, period_start='2023-01-01', period_end='2023-12-31', is_full_year=True,
            source_type='search_index_extract',
            extraction_method=('HTML table parsed from web search indexed PDF excerpt; annual comparison columns selected, monthly columns excluded'
                               if province else 'HTML tables parsed from search-indexed official PDF excerpt pages 3 and 4; annual 2022 and 2023 comparison columns'),
            source_priority=2023, category_partition_complete=True, selection_eligible=True,
            source_snapshots=evidence, observations=observations,
        )
        reports.append(report)
    return reports


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--raw-dir', type=Path, default=WORKSPACE / 'raw_data/sindh_police/recovery_2019_2022')
    parser.add_argument('--out-dir', '--output-dir', dest='output_dir', type=Path,
                        default=WORKSPACE / 'data_darbar_warehouse/sindh_police/recovered_source_extracts')
    args = parser.parse_args()
    if args.output_dir.resolve() == args.raw_dir.resolve():
        parser.error('Output directory must differ from the retained raw directory')
    reports = build_reports(args.raw_dir)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for report in reports:
        output = args.output_dir / (report['report_id'] + '.json')
        output.write_text(json.dumps(report, indent=2) + '\n')
        print(f'{output}: {len(report["observations"])} observations')
    print('Verified all 630 observations, 14 published totals, 90 province/range sums and printed differences.')


if __name__ == '__main__':
    main()
