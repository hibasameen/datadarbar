"""Reproducible checks supporting two joint areas; no population allocation.

The 893-person adjustment is not explained by a whole rural locality moving.
The Kachhi report's twelve transfers are verified by name and acreage, while
its 1,763-person adjustment remains distinct from Table 1's 1,932.
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path('raw_data/geography/2026-09-25-district-resolution')
SPECS = [
    ('district_059_jhang_2017.pdf', 2017, 'JHANG', 58, 64),
    ('district_060_toba_tek_singh_2017.pdf', 2017, 'TOBA TEK SINGH', 51, 55),
    ('district_116_kachhi_2017.pdf', 2017, 'KACHHI', 69, 71),
    ('district_117_nasirabad_2017.pdf', 2017, 'NASIRABAD', 44, 46),
    ('table31_punjab_2023.pdf', 2023, None, 1, 789),
    ('table31_balochistan_2023.pdf', 2023, None, 1, 169),
]
TRANSFER_NAMES = ['ADMANI KOHNA', 'GAHI', 'GARHI KARAM', 'KAMAL', 'KHANWAH NISAF ANBARI',
                  'KOT SULTAN', 'LANDHI KHAIR PUR', 'MAROR PUR', 'MAT QABOOL',
                  'MIR PUR MANJHU', 'NAWARA', 'SANJRANI']


def require(condition, message):
    if not condition:
        raise ValueError(message)


def number(raw):
    return None if raw in {'', '-'} else int(raw.replace(',', ''))


def label_key(label):
    # Punctuation and the printed abbreviation NO vary; digits are preserved.
    return re.sub(r'[^A-Z0-9]', '', re.sub(r'\bNO\b\.?\s*', '', label.upper()))


def parse_localities(text, filename, year, district, first, last):
    tehsil = qh = pc = None
    width = 25 if year == 2017 else 23
    rows = []
    for pn, page in enumerate(text.split('\f'), 1):
        if not first <= pn <= last:
            continue
        for ln, line in enumerate(page.splitlines(), 1):
            parts = line.split()
            if len(parts) < width + 1 or not all(re.fullmatch(r'[\d,.\-]+', v) for v in parts[-width:]):
                continue
            label, code = ' '.join(parts[:-width]), ''
            if not re.search('[A-Z]', label):
                continue  # Column-number headers are not records.
            if re.search(r'\s\d{7}$', label):
                label, code = label.rsplit(' ', 1)
            if label.endswith('DISTRICT'):
                district, tehsil, qh, pc, kind = label[:-9], None, None, None, 'district'
            elif re.search(r' (TEHSIL|SUB-TEHSIL|SUB-DIVISION)$', label):
                tehsil, qh, pc, kind = label, None, None, 'tehsil'
            elif label.endswith(' QH') or (year == 2017 and label == 'SHORKOT CANTONMENT'):
                qh, pc, kind = label, None, 'qh'
            elif re.search(r'(?:\s|\.)PC\.?$', label):
                pc, kind = label, 'pc'
            else:
                kind = 'village'
            if district not in {'JHANG', 'TOBA TEK SINGH', 'KACHHI', 'NASIRABAD'}:
                continue
            values = parts[-width:]
            rows.append(dict(year=year, district=district, tehsil=tehsil, qh=qh, pc=pc,
                             kind=kind, name=label, hadbast_code=code,
                             population=number(values[0]), population_raw=values[0],
                             area_acres=number(values[-1]), source_path=str(ROOT / filename),
                             source_locator=f'PDF page {pn}; text line {ln}'))
    return rows


def pair_row(a, b, purpose):
    require(a['area_acres'] == b['area_acres'], f'Locality acreage changed: {a["name"]}')
    if a['hadbast_code'] and b['hadbast_code']:
        require(a['hadbast_code'] == b['hadbast_code'], f'Locality code changed: {a["name"]}')
    return dict(purpose=purpose, name_2017=a['name'], name_2023=b['name'],
                district_2017=a['district'], district_2023=b['district'],
                tehsil_2017=a['tehsil'], tehsil_2023=b['tehsil'],
                hadbast_2017=a['hadbast_code'], hadbast_2023=b['hadbast_code'],
                code_status='equal' if a['hadbast_code'] == b['hadbast_code'] else '2023_code_blank_name_and_acreage_match',
                area_acres=a['area_acres'], population_2017=a['population'], population_2023=b['population'],
                population_2017_raw=a['population_raw'], population_2023_raw=b['population_raw'],
                source_2017=a['source_path'], locator_2017=a['source_locator'],
                source_2023=b['source_path'], locator_2023=b['source_locator'])


def audit_localities(root, read_pdf):
    rows = []
    for file, year, district, first, last in SPECS:
        rows.extend(parse_localities(read_pdf(root / ROOT / file), file, year, district, first, last))
    matched = []
    checks = []
    for district, tehsil, expected, totals in [
        ('JHANG', 'SHORKOT TEHSIL', 126, {2017: 474764, 2023: 537269}),
        ('TOBA TEK SINGH', 'PIRMAHAL TEHSIL', 132, {2017: 378026, 2023: 437179}),
    ]:
        years = {}
        for year in (2017, 2023):
            selected = [r for r in rows if r['year'] == year and r['district'] == district and r['tehsil'] == tehsil and r['kind'] == 'village']
            years[year] = {label_key(r['name']): r for r in selected}
            require(len(selected) == len(years[year]) == expected, f'Changed locality roster: {district} {year}')
            require(all(r['hadbast_code'] for r in selected) if year == 2017 else True, 'Unidentified 2017 rural locality')
            # This is a lower-bound reconciliation, not imputation of source dashes.
            require(sum(r['population'] for r in selected if r['population'] is not None) == totals[year], f'Locality population subtotal mismatch: {district} {year}')
        require(years[2017].keys() == years[2023].keys(), f'Changed affected-tehsil locality membership: {district}')
        for key in sorted(years[2017]):
            matched.append(pair_row(years[2017][key], years[2023][key], 'same_published_locality_membership_and_area'))
        checks.append(dict(check='affected_tehsil_locality_roster', comparison_id='DDG-0130', result='pass',
                           detail=f'{tehsil}: {expected} localities match by name/acreage; available codes agree; published rural totals reconcile. This does not explain the 893-person retrospective adjustment.'))
    transfers = []
    for name in TRANSFER_NAMES:
        a = [r for r in rows if r['year'] == 2017 and r['district'] == 'KACHHI' and r['tehsil'] == 'BHAG TEHSIL' and r['kind'] == 'village' and r['name'] == name]
        b = [r for r in rows if r['year'] == 2023 and r['district'] == 'NASIRABAD' and r['tehsil'] == 'LANDHI TEHSIL' and r['kind'] == 'village' and r['name'] == name]
        require(len(a) == len(b) == 1, f'Ambiguous or missing documented transfer: {name}')
        transfers.append(pair_row(a[0], b[0], 'documented_kachhi_to_nasirabad_locality_transfer'))
    require(sum(r['area_acres'] for r in transfers) == 45917, 'Transferred acreage changed')
    require(sum(r['population_2017'] for r in transfers if r['population_2017'] is not None) == 1763, 'Known 2017 transfer counts changed')
    require(sum(r['population_2017'] is None for r in transfers) == 3, 'Missing transfer counts changed')
    require(sum(r['population_2023'] for r in transfers) == 2576, '2023 transferred locality count changed')
    checks.append(dict(check='documented_locality_transfers', comparison_id='DDG-0131', result='pass',
                       detail='12 localities and 45,917 acres match; 2017 known-count subtotal 1,763 with 3 source dashes; 2023 population 2,576. Retrospective Table 1 adjustment 1,932 remains a distinct source figure. No district allocation estimated.'))
    matched.extend(transfers)
    return matched, checks


def administrative_rows(text, year):
    """Table 1 administrative summaries, including wrapped subdistrict names."""
    result = []
    for pn, page in enumerate(text.split('\f'), 1):
        lines = page.splitlines()
        for i, line in enumerate(lines):
            parts = line.split()
            if len(parts) < 11 or not all(re.fullmatch(r'[\d,.\-]+', v) for v in parts[-11:]):
                continue
            label = ' '.join(parts[:-11])
            if not label and i:
                label = ' '.join(lines[i - 1].split())
                if i + 1 < len(lines) and lines[i + 1].strip() in {'DISTRICT', 'TEHSIL', 'SUB-DIVISION'}:
                    label += ' ' + lines[i + 1].strip()
            if not re.search(r' (DISTRICT|TEHSIL|SUB-TEHSIL|SUB-DIVISION)$', label):
                continue
            result.append(dict(name=label, year=year, population=number(parts[-10]),
                               retrospective_2017=number(parts[-2]) if year == 2023 else None,
                               source_locator=f'PDF page {pn}; text line {i + 1}'))
    return result


def audit_subdistricts(root, read_pdf):
    before_paths = {
        'JHANG': '059_jhang', 'TOBA TEK SINGH': '060_toba_tek_singh',
        'KACHHI': '116_kachhi', 'NASIRABAD': '117_nasirabad',
    }
    before, after, checks = {}, {}, []
    for district, stem in before_paths.items():
        path = 'raw_data/pbs/Census 2017/pbs_2017_table01/' + stem + '_table1.pdf'
        before[district] = [{**r, 'district': district, 'source_path': path}
                            for r in administrative_rows(read_pdf(root / path), 2017)]
    for province in ['punjab', 'balochistan']:
        path = f'raw_data/pbs/stage1_sources/2026-09-25-official/table_1_{province}_districts.pdf'
        district = None
        for row in administrative_rows(read_pdf(root / path), 2023):
            if row['name'].endswith(' DISTRICT'):
                district = row['name'][:-9]
            if district in before_paths:
                after.setdefault(district, []).append({**row, 'district': district, 'source_path': path})
    for district, expected in [('JHANG', (5, 5)), ('TOBA TEK SINGH', (5, 5)), ('KACHHI', (7, 7)), ('NASIRABAD', (5, 7))]:
        a, b = before[district], after[district]
        require((len(a), len(b)) == expected, f'Changed subdistrict coverage: {district}')
        require(sum(x['population'] for x in a[1:]) == a[0]['population'], f'2017 subdistrict total mismatch: {district}')
        require(sum(x['population'] for x in b[1:]) == b[0]['population'], f'2023 subdistrict total mismatch: {district}')
        require(sum(x['retrospective_2017'] for x in b[1:]) == b[0]['retrospective_2017'], f'Retrospective subdistrict total mismatch: {district}')
    expected_deltas = {'SHORKOT TEHSIL': 893, 'PIRMAHAL TEHSIL': -893}
    for district in ['JHANG', 'TOBA TEK SINGH']:
        a = {r['name']: r for r in before[district][1:]}
        b = {r['name']: r for r in after[district][1:]}
        require(a.keys() == b.keys(), 'Punjab tehsil membership changed')
        for name in a:
            require(b[name]['retrospective_2017'] - a[name]['population'] == expected_deltas.get(name, 0), f'Unexpected retrospective tehsil change: {name}')
    a = next(r for r in before['KACHHI'] if r['name'] == 'BHAG TEHSIL')
    b = next(r for r in after['KACHHI'] if r['name'] == 'BHAG SUB-DIVISION')
    require(b['retrospective_2017'] - a['population'] == -1932, 'Bhag adjustment changed')
    checks.append(dict(check='subdistrict_reconciliation', comparison_id='DDG-0130', result='pass',
                       detail='All eight tehsil totals reconcile; only Shorkot (+893) and Pir Mahal (-893) have changed retrospective 2017 counts.'))
    checks.append(dict(check='subdistrict_reconciliation', comparison_id='DDG-0131', result='pass',
                       detail='All original and 2023 subdistrict totals reconcile to their district totals; Bhag has -1932 retrospective adjustment.'))
    return [r for d in before_paths for year in [before, after] for r in year[d]], checks
