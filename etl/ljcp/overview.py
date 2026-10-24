"""National court-level overview tables printed at the front of each text-layer edition.

Each annual report opens with one table of pendency, institution, disposal and balance for
every superior court and every province's district judiciary. These figures are LJCP's own
headline numbers. They are kept separate from the chapter tables: the overview's district
rows routinely differ from the district chapters' printed totals (different cut-off or
reconciliation), and the comparison is published in overview_reconciliation.

2025 break: the 27th Amendment created the Federal Constitutional Court (FCC). The Supreme
Court transferred 21,570 pending cases to it during 2025 (table 1.5: 14,660 petitions, 3,985
appeals, 2,925 other cases). The FCC itself is not covered by the report, so the 2025
Supreme Court and superior-court closing balances are not comparable with earlier years:
part of the fall is a transfer out of scope, not disposal. Those transfers are carried
in their own column and never netted into disposals.
"""
from __future__ import annotations

import hashlib
import json
import re

import pdfplumber

from extract import HERE, clean, integer

# Text-layer annual editions with an overview table (2021 is scanned and not covered).
EDITIONS = {2020: 'annual_2020', 2022: 'annual_2022', 2023: 'annual_2023', 2024: 'annual_2024', 2025: 'annual_2025'}
COURTS = [
    ('supreme_court', 'superior', r'Supreme Court of Pakistan'),
    ('federal_shariat_court', 'superior', r'Federal Shariat Court'),
    ('lahore_high_court', 'superior', r'Lahore High Court(?:,? Lahore)?'),
    ('sindh_high_court', 'superior', r'High Court of Sindh(?:,? ?Karachi)?'),
    ('peshawar_high_court', 'superior', r'Peshawar High Court(?:,? Peshawar)?'),
    ('balochistan_high_court', 'superior', r'High Court of Balochistan(?:,? Quetta)?'),
    ('islamabad_high_court', 'superior', r'Islamabad High Court(?:,? Islamabad)?'),
    ('total_high_courts', 'total', r'Total\s*[–-]\s*High Courts'),
    ('total_superior_courts', 'total', r'Total\s*[–-]\s*Superior Courts'),
    ('district_punjab', 'district', r'District Courts,? Punjab'),
    ('district_sindh', 'district', r'District Courts,? Sindh'),
    ('district_khyber_pakhtunkhwa', 'district', r'District Courts,? Khyber Pakhtunkhwa'),
    ('district_balochistan', 'district', r'District Courts,? Balochistan'),
    ('district_islamabad', 'district', r'District Courts,? Islamabad'),
    ('total_district_judiciary', 'total', r'Total\s*[–-]\s*District Judiciary'),
    ('grand_total', 'total', r'Grand Total'),
]
PROVINCE = {'district_punjab': 'Punjab', 'district_sindh': 'Sindh', 'district_khyber_pakhtunkhwa': 'Khyber Pakhtunkhwa',
            'district_balochistan': 'Balochistan', 'district_islamabad': 'Islamabad'}
NUM = r'([\d,]+)'
FOUR = ['pending_start', 'instituted', 'disposed', 'pending_end']

FCC_NOTE = ('Supreme Court 2025: the overview row omits the 21,570 cases the Supreme Court transferred to the new Federal '
            'Constitutional Court (table 1.5; 14,660 petitions, 3,985 appeals, 2,925 other cases). The FCC is outside the report, so the '
            'closing balance is not comparable with earlier years. The chapter table (58,294 pending, 23,155 instituted, 24,975 disposed, '
            '21,570 transferred, 34,904 balance) balances exactly but differs from the overview row.')


def _rows_from_text(text):
    lines = [clean(l) for l in (text or '').splitlines()]
    out = {}
    for i, line in enumerate(lines):
        # A court name can wrap around its numbers (2022: "District Courts, Khyber" /
        # "260160 ... 1.3%" / "Pakhtunkhwa"); rejoin the name before matching.
        candidates = [line]
        if i + 2 < len(lines) and not re.search(r'\d', line) and not re.search(r'\d', lines[i + 2]) and re.match(r'^[\d,]+\s', lines[i + 1]):
            candidates.append(f'{line} {lines[i + 2]} {lines[i + 1]}')
        for cand in candidates:
            for key, tier, pat in COURTS:
                m = re.match(rf'^{pat}\s+{NUM}\s+{NUM}\s+{NUM}\s+{NUM}(?:\s+(-?[\d.]+\s*%))?', cand)
                if m:
                    out.setdefault(key, (tier, [integer(v) for v in m.groups()[:4]], m.group(5), cand))
    return out


def sc_chapter_2025(pdf):
    """Supreme Court table 1.5 (all cases) and 1.5.1-1.5.3 (petitions, appeals, other) totals."""
    out = {}
    for label, page, ti in [('all', 22, 2), ('petitions', 23, 0), ('appeals', 23, 1), ('other', 23, 2)]:
        tables = [t for t in pdf.pages[page - 1].find_tables() if t.bbox[2] - t.bbox[0] > 300]
        total = [r for r in tables[ti].extract() if clean(r[0]) == 'Total']
        if len(total) != 1:
            raise ValueError(f'Supreme Court table {label} total row not found')
        vals = [integer(v) for v in total[0][1:] if clean(v)]
        if len(vals) != 5:
            raise ValueError(f'Supreme Court table {label} has {len(vals)} values: {total[0]}')
        start, inst, disp, fcc, end = vals
        if start + inst - disp - fcc != end:
            raise ValueError(f'Supreme Court table {label} does not balance: {vals}')
        out[label] = dict(pending_start=start, instituted=inst, disposed=disp, transferred_to_fcc=fcc, pending_end=end, pdf_page=page, table_index=ti)
    if out['all']['transferred_to_fcc'] != sum(out[k]['transferred_to_fcc'] for k in ('petitions', 'appeals', 'other')):
        raise ValueError('Supreme Court FCC transfers by case type do not add to table 1.5')
    return out


def extract_overview(raw, max_pages=20):
    catalog = {s['id']: s for s in json.loads((HERE / 'sources.json').read_text())}
    rows, sc_chapter = [], {}
    for year, sid in EDITIONS.items():
        path = raw / 'pdfs' / f'{sid}.pdf'
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        if sha != json.loads(path.with_suffix('.metadata.json').read_text())['sha256']:
            raise ValueError(f'Checksum mismatch: {sid}')
        found = {}
        with pdfplumber.open(path) as pdf:
            for pno in range(1, min(max_pages, len(pdf.pages)) + 1):
                for key, val in _rows_from_text(pdf.pages[pno - 1].extract_text()).items():
                    found.setdefault(key, (pno, *val))
            if year == 2025:
                sc_chapter = sc_chapter_2025(pdf)
        courts = [k for k, t, _ in COURTS if t != 'total']
        missing = [k for k in courts if k not in found]
        if missing:
            raise ValueError(f'{sid}: overview rows not found: {missing}')
        for key, tier, _ in COURTS:
            if key not in found:
                continue
            pno, _, vals, pct, line = found[key]
            r = dict(year=year, period_start=f'{year}-01-01', period_end=f'{year}-12-31', court=key, court_level=tier,
                     province=PROVINCE.get(key), **dict(zip(FOUR, vals)), printed_change_pct=pct,
                     transferred_to_fcc=None, unexplained_outflow=vals[0] + vals[1] - vals[2] - vals[3],
                     raw_line=line, pdf_page=pno, source_id=sid, source_url=catalog[sid]['source_url'], source_sha256=sha,
                     source_record_id=f'{sid}:overview:{pno}:{key}', scope_note='')
            if year == 2025 and key in ('supreme_court', 'total_superior_courts', 'grand_total'):
                r['scope_note'] = FCC_NOTE
                r['series_break'] = 'federal_constitutional_court_transfer_2025'
            if year == 2025 and key == 'supreme_court':
                r['transferred_to_fcc'] = sc_chapter['all']['transferred_to_fcc']
                r['chapter_table_1_5'] = sc_chapter
            rows.append(r)
    return rows


def overview_checks(rows):
    """Printed totals against the sum of their court rows, per edition."""
    checks = []
    for year in sorted({r['year'] for r in rows}):
        y = {r['court']: r for r in rows if r['year'] == year}
        superior = [k for k, t, _ in COURTS if t == 'superior']
        groups = [('total_district_judiciary', [k for k, t, _ in COURTS if t == 'district']),
                  ('total_superior_courts', superior),
                  ('total_high_courts', [k for k in superior if k.endswith('high_court')]),
                  ('grand_total', superior + [k for k, t, _ in COURTS if t == 'district'])]
        for total, parts in groups:
            if total not in y:
                continue
            for f in FOUR:
                s = sum(y[k][f] for k in parts)
                checks.append(dict(year=year, total_row=total, metric=f, sum_of_rows=s, printed_total=y[total][f], difference=s - y[total][f],
                                   status='pass' if s == y[total][f] else 'source_mismatch'))
    return checks


def reconcile(rows, province_annual):
    """Overview district rows against the chapter-derived provincial annual totals."""
    chapter = {(r['year'], r['province']): r for r in province_annual if r.get('category') == 'all' and r.get('period_type', 'annual') == 'annual'
               and r.get('court_tier', 'all_courts') == 'all_courts'}
    out = []
    for r in rows:
        if r['court_level'] != 'district':
            continue
        c = chapter.get((r['year'], r['province']))
        for f in FOUR:
            cv = c.get(f) if c else None
            out.append(dict(year=r['year'], province=r['province'], metric=f, overview=r[f], chapter=cv,
                            chapter_source_ids=(c.get('source_ids') or [c.get('source_id')]) if c else None,
                            chapter_method=c.get('method') if c else None,
                            difference=None if cv is None else r[f] - cv,
                            status='no_chapter_total' if cv is None else ('chapter_has_no_total_overview_used' if c.get('method') == 'overview_table_only'
                                                                          else ('matches' if r[f] == cv else 'overview_differs_from_chapter'))))
    return out
