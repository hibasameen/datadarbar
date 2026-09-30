"""District-wise case-category tables (family, narcotics, murder, bail, rent, appeals,
revisions, seven-years-plus, hudood) from the text-layer LJCP annual editions.

Tables are located by their printed "District-wise ..." headings inside each province's
district-judiciary chapter, classified by keyword, and read with the same grid reader as
the curated all/civil/criminal series. Column order for six-column tables (whether the
4th/5th columns are transfers-out/disposal or disposal/transfers-out) is decided from the
printed header where explicit, otherwise from column magnitudes; the evidence is recorded
on every row. The reader is validated by build.py against the curated manual series on the
all/civil/criminal tables it also reads (category_crosscheck). The 2021 edition is scanned
and is not covered here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path

import pdfplumber

from extract import FIELDS, FOUR, HERE, RECEIVED, STANDARD, WORKSPACE, clean, data_name, is_total, read_table, specs

PROVINCES = ['Punjab', 'Sindh', 'Khyber Pakhtunkhwa', 'Balochistan', 'Islamabad']
CATEGORY_RULES = [
    ('civil_appeals', r'civil\s+ap+eals?'), ('criminal_appeals', r'criminal\s+ap+eals?'),
    ('civil_revisions', r'civil\s+revisions?'), ('criminal_revisions', r'criminal\s+revisions?'),
    ('murder', r'murder'), ('narcotics', r'narcot'), ('hudood', r'h[au]d+ood'), ('bail', r'bail'),
    ('rent', r'\brent\b'), ('family', r'family'), ('sentence_7yr_plus', r'(seven|7)\s*years|sentence'),
    ('miscellaneous', r'miscellaneous'),
    ('civil', r'civil\s+cases'), ('criminal', r'criminal\s+cases'), ('all', r'of\s+cases|consolidat'),
]
FIVE = ['pending_start', 'instituted', 'transfers', 'disposed', 'pending_end']
HEADER_FRAGMENTS = ('In', 'Out', 'On', 'in', 'out', 'on')


def classify(heading):
    h = re.sub(r'\s+', ' ', re.sub(r'\b\d\.\d{1,2}(\.\d)?\b', ' ', heading)).lower()
    if re.search(r'age[\s-]*wise|category[\s-]*wise|bench[\s-]*wise|break[\s-]*up|high court|strength|budget', h):
        return None
    for cat, pat in CATEGORY_RULES:
        if re.search(pat, h):
            return cat
    return None


def tier(heading, province='Punjab'):
    """Only Punjab prints separate civil-court and sessions-court tables."""
    h = heading.lower()
    if province != 'Punjab':
        return 'all_courts'
    if 'session' in h:
        return 'sessions_courts'
    if 'civil court' in h:
        return 'civil_courts'
    return 'all_courts'


def windows(year):
    """Province page windows: from the all-cases anchor page to the next chapter's first table."""
    firsts = defaultdict(list)
    anchors = {}
    for s in specs():
        if s['year'] != year:
            continue
        pages = [p for p, _ in s['refs']]
        firsts[s['province']] += pages
        if s['kind'] == 'cases':
            anchors[s['province']] = min(anchors.get(s['province'], 10 ** 6), *pages)
    out = {}
    for i, prov in enumerate(PROVINCES):
        if prov not in firsts:
            continue
        # A chapter with no curated case table (Balochistan 2023) still has category tables
        # after its staffing tables; start from the chapter's first known table instead.
        start = anchors.get(prov, min(firsts[prov]))
        nxt = [min(firsts[p]) for p in PROVINCES[i + 1:] if p in firsts]
        out[prov] = (start, (nxt[0] - 1) if nxt else start + 15)
    return out


# Printed titles that are wrong or missing, resolved by evidence recorded in the note.
OVERRIDES = {
    ('annual_2024', 'Sindh', '4.28'): ('civil_revisions', 'Printed title repeats 4.27 (seven-years-plus). Every district pending_start equals 2023 table 4.24 civil revisions pending_end, so this is the 2024 civil revisions table.'),
    ('annual_2024', 'Khyber Pakhtunkhwa', '5.22'): ('criminal_appeals', 'First title line missing from the PDF text layer; the remaining text "(... Sessions Judges and Additional Sessions Judges)" and the position between 5.21 criminal cases and 5.23 murder match the criminal appeals table in every other edition.'),
}


def has_header(rows):
    head = ' '.join(clean(c) for row in rows[:4] for c in row if c).lower()
    return bool(re.search(r'pending|pendency|previous|name of district|institution', head))


def has_serial(rows):
    """True when data rows start with a serial number before the district name."""
    for row in rows:
        r = [clean(v) for v in row if clean(v)]
        if len(r) > 2 and re.fullmatch(r'\d+[.]?', r[0]) and re.search('[A-Za-z]', r[1]):
            return True
    return False


def has_total(rows):
    return any(is_total(data_name(row, True) or '') for row in rows)


def locate(pdf, year, province, lo, hi):
    """Yield (heading, section, [(page, table_index), ...]) inside a page window."""
    found = []
    open_spec = None
    for pno in range(lo, min(hi, len(pdf.pages)) + 1):
        page = pdf.pages[pno - 1]
        text = page.extract_text() or ''
        # The next chapter (a High Court) ends this province's district-judiciary section.
        if pno > lo + 2 and re.search(r'high court|federal shariat', '\n'.join(text.splitlines()[:4]), re.I):
            break
        heads = page.search(r'District[\s-]*wise', regex=True, case=False) or []
        words = page.extract_words(x_tolerance=1, y_tolerance=2)
        tables = [t for t in page.find_tables() if t.bbox[2] - t.bbox[0] > 300]
        # Numbered titles whose "District-wise" line is missing from the text layer.
        for m in page.search(r'\b\d\.\d{2}\s+[A-Z(]', regex=True) or []:
            inside = any(t.bbox[1] <= m['top'] <= t.bbox[3] for t in tables)
            line = ' '.join(w['text'] for w in words if m['top'] - 3 <= w['top'] <= m['top'] + 34)
            cat = classify(line)
            if m['x0'] < 160 and not inside and cat and (cat != 'all' or 'district' in line.lower()) and not any(abs(m['top'] - h['top']) < 3 for h in heads):
                heads.append(m)
        heads = sorted(heads, key=lambda m: m['top'])
        # Heading text = words on the heading line and the following two lines.
        htexts = []
        for m in heads:
            band = [w for w in words if m['top'] - 3 <= w['top'] <= m['top'] + 34 and not w['text'].isdigit()]
            text = ' '.join(w['text'] for w in sorted(band, key=lambda w: (round(w['top']), w['x0'])))
            sec = re.search(r'\b(\d\.\d{1,2}(?:\.\d)?)\b', ' '.join(w['text'] for w in words if abs(w['top'] - m['top']) < 3 and w['x0'] <= m['x0'] + 1))
            htexts.append((m['top'], text, sec.group(1) if sec else None))
        other = page.search(r'(Age|Category|Bench)[\s-]*wise', regex=True, case=False) or []
        stops = sorted(m['top'] for m in other)
        used = set()
        for ti, t in enumerate(tables):
            rows = t.extract()
            if numeric_width_rows(rows) is None:
                continue  # a title box or notes grid, not a data table
            # A heading printed inside the table's first rows still belongs to it.
            above = [h for h in htexts if h[0] < t.bbox[1] + 40 and h[0] not in used]
            if above and 'strength' in above[-1][1].lower():
                used.add(above[-1][0])
                open_spec = None
                continue
            repeated = (above and open_spec is not None and not open_spec['closed']
                        and classify(above[-1][1]) == classify(open_spec['heading']) and numeric_width_rows(rows) == open_spec['width'])
            if repeated:
                used.add(above[-1][0])
            elif above and (has_header(rows) or open_spec is None or open_spec['closed']):
                top, text, sec = above[-1]
                used.add(top)
                open_spec = dict(heading=text, section=sec, refs=[], page_first=pno, width=numeric_width_rows(rows), closed=False,
                                 serial=has_serial(rows))
                found.append(open_spec)
            elif open_spec is None or open_spec['closed']:
                continue
            else:
                # Continuation (possibly with a repeated header) only if nothing else
                # intervenes, the previous part had no Total row, and the shape matches.
                if any(s < t.bbox[1] for s in stops) or numeric_width_rows(rows) != open_spec['width']:
                    open_spec = None
                    continue
            open_spec['refs'].append((pno, ti))
            if has_total(rows):
                open_spec['closed'] = True
    return found


def numeric_width_rows(rows):
    counts = []
    for row in rows:
        vals = [clean(v) for v in row if clean(v)]
        nums = [v for v in vals if re.fullmatch(r'-?[\d,]+|-{1,2}|—|–', v)]
        # drop a leading serial number if it was counted
        if nums and vals and nums[0] == vals[0] and re.fullmatch(r'\d+[.]?', vals[0]):
            nums = nums[1:]
        if len(nums) >= 4:
            counts.append(len(nums))
    return max(set(counts), key=counts.count) if counts else None


def choose_order(rows, header):
    """Decide whether the 4th/5th numeric columns are (transfers_out, disposed) [STANDARD]
    or (disposed, transfers_out) [RECEIVED]. The stock-flow identity cannot tell them apart
    (it is symmetric in the two), so use the printed header order when it is explicit and
    otherwise the column magnitudes: disposals exceed outward transfers in every district.
    """
    h = header.lower()
    m = [(h.find(k), k) for k in ('received', 'disposal', 'transfer') if h.find(k) >= 0]
    order = [k for _, k in sorted(m)]
    if order[:3] == ['received', 'disposal', 'transfer']:
        return RECEIVED, 'header_received_disposal_transfer'
    if 'out' in h and h.find('out') < h.find('disposal') and 'transfer' in h:
        return STANDARD, 'header_transfer_out_before_disposal'
    c4 = sum(r[STANDARD[3]] or 0 for r in rows)
    c5 = sum(r[STANDARD[4]] or 0 for r in rows)
    if c4 > c5:
        return RECEIVED, f'magnitude_column4_{c4}_gt_column5_{c5}'
    return STANDARD, f'magnitude_column5_{c5}_ge_column4_{c4}'


def extract_edition(pdf, sid, year, catalog, digest):
    records, notes = [], []
    for province, (lo, hi) in windows(year).items():
        seen = defaultdict(int)
        for spec in locate(pdf, year, province, lo, hi):
            cat = classify(spec['heading'])
            override = OVERRIDES.get((sid, province, spec['section']))
            if override:
                cat, spec['override_note'] = override
            if cat is None:
                notes.append(dict(source_id=sid, province=province, page=spec['page_first'], heading=spec['heading'], issue='unclassified_heading'))
                continue
            seen[(cat, tier(spec["heading"], province))] += 1
            if seen[(cat, tier(spec["heading"], province))] > 1:
                # Two tables printed under the same title (e.g. Sindh 2024 4.27/4.28). Keep the
                # second under a suffixed category so nothing is pooled or silently dropped.
                notes.append(dict(source_id=sid, province=province, page=spec['page_first'], heading=spec['heading'], issue=f'duplicate_title_kept_as_{cat}__dup{seen[(cat, tier(spec["heading"], province))]}'))
                cat = f'{cat}__dup{seen[(cat, tier(spec["heading"], province))]}'
            width = spec['width']
            fields = {6: STANDARD, 4: FOUR, 5: FIVE}.get(width)
            if fields is None:
                notes.append(dict(source_id=sid, province=province, page=spec['page_first'], heading=spec['heading'], issue=f'unsupported_width_{width}'))
                continue
            parsed = []
            try:
                for p, t in spec['refs']:
                    parsed += read_table(pdf.pages[p - 1], t, fields, serial=spec['serial'], ignore_names=HEADER_FRAGMENTS)
            except Exception as exc:
                notes.append(dict(source_id=sid, province=province, page=spec['page_first'], heading=spec['heading'], issue=f'read_error: {exc}'))
                continue
            names = {r['name'].lower() for r in parsed if not r['is_total']}
            if names <= {'civil cases', 'criminal cases'}:
                continue  # a province-level civil/criminal summary, not a district table
            evidence = 'four_or_five_columns'
            if width == 6:
                detail = [r for r in parsed if not r['is_total']]
                p0, t0 = spec['refs'][0]
                first = [t for t in pdf.pages[p0 - 1].find_tables() if t.bbox[2] - t.bbox[0] > 300][t0].extract()
                header = ' '.join(clean(c) for row in first[:5] for c in row if c)
                fields, evidence = choose_order(detail, header)
                if fields is RECEIVED:
                    for r in parsed:
                        vals = [r[f] for f in STANDARD]
                        r.update(dict(zip(RECEIVED, vals)))
            section = spec['section'] or f'unnumbered p{spec["page_first"]}'
            for r in parsed:
                r.update(source_id=sid, year=year, province=province, category=cat, court_tier=tier(spec["heading"], province),
                         section=section, heading=re.sub(r'\s+', ' ', spec['heading'])[:200], kind='category_cases',
                         fields=fields, column_order_evidence=evidence, serial=spec['serial'], scope_note=spec.get('override_note', ''),
                         source_url=catalog[sid]['source_url'], source_sha256=digest,
                         record_id=f"{sid}:{province}:{section}:{cat}:{r['pdf_page']}:{r['table_index']}:{r['row_index']}")
            records += parsed
    return records, notes


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--raw', type=Path, default=WORKSPACE / 'raw_data/ljcp')
    ap.add_argument('--out', type=Path, default=WORKSPACE / 'data_darbar_warehouse/ljcp')
    ap.add_argument('--years', type=int, nargs='*', default=[2020, 2022, 2023, 2024])
    a = ap.parse_args()
    catalog = {s['id']: s for s in json.loads((HERE / 'sources.json').read_text())}
    records, notes = [], []
    for year in a.years:
        sid = f'annual_{year}'
        path = a.raw / 'pdfs' / f'{sid}.pdf'
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        if catalog[sid].get('sha256') and digest != catalog[sid]['sha256']:
            raise ValueError(f'PDF differs from pinned source edition: {sid}')
        with pdfplumber.open(path) as pdf:
            r, n = extract_edition(pdf, sid, year, catalog, digest)
        records += r
        notes += n
    (a.out / 'category_observations.json').write_text(json.dumps(records, indent=2, ensure_ascii=False))
    (a.out / 'category_extraction_notes.json').write_text(json.dumps(notes, indent=2, ensure_ascii=False))
    summary = defaultdict(int)
    for r in records:
        if not r['is_total']:
            summary[(r['year'], r['province'], r['category'], r['court_tier'])] += 1
    for k in sorted(summary):
        print(*k, summary[k])
    print(json.dumps(dict(records=len(records), notes=len(notes)), indent=2))


if __name__ == '__main__':
    main()
