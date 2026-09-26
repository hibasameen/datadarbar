"""Reconcile the Census 2017 spreadsheets against the combined district PDFs.

PBS publishes 2017 twice over: 5,356 per-district spreadsheets, and 135 combined
PDFs with all 40 tables for a district in one file. The spreadsheets are a
PDF-to-Excel conversion of the same material, and the 2023 release showed what
that costs - there, the Excel turned the printed missing-value dash into 0 in some
tables and not others, destroying the difference between "none" and "not
reported", and 1,120,488 cells had to be recovered from the PDFs. It also showed
that a whole indicator can differ between the two renderings.

Nothing equivalent has been measured for 2017, so until this runs the 2017 panel
rests on a single rendering.

The comparison is rendering against rendering, not rendering against my extract:
the workbook and the PDF section are each read from scratch and lined up on their
own row labels. A defect in the extraction therefore cannot hide a defect here,
and vice versa.

Alignment: both renderings are generated from the same source in the same order,
so a sequential walk keyed on the stub label is enough. Where a row's token count
does not match the workbook's numeric-cell count the row is recorded as unaligned
rather than compared - guessing an offset would manufacture disagreements.
"""
import argparse, collections, csv, difflib, json, os, pathlib, re, subprocess, sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'stage2'))

from read_xls import load
from read_workbook import anchor, numbered_columns, txt
from table_spec_2017 import SPEC_2017

# `TABLE - 1`, `TABLE 4 -`, `TABLE- 13`, `TABLE -16` all occur
HEADING = re.compile(r'^\s*TABLE\s*-?\s*(\d+)\s*-?\s', re.I)
DASH = {'-', '–', '—', '−'}
NUM = re.compile(r'^-?[\d,]+(?:\.\d+)?$')


def pdf_text(path):
    r = subprocess.run(['pdftotext', '-layout', path, '-'],
                       capture_output=True, text=True)
    return r.stdout


def sections(text):
    """{table number: [lines]} for a combined district PDF.

    A table's section runs from its heading to the next heading. The same heading
    can appear more than once when a long table spans many pages, so the lines are
    accumulated rather than the first block taken.
    """
    out = collections.defaultdict(list)
    current = None
    for line in text.splitlines():
        m = HEADING.match(line)
        if m:
            current = m.group(1).lstrip('0') or '0'
            continue
        if current:
            out[current].append(line)
    return out


COLNUM_LINE = re.compile(r'^\s*1\s+2\s+3\b')


def data_offset(lines):
    """The character column at which a section's data cells begin.

    Needed because a row label can itself contain digits. Tables 6, 12, 14, 15
    and 16 are stubbed with age ranges, so `10 -- 14` yields the tokens "10" and
    "14"; scanning a line for its first numeric token then puts the label region
    at zero width and the row is dropped. Not one row of tables 6, 12 or 16
    aligned until this was fixed.

    The offset is taken from the row in which PBS numbers its columns, midway
    between the end of the "1" under the stub and the start of the "2" over the
    first data column, so that a right-aligned value cannot fall to the left of
    it.
    """
    for line in lines:
        if not COLNUM_LINE.match(line):
            continue
        one = line.index('1')
        two = line.index('2', one + 1)
        return (one + 1 + two) // 2
    return 0


def cells(line, offset=0):
    """The printed cells of a PDF line, in order: numbers and dashes only.

    Two columns printed with a single space between them - `13,957 105.18` - are
    two cells, and a lone dash is a cell, not a separator.
    """
    out = []
    for tok in line[offset:].split():
        if tok in DASH:
            out.append(('dash', tok))
        elif NUM.match(tok):
            out.append(('num', tok))
    return out


def same_number(excel, printed):
    """Compare at the precision the PDF actually prints."""
    t = printed.replace(',', '')
    try:
        p = float(t)
    except ValueError:
        return False
    dp = len(t.split('.')[1]) if '.' in t else 0
    return abs(p - float(excel)) <= 0.5 * 10 ** -dp + 1e-9


def stub_key(s):
    """A row label reduced to what both renderings agree on."""
    return re.sub(r'[^A-Z0-9]', '', str(s or '').upper())


def workbook_rows(path):
    """[(stub key, stub text, row index, [(column, value) ...])] for the data rows.

    The row index travels with the row so that a mask produced here joins the
    panel on (district, table, src_row, src_col) - an exact key. Matching on the
    row's printed label instead would be ambiguous: a label like "10 -- 14" recurs
    once per unit, locality and sex.
    """
    rows, merges = load(path)
    a = anchor(rows)
    if a is None:
        return None, None
    stub, dcols = numbered_columns(rows, a)
    if not dcols:
        return None, None
    out = []
    for i, r in enumerate(rows):
        if i <= a or stub >= len(r):
            continue
        lab = txt(r[stub])
        if not lab:
            continue
        vals = [(c, r[c]) for c in dcols
                if c < len(r) and isinstance(r[c], (int, float))]
        out.append((stub_key(lab), lab, i, vals))
    return out, stub


def pdf_rows(pdf_lines):
    """[(stub key, label, cells)] for the lines of a PDF section that carry data."""
    off = data_offset(pdf_lines)
    out = []
    for line in pdf_lines:
        c = cells(line, off)
        if not c:
            continue
        label = line[:off].strip()
        if label:
            out.append((stub_key(label), label, c))
    return out


def reconcile(xls_path, pdf_lines, window=40):
    """Compare one workbook against its PDF section by a forward-only walk.

    Both renderings come from the same source in the same order, so the PDF
    pointer only ever moves forward: for each workbook row, the next PDF row with
    the same label is looked for ahead of the current position. That is what makes
    a label like "10 -- 14" or "ALL SEXES", which recurs once per unit, locality
    and sex, resolve to the right occurrence.

    A key-indexed lookup was tried first and failed badly on exactly those tables
    - 12, 16 and 6 aligned not one row - because the PDF repeats its column
    headers at every page break, and those repeats consumed the queued
    occurrences ahead of the data rows that needed them.

    `window` bounds the search so that one genuinely missing row cannot drag the
    pointer past a whole block; beyond it the row is recorded as unaligned and the
    pointer stays put.
    """
    wrows, _ = workbook_rows(xls_path)
    if wrows is None:
        return 0, [], [], 0
    prows = pdf_rows(pdf_lines)

    compared, masked, differ, unaligned = 0, [], [], 0
    j = 0
    for skey, label, ri, vals in wrows:
        hit = None
        for k in range(j, min(len(prows), j + window)):
            if prows[k][0] == skey and len(prows[k][2]) == len(vals):
                hit = k
                break
        if hit is None:
            unaligned += 1
            continue
        c = prows[hit][2]
        j = hit + 1
        for (col, excel), (kind, printed) in zip(vals, c):
            compared += 1
            if kind == 'dash':
                if float(excel) == 0.0:
                    masked.append((label, ri, col, printed))
                else:
                    differ.append((label, ri, col, excel, printed))
            elif not same_number(excel, printed):
                differ.append((label, ri, col, excel, printed))
    return compared, masked, differ, unaligned


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', required=True, help='the dated capture directory')
    ap.add_argument('--out', required=True)
    ap.add_argument('--tables', default='')
    ap.add_argument('--districts', type=int, default=0, help='limit, for a trial run')
    a = ap.parse_args()

    man = json.load(open(os.path.join(a.dir, 'retrieval_manifest.json')))
    want = set(a.tables.split(',')) if a.tables else set(SPEC_2017)
    want &= set(SPEC_2017)

    pdfs = [f for f in man['files'] if f['kind'] == 'pdf']
    xls = collections.defaultdict(dict)
    for f in man['files']:
        if f['kind'] == 'xlsx':
            xls[f['district']][('1' if f['table'] == '?' else f['table'])] = f['path']

    # the two renderings name districts differently: ABBOTTABAD vs ABBOTTABAD
    # DISTRICT, Rajan Pur vs RAJANPUR DISTRICT. Matched on letters only.
    def dkey(s):
        return re.sub(r'[^A-Z]', '', s.upper()).replace('DISTRICT', '') \
                 .replace('AGENCY', '').replace('PROTECTEDAREA', '')
    by_key = {}
    for dist, tabs in xls.items():
        by_key.setdefault(dkey(dist), (dist, tabs))

    # Spelling pairs too far apart for the nearest-name cutoff to reach safely.
    # MALIR/MILIR scores 0.80 against a 0.82 threshold, and lowering the
    # threshold to catch it would start admitting pairs that are merely similar.
    EXPLICIT = {'MALIR': 'MILIR'}

    def pair(pdf_district):
        """Match a PDF district to its spreadsheet directory.

        Exact on letters first, then nearest neighbour. The two renderings spell
        nine districts differently enough that letters alone do not match:
        CHAGAI/CHAGHI, KHUZDAR/KHUZADAR, MALIR/MILIR, MUZAFFARGARH/MUZAFARGARH,
        DERA BUGTI/DERABUGHTI, KILLA SAIFULLAH/KILLASAIFULLA, MANDI
        BAHAUDDIN/MANDI BAHUDDIN, SHAHEED BENAZIRABAD/SHAHEEDBANAZIRABAD. Left
        unmatched, those districts are simply never checked against their PDF.

        The cutoff is high and every inexact match is recorded, because a wrong
        pairing would compare one district's spreadsheet against another's PDF
        and report the difference as a defect.
        """
        k = dkey(pdf_district)
        if k in by_key:
            return by_key[k], None
        if k in EXPLICIT and dkey(EXPLICIT[k]) in by_key:
            return by_key[dkey(EXPLICIT[k])], EXPLICIT[k]
        near = difflib.get_close_matches(k, list(by_key), n=1, cutoff=0.82)
        if not near:
            return None, None
        return by_key[near[0]], near[0]

    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    tot = collections.Counter()
    per_table = collections.defaultdict(collections.Counter)
    unmatched_districts = []
    fuzzy_matches = []
    mask_rows, differ_rows = [], []

    todo = pdfs[:a.districts] if a.districts else pdfs
    for i, pf in enumerate(todo, 1):
        hit, fuzzy = pair(pf['district'])
        if not hit:
            unmatched_districts.append(pf['district'])
            continue
        dist, tabs = hit
        if fuzzy:
            fuzzy_matches.append(dict(pdf=pf['district'], spreadsheet=dist))
        secs = sections(pdf_text(os.path.join(a.dir, pf['path'])))
        for t in sorted(want, key=int):
            if t not in tabs or t not in secs:
                per_table[t]['absent_in_one_rendering'] += 1
                continue
            comp, masked, differ, unal = reconcile(os.path.join(a.dir, tabs[t]), secs[t])
            tot['compared'] += comp
            tot['masked'] += len(masked)
            tot['differ'] += len(differ)
            tot['unaligned_rows'] += unal
            per_table[t]['compared'] += comp
            per_table[t]['masked'] += len(masked)
            per_table[t]['differ'] += len(differ)
            per_table[t]['unaligned_rows'] += unal
            for lab, ri, col, printed in masked:
                mask_rows.append(dict(district=dist, table_id=t, row_label=lab,
                                      src_row=ri + 1, src_col=col + 1, pdf=printed))
            for lab, ri, col, excel, printed in differ:
                differ_rows.append(dict(district=dist, table_id=t, row_label=lab,
                                        src_row=ri + 1, src_col=col + 1,
                                        excel=excel, pdf=printed))
        if i % 10 == 0 or i == len(todo):
            print(f'  {i}/{len(todo)} districts  compared {tot["compared"]:,}  '
                  f'dash-as-zero {tot["masked"]:,}  disagree {tot["differ"]:,}  '
                  f'unaligned {tot["unaligned_rows"]:,}', flush=True)

    for name, rowset, fields in [
            ('missingness_mask_2017.csv', mask_rows,
             ['district', 'table_id', 'row_label', 'src_row', 'src_col', 'pdf']),
            ('value_disagreements_2017.csv', differ_rows,
             ['district', 'table_id', 'row_label', 'src_row', 'src_col', 'excel', 'pdf'])]:
        with open(out / name, 'w', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=fields, quoting=csv.QUOTE_MINIMAL)
            w.writeheader()
            for r in rowset:
                w.writerow(r)

    json.dump(dict(totals=dict(tot), per_table={t: dict(c) for t, c in per_table.items()},
                   districts=len(todo), unmatched_districts=unmatched_districts,
                   fuzzy_district_matches=fuzzy_matches),
              open(out / 'reconciliation_report_2017.json', 'w'), indent=1, sort_keys=True)
    print('\n' + json.dumps(dict(tot), indent=1, sort_keys=True))
    if fuzzy_matches:
        print(f'\ndistricts paired by nearest name ({len(fuzzy_matches)}):')
        for m in fuzzy_matches:
            print(f"   {m['pdf']:32s} -> {m['spreadsheet']}")
    if unmatched_districts:
        print(f'districts whose PDF could not be paired: {unmatched_districts}')


if __name__ == '__main__':
    main()
