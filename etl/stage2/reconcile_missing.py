"""Stage 3: recover the printed dash that the Excel release turned into zero.

PBS publishes each table twice. The Excel files are clean grids but, in some
tables, the printed missing-value dash has been replaced by 0 — destroying the
difference between "none" and "not reported". The PDFs keep the dash.

This aligns the two renderings row by row. Both are generated from the same
workbook and so run in the same order, which means a sequential walk keyed on
the row label is enough; no geometric parsing of the PDF is needed.

Output is a mask of cells where the Excel says 0 and the PDF says dash, plus a
log of anywhere the two renderings disagree on an actual value.
"""
import argparse, collections, csv, json, pathlib, re, subprocess, sys
import openpyxl
from read_workbook import anchor, label_column, txt, ADMIN, unit_type
from table_spec import SPEC

DASH = {'-', '–', '—'}
NUM = re.compile(r'^-?[\d,]+(?:\.\d+)?$')


def same_number(excel, pdf_text):
    """Compare at the precision the PDF actually prints.

    The PDF is a display of the data, not the data: table 13(a) prints a
    literacy rate of 91.795% as "92". Comparing full precision against a
    rounded display reports a disagreement that does not exist.
    """
    t = pdf_text.replace(',', '')
    try:
        p = float(t)
    except ValueError:
        return False
    dp = len(t.split('.')[1]) if '.' in t else 0
    # Half a unit in the last printed place, plus an epsilon: 40.625 displayed
    # as "40.63" is a rounding choice, not a different number, and the
    # subtraction lands a hair either side of the boundary in binary floats.
    return abs(p - float(excel)) <= 0.5 * 10 ** -dp + 1e-9


def pdf_lines(path):
    out = subprocess.run(['pdftotext', '-layout', str(path), '-'],
                         capture_output=True, text=True, check=True).stdout
    return out.splitlines()


CELL = re.compile(r'-?[\d,]+(?:\.\d+)?|[-\u2013\u2014]')


def tokens(line):
    """Split a laid-out PDF line into its label and the cells after it.

    Columns are normally separated by two or more spaces, but a wide value can
    crowd its neighbour: Punjab's table 1 prints "13,957 105.18" with a single
    space between the transgender count and the sex ratio. Any chunk that turns
    out to hold more than one value is split again on the values themselves.
    """
    parts = [p.strip() for p in re.split(r'\s{2,}', line.strip()) if p.strip()]
    out = []
    for i, p in enumerate(parts):
        found = CELL.findall(p)
        # Only re-split a chunk that is entirely values; a label may contain
        # digits ("Not L.F & Stud (15 to 24)") and must be left alone.
        if i > 0 and len(found) > 1 and ''.join(found) == re.sub(r'\s+', '', p):
            out.extend(found)
        else:
            out.append(p)
    return out


def key(label):
    return re.sub(r'[^A-Z0-9]', '', (label or '').upper())


def excel_rows(path, table):
    ws = openpyxl.load_workbook(path, read_only=True, data_only=True).worksheets[0]
    rows = [list(r) for r in ws.iter_rows(values_only=True)]
    a = anchor(rows)
    lc = label_column(rows, a)
    out = []
    for i, r in enumerate(rows):
        if i <= a:
            continue
        lab = txt(r[lc]) if lc < len(r) else None
        if not lab:
            continue
        vals = [(j, v) for j, v in enumerate(r) if j > lc]
        out.append(dict(src_row=i + 1, label=lab, key=key(lab), values=vals))
    return out


def is_unit(label):
    L = (label or '').upper()
    return bool(ADMIN.search(L)) and unit_type(label) is not None


def reconcile(xlsx, pdf, table, region):
    """Align the two renderings unit by unit.

    A single global walk slips permanently the first time one rendering carries
    a row the other does not — a repeated page header, a stray blank. Resetting
    the pointer at every unit boundary confines any slip to one unit, and both
    renderings list units in the same order.
    """
    xr = excel_rows(xlsx, table)
    pl = [t for t in (tokens(l) for l in pdf_lines(pdf)) if t]
    pdf_units = [(i, key(t[0])) for i, t in enumerate(pl) if is_unit(t[0])]
    mask, mismatch, unaligned = [], [], []
    clipped_rows = 0
    pi = 0
    unit_ptr = 0
    matched = verified = 0
    for row in xr:
        if is_unit(row['label']):
            # jump the PDF pointer to the same unit
            nxt = next((i for i, (_, k) in enumerate(pdf_units[unit_ptr:], unit_ptr)
                        if k == row['key']), None)
            if nxt is not None:
                unit_ptr = nxt + 1
                pi = pdf_units[nxt][0]
        span = pdf_units[unit_ptr][0] if unit_ptr < len(pdf_units) else len(pl)
        j, found = pi, None
        while j < len(pl) and j <= max(span, pi + 40):
            if key(pl[j][0]) == row['key']:
                found = j
                break
            j += 1
        if found is None:
            continue
        matched += 1
        pi = found + 1
        cells = pl[found][1:]
        seq = [(c, v) for c, v in row['values'] if v is not None]

        # Only trust the positional mapping if the row's unambiguous values —
        # the non-zero numbers — agree between the two renderings. A dash in one
        # and a zero in the other is what we are here to find, so those are
        # allowed to differ; anything else means the columns have drifted and
        # the row is reported unreconciled rather than masked.
        clipped = 0
        if len(seq) != len(cells):
            # The Punjab table 1 PDF is too narrow for its last column: the
            # growth-rate values run off the page. Where the PDF is short only
            # at the right-hand end, check the prefix and leave the missing
            # columns unchecked rather than discarding the whole row.
            if 0 < len(seq) - len(cells) <= 2:
                clipped = len(seq) - len(cells)
                seq = seq[:len(cells)]
            else:
                unaligned.append(dict(table_id=table, region=region, src_row=row['src_row'],
                                      label=row['label'],
                                      reason=f'{len(seq)} excel cells vs {len(cells)} pdf cells',
                                      agreeing=0, disagreeing=0))
                continue
        # Alignment is trusted only if most unambiguous values agree. One or two
        # disagreements in an otherwise matching row are a data defect in one of
        # the renderings; many disagreements mean the columns have drifted.
        agree = disagree = 0
        for (c, v), p in zip(seq, cells):
            if isinstance(v, (int, float)) and v != 0 and NUM.match(p):
                if same_number(v, p):
                    agree += 1
                else:
                    disagree += 1
        if disagree and (agree < 2 or disagree > agree):
            # Distinguish a genuine disagreement from a drifted row. If some
            # columns agree exactly while others differ, the columns are lined
            # up and PBS's two renderings simply do not say the same thing.
            reason = ('the two renderings disagree on this row'
                      if agree >= 1 else 'columns not aligned')
            if clipped:
                reason += f' (pdf clipped {clipped} trailing column(s))'
            unaligned.append(dict(table_id=table, region=region, src_row=row['src_row'],
                                  label=row['label'], reason=reason,
                                  agreeing=agree, disagreeing=disagree))
            continue

        verified += 1
        if clipped:
            clipped_rows += 1
        for (c, v), p in zip(seq, cells):
            if isinstance(v, (int, float)) and v != 0 and NUM.match(p) and not same_number(v, p):
                mismatch.append(dict(table_id=table, region=region, src_row=row['src_row'],
                                     src_col=c + 1, label=row['label'], excel=v, pdf=p))
                continue
            if v == 0 and p in DASH:
                mask.append(dict(table_id=table, region=region, src_row=row['src_row'], src_col=c + 1,
                                 label=row['label'], pdf='dash'))
            elif v == 0 and NUM.match(p) and not same_number(0, p):
                mismatch.append(dict(table_id=table, region=region, src_row=row['src_row'], src_col=c + 1,
                                     label=row['label'], excel=0, pdf=p))
    return mask, mismatch, unaligned, matched, verified, len(xr), clipped_rows


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--out', required=True)
    ap.add_argument('--tables', default=','.join(t for t in SPEC if not SPEC[t].get('locality_list')))
    a = ap.parse_args()
    src, out = pathlib.Path(a.src), pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    regions = ['kp', 'punjab', 'sindh', 'balochistan', 'islamabad']
    allmask, allmis, allun, report = [], [], [], []
    for t in a.tables.split(','):
        for reg in regions:
            x, p = src / 'xlsx' / f'table_{t}_{reg}.xlsx', src / 'pdf' / f'table_{t}_{reg}.pdf'
            if not (x.exists() and p.exists()):
                report.append(dict(table_id=t, region=reg, status='missing file')); continue
            try:
                m, mis, un, matched, verified, total, clip = reconcile(x, p, t, reg)
            except Exception as e:
                report.append(dict(table_id=t, region=reg, status=f'error: {type(e).__name__}: {e}'))
                continue
            allmask += m; allmis += mis; allun += un
            report.append(dict(table_id=t, region=reg, status='ok', rows=total,
                               label_matched=matched, verified=verified,
                               verified_pct=round(100 * verified / max(total, 1), 1),
                               unaligned=len(un), dashes_recovered=len(m),
                               value_mismatches=len(mis), pdf_clipped_rows=clip))
            print(f"  T{t:<4} {reg:12s} verified {verified}/{total} ({100*verified/max(total,1):5.1f}%)  "
                  f"dashes {len(m):7d}  unaligned {len(un):5d}  mismatches {len(mis)}")
    for name, data, cols in [('missingness_mask.csv', allmask, ['table_id','region','src_row','src_col','label','pdf']),
                             ('value_mismatches.csv', allmis, ['table_id','region','src_row','src_col','label','excel','pdf']),
                             ('unaligned_rows.csv', allun, ['table_id','region','src_row','label','reason','agreeing','disagreeing'])]:
        with open(out / name, 'w', newline='') as fh:
            w = csv.DictWriter(fh, fieldnames=cols); w.writeheader(); w.writerows(data)
    (out / 'reconciliation_report.json').write_text(json.dumps(report, indent=2))
    print(f"\ndashes recovered  {len(allmask):,}")
    print(f"value mismatches  {len(allmis):,}")
    byr = collections.Counter(r['reason'] if 'cells vs' not in r['reason']
                              else 'cell count differs' for r in allun)
    print(f"rows not masked   {len(allun):,}")
    for k, v in byr.most_common():
        print(f"    {k:44s} {v:,}")


if __name__ == '__main__':
    main()
