#!/usr/bin/env python3
"""NEPRA State of Industry Reports (annual PDFs, 2010-2025) -> every ruled table, cell by cell.

Approach: PyMuPDF `page.find_tables(strategy='lines_strict')` on every page; each table's cells
are kept verbatim (cells layer) and then tidied (long layer) with header/row-label inference.
No per-table code: the ~90 annex tables per report (plant-wise capacity/generation, GENCO/IPP
fuel & heat-rate data, DISCO consumers/sales/losses/recovery, feeders, transformers, safety,
subsidies, tariffs...) all come through the same path. Scanned reports (no text layer) are skipped
and listed in nepra_soi_pages.parquet.

Inputs : $NEPRA_RAW/SOI Reports/State of Industry Report YYYY.pdf  (YYYY = report year; data FY = YYYY-1/YYYY)
Outputs: $DD_WAREHOUSE/nepra_soi_cells.parquet   raw grid: report_year,page,table_seq,table_no,title,row,col,value
         $DD_WAREHOUSE/nepra_soi_long.parquet    tidy: one row per numeric cell with inferred labels
         $DD_WAREHOUSE/nepra_soi_pages.parquet   per-page diagnostics (chars, n_tables)

Long-layer columns
  report_year  int (the report's cover year, e.g. 2020 = SOI 2019-20)
  table_no     'TABLE N' number printed above the table (annex tables), NULL for in-text tables
  title        first full-width row of the grid, else nearest text line above the table
  page         0-based PDF page index
  table_seq    table order on that page
  group        left-most label column, forward-filled (e.g. DISCO name in DISCO x Year tables)
  row_label    second label column when present (e.g. '2018-19'), else = group
  col_label    header text for that column ('|'-joined when the header spans several rows)
  col          0-based column index in the grid
  value_raw    the cell string
  value        parsed number (commas/spaces/parentheses handled), NULL if not numeric
"""
import os, re, sys
from pathlib import Path
import pandas as pd

from dd_paths import NEPRA_RAW as RAW, WAREHOUSE as WH, NEPRA_CACHE
NUM_RE = re.compile(r'^\(?-?[\d,]*\.?\d+\)?%?$')

def to_num(s):
    if s is None:
        return None
    t = s.strip().replace(',', '').replace(' ', '').replace('\n', '')
    if t in ('', '-', '--', '---', 'N/A', 'NA', 'nil', 'Nil', '_'):
        return None
    neg = t.startswith('(') and t.endswith(')')
    t = t.strip('()').rstrip('%')
    try:
        v = float(t)
    except ValueError:
        return None
    return -v if neg else v

def clean(s):
    if s is None:
        return None
    s = re.sub(r'\s+', ' ', s.replace('\n', ' ')).strip()
    return s or None

def merged_owners(t):
    """For every grid position covered by a merged (spanning) cell, return the (row,col) of the
    cell that owns it. Uses cell bboxes: single-column width = narrowest cell starting at that
    column's left edge; same for rows."""
    nr, nc = t.row_count, t.col_count
    rows = [r.cells for r in t.rows]
    col_x0 = [min((c[0] for r in rows for c in [r[ci]] if c), default=None) for ci in range(nc)]
    col_x1 = []
    for ci in range(nc):
        xs = [c[2] for r in rows for c in [r[ci]] if c and col_x0[ci] is not None and abs(c[0] - col_x0[ci]) < 2]
        col_x1.append(min(xs) if xs else None)
    row_y0 = [min((c[1] for c in r if c), default=None) for r in rows]
    row_y1 = []
    for ri, r in enumerate(rows):
        ys = [c[3] for c in r if c and row_y0[ri] is not None and abs(c[1] - row_y0[ri]) < 2]
        row_y1.append(min(ys) if ys else None)
    owner = {}
    for ri in range(nr):
        for ci in range(nc):
            if rows[ri][ci] is not None:
                continue
            if None in (col_x0[ci], col_x1[ci], row_y0[ri], row_y1[ri]):
                continue
            cx = (col_x0[ci] + col_x1[ci]) / 2; cy = (row_y0[ri] + row_y1[ri]) / 2
            found = None
            for r2 in range(ri, -1, -1):
                for c2 in range(ci, -1, -1):
                    b = rows[r2][c2]
                    if b and b[0] - 1 <= cx <= b[2] + 1 and b[1] - 1 <= cy <= b[3] + 1:
                        found = (r2, c2); break
                if found: break
            if found:
                owner[(ri, ci)] = found
    return owner

def extract_report(pdf_path, year):
    import pymupdf  # only extraction needs it; `--from-cells` re-tidies without it
    doc = pymupdf.open(pdf_path)
    cells, pages = [], []
    for pi, pg in enumerate(doc):
        text = pg.get_text()
        nchar = len(text.strip())
        ntab = 0
        if nchar > 50:
            # landscape tables are drawn rotated on portrait pages: detect the dominant text
            # direction and rotate the page so find_tables sees proper rows/columns
            dirs = {}
            for b in pg.get_text('dict').get('blocks', []):
                for ln in b.get('lines', []):
                    d = tuple(round(x) for x in ln['dir'])
                    dirs[d] = dirs.get(d, 0) + len(ln.get('spans', []))
            dom = max(dirs, key=dirs.get) if dirs else (1, 0)
            rot = {(0, -1): 90, (0, 1): 270, (-1, 0): 180}.get(dom, 0)
            if rot:
                pg.set_rotation(rot)
            # "TABLE N <title>" labels in reading order (the label may wrap onto the next line)
            lines = [l.strip() for l in text.split('\n')]
            labels = []
            for li, l in enumerate(lines):
                m = re.match(r'^TABLE\s*[-\s]?(\d+)\s*[:.\-]?\s*(.*)$', l, re.I)
                if m:
                    ttl = m.group(2).strip()
                    if not ttl:
                        nxt = [x for x in lines[li + 1:li + 3] if x]
                        ttl = nxt[0] if nxt else ''
                    labels.append((int(m.group(1)), ttl[:200]))
            try:
                tabs = pg.find_tables(strategy='lines_strict').tables
            except Exception as e:
                tabs = []
                print(f'  {year} p{pi}: find_tables failed {e}', file=sys.stderr)
            tabs = [t for t in tabs if t.row_count >= 2 and t.col_count >= 2]
            tabs.sort(key=lambda t: (t.bbox[1], t.bbox[0]))
            # assign labels to tables by position: each label goes to the first table below it
            assign = {}
            if labels and not rot:
                lab_pos = []
                words = pg.get_text('words')
                found = {}
                for wi, w in enumerate(words):
                    tok = w[4].upper()
                    no = None
                    if tok == 'TABLE' and wi + 1 < len(words):
                        nxt = re.sub(r'[^\d]', '', words[wi + 1][4])
                        no = int(nxt) if nxt else None
                    elif re.fullmatch(r'TABLE[-]?\d+', tok):
                        no = int(re.sub(r'\D', '', tok))
                    if no is not None and no not in found:
                        found[no] = w[1]
                for k, (no, ttl) in enumerate(labels):
                    if no in found:
                        lab_pos.append((found[no], k))
                lab_pos.sort()
                used = set()
                for y, k in lab_pos:
                    for ti, t in enumerate(tabs):
                        if ti not in used and t.bbox[1] > y - 2:
                            assign[ti] = k; used.add(ti); break
            elif labels:  # rotated page: fall back to order
                for ti in range(len(tabs)):
                    assign[ti] = min(ti, len(labels) - 1)
            for ti, t in enumerate(tabs):
                grid = t.extract()
                ntab += 1
                if ti in assign:
                    table_no, near = labels[assign[ti]]
                else:
                    table_no, near = None, None
                if rot:
                    near = (near or '') + ' [rotated]'
                # title: first row that is a single full-width cell
                title = None
                r0 = grid[0]
                nonnull = [c for c in r0 if clean(c)]
                if len(nonnull) == 1 and clean(r0[0]):
                    title = clean(r0[0])
                owner = merged_owners(t)
                for ri, row in enumerate(grid):
                    for ci, v in enumerate(row):
                        orr, occ = owner.get((ri, ci), (ri, ci))
                        cells.append((year, pi, ti, table_no, title, near, ri, ci, clean(v), orr, occ))
            if rot:
                pg.set_rotation(0)
        pages.append((year, pi, nchar, ntab))
    doc.close()
    return cells, pages

def tidy(cells):
    """cells -> long: infer header rows, group/row labels per table."""
    out = []
    cols = ['report_year','page','table_seq','table_no','title','near','row','col','value_raw','owner_row','owner_col']
    # object dtype keeps empty cells as None (newer pandas would turn them into NaN, which is truthy)
    df = pd.DataFrame(cells, columns=cols, dtype=object)
    YEAR_RE = re.compile(r'^(19|20)\d\d(-\d{2,4})?$')
    prev = None  # last labelled table: (year, page, table_no, title, ncol)
    for (yr, pg, ts), g in df.groupby(['report_year','page','table_seq'], sort=True):
        ncol = g.col.max() + 1
        grid, owner = {}, {}
        for r in g.itertuples(index=False):
            grid[(r.row, r.col)] = r.value_raw
            owner[(r.row, r.col)] = (r.owner_row, r.owner_col)
        def gv(ri, c):
            """value at (ri,c), resolving merged cells to their owner."""
            v = grid.get((ri, c))
            if v is None:
                o = owner.get((ri, c))
                if o and o != (ri, c):
                    v = grid.get(o)
            return v
        nrow = g.row.max() + 1
        title = g.title.iloc[0]; near = g.near.iloc[0]; tno = g.table_no.iloc[0]
        tno = None if pd.isna(tno) else int(tno)
        start = 1 if title else 0
        section0 = None
        continued = False
        # `near` = the "TABLE N <caption>" caption printed above the grid (most years), if any
        near_title = re.sub(r'\s*\[rotated\]$', '', near).strip() if near else None
        near_title = near_title or None
        if title and near_title and (title.startswith('(') or len(title) < 12):
            section0 = title; title = near_title   # first grid row was a sub-heading, not the caption
        elif title is None and near_title:
            title = near_title
        # header rows: leading rows where numeric cells are a minority
        def numeric_share(ri):
            vals = [grid.get((ri, c)) for c in range(ncol)]
            vals = [v for v in vals if v]
            if not vals:
                return 0.0
            # years ('2016', '2018*', '2015-16') are labels, not data
            return sum(1 for v in vals if to_num(v) is not None and not re.match(r'^(19|20)\d\d(-\d{2,4})?\*?$', v.strip())) / len(vals)
        def is_section(ri):
            """a body row that is one label spanning the table (e.g. 'A1: Hydel (WAPDA)')."""
            vals = [(c, grid.get((ri, c))) for c in range(ncol)]
            nn = [(c, v) for c, v in vals if v]
            if len(nn) != 1 or to_num(nn[0][1]) is not None:
                return False
            c0 = nn[0][0]
            covered = sum(1 for c in range(ncol) if c != c0 and owner.get((ri, c)) == (ri, c0))
            return covered >= max(1, ncol // 2)
        hdr_rows = []
        ri = start
        while ri < nrow and numeric_share(ri) < 0.5 and len(hdr_rows) < 4 and not (hdr_rows and is_section(ri)):
            hdr_rows.append(ri); ri += 1
        data_start = ri
        # column labels: merged header cells resolve to their owning cell (both directions)
        col_label = {}
        for c in range(ncol):
            parts = []
            for hr in hdr_rows:
                v = gv(hr, c)
                if v and (not parts or parts[-1] != v):
                    parts.append(v)
            col_label[c] = '|'.join(parts) if parts else None
        hdr_sig = tuple(col_label.get(c) for c in range(ncol))
        if tno is None and near_title is None and prev is not None and prev[0] == yr and (pg - prev[1]) <= 1 \
                and ncol == prev[4]:
            # continuation of the previous labelled table on the same/next page: accept when the header is
            # repeated verbatim, when there is no header at all, or when the caption row repeats the title
            same_hdr = hdr_sig == prev[5]
            no_hdr = not hdr_rows
            same_title = bool(title and prev[3] and title[:25].lower() == prev[3][:25].lower())
            if same_hdr or no_hdr or same_title:
                tno, continued = prev[2], True
                title = title or prev[3]
        if tno is not None:
            prev = (yr, pg, tno, title, ncol, hdr_sig)
        # label columns: leading columns whose data cells are mostly non-numeric
        label_cols = []
        for c in range(min(ncol, 3)):
            vals = [grid.get((r, c)) for r in range(data_start, nrow)]
            vals = [v for v in vals if v]
            if not vals:
                if c == 0:
                    label_cols.append(c); continue
                break
            nonnum = sum(1 for v in vals if to_num(v) is None or re.match(r'^\d{4}-\d{2}$', v))
            if nonnum / len(vals) >= 0.6:
                label_cols.append(c)
            else:
                break
        if not label_cols:
            label_cols = [0]
        # a 'year' column among the value columns (e.g. 'As on 30th June' = 2016) becomes the row label
        year_col = None
        if len(label_cols) == 1:
            for c in range(label_cols[-1] + 1, ncol):
                vals = [grid.get((r, c)) for r in range(data_start, nrow)]
                vals = [v for v in vals if v]
                if vals and sum(1 for v in vals if YEAR_RE.match(v.replace(' ', ''))) / len(vals) >= 0.6:
                    year_col = c; break
        # A header block inside the body re-labels the columns below it. KE's rows in the units-purchased
        # table, for instance, carry their own header (KE Own | Through NTDC | ... | Total Unit Purchased)
        # under the DISCOs' one; without this every KE column took the DISCO label of its position.
        # Strict on purpose: short text cells with letters, no numbers, no placeholders (n.p., Nil, ...),
        # at least three of them, and numbers in a row soon after -- so prose tables and placeholder
        # rows are left alone.
        PLACEHOLDER = re.compile(r'^(n\.?\s*[apr]\.?|nil|none|-+|_+|—|no outage)$', re.I)
        vcols = [c for c in range(ncol) if c not in label_cols and c != year_col]
        def texty(ri, letters=True):
            vals = [grid.get((ri, c)) for c in vcols]
            vals = [v for v in vals if v]
            ok = bool(vals) and all(to_num(v) is None and (re.search('[A-Za-z]', v) or not letters)
                                    and len(v) <= 60 and not PLACEHOLDER.match(v.strip()) for v in vals)
            return ok, len(vals)
        def data_soon(ri):
            return any(numeric_share(rr) >= 0.5 for rr in range(ri + 1, min(ri + 4, nrow)))
        group_prev = None
        section = section0
        skip_to = data_start
        for r in range(data_start, nrow):
            if r < skip_to:
                continue
            ok, n = texty(r)
            if ok and not is_section(r):
                block, rr = [], r
                # later rows of the block may be letter-free sub-headers such as '80-90%'
                while rr < nrow and len(block) < 4 and texty(rr, letters=not block)[0] and not is_section(rr):
                    block.append(rr); rr += 1
                if any(texty(b)[1] >= 3 for b in block) and data_soon(block[-1]):
                    for c in vcols:
                        parts = []
                        for hr in block:
                            v = gv(hr, c)
                            if v and (not parts or parts[-1] != v):
                                parts.append(v)
                        if parts:
                            col_label[c] = '|'.join(parts)
                    for hr in block:
                        c0 = grid.get((hr, label_cols[0]))
                        if c0 is not None:
                            group_prev = c0
                    skip_to = rr
                    continue
            if is_section(r):
                section = next(v for v in (grid.get((r, c)) for c in range(ncol)) if v)
                group_prev = None
                continue
            c0 = grid.get((r, label_cols[0]))
            if c0 is not None:
                group_prev = c0
            group = group_prev
            row_label = grid.get((r, label_cols[1])) if len(label_cols) > 1 else None
            if row_label is None and year_col is not None:
                row_label = grid.get((r, year_col))
            if row_label is None:
                row_label = c0 if c0 is not None else group
            for c in range(ncol):
                if c in label_cols or c == year_col:
                    continue
                v = grid.get((r, c))
                if v is None:
                    continue
                out.append(dict(report_year=yr, table_no=tno, title=title, near=near, page=pg, table_seq=ts,
                                continued=continued, section=section, group=group, row_label=row_label, col_label=col_label.get(c), col=c,
                                row=r, value_raw=v, value=to_num(v)))
    return df, pd.DataFrame(out)

def main():
    import pickle
    if '--from-cells' in sys.argv:
        # re-tidy the stored grids (nepra_soi_cells) without the PDFs or the extraction cache
        cells = pd.read_parquet(WH / 'nepra_soi_cells.parquet')
        cols = ['report_year','page','table_seq','table_no','title','near','row','col','value_raw','owner_row','owner_col']
        rows = [tuple(None if pd.isna(v) else v for v in t) for t in cells[cols].itertuples(index=False, name=None)]
        _, long_df = tidy(rows)
        long_df.to_parquet(WH / 'nepra_soi_long.parquet', index=False)
        print(f're-tidied from cells: long={len(long_df)}', file=sys.stderr)
        return
    force = '--force' in sys.argv
    years = [int(a) for a in sys.argv[1:] if a.isdigit()] or list(range(2010, 2026))
    cache = NEPRA_CACHE; cache.mkdir(parents=True, exist_ok=True)
    all_cells, all_pages = [], []
    for y in years:
        p = RAW / 'SOI Reports' / f'State of Industry Report {y}.pdf'
        if not p.exists():
            print(f'missing {p}', file=sys.stderr); continue
        cp = cache / f'soi_{y}.pkl'
        if cp.exists() and not force:
            cells, pages = pickle.load(open(cp, 'rb'))
        else:
            cells, pages = extract_report(p, y)
            pickle.dump((cells, pages), open(cp, 'wb'))
        ntab = len({(c[1], c[2]) for c in cells})
        textpages = sum(1 for pg in pages if pg[2] > 50)
        print(f'{y}: {len(pages)} pages ({textpages} with text), {ntab} tables, {len(cells)} cells', file=sys.stderr)
        all_cells += cells; all_pages += pages
    cells_df, long_df = tidy(all_cells)
    cells_df.to_parquet(WH / 'nepra_soi_cells.parquet', index=False)
    long_df.to_parquet(WH / 'nepra_soi_long.parquet', index=False)
    pd.DataFrame(all_pages, columns=['report_year','page','n_chars','n_tables']).to_parquet(WH / 'nepra_soi_pages.parquet', index=False)
    print(f'wrote cells={len(cells_df)} long={len(long_df)}', file=sys.stderr)

if __name__ == '__main__':
    main()
