"""Read a legacy .xls workbook into the shape the shared reader expects.

Two jobs, both about matching what `etl/stage2/read_workbook.py` gets from an
.xlsx file:

  Empty cells. xlrd yields '' for a blank cell where openpyxl yields None, and
  the reader tests for None. Without this every blank cell looks like text.

  Merged ranges. PBS uses a merged banner to say that one label covers several
  columns - table 10's header reads TOTAL POPULATION across columns 1 to 3,
  RURAL across 4 to 6 and URBAN across 7 to 9, with TOTAL, PAKISTANI and
  NON-PAKISTANI repeating underneath. Without the merge information those nine
  columns are indistinguishable by label, and the rural and urban blocks
  silently collapse onto the total one: the extract reported every nationality
  figure as locality 'all', losing two thirds of the table's meaning while
  looking complete.

  xlrd only reports merges when the workbook is opened with formatting_info, and
  reports them as (row_lo, row_hi, col_lo, col_hi) with exclusive upper bounds,
  where the reader expects inclusive (r0, c0, r1, c1). Both are converted here.
"""
import warnings

import xlrd


def extend_short_merges(rows, merges, last_col=None, limit=12):
    """Widen a banner merge that stops short of the block it heads.

    PBS's own merge spans are sometimes one column narrow. Table 10 arranges nine
    columns as three locality blocks - TOTAL POPULATION, RURAL, URBAN - each over
    TOTAL, PAKISTANI and NON-PAKISTANI, but in Awaran's workbook the URBAN merge
    covers only two of its three columns. The third column then falls outside
    every banner and its urban figures are reported as the district total: the
    table looks complete and two thirds of it is mislabelled.

    Only an EXISTING merge is widened, and only up to the column before the next
    labelled cell in the same row - or, for the rightmost banner in the row, up to
    `last_col`, the final column PBS numbers. That last case is the one that
    matters: URBAN is the rightmost banner, so there is no following label to stop
    against, and without `last_col` it stays two columns wide and the ninth column
    falls back to the district total, splitting the table 4:3:2 instead of 3:3:3.

    Widening only existing merges is what makes this safe where a general forward
    fill is not. Table 1's header carries several single-cell labels - POPULATION
    1998, the growth rate - which a forward fill would smear across the sex ratio,
    density and household size columns, inventing three different names for one
    column; because those labels are not merges, this leaves them alone.
    """
    out = []
    for r0, c0, r1, c1 in merges:
        if r0 >= limit:
            out.append((r0, c0, r1, c1))
            continue
        row = rows[r0] if r0 < len(rows) else []
        nxt = None
        for c in range(c1 + 1, len(row)):
            if row[c] is not None and str(row[c]).strip():
                nxt = c
                break
        if nxt is not None:
            stop = nxt - 1
        elif last_col is not None:
            stop = last_col
        else:
            stop = c1
        out.append((r0, c0, r1, max(c1, stop)))
    return out


def load(path):
    """(rows, merges) - rows with None for blanks, merges inclusive and widened."""
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        try:
            book = xlrd.open_workbook(path, formatting_info=True)
            merged = book.sheet_by_index(0).merged_cells
        except Exception:
            # A workbook whose formatting record xlrd cannot parse still yields
            # its values; it loses only the banner spans, so say so rather than
            # failing the file.
            book = xlrd.open_workbook(path)
            merged = []
    s = book.sheet_by_index(0)
    rows = [[(None if str(s.cell_value(r, c)).strip() == '' else s.cell_value(r, c))
             for c in range(s.ncols)] for r in range(s.nrows)]
    merges = [(rlo, clo, rhi - 1, chi - 1) for rlo, rhi, clo, chi in merged]
    last_col = max((c for r in rows[:12] for c in range(len(r))
                    if r[c] is not None), default=None)
    return rows, extend_short_merges(rows, merges, last_col)


def synthesise_banner_merges(rows, merges, a, data_cols):
    """Add the banner spans PBS left unmerged in some converted workbooks.

    A banner row names column groups - TOTAL, RURAL, URBAN over four sex columns
    each - and usually says so with a merged cell. Some files carry the text
    without the merge: Sanghar's table 5 has the same layout as Abbottabad's, but
    where Abbottabad merges columns 1-4, 5-8 and 9-12, Sanghar merges nothing, so
    nine of its twelve columns get no locality at all and its rural and urban
    figures are read as if they were the district total.

    The gate is what makes this safe. A row is treated as a banner only when the
    row IMMEDIATELY BELOW it carries a label for every numbered data column - a
    complete sub-header - and the banner row itself has fewer. That is a genuine
    two-level header, and each banner then spans from its own column to just
    before the next label in its row.

    Table 1 is the reason for the gate rather than a plain forward fill. Its
    header has no complete sub-header row: the columns for area, 1998 population
    and the growth rate are labelled on the upper row only. Filling there smears
    POPULATION - 2017 across the sex ratio, density and household size columns and
    invents three different names for one column, which is what a general fill did
    when it was tried.
    """
    if a is None or a < 2 or not data_cols:
        return merges
    have = {(r0, c0) for r0, c0, _, _ in merges}
    out = list(merges)
    for ri in range(a - 1):
        below = ri + 1
        if below >= a:
            break
        labelled_below = [c for c in data_cols
                          if c < len(rows[below]) and str(rows[below][c] or '').strip()]
        if len(labelled_below) != len(data_cols):
            continue                      # not a complete sub-header: not a banner row
        labels = sorted(c for c in range(len(rows[ri]))
                        if str(rows[ri][c] or '').strip())
        if len(labels) >= len(data_cols) or len(labels) < 2:
            continue                      # not sparse enough to be grouping anything
        for j, c0 in enumerate(labels):
            if (ri, c0) in have:
                continue                  # PBS already merged this one
            stop = labels[j + 1] - 1 if j + 1 < len(labels) else max(data_cols)
            if stop > c0:
                out.append((ri, c0, ri, stop))
    return out
