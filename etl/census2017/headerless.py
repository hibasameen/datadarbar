"""Recover the 57 Census 2017 workbooks that carry no column-number row.

PBS's 2017 spreadsheet release is a PDF-to-Excel conversion, and for 57 of the
5,611 files the conversion dropped the header block entirely: no title, no
banner, and - the part that matters - no row in which PBS numbers its columns.
`read_workbook.anchor()` correctly returns None for these, because there is
nothing to anchor on. They are not corrupt; the data rows are intact.

Two different things hide behind that one symptom, and they need opposite
treatment:

  36 files are effectively EMPTY, and explicably so. Every one is a locality
     table for a district that has no localities of that kind: tables 25 and 26
     (urban localities) for fourteen districts whose own table 1 reports zero
     urban population, and tables 3, 23 and 24 (rural localities) for Karachi
     East and Karachi South, which are 100% urban. PBS even writes the reason
     into the sheet - Lahore's table 23 contains the single cell
     "LAHORE IS URBANIZED." These must not be repaired into existence.

  21 files carry REAL DATA with no header. These are recoverable, because the
     sequence of measures across a row is the same as in every other district's
     copy of that table - only the column POSITIONS differ, the converted grid
     being sparse (Swabi's table 1 spreads 12 measures over 26 columns, with
     spacer columns between them, where Mardan's uses 12 columns exactly).

So position cannot be borrowed from a sibling file, but order can. The widest
data row in the file is the unit's own total row, which carries every measure the
table publishes; reading the column positions off that row reconstructs exactly
what PBS's numbered row would have said, and the rest of the file can then be
read by the ordinary reader.

The recovery is checked, not trusted: `verify` requires the recovered district
total to be the figure that makes its province close against the independently
published provincial table. Swabi (1,625,477) and Okara (3,040,826) are precisely
the two amounts by which KP and Punjab failed to close before this ran.
"""
import re

DASH = re.compile(r'^[-‐-―−]+$')


def _is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _txt(v):
    if v is None:
        return ''
    return str(v).strip()


def data_rows(rows):
    """Rows carrying at least `min_num` numeric cells, as (index, numeric columns)."""
    out = []
    for i, r in enumerate(rows):
        cols = [c for c, v in enumerate(r) if _is_num(v)]
        if cols:
            out.append((i, cols))
    return out


def synth_columns(rows, min_measures=3):
    """(stub column, data columns) inferred from the widest data row.

    Returns (None, []) when no row is wide enough to be a unit total row, which
    is how the empty files are distinguished from the recoverable ones - they are
    left alone rather than repaired.
    """
    dr = data_rows(rows)
    if not dr:
        return None, []
    # widest first; on a tie prefer the earliest, which is the unit's total row
    i, cols = max(dr, key=lambda t: (len(t[1]), -t[0]))
    if len(cols) < min_measures:
        return None, []
    # the stub is the last text cell to the left of the first number
    stub = None
    for c in range(cols[0] - 1, -1, -1):
        if _txt(rows[i][c]) and not _is_num(rows[i][c]):
            stub = c
            break
    if stub is None:
        stub = 0
    return stub, cols


def is_empty(rows, min_measures=3):
    """True when the file has no row wide enough to be a unit total row."""
    stub, cols = synth_columns(rows, min_measures)
    return not cols


def reason_text(rows):
    """PBS's own note on why a locality table is empty, when it wrote one."""
    for r in rows:
        for v in r:
            t = _txt(v)
            if t and re.search(r'\bIS\s+(URBANIZED|RURAL)', t, re.I):
                return t
    return None
