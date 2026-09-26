"""Read PBS Census 2023 district-wise workbooks into tidy observations.

The unit hierarchy (district -> sub-division -> tehsil/taluka) and the stub
levels below it are walked generically; what differs per table is declared in
`table_spec.SPEC` rather than inferred, so a layout change shows up as a failed
check instead of silently wrong data.
"""
import re, unicodedata, zipfile
from xml.etree import ElementTree
from table_spec import SPEC
from unit_aliases import apply as apply_alias

ADMIN = re.compile(r'\b(DISTRICT|TEHSIL|TALUKA|TALUKO|SUB-?TEHSIL|SUB-?DIVISION|TOWN'
                   r'|AGENCY)\b|^F\.?R\.? |\bF\.?R\.?$')
# FATA existed as its own unit in 2017 and was merged into KP in 2018, so these
# labels appear in the 2017 tables and in no 2023 table.
#
# Two of them are traps. An agency's own row - BAJAUR AGENCY - carries none of
# the 2023 keywords, so without AGENCY here all seven agencies lose their
# district-level row silently. And each Frontier Region's row, FR BANNU, is
# followed by a sub-unit called TRIBAL AREA ADJ. BANNU DISTRICT, which contains
# the word DISTRICT: recognising that as the district promotes a sub-unit and
# discards the FR, which is what happened before TRIBAL_AREA was added below.
TRIBAL_AREA = re.compile(r'^TRIBAL AREA\b')
GROUP = re.compile(r'\s(QH|PC|STC|TC)$')
# Punjab publishes DE-EXCLUDED AREA RAJANPUR as a sub-district unit carrying
# none of the usual keywords. It holds 41,741 people and Rajanpur does not
# close without it. It is the only unit of its kind in the country.
OTHER_UNIT = re.compile(r'\bDE-?EXCLUDED AREA\b')
# The 2017 tables say OVERALL where the 2023 tables say ALL LOCALITIES, and
# some 2017 tables use both across their own table set.
LOCALITY = {'ALL LOCALITIES': 'all', 'ALL LOCALITY': 'all', 'TOTAL': 'all',
            'OVERALL': 'all', 'ALL DISABLED': 'all',
            'RURAL': 'rural', 'URBAN': 'urban'}
SEX = {'ALL SEXES': 'all', 'BOTH SEXES': 'all', 'TOTAL': 'all', 'MALE': 'male',
       'FEMALE': 'female', 'TRANSGENDER': 'transgender', 'TRANS GENDER': 'transgender'}
PROVINCES = {'KHYBER PAKHTUNKHWA', 'PUNJAB', 'SINDH', 'BALOCHISTAN', 'ISLAMABAD',
             'ISLAMABAD CAPITAL TERRITORY', 'PAKISTAN', 'KP'}
SKIP_PREFIX = ('TABLE', 'NAME OF', 'HADBAST', 'INDICATOR', 'AREA/SEX', 'LOCALITY /',
               'SEX /', 'DISTRICT /')
DASH = {'-', '–', '—'}


def txt(v):
    if not isinstance(v, str):
        return None
    t = re.sub(r'\s+', ' ', unicodedata.normalize('NFKC', v).replace('\n', ' ')).strip()
    return t or None


def cell(v):
    """(value, kind); the printed dash stays distinct from a true zero."""
    if v is None:
        return None, 'blank'
    if isinstance(v, str):
        t = txt(v)
        if t is None:
            return None, 'blank'
        return (None, 'dash') if t in DASH else (t, 'text')
    return v, 'number'


def _plain(s):
    """Upper-case alphanumerics only, for comparing labels PBS punctuates loosely."""
    return re.sub(r'[^A-Z0-9]', '', str(s).upper())


def unit_type(label):
    L = label.upper()
    if L in PROVINCES:
        return None
    # Checked before DISTRICT, because the label contains that word while being a
    # sub-unit of a Frontier Region rather than a district.
    if TRIBAL_AREA.search(L):
        return 'sub_division'
    if 'DISTRICT' in L:
        return 'district'
    # A Frontier Region and an Agency are district-level units of 2017 FATA;
    # "CENTRAL KURRAM F.R", with the marker trailing, sits alongside that
    # agency's tehsils and is not one.
    if re.match(r'^F\.?R\.? \S', L):
        return 'district'
    if re.search(r'\bAGENCY\b', L):
        return 'district'
    if re.search(r'\bF\.?R\.?$', L):
        return 'tehsil'
    if re.search(r'\bSUB-?TEHSIL\b', L):
        return 'sub_tehsil'
    if re.search(r'\bSUB-?DIVISION\b', L):
        return 'sub_division'
    if re.search(r'\b(TEHSIL|TALUKA|TALUKO|TOWN)\b', L):
        return 'tehsil'
    if OTHER_UNIT.search(L):
        return 'other'
    return None


COLNUM = re.compile(r'^(\d+)(?:\.0+)?$')


def _colnum(v):
    """The integer a column-number cell carries, or None.

    The 2023 workbooks store these as integers; the 2017 ones store them as
    floats, so the cell reads "1.0" rather than "1".
    """
    if v is None:
        return None
    m = COLNUM.match(str(v).strip())
    return int(m.group(1)) if m else None


def anchor(rows, limit=40):
    """The '1, 2, 3 …' column-number row PBS prints under every header.

    Some 2017 tables print two of them: one numbering only the data columns,
    then one numbering the stub column as well. The second is the one that
    describes the whole table, so among candidates take the one whose numbering
    starts furthest left, and the later row on a tie. Reading the first instead
    treats the stub as data and the unit label is never seen.
    """
    best = None
    for i, r in enumerate(rows[:limit]):
        cells = [(c, x) for c, x in enumerate(r) if x is not None and str(x).strip() != '']
        if len(cells) < 3:
            continue
        # Must be a consecutive sequence 1, 2, 3 — not merely three numbers
        # starting with one. Table 4's single-year-age rows are stubbed "01",
        # "02" and carry population counts, which a looser test reads as a
        # column-number row and the whole table shifts by a column.
        if [_colnum(x) for _, x in cells[:3]] != [1, 2, 3]:
            continue
        start = cells[0][0]
        if best is None or start < best[1] or (start == best[1] and i > best[0]):
            best = (i, start)
    return best[0] if best else None


def numbered_columns(rows, a):
    """(stub column, data columns) taken from the column-number row.

    That row is PBS's own statement of which columns exist, so it is the only
    reliable source: table 7's Punjab workbook carries 256 columns of which 249
    are empty, and table 10 puts its unit banner inside the data region at
    column 5, where inferring from content gets both wrong.
    """
    nums = [(c, _colnum(v)) for c, v in enumerate(rows[a]) if _colnum(v) is not None]
    if not nums:
        return 0, []
    stub = nums[0][0]
    return stub, [c for c, _ in nums[1:]]


def label_column(rows, a):
    """The stub column, as numbered by PBS itself."""
    return numbered_columns(rows, a)[0]


def row_unit(r, stub):
    """A unit label anywhere in the row, with the column it was found in.

    Most tables put the unit in the stub column; table 10 centres it in the
    middle of the data region instead.
    """
    for c, v in enumerate(r):
        t = txt(v)
        if not t:
            continue
        U = t.upper()
        if (ADMIN.search(U) or OTHER_UNIT.search(U)) and unit_type(t):
            return t, c
    return None, None


def merged_ranges(xlsx_path):
    """Merged cell ranges as (r0, c0, r1, c1), zero-based inclusive.

    Read straight from the sheet XML: openpyxl does not expose these in
    read-only mode, and the workbooks are too large to open otherwise.
    """
    try:
        with zipfile.ZipFile(xlsx_path) as z:
            name = next((n for n in z.namelist()
                         if n.startswith('xl/worksheets/sheet') and n.endswith('.xml')), None)
            if not name:
                return []
            root = ElementTree.fromstring(z.read(name))
    except Exception:
        return []
    out = []
    for el in root.iter():
        if not el.tag.endswith('mergeCell'):
            continue
        ref = el.get('ref') or ''
        m = re.match(r'([A-Z]+)(\d+):([A-Z]+)(\d+)$', ref)
        if not m:
            continue
        def col(letters):
            n = 0
            for ch in letters:
                n = n * 26 + ord(ch) - 64
            return n - 1
        out.append((int(m.group(2)) - 1, col(m.group(1)),
                    int(m.group(4)) - 1, col(m.group(3))))
    return out


def header(rows, a, lc, merges=(), data_cols=None):
    """Column labels built from the header rows above the column-number row.

    A merged cell's value applies across its span and no further. Forward-filling
    instead would carry a label past where it belongs: in table 1 it gave column
    11 the label "POPULATION 2017 / AVERAGE H.HOLD SIZE", merging two unrelated
    columns.
    """
    width = (max(data_cols) + 1) if data_cols else max((len(r) for r in rows[:a]), default=0)
    grid = {}
    for ri in range(a):
        for ci in range(width):
            v = txt(rows[ri][ci]) if ci < len(rows[ri]) else None
            # The title banner spans the whole sheet and names the table, not a
            # column. It must not become part of any column label.
            if v and not v.upper().startswith('TABLE'):
                grid[(ri, ci)] = v
    for r0, c0, r1, c1 in merges:
        if r0 >= a:
            continue
        if (c1 - c0 + 1) >= width - lc:      # full-width banner, not a column group
            continue
        v = grid.get((r0, c0))
        if not v:
            continue
        for ri in range(r0, min(r1, a - 1) + 1):
            for ci in range(c0, c1 + 1):
                grid.setdefault((ri, ci), v)
    out = {}
    for c in (data_cols if data_cols is not None else range(lc + 1, width)):
        parts = []
        for ri in range(a):
            v = grid.get((ri, c))
            if v and v not in parts:
                parts.append(v)
        out[c] = ' / '.join(parts) if parts else f'col{c + 1}'
    return out


def split_header(label, want):
    """Pull locality and sex out of a column label, leaving the rest."""
    loc = sex = None
    keep = []
    for part in [p.strip() for p in label.split('/')]:
        U = part.upper()
        if 'locality' in want and U in LOCALITY and loc is None:
            loc = LOCALITY[U]
        elif 'sex' in want and U in SEX and sex is None:
            sex = SEX[U]
        else:
            keep.append(part)
    return loc, sex, ' / '.join(keep)


def read(rows, province, table, merges=(), spec=None, layout=None, unit_at=None):
    """Yield one observation per numeric cell.

    The unit hierarchy is walked the same way for every table; what differs is
    the stack of stub levels beneath a unit, declared in `table_spec.SPEC`.

    A row emits whenever it carries numbers, tagged with whatever levels are
    set at that point. That single rule covers every shape in the corpus: a
    unit row that carries its own totals (table 1), a locality row that is both
    a subtotal and a level (table 7's ALL LOCALITIES), and a leaf indicator row
    nested three deep (table 13(a)).

    `unit_at` maps a row index to a unit name, for the 2017 workbooks where PBS
    omitted a unit's name in the middle of the file - sometimes leaving the cell
    blank, sometimes leaving out the row entirely. Supplying it here rather than
    editing the sheet keeps `src_row` pointing at the row the figure came from.

    `spec` and `layout` exist for Census 2017, which this reader also serves.
    2017 declares its shapes in `census2017/table_spec_2017.py`, and 57 of its
    workbooks lost their header block in PBS's PDF-to-Excel conversion, so the
    caller must supply the column positions and labels it recovered instead of
    this function reading them off a row that is not there. Both default to the
    2023 behaviour, which is therefore unchanged.
    """
    spec = spec or SPEC[table]
    levels, want = spec['levels'], spec['header']
    # Census 2017 options; all default off, so 2023 reads exactly as before.
    group_indicator = spec.get('group_indicator', False)
    group_exits = {_plain(x) for x in spec.get('group_exits', ())}
    not_locality = {x.upper() for x in spec.get('not_locality', ())}
    if layout is not None:
        a, lc, data_cols, cols = layout
    else:
        a = anchor(rows)
        if a is None:
            raise ValueError(f'table {table}: no column-number row')
        lc, data_cols = numbered_columns(rows, a)
        cols = header(rows, a, lc, merges, data_cols)
    district = unit = utype = None
    unit_source = None
    state = {}
    group = None

    def emit(r, i, indicator):
        for c, raw in cols.items():
            if c >= len(r):
                continue
            v, kind = cell(r[c])
            if kind == 'text':
                continue
            hloc, hsex, rest = split_header(raw, want)
            yield dict(province=province, table_id=table, district=district, unit=unit,
                       unit_type=utype, unit_source=unit_source,
                       locality=hloc or state.get('locality', 'all'),
                       sex=hsex or state.get('sex', 'all'),
                       indicator=indicator or state.get('indicator') or rest or 'value',
                       col_label=rest, value=v, missing=(kind == 'dash'),
                       src_row=i + 1, src_col=c + 1)

    for i, r in enumerate(rows):
        if i <= a:
            continue
        if unit_at and i in unit_at:
            recovered = unit_at[i]
            rut = unit_type(recovered)
            if rut == 'district':
                district = recovered
            unit, utype, unit_source = recovered, rut, 'recovered: PBS omitted the label'
            state = {}
            group = None
        label = txt(r[lc]) if lc < len(r) else None
        co_unit = None
        if not label:
            # Table 10 centres its unit banner in the middle of the data
            # region rather than putting it in the stub column.
            found, _ = row_unit(r, lc)
            if not found:
                continue
            label = found
        elif unit_type(label) is None:
            # The stub names something else, but the row may still introduce a
            # unit alongside it: Attock's 2017 table 14 puts FATEH JANG TEHSIL in
            # column 4 of the same row whose stub reads OVERALL. Taken as an
            # ordinary locality row, that block and its five successors are all
            # filed under the previous tehsil.
            found, fc = row_unit(r, lc)
            if found and fc != lc:
                co_unit = found

        if co_unit is not None:
            cu, _ = apply_alias(table, co_unit)
            cut = unit_type(cu)
            if cut == 'district':
                district = cu
            unit, utype, unit_source = cu, cut, co_unit
            state = {}
            group = None
        U = label.upper()
        if U.isdigit() or U.startswith(SKIP_PREFIX) or GROUP.search(U):
            continue
        # A row of nothing but dashes still carries information: those cells
        # are reported-missing, not absent. Treating them as an empty row
        # dropped 186 all-dash transgender rows from table 11 alone.
        has_num = any(cell(r[c])[1] in ('number', 'dash')
                      for c in data_cols if c < len(r))

        source_label = label
        label, alias_reason = apply_alias(table, label)
        U = label.upper()
        ut = unit_type(label) if (ADMIN.search(U) or OTHER_UNIT.search(U)
                                  or U in PROVINCES) else None
        if ut:
            if ut == 'district':
                district = label
            unit, utype, unit_source = label, ut, source_label
            state = {}
            group = None
            if has_num:
                yield from emit(r, i, None)
            continue
        if unit is None:
            continue

        # Set whichever declared level this row names, clearing deeper ones.
        for depth, lv in enumerate(levels):
            vocab = LOCALITY if lv == 'locality' else SEX if lv == 'sex' else None
            if vocab is None or U not in vocab:
                continue
            # `TOTAL` means "all localities" in most tables, but 2017's table 27
            # uses it for a sum over housing types inside a locality block. Read
            # as a locality it resets the block to the district total and throws
            # away which locality the row belonged to.
            if lv == 'locality' and U in not_locality:
                continue
            state[lv] = vocab[U]
            for deeper in levels[depth + 1:]:
                state.pop(deeper, None)
            group = None
            break
        else:
            if 'indicator' in levels:
                if group_indicator:
                    # The stub nests one tier deeper than `levels` can hold, and
                    # the workbook says which rows are headings: a heading carries
                    # no figures of its own. 2017's table 37 lists KITCHEN,
                    # BATHROOM and LATRINE as headings, each over SEPARATE, SHARED
                    # and NONE, so without this the three NONE rows - 22,852 /
                    # 19,154 / 13,593 for Abbottabad - collapse onto one key.
                    if _plain(U) in group_exits:
                        # A leaf at the heading's own level, closing the group:
                        # table 37's "TOTAL :" totals the locality, not the latrine.
                        group = None
                        state['indicator'] = label
                    elif not has_num:
                        group = label
                        state.pop('indicator', None)
                    else:
                        state['indicator'] = f'{group} / {label}' if group else label
                else:
                    state['indicator'] = label
                for deeper in levels[levels.index('indicator') + 1:]:
                    state.pop(deeper, None)
            elif not has_num:
                continue

        if has_num:
            yield from emit(r, i, None)


def validate_spec(rows, table):
    """Cheap per-file check that the declared layout still holds."""
    a = anchor(rows)
    if a is None:
        return 'no column-number row'
    lc, data_cols = numbered_columns(rows, a)
    spec = SPEC[table]
    for r in rows[a + 1:a + 400]:
        t = txt(r[lc]) if lc < len(r) else None
        if not t:
            t, _ = row_unit(r, lc)
        if not t or not unit_type(t) or not (ADMIN.search(t.upper()) or OTHER_UNIT.search(t.upper())):
            continue
        # A row of nothing but dashes still carries information: those cells
        # are reported-missing, not absent. Treating them as an empty row
        # dropped 186 all-dash transgender rows from table 11 alone.
        has_num = any(cell(r[c])[1] in ('number', 'dash')
                      for c in data_cols if c < len(r))
        want_num = spec['shape'] == 'stub_unit'
        if has_num != want_num:
            return f"shape mismatch: declared {spec['shape']}, unit row {'has' if has_num else 'has no'} values"
        return None
    return 'no unit rows found'
