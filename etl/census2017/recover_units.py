"""Recover unit labels that PBS omitted from the middle of a 2017 workbook.

Some workbooks contain more unit blocks than unit names. Sargodha's table 12 has
eight blocks - the district and its seven tehsils - but only five are labelled;
where BHALWAL TEHSIL's block is introduced by its name, the sixth, seventh and
eighth blocks are introduced by an empty cell:

    r201  '75 AND ABOVE'          r985  '75 AND ABOVE'
    r202  'BHALWAL TEHSIL'        r986  (blank)
    r203  'OVERALL'               r987  'OVERALL'

The reader cannot start a new unit at an empty cell, so those three blocks are
read as continuations of SAHIWAL TEHSIL, the last unit that was named. The figures
are not lost but they are attributed to the wrong tehsil, and three tehsils vanish
from the panel - the single most damaging defect found in this release, because
unlike a mislabelled column it makes specific numbers wrong for specific places.

Each block opens with the same stub label - the first value of the table's
shallowest declared level, OVERALL here. An opener with no unit name between it
and the previous opener is therefore an unnamed block, and the count of openers
minus the count of names is the number of units to recover.

The names come from the district's own roster in table 1, aligned alphabetically:
the blocks appear in alphabetical order, so the labelled ones already sit at their
alphabetical positions and the unnamed ones take the names left over. For
Sargodha that assigns SARGODHA, SHAHPUR and SILLANWALI to the three blanks, and
the assignment is confirmed by the data rather than by the ordering alone - table
13, which does label all eight units, reports those three tehsils' 10+ population
as 1,160,533, 266,223 and 251,551, exactly the three unlabelled blocks' totals.

Nothing is inserted or renumbered: the name is written into the empty cell that
was always meant to hold it, so `src_row` still points at the workbook row the
figure came from.
"""
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / 'stage2'))
from read_workbook import unit_type, row_unit, txt


def unit_in_row(r, stub):
    """The unit this row names, wherever in the row it sits, or None.

    Deliberately the reader's own `row_unit` and `unit_type` rather than a copy of
    their logic, because a private copy drifted from both at once: it omitted
    DE-EXCLUDED AREA RAJANPUR, the one unit in the country carrying none of the
    usual keywords, and it looked only in the stub column, while Attock's table 14
    puts its unit labels elsewhere in the row - so five of that file's six units
    were invisible to it.
    """
    found, _ = row_unit(r, stub)
    return found


def _txt(v):
    return '' if v is None else str(v).strip()


def opener_label(rows, stub, a):
    """The stub label that introduces each unit block, or None.

    Taken from the workbook: it is the label that most often follows a unit name.
    """
    seen = {}
    for i, r in enumerate(rows):
        if i <= a or stub >= len(r):
            continue
        if not unit_in_row(r, stub):
            continue
        for j in range(i + 1, min(i + 4, len(rows))):
            t = _txt(rows[j][stub]) if stub < len(rows[j]) else ''
            if t:
                seen[t.upper()] = seen.get(t.upper(), 0) + 1
                break
    if not seen:
        return None
    return max(seen.items(), key=lambda kv: kv[1])[0]


def scan(rows, stub, a, opener):
    """(block openers, unnamed openers) as row indices.

    A row can be both at once. Attock's table 14 puts FATEH JANG TEHSIL in column
    4 of the very row whose stub reads OVERALL, so that row names a unit AND opens
    its block. Skipping it as "a unit row" drops it from the opener list, and the
    remaining openers then line up against the wrong names - the first version of
    this function assigned HAZRO TEHSIL to Hasan Abdal's block, corrupting a file the
    reader already handled correctly.
    """
    openers, unnamed, named_since = [], [], True
    for i, r in enumerate(rows):
        if i <= a or stub >= len(r):
            continue
        # The unit is looked for BEFORE the empty-stub guard: Attock's table 14
        # puts ATTOCK TEHSIL in column 3 of a row whose stub is blank, so
        # skipping blank-stub rows first hides the name and the block that
        # follows looks unnamed.
        if unit_in_row(r, stub):
            named_since = True
        t = _txt(r[stub]).upper()
        if not t:
            continue
        if t == opener:
            openers.append(i)
            if not named_since:
                unnamed.append(i)
            named_since = False
    return openers, unnamed


def assign(labelled, roster):
    """Names for the unnamed blocks, aligning the block sequence to `roster`.

    `labelled` is the block sequence in file order, with None for each unnamed
    block. `roster` is the district's sub-district units, sorted. Returns a list
    of names, one per None, in order.
    """
    left = [u for u in roster if u not in {x for x in labelled if x}]
    out, k = [], 0
    for name in labelled:
        if name is None:
            out.append(left[k] if k < len(left) else None)
            k += 1
    return out


def plan(rows, stub, a, roster):
    """({opener row index: unit name}, report) for the blocks PBS left unnamed.

    Nothing in the sheet is altered. The caller hands the map to
    `read_workbook.read`, which starts a new unit at those rows - so `src_row`
    still points at the workbook row a figure came from, and a workbook whose
    missing name has no row at all is handled the same way as one where the cell
    is merely blank. Washuk's table 14 is the second kind: between '75 AND ABOVE'
    and the next 'OVERALL' there is no row to write into.

    A block that cannot be named is reported and left unnamed rather than guessed
    at.
    """
    opener = opener_label(rows, stub, a)
    if not opener:
        return {}, []
    openers, unnamed = scan(rows, stub, a, opener)
    if not unnamed:
        return {}, []

    labelled, done = [], set()
    for i in openers:
        if i in unnamed:
            labelled.append(None)
            continue
        for j in range(i - 1, a, -1):
            u = unit_in_row(rows[j], stub)
            if u and j not in done:
                labelled.append(u)
                done.add(j)
                break
        else:
            labelled.append(None)

    # REFUSE THE WHOLE FILE unless the unnamed blocks account for exactly the
    # roster names that are missing. The opener is a heuristic - the label that
    # usually follows a unit name - and in some layouts it recurs WITHIN a unit
    # instead of once per unit. Kohistan's table 9 opens each block with ALL
    # SEXES, which appears once per locality, so eight of its ten openers looked
    # unnamed; the recovery then christened the district's own rural and urban
    # sub-blocks DASSU, KANDIA, PALAS and PATTAN. That is worse than the defect it
    # was fixing: the figures were merely misattributed before, and afterwards
    # they were confidently wrong under real place names, with the ambiguity flag
    # that had been signalling the problem switched off.
    #
    # Requiring equality rather than a bound is deliberate. A file should hold
    # exactly the district's units, so any other count means the block structure
    # is not what this function assumes, and no part of its guess can be trusted.
    unused = [u for u in roster if u not in {x for x in labelled if x}]
    if len(unnamed) != len(unused):
        return {}, [dict(opener_row=i + 1, unit=None, applied=False,
                         why=f'{len(unnamed)} unnamed blocks but {len(unused)} roster '
                             f'names unused: the block structure is not one opener '
                             f'per unit, so no assignment is trustworthy')
                    for i in unnamed]

    names = assign(labelled, roster)
    out, report, k = {}, [], 0
    for i in openers:
        if i not in unnamed:
            continue
        name = names[k] if k < len(names) else None
        k += 1
        if name is None:
            report.append(dict(opener_row=i + 1, unit=None, applied=False,
                               why='no roster name left for this block'))
            continue
        out[i] = name
        report.append(dict(opener_row=i + 1, unit=name, applied=True))
    return out, report
