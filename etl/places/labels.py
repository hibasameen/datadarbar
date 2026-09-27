"""Turn PBS's own column headings into something a picker can list.

The census labels are the concatenation of an indicator and a column heading,
exactly as PBS printed them, which is the right thing for an audit trail and
the wrong thing for a list a reader scans. Of 5,133 census entries: 3,962 are
shouted in capitals, 2,757 carry a slash path, 2,140 lead with an age band and
put the measure second, 825 use a double hyphen for a dash, and 316 have a
column heading that is a bare number.

What is changed here is presentation: case, dashes, the order of two parts, and
a redundant repetition. What is not changed is the words. PBS's own spellings
survive - REALATIONSHIP, HOUSE HOLD - because the label is how a reader finds
the series in PBS's published table, and silently correcting it would break
that. The raw label is kept alongside for the same reason.
"""
import re

# Words that are not Title Case when a label is cased: acronyms PBS uses, and
# the short joining words that read wrong capitalised mid-phrase.
KEEP_UPPER = {
    'CNIC', 'C.N.I.', 'ICT', 'FATA', 'AJK', 'GB', 'SQ', 'KM', 'NWFP',
    'MBBS', 'RHC', 'BHU', 'MCH', 'TV', 'ID', 'LPG', 'RO', 'PVC', 'GI',
}
LOWER_WORDS = {'a', 'an', 'and', 'as', 'at', 'by', 'for', 'from', 'in', 'of',
               'on', 'or', 'per', 'the', 'to', 'with'}

# A band, not only an age band. PBS puts the banded dimension in the row for
# age AND for locality size ("1,000 -- 1,999" in table 3), and both are a
# breakdown of some measure rather than a measure themselves - so both belong
# on the same side of the split. The thousands separator is why the size bands
# were missed at first, and table 3's measures ended up named after them.
AGE = re.compile(r'^(?:[0-9][0-9,]*\s*[–-]+\s*[0-9][0-9,]*'
                 r'|[0-9][0-9,]*\s*(?:&|AND)\s*(?:ABOVE|OVER)'
                 r'|ALL\s+AGES|UNDER\s*[0-9][0-9,]*|[0-9][0-9,]*\s*\+)$', re.I)
BARE_NUMBER = re.compile(r'^\d+$')


def dashes(t):
    """PBS writes ranges as 10 -- 14 and 0 - 4; both mean 10-14."""
    t = re.sub(r'\s*-{2,}\s*', '–', t)
    t = re.sub(r'(?<=\d)\s*-\s*(?=\d)', '–', t)
    # the workbooks pad single digits: 00-04 means 0-4
    t = re.sub(r'\b0(\d)\b', r'\1', t)
    return t


def case(t):
    """Title case that leaves acronyms and PBS's own oddities alone."""
    if t != t.upper() and t != t.lower():
        return t                      # already mixed: the source cased it itself
    out, first = [], True
    for w in t.split():
        bare = w.strip('.,()/:')
        if bare.upper() in KEEP_UPPER or (len(bare) <= 4 and '.' in bare):
            out.append(w)
        elif not first and bare.lower() in LOWER_WORDS:
            out.append(w.lower())
        elif re.search(r'\d', w):
            out.append(w)             # 10-14, 1998-2017, 5+
        else:
            out.append(w[:1].upper() + w[1:].lower())
        first = False
    return ' '.join(out)


def tidy(t):
    t = dashes((t or '').strip())
    # A slash with a space on BOTH sides is PBS flattening a hierarchy:
    # "RESIDENTIAL STATUS / OTHERS". A slash without one is the source's
    # own shorthand for "or" - "Walking/ Climbing" - and stays a slash.
    t = re.sub(r' +/ +', ' — ', t)
    t = re.sub(r'\s*:\s*$', '', t)            # trailing colons from the workbooks
    t = re.sub(r'\s{2,}', ' ', t).strip(' —')
    return case(t)


def split(indicator, col_label):
    """A census cell as (measure, breakdown).

    PBS's tables are cross-tabs and it did not put the same dimension in the
    rows each time. Table 10 has the age band in `indicator` and nationality
    in `col_label`; table 24 has the detail in `indicator` and a truncated
    heading in `col_label`. So which side is the thing being measured flips
    between tables, and a picker that assumed one of them would read
    backwards on the other - "75 & Above" as the indicator and "Pakistani" as
    its breakdown.

    The rule is the one display() already used to decide word order: an age
    band is a breakdown, never a measure. Both share it now, so the label and
    the two dropdowns cannot disagree about which half is which.

    Returns (measure, breakdown) with breakdown '' when the cell has none.
    """
    ind = tidy(indicator)
    col = tidy(col_label) if col_label else ''
    if col_label and BARE_NUMBER.match(col_label.strip()):
        return ind, f'unlabelled column {col_label.strip()}'
    if not col:
        return ind, ''
    # The column repeats the indicator: one dimension, not two.
    if col.lower() == ind.lower() or ind.lower().endswith(' — ' + col.lower()):
        return ind, ''
    # A truncated heading is a prefix of the detail, not a second dimension:
    # "TYPE OF WASHROOM" against "TYPE OF WASHROOM / NO WASHROOM".
    if ind.lower().startswith(col.lower()):
        return ind, ''
    if AGE.match(indicator.strip()) and not AGE.match((col_label or '').strip()):
        return col, ind          # the age band is the breakdown
    return ind, col


def display(indicator, col_label, table_id=None):
    """The label a picker shows. `indicator` and `col_label` are PBS's own."""
    ind = tidy(indicator)
    col = tidy(col_label) if col_label else ''

    # A column heading that is only a number tells a reader nothing. Say what it
    # is instead of pretending it is a category: these are columns whose header
    # the extraction could not read, and hiding that would be worse.
    if col_label and BARE_NUMBER.match(col_label.strip()):
        return f'{ind} (unlabelled column {col_label.strip()})'

    if not col:
        return ind
    # "POPULATION - 2017 - All Sexes" + column "All Sexes" says it twice
    if col.lower() == ind.lower() or ind.lower().endswith(' — ' + col.lower()):
        return ind
    # PBS often puts the age band first and the measure second, which reads
    # backwards in a list. "Total population, 50-54" beats "50 - 54 - Total
    # Population".
    if AGE.match(indicator.strip()) and not AGE.match((col_label or '').strip()):
        return f'{col}, {ind}'
    return f'{ind} — {col}'


if __name__ == '__main__':
    cases = [
        ('50 - 54', 'TOTAL POPULATION', 'Total Population, 50–54'),
        ('70 -- 74', 'REALATIONSHIP TO THE HEAD OF HOUSEHOLD / BROTHER',
         'Realationship to the Head of Household — Brother, 70–74'),
        ('POPULATION - 2017 / ALL SEXES', 'ALL SEXES', 'Population - 2017 — All Sexes'),
        ('15 - 19', '6', '15–19 (unlabelled column 6)'),
        ('75 AND ABOVE', 'INTERMEDIATE', 'Intermediate, 75 and Above'),
        ('BRAHVI', None, 'Brahvi'),
        ('Walking/ Climbing', 'AGE BRACKETS / 15-24',
         'Walking/ Climbing — Age Brackets — 15–24'),
        ('00 -- 04', 'TOTAL POPULATION', 'Total Population, 0–4'),
        ('Never to School (all)', 'AGE BRACKETS / 25-40',
         'Never to School (all) — Age Brackets — 25–40'),
    ]
    bad = 0
    for ind, col, want in cases:
        got = display(ind, col)
        flag = 'ok  ' if got == want else 'FAIL'
        if got != want:
            bad += 1
        print(f'  {flag} {ind!r} + {col!r}\n         -> {got!r}')
        if got != want:
            print(f'         want {want!r}')
    raise SystemExit(bad and 1 or 0)
