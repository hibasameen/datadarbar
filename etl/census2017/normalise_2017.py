"""Normalise the Census 2017 extract: column labels, unit names, missing districts.

Four defects in PBS's 2017 release would each split or lose a series if carried
into the panel. All four are corrected here rather than in the reader, so the
extract stays a faithful record of what the workbooks say and every correction is
one auditable step away from it.

1. HEADER-PREFIX LEAKAGE. A column's label is built from the header rows above
   it, and the number of those rows varies between files. Table 15's TOTAL column
   is labelled three different ways across the corpus - "SEX, AGE GROUP AND RURAL
   / URBAN / LITERATE POPULATION BY EDUCATIONAL ATTAINMENT / TOTAL" in 119 files,
   "LITERATE POPULATION BY EDUCATIONAL ATTAINMENT / TOTAL" in 10 and
   "EDUCATIONAL ATTAINMENT / TOTAL" in 4 - because the leftmost header cell is
   the stub's own caption, which leaks in when the file has an extra header row.
   The innermost segment is the measure, so the label is the text after the last
   separator.

   Taking the last segment could in principle merge two different columns that
   both end in TOTAL, so `check_collisions` refuses that outcome instead of
   silently producing it.

2. SPELLING VARIANTS. A handful of files spell a measure differently from the
   other 130-odd: INTER-MEDIATE for INTERMEDIATE, MASTER & ABOVE for MASTERS &
   ABOVE, AREA (SQ KM) for AREA (SQ. KM.). These are resolved by majority within
   the table, which is evidence from the corpus rather than a judgement call, and
   every choice made is reported.

   Note that the majority spelling is sometimes itself a typo - PBS writes
   NEVER ATTAINDED in 132 files and NEVER ATTENDED in one. The majority is still
   what the series is called; correcting PBS's spelling would be a different
   decision and is not taken here.

3. LOCALITY FOLDED INTO THE UNIT NAME. Kohistan's workbooks label the unit
   "KOHISTAN DISTRICT - RURAL", "KOHISTAN DISTRICT-RURAL" and "KOHISTAN DISTRICT
   - URBAN" instead of putting RURAL and URBAN in their own stub rows as every
   other district does. Left alone these become three extra districts.

4. THE DISTRICT ROW IS SOMETIMES ABSENT. Twelve workbooks in five districts -
   eight of them Musakhel's - begin at the tehsil, with no district total row, so
   11,934 observations have no district. The district is recovered from the
   tehsil, using the tehsil-to-district register that table 1 establishes for all
   134 districts. That is evidence from the census itself; the file's directory
   name is deliberately not used, because 2017's filenames are unreliable
   (Ghotki's workbooks are named for Dadu).
"""
import collections
import re

SEP = ' / '

UNIT_FIX = {
    'SHEIKUPURA DISTRICT': ('SHEIKHUPURA DISTRICT', 'PBS drops the H; table 6 only'),
    'KILLA ABDULLAB DISTRICT': ('KILLA ABDULLAH DISTRICT', 'PBS writes B for H; table 17 only'),
    'KILLAH ABDULLAH DISTRICT': ('KILLA ABDULLAH DISTRICT', 'PBS adds an H; table 34 only'),
    'FR D.I KHAN': ('FR D.I.KHAN', 'PBS drops the second dot; table 29 only'),
}

# unit label -> (unit, locality). Kohistan alone folds the locality into the name.
UNIT_LOCALITY = re.compile(r'^(?P<unit>.*DISTRICT)\s*-\s*(?P<loc>RURAL|URBAN)\s*$')


def last_segment(s):
    """The innermost header segment, whitespace-normalised and upper-cased."""
    if s is None:
        return None
    t = re.sub(r'\s+', ' ', str(s).replace('\n', ' ')).strip().upper()
    if not t:
        return None
    return (t.rsplit(SEP, 1)[1].strip() if SEP in t else t) or None


def _key(s):
    # "&" and "AND" are the same word. Dropping punctuation alone made
    # "18 & ABOVE" key as 18ABOVE and "18 AND ABOVE" as 18ANDABOVE, so the two
    # spellings of one age band stayed apart and the district using the minority
    # form dropped out of every area total for that series.
    return re.sub(r'[^A-Z0-9]', '', re.sub(r'&', ' AND ', (s or '').upper()))


# Variants that co-occurrence cannot resolve, because they differ in the segment
# itself rather than in its prefix or punctuation. Each is a single file against
# 132 or 133 spelling it the other way; the majority is recorded in the comment
# so the asymmetry is visible.
LABEL_ALIAS = {
    ('1', 'SEX RATIO ALL AGES'): 'SEX RATIO',                       # 1 file vs 133
    ('1', 'AVERAGE H. HOLD SIZE (REGULAR HOUSEHOLDS)'): 'AVERAGE HOUSEHOLD SIZE',   # 1 vs 133
    ('15', 'MASTER & ABOVE'): 'MASTERS & ABOVE',                    # 1 vs 132
    ('15', 'NEVER ATTENDED'): 'NEVER ATTAINDED',                    # 1 vs 132; PBS's
                                                                    # majority spelling
                                                                    # is itself a typo
    ('17', 'ALLSEXES'): 'DISABLED POPULATION',                      # 1 vs 132
    ('10', 'NON-PAKISTAN'): 'NON-PAKISTANI',                        # 64 files vs 66;
                                                                    # PBS drops the
                                                                    # final I in half
                                                                    # the corpus
    ('7', 'TOTAL POPULATION'): 'TOTAL POPULATION',                  # unchanged; listed
                                                                    # so the table is
                                                                    # complete

    # Found on 3 October 2026 by matching each minority label to the label the
    # other workbooks print at the SAME source column for the SAME row - the
    # position, not the wording, is the evidence. None of these ever shares a
    # workbook with its target, so no two columns are merged. Each was leaving
    # the districts that used it blank on the 2017 map.
    #
    # The literacy column of table 22 inherits the NON-MUSLIM of the merged
    # religion header beside it in 25 workbooks (Lahore, Gujranwala, Korangi
    # ...): column 9 in every file, LITERATE ( 10 YEARS & ABOVE ) in the rest.
    ('22', 'LITERATE ( 10 YEARS & ABOVE ) / NON-MUSLIM'): 'LITERATE ( 10 YEARS & ABOVE )',
    ('22', 'WORKED (INCLUDING UNPAID FAMILY HELPER)'): 'WORKED',     # Kohistan, Sanghar
    ('16', 'WORKED'): 'WORKED (INCLUDED UN PAID FAMILY WORKER)',    # 1 file; PBS's own
                                                                    # majority wording
    ('3', 'POPULATION'): 'POPULATION - 2017',                       # 4 files
    ('27', 'POPULATION'): 'POPULATION - 2017',                      # 1 file
    ('28', 'TOTAL NUMBER OF HOUSEHOLDS'): 'TOTAL',                  # Kohistan
    ('9', 'TOTAL POPULATION'): 'TOTAL',                             # Kohistan
    ('30', '9 AND MORE'): '9',                                      # 1 file
    # Kohistan's table 6 has no header text at all, only column numbers.
    ('6', '2'): 'TOTAL POPULATION',
    ('6', '3'): 'NEVER MARRIED',
    ('6', '4'): 'MARRIED',
    ('6', '5'): 'WIDOWED',
    ('6', '6'): 'DIVORCED',
    # A figure read as a header: one workbook's first count landed in the
    # header row of the 11-50 years column.
    ('34', '18568'): '11-50',
    ('36', '18568'): '11-50',
    ('38', '18568'): '11-50',
    # Zhob and Ziarat write PERSON for PERSONS throughout table 28.
    **{('28', f'{n} PERSON'): f'{n} PERSONS' for n in range(2, 10)},
    # Table 31's all-sexes column has no label in 135 workbooks; in 38 its
    # header ALL SEXS was read in as the label. '' means no label, as in the rest.
    ('31', 'ALL SEXS'): '',
}


# Indicator wordings that differ in the word itself, so majority-by-spelling
# cannot resolve them. Kohistan alone labels table 4's total row `ALL` where the
# other 134 districts write `All Ages`, which left Khyber Pakhtunkhwa 784,711
# short - exactly Kohistan's population - against its own published total.
INDICATOR_ALIAS = {
    ('4', 'ALL'): 'All Ages',
    ('5', 'ALL'): 'All Ages',
    # Kohistan writes the same total row `ALL` in every table it appears in, not
    # only 4 and 5. Each one cost Khyber Pakhtunkhwa a district's worth of
    # population in that table's area check.
    ('8', 'ALL'): 'ALL AGES',
    ('10', 'ALL'): 'ALL AGES',
    ('17', 'ALL'): 'ALL AGES',
    ('22', 'ALL'): 'ALL AGES',
    ('19', '10 YEAR AND ABOVE'): '10 & ABOVE',
    ('28', 'HOUSEHOLD BY NUMBER OF PERSONS / TOTAL NUMBER OF HOUSEHOLDS'):
        'HOUSEHOLD BY NUMBER OF PERSONS / TOTAL',
    # PBS's majority spelling is the ungrammatical one; Kohistan's is correct.
    # The alias points at the majority because the job here is to join them, not
    # to pick the better English.
    ('34', 'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEARS) / LESS THAN 5'):
        'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEAR ) / LESS THAN 5',
    ('34', 'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEARS) / 5-10'):
        'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEAR ) / 5-10',
    ('34', 'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEARS) / 11-50'):
        'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEAR ) / 11-50',
    ('34', 'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEARS) / OVER 50'):
        'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEAR ) / OVER 50',
    ('34', 'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEARS) / UNDER CONSTRUCTION'):
        'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEAR ) / UNDER CONSTRUCTION',
    ('35', 'HOUSING UNITS BY TENURE / OWNED'): 'HOUSING UNITS BY OWNERSHIP / OWNED',
    ('35', 'HOUSING UNITS BY TENURE / RENTED'): 'HOUSING UNITS BY OWNERSHIP / RENTED',
    ('35', 'HOUSING UNITS BY TENURE / RENT-FREE'): 'HOUSING UNITS BY OWNERSHIP / RENT-FREE',

    # Found on 3 October 2026: rows whose parent heading was lost, in one or two
    # workbooks each, so the district dropped out of every series in the table.
    # Each target is absent from the workbook concerned and the two never
    # appear together, and the rows sit in the same order as everyone else's.
    #
    # Kohistan's table 1 qualifies two of its headings.
    ('1', 'POPULATION - 2017 / SEX RATIO ALL AGES'): 'POPULATION - 2017 / SEX RATIO',
    ('1', 'POPULATION - 2017 / AVERAGE H. HOLD SIZE (REGULAR HOUSEHOLDS)'):
        'POPULATION - 2017 / AVERAGE HOUSEHOLD SIZE',
    # Haripur's table 11 drops POPULATION BY MOTHER TONGUE from every language.
    **{('11', lang): f'POPULATION BY MOTHER TONGUE / {lang}'
       for lang in ('BALOCHI', 'BRAHVI', 'HINDKO', 'KASHMIRI', 'OTHERS', 'PUNJABI',
                    'PUSHTO', 'SARAIKI', 'SINDHI', 'URDU')},
    ('13', 'LITERACY RATIO'): 'LITERATE / LITERACY RATIO',          # Kohistan, Sanghar
    ('3', 'POPULATION'): 'POPULATION - 2017',                       # the total row, in
                                                                    # 5 workbooks
    ('9', 'TOTAL POPULATION'): 'TOTAL',                             # Kohistan
    ('34', 'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEAR ) / 18568'):
        'HOUSING UNITS BY PERIOD OF CONSTRUCTION (IN YEAR ) / 11-50',  # see LABEL_ALIAS
    **{('28', f'HOUSEHOLD BY NUMBER OF PERSONS / {n} PERSON'):
       f'HOUSEHOLD BY NUMBER OF PERSONS / {n} PERSONS' for n in range(2, 10)},
    # Umer Kot repeats the first wall material where OUTER WALLS should be.
    **{('34', f'BAKED BRICKS / BLOCKS / STONES / {m}'): f'OUTER WALLS / {m}'
       for m in ('BAKED BRICKS / BLOCKS / STONES', 'UNBAKED BRICKS / MUD',
                 'WOOD / BAMBOO', 'OTHERS')},
    # Torghar's table 38 drops KITCHEN from its first three rows.
    ('38', 'SEPARATE'): 'KITCHEN / SEPARATE',
    ('38', 'SHARED'): 'KITCHEN / SHARED',
    ('38', 'NONE'): 'KITCHEN / NONE',
    ('31', 'ALL SEXS'): 'value',                                    # see LABEL_ALIAS
}


def canon_indicator_map(observed):
    """{(table, raw indicator): canonical indicator}, resolved by majority.

    Indicators vary in case and punctuation between workbooks - `Below 1` and
    `BELOW 1`, `All Ages` and `ALL AGES`, six such pairs in table 7 - and nothing
    reconciled them, so one series became two. Column labels had this treatment
    from the start; indicators did not.

    Two spellings are the same indicator when they agree after case is folded and
    non-alphanumerics are dropped. That is safe here in a way it would not be for
    column labels, because an indicator is a stub label rather than a compound of
    header rows: there is no prefix to lose and no risk of merging two columns
    that merely share a final word.
    """
    groups = collections.defaultdict(dict)
    for (table, raw), n in observed.items():
        if raw is None:
            continue
        groups[(table, _key(raw))][raw] = n
    out, merged = {}, []
    for (table, key), variants in groups.items():
        if not key:
            continue
        winner = max(variants.items(), key=lambda kv: (kv[1], -len(kv[0])))[0]
        winner = INDICATOR_ALIAS.get((table, winner.upper()), winner)
        for raw in variants:
            out[(table, raw)] = winner
        if len(variants) > 1:
            merged.append((table, winner, sorted(variants, key=lambda r: -variants[r])))
    # aliases that the majority rule cannot reach, because the word differs
    for (table, raw), canon in list(out.items()):
        alias = INDICATOR_ALIAS.get((table, canon.upper()))
        if alias:
            out[(table, raw)] = alias
    for (table, up), canon in INDICATOR_ALIAS.items():
        for (t, raw) in list(out):
            if t == table and raw.upper() == up:
                out[(t, raw)] = canon
    return out, merged


def canon_map(observed):
    """{(table, raw label): canonical label}, decided by co-occurrence.

    `observed` is {(table, raw label): set of (file, locality, sex) keys}. The key
    includes locality and sex because those are separate dimensions in the output,
    not part of the measure: table 10 labels its rural block's column PAKISTANI and
    its all-localities block TOTAL POPULATION / PAKISTANI, and both appear in every
    file. Judged by file alone they look like two columns; judged within a
    locality they are one measure reported for two localities, which is what they
    are.

    Two raw labels whose innermost segment matches are the same measure spelled
    two ways ONLY IF they never appear in the same file. If they do appear
    together, they are different columns that happen to share a final word, and
    the full label is kept.

    That distinction is not cosmetic. Table 20 publishes RURAL / BOTH SEXES,
    TOTAL / BOTH SEXES and URBAN / BOTH SEXES side by side, and table 8 publishes
    SON / DAUGHTER next to GRAND SON / DAUGHTER; collapsing either to its last
    segment would merge three localities into one series and two relationships
    into one. Table 15's three labels for its TOTAL column, by contrast, are
    spread across 119, 10 and 4 files and never share one, because they differ
    only in how many header rows leaked into the label.
    """
    groups = collections.defaultdict(dict)
    for (table, raw), files in observed.items():
        if raw is None:
            continue
        groups[(table, _key(last_segment(raw)))][raw] = set(files)  # keys, not paths

    out, merged, kept = {}, [], []
    for (table, key), variants in groups.items():
        if not key:
            continue
        together = any(a is not b and (fa & fb)
                       for a, fa in variants.items() for b, fb in variants.items())
        if len(variants) > 1 and together:
            for raw in variants:
                out[(table, raw)] = re.sub(r'\s+', ' ', raw).strip().upper()
            kept.append((table, sorted(variants)))
            continue
        winner = max(variants.items(), key=lambda kv: (len(kv[1]), -len(kv[0])))[0]
        canon = last_segment(winner)
        canon = LABEL_ALIAS.get((table, canon), canon)
        for raw in variants:
            out[(table, raw)] = canon
        if len(variants) > 1:
            merged.append((table, canon, sorted(variants, key=lambda r: -len(variants[r]))))

    # the explicit aliases, applied after grouping
    for (table, raw), canon in list(out.items()):
        out[(table, raw)] = LABEL_ALIAS.get((table, canon), canon)
    return out, merged, kept


def split_unit(label):
    """(unit, locality) for a unit label that carries its own locality, else (label, None)."""
    if label is None:
        return None, None
    m = UNIT_LOCALITY.match(re.sub(r'\s+', ' ', label).strip().upper())
    if not m:
        return label, None
    return m.group('unit'), m.group('loc').lower()


def fix_unit(label):
    """(unit, note) applying the two spelling corrections."""
    if label is None:
        return None, None
    key = re.sub(r'\s+', ' ', label).strip().upper()
    if key in UNIT_FIX:
        return UNIT_FIX[key][0], UNIT_FIX[key][1]
    return label, None


