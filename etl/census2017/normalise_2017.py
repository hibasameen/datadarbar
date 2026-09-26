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
    return re.sub(r'[^A-Z0-9]', '', (s or '').upper())


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
}


# Indicator wordings that differ in the word itself, so majority-by-spelling
# cannot resolve them. Kohistan alone labels table 4's total row `ALL` where the
# other 134 districts write `All Ages`, which left Khyber Pakhtunkhwa 784,711
# short - exactly Kohistan's population - against its own published total.
INDICATOR_ALIAS = {
    ('4', 'ALL'): 'All Ages',
    ('5', 'ALL'): 'All Ages',
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


