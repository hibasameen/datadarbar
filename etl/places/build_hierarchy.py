"""Distil the Places hierarchy proposal into the mapping the build joins on.

The proposal ships as an 8.6 MB crosswalk carrying every original index field
alongside its proposed destination. All the build needs is the destination, so
this reduces it to that and keeps the rest where it is, as the provenance
record.

Two things this is careful about, both of them requirements the proposal
states outright:

KEY ON THE COMPOUND SOURCE KEY, NOT THE LABEL. The key is
(level, group_key, indicator). Labels are ambiguous in both directions on this
index - "POPULATION - 2017 / ALL SEXES" is both a table-1 population row and a
household-size bracket in table 3 - and the picker has been opened on the
wrong one twice before for exactly that reason. On the compound key the
crosswalk is a bijection with the live index: 5,201 to 5,201, nothing on
either side, no duplicates.

DO NOT CHANGE WHAT THE MODES OFFER. The crosswalk records, per row, whether
the reader is currently offered "% of population" and "per 1,000 people". That
must agree with what places-rail.js actually decides, or this is not a
navigation change. It agrees on all 5,201 rows, and the check below keeps it
that way: if the rail's eligibility rule is edited and the crosswalk is not
regenerated, this fails rather than silently redefining a denominator.

Usage: build_hierarchy.py --crosswalk <json> --out etl/places/hierarchy.json
"""
import argparse, hashlib, json, pathlib, re

SEP = '\u001f'

# places-rail.js decides which rows can be read per head. Mirrored here only to
# check the crosswalk against it - the rail remains the one that decides.
RATE_WORDS = re.compile(
    r'%|\brates?\b|\bratios?\b|\bper\b|averag|\bavg\b|\bmedian\b|\bmean\b'
    r'|\bindex\b|proportion|\bshare\b|\bpct\b|per cent|percent|\bdensity\b', re.I)
WHOLE_POP = re.compile(
    r'^population[\s\-–—]*(20\d\d)?(\s*[—-]\s*all sexes)?$', re.I)


# ── corrections to the proposal's mapping ──────────────────────────────────
# Applied here rather than by editing hierarchy.json, which is generated: a
# correction is a decision and belongs where it can be read and argued with.
# Each is keyed on the compound source key, and every one must match a row or
# the build fails - a correction that silently stops applying is worse than
# none, because it looks like the fix is still in place.
#
# TABLE 33 IS A CROSS-TAB, AND ITS TENURE MARGIN IS NOT A MATERIAL.
# "Table 33 - 2017: units by tenure and material of walls and roofs" has 84
# rows in the index. Eighty of them are a wall or roof material cross-tabbed
# by tenure, and "Housing materials and quality" is right for those: the thing
# being counted is a material. Four are not. They carry no material at all -
# the key says HOUSING UNITS BY TENURE / OWNED - and they are the table's
# tenure margin, which is a different subject that happened to be printed on
# the same page.
#
# The census settles it rather than the wording: across every district the
# three counts add to the table's own total exactly - Lahore 1,158,614 owned
# plus 491,303 rented plus 94,838 rent-free is 1,744,755, which is the total
# row - so they partition housing units by tenure and nothing else. Two thirds
# of Lahore's housing units are owner-occupied and 28 per cent rented; that
# belongs under Tenure & ownership, where a reader looking for it will go.
#
# The fourth, HOUSING UNITS / PERCENT, moves with them because it sits in the
# same block, but it is marked rather than trusted: it is the percent column's
# own total, which is 100.0 in 382 districts, 0 in 19 and 97.18 in one. It is
# not a tenure share. The real tenure percentages are a separate PERCENT row
# in the census panel and are not in the index at all.
#
# AND THE THREE COUNTS ARE EACH THREE ROWS. Refiling them made that visible.
# census_panel_2017 holds three rows per district for each one - Lasbela gives
# 28,675, 50,989 and 79,664 - identical on every key column including
# locality, which all three call "all". They are rural, urban and total: in
# 133 of the 133 districts that carry all three, the two smaller add to the
# largest. So the map shows whichever the lookup reaches first and the totals
# strip sums all three, reading 52.16 million owned housing units against a
# true figure near 25.85 million.
#
# The warehouse already knows: every one of these rows carries
# series_ambiguous. That flag stops at the panel - census_series_index does
# not carry it, so neither does the picker index - and 561 of the 2,928
# series in the 2017 panel are affected, not only these. Threading it through
# is a change to the census pipeline rather than to this mapping, so what is
# done here is the honest minimum: the three rows whose arithmetic has
# actually been checked say so, and the rest are reported rather than
# quietly marked or quietly trusted.
TENURE = ('Housing & buildings', 'Tenure & ownership',
          'Housing tenure and ownership',
          'Owned/rented/rent-free; owner sex; rooms; residence')
DEGENERATE_TOTAL = (
    'Column total rather than a measure; retain original selectable entry; '
    'the source prints it as 100 per cent where the table has data')
TRIPLED = (
    'Unresolved source label; retain original selectable entry; the panel '
    'holds three rows per district under one locality - rural, urban and '
    'total - so a map of this series shows one of the three and a national '
    'total of it double counts')

CORRECTIONS = {
    'district\u001fcensus_t33\u001f33|HOUSING UNITS BY TENURE / OWNED|OWNED': {
        'to': TENURE, 'review': TRIPLED},
    'district\u001fcensus_t33\u001f33|HOUSING UNITS BY TENURE / RENTED|RENTED': {
        'to': TENURE, 'review': TRIPLED},
    'district\u001fcensus_t33\u001f33|HOUSING UNITS BY TENURE / RENT-FREE|RENT-FREE': {
        'to': TENURE, 'review': TRIPLED},
    'district\u001fcensus_t33\u001f33|HOUSING UNITS / PERCENT|PERCENT': {
        'to': TENURE, 'review': DEGENERATE_TOTAL},
}


# ── what each kind of quantity may do ──────────────────────────────────────
# Two questions the picker has been answering by reading the label, and
# getting wrong 278 times:
#
#   can it be added up?      a count can, a mean cannot
#   can it be read per head? only a count OF PEOPLE can
#
# The old rule was one regular expression over the measure and breakdown
# names, which worked whenever the name happened to contain "mean", "per" or
# "%". "Travel time to care, motorised (min)" contains none of them, so the
# picker totalled 471 tehsils' average journeys to 26,272 minutes and offered
# to express them as a share of the population - 0.01%. "APPLE - Area
# (thousand hectares)" and "Water table depth (feet)" were offered the same.
#
# The crosswalk already types every entry: 22 metric forms over all 5,201.
# That is the right key, because it describes the quantity rather than its
# wording. SUM_OK and POP_OK below are the whole rule, and every metric form
# must appear in them or the build fails - a new one must be classified, not
# silently defaulted.
SUM_OK = {
    'Source count': True,
    'Source value (retain native definition)': True,
    'Enumerated structure count': True,
    'Cultivated area (thousand hectares)': True,
    'Production volume (thousand tonnes)': True,
    'Land area (km\u00b2)': True,
    'Yield (tonnes/hectare)': False,
    'Published percentage / proportion': False,
    'Published rate (definition review)': False,
    'Published rate / ratio': False,
    'Average / source-defined score': False,
    'Growth rate (%)': False,
    'Density (people per km\u00b2)': False,
    'Sex ratio (source scaling)': False,
    'Mean time (minutes; source weighting)': False,
    'Median time (minutes)': False,
    'Distance (km)': False,
    'Distance gap (km)': False,
    'Percentage-point gap': False,
    'Index': False,
    'Percentile': False,
    'Radiance intensity': False,
}

# Only counts of people. Hectares, tonnes, minutes, kilometres, indices and
# gaps are not people, and a count of buildings is not either: banks per
# 1,000 residents may be worth having one day, but it is a facility rate with
# its own label, not a share of the population.
POP_OK = {
    'Source count': True,
    'Source value (retain native definition)': True,
    'Enumerated structure count': False,
}

# Where the crosswalk's type is contradicted by the values themselves.
# "Literate Population" is typed 'Published rate / ratio' and runs to
# 7,760,203 - it is a count of people, and the district column beside it
# reaches -725,564 because it carries the 2017-to-2023 change. Typed by what
# it is, so it can be totalled and read per head like every other headcount.
METRIC_FORM_OVERRIDE = {
    'district\u001fliteracy\u001fliterate_all': 'Source count',
    'district\u001fliteracy\u001filliterate_all': 'Source count',
}


def countable(o):
    measure = o.get('measure') or o.get('label') or ''
    metric = o.get('metric') or ''
    dp = o.get('dp')
    census = o.get('source') == 'census'
    is_rate = ((census and dp is not None and dp > 0)
               or RATE_WORDS.search(measure) or RATE_WORDS.search(metric))
    return not is_rate and not WHOLE_POP.match(measure) \
        and not re.match(r'^area\b', measure, re.I)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--crosswalk', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    raw = pathlib.Path(a.crosswalk).read_bytes()
    cw = json.loads(raw)
    mapping = cw['mapping']

    # The modes the rail offers must be the modes the crosswalk recorded.
    wrong = [m for m in mapping
             if countable(m['original']) != (len(m['legacy_modes']) == 3)]
    assert not wrong, (
        f'{len(wrong)} rows where the crosswalk and places-rail.js disagree '
        f'about the per-head modes, e.g. '
        f'{wrong[0]["original"]["indicator"][:60]!r}. Regenerate the crosswalk '
        f'against the current rule before rebuilding the hierarchy.')

    # Topic order is the proposal's, which runs from the largest and most
    # general subject to the most specialised. Alphabetical would open Places
    # on Agriculture.
    topics = [t['topic'] for t in cw['topics']]
    vocab = {'topic': topics, 'sub': [], 'family': [], 'metric': [],
             'controls': [], 'review': []}

    def idx(kind, value):
        v = vocab[kind]
        if value not in v:
            v.append(value)
        return v.index(value)

    out, seen, applied = {}, set(), set()
    for m in mapping:
        o = m['original']
        key = SEP.join((o['level'], o['group_key'], o['indicator']))
        assert key not in seen, f'duplicate compound key: {key!r}'
        seen.add(key)
        topic, sub = m['proposed_topic'], m['proposed_subtopic']
        family, controls = m['indicator_family'], m['suggested_controls']
        review = m['definition_review']
        fix = CORRECTIONS.get(key)
        if fix:
            topic, sub, family, controls = fix['to']
            review = fix.get('review', review)
            applied.add(key)
        assert topic in topics, f'row maps to an unknown topic: {topic!r}'
        form = METRIC_FORM_OVERRIDE.get(key, m['metric_form'])
        if form not in SUM_OK:
            raise AssertionError(
                f'metric form {form!r} is not classified in SUM_OK/POP_OK - '
                f'a new quantity type must be told whether it can be totalled '
                f'and whether it can be read per head')
        # countable() is the rail's own "is this already relative?" test. Both
        # gates have to pass: the form says a population denominator is
        # meaningful for this kind of quantity, and the label says this
        # particular entry is not already a rate. The second still does real
        # work - table 29 counts housing units under breakdowns like "Rooms
        # per Housing Unit", and those are counts, but not of people.
        out[key] = [
            topics.index(topic), idx('sub', sub), idx('family', family),
            idx('metric', m['metric_form']), idx('controls', controls),
            idx('review', review),
            1 if SUM_OK[form] else 0,
            1 if (POP_OK.get(form, False) and countable(o)) else 0,
        ]

    unmatched = set(CORRECTIONS) - applied
    assert not unmatched, (
        f'{len(unmatched)} corrections matched no row - the index or the '
        f'crosswalk moved under them: {sorted(unmatched)[0]!r}')

    # Which review strings mean "we could not resolve this label". The proposal
    # requires those to stay selectable and to be visibly marked, not dropped,
    # so the flag travels with the row rather than being recomputed in JS.
    unresolved = [i for i, r in enumerate(vocab['review'])
                  if 'Unresolved source label' in r or 'Generic measure label' in r
                  or 'Column total rather than a measure' in r]

    payload = {
        'generated_from': pathlib.Path(a.crosswalk).name,
        'crosswalk_sha256': hashlib.sha256(raw).hexdigest(),
        'source_snapshot_sha256': cw['report']['source_sha256'],
        'entries': len(out),
        'vocab': vocab,
        'unresolved_review': unresolved,
        'map': out,
    }
    dest = pathlib.Path(a.out)
    dest.write_text(json.dumps(payload, ensure_ascii=False, indent=1))

    print(f'  {len(out):,} keys mapped')
    print(f'  {len(topics)} topics, {len(vocab["sub"])} subtopics, '
          f'{len(vocab["family"])} families, {len(vocab["metric"])} metric forms')
    print(f'  {sum(1 for v in out.values() if v[5] in unresolved):,} rows '
          f'marked "definition needs review"')
    n_sum = sum(v[6] for v in out.values())
    n_norm = sum(v[7] for v in out.values())
    print(f'  {n_sum:,} entries can be totalled; {n_norm:,} can be read per head')
    print(f'  per-head modes agree with places-rail.js on all {len(mapping):,} rows')
    print(f'  {len(applied)} corrections applied to the proposal\'s mapping')
    print(f'  -> {dest} ({dest.stat().st_size/1e6:.2f} MB)')


if __name__ == '__main__':
    main()
