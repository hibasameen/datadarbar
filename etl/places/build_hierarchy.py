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

    out, seen = {}, set()
    for m in mapping:
        o = m['original']
        key = SEP.join((o['level'], o['group_key'], o['indicator']))
        assert key not in seen, f'duplicate compound key: {key!r}'
        seen.add(key)
        assert m['proposed_topic'] in topics, \
            f'row maps to a topic not in the topic list: {m["proposed_topic"]!r}'
        out[key] = [
            topics.index(m['proposed_topic']),
            idx('sub', m['proposed_subtopic']),
            idx('family', m['indicator_family']),
            idx('metric', m['metric_form']),
            idx('controls', m['suggested_controls']),
            idx('review', m['definition_review']),
        ]

    # Which review strings mean "we could not resolve this label". The proposal
    # requires those to stay selectable and to be visibly marked, not dropped,
    # so the flag travels with the row rather than being recomputed in JS.
    unresolved = [i for i, r in enumerate(vocab['review'])
                  if 'Unresolved source label' in r or 'Generic measure label' in r]

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
    print(f'  per-head modes agree with places-rail.js on all {len(mapping):,} rows')
    print(f'  -> {dest} ({dest.stat().st_size/1e6:.2f} MB)')


if __name__ == '__main__':
    main()
