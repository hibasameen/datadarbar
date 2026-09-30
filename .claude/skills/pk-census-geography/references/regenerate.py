#!/usr/bin/env python3
"""Regenerate boundary-changes-2017-2023.md from the crosswalks.

Run from the repository root:
    python3 .claude/skills/pk-census-geography/references/regenerate.py

The tables in that file are counts and names from the crosswalks, so they are
generated rather than typed - a hand-maintained copy would drift from the
crosswalk it describes, and the crosswalk is the thing under test.
"""
import collections, csv, pathlib

ROOT = pathlib.Path(__file__).resolve().parents[4]
XW = ROOT / 'etl' / 'census2017'
OUT = pathlib.Path(__file__).resolve().parent / 'boundary-changes-2017-2023.md'


def nice(n):
    return ' '.join(w if (len(w.strip('.,')) <= 3 and w.isupper()) or '.' in w
                    else w.title() for w in n.split())


def main():
    D = list(csv.DictReader(open(XW / 'district_crosswalk_2017_2023.csv')))
    S = list(csv.DictReader(open(XW / 'subdistrict_crosswalk_2017_2023.csv')))
    U = list(csv.DictReader(open(XW / 'census_unit_map.csv')))

    L = ['# Every boundary change between Census 2017 and Census 2023', '',
         'Generated from the crosswalks in `etl/census2017/`, which are themselves',
         "checked against PBS's restated 2017 population: for every group below, the",
         "2017 units' published population equals the 2023 units' restated figure.",
         'Regenerate with `.claude/skills/pk-census-geography/references/regenerate.py`.', '']

    tally = collections.Counter(r['relation'] for r in D)
    L += ['## Districts', '',
          f"135 units in 2017, 136 in 2023, in {len(D)} groups: "
          + ', '.join(f'{v} {k}' for k, v in sorted(tally.items())) + '.', '']
    for rel, title, note in [
        ('renamed', 'Renamed', "FATA's agencies became districts."),
        ('merged', 'Merged', 'Each Frontier Region was absorbed by its host district. '
                             'Two 2017 units share one 2023 shape, so their figures add '
                             '— or, for a rate, average on 2017 population.'),
        ('split', 'Split', 'One 2017 unit became several. Its count cannot be divided '
                           'between them; drawn across all of them instead.'),
        ('boundary transfer', 'Boundary transfer',
         'Both districts persist, but territory moved, so the two years cover '
         'slightly different ground.')]:
        rows = [r for r in D if r['relation'] == rel]
        if not rows:
            continue
        L += [f'### {title} ({len(rows)})', '', note, '',
              '| 2017 | 2023 | 2017 population |', '|---|---|---:|']
        for r in sorted(rows, key=lambda x: x['units_2017']):
            L.append(f"| {nice(r['units_2017'])} | {nice(r['units_2023'])} "
                     f"| {int(r['population_2017']):,} |")
        L.append('')

    st = collections.Counter(r['relation'] for r in S)
    L += ['## Sub-districts', '',
          f"537 units in 2017, 591 in 2023, in {len(S)} groups: "
          + ', '.join(f'{v} {k}' for k, v in sorted(st.items())) + '.', '',
          'Renamed pairs are matched by name inside a district group, or — where the',
          'names give nothing — by a population that is identical and uniquely so',
          "within the group. Dera Bugti's Phelawagh Tehsil and Qadirabad Sub-Division",
          'are one place under two names, and only the 28,054 says so.', '']

    par = [r for r in U if r['comparable'] == 'parent' and r['unit_type'] == 'tehsil']
    L += [f'### Restructured, resolved as clean splits ({len(par)})', '',
          'One 2017 unit became several 2023 ones, so the parent is drawn across its',
          'successors. The remaining restructured groups are many-to-many and stay',
          'undrawn: the correspondence inside them does not exist to be drawn.', '',
          '| 2017 unit | drawn on |', '|---|---:|']
    for r in sorted(par, key=lambda x: x['unit']):
        L.append(f"| {nice(r['unit'])} | {len(r['map_key'].split())} shapes |")
    L.append('')

    und = [r for r in U if r['comparable'] == 'no' and r['unit_type'] == 'tehsil']
    byd = collections.Counter(r['district'] for r in und)
    L += [f'### Restructured, left undrawn ({len(und)} units)', '',
          'Several 2017 units became several 2023 ones. The group balances; which',
          'unit corresponds to which does not follow from that, and is not guessed.', '',
          '| district | units |', '|---|---:|']
    for d, n in sorted(byd.items()):
        L.append(f'| {nice(d)} | {n} |')
    L.append('')

    OUT.write_text('\n'.join(L) + '\n')
    print(f'{OUT} — {len(L)} lines')


if __name__ == '__main__':
    main()
