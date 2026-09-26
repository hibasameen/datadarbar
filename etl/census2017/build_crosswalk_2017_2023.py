"""Workstream C: the 2017 -> 2023 district crosswalk, checked against PBS.

Every 2017 district is given a documented relationship to 2023, or an explicit
statement that it has none. The relationships are grouped: a group is the
smallest set of 2017 units and 2023 units that covers the same ground, so a
merge has two 2017 units and one 2023 unit, a split has one and several, and
the ordinary case has one of each.

What makes this checkable rather than argued is that Census 2023's table 1
prints a POPULATION 2017 column - PBS's own restatement of the 2017 count on
2023 boundaries. It sums to 207,684,626 across the 136 districts, the 2017 total
to the person, so for every group we can ask whether the 2017 units' published
population equals the 2023 units' restated 2017 population. If a group does not
balance, the relation asserted for it is wrong.

Usage:
  build_crosswalk_2017_2023.py --panel17 <dir> --panel23 <parquet> --out <dir>
"""
import argparse, collections, csv, json, pathlib, re, sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from crosswalk_map import RENAMED, MERGED_INTO, SPLIT_INTO, TRANSFERRED


def norm(name):
    """A district name reduced to what both censuses agree on."""
    return re.sub(r'[^A-Z]', '', name.upper()
                  .replace('DISTRICT', '').replace('AGENCY', ''))


def districts_2017(con, panel):
    return con.sql(f"""
        SELECT province_area, unit,
               sum(value) FILTER (WHERE indicator LIKE '%POPULATION%2017%'
                                    AND col_label LIKE '%ALL SEXES%') AS pop
        FROM '{panel}'
        WHERE table_id='1' AND unit_type='district' AND locality='all' AND sex='all'
          AND value IS NOT NULL
        GROUP BY 1,2 ORDER BY 2""").fetchall()


def districts_2023(con, panel):
    return con.sql(f"""
        SELECT province_area, unit,
               sum(value) FILTER (WHERE indicator='POPULATION 2017')   AS pop17,
               sum(value) FILTER (WHERE indicator='POPULATION-2023 / ALL SEXES') AS pop23
        FROM '{panel}'
        WHERE table_id='1' AND unit_type='district' AND locality='all' AND NOT missing
        GROUP BY 1,2 ORDER BY 2""").fetchall()


def build_groups(d17, d23):
    """[(relation, [2017 units], [2023 units])] covering every unit on both sides."""
    by17 = {u: (pa, pop) for pa, u, pop in d17}
    by23 = {u: (pa, p17, p23) for pa, u, p17, p23 in d23}
    n23 = {}
    for u in by23:
        n23.setdefault(norm(u), u)

    groups, used17, used23 = [], set(), set()

    # Territory exchanged between two districts that both kept their names. Each
    # looks like an ordinary identity and only fails when its population is
    # checked, so the pair is grouped and balances together.
    for pair in TRANSFERRED:
        if all(u in by17 for u in pair) and all(u in by23 for u in pair):
            groups.append(('boundary transfer', list(pair), list(pair)))
            used17.update(pair); used23.update(pair)

    # Splits first: they claim a 2017 unit and several 2023 units, including a
    # same-named one, so resolving them before the identity pass keeps the
    # parent from being paired with its own remnant as though nothing happened.
    for parent, kids in SPLIT_INTO.items():
        if parent not in by17:
            continue
        present = [k for k in kids if k in by23]
        if not present:
            continue
        groups.append(('split', [parent], present))
        used17.add(parent); used23.update(present)

    # Merges: the absorbed unit and its host on the 2017 side, the host alone on
    # the 2023 side.
    hosts = collections.defaultdict(list)
    for absorbed, host in MERGED_INTO.items():
        if absorbed in by17 and host in by23:
            hosts[host].append(absorbed)
    for host, absorbed in hosts.items():
        members17 = [h for h in [host] if h in by17] + absorbed
        groups.append(('merged', members17, [host]))
        used17.update(members17); used23.add(host)

    for old, new in RENAMED.items():
        if old in by17 and new in by23 and old not in used17 and new not in used23:
            groups.append(('renamed', [old], [new]))
            used17.add(old); used23.add(new)

    for u in by17:
        if u in used17:
            continue
        hit = n23.get(norm(u))
        if hit and hit not in used23:
            groups.append(('exact' if hit == u else 'renamed', [u], [hit]))
            used17.add(u); used23.add(hit)

    for u in by17:
        if u not in used17:
            groups.append(('no counterpart in 2023', [u], []))
    for u in by23:
        if u not in used23:
            groups.append(('no counterpart in 2017', [], [u]))
    return groups, by17, by23


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--panel17', required=True)
    ap.add_argument('--panel23', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    p17 = pathlib.Path(a.panel17)
    p17 = (p17 / 'panel_2017.parquet') if p17.is_dir() else p17
    d17 = districts_2017(con, p17.as_posix())
    d23 = districts_2023(con, a.panel23)
    groups, by17, by23 = build_groups(d17, d23)

    rows, tally = [], collections.Counter()
    for relation, m17, m23 in groups:
        a17 = sum(by17[u][1] or 0 for u in m17)
        a23 = sum(by23[u][1] or 0 for u in m23)          # 2023's restated 2017
        now = sum(by23[u][2] or 0 for u in m23)
        balanced = (not m17 or not m23) and None or abs(a17 - a23) < 0.5
        tally[relation] += 1
        if m17 and m23:
            tally['balanced' if balanced else 'DOES NOT BALANCE'] += 1
        rows.append(dict(
            relation=relation,
            units_2017=' + '.join(sorted(m17)),
            units_2023=' + '.join(sorted(m23)),
            province_area=(by17[m17[0]][0] if m17 else by23[m23[0]][0]),
            population_2017=int(a17) if m17 else None,
            restated_2017=int(a23) if m23 else None,
            population_2023=int(now) if m23 else None,
            balances=('' if balanced is None else 'yes' if balanced else 'no')))

    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rows.sort(key=lambda r: (r['relation'], r['units_2017'], r['units_2023']))
    with open(out / 'district_crosswalk_2017_2023.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader(); w.writerows(rows)

    unbalanced = [r for r in rows if r['balances'] == 'no']
    report = dict(groups=len(rows), districts_2017=len(by17), districts_2023=len(by23),
                  relations={k: v for k, v in sorted(tally.items())},
                  unbalanced=unbalanced)
    (out / 'district_crosswalk_report.json').write_text(
        json.dumps(report, indent=1, sort_keys=True))

    print(f"2017 districts {len(by17)}   2023 districts {len(by23)}   groups {len(rows)}")
    for k, v in sorted(tally.items()):
        print(f"   {k:24s} {v}")
    if unbalanced:
        print("\ngroups whose population does not balance:")
        for r in unbalanced:
            print(f"   {r['units_2017']} -> {r['units_2023']}: "
                  f"{r['population_2017']:,} vs {r['restated_2017']:,}")
    else:
        print("\nevery group balances against PBS's own restated 2017 population")


if __name__ == '__main__':
    main()
