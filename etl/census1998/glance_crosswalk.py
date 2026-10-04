"""Which 2017 districts each 1998 'District at a Glance' district became.

The glance sheets are on the districts of about 2000-01, and between then and
2017 Pakistan both split districts (Hyderabad into four, Karachi into six) and
moved tehsils between neighbours (Lahore gained 21,369 people of 1998 from
Kasur). So the link is made in GROUPS that cover the same ground, and the test
is PBS's own: the 2017 census restates every 2017 district's 1998 population
(table 1, POPULATION 1998), and a group is accepted only when the glance
populations and those restated populations agree to the person.

  1. each glance district starts with its 2017 namesake (spellings fixed below);
  2. a 2017 district with no namesake - one created later - joins the group
     whose shortfall it closes exactly, alone or with one or two others;
  3. groups left unbalanced are merged with groups in the same province whose
     imbalance cancels theirs exactly.
Anything still unbalanced is reported and NOT linked.
"""
import itertools, re

SPELLING = {   # glance title -> 2017 name, where PBS spells them differently
    'BOLAN': 'KACHHI', 'DERA ISMIAL KHAN': 'DERA ISMAIL KHAN', 'JACCOBABAD': 'JACOBABAD',
    'MALAKAND PROTECTED AREA': 'MALAKAND', 'NAWABSHAH': 'SHAHEED BENAZIRABAD',
    'SHAIWAL': 'SAHIWAL', 'SHIKAPUR': 'SHIKARPUR', 'UMERKOT': 'UMER KOT',
    'LAHORE': 'LAHORE', 'KARACHI': None,
    # the spellings of the 1951-1998 administrative units table
    'D.G.KHAN': 'DERA GHAZI KHAN', 'D.I.KHAN': 'DERA ISMAIL KHAN', 'NAWAB SHAH': 'SHAHEED BENAZIRABAD',
    'JAFARABAD': 'JAFFARABAD',
    **{f'TRIBAL AREA ADJOINING {t}': f'FR {t}'
       for t in ('BANNU', 'D.I.KHAN', 'KOHAT', 'LAKKI MARWAT', 'PESHAWAR', 'TANK')},
}


def norm(s):
    s = s.upper().replace('CITY DISTRICT', '').replace('DISTRICT', '').replace('AGENCY', '')
    return re.sub(r'\s+', ' ', s).strip()


def key(s):
    return re.sub(r'[^A-Z]', '', s)


def build(glance, d17):
    """glance: {name: pop1998}; d17: {(province, name): restated pop1998}.
    Returns (groups, unlinked): groups = [(glance names, 2017 names, province)]."""
    by17 = {key(norm(n)): (p, n) for p, n in d17}
    comp = {}           # component id -> {'g': set, 'd': set, 'prov': p}
    for gname in glance:
        n = norm(gname)
        target = SPELLING.get(n, n)
        if n == 'KARACHI':
            ds = {(p, x) for p, x in d17 if x.startswith(('KARACHI', 'KORANGI', 'MALIR'))}
        else:
            hit = by17.get(key(target))
            ds = {hit} if hit else set()
        prov = next(iter(ds))[0] if ds else None
        comp[gname] = {'g': {gname}, 'd': set(ds), 'prov': prov}
    used = {d for c in comp.values() for d in c['d']}
    orphans = [d for d in d17 if d not in used]

    def gap(c):
        return sum(glance[g] for g in c['g']) - sum(d17[d] for d in c['d'])

    # 2. orphans close a shortfall exactly
    for c in sorted(comp.values(), key=lambda c: -gap(c)):
        if gap(c) <= 0 or c['prov'] is None:
            continue
        pool = [d for d in orphans if d[0] == c['prov']]
        for r in (1, 2, 3):
            hit = next((s for s in itertools.combinations(pool, r)
                        if sum(d17[d] for d in s) == gap(c)), None)
            if hit:
                c['d'] |= set(hit)
                orphans = [d for d in orphans if d not in hit]
                break
    # 2b. orphans that close a shortfall only together with a neighbour's
    #     transfer are left for step 3; give each remaining orphan to the
    #     unbalanced group in its province it brings closest to zero
    # 3. merge unbalanced groups whose imbalances cancel
    def unbalanced():
        return [k for k, c in comp.items() if c['prov'] and gap(c) != 0]
    changed = True
    while changed:
        changed = False
        ub = unbalanced()
        for r in (2, 3):
            for ks in itertools.combinations(ub, r):
                cs = [comp[k] for k in ks]
                if len({c['prov'] for c in cs}) != 1:
                    continue
                if sum(gap(c) for c in cs) == 0:
                    base = cs[0]
                    for c in cs[1:]:
                        base['g'] |= c['g']; base['d'] |= c['d']
                    for k in ks[1:]:
                        del comp[k]
                    changed = True
                    break
                # with orphans of the province
                pool = [d for d in orphans if d[0] == cs[0]['prov']]
                g = sum(gap(c) for c in cs)
                hit = next((s for q in (1, 2, 3) for s in itertools.combinations(pool, q)
                            if sum(d17[d] for d in s) == g), None)
                if hit and g > 0:
                    base = cs[0]
                    for c in cs[1:]:
                        base['g'] |= c['g']; base['d'] |= c['d']
                    base['d'] |= set(hit)
                    orphans = [d for d in orphans if d not in hit]
                    for k in ks[1:]:
                        del comp[k]
                    changed = True
                    break
            if changed:
                break
    groups = [(sorted(c['g']), sorted(n for _, n in c['d']), c['prov'], gap(c))
              for c in comp.values()]
    return groups, orphans
