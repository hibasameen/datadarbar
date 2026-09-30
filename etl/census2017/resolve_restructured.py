"""Resolve the restructured crosswalk groups by overlaying the two geographies.

`build_crosswalk_2017_2023.py` leaves 39 groups unpaired because their
populations balance under every possible pairing: the census tables cannot
distinguish them. Geometry can.

Two layers, both from PBS's own census frame:

  2017  geoBoundaries ADM3, "Boundary Representative of Year: 2017", 554 tehsils,
        built from PBS's 2017 census geography
  2023  PBS Insight Explorer's Digital Census 2023 tehsil layer, 591 units that
        map one-to-one onto the census's sub-district units by `dds_id`

For each 2023 unit the share of its area falling inside each 2017 polygon is
computed against every 2017 polygon, not against a name-filtered shortlist: a
new tehsil need not carry any part of its parent's name, and Quetta's Panjpai
scores 1% against the polygons whose names contain "Quetta" while sitting almost
entirely in one that does not.

The answer is reported as a share, not as a verdict. A unit lying 98% inside one
2017 tehsil is settled; one split 56/44 is not, and saying so is the point.
"""
import argparse, collections, csv, difflib, json, pathlib, re, sys

from shapely.geometry import shape
from shapely.strtree import STRtree

SUB_STRIP = (r'\b(TEHSIL|TALUKA|TALUKO|SUB-?TEHSIL|SUB-?DIVISION|SUB|TOWN|'
             r'MUNICIPAL COMMITTEE|MC|CANTONMENT|CANTT)\b')
# Trailing roman numerals: the 2017 layer divides a tehsil the census publishes
# whole, so Peshawar is four polygons named Peshawar I to Peshawar IV.
ROMAN_TAIL = re.compile(r'(I{1,3}|IV|V|VI{1,3}|IX|X)$')


def norm(name):
    s = re.sub(r'\(.*?\)', '', (name or '').upper())
    return re.sub(r'[^A-Z0-9]', '', re.sub(SUB_STRIP, '', s))


def valid(geom):
    g = shape(geom)
    return g if g.is_valid else g.buffer(0)


def load_2023(path):
    """[(dds_id, census unit, district, geometry)] for the census units."""
    out = []
    for f in json.loads(pathlib.Path(path).read_text())['features']:
        p = f['properties']
        if not p.get('in_census_2023'):
            continue
        out.append((p.get('dds_id'), p.get('census_unit') or p.get('tehsil'),
                    p.get('district'), valid(f['geometry'])))
    return out


def load_2017(path):
    out = []
    for f in json.loads(pathlib.Path(path).read_text())['features']:
        out.append((f['properties'].get('shapeName'), valid(f['geometry'])))
    return out


def overlaps(units23, units17, floor=0.02):
    """{dds_id: [(share, 2017 name)]} sorted, keeping shares above the floor."""
    geoms = [g for _, g in units17]
    tree = STRtree(geoms)
    out = {}
    for dds, name, dist, g in units23:
        area = g.area
        if not area:
            continue
        scores = []
        for idx in tree.query(g):
            n17, g17 = units17[int(idx)]
            try:
                inter = g.intersection(g17).area
            except Exception:
                continue
            if inter / area >= floor:
                scores.append((inter / area, n17))
        scores.sort(reverse=True)
        out[dds] = (name, dist, scores)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--tehsils23', required=True)
    ap.add_argument('--tehsils17', required=True)
    ap.add_argument('--crosswalk', required=True, help='subdistrict_crosswalk csv')
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    u23, u17 = load_2023(a.tehsils23), load_2017(a.tehsils17)
    print(f"2023 census units with geometry {len(u23)}   2017 polygons {len(u17)}")
    ov = overlaps(u23, u17)

    groups = [r for r in csv.DictReader(open(a.crosswalk))
              if r['relation'] == 'restructured']
    by_unit = {}
    for r in groups:
        for u in [x for x in r['units_2023'].split(' + ') if x]:
            by_unit[u] = r

    # which 2017 census units the layer actually draws
    poly_keys = {norm(n) for n, _ in u17}

    def polygon_for(member):
        k = norm(member)
        if k in poly_keys or any(p.startswith(k) for p in poly_keys if k):
            return True
        return bool(difflib.get_close_matches(k, list(poly_keys), n=1, cutoff=0.85))

    rows, settled, split_evidence, absent, unreliable = [], 0, 0, 0, 0
    for dds, (name, dist, scores) in ov.items():
        r = by_unit.get(name)
        if r is None:
            continue
        # only 2017 units that the crosswalk already places in this group are
        # candidates: geometry chooses among them, it does not overrule the
        # population arithmetic that built the group
        members = {norm(x): x for x in r['units_2017'].split(' + ') if x}

        def member_of(polygon_name):
            """The group member a 2017 polygon belongs to, if any.

            The two layers cut the same ground differently. The 2017 layer
            splits a tehsil the census publishes whole - Peshawar is four
            polygons, Peshawar I to Peshawar IV - and elsewhere it holds one
            polygon where the census publishes two, as GUJRANWALA covers both
            Gujranwala City and Gujranwala Saddar. So one normalised name being
            a prefix of the other counts as a match, in either direction. That
            rule is only safe because the candidates are already limited to the
            members of this group: across the whole country it would be far too
            loose.
            """
            k = norm(polygon_name)
            if k in members:
                return members[k]
            for m, raw in members.items():
                if not m or not k:
                    continue
                if k.startswith(m) and ROMAN_TAIL.fullmatch(k[len(m):] or 'I'):
                    return raw
            # A bare prefix in either direction, but only when exactly one
            # member matches that way. Lahore shows why the guard is needed:
            # the 2017 layer has LAHORE CITY and LAHORE CANTT and no Model Town
            # at all, so a loose rule would hand Model Town's territory to
            # Lahore City on the strength of a shared first word.
            loose = [raw for m, raw in members.items()
                     if m and k and (k.startswith(m) or m.startswith(k))]
            if len(loose) == 1:
                return loose[0]
            # The two layers spell a good many places differently - Jhal Jhao
            # against JHAL JAO, Dalbadin against DALBANDIN, CHIKSAR against
            # CHAKISAR. A close match is accepted, again only because the
            # candidates are this group's own members.
            near = difflib.get_close_matches(k, list(members), n=1, cutoff=0.85)
            return members[near[0]] if near else None

        # Share is summed per parent, not taken from the largest single polygon.
        # The 2017 layer splits Peshawar into four, so Cham Kani reads 67% in
        # Peshawar IV and 27% in Peshawar II while lying 94% inside the one
        # census tehsil both belong to.
        per_parent = collections.defaultdict(float)
        for sc, n in scores:
            m = member_of(n)
            if m:
                per_parent[m] += sc
        inside = sorted(((v, k) for k, v in per_parent.items()), reverse=True)
        # A group whose 2017 side is not fully drawn cannot be resolved by
        # overlap at all. The 2017 layer has no Model Town, so Model Town's
        # ground sits inside the LAHORE CANTT polygon and any share computed
        # for it would be attributed to whichever sibling does have a shape.
        # Eight groups are in this position and are reported, not answered.
        undrawn = [m for m in members.values() if not polygon_for(m)]
        if undrawn:
            unreliable += 1
            verdict = 'unreliable: 2017 layer has no polygon for ' + ', '.join(sorted(undrawn))
        elif not scores:
            absent += 1
            verdict = 'no overlap found'
        elif inside and inside[0][0] >= 0.75:
            settled += 1
            verdict = 'settled'
        else:
            split_evidence += 1
            verdict = 'divided'
        rows.append(dict(
            district_group=r['district_group'], unit_2023=name, dds_id=dds,
            verdict=verdict,
            parent_2017=(inside[0][1] if inside else ''),
            share=round(inside[0][0], 3) if inside else '',
            all_overlaps='; '.join(f"{n} {100*s:.0f}%" for s, n in scores[:4])))

    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    rows.sort(key=lambda r: (r['district_group'], r['unit_2023']))
    with open(out / 'restructured_resolution.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader(); w.writerows(rows)
    print(f"\n2023 units in restructured groups examined: {len(rows)}")
    print(f"   settled (75% or more inside one group member): {settled}")
    print(f"   divided across members, or mostly outside them: {split_evidence}")
    print(f"   no overlap found: {absent}")
    print(f"   unreliable, the 2017 layer is missing a group member: {unreliable}")
    done = collections.Counter(r['district_group'] for r in rows if r['verdict'] == 'settled')
    tot = collections.Counter(r['district_group'] for r in rows)
    whole = [g for g in tot if done[g] == tot[g]]
    print(f"   groups settled outright: {len(whole)} of {len(tot)}")


if __name__ == '__main__':
    main()
