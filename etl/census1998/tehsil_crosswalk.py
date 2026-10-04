"""Which 2017 tehsils each 1998 tehsil became, by the ground they cover.

The US Census Bureau's Pakistan Demobase carries the 1998 census for 365
tehsils, sub-divisions, talukas and agencies on their 1998 boundaries, with
the boundaries themselves. PBS's 2017 census restates every 2017 tehsil's
1998 population (table 1, POPULATION 1998). So a link can be both drawn and
checked:

  1. each 2017 tehsil's 2023 shapes are overlaid on the 1998 tehsils, and a
     2017 tehsil and a 1998 tehsil are joined where at least half of either
     lies in the other;
  2. a group is accepted only where the 1998 population of its 1998 tehsils
     equals PBS's restated 1998 population of its 2017 tehsils, to the
     person;
  3. neighbouring groups that do not balance are merged - in twos, threes or
     fours - where their imbalances cancel exactly, widening the definition
     of a neighbour step by step; whatever is left is merged only if, taken
     together, it balances too.

Boundaries from two sources never coincide exactly, so the overlay only
proposes; the population decides. Nothing that does not balance is linked.
"""
import collections, itertools, json, pathlib, struct, zipfile

import pyproj
from shapely.geometry import Polygon, shape
from shapely.ops import transform, unary_union
from shapely.strtree import STRtree
from shapely.validation import make_valid

STEM = 'PK_ADM3_POP98_10_06302010'


def read_demobase(zip_path):
    """[(attributes, geometry in WGS84)] for every record of the archive."""
    z = zipfile.ZipFile(zip_path)
    dbf, shp = z.read(STEM + '.dbf'), z.read(STEM + '.shp')
    prj = z.read(STEM + '.prj').decode()
    n = struct.unpack('<I', dbf[4:8])[0]
    hs, rs = struct.unpack('<HH', dbf[8:12])
    fields = []
    for o in range(32, hs - 1, 32):
        d = dbf[o:o + 32]
        if d[0] == 13:
            break
        fields.append((d[:11].split(b'\0')[0].decode('ascii'), d[16]))
    recs = []
    for i in range(n):
        raw = dbf[hs + i * rs: hs + (i + 1) * rs]
        o, r = 1, {}
        for name, w in fields:
            r[name] = raw[o:o + w].decode('latin-1').strip()
            o += w
        recs.append(r)
    geoms, p = [], 100
    while p < len(shp):
        _, clen = struct.unpack('>ii', shp[p:p + 8])
        body = shp[p + 8: p + 8 + clen * 2]
        p += 8 + clen * 2
        if struct.unpack('<i', body[:4])[0] == 0:
            geoms.append(None)
            continue
        nparts, npts = struct.unpack('<ii', body[36:44])
        parts = list(struct.unpack('<%di' % nparts, body[44:44 + 4 * nparts]))
        xy = struct.unpack('<%dd' % (2 * npts), body[44 + 4 * nparts: 44 + 4 * nparts + 16 * npts])
        pts = list(zip(xy[0::2], xy[1::2]))
        rings = [pts[a:b] for a, b in zip(parts, parts[1:] + [npts])]
        geoms.append(make_valid(unary_union([make_valid(Polygon(r)) for r in rings if len(r) >= 4])))
    assert len(geoms) == len(recs), 'shapes and records disagree'
    to_wgs = pyproj.Transformer.from_crs(pyproj.CRS.from_wkt(prj), 'EPSG:4326',
                                         always_xy=True).transform
    return [(r, transform(to_wgs, g) if g is not None else None) for r, g in zip(recs, geoms)]


def read_geo_js(path):
    s = pathlib.Path(path).read_text()
    gj = json.loads(s[s.index('{'): s.rstrip().rstrip(';').rindex('}') + 1])
    return {f['properties']['dds_id']: make_valid(shape(f['geometry'])) for f in gj['features']}


def build(old, shapes23, units):
    """old: [(name, pop1998, geometry)]; units: [(key, map_key, restated pop1998)].
    Returns (groups, unlinked) - groups as {'t': [old idx], 'u': [unit idx]}."""
    og = [o[2] for o in old]
    tree = STRtree(og)
    links = []
    for ui, (_, mk, _) in enumerate(units):
        g = unary_union([shapes23[k] for k in (mk or '').split() if k in shapes23]) if mk else None
        if g is None or g.is_empty:
            continue
        for ti in tree.query(g):
            inter = g.intersection(og[ti]).area
            if inter > 0:
                links.append((ui, int(ti), inter / g.area, inter / og[ti].area))

    par = {}

    def find(x):
        par.setdefault(x, x)
        while par[x] != x:
            par[x] = par[par[x]]
            x = par[x]
        return x
    for ui, ti, fu, ft in links:
        if fu >= .5 or ft >= .5:
            par[find(('u', ui))] = find(('t', ti))
    for ui in range(len(units)):
        find(('u', ui))
    for ti in range(len(old)):
        find(('t', ti))
    comp = collections.defaultdict(lambda: {'u': set(), 't': set()})
    for x in list(par):
        comp[find(x)][x[0]].add(x[1])
    comps = list(comp.values())

    def gap(c):
        return sum(old[i][1] for i in c['t']) - round(sum(units[i][2] for i in c['u']))

    def good(c):
        return c['u'] and c['t'] and gap(c) == 0

    def merge_round(comps, weak):
        idx = {}
        for ci, c in enumerate(comps):
            for i in c['u']:
                idx[('u', i)] = ci
            for i in c['t']:
                idx[('t', i)] = ci
        adj = collections.defaultdict(set)
        for ui, ti, fu, ft in links:
            if fu >= weak or ft >= weak:
                a, b = idx[('u', ui)], idx[('t', ti)]
                if a != b:
                    adj[a].add(b)
                    adj[b].add(a)
        bad = {ci for ci, c in enumerate(comps) if not good(c)}
        used, merges = set(), []
        for r in (2, 3, 4):
            for a in sorted(bad):
                if a in used:
                    continue
                pool = {b for b in adj[a] if b in bad and b not in used}
                for b in list(pool):
                    pool |= {x for x in adj[b] if x in bad and x not in used}
                pool.discard(a)
                for combo in itertools.combinations(sorted(pool), r - 1):
                    grp = (a,) + combo
                    if sum(gap(comps[g]) for g in grp) == 0:
                        merges.append(grp)
                        used |= set(grp)
                        break
        if not merges:
            return comps, 0
        out = [c for ci, c in enumerate(comps) if ci not in used]
        for grp in merges:
            m = {'u': set(), 't': set()}
            for g in grp:
                m['u'] |= comps[g]['u']
                m['t'] |= comps[g]['t']
            out.append(m)
        return out, len(merges)

    for weak in (0.02, 0.01, 0.005, 0.001):
        while True:
            comps, k = merge_round(comps, weak)
            if not k:
                break
    rest = [c for c in comps if not good(c)]
    if rest and sum(gap(c) for c in rest) == 0:
        m = {'u': set(), 't': set()}
        for c in rest:
            m['u'] |= c['u']
            m['t'] |= c['t']
        comps = [c for c in comps if good(c)] + [m]
    groups = [{'t': sorted(c['t']), 'u': sorted(c['u'])} for c in comps if good(c)]
    unlinked = [c for c in comps if not good(c)]
    return groups, unlinked
