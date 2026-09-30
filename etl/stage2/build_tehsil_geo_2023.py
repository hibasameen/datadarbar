"""The map's Census-2023 sub-district geometry, keyed by the census's own unit id.

The site's existing tehsil layer (`tehsils_geo.js`, window.DD_GEO_T) is
geoBoundaries ADM3, which is 2017 vintage. Census 2023 subdivided a good deal of
it, so 51 of its sub-district units share 23 of those polygons and the census map
could only draw 331 of them - a unit that shares a shape with a neighbour cannot
be coloured without choosing arbitrarily between the two.

PBS's Digital Census 2023 tehsil layer has one polygon per census unit, carrying
`dds_id`, the same identifier the panel uses. So every one of the 591
sub-district units gets its own shape and the ambiguity disappears.

Geometry is simplified to 0.001 degrees and coordinates kept to three decimals,
about 110 m either way. That holds the worst area change across the 591 units to
1.6% and the median to nothing measurable, for 1.6 MB - 0.42 MB over the wire,
which is less than the 2017 layer costs gzipped despite covering more units.

Usage: build_tehsil_geo_2023.py --src <pbs_tehsils_2023.geojson> [--out <js>]
"""
import argparse, datetime, json, pathlib, re

from shapely.geometry import shape, mapping

TOLERANCE = 0.001
PRECISION = 3


def round_coords(o, p=PRECISION):
    if isinstance(o, float):
        return round(o, p)
    if isinstance(o, (list, tuple)):
        return [round_coords(x, p) for x in o]
    return o


def stamp_asset_version(out):
    """Bump app.js's ASSET_V so returning visitors do not keep a stale payload.

    These files are loaded by <script src> under a name that never changes, so
    without this a browser holds whatever it cached. It is how a rebuilt tehsil
    layer of 649 shapes still rendered as 591.
    """
    app = pathlib.Path(out).resolve().parents[1] / 'assets' / 'js' / 'app.js'
    if not app.exists():
        return
    text = app.read_text()
    today = datetime.date.today().isoformat()
    m = re.search(r"const ASSET_V = '(\d{4}-\d{2}-\d{2})([a-z])';", text)
    if not m:
        return
    # same day, next letter; a new day starts again at 'a'
    nxt = today + (chr(ord(m.group(2)) + 1) if m.group(1) == today else 'a')
    if nxt != m.group(1) + m.group(2):
        app.write_text(text.replace(m.group(0), f"const ASSET_V = '{nxt}';"))
        print(f'  app.js ASSET_V -> {nxt}')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--out', default='app/data/tehsils_2023_geo.js')
    ap.add_argument('--level', choices=['tehsil', 'district'], default='tehsil')
    ap.add_argument('--global-name', default='DD_GEO_T23')
    a = ap.parse_args()

    src = json.loads(pathlib.Path(a.src).read_text())
    feats, worst = [], 0.0
    for f in src['features']:
        p = f['properties']
        # Every place PBS draws, except the one polygon it labels OCCUPIED
        # KASHMIR - Indian-occupied Kashmir, the part of Jammu & Kashmir not
        # under Pakistani administration. That is excluded deliberately.
        #
        # Azad Jammu & Kashmir and Gilgit-Baltistan, which are Pakistani-
        # administered, are always drawn - whether or not the census reaches
        # them. They are greyed for want of data, not left off: a map of
        # Pakistan that stops at the census frame is a different claim about
        # the country.
        if p.get('province') == 'OCCUPIED KASHMIR':
            continue
        if a.level == 'tehsil':
            if not p.get('tehsil_code'):
                continue
        elif not p.get('district_code'):
            continue
        g = shape(f['geometry'])
        if not g.is_valid:
            g = g.buffer(0)
        before = g.area
        g = g.simplify(TOLERANCE, preserve_topology=True)
        if before:
            worst = max(worst, abs(g.area - before) / before)
        m = mapping(g)
        if a.level == 'tehsil':
            # A tehsil outside the census has no dds_id, so it is keyed on PBS's
            # own tehsil code instead. Nothing joins to that key, which is the
            # point: the shape draws and stays grey. `nc` marks it so the map
            # can say why rather than looking like missing data.
            props = {'dds_id': p['dds_id'] or 'PBS-' + str(p['tehsil_code']),
                     'n': p.get('census_unit') or p.get('tehsil'),
                     'd': p.get('district'), 'p': p.get('province')}
            if not p.get('in_census_2023'):
                props['nc'] = 1
        
        else:
            # `census_district` is the census's own name for the district and is
            # null for the 21 units outside the census frame - Gilgit-Baltistan,
            # Azad Jammu & Kashmir and the Occupied Kashmir polygon. They are
            # drawn, because the country does not stop at the census frame, but
            # they carry no census value and the map greys them.
            props = {'code': p['district_code'], 'n': p.get('district'),
                     'c': p.get('census_district'), 'p': p.get('province')}
            if not p.get('in_census_2023'):
                props['nc'] = 1
        feats.append({
            'type': 'Feature',
            'properties': props,
            'geometry': {'type': m['type'], 'coordinates': round_coords(m['coordinates'])},
        })

    feats.sort(key=lambda f: f['properties'].get('dds_id') or f['properties'].get('code'))
    body = json.dumps({'type': 'FeatureCollection', 'features': feats}, separators=(',', ':'))
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        '/* Data Darbar - PBS Digital Census 2023 ' + a.level + ' geometry.\n'
        '   Generated by etl/stage2/build_tehsil_geo_2023.py; do not edit by hand. */\n'
        'window.' + a.global_name + '=' + body + ';\n')
    stamp_asset_version(out)
    print(f"{len(feats)} units written to {out}  ({out.stat().st_size/1e6:.2f} MB)")
    print(f"worst area change from simplification: {100*worst:.1f}%")


if __name__ == '__main__':
    main()
