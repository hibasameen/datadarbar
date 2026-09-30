"""Health facility points: two published sources, cleaned and keyed to districts.

  health_facilities_pk   ALHASAN Systems' Pakistan Health Facilities, published
                         on HDX on 15 November 2017 under CC0. 25,704 public and
                         private facilities with a category, government or
                         private, and the source's own tehsil and district.
  healthsites_osm_2019   The healthsites.io export of OpenStreetMap health
                         amenities for Pakistan, October 2019. 2,638 points.
                         OpenStreetMap data: ODbL, and it stays ODbL.

What is dropped, and why. Phone, mobile, email, URL and street address go from
the ALHASAN table (they are nearly empty - 28 phones, one email - and a phone
number beside a private clinic's name is often a person's). The OSM table loses
its contact numbers and the editor usernames and changeset ids that came with
the export. Nothing else is altered: names, categories and positions are as
published.

What is added. Every point is placed in a PBS Census 2023 district by its
position (district_code, district_2023, province_2023), and in Data Darbar's
147-district frame (district_key), so the tables join the rest of the site.
The ALHASAN categories are grouped into care types (care_group), and
in_care_layer marks the ones the Places map draws: care facilities, not
pharmacies, vets, opticians, ambulances or the morgue.

Usage:
  build_health_facilities.py --alhasan <.../Pakistan_Health_Facilities>
                             --osm <.../healthsites> --app app --out etl/health_facilities
(paths to shapefiles without the extension; the raw copies are archived under
raw_data/health_facilities/)
"""
import argparse, hashlib, json, re, struct
from pathlib import Path

import pandas as pd
from shapely.geometry import shape, Point
from shapely.strtree import STRtree

RELEASE_ALHASAN = '2017-11-15'
RELEASE_OSM = '2019-10-09'

# ALHASAN category -> care group. None means not a care facility, kept in the
# table and left off the map.
CARE = {
    'GENERAL HOSPITALS': 'Hospital', 'CHILDREN HOSPITAL': 'Hospital',
    'DISTRICT HEADQUARTER HOSPITAL': 'Hospital', 'TEHSIL HEADQUARTER HOSPITAL': 'Hospital',
    'AGENCY HEADQUARTER HOSPITAL': 'Hospital',
    'BASIC HEALTH UNIT': 'Primary care', 'RURAL HEALTH CENTER': 'Primary care',
    'SUB-HEALTH CENTER': 'Primary care', 'URBAN HEALTH CENTRE': 'Primary care',
    'DISPENSARY': 'Primary care',
    'MATERNITY HOME': 'Mother and child', 'MCH CENTRE': 'Mother and child',
    'FAMILY WELFARE CENTER': 'Mother and child',
    'GENERAL PHYSICIAN': 'Clinic', 'SPECIALIST': 'Clinic', 'DENTAL CLINIC': 'Clinic',
    'HOMEOPATHIC': 'Traditional medicine', 'DAWAKHANA': 'Traditional medicine',
    'ROUTINE TEST LABORATORIES': 'Laboratory and diagnostics',
    'CLINICAL LABORATORIES': 'Laboratory and diagnostics',
    'DIAGNOSTIC CENTRE': 'Laboratory and diagnostics', 'BLOOD BANK': 'Laboratory and diagnostics',
    'TB': 'TB and leprosy', 'LEPROSY CENTRE': 'TB and leprosy',
    'MEDICAL STORES': None, 'VETERINARY': None, 'OPTICS': None,
    'AMBULANCE SERVICE': None, 'MORGUE': None,
}
GROUPS = ['Hospital', 'Primary care', 'Mother and child', 'Clinic',
          'Traditional medicine', 'Laboratory and diagnostics', 'TB and leprosy']


# ── a point shapefile, without GDAL ─────────────────────────────────────────
def read_dbf(path):
    with open(path, 'rb') as f:
        n, hlen, rlen = struct.unpack('<xxxxLHH20x', f.read(32))
        fields = []
        while True:
            d = f.read(32)
            if d[0] == 0x0D:
                break
            fields.append((d[:11].split(b'\x00')[0].decode('latin-1'), chr(d[11]), d[16]))
        f.seek(hlen)
        rows = []
        for _ in range(n):
            rec = f.read(rlen)
            if not rec or rec[:1] == b'\x1a':
                break
            pos, row = 1, {}
            for name, typ, size in fields:
                raw = rec[pos:pos + size]
                pos += size
                try:
                    v = raw.decode('utf-8').strip()
                except UnicodeDecodeError:
                    v = raw.decode('latin-1').strip()
                if typ in 'NF':
                    v = float(v) if v and set(v) != {'*'} else None
                row[name] = v if v != '' else None
            rows.append(row)
    return pd.DataFrame(rows)


def read_points(base):
    df = read_dbf(base + '.dbf')
    data = Path(base + '.shp').read_bytes()
    assert struct.unpack('<i', data[32:36])[0] == 1, 'not a point shapefile'
    pts, pos = [], 100
    while pos < len(data):
        clen = struct.unpack('>i', data[pos + 4:pos + 8])[0]
        pts.append(struct.unpack('<dd', data[pos + 12:pos + 28]))
        pos += 8 + clen * 2
    assert len(pts) == len(df)
    df['lng'] = [round(p[0], 6) for p in pts]
    df['lat'] = [round(p[1], 6) for p in pts]
    return df


def sha(base):
    h = hashlib.sha256()
    for ext in ('.shp', '.dbf'):
        h.update(Path(base + ext).read_bytes())
    return h.hexdigest()


# ── districts ────────────────────────────────────────────────────────────────
def norm(s):
    return re.sub('[^a-z0-9]', '', str(s).lower())


def district_lookup(app):
    js = (app / 'data/districts_2023_geo.js').read_text()
    d23 = json.loads(js[js.index('=') + 1:].strip().rstrip(';'))['features']
    t23 = STRtree([shape(f['geometry']) for f in d23])
    old = json.loads((app / 'data/pakistan_districts_province_boundries.geojson').read_text())['features']
    keys = {norm(k): k for k in json.loads((app / 'data/districts.json').read_text())}
    t_old = STRtree([shape(f['geometry']) for f in old])
    k_old = [keys.get(norm(f['properties']['districts'])) for f in old]

    def at(lng, lat):
        p = Point(lng, lat)
        h = t23.query(p, predicate='within')
        g = d23[int(h[0])]['properties'] if len(h) else None
        o = t_old.query(p, predicate='within')
        return ((g['code'], g['n'].title().replace(' District', ''), g['p'].title()) if g else (None, None, None),
                k_old[int(o[0])] if len(o) else None)
    return at


def place(df, at):
    got = [at(x, y) for x, y in zip(df.lng, df.lat)]
    df['district_code'] = [g[0][0] for g in got]
    df['district_2023'] = [g[0][1] for g in got]
    df['province_2023'] = [g[0][2] for g in got]
    df['district_key'] = [g[1] for g in got]
    return df


# ── the two tables ───────────────────────────────────────────────────────────
def alhasan(base, at):
    raw = read_points(base)
    unknown = set(raw.Category) - set(CARE)
    assert not unknown, f'categories with no care group: {unknown}'
    beds = pd.to_numeric(raw.Beds.where(raw.Beds != '-'), errors='coerce')
    df = pd.DataFrame({
        'row_id': range(1, len(raw) + 1),
        'source_object_id': raw.OBJECTID.astype(int),
        'name': raw.Name,
        'category': raw.Category.str.title(),
        'sub_category': raw.Sub_Cat,
        'sector': raw.Gov_Pvt.str.title(),
        'care_group': raw.Category.map(CARE),
        'in_care_layer': raw.Category.map(CARE).notna(),
        'facility_class': raw.Class, 'status': raw.Status,
        'beds': beds.astype('Int64'),
        'locality': raw.Locality, 'union_council': raw.Uc, 'town': raw.Town,
        'mauza': raw.Muza, 'tehsil': raw.Tehsil, 'district': raw.District,
        'province': raw.Province,
        'lat': raw.lat, 'lng': raw.lng,
    })
    df = place(df, at)
    df['source'] = 'ALHASAN Systems, Pakistan Health Facilities, via HDX (CC0)'
    df['release'] = RELEASE_ALHASAN
    return df


def osm(base, at):
    raw = read_points(base)
    date = raw['changese_2'].astype(str).str.replace(r'(\d{4})(\d\d)(\d\d)', r'\1-\2-\3', regex=True)
    df = pd.DataFrame({
        'row_id': range(1, len(raw) + 1),
        'osm_id': raw.osm_id.astype('Int64'), 'osm_type': raw.osm_type,
        'healthsites_uuid': raw.uuid,
        'name': raw['name'], 'amenity': raw.amenity, 'healthcare': raw.healthcare,
        'speciality': raw.speciality, 'operator': raw.operator,
        'operator_type': raw.operator_t, 'beds': raw.beds, 'emergency': raw.emergency,
        'opening_hours': raw.opening_ho, 'wheelchair': raw.wheelchair,
        'address': raw.addr_full, 'completeness_pct': raw.completene,
        'last_edited': date.where(date != 'None'),
        'lat': raw.lat, 'lng': raw.lng,
    })
    df = place(df, at)
    df['source'] = '© OpenStreetMap contributors, via healthsites.io (ODbL)'
    df['release'] = RELEASE_OSM
    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--alhasan', required=True)
    ap.add_argument('--osm', required=True)
    ap.add_argument('--app', type=Path, default=Path('app'))
    ap.add_argument('--out', type=Path, default=Path(__file__).resolve().parent)
    a = ap.parse_args()
    at = district_lookup(a.app)
    h = alhasan(a.alhasan, at)
    o = osm(a.osm, at)
    a.out.mkdir(parents=True, exist_ok=True)
    gz = {'method': 'gzip', 'mtime': 0}
    h.to_csv(a.out / f'health_facilities_pk_{RELEASE_ALHASAN}.csv.gz', index=False, compression=gz)
    o.to_csv(a.out / f'healthsites_osm_{RELEASE_OSM}.csv.gz', index=False, compression=gz)
    report = {
        'alhasan': {'rows': len(h), 'source_sha256': sha(a.alhasan),
                    'by_sector': h.sector.value_counts().to_dict(),
                    'in_care_layer': int(h.in_care_layer.sum()),
                    'by_care_group': h.care_group.value_counts().to_dict(),
                    'outside_2023_districts': int(h.district_code.isna().sum())},
        'osm': {'rows': len(o), 'source_sha256': sha(a.osm),
                'by_amenity': o.amenity.fillna('none').value_counts().to_dict(),
                'outside_2023_districts': int(o.district_code.isna().sum())},
    }
    (a.out / 'health_facilities_report.json').write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
