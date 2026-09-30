#!/usr/bin/env python3
"""Build an offline, local-only census explorer from a verified geography release."""
import argparse
import csv
import hashlib
import io
import json
import shutil
from pathlib import Path

import shapely
from shapely.geometry import shape, mapping
from shapely.ops import unary_union
from shapely.validation import make_valid

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
WORKSPACE = REPO.parent
DEFAULT_RELEASE = WORKSPACE / 'data_darbar_warehouse/stage1/geography-2026-09-25-v0.2'
DEFAULT_OUT = WORKSPACE / 'local_preview/census'
GEOMETRY = REPO / 'app/data/pakistan_districts_province_boundries.geojson'
RELEASE_ID = 'geography-8cee002c8d474fd6'
TOTALS = {'2017': 207684626, '2023': 241499431}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def read_csv(path):
    with Path(path).open(newline='') as f:
        return list(csv.DictReader(f))


def csv_text(rows):
    out = io.StringIO(newline='')
    writer = csv.DictWriter(out, fieldnames=list(rows[0]), lineterminator='\n')
    writer.writeheader()
    writer.writerows(rows)
    return out.getvalue()


def json_text(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def verify_release(release):
    manifest = json.loads((release / 'build_manifest.json').read_text())
    require(manifest['release_id'] == RELEASE_ID, 'Unexpected census geography release')
    for name, record in manifest['outputs'].items():
        require(digest(release / name) == record['sha256'], f'Release checksum mismatch: {name}')
    return manifest


def polygon_parts(geometry):
    if geometry.geom_type == 'Polygon':
        return [geometry]
    if geometry.geom_type in ('MultiPolygon', 'GeometryCollection'):
        return [p for g in geometry.geoms for p in polygon_parts(g)]
    return []


def build_geometry(groups, registry, geo):
    """Assign every source outline once and dissolve groups without duplicating data."""
    require({r['comparison_id'] for r in registry['areas']} == set(groups), 'Incomplete boundary mapping')
    require(len(registry['areas']) == len(groups), 'Duplicate comparison mapping')
    by_name = {f['properties']['districts']: f for f in geo['features']}
    require(len(by_name) == len(geo['features']), 'Ambiguous source polygon name')
    used = set()
    features, audit = [], []
    for r in registry['areas']:
        shapes, repaired = [], []
        for name in r['polygon_names']:
            require(name not in used, f'Polygon assigned more than once: {name}')
            require(name in by_name, f'Unknown polygon: {name}')
            f = by_name[name]
            province = f['properties']['province_territory']
            province = {'Federally Administered Tribal Areas': 'Khyber Pakhtunkhwa'}.get(province, province)
            require(province == r['province'], f'Province mismatch: {name}')
            used.add(name)
            s = shape(f['geometry'])
            if not s.is_valid:
                s = unary_union(polygon_parts(make_valid(s)))
                repaired.append(name)
            require(not s.is_empty and s.is_valid, f'Invalid outline: {name}')
            shapes.append(s)
        merged = unary_union(shapes)
        require(merged.is_valid and not merged.is_empty, f'Invalid combined outline: {r["comparison_id"]}')
        props = {k: r[k] for k in ('comparison_id', 'comparison_name', 'province', 'map_status')}
        features.append({'type': 'Feature', 'properties': props, 'geometry': mapping(merged)})
        audit.append({**props, 'source_polygons': ';'.join(r['polygon_names']),
                      'geometry_repairs': ';'.join(repaired), 'polygon_match_certified': False,
                      'note': r['note']})
    context = [f for n, f in by_name.items() if n not in used]
    require(all(f['properties']['province_territory'] in ('Azad Jammu & Kashmir', 'Gilgit-Baltistan')
                for f in context), 'Unassigned in-scope source polygon')
    return {'type': 'FeatureCollection', 'features': features}, {'type': 'FeatureCollection', 'features': context}, audit


def build(release=DEFAULT_RELEASE, out=DEFAULT_OUT):
    release, out = release.resolve(), out.resolve()
    # This builder cannot write into the deployment directory or frozen releases.
    require(not out.is_relative_to(REPO / 'app'), 'Preview must stay outside the published app')
    require(not out.is_relative_to(WORKSPACE / 'data_darbar_warehouse'), 'Do not overwrite frozen releases')
    require(out != HERE and not HERE.is_relative_to(out), 'Output cannot contain preview source code')
    manifest = verify_release(release)
    registry = json.loads((HERE / 'boundary_mapping.json').read_text())
    require(digest(GEOMETRY) == registry['geometry_sha256'], 'Boundary file changed; mapping needs review')
    groups = {r['comparison_id']: r for r in read_csv(release / 'comparison_geographies.csv')}
    units = {r['unit_version_id']: r for r in read_csv(release / 'geography_register.csv')}
    panel = read_csv(release / 'panel.csv')
    lineage = read_csv(release / 'observation_lineage.csv')
    geo, context, audit = build_geometry(groups, registry, json.loads(GEOMETRY.read_text()))
    out.mkdir(parents=True, exist_ok=True)
    (out / 'sources').mkdir(exist_ok=True)
    (out / 'downloads').mkdir(exist_ok=True)
    source_files = {}
    for r in lineage:
        source_files[r['source_path']] = r['source_sha256']
    for name, sha in sorted(source_files.items()):
        src = WORKSPACE / name
        require(digest(src) == sha, f'Archived source checksum mismatch: {name}')
        shutil.copyfile(src, out / 'sources' / (sha + '.pdf'))
    areas = {}
    audit_by_id = {r['comparison_id']: r for r in audit}
    for cid, g in groups.items():
        a = {'id': cid, 'name': g['name'], 'province': audit_by_id[cid]['province'],
             'relationship': g['relationship'], 'review_note': g['review_note'],
             'map_status': audit_by_id[cid]['map_status'], 'map_note': audit_by_id[cid]['note'],
             'polygon_names': audit_by_id[cid]['source_polygons'].split(';'),
             'members': {}, 'values': {}, 'sources': {}}
        for year in ('2017', '2023'):
            a['members'][year] = [units[x]['canonical_name'].title() for x in json.loads(g[f'members_{year}'])]
            a['values'][year] = {}
            a['sources'][year] = {'population': [], 'education': []}
        areas[cid] = a
    seen = set()
    for r in panel:
        key = (r['comparison_id'], r['year'], r['module'], r['indicator'])
        require(key not in seen, f'Duplicate observation: {key}')
        seen.add(key)
        value = float(r['value']) if r['value'] else None
        if value is not None and r['unit'] == 'persons':
            require(value.is_integer(), 'Noninteger person count')
            value = int(value)
        areas[r['comparison_id']]['values'][r['year']][r['indicator']] = {
            'value': value, 'status': r['status'], 'unit': r['unit'], 'module': r['module'],
            'numerator': float(r['numerator']) if r['numerator'] else None,
            'denominator': float(r['denominator']) if r['denominator'] else None}
    source_seen = set()
    for r in lineage:
        year, module = r['source_dataset'].replace('census', '').split('_')
        key = (r['comparison_id'], year, module, r['unit_version_id'], r['source_sha256'])
        if key in source_seen:
            continue
        source_seen.add(key)
        areas[r['comparison_id']]['sources'][year][module].append({
            'name': units[r['unit_version_id']]['canonical_name'].title(),
            'url': r['source_url'], 'archived': 'sources/' + r['source_sha256'] + '.pdf',
            'locator': r['source_locator'], 'sha256': r['source_sha256']})
    for year, expected in TOTALS.items():
        require(sum(a['values'][year]['pop_total']['value'] for a in areas.values()) == expected,
                f'Population total changed: {year}')
    require(len(areas) == 127 and len(panel) == 4572, 'Unexpected panel coverage')
    download_names = ('panel.csv', 'comparison_geographies.csv', 'crosswalk.csv',
                      'geography_register.csv', 'observation_lineage.csv', 'indicator_dictionary.csv',
                      'district_population_bridge.csv', 'issues.csv', 'evidence.csv', 'build_manifest.json')
    for name in download_names:
        shutil.copyfile(release / name, out / 'downloads' / name)
    audit_text = csv_text(audit)
    (out / 'downloads/boundary_audit.csv').write_text(audit_text)
    meta = {'release_id': manifest['release_id'], 'source_release': manifest['source_release'],
            'area_count': len(areas), 'observation_count': len(panel), 'totals': TOTALS,
            'map_coloured_areas': sum(r['map_status'] == 'illustrative_outline' for r in audit),
            'unverified_outlines': sum(r['map_status'] == 'unverified_frontier_extent' for r in audit),
            'geometry_sha256': digest(GEOMETRY), 'education_change_enabled': False,
            'scope': 'Census source coverage: four provinces and Islamabad. AJK and Gilgit-Baltistan are outside this release.',
            'population_note': 'Published population counts; 2023 includes headcount-only records. Common statistical areas hold geography consistent; differences can also reflect enumeration coverage.',
            'education_note': 'Published education categories for ages 5+. Questionnaire equivalence across years is not yet certified. The 2023 education tables exclude headcount-only records and use their own age-5+ denominator. Change comparisons are unavailable.'}
    payload = {'meta': meta, 'areas': list(areas.values()), 'geometry': geo, 'context': context}
    (out / 'census-preview-data.js').write_text('window.DD_CENSUS_PREVIEW=' + json_text(payload) + ';\n')
    for name in ('index.html', 'preview.css', 'preview.js'):
        shutil.copyfile(HERE / name, out / name)
    shutil.copyfile(REPO / 'app/assets/js/d3.v7.min.js', out / 'd3.v7.min.js')
    shutil.copyfile(REPO / 'app/assets/img/logo.svg', out / 'logo.svg')
    build_inputs = {name: digest(HERE / name) for name in
                    ('build_preview.py', 'boundary_mapping.json', 'index.html', 'preview.css', 'preview.js')}
    build_record = {'schema': 'census-local-preview-v1', 'release_id': manifest['release_id'],
                    'release_manifest_sha256': digest(release / 'build_manifest.json'),
                    'geometry_sha256': digest(GEOMETRY), 'code': build_inputs,
                    'runtime': {'shapely': shapely.__version__, 'geos': shapely.geos_version_string},
                    'source_pdf_count': len(source_files), 'areas': len(areas),
                    'outlines_withheld': meta['unverified_outlines'], 'boundary_certification': False,
                    'outputs': {str(p.relative_to(out)): digest(p) for p in sorted(out.rglob('*'))
                                if p.is_file() and p.name != 'preview_build.json'}}
    (out / 'preview_build.json').write_text(json.dumps(build_record, indent=2, sort_keys=True) + '\n')
    return build_record


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--release', type=Path, default=DEFAULT_RELEASE)
    parser.add_argument('--out', type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()
    result = build(args.release, args.out)
    print(json.dumps({k: result[k] for k in ('release_id', 'areas', 'outlines_withheld', 'source_pdf_count')}, indent=2))
    print(args.out / 'index.html')
