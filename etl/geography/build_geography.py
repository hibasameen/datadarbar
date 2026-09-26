#!/usr/bin/env python3
"""Offline, reviewed census geography register and conservative two-year panel."""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
import platform
import re
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from locality_evidence import audit_localities, audit_subdistricts

HERE = Path(__file__).resolve().parent
WORKSPACE = HERE.parents[2]
SOURCE_RELEASE = Path('data_darbar_warehouse/stage1/census-2026-09-25-release')
SCHEMA = 'census-geography-panel-v0.2'
COMMON = ['pop_total', 'pop_male', 'pop_female', 'pop_transgender', 'total',
          'never_attended', 'below_primary', 'primary', 'middle', 'matric',
          'intermediate', 'graduate', 'masters_above', 'diploma_certificate',
          'others', 'matric_plus', 'pct_never_attended', 'pct_matric_plus']
RATES = {'pct_never_attended': 'never_attended', 'pct_matric_plus': 'matric_plus'}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def json_bytes(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2,
                       allow_nan=False) + '\n').encode()


def write_json(path, value):
    path.write_bytes(json_bytes(value))


def read_csv(path):
    with path.open(encoding='utf-8-sig', newline='') as handle:
        return list(csv.DictReader(handle))


def write_csv(path, rows):
    require(bool(rows), f'Unexpected empty output: {path.name}')
    with path.open('w', encoding='utf-8', newline='') as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]), lineterminator='\n')
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, ensure_ascii=False, separators=(',', ':'))
                             if isinstance(v, (list, dict)) else v for k, v in row.items()})


def verify_inputs(root, lock):
    require(len({f['path'] for f in lock['files']}) == len(lock['files']), 'Duplicate input path')
    for f in lock['files']:
        path = (root / f['path']).resolve()
        require(path.is_relative_to(root.resolve()), f'Input escapes root: {f["path"]}')
        require(path.is_file(), f'Missing input: {f["path"]}')
        require(path.stat().st_size == f['bytes'] and digest(path) == f['sha256'],
                f'Changed input; review before adopting: {f["path"]}')


def validate_registry(config, source_units):
    units = {u['unit_version_id']: u for u in config['units']}
    groups = {g['comparison_id']: g for g in config['groups']}
    require(len(units) == len(config['units']), 'Duplicate version ID')
    require(len(groups) == len(config['groups']), 'Duplicate comparison ID')
    retired = {g['comparison_id'] for g in config.get('retired_groups', [])}
    require(not retired.intersection(groups), 'Retired comparison ID reused')
    require(all(g['superseded_by'] in groups for g in config.get('retired_groups', [])), 'Unknown replacement comparison ID')
    aliases = {(a['source_dataset'], a['source_unit_id']): a for a in config['aliases']}
    require(len(aliases) == len(config['aliases']), 'Duplicate source alias')
    expected = {(u['source_dataset'], u['source_unit_id']): u for u in source_units}
    require(aliases.keys() == expected.keys(), 'Unreviewed or missing module/source unit')
    covered = collections.defaultdict(set)
    for key, alias in aliases.items():
        require(alias['source_name'] == expected[key]['source_name'], f'Changed source label: {key}')
        require(alias['unit_version_id'] in units, f'Unknown version: {key}')
        u = units[alias['unit_version_id']]
        require(u['year'] == int(key[0][6:10]), f'Alias crosses census years: {key}')
        module = key[0].split('_')[-1]
        require(module not in covered[u['unit_version_id']], f'Duplicate module on version: {key}')
        covered[u['unit_version_id']].add(module)
    evidence_ids = {e['evidence_id'] for e in config['evidence']}
    for u in units.values():
        require(u['comparison_id'] in groups, 'Unknown comparison geography')
        require(covered[u['unit_version_id']] == {'population', 'education'}, 'Incomplete version coverage')
        require(u['legal_valid_from'] is None and u['legal_valid_to'] is None,
                'This release has no reviewed legal effective dates')
    for g in groups.values():
        counts = collections.Counter(u['year'] for u in units.values() if u['comparison_id'] == g['comparison_id'])
        require(set(counts) == {2017, 2023}, f'Incomplete year partition: {g["comparison_id"]}')
        require(set(g['evidence_ids']) <= evidence_ids, 'Unknown evidence ID')
        require(g['approval'] in {'supported_tabular', 'held_for_review'}, 'Unknown approval')
        require(g['relationship'] in {'exact', 'aggregated', 'partial', 'unsupported'}, 'Unknown relationship')
        if g['relationship'] == 'exact':
            require(counts == {2017: 1, 2023: 1}, 'Exact link is not one to one')
        if g['approval'] == 'supported_tabular':
            require(g['relationship'] in {'exact', 'aggregated'}, 'Unresolved link cannot enter panel')
    return units, groups, aliases


def pdf_text(path):
    return subprocess.check_output(['pdftotext', '-layout', str(path), '-'], text=True, timeout=60)


def parse_2023_table1(text):
    """Retain full 11-cell district rows, including area and retrospective population."""
    result = {}
    for pn, page in enumerate(text.split('\f'), 1):
        lines = page.splitlines()
        for i, line in enumerate(lines):
            parts = line.split()
            if len(parts) < 11 or not all(re.fullmatch(r'[\d,.\-]+', x) for x in parts[-11:]):
                continue
            label = ' '.join(parts[:-11])
            if not label and i:
                label = ' '.join(lines[i - 1].split())
                if i + 1 < len(lines) and lines[i + 1].strip() == 'DISTRICT':
                    label += ' DISTRICT'
            if not re.search(r'\s(DISTRICT|PROTECTED AREA)$', label):
                continue
            require(label not in result, f'Duplicate Table 1 district: {label}')
            result[label] = (parts[-11:], f'PDF page {pn}; text line {i + 1}')
    return result


def count(value):
    if value in ('', '-', None):
        return None
    n = float(str(value).replace(',', ''))
    require(n >= 0 and n.is_integer(), f'Invalid count: {value}')
    return int(n)


def strict_sum(values):
    values = list(values)
    return sum(values) if values and all(v is not None for v in values) else None


def aggregate_indicator(members, indicator):
    """Never average rates or convert a source dash to zero."""
    if indicator in RATES:
        numerator = strict_sum(m[RATES[indicator]] for m in members)
        denominator = strict_sum(m['total'] for m in members)
        value = 100 * numerator / denominator if numerator is not None and denominator else None
        return value, numerator, denominator
    return strict_sum(m[indicator] for m in members), None, None


def source_table1(root, observations, aliases, locked):
    result = []
    cache = {}
    used23 = collections.defaultdict(set)
    for o in observations:
        if o['indicator'] != 'pop_total':
            continue
        relative = 'raw_data/pbs/' + o['source_file']
        require(relative in locked, 'Table 1 input is not locked')
        path = root / relative
        if o['year'] == '2017':
            text = pdf_text(path)
            line_no = int(re.search(r'text line (\d+)', o['source_locator'])[1])
            lines = text.split('\f')[0].splitlines()
            parts = lines[line_no - 1].split()
            if len(parts) < 11 or not all(re.fullmatch(r'[\d,.\-]+', v) for v in parts[-11:]):
                parts = (lines[line_no - 1] + ' ' + lines[line_no]).split()
            require(len(parts) >= 12 and all(re.fullmatch(r'[\d,.\-]+', v) for v in parts[-11:]),
                    f'Changed 2017 Table 1 structure: {relative}')
            raw, locator = parts[-11:], o['source_locator']
            retro = None
        else:
            if relative not in cache:
                cache[relative] = parse_2023_table1(pdf_text(path))
            require(o['source_name'] in cache[relative], f'Unparsed 2023 district: {o["source_name"]}')
            raw, locator = cache[relative][o['source_name']]
            used23[relative].add(o['source_name'])
            retro = count(raw[-2])
        require(count(raw[1]) == count(o['value']), f'Source population mismatch: {relative}')
        result.append(dict(unit_version_id=aliases[o['source_dataset'], o['source_unit_id']]['unit_version_id'],
                           year=int(o['year']), source_name=o['source_name'], area_sq_km=float(raw[0].replace(',', '')),
                           population=count(raw[1]), retrospective_2017_population=retro,
                           source_path=relative, source_locator=locator, source_url=o['source_url'],
                           source_sha256=locked[relative]['sha256']))
    for path, parsed in cache.items():
        require(used23[path] == parsed.keys(), f'Unregistered Table 1 district in {path}')
    return sorted(result, key=lambda x: x['unit_version_id'])


def build(root, out, config_path=HERE / 'registry.json', lock_path=HERE / 'inputs.lock.json'):
    root, out = root.resolve(), out.resolve()
    require(not out.exists(), 'Output exists; use a new release directory')
    require(out != root and not root.is_relative_to(out), 'Output cannot contain workspace')
    require(not out.is_relative_to(root / 'datadarbar') and not out.is_relative_to(root / 'raw_data'),
            'Output cannot be inside code or raw sources')
    config, lock = json.loads(config_path.read_text()), json.loads(lock_path.read_text())
    verify_inputs(root, lock)
    locked = {f['path']: f for f in lock['files']}
    source = root / SOURCE_RELEASE
    manifest = json.loads((source / 'build_manifest.json').read_text())
    require(manifest['release_id'] == config['source_release_id'] == lock['source_release_id'], 'Wrong census release')
    observations = read_csv(source / 'source_observations.csv')
    units, groups, aliases = validate_registry(config, read_csv(source / 'source_units.csv'))
    require(collections.Counter(u['year'] for u in units.values()) == {2017: 135, 2023: 136}, 'Changed census coverage')
    table1 = source_table1(root, observations, aliases, locked)
    require(len(table1) == len(units), 'Missing Table 1 records')
    locality_rows, locality_checks = audit_localities(root, pdf_text)
    administrative_rows, administrative_checks = audit_subdistricts(root, pdf_text)
    summaries, checks = [], locality_checks + administrative_checks
    for gid, g in groups.items():
        members = [u for u in units.values() if u['comparison_id'] == gid]
        ids = {u['unit_version_id'] for u in members}
        a = [x for x in table1 if x['unit_version_id'] in ids and x['year'] == 2017]
        b = [x for x in table1 if x['unit_version_id'] in ids and x['year'] == 2023]
        original, retro = sum(x['population'] for x in a), sum(x['retrospective_2017_population'] for x in b)
        area17, area23 = sum(x['area_sq_km'] for x in a), sum(x['area_sq_km'] for x in b)
        passed = original == retro and area17 == area23
        if g['approval'] == 'supported_tabular':
            require(passed, f'Approved geography does not reconcile: {gid}')
        summaries.append({**g, 'members_2017': sorted(x['unit_version_id'] for x in a),
                          'members_2023': sorted(x['unit_version_id'] for x in b),
                          'population_2017_original': original, 'population_2017_retrospective': retro,
                          'population_2017_difference': retro - original, 'area_2017_sq_km': area17,
                          'area_2023_sq_km': area23, 'numeric_reconciliation': 'pass' if passed else 'fail',
                          'evidence_scope': config['evidence_scope'], 'polygon_match_certified': False})
        checks.append(dict(check='geography_reconciliation', comparison_id=gid,
                           result='pass' if passed else 'expected_hold',
                           detail=f'Population difference {retro-original}; area difference {area23-area17}'))
    cells = collections.defaultdict(dict)
    lineage = []
    input_hashes = {x['path']: x['sha256'] for x in manifest['inputs']['files']}
    for o in observations:
        u = units[aliases[o['source_dataset'], o['source_unit_id']]['unit_version_id']]
        require(o['indicator'] not in cells[u['unit_version_id']], 'Duplicate indicator on unit')
        cells[u['unit_version_id']][o['indicator']] = float(o['value']) if o['indicator'].startswith('pct_') and o['value'] else count(o['value'])
        g = groups[u['comparison_id']]
        lineage.append(dict(unit_version_id=u['unit_version_id'], comparison_id=u['comparison_id'],
                            source_dataset=o['source_dataset'], source_unit_id=o['source_unit_id'],
                            indicator=o['indicator'], source_value=o['value'], source_status=o['status'],
                            validation_status=o['validation_status'], universe=o['universe'],
                            source_path='raw_data/pbs/' + o['source_file'], source_locator=o['source_locator'],
                            source_url=o['source_url'], source_sha256=input_hashes[o['source_file']],
                            input_release=config['source_release_id'],
                            panel_disposition='included' if g['approval'] == 'supported_tabular' and o['indicator'] in COMMON else
                            'component_of_harmonized_category' if g['approval'] == 'supported_tabular' else 'geography_withheld'))
        require(o['validation_status'] in {'pass', 'source_symbol_present'}, f'Unaccepted source validation: {o["validation_status"]}')
    panel, crosswalk = [], []
    for u in units.values():
        g = groups[u['comparison_id']]
        require(set(COMMON) <= cells[u['unit_version_id']].keys(), 'Missing common indicator')
        crosswalk.append(dict(unit_version_id=u['unit_version_id'], year=u['year'], comparison_id=u['comparison_id'],
                              relationship=g['relationship'], status=g['approval'],
                              count_weight=1 if g['approval'] == 'supported_tabular' else None,
                              allocation_rule='whole_source_unit' if g['approval'] == 'supported_tabular' else 'withheld',
                              evidence_ids=g['evidence_ids'], polygon_match_certified=False))
    for gid, g in groups.items():
        if g['approval'] != 'supported_tabular':
            continue
        for year in (2017, 2023):
            members = [u for u in units.values() if u['comparison_id'] == gid and u['year'] == year]
            for indicator in COMMON:
                value, numerator, denominator = aggregate_indicator([cells[u['unit_version_id']] for u in members], indicator)
                edu = not indicator.startswith('pop_')
                panel.append(dict(comparison_id=gid, comparison_name=g['name'], year=year,
                                  module='education' if edu else 'population', indicator=indicator, value=value,
                                  unit='percent' if indicator in RATES else 'persons',
                                  status='complete' if value is not None else 'missing_source_component',
                                  numerator=numerator, denominator=denominator, member_count=len(members),
                                  source_unit_versions=';'.join(u['unit_version_id'] for u in members),
                                  geography_status=g['approval'], geography_relationship=g['relationship'],
                                  universe='age_5_plus_all_sexes_detailed_enumeration' if edu and year == 2023 else
                                  'age_5_plus_all_sexes' if edu else 'all_ages_all_sexes_including_headcount_only' if year == 2023 else 'all_ages_all_sexes',
                                  comparability='geography_supported_definitions_provisional' if edu else 'supported_tabular_geography_coverage_caveat',
                                  source_release=config['source_release_id']))
    # Test arithmetic after combining units, independently of source-level tests.
    by_group_year = collections.defaultdict(dict)
    for row in panel:
        by_group_year[row['comparison_id'], row['year']][row['indicator']] = row['value']
    categories = ['never_attended', 'below_primary', 'primary', 'middle', 'matric',
                  'intermediate', 'graduate', 'masters_above', 'diploma_certificate', 'others']
    for (gid, year), values in by_group_year.items():
        require(strict_sum(values[i] for i in categories) == values['total'], 'Aggregated education partition mismatch')
        require(strict_sum(values[i] for i in ['matric', 'intermediate', 'graduate', 'masters_above']) == values['matric_plus'],
                'Aggregated matric-plus mismatch')
        require(all(values[i] is None or 0 <= values[i] <= 100 for i in RATES), 'Percentage outside valid range')
        checks.append(dict(check='education_partition_and_rates', comparison_id=gid, result='pass', detail=f'{year}: counts reconcile; derived groups reconcile; percentages bounded'))
        known_sexes = strict_sum(values[i] for i in ['pop_male', 'pop_female', 'pop_transgender'])
        if known_sexes is not None:
            require(known_sexes == values['pop_total'], 'Aggregated population partition mismatch')
        else:
            require(sum(values[i] or 0 for i in ['pop_male', 'pop_female', 'pop_transgender']) <= values['pop_total'],
                    'Known sex counts exceed population')
        checks.append(dict(check='population_partition', comparison_id=gid,
                           result='pass' if known_sexes is not None else 'source_symbol_retained',
                           detail=f'{year}: sex total reconciles' if known_sexes is not None else f'{year}: missing source component remains missing'))
    coverage = []
    for year in (2017, 2023):
        full = [x for x in table1 if x['year'] == year]
        included = [x for x in full if groups[units[x['unit_version_id']]['comparison_id']]['approval'] == 'supported_tabular']
        coverage.append(dict(year=year, source_units=len(full), included_source_units=len(included),
                             withheld_source_units=len(full)-len(included), source_population=sum(x['population'] for x in full),
                             included_population=sum(x['population'] for x in included),
                             withheld_population=sum(x['population'] for x in full)-sum(x['population'] for x in included)))
        require(sum(x['value'] for x in panel if x['year'] == year and x['indicator'] == 'pop_total') == coverage[-1]['included_population'],
                'Population not conserved in supported panel')
    evidence = []
    for e in config['evidence']:
        sources = json.loads((root / Path(e['path']).parent / 'retrieval_manifest.json').read_text())
        evidence.append({**e, 'source_url': next(x['url'] for x in sources['files'] if x['path'] == Path(e['path']).name),
                         'sha256': locked[e['path']]['sha256']})
    district_bridge = []
    for name in ['JHANG', 'TOBA TEK SINGH', 'KACHHI', 'NASIRABAD']:
        a = next(r for r in table1 if r['source_name'] == name + ' DISTRICT' and r['year'] == 2017)
        b = next(r for r in table1 if r['source_name'] == name + ' DISTRICT' and r['year'] == 2023)
        district_bridge.append(dict(district=name, comparison_id=units[a['unit_version_id']]['comparison_id'],
                                    original_2017_population=a['population'],
                                    pbs_retrospective_2017_population=b['retrospective_2017_population'],
                                    retrospective_difference=b['retrospective_2017_population']-a['population'],
                                    population_2023=b['population'],
                                    status='as_published_population_baselines_only_not_a_full_indicator_crosswalk',
                                    source_2017=a['source_path'], locator_2017=a['source_locator'],
                                    source_2023=b['source_path'], locator_2023=b['source_locator']))
    out.parent.mkdir(parents=True, exist_ok=True)
    temp = Path(tempfile.mkdtemp(prefix='.geography-', dir=out.parent))
    try:
        dictionary = []
        for item in read_csv(source / 'indicator_dictionary.csv'):
            if item['indicator'] not in COMMON:
                continue
            edu = item['source_dataset'].endswith('education')
            dictionary.append({**item,
                               'cross_year_comparability': 'geography_supported_definitions_provisional' if edu else 'supported_tabular_geography_coverage_caveat',
                               'definition_review': 'published labels and additive category grouping only; questionnaire equivalence not certified' if edu else 'all-sex/sex population counts as published',
                               'coverage_note': '2023 detailed enumeration excludes headcount-only records; use own age-5+ denominator' if edu else '2023 includes headcount-only records',
                               'panel_aggregation': 'sum numerator and denominator before division' if item['indicator'] in RATES else 'sum whole-unit counts; any missing component withholds result'})
        outputs = {'geography_register.csv': config['units'], 'source_unit_aliases.csv': config['aliases'],
                   'comparison_geographies.csv': summaries, 'crosswalk.csv': crosswalk,
                   'source_table1.csv': table1, 'evidence.csv': evidence, 'change_events.csv': config['events'],
                   'issues.csv': config['issues'], 'observation_lineage.csv': lineage, 'panel.csv': panel,
                   'validation_checks.csv': checks, 'coverage.csv': coverage,
                   'indicator_dictionary.csv': dictionary,
                   'locality_correspondences.csv': locality_rows,
                   'subdistrict_source_rows.csv': administrative_rows,
                   'district_population_bridge.csv': district_bridge,
                   'retired_comparison_geographies.csv': config['retired_groups']}
        for name, rows in outputs.items():
            write_csv(temp / name, rows)
        con = duckdb.connect()
        con.execute('SET threads=1')
        con.execute("CREATE TABLE panel AS SELECT * REPLACE (CAST(year AS INTEGER) AS year, CAST(value AS DOUBLE) AS value, CAST(numerator AS BIGINT) AS numerator, CAST(denominator AS BIGINT) AS denominator, CAST(member_count AS INTEGER) AS member_count) FROM read_csv(?, header=true, all_varchar=true)", [str(temp / 'panel.csv')])
        con.execute("COPY panel TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(temp / 'panel.parquet')])
        require(con.execute('SELECT count(*) FROM read_parquet(?)', [str(temp / 'panel.parquet')]).fetchone()[0] == len(panel), 'Parquet row count mismatch')
        require(con.execute('SELECT count(*) FROM ((SELECT * FROM panel EXCEPT ALL SELECT * FROM read_parquet(?)) UNION ALL (SELECT * FROM read_parquet(?) EXCEPT ALL SELECT * FROM panel))', [str(temp / 'panel.parquet')]*2).fetchone()[0] == 0, 'Parquet values changed')
        con.close()
        runtime = {'python': platform.python_version(), 'duckdb': duckdb.__version__,
                   'pdftotext': subprocess.run(['pdftotext', '-v'], capture_output=True, text=True, check=True).stderr.splitlines()[0]}
        report = dict(schema_version=SCHEMA, source_release=config['source_release_id'], unit_versions=len(units),
                      source_module_aliases=len(aliases), comparison_geographies=len(groups),
                      supported_geographies=sum(g['approval']=='supported_tabular' for g in groups.values()),
                      withheld_geographies=[g['name'] for g in groups.values() if g['approval']=='held_for_review'],
                      resolved_through_joint_areas=['Jhang + Toba Tek Singh', 'Kachhi + Nasirabad'],
                      individual_district_allocations_unresolved=['Jhang', 'Toba Tek Singh', 'Kachhi', 'Nasirabad'],
                      locality_correspondences=len(locality_rows),
                      panel_rows=len(panel), missing_panel_cells=sum(x['value'] is None for x in panel),
                      coverage=coverage, check_results=dict(collections.Counter(x['result'] for x in checks)),
                      geometry_certified=False, legal_effective_dates_certified=False,
                      exclusions=['AJK', 'Gilgit-Baltistan', 'post-census administrative changes', 'village geography', 'automatic cross-year change rankings'])
        write_json(temp / 'validation_report.json', report)
        write_json(temp / 'input_lock.json', lock)
        code = {p.name: digest(p) for p in sorted(HERE.iterdir()) if p.suffix in {'.py', '.json', '.txt'}}
        identity = dict(schema_version=SCHEMA, source_release=config['source_release_id'], inputs=lock,
                        registry_sha256=digest(config_path), code=code, runtime=runtime)
        identity['release_id'] = 'geography-' + hashlib.sha256(json_bytes(identity)).hexdigest()[:16]
        identity['outputs'] = {p.name: {'bytes': p.stat().st_size, 'sha256': digest(p)} for p in sorted(temp.iterdir())}
        write_json(temp / 'build_manifest.json', identity)
        write_json(temp / 'run.json', dict(built_at=datetime.now(timezone.utc).isoformat(), root=str(root), output=str(out)))
        temp.rename(out)
    except BaseException:
        shutil.rmtree(temp)
        raise
    return report


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workspace', type=Path, default=WORKSPACE)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.workspace, args.out), indent=2))
