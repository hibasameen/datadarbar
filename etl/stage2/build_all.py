#!/usr/bin/env python3
"""Run the whole Stage 2 Option A pipeline into one dated release directory.

    python3 build_all.py --workspace /path/to/Data\\ Darbar --release optionA-YYYY-MM-DD

Each stage writes into its own subdirectory and the next reads from it, so a
failure stops the chain rather than leaving a half-built release.
"""
import argparse, datetime, json, pathlib, subprocess, sys, time
from lock import build_lock, manifest

HERE = pathlib.Path(__file__).resolve().parent


def run(step, args):
    print(f"\n=== {step} " + "=" * (66 - len(step)))
    t = time.time()
    r = subprocess.run([sys.executable, str(HERE / f'{step}.py')] + args)
    if r.returncode:
        sys.exit(f"{step} failed with exit code {r.returncode}")
    print(f"--- {step} done in {time.time()-t:.1f}s")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--workspace', required=True, help='the Data Darbar workspace root')
    ap.add_argument('--sources', default='raw_data/pbs/stage2_sources/2026-09-26-optionB')
    ap.add_argument('--release', required=True, help='name of the release directory')
    ap.add_argument('--skip-tests', action='store_true')
    a = ap.parse_args()
    ws = pathlib.Path(a.workspace)
    src = ws / a.sources
    rel = ws / 'data_darbar_warehouse' / 'stage2' / a.release
    repo = ws / 'datadarbar'
    ex, geo, mis, pan, wh = (rel / n for n in
                             ('extract', 'geography', 'missingness', 'panel', 'warehouse'))

    if not a.skip_tests:
        r = subprocess.run([sys.executable, '-m', 'unittest', 'discover', '-s', str(HERE),
                            '-p', 'test_*.py'], cwd=HERE)
        if r.returncode:
            sys.exit('tests failed')

    run('build_panel', ['--src', str(src),
                        '--crosswalk', str(repo / 'etl/mouza2020/mouza2020_tehsil_crosswalk.csv'),
                        '--out', str(ex)])
    run('build_crosswalk', ['--units', str(ex / 'units.csv'),
                            '--mouza', str(repo / 'etl/mouza2020/mouza2020_tehsil_crosswalk.csv'),
                            '--geo', str(repo / 'app/data/tehsils_geo.js'),
                            '--boundaries', str(ws / 'raw_data/geospatial/boundaries/'
                                                'geoBoundaries-PAK-ADM3-all/geoBoundaries-PAK-ADM3.geojson'),
                            '--cod', str(ws / 'raw_data/geospatial/boundaries/'
                                         'cod-ab-pak-2026-09-26/pak_admin_boundaries.xlsx'),
                            '--out', str(geo)])
    run('reconcile_missing', ['--src', str(src), '--out', str(mis)])
    run('build_final_panel', ['--obs', str(ex / 'observations.parquet'),
                              '--crosswalk', str(geo / 'sub_district_crosswalk.csv'),
                              '--mask', str(mis / 'missingness_mask.csv'),
                              '--mismatches', str(mis / 'value_mismatches.csv'),
                              '--unaligned', str(mis / 'unaligned_rows.csv'),
                              '--out', str(pan)])
    run('build_urban_localities', ['--src', str(src), '--out', str(pan)])
    run('build_dictionary', ['--panel', str(pan / 'panel.parquet'), '--out', str(pan)])
    run('build_warehouse', ['--panel', str(pan / 'panel.parquet'),
                            '--crosswalk', str(geo / 'sub_district_crosswalk.csv'),
                            '--dictionary', str(pan / 'indicator_dictionary.csv'),
                            '--withheld', str(geo / 'withheld.csv'),
                            '--localities', str(pan / 'urban_localities.csv'),
                            '--out', str(wh)])
    lock = build_lock([src], [repo / 'etl/mouza2020/mouza2020_tehsil_crosswalk.csv',
                              repo / 'app/data/tehsils_geo.js',
                              ws / 'raw_data/geospatial/boundaries/geoBoundaries-PAK-ADM3-all/geoBoundaries-PAK-ADM3.geojson',
                              ws / 'raw_data/geospatial/boundaries/cod-ab-pak-2026-09-26/pak_admin_boundaries.xlsx'],
                      rel / 'input_lock.json')
    man = manifest(rel, rel / 'build_manifest.json')
    (rel / 'run.json').write_text(json.dumps(dict(
        finished_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        workspace=str(ws), release=str(rel)), indent=2))
    print(f"\ninputs locked   {lock['input_count']} files, {lock['input_bytes']/1048576:.1f} MB")
    print(f"artifacts       {man['artifact_count']}")
    print(f"release id      {man['release_id']}")
    print(f"release written to {rel}")


if __name__ == '__main__':
    main()
