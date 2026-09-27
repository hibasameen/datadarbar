"""Build the Places tables end to end: districts, tehsils, values and index.

    python3 etl/places/build_all.py
"""
import pathlib, subprocess, sys, tempfile

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parent.parent
APP = REPO / 'app'
RAW = REPO.parent / 'raw_data'
BRIDGE = (RAW / 'pbs_insight_explorer' / 'economic_2026-09-27' / 'derived'
          / 'census2023_units_to_pbs_geojson.csv')
PBS_T = (RAW / 'geospatial' / 'boundaries' / 'pbs-census2023-2026-09-27'
         / 'pbs_tehsils_2023.geojson')


def run(script, *args):
    print(f'\n── {script} ' + '─' * (54 - len(script)))
    r = subprocess.run([sys.executable, str(HERE / script), *map(str, args)],
                       cwd=REPO)
    if r.returncode:
        sys.exit(f'{script} failed')


def main():
    for p in (BRIDGE, PBS_T):
        if not p.exists():
            sys.exit(f'missing input: {p}')
    with tempfile.TemporaryDirectory() as tmp:
        d = pathlib.Path(tmp) / 'districts.parquet'
        t = pathlib.Path(tmp) / 'tehsils.parquet'
        run('build_place_indicators.py', '--src', APP / 'data' / 'warehouse',
            '--geo', APP / 'data' / 'districts_2023_geo.js', '--out', d)
        run('build_place_tehsils.py', '--src', APP / 'data' / 'warehouse',
            '--bridge', BRIDGE, '--pbs', PBS_T, '--out', t)
        run('build_place_index.py', '--districts', d, '--tehsils', t,
            '--census-index', APP / 'data' / 'warehouse' / 'census_series_index.parquet',
            '--pbs', PBS_T,
            '--out-values', APP / 'data' / 'warehouse' / 'place_indicators.parquet',
            '--out-index', APP / 'data' / 'warehouse' / 'place_indicator_index.parquet')


if __name__ == '__main__':
    main()
