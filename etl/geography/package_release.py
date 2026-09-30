#!/usr/bin/env python3
"""Package the locked inputs and code for a network-free fresh-folder rebuild."""
import argparse
import gzip
import io
import json
import tarfile
from pathlib import Path

from build_geography import HERE, WORKSPACE, digest, require, verify_inputs
from verify_release import verify


def package(root, release, output):
    require(not output.exists(), 'Package already exists')
    manifest = verify(release)
    verify_inputs(root, manifest['inputs'])
    files = {f['path']: root / f['path'] for f in manifest['inputs']['files']}
    files.update({'datadarbar/etl/geography/' + name: HERE / name for name in manifest['code']})
    files.update({'reference_release/' + p.name: p for p in release.iterdir()})
    readme = '''# Data Darbar geography reproduction package

This package rebuilds a tabular 2017/2023 comparison panel from the immutable
census source release and locked official geography evidence. It does not
rebuild the earlier census extraction; that has its own reproduction package.

Requirements: Python 3.11.5, DuckDB 1.5.5, Poppler pdftotext 21.11.0 on PATH.
The build itself is offline. From the extracted package root:

python3 -m unittest discover -s datadarbar/etl/geography -p 'test_*.py'
python3 datadarbar/etl/geography/build_geography.py --out rebuilt
python3 datadarbar/etl/geography/verify_release.py --release reference_release --repeat rebuilt

All outputs except run.json must be byte identical. Other runtime versions
receive a different release identity and require separate comparison/review.
All source units enter 127 common areas. Jhang + Toba Tek Singh and Kachhi +
Nasirabad enter as joint areas. Individual full-indicator historical allocations
for those four districts remain unsupported; source discrepancies are preserved.
The locality correspondences, subdistrict rows and population bridge document
this resolution. No source cells are corrected and no population weights guessed.
Geographic support concerns published statistical units, not polygon overlays
or legal effective dates. Education uses each source table's age-5+ universe.
'''
    with output.open('xb') as target, gzip.GzipFile(filename='', mode='wb', fileobj=target, mtime=0) as compressed:
        with tarfile.open(fileobj=compressed, mode='w') as archive:
            entries = {name: path.read_bytes() for name, path in files.items()}
            entries['README.md'] = readme.encode()
            for name, content in sorted(entries.items()):
                info = tarfile.TarInfo(name)
                info.size = len(content)
                info.mode = 0o644
                archive.addfile(info, io.BytesIO(content))
    return {'path': str(output), 'files': len(entries), 'bytes': output.stat().st_size,
            'sha256': digest(output), 'release_id': manifest['release_id']}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--workspace', type=Path, default=WORKSPACE)
    p.add_argument('--release', type=Path, required=True)
    p.add_argument('--out', type=Path, required=True)
    a = p.parse_args()
    print(json.dumps(package(a.workspace, a.release, a.out), indent=2))
