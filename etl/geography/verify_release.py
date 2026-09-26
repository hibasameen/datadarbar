#!/usr/bin/env python3
"""Verify hashes and optionally byte equality of independent geography builds."""
import argparse
import json
from pathlib import Path

from build_geography import HERE, digest, require


def verify(path):
    manifest = json.loads((path / 'build_manifest.json').read_text())
    expected = set(manifest['outputs']) | {'build_manifest.json', 'run.json'}
    require({p.name for p in path.iterdir()} == expected, 'Unexpected or missing release files')
    for name, record in manifest['outputs'].items():
        require((path / name).stat().st_size == record['bytes'] and digest(path / name) == record['sha256'],
                f'Artifact hash mismatch: {name}')
    for name, sha in manifest['code'].items():
        require(digest(HERE / name) == sha, f'Current code differs from release: {name}')
    return manifest


def compare(first, second):
    a, b = verify(first), verify(second)
    require(a['release_id'] == b['release_id'], 'Release identities differ (inputs/code/runtime)')
    names = list(a['outputs']) + ['build_manifest.json']
    for name in names:
        require((first / name).read_bytes() == (second / name).read_bytes(), f'Non-reproducible artifact: {name}')
    return {'release_id': a['release_id'], 'byte_identical_files': len(names), 'excluded': ['run.json']}


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--release', type=Path, required=True)
    p.add_argument('--repeat', type=Path)
    a = p.parse_args()
    print(json.dumps(compare(a.release, a.repeat) if a.repeat else {'verified': verify(a.release)['release_id']}, indent=2))
