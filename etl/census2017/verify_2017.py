"""Check that a 2017 build is reproducible and matches its recorded inputs.

The 2023 release is rebuilt in a fresh directory and compared artifact by
artifact; 2017 had no equivalent, so nothing said whether a second run would
produce the same numbers. Non-determinism has bitten this project before - DuckDB
returning rows in a different order, `any_value()` picking a different row - and
it is invisible unless something looks.

Three things are checked:

  REPEAT. Every artifact of two independent runs must be byte-identical. The
  Parquet writer is deterministic only if the query that feeds it is, so this is
  really a test of the ORDER BY clauses and the single-threaded settings.

  INPUTS. The capture's own lock is re-verified, so a build cannot silently have
  read a different set of source files from the one it claims.

  CODE. The fingerprint of the modules that produced the build is recorded, so a
  later run that differs can be attributed to a code change rather than guessed
  at.
"""
import argparse, hashlib, json, os, pathlib, sys


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def artifacts(root):
    """{relative path: sha256} for every file under root, except the logs."""
    out = {}
    for p in sorted(pathlib.Path(root).rglob('*')):
        if p.is_file():
            out[str(p.relative_to(root))] = sha(p)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--build', required=True)
    ap.add_argument('--repeat', required=True)
    ap.add_argument('--capture', required=True)
    a = ap.parse_args()

    one, two = artifacts(a.build), artifacts(a.repeat)
    only_one = sorted(set(one) - set(two))
    only_two = sorted(set(two) - set(one))
    differ = sorted(k for k in set(one) & set(two) if one[k] != two[k])
    same = len(set(one) & set(two)) - len(differ)

    print(f'artifacts: {len(one)} in build, {len(two)} in repeat')
    print(f'  byte-identical : {same}')
    print(f'  differing      : {len(differ)}')
    for k in differ:
        print(f'      {k}')
    for k in only_one:
        print(f'      only in build:  {k}')
    for k in only_two:
        print(f'      only in repeat: {k}')

    lock_path = os.path.join(a.capture, 'acquisition_lock.json')
    lock = json.load(open(lock_path))
    man = json.load(open(os.path.join(a.capture, 'retrieval_manifest.json')))
    h = hashlib.sha256()
    for f in sorted(man['files'], key=lambda f: f['path']):
        h.update(f['path'].encode())
        h.update(f['sha256'].encode())
    capture_ok = h.hexdigest()[:16] == lock['capture_id']
    print(f"\ninput capture {lock['capture_id']}: "
          f"{'matches the lock' if capture_ok else 'DOES NOT MATCH THE LOCK'}")

    here = pathlib.Path(__file__).resolve().parent
    ch = hashlib.sha256()
    for name in sorted(p.name for p in here.glob('*.py')):
        ch.update(name.encode())
        ch.update(sha(here / name).encode())
    print(f'code fingerprint: {ch.hexdigest()[:16]}')

    ok = not differ and not only_one and not only_two and capture_ok
    print(f"\nreproducible: {ok}")
    return 0 if ok else 1


if __name__ == '__main__':
    raise SystemExit(main())
