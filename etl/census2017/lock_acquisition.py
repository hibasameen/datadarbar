"""Lock the Census 2017 acquisition.

The manifest already carries a sha256 per file, recorded at download time. This
turns that into a single checkable identity for the capture as a whole, together
with a fingerprint of the code that fetched it and the runtime that ran it, so a
later extract can prove it read exactly this capture and refuse if it did not.
"""
import argparse, collections, hashlib, json, os, pathlib, platform, sys

HERE = pathlib.Path(__file__).resolve().parent


def sha_file(p):
    h = hashlib.sha256()
    with open(p, 'rb') as fh:
        for c in iter(lambda: fh.read(1 << 20), b''):
            h.update(c)
    return h.hexdigest()


def code_fingerprint():
    h = hashlib.sha256()
    for name in sorted(p.name for p in HERE.glob('*.py')):
        h.update(name.encode())
        h.update(sha_file(HERE / name).encode())
    return h.hexdigest()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', required=True)
    a = ap.parse_args()
    man = json.load(open(os.path.join(a.dir, 'retrieval_manifest.json')))
    files = man['files']

    # verify on disk before locking: a lock over files we have not checked is worthless
    bad = []
    for f in files:
        p = os.path.join(a.dir, f['path'])
        if not os.path.exists(p):
            bad.append((f['path'], 'absent'))
        elif os.path.getsize(p) != f['bytes']:
            bad.append((f['path'], f"size {os.path.getsize(p)} != {f['bytes']}"))
    if bad:
        print(f'refusing to lock: {len(bad)} files do not match the manifest', file=sys.stderr)
        for p, e in bad[:10]:
            print('  ', p, e, file=sys.stderr)
        return 1

    # one identity for the whole capture, over (path, sha256) in sorted order
    h = hashlib.sha256()
    for f in sorted(files, key=lambda f: f['path']):
        h.update(f['path'].encode())
        h.update(f['sha256'].encode())
    capture_id = h.hexdigest()[:16]

    kinds = collections.Counter(f['kind'] for f in files)
    byprov = collections.Counter(f.get('province') or f.get('area') for f in files)
    lock = dict(
        capture_id=capture_id,
        captured_utc=man['captured_utc'],
        source_page=man['source_page'],
        files=len(files),
        bytes=sum(f['bytes'] for f in files),
        by_kind=dict(sorted(kinds.items())),
        by_area=dict(sorted(byprov.items())),
        # Counted per rendering, deliberately: the two renderings spell the same
        # district differently ("ABBOTTABAD" in the spreadsheets, "ABBOTTABAD
        # DISTRICT" in the PDFs, "Rajan Pur" vs "RAJANPUR DISTRICT"), so a single
        # distinct count over both would read 269 for 135 districts. Reconciling
        # the two spellings is the geography register's job, not the lock's.
        districts_pdf=len({f['district'] for f in files
                           if f.get('district') and f['kind'] == 'pdf'}),
        districts_xls=len({f['district'] for f in files
                           if f.get('district') and f['kind'] == 'xlsx'}),
        tables=len({f['table'] for f in files if f.get('table') not in (None, 'combined')}),
        code_fingerprint=code_fingerprint(),
        runtime=dict(python=sys.version.split()[0], platform=platform.platform(),
                     xlrd=__import__('xlrd').__version__),
    )
    out = os.path.join(a.dir, 'acquisition_lock.json')
    json.dump(lock, open(out, 'w'), indent=1, sort_keys=True)
    print(json.dumps(lock, indent=1, sort_keys=True))
    print(f'\nwrote {out}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
