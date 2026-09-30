"""Input lock and release manifest for a Stage 2 build.

Records the identity of every source file, every line of build code and the
runtime, so a release can be shown to have been produced from exactly those
inputs — and so a later build that quietly reads something different fails
instead of publishing.
"""
import hashlib, json, pathlib, platform, subprocess, sys

HERE = pathlib.Path(__file__).resolve().parent
CODE = sorted(p.name for p in HERE.glob('*.py'))


def sha(path):
    h = hashlib.sha256()
    with open(path, 'rb') as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b''):
            h.update(chunk)
    return h.hexdigest()


def code_fingerprint():
    h = hashlib.sha256()
    for name in CODE:
        h.update(name.encode())
        h.update(sha(HERE / name).encode())
    return h.hexdigest()


def runtime():
    try:
        pdf = subprocess.run(['pdftotext', '-v'], capture_output=True, text=True).stderr.strip().splitlines()[0]
    except Exception:
        pdf = 'unavailable'
    import duckdb, openpyxl
    return dict(python=sys.version.split()[0], platform=platform.platform(),
                duckdb=duckdb.__version__, openpyxl=openpyxl.__version__, poppler=pdf)


def build_lock(sources, extra, out):
    """Hash every input; write input_lock.json next to the release."""
    files = []
    for root in sources:
        root = pathlib.Path(root)
        for p in sorted(root.rglob('*')):
            if p.is_file() and not p.name.startswith('.'):
                files.append(dict(path=str(p.relative_to(root.parent)),
                                  bytes=p.stat().st_size, sha256=sha(p)))
    for p in extra:
        p = pathlib.Path(p)
        if p.exists():
            files.append(dict(path=p.name, bytes=p.stat().st_size, sha256=sha(p)))
    lock = dict(inputs=files, input_count=len(files),
                input_bytes=sum(f['bytes'] for f in files),
                code=[dict(file=n, sha256=sha(HERE / n)) for n in CODE],
                code_fingerprint=code_fingerprint(), runtime=runtime())
    pathlib.Path(out).write_text(json.dumps(lock, indent=2, sort_keys=True))
    return lock


def manifest(release_dir, out):
    """Hash every produced artifact."""
    release_dir = pathlib.Path(release_dir)
    files = []
    for p in sorted(release_dir.rglob('*')):
        if p.is_file() and p.name not in ('build_manifest.json', 'run.json'):
            files.append(dict(path=str(p.relative_to(release_dir)),
                              bytes=p.stat().st_size, sha256=sha(p)))
    man = dict(artifacts=files, artifact_count=len(files),
               code_fingerprint=code_fingerprint(),
               release_id='stage2-' + hashlib.sha256(
                   ''.join(f['sha256'] for f in files).encode()).hexdigest()[:16])
    pathlib.Path(out).write_text(json.dumps(man, indent=2, sort_keys=True))
    return man
