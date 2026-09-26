"""Check a Stage 2 release against its manifest, and against a repeat build."""
import argparse, json, pathlib, sys
from lock import sha, code_fingerprint


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--release', required=True)
    ap.add_argument('--repeat')
    a = ap.parse_args()
    rel = pathlib.Path(a.release)
    man = json.loads((rel / 'build_manifest.json').read_text())
    bad = []
    for f in man['artifacts']:
        p = rel / f['path']
        if not p.exists():
            bad.append(f"missing: {f['path']}")
        elif sha(p) != f['sha256']:
            bad.append(f"changed: {f['path']}")
    print(f"artifacts checked   {len(man['artifacts'])}")
    print(f"code fingerprint    {'matches' if code_fingerprint() == man['code_fingerprint'] else 'DIFFERS'}")
    if a.repeat:
        rep = pathlib.Path(a.repeat)
        diff = [f['path'] for f in man['artifacts']
                if not (rep / f['path']).exists() or sha(rep / f['path']) != f['sha256']]
        print(f"repeat build        {len(man['artifacts']) - len(diff)}/{len(man['artifacts'])} byte-identical")
        bad += [f"repeat differs: {d}" for d in diff]
    for b in bad:
        print("  " + b)
    print("\nRELEASE VERIFIED" if not bad else "\nVERIFICATION FAILED")
    sys.exit(1 if bad else 0)


if __name__ == '__main__':
    main()
