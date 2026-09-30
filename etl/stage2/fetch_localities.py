"""Capture tables 31-35, the individual-locality tables, with a manifest."""
import argparse, datetime, hashlib, json, pathlib, urllib.request
from concurrent.futures import ThreadPoolExecutor

XLSX = "https://www.pbs.gov.pk/wp-content/uploads/2020/07"
PDF = "https://www.pbs.gov.pk/wp-content/uploads/census_tables/tables"
TABLES = ["31", "32", "33", "34", "35"]
REGIONS = ["kp", "punjab", "sindh", "balochistan", "islamabad"]
UA = {"User-Agent": "DataDarbar/1.0 (research; https://darbar.adaad.org)"}


def names(t, reg):
    if reg == "islamabad":
        yield f"table_{t}_islamabad"
        yield f"table_{t}_islamabad_districts"
        yield f"table_{t}_islamabad_district"
        yield f"table_{t}_islamabad_district_0"
    else:
        yield f"table_{t}_{reg}_districts"
        yield f"table_{t}_{reg}_district"
        yield f"Table_{t}_{reg}_district"


def fetch(job):
    t, reg, kind, out = job
    base, ext, magic = (XLSX, "xlsx", b"PK") if kind == "xlsx" else (PDF, "pdf", b"%PDF")
    for stem in names(t, reg):
        url = f"{base}/{stem}.{ext}"
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=600) as r:
                body = r.read()
        except Exception:
            continue
        if not body.startswith(magic):
            continue
        p = out / kind / f"table_{t}_{reg}.{ext}"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(body)
        return dict(table=t, region=reg, kind=kind, path=str(p.relative_to(out)), url=url,
                    bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                    retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
    return dict(table=t, region=reg, kind=kind, error="not found")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True)
    ap.add_argument('--kinds', default='xlsx')
    a = ap.parse_args()
    out = pathlib.Path(a.out); out.mkdir(parents=True, exist_ok=True)
    jobs = [(t, r, k, out) for k in a.kinds.split(',') for t in TABLES for r in REGIONS]
    res = list(ThreadPoolExecutor(6).map(fetch, jobs))
    ok = [r for r in res if 'sha256' in r]
    mpath = out / 'retrieval_manifest.json'
    prior = json.loads(mpath.read_text())['files'] if mpath.exists() else []
    kinds = {r['kind'] for r in ok}
    ok = [r for r in prior if r['kind'] not in kinds] + ok
    mpath.write_text(json.dumps(dict(
        captured_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        source_pages=["https://www.pbs.gov.pk/result-excel/", "https://www.pbs.gov.pk/census/"],
        tables=TABLES, files=sorted(ok, key=lambda x: (x['kind'], x['table'], x['region'])),
        missing=[r for r in res if 'error' in r]), indent=2, sort_keys=True))
    print(f"captured {len(ok)} files, {sum(r['bytes'] for r in ok)/1048576:.1f} MB")
    for r in res:
        if 'error' in r:
            print(f"  MISSING  table {r['table']} {r['region']} ({r['kind']})")


if __name__ == '__main__':
    main()
