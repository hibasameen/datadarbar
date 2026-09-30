"""Capture the Stage 2 source corpus: Option A tables, both PBS releases.

Writes a dated snapshot with a retrieval manifest (URL, timestamp, bytes,
SHA-256) in the same shape as the Stage 1 capture. Never called by a build.
"""
import argparse, datetime, hashlib, json, pathlib, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor

XLSX = "https://www.pbs.gov.pk/wp-content/uploads/2020/07"
PDF = "https://www.pbs.gov.pk/wp-content/uploads/census_tables/tables"
# All 28 cross-tabulated tables PBS publishes for Census 2023. Tables 27-30 do
# not exist; 31-35 are the locality tables and are a separate stage.
TABLES = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13",
          "13a", "13b", "14", "15", "16", "17", "18", "19", "20", "21", "22",
          "23", "24", "25", "26"]
REGIONS = ["kp", "punjab", "sindh", "balochistan"]
UA = {"User-Agent": "DataDarbar/1.0 (research; https://darbar.adaad.org)"}

# PBS filenames are irregular: Islamabad drops the _districts suffix, and a few
# tables carry a typo. Overrides are keyed (table, region).
OVERRIDE = {("4", "punjab"): "table_4_punajb_districts",
            ("12", "balochistan"): "table_12_balochistan_district",
            ("18", "islamabad"): "table_18_islamabad_province"}


def names(table, region, kind):
    if (table, region) in OVERRIDE:
        yield OVERRIDE[(table, region)]
    if region == "islamabad":
        yield f"table_{table}_islamabad"
        yield f"table_{table}_islamabad_districts"
        yield f"table_{table}_islamabad_district"
    else:
        yield f"table_{table}_{region}_districts"
        yield f"table_{table}_{region}_district"


def fetch(job):
    table, region, kind, out = job
    base, ext = (XLSX, "xlsx") if kind == "xlsx" else (PDF, "pdf")
    for stem in names(table, region, kind):
        url = f"{base}/{stem}.{ext}"
        try:
            req = urllib.request.Request(url, headers=UA)
            with urllib.request.urlopen(req, timeout=300) as r:
                body = r.read()
                ctype = r.headers.get("Content-Type", "")
        except Exception as e:
            continue
        if ext == "xlsx" and not body.startswith(b"PK"):
            continue
        if ext == "pdf" and not body.startswith(b"%PDF"):
            continue
        path = out / kind / f"table_{table}_{region}.{ext}"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
        return dict(table=table, region=region, kind=kind,
                    path=str(path.relative_to(out)), url=url, bytes=len(body),
                    content_type=ctype, sha256=hashlib.sha256(body).hexdigest(),
                    retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
    return dict(table=table, region=region, kind=kind, error="not found",
                tried=[f"{base}/{s}.{ext}" for s in names(table, region, kind)])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--kinds", default="xlsx,pdf")
    a = ap.parse_args()
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    regions = REGIONS + ["islamabad"]
    jobs = [(t, r, k, out) for k in a.kinds.split(",") for t in TABLES for r in regions]
    with ThreadPoolExecutor(8) as ex:
        res = list(ex.map(fetch, jobs))
    ok = [r for r in res if "sha256" in r]
    bad = [r for r in res if "error" in r]
    # Merge with any existing manifest so capturing xlsx and pdf in separate
    # runs does not discard the earlier run's entries.
    mpath = out / "retrieval_manifest.json"
    prior = json.loads(mpath.read_text())["files"] if mpath.exists() else []
    kinds = {r["kind"] for r in ok}
    ok = [r for r in prior if r["kind"] not in kinds] + ok
    manifest = dict(captured_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    source_pages=["https://www.pbs.gov.pk/result-excel/",
                                  "https://www.pbs.gov.pk/census/"],
                    tables=TABLES, files=sorted(ok, key=lambda x: (x["kind"], x["table"], x["region"])),
                    missing=bad)
    mpath.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"captured {len(ok)} files, {sum(r['bytes'] for r in ok)/1048576:.1f} MB")
    for r in bad:
        print(f"  MISSING  table {r['table']} {r['region']} ({r['kind']})")


if __name__ == "__main__":
    main()
