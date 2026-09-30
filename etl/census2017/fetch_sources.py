"""Capture the Census 2017 district table corpus.

The archive page at /censusarchive/ carries two complete renderings of the same
data, and neither is reachable by following links:

  * 5,356 Excel files — 40 tables for each of 134 districts — indexed in a
    JavaScript object (`var DATA={...}`) under `var PREFIX="..."`.
  * 135 combined per-district PDFs — indexed in a separate JSON array.

Anything that follows `href` attributes finds neither, which is how two earlier
passes over this page concluded the data did not exist. So the index is read
from the page's scripts, and the page itself is archived alongside the files as
provenance.

Paths are taken from the index, never constructed: Sindh names its files
`Table-23-BAD.xls` where every other province uses `Table23d.xls`.
"""
import argparse, datetime, hashlib, json, pathlib, re, sys, urllib.request
from concurrent.futures import ThreadPoolExecutor

ARCHIVE = "https://www.pbs.gov.pk/censusarchive/"
HOST = "https://www.pbs.gov.pk"
UA = {"User-Agent": "DataDarbar/1.0 (research; https://darbar.adaad.org)"}


def get(url, timeout=300):
    with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout) as r:
        return r.read()


def read_index(page):
    """(xls jobs, pdf jobs) from the page's own script blocks."""
    prefix = re.search(r'var PREFIX="([^"]+)"', page).group(1)
    i = page.index('var DATA=')
    data = json.loads(page[i + len('var DATA='):page.index('};', i) + 1])
    xls = []
    for prov, districts in data.items():
        for district, tables in districts:
            for tname, rel in tables:
                n = re.search(r'Table[-]?(\d+)', tname) or re.search(r'Table[-]?(\d+)', rel)
                xls.append(dict(kind='xlsx', province=prov, district=district,
                                table=(n.group(1).lstrip('0') or '0') if n else '?',
                                url=HOST + prefix + rel,
                                path=f"xlsx/{prov}/{district}/{pathlib.Path(rel).name}"))
    pdf = []
    for prov, dists in re.findall(r'"([A-Z][A-Z &]+)":\s*\[((?:[^][]|\[[^]]*\])*)\]', page):
        for name, url in re.findall(r'\{"name":\s*"([^"]+)",\s*"url":\s*"([^"]*District\d+_Combined\.pdf)"\}', dists):
            pdf.append(dict(kind='pdf', province=prov, district=name, table='combined',
                            url=url if url.startswith('http') else HOST + url,
                            path=f"pdf/{pathlib.Path(url).name}"))
    return xls, pdf


def fetch(job, out, retries=3):
    dest = out / job['path']
    if dest.exists() and dest.stat().st_size > 0:
        body = dest.read_bytes()
        return dict(job, bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                    retrieved_at_utc='(already present)')
    for attempt in range(retries):
        try:
            body = get(job['url'])
        except Exception as e:
            if attempt == retries - 1:
                return dict(job, error=f'{type(e).__name__}: {e}'[:120])
            continue
        magic = b'PK' if job['kind'] == 'xlsx' else b'%PDF'
        # .xls is the old OLE container, not a zip
        if job['kind'] == 'xlsx' and not (body.startswith(b'PK') or body[:4] == b'\xd0\xcf\x11\xe0'):
            return dict(job, error='not a spreadsheet')
        if job['kind'] == 'pdf' and not body.startswith(magic):
            return dict(job, error='not a pdf')
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(body)
        return dict(job, bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                    retrieved_at_utc=datetime.datetime.now(datetime.timezone.utc).isoformat())
    return dict(job, error='exhausted retries')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True)
    ap.add_argument('--kinds', default='xlsx')
    ap.add_argument('--workers', type=int, default=8)
    ap.add_argument('--limit', type=int, help='stop after N files, for a trial run')
    a = ap.parse_args()
    out = pathlib.Path(a.out); out.mkdir(parents=True, exist_ok=True)

    page_path = out / 'source_page_censusarchive.html'
    if page_path.exists():
        page = page_path.read_text(errors='replace')
    else:
        page = get(ARCHIVE).decode('utf-8', 'replace')
        page_path.write_text(page)
    xls, pdf = read_index(page)
    print(f"index: {len(xls)} spreadsheets, {len(pdf)} combined PDFs")

    jobs = []
    if 'xlsx' in a.kinds:
        jobs += xls
    if 'pdf' in a.kinds:
        jobs += pdf
    if a.limit:
        jobs = jobs[:a.limit]

    done = 0
    results = []
    with ThreadPoolExecutor(a.workers) as ex:
        for r in ex.map(lambda j: fetch(j, out), jobs):
            results.append(r); done += 1
            if done % 250 == 0 or done == len(jobs):
                ok = sum(1 for x in results if 'sha256' in x)
                mb = sum(x.get('bytes', 0) for x in results) / 1048576
                print(f"  {done}/{len(jobs)}  ok {ok}  {mb:.0f} MB", flush=True)

    ok = [r for r in results if 'sha256' in r]
    bad = [r for r in results if 'error' in r]
    mpath = out / 'retrieval_manifest.json'
    prior = json.loads(mpath.read_text())['files'] if mpath.exists() else []
    kinds = {r['kind'] for r in ok}
    merged = [p for p in prior if p['kind'] not in kinds] + ok
    mpath.write_text(json.dumps(dict(
        captured_utc=datetime.datetime.now(datetime.timezone.utc).isoformat(),
        source_page=ARCHIVE,
        note='Index read from the page\'s script blocks; paths taken from the index, '
             'never constructed, because Sindh uses a different filename convention.',
        files=sorted(merged, key=lambda x: (x['kind'], x['province'], x['district'], x['path'])),
        missing=bad), indent=2, sort_keys=True))
    print(f"\ncaptured {len(ok)} files, {sum(r.get('bytes',0) for r in ok)/1048576:.0f} MB")
    if bad:
        print(f"failed {len(bad)}:")
        for r in bad[:12]:
            print(f"  {r['province']}/{r['district']} table {r['table']}: {r['error']}")
    return 1 if len(bad) > len(jobs) * 0.02 else 0


if __name__ == '__main__':
    sys.exit(main())
