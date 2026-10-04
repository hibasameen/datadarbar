"""Capture every 1998 census publication PBS still serves.

The Census Archive page (pbs.gov.pk/censusarchive/) links the 1998 material
alongside 2017's. What is online is the summary layer: District at a Glance for
each district, and Area & Population of Administrative Units, which carries
every district and sub-division at all five censuses from 1951 to 1998. The District Census Reports themselves are
sold in print and are not online (the archive links only their price list).

Links are read from the page, never constructed, and the page is kept with the
capture so the list can be checked against it.

    python3 etl/census1998/fetch.py --out ../raw_data/pbs/census1998_sources/<date>
"""
import argparse, datetime, hashlib, html, json, pathlib, re, time, urllib.request

PAGE = 'https://www.pbs.gov.pk/censusarchive/'
UA = {'User-Agent': 'Mozilla/5.0 (Data Darbar; data@adaad.org)'}
# The NCR/PCR/RCR links on the same page are the 2017 census reports (their
# own title pages say Census-2017), filed beside the 1998 material - so they are
# deliberately not matched here.
KEEP = re.compile(r'District at (a )?glance|City District Karachi at a glance|'
                  r'Area & Population of Administrative Units \(1998\)', re.I)


def get(url):
    # PBS's server sometimes answers a PDF request with an HTML "403 Forbidden"
    # page and a 200 status - Charsadda's came back that way once - so a PDF
    # is only accepted if it starts like one.
    for k in range(5):
        try:
            with urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=120) as r:
                body = r.read()
            if url.lower().endswith('.pdf') and not body.startswith(b'%PDF'):
                raise IOError(f'not a PDF: {body[:60]!r}')
            return body
        except Exception:
            if k == 4:
                raise
            time.sleep(5 * (k + 1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True)
    out = pathlib.Path(ap.parse_args().out)
    out.mkdir(parents=True, exist_ok=True)
    page = get(PAGE)
    (out / 'source_page_censusarchive.html').write_bytes(page)
    links = re.findall(r'<a[^>]+href="([^"]+\.pdf)"[^>]*>(.*?)</a>', page.decode('utf-8', 'replace'), re.S)
    seen, files, missing = set(), [], []
    for href, label in links:
        label = re.sub(r'\s+', ' ', html.unescape(re.sub(r'<[^>]+>', '', label))).strip()
        if not KEEP.search(label) or href in seen:
            continue
        seen.add(href)
        kind = ('district_at_a_glance' if 'glance' in label.lower()
                else 'administrative_units' if 'Administrative' in label else 'census_report')
        name = href.rsplit('/', 1)[1]
        try:
            body = get(href)
        except Exception as e:
            # Recorded, not skipped silently: Charsadda's summary is linked but
            # the server refuses the file itself (an HTML 403, every time).
            missing.append({'label': label, 'url': href, 'error': str(e)[:200]})
            print(f'  MISSING {name}: {str(e)[:70]}')
            continue
        p = out / kind / name
        p.parent.mkdir(exist_ok=True)
        p.write_bytes(body)
        files.append({'label': label, 'kind': kind, 'url': href, 'path': f'{kind}/{name}',
                      'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest(),
                      'retrieved_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat()})
        print(f'  {kind:<22} {name:<34} {len(body):>10,}')
    (out / 'retrieval_manifest.json').write_text(json.dumps({
        'page': PAGE, 'captured_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'note': 'Links read from the Census Archive page. District Census Reports are print-only.',
        'files': files, 'missing': missing}, indent=1))
    print(f'{len(files)} files, {len(missing)} unavailable')


if __name__ == '__main__':
    main()
