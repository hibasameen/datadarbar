"""Fetch the 2017 tables listed as plain HTML anchors, not in the script index.

`fetch_sources.py` reads the index out of the census-archive page's script
blocks (`var PREFIX`, `var DATA`), which is where the 5,356 per-district
spreadsheets and 135 combined PDFs live. A second set is published only as
ordinary <a href> markup in the page's tables, and is therefore absent from that
index:

    Pakistan      36 tables   TableNn.xls
    Punjab        36          TableNp-N.xls / TableNp.xls
    Sindh         36
    KPK           36
    Balochistan   36
    FATA          36
    Islamabad     39          TableNd.xls

That is 255 files, and they matter more than their count suggests: the national
and provincial tables are independently published totals, which is the only kind
of check that has ever caught a real error in this project. Islamabad has no
per-district spreadsheets in the script index at all, so these 39 are its only
spreadsheet rendering.

The province is carried in the ANCHOR TEXT, not the filename - `Table01p-1.xls`
and `Table01p-2.xls` are two different provinces, WordPress having appended its
duplicate-upload suffix. So the label comes from the anchor and is then checked
against the workbook's own unit row, never inferred from the path. The same rule
that Ghotki's `-DAD`-suffixed files and 2023's table 6 both teach.
"""
import argparse, hashlib, html, json, os, re, sys, time, urllib.request

UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36'

# anchor text -> (level, label, subdir)
AREA = {
    'pakistan':    ('national', 'PAKISTAN',    'pakistan'),
    'punjab':      ('province', 'PUNJAB',      'punjab'),
    'sindh':       ('province', 'SINDH',       'sindh'),
    'kpk':         ('province', 'KHYBER PAKHTUNKHWA', 'kp'),
    'balochistan': ('province', 'BALOCHISTAN', 'balochistan'),
    'fata':        ('province', 'FATA',        'fata'),
    'islamabad':   ('district', 'ISLAMABAD',   'islamabad'),
}

MAGIC = (b'PK', b'\xd0\xcf\x11\xe0')   # xlsx (zip) or legacy xls (OLE2)


def anchors(page):
    """Spreadsheet anchors, as (url, level, label, subdir, table)."""
    out, seen = [], set()
    for u, t in re.findall(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', page, re.S | re.I):
        if not re.search(r'\.xlsx?($|\?)', u, re.I):
            continue
        u = html.unescape(u)
        text = re.sub(r'<[^>]+>', '', t).strip()
        key = text.split('(')[0].strip().lower()
        if key not in AREA or u in seen:
            continue
        seen.add(u)
        level, label, sub = AREA[key]
        # Table number from the basename: Table<NN><suffix>[-<dup>].xls
        m = re.match(r'Table[\s\-_]*(\d+)', os.path.basename(u), re.I)
        out.append(dict(url=u, level=level, area=label, subdir=sub,
                        table=m.group(1).lstrip('0') or '0' if m else '?'))
    return out


def get(url, tries=4):
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={'User-Agent': UA})
            with urllib.request.urlopen(req, timeout=90) as r:
                return r.read()
        except Exception as e:
            if i == tries - 1:
                raise
            time.sleep(2 * (i + 1))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', required=True, help='the dated capture directory')
    a = ap.parse_args()

    page_path = os.path.join(a.dir, 'source_page_censusarchive.html')
    page = open(page_path, encoding='utf-8', errors='replace').read()
    items = anchors(page)
    print(f'index: {len(items)} anchor-listed spreadsheets', flush=True)
    by = {}
    for it in items:
        by[it['area']] = by.get(it['area'], 0) + 1
    print('  ' + ', '.join(f'{k} {v}' for k, v in sorted(by.items())), flush=True)

    mpath = os.path.join(a.dir, 'retrieval_manifest.json')
    man = json.load(open(mpath))
    have = {f['url'] for f in man['files']}

    files, failed, done = [], [], 0
    for it in items:
        rel = os.path.join('xlsx_area', it['subdir'], os.path.basename(it['url']))
        dest = os.path.join(a.dir, rel)
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if it['url'] in have:
            done += 1
            continue
        if os.path.exists(dest) and os.path.getsize(dest) > 0:
            body = open(dest, 'rb').read()
        else:
            try:
                body = get(it['url'])
            except Exception as e:
                failed.append(dict(url=it['url'], error=f'{type(e).__name__}: {e}'))
                continue
            if not body.startswith(MAGIC):
                failed.append(dict(url=it['url'], error=f'not a spreadsheet: starts {body[:8]!r}'))
                continue
            open(dest, 'wb').write(body)
        files.append(dict(url=it['url'], path=rel, kind='xls_area', level=it['level'],
                          area=it['area'], table=it['table'], bytes=len(body),
                          sha256=hashlib.sha256(body).hexdigest(),
                          retrieved_at_utc=__import__('datetime').datetime.now(
                              __import__('datetime').timezone.utc).isoformat()))
        done += 1
        if done % 25 == 0 or done == len(items):
            print(f'  {done}/{len(items)}  ok {len(files)}  '
                  f'{sum(f["bytes"] for f in files)/1e6:.0f} MB', flush=True)

    man['files'].extend(files)
    man.setdefault('anchor_note', (
        'The 255 national/provincial/Islamabad tables are published only as HTML '
        'anchors, not in the page script index; area label comes from the anchor '
        'text because the filename does not carry it.'))
    if failed:
        man.setdefault('failed', []).extend(failed)
    json.dump(man, open(mpath, 'w'), indent=1, sort_keys=True)
    print(f'\ncaptured {len(files)} files, {sum(f["bytes"] for f in files)/1e6:.1f} MB; '
          f'{len(failed)} failed', flush=True)
    for f in failed[:10]:
        print('  FAIL', f['url'], f['error'], flush=True)


if __name__ == '__main__':
    main()
