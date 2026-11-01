"""Monthly trade from PBS's rebuilt trade portal, "Pakistan Trade Pulse".

trade.data.gov.pk was rebuilt in October 2026. The three POST endpoints the
2026-09-27 pull read (/Data/GetBoth, /Data/GetByGroup, /Data/GetByCountry) now
return 404. The public page /Home/Index carries the reference data inline as
window.TRADE_META (first and last month loaded, the month the dollar series
starts, every partner with its ISO codes and continent), and the dashboard
draws from one GET endpoint:

    /api/agg?flow=B&from=YYYYMM&to=YYYYMM&by=month|fy|country|total

Each value in a row is a four-array, [US dollars, rupees, SDR, quantity
index], for exports (x) and imports (m), in units rather than millions. The
US-dollar and rupee figures for January 2026 match the old ED/ER fields to the
unit.

WHY THIS PULL REPLACES THE LAST ONE RATHER THAN EXTENDING IT. The 2026-09-27
pull had July to September 2025 roughly doubled on every endpoint - July 2025
exports at 5,373.6 US dollars million where PBS now serves 2,683.1 - and no
cross-check between endpoints could catch it, because they were all drawn
from the same defective table. The rebuilt portal serves the corrected months,
adds June (the old portal's month list had none) and runs on to August 2026.

WHAT IT CANNOT GIVE. There is no public commodity-group view: by=sitc returns
400 and by=section 403 without signing in, and this pull does not sign in. The
group table is therefore carried forward from the 2026-09-27 pull unchanged,
and trade_reconciliation compares against it only where the totals under it
have not been revised since (see the note above FLOWS).

WHAT IS DERIVED, AND HOW. Everything is built from months. A quarter is its
months added, a year (period FY_from_quarters, the name kept for the warehouse)
is its quarters added, and months_in_totals says how many months are actually
in it - the old file said 3 for every quarter, including the two-month Q4 of
an unfinished year. The portal's own quarter and year rows are not used: on
the old portal they overshot the months by 1 to 4 per cent on exports.

Two old bugs fixed on the way: 'NA' is North America as a continent and
Namibia as an ISO2 code, and both are written as the string rather than read
as a missing value; and the dollar columns are empty only where the portal has
no dollar series (before META.usdFrom), not zero for a decade.

Usage:
  pull_trade_portal.py --out <raw dir> --old <previous pull dir>
  pull_trade_portal.py --out <raw dir> --old <previous pull dir> --derive-only
"""
import argparse, csv, datetime as dt, hashlib, http.cookiejar, json, pathlib, shutil
import sys, time, urllib.error, urllib.parse, urllib.request

import duckdb

BASE = 'https://trade.data.gov.pk/'
UA = ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 '
      '(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36')
PAUSE = 1.0  # seconds between requests; the portal is a public service

# The frozen group table is a different vintage from these totals. PBS has
# since revised the months under it - by 0.1 to 10 per cent, and by half in
# the periods the old portal doubled (exports in 2007-08 and 2009-10, imports
# in 2011-12 and 2012-13, both in July-September 2025) - and its dollars are
# zero before 2013-14. A gap against a revised total measures the revision,
# not the groups' coverage. So a group column is compared only in a period
# whose every month has the same value for that column in both pulls, the
# comparison the old file made; everywhere else it is left empty. Only one
# fiscal-year cell survives (2013-14 imports in rupees): PBS revised at least
# one month of every other year. January-May, Q3 and Q4 mostly survive.

FLOWS = (('imports', 'm'), ('exports', 'x'))
USD, PKR = 0, 1

TOTALS_COLS = ['fy', 'year', 'month', 'imports_pkr', 'imports_usd',
               'exports_pkr', 'exports_usd']
COUNTRY_COLS = ['fy', 'period', 'country', 'iso2', 'iso3', 'continent',
                'pbs_country_code', 'imports_pkr', 'imports_usd',
                'exports_pkr', 'exports_usd']
VALUE_COLS = ['imports_pkr', 'imports_usd', 'exports_pkr', 'exports_usd']


# ── fetching ─────────────────────────────────────────────────────────────────

class Portal:
    def __init__(self, out, pause=PAUSE):
        self.out = out
        self.pause = pause
        self.last = 0.0
        self.log = []          # every file written: url, retrieved_at, sha256
        self.unavailable = []  # requests that failed or need sign-in
        jar = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(jar))

    def get(self, path, accept='application/json', tries=4):
        url = BASE + path
        for attempt in range(tries):
            wait = self.pause - (time.time() - self.last)
            if wait > 0:
                time.sleep(wait)
            req = urllib.request.Request(url, headers={
                'User-Agent': UA, 'Accept': accept,
                'Accept-Language': 'en-GB,en;q=0.9',
                'Referer': BASE + 'Home/Index'})
            self.last = time.time()
            try:
                with self.opener.open(req, timeout=60) as r:
                    return r.status, r.read(), url
            except urllib.error.HTTPError as e:
                body = e.read()
                if e.code in (429, 500, 502, 503, 504) and attempt < tries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return e.code, body, url
            except (urllib.error.URLError, TimeoutError) as e:
                if attempt < tries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                return None, str(e).encode(), url

    def save(self, rel, body, url):
        p = self.out / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(body)
        self.log.append({
            'file': rel, 'url': url,
            'retrieved_at': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'),
            'sha256': hashlib.sha256(body).hexdigest(), 'bytes': len(body)})


def extract_meta(html):
    key = 'window.TRADE_META'
    i = html.index(key)
    i = html.index('=', i) + 1
    while html[i].isspace():
        i += 1
    meta, _ = json.JSONDecoder().raw_decode(html, i)
    return meta


def months_between(a, b):
    y, m = divmod(a, 100)
    while y * 100 + m <= b:
        yield y * 100 + m
        m += 1
        if m > 12:
            y, m = y + 1, 1


def fetch(out, pause):
    P = Portal(out, pause)

    st, body, url = P.get('Home/Index', accept='text/html')
    if st != 200:
        sys.exit(f'Home/Index returned {st}; stopping')
    html = body.decode('utf-8')
    if 'window.TRADE_META' not in html:
        sys.exit('Home/Index has no window.TRADE_META; the page changed again')
    meta = extract_meta(html)
    # Only the embedded reference data is kept: the page itself carries a
    # per-visit anti-forgery token that has no business in the raw folder.
    P.save('trade_meta.json',
           json.dumps(meta, ensure_ascii=False, indent=1).encode('utf-8'),
           url + ' (window.TRADE_META, extracted)')
    P.log[-1]['page_sha256'] = hashlib.sha256(body).hexdigest()

    first, last = meta['earliest'], meta['latest']
    print(f'  portal: {first} to {last}, USD from {meta["usdFrom"]}, '
          f'{len(meta["countries"])} countries, version {meta.get("version")}')

    st, body, url = P.get(f'api/agg?flow=B&from={first}&to={last}&by=month')
    if st != 200:
        sys.exit(f'monthly totals returned {st}; stopping')
    P.save('monthly_totals.json', body, url)
    got = len(json.loads(body)['rows'])
    months = list(months_between(first, last))
    if got != len(months):
        print(f'  WARNING: by=month returned {got} rows for {len(months)} months')

    for n, ym in enumerate(months, 1):
        st, body, url = P.get(f'api/agg?flow=B&from={ym}&to={ym}&by=country')
        if st == 200:
            P.save(f'by_country_month/{ym}.json', body, url)
        else:
            P.unavailable.append({'url': url, 'status': st,
                                  'note': body[:200].decode('utf-8', 'replace')})
            print(f'  {ym}: HTTP {st}')
        if n % 24 == 0:
            print(f'  by country: {n}/{len(months)} months')

    # What the public tier will not serve, recorded so the README is checkable.
    y, m = divmod(last, 100)
    nxt = (y + 1) * 100 + 1 if m == 12 else last + 1
    for path, why in [
            (f'api/agg?flow=B&from={last}&to={last}&by=sitc',
             'commodity groups by SITC'),
            (f'api/agg?flow=B&from={last}&to={last}&by=section',
             'commodity groups by HS section'),
            (f'api/agg?flow=B&from={nxt}&to={nxt}&by=month',
             'the month after META.latest')]:
        st, body, url = P.get(path)
        P.unavailable.append({'url': url, 'status': st, 'what': why,
                              'note': body[:300].decode('utf-8', 'replace')})

    manifest = {
        'captured_utc': dt.datetime.now(dt.timezone.utc).isoformat(timespec='seconds'),
        'source': BASE + ' (PBS, Pakistan Trade Pulse; public tier, not signed in)',
        'fields': 'api/agg rows: k = YYYYMM (by=month) or PBS partner code '
                  '(by=country); x = exports, m = imports, each '
                  '[US dollars, rupees, SDR, quantity index] in units',
        'meta': {k: meta.get(k) for k in
                 ('earliest', 'latest', 'usdFrom', 'sdrFrom', 'version')},
        'files': P.log,
        'unavailable': P.unavailable,
    }
    (out / 'manifest.json').write_text(json.dumps(manifest, indent=1,
                                                  ensure_ascii=False))
    return manifest


# ── deriving ─────────────────────────────────────────────────────────────────

def fy_of(y, m):
    s = y if m >= 7 else y - 1
    return f'{s}-{(s + 1) % 100:02d}'


def quarter_of(m):
    return f'Q{(m - 7) % 12 // 3 + 1}'


def val(arr, i, ym, usd_from):
    if arr is None:
        return None
    if i == USD and ym < usd_from:
        return None  # no dollar series before META.usdFrom: missing, not zero
    v = arr[i]
    return None if v is None else v


def add(a, b):
    if b is None:
        return a
    return b if a is None else a + b


def fmt_int(v):
    return '' if v is None else str(int(round(v)))


def fmt_float(v):
    return '' if v is None else repr(float(v))


def write_csv(path, cols, rows, fmt):
    with open(path, 'w', newline='') as f:
        w = csv.writer(f, lineterminator='\n')
        w.writerow(cols)
        for r in rows:
            w.writerow([fmt(r[c]) if c in VALUE_COLS else
                        ('' if r[c] is None else r[c]) for c in cols])


def old_partner_map(old):
    """Partner codes and names as the 2026-09-27 pull carried them."""
    p = old / 'trade_by_country_fy_period.csv'
    m = {}
    if not p.exists():
        return m
    with open(p, newline='') as f:
        # The old file wrote 'NA' (North America, Namibia) as empty, so it
        # is only ever a fallback below the portal's own codes.
        for r in csv.DictReader(f):
            m.setdefault(r['pbs_country_code'], r)
    return m


def derive(out, old):
    meta = json.loads((out / 'trade_meta.json').read_text())
    usd_from = meta['usdFrom']
    ref = {str(c['id']): c for c in meta['countries']}
    oldmap = old_partner_map(old)

    # monthly totals
    tot = json.loads((out / 'monthly_totals.json').read_text())
    months = {}
    for r in tot['rows']:
        ym = int(r['k'])
        y, m = divmod(ym, 100)
        months[ym] = {
            'fy': fy_of(y, m), 'year': y, 'month': m,
            **{f'{fl}_{cur}': val(r.get(k), i, ym, usd_from)
               for fl, k in FLOWS for cur, i in (('pkr', PKR), ('usd', USD))}}
    trows = [months[k] for k in sorted(months)]

    # partners by month
    unmapped, ct_src = {}, {}
    cty = {}  # (ym, code) -> row
    names = {}
    for p in sorted((out / 'by_country_month').glob('*.json')):
        ym = int(p.stem)
        d = json.loads(p.read_text())
        for r in d['rows']:
            code = str(r['k']).zfill(3)
            rf = ref.get(str(r['k']), {})
            o = oldmap.get(code, {})
            iso2 = r.get('i2') or rf.get('i2') or o.get('iso2') or None
            iso3 = r.get('i3') or rf.get('i3') or o.get('iso3') or None
            ct = r.get('ct') or rf.get('ct')
            if not ct or ct == 'XX':  # the portal's own 'no continent'
                ct = o.get('continent') or None
            names.setdefault(code, set()).add(r['n'])
            if not iso2 or not ct:
                unmapped.setdefault(code, {'country': r['n'], 'iso2': iso2,
                                           'iso3': iso3, 'continent': ct,
                                           'portal_ct': r.get('ct'),
                                           'months': 0})['months'] += 1
            y, m = divmod(ym, 100)
            cty[(ym, code)] = {
                'fy': fy_of(y, m), 'ym': ym, 'country': r['n'], 'iso2': iso2,
                'iso3': iso3, 'continent': ct, 'pbs_country_code': code,
                **{f'{fl}_{cur}': val(r.get(k), i, ym, usd_from)
                   for fl, k in FLOWS for cur, i in (('pkr', PKR), ('usd', USD))}}

    # a partner keeps one name, the latest it was served under
    latest_name = {}
    for (ym, code), r in sorted(cty.items()):
        latest_name[code] = (r['country'], r['iso2'], r['iso3'], r['continent'])

    def periods(ym):
        y, m = divmod(ym, 100)
        return [f'M{m:02d}', quarter_of(m), 'FY_from_quarters']

    agg = {}
    for (ym, code), r in cty.items():
        for per in periods(ym):
            key = (r['fy'], per, code)
            a = agg.setdefault(key, {c: None for c in VALUE_COLS})
            for c in VALUE_COLS:
                a[c] = add(a[c], r[c])
    crows = []
    for (fy, per, code), a in agg.items():
        n, i2, i3, ct = latest_name[code]
        crows.append({'fy': fy, 'period': per, 'country': n, 'iso2': i2,
                      'iso3': i3, 'continent': ct, 'pbs_country_code': code, **a})
    crows.sort(key=lambda r: (r['fy'], r['period'], r['country'],
                              r['pbs_country_code']))

    # reconciliation: totals against country sums and (frozen) group sums
    T, C, G, N = {}, {}, {}, {}
    for ym, r in months.items():
        for per in periods(ym):
            k = (r['fy'], per)
            N[k] = N.get(k, 0) + 1
            t = T.setdefault(k, {c: None for c in VALUE_COLS})
            for c in VALUE_COLS:
                t[c] = add(t[c], r[c])
    for (fy, per, code), a in agg.items():
        c_ = C.setdefault((fy, per), {c: None for c in VALUE_COLS})
        for c in VALUE_COLS:
            c_[c] = add(c_[c], a[c])
    # Group sums are compared only where the old pull's monthly totals equal
    # this pull's, column by column, in every month of the period.
    prev = {}
    for p in old.glob('trade_monthly_totals_*.csv'):
        with open(p, newline='') as f:
            for r in csv.DictReader(f):
                prev[int(r['year']) * 100 + int(r['month'])] = {
                    c: (int(r[c]) if r[c] != '' else None) for c in VALUE_COLS}
    revised = {}  # (fy, period) -> set of columns revised in some month
    for ym, r in months.items():
        o = prev.get(ym)
        for per in periods(ym):
            s_ = revised.setdefault((r['fy'], per), set())
            for c in VALUE_COLS:
                if o is None or o[c] != r[c] or not r[c]:
                    s_.add(c)
    gp = old / 'trade_by_commodity_group_fy_period.csv'
    with open(gp, newline='') as f:
        for r in csv.DictReader(f):
            k = (r['fy'], r['period'])
            g = G.setdefault(k, {c: None for c in VALUE_COLS})
            for c in VALUE_COLS:
                if c not in revised.get(k, set(VALUE_COLS)) and r[c] != '':
                    g[c] = add(g[c], int(r[c]))

    def sub(a, b):
        return None if a is None or b is None else a - b

    rrows = []
    for k in sorted(T):
        t, c_, g = T[k], C.get(k, {}), G.get(k, {})
        r = {'fy': k[0], 'period': k[1]}
        for cur in ('pkr', 'usd'):
            for fl in ('imports', 'exports'):
                r[f'totals_{fl}_{cur}'] = t[f'{fl}_{cur}']
        for src, d in (('countries', c_), ('groups', g)):
            for cur in ('pkr', 'usd'):
                for fl in ('imports', 'exports'):
                    v = d.get(f'{fl}_{cur}')
                    r[f'{src}_{fl}_{cur}'] = None if v is None else int(round(v))
        for cur in ('pkr', 'usd'):
            for fl in ('imports', 'exports'):
                for src in ('countries', 'groups'):
                    r[f'{src}_minus_totals_{fl}_{cur}'] = sub(
                        r[f'{src}_{fl}_{cur}'], r[f'totals_{fl}_{cur}'])
        r['months_in_totals'] = N[k]
        rrows.append(r)

    # file names and columns as the 2026-09-27 pull wrote them
    y0, y1 = trows[0]['year'], trows[-1]['year']
    tname = f'trade_monthly_totals_{y0}_{y1}.csv'
    write_csv(out / tname, TOTALS_COLS, trows, fmt_int)
    write_csv(out / 'trade_by_country_fy_period.csv', COUNTRY_COLS, crows, fmt_float)
    rcols = (['fy', 'period']
             + [f'totals_{fl}_{cur}' for cur in ('pkr', 'usd') for fl in ('imports', 'exports')]
             + [f'countries_{fl}_{cur}' for cur in ('pkr', 'usd') for fl in ('imports', 'exports')]
             + [f'groups_{fl}_{cur}' for cur in ('pkr', 'usd') for fl in ('imports', 'exports')]
             + [f'{s}_minus_totals_{fl}_{cur}' for cur in ('pkr', 'usd')
                for fl in ('imports', 'exports') for s in ('countries', 'groups')]
             + ['months_in_totals'])
    with open(out / 'trade_reconciliation_fy_period.csv', 'w', newline='') as f:
        w = csv.writer(f, lineterminator='\n')
        w.writerow(rcols)
        for r in rrows:
            w.writerow(['' if r[c] is None else r[c] for c in rcols])

    # the parquet twin of the country table, typed as the old one was
    con = duckdb.connect()
    src = (out / 'trade_by_country_fy_period.csv').as_posix()
    types = {c: 'VARCHAR' for c in COUNTRY_COLS[:7]} | {c: 'DOUBLE' for c in VALUE_COLS}
    con.sql(f"""COPY (SELECT * FROM read_csv('{src}', header=true,
                       columns={types!r}, nullstr=''))
                TO '{(out / 'trade_by_country_fy_period.parquet').as_posix()}'
                (FORMAT PARQUET, COMPRESSION ZSTD)""")

    # the group table, frozen
    shutil.copy2(old / 'trade_by_commodity_group_fy_period.csv',
                 out / 'trade_by_commodity_group_fy_period.csv')

    renamed = {c: sorted(n) for c, n in names.items() if len(n) > 1}
    summary = {'totals_file': tname, 'months': len(trows),
               'country_rows': len(crows), 'recon_rows': len(rrows),
               'partners': len(latest_name), 'unmapped': unmapped,
               'renamed_partners': renamed}
    (out / 'derive_summary.json').write_text(json.dumps(summary, indent=1,
                                                        ensure_ascii=False))
    print(f'  {tname}: {len(trows)} months')
    print(f'  trade_by_country_fy_period: {len(crows):,} rows, '
          f'{len(latest_name)} partners')
    print(f'  trade_reconciliation_fy_period: {len(rrows)} rows')
    for code, u in sorted(unmapped.items()):
        print(f'  unmapped {code} {u}')
    for code, n in sorted(renamed.items()):
        print(f'  partner {code} served under several names: {n}')
    return summary


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--out', required=True, help='new raw folder, e.g. '
                    'raw_data/pbs_insight_explorer/trade_2026-10-24')
    ap.add_argument('--old', required=True, help='the previous pull, for the '
                    'frozen group table and the partner-code fallback')
    ap.add_argument('--derive-only', action='store_true',
                    help='rebuild the CSVs from the JSON already in --out')
    ap.add_argument('--pause', type=float, default=PAUSE)
    a = ap.parse_args()
    out, old = pathlib.Path(a.out), pathlib.Path(a.old)
    out.mkdir(parents=True, exist_ok=True)
    if not a.derive_only:
        fetch(out, a.pause)
    derive(out, old)


if __name__ == '__main__':
    main()
