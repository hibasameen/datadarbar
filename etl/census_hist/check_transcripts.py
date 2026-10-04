"""Check page transcripts of a historical census table against their own arithmetic.

Each page JSON (see TRANSCRIPTION_SPEC.md in the warehouse run) holds printed
rows only. This stitches the pages of one table and region into a single
sequence and tests what the agents could not test on a single page:

  1. males + females = total on every row;
  2. every unit equals the sum of its next level down (tehsils to district,
     districts to division, ...), across page breaks; urban_sub_row rows
     (cantonments printed inside their municipality) are never summed;
  3. cells flagged uncertain, and checks the transcriber could not resolve.

It reports; it does not correct. A failure means re-reading the page image.

    python3 etl/census_hist/check_transcripts.py \
        ../data_darbar_warehouse/census_hist/2026-10-04/transcripts/b03
"""
import collections, json, pathlib, re, sys

RANK = {'country': 0, 'province': 1, 'province_part': 2, 'state': 2, 'division': 3,
        'district': 4, 'agency': 4, 'tribal_area': 5, 'subdivision': 5, 'tehsil': 5,
        'taluka': 5, 'thana': 6, 'urban_locality': 6, 'urban_sub_row': 9}
NUM_COLS = ['total', 'males', 'females', 'area_sq_mile', 'urban_area_sq_mile',
            'urban_total', 'urban_males', 'urban_females', 'rural_total', 'rural_males', 'rural_females']
# (total, parts) identities every row must satisfy
IDENTITIES = [('total', ['males', 'females']), ('urban_total', ['urban_males', 'urban_females']),
              ('rural_total', ['rural_males', 'rural_females']), ('total', ['urban_total', 'rural_total']),
              ('males', ['urban_males', 'rural_males']), ('females', ['urban_females', 'rural_females'])]


def region_key(heading):
    h = (heading or '').upper()
    h = re.sub(r'[-—–\s]*(CONTD|CONTINUED|CON[A-Z. ]*)\.*.*$', '', h)
    return re.sub(r'[^A-Z ]', '', h).strip()


def load(d):
    pages = []
    for f in sorted(pathlib.Path(d).glob('p-*.json')):
        p = json.loads(f.read_text())
        p['_file'] = f.name
        pages.append(p)
    return pages


def num(row, col):
    v = row.get('values', {}).get(col)
    return None if v is None else v.get('num')


def check(pages):
    out = collections.defaultdict(list)
    seqs = collections.OrderedDict()
    for p in pages:
        if not p.get('table'):
            continue
        k = (p['table'], region_key(p.get('region_heading')))
        tol = 2 if 'thousand' in (p.get('units') or '').lower() else 0
        rows = seqs.setdefault(k, [])
        by_serial = {r['serial']: r for r in rows if r.get('serial')}
        for r in p.get('rows', []):
            # Table 1 is printed across facing pages (totals left, urban/rural
            # right) keyed by the margin serial: join the halves into one row.
            if r.get('serial') and r['serial'] in by_serial:
                tgt = by_serial[r['serial']]
                tgt['values'] = {**tgt['values'], **{c: v for c, v in r['values'].items() if c not in tgt['values']}}
                tgt['_pages'].append(p['pdf_page'])
                continue
            r = dict(r, _page=p['pdf_page'], _pages=[p['pdf_page']], _tol=tol)
            rows.append(r)
            if r.get('serial'):
                by_serial[r['serial']] = r
        for c in p.get('checks_failed', []):
            out['agent_checks_failed'].append((p['pdf_page'], c))
        for r in p.get('rows', []):
            for col in r.get('uncertain', []):
                v = r.get('values', {}).get(col, {})
                out['uncertain'].append((p['pdf_page'], r.get('name'), col, v.get('raw'), r.get('note', '')))

    for (table, region), rows in seqs.items():
        for r in rows:
            for tot, parts in IDENTITIES:
                vals = [num(r, c) for c in [tot] + parts]
                if None not in vals and abs(sum(vals[1:]) - vals[0]) > r['_tol']:
                    out['sex_sum'].append((table, region, r['_pages'], r['name'], f"{tot}={'+'.join(parts)}", vals[0], sum(vals[1:])))
        # Rank by how the row is printed, not by the transcriber's label: indent,
        # then bold (a bold row at an indent is a subtotal of the plain rows
        # under it at the same indent, e.g. Sibi's 'Administered Area'), then
        # province above province part. urban_sub_row is never a child.
        def rank(r):
            if r.get('level') == 'urban_sub_row':
                return None
            lv = {'country': 0, 'province': 1, 'province_part': 2}.get(r.get('level'), 3)
            if r.get('level') == 'state' and r.get('indent', 0) == 0:
                lv = 2   # Bahawalpur / Khairpur State printed beside the province proper
            return (r.get('indent', 0), 0 if r.get('bold') else 1, lv)
        rows[:] = [r for r in rows if any(v.get('num') is not None for v in r.get('values', {}).values())
                   or r.get('level') != 'province_part']   # drop empty '—contd.' heading rows
        ranks = [rank(r) for r in rows]
        for i, r in enumerate(rows):
            ri = ranks[i]
            if ri is None:
                continue
            span = []
            for j in range(i + 1, len(rows)):
                if ranks[j] is not None and ranks[j] <= ri:
                    break
                span.append(j)
            kids_rank = min((ranks[j] for j in span if ranks[j] is not None), default=None)
            if kids_rank is None:
                continue
            kids = [rows[j] for j in span if ranks[j] == kids_rank]
            for col in NUM_COLS:
                parent = num(r, col)
                vals = [num(k, col) for k in kids]
                if parent is None or any(v is None for v in vals):
                    continue
                s = sum(vals)
                tol = r['_tol'] * len(vals) if r['_tol'] else 0.051 * len(vals) if isinstance(parent, float) or any(isinstance(v, float) for v in vals) else 0
                if abs(s - parent) > tol:
                    out['child_sum'].append((table, region, r['_page'], r['name'], col, parent, s,
                                             len(kids), rows[span[-1]]['_page']))
    return out, seqs


def main():
    d = sys.argv[1]
    pages = load(d)
    out, seqs = check(pages)
    print(f'{len(pages)} pages, {sum(len(v) for v in seqs.values())} table rows in {len(seqs)} table/region runs')
    for k, rows in seqs.items():
        print(f'  {k[0]:>4} {k[1]:<45} {len(rows):>4} rows  pages {rows[0]["_page"]}-{rows[-1]["_page"]}')
    for kind in ['sex_sum', 'child_sum', 'agent_checks_failed', 'uncertain']:
        print(f'\n== {kind}: {len(out[kind])}')
        for x in out[kind]:
            print('  ', x)
    return 1 if (out['sex_sum'] or out['child_sum']) else 0


if __name__ == '__main__':
    sys.exit(main())
