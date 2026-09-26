"""Stage 2: resolve every published sub-district unit to an ADM3 polygon.

Four sources of evidence, applied in order and recorded per unit:

  manual       a reviewed decision in manual_map.py
  mouza        the existing PBS-tehsil crosswalk, which carries prior manual work
  boundary     geoBoundaries ADM3 name, matched within the same district
  fuzzy        the same, approximately, within the same district

Anything left is withheld with its candidate polygons listed, so the remaining
review is a short task rather than a research project.
"""
import argparse, collections, csv, difflib, json, pathlib, re
import openpyxl
from manual_map import MANUAL, WITHHELD

PMAP = {'KHYBER PAKHTUNKHWA': 'KHYBER PAKHTUNKHWA', 'PUNJAB': 'PUNJAB', 'SINDH': 'SINDH',
        'BALOCHISTAN': 'BALOCHISTAN', 'ISLAMABAD': 'ISLAMABAD CAPITAL TERRITORY'}
STRIP = r'\b(TEHSIL|TALUKA|TALUKO|SUB-?TEHSIL|SUB-?DIVISION|TOWN|DISTRICT|CANTONMENT|PROTECTED AREA|DE-?EXCLUDED AREA)\b'


# Names the two sources spell differently in a way no fuzzy rule should be
# trusted to guess. Each is a documented abbreviation, not a near-match.
ABBREV = {
    'DERAISMAILKHAN': 'DIKHAN',
    'DERAGHAZIKHAN': 'DGKHAN',
    'MIRPURKHAS': 'MIRPURKHAS',
}


def norm(s):
    s = re.sub(r'\(.*?\)', '', s.upper())
    n = re.sub(r'[^A-Z0-9]', '', re.sub(STRIP, '', s))
    return ABBREV.get(n, n)


def load_polygons(geo_js, gb_geojson):
    s = pathlib.Path(geo_js).read_text(errors='replace')
    site = json.loads(s[s.find('{'):s.rfind('}') + 1])['features']
    gb = json.loads(pathlib.Path(gb_geojson).read_text())['features']
    nm = {g['properties']['shapeID']: g['properties']['shapeName'] for g in gb}
    out = []
    for f in site:
        p = f['properties']
        out.append(dict(dd_id=p['dd_id'], dk=(p.get('dk') or '').upper(),
                        prov=(p.get('prov') or '').upper(), name=nm.get(p['dd_id'], '')))
    return out


def load_cod(path):
    """OCHA COD-AB ADM3 gazetteer: 577 tehsils with stable p-codes, reviewed
    September 2024. Newer than geoBoundaries (554 units, 2017 vintage), so it
    carries tehsils created since — Baka Khel, Chagai, Chaman and the rest."""
    ws = openpyxl.load_workbook(path, read_only=True, data_only=True)['pak_admin3']
    rows = list(ws.iter_rows(values_only=True))
    ix = {h: i for i, h in enumerate(rows[0])}
    return [dict(name=r[ix['adm3_name']], pcode=r[ix['adm3_pcode']],
                 district=r[ix['adm2_name']], dpcode=r[ix['adm2_pcode']])
            for r in rows[1:] if r[ix['adm3_name']]]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--cod')
    ap.add_argument('--units', required=True)
    ap.add_argument('--mouza', required=True)
    ap.add_argument('--geo', required=True)
    ap.add_argument('--boundaries', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    poly = load_polygons(a.geo, a.boundaries)
    cod = load_cod(a.cod) if a.cod else []
    cod_by_d = collections.defaultdict(list)
    for c in cod:
        cod_by_d[norm(c['district'])].append(c)

    def cod_match(unit, dist):
        pool = cod_by_d.get(norm(dist), [])
        n = norm(unit)
        hit = next((c for c in pool if norm(c['name']) == n), None)
        if hit:
            return hit, 'exact'
        g = difflib.get_close_matches(n, [norm(c['name']) for c in pool], n=1, cutoff=0.85)
        if g:
            return next(c for c in pool if norm(c['name']) == g[0]), 'approximate'
        return None, None
    by_name = collections.defaultdict(list)
    by_dk = collections.defaultdict(list)
    for p in poly:
        by_name[norm(p['name'])].append(p)
        by_dk[norm(p['dk'])].append(p)

    mouza = collections.defaultdict(dict)
    for r in csv.DictReader(open(a.mouza)):
        mouza[r['province']].setdefault(norm(r['tehsil']), r)

    units = [r for r in csv.DictReader(open(a.units)) if r['unit_type'] != 'district']
    rows, withheld = [], []
    for u in units:
        unit, dist, prov = u['unit'], u['district'], u['province']
        rec = dict(province=prov, district=dist, unit=unit, unit_type=u['unit_type'],
                   dd_id='', polygon='', adm3_pcode='', adm3_name='', method='', evidence='')
        c, how = cod_match(unit, dist)
        if c:
            rec.update(adm3_pcode=c['pcode'], adm3_name=c['name'])
            rec['cod_match'] = how

        if unit.upper() in MANUAL:
            want, cod_name, why = MANUAL[unit.upper()]
            cand = [p for p in poly if p['name'].upper() == want]
            cc = next((c for c in cod if norm(c['name']) == norm(cod_name)), None)
            if cc:
                rec.update(adm3_pcode=cc['pcode'], adm3_name=cc['name'])
            if cand:
                rec.update(dd_id=cand[0]['dd_id'], polygon=cand[0]['name'],
                           method='manual', evidence=why)
                rows.append(rec); continue

        if unit.upper() in WITHHELD:
            cands = by_dk.get(norm(dist), [])
            withheld.append(dict(province=prov, district=dist, unit=unit,
                                 reason=WITHHELD[unit.upper()],
                                 candidates='; '.join(sorted(p['name'] for p in cands))))
            rec.update(method='withheld', evidence=WITHHELD[unit.upper()])
            rows.append(rec); continue

        m = mouza[PMAP[prov]].get(norm(unit))
        if m and m['dd_id']:
            nmp = next((p for p in poly if p['dd_id'] == m['dd_id']), None)
            rec.update(dd_id=m['dd_id'], polygon=nmp['name'] if nmp else '',
                       method='mouza', evidence=f"mouza crosswalk, match={m['match']}")
            rows.append(rec); continue

        cands = by_name.get(norm(unit), [])
        if not cands:
            # PBS sometimes inverts the label: Quetta's "SUB-DIVISION CITY" is
            # the polygon layer's "QUETTA CITY". Try the district-qualified form.
            cands = by_name.get(norm(f'{dist} {unit}'), [])
        same = [p for p in cands if norm(p['dk']) == norm(dist)]
        if same:
            rec.update(dd_id=same[0]['dd_id'], polygon=same[0]['name'], method='boundary',
                       evidence=f"geoBoundaries name match within {dist}")
            rows.append(rec); continue
        if len(cands) == 1:
            rec.update(dd_id=cands[0]['dd_id'], polygon=cands[0]['name'], method='boundary',
                       evidence='geoBoundaries unique name match (district label differs)')
            rows.append(rec); continue

        pool = by_dk.get(norm(dist), [])
        g = difflib.get_close_matches(norm(unit), [norm(p['name']) for p in pool], n=1, cutoff=0.85)
        if g:
            p = next(p for p in pool if norm(p['name']) == g[0])
            rec.update(dd_id=p['dd_id'], polygon=p['name'], method='fuzzy',
                       evidence=f"approximate name match within {dist}")
            rows.append(rec); continue

        if rec['adm3_pcode']:
            rec.update(method='cod', evidence=f"COD-AB {rec['cod_match']} name match within {dist}; "
                                              f"no geoBoundaries polygon (unit postdates that layer)")
            rows.append(rec); continue
        withheld.append(dict(province=prov, district=dist, unit=unit,
                             reason='no polygon or p-code matched by name within the district',
                             candidates='; '.join(sorted(p['name'] for p in pool))))
        rec.update(method='withheld', evidence='no polygon or p-code matched by name within the district')
        rows.append(rec)

    # Mint a stable project identifier for every published sub-district unit.
    # COD-AB covers 83% of them and geoBoundaries fewer, and neither is a
    # superset, so the panel cannot key on either. DDS is ours; adm3_pcode and
    # dd_id ride alongside as external keys where they exist. Assignment is by
    # a fixed sort, so a rebuild reproduces the same identifiers.
    PROV = {'KHYBER PAKHTUNKHWA': 'KP', 'PUNJAB': 'PB', 'SINDH': 'SD',
            'BALOCHISTAN': 'BA', 'ISLAMABAD': 'IS'}
    for i, r in enumerate(sorted(rows, key=lambda x: (x['province'], x['district'], x['unit'])), 1):
        r['dds_id'] = f"DDS-{PROV[r['province']]}-{i:04d}"
    rows.sort(key=lambda x: x['dds_id'])
    order = ['dds_id', 'province', 'district', 'unit', 'unit_type', 'adm3_pcode',
             'adm3_name', 'dd_id', 'polygon', 'method', 'evidence']
    rows = [{k: r.get(k, '') for k in order} for r in rows]

    out = pathlib.Path(a.out); out.mkdir(parents=True, exist_ok=True)
    with open(out / 'sub_district_crosswalk.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=order); w.writeheader(); w.writerows(rows)
    with open(out / 'withheld.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=['province', 'district', 'unit', 'reason', 'candidates'])
        w.writeheader(); w.writerows(withheld)

    # Bridge: where a unit was placed on a geoBoundaries polygon but matched no
    # COD-AB name directly, take the p-code of the COD unit with that polygon's
    # name. Only when that name is unique in COD-AB, so it cannot mis-assign.
    cod_name_count = collections.Counter(norm(c['name']) for c in cod)
    cod_by_name = {norm(c['name']): c for c in cod}
    all_cod_norms = list(cod_by_name)
    bridged = collections.Counter()
    for r in rows:
        if r['adm3_pcode']:
            continue
        # (a) exact polygon name, unique in COD-AB
        if r['polygon'] and cod_name_count.get(norm(r['polygon'])) == 1:
            c = cod_by_name[norm(r['polygon'])]
            r.update(adm3_pcode=c['pcode'], adm3_name=c['name'])
            r['evidence'] += '; p-code via polygon name (unique in COD-AB)'
            bridged['polygon exact'] += 1
            continue
        # (b) polygon name, approximately, within the district's COD pool
        pool = cod_by_d.get(norm(r['district']), [])
        if r['polygon'] and pool:
            g = difflib.get_close_matches(norm(r['polygon']),
                                          [norm(c['name']) for c in pool], n=1, cutoff=0.88)
            if g:
                c = next(c for c in pool if norm(c['name']) == g[0])
                r.update(adm3_pcode=c['pcode'], adm3_name=c['name'])
                r['evidence'] += f"; p-code via approximate polygon name within {r['district']}"
                bridged['polygon fuzzy in district'] += 1
                continue
        # (c) the unit's own name, exact and unique across COD-AB. Karachi and a
        # few other places were re-districted after COD-AB was drawn, so the
        # district constraint has to be dropped — but only where the name is
        # unique nationally, so it cannot mis-assign.
        k = norm(r['unit'])
        if cod_name_count.get(k) == 1:
            c = cod_by_name[k]
            r.update(adm3_pcode=c['pcode'], adm3_name=c['name'])
            r['evidence'] += '; p-code via unit name, unique in COD-AB (district differs)'
            bridged['unit name, other district'] += 1
    for r in rows:
        r.pop('cod_match', None)
    c = collections.Counter(r['method'] for r in rows)
    placed = sum(v for k, v in c.items() if k != 'withheld')
    print(f"units            {len(rows)}")
    for k in ('manual', 'mouza', 'boundary', 'fuzzy', 'cod', 'withheld'):
        print(f"  {k:10s}     {c.get(k, 0)}")
    print(f"placed           {placed}/{len(rows)} ({100*placed/len(rows):.1f}%)")
    print(f"distinct polygons used  {len({r['dd_id'] for r in rows if r['dd_id']})}")
    npc = sum(1 for r in rows if r['adm3_pcode'])
    print(f"with a COD-AB p-code    {npc}/{len(rows)} ({100*npc/len(rows):.1f}%)")
    for k, v in bridged.most_common():
        print(f"    bridged: {k:34s} {v}")
    dup = [k for k, v in collections.Counter(r['adm3_pcode'] for r in rows if r['adm3_pcode']).items() if v > 1]
    print(f"p-codes used by >1 unit {len(dup)}")
    print(f"DDS identifiers minted  {len({r['dds_id'] for r in rows})}")
    ext = sum(1 for r in rows if r['adm3_pcode'] or r['dd_id'])
    print(f"with an external key    {ext}/{len(rows)} ({100*ext/len(rows):.1f}%)")


if __name__ == '__main__':
    main()
