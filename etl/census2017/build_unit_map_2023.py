"""Put both censuses on one frame: PBS's Digital Census 2023 boundaries.

The two censuses count different districts - 135 against 136 - and eight of
those differences are real boundary changes, not spellings. A map with a year
toggle therefore has to decide, per unit, what a 2017 figure drawn on a 2023
shape actually means. This writes that decision down, one row per unit, so the
app never has to infer it and a reader can audit it.

The decision follows the crosswalk, which is itself checked against PBS's own
restatement of the 2017 population on 2023 boundaries:

  exact / renamed   the same ground under the same or a new name. The 2017
                    figure belongs on the 2023 shape unchanged.
  merged            an FR was absorbed into its host district. Two 2017 units
                    cover one 2023 shape, so the 2017 figure is the two added
                    together - or, for a rate, their population-weighted mean,
                    which is what the district pages already do.
  split             one 2017 unit became several 2023 shapes. Its count cannot
                    be divided between them without inventing the division, but
                    it can be drawn over all of them at once: the successors
                    together are exactly the ground the parent covered, so one
                    colour across the group states the published figure and
                    claims nothing more. map_key then holds every successor's
                    key, space-separated, and the unit is labelled by its 2017
                    name.
  restructured      below the district, several 2017 units were redrawn into
                    several 2023 ones and no single pair corresponds. The group
                    does: its 2017 units cover exactly the ground of its 2023
                    units (the restated 2017 population balances), so each 2017
                    unit is keyed to the whole group's 2023 shapes. The map joins
                    them into one footprint - a count is the group's total, a
                    rate the group's population-weighted mean - drawn across the
                    group and divided between none of it.
  boundary transfer territory moved between two districts that both still exist.
                    The 2017 figure is drawn, because the district is the same
                    district, and flagged, because its area is not.

Population is the exception that needs none of this: Census 2023 table 1 prints
PBS's own POPULATION 2017 for all 136 districts on 2023 boundaries. Where that
column covers a series, it is used in preference to anything computed here.

Usage:
  build_unit_map_2023.py --crosswalk <dir> --panel17 <p> --panel23 <p>
                         --districts-geo <js> --out <csv>
"""
import argparse, collections, csv, json, pathlib, re, sys

import duckdb

def nice(name):
    """A unit name for prose, without mangling PBS's abbreviations.

    str.title() turns FR BANNU into Fr Bannu and D.I.KHAN into D.I.Khan, and
    these notes are shown to readers, so the short all-capital tokens - FR, ICT,
    and the initials in D.I.Khan - are left as PBS writes them.
    """
    out = []
    for word in name.split():
        core = word.strip('.,')
        out.append(word if (len(core) <= 3 and core.isupper()) or '.' in word
                   else word.title())
    return ' '.join(out)


RATE = re.compile(r'(RATIO|RATE|PER CENT|PERCENT|PROPORTION|AVERAGE|DENSITY|'
                  r'PER SQ|HOUSEHOLD SIZE|PERSONS PER)', re.I)


def is_rate(indicator, col_label):
    """True for a series that must not be added when two units are combined."""
    return bool(RATE.search(indicator or '') or RATE.search(col_label or ''))


def district_keys(path):
    """{census district name (upper) -> PBS district code} from the 2023 layer."""
    js = pathlib.Path(path).read_text()
    gj = json.loads(js[js.index('=') + 1:].rstrip().rstrip(';'))
    out = {}
    for f in gj['features']:
        p = f['properties']
        if p.get('c'):
            out[p['c'].upper().strip()] = p['code']
    return out


def sub_keys(con, panel23):
    """{(district, unit) -> dds_id} for every 2023 sub-district unit."""
    rows = con.sql(f"""SELECT DISTINCT district, unit, map_key FROM '{panel23}'
                       WHERE unit_type <> 'district' AND map_key IS NOT NULL""").fetchall()
    return {(d.upper().strip(), u.upper().strip()): k for d, u, k in rows}


def sub_home_23(con, panel23):
    """{unit (upper) -> [districts]} on the 2023 side, to resolve a group's keys."""
    out = {}
    for d, u in con.sql(f"""SELECT DISTINCT district, unit FROM '{panel23}'
                            WHERE unit_type <> 'district'""").fetchall():
        out.setdefault(u.upper().strip(), []).append(d)
    return out


def sub_home(con, panel17):
    """{unit (upper) -> [districts that publish it]} on the 2017 side.

    A restructured group can span two districts, so the group label does not say
    which district a listed unit belongs to. The panel does.
    """
    out = {}
    for d, u in con.sql(f"""SELECT DISTINCT district, unit FROM '{panel17}'
                            WHERE unit_type <> 'district'""").fetchall():
        out.setdefault(u.upper().strip(), []).append(d)
    return out


def pop17(con, panel17):
    """{(district, unit) -> 2017 population}, the weight for combining rates."""
    rows = con.sql(f"""SELECT district, unit, sum(value) FROM '{panel17}'
        WHERE table_id='1' AND locality='all' AND sex='all' AND value IS NOT NULL
          AND indicator LIKE '%POPULATION%2017%' AND col_label LIKE '%ALL SEXES%'
        GROUP BY 1,2""").fetchall()
    return {(d.upper().strip(), u.upper().strip()): (p or 0) for d, u, p in rows}


def pop23(con, panel23):
    """{(district, unit) -> 2023 population}. A 2017 unit later split into
    several 2023 tehsils is compared with all of them together, and for a rate
    that means averaging the 2023 side too - which needs the 2023 weights. Left
    out, every such footprint dropped off the change map: 2017-23 literacy
    reached 470 tehsils where 2017 alone reached 582."""
    rows = con.sql(f"""SELECT district, unit, sum(value) FROM '{panel23}'
        WHERE table_id='1' AND locality='all' AND sex='all' AND value IS NOT NULL
          AND indicator = 'POPULATION-2023 / ALL SEXES'
        GROUP BY 1,2""").fetchall()
    return {(d.upper().strip(), u.upper().strip()): (p or 0) for d, u, p in rows}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--crosswalk', required=True)
    ap.add_argument('--panel17', required=True)
    ap.add_argument('--panel23', required=True)
    ap.add_argument('--districts-geo', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect(); con.execute('SET threads TO 1')
    xw = pathlib.Path(a.crosswalk)
    dkey = district_keys(a.districts_geo)
    skey = sub_keys(con, a.panel23)
    w17 = pop17(con, a.panel17)
    w23 = pop23(con, a.panel23)
    home = sub_home(con, a.panel17)
    home23 = sub_home_23(con, a.panel23)

    rows, tally = [], collections.Counter()

    def emit(year, tier, district, unit, key, relation, comparable, note, weight=None):
        rows.append(dict(census_year=year, unit_type=tier, district=district, unit=unit,
                         map_key=key or '', relation=relation, comparable=comparable,
                         note=note, weight='' if weight is None else int(weight)))
        tally[f'{year} {tier} {comparable}'] += 1

    # ── districts ───────────────────────────────────────────────────────────
    for r in csv.DictReader(open(xw / 'district_crosswalk_2017_2023.csv')):
        u17 = [u for u in r['units_2017'].split(' + ') if u]
        u23 = [u for u in r['units_2023'].split(' + ') if u]
        rel = r['relation']
        for u in u23:                                  # the 2023 side is always itself
            emit(2023, 'district', u, u, dkey.get(u.upper().strip()), rel, 'yes', '',
                 w23.get((u.upper().strip(), u.upper().strip())))
        if not u17 or not u23:
            continue
        if rel in ('exact', 'renamed'):
            emit(2017, 'district', u17[0], u17[0], dkey.get(u23[0].upper().strip()),
                 rel, 'yes', '' if rel == 'exact' else f'known in 2023 as {nice(u23[0])}',
                 w17.get((u17[0].upper().strip(), u17[0].upper().strip())))
        elif rel == 'merged':
            host = u23[0]
            note = ('2017 figure combines ' +
                    ' and '.join(nice(x) for x in sorted(u17)) +
                    f', merged into {nice(host)} after 2017')
            for u in u17:
                emit(2017, 'district', u, u, dkey.get(host.upper().strip()), rel,
                     'combined', note, w17.get((u.upper().strip(), u.upper().strip())))
        elif rel == 'split':
            note = (f'{nice(u17[0])} was split after 2017 into ' +
                    ', '.join(nice(x) for x in sorted(u23)) +
                    '. The 2017 figure is for the whole of the old district and '
                    'is shown across all of them, not divided between them')
            keys = [dkey.get(x.upper().strip()) for x in sorted(u23)]
            emit(2017, 'district', u17[0], u17[0],
                 ' '.join(k for k in keys if k) if all(keys) else None,
                 rel, 'parent' if all(keys) else 'no', note,
                 w17.get((u17[0].upper().strip(), u17[0].upper().strip())))
        else:                                          # boundary transfer
            note = ('territory moved between ' +
                    ' and '.join(nice(x) for x in sorted(u23)) +
                    ' after 2017, so the two years cover slightly different ground')
            for u in u17:
                emit(2017, 'district', u, u, dkey.get(u.upper().strip()), rel,
                     'flagged', note, w17.get((u.upper().strip(), u.upper().strip())))

    # ── sub-districts ───────────────────────────────────────────────────────
    for r in csv.DictReader(open(xw / 'subdistrict_crosswalk_2017_2023.csv')):
        rel = r['relation']
        if rel == 'restructured':
            a17 = [x for x in r['units_2017'].split(' + ') if x]
            a23 = [x for x in r['units_2023'].split(' + ') if x]
            if len(a17) == 1 and len(a23) > 1:
                # One 2017 unit became several: the same clean split as above, one
                # tier down. Its successors together are the ground it covered.
                ks = []
                for x in a23:
                    cand = [d for d in home23.get(x.upper().strip(), [])
                            if d in r['district_group'].split(' + ')
                            or len(home23.get(x.upper().strip(), [])) == 1]
                    ks.append(skey.get((cand[0].upper().strip(), x.upper().strip()))
                              if len(cand) == 1 else None)
                d = [dd for dd in home.get(a17[0].upper().strip(), [])
                     if dd in r['district_group'].split(' + ')]
                note = (f'{nice(a17[0])} was split after 2017 into ' +
                        ', '.join(nice(x) for x in sorted(a23)) +
                        '. The 2017 figure is for the whole of the old unit and is '
                        'shown across all of them, not divided between them')
                emit(2017, 'tehsil', d[0] if len(d) == 1 else '', a17[0],
                     ' '.join(k for k in ks if k) if all(ks) else None,
                     rel, 'parent' if all(ks) else 'no', note,
                     w17.get((d[0].upper().strip(), a17[0].upper().strip())) if len(d) == 1 else None)
                continue
            # Many to many. No 2017 unit has a 2023 counterpart, but the group
            # has: key every 2017 unit to all of the group's 2023 shapes, and
            # the map joins them into one footprint.
            ks = []
            for x in a23:
                cand = [d for d in home23.get(x.upper().strip(), [])
                        if d in r['district_group'].split(' + ')
                        or len(home23.get(x.upper().strip(), [])) == 1]
                ks.append(skey.get((cand[0].upper().strip(), x.upper().strip()))
                          if len(cand) == 1 else None)
            grouped = all(ks)
            note = ('the tehsils in ' + nice(r['district_group']) + ' were redrawn after '
                    '2017. Together, 2017\u2019s ' + ', '.join(nice(x) for x in sorted(a17))
                    + ' cover the same ground as 2023\u2019s '
                    + ', '.join(nice(x) for x in sorted(a23))
                    + ', so the 2017 figure is for the group and is shown across all of '
                    'it, not divided between its tehsils' if grouped else
                    'the units in ' + nice(r['district_group']) + ' were redrawn after '
                    '2017; which 2023 tehsil corresponds to which 2017 one is not '
                    'established, so no 2017 value is drawn')
            for u in a17:
                # The group may span two districts; take the one the panel puts
                # this unit in, and only when that is unambiguous.
                cand = [d for d in home.get(u.upper().strip(), [])
                        if d in r['district_group'].split(' + ')]
                dd = cand[0] if len(cand) == 1 else ''
                emit(2017, 'tehsil', dd, u,
                     ' '.join(ks) if grouped else None, rel,
                     'group' if grouped else 'no', note,
                     w17.get((dd.upper().strip(), u.upper().strip())) if dd else None)
            continue
        d17, u17 = r['district_2017'], r['units_2017']
        d23, u23 = r['district_2023'], r['units_2023']
        emit(2017, 'tehsil', d17, u17, skey.get((d23.upper().strip(), u23.upper().strip())),
             rel, 'yes', '' if rel == 'exact' else f'known in 2023 as {nice(u23)}',
             w17.get((d17.upper().strip(), u17.upper().strip())))

    for (d, u), k in sorted(skey.items()):
        emit(2023, 'tehsil', d, u, k, 'exact', 'yes', '',
             w23.get((d.upper().strip(), u.upper().strip())))

    out = pathlib.Path(a.out); out.parent.mkdir(parents=True, exist_ok=True)
    rows.sort(key=lambda r: (r['census_year'], r['unit_type'], r['district'], r['unit']))
    with open(out, 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader(); w.writerows(rows)

    print(f'{len(rows)} unit rows -> {out}')
    for k, v in sorted(tally.items()):
        print(f'   {k:28s} {v}')
    unmapped = [r for r in rows if not r['map_key'] and r['comparable'] == 'yes']
    if unmapped:
        print(f'\n{len(unmapped)} units claim to be comparable but got no map key:')
        for r in unmapped[:25]:
            print(f"   {r['census_year']} {r['unit_type']:9s} {r['district']} / {r['unit']}")


if __name__ == '__main__':
    main()
