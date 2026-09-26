"""Census 2017 table 2: named urban localities.

A row is a place - a named town, cantonment or municipal committee - with its
parent tehsil and its population, not a published administrative unit. It does not
belong in the unit panel, where every row has to be a census unit, so it is built
separately, as 2023's table 2 is.

The sheet has the same shape in both years, so the row walk is 2023's
`build_urban_localities.read_sheet` rather than a copy of it. What differs is the
input: 2023 publishes one workbook per region, 2017 one per district.
"""
import argparse, collections, csv, json, os, pathlib, sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'stage2'))

from build_urban_localities import read_sheet
from read_workbook import anchor, numbered_columns, header
from read_xls import load, synthesise_banner_merges
from headerless import is_empty, reason_text, synth_columns
from extract_2017 import area_of, dir_of, district_workbooks, units_in, read_layout


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', required=True, help='the dated capture directory')
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    man = json.load(open(os.path.join(a.dir, 'retrieval_manifest.json')))
    out = pathlib.Path(a.out); out.mkdir(parents=True, exist_ok=True)

    # The district roster, from table 1. Table 2 sometimes omits its district
    # row - 220 of 589 localities came through unattributed - and the name then
    # has to come from that district's own table 1, reached through the directory
    # the capture recorded rather than from the filename.
    roster = {}
    for f in district_workbooks(man, {'1', '?'}):
        rows, merges = load(os.path.join(a.dir, f['path']))
        lay = read_layout(rows, merges)
        if lay is not None:
            ai, stub = lay[0], lay[1]
        else:
            # Swabi's, Okara's and Malakand's table 1 have no header block; the
            # stub column still comes off the widest data row, and the roster
            # needs only that. Skipping them left Malakand's one urban locality
            # unattributed.
            stub, dcols = synth_columns(rows)
            ai = -1
            if not dcols:
                continue
        district, _ = units_in(rows, stub, ai)
        if district:
            roster[dir_of(f)] = district
    print(f'roster: {len(roster)} districts', flush=True)

    files = [f for f in man['files'] if f['kind'] == 'xlsx' and f['table'] == '2']
    files += [f for f in man['files'] if f['kind'] == 'xls_area'
              and f.get('area') == 'ISLAMABAD' and f['table'] == '2']
    files.sort(key=lambda f: f['path'])
    print(f'{len(files)} table-2 workbooks', flush=True)

    rows_out, empty, problems = [], [], []
    for f in files:
        rows, merges = load(os.path.join(a.dir, f['path']))
        an = anchor(rows)
        if an is None:
            # A district with no urban population has no urban localities to
            # list. Read off the sheet, not from a list of district names.
            (empty if is_empty(rows) else problems).append(
                dict(path=f['path'], note=reason_text(rows) or 'no data rows')
                if is_empty(rows) else
                dict(path=f['path'], issue='header block missing, but the file holds data'))
            continue
        stub, data_cols = numbered_columns(rows, an)
        if not data_cols:
            problems.append(dict(path=f['path'], issue='no data columns'))
            continue
        merges = synthesise_banner_merges(rows, merges, an, data_cols)
        cols = header(rows, an, stub, merges, data_cols)
        got = read_sheet(rows, an, stub, data_cols, cols, area_of(f), f['path'])
        for g in got:
            if not g['district']:
                g['district'] = roster.get(dir_of(f))
                g['district_from_roster'] = True
        if got:
            rows_out += got
        else:
            empty.append(dict(path=f['path'], note='header present, no locality rows'))

    for r in rows_out:
        r.setdefault('district_from_roster', False)
    keys = ['province_area', 'district', 'tehsil', 'locality', 'size_class']
    extra = [k for k in dict.fromkeys(k for r in rows_out for k in r)
             if k not in keys + ['missing', 'source_file', 'src_row', 'district_from_roster']]
    fields = keys + extra + ['district_from_roster', 'missing', 'source_file', 'src_row']
    rows_out.sort(key=lambda r: (r['province_area'], r['district'] or '', r['locality']))
    with open(out / 'urban_localities_2017.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction='ignore')
        w.writeheader(); w.writerows(rows_out)

    byarea = collections.Counter(r['province_area'] for r in rows_out)
    rep = dict(localities=len(rows_out),
               with_parent_tehsil=sum(1 for r in rows_out if r['tehsil']),
               districts=len({r['district'] for r in rows_out if r['district']}),
               district_supplied_from_roster=sum(1 for r in rows_out if r['district_from_roster']),
               still_unattributed=sum(1 for r in rows_out if not r['district']),
               by_area=dict(sorted(byarea.items())), columns=extra,
               empty_workbooks=empty, problems=problems)
    (out / 'urban_localities_report_2017.json').write_text(
        json.dumps(rep, indent=2, sort_keys=True))
    print(f"urban localities      {len(rows_out):,}")
    print(f"  with a parent tehsil {rep['with_parent_tehsil']:,}")
    print(f"  districts covered    {rep['districts']}")
    print(f"  district supplied    {rep['district_supplied_from_roster']}")
    print(f"  still unattributed   {rep['still_unattributed']}")
    print(f"  by area              {rep['by_area']}")
    print(f"  columns              {extra}")
    print(f"empty workbooks        {len(empty)}")
    print(f"problems               {len(problems)}")
    for p in problems[:8]:
        print('  PROBLEM', p)


if __name__ == '__main__':
    main()
