"""Extract the Census 2017 district tables into one long table of observations.

Reads through `etl/stage2/read_workbook.py`, the same reader the 2023 panel uses,
with 2017's own layout declarations (`table_spec_2017.SPEC_2017`) and two
additions this release needs:

  Column labels for headerless files come from a sibling. 57 workbooks lost their
  header block in PBS's PDF-to-Excel conversion. `headerless.synth_columns`
  recovers WHERE the columns are from the widest data row, but not what they are
  called, because the label rows are gone. The labels are therefore taken, in
  order, from a reference copy of the same table that does have its header - the
  measures are published in the same sequence in every district's copy, which is
  what makes this sound. The counts must match or the file is refused rather than
  guessed at.

  The district comes from the workbook, never the path. Ghotki's directory holds
  files named for Dadu; Kohistan's table 1 carries the province files' suffix.
  The reader already takes the unit from the sheet's own label, so this only has
  to avoid reintroducing the filename as a source of truth.

Every observation records the file it came from and whether its layout was read
or recovered, so the 21 recovered files can be isolated in any later check.
"""
import argparse, collections, csv, json, os, pathlib, sys, warnings

warnings.filterwarnings('ignore')
HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'stage2'))

from read_workbook import (anchor, numbered_columns, header, read, unit_type, txt,
                           apply_alias, row_unit)
from read_xls import load, synthesise_banner_merges
from recover_units import plan
from headerless import synth_columns, is_empty, reason_text
from table_spec_2017 import SPEC_2017, KNOWN_ABSENT

PROVINCE = {'punjab': 'PUNJAB', 'sindh': 'SINDH', 'kp': 'KHYBER PAKHTUNKHWA',
            'balochistan': 'BALOCHISTAN', 'fata': 'FATA', 'islamabad': 'ISLAMABAD'}

FIELDS = ['census_year', 'province', 'table_id', 'district', 'unit', 'unit_type',
          'unit_source', 'locality', 'sex', 'indicator', 'col_label', 'value',
          'missing', 'src_row', 'src_col', 'src_file', 'layout']


def read_layout(rows, merges=()):
    """(a, stub, data_cols, labels) from PBS's own numbered row, or None."""
    a = anchor(rows)
    if a is None:
        return None
    lc, data_cols = numbered_columns(rows, a)
    if not data_cols:
        return None
    merges = synthesise_banner_merges(rows, merges, a, data_cols)
    return a, lc, data_cols, header(rows, a, lc, merges, data_cols)


def units_in(rows, stub, a):
    """(district, sub-district units in file order) from the sheet's own labels."""
    district, subs = None, []
    for r in rows[a + 1:]:
        if stub >= len(r):
            continue
        # Scanned across the whole row, as the reader does: Attock's table 14
        # puts its unit labels outside the stub column.
        lab, _ = row_unit(r, stub)
        if not lab:
            continue
        # MALAKAND PROTECTED AREA is a district under an alias; without applying
        # it here the roster loses Malakand and its blocks cannot be repaired.
        lab, _ = apply_alias('1', lab)
        ut = unit_type(lab)
        if ut == 'district' and district is None:
            district = lab
        elif ut:
            if lab not in subs:
                subs.append(lab)
    return district, subs


def recovered_layout(rows, ref_labels):
    """(a, stub, data_cols, labels) for a workbook with no header block.

    `a` is -1: there are no rows above the data, so the reader must not skip any.
    Labels are borrowed in order from `ref_labels`; a length mismatch means the
    recovered column set is not the published one, and is refused.
    """
    stub, cols = synth_columns(rows)
    if not cols or ref_labels is None or len(cols) != len(ref_labels):
        return None
    return -1, stub, cols, dict(zip(cols, ref_labels))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--dir', required=True, help='the dated capture directory')
    ap.add_argument('--out', required=True)
    ap.add_argument('--tables', default='', help='comma-separated subset')
    a = ap.parse_args()

    man = json.load(open(os.path.join(a.dir, 'retrieval_manifest.json')))
    want = set(a.tables.split(',')) if a.tables else set(SPEC_2017)
    want &= set(SPEC_2017)
    files = [f for f in man['files'] if f['kind'] == 'xlsx' and f['table'] in want]
    # Ghotki's table 1 is named Table--0-GHO.xls, which the manifest could not
    # parse a number out of; it is table 1 and belongs in the extract.
    if '1' in want:
        files += [f for f in man['files'] if f['kind'] == 'xlsx' and f['table'] == '?']
    print(f'{len(files)} workbooks across {len(want)} tables', flush=True)

    # The district roster, from table 1, which labels every unit in all 134
    # districts. It is what lets a workbook that omits a unit name mid-file be
    # repaired, so it has to be built before anything else is read.
    roster = {}
    dir_to_district = {}
    for f in man['files']:
        if f['kind'] != 'xlsx' or f['table'] not in ('1', '?'):
            continue
        rows, merges = load(os.path.join(a.dir, f['path']))
        lay = read_layout(rows, merges)
        if lay is not None:
            ai, stub = lay[0], lay[1]
        else:
            # Swabi's and Okara's table 1 have no header block at all; the column
            # positions still come off the widest data row, and the roster needs
            # only the stub column, not the labels.
            stub, cols = synth_columns(rows)
            ai = -1
            if not cols:
                continue
        district, subs = units_in(rows, stub, ai)
        if district:
            roster[district] = sorted(subs)
            dir_to_district[f['district']] = district
    print(f'roster: {len(roster)} districts, '
          f'{sum(len(v) for v in roster.values())} sub-district units', flush=True)

    # one reference label list per table, from a file that has its header
    refs = {}
    for f in files:
        t = '1' if f['table'] == '?' else f['table']
        if t in refs:
            continue
        rows, merges = load(os.path.join(a.dir, f['path']))
        lay = read_layout(rows, merges)
        if lay:
            _, _, dc, labels = lay
            refs[t] = [labels[c] for c in dc]
    print(f'reference headers found for {len(refs)} of {len(want)} tables', flush=True)

    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    stats = collections.Counter()
    skipped = []
    recovered_units = []
    n = 0
    with open(out / 'observations_2017.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS, quoting=csv.QUOTE_MINIMAL)
        w.writeheader()
        for i, f in enumerate(sorted(files, key=lambda f: (f['table'], f['path'])), 1):
            table = '1' if f['table'] == '?' else f['table']
            rows, merges = load(os.path.join(a.dir, f['path']))
            lay, kind = read_layout(rows, merges), 'read'
            unit_at = None
            if lay is not None and table not in ('1', '?'):
                ai, stub, _, _ = lay
                district, _ = units_in(rows, stub, ai)
                if district is None:
                    # Sixteen of table 37's workbooks carry no unit row at all -
                    # they open straight at ALL LOCALITIES - so the reader never
                    # opens a block and the file yields nothing. Those districts
                    # then go missing from the table entirely, which is how table
                    # 37 came to be short 21 districts nationally.
                    #
                    # The name comes from the district's OWN table 1, reached
                    # through the directory the capture recorded, not from the
                    # filename - 2017's filenames are unreliable. The claim is
                    # checked afterwards by the same bound applied to recovered
                    # units: the file cannot hold more people than table 1 gives
                    # that district.
                    district = dir_to_district.get(f.get('district'))
                    if district:
                        first = next((i for i in range(ai + 1, len(rows))
                                      if stub < len(rows[i]) and txt(rows[i][stub])), None)
                        if first is not None:
                            unit_at = {first: district}
                            recovered_units.append(dict(
                                opener_row=first + 1, unit=district, applied=True,
                                table=table, path=f['path'], district=district,
                                why='workbook names no unit; taken from the district roster'))
                            stats['district_rows_supplied'] += 1
                unit_at2, rep = plan(rows, stub, ai, roster.get(district, []))
                if unit_at is None:
                    unit_at = unit_at2
                elif unit_at2:
                    unit_at = {**unit_at2, **unit_at}
                for rec in rep:
                    rec.update(table=table, path=f['path'], district=district)
                    recovered_units.append(rec)
                    stats['unit_labels_recovered' if rec['applied']
                          else 'unit_labels_unrecoverable'] += 1
            if lay is None:
                if is_empty(rows):
                    stats['empty'] += 1
                    skipped.append(dict(path=f['path'], table=table, why='empty',
                                        note=reason_text(rows)))
                    continue
                lay, kind = recovered_layout(rows, refs.get(table)), 'recovered'
                if lay is None:
                    stats['refused'] += 1
                    skipped.append(dict(path=f['path'], table=table,
                                        why='no header and column count did not match the reference'))
                    continue
            stats[kind] += 1
            prov = PROVINCE.get((f.get('province') or '').lower(), (f.get('province') or '').upper())
            try:
                for o in read(rows, prov, table, spec=SPEC_2017[table], layout=lay,
                              unit_at=unit_at):
                    o.update(census_year=2017, src_file=f['path'], layout=kind)
                    w.writerow(o)
                    n += 1
            except Exception as e:
                stats['error'] += 1
                skipped.append(dict(path=f['path'], table=table, why=f'{type(e).__name__}: {e}'))
            if i % 250 == 0:
                print(f'  {i}/{len(files)}  {n:,} observations', flush=True)

    report = dict(observations=n, workbooks=len(files), layout=dict(stats),
                  skipped=skipped, recovered_unit_labels=recovered_units,
                  known_absent={f'{k[0]}|{k[1]}': v for k, v in KNOWN_ABSENT.items()})
    json.dump(report, open(out / 'extract_report_2017.json', 'w'), indent=1, sort_keys=True)
    print(f'\n{n:,} observations from {len(files)} workbooks')
    print('  layout: ' + ', '.join(f'{k} {v}' for k, v in sorted(stats.items())))
    print(f'  recovered unit labels: {stats["unit_labels_recovered"]}'
          f' ({stats["unit_labels_unrecoverable"]} could not be named)')
    print(f'  skipped: {len(skipped)}')
    for s in skipped[:8]:
        print(f'    {s["path"]} -- {s["why"]}')


if __name__ == '__main__':
    main()
