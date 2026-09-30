"""Table 2: named urban localities.

Table 2's rows are places, not administrative units — a named town or
cantonment, its parent tehsil, and its population. It does not belong in the
unit panel, where every row has to be a published census unit, so it is built
separately into its own table.
"""
import argparse, csv, pathlib, re
import openpyxl
from read_workbook import anchor, numbered_columns, header, merged_ranges, txt, cell, unit_type, ADMIN

REGIONS = {'kp': 'KHYBER PAKHTUNKHWA', 'punjab': 'PUNJAB', 'sindh': 'SINDH',
           'balochistan': 'BALOCHISTAN', 'islamabad': 'ISLAMABAD'}
SIZE = re.compile(r'^[\d,]+\s*[-–]+\s*[\d,]+$|AND ABOVE|LESS THAN|UN-?INHABITED')


def read_sheet(rows, an, stub, data_cols, cols, area, source):
    """[dict] of named urban localities from one table-2 sheet.

    Factored out so Census 2017 can call it: the table has the same shape in both
    years - a district row, then a size-class row, then one row per named locality
    with its parent tehsil in the second column - but 2023 publishes one workbook
    per region and 2017 one per district. Two copies of this walk would drift, as
    a duplicated vocabulary already did elsewhere in this pipeline.
    """
    out = []
    district = size_class = None
    for i, r in enumerate(rows):
        if i <= an:
            continue
        t = txt(r[stub]) if stub < len(r) else None
        if not t:
            continue
        U = t.upper()
        if U.isdigit() or U.startswith(('TABLE', 'URBAN LOCALITIES')):
            continue
        if ADMIN.search(U) and unit_type(t) == 'district':
            district, size_class = t, None
            continue
        if SIZE.search(U):
            size_class = t
            continue
        vals, miss = {}, []
        for c in data_cols:
            if c >= len(r):
                continue
            v, kind = cell(r[c])
            name = cols.get(c, f'col{c+1}')
            if kind == 'text' and c == data_cols[0]:
                vals['tehsil'] = v          # column 2 names the parent tehsil
            elif kind == 'number':
                vals[name] = v
            elif kind == 'dash':
                miss.append(name)
        if not any(k for k in vals if k != 'tehsil'):
            continue
        out.append(dict(province_area=area, district=district,
                        tehsil=vals.pop('tehsil', None), locality=t,
                        size_class=size_class, missing=';'.join(miss),
                        source_file=source, src_row=i + 1, **vals))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    out = pathlib.Path(a.out); out.mkdir(parents=True, exist_ok=True)
    rows_out = []
    for reg, prov in REGIONS.items():
        f = pathlib.Path(a.src) / 'xlsx' / f'table_2_{reg}.xlsx'
        if not f.exists():
            continue
        ws = openpyxl.load_workbook(f, read_only=True, data_only=True).worksheets[0]
        rows = [list(r) for r in ws.iter_rows(values_only=True)]
        an = anchor(rows)
        stub, data_cols = numbered_columns(rows, an)
        cols = header(rows, an, stub, merged_ranges(f), data_cols)
        rows_out += read_sheet(rows, an, stub, data_cols, cols, prov, f.name)

    keys = ['province_area', 'district', 'tehsil', 'locality', 'size_class']
    extra = [k for k in dict.fromkeys(k for r in rows_out for k in r)
             if k not in keys + ['missing', 'source_file', 'src_row']]
    fields = keys + extra + ['missing', 'source_file', 'src_row']
    rows_out.sort(key=lambda r: (r['province_area'], r['district'] or '', r['locality']))
    with open(out / 'urban_localities.csv', 'w', newline='') as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction='ignore')
        w.writeheader(); w.writerows(rows_out)
    print(f"urban localities  {len(rows_out)}")
    print(f"  with a parent tehsil  {sum(1 for r in rows_out if r['tehsil'])}")
    print(f"  districts covered     {len({r['district'] for r in rows_out})}")
    print(f"  columns               {extra}")


if __name__ == '__main__':
    main()
