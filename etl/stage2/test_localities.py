"""Go/no-go for the locality tier: is a composite key unique within a tehsil?"""
import argparse, collections, pathlib
import openpyxl
from read_localities import read
from read_workbook import merged_ranges

REGIONS = {'kp': 'KHYBER PAKHTUNKHWA', 'punjab': 'PUNJAB', 'sindh': 'SINDH',
           'balochistan': 'BALOCHISTAN', 'islamabad': 'ISLAMABAD'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    a = ap.parse_args()
    src = pathlib.Path(a.src)
    for t in ('31', '32', '33', '34'):
        rows = []
        for reg, prov in REGIONS.items():
            f = src / 'xlsx' / f'table_{t}_{reg}.xlsx'
            if not f.exists():
                continue
            ws = openpyxl.load_workbook(f, read_only=True, data_only=True).worksheets[0]
            g = [list(r) for r in ws.iter_rows(values_only=True)]
            rows += list(read(g, prov, t, merged_ranges(f)))
        # key = province + district + sub-district + grouping + name + hadbast
        keys = collections.Counter(
            (r['province'], r['district'], r['sub_district'], r['patwar_circle'],
             r['locality'], r['charge'], r['name'], r['hadbast']) for r in rows)
        dup = sum(v - 1 for v in keys.values() if v > 1)
        # and without the grouping, to see how much work the grouping is doing
        loose = collections.Counter(
            (r['province'], r['district'], r['sub_district'], r['name']) for r in rows)
        ldup = sum(v - 1 for v in loose.values() if v > 1)
        hb = sum(1 for r in rows if r['hadbast'])
        lv = collections.Counter(r['level'] for r in rows)
        print(f"    levels: {dict(lv)}")
        print(f"T{t}: {len(rows):6,} localities | unique on full key {100*(1-dup/max(len(rows),1)):6.2f}% "
              f"| unique on name+tehsil only {100*(1-ldup/max(len(rows),1)):6.2f}% "
              f"| with an identifier {100*hb/max(len(rows),1):5.1f}%")


if __name__ == '__main__':
    main()
