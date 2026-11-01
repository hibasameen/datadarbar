#!/usr/bin/env python3
"""
The 160-district COD-AB frame onto PBS's Census-2023 districts.

Adaad's datasets (climate, fires, forest, land cover, floods, PM2.5, the DISCO
crosswalk) are keyed on the 160-district frame built from OCHA's COD-AB v01 in
raw_data/geospatial/boundaries/cod-ab-pak-2026-09-26/derived/: the 136 census
districts dissolved from COD tehsils, plus Azad Jammu & Kashmir and
Gilgit-Baltistan carried through from COD's own districts. Its district_key is
an upper-case name - not Data Darbar's district_key, the lower-case slug on a
2015 frame - and nothing on the map joins to it. This table is the join: one
row per frame district, naming the PBS district_code it belongs to. The frame's
key is published as cod_district_key, so the catalogue never offers it as
Data Darbar's own.

Three ways in, tried in this order, and matched_by says which one held:

  tehsil codes  the frame district's COD tehsils (adm3_pcodes) are found on
                PBS's tehsil layer, which carries adm3_pcode, and all of them
                sit in one PBS district. 127 districts.
  name          no COD tehsil code survives on the PBS layer - all of AJK and
                GB, and nine census districts created or redrawn after 2022 -
                so the name is matched, "DISTRICT" dropped, with two spellings
                aliased (Malakand is PBS's Malakand Protected Area, Diamir its
                Diamer). 29 districts.
  area overlap  COD draws Gilgit-Baltistan in 14 districts and PBS in 10. The
                four with no PBS namesake go to the PBS district holding most of
                their area: Darel and Tangir to Diamer, Gupis Yasin to Ghizer,
                Rondu to Skardu. 4 districts.

Every match is then measured: overlap_share is the part of the frame polygon
lying inside the PBS polygon it was matched to. The two layers are different
digitisations of the same borders, so this is rarely 1; where it falls under
0.8, or where the polygon lies mostly in a different PBS district, the note
says so. That happens most in Karachi, Killa Abdullah and Chaman, and in
Kharmang: matched by name, but COD draws it larger, and 56 per cent of it
lies inside PBS's Skardu. The assignment stands - a dataset keyed on the frame
means the district by name - but it is why a GB value moved onto the PBS map
is approximate.

The p-code is COD's own, so a table keyed on COD districts rather than the
frame joins on cod_adm2_pcode. Surab carries PK233 - it is COD's Shaheed
Sikandarabad renamed, whole - though the frame leaves it blank; Keamari, built
from three Karachi towns, has none. COD's Lehri (PK218) has no row because the
frame has no Lehri: its two tehsils went to different districts, Bhag to
Kachhi and Lehri to Sibi, so a Lehri value cannot be placed whole on either.

Where several frame districts fold into one PBS district (relation 'merged':
Diamer, Ghizer, Skardu), counts are summed and a mean or a share needs
weighting: area_weight is each frame district's share of the group's area, and
sums to 1 in every PBS district. For anything people-weighted, weight by
population instead.

Inputs  --frame     districts_2023_frame.geojson (160)
        --pbs-d     pbs_districts_2023.geojson (157 with the disputed polygon)
        --pbs-t     pbs_tehsils_2023.geojson (carries adm3_pcode)
Output  district_crosswalk_cod_2023.csv  one row per frame district
"""
import argparse, json
from pathlib import Path

import pandas as pd
from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform
from shapely.validation import make_valid

# frame spelling -> PBS spelling, both without " DISTRICT"
ALIASES = {'MALAKAND': 'MALAKAND PROTECTED AREA', 'DIAMIR': 'DIAMER'}
# frame districts that are a COD district renamed, whole: the frame leaves the p-code
# blank, but a table keyed on COD p-codes still names them by it
RENAMED_PCODE = {'SURAB': ('PK233', 'COD’s Shaheed Sikandarabad (PK233), renamed; '
                                    'its one tehsil is the whole district')}
MIN_OVERLAP = 0.85   # the least share of a frame district a parent must hold
FLAG_BELOW = 0.8     # overlap_share under this gets a note

ap = argparse.ArgumentParser()
for k in ('frame', 'pbs-d', 'pbs-t', 'out'):
    ap.add_argument('--' + k, required=True, type=Path)
a = ap.parse_args()

# equal-area, so shares are shares of ground and not of degrees
ea = Transformer.from_crs(4326, 6933, always_xy=True).transform


def load(p, skip=None):
    out = []
    for f in json.load(open(p))['features']:
        pr = f['properties']
        if skip and skip(pr):
            continue
        out.append((pr, transform(ea, make_valid(shape(f['geometry'])))))
    return out


def bare(name):
    return name.upper().removesuffix(' DISTRICT').strip()


frame = load(a.frame)
pbs = load(a.pbs_d, skip=lambda p: p['district_code'] in ('0', '000'))
teh = [p for p, _ in load(a.pbs_t, skip=lambda p: p['tehsil_code'] == '000')]

code_of_pcode = {p['adm3_pcode']: p['district_code'] for p in teh if p.get('adm3_pcode')}
by_name = {}
for p, _ in pbs:
    by_name.setdefault(bare(p['district']), []).append(p['district_code'])
assert all(len(v) == 1 for v in by_name.values()), 'PBS district names are not unique'
pbs_geom = {p['district_code']: g for p, g in pbs}
pbs_prop = {p['district_code']: p for p, _ in pbs}

rows = []
for p, g in frame:
    key = p['district_key']
    overlaps = sorted(((g.intersection(pg).area / g.area, c) for c, pg in pbs_geom.items()
                       if g.intersects(pg)), reverse=True)
    best_share, best_code = overlaps[0]

    codes = {code_of_pcode[c] for c in (p['adm3_pcodes'] or '').split(',') if c in code_of_pcode}
    name = ALIASES.get(key, key)
    if len(codes) == 1:
        code, how = codes.pop(), 'tehsil codes'
    elif not codes and name in by_name:
        code, how = by_name[name][0], 'name'
    elif not codes:
        assert best_share >= MIN_OVERLAP, f'{key}: best parent holds only {best_share:.2f}'
        code, how = best_code, 'area overlap'
    else:
        raise SystemExit(f'{key}: COD tehsils fall in several PBS districts {sorted(codes)}')

    share = g.intersection(pbs_geom[code]).area / g.area
    notes = []
    if best_code != code:
        notes.append(f'COD draws it differently: {share:.0%} lies inside '
                     f'{pbs_prop[code]["district"].title()}, {best_share:.0%} inside '
                     f'{pbs_prop[best_code]["district"].title()}')
    elif share < FLAG_BELOW:
        note = (f'COD and PBS draw it differently: {share:.0%} of the COD polygon lies '
                f'inside PBS’s {pbs_prop[code]["district"].title()}')
        second = next(((s, c) for s, c in overlaps if c != code), None)
        if second and second[0] >= 0.05:
            note += f', {second[0]:.0%} inside {pbs_prop[second[1]]["district"].title()}'
        notes.append(note)
    if p.get('clean_union') is False:
        notes.append('not a clean union of COD tehsils in the frame (see the frame’s '
                     'district_flags.csv)')
    pcode = p.get('cod_adm2_pcode') or None
    if key in RENAMED_PCODE:
        pcode, why = RENAMED_PCODE[key]
        notes.append(why)
    elif not pcode:
        notes.append('no COD p-code: built from COD towns after COD v01')

    rows.append(dict(cod_district_key=key, district_2023=p['district_2023'],
                     cod_adm2_pcode=pcode, province=p['province'],
                     district_code=code, pbs_district=pbs_prop[code]['district'],
                     pbs_province=pbs_prop[code]['province'], matched_by=how,
                     frame_area_km2=round(g.area / 1e6, 1),
                     overlap_share=round(share, 3), note='; '.join(notes) or None))

df = pd.DataFrame(rows)
n_in = df.groupby('district_code').cod_district_key.transform('size')
same = df.cod_district_key == df.pbs_district.map(bare)
df['relation'] = 'merged'
df.loc[(n_in == 1) & same, 'relation'] = 'same'
df.loc[(n_in == 1) & ~same, 'relation'] = 'renamed'
df['area_weight'] = (df.frame_area_km2 / df.groupby('district_code').frame_area_km2
                     .transform('sum')).round(4)

# the checks that make this a crosswalk rather than a guess
assert df.cod_district_key.is_unique and len(df) == 160, len(df)
missing = set(pbs_geom) - set(df.district_code)
assert not missing, f'PBS districts with no frame district: {sorted(missing)}'
assert (df.groupby('district_code').area_weight.sum().sub(1).abs() < 0.001).all()

cols = ['cod_district_key', 'district_2023', 'cod_adm2_pcode', 'province', 'district_code',
        'pbs_district', 'pbs_province', 'relation', 'matched_by', 'frame_area_km2',
        'area_weight', 'overlap_share', 'note']
df = df[cols].sort_values(['pbs_province', 'district_code', 'cod_district_key'])
df.to_csv(a.out, index=False)

print(f'{len(df)} frame districts onto {df.district_code.nunique()} PBS districts')
print(df.groupby(['matched_by', 'relation']).size().to_string())
print('\nmerged:')
print(df[df.relation == 'merged'][['cod_district_key', 'pbs_district', 'area_weight',
                                   'overlap_share']].to_string(index=False))
print('\nnoted:')
print(df[df.note.notna()][['cod_district_key', 'pbs_district', 'overlap_share', 'note']]
      .to_string(index=False))
