# The government school layer (release 2026-09)

The tables in this folder are the data behind Adaad's *How far is the girls'
school?* (September 2026 issue): one row per listed government school with a
position and its provenance, the district distance statistics computed from
those positions, the coverage ledger, and the external-validity tests against
the Mouza Census 2020. `build_web_warehouse.py` reads them into the public
warehouse as `schools_pk`, `school_access_district`, `school_distance_stats`,
`school_layer_coverage`, `school_validation_district`,
`school_validation_tehsil`, `school_validation_summary`,
`census_enrolment_5_16_by_sex` and `school_access_tehsil`.

The journal's copy of the same tables, frozen on the issue date, is at
adaad.org/datasets/. This folder is the canonical, updatable version: when a
region is filled (the ex-FATA merged districts, the other eight AJK
districts), a new release tag goes on every row and the old files stay in git.

## Files

| file | rows | what |
|---|---:|---|
| `schools_pk_2026-09.csv.gz` | 125,317 | one row per listed government school; 121,020 positioned; 118,673 in the analysis |
| `school_access_district.csv` | 132 | median distance to the nearest girls' and boys' school, enrolment aged 5–16 by sex, school counts |
| `school_distance_stats.csv` | 832 | the full distance distributions by region and district × sex × level |
| `school_layer_coverage.csv` | 9 | what each region lists, what the layer positions, where positions come from |
| `school_validation_district.csv` | 147 | Mouza Census count floors and village-reported distances against the layer, by district |
| `school_validation_tehsil.csv` | 403 | the count floors by tehsil (Sindh, Punjab, Balochistan, GB) |
| `school_validation_summary.csv` | 18 | rank agreement between the layer's district distances and the villages' reports |
| `census_enrolment_5_16_by_sex.csv` | 130 | Census 2023 Table 13(b) on the 2017 district frame |
| `school_access_tehsil.csv` | 498 | the district distance measure recomputed on Data Darbar's 553 ADM3 polygons; feeds the map's Education → Distance to School layer |

## How the school table was built

Each province's register was read from its public portal and harmonised to one
schema by `build_schools_pk.py`, with the same filters the analysis applied,
so `in_analysis` reproduces the network sizes in the piece exactly (Balochistan
9,048 boys' / 3,932 girls'; Sindh 33,197 / 6,544; KP 16,620 / 10,841; Punjab
17,221 / 19,074; GB 886 / 469; AJK 386 / 377; Islamabad 28 / 50) and the
per-district middle-plus counts of its Figure 1 (13,754 girls', 18,060 boys').

| region | register | positions |
|---|---|---|
| Balochistan | SED open-data portal, 14,709 schools | GPS at source for 12,980 |
| Sindh | SELD Institution Checker roster, 41,383 schools (856 office rows dropped) | RSU district GIS pins where a pin lies within 300 m of the position solved from the SELD Distance Checker (23,887); otherwise the solved position (17,470); 22 unsolved |
| KP, settled districts | KPEMA school locator, 27,689 schools | KPEMA per-school detail for every non-primary school and for Mardan (8,562); the JSiMS mirror's coordinates for 19,120 primaries (median 29 m from KPEMA on a validation sample) |
| Punjab | SIS roster, 38,149 schools | geocoded from the settlement in the school's name against GeoNames/OSM: 18,339 at settlement precision, 8,179 at the markaz (school-cluster) centroid, 9,777 at the tehsil centroid, 1,854 unplaced |
| Gilgit-Baltistan | GB EMIS roster, 1,890 schools | 141 matched to an OSM school point, 859 to a settlement, 788 to their cluster's centroid, 102 unplaced; the roster has no sex field, so sex is read from the name (448 unknown) |
| AJK | Mirpur and Kotli exam-board rosters, 1,367 government schools | settlement geocoding, 784 placed |
| Islamabad | OpenStreetMap, 130 Federal Directorate of Education institutions | OSM feature positions |

The Sindh positions deserve a sentence. The RSU map pins were found to sit
more than 1 km from the school for 15 per cent of schools, so every school was
re-positioned by multilateration on the sphere from the distances the SELD
Distance Checker returns between it and 540 seed schools (1.2 million
distances). `coord_resid_m` is the residual of that solve and
`pin_vs_solved_m` the disagreement with the pin, kept so the choice can be
audited row by row.

`district_key` is the register's district mapped to Data Darbar's 147-district
frame, with districts created after 2017 folded into their parents (Lower and
Upper Chitral into Chitral, the three Kohistans, Duki into Loralai, Chaman into
Killa Abdullah, Surab into Kalat, Kot Addu into Muzaffargarh, and so on).
`district_key_boundary` is the polygon the point falls in. They differ for
3,532 rows: border villages, and the Punjab rows placed at a tehsil centroid.

## The tehsil layer on the map

`build_tehsil_access.py` recomputes the piece's distance measure on the same
1 km grid, then aggregates it to the 553 ADM3 polygons the Mouza Census layers
use instead of the 147 districts. The district aggregates it writes alongside
reproduce `school_distance_stats` to within 0.001 km, which is the check that
the two runs are the same method. A tehsil keeps its row only when at least
half of its population lay inside the analysed region: the 55 polygons dropped
are the merged districts, AJK's other eight districts, and the slivers of those
that a neighbouring district polygon happens to cover. `build_map_payload.py`
writes the wide table (`school_access_tehsil.csv`) and injects it as the
`schools` table of `window.DD_POV` in `app/data/poverty_data.js`, which the
map's `schoolAccess` group reads through the same `pov` mechanism as the
satellite layers. Re-running it replaces the table; nothing else in that file
changes.

## What the validation tests can and cannot say

The Mouza Census asked every rural mouza in 2020 whether an institution for
boys and one for girls exists in the village at each level and, if not, how
far the nearest one is. It is sector-blind and counts villages, so it cannot
audit a school list line by line. It can put a floor under the number of
institutions of each sex and level in a district (test A) or tehsil (test B),
and it gives an independent distance to the nearest institution of each sex
(test C). `validate_released_layer.py` runs all three on the `in_analysis`
rows of `schools_pk`, so the tables describe the released layer, not the raw
registers.

On this release the layer clears its floors in every province at middle and
high level (girls' middle-plus ratios: Balochistan 1.87, Sindh 1.44, KP 1.15,
Punjab 1.22, GB 0.99, AJK 1.01). Punjab's primary ratios are 0.53 and 0.61
because private schools serve those villages; GB's girls' primary ratio is
0.61 because sex is inferred from names. Sindh's "girls undercounted relative
to boys" flag fires in 20 districts by construction, because designated boys'
schools include the Mixed majority; the girls' ratio itself is below 0.9 only
in Tharparkar (0.67), Sujawal (0.81), Tando Allahyar (0.85) and Ghotki (0.87).
On distances, the layer's ordering of districts by how far girls are from a
middle school agrees with the villages' (Spearman 0.65 nationally, 0.76 in KP)
and the two sources agree on the sign of the girls-minus-boys gap in 92 to
100 per cent of districts in Balochistan, KP and Sindh, but in only 40 per
cent in Punjab, where both put the gap within a kilometre of zero. The
ordering by the size of the gap is confirmed at primary and middle level and
only weakly at high level (Balochistan 0.05).

## Why publish positions of girls' schools

Every position in `schools_pk` is either published by a provincial education
department (Balochistan's portal, KPEMA's locator, Sindh's RSU pins), solved
from distances the Sindh department's own public distance checker returns, or
geocoded from a settlement name in a public roster. Compiling them lowers the
cost of finding a school; it adds no fact a department has not itself put
online. The table therefore carries nothing beyond the registers — no head
teacher, no telephone number, no enrolment beyond what the SELD roster
publishes — and no attempt was made to locate schools in the ex-FATA merged
districts or the eight AJK districts whose departments publish no list.

## Licence

The compilation is released under CC BY 4.0, as is everything on Data Darbar.
The underlying records remain the departments'; cite them (the `source` and
`source_url` columns) alongside Adaad when reusing the table.

## Rebuilding

```
python3 etl/schools/build_schools_pk.py --src <Adaad/data/pk-school-access> \
    --darbar app/data/districts.json \
    --geojson app/data/pakistan_districts_province_boundries.geojson --out /tmp/rel
python3 etl/schools/validate_released_layer.py /tmp/rel/schools_pk_2026-09.csv.gz \
    <rerun-2026-09/filled_designation> <mc2020_mouza_level.csv.gz> /tmp/rel/validation
python3 etl/schools/prepare_release.py --rerun <rerun-2026-09> --validation /tmp/rel/validation \
    --schools /tmp/rel/schools_pk_2026-09.csv.gz --darbar app/data/districts.json --out /tmp/rel/release
python3 etl/schools/build_tehsil_access.py --schools etl/schools/schools_pk_2026-09.csv.gz \
    --pop <raw_data/geospatial/pak_ppp_2020_1km_Aggregated_UNadj.tif> \
    --districts app/data/pakistan_districts_province_boundries.geojson \
    --tehsils app/data/tehsils_geo.js --out /tmp/rel/tehsil
python3 etl/schools/build_map_payload.py --stats /tmp/rel/tehsil/tehsil_distance_stats.csv \
    --counts /tmp/rel/tehsil/tehsil_school_counts.csv --pov app/data/poverty_data.js \
    --out etl/schools/school_access_tehsil.csv
python3 etl/build_web_warehouse.py
```

The first three scripts need the Adaad working folder (the per-province
registers, the analysis rerun and the Mouza Census microdata); the tehsil pair
needs only this folder plus the WorldPop raster (rasterio, geopandas, scipy);
the warehouse build needs only this folder.
