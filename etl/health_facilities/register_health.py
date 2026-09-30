"""The two health-facility tables, for build_web_warehouse.py.

register_tables() is called from the full warehouse build; update_warehouse()
publishes just these two into an existing warehouse (build_web_warehouse.py
--only health), stamping the catalogue's shelf, period and keys the same way
the full build does.
"""
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
ALHASAN = 'health_facilities_pk_2017-11-15.csv.gz'
OSM = 'healthsites_osm_2019-10-09.csv.gz'
SOURCE_ALHASAN = ('ALHASAN Systems Private Limited, Pakistan Health Facilities (15 November 2017), '
                  'published on the Humanitarian Data Exchange under CC0 - '
                  'https://data.humdata.org/dataset/pakistan-health-facilities')
SOURCE_OSM = ('© OpenStreetMap contributors, exported by healthsites.io (9 October 2019); '
              'Open Database Licence (ODbL) 1.0 - https://healthsites.io, '
              'https://www.openstreetmap.org/copyright')

PLACE = {
    'lat': 'latitude, WGS84, 6 dp', 'lng': 'longitude, WGS84, 6 dp',
    'district_code': 'PBS Census 2023 district code of the polygon the point falls in; joins place_indicators and the census panels',
    'district_2023': 'that district’s name on the PBS 2023 layer',
    'province_2023': 'its province or territory on the PBS 2023 layer',
    'district_key': 'Data Darbar’s 147-district key of the polygon the point falls in; joins district_indicators, mpi_districts, health_access_district',
    'release': 'the source’s own date', 'source': 'source and licence',
}


def register_tables(register, d=HERE):
    d = Path(d)
    register(
        'health_facilities_pk',
        'Public and private health facilities of Pakistan with positions: 25,704 hospitals, basic health units, '
        'rural health centres, dispensaries, clinics, laboratories, pharmacies and others (ALHASAN Systems, 2017).',
        'A 2017 snapshot: facilities opened or closed since are not reflected, and status is recorded for only 619 '
        'rows. 7,399 are government and 18,305 private. care_group sorts the categories into care types; '
        'in_care_layer marks the 20,536 the Places map draws, leaving out medical stores (4,484), veterinary (347), '
        'opticians (334), ambulance services and the morgue. Positions are precise (4 to 6 decimal places, only 57 '
        'points share a location) and sit in the district the source names in 86 per cent of rows; nearly all the '
        'rest are spelling differences or 2017-to-2023 district splits (Karachi, Chitral, Duki, Sujawal), not '
        'misplaced points. Against Census 2023’s count of hospital buildings, district totals of these hospitals '
        'correlate at 0.88 (log scale), and only eight districts have none. The public network is not complete: '
        'official counts of basic health units and dispensaries are higher than the 4,023 and 1,476 here. Phone, '
        'mobile, email, URL and street address are left out. district_code and district_key come from the point’s '
        'position, not the source’s district column. Published by ALHASAN Systems under CC0.',
        {'row_id': 'row number in this release', 'source_object_id': 'the source’s OBJECTID',
         'name': 'facility name as published', 'category': 'the source’s category, e.g. Basic Health Unit, General Physician, Medical Stores',
         'sub_category': 'the source’s sub-category: BHU, RHC, THQ, DHQ, MCH, CLINIC, MEDICAL SERVICES ...',
         'sector': 'Government or Private', 'care_group': 'Hospital | Primary care | Mother and child | Clinic | Traditional medicine | Laboratory and diagnostics | TB and leprosy; empty where not a care facility',
         'in_care_layer': 'true for the care facilities the Places map draws',
         'facility_class': 'the source’s class, where given (619 rows)', 'status': 'Functional | Not Functional | Closed, where given (619 rows)',
         'beds': 'beds, where given (105 rows)', 'locality': 'locality as published', 'union_council': 'union council as published',
         'town': 'town as published', 'mauza': 'mauza as published', 'tehsil': 'tehsil as the source writes it',
         'district': 'district as the source writes it (2017 names)', 'province': 'province as the source writes it', **PLACE},
        SOURCE_ALHASAN,
        f"SELECT * FROM read_csv('{(d / ALHASAN).as_posix()}', header=true, auto_detect=true, sample_size=-1, "
        "types={'district_code':'VARCHAR','beds':'INTEGER'}) ORDER BY row_id",
        unit='facilities')
    register(
        'healthsites_osm_2019',
        'Health amenities of Pakistan mapped on OpenStreetMap, as exported by healthsites.io in October 2019: 2,638 '
        'hospitals, clinics, pharmacies, doctors and dentists.',
        'Volunteer-mapped and uneven: most points were last edited in 2016 to 2018, cities are far better covered '
        'than rural districts, and 50 of 156 districts have no clinic, hospital, doctor or dentist in it at all. '
        'District totals correlate with Census 2023’s hospital buildings at 0.68, against 0.88 for '
        'health_facilities_pk, and 81 per cent of these care points have an ALHASAN facility within 250 metres, so '
        'for coverage use health_facilities_pk and treat this as a cross-check. amenity is OSM’s tag. Contact '
        'numbers and the editors’ usernames are left out. This table is OpenStreetMap data under the ODbL, not '
        'CC BY: a database made from it must credit OpenStreetMap contributors and stay under the ODbL.',
        {'row_id': 'row number in this release', 'osm_id': 'OpenStreetMap element id', 'osm_type': 'node or way',
         'healthsites_uuid': 'healthsites.io identifier', 'name': 'name as mapped (125 rows have none)',
         'amenity': 'OSM amenity tag: hospital | clinic | pharmacy | doctors | dentist',
         'healthcare': 'OSM healthcare tag, where set', 'speciality': 'OSM healthcare:speciality, where set',
         'operator': 'operator, where set', 'operator_type': 'operator type, where set', 'beds': 'beds, where set',
         'emergency': 'emergency department, where set', 'opening_hours': 'opening hours, where set',
         'wheelchair': 'wheelchair access, where set', 'address': 'address as mapped, where set',
         'completeness_pct': 'healthsites.io’s own completeness score', 'last_edited': 'date the element was last edited on OSM',
         **PLACE},
        SOURCE_OSM,
        f"SELECT * FROM read_csv('{(d / OSM).as_posix()}', header=true, auto_detect=true, sample_size=-1, "
        "types={'district_code':'VARCHAR','beds':'VARCHAR'}) ORDER BY row_id",
        unit='facilities')


def update_warehouse(out):
    import sys
    import duckdb
    sys.path.insert(0, str(HERE.parent))
    from catalog_meta import KIND, TIME_COLS, KEY_COLS
    out = Path(out)
    con = duckdb.connect()
    cat = json.loads((out / 'catalog.json').read_text())
    samples = json.loads((out / 'catalog_samples.json').read_text())

    def register(name, desc, notes, columns, source, sql, unit=None):
        target = out / (name + '.parquet')
        con.sql(f"COPY ({sql}) TO '{target.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)")
        p = target.as_posix()
        schema = con.sql(f"DESCRIBE SELECT * FROM '{p}'").fetchall()
        cols = [c[0] for c in schema]
        entry = {'name': name, 'file': name + '.parquet', 'bytes': target.stat().st_size,
                 'rows': con.sql(f"SELECT count(*) FROM '{p}'").fetchone()[0],
                 'description': desc, 'notes': notes, 'unit': unit, 'source': source,
                 'columns': [{'name': c[0], 'type': c[1], 'description': columns.get(c[0], '')} for c in schema],
                 'kind': KIND[name], 'keys': [k for k in KEY_COLS if k in cols]}
        tc = next((c for c in TIME_COLS if c in cols), None)
        if tc:
            lo, hi = con.sql(f'SELECT min("{tc}")::VARCHAR, max("{tc}")::VARCHAR FROM \'{p}\'').fetchone()
            entry['span'] = {'column': tc, 'from': lo, 'to': hi}
        cat['tables'] = sorted([t for t in cat['tables'] if t['name'] != name] + [entry],
                               key=lambda t: t['name'])
        samples[name] = [[None if v is None else str(v)[:80] for v in r]
                         for r in con.sql(f"SELECT * FROM '{p}' LIMIT 5").fetchall()]
        print(f'  {name:<24} {entry["rows"]:>7,} rows  {entry["bytes"]/1e6:.2f} MB')

    register_tables(register)
    (out / 'catalog_samples.json').write_text(json.dumps(samples, ensure_ascii=False, separators=(',', ':')))
    (out / 'catalog.json').write_text(json.dumps(cat, indent=1))
    con.close()
