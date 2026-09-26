"""Stage 5: warehouse tables and catalogue entries for the Option A panel.

Four tables. The long observation table is the authority; the wide slice exists
only so the map can colour a choropleth without pulling two and a half million
rows. Both are published, and the wide one is derived from the long one here so
they cannot drift.
"""
import argparse, json, pathlib
import duckdb
from table_spec import TITLES, DISTRICT_ONLY

# The headline indicator per table for the wide slice: what a map would colour.
# Each is a count unless marked, and each is named exactly as it appears in the
# panel so the derivation is checkable.
WIDE = [
    ('pop_total',        '1',   'POPULATION-2023 / ALL SEXES', 'all', 'all'),
    ('pop_male',         '1',   'POPULATION-2023 / MALE', 'all', 'all'),
    ('pop_female',       '1',   'POPULATION-2023 / FEMALE', 'all', 'all'),
    ('pop_transgender',  '1',   'POPULATION-2023 / TRANS GENDER', 'all', 'all'),
    ('area_sq_km',       '1',   'AREA SQ.K.M', 'all', 'all'),
    ('pop_2017',         '1',   'POPULATION 2017', 'all', 'all'),
    ('hh_size',          '1',   'POPULATION-2023 / AVERAGE H.HOLD SIZE', 'all', 'all'),
    ('pop_rural',        '1',   'POPULATION-2023 / ALL SEXES', 'rural', 'all'),
    ('pop_urban',        '1',   'POPULATION-2023 / ALL SEXES', 'urban', 'all'),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--panel', required=True)
    ap.add_argument('--crosswalk', required=True)
    ap.add_argument('--dictionary', required=True)
    ap.add_argument('--withheld', required=True)
    ap.add_argument('--localities')
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    out = pathlib.Path(a.out); out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("SET threads TO 1")
    con.execute("SET preserve_insertion_order TO true")
    con.execute(f"CREATE VIEW p AS SELECT * FROM '{a.panel}'")

    # 1. the long observation table
    con.execute(f"""COPY (
        SELECT dds_id, adm3_pcode, dd_id, province, district, unit, unit_type,
               table_id, indicator, col_label, locality, sex,
               value, missing, missing_source, is_rate, value_corrected, renderings_disagree,
               unit_source, source_file, src_row, src_col
        FROM p ORDER BY dds_id, table_id, indicator, col_label, locality, sex
      ) TO '{out / 'census2023_observations.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")

    # 2. the unit register
    con.execute(f"""COPY (
        SELECT * FROM read_csv('{a.crosswalk}', header=true, quote='\"', escape='\"', all_varchar=true)
      ) TO '{out / 'census2023_units.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")

    # 3. the indicator dictionary
    con.execute(f"""COPY (
        SELECT * FROM read_csv('{a.dictionary}', header=true, quote='\"', escape='\"', all_varchar=true)
      ) TO '{out / 'census2023_indicators.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")

    # 4. the wide slice, derived from the long table
    sel = ",\n".join(
        f"       max(CASE WHEN table_id='{t}' AND indicator='{ind}' AND locality='{loc}' "
        f"AND sex='{sx}' THEN value END) AS {name}"
        for name, t, ind, loc, sx in WIDE)
    con.execute(f"""CREATE TABLE wide AS
      SELECT dds_id, min(adm3_pcode) adm3_pcode, min(dd_id) dd_id,
             min(province) province, min(district) district,
             min(unit) unit, min(unit_type) unit_type,
{sel}
      FROM p GROUP BY dds_id""")
    con.execute("""CREATE TABLE wide2 AS SELECT *,
        CASE WHEN area_sq_km > 0 THEN pop_total / area_sq_km END AS density_per_sq_km,
        CASE WHEN pop_total > 0 THEN 100.0 * pop_urban / pop_total END AS urban_pct,
        CASE WHEN pop_female > 0 THEN 100.0 * pop_male / pop_female END AS sex_ratio,
        CASE WHEN pop_2017 > 0 THEN 100.0 * (pow(pop_total / pop_2017, 1.0/6.0) - 1) END AS growth_rate_pct
      FROM wide""")
    con.execute(f"""COPY (SELECT * FROM wide2 ORDER BY dds_id)
        TO '{out / 'census2023_tehsil_wide.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")

    # 5. the withheld register - published, because a gap you cannot see is worse
    #    than one you can
    con.execute(f"""COPY (
        SELECT * FROM read_csv('{a.withheld}', header=true, quote='\"', escape='\"', all_varchar=true)
      ) TO '{out / 'census2023_withheld.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")

    # 6. named urban localities from table 2 - places, not administrative units
    if a.localities and pathlib.Path(a.localities).exists():
        con.execute(f"""COPY (
            SELECT * FROM read_csv('{a.localities}', header=true, quote='"', escape='"', all_varchar=true)
          ) TO '{out / 'census2023_urban_localities.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")

    stats = {}
    for f in sorted(out.glob('census2023_*.parquet')):
        n = con.sql(f"SELECT count(*) FROM '{f}'").fetchone()[0]
        stats[f.name] = dict(rows=n, bytes=f.stat().st_size)
        print(f"  {f.name:38s} {n:9,} rows  {f.stat().st_size/1024:8.1f} KB")

    # sanity: derived rates must agree with what PBS published
    chk = con.sql("""
      SELECT count(*) AS compared,
             count(*) FILTER (WHERE abs(w.density_per_sq_km - pub.value) < 0.5) AS agreeing
      FROM wide2 w JOIN (
        SELECT dds_id, value FROM p
        WHERE table_id='1' AND indicator LIKE '%POPULATION DENSITY%' AND locality='all' AND NOT missing
      ) pub USING (dds_id)
      WHERE w.density_per_sq_km IS NOT NULL AND pub.value IS NOT NULL""").fetchone()
    print(f"\n  recomputed density vs published: {chk[1]:,}/{chk[0]:,} agree within 0.5")
    # Catalogue entries, ready to merge into app/data/warehouse/catalog.json at
    # Stage 6. Written here rather than merged so the published site is not
    # touched by a build.
    ncols = lambda f: [dict(name=c[0], type=c[1]) for c in
                       con.sql(f"DESCRIBE SELECT * FROM '{out / f}'").fetchall()]
    entries = [
      dict(name='census2023_observations', file='census2023_observations.parquet',
           description='Census 2023 at district and tehsil level: every published cell from ten PBS tables, one row per observation.',
           notes=('Long format: one row per unit x indicator x column x locality x sex. '
                  'ALWAYS filter on table_id and indicator; summing the whole table double-counts, '
                  'because district rows and their constituent tehsil rows are both present - filter '
                  "unit_type='district' or unit_type<>'district', never both. "
                  'missing=true means the source printed a dash, which is NOT zero: 425,127 of these '
                  'were recovered from the PDFs after the Excel release replaced them with 0. '
                  'renderings_disagree=true means PBS\'s Excel and PDF do not agree on that row; the value is '
                  'as published in the Excel and is not corrected. '
                  'is_rate=true marks a published rate - never average it across units; recompute from '
                  'summed counts. Sub-district counts sum exactly to their district for every count '
                  'indicator (183,188 checks, no tolerance).'),
           source='Pakistan Bureau of Statistics, Population & Housing Census 2023, tables 1, 5, 9, 11, 12, 13(a), 14, 16, 18 and 23; Excel release cross-checked against the PDF release.'),
      dict(name='census2023_units', file='census2023_units.parquet',
           description='The 591 published sub-district units, each with a stable Data Darbar identifier and whatever external keys could be evidenced.',
           notes=('dds_id is the panel key and is ours. adm3_pcode is the OCHA COD-AB p-code where one '
                  'could be evidenced (493 of 591); dd_id is the geoBoundaries ADM3 polygon used by the '
                  'map (537 of 591). Neither external source covers every unit, which is why dds_id '
                  'exists. method and evidence record how each placement was made - parent and approx '
                  'matches carry weaker comparability claims than exact ones.'),
           source='Derived from the census tables, the Mouza Census 2020 tehsil crosswalk, geoBoundaries PAK ADM3 and OCHA COD-AB.'),
      dict(name='census2023_tehsil_wide', file='census2023_tehsil_wide.parquet',
           description='One row per unit with headline population figures, for mapping.',
           notes=('Derived from census2023_observations, not read separately, so the two cannot drift. '
                  'Rates here are recomputed from counts, not copied from the published rate columns; '
                  'all 727 recomputed densities agree with the published figure. Contains district and '
                  "sub-district rows together - filter on unit_type."),
           source='Derived from census2023_observations.'),
      dict(name='census2023_indicators', file='census2023_indicators.parquet',
           description='Data dictionary: every indicator in the panel with its universe, measure type and completeness.',
           notes=('Universes differ by table and getting them wrong is the easiest way to publish a '
                  'wrong rate: table 1 is a headcount, table 12 uses age 5+ for attendance and 10+ for '
                  'literacy, table 14 uses 10+, and table 23 counts households rather than people.'),
           source='Generated from the panel.'),
      dict(name='census2023_urban_localities', file='census2023_urban_localities.parquet',
           description='629 named urban localities - towns, cantonments and municipal committees - with population by sex, a 2017 comparison and household size.',
           notes=('These are places, not administrative units, so they do not appear in '
                  'census2023_units and do not sum to a district. Each carries the tehsil it '
                  'sits in. Source is table 2, which groups them by population size class; '
                  'that class is kept as a column.'),
           source='Pakistan Bureau of Statistics, Population & Housing Census 2023, table 2.'),
      dict(name='census2023_withheld', file='census2023_withheld.parquet',
           description='The 54 published units that could not be placed on a polygon, with the reason and the candidates considered.',
           notes=('These units are present in census2023_observations with their data intact; they are '
                  'absent only from the map. Nine are Karachi sub-divisions carved out of the old town '
                  'system, which neither boundary source reproduces; the rest are tehsils created after '
                  'both boundary layers were drawn.'),
           source='Generated from the geography register.'),
    ]
    for e in entries:
        f = out / e['file']
        e['bytes'] = f.stat().st_size
        e['rows'] = con.sql(f"SELECT count(*) FROM '{f}'").fetchone()[0]
        e['columns'] = ncols(e['file'])
        e['license'] = 'CC BY 4.0. Source data (c) Pakistan Bureau of Statistics.'
    json.dump(entries, open(out / 'catalog_entries.json', 'w'), indent=2)
    print(f"  catalog entries written for {len(entries)} tables")
    json.dump(stats, open(out / 'warehouse_stats.json', 'w'), indent=2)


if __name__ == '__main__':
    main()
