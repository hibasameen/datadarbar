#!/usr/bin/env python3
"""Agricultural Census 2010: the parsed tables as three warehouse Parquet files.

    python3 etl/agri_census_2010/build_warehouse.py <raw_data/pbs/agricultural_census_2010/derived> <app/data/warehouse>

Reads what parse_tables.py and build_district_headline.py wrote and changes no
figure: the long table gains each table's title and area note, the column index
is published beside it because the long table carries PBS's printed column
numbers rather than headers, and the headline table is as built.

The geography is the 2010 district frame under PBS's own unit names. Nothing is
keyed to the 2023 districts: that needs a crosswalk that does not exist yet.
"""
import pathlib, sys
import duckdb


def main(src, out):
    src, out = pathlib.Path(src), pathlib.Path(out)
    con = duckdb.connect()
    con.execute("SET threads TO 1")
    rd = lambda f: f"read_csv('{(src / f).as_posix()}', header=true, all_varchar=true, quote='\"', escape='\"', sample_size=-1)"
    con.execute(f"CREATE TABLE cat AS SELECT * FROM {rd('tables_catalog.csv')}")
    # One block in the source breaks across a page (Khyber Pakhtunkhwa, Table 6.12: Bajaur
    # Agency's last size class), leaving 14 cells with no unit. They take the unit of the row
    # before them, in file order, and say so in qc.
    con.execute(f"""CREATE TABLE lng AS
        SELECT * EXCLUDE (unit, qc, rn),
               coalesce(unit, last_value(unit IGNORE NULLS) OVER (ORDER BY rn)) AS unit,
               CASE WHEN unit IS NULL THEN 'unit_carried' ELSE qc END AS qc
        FROM (SELECT *, row_number() OVER () AS rn FROM {rd('agri_census_2010_long.csv.gz')})""")
    carried = con.sql("SELECT count(*), any_value(unit) FROM lng WHERE qc = 'unit_carried'").fetchone()
    print(f'  {carried[0]} cells without a unit carried from the row above ({carried[1]})')
    con.execute(f"CREATE TABLE hd AS SELECT * FROM read_csv('{(src / 'agri_census_2010_district_headline.csv').as_posix()}', header=true, sample_size=-1)")
    order = "CAST(split_part(table_id, '.', 1) AS INTEGER), CAST(split_part(table_id, '.', 2) AS INTEGER)"
    con.execute("""CREATE TABLE t AS
        SELECT scope AS source_file, "table" AS table_id,
               CAST(split_part("table", '.', 1) AS INTEGER) AS chapter,
               any_value(title) AS table_title, any_value(unit_note) AS notes
        FROM cat GROUP BY 1, 2, 3""")
    con.execute(f"""CREATE TABLE agri_census_2010_long AS
        SELECT l.scope AS source_file, l."table" AS table_id, t.chapter, t.table_title, t.notes,
               l.unit, coalesce(h.unit_type, CASE WHEN l.unit = 'PAKISTAN' THEN 'country'
                                                   WHEN l.unit LIKE '%PROVINCE%' THEN 'province' END) AS unit_level,
               l."group", l.row_label AS category, CAST(l.col AS INTEGER) AS col_index,
               l.value_raw, CAST(l.value AS DOUBLE) AS value, l.flag, l.qc, CAST(l.pdf_page AS INTEGER) AS pdf_page
        FROM lng l LEFT JOIN t ON t.source_file = l.scope AND t.table_id = l."table"
             LEFT JOIN (SELECT DISTINCT unit, unit_type FROM hd) h ON h.unit = l.unit
        ORDER BY source_file, {order}, pdf_page, unit, col_index""")
    con.execute(f"""CREATE TABLE agri_census_2010_tables AS
        SELECT scope AS source_file, "table" AS table_id, CAST(split_part("table", '.', 1) AS INTEGER) AS chapter,
               title AS table_title, unit_note AS notes, CAST(n_cols AS INTEGER) AS n_cols,
               CAST(first_page AS INTEGER) AS first_pdf_page, file AS source_pdf,
               CAST(col AS INTEGER) AS col_index, header_guess
        FROM cat ORDER BY source_file, {order}, col_index""")
    con.execute("CREATE TABLE agri_census_2010_district AS SELECT * FROM hd ORDER BY province, unit_type <> 'province', unit")
    bad = con.sql("SELECT DISTINCT source_file, unit FROM agri_census_2010_long WHERE table_title IS NULL OR unit_level IS NULL").fetchall()
    assert not bad, bad
    for name in ("agri_census_2010_district", "agri_census_2010_long", "agri_census_2010_tables"):
        p = out / f"{name}.parquet"
        con.execute(f"COPY {name} TO '{p.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)")
        print(f"  {name:28s} {con.sql(f'SELECT count(*) FROM {name}').fetchone()[0]:>9,} rows  {p.stat().st_size/1e6:6.2f} MB")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
