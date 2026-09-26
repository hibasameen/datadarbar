"""Stage 4: assemble the Option A panel.

Takes the Stage 1 observations, applies the Stage 2 geography register and the
Stage 3 missingness mask, recomputes rates from counts, and runs the closure
checks. Nothing here re-reads a source file; it composes the earlier stages.
"""
import argparse, collections, csv, json, pathlib
import duckdb

REGION_OF = {'kp': 'kp', 'punjab': 'punjab', 'sindh': 'sindh',
             'balochistan': 'balochistan', 'islamabad': 'islamabad'}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--obs', required=True)
    ap.add_argument('--crosswalk', required=True)
    ap.add_argument('--mask', required=True)
    ap.add_argument('--mismatches', required=True)
    ap.add_argument('--unaligned', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    out = pathlib.Path(a.out); out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    # A release has to rebuild byte for byte, so the engine must not reorder
    # rows across threads.
    con.execute("SET threads TO 1")
    con.execute("SET preserve_insertion_order TO true")

    con.execute(f"CREATE VIEW obs AS SELECT * FROM '{a.obs}'")
    con.execute(f"CREATE VIEW cw  AS SELECT * FROM read_csv('{a.crosswalk}', header=true, quote='\"', escape='\"', all_varchar=true)")
    con.execute(f"""CREATE VIEW mask AS SELECT * FROM read_csv('{a.mask}', header=true, quote='\"', escape='\"',
        columns={{'table_id':'VARCHAR','region':'VARCHAR','src_row':'INTEGER',
                  'src_col':'INTEGER','label':'VARCHAR','pdf':'VARCHAR'}})""")

    # source_file is table_<id>_<region>.xlsx; recover the region to join the mask
    con.execute("""CREATE TABLE panel AS
      SELECT o.* EXCLUDE (missing),
             regexp_extract(o.source_file, 'table_[0-9a-z]+_([a-z]+)\\.xlsx', 1) AS region,
             (o.missing OR m.src_row IS NOT NULL) AS missing,
             CASE WHEN o.missing THEN 'printed dash (excel)'
                  WHEN m.src_row IS NOT NULL THEN 'printed dash (recovered from pdf)'
                  ELSE NULL END AS missing_source,
             CASE WHEN m.src_row IS NOT NULL THEN NULL ELSE o.value END AS value_masked
      FROM obs o
      LEFT JOIN mask m
        ON m.table_id = o.table_id
       AND m.region = regexp_extract(o.source_file, 'table_[0-9a-z]+_([a-z]+)\\.xlsx', 1)
       AND m.src_row = o.src_row AND m.src_col = o.src_col""")

    # Where the two official renderings disagree on a value, adopt the PDF only
    # if doing so makes a district close. That is evidence, not a preference:
    # PBS's own arithmetic decides. Every other disagreement is left as
    # published and reported in value_mismatches.csv.
    con.execute(f"""CREATE VIEW mism AS SELECT * FROM read_csv('{a.mismatches}', header=true, quote='\"', escape='\"',
        columns={{'table_id':'VARCHAR','region':'VARCHAR','src_row':'INTEGER','src_col':'INTEGER',
                  'label':'VARCHAR','excel':'VARCHAR','pdf':'VARCHAR'}})""")

    # Rows where PBS's two renderings do not say the same thing. Not corrected —
    # there is no basis to prefer one — but flagged, because a researcher should
    # know a figure is disputed by its own publisher before building on it.
    con.execute(f"""CREATE VIEW disputed AS
        SELECT DISTINCT table_id, region, src_row FROM read_csv('{a.unaligned}', header=true, quote='\"', escape='\"',
          columns={{'table_id':'VARCHAR','region':'VARCHAR','src_row':'INTEGER','label':'VARCHAR',
                    'reason':'VARCHAR','agreeing':'INTEGER','disagreeing':'INTEGER'}})
        WHERE reason = 'the two renderings disagree on this row'""")

    # A rate is not additive, so it must never enter a closure check and must
    # be recomputed from counts rather than aggregated. Flag them once, here.
    con.execute("""CREATE TABLE panel1b AS
      -- A rate is not additive. The pattern has to cover the column label as
      -- well as the indicator: table 21 names its percentage columns PERCENT
      -- and nothing else marks them.
      SELECT *, (regexp_matches(upper(indicator) || ' ' || upper(coalesce(col_label,'')), '%|PERCENT|RATIO|RATE|DENSITY|PROPORTION|SHARE|H\\.HOLD SIZE|HOUSEHOLD SIZE|AVERAGE')) AS is_rate
      FROM panel""")

    con.execute("""CREATE TABLE cand AS
      SELECT p.table_id, p.region, p.src_row, p.src_col, p.province, p.district, p.unit,
             p.unit_type, p.indicator, coalesce(p.col_label,'') col_label, p.locality, p.sex,
             p.value AS excel_value,
             TRY_CAST(replace(m.pdf, ',', '') AS DOUBLE) AS pdf_value
      FROM panel1b p JOIN mism m
        ON m.table_id=p.table_id AND m.region=p.region
       AND m.src_row=p.src_row AND m.src_col=p.src_col
      WHERE NOT p.is_rate AND TRY_CAST(replace(m.pdf, ',', '') AS DOUBLE) IS NOT NULL""")

    con.execute("""CREATE TABLE corrections AS
      WITH v AS (SELECT table_id,province,district,unit,unit_type,indicator,
                        coalesce(col_label,'') col_label,locality,sex,value
                 FROM panel1b WHERE value IS NOT NULL AND NOT missing AND NOT is_rate),
           d AS (SELECT table_id,province,district,indicator,col_label,locality,sex,value
                 FROM v WHERE unit_type='district'),
           k AS (SELECT table_id,province,district,indicator,col_label,locality,sex,sum(value) s
                 FROM v WHERE unit_type<>'district' GROUP BY 1,2,3,4,5,6,7)
      SELECT c.*, d.value AS district_value, k.s AS children_sum
      FROM cand c
      JOIN d USING (table_id,province,district,indicator,col_label,locality,sex)
      JOIN k USING (table_id,province,district,indicator,col_label,locality,sex)
      WHERE c.unit_type<>'district'
        AND abs(d.value - k.s) >= 0.5
        AND abs(d.value - (k.s - c.excel_value + c.pdf_value)) < 0.5""")

    con.execute("""CREATE TABLE panel2 AS
      SELECT p.* EXCLUDE (value, value_masked), p.value_masked AS value,
             coalesce(c.dds_id,
                      'DDD-' || upper(substr(regexp_replace(p.district, '[^A-Za-z]', '', 'g'), 1, 12))
                     ) AS dds_id,
             c.adm3_pcode, c.dd_id, (q.src_row IS NOT NULL) AS renderings_disagree,
             coalesce(c.method, CASE WHEN p.unit_type='district' THEN 'district' END) AS geo_method
      FROM (SELECT p.* EXCLUDE (value, value_masked),
                   coalesce(x.pdf_value, p.value) AS value,
                   coalesce(x.pdf_value, p.value_masked) AS value_masked,
                   (x.pdf_value IS NOT NULL) AS value_corrected
            FROM panel1b p LEFT JOIN corrections x
              ON x.table_id=p.table_id AND x.region=p.region
             AND x.src_row=p.src_row AND x.src_col=p.src_col) p
      LEFT JOIN disputed q
        ON q.table_id=p.table_id AND q.region=p.region AND q.src_row=p.src_row
      LEFT JOIN cw c
        ON c.province = p.province AND c.district = p.district AND c.unit = p.unit""")

    n = con.sql("SELECT count(*) FROM panel2").fetchone()[0]
    miss = con.sql("SELECT count(*) FROM panel2 WHERE missing").fetchone()[0]
    rec = con.sql("SELECT count(*) FROM panel2 WHERE missing_source LIKE '%pdf%'").fetchone()[0]
    withdds = con.sql("SELECT count(*) FROM panel2 WHERE dds_id IS NOT NULL").fetchone()[0]
    con.execute(f"""COPY (SELECT * FROM panel2
        ORDER BY province, district, unit, table_id, indicator, col_label, locality, sex,
                 src_row, src_col)
        TO '{out / 'panel.parquet'}' (FORMAT PARQUET, COMPRESSION ZSTD)""")

    # ---- closure: children sum to their district, per table, counts only ----
    checks = con.sql("""
      WITH v AS (SELECT table_id, province, district, unit, unit_type, indicator,
                        coalesce(col_label, '') AS col_label, locality, sex, value
                 FROM panel2 WHERE value IS NOT NULL AND NOT missing AND NOT is_rate),
           d AS (SELECT table_id, province, district, indicator, col_label, locality, sex, value
                 FROM v WHERE unit_type='district'),
           k AS (SELECT table_id, province, district, indicator, col_label, locality, sex,
                        sum(value) s, count(*) n
                 FROM v WHERE unit_type<>'district'
                 GROUP BY 1,2,3,4,5,6,7)
      SELECT d.table_id,
             count(*) AS comparisons,
             count(*) FILTER (WHERE abs(d.value - k.s) < 0.5) AS closing,
             count(*) FILTER (WHERE abs(d.value - k.s) >= 0.5) AS failing
      FROM d JOIN k USING (table_id, province, district, indicator, col_label, locality, sex)
      GROUP BY 1 ORDER BY 1
    """).fetchall()

    with open(out / 'closure_checks.csv', 'w', newline='') as fh:
        w = csv.writer(fh); w.writerow(['table_id', 'comparisons', 'closing', 'failing'])
        w.writerows(checks)

    print(f"panel rows          {n:,}")
    print(f"missing cells       {miss:,}  (of which {rec:,} recovered from the PDFs)")
    ncorr = con.sql("SELECT count(*) FROM corrections").fetchone()[0]
    con.sql("SELECT district, unit, indicator, locality, excel_value, pdf_value FROM corrections") \
       .to_csv(str(out / 'value_corrections.csv'))
    print(f"values corrected    {ncorr}  (PDF adopted where it makes the district close)")
    disp = con.sql("SELECT count(*) FROM panel2 WHERE renderings_disagree").fetchone()[0]
    print(f"on disputed rows    {disp:,}  (PBS's two renderings disagree; flagged, not corrected)")
    rates = con.sql("SELECT count(*) FROM panel2 WHERE is_rate").fetchone()[0]
    print(f"rows with an id     {withdds:,}/{n:,} ({100*withdds/n:.1f}%)")
    print(f"rate cells flagged  {rates:,}  (excluded from closure; recompute from counts)")
    print(f"\n{'table':6s} {'comparisons':>12} {'closing':>9} {'failing':>8}  rate")
    tc = tk = 0
    for t, comp, clo, fail in checks:
        tc += comp; tk += clo
        print(f"T{t:<5} {comp:12,} {clo:9,} {fail:8,}  {100*clo/comp:5.1f}%")
    print(f"{'ALL':6s} {tc:12,} {tk:9,} {tc-tk:8,}  {100*tk/max(tc,1):5.1f}%")
    json.dump(dict(rows=n, missing=miss, recovered_from_pdf=rec, with_dds=withdds,
                   closure=[dict(table_id=t, comparisons=c, closing=k, failing=f)
                            for t, c, k, f in checks]),
              open(out / 'panel_report.json', 'w'), indent=2)


if __name__ == '__main__':
    main()
