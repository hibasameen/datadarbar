"""Union the two levels into place_indicators, and derive the picker's index.

Two tables, and the split between them is the point:

  place_indicators       values only, one row per (shape, indicator, year).
                         No labels, no dataset names, no topic - repeating those
                         strings 90,000 times is most of what makes a payload
                         large.

  place_indicator_index  one row per selectable indicator, carrying the words a
                         reader sees and the counts that let the picker say how
                         much is behind each one. It also carries `source`,
                         which says where the values live: in place_indicators,
                         or in a census panel. That is what lets one picker sit
                         over both without the census being copied into a table
                         it does not fit.

Usage:
  build_place_index.py --districts <parquet> --tehsils <parquet>
                       --census-index <parquet> --pbs <tehsils geojson>
                       --out-values <parquet> --out-index <parquet>
"""
import argparse, json, pathlib

import duckdb

HERE = pathlib.Path(__file__).resolve().parent


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--districts', required=True)
    ap.add_argument('--tehsils', required=True)
    ap.add_argument('--census-index', required=True)
    ap.add_argument('--pbs', required=True)
    ap.add_argument('--out-values', required=True)
    ap.add_argument('--out-index', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')

    # the curated vocabulary: which topic a group sits under, and what each
    # indicator is called
    groups = json.loads((HERE / 'curated_groups.json').read_text())
    vals = []
    for g in groups:
        for ind, label in g['indicators'].items():
            dp = g['dp'].get(ind)
            vals.append((g['group_key'], g['topic'], g['topic_label'],
                         g['group_label'], g['dataset'], ind, str(label),
                         dp if dp is not None else -1))
    con.execute("""CREATE TABLE vocab(group_key TEXT, topic TEXT, topic_label TEXT,
                   group_label TEXT, dataset TEXT, indicator TEXT, label TEXT, dp INTEGER)""")
    con.executemany('INSERT INTO vocab VALUES (?,?,?,?,?,?,?,?)', vals)

    # tehsil names, for the detail panel
    g = json.loads(pathlib.Path(a.pbs).read_text())
    con.execute('CREATE TABLE tname(dds_id TEXT, name TEXT, district TEXT, province TEXT)')
    con.executemany('INSERT INTO tname VALUES (?,?,?,?)',
                    [(f['properties']['dds_id'], f['properties'].get('census_unit')
                      or f['properties'].get('tehsil'), f['properties'].get('district'),
                      f['properties'].get('province'))
                     for f in g['features'] if f['properties'].get('dds_id')])

    # ── values ──────────────────────────────────────────────────────────────
    con.execute(f"""CREATE TABLE place_indicators AS
        SELECT 'district' AS level, map_key, source_key, relation, note,
               group_key, indicator, year, value
        FROM '{a.districts}'
        UNION ALL
        SELECT 'tehsil' AS level, map_key, source_key,
               CASE WHEN n_units > 1 THEN 'shared' ELSE 'exact' END AS relation,
               CASE WHEN n_units > 1
                    THEN 'This figure is for a single 2017 boundary that is now '
                         || n_units || ' census units (' || units || '), and is '
                         || 'shown across all of them rather than divided between them'
               END AS note,
               group_key, indicator, year, value
        FROM '{a.tehsils}'""")

    out_v = pathlib.Path(a.out_values)
    out_v.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT * FROM place_indicators
                          ORDER BY level, group_key, indicator, year, map_key)
                    TO '{out_v.as_posix()}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")

    # ── index ───────────────────────────────────────────────────────────────
    con.execute(f"""CREATE TABLE place_indicator_index AS
        WITH curated AS (
          SELECT p.level, v.topic, v.topic_label, v.group_key, v.group_label,
                 v.dataset, p.indicator, v.label,
                 nullif(v.dp, -1) AS dp,
                 'place_indicators' AS source,
                 count(DISTINCT p.year) FILTER (WHERE p.year IS NOT NULL) AS years,
                 length(list_distinct(flatten(list(str_split(p.map_key, ' '))))) AS shapes,
                 count(DISTINCT p.source_key) AS units,
                 min(p.value) AS min_value, max(p.value) AS max_value
          FROM place_indicators p
          JOIN vocab v ON v.group_key = p.group_key AND v.indicator = p.indicator
          WHERE p.value IS NOT NULL
          GROUP BY ALL),
        census AS (
          SELECT unit_type AS level,
                 'census' AS topic, 'Census' AS topic_label,
                 'census' || census_year || CASE WHEN unit_type='district' THEN 'd' ELSE 't' END
                   || '_t' || regexp_replace(table_id, '[^0-9a-z]', '', 'g') AS group_key,
                 'Table ' || table_id || ' — ' || coalesce(table_title, '') AS group_label,
                 'PBS Census ' || census_year AS dataset,
                 indicator || '|' || coalesce(col_label, '') || '|' || locality || '|' || sex
                   AS indicator,
                 indicator || CASE WHEN col_label IS NOT NULL AND col_label <> indicator
                                   THEN ' · ' || col_label ELSE '' END
                   || CASE WHEN locality <> 'all' OR sex <> 'all'
                           THEN ' (' || nullif(concat_ws(', ',
                                nullif(locality,'all'), nullif(sex,'all')), '') || ')'
                           ELSE '' END AS label,
                 CASE WHEN is_rate THEN 2 END AS dp,
                 'census_panel_' || census_year AS source,
                 1 AS years, mappable_units AS shapes, units_with_value AS units,
                 min_value, max_value
          FROM '{a.census_index}')
        SELECT * FROM curated UNION ALL SELECT * FROM census""")

    out_i = pathlib.Path(a.out_index)
    con.execute(f"""COPY (SELECT * FROM place_indicator_index
                          ORDER BY topic, group_key, label, level)
                    TO '{out_i.as_posix()}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")

    nv = con.sql('SELECT count(*) FROM place_indicators').fetchone()[0]
    ni = con.sql('SELECT count(*) FROM place_indicator_index').fetchone()[0]
    print(f'place_indicators       {nv:>8,} rows  {out_v.stat().st_size/1e6:.2f} MB')
    print(f'place_indicator_index  {ni:>8,} rows  {out_i.stat().st_size/1e6:.2f} MB')
    print()
    print(con.sql("""SELECT source, level, count(*) AS indicators,
                            max(shapes) AS max_shapes
                     FROM place_indicator_index GROUP BY 1,2
                     ORDER BY 1,2""").df().to_string(index=False))
    # Not every value is a selectable indicator. The survey layers carry
    # provenance alongside their figures - dhs_coverage, hies_inherited_from,
    # the *_n_obs and *_low_n flags - which the detail panel reads to explain a
    # borrowed or small-sample figure. They belong in the values and not in a
    # picker, so they are counted here rather than treated as a gap.
    extra = con.sql("""SELECT count(*) FROM (
        SELECT DISTINCT group_key, indicator FROM place_indicators
        EXCEPT SELECT group_key, indicator FROM place_indicator_index)""").fetchone()[0]
    print(f'\n{extra} value fields are provenance rather than indicators, '
          f'and are deliberately not in the index')


if __name__ == '__main__':
    main()
