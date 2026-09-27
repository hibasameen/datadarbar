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
    ap.add_argument('--extra', nargs='*', default=[],
                    help='further place-row parquets to union in')
    ap.add_argument('--extra-index', nargs='*', default=[],
                    help='index-row parquets for sources whose values are too '
                         'large to ship and are read on demand instead')
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
    # groups ingested since, declared in the ETL rather than in app.js
    import sys
    sys.path.insert(0, str(HERE))
    from extra_groups import GROUPS as EXTRA
    groups = groups + EXTRA
    vals = []
    for g in groups:
        for ind, label in g['indicators'].items():
            dp = g['dp'].get(ind)
            vals.append((g['group_key'], g['topic'], g['topic_label'],
                         g['group_label'], g['dataset'], ind, str(label),
                         dp if dp is not None else -1,
                         ' '.join(g.get('families', []))))
    con.execute("""CREATE TABLE vocab(group_key TEXT, topic TEXT, topic_label TEXT,
                   group_label TEXT, dataset TEXT, indicator TEXT, label TEXT,
                   dp INTEGER, families TEXT)""")
    con.executemany('INSERT INTO vocab VALUES (?,?,?,?,?,?,?,?,?)', vals)

    # tehsil names, for the detail panel
    g = json.loads(pathlib.Path(a.pbs).read_text())
    con.execute('CREATE TABLE tname(dds_id TEXT, name TEXT, district TEXT, province TEXT)')
    con.executemany('INSERT INTO tname VALUES (?,?,?,?)',
                    [(f['properties']['dds_id'], f['properties'].get('census_unit')
                      or f['properties'].get('tehsil'), f['properties'].get('district'),
                      f['properties'].get('province'))
                     for f in g['features'] if f['properties'].get('dds_id')])

    # ── values ──────────────────────────────────────────────────────────────
    # Sources ingested after the first two arrive already in this shape, so they
    # union straight in rather than needing a branch of their own.
    extra_sql = ''.join(
        """
        UNION ALL
        SELECT level, map_key, source_key, relation, note,
               group_key, indicator, year, value
        FROM '%s'""" % x for x in a.extra)
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
        FROM '{a.tehsils}'
        {extra_sql}""")

    out_v = pathlib.Path(a.out_values)
    out_v.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT * FROM place_indicators
                          ORDER BY level, group_key, indicator, year, map_key)
                    TO '{out_v.as_posix()}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")

    # ── index ───────────────────────────────────────────────────────────────
    from census_topics import TOPIC_OF_TABLE
    from topics import TOPICS, GROUP_TOPIC, ORDER
    # A source whose values are too large to ship supplies index rows only; the
    # picker lists them and the values are read when one is chosen. Same shape
    # as the two halves below, so it unions straight in.
    extra_index = ''.join(
        """
        UNION ALL SELECT level, topic, topic_label, group_key, group_label,
               dataset, indicator, label, '' AS raw_indicator, '' AS raw_col,
               dp, source, NULL AS families, years, localities, sexes,
               shapes, units, min_value, max_value FROM '%s'""" % x for x in a.extra_index)
    tvals = ', '.join(
        "('" + t + "', '" + k + "', '" + TOPICS[k].replace("'", "''") + "')"
        for t, k in sorted(TOPIC_OF_TABLE.items()))
    # the curated side: group -> the same topic vocabulary
    gvals = ', '.join(
        "('" + g + "', '" + k + "', '" + TOPICS[k].replace("'", "''") + "', "
        + str(ORDER.index(k)) + ")"
        for g, k in sorted(GROUP_TOPIC.items()))
    con.execute(f"""CREATE TABLE place_indicator_index AS
        WITH topics(table_id, topic, topic_label) AS (VALUES {tvals}),
        gtopics(group_key, topic, topic_label, topic_order) AS (VALUES {gvals}),
        curated AS (
          SELECT p.level, gt.topic, gt.topic_label, v.group_key, v.group_label,
                 v.dataset, p.indicator, v.label,
                 '' AS raw_indicator, '' AS raw_col,
                 nullif(v.dp, -1) AS dp,
                 'place' AS source,
                 -- which family's low_n and n_obs qualify this group. The
                 -- provenance is filed under the survey family (dhs_fert) and
                 -- the indicators under the app's group (dhsFertility), so
                 -- without this the flags never meet the figures they qualify.
                 nullif(v.families, '') AS families,
                 list_sort(list_distinct(list(p.year) FILTER (WHERE p.year IS NOT NULL)))
                   AS years,
                 ['all'] AS localities, ['all'] AS sexes,
                 length(list_distinct(flatten(list(str_split(p.map_key, ' '))))) AS shapes,
                 count(DISTINCT p.source_key) AS units,
                 min(p.value) AS min_value, max(p.value) AS max_value
          FROM place_indicators p
          JOIN vocab v ON v.group_key = p.group_key AND v.indicator = p.indicator
          JOIN gtopics gt ON gt.group_key = p.group_key
          WHERE p.value IS NOT NULL
          GROUP BY ALL),
        census AS (
          -- One row per cell definition, not per series. The design picks an
          -- c.indicator and then its facets, so c.locality and c.sex are collected
          -- into lists here rather than multiplying the rows: 37,971 series are
          -- 4,051 definitions. The year is collected the same way, but it is
          -- rarely a real choice - only 34 district definitions exist in both
          -- censuses - so `years` usually holds one, and the picker must offer
          -- what is there rather than assuming two.
          SELECT c.unit_type AS level,
                 t.topic, t.topic_label,
                 'census_t' || regexp_replace(c.table_id, '[^0-9a-z]', '', 'g') AS group_key,
                 'Table ' || c.table_id || ' \u2014 ' || any_value(coalesce(c.table_title, ''))
                   AS group_label,
                 'PBS Census ' || string_agg(DISTINCT CAST(c.census_year AS TEXT), ' and '
                                             ORDER BY CAST(c.census_year AS TEXT)) AS dataset,
                 c.table_id || '|' || c.indicator || '|' || coalesce(c.col_label, '') AS indicator,
                 c.indicator || CASE WHEN c.col_label IS NOT NULL AND c.col_label <> c.indicator
                                   THEN ' \u00b7 ' || c.col_label ELSE '' END AS label,
                 c.indicator AS raw_indicator,
                 coalesce(c.col_label, '') AS raw_col,
                 CASE WHEN bool_or(c.is_rate) THEN 2 END AS dp,
                 'census' AS source, NULL AS families,
                 list_sort(list_distinct(list(CAST(c.census_year AS TEXT)))) AS years,
                 list_sort(list_distinct(list(c.locality))) AS localities,
                 list_sort(list_distinct(list(c.sex))) AS sexes,
                 max(c.mappable_units) AS shapes,
                 max(c.units_with_value) AS units,
                 min(c.min_value) AS min_value, max(c.max_value) AS max_value
          FROM '{a.census_index}' AS c
          JOIN topics AS t ON t.table_id = c.table_id
          GROUP BY c.unit_type, t.topic, t.topic_label, c.table_id, c.indicator, c.col_label)
        SELECT * FROM curated UNION ALL SELECT * FROM census{extra_index}""")

    out_i = pathlib.Path(a.out_index)
    # PBS's own concatenation is the audit trail; the picker needs something a
    # reader can scan. labels.py changes case, dashes, the order of two parts
    # and a redundant repetition - never the words.
    from labels import display
    rows = con.sql("""SELECT rowid, label, raw_indicator, raw_col
                      FROM place_indicator_index WHERE raw_indicator <> ''""").fetchall()
    con.execute('CREATE TABLE relabel(rid BIGINT, lab TEXT)')
    con.executemany('INSERT INTO relabel VALUES (?,?)',
                    [(r[0], display(r[2], r[3] or None)) for r in rows])
    con.execute("""CREATE OR REPLACE TABLE place_indicator_index AS
        SELECT i.* REPLACE (coalesce(r.lab, i.label) AS label),
               nullif(i.label, coalesce(r.lab, i.label)) AS label_source
        FROM place_indicator_index i LEFT JOIN relabel r ON r.rid = i.rowid""")
    con.execute('ALTER TABLE place_indicator_index DROP COLUMN raw_indicator')
    con.execute('ALTER TABLE place_indicator_index DROP COLUMN raw_col')
    print(f'  {len(rows):,} census labels rewritten for reading; '
          f'PBS\u2019s own kept in label_source')

    ovals = ', '.join("('" + k + "', " + str(i) + ")" for i, k in enumerate(ORDER))
    con.execute(f"""COPY (SELECT i.* FROM place_indicator_index i
                          LEFT JOIN (VALUES {ovals}) AS o(topic, ord) ON o.topic = i.topic
                          ORDER BY o.ord, i.group_key, i.label, i.level)
                    TO '{out_i.as_posix()}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")

    # SQL does not read Python's \uXXXX escapes, so a label built in a query
    # string keeps them literally. It has happened twice; it fails the build now.
    bad = con.sql(r"""SELECT count(*) FROM place_indicator_index
        WHERE label LIKE '%' || chr(92) || 'u%'
           OR group_label LIKE '%' || chr(92) || 'u%'
           OR topic_label LIKE '%' || chr(92) || 'u%'""").fetchone()[0]
    if bad:
        ex = con.sql(r"""SELECT DISTINCT coalesce(
              nullif(CASE WHEN label LIKE '%' || chr(92) || 'u%' THEN label END, ''),
              nullif(CASE WHEN group_label LIKE '%' || chr(92) || 'u%' THEN group_label END, ''),
              topic_label) FROM place_indicator_index
            WHERE label LIKE '%' || chr(92) || 'u%'
               OR group_label LIKE '%' || chr(92) || 'u%'
               OR topic_label LIKE '%' || chr(92) || 'u%' LIMIT 3""").fetchall()
        raise SystemExit(
            f'{bad} index labels carry a literal backslash-u escape, e.g. '
            + '; '.join(x[0] for x in ex)
            + ' - use the character itself in SQL, not a Python escape')

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
