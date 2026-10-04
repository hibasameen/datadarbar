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
import argparse, json, pathlib, re

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
    # Keyed on (census_year, table_id): PBS renumbered between censuses, so
    # table 16 is usual activity in 2017 and disability in 2023.
    tvals = ', '.join(
        "('" + y + "', '" + t + "', '" + k + "', '"
        + TOPICS[k].replace("'", "''") + "')"
        for (y, t), k in sorted(TOPIC_OF_TABLE.items()))
    # the curated side: group -> the same topic vocabulary
    gvals = ', '.join(
        "('" + g + "', '" + k + "', '" + TOPICS[k].replace("'", "''") + "', "
        + str(ORDER.index(k)) + ")"
        for g, k in sorted(GROUP_TOPIC.items()))
    con.execute(f"""CREATE TABLE place_indicator_index AS
        WITH topics(census_year, table_id, topic, topic_label) AS (VALUES {tvals}),
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
                 'Population Census' AS dataset,
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
                          AND t.census_year = CAST(c.census_year AS TEXT)
          GROUP BY c.unit_type, t.topic, t.topic_label, c.table_id, c.indicator,
                   c.col_label)
        SELECT * FROM curated UNION ALL SELECT * FROM census{extra_index}""")

    out_i = pathlib.Path(a.out_index)
    # PBS's own concatenation is the audit trail; the picker needs something a
    # reader can scan. labels.py changes case, dashes, the order of two parts
    # and a redundant repetition - never the words.
    from labels import display, split
    rows = con.sql("""SELECT rowid, label, raw_indicator, raw_col
                      FROM place_indicator_index WHERE raw_indicator <> ''""").fetchall()
    # label is the whole cell, for the legend, the search and the CSV header.
    # measure and metric are the same cell split in two, for the picker: one
    # census topic held 1,525 entries, which is not a list anyone reads, and
    # most of that is one measure repeated across its age bands.
    con.execute('CREATE TABLE relabel(rid BIGINT, lab TEXT, meas TEXT, met TEXT)')
    con.executemany('INSERT INTO relabel VALUES (?,?,?,?)',
                    [(r[0], display(r[2], r[3] or None))
                     + split(r[2], r[3] or None) for r in rows])
    con.execute("""CREATE OR REPLACE TABLE place_indicator_index AS
        SELECT i.* REPLACE (coalesce(r.lab, i.label) AS label),
               nullif(i.label, coalesce(r.lab, i.label)) AS label_source,
               coalesce(r.meas, i.label) AS measure,
               coalesce(r.met, '') AS metric
        FROM place_indicator_index i LEFT JOIN relabel r ON r.rid = i.rowid""")
    con.execute('ALTER TABLE place_indicator_index DROP COLUMN raw_indicator')
    con.execute('ALTER TABLE place_indicator_index DROP COLUMN raw_col')
    # Redundant for browsing, not absent from the catalogue. Set by the two
    # rules below; everything else stays false.
    con.execute('ALTER TABLE place_indicator_index '
                'ADD COLUMN redundant BOOLEAN DEFAULT FALSE')
    print(f'  {len(rows):,} census labels rewritten for reading; '
          f'PBS\u2019s own kept in label_source')

    # ── the total of a partition tells you nothing ──────────────────────
    # A table that splits the population by mother tongue, religion or
    # nationality also carries the total of those parts, which is just the
    # population again under another name. It is dropped where the table
    # genuinely partitions something - three or more sibling measures - and
    # kept otherwise, because "Total Population" in the homeless table is the
    # homeless total and is the whole point of that table.
    # Bare - "Total", "All" - and qualified: "Population by Mother Tongue -
    # Total" is the same thing wearing its table's name, and PBS writes it
    # both ways. The guard is on siblings: drop only where three or more other
    # measures share the table, so a table whose whole point is a total keeps
    # it.
    TOTALISH = """(lower(measure) IN ('total', 'all', 'total population')
          OR lower(measure) LIKE '%\u2014 total'
          OR lower(measure) LIKE '%\u2014 all'
          OR lower(measure) LIKE '%\u2014 total population')"""
    con.execute(f"""CREATE OR REPLACE TABLE _drop AS
        SELECT DISTINCT i.level, i.group_key, i.measure
        FROM place_indicator_index i
        WHERE i.source = 'census' AND {TOTALISH.replace('measure', 'i.measure')}
          AND (SELECT count(DISTINCT j.measure) FROM place_indicator_index j
               WHERE j.group_key = i.group_key AND j.level = i.level
                 AND NOT {TOTALISH.replace('measure', 'j.measure')}) >= 3""")
    gone = con.sql('SELECT count(*) FROM place_indicator_index i JOIN _drop d '
                   'USING (level, group_key, measure)').fetchone()[0]
    names = con.sql('SELECT DISTINCT measure FROM _drop ORDER BY 1').fetchall()
    # Marked, not deleted. These are redundant for BROWSING - the total of a
    # partition is the population again - but several are the denominator of
    # their own table, and a denominator should not disappear because its
    # label resembles a total. They stay in the catalogue, out of the subject
    # tree, and findable by search.
    con.execute("""UPDATE place_indicator_index i SET redundant = TRUE
        WHERE EXISTS (SELECT 1 FROM _drop d WHERE d.level = i.level
                      AND d.group_key = i.group_key AND d.measure = i.measure)""")
    con.execute('DROP TABLE _drop')
    print(f'  {gone} rows marked as the total of their own table '
          f'({len(names)} measures):')
    for (nm,) in names:
        print(f'      {nm}')

    # A column heading standing in for a measure. PBS repeats the column name
    # in the row when it prints that column's total, so "POPULATION / ALL
    # SEXES" against itself becomes a measure called "Population - All Sexes"
    # with no breakdown - which is why a bare population appeared under
    # Housing, in a table about types of household. It is the total of the
    # household types beside it, and it is recognised by the measure text
    # turning up as a METRIC on its siblings.
    cols = con.sql("""SELECT count(*) FROM place_indicator_index i
        WHERE i.source = 'census' AND i.metric = ''
          AND EXISTS (SELECT 1 FROM place_indicator_index j
                      WHERE j.group_key = i.group_key AND j.level = i.level
                        AND j.metric = i.measure)""").fetchone()[0]
    con.execute("""UPDATE place_indicator_index i SET redundant = TRUE
        WHERE i.source = 'census' AND i.metric = ''
          AND EXISTS (SELECT 1 FROM place_indicator_index j
                      WHERE j.group_key = i.group_key AND j.level = i.level
                        AND j.metric = i.measure)""")
    print(f'  {cols} column totals marked (a heading standing in for a measure)')

    # ── the same cell in both censuses, and across the sexes ────────────
    # A census cell keyed on PBS's raw strings cannot merge across censuses,
    # because the two spell the same thing differently: "00 - 04" against
    # "00 -- 04". So the identical measure appeared twice, once per census,
    # and the year control - which exists to offer 2017, 2023 and the change -
    # was disabled on both, each being a single-year row.
    #
    # THE YEAR AND THE SEX WERE ALSO IN THE NAME. PBS publishes population as
    # four columns per round, so the Population family carried eight entries -
    # "POPULATION-2023 - All Sexes", "Population - 2017 - Female" and six more
    # - for one variable that the page already has a year control and a sex
    # control for. A reader choosing between eight names for one number is
    # choosing between spellings.
    #
    # So the merge key is the one the year merge already used - level, topic,
    # dataset, measure, metric - with the measure normalised by removing the
    # sex word and the panel's OWN census year.
    #
    # The table is deliberately NOT in the key. PBS moves content between
    # tables across rounds: the 2017 homeless table is table 22 and in 2023 it
    # is a category inside table 10, and keying on the table would split those
    # apart again after they had been correctly joined.
    #
    # Removing only the panel's own year matters. The 2023 Table 1 prints a
    # "POPULATION 2017" column beside "POPULATION-2023" - the previous round,
    # for comparison - and the 2017 table prints "POPULATION 1998". Stripping
    # every year would make those look like the same measure as the round they
    # sit in, merging a historical comparison column into the headline figure.
    #
    # Where two rows in a group claim the same year and sex the merge would
    # have to choose between them, so that group is left alone and counted.
    # All of those are a different fault - PBS spelling one category two ways
    # inside one table, "Non- Pakistani" against "Non-pakistani" - which is a
    # label problem and not this one.
    con.execute("""CREATE OR REPLACE MACRO _sex_of(m) AS CASE
        WHEN lower(m) LIKE '%all sexes%' OR lower(m) LIKE '%both sexes%'
          THEN 'all'
        WHEN lower(m) LIKE '%trans%' THEN 'transgender'
        WHEN lower(m) LIKE '%female%' THEN 'female'
        WHEN lower(m) LIKE '%male%' THEN 'male' ELSE NULL END""")
    con.execute(r"""CREATE OR REPLACE MACRO _norm_meas(m, y) AS
        trim(regexp_replace(regexp_replace(regexp_replace(lower(m),
          '(all sexes|both sexes|trans ?gender|female|male)', '', 'g'),
          '-? ?' || y, '', 'g'), '[^a-z0-9]+', ' ', 'g'))""")
    con.execute(r"""CREATE OR REPLACE MACRO _disp_meas(m, y) AS
        trim(regexp_replace(regexp_replace(regexp_replace(regexp_replace(m,
          '\s*[—–-]?\s*(all sexes|both sexes|trans ?gender|female|male)\s*$',
          '', 'gi'),
          '\s*-?\s*' || y, '', 'g'),
          '\s*[—–-]\s*$', '', 'g'),
          '\s+', ' ', 'g'))""")

    con.execute("""CREATE OR REPLACE TEMP TABLE _cand AS
        SELECT level, topic, dataset, metric, measure, indicator,
               years[1] AS yr, coalesce(_sex_of(measure), 'all') AS sx,
               _norm_meas(measure, years[1]) AS nm
        FROM place_indicator_index
        -- 1998 joins only through ATTACH_1998 below, a reviewed list: left to
        -- this spelling match, 1998 glance Area merged itself with 2017 Area
        WHERE source = 'census' AND NOT redundant AND len(years) = 1
          AND years <> ['1998']""")
    con.execute("""CREATE OR REPLACE TEMP TABLE _merge AS
        SELECT level, topic, dataset, nm, metric
        FROM _cand
        GROUP BY 1, 2, 3, 4, 5
        HAVING count(*) > 1 AND count(*) = count(DISTINCT yr || '/' || sx)""")
    skipped = con.sql("""SELECT count(*) FROM (
        SELECT 1 FROM _cand GROUP BY level, topic, dataset, nm, metric
        HAVING count(*) > 1 AND count(*) <> count(DISTINCT yr || '/' || sx))
        """).fetchone()[0]

    con.execute("""CREATE OR REPLACE TABLE place_indicator_index AS
        SELECT i.level, i.topic, i.topic_label,
               any_value(i.group_key) AS group_key,
               any_value(i.group_label) AS group_label,
               i.dataset,
               -- The raw key per year and sex travels with the row, because
               -- PBS spells the same band differently in each round and puts
               -- each sex in its own column. Only the sex half is added when
               -- there is more than one, so a year-only merge keeps the
               -- shorter "2017=<key>" form it already had.
               CASE WHEN m.nm IS NULL THEN any_value(i.indicator)
                    WHEN count(DISTINCT c.sx) > 1
                      THEN string_agg(c.yr || ':' || c.sx || '=' || i.indicator,
                                      '\x1f' ORDER BY c.yr, c.sx)
                    ELSE string_agg(c.yr || '=' || i.indicator,
                                    '\x1f' ORDER BY c.yr) END AS indicator,
               CASE WHEN m.nm IS NULL THEN any_value(i.label)
                    ELSE any_value(_disp_meas(i.measure, c.yr)) END AS label,
               CASE WHEN m.nm IS NULL THEN any_value(i.measure)
                    ELSE any_value(_disp_meas(i.measure, c.yr)) END AS measure,
               i.metric,
               any_value(i.dp) AS dp, i.source,
               any_value(i.families) AS families,
               list_sort(list_distinct(flatten(list(i.years)))) AS years,
               any_value(i.localities) AS localities,
               CASE WHEN m.nm IS NULL THEN any_value(i.sexes)
                    ELSE list_sort(list_distinct(list(c.sx))) END AS sexes,
               max(i.shapes) AS shapes, max(i.units) AS units,
               min(i.min_value) AS min_value, max(i.max_value) AS max_value,
               any_value(i.label_source) AS label_source, i.redundant
        FROM place_indicator_index i
        -- Joined on the indicator, which is the row's own identity. Joining
        -- on measure and metric fans out where PBS spells one age band two
        -- ways inside a table: each index row then matched both candidate
        -- rows and the compound key came out with the same year twice.
        LEFT JOIN _cand c
          ON c.level = i.level AND c.indicator = i.indicator
        LEFT JOIN _merge m
          ON m.level = c.level AND m.topic = c.topic AND m.dataset = c.dataset
         AND m.nm = c.nm AND m.metric = c.metric
        -- redundant is grouped on, and excluded from _cand above, so a row
        -- kept only for the catalogue can neither merge with another nor pull
        -- a real measure into a compound key. Leaving it out folded
        -- demographics/pop_total and urbanRural/total_all into a single entry
        -- keyed "2017=pop_total\x1f2017=total_all", which is two different
        -- measures wearing one name.
        GROUP BY i.level, i.topic, i.topic_label, i.dataset, i.metric,
                 i.source, i.redundant, m.nm,
                 CASE WHEN m.nm IS NULL THEN i.indicator ELSE '' END,
                 CASE WHEN m.nm IS NULL THEN i.measure ELSE '' END""")
    print(f'  {skipped} groups left unmerged: two rows claim the same year '
          f'and sex, which is PBS spelling one category two ways')

    n_both = con.sql("""SELECT count(*) FROM place_indicator_index
                        WHERE len(years) > 1 AND source = 'census'""").fetchone()[0]
    print(f'  {n_both} census cells published in both censuses, merged into '
          f'one row each with a year to pick')

    # ── the same measure, worded differently in each census ─────────────
    # The merge above joins cells that are spelled alike once the year and
    # sex are taken out. These are not: 2017 calls literacy LITERACY RATIO in
    # table 13 and 2023 calls it Literate % in table 12. At district level
    # the curated series already join them, so the gap was invisible there;
    # at tehsil level, where there is no curated series, every one of these
    # was a 2017-only row beside a 2023-only row, and the year control offered
    # neither the other census nor the change.
    #
    # Each pair is declared, not matched, and checked: the two must be the
    # same quantity on the same base. Literacy is the population aged 10 and
    # above in both rounds - Bannu district reads 46.55 (2017, FR Bannu
    # included) and 41.75 (2023) in the curated series and in these cells.
    # The growth rates cover different periods by construction (1998-2017,
    # 2017-2023): each is the rate since the census before, which is the
    # measure PBS publishes, and the label says so.
    CROSS_CENSUS = [
        ('13|LITERATE / LITERACY RATIO|LITERACY RATIO', '12|Literate %|',
         'Literacy rate (%, aged 10+)'),
        ('1|POPULATION - 2017 / AVERAGE HOUSEHOLD SIZE|AVERAGE HOUSEHOLD SIZE',
         '1|POPULATION-2023 / AVERAGE H.HOLD SIZE|POPULATION-2023 / AVERAGE H.HOLD SIZE',
         'Average household size'),
        ('1|POPULATION - 2017 / POPULATION DENSITY PER SQ. KM.|POPULATION DENSITY PER SQ. KM.',
         '1|POPULATION-2023 / POPULATION DENSITY PER SQ.K.M|POPULATION-2023 / POPULATION DENSITY PER SQ.K.M',
         'Population density (per km²)'),
        ('1|1998-2017 AVERAGE ANNUAL GROWTH RATE|1998-2017 AVERAGE ANNUAL GROWTH RATE',
         '1|2017-2023 AVERAGE ANNUAL G.RATE|2017-2023 AVERAGE ANNUAL G.RATE',
         'Annual growth rate since the previous census (%)'),
    ]
    paired = 0
    for k17, k23, label in CROSS_CENSUS:
        got = con.execute("""
            SELECT level FROM place_indicator_index
            WHERE source = 'census' AND NOT redundant
              AND ((indicator = ? AND years = ['2017']) OR (indicator = ? AND years = ['2023']))
            GROUP BY level HAVING count(*) = 2""", [k17, k23]).fetchall()
        assert got, f'a declared cross-census pair matches nothing: {label}'
        con.execute("""CREATE OR REPLACE TABLE place_indicator_index AS
            WITH a AS (SELECT * FROM place_indicator_index
                       WHERE source = 'census' AND NOT redundant
                         AND indicator = $k17 AND years = ['2017']),
                 b AS (SELECT * FROM place_indicator_index
                       WHERE source = 'census' AND NOT redundant
                         AND indicator = $k23 AND years = ['2023']),
                 m AS (SELECT a.* REPLACE (
                          '2017=' || $k17 || '\x1f2023=' || $k23 AS indicator,
                          $label AS label, $label AS measure,
                          ['2017', '2023'] AS years,
                          list_sort(list_distinct(a.localities || b.localities)) AS localities,
                          list_sort(list_distinct(a.sexes || b.sexes)) AS sexes,
                          greatest(a.shapes, b.shapes) AS shapes,
                          greatest(a.units, b.units) AS units,
                          least(a.min_value, b.min_value) AS min_value,
                          greatest(a.max_value, b.max_value) AS max_value)
                       FROM a JOIN b USING (level))
            SELECT * FROM place_indicator_index
            WHERE NOT (source = 'census' AND NOT redundant
                       AND level IN (SELECT level FROM m)
                       AND ((indicator = $k17 AND years = ['2017'])
                         OR (indicator = $k23 AND years = ['2023'])))
            UNION ALL BY NAME SELECT * FROM m""",
            {'k17': k17, 'k23': k23, 'label': label})
        paired += len(got)
    print(f'  {paired} rows joined across censuses where PBS worded one measure two ways')

    # ── 1998, attached to the rows it continues ─────────────────────────
    # census_panel_1998 sits on the same 2023 frame, so a 1998 figure that
    # measures what a 2017 row measures is offered as that row's third year,
    # not as a separate indicator a reader has to find. Each 1998 cell is
    # attached to the row that already carries the named 2017 cell, at the
    # same level - population at district AND tehsil, the glance indicators at
    # district. What has no later counterpart (housing units, 1981 population)
    # stays in the panel and the catalogue, off the map.
    ATTACH_1998 = [
        # 1998 cell, sex part (None = not split by sex), the 2017 cell it joins
        ('1|POPULATION - 1998|POPULATION - 1998 / ALL SEXES', 'all',
         '1|POPULATION - 2017 / ALL SEXES|ALL SEXES'),
        ('glance|POPULATION - 1998 BY SEX|MALE', 'male', '1|POPULATION - 2017 / ALL SEXES|ALL SEXES'),
        ('glance|POPULATION - 1998 BY SEX|FEMALE', 'female', '1|POPULATION - 2017 / ALL SEXES|ALL SEXES'),
        ('glance|LITERACY RATIO (10+)|LITERACY RATIO', None, '13|LITERATE / LITERACY RATIO|LITERACY RATIO'),
        ('glance|AVERAGE HOUSEHOLD SIZE|AVERAGE HOUSEHOLD SIZE', None,
         '1|POPULATION - 2017 / AVERAGE HOUSEHOLD SIZE|AVERAGE HOUSEHOLD SIZE'),
        ('glance|POPULATION DENSITY PER SQ. KM.|POPULATION DENSITY PER SQ. KM.', None,
         '1|POPULATION - 2017 / POPULATION DENSITY PER SQ. KM.|POPULATION DENSITY PER SQ. KM.'),
        ('glance|1981-1998 AVERAGE ANNUAL GROWTH RATE|1981-1998 AVERAGE ANNUAL GROWTH RATE', None,
         '1|1998-2017 AVERAGE ANNUAL GROWTH RATE|1998-2017 AVERAGE ANNUAL GROWTH RATE'),
        ('glance|SEX RATIO|SEX RATIO', None, '1|POPULATION - 2017 / SEX RATIO|SEX RATIO'),
    ]
    attached = 0
    for k98, sx, k17 in ATTACH_1998:
        src = con.execute("""SELECT level, units, shapes, min_value, max_value, localities
            FROM place_indicator_index WHERE source = 'census' AND years = ['1998']
              AND indicator = ?""", [k98]).fetchall()
        for level, units, shapes, lo, hi, locs in src:
            tgt = con.execute("""SELECT rowid, indicator FROM place_indicator_index
                WHERE source = 'census' AND level = ? AND list_contains(years, '2017')
                  AND (indicator = ? OR indicator LIKE ? OR indicator LIKE ?)""",
                [level, k17, '%=' + k17, '%=' + k17 + '\x1f%']).fetchall()
            assert len(tgt) == 1, f'1998 {k98} at {level}: {len(tgt)} rows carry {k17}'
            rid, ind = tgt[0]
            compound = bool(re.match(r'^(?:19|20)\d\d(?::[a-z]+)?=', ind))
            with_sex = compound and bool(re.match(r'^\d{4}:[a-z]+=', ind))
            if not compound:
                ind = '2017=' + ind
            part = ('1998:' + (sx or 'all') if with_sex else '1998') + '=' + k98
            con.execute("""UPDATE place_indicator_index SET
                indicator = ?, years = list_sort(list_distinct(years || ['1998'])),
                localities = list_sort(list_distinct(localities || ?)),
                units = greatest(units, ?), shapes = greatest(shapes, ?),
                min_value = least(min_value, ?), max_value = greatest(max_value, ?)
                WHERE rowid = ?""", [part + '\x1f' + ind, locs, units, shapes, lo, hi, rid])
            attached += 1
    dropped = con.sql("SELECT count(*) FROM place_indicator_index WHERE years = ['1998']").fetchone()[0]
    con.execute("DELETE FROM place_indicator_index WHERE years = ['1998']")
    print(f'  {attached} 1998 cells attached to their 2017/2023 rows; {dropped} 1998-only '
          f'cells left to the panel')

    # Folding five census entries into one dataset puts six identical
    # "Total Population, 50-54" rows in the Demographics list - one per table
    # that happens to publish that cell. The table is what distinguishes them,
    # so it is appended, and ONLY where the label is otherwise ambiguous:
    # qualifying all 5,205 would make every name longer to fix 473.
    con.execute("""CREATE OR REPLACE TABLE place_indicator_index AS
        WITH dup AS (
          SELECT dataset, topic, level, label
          FROM place_indicator_index
          GROUP BY 1, 2, 3, 4 HAVING count(*) > 1)
        SELECT i.* REPLACE (
          CASE WHEN d.label IS NOT NULL AND i.group_key LIKE 'census\\_t%' ESCAPE '\\'
               THEN i.label || ' \u00b7 table '
                    || replace(i.group_key, 'census_t', '')
               ELSE i.label END AS label)
        FROM place_indicator_index i
        LEFT JOIN dup d USING (dataset, topic, level, label)""")
    # Some survive the table qualifier: the two censuses spell the same age
    # bracket differently ("00 - 04" against "00 -- 04") and labels.py
    # normalises both to "0-4", so one table yields two rows with one name.
    # Those are separated by the census they come from.
    con.execute("""CREATE OR REPLACE TABLE place_indicator_index AS
        WITH dup AS (
          SELECT dataset, topic, level, label
          FROM place_indicator_index
          GROUP BY 1, 2, 3, 4 HAVING count(*) > 1)
        SELECT i.* REPLACE (
          CASE WHEN d.label IS NOT NULL AND len(i.years) > 0
               THEN i.label || ' \u00b7 ' || list_aggregate(i.years, 'string_agg', '/')
               ELSE i.label END AS label)
        FROM place_indicator_index i
        LEFT JOIN dup d USING (dataset, topic, level, label)""")
    left = con.sql("""SELECT count(*) FROM (
        SELECT 1 FROM place_indicator_index
        GROUP BY dataset, topic, level, label HAVING count(*) > 1)""").fetchone()[0]
    print(f'  ambiguous labels qualified by table, then census; '
          f'{left} still duplicated')

    # Whose figure is it? Census-panel series are PBS's as published; a
    # curated group is Data Darbar's where we computed, estimated or
    # aggregated it (etl/catalog_meta.PLACES_DERIVED, audited per group).
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
    from catalog_meta import place_value_derived
    con.create_function('_derived', lambda g, i: bool(place_value_derived(g, i)),
                        ['VARCHAR', 'VARCHAR'], 'BOOLEAN')
    con.execute("""CREATE OR REPLACE TABLE place_indicator_index AS
        SELECT *, CASE WHEN source = 'census' THEN FALSE
                       ELSE _derived(group_key, indicator) END AS derived
        FROM place_indicator_index""")
    nd = con.sql("SELECT count(*) FILTER (WHERE derived), count(*) FROM place_indicator_index").fetchone()
    print(f'  {nd[0]:,} of {nd[1]:,} indicators are Data Darbar\u2019s constructions, the rest as published')

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
