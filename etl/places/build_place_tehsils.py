"""The curated tehsil layers, on PBS's 2023 sub-district frame.

The district half of this was a rename; the tehsil half is not, for two reasons.

The sources do not agree on a key. The school and health layers carry dd_id, a
geoBoundaries 2017 identifier; the satellite layers call the same thing
tehsil_id; the Mouza Census has codes of its own. Each needs its own bridge to
the dds_id the map draws.

And dd_id is not unique against the census frame. 591 census sub-districts share
471 dd_ids, because units were split after the 2017 boundary was drawn: 35 of
those polygons cover more than one census unit, two of them four. A value
carried on such a polygon belongs to all of its successors and to no one of
them, so it is drawn across the group - the same treatment a split district
gets, one tier down, and never a division of the figure between them.

Usage:
  build_place_tehsils.py --src <warehouse> --bridge <units_to_pbs.csv>
                         --pbs <pbs_tehsils_2023.geojson> --out <parquet>
"""
import argparse, collections, json, pathlib, sys

import duckdb

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'mouza2020'))
from tehsil_spec import GROUPS
import build_payload as mouza          # the Mouza derivation, not a copy of it


def bridges(con, bridge, pbs_geo):
    """dd_id -> map_key, and PBS tehsil code -> map_key. Both may name several."""
    con.execute(f"""CREATE OR REPLACE VIEW br AS
        SELECT * FROM read_csv('{bridge}', header=true, quote='"', escape='"')""")
    con.execute("""CREATE OR REPLACE TABLE dd_map AS
        SELECT dd_id, string_agg(dds_id, ' ' ORDER BY dds_id) AS map_key,
               count(*) AS n_units,
               string_agg(unit, ', ' ORDER BY unit) AS units
        FROM br WHERE dd_id IS NOT NULL GROUP BY dd_id""")

    g = json.loads(pathlib.Path(pbs_geo).read_text())
    rows = [(str(f['properties']['tehsil_code']), f['properties'].get('dds_id'))
            for f in g['features'] if f['properties'].get('dds_id')]
    con.execute("CREATE OR REPLACE TABLE pbs_map(tehsil_code TEXT, map_key TEXT)")
    con.executemany("INSERT INTO pbs_map VALUES (?,?)", rows)
    return con.sql("SELECT count(*) FROM dd_map").fetchone()[0], len(rows)


def mouza_rows(con, src):
    """Mouza's derived indicators, from the derivation in etl/mouza2020."""
    raw = con.sql(f"SELECT * FROM '{src}/mouza_tehsil.parquet'").df()
    xw = con.sql(f"""SELECT tehsil_code, dd_id AS pbs_code FROM
        read_csv('{(HERE.parent / 'mouza2020' / 'mouza2020_tehsil_crosswalk_pbs.csv').as_posix()}',
                 header=true, quote='"', escape='"',
                 types={{'tehsil_code':'VARCHAR','dd_id':'VARCHAR'}})
        WHERE dd_id IS NOT NULL""").df()
    code_of = dict(zip(xw.tehsil_code.astype(str), xw.pbs_code.astype(str)))

    by_code = {}
    for r in raw.to_dict('records'):
        code = code_of.get(str(r.get('tehsil_code')))
        if code:
            by_code.setdefault(code.zfill(3), []).append(r)

    out = []
    for code, group in by_code.items():
        tot = sum(mouza.n(r, 'TotalMauzaCount') for r in group)
        if not tot:
            continue
        # The base is the MODAL denominator across the exclusive blocks, not a
        # mouza count: the dashboard's blocks disagree about how many mouzas
        # answered, and build_payload picks the most common. Substituting a
        # plausible-looking count instead moved roughly 40 per cent of every
        # indicator, which is what comparing against the live payload caught.
        bases = []
        for cols in (mouza.ELEC, mouza.HOUSE):
            bases.append(sum(mouza.n(r, c) for r in group for c in cols))
        for x, y in mouza.PAIR_BLOCKS:
            bases.append(sum(mouza.n(r, x) + mouza.n(r, y) for r in group))
        bases = [b for b in bases if b]
        base = collections.Counter(bases).most_common(1)[0][0] if bases else tot
        for ind, (cols, den) in mouza.IND.items():
            num = sum(mouza.n(r, c) for r in group for c in cols)
            # Three denominators, and they are not interchangeable. A block
            # divides by a set of mutually exclusive categories; a pair divides
            # by "yes plus no", where den[1] names the no column alone; base
            # divides by the mouza count. Reading a pair as a block iterates the
            # column name's characters and silently yields nothing, which is how
            # 17 indicators came back empty the first time.
            if den[0] == 'block':
                d = sum(mouza.n(r, c) for r in group for c in den[1])
            elif den[0] == 'pair':
                d = num + sum(mouza.n(r, den[1]) for r in group)
            else:
                d = base
            if d:
                out.append((code, ind, round(num / d * 100, 1)))
        # water table depth is a mouza-weighted mean of tehsil averages, not a
        # share of anything, so it is computed the way build_payload computes it
        if tot:
            wd = sum(mouza.n(r, 'AvgDepthOfWater') * mouza.n(r, 'TotalMauzaCount')
                     for r in group)
            out.append((code, 'wdepth', round(wd / tot, 0)))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--bridge', required=True)
    ap.add_argument('--pbs', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    n_dd, n_pbs = bridges(con, a.bridge, a.pbs)
    print(f'bridges: {n_dd} dd_id -> map_key, {n_pbs} PBS tehsil codes -> map_key')

    con.execute("""CREATE TABLE rows_out(
        map_key TEXT, source_key TEXT, n_units INTEGER, units TEXT,
        topic TEXT, group_key TEXT, group_label TEXT, dataset TEXT,
        indicator TEXT, label TEXT, year TEXT, value DOUBLE)""")

    # ── column-shaped sources: one column per indicator, keyed on dd_id ──
    for gk, g in GROUPS.items():
        if g['table'] == 'mouza_tehsil':
            continue
        tbl = f"'{a.src}/{g['table']}.parquet'"
        cols = {c[0] for c in con.sql(f'DESCRIBE SELECT * FROM {tbl}').fetchall()}
        for ind, label in g['indicators'].items():
            if ind not in cols:
                continue                      # handled below, or genuinely absent
            con.execute(f"""INSERT INTO rows_out
                SELECT m.map_key, t.{g['key']}, m.n_units, m.units,
                       ?, ?, ?, ?, ?, ?, NULL, t.{ind}
                FROM {tbl} t JOIN dd_map m ON m.dd_id = t.{g['key']}
                WHERE t.{ind} IS NOT NULL""",
                [g['topic'], gk, g['label'], g['dataset'], ind, label])

    # ── nightlights: radiance is already per square kilometre, and it has a
    #    year of its own, so it comes across as one row per year ──
    nl = GROUPS.get('nightlights')
    if nl:
        con.execute(f"""INSERT INTO rows_out
            SELECT m.map_key, t.tehsil_id, m.n_units, m.units,
                   ?, ?, ?, ?, 'density', ?, CAST(t.year AS TEXT), t.radiance
            FROM '{a.src}/tehsil_nightlights.parquet' t
            JOIN dd_map m ON m.dd_id = t.tehsil_id
            WHERE t.radiance IS NOT NULL""",
            [nl['topic'], 'nightlights', nl['label'], nl['dataset'],
             nl['indicators'].get('density', 'Lights per km2')])
        con.execute(f"""INSERT INTO rows_out
            SELECT m.map_key, t.tehsil_id, m.n_units, m.units,
                   ?, ?, ?, ?, 'growth', ?, NULL, t.nl_growth
            FROM '{a.src}/tehsil_satellite.parquet' t
            JOIN dd_map m ON m.dd_id = t.tehsil_id
            WHERE t.nl_growth IS NOT NULL""",
            [nl['topic'], 'nightlights', nl['label'], nl['dataset'],
             nl['indicators'].get('growth', 'Growth in lights')])

    # ── Mouza: derived, and on PBS tehsil codes rather than dd_id ──
    mz = mouza_rows(con, a.src)
    con.execute("CREATE TABLE mz(pbs_code TEXT, indicator TEXT, value DOUBLE)")
    con.executemany("INSERT INTO mz VALUES (?,?,?)", mz)
    label_of, group_of = {}, {}
    for gk, g in GROUPS.items():
        if g['table'] != 'mouza_tehsil':
            continue
        for ind, label in g['indicators'].items():
            label_of[ind] = label
            group_of[ind] = (g['topic'], gk, g['label'], g['dataset'])
    lv = ', '.join("('" + i + "', '" + l.replace("'", "''") + "', '"
                   + group_of[i][0] + "', '" + group_of[i][1] + "', '"
                   + group_of[i][2].replace("'", "''") + "', '"
                   + group_of[i][3].replace("'", "''") + "')"
                   for i, l in sorted(label_of.items()))
    con.execute(f"""INSERT INTO rows_out
        WITH lab(ind, label, topic, gk, glabel, dataset) AS (VALUES {lv})
        SELECT p.map_key, mz.pbs_code, 1, NULL,
               lab.topic, lab.gk, lab.glabel, lab.dataset,
               mz.indicator, lab.label, NULL, mz.value
        FROM mz JOIN lab ON lab.ind = mz.indicator
                JOIN pbs_map p ON p.tehsil_code = mz.pbs_code""")

    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT * FROM rows_out
                          ORDER BY group_key, indicator, year, map_key)
                    TO '{out.as_posix()}'
                    (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")

    print(f'\n{con.sql("SELECT count(*) FROM rows_out").fetchone()[0]:,} rows '
          f'-> {out}  ({out.stat().st_size/1e6:.2f} MB)')
    print(con.sql("""SELECT group_key, count(DISTINCT indicator) AS inds,
                            count(DISTINCT map_key) AS keys, count(*) AS rows
                     FROM rows_out GROUP BY 1 ORDER BY 4 DESC""").df().to_string(index=False))
    spread = con.sql("""SELECT count(DISTINCT map_key) FROM rows_out
                        WHERE n_units > 1""").fetchone()[0]
    print(f'\n{spread} map keys name more than one census unit '
          f'(one value drawn across the group)')


if __name__ == '__main__':
    main()
