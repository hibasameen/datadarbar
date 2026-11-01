"""Agricultural Census 2024 onto PBS's 2023 district frame: two warehouse tables
and the Places rows for two groups.

The census (fieldwork Sep 2024 - Feb 2025) is a weighted sample, not a count:
PBS drew 11,083 mouzas and blocks, listed every household in them, took the
large holders at 100 per cent and sub-sampled the rest, and raised the result
to the frame. That is why every figure is a fraction, why PBS says the design
aims at a 15 per cent margin at district level, and why a district whose
sampled mouzas went wrong carries a visibly wrong weight: Umerkot comes out at
2,098 farms, Khyber at 756,000.

Three consequences for what is shown.

**Ratios, not levels.** Everything mapped is a share or a per-farm / per-acre
rate, which survives a wrong weight far better than a count does. The counts
are in the table for anyone who wants them.

**Per-farm rates are withheld where the farm count is implausible.** Below
FARMS_MIN farms (the Karachi districts, Hyderabad, Umerkot, Nushki, Kech,
Awaran, Duki, Chagai), or above FARMS_PER_HH_MAX times the district's rural
households in the 2023 census (Khyber, Harnai, Mohmand), the average farm size and the per-farm livestock and
tractor rates are not drawn, and the note says why. Shares of area and
per-acre rates stay: the area and the livestock figures for those districts
look ordinary.

**The frame is PBS's own 2023 one.** All 134 districts in the tables match the
geography layer by name, with no renaming. Cholistan Area (inside Bahawalpur
division) and Malakand Protected Area (KP) are published as units of their
own and are kept in the table but not drawn - no polygon holds them alone.
Islamabad Capital Territory is PBS code 128. Gilgit-Baltistan and AJK were
enumerated but are in none of the published tables.

Usage:
  build_agri_census.py --src <derived dir> --geo <districts_2023_geo.js>
                       --census <census_panel_2023.parquet>
                       --out-district <parquet> --out-long <parquet> --out-places <parquet>
"""
import argparse, json, pathlib

import duckdb

FARMS_MIN = 5000          # fewer farms than this: the weight has gone wrong
FARMS_PER_HH_MAX = 1.5    # more farms than 1.5 x rural households (Census 2023): likewise

# indicator -> (label, SQL expression over the headline table h, dp, farm-denominated?)
FARMS = {
    'farms':                  ('Farms (PBS weighted estimate)', 'h.farms', 0, False),
    'farm_area_ac':           ('Farm area (acres)', 'h.farm_area_ac', 0, False),
    'cultivated_area_ac':     ('Cultivated area (acres)', 'h.cultivated_area_ac', 0, False),
    'avg_farm_size_ac':       ('Average farm size (acres)', 'h.farm_area_ac / h.farms', 1, True),
    'farms_per_100_rural_hh': ('Farms per 100 rural households (Census 2023)',
                               '100 * h.farms / c.rural_hh', 1, False),
    'share_farms_under_2_5ac':('Farms under 2.5 acres (%)', '100 * h.share_farms_under_2_5ac', 1, False),
    'share_area_25ac_plus':   ('Farm area in holdings of 25 acres or more (%)',
                               '100 * h.share_area_25ac_plus', 1, False),
    'share_farms_owner':      ('Owner-operated farms (%)', '100 * h.share_farms_owner', 1, False),
    'share_farms_tenant':     ('Tenant farms (%)', '100 * h.share_farms_tenant', 1, False),
    'share_farms_female':     ('Farms operated by women (%)', '100 * h.share_farms_female', 1, False),
    'share_farms_fragmented': ('Fragmented farms (%)', '100 * h.share_farms_fragmented', 1, False),
    'share_cultivated':       ('Cultivated share of farm area (%)', '100 * h.share_cultivated', 1, False),
    'share_irrigated':        ('Irrigated share of cultivated area (%)', '100 * h.share_irrigated', 1, False),
    'share_irrig_tubewell_only': ('Tubewell-only share of irrigated area (%)',
                               '100 * h.share_irrig_tubewell_only', 1, False),
    'cropping_intensity_pct': ('Cropping intensity (%)', 'h.cropping_intensity_pct', 1, False),
    'share_wheat_of_cropped': ('Wheat share of cropped area (%)', '100 * h.share_wheat_of_cropped', 1, False),
    'share_farms_wheat':      ('Farms reporting wheat (%)', '100 * h.wheat_farms / h.farms', 1, False),
    'share_farms_tunnel':     ('Farms using tunnel farming (%)', '100 * h.tunnel_farms / h.farms', 2, False),
}
LIVESTOCK = {
    'cattle':                 ('Cattle (PBS weighted estimate)', 'h.cattle', 0, False),
    'buffaloes':              ('Buffaloes (PBS weighted estimate)', 'h.buffaloes', 0, False),
    'sheep':                  ('Sheep (PBS weighted estimate)', 'h.sheep', 0, False),
    'goats':                  ('Goats (PBS weighted estimate)', 'h.goats', 0, False),
    'cattle_buffalo_per_farm':('Cattle and buffaloes per farm', '(h.cattle + h.buffaloes) / h.farms', 2, True),
    'sheep_goats_per_farm':   ('Sheep and goats per farm', '(h.sheep + h.goats) / h.farms', 2, True),
    'buffalo_share_bovine':   ('Buffaloes among cattle and buffaloes (%)',
                               '100 * h.buffaloes / (h.cattle + h.buffaloes)', 1, False),
    'tractors':               ('Tractors (PBS weighted estimate)', 'h.tractors', 0, False),
    'tractors_per_1000_farms':('Tractors per 1,000 farms', '1000 * h.tractors / h.farms', 1, True),
    'cultivated_ac_per_tractor': ('Cultivated acres per tractor', 'h.cultivated_area_ac / nullif(h.tractors, 0)', 0, False),
    'tubewells_per_1000_cult_ac': ('Tubewells per 1,000 cultivated acres',
                               '1000 * h.tubewells / h.cultivated_area_ac', 1, False),
    'share_tubewells_solar':  ('Solar-powered tubewells (%)', '100 * h.tubewells_solar / nullif(h.tubewells, 0)', 1, False),
    'share_tubewells_electric': ('Electric tubewells (%)', '100 * h.tubewells_electric / nullif(h.tubewells, 0)', 1, False),
    'share_tubewells_diesel': ('Diesel tubewells (%)', '100 * h.tubewells_diesel / nullif(h.tubewells, 0)', 1, False),
}
GROUPS = {'agriFarms': FARMS, 'agriLivestock': LIVESTOCK}


def vocab():
    """Same shape as a curated_groups.json entry; read by extra_groups.py."""
    out = []
    for gk, ind in GROUPS.items():
        out.append({
            'group_key': gk, 'topic': 'agriculture', 'topic_label': 'Agriculture',
            'group_label': ('Farm structure — Agricultural Census 2024' if gk == 'agriFarms'
                            else 'Livestock & machinery — Agricultural Census 2024'),
            'dataset': 'PBS Agricultural Census 2024 (weighted sample estimates)',
            'dp': {k: v[2] for k, v in ind.items()},
            'indicators': {k: v[0] for k, v in ind.items()},
        })
    return out


# ── the picker's navigation hierarchy ───────────────────────────────────────
# Series that arrived after the hierarchy crosswalk was drawn declare their own
# place in it, in the crosswalk's shape (see issue4_groups.py). Subtopic and
# metric form per indicator; the forms say what can be totalled and what can
# be read per head (none of these are counts of people).
SUM_OK = {'Farm or holding count': True, 'Area (acres)': True, 'Livestock (head)': True,
          'Machine count': True}
HREVIEW = 'Weighted sample estimate (PBS Agricultural Census 2024); 15% design margin at district level'
_SUB = {
    'farms': ('Farm structure & tenure', 'Farm or holding count'),
    'farm_area_ac': ('Farm structure & tenure', 'Area (acres)'),
    'cultivated_area_ac': ('Farm structure & tenure', 'Area (acres)'),
    'avg_farm_size_ac': ('Farm structure & tenure', 'Average / source-defined score'),
    'farms_per_100_rural_hh': ('Farm structure & tenure', 'Published rate / ratio'),
    'share_farms_under_2_5ac': ('Farm structure & tenure', 'Published percentage / proportion'),
    'share_area_25ac_plus': ('Farm structure & tenure', 'Published percentage / proportion'),
    'share_farms_owner': ('Farm structure & tenure', 'Published percentage / proportion'),
    'share_farms_tenant': ('Farm structure & tenure', 'Published percentage / proportion'),
    'share_farms_female': ('Farm structure & tenure', 'Published percentage / proportion'),
    'share_farms_fragmented': ('Farm structure & tenure', 'Published percentage / proportion'),
    'share_cultivated': ('Irrigation & cropping', 'Published percentage / proportion'),
    'share_irrigated': ('Irrigation & cropping', 'Published percentage / proportion'),
    'share_irrig_tubewell_only': ('Irrigation & cropping', 'Published percentage / proportion'),
    'cropping_intensity_pct': ('Irrigation & cropping', 'Published rate / ratio'),
    'share_wheat_of_cropped': ('Irrigation & cropping', 'Published percentage / proportion'),
    'share_farms_wheat': ('Irrigation & cropping', 'Published percentage / proportion'),
    'share_farms_tunnel': ('Irrigation & cropping', 'Published percentage / proportion'),
    'cattle': ('Livestock', 'Livestock (head)'), 'buffaloes': ('Livestock', 'Livestock (head)'),
    'sheep': ('Livestock', 'Livestock (head)'), 'goats': ('Livestock', 'Livestock (head)'),
    'cattle_buffalo_per_farm': ('Livestock', 'Published rate / ratio'),
    'sheep_goats_per_farm': ('Livestock', 'Published rate / ratio'),
    'buffalo_share_bovine': ('Livestock', 'Published percentage / proportion'),
    'tractors': ('Farm machinery & tubewells', 'Machine count'),
    'tractors_per_1000_farms': ('Farm machinery & tubewells', 'Published rate / ratio'),
    'cultivated_ac_per_tractor': ('Farm machinery & tubewells', 'Published rate / ratio'),
    'tubewells_per_1000_cult_ac': ('Farm machinery & tubewells', 'Published rate / ratio'),
    'share_tubewells_solar': ('Farm machinery & tubewells', 'Published percentage / proportion'),
    'share_tubewells_electric': ('Farm machinery & tubewells', 'Published percentage / proportion'),
    'share_tubewells_diesel': ('Farm machinery & tubewells', 'Published percentage / proportion'),
}


def mapping(countable):
    """Entries for build_hierarchy, in the crosswalk's own shape."""
    out = []
    for gk, ind in GROUPS.items():
        for k, (label, expr, dp, farm_denom) in ind.items():
            sub, form = _SUB[k]
            o = dict(level='district', group_key=gk, indicator=k, label=label,
                     measure=label, metric='', dp=dp, source='place')
            out.append(dict(
                proposed_topic='Agriculture', proposed_subtopic=sub,
                indicator_family=('Farm structure (Agricultural Census 2024)' if gk == 'agriFarms'
                                  else 'Livestock & machinery (Agricultural Census 2024)'),
                metric_form=form, suggested_controls='District', definition_review=HREVIEW,
                legacy_modes=(['Source value', '% of total population', 'Per 1,000 total population']
                              if countable(o) else ['Source value']),
                original=o))
    assert set(_SUB) == {k for ind in GROUPS.values() for k in ind}, 'every indicator needs a place'
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--geo', required=True)
    ap.add_argument('--census', required=True)
    ap.add_argument('--out-district', required=True)
    ap.add_argument('--out-long', required=True)
    ap.add_argument('--out-places', required=True)
    a = ap.parse_args()
    src = pathlib.Path(a.src)

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    js = pathlib.Path(a.geo).read_text()
    gj = json.loads(js[js.index('=') + 1:].rstrip().rstrip(';'))
    con.execute('CREATE TABLE geo(code TEXT, name TEXT, prov TEXT)')
    con.executemany('INSERT INTO geo VALUES (?,?,?)',
                    [(f['properties']['code'], f['properties']['n'].upper(),
                      f['properties']['p']) for f in gj['features']])

    con.execute(f"""CREATE TABLE h0 AS SELECT * FROM read_csv('{(src / "agri_census_2024_district_headline.csv").as_posix()}',
                    header=true, quote='"', escape='"', sample_size=-1)""")
    # rural households, Census 2023 table 20
    con.execute(f"""CREATE TABLE c AS
        SELECT map_key AS district_code, value AS rural_hh
        FROM '{a.census}'
        WHERE table_id = '20' AND unit_type = 'district' AND locality = 'rural'
          AND sex = 'all' AND indicator = 'Households'""")

    # the join key: unit name = geo name, ICT by hand. A farm is a household's
    # holding, so more farms than rural households is as implausible as too few.
    con.execute("""CREATE TABLE h AS
        WITH k AS (SELECT h0.*, CASE WHEN h0.unit = 'ISLAMABAD CAPITAL TERRITORY' THEN '128' ELSE g.code END AS district_code
                   FROM h0 LEFT JOIN geo g ON g.name = h0.unit)
        SELECT k.*, c.rural_hh,
               (k.farms < %d) OR (k.farms > %s * c.rural_hh) AS farms_implausible
        FROM k LEFT JOIN c ON c.district_code = k.district_code""" % (FARMS_MIN, FARMS_PER_HH_MAX))
    unmatched = con.sql("""SELECT unit FROM h WHERE unit_level = 'district' AND district_code IS NULL""").fetchall()
    if unmatched:
        raise SystemExit(f'districts not in the geography layer: {unmatched}')
    extra = con.sql("""SELECT code FROM geo WHERE code <> '0' AND code NOT IN
                       (SELECT district_code FROM h WHERE district_code IS NOT NULL)""").fetchall()
    print(f'{len(extra)} geography districts without census rows (GB/AJK expected): '
          + ', '.join(c for (c,) in extra))

    # ── source-faithful tables ──────────────────────────────────────────────
    con.execute("""CREATE TABLE agri_census_2024_district AS
        SELECT district_code, unit, unit_level, prov_file AS source_file, farms_implausible,
               filled_from_T1_2 AS farms_filled_from_hectare_table,
               h.* EXCLUDE (district_code, unit, unit_level, prov_file, farms_implausible, filled_from_T1_2, district_name)
        FROM h ORDER BY unit_level, prov_file, unit""")
    con.execute(f"""CREATE TABLE agri_census_2024_long AS
        SELECT l.prov_file AS source_file, l.table_id, l.chapter, l.table_no, l.table_title, l.notes,
               CASE WHEN l.unit = 'ISLAMABAD CAPITAL TERRITORY' THEN '128' ELSE g.code END AS district_code,
               l.unit, l.unit_level, l."group", l.category, l.col_index, l."column", l.value
        FROM read_csv('{(src / "agri_census_2024_long.csv.gz").as_posix()}', header=true, quote='"', escape='"', delim=',', new_line='\\r\\n', sample_size=-1,
                      types={{'table_id':'VARCHAR','notes':'VARCHAR','group':'VARCHAR','category':'VARCHAR',
                              'col_index':'INTEGER','value':'DOUBLE'}}) l
        LEFT JOIN geo g ON g.name = l.unit
        ORDER BY l.prov_file, l.chapter, l.table_no, l.unit_level, l.unit, l.col_index""")

    # ── the Places rows ─────────────────────────────────────────────────────
    sel = []
    for gk, ind in GROUPS.items():
        for k, (label, expr, dp, farm_denom) in ind.items():
            guard = 'CASE WHEN h.farms_implausible THEN NULL ELSE (%s) END' % expr if farm_denom else expr
            sel.append(f"""SELECT 'district' AS level, h.district_code AS map_key, h.unit AS source_key,
                'exact' AS relation,
                CASE WHEN h.farms < {FARMS_MIN} THEN
                  'PBS''s weighted estimate gives this district only ' || CAST(round(h.farms) AS INTEGER)
                  || ' farms, which is implausibly few for its area and livestock; per-farm rates are not drawn'
                     WHEN h.farms_implausible THEN
                  'PBS''s weighted estimate gives this district ' || CAST(round(h.farms) AS INTEGER)
                  || ' farms against ' || CAST(round(c.rural_hh) AS INTEGER) || ' rural households in the 2023 census, '
                  || 'which is implausibly many; per-farm rates are not drawn'
                END AS note,
                '{gk}' AS group_key, '{k}' AS indicator, NULL AS year,
                CAST({guard} AS DOUBLE) AS value
              FROM h LEFT JOIN c ON c.district_code = h.district_code
              WHERE h.unit_level = 'district' OR h.unit = 'ISLAMABAD CAPITAL TERRITORY'""")
    con.execute('CREATE TABLE places AS ' + ' UNION ALL '.join(sel))
    con.execute("DELETE FROM places WHERE value IS NULL OR isnan(value) OR isinf(value)")

    for tbl, out in (('agri_census_2024_district', a.out_district),
                     ('agri_census_2024_long', a.out_long), ('places', a.out_places)):
        p = pathlib.Path(out); p.parent.mkdir(parents=True, exist_ok=True)
        con.execute(f"""COPY (SELECT * FROM {tbl}) TO '{p.as_posix()}'
                        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12)""")
        n = con.sql(f'SELECT count(*) FROM {tbl}').fetchone()[0]
        print(f'  {tbl:28s} {n:>9,} rows  {p.stat().st_size/1e6:6.2f} MB')
    print(con.sql("""SELECT group_key, count(DISTINCT indicator) AS indicators,
                            count(DISTINCT map_key) AS districts, count(*) AS rows_
                     FROM places GROUP BY 1 ORDER BY 1""").df().to_string(index=False))
    print(con.sql("""SELECT unit, round(farms) AS farms FROM h WHERE farms_implausible AND unit_level='district'
                     ORDER BY farms""").df().to_string(index=False))


if __name__ == '__main__':
    main()
