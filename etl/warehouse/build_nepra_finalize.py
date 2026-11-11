#!/usr/bin/env python3
"""Finalize the NEPRA layer: plant crosswalk, curated DISCO series, DuckDB registration.

Runs after build_nepra_month.py, build_nepra_daily.py, build_nepra_soi.py, build_nepra_per.py.

1. nepra_plants.parquet       one row per canonical plant (plant_id) with every name variant seen in the
                              monthly sheets; plant_id is also written back into nepra_plant_month/day.
2. nepra_disco_annual.parquet tidy DISCO x fiscal-year series pulled out of the SOI tables (consumers,
                              sanctioned load, units purchased/sold/losses, billing & recovery, category
                              sales, peak demand, feeders/transformers, safety, new connections, village
                              electrification). Overlapping 5-year windows are de-duplicated: the most
                              recent report wins (`report_year`), every source row is kept in
                              nepra_disco_annual_all.parquet for audit.
3. registers views/tables in $DD_WAREHOUSE/data_darbar.duckdb and appends rows to file_catalog.
"""
import os, re, sys
from pathlib import Path
import pandas as pd
import duckdb

from dd_paths import NEPRA_RAW, WAREHOUSE as WH

# ------------------------------------------------------------------ 1. plant crosswalk
DROP_WORDS = {'limited', 'ltd', 'pvt', 'private', 'company', 'co', 'the', 'project', 'projects', 'plant',
              'hydropower', 'hydro', 'power', 'station', 'thermal', 'wapda', 'ipp', 'ipps', 'formerly',
              'consulting', 'corporation', 'energy', 'generation', 'development', 'mills', 'sugar', 'wind',
              'farm', 'solar', 'electric', 'nuclear', 'green', 'new', 'pakistan', 'pakistani', 'l'}
ALIASES = {  # normalised key -> canonical plant name
    'zhenfa': 'Atlas Solar (ex-Zhenfa Pakistan New Energy)',
    'atlas zhenfa': 'Atlas Solar (ex-Zhenfa Pakistan New Energy)',
    'din': 'Din Energy (wind)', 'din energy': 'Din Energy (wind)',
    'lake side': 'Lakeside Energy (wind)', 'lakeside': 'Lakeside Energy (wind)',
    'liberty 1': 'Liberty Wind Power 1', 'liberty i': 'Liberty Wind Power 1',
    'liberty 2': 'Liberty Wind Power 2', 'liberty ii': 'Liberty Wind Power 2',
    'metro mpcl ii': 'Metro Wind Power II (MWPL-II)', 'metro mwpl ii': 'Metro Wind Power II (MWPL-II)',
    'nasda': 'NASDA Green Energy (wind)',
    'tricom': 'Lucky Renewables (ex-Tricom Wind Power)', 'lucky renewables tricom': 'Lucky Renewables (ex-Tricom Wind Power)',
    'karot': 'Karot Hydropower (KPCL)', 'karot kpcl': 'Karot Hydropower (KPCL)',
    'laraib bong escape': 'New Bong Escape Hydropower (Laraib)', 'bong escape': 'New Bong Escape Hydropower (Laraib)',
    'lucky lepcl': 'Lucky Electric Power (LEPCL, Thar coal)', 'lucky': 'Lucky Electric Power (LEPCL, Thar coal)',
    'punjab': 'Punjab Thermal Power (RLNG)',
    'narowal': 'Narowal Energy (HUBCO)', 'narowal hubco': 'Narowal Energy (HUBCO)',
    'tapal ii': 'Tapal Wind - II', 'tapal 2 twepl ii': 'Tapal Wind - II',
    'tapal twep': 'Tapal Wind Power Energy (TWEPL)', 'tapal twepl': 'Tapal Wind Power Energy (TWEPL)',
    'artistic ii': 'Artistic Wind Power - II',
    'gul ahmed ii': 'Gul Ahmed Electric (wind II)', 'gul ahmed electric': 'Gul Ahmed Electric (wind II)',
    'indus': 'Indus Wind Energy',
    'master': 'Master Green Energy (wind)',
    'nppcl balloki': 'NPPMCL Balloki (RLNG)', 'nppmcl balloki': 'NPPMCL Balloki (RLNG)',
    'nppcl haveli bahadar shah': 'NPPMCL Haveli Bahadur Shah (RLNG)', 'nppmcl haveli bahadar shah': 'NPPMCL Haveli Bahadur Shah (RLNG)',
    'huaneng shandong ruyi hsrpel': 'Huaneng Shandong Ruyi (HSRPEL, Sahiwal coal)',
    'thar coal block i tcb i': 'Thar Coal Block-I (TCB-I)',
    'hydrochina dawood hdpp': 'Hydrochina Dawood Power (HDPPL)', 'hydrochina dawood hdppl': 'Hydrochina Dawood Power (HDPPL)',
    'sachal sedpl': 'Sachal Energy Development (SEDPL)',
    'jamshoro': 'Jamshoro Power Generation Company (GENCO-I)', 'jamshoro limit ed': 'Jamshoro Power Generation Company (GENCO-I)',
    'foundation i fwel i': 'Foundation Wind Energy-I (FWEL-I)', 'foundation i fwel ii': 'Foundation Wind Energy-II (FWEL-II)',
}

FULLNAME = {  # exact published name -> canonical, where the normalised key would wrongly merge plants
    'Gul Ahmed Wind Power Ltd.': 'Gul Ahmed Wind Power (I)',
    'Gul Ahmed Wind Power Ltd. - II': 'Gul Ahmed Electric (wind II)',
    'Gul Ahmed Electric Limited': 'Gul Ahmed Electric (wind II)',
}

def norm_key(name):
    s = name.lower()
    s = re.sub(r'\(.*?\)', lambda m: ' ' + m.group(0) + ' ', s)  # keep acronyms as tokens
    s = s.replace('&', ' ').replace('/', ' ').replace('-', ' ').replace('.', ' ').replace(',', ' ')
    s = re.sub(r'[()]', ' ', s)
    toks = [t for t in s.split() if t not in DROP_WORDS]
    return ' '.join(toks).strip()

def build_plants(month):
    names = month[['plant', 'technology', 'fuel', 'installed_mw', 'fiscal_year']].drop_duplicates('plant')
    rows = []
    for r in names.itertuples(index=False):
        k = norm_key(r.plant)
        canon = FULLNAME.get(r.plant.strip()) or ALIASES.get(k)
        if r.plant.strip() in FULLNAME:
            k = 'full:' + r.plant.strip()
        rows.append(dict(plant=r.plant, key=k, canonical=canon))
    df = pd.DataFrame(rows)
    # names sharing a key collapse to one canonical (the longest variant, cleaned of trailing dots/spaces)
    for k, g in df.groupby('key'):
        if g.canonical.isna().all():
            canon = max(g.plant, key=len).strip().rstrip('.').strip()
            canon = re.sub(r'\s+', ' ', canon).replace(' )', ')').replace('( ', '(')
            df.loc[g.index, 'canonical'] = canon
    df['plant_id'] = df.canonical.map(lambda s: re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_'))
    meta = month[month.month == 'FY'].groupby('plant').agg(technology=('technology', 'last'), fuel=('fuel', 'last'),
                                                          installed_mw=('installed_mw', 'max'),
                                                          first_fy=('fiscal_year', 'min'), last_fy=('fiscal_year', 'max')).reset_index()
    df = df.merge(meta, on='plant', how='left')
    plants = df.groupby('plant_id').agg(plant_name=('canonical', 'first'), name_variants=('plant', lambda x: ' || '.join(sorted(set(x)))),
                                        technology=('technology', 'first'), fuel=('fuel', 'first'), installed_mw=('installed_mw', 'max'),
                                        first_fy=('first_fy', 'min'), last_fy=('last_fy', 'max')).reset_index()
    return df[['plant', 'plant_id']], plants

# ------------------------------------------------------------------ 2. curated DISCO series
DISCOS = ['PESCO', 'TESCO', 'IESCO', 'GEPCO', 'LESCO', 'FESCO', 'MEPCO', 'HESCO', 'SEPCO', 'QESCO', 'KE', 'K-ELECTRIC', 'KEL', 'HAZECO', 'TOTAL', 'ALL DISCOS']
SERIES = [  # (series name, title regex, unit)
    ('consumers_by_category', r'category-?wise number of consumers', 'number'),
    ('sanctioned_load_by_category', r'category-?wise sanctioned load', 'MW'),
    ('units_purchased_sold_losses', r'units? purchased.*sold.*loss', 'GWh / %'),
    ('units_billed_amount_realized', r'units billed.*(amount|realiz)', 'GWh / Rs million / %'),
    ('billing_collection_recovery', r'billing.*collection.*recovery', 'Rs million / %'),
    ('electricity_sold_by_category', r'category-?wise electricity sold', 'GWh'),
    ('consumption_per_connection', r'average annual electricity consumption per connection', 'kWh'),
    ('distribution_losses_by_voltage', r'distribution losses \(voltage', 'GWh / %'),
    ('peak_demand', r'peak demand of distribution companies', 'MW'),
    ('feeders_and_loading', r'circles, divisions, sub-?divisions', 'number'),
    ('distribution_lines', r'status of distribution lines', 'km'),
    ('grid_stations', r'status of grid stations', 'number / MVA'),
    ('power_transformers', r'power transformers, capacities', 'number / MVA / %'),
    ('distribution_transformers', r'distribution transformers, capacities', 'number / MVA / %'),
    ('feeder_outages', r'feeders? outages statistics', 'number / hours'),
    ('safety_fatal_accidents', r'safety.*fatal accidents', 'number'),
    ('new_connections', r'new connections|applications of new connections', 'number / MW'),
    ('village_electrification', r'village electrification', 'number'),
    ('receivables', r'receivables of distribution companies', 'Rs million'),
    ('technical_commercial_losses', r"actual technical and commercial losses", '%'),
    ('growth_rate_consumption', r'annual growth rate of electricity consumption', '%'),
]

def fy_norm(s):
    """'2015-16', '2018*', '2016 ', 'FY 2019-20' -> '2015-16' / '2017-18' (as-on-June year -> fiscal year)."""
    if s is None:
        return None
    t = str(s).strip().replace('*', '').replace('FY', '').strip()
    m = re.match(r'^(20\d\d)\s*[-–/]\s*(\d{2,4})$', t)
    if m:
        return f'{m.group(1)}-{m.group(2)[-2:]}'
    m = re.match(r'^(20\d\d)$', t)
    if m:
        y = int(m.group(1)); return f'{y-1}-{str(y)[-2:]}'
    return None

def relabel_ocr_groups(allrows):
    """The 2025 report is a scan with a poor OCR layer: numbers are fine but the vertical DISCO labels are
    garbage. Re-label each garbage group by matching its numbers for the overlapping years against the
    same series in earlier (clean) reports; also fixes the OCR'd column labels by column position."""
    good = allrows[allrows.disco.isin(DISCOS)]
    bad_mask = ~allrows.disco.isin(DISCOS) & allrows.fy.notna()
    bad = allrows[bad_mask]
    if bad.empty:
        return allrows
    # signature: (series, fy, col) -> {disco: value}
    sig = {}
    for r in good.dropna(subset=['value']).itertuples(index=False):
        sig.setdefault((r.series, r.fy, r.col if hasattr(r, 'col') else None), {})[r.disco] = r.value
    # Rules (17 Oct 2026; before then zeros voted, and Total rows were relabelled as PESCO):
    #  * zeros carry no identity -- a zero-filled row matches every DISCO with a zero in that cell --
    #    so only non-zero values vote;
    #  * a group cannot become a DISCO that already appears, cleanly labelled, in the same table on
    #    the same page (a Total row shares PESCO's small net-metering figures, for instance);
    #  * KE, K-ELECTRIC and KEL are one company and vote as one candidate (it takes whichever of its
    #    labels matched most);
    #  * the winner needs >= 3 votes, at least half of the group's comparable values (non-zero cells
    #    that some other report also has), and must beat the runner-up outright.
    ke = {'KE', 'K-ELECTRIC', 'KEL'}
    canon = lambda d: 'KE' if d in ke else d
    present = good.groupby(['report_year', 'page', 'table_no']).disco.agg(lambda s: {canon(d) for d in s}).to_dict()
    fixes = {}
    for (yr, pg, ts, grp), g in bad.groupby(['report_year', 'page', 'table_no', 'disco'], dropna=False):
        votes, labels, comparable = {}, {}, 0
        nz = g[g.value.notna() & (g.value != 0)]
        taken = present.get((yr, pg, ts), set())
        for r in nz.itertuples(index=False):
            cand = {d: v for d, v in sig.get((r.series, r.fy, r.col if hasattr(r, 'col') else None), {}).items()
                    if canon(d) not in taken}
            comparable += bool(cand)
            hit = {d for d, v in cand.items() if v and abs(v - r.value) / abs(v) < 0.002}
            for c in {canon(d) for d in hit}:
                votes[c] = votes.get(c, 0) + 1
            for d in hit:
                labels[d] = labels.get(d, 0) + 1
        if votes:
            ranked = sorted(votes.values(), reverse=True)
            best = max(votes, key=votes.get)
            if (votes[best] >= 3 and votes[best] >= 0.5 * comparable
                    and (len(ranked) == 1 or ranked[0] > ranked[1])):
                if best == 'KE':
                    best = max((d for d in labels if d in ke), key=lambda d: (labels[d], d))
                fixes[(yr, pg, ts, grp)] = best
    if fixes:
        key = list(zip(allrows.report_year, allrows.page, allrows.table_no, allrows.disco))
        allrows = allrows.copy()
        allrows['disco'] = [fixes.get(k, d) for k, d in zip(key, allrows.disco)]
        # column labels: borrow from the latest clean report of the same series, by column index -- the
        # same company's labels first (KE's block has its own column layout), any company's otherwise
        srt = good.sort_values('report_year')
        lab_d = srt.drop_duplicates(['series', 'col', 'disco'], keep='last').set_index(['series', 'col', 'disco']).col_label
        lab = srt.drop_duplicates(['series', 'col'], keep='last').set_index(['series', 'col']).col_label
        fixed_rows = allrows.index[[k in fixes for k in key]]
        allrows.loc[fixed_rows, 'col_label'] = [lab_d.get((s, c, d), lab.get((s, c), l)) for s, c, d, l in
                                                zip(allrows.loc[fixed_rows, 'series'], allrows.loc[fixed_rows, 'col'],
                                                    allrows.loc[fixed_rows, 'disco'], allrows.loc[fixed_rows, 'col_label'])]
        allrows.loc[fixed_rows, 'relabelled'] = True
        print(f'  relabelled {len(fixes)} OCR-garbled DISCO groups ({len(fixed_rows)} rows)', file=sys.stderr)
    return allrows

def build_disco(long):
    long = long.copy()
    long['t'] = long.title.fillna('').str.lower()
    out = []
    for name, pat, unit in SERIES:
        s = long[long.t.str.contains(pat, regex=True)].copy()
        if s.empty:
            continue
        s['disco'] = s.group.fillna('').str.upper().str.replace(r'[^A-Z\- ]', '', regex=True).str.strip()
        # the same company under its full name (SOI 2015) and its pre-2014 name (SOI 2011); exact matches
        # only, so rows such as 'Supply to KESC' (energy sold to Karachi) keep their own label
        s['disco'] = s.disco.replace({'K-ELECTRIC LIMITED': 'K-ELECTRIC', 'KESC': 'K-ELECTRIC'})
        s['fy'] = s.row_label.map(fy_norm)
        # tables with the DISCO in the column header and the year in the row (or vice-versa) are left as-is;
        # keep rows where either the group or the row label identifies a DISCO
        s.loc[~s.disco.isin(DISCOS), 'disco'] = s.loc[~s.disco.isin(DISCOS), 'group']
        s['series'] = name; s['unit_hint'] = unit
        out.append(s[['series', 'report_year', 'table_no', 'title', 'section', 'disco', 'fy', 'row_label', 'col_label', 'value_raw', 'value', 'unit_hint', 'page', 'col']])
    allrows = pd.concat(out, ignore_index=True)
    allrows = relabel_ocr_groups(allrows)
    # de-duplicate overlapping windows: keep the latest report for each (series, disco, fy, col_label)
    keyed = allrows.dropna(subset=['fy'])
    keyed = keyed.sort_values('report_year')
    dedup = keyed.drop_duplicates(subset=['series', 'disco', 'fy', 'col_label', 'section'], keep='last')
    return allrows, dedup

# ------------------------------------------------------------------ 3. main
def main():
    month = pd.read_parquet(WH / 'nepra_plant_month.parquet')
    xw, plants = build_plants(month)
    month = month.drop(columns=[c for c in ['plant_id'] if c in month]).merge(xw, on='plant', how='left')
    month.to_parquet(WH / 'nepra_plant_month.parquet', index=False)
    day = pd.read_parquet(WH / 'nepra_plant_day.parquet')
    day = day.drop(columns=[c for c in ['plant_id'] if c in day]).merge(xw, on='plant', how='left')
    day.to_parquet(WH / 'nepra_plant_day.parquet', index=False)
    plants.to_parquet(WH / 'nepra_plants.parquet', index=False)
    print(f'plants: {len(xw)} name variants -> {len(plants)} plant_ids', file=sys.stderr)

    long = pd.read_parquet(WH / 'nepra_soi_long.parquet')
    allrows, dedup = build_disco(long)
    allrows.to_parquet(WH / 'nepra_disco_annual_all.parquet', index=False)
    dedup.to_parquet(WH / 'nepra_disco_annual.parquet', index=False)
    print(f'disco series: {len(allrows)} source rows -> {len(dedup)} de-duplicated; '
          f'{dedup.series.nunique()} series, FY {dedup.fy.min()}..{dedup.fy.max()}', file=sys.stderr)

    # DuckDB registration (parquet-backed views; the DB stays small)
    os.chdir(WH)
    # cloud mounts may refuse to delete DuckDB's WAL file: work on a scratch copy, copy back at the end
    import shutil, tempfile
    scratch = Path(tempfile.mkdtemp()) / 'data_darbar.duckdb'
    shutil.copy(WH / 'data_darbar.duckdb', scratch)
    con = duckdb.connect(str(scratch))
    for t in ['nepra_plant_month', 'nepra_plant_day', 'nepra_plant_day_files', 'nepra_plants', 'nepra_soi_cells',
              'nepra_soi_long', 'nepra_soi_pages', 'nepra_per_cells', 'nepra_per_long', 'nepra_disco_annual',
              'nepra_disco_annual_all']:
        if (WH / f'{t}.parquet').exists():
            con.execute(f'CREATE OR REPLACE VIEW {t} AS SELECT * FROM read_parquet(\'{t}.parquet\')')
    # one row per DISCO x FY; each measure taken from the most recent report that has it
    con.execute('''CREATE OR REPLACE VIEW nepra_disco_losses AS
        SELECT disco, fy,
               max_by(value, report_year) FILTER (WHERE col_label ILIKE '%total unit purchased%' OR col_label ILIKE '%|unit purchased' OR col_label ILIKE '%|units purchased') AS purchased_gwh,
               max_by(value, report_year) FILTER (WHERE col_label ILIKE '%unit sold%' OR col_label ILIKE '%units sold%') AS sold_gwh,
               max_by(value, report_year) FILTER (WHERE col_label ILIKE 'losses%gwh%') AS losses_gwh,
               max_by(value, report_year) FILTER (WHERE col_label ILIKE 'losses%\\%%' ESCAPE '\\' OR col_label ILIKE 'losses%age%') AS losses_pct,
               max(report_year) AS latest_report
        FROM nepra_disco_annual_all WHERE series='units_purchased_sold_losses' AND fy IS NOT NULL AND value IS NOT NULL
        GROUP BY disco, fy''')
    con.execute('''CREATE OR REPLACE VIEW nepra_disco_consumers AS
        SELECT disco, fy, regexp_replace(col_label, '[^A-Za-z/ ]', '', 'g') AS category,
               max_by(value, report_year) AS consumers, max(report_year) AS latest_report
        FROM nepra_disco_annual_all WHERE series='consumers_by_category' AND fy IS NOT NULL AND value IS NOT NULL
        GROUP BY 1, 2, 3''')
    con.execute('''CREATE OR REPLACE VIEW nepra_disco_sales AS
        SELECT disco, fy, regexp_replace(col_label, '[^A-Za-z/ ]', '', 'g') AS category,
               max_by(value, report_year) AS sold_gwh, max(report_year) AS latest_report
        FROM nepra_disco_annual_all WHERE series='electricity_sold_by_category' AND fy IS NOT NULL AND value IS NOT NULL
        GROUP BY 1, 2, 3''')
    con.execute('''CREATE OR REPLACE VIEW nepra_plant_annual AS
        SELECT plant_id, plant, fiscal_year, technology, fuel, installed_mw, dependable_mw,
               generation_gwh AS generation_gwh, utilization_pct
        FROM nepra_plant_month WHERE month='FY' ''')
    # file catalog rows
    try:
        cat = con.execute('SELECT * FROM file_catalog').df()
    except Exception:
        cat = pd.DataFrame()
    raw = NEPRA_RAW
    rows = []
    for y in range(2010, 2026):
        rows.append(dict(dataset='nepra_soi', category='State of Industry Report', period=f'{y-1}-{str(y)[-2:]}',
                         item=f'SOI {y}', filename=f'SOI Reports/State of Industry Report {y}.pdf', format='pdf',
                         source_url=f'https://nepra.org.pk/publications/State of Industry Reports/State of Industry Report {y}.pdf',
                         status='downloaded', parsed_into_db=y not in (2012, 2013, 2014, 2019)))
    for p in sorted((raw / 'PER').glob('*.pdf')):
        rows.append(dict(dataset='nepra_per', category='Performance Evaluation Report', period=None, item=p.stem,
                         filename=f'PER/{p.name}', format='pdf', source_url='https://nepra.org.pk/publications/Performance Reports.php',
                         status='downloaded', parsed_into_db=True))
    for fy in sorted(month.fiscal_year.unique()):
        rows.append(dict(dataset='nepra_generation', category='plant x month sheet + daily PDFs', period=fy,
                         item=f'List of Companies Generation wise {fy}', filename='html/... + Detail of Generation/... (see nepra_download_manifest.json)',
                         format='htm+pdf', source_url='https://nepra.org.pk/publications/State of Industry Reports/Detail of Generation/SIR Data 2022.htm (and 2023/2024/2025)',
                         status='downloaded', parsed_into_db=True))
    new = pd.DataFrame(rows)
    if not cat.empty:
        cat = cat[~cat.dataset.isin(['nepra_soi', 'nepra_per', 'nepra_generation'])]
        for c in cat.columns:
            if c not in new:
                new[c] = None
        new = new[cat.columns]
        cat = pd.concat([cat, new], ignore_index=True)
    else:
        cat = new
    cat.to_parquet(WH / 'file_catalog.parquet', index=False)
    con.execute('CREATE OR REPLACE TABLE file_catalog AS SELECT * FROM read_parquet(\'file_catalog.parquet\')')
    print(con.execute("SELECT view_name AS table_name FROM duckdb_views() WHERE NOT internal AND view_name LIKE 'nepra%'").df().table_name.tolist(), file=sys.stderr)
    con.close()
    shutil.copy(scratch, WH / 'data_darbar.duckdb')
    wal = WH / 'data_darbar.duckdb.wal'
    if wal.exists():
        open(wal, 'wb').close()  # cannot delete on the mount; an empty WAL is ignored

if __name__ == '__main__':
    main()
