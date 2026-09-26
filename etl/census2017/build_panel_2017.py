"""Turn the Census 2017 extract into a panel, and check it against PBS's own totals.

The extract is a faithful record of what each workbook says. This applies the
four corrections in `normalise_2017.py`, mints the unit roster, and then tests the
result the only way that has ever caught a real error in this project: against
figures PBS published independently.

Two checks run, and both are reported rather than asserted away:

  CLOSURE. For every count series, in every district, the sub-district units must
  sum to the district's own published figure. Rates are excluded, because a rate
  is not additive. The comparison is exact, with no tolerance.

  PROVINCIAL RECONCILIATION. Each province's districts must sum to the figure in
  that province's own table, which is a separate PBS publication reached through
  a different part of the archive page. This is what caught the two headerless
  table-1 workbooks: KP and Punjab were short by exactly Swabi's and Okara's
  populations.
"""
import argparse, collections, csv, json, os, pathlib, re, sys

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'stage2'))

import duckdb
from normalise_2017 import canon_map, fix_unit, split_unit
from read_xls import load
from read_workbook import anchor, numbered_columns

# A rate, ratio, percentage or average is not additive and must not be summed.
IS_RATE = re.compile(r'\b(RATE|RATIO|PERCENT|PROPORTION|AVERAGE|DENSITY|SIZE)\b')


def connect():
    d = duckdb.connect()
    d.execute('SET threads TO 1')
    d.execute('SET preserve_insertion_order=true')
    return d


def area_totals(capture):
    """{(area, measure): value} from the six area table-1 workbooks.

    "Area" rather than "province" on purpose. Pakistan has four provinces -
    Punjab, Sindh, Khyber Pakhtunkhwa and Balochistan. The other two units in the
    2017 census frame are not provinces: FATA was the Federally Administered
    Tribal Areas, a federal territory merged into Khyber Pakhtunkhwa in 2018, and
    Islamabad is the federal capital territory. Calling all six "provinces"
    misdescribes two of them.
    """
    man = json.load(open(os.path.join(capture, 'retrieval_manifest.json')))
    out = {}
    for f in man['files']:
        if f['kind'] != 'xls_area' or f['table'] != '1':
            continue
        rows, merges = load(os.path.join(capture, f['path']))
        a = anchor(rows)
        if a is None:
            continue
        stub, dcols = numbered_columns(rows, a)
        want = f['area'].split()[0][:6].upper()
        for r in rows[a + 1:]:
            lab = str(r[stub] or '').strip().upper()
            if not lab.startswith(want):
                continue
            vals = [r[c] if isinstance(r[c], (int, float)) else None for c in dcols]
            if not any(v is not None for v in vals):
                continue          # Sindh repeats a bare banner row before its totals
            out[f['area']] = vals
            break
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--extract', required=True, help='directory holding observations_2017.csv')
    ap.add_argument('--capture', required=True, help='the dated source capture')
    ap.add_argument('--out', required=True)
    ap.add_argument('--mask', default='', help="directory holding the PDF reconciliation's mask")
    a = ap.parse_args()
    out = pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)

    d = connect()
    src = os.path.join(a.extract, 'observations_2017.csv')
    d.execute(f"""CREATE TABLE raw AS SELECT * FROM read_csv('{src}', header=true,
                  quote='"', escape='"', all_varchar=true)""")
    n_raw = d.execute('SELECT count(*) FROM raw').fetchone()[0]

    # --- the label map, decided on the corpus rather than declared ---
    rows = d.execute("""SELECT table_id, col_label, src_file, locality, sex FROM raw
                        WHERE col_label IS NOT NULL GROUP BY ALL""").fetchall()
    seen = collections.defaultdict(set)
    for t, lab, f, loc, sx in rows:
        seen[(t, lab)].add((f, loc, sx))
    cmap, merged, kept = canon_map(seen)
    with open(out / 'label_map.csv', 'w', newline='') as fh:
        w = csv.writer(fh)
        w.writerow(['table_id', 'raw_label', 'canonical_label', 'files'])
        for (t, lab), canon in sorted(cmap.items()):
            w.writerow([t, lab, canon, len(seen[(t, lab)])])
    d.execute('CREATE TABLE lmap (table_id VARCHAR, raw VARCHAR, canon VARCHAR)')
    d.executemany('INSERT INTO lmap VALUES (?,?,?)',
                  [(t, lab, canon) for (t, lab), canon in cmap.items()])

    # --- unit corrections: spelling, and the locality folded into Kohistan's name ---
    units = [r[0] for r in d.execute('SELECT DISTINCT unit FROM raw WHERE unit IS NOT NULL').fetchall()]
    urows, notes = [], []
    for u in units:
        fixed, note = fix_unit(u)
        base, loc = split_unit(fixed)
        urows.append((u, base, loc))
        if note or loc:
            notes.append(dict(raw=u, unit=base, locality=loc, note=note))
    d.execute('CREATE TABLE umap (raw VARCHAR, unit VARCHAR, loc VARCHAR)')
    d.executemany('INSERT INTO umap VALUES (?,?,?)', urows)

    # --- tehsil -> district register, from table 1, which covers all 134 districts ---
    #
    # Keyed on (province, unit) and restricted to names that resolve to exactly
    # one district. A bare unit name is not unique in Pakistan - SAHIWAL TEHSIL
    # exists in both Sahiwal and Sargodha - so a register that ignored that would
    # match one tehsil to two districts and multiply its rows, which is how the
    # first attempt produced 25,500 observations more than it read.
    d.execute("""CREATE TABLE reg AS
                 WITH pairs AS (
                   SELECT DISTINCT r.province, m.unit AS unit, dm.unit AS district
                   FROM raw r JOIN umap m ON m.raw = r.unit
                   JOIN umap dm ON dm.raw = r.district
                   WHERE r.table_id='1' AND r.district IS NOT NULL
                 )
                 SELECT province, unit, min(district) AS district
                 FROM pairs GROUP BY 1, 2 HAVING count(DISTINCT district) = 1""")
    ambiguous = d.execute("""WITH pairs AS (
                   SELECT DISTINCT r.province, m.unit AS unit, dm.unit AS district
                   FROM raw r JOIN umap m ON m.raw = r.unit
                   JOIN umap dm ON dm.raw = r.district
                   WHERE r.table_id='1' AND r.district IS NOT NULL)
                 SELECT province, unit, count(DISTINCT district) n
                 FROM pairs GROUP BY 1,2 HAVING count(DISTINCT district) > 1
                 ORDER BY 3 DESC, 1, 2""").fetchall()

    d.execute("""CREATE TABLE panel AS
      SELECT 2017 AS census_year, r.province,
             r.table_id,
             coalesce(dm.unit, reg.district)                AS district,
             m.unit                                        AS unit,
             r.unit_type,
             coalesce(m.loc, r.locality)                   AS locality,
             r.sex,
             r.indicator,
             coalesce(lm.canon, r.col_label)               AS col_label,
             TRY_CAST(r.value AS DOUBLE)                   AS value,
             r.missing = 'True'                            AS missing,
             r.layout, r.src_file, r.src_row, r.src_col
      FROM raw r
      JOIN umap m        ON m.raw = r.unit
      LEFT JOIN umap dm  ON dm.raw = r.district
      LEFT JOIN reg      ON reg.unit = m.unit AND reg.province = r.province
      LEFT JOIN lmap lm  ON lm.table_id = r.table_id AND lm.raw = r.col_label
      ORDER BY r.table_id, district, unit, locality, sex, indicator, col_label,
               r.src_row, r.src_col""")

    # --- the dash the spreadsheet turned into a zero ---
    #
    # PBS's 2017 spreadsheets are a PDF-to-Excel conversion, and in 29.5% of the
    # cells checked the printed missing-value dash arrived as 0. Left alone, the
    # panel cannot tell "none" from "not reported", and any rate computed over
    # those columns is wrong.
    #
    # The mask comes from `reconcile_pdf.py`, which compares the two renderings
    # directly. It is keyed on (district, table, src_row, src_col) - the position
    # in the workbook - rather than on the row's printed label, because a label
    # like "10 -- 14" recurs once per unit, locality and sex.
    #
    # Only the value's status changes: a masked cell keeps its 0 in `value` and
    # gains `missing = true`, so nothing is destroyed and the correction can be
    # undone by ignoring the flag.
    masked_n = 0
    if a.mask:
        mpath = os.path.join(a.mask, 'missingness_mask_2017.csv')
        if not os.path.exists(mpath):
            mpath += '.gz'
        d.execute(f"""CREATE TABLE mask AS
                      SELECT DISTINCT district, table_id,
                             CAST(src_row AS INTEGER) AS src_row,
                             CAST(src_col AS INTEGER) AS src_col
                      FROM read_csv('{mpath}', header=true, quote='"', escape='"')""")
        # The two sides name districts differently - the mask carries the
        # spreadsheet directory's name (ABBOTTABAD) and the panel the workbook's
        # own label (ABBOTTABAD DISTRICT) - so the join is on letters only, with
        # the tier word removed. The same normalisation pairs the renderings in
        # `reconcile_pdf.py`.
        d.execute("""CREATE TABLE mask2 AS
                     SELECT DISTINCT
                            replace(replace(replace(
                              regexp_replace(upper(district), '[^A-Z]', '', 'g'),
                              'DISTRICT',''),'AGENCY',''),'PROTECTEDAREA','') AS dkey,
                            table_id, src_row, src_col
                     FROM mask""")
        d.execute("""CREATE TABLE panel_m AS
                     SELECT p.* EXCLUDE (missing),
                            (p.missing OR k.src_row IS NOT NULL) AS missing
                     FROM panel p
                     LEFT JOIN mask2 k
                       ON  k.table_id = p.table_id
                       AND k.dkey = replace(replace(replace(
                             regexp_replace(upper(p.district), '[^A-Z]', '', 'g'),
                             'DISTRICT',''),'AGENCY',''),'PROTECTEDAREA','')
                       AND k.src_row = CAST(p.src_row AS INTEGER)
                       AND k.src_col = CAST(p.src_col AS INTEGER)""")
        before = d.execute('SELECT count(*) FROM panel WHERE missing').fetchone()[0]
        after = d.execute('SELECT count(*) FROM panel_m WHERE missing').fetchone()[0]
        masked_n = after - before
        d.execute('DROP TABLE panel')
        d.execute('ALTER TABLE panel_m RENAME TO panel')

    # --- uniqueness: does the panel's own key identify a single figure? ---
    #
    # A series is identified by (table, unit, district, locality, sex, indicator,
    # col_label). Where two rows share that key AND disagree on the value, a real
    # distinction present in the workbook has been lost - the stub nests deeper
    # than the declared level stack, or a column header carries a dimension the
    # spec does not name. Closure does not catch this, because the duplicate
    # appears on both the district and the sub-district side and sums consistently:
    # the check passes while the series is wrong. Those tables are marked so that
    # nothing downstream treats them as settled.
    # col_label is NULL wherever the whole column header is consumed by the
    # locality and sex dimensions - all of tables 4, 5 and 20 - and NULL never
    # equals NULL in a join, so the key is coalesced to a sentinel. Without it the
    # flag silently skipped table 5's 1,803 affected rows and reported the table
    # as clean.
    d.execute("""CREATE TABLE dupes AS
                 SELECT table_id, unit, district, locality, sex, indicator,
                        coalesce(col_label, '~none~') AS label_key,
                        count(*) AS n, count(DISTINCT value) AS n_values
                 FROM panel GROUP BY ALL HAVING count(*) > 1""")
    # The flag is per KEY, not per table. A table-level flag marked 2,459,883
    # observations when 55,792 rows are actually affected - table 14 has 1.19m
    # rows and 4,632 bad ones, so calling the whole table unusable is 250 times
    # worse than the truth and would bury the tables that really are clean.
    d.execute("""CREATE TABLE panel2 AS
                 SELECT p.*, (k.table_id IS NOT NULL) AS series_ambiguous
                 FROM panel p
                 LEFT JOIN (SELECT DISTINCT table_id, unit, district, locality, sex,
                                   indicator, label_key
                            FROM dupes WHERE n_values > 1) k
                 ON  k.table_id = p.table_id AND k.unit = p.unit
                 AND k.district IS NOT DISTINCT FROM p.district
                 AND k.locality = p.locality AND k.sex = p.sex
                 AND k.indicator IS NOT DISTINCT FROM p.indicator
                 AND k.label_key = coalesce(p.col_label, '~none~')""")
    d.execute('DROP TABLE panel')
    d.execute('ALTER TABLE panel2 RENAME TO panel')
    ambiguous_tables = [r[0] for r in d.execute(
        'SELECT DISTINCT table_id FROM dupes WHERE n_values > 1 ORDER BY 1').fetchall()]

    stats = dict(
        observations=d.execute('SELECT count(*) FROM panel').fetchone()[0],
        raw_observations=n_raw,
        districts=d.execute('SELECT count(DISTINCT district) FROM panel').fetchone()[0],
        units=d.execute('SELECT count(DISTINCT unit) FROM panel').fetchone()[0],
        tables=d.execute('SELECT count(DISTINCT table_id) FROM panel').fetchone()[0],
        series=d.execute('SELECT count(DISTINCT (table_id, indicator, col_label)) FROM panel').fetchone()[0],
        no_district=d.execute('SELECT count(*) FROM panel WHERE district IS NULL').fetchone()[0],
        label_groups_merged=len(merged),
        label_groups_kept_distinct=len(kept),
        ambiguous_unit_names=len(ambiguous),
        cells_masked_from_pdf=masked_n,
        tables_touched_by_ambiguity=ambiguous_tables,
        observations_flagged_ambiguous=d.execute(
            'SELECT count(*) FROM panel WHERE series_ambiguous').fetchone()[0],
        duplicate_keys_with_differing_values=d.execute(
            'SELECT count(*) FROM dupes WHERE n_values > 1').fetchone()[0],
    )

    # --- closure ---
    d.execute(f"""CREATE TABLE closure AS
      WITH c AS (
        SELECT table_id, district, locality, sex, indicator, col_label,
               sum(CASE WHEN unit_type='district' THEN value END)  AS published,
               sum(CASE WHEN unit_type<>'district' THEN value END) AS parts,
               count(CASE WHEN unit_type<>'district' THEN 1 END)   AS n_parts
        FROM panel
        WHERE NOT missing AND value IS NOT NULL AND district IS NOT NULL
          AND NOT regexp_matches(upper(coalesce(indicator,'') || ' ' || coalesce(col_label,'')),
                                 '{IS_RATE.pattern[2:-2]}')
        GROUP BY ALL
      )
      SELECT *, abs(published - parts) < 0.5 AS closes FROM c
      WHERE published IS NOT NULL AND n_parts > 0""")
    tot = d.execute('SELECT count(*) FROM closure').fetchone()[0]
    ok = d.execute('SELECT count(*) FROM closure WHERE closes').fetchone()[0]
    stats['closure_comparisons'] = tot
    stats['closure_pass'] = ok

    # --- recovered unit labels: a one-sided bound that is exact ---
    #
    # A unit cannot contain more people than table 1 says it contains. So for
    # every unit whose name was recovered, the largest count anywhere in that
    # table must not exceed its table 1 population. This is the check that caught
    # the recovery naming Kohistan's own rural sub-block KANDIA SUB-DIVISION: it
    # held 228,350 where Kandia has 77,101.
    #
    # One-sided on purpose. Tables report different universes - 10 years and
    # above, literate population, housing units - so equality cannot be required,
    # but exceeding the unit's own population is impossible under any universe
    # and needs no judgement.
    rec = json.load(open(os.path.join(a.extract, 'extract_report_2017.json'))
                    ).get('recovered_unit_labels', [])
    recovered = [r for r in rec if r.get('applied')]
    checks = []
    for r in recovered:
        pop = d.execute("""
            SELECT max(value) FROM panel
            WHERE table_id='1' AND unit=? AND district=? AND locality='all'
              AND col_label='ALL SEXES' AND NOT missing""",
            [r['unit'], r['district']]).fetchone()[0]
        biggest = d.execute("""
            SELECT max(value) FROM panel
            WHERE table_id=? AND unit=? AND district=? AND NOT missing
              AND NOT regexp_matches(
                  upper(coalesce(indicator,'') || ' ' || coalesce(col_label,'')), ?)""",
            [r['table'], r['unit'], r['district'], IS_RATE.pattern[2:-2]]).fetchone()[0]
        within = (pop is None or biggest is None or biggest <= pop + 0.5)
        checks.append(dict(table_id=r['table'], district=r['district'], unit=r['unit'],
                           table1_population=pop, largest_count=biggest,
                           within_bound=within))
    stats['unit_labels_recovered'] = len(recovered)
    stats['unit_labels_within_bound'] = sum(1 for c in checks if c['within_bound'])

    # --- reconciliation against each area's own published table ---
    pub = area_totals(a.capture)
    recon = []
    for prov, vals in sorted(pub.items()):
        if prov == 'PAKISTAN':
            continue
        got = d.execute("""SELECT sum(value) FROM panel
                           WHERE unit_type='district' AND locality='all' AND table_id='1'
                             AND col_label ILIKE '%ALL SEXES%' AND province = ?""",
                        [prov]).fetchone()[0]
        recon.append(dict(area=prov, published=vals[1], panel=got,
                          exact=(got is not None and abs(vals[1] - got) < 0.5)))
    stats['areas_reconciled'] = sum(1 for r in recon if r['exact'])
    stats['areas_checked'] = len(recon)

    d.execute(f"COPY panel TO '{out / 'panel_2017.parquet'}' (FORMAT PARQUET)")
    d.execute(f"COPY closure TO '{out / 'closure_checks_2017.csv'}' (HEADER, DELIMITER ',', QUOTE '\"', ESCAPE '\"')")
    d.execute(f"COPY dupes TO '{out / 'ambiguous_series_2017.csv'}' (HEADER, DELIMITER ',', QUOTE '\"', ESCAPE '\"')")
    d.execute(f"COPY reg TO '{out / 'unit_register_2017.csv'}' (HEADER, DELIMITER ',')")
    json.dump(dict(stats=stats, area_reconciliation=recon,
                   recovered_unit_checks=checks,
                   unit_corrections=notes,
                   ambiguous_unit_names=[dict(province=p, unit=u, districts=n)
                                         for p, u, n in ambiguous],
                   label_groups_merged=[dict(table_id=t, canonical=c, variants=v)
                                        for t, c, v in merged],
                   label_groups_kept_distinct=[dict(table_id=t, variants=v) for t, v in kept]),
              open(out / 'panel_report_2017.json', 'w'), indent=1, sort_keys=True, default=str)

    print(json.dumps(stats, indent=1, sort_keys=True))
    print('\nreconciliation against each area\'s own table, on total population')
    print('(four provinces, plus FATA and the federal capital territory):')
    for r in recon:
        mark = 'EXACT' if r['exact'] else f"differs by {(r['published'] or 0) - (r['panel'] or 0):,.0f}"
        print(f"  {r['area']:22s} published {r['published']:>14,.0f}  "
              f"panel {(r['panel'] or 0):>14,.0f}  {mark}")
    if checks:
        bad = [c for c in checks if not c['within_bound']]
        print(f'\nrecovered unit labels: {len(checks)}, '
              f'{len(checks) - len(bad)} within their table 1 population')
        for c in bad:
            print(f"  EXCEEDS BOUND table {c['table_id']} {c['unit']}: largest "
                  f"{c['largest_count']:,.0f} > population {c['table1_population']:,.0f}")
    if masked_n:
        print(f"\ndash-as-zero cells recovered from the PDFs: {masked_n:,}")
    print(f"\nclosure: {ok:,} of {tot:,} comparisons pass exactly")
    if ambiguous_tables:
        print(f"series key not unique in tables {', '.join(ambiguous_tables)}: "
              f"{stats['observations_flagged_ambiguous']:,} of "
              f"{stats['observations']:,} rows flagged")


if __name__ == '__main__':
    main()
