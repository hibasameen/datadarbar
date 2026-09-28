"""The headline macro series, and the quarterly national accounts.

WHAT THIS GOT WRONG THE FIRST TIME, because the column was named after the
wrong thing. gdp_indicators.gdp_constant_pkr_mn is not GDP. It is GVA at basic
prices - PBS's "D GDP {Total of GVA at bp}" - and the national accounts put
three different aggregates in a column each:

    GVA at basic prices          38.843tn   FY2021-22
    + taxes - subsidies
    = GDP at market prices       40.970tn
    + net primary income         + 2.807tn
    = gross national income      43.776tn

The first version labelled the first of those GDP, added NPI to it, and called
the result GNI - producing 41.650tn against a published 43.776tn, two trillion
rupees adrift. Worse, it coalesced a missing NPI to zero, so the last two
years drew a GNI line sitting exactly on top of GDP: a line for a quantity
nobody had measured. The NPI was not missing from PBS at all, only from the
gdp_indicators extract.

So the aggregates come from national_accounts Table 5 now, which carries all
four on the 2015-16 constant-price base for 27 years, and both identities are
asserted rather than assumed: GVA + taxes - subsidies = GDP, and GDP + NPI =
GNI, to the rupee in every year. Nothing is coalesced. A missing value stays
missing and the chart leaves a gap.

AND THE INCOME SERIES WAS NOT MISSING EITHER. The page said per-person income
was not published for 2024-25 or 2025-26 and called 2021-22 the peak. PBS
publishes both years, on two population bases, and on the 2017-Census basis
they are 198,864 and 203,118 - above the 192,848 that was being called a
peak. The peak claim was an artefact of an incomplete extract. Both bases are
carried here, because from 2023-24 PBS reprojects on the 2023 Census and the
two series are not continuations of each other.

The second half of this file is unchanged: gva_by_activity_quarterly, the only
quarterly output series Pakistan publishes, and the only place the lockdown
quarter is an event rather than a slightly worse year. Its four quarters
reconcile against GVA - which is what they sum to - not against GDP.

Usage: build_macro_payload.py --warehouse <dir> --out <js>
"""
import argparse, json, pathlib

import duckdb


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    W = pathlib.Path(a.warehouse).as_posix()
    rows = lambda sql: [list(r) for r in con.sql(sql).fetchall()]

    # Table 5 is the national accounts at 2015-16 constant prices. One row per
    # aggregate per year, so it is pivoted here; nothing is coalesced, and a
    # year PBS has not published stays null so the chart can leave a gap.
    NA = f"'{W}/national_accounts.parquet'"
    head = rows(f"""
        WITH t AS (
          SELECT year,
            max(CASE WHEN item LIKE 'D GDP%'            THEN value END) AS gva,
            max(CASE WHEN item = 'E Taxes'              THEN value END) AS tax,
            max(CASE WHEN item = 'F Subsidies'          THEN value END) AS sub,
            max(CASE WHEN item LIKE 'G GDP at mp%'      THEN value END) AS gdp,
            max(CASE WHEN item LIKE 'H Net Primary%'    THEN value END) AS npi,
            max(CASE WHEN item LIKE 'I Gross National%' THEN value END) AS gni,
            max(CASE WHEN item LIKE 'K Per Capita%2017%'    THEN value END) AS pci17,
            max(CASE WHEN item LIKE 'Per Capita%2023%'      THEN value END) AS pci23
          FROM {NA} WHERE table_sheet = 'Table 5' GROUP BY 1)
        SELECT t.year AS fy, CAST(substr(t.year, 1, 4) AS INT) + 1 AS fy_end,
               round(t.gva / 1e6, 3) AS gva_tn,
               round(t.gdp / 1e6, 3) AS gdp_tn,
               round(t.gni / 1e6, 3) AS gni_tn,
               round(t.npi / 1e6, 3) AS npi_tn,
               CASE WHEN t.pci17 IS NULL THEN NULL ELSE round(t.pci17) END AS pci17,
               CASE WHEN t.pci23 IS NULL THEN NULL ELSE round(t.pci23) END AS pci23,
               round(g.exchange_rate_pkr_per_usd, 2) AS usd
        FROM t LEFT JOIN '{W}/gdp_indicators.parquet' g ON g.fy = t.year
        ORDER BY fy_end""")

    # Every activity, every quarter. 880 rows is small enough to ship whole and
    # aggregate in the browser, which keeps the sector / subsector / activity
    # views one dataset rather than three pre-baked ones that could disagree.
    qtr = rows(f"""
        SELECT fy, fy_end, quarter, sector, subsector, category,
               round(gva_constant_pkr_mn, 1)
        FROM '{W}/gva_by_activity_quarterly.parquet'
        WHERE gva_constant_pkr_mn IS NOT NULL
        ORDER BY fy_end, quarter, sector, subsector, category""")

    # Does the quarterly split add back up to the year? Reported, not assumed.
    recon = rows(f"""
        SELECT q.fy,
               round(sum(q.gva_constant_pkr_mn) / 1e6, 3)               AS qtr_tn,
               round(any_value(g.gdp_constant_pkr_mn) / 1e6, 3)         AS ann_tn,
               round(100.0 * sum(q.gva_constant_pkr_mn)
                     / any_value(g.gdp_constant_pkr_mn), 2)             AS pct
        FROM '{W}/gva_by_activity_quarterly.parquet' q
        JOIN '{W}/gdp_indicators.parquet' g USING (fy)
        GROUP BY 1 ORDER BY 1""")

    payload = {
        'headline': {'rows': head,
                     'cols': ['fy', 'fy_end', 'gva_tn', 'gdp_tn', 'gni_tn',
                              'npi_tn', 'pci17', 'pci23', 'usd']},
        'quarterly': {'rows': qtr,
                      'cols': ['fy', 'fy_end', 'quarter', 'sector',
                               'subsector', 'category', 'gva_mn']},
        'qtr_check': {'rows': recon, 'cols': ['fy', 'qtr_tn', 'ann_tn', 'pct']},
    }
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        '/* Data Darbar - headline macro series and quarterly national\n'
        '   accounts. Generated by etl/economy/build_macro_payload.py;\n'
        '   do not edit by hand. */\n'
        'window.DD_MACRO=' + json.dumps(payload, separators=(',', ':')) + ';\n')

    # ── the identities, checked rather than assumed ───────────────────────
    # These are what the first version got wrong by not asking. Both hold to
    # the rupee in every one of the 27 years, so any tolerance here would be
    # hiding a problem rather than allowing for rounding.
    C = {n: i for i, n in enumerate(
        ['fy', 'fy_end', 'gva_tn', 'gdp_tn', 'gni_tn', 'npi_tn',
         'pci17', 'pci23', 'usd'])}
    con.execute(f"""CREATE OR REPLACE TEMP VIEW t5 AS
        SELECT year,
          max(CASE WHEN item LIKE 'D GDP%'            THEN value END) AS gva,
          max(CASE WHEN item = 'E Taxes'              THEN value END) AS tax,
          max(CASE WHEN item = 'F Subsidies'          THEN value END) AS sub,
          max(CASE WHEN item LIKE 'G GDP at mp%'      THEN value END) AS gdp,
          max(CASE WHEN item LIKE 'H Net Primary%'    THEN value END) AS npi,
          max(CASE WHEN item LIKE 'I Gross National%' THEN value END) AS gni
        FROM '{W}/national_accounts.parquet'
        WHERE table_sheet = 'Table 5' GROUP BY 1""")
    bad_gdp = con.sql('SELECT count(*) FROM t5 '
                      'WHERE abs(gva + tax - sub - gdp) > 0.5').fetchone()[0]
    bad_gni = con.sql('SELECT count(*) FROM t5 '
                      'WHERE abs(gdp + npi - gni) > 0.5').fetchone()[0]
    assert not bad_gdp, f'{bad_gdp} years where GVA + taxes - subsidies != GDP'
    assert not bad_gni, f'{bad_gni} years where GDP + NPI != GNI'

    # And nothing silently substituted for a missing figure: a GNI that equals
    # its GDP to the rupee is what the old COALESCE produced.
    same = [r[C['fy']] for r in head
            if r[C['gni_tn']] is not None and r[C['gdp_tn']] is not None
            and r[C['gni_tn']] == r[C['gdp_tn']]]
    assert not same, f'GNI equals GDP exactly in {same} - a missing NPI read as zero'

    # The quarters must reconstruct the year, or the chart is drawing a
    # different economy from the one on the rest of the page. They sum to GVA,
    # which is what the quarterly table publishes - not to GDP.
    worst = min(r[3] for r in recon)
    assert worst > 99.5, f'quarters only reach {worst}% of the annual GVA'
    assert len({(r[0], r[2]) for r in qtr}) == len(recon) * 4, \
        'a fiscal year is missing a quarter'
    for k in payload:
        print(f'  {k:<10} {len(payload[k]["rows"]):>5,} rows')
    last = head[-1]
    print(f"  {last[C['fy']]}: GVA {last[C['gva_tn']]}tn, GDP {last[C['gdp_tn']]}tn, "
          f"GNI {last[C['gni_tn']]}tn")
    n23 = sum(1 for r in head if r[C['pci23']] is not None)
    print(f'  per-person income on the 2017-Census basis for all '
          f'{sum(1 for r in head if r[C["pci17"]] is not None)} years, '
          f'and on the 2023-Census basis for the last {n23}')
    print('  identities hold to the rupee in every year; no value coalesced')
    print(f'  quarters reconcile to the annual GVA: {worst:.2f}% at worst')
    print(f'  -> {out} ({out.stat().st_size/1e3:.1f} KB)')


if __name__ == '__main__':
    main()
