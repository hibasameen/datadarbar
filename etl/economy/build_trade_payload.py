"""Trade totals and partners, rebuilt from the warehouse.

TWO NUMBERS FOR THE SAME YEAR, and the page showed whichever it reached for.
The old econ_data.js block put FY2023-24 imports at Rs14,382.6bn; the
warehouse's published monthly totals put them at Rs15,488.4bn, 7.7 per cent
higher, and FY2021-22 differs by 6.7 per cent.

Neither is an arithmetic mistake. The old block was built by adding up D-10
detail tables, and its own note says those cover "~93-100% of grand total".
One is a sum of the detail PBS managed to publish; the other is the total PBS
published. Only the second is the country's trade, so it is canonical here,
and the coverage of the detail travels beside it rather than sitting in a
footer: for each year this payload records what share of the published total
the partner tables actually name. Before 2011-12 that is 85 to 91 per cent of
imports - a seventh of everything Pakistan bought attributed to no country at
all - so a chart of that year's largest partners is a chart of the partners
PBS managed to name, and the number saying so is in the payload.

Rebuilding also lengthens the series: the warehouse carries 2003-04 onward
where the artefact began in 2015-16.

WHAT THE SCREEN IS FOR. The pull of 2026-09-27 returns a Q1 for FY2025-26 of
$33,973m of imports, against a range of $11,200m to $18,800m across the seven
years before it, with Q2 reverting immediately to $17,526m. Imports do not
double for one quarter and halve the next. Every endpoint agrees on it -
months, quarters, country rows and group rows are all self-consistent - which
means the defect is in the table they are all drawn from, and no amount of
cross-checking between endpoints would have caught it. Only the shape of the
series does. quarter_screen() is that check, run on every year rather than on
the one somebody thought to look at, and the years it flags are marked in the
payload so the page can refuse to draw them as fact.

Usage: build_trade_payload.py --warehouse <dir> --out <js>
"""
import argparse, json, pathlib, statistics

import duckdb

TOP_N = 14

# PBS writes partner names into a field about twenty-two characters wide and
# lets them break where they will, so the United States reaches the page as
# "U.S.America" and Hong Kong as "Hong Kong S.A.Re.Chi". These are the
# mangled ones, spelled out. Only names that actually occur are allowed - a
# key that matches nothing fails the build, so this list cannot quietly rot
# into a record of what PBS used to call things.
#
# "O.Asia(Tai.For.Pe.Ki)" is not a typo but a euphemism: the Comtrade code
# "Other Asia, not elsewhere specified", which in Pakistan's returns is
# Taiwan and Rs1,472bn of trade. It is named here rather than left as a
# cipher, with the classification it comes from kept in the label.
COUNTRY_NAME = {
    'U.S.America': 'United States',
    'US.Minor Outlying Is.': 'US Minor Outlying Islands',
    'US.Virgin Islands': 'US Virgin Islands',
    'O.Asia(Tai.For.Pe.Ki)': 'Taiwan (Other Asia, n.e.s.)',
    'Hong Kong S.A.Re.Chi': 'Hong Kong',
    'Macao.China': 'Macao',
    'Korea, Republic of': 'South Korea',
    'Korea D.P.Republic': 'North Korea',
    'D.R.of Congo': 'DR Congo',
    'Congo, Republic of': 'Republic of the Congo',
    'U.R.of Tanzania': 'Tanzania',
    'Egypt(U.A.R.)': 'Egypt',
    'Burkina Faso(Fr.Up.V)': 'Burkina Faso',
    'Cambodia Fr.Kampuche': 'Cambodia',
    'Co,te d,Ivoire(Fr.Iv)': "C\u00f4te d'Ivoire",
    'Equatorial G.(Ri.Mi)': 'Equatorial Guinea',
    'Fr.Yugoslav R.Macedo': 'North Macedonia',
    "Lao People's D.R.": 'Laos',
    'Venezuela,Bolivar. R': 'Venezuela',
    'Micronesia,F.States': 'Micronesia',
    'Republic of Moldova': 'Moldova',
    'Kyrgyzstan/Kyrgyz R.': 'Kyrgyzstan',
    'Cayman  Islands': 'Cayman Islands',
    'B.Indian Ocean Terr.': 'British Indian Ocean Territory',
    'Falkland Is.Malvinas': 'Falkland Islands',
    'S.Georgia S.Sandw.Is': 'South Georgia and the South Sandwich Islands',
    'St.Kitts and Nevis': 'St Kitts and Nevis',
    'St.Pierre & Miquelon': 'St Pierre and Miquelon',
    'St.Vincent/Grenadine': 'St Vincent and the Grenadines',
    'Svalbard/Jan May.Is.': 'Svalbard and Jan Mayen',
    'Northern Mariana Is.': 'Northern Mariana Islands',
    'French Southern Terr': 'French Southern Territories',
    'Heard/Mac Donald Is.': 'Heard Island and McDonald Islands',
    'Cocos (Keeling) Is.': 'Cocos (Keeling) Islands',
    'Wallis & Futuna Is.': 'Wallis and Futuna',
    'Palau(Fr.pacific Is)': 'Palau',
    'Western Sahara(Ri.De)': 'Western Sahara',
    'Saint Martin(French P)': 'Saint Martin (French part)',
    'Saint Martin(Dutch po)': 'Sint Maarten',
    'Bonaire Saint & Saba': 'Bonaire, Sint Eustatius and Saba',
    'Sao Tome & Principe': 'S\u00e3o Tom\u00e9 and Pr\u00edncipe',
    'Bosnia & Herzegovina': 'Bosnia and Herzegovina',
}

# A quarter this far from the recent typical quarter is not a business cycle.
HI, LO = 1.8, 0.45
BASE_YEARS = 3


def quarter_screen(con, src):
    """Flag quarters that break the shape of the series.

    Each quarter is measured against the median quarter of the three fiscal
    years before it, which is a wide enough base to absorb a real surge - the
    2021-22 import boom passes - and narrow enough to sit near the current
    level. Judged in dollars, so a devaluation year is not flagged for it.
    """
    rows = con.sql(f"""
        SELECT fy, period, sum(imports_usd) AS v FROM {src}
        WHERE period IN ('Q1','Q2','Q3','Q4') GROUP BY 1, 2""").fetchall()
    by_fy = {}
    for fy, q, v in rows:
        by_fy.setdefault(fy, {})[q] = v
    years = sorted(by_fy)
    flags = {}
    for i, fy in enumerate(years):
        base = [v for y in years[max(0, i - BASE_YEARS):i]
                for v in by_fy[y].values() if v]
        if len(base) < 4 * BASE_YEARS:
            continue  # not enough history behind it to judge
        med = statistics.median(base)
        for q in sorted(by_fy[fy]):
            v = by_fy[fy][q]
            if not v:
                continue
            r = v / med
            if r > HI or r < LO:
                flags.setdefault(fy, []).append(
                    f'{q} imports are {r:.1f} times the typical quarter of the '
                    f'three years before it (${v/1e6:,.0f}m against a median '
                    f'of ${med/1e6:,.0f}m). PBS publishes this figure on every '
                    f'endpoint; it is not credible as trade.')
    return flags


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--warehouse', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()

    con = duckdb.connect()
    con.execute('SET threads TO 1')
    W = pathlib.Path(a.warehouse).as_posix()
    TOT = f"'{W}/trade_monthly_totals.parquet'"
    CTY = f"'{W}/trade_by_country.parquet'"
    GRP = f"'{W}/trade_by_group.parquet'"

    # ── the published totals ──────────────────────────────────────────────
    tot = con.sql(f"""
        SELECT fy,
               round(sum(exports_pkr) / 1e9, 1) AS exp_bn,
               round(sum(imports_pkr) / 1e9, 1) AS imp_bn,
               count(*) AS months
        FROM {TOT} GROUP BY 1 ORDER BY fy""").fetchall()
    years = [r[0] for r in tot]
    months = {r[0]: r[3] for r in tot}
    totals = {
        'years': years,
        'export': {r[0]: r[1] for r in tot},
        'import': {r[0]: r[2] for r in tot},
        'balance': {r[0]: round(r[1] - r[2], 1) for r in tot},
        'months': months,
    }
    assert len(years) > 20, f'only {len(years)} years of totals'
    for r in tot:
        assert r[1] > 0 and r[2] > 0, f'{r[0]} has a non-positive total'

    flags = quarter_screen(con, GRP)
    for fy, n in months.items():
        if n < 12:
            flags.setdefault(fy, []).insert(
                0, f'{n} of 12 months published; the year is not complete.')

    # The last year fit to be quoted without a caveat, and where a chart of
    # the published series should stop by default.
    last_clean = next(y for y in reversed(years) if y not in flags)

    # ── partners, and how much of the total they account for ──────────────
    # FY_from_quarters is the fiscal-year row the country tables assemble from
    # their own quarters; the monthly rows would double count against it.
    part = {}
    for direction, col in (('export', 'exports_pkr'), ('import', 'imports_pkr')):
        by_year = {}
        for fy, country, bn in con.sql(f"""
                SELECT fy, country, round(sum({col}) / 1e9, 2) AS bn
                FROM {CTY} WHERE period = 'FY_from_quarters' AND {col} > 0
                GROUP BY 1, 2 ORDER BY fy, bn DESC""").fetchall():
            by_year.setdefault(fy, []).append(
                {'country': COUNTRY_NAME.get(country, country), 'bn': bn})
        part[direction] = {y: v[:TOP_N] for y, v in by_year.items()}

    # Assembled from quarters because the country endpoint's monthly rows
    # have no June in any year - a year summed from them is an eleven-month
    # year, short by about a month in twelve. The quarters are complete, at
    # the cost of overshooting exports (3.3 per cent on average, most often
    # in Q4), which is why this ratio can sit above 100.
    seen = {r[0] for r in con.sql(
        f'SELECT DISTINCT country FROM {CTY}').fetchall()}
    stale = sorted(set(COUNTRY_NAME) - seen)
    assert not stale, f'COUNTRY_NAME keys match no partner in the data: {stale}'

    cover = {}
    for fy, e, i in con.sql(f"""
            SELECT fy, round(sum(exports_pkr) / 1e9, 1),
                       round(sum(imports_pkr) / 1e9, 1)
            FROM {CTY} WHERE period = 'FY_from_quarters'
            GROUP BY 1""").fetchall():
        if fy in totals['export']:
            cover[fy] = {
                'export': round(100.0 * e / totals['export'][fy], 1),
                'import': round(100.0 * i / totals['import'][fy], 1),
            }

    payload = {
        'totals': totals, 'partners': part, 'coverage': cover,
        'flags': flags, 'last_clean': last_clean,
        'country_renames': COUNTRY_NAME,
        'meta': {
            'totals_src': 'PBS External Trade Statistics, monthly totals as '
                          'published (trade_monthly_totals)',
            'partners_src': 'PBS trade by partner country, fiscal year '
                            'assembled from quarters (trade_by_country)',
            'units': 'Rs billion',
            'coverage_note': 'The partner year measured against the '
                             'published total. Below 100 it is trade no '
                             'country is named for: before 2011-12 that is '
                             '85 to 91 per cent of imports, so a chart of '
                             'those years shows the partners PBS managed to '
                             'name, not all of them. Above 100 it is the '
                             'quarterly assembly overshooting, which PBS '
                             'does on exports by 3.3 per cent on average.',
            'flags_note': 'Years whose quarterly shape breaks against the '
                          'three years before them, or which are incomplete. '
                          'Charts of the published series stop at '
                          f'{last_clean} unless the reader asks for more.',
            'vintage': 'PBS National Trade Database, pull of 2026-09-27',
        },
    }
    out = pathlib.Path(a.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(
        '/* Data Darbar - trade totals and partners. Generated by\n'
        '   etl/economy/build_trade_payload.py; do not edit by hand. */\n'
        'window.DD_TRADE=' + json.dumps(payload, separators=(',', ':')) + ';\n')

    imp_cov = [v['import'] for v in cover.values()]
    print(f'  {len(years)} fiscal years, {years[0]} to {years[-1]}')
    print(f"  {len(part['export'])} years of partners, top {TOP_N} each; "
          f'partner coverage of imports {min(imp_cov):.1f}% to '
          f'{max(imp_cov):.1f}%')
    print(f'  clean through {last_clean}: exports '
          f"{totals['export'][last_clean]:,}bn, imports "
          f"{totals['import'][last_clean]:,}bn")
    for fy in sorted(flags):
        for f in flags[fy]:
            print(f'  FLAG {fy}: {f}')
    print(f'  -> {out} ({out.stat().st_size/1e3:.1f} KB)')


if __name__ == '__main__':
    main()
