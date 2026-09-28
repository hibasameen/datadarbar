"""One provenance record per chart, for the source panel, the rail and the CSV.

THE SAME FACT WAS WRITTEN DOWN IN THREE PLACES and they disagreed. Which
warehouse table feeds a chart lived in economy-rail.js; what the chart is made
of lived in hand-written prose under it; what its CSV contained lived in a
function somewhere else. So the structural-share charts told the rail they
came from gva_by_activity_annual - constant-price leaf activity data - when
they are Table 7b at current prices, and anyone who followed the link to
reproduce the chart got different numbers. The manufacturing-census and
input-output cards pointed at national_accounts, which does not contain
either of them at all: both are artefacts with no warehouse source, and the
card said nothing about that.

This is the one record. Publisher, publication, vintage, unit and grain are
read from the warehouse catalogue rather than retyped, so a chart cannot
claim a vintage the warehouse does not have. What is genuinely per-chart -
which rows it selects, what it computes, and the caveat that belongs at the
point of use - is written here, once, and the source panel, the rail's
dataset label and the CSV header are all generated from it.

The build fails if a card on the page has no record, if a record names a card
that does not exist, or if it names a warehouse table that is not in the
catalogue. A provenance record that can go stale silently is worth less than
no record at all.

Usage: build_provenance.py --app <dir> --out <js>
"""
import argparse, json, pathlib, re
from html.parser import HTMLParser


class Cards(HTMLParser):
    """Card ids and titles, read off the page rather than kept in a second list."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.stack, self.cards, self.cur, self.grab, self.buf = [], [], None, None, []

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        self.stack.append(tag)
        if tag == 'div' and 'card' in a.get('class', '').split() and a.get('id'):
            self.cur = {'id': a['id'], 'topic': a.get('data-topic'),
                        'depth': len(self.stack), 'title': ''}
            self.cards.append(self.cur)
        if self.cur and tag == 'h2':
            self.grab, self.buf = 'h2', []

    def handle_endtag(self, tag):
        if self.grab and tag == 'h2':
            if self.cur and not self.cur['title']:
                self.cur['title'] = re.sub(r'\s+', ' ', ''.join(self.buf)).strip()
            self.grab = None
        if self.stack:
            self.stack.pop()
        if self.cur and len(self.stack) < self.cur['depth']:
            self.cur = None

    def handle_data(self, d):
        if self.grab:
            self.buf.append(d)


# ─────────────────────────────────────────────────────────────────────────
# selection  - which rows of the table the chart takes
# calc       - what it does to them, where that is not just "plots them"
# note       - the caveat that belongs under this chart and not in a footer
# artefact   - drawn from a pre-warehouse extract; `table` is then the
#              nearest published equivalent, NOT the chart's own input, and
#              saying so is the whole point of the flag
# ─────────────────────────────────────────────────────────────────────────
CHARTS = {
 # ---- national accounts ----
 'sec-arc': dict(
    table='national_accounts', publication='Table 7b, sector shares at current prices',
    selection='agriculture, industry and services shares, 1999-2000 onwards',
    calc='before 1999-2000, backcast from official real sectoral growth rates '
         'anchored at 1999-2000'),
 'sec-mix': dict(
    table='national_accounts', publication='Table 7b, sector shares at current prices',
    selection='the three sector shares for the selected year'),
 'sec-macro': dict(
    table='national_accounts', publication='Table 5, GVA, GDP and GNI at constant 2015-16 prices',
    selection='GVA at basic prices, GDP at market prices, GNI, and income per person',
    calc='GDP = GVA + taxes - subsidies; GNI = GDP + net primary income. Both '
         'identities are checked to the rupee at build time. The exchange rate '
         'is the annual average, from gdp_indicators.',
    also=['gdp_indicators']),
 'sec-qtr': dict(
    table='gva_by_activity_quarterly', publication='Quarterly national accounts',
    selection='value added by sector, fiscal quarters (Q1 is July-September)',
    note='Unadjusted sequential quarters, not seasonally adjusted. The four '
         'quarters sum to annual GVA, not to GDP.'),
 'sec-contrib': dict(
    table='national_accounts', publication='Tables 5 and 6, constant-price levels and growth',
    selection='18 mutually exclusive leaf activities',
    calc='contribution = (level this year - level last year) / GVA last year. '
         'The leaves sum to GVA exactly, so the parts sum to headline growth '
         'by construction rather than by choice of weight.'),
 'sec-cybreak': dict(
    table='national_accounts', publication='Tables 5 and 6, constant-price levels and growth',
    selection='the same 18 leaf activities, for the selected year',
    calc='as sec-contrib'),
 'sec-ceras': dict(
    table='national_accounts', publication='Tables 5 and 6, constant-price levels and growth',
    selection='the same 18 leaf activities, averaged over each period',
    note='Periods: 2000s (2000-01 to 2007-08), energy crisis (2008-09 to '
         '2012-13), CPEC era (2013-14 to 2019-20), COVID and after (2020-21 '
         'to 2025-26).'),
 # ---- industry ----
 'sec-lsm': dict(
    table='lsm_qim', publication='Quantum Index of Manufacturing',
    selection='sector indices, 2015-16 = 100',
    note='Series before 2015-16 are the old 2005-06-base indices linked at the '
         'overlap year; coverage and weights differ, so the join is indicative.'),
 'sec-qimonth': dict(
    table='lsm_qim', publication='Quantum Index of Manufacturing',
    selection='the overall index, monthly'),
 'sec-weights': dict(
    table='lsm_sector_indices', publication='QIM sector weights and indices',
    selection='each sector’s weight in the index, with its latest growth',
    note='Growth is July-May 2025-26 against the same months a year earlier, '
         'because the current year has eleven months published, not twelve.'),
 # ---- the two with no warehouse source ----
 'sec-cmi': dict(
    table=None, artefact=True,
    publisher='Pakistan Bureau of Statistics',
    publication='Census of Manufacturing Industries, 2005-06 and 2015-16',
    origin='A hand-built extract from the two published census reports. There '
           'is no warehouse table behind this chart: PBS has not released the '
           'CMI as data, so it cannot be queried or re-derived here.',
    selection='employment and establishments by industry division, both censuses',
    calc='shown as shares of all manufacturing, so the two censuses stay '
         'comparable despite the large jump in coverage'),
 'sec-io': dict(
    table=None, artefact=True,
    publisher='Pakistan Bureau of Statistics',
    publication='Supply and Use / Input-Output table, 2015-16',
    origin='A hand-built extract from the only Supply-Use table PBS has '
           'published. There is no warehouse table behind this chart, and no '
           'second year to compare it with.',
    selection='inter-industry flows, diagonal removed',
    note='A 2015-16 snapshot. It describes the structure of that year, not of '
         'the economy today.'),
 # ---- budget ----
 'sec-budget': dict(
    table='budget_lines', publication='Budget in Brief',
    selection='own-year Budget Estimates only (is_own_year_be), FY2009-10 onwards',
    calc='line items summed to their printed category. Real values use the GDP '
         'deflator; where the deflator is not yet published it is extrapolated '
         'and the point is flagged.',
    note='These are budget estimates, not outturn. Receipts are gross, before '
         'the provincial share; expenditure excludes development spending. The '
         'two sides cannot be subtracted to infer a federal deficit.'),
 # ---- money and prices (all SBP) ----
 'sec-usd': dict(table='sbp_observations', publication='Bank floating average exchange rates',
    selection='PKR per US$, monthly from August 1947',
    note='Rates before 1982 are the managed peg, so the flat stretches are '
         'policy, not market calm.'),
 'sec-reer': dict(table='sbp_observations', publication='Nominal and real effective exchange rate indices, base 2010',
    selection='NEER and REER, monthly from July 2001',
    note='An index against its own 2010 base, so a reading above or below '
         '100 measures movement since 2010, not distance from a fair value. '
         'Nothing here estimates an equilibrium rate, and the chart is not '
         'titled as though it did. SBP publishes a methodology note making '
         'the same point; neither of its published URLs served the document '
         'when this was checked on 2026-09-29, so it is not linked.'),
 'sec-cpi': dict(table='sbp_observations', publication='Consumer price index, 2015-16 base',
    selection='headline CPI', calc='year-on-year percentage change'),
 'sec-food': dict(table='sbp_observations', publication='CPI components, 2015-16 base',
    selection='components by urban and rural', calc='year-on-year percentage change',
    note='Core is non-food non-energy (NFNE).'),
 'sec-policy': dict(table='sbp_observations', publication='Structure of interest rates',
    selection='the policy (target) rate',
    note='The policy rate proper begins in May 2015; before that the reverse '
         'repo ceiling is the closest continuous equivalent.'),
 'sec-kibor': dict(table='sbp_observations', publication='KIBOR offer rates',
    selection='offer rates, thinned to month-end for legibility',
    note='The full daily series is in the SQL console.'),
 'sec-spread': dict(table='sbp_observations', publication='Weighted average lending and deposit rates',
    selection='rates on outstanding loans and deposits, including zero-markup and interbank'),
 'sec-res': dict(table='sbp_observations', publication='Gold and foreign exchange reserves',
    selection='reserves, monthly from June 1948',
    calc='import cover uses a trailing 12-month average of goods imports'),
 'sec-bop': dict(table='sbp_observations', publication='Balance of payments summary, BPM6',
    selection='monthly components, summed to fiscal years (July-June)',
    calc='inflows minus outflows equal SBP’s published current account balance'),
 'sec-m': dict(table='sbp_observations', publication='Monetary aggregates, M3 monthly profile',
    selection='June 2006 onwards'),
 'sec-npl': dict(table='sbp_observations', publication='Segment-wise advances and non-performing loans',
    selection='NPLs as a share of gross advances',
    note='The series ends June 2025.'),
 # ---- external ----
 # NOT diaspora_remittances_monthly, which the rail used to name. That table
 # is 236 rows of a NATIONAL series with no country dimension at all - the
 # portal serves it keyed by country and 141 keys return the same numbers -
 # and its unit is unlabelled. It cannot produce a chart of where remittances
 # come from. This is SBP's country-wise series.
 'sec-remit': dict(table='sbp_observations',
    publication='Country-wise Workers’ Remittances (TS_GP_BOP_WR_M)',
    series_refresh='2026-08-10, covering to 2026-07-31',
    selection='monthly since July 1972, summed to fiscal years (July-June)',
    note='These series are hierarchical: U.A.E. already contains Dubai, Abu '
         'Dhabi and Sharjah, so they cannot all be added together.'),
 'sec-emig': dict(table='diaspora_destinations',
    publisher='Bureau of Emigration & Overseas Employment, via PBS',
    publication='Registered emigrants by destination',
    selection='emigrants by destination country',
    note='Registered emigrants only - people who left through the Bureau’s '
         'process, which is not everyone who emigrated.'),
 # ---- trade: the published totals ----
 'sec-totals': dict(table='trade_monthly_totals', publication='National Trade Database, monthly totals',
    selection='all months, summed to fiscal years',
    calc='this is the published grand total, not the sum of the product detail '
         'shown elsewhere on this page',
    note='Years whose quarterly shape breaks against the three before them, or '
         'which are incomplete, are drawn dashed and named under the chart.'),
 'sec-partners': dict(table='trade_by_country', publication='National Trade Database, trade by partner country',
    selection='fiscal years assembled from quarters, because the monthly rows '
              'have no June in any year',
    note='The partner tables do not always account for the whole published '
         'total. Before 2011-12 they name 85 to 91 per cent of imports.'),
 'sec-country': dict(table='trade_by_country', publication='National Trade Database, trade by partner country',
    selection='one country’s exports and imports, by fiscal year'),
 'sec-recon': dict(table='trade_reconciliation', publication='Totals reconciled against their parts',
    selection='each period’s published total beside the sum of its country and group rows'),
 # ---- trade: the 8-digit artefact ----
 'sec-drill': dict(table='trade_hs8', artefact=True,
    publication='External Trade Statistics, 8-digit by commodity and country',
    origin='Drawn from a pre-warehouse extract of the same PBS series, built '
           '2026-08-04. trade_hs8 is the nearest published equivalent but will '
           'not reproduce these numbers exactly.',
    selection='products grouped into HS chapters and sections, by direction and year',
    note='A sum of detail, not a total: for FY2021-22 to FY2023-24 it was '
         'parsed from PBS PDFs and covers 93 to 100 per cent of the grand '
         'total. Exports 2017-18 are UN Comtrade calendar-2018 at section '
         'level; imports 2018-19 and 2019-20 are Economic Survey totals only.'),
 'sec-products': dict(table='trade_hs8', artefact=True,
    publication='External Trade Statistics, 8-digit by commodity and country',
    origin='As sec-drill: a pre-warehouse extract built 2026-08-04.',
    selection='the largest single commodities for the selected direction and year',
    note='Read a product share off this chart and a national total off Trade '
         'over time; they are different series and do not agree.'),
 'sec-movers': dict(table='trade_hs8', artefact=True,
    publication='External Trade Statistics, 8-digit by commodity and country',
    origin='As sec-drill: a pre-warehouse extract built 2026-08-04.',
    selection='first and last years PBS published 8-digit data for each '
              'direction (exports 2015-16 to 2024-25)',
    calc='change between those two years at the chosen level of detail'),
}

# ─────────────────────────────────────────────────────────────────────────
# The State page. Its own index already carries a dataset per chart, so the
# table is read from there rather than retyped here and cannot drift from
# what the page draws; only the per-chart part is written below.
# ─────────────────────────────────────────────────────────────────────────
STATE = {
 'taxStack': dict(publication='Revenue collection by head',
    selection='all heads, every fiscal year',
    calc='stacked to the annual total, which is the total of the heads '
         'present that year',
    note='Nominal rupees, not inflation-adjusted. FY2023-24 is the latest '
         'year in this extract, not the latest FBR publishes - its own '
         'revenue-collection page lists FY2024-25. Wealth tax has no row '
         'from 2016 and is drawn as absent rather than as zero; no row in '
         'this dataset is reported as zero.'),
 'taxLines': dict(publication='Revenue collection by head',
    selection='each head as its own line', note='Nominal rupees.'),
 'taxShare': dict(publication='Revenue collection by head',
    selection='each head as a share of the year\u2019s total',
    calc='head divided by the total of the heads present that year'),
 'budgetPanel': dict(publication='Budget in Brief',
    selection='what the eighteen budget documents contain, and where each '
              'figure is drawn from',
    note='Own-year Budget Estimates. This panel describes the extraction, not '
         'a time series.'),
 'courtsPending': dict(publication='Judicial Statistics of Pakistan',
    selection='cases pending at year end, by court'),
 'courtsClearance': dict(publication='Judicial Statistics of Pakistan',
    selection='disposed against instituted, by court and year',
    calc='clearance = disposed / instituted',
    note='Clearance above 100 per cent means more cases were disposed of than '
         'filed that year. It does not on its own show the backlog fell, '
         'which depends on the opening stock.'),
 'courtsFlow': dict(publication='Judicial Statistics of Pakistan',
    selection='cases instituted and disposed, by court and year'),
 'courtsCategory': dict(publication='Judicial Statistics of Pakistan',
    selection='civil against criminal, by court and year'),
 'judgesComposition': dict(publication='Judicial strength returns',
    selection='sanctioned posts by rank: working, vacant, and neither'),
 'judgesTrend': dict(publication='Judicial strength returns',
    selection='working and vacant posts over time'),
 'crimeForce': dict(publication='Reported offences by police force',
    selection='cases reported, by force and year',
    note='The national figure reconciles to eight regional and force '
         'components plus Pakistan, not to nine independent forces. These are '
         'offences reported to police, not offences committed.'),
 'crimeOffence': dict(publication='Reported offences by police force',
    selection='cases reported by offence, across Pakistan',
    note='Reported offences, not offences committed.'),
 'crimeAjk': dict(publication='Reported offences by police force',
    selection='Azad Jammu & Kashmir, by district',
    note='AJK\u2019s own series and the PBS series differ for 2022 by 300 cases, '
         'because one is a fiscal year and the other calendar. Both are kept.'),
 'crimeKp': dict(publication='Reported offences by police force',
    selection='Khyber Pakhtunkhwa, seven serious offences'),
 'sindhGroup': dict(publication='Sindh Police crime returns',
    selection='cases by category group and year'),
 'sindhCategory': dict(publication='Sindh Police crime returns',
    selection='the twelve largest categories'),
 'sindhRange': dict(publication='Sindh Police crime returns',
    selection='cases by police range'),
 'firsDaily': dict(publication='Sindh Police first information reports',
    selection='first information reports by day',
    note='Not every day is present. The series covers the days the returns '
         'were published for, not a complete daily record.'),
 'plantsFuel': dict(publication='State of Industry Report, plants by fiscal year',
    selection='the plants in one chosen report year, grouped into fuel families',
    calc='capacity is what that report rated the plant at, not a maximum '
         'across years: 22 plants are revised between reports',
    note='One report year at a time. The union of all eight is 133 plants and '
         '45,405 MW, which is a figure for no year - NEPRA reported 118 '
         'plants and 41,440 MW for 2024-25. This is the reporting universe '
         'NEPRA published, not a register of every plant in the country.'),
 'plantsLargest': dict(publication='State of Industry Report, plants by fiscal year',
    selection='the eighteen largest plants in the chosen report year',
    note='Ranked on that year\u2019s reported capacity, not a maximum across years.'),
 'plantsReports': dict(publication='State of Industry Report, plants by fiscal year',
    selection='the plants each report actually names, by fiscal year',
    calc='presence is observed in each report rather than inferred from a '
         'first-to-last span, which would place one plant in a year its '
         'report does not contain',
    note='Plants listed without a capacity are counted but add no megawatts: '
         '11 of 108 in 2017-18, none from 2021-22, and where the two differ '
         'both are shown. A plant leaving the series has left the reports, '
         'which is not the same as having closed.'),
 # NEPRA publishes these as three separate series and the page draws one of
 # them. units_purchased_sold_losses is energy: units in, units billed, the
 # difference. technical_commercial_losses splits T&D losses (in units) from
 # commercial losses (in RUPEES billed against collected). And
 # billing_collection_recovery is recovery, also in rupees. A unit that was
 # billed and never paid for is counted as SOLD in the series drawn here.
 'discoLosses': dict(publication='State of Industry Report, units purchased, '
                                 'sold and lost by distribution company',
    selection='transmission and distribution loss rate, by company and year',
    calc='units never billed divided by units entering the system',
    note='A T&D energy loss: units that entered the system and never reached '
         'a billed meter. It is not electricity delivered and then not paid '
         'for, which NEPRA reports separately as commercial losses and '
         'recovery, in rupees. Denominators differ: for every DISCO it is '
         'units purchased, but K-Electric generates most of what it sells and '
         'its purchased column is grid imports alone, so its rate is NEPRA\u2019s '
         'published one on its own available energy.'),
 'discoLatest': dict(publication='State of Industry Report, units purchased, '
                                 'sold and lost by distribution company',
    selection='the latest year, companies ranked by loss rate',
    note='As the loss chart: energy never billed, not bills unpaid, and '
         'K-Electric\u2019s rate is on a different base.'),
 'discoUnits': dict(publication='State of Industry Report, units purchased, '
                                'sold and lost by distribution company',
    selection='units entering the system, units billed and units never '
              'billed, by company and year',
    calc='summed across companies, so a year with fewer companies reporting '
         'is a smaller year and is drawn faded',
    note='Excludes K-Electric, whose purchased column is grid imports only. '
         'Units in minus units billed and the printed loss figure differ by '
         'up to 1 GWh, which is rounding in the source.'),
 'eventsTimeline': dict(publication='Disaster alerts',
    selection='alerts by hazard and year',
    note='Alerts issued, not events that occurred. These records do not join '
         'to the monsoon impact records: they are separate universes.'),
 'impactsMetric': dict(publication='Monsoon impact returns',
    selection='the selected impact measure, by province',
    note='These records do not join to the alert records.'),
}

STATE_PUBLISHER = {
    'fbr_tax_collection': 'Federal Board of Revenue',
    'budget_lines': 'Finance Division',
    'ljcp_case_flows': 'Law and Justice Commission of Pakistan',
    'ljcp_judicial_strength': 'Law and Justice Commission of Pakistan',
    'police_crime_annual': 'Pakistan Bureau of Statistics, from provincial police returns',
    'sindh_crime_annual': 'Sindh Police',
    'sindh_fir_daily': 'Sindh Police',
    'nepra_plants': 'NEPRA',
    'nepra_plant_years': 'NEPRA',
    'nepra_disco_annual': 'NEPRA',
    'climate_events': 'National Disaster Management Authority',
    'climate_impacts': 'National Disaster Management Authority',
}

# The catalogue's `source` is one string mixing publisher and publication -
# "PBS National Accounts annual tables (2015-16 base)" - so splitting it on a
# comma yields a publication where a publisher belongs. Named here instead.
PUBLISHER = {
    'national_accounts': 'Pakistan Bureau of Statistics',
    'gva_by_activity_quarterly': 'Pakistan Bureau of Statistics',
    'gdp_indicators': 'Pakistan Bureau of Statistics',
    'lsm_qim': 'Pakistan Bureau of Statistics',
    'lsm_sector_indices': 'Pakistan Bureau of Statistics',
    'trade_monthly_totals': 'Pakistan Bureau of Statistics',
    'trade_by_country': 'Pakistan Bureau of Statistics',
    'trade_by_group': 'Pakistan Bureau of Statistics',
    'trade_reconciliation': 'Pakistan Bureau of Statistics',
    'trade_hs8': 'Pakistan Bureau of Statistics',
    'sbp_observations': 'State Bank of Pakistan',
    'budget_lines': 'Finance Division',
    'diaspora_destinations': 'Bureau of Emigration & Overseas Employment, via PBS',
    'diaspora_remittances_monthly': 'Pakistan Bureau of Statistics',
}


def slug(table):
    return table.replace('_', '-')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--app', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    app = pathlib.Path(a.app)

    cat = json.loads((app / 'data/warehouse/catalog.json').read_text())
    tables = {t['name']: t for t in cat['tables']}
    release = cat.get('generated', '')[:10]

    p = Cards()
    p.feed((app / 'finance.html').read_text())
    on_page = {c['id']: c for c in p.cards}

    missing = sorted(set(on_page) - set(CHARTS))
    assert not missing, f'cards on finance.html with no provenance record: {missing}'
    ghost = sorted(set(CHARTS) - set(on_page))
    assert not ghost, f'provenance records naming no card on finance.html: {ghost}'

    out = {}
    for cid, rec in CHARTS.items():
        t = rec.get('table')
        named = [x for x in ([t] if t else []) + rec.get('also', []) if x]
        for n in named:
            assert n in tables, f'{cid} names {n}, which is not in the catalogue'
        assert rec.get('publisher') or not t or t in PUBLISHER, (
            f'{cid} names {t}, which has no publisher')
        meta = tables.get(t, {})
        r = {
            'card': cid,
            'title': on_page[cid]['title'],
            'topic': on_page[cid]['topic'],
            'publisher': rec.get('publisher') or PUBLISHER.get(t)
                         or meta.get('source', '').split(',')[0],
            'publication': rec['publication'],
            'table': t,
            'catalogue': f'datasets/{slug(t)}/' if t else None,
            'rows': meta.get('rows'),
            'unit': meta.get('unit'),
            'vintage': release if t and not rec.get('artefact') else None,
            'series_refresh': rec.get('series_refresh'),
            'selection': rec.get('selection'),
            'calc': rec.get('calc'),
            'note': rec.get('note'),
            'artefact': bool(rec.get('artefact')),
            'origin': rec.get('origin'),
            'also': rec.get('also'),
        }
        out[cid] = {k: v for k, v in r.items() if v not in (None, [], '')}

    # ── the State page ────────────────────────────────────────────────
    # Its index is the list of charts, so a chart added there without a
    # record here fails the build the same way a finance card does.
    sd = (app / 'data/state_data.js').read_text()
    idx = json.loads(sd[sd.index('{'):sd.rindex(';')])['index']
    ds_of = {}
    for row in idx:
        ds_of.setdefault(row['chart'], row['ds'])

    miss = sorted(set(ds_of) - set(STATE))
    assert not miss, f'charts in the State index with no provenance record: {miss}'
    ghost = sorted(set(STATE) - set(ds_of))
    assert not ghost, f'State records naming no chart in the index: {ghost}'

    for chart, rec in STATE.items():
        t = ds_of[chart]
        assert t in tables, f'{chart} names {t}, which is not in the catalogue'
        assert t in STATE_PUBLISHER, f'{chart} names {t}, which has no publisher'
        meta = tables[t]
        label = next(r['label'] for r in idx if r['chart'] == chart)
        r = {
            'card': 'state:' + chart, 'title': label,
            'topic': next(r['topic'] for r in idx if r['chart'] == chart),
            'publisher': STATE_PUBLISHER[t],
            'publication': rec['publication'],
            'table': t, 'catalogue': f'datasets/{slug(t)}/',
            'rows': meta.get('rows'), 'unit': meta.get('unit'),
            'vintage': release,
            'selection': rec.get('selection'), 'calc': rec.get('calc'),
            'note': rec.get('note'), 'artefact': False,
        }
        out['state:' + chart] = {k: v for k, v in r.items()
                                 if v not in (None, [], '', False)}

    dest = pathlib.Path(a.out)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        '/* Data Darbar - one provenance record per chart. Generated by\n'
        '   etl/build_provenance.py; do not edit by hand. */\n'
        'window.DD_PROV=' + json.dumps(out, separators=(',', ':')) + ';\n')

    art = [c for c, r in out.items() if r.get('artefact')]
    no_tbl = [c for c, r in out.items() if not r.get('table')]
    used = sorted({r['table'] for r in out.values() if r.get('table')})
    n_state = len([c for c in out if c.startswith('state:')])
    print(f'  {len(out)} charts ({len(out)-n_state} economy, {n_state} state), '
          f'{len(used)} warehouse tables, release {release}')
    print(f'  artefacts (vintage is the extract, not the warehouse): '
          f'{", ".join(art)}')
    print(f'  no warehouse table at all: {", ".join(no_tbl)}')
    print(f'  -> {dest} ({dest.stat().st_size/1e3:.1f} KB)')


if __name__ == '__main__':
    main()
