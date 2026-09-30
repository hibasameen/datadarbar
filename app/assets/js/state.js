/*
   state.js — the State explorer.

   Every topic here opens on the chart that carries its story, and the other
   forms of the same data sit behind it as variants. The first build drew
   nearly everything the same way - several lines over five years, often on a
   log scale - and that form hid exactly what the deks said: Punjab is nearly
   all of the rise in reported crime, but as a flat line three hundred times
   above the others it looked like nothing; clearance above 100 does not mean
   the backlog fell, but on a 0-100 axis all five courts sat on one line.

   The forms used, and why:

     share of GDP     tax, budget. Thirty-three years of nominal rupees are
                      mostly inflation; the ratio is the number the reader
                      wants. GDP before 1999-00 is spliced onto the 2015-16
                      base and drawn dashed, and the coverage strip says so.
     small multiples  case flows. One panel per court with its own scale,
                      because one axis for Punjab (1.5m pending) flattens
                      Balochistan (19k) to the baseline.
     diverging bars   clearance around 100, and the backlog's change over
                      each year around zero. What matters is the sign.
     dumbbells        then-and-now by offence, district, category, range,
                      rank, company. Two years per row, the change written
                      beside it, sorted by the latest value.
     stacked bars     where the parts genuinely add: Sindh's category groups,
                      capacity by fuel, units billed and unbilled.
     per head         reported cases per 100,000 people, where the 2023
                      census gives a denominator. Three forces have none and
                      are named rather than guessed at.

   What each theme does NOT cover is still said on the theme rather than left
   for the reader to infer, because every one of these series is patchy in a
   way that changes what it can be read to mean:

     judges    Balochistan only, and its own working/vacant columns do not add
               to sanctioned - a post filled by an ex-cadre officer is neither,
               so the remainder is drawn grey and named rather than hidden.
     crime     eight forces on PBS's definition. AJK appears twice in the
               source and the two figures differ; the chart uses PBS and marks
               the force with a dagger. Only AJK publishes a district total:
               KP publishes seven named offences, which come to 5,971 cases in
               2024 against a provincial 216,872, so they are never summed
               into anything called a total.
     events    GDACS alerts carry no casualty figures at all. They are a
               timeline of severity grades, not of losses.
     impacts   NDMA monsoon reports, one season, cumulative. They are not the
               impacts of the events above and do not join to them.
*/(function () {
  'use strict';

  var D = window.DD_STATE;
  if (!D || !window.d3) return;

  function asObjects(block) {
    return block.rows.map(function (r) {
      var o = {};
      block.cols.forEach(function (c, i) { o[c] = r[i]; });
      return o;
    });
  }
  var tax = D.tax.rows.map(function (r) {
    return { fy: r[0], fyEnd: r[1], type: r[2], head: r[3], mn: r[4] };
  });
  var courts = asObjects(D.courts), judges = asObjects(D.judges);
  var courtDistricts = asObjects(D.courtDistricts);
  var judgesProv = asObjects(D.judgesProvince);
  var crime = asObjects(D.crime), offences = asObjects(D.offences);
  var crimeDistricts = asObjects(D.crimeDistricts);
  var sindhCrime = asObjects(D.sindhCrime), firs = asObjects(D.firs);
  var plants = asObjects(D.plants), events = asObjects(D.events);
  var discos = asObjects(D.discos);
  var recovery = asObjects(D.recovery);
  var impacts = asObjects(D.impacts);

  /* Derived denominators, carried with the payload. GDP at current market
     prices by fiscal-year end, with a flag for the years spliced onto the
     2015-16 base; Census 2023 population by province. Both are named as
     derived on every chart that divides by them. */
  var GDP = {}, GDP_SPLICED = {}, POP = {};
  (D.derived && D.derived.gdp ? asObjects(D.derived.gdp) : []).forEach(function (g) {
    GDP[g.fy_end] = g.gdp_mp_pkr_mn;
    if (g.spliced) GDP_SPLICED[g.fy_end] = true;
  });
  (D.derived && D.derived.population ? asObjects(D.derived.population) : [])
    .forEach(function (p) { POP[p.region] = p.population; });
  var BUDGET = window.DD_BUDGET || null;

  var TAX_HEAD = { income: 'Income tax', sale: 'Sales tax', custom: 'Customs',
                   excise: 'Federal excise', wppf: 'WPPF', cvt: 'CVT',
                   wealth: 'Wealth tax' };
  function headLabel(h) { return TAX_HEAD[h] || h; }

  var TOPICS = {
    tax: {
      theme: 'Public money',
      title: 'What the state collects',
      dek: 'Federal tax collection by head, as FBR reports it, against the '
         + 'size of the economy. FBR collected 7.1 per cent of GDP in 1991-92 '
         + 'and 8.8 per cent in 2023-24: thirty-three years, nine governments, '
         + 'and the ratio moved less than two points. What changed is the mix '
         + '— customs duty was the largest head in 1992 and is a tenth of '
         + 'the total now; income and sales tax are four-fifths.',
      note: 'Direct and indirect tax nest inside their heads, so the chart '
          + 'stacks heads within a type rather than adding both. GDP at '
          + 'current market prices is PBS’s 2015-16 base from 1999-00; '
          + 'earlier years are spliced onto it by the overlap ratio and drawn '
          + 'dashed. The nominal series is kept as a variant; most of its rise '
          + 'after 2021 is prices, not base.',
    },
    budget: {
      theme: 'Public money',
      title: 'The federal budget',
      dek: 'Eighteen years of budget documents, 2009-10 to 2026-27: what the '
         + 'federal government expected to collect and what it set aside for '
         + 'current spending, in the year the budget was presented. Debt '
         + 'servicing alone is now close to half of all current spending.',
      note: 'Budget estimates, not outturn. Receipts run from the Explanatory '
          + 'Memorandum on Federal Receipts, expenditure from Budget in Brief; '
          + 'development spending is budgeted separately and is not in it. '
          + 'Item labels drift between documents, so every group here rests '
          + 'on a crosswalk between them — a reading of the documents, not '
          + 'a fact about them. The share of GDP uses PBS’s GDP for the '
          + 'budget year; 2026-27 has no GDP yet and is left off that view.',
    },
    courts: {
      theme: 'Justice',
      title: 'Case flows and pendency',
      dek: 'What was pending, what came in and what was decided, across four '
         + 'provinces and Islamabad and district by district, 2020 to 2024. '
         + 'Lahore had 2,700 cases pending for every 100,000 people at the end '
         + 'of 2024 and 1,291 for each judge in post. Punjab carries 1.5 million '
         + 'of the 1.96 million cases pending; only Khyber Pakhtunkhwa and '
         + 'Islamabad ended 2024 with fewer than they began it. A clearance rate above 100 per '
         + 'cent means a court decided more cases than it received that year. '
         + 'It does not mean the backlog fell: in seven of these rows it rose '
         + 'anyway — Punjab cleared 100.5 per cent in 2022 and ended the '
         + 'year with 2,611 more cases pending than it started with.',
      note: 'Categories nest: ‘all’ contains civil and criminal, so the three '
          + 'are not added together. Where opening plus instituted minus '
          + 'disposed does not equal the closing figure, transfers between '
          + 'courts usually explain it; the table carries the residual, and '
          + 'the backlog chart uses the closing and opening stocks as printed.',
    },
    judges: {
      theme: 'Justice',
      title: 'Judges and vacancies',
      dek: 'Sanctioned posts against the judges actually sitting in them. '
         + 'Punjab had 1,565 judges for 2,364 posts in 2024 — a third of its '
         + 'district bench empty, every year since 2020 — while Sindh filled '
         + 'nine in ten. In Balochistan, by rank, the shortfall sits almost '
         + 'entirely in the district and sessions ranks.',
      note: 'The province chart covers every province that printed a '
          + 'consolidated strength; the charts by rank are Balochistan only, '
          + 'for 2023 and 2024, because only its rank tables were extracted. '
          + 'Working and vacant do not '
          + 'add to sanctioned: the source counts as working only judges '
          + 'sitting in their own cadre, so a post filled by an officer on '
          + 'deputation is neither, and the grey remainder is those posts.',
    },
    crime: {
      theme: 'Crime & policing',
      title: 'Reported offences',
      dek: 'Cases reported to police by force and year, 2019 to 2024. The '
         + 'national figure nearly doubles over the six years, from 786,000 to '
         + '1.51 million, and almost all of the rise is Punjab — which also '
         + 'reports the most cases per head of population. Islamabad reports '
         + 'more per head than any province.',
      note: 'Offences reported, not crimes committed — the two move for '
          + 'different reasons, and reporting rises with confidence in the '
          + 'police as much as with crime. Azad Jammu & Kashmir appears twice '
          + 'in the source, once in PBS’s compilation and once in its own '
          + 'yearbooks, and the two disagree by a few dozen cases a year; the '
          + 'chart uses PBS, which is the only series covering all forces on '
          + 'one definition. Rates per 100,000 divide by the 2023 census, '
          + 'which does not cover Azad Jammu & Kashmir or Gilgit-Baltistan, '
          + 'and Railways polices no territory; those three are named without '
          + 'a rate. The eleven named offences are a partial breakdown and do '
          + 'not add to a force’s total.',
    },
    sindhCrime: {
      theme: 'Crime & policing',
      title: 'Sindh’s own crime tables',
      dek: 'Sindh police publish on their own schema — 44 offence categories '
         + 'in six groups, by police range, 2019 to 2025 — which is a '
         + 'longer and finer series than the national compilation carries. '
         + 'Crime against property is the largest group and the one that '
         + 'grew; Karachi Range is more than half of everything reported.',
      note: 'These figures do not line up with the national compilation and '
          + 'should not be spliced onto it: the categories are Sindh’s own, and '
          + 'the geography is police ranges, which are not districts and do '
          + 'not nest inside the census frame. Ranges and categories are '
          + 'grouped on the keys the warehouse carries rather than on the '
          + 'printed names, because the reports change capitalisation partway '
          + 'through. Prior-year comparison columns ARE used: they carry 2019 '
          + 'from the 2020 report, and no cell appears both as a comparison '
          + 'column and as its own year, so nothing is counted twice. Within '
          + 'Sindh the groups do partition the categories, so those add up.',
    },
    firs: {
      theme: 'Crime & policing',
      title: 'First information reports',
      dek: 'The daily FIR bulletins Sindh police published in the autumn of '
         + '2025 — 11 days observed out of the 54 between 15 September and '
         + '7 November, between 260 and 380 reports a day.',
      note: 'These are the bulletins that exist, not a daily series. Forty-'
          + 'three of the 54 calendar days have no observation at all, '
          + 'including the whole of 25 September to 5 November, so the days '
          + 'are drawn as the separate observations they are and never joined. '
          + 'The source’s own quality report calls the historical series '
          + 'incomplete. The table also carries a year-to-date column, which '
          + 'is a running total: adding those up would count the same reports '
          + 'once for every day left in the year, and it is not consistent '
          + 'either — 89,289 on 17 September falls to 88,944 on 19 September, '
          + 'which is the source disagreeing with itself rather than a '
          + 'negative number of reports. Only the daily figure is drawn.',
    },
    discos: {
      theme: 'Energy',
      title: 'Electricity lost and bills unpaid',
      dek: 'The share of the electricity entering each distribution '
         + 'company’s system that never reaches a billed meter, 2006-07 to '
         + '2024-25. The spread is the story: 8.4 per cent in Islamabad and '
         + '38.8, 38.4 and 39.0 per cent in Peshawar, Quetta and Sukkur '
         + '— two units in every five — and in eighteen years the worst '
         + 'companies have barely moved. Of what was billed in 2024-25, Quetta '
         + 'collected 39 per cent and Sukkur 70.',
      note: 'This is a T&D loss: units that entered the system and were '
          + 'never billed, whether they leaked away in the wires or were '
          + 'taken off them. It is NOT electricity delivered and then not '
          + 'paid for. NEPRA reports that separately, in rupees billed against '
          + 'rupees collected, and it is drawn separately here as bills paid '
          + '— in the loss charts a unit that was billed and never paid for is '
          + 'counted as sold, not lost. Rates are computed from the '
          + 'units bought and units sold printed in the same row rather than '
          + 'from the percentage column beside them, which contradicts its '
          + 'own row for PESCO in the 2011 edition. The 2024 edition prints '
          + 'the whole system’s figures under PESCO’s name for 2019-20 '
          + '— 114,360 GWh against its own 14,750 — and that row is '
          + 'rejected. Company names in the source run to five spellings of '
          + 'K-Electric and one that is not a name at all; those are '
          + 'crosswalked, and the totals rows are kept apart from the companies.',
    },
    plants: {
      theme: 'Energy',
      title: 'Power plants and capacity',
      dek: 'The plants in NEPRA’s reports, what they burn, how much they '
         + 'were rated at and how much of that they ran, 2017-18 to 2024-25. '
         + 'The plants ran at 43 per cent of their capacity in 2017-18 and 34 '
         + 'per cent in 2024-25. Rated capacity in the reports grew '
         + 'from 32,400 MW in 2017-18 (with eleven plants listed unrated) to 41,400 '
         + 'MW in 2024-25, and nearly all of the addition is coal and nuclear; '
         + 'oil-fired capacity has been leaving the reports.',
      note: 'Every figure here belongs to one report year. Adding the years '
          + 'together would count the same plant up to eight times, and 22 '
          + 'plants have their capacity revised between reports — Tarbela '
          + 'is 3,948 MW in 2017-18 and 3,478 MW after it. This is the '
          + 'reporting universe NEPRA published, not a register of every '
          + 'plant in the country, and a plant leaving the series has left '
          + 'the reports rather than necessarily closed. Installed capacity '
          + 'is nameplate — what a plant could produce, not what it does, so '
          + 'the fuel mix here is not the generation mix. NEPRA spells the '
          + 'same fuel more than one way across its tables (‘Coal’ and '
          + '‘THERMAL- COAL’ are both coal), so the bars group its '
          + 'technology and fuel strings into families; each plant keeps the '
          + 'original two, visible on hover.',
    },
    events: {
      theme: 'Public services',
      title: 'Disaster alerts',
      dek: 'Every flood, drought and cyclone alert GDACS has issued for '
         + 'Pakistan since 2001, graded orange or red: 31 alerts, 25 of them '
         + 'floods, and the only red ones the floods of 2010, 2022 and the '
         + 'cyclone of 2021.',
      note: 'These are alerts, not losses. GDACS grades how severe an event '
          + 'looks as it happens and carries no casualty figures at all — '
          + 'deaths, people affected and damage are empty in all 31 records, '
          + 'which is an absence of measurement rather than an absence of harm. '
          + 'For impact figures see the monsoon reports, which cover one '
          + 'season and do not correspond to these events. An event’s '
          + 'footprint may cross borders; it is listed here because Pakistan '
          + 'was affected.',
    },
    impacts: {
      theme: 'Public services',
      title: 'Monsoon impacts',
      dek: 'What the 2026 monsoon did, as NDMA’s situation reports counted '
         + 'it: 181 deaths, 515 injured and 1,729 houses damaged by 5 '
         + 'September. Khyber Pakhtunkhwa and Punjab carry three-quarters of '
         + 'the deaths.',
      note: 'Cumulative for the season to the date of the latest report, not a '
          + 'daily count, so these figures cannot be added to earlier ones or '
          + 'to each other. The province rows add to the national row for every '
          + 'measure except damaged roads, where NDMA rounds 34.56 km to 35. A '
          + 'missing province is one no report covered rather than a province '
          + 'with nothing to report. This is one season — the only one '
          + 'extracted — and is not a series.',
    },
  };

  /* State is an index of 38 charts over 11 topics. The subject tree and the
     three dropdowns are two ways into the same row; whichever the reader
     uses, `current` is that row and `CHART[row.chart]` draws it. */
  var state = { ds: 'fbr_tax_collection', topic: 'tax', ind: 'gdp' };
  var current = D.index[0];
  var rail;

  function $(id) { return document.getElementById(id); }

  function fmtTn(v) { return (v / 1e6).toFixed(1); }
  function pct(v, d) { return v.toFixed(d == null ? 1 : d) + '%'; }
  function signed(v, d) { return (v > 0 ? '+' : v < 0 ? '−' : '') + Math.abs(v).toFixed(d == null ? 0 : d); }
  function fyOf(end) { return (end - 1) + '-' + String(end).slice(2); }

  var CHART = {
    taxGdp: function () { renderTax('gdp'); },
    taxShare: function () { renderTax('share'); },
    taxStack: function () { renderTax('stack'); },
    taxShift: renderTaxShift,
    budgetTree: function () { renderBudget('tree'); },
    budgetGdp: function () { renderBudget('gdp'); },
    budgetTrend: function () { renderBudget('trend'); },
    courtsPending: renderCourtsPending,
    courtsDistricts: renderCourtsDistricts,
    courtsNet: renderCourtsNet,
    courtsClearance: renderCourtsClearance,
    courtsCategory: renderCourtsCategory,
    judgesComposition: renderJudges,
    judgesVacancy: renderJudgesVacancy,
    judgesProvince: renderJudgesProvince,
    crimeRate: renderCrimeRate,
    crimeIndexed: renderCrimeIndexed,
    crimeForce: renderCrimeForce,
    crimeOffence: renderCrimeOffence,
    crimeAjk: renderCrimeAjk,
    crimeKp: renderCrimeKp,
    sindhGroup: renderSindhGroup,
    sindhCategory: function () { renderSindhChange('category'); },
    sindhRange: function () { renderSindhChange('range'); },
    firsDaily: renderFirs,
    plantsUsed: renderPlantsUsed,
    plantsUseTrend: renderPlantsUseTrend,
    plantsMix: renderPlantsMix,
    plantsFuel: function () { renderPlants('fuel'); },
    plantsLargest: function () { renderPlants('largest'); },
    plantsReports: function () { renderPlants('reports'); },
    discoRecovery: renderDiscoRecovery,
    discoChange: renderDiscoChange,
    discoLosses: renderDiscoLosses,
    discoLatest: renderDiscoLatest,
    discoUnits: renderDiscoUnits,
    eventsTimeline: renderEvents,
    eventsCount: renderEventsCount,
    impactsPanel: renderImpactsPanel,
    impactsMetric: renderImpacts,
  };

  function render(row) {
    current = row || current;
    var t = TOPICS[current.topic] || {};
    $('paneTheme').textContent = current.theme;
    $('paneTitle').textContent = t.title || current.topicLabel;
    $('paneDek').textContent = t.dek || '';
    $('cardNote').textContent = t.note || '';
    renderSrc(current.chart);
    clearCtl();
    $('chart').className = 'chart';
    (CHART[current.chart] || renderTaxGdpFallback)();
  }
  function renderTaxGdpFallback() { renderTax('gdp'); }

  /* ── what the state collects ─────────────────────────────────────────── */
  function taxTable() {
    var years = Array.from(new Set(tax.map(function (d) { return d.fyEnd; }))).sort(d3.ascending);
    var heads = Array.from(new Set(tax.map(function (d) { return d.head; })));
    var byYear = d3.rollup(tax, function (v) {
      var o = {};
      v.forEach(function (d) { o[d.head] = (o[d.head] || 0) + d.mn; });
      return o;
    }, function (d) { return d.fyEnd; });
    /* ABSENT IS NOT ZERO. `|| 0` turned a head FBR stopped reporting into a
       head that collected nothing: wealth tax has no row from 2016 and was
       drawn as a flat zero line for nine years. Not a single row in this
       dataset is reported as zero, so every zero on the chart was
       manufactured by that expression. The value stays null where there is
       no row; a stack still needs a number and zero is right for the TOTAL
       - an abolished tax contributes nothing to it - so the stacking value
       is kept separately from the value read out. */
    var data = years.map(function (y) {
      var o = { year: y }, got = byYear.get(y) || {};
      heads.forEach(function (h) {
        o[h] = got[h] == null ? null : got[h];
        o['_' + h] = o[h] == null ? 0 : o[h];
      });
      o.total = d3.sum(heads, function (h) { return o['_' + h]; });
      o.gdp = GDP[y] || null;
      return o;
    });
    var absent = heads.filter(function (h) {
      return data.some(function (d) { return d[h] == null; });
    });
    return { years: years, heads: heads, data: data, absent: absent };
  }

  function renderTax(mode) {
    state.mode = mode;
    var T = taxTable(), years = T.years, heads = T.heads, data = T.data;
    var first = data[0], last = data[data.length - 1];
    var bits = [tax[0].fy + ' to ' + tax[tax.length - 1].fy
                + ' (latest in this dataset)',
                years.length + ' fiscal years', heads.length + ' heads'];
    if (mode === 'gdp') {
      var spliced = years.filter(function (y) { return GDP_SPLICED[y]; });
      bits.push(pct(100 * first.total / first.gdp) + ' of GDP in ' + first.year
                + ', ' + pct(100 * last.total / last.gdp) + ' in ' + last.year);
      bits.push('<span class="cov-derived">derived: divided by GDP at current '
                + 'market prices' + (spliced.length
                  ? '; ' + fyOf(spliced[0]) + ' to ' + fyOf(spliced[spliced.length - 1])
                    + ' spliced onto the 2015-16 base' : '') + '</span>');
    } else {
      bits.push('Nominal rupees, not inflation-adjusted');
    }
    cover(bits);
    if (T.absent.length) {
      var gapYears = {};
      T.absent.forEach(function (h) {
        var g = data.filter(function (d) { return d[h] == null; })
          .map(function (d) { return d.year; });
        gapYears[h] = g[0] + (g.length > 1 ? '–' + g[g.length - 1] : '');
      });
      $('coverage').innerHTML += '<span class="cov-gap">not reported: '
        + T.absent.map(function (h) { return headLabel(h) + ' ' + gapYears[h]; }).join(', ')
        + '</span>';
    }
    drawTax(data, heads, years, mode);
  }

  function drawTax(data, heads, years, mode) {
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 14, right: 175, bottom: 26, left: 52 }, W);
    host.say(mode === 'share'
      ? 'Each head as a share of the year’s total collection.'
      : mode === 'gdp'
        ? 'Each head as a share of GDP at current market prices, stacked to '
          + 'FBR’s total. Dashed years divide by a GDP spliced onto the '
          + '2015-16 base.'
        : 'Trillions of rupees collected, as FBR reports them.', '');

    var x = d3.scaleLinear().domain(d3.extent(years)).range([m.left, W - m.right]);
    var colour = d3.scaleOrdinal().domain(heads)
      .range(['#0c3a1e', '#1e6b3e', '#4d8a62', '#7aa88c', '#b5860b', '#d4a017', '#9dbfa9']);
    var val = function (d, h) {
      return mode === 'gdp' ? (d.gdp ? 100 * d['_' + h] / d.gdp : 0) : d['_' + h];
    };
    var rows = data.map(function (d) {
      var o = { year: d.year, gdp: d.gdp, total: 0 };
      heads.forEach(function (h) { o[h] = val(d, h); o.total += o[h]; o['raw' + h] = d[h]; });
      return o;
    });
    var stack = d3.stack().keys(heads)
      .offset(mode === 'share' ? d3.stackOffsetExpand : d3.stackOffsetNone);
    var series = stack(rows);
    var y = d3.scaleLinear()
      .domain([0, mode === 'share' ? 1 : d3.max(rows, function (d) { return d.total; })])
      .nice().range([H - m.bottom, m.top]);
    var area = d3.area().x(function (d) { return x(d.data.year); })
      .y0(function (d) { return y(d[0]); }).y1(function (d) { return y(d[1]); });
    svg.selectAll('path.area').data(series).join('path')
      .attr('class', 'area')
      .attr('fill', function (d) { return colour(d.key); })
      .attr('opacity', .92)
      .attr('d', area)
      .append('title').text(function (d) { return headLabel(d.key); });

    /* The spliced stretch: a hatched veil over the years whose denominator
       is a reading rather than a published figure. */
    if (mode === 'gdp') {
      var sp = years.filter(function (yv) { return GDP_SPLICED[yv]; });
      if (sp.length) {
        var pat = svg.append('defs').append('pattern')
          .attr('id', 'splice-hatch').attr('patternUnits', 'userSpaceOnUse')
          .attr('width', 6).attr('height', 6).attr('patternTransform', 'rotate(45)');
        pat.append('line').attr('x1', 0).attr('y1', 0).attr('x2', 0).attr('y2', 6)
          .attr('stroke', '#faf7ef').attr('stroke-width', 1.6).attr('opacity', .8);
        svg.append('rect').attr('x', x(sp[0])).attr('y', m.top)
          .attr('width', x(sp[sp.length - 1] + 1) - x(sp[0]))
          .attr('height', H - m.bottom - m.top)
          .attr('fill', 'url(#splice-hatch)').attr('pointer-events', 'none');
        svg.append('line').attr('x1', x(sp[sp.length - 1] + 1)).attr('x2', x(sp[sp.length - 1] + 1))
          .attr('y1', m.top).attr('y2', H - m.bottom)
          .attr('stroke', muted()).attr('stroke-dasharray', '3 3');
        svg.append('text').attr('x', x(sp[sp.length - 1] + 1) - 4).attr('y', m.top + 11)
          .attr('text-anchor', 'end').attr('font-size', 10).attr('fill', muted())
          .text('spliced GDP base');
      }
      // The total, drawn as a line the eye can follow across the stack.
      svg.append('path').datum(rows).attr('fill', 'none')
        .attr('stroke', '#0c3a1e').attr('stroke-width', 1.2).attr('opacity', .7)
        .attr('d', d3.line().x(function (d) { return x(d.year); }).y(function (d) { return y(d.total); }));
    }
    // Hover: the year's figures.
    hoverYears(svg, rows, x, m, W, H, function (d) {
      var lines = [fyOf(d.year) + (mode === 'gdp' ? ' · ' + pct(d.total) + ' of GDP'
                   : mode === 'stack' ? ' · ' + fmtTn(d.total) + ' tn' : '')];
      heads.slice().reverse().forEach(function (h) {
        if (d['raw' + h] == null) return;
        lines.push(headLabel(h) + ': ' + (mode === 'gdp' ? pct(d[h], 2)
          : mode === 'share' ? pct(100 * d[h] / d.total) : fmtTn(d[h]) + ' tn'));
      });
      return lines.join('\n');
    });

    var ink_ = muted();
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x).ticks(8).tickFormat(d3.format('d')))
      .attr('color', ink_).attr('font-size', 11);
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(6).tickFormat(
        mode === 'share' ? d3.format('.0%') : mode === 'gdp'
          ? function (v) { return v + '%'; } : function (v) { return fmtTn(v); }))
      .attr('color', ink_).attr('font-size', 11);
    svg.append('text').attr('x', m.left).attr('y', m.top - 3)
      .attr('font-size', 10.5).attr('fill', ink_)
      .text(mode === 'share' ? 'share of collection' : mode === 'gdp'
            ? 'per cent of GDP' : 'trillion rupees');

    var last = rows[rows.length - 1];
    var order = heads.slice().sort(function (a, b) {
      return (last['raw' + b] == null ? -1 : last[b]) - (last['raw' + a] == null ? -1 : last[a]);
    });
    var lg = svg.append('g').attr('transform', 'translate(' + (W - m.right + 10) + ',' + (m.top + 4) + ')');
    order.forEach(function (h, i) {
      var absentNow = last['raw' + h] == null;
      var g = lg.append('g').attr('transform', 'translate(0,' + i * 17 + ')');
      g.append('rect').attr('width', 10).attr('height', 10).attr('rx', 2)
        .attr('fill', absentNow ? 'none' : colour(h))
        .attr('stroke', absentNow ? colour(h) : 'none').attr('stroke-width', 1.4);
      g.append('text').attr('x', 15).attr('y', 9).attr('font-size', 11.5)
        .attr('fill', ink()).attr('opacity', absentNow ? .6 : 1)
        /* Not "0.00": FBR does not report this head any more, and a zero
           here would be a number nobody published. */
        .text(headLabel(h) + '  ' + (absentNow ? 'not reported'
          : mode === 'gdp' ? pct(last[h]) : mode === 'share'
            ? pct(100 * last[h] / last.total, 0) : fmtTn(last[h])));
    });
  }

  /* Then and now: each head's share of the total in the first and last
     year, one row per head. The dumbbell is the whole story of the mix in
     one glance, which thirty-three stacked years cannot be. */
  function renderTaxShift() {
    var T = taxTable(), data = T.data;
    var a = data[0], b = data[data.length - 1];
    cover([fyOf(a.year) + ' against ' + fyOf(b.year), T.heads.length + ' heads',
           'share of FBR’s total collection']);
    var rows = T.heads.map(function (h) {
      return { name: headLabel(h),
               a: a[h] == null ? null : 100 * a[h] / a.total,
               b: b[h] == null ? null : 100 * b[h] / b.total,
               sub: (a[h] == null ? 'not reported' : fmtTn(a[h]) + ' tn') + ' → '
                  + (b[h] == null ? 'not reported' : fmtTn(b[h]) + ' tn') };
    });
    dumbbell(rows, {
      aLabel: fyOf(a.year), bLabel: fyOf(b.year),
      fmt: function (v) { return pct(v, 0); },
      delta: function (r) { return signed(r.b - r.a, 0) + ' pts'; },
      lede: 'Each head’s share of the year’s collection, first year against '
          + 'last. Customs duty was the largest head in ' + fyOf(a.year)
          + '; income and sales tax are four-fifths of the total now.',
      foot: 'A head not reported in one of the two years is drawn at one end only.',
    });
  }

  /* ── the federal budget ──────────────────────────────────────────────── */
  var bSide = 'expenditure', bYear = null;

  function budgetGroups(side, year) {
    return (BUDGET && BUDGET[side] && BUDGET[side][year]) || [];
  }
  function budgetYears() { return BUDGET ? BUDGET.years.slice() : []; }
  function fyEndOf(fy) { return +fy.slice(0, 4) + 1; }

  function renderBudget(mode) {
    state.mode = mode;
    if (!BUDGET) {
      $('chart').className = 'chart panel';
      $('chart').innerHTML = '<div class="notice"><p>The budget payload '
        + '(data/budget_data.js) is not on this page.</p></div>';
      return;
    }
    var years = budgetYears();
    if (!bYear) bYear = years[years.length - 1];
    var b = D.budget;
    var groups = budgetGroups(bSide, bYear);
    var total = d3.sum(groups, function (g) { return d3.sum(g.children, function (c) { return c.bn; }); });
    var bits = [b.first + ' to ' + b.last, b.docs + ' budget documents',
                bSide === 'expenditure' ? 'current expenditure by function' : 'receipts by source'];
    if (mode === 'tree') bits.push(bYear + ': Rs ' + fmtBn(total) + ' budgeted');
    if (mode === 'gdp') bits.push('<span class="cov-derived">derived: divided by GDP at '
                                  + 'current market prices for the budget year</span>');
    cover(bits);
    sideControl(mode);
    if (mode === 'tree') return drawBudgetTree(groups, total);
    drawBudgetTrend(mode === 'gdp');
  }

  function sideControl(mode) {
    var bar = ctlRow();
    seg(bar, 'Side', [['expenditure', 'Current expenditure'], ['receipts', 'Receipts']],
        bSide, function (v) { bSide = v; render(); });
    if (mode === 'tree') {
      var yrs = budgetYears();
      var lab = document.createElement('span');
      lab.className = 'ctl-lbl'; lab.textContent = 'Budget year';
      bar.appendChild(lab);
      var sel = document.createElement('select');
      sel.className = 'seg';
      yrs.forEach(function (y) {
        var o = document.createElement('option'); o.value = y; o.textContent = y;
        sel.appendChild(o);
      });
      sel.value = bYear;
      sel.onchange = function () { bYear = sel.value; render(); };
      bar.appendChild(sel);
    }
  }

  /* Debt servicing is the one group drawn in its own colour, so the budget's
     headline - interest is close to half of current spending - is the first
     thing the eye finds. The other groups keep the site palette between
     them, so their colours do not shift when debt is taken out. */
  var DEBT = /^Debt servicing/;
  function budgetColour(labels) {
    // The palette's three reds - rust, sienna and brick, slots 3, 6 and 9 -
    // are left out for the other groups, or Social Protection came out the
    // same colour as debt. A placeholder holds each slot so nothing lands there.
    var rest = labels.filter(function (l) { return !DEBT.test(l); }), dom = [];
    rest.forEach(function (l) {
      while ([3, 6, 9].indexOf(dom.length) >= 0) dom.push('\u0000' + dom.length);
      dom.push(l);
    });
    var pick = palette(dom);
    return function (k) { return DEBT.test(k) ? '#9a2c1f' : pick(k); };
  }

  function drawBudgetTree(groups, total) {
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var labels = groups.map(function (g) { return g.label; });
    var colour = budgetColour(labels);
    var root = d3.hierarchy({ children: groups.map(function (g) {
      return { name: g.label, children: g.children.map(function (c) {
        return { name: c.name, value: c.bn }; }) };
    }) }).sum(function (d) { return d.value; })
      .sort(function (a, b) { return b.value - a.value; });
    d3.treemap().size([W, H]).paddingOuter(2).paddingTop(18).paddingInner(1).round(true)(root);
    var g = svg.selectAll('g.tm').data(root.descendants().filter(function (d) { return d.depth; }))
      .join('g').attr('class', 'tm')
      .attr('transform', function (d) { return 'translate(' + d.x0 + ',' + d.y0 + ')'; });
    g.append('rect')
      .attr('width', function (d) { return Math.max(0, d.x1 - d.x0); })
      .attr('height', function (d) { return Math.max(0, d.y1 - d.y0); })
      .attr('fill', function (d) {
        return d.depth === 1 ? colour(d.data.name) : colour(d.parent.data.name);
      })
      .attr('opacity', function (d) { return d.depth === 1 ? .35 : .9; })
      .attr('stroke', '#fff').attr('stroke-width', .6)
      .append('title').text(function (d) {
        var top = d.depth === 1 ? d : d.parent;
        return (d.depth === 2 ? d.data.name + '\n' : '') + top.data.name
             + '\nRs ' + fmtBn(d.value) + ' · ' + pct(100 * d.value / total);
      });
    g.filter(function (d) { return d.depth === 1; }).append('text')
      .attr('x', 4).attr('y', 13).attr('font-size', 11).attr('font-weight', 700)
      .attr('fill', ink())
      .text(function (d) {
        var w = d.x1 - d.x0;
        var t = d.data.name + ' · ' + pct(100 * d.value / total, 0);
        return w > t.length * 6.2 ? t : (w > 40 ? pct(100 * d.value / total, 0) : '');
      });
    g.filter(function (d) { return d.depth === 2 && d.x1 - d.x0 > 60 && d.y1 - d.y0 > 22; })
      .append('text').attr('x', 4).attr('y', 13).attr('font-size', 10.5).attr('fill', '#fff')
      .text(function (d) {
        var w = d.x1 - d.x0, t = d.data.name;
        return t.length * 6 > w - 6 ? t.slice(0, Math.max(3, Math.floor((w - 6) / 6)) - 1) + '…' : t;
      });
    host.say((bSide === 'expenditure' ? 'Current expenditure by function, ' : 'Receipts by source, ')
             + bYear + ' · Rs ' + fmtBn(total) + ' · budget estimate, not outturn.',
             'Area is rupees budgeted. Hover a block for its figure and share.');
  }

  function drawBudgetTrend(asGdp) {
    var years = budgetYears();
    var labels = [];
    years.forEach(function (y) {
      budgetGroups(bSide, y).forEach(function (g) {
        if (labels.indexOf(g.label) < 0) labels.push(g.label);
      });
    });
    var rows = years.map(function (y) {
      var o = { year: y, end: fyEndOf(y), total: 0 };
      budgetGroups(bSide, y).forEach(function (g) {
        o[g.label] = d3.sum(g.children, function (c) { return c.bn; });
      });
      labels.forEach(function (l) { o[l] = o[l] || 0; o.total += o[l]; });
      o.gdp = GDP[o.end] ? GDP[o.end] / 1000 : null;   // bn
      return o;
    });
    var usable = asGdp ? rows.filter(function (r) { return r.gdp; }) : rows;
    var left = asGdp ? rows.filter(function (r) { return !r.gdp; }).map(function (r) { return r.year; }) : [];
    var tot = function (r) { return asGdp ? 100 * r.total / r.gdp : r.total; };
    labels.sort(function (a, b) {
      return d3.sum(rows, function (r) { return r[b]; }) - d3.sum(rows, function (r) { return r[a]; });
    });
    var colour = budgetColour(labels);
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 14, right: 210, bottom: 30, left: 52 }, W);
    var x = d3.scaleBand().domain(usable.map(function (r) { return r.year; }))
      .range([m.left, W - m.right]).padding(.18);
    var y = d3.scaleLinear().domain([0, d3.max(usable, tot)]).nice().range([H - m.bottom, m.top]);
    var acc = {};
    labels.forEach(function (l) {
      svg.append('g').attr('fill', colour(l)).selectAll('rect').data(usable).join('rect')
        .attr('x', function (r) { return x(r.year); }).attr('width', x.bandwidth())
        .attr('y', function (r) {
          var base = acc[r.year] || 0, v = asGdp ? 100 * r[l] / r.gdp : r[l];
          return y(base + v);
        })
        .attr('height', function (r) {
          var base = acc[r.year] || 0, v = asGdp ? 100 * r[l] / r.gdp : r[l];
          return Math.max(0, y(base) - y(base + v));
        })
        .append('title').text(function (r) {
          return r.year + '\n' + l + ': Rs ' + fmtBn(r[l])
               + (asGdp ? ' · ' + pct(100 * r[l] / r.gdp) + ' of GDP' : '')
               + '\nall ' + bSide + ': Rs ' + fmtBn(r.total)
               + (asGdp ? ' · ' + pct(tot(r)) + ' of GDP' : '');
        });
      usable.forEach(function (r) { acc[r.year] = (acc[r.year] || 0) + (asGdp ? 100 * r[l] / r.gdp : r[l]); });
    });
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x).tickValues(x.domain().filter(function (v, i) {
        return i % Math.ceil(x.domain().length / 9) === 0; })))
      .attr('color', muted()).attr('font-size', 10.5);
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(6).tickFormat(asGdp ? function (v) { return v + '%'; }
                                              : function (v) { return v >= 1000 ? (v / 1000) + ' tn' : v + ' bn'; }))
      .attr('color', muted()).attr('font-size', 11);
    svg.append('text').attr('x', m.left).attr('y', m.top - 3).attr('font-size', 10.5)
      .attr('fill', muted()).text(asGdp ? 'per cent of GDP' : 'billion rupees, nominal');
    var lastRow = usable[usable.length - 1];
    legend(svg, labels, colour, W, m, function (l) {
      var v = asGdp ? pct(100 * lastRow[l] / lastRow.gdp) : fmtBn(lastRow[l]);
      var t = l.replace(' Affairs & Services', '').replace(' & Services', '');
      return (t.length > 22 ? t.slice(0, 21) + '…' : t) + '  ' + v;
    });
    host.say((bSide === 'expenditure' ? 'Current expenditure by function' : 'Receipts by source')
             + (asGdp ? ' as a share of GDP' : ' in nominal rupees')
             + ', budget year by budget year. Legend figures are ' + lastRow.year + '.',
             (asGdp ? 'GDP at current market prices for the budget year, from PBS. '
                + (left.length ? left.join(', ') + ' has no GDP figure yet and is left off. ' : '')
              : 'Nominal rupees: most of the rise since 2021 is prices. ')
             + 'Budget estimates as presented, not outturn.');
  }

  /* ── courts ──────────────────────────────────────────────────────────── */
  /* Islamabad Capital Territory is not a province, and the LJCP tables cover
     four of them plus ICT. Counting the rows and calling the answer
     "5 provinces" invented a fifth. */
  function jurisdictionLabel(list) {
    var ict = list.some(function (p) { return /islamabad/i.test(p); });
    var n = list.length - (ict ? 1 : 0);
    return n + ' province' + (n === 1 ? '' : 's') + (ict ? ' and Islamabad' : '');
  }
  function courtRows() {
    var rows = courts.filter(function (d) { return d.category === 'all'; });
    var provs = Array.from(new Set(rows.map(function (d) { return d.province; })));
    var last = {};
    rows.forEach(function (d) { if (!last[d.province] || d.year > last[d.province].year) last[d.province] = d; });
    provs.sort(function (a, b) { return last[b].pending_end - last[a].pending_end; });
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    return { rows: rows, provs: provs, years: years, last: last };
  }
  function courtsCover(C, extra) {
    var tot = d3.sum(C.provs, function (p) { return C.last[p].pending_end; });
    cover([C.years[0] + ' to ' + C.years[C.years.length - 1], jurisdictionLabel(C.provs),
           shortNum(tot) + ' cases pending at end ' + C.years[C.years.length - 1]]
          .concat(extra || []));
  }

  /* ── courts by district ──────────────────────────────────────────────── */
  /* Every district as a dot on its province's line: the spread inside a
     province is as much the story as the gap between them, and 150 names do
     not fit a bar chart. The highest in each province is named. */
  var CD_YEARS = Array.from(new Set(courtDistricts.map(function (d) { return d.year; }))).sort();
  var cdYear = CD_YEARS[CD_YEARS.length - 1], cdMetric = 'per_100k';
  function titleCase(s) {
    return String(s).replace(/\b[a-z]/g, function (c) { return c.toUpperCase(); });
  }

  function renderCourtsDistricts() {
    var bar = ctlRow();
    seg(bar, 'Year', CD_YEARS.map(function (y) { return [y, String(y)]; }), cdYear,
        function (v) { cdYear = v; render(); });
    var bar2 = ctlRow();
    seg(bar2, 'Measure', [['per_100k', 'Per 100,000 people'],
                          ['per_judge', 'Per working judge']], cdMetric,
        function (v) { cdMetric = v; render(); });
    var M = cdMetric, per = M === 'per_judge';
    var all = courtDistricts.filter(function (d) { return d.year === cdYear; });
    var rows = all.filter(function (d) { return d[M] != null; });
    var provs = Array.from(new Set(rows.map(function (d) { return d.province; })))
      .map(function (p) {
        var v = rows.filter(function (d) { return d.province === p; })
          .map(function (d) { return d[M]; }).sort(d3.ascending);
        return { p: p, med: d3.median(v), n: v.length };
      }).sort(function (a, b) { return b.med - a.med; });
    var missing = Array.from(new Set(all.map(function (d) { return d.province; })))
      .filter(function (p) { return !provs.some(function (q) { return q.p === p; }); });
    cover([String(cdYear), rows.length + ' districts',
           Math.round(d3.sum(all, function (d) { return d.pending_end; })).toLocaleString()
             + ' cases pending',
           per ? 'judges in post where matched' : 'rates on the 2023 census']);
    if (!rows.length) {
      var h0 = chartHost(120);
      h0.say('No district has a matched count of working judges in ' + cdYear + '.',
             'The 2021 report’s staffing tables were not extracted. Switch to '
           + 'per 100,000 people to see this year.');
      return;
    }
    var rowH = 46, host = chartHost(provs.length * rowH + 60);
    var W = host.w, svg = host.svg;
    var m = fit({ top: 24, right: 24, bottom: 30, left: 150 }, W);
    var y = d3.scaleBand().domain(provs.map(function (d) { return d.p; }))
      .range([m.top, m.top + provs.length * rowH]).padding(0.3);
    var x = d3.scaleLinear().domain([0, d3.max(rows, function (d) { return d[M]; })]).nice()
      .range([m.left, W - m.right]);
    var fmt = function (v) { return Math.round(v).toLocaleString(); };
    svg.append('g').selectAll('line').data(x.ticks(6)).join('line')
      .attr('x1', x).attr('x2', x).attr('y1', m.top - 6)
      .attr('y2', m.top + provs.length * rowH).attr('stroke', muted()).attr('opacity', 0.18);
    provs.forEach(function (P) {
      var cy = y(P.p) + y.bandwidth() / 2;
      svg.append('line').attr('x1', m.left).attr('x2', W - m.right)
        .attr('y1', cy).attr('y2', cy).attr('stroke', muted()).attr('opacity', 0.35);
      svg.append('line').attr('x1', x(P.med)).attr('x2', x(P.med))
        .attr('y1', cy - 12).attr('y2', cy + 12).attr('stroke', ink()).attr('stroke-width', 2);
      svg.append('text').attr('x', m.left - 10).attr('y', cy - 2).attr('text-anchor', 'end')
        .attr('font-size', 12).attr('fill', ink()).text(P.p);
      svg.append('text').attr('x', m.left - 10).attr('y', cy + 12).attr('text-anchor', 'end')
        .attr('font-size', 10.5).attr('fill', muted())
        .text(P.n + (P.n === 1 ? ' unit' : ' districts') + ' · median ' + fmt(P.med));
      var ds = rows.filter(function (d) { return d.province === P.p; });
      svg.append('g').selectAll('circle').data(ds).join('circle')
        .attr('cx', function (d) { return x(d[M]); }).attr('cy', cy)
        .attr('r', 5.5).attr('fill', DDPalette.accent()).attr('fill-opacity', 0.55)
        .attr('stroke', DDPalette.accent())
        .append('title').text(function (d) {
          return titleCase(d.district) + ', ' + d.province + ' · ' + cdYear
            + '\n' + fmt(d.pending_end) + ' cases pending'
            + '\n' + fmt(d.per_100k) + ' per 100,000 people (2023 census, '
            + fmt(d.population_2023) + ')'
            + (d.per_judge != null ? '\n' + fmt(d.per_judge) + ' per working judge ('
                                     + d.working_judges + ' judges)' : '\nno matched staffing')
            + (d.clearance_pct != null ? '\nclearance ' + d.clearance_pct.toFixed(0) + '%' : '')
            + (d.sessions && d.sessions !== d.district ? '\nsessions: ' + titleCase(d.sessions) : '')
            + (d.hosted ? '\nalso counts the population of ' + titleCase(d.hosted) : '');
        });
      var top = ds.reduce(function (a, b) { return b[M] > a[M] ? b : a; });
      if (ds.length > 1) svg.append('text').attr('x', Math.min(x(top[M]), W - m.right - 4)).attr('y', cy - 10)
        .attr('text-anchor', x(top[M]) > W - m.right - 60 ? 'end' : 'middle')
        .attr('font-size', 10.5).attr('fill', ink()).text(titleCase(top.district));
    });
    svg.append('g').attr('transform', 'translate(0,' + (m.top + provs.length * rowH) + ')')
      .call(d3.axisBottom(x).ticks(6).tickFormat(function (v) { return v.toLocaleString(); }))
      .attr('color', muted()).attr('font-size', 11);
    host.say((per ? 'Cases pending at the end of ' + cdYear + ' for each judge in post, '
                  : 'Cases pending at the end of ' + cdYear + ' per 100,000 people, ')
             + 'one dot per district, with each province’s median as a bar. '
             + 'Hover a dot for the district.',
             (per ? 'Judges are matched to districts in 384 of 585 district-years: none in '
                  + '2021, none for Khyber Pakhtunkhwa in 2024, and Punjab 2022 is left out '
                  + 'because its staffing is dated 2021. '
                  : 'The population is the 2023 census for every year, not an estimate for '
                  + 'the year. ')
           + 'A low figure where courts are few can mean cases are not filed, not that '
           + 'they are decided quickly; none of this shows that staffing causes backlog.'
           + (missing.length ? ' Not in this view: ' + missing.join(', ') + '.' : ''));
  }

  /* One panel per court, its own scale. The stock is a line with its points;
     the panel's caption carries where it ended and how far it moved. */
  function renderCourtsPending() {
    var C = courtRows();
    courtsCover(C, ['each court on its own scale']);
    var host = chartHost();
    panels(host, C.provs, function (g, w, h, p) {
      var series = C.years.map(function (yv) {
        return C.rows.filter(function (d) { return d.province === p && d.year === yv; })[0]
            || { year: yv, pending_end: null };
      });
      var have = series.filter(function (d) { return d.pending_end != null; });
      var x = d3.scaleLinear().domain(d3.extent(C.years)).range([8, w - 8]);
      var y = d3.scaleLinear().domain([0, d3.max(have, function (d) { return d.pending_end; })])
        .nice().range([h - 22, 30]);
      g.append('path').datum(series).attr('fill', 'none')
        .attr('stroke', DDPalette.accent()).attr('stroke-width', 2)
        .attr('d', d3.line().defined(function (d) { return d.pending_end != null; })
          .x(function (d) { return x(d.year); }).y(function (d) { return y(d.pending_end); }));
      g.selectAll('circle').data(have).join('circle')
        .attr('cx', function (d) { return x(d.year); }).attr('cy', function (d) { return y(d.pending_end); })
        .attr('r', 3).attr('fill', DDPalette.accent())
        .append('title').text(function (d) {
          return p + ' ' + d.year + '\n' + d.pending_end.toLocaleString() + ' pending at year end'
               + '\n' + d.instituted.toLocaleString() + ' instituted, '
               + d.disposed.toLocaleString() + ' disposed';
        });
      g.append('g').attr('transform', 'translate(0,' + (h - 22) + ')')
        .call(d3.axisBottom(x).tickValues(C.years).tickFormat(d3.format('d')).tickSize(3))
        .attr('color', muted()).attr('font-size', 9.5)
        .call(function (a) { a.select('.domain').attr('stroke', 'var(--line)'); });
      var f = have[0], l = have[have.length - 1];
      var ch = 100 * (l.pending_end - f.pending_end) / f.pending_end;
      g.append('text').attr('x', 8).attr('y', 12).attr('font-size', 11.5).attr('font-weight', 700)
        .attr('fill', ink()).text(p);
      g.append('text').attr('x', 8).attr('y', 24).attr('font-size', 10.5).attr('fill', muted())
        .text(shortNum(l.pending_end) + ' pending · ' + signed(ch, 0) + '% since ' + f.year
              + (have.length < C.years.length ? ' · ' + (C.years.length - have.length) + ' year missing' : ''));
      g.append('text').attr('x', w - 6).attr('y', 12).attr('text-anchor', 'end')
        .attr('font-size', 9.5).attr('fill', muted()).text('0 to ' + shortNum(y.domain()[1]));
    }, { cols: 3 });
    host.say('Cases pending at year end, one panel per court. Each panel has its own scale, '
             + 'from zero; the caption gives the latest stock and its change over the series.',
             'Balochistan has no row for 2023. Punjab’s panel reaches 1.5 million; '
             + 'Balochistan’s 19 thousand. On one axis the smaller four are flat lines.');
  }

  /* The backlog's movement over each year, as a share of the opening stock:
     what the reader wants clearance to tell them and it cannot. */
  function renderCourtsNet() {
    var C = courtRows();
    var rows = C.rows.map(function (d) {
      return { k: d.province, year: d.year,
               value: 100 * (d.pending_end - d.pending_start) / d.pending_start,
               abs: d.pending_end - d.pending_start, d: d };
    });
    var rose = rows.filter(function (r) { return r.abs > 0; }).length;
    courtsCover(C, [rose + ' of ' + rows.length + ' court-years the backlog rose']);
    divergingBars(rows, C.provs, C.years, {
      fmt: function (v) { return signed(v, 1) + '%'; },
      axis: 'change in cases pending over the year, % of the opening stock',
      title: function (r) {
        return r.k + ' ' + r.year + '\n' + signed(r.abs, 0).replace('−', '−') + ' cases over the year ('
             + signed(r.value, 1) + '%)\nopened with ' + r.d.pending_start.toLocaleString()
             + ', closed with ' + r.d.pending_end.toLocaleString()
             + '\nclearance ' + r.d.clearance_pct + '%';
      },
      lede: 'How the backlog moved over each year: closing stock against opening stock, '
          + 'as a share of the opening stock. Above the line the backlog grew; below, it shrank.',
      foot: 'The opening stock is the table’s own, and can differ from the previous year’s '
          + 'closing figure where cases transferred between courts. Bars are grouped by court, '
          + 'largest backlog first.',
    });
  }

  /* Clearance around 100, which is the only number on that scale that means
     anything: above it the court decided more than it received. */
  function renderCourtsClearance() {
    var C = courtRows();
    var rows = C.rows.map(function (d) {
      return { k: d.province, year: d.year, value: d.clearance_pct - 100, d: d };
    });
    var above = rows.filter(function (r) { return r.value > 0; }).length;
    courtsCover(C, [above + ' of ' + rows.length + ' court-years above 100 per cent']);
    divergingBars(rows, C.provs, C.years, {
      fmt: function (v) { return signed(v, 1) + ' pts'; },
      axis: 'clearance rate, percentage points above or below 100',
      title: function (r) {
        return r.k + ' ' + r.year + '\nclearance ' + r.d.clearance_pct + '%\n'
             + r.d.disposed.toLocaleString() + ' disposed of '
             + r.d.instituted.toLocaleString() + ' instituted\nbacklog '
             + signed(r.d.pending_end - r.d.pending_start, 0) + ' over the year';
      },
      lede: 'Cases disposed as a share of cases instituted, drawn as the distance from 100 per '
          + 'cent. Above the line a court decided more than it received that year.',
      foot: 'Clearance says nothing about the stock: Punjab cleared 100.5 per cent in 2022 and '
          + 'still ended the year with more pending, because the opening stock was not the '
          + 'previous close. See the backlog chart for the movement itself.',
    });
  }

  /* Civil and criminal nest inside 'all', so they are never added — they are
     drawn as the two shares of each court's backlog. */
  function renderCourtsCategory() {
    var C = courtRows();
    var last = C.years[C.years.length - 1];
    var rows = C.provs.map(function (p) {
      var pick = function (c, yv) {
        return courts.filter(function (d) { return d.province === p && d.category === c && d.year === yv; })[0];
      };
      var yv = last, civ = pick('civil', yv), cri = pick('criminal', yv);
      if (!civ || !cri) { yv = C.years.filter(function (y2) { return pick('civil', y2) && pick('criminal', y2); }).pop(); civ = pick('civil', yv); cri = pick('criminal', yv); }
      var f = C.years.filter(function (y2) { return pick('civil', y2) && pick('criminal', y2); })[0];
      var fc = pick('civil', f), fr = pick('criminal', f);
      return { name: p, year: yv, civil: civ.pending_end, criminal: cri.pending_end,
               firstYear: f, firstCivil: 100 * fc.pending_end / (fc.pending_end + fr.pending_end) };
    });
    courtsCover(C, ['civil against criminal', 'pending at year end']);
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 26, right: 120, bottom: 8, left: 150 }, W);
    var y = d3.scaleBand().domain(rows.map(function (r) { return r.name; }))
      .range([m.top, H - m.bottom]).padding(.3);
    var x = d3.scaleLinear().domain([0, 100]).range([m.left, W - m.right]);
    var cols = { civil: '#1b5e4a', criminal: '#d4a017' };
    rows.forEach(function (r) {
      var tot = r.civil + r.criminal, cs = 100 * r.civil / tot;
      svg.append('rect').attr('x', x(0)).attr('y', y(r.name)).attr('width', x(cs) - x(0))
        .attr('height', y.bandwidth()).attr('fill', cols.civil)
        .append('title').text(r.name + ' ' + r.year + '\ncivil: ' + r.civil.toLocaleString() + ' (' + pct(cs) + ')');
      svg.append('rect').attr('x', x(cs)).attr('y', y(r.name)).attr('width', x(100) - x(cs))
        .attr('height', y.bandwidth()).attr('fill', cols.criminal)
        .append('title').text(r.name + ' ' + r.year + '\ncriminal: ' + r.criminal.toLocaleString() + ' (' + pct(100 - cs) + ')');
      // where the civil share stood in the first year, as a tick
      svg.append('line').attr('x1', x(r.firstCivil)).attr('x2', x(r.firstCivil))
        .attr('y1', y(r.name) - 3).attr('y2', y(r.name) + y.bandwidth() + 3)
        .attr('stroke', ink()).attr('stroke-width', 1.5)
        .append('title').text('civil share in ' + r.firstYear + ': ' + pct(r.firstCivil));
      svg.append('text').attr('x', x(cs) - 6).attr('y', y(r.name) + y.bandwidth() / 2 + 4)
        .attr('text-anchor', 'end').attr('font-size', 11).attr('fill', '#fff').attr('font-weight', 600)
        .text(pct(cs, 0) + ' civil');
      svg.append('text').attr('x', x(100) + 6).attr('y', y(r.name) + y.bandwidth() / 2 + 4)
        .attr('font-size', 11).attr('fill', ink()).text(shortNum(tot) + ' pending');
    });
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).tickSize(0)).attr('color', muted()).attr('font-size', 11.5)
      .call(function (g) { g.select('.domain').attr('stroke', 'none'); });
    svg.append('g').attr('transform', 'translate(0,' + m.top + ')')
      .call(d3.axisTop(x).ticks(5).tickFormat(function (v) { return v + '%'; }).tickSize(0))
      .attr('color', muted()).attr('font-size', 10)
      .call(function (g) { g.select('.domain').attr('stroke', 'none'); });
    host.say('Civil and criminal cases as shares of each court’s backlog at end ' + last
             + '. The black tick marks the civil share in ' + C.years[0] + '.',
             'Sindh is the only court where criminal cases outnumber civil. The two are the parts of '
             + 'the ‘all courts’ figure and are never added to it. A court missing the last '
             + 'year is drawn at its latest year with both categories.');
  }

  /* ── judges ──────────────────────────────────────────────────────────── */
  var TIER_LABEL = {
    district_sessions_judges: 'District & sessions judges',
    additional_district_sessions_judges: 'Additional district & sessions',
    civil_judges_magistrates_family_judges: 'Civil judges & magistrates',
    senior_civil_judges: 'Senior civil judges',
    majlis_e_shoora: 'Majlis-e-Shoora',
    qazi: 'Qazi courts',
  };

  function renderJudges() {
    var years = Array.from(new Set(judges.map(function (d) { return d.year; })))
      .sort(d3.descending);
    var year = years[0];
    var rows = judges.filter(function (d) { return d.year === year; })
      .sort(function (a, b) { return b.sanctioned - a.sanctioned; });
    var tot = function (f) { return d3.sum(rows, function (d) { return d[f]; }); };
    cover(['Balochistan only', year, tot('sanctioned') + ' posts sanctioned',
           tot('working') + ' filled',
           Math.round(100 * tot('vacant') / tot('sanctioned')) + ' per cent vacant']);

    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    // The key sits in its own row above the bars: in the right gutter it
    // collided with the longest bar's label.
    var m = fit({ top: 34, right: 84, bottom: 10, left: 196 }, W);
    var parts = [['working', 'Working', '#1e6b3e'],
                 ['vacant', 'Vacant', '#d4a017'],
                 ['unaccounted', 'Neither', '#b9bfc6']];
    var y = d3.scaleBand()
      .domain(rows.map(function (d) { return TIER_LABEL[d.tier] || d.tier; }))
      .range([m.top, H - m.bottom]).padding(.24);
    var x = d3.scaleLinear().domain([0, d3.max(rows, function (d) { return d.sanctioned; })])
      .nice().range([m.left, W - m.right]);
    rows.forEach(function (d) {
      var at = 0;
      parts.forEach(function (p) {
        var v = d[p[0]] || 0;
        if (v > 0) {
          svg.append('rect')
            .attr('x', x(at)).attr('y', y(TIER_LABEL[d.tier] || d.tier))
            .attr('width', Math.max(0, x(at + v) - x(at)))
            .attr('height', y.bandwidth())
            .attr('fill', p[2])
            .append('title').text((TIER_LABEL[d.tier] || d.tier) + '\n' + p[1]
                                  + ': ' + v + ' of ' + d.sanctioned);
        }
        at += v;
      });
      svg.append('text').attr('x', x(d.sanctioned) + 7)
        .attr('y', y(TIER_LABEL[d.tier] || d.tier) + y.bandwidth() / 2 + 4)
        .attr('font-size', 11).attr('fill', ink())
        .text(d.working + ' of ' + d.sanctioned);
    });
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).tickSize(0)).attr('color', muted()).attr('font-size', 11)
      .call(function (g) { g.select('.domain').attr('stroke', 'none'); });
    var foot = tot('unaccounted')
      ? '‘Neither’ is ' + tot('unaccounted') + ' posts counted as neither working '
        + 'nor vacant — the table excludes ex-cadre officers'
      : '';
    host.say('Balochistan: sanctioned posts, by what fills them · ' + year, foot);
    var kx = m.left;
    parts.forEach(function (p) {
      if (!tot(p[0])) return;
      var g = svg.append('g').attr('transform', 'translate(' + kx + ',8)');
      g.append('rect').attr('width', 10).attr('height', 10).attr('rx', 2).attr('fill', p[2]);
      var t = g.append('text').attr('x', 15).attr('y', 9).attr('font-size', 11.5)
        .attr('fill', ink()).text(p[1] + '  ' + tot(p[0]));
      kx += 15 + t.node().getComputedTextLength() + 18;
    });
  }

  /* Two years, six ranks: a dumbbell of the vacancy rate, which is what a
     twelve-line chart of working and vacant was trying to say. */
  /* Every province that printed a consolidated strength: a track for the
     posts sanctioned, filled to the judges in post. The empty end is the
     shortfall, whatever the source calls it. */
  var JP_YEARS = Array.from(new Set(judgesProv.filter(function (d) {
    return d.sanctioned != null; }).map(function (d) { return d.year; }))).sort();
  var jpYear = JP_YEARS[JP_YEARS.length - 1];

  function renderJudgesProvince() {
    var bar = ctlRow();
    seg(bar, 'Year', JP_YEARS.map(function (y) { return [y, String(y)]; }), jpYear,
        function (v) { jpYear = v; render(); });
    var yr = judgesProv.filter(function (d) { return d.year === jpYear; });
    var rows = yr.filter(function (d) { return d.sanctioned != null && d.working != null; })
      .map(function (d) {
        d.empty = 100 * (d.sanctioned - d.working) / d.sanctioned; return d; })
      .sort(function (a, b) { return b.empty - a.empty; });
    var off = yr.filter(function (d) { return rows.indexOf(d) < 0; });
    var S = d3.sum(rows, function (d) { return d.sanctioned; }),
        Wk = d3.sum(rows, function (d) { return d.working; });
    cover([String(jpYear), rows.length + ' provinces',
           Wk.toLocaleString() + ' judges in post of ' + S.toLocaleString() + ' posts',
           Math.round(100 * (S - Wk) / S) + '% without a judge']);
    var rowH = 54, host = chartHost(rows.length * rowH + 40);
    var W = host.w, svg = host.svg;
    var m = fit({ top: 10, right: 20, bottom: 10, left: 150 }, W);
    var x = d3.scaleLinear().domain([0, 100]).range([m.left, W - m.right]);
    rows.forEach(function (d, i) {
      var top = m.top + i * rowH, h = 18;
      var g = svg.append('g');
      g.append('rect').attr('x', x(0)).attr('y', top + 8).attr('width', x(100) - x(0))
        .attr('height', h).attr('rx', 3).attr('fill', 'var(--rust)').attr('opacity', 0.28);
      g.append('rect').attr('x', x(0)).attr('y', top + 8)
        .attr('width', x(100 - d.empty) - x(0)).attr('height', h).attr('rx', 3)
        .attr('fill', DDPalette.accent());
      g.append('text').attr('x', m.left - 10).attr('y', top + 21).attr('text-anchor', 'end')
        .attr('font-size', 12).attr('fill', ink()).text(d.province);
      g.append('text').attr('x', x(0)).attr('y', top + h + 22).attr('font-size', 10.5)
        .attr('fill', muted())
        .text(d.working.toLocaleString() + ' in post of ' + d.sanctioned.toLocaleString()
              + ' sanctioned · ' + d.empty.toFixed(0) + '% without a judge'
              + (d.date_status !== 'same_year' ? ' · as at ' + d.as_of : ''));
      g.append('title').text(d.province + ', ' + jpYear + '\n' + d.sanctioned + ' sanctioned, '
        + d.working + ' working, ' + d.vacant + ' reported vacant\n'
        + d.divisions + ' session divisions · strength as at ' + d.as_of);
    });
    host.say('Judicial posts in the district courts, ' + jpYear + ': the full bar is the posts '
           + 'sanctioned, the filled part the judges in post, the pale end the posts without one.',
             'Working excludes officers on deputation outside their cadre, so working and '
           + 'reported vacant do not always add to sanctioned. Balochistan sums four rank '
           + 'tables and counts fewer ranks than the chart by rank. '
           + (off.length ? 'Not drawn: ' + off.map(function (d) {
               return d.province + (d.working ? ' (' + d.working + ' judges; no sanctioned figure)' : '');
             }).join(', ') + '. ' : '')
           + (jpYear === 2024 ? 'Khyber Pakhtunkhwa’s 2024 staffing was not found. ' : '')
           + 'No 2021 staffing tables were extracted.');
  }

  function renderJudgesVacancy() {
    var years = Array.from(new Set(judges.map(function (d) { return d.year; }))).sort(d3.ascending);
    var a = years[0], b = years[years.length - 1];
    var tiers = Array.from(new Set(judges.map(function (d) { return d.tier; })));
    var rows = tiers.map(function (t) {
      var ra = judges.filter(function (d) { return d.tier === t && d.year === a; })[0];
      var rb = judges.filter(function (d) { return d.tier === t && d.year === b; })[0];
      return { name: TIER_LABEL[t] || t,
               a: ra ? 100 * ra.vacant / ra.sanctioned : null,
               b: rb ? 100 * rb.vacant / rb.sanctioned : null,
               sub: (ra ? ra.vacant + ' of ' + ra.sanctioned : '—') + ' → '
                  + (rb ? rb.vacant + ' of ' + rb.sanctioned : '—') + ' vacant' };
    });
    var tb = judges.filter(function (d) { return d.year === b; });
    cover(['Balochistan only', a + ' to ' + b,
           d3.sum(tb, function (d) { return d.vacant; }) + ' of '
           + d3.sum(tb, function (d) { return d.sanctioned; }) + ' posts vacant in ' + b,
           tiers.length + ' ranks']);
    dumbbell(rows, {
      aLabel: String(a), bLabel: String(b), upIsBad: true,
      fmt: function (v) { return pct(v, 0); },
      delta: function (r) { return signed(r.b - r.a, 0) + ' pts'; },
      lede: 'Vacant posts as a share of sanctioned posts, by rank, ' + a + ' against ' + b + '.',
      foot: 'Balochistan only. Vacant and working do not add to sanctioned: an ex-cadre '
          + 'officer’s post counts as neither, so a falling vacancy rate can be a post '
          + 'filled on deputation rather than a judge appointed.',
    });
  }

  /* ── crime ───────────────────────────────────────────────────────────── */
  var FORCE_LABEL = { KP: 'Khyber Pakhtunkhwa', ICT: 'Islamabad', GB: 'Gilgit-Baltistan',
                      AJK: 'Azad Jammu & Kashmir' };
  function forceLabel(f) { return FORCE_LABEL[f] || f; }
  function crimeYears() {
    return Array.from(new Set(crime.map(function (d) { return d.year; }))).sort(d3.ascending);
  }
  function crimeCover(extra) {
    var years = crimeYears();
    var nat = crime.filter(function (d) { return d.region === 'Pakistan'; })
      .sort(function (a, b) { return a.year - b.year; });
    cover([years[0] + ' to ' + years[years.length - 1],
           shortNum(nat[nat.length - 1].value) + ' cases in ' + nat[nat.length - 1].year]
          .concat(extra || []));
  }
  function forces() {
    return Array.from(new Set(crime.map(function (d) { return d.region; })))
      .filter(function (r) { return r !== 'Pakistan'; });
  }
  function dagger(k) {
    return crime.some(function (d) { return d.region === k && d.own_value && d.own_value !== d.value; })
      ? ' †' : '';
  }

  /* Reported cases per 100,000 people, latest year, ranked. Three forces
     have no census denominator and are named beneath rather than guessed. */
  function renderCrimeRate() {
    var years = crimeYears(), last = years[years.length - 1];
    var rows = [], none = [];
    forces().forEach(function (f) {
      var d = crime.filter(function (r) { return r.region === f && r.year === last; })[0];
      if (!d) return;
      if (POP[f]) rows.push({ k: f, name: forceLabel(f) + dagger(f), cases: d.value, pop: POP[f],
                              rate: 1e5 * d.value / POP[f] });
      else none.push(forceLabel(f) + ' (' + d.value.toLocaleString() + ' cases)');
    });
    rows.sort(function (a, b) { return b.rate - a.rate; });
    var withPop = rows.reduce(function (s, r) { return s + r.cases; }, 0);
    var popAll = rows.reduce(function (s, r) { return s + r.pop; }, 0);
    var natRate = 1e5 * withPop / popAll;
    crimeCover([last, rows.length + ' forces with a census denominator',
                '<span class="cov-derived">derived: divided by Census 2023 population</span>']);
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 22, right: 150, bottom: 8, left: 170 }, W);
    var y = d3.scaleBand().domain(rows.map(function (r) { return r.name; }))
      .range([m.top, H - m.bottom - (none.length ? 24 : 0)]).padding(.26);
    var x = d3.scaleLinear().domain([0, d3.max(rows, function (r) { return r.rate; })]).nice()
      .range([m.left, W - m.right]);
    svg.selectAll('rect').data(rows).join('rect')
      .attr('x', m.left).attr('y', function (r) { return y(r.name); })
      .attr('width', function (r) { return Math.max(1, x(r.rate) - m.left); })
      .attr('height', y.bandwidth()).attr('fill', DDPalette.accent()).attr('rx', 2)
      .append('title').text(function (r) {
        return r.name + ' ' + last + '\n' + Math.round(r.rate).toLocaleString() + ' per 100,000\n'
             + r.cases.toLocaleString() + ' cases · ' + shortNum(r.pop) + ' people (2023 census)';
      });
    svg.selectAll('text.v').data(rows).join('text').attr('class', 'v')
      .attr('x', function (r) { return x(r.rate) + 6; })
      .attr('y', function (r) { return y(r.name) + y.bandwidth() / 2 + 4; })
      .attr('font-size', 11).attr('fill', ink())
      .text(function (r) { return Math.round(r.rate).toLocaleString() + ' · ' + shortNum(r.cases) + ' cases'; });
    svg.append('line').attr('x1', x(natRate)).attr('x2', x(natRate)).attr('y1', m.top - 4)
      .attr('y2', H - m.bottom - (none.length ? 20 : 0))
      .attr('stroke', ink()).attr('stroke-dasharray', '3 3');
    svg.append('text').attr('x', x(natRate) + 4).attr('y', m.top - 8).attr('font-size', 10)
      .attr('fill', muted()).text('all ' + rows.length + ' together: ' + Math.round(natRate) + ' per 100,000');
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).tickSize(0)).attr('color', muted()).attr('font-size', 11.5)
      .call(function (g) { g.select('.domain').attr('stroke', 'none'); });
    if (none.length) {
      svg.append('text').attr('x', m.left).attr('y', H - m.bottom - 4).attr('font-size', 10.5)
        .attr('fill', muted()).text('no census denominator: ' + none.join(', '));
    }
    host.say('Reported cases per 100,000 people in ' + last + ', by force, using the 2023 census. '
             + 'The dashed line is the rate across the forces shown.',
             'Offences reported to police, not committed. Azad Jammu & Kashmir and Gilgit-'
             + 'Baltistan lie outside the 2023 census frame and Railways polices no territory, '
             + 'so those three have cases and no rate. † marks a force whose own yearbook '
             + 'gives a different total from PBS.');
  }

  /* Every force at 100 in the first year: who rose fastest, regardless of size. */
  function renderCrimeIndexed() {
    var years = crimeYears(), base = years[0];
    var rows = [];
    forces().forEach(function (f) {
      var b = crime.filter(function (d) { return d.region === f && d.year === base; })[0];
      if (!b) return;
      crime.filter(function (d) { return d.region === f; }).forEach(function (d) {
        rows.push({ k: forceLabel(f) + dagger(f), year: d.year, value: 100 * d.value / b.value, raw: d.value });
      });
    });
    var nat = crime.filter(function (d) { return d.region === 'Pakistan'; });
    var natBase = nat.filter(function (d) { return d.year === base; })[0];
    nat.forEach(function (d) { rows.push({ k: 'Pakistan', year: d.year, value: 100 * d.value / natBase.value, raw: d.value }); });
    var keys = Array.from(new Set(rows.map(function (d) { return d.k; }))).sort(function (a, b) {
      var la = rows.filter(function (d) { return d.k === a; }).pop();
      var lb = rows.filter(function (d) { return d.k === b; }).pop();
      return lb.value - la.value;
    });
    crimeCover([base + ' = 100', keys.length - 1 + ' forces and the national total']);
    lineChart(rows, keys, years, 'value', 'reported cases, ' + base + ' = 100',
      function (v) { return Math.round(v); },
      { colour: function (k) { return k === 'Pakistan' ? 'var(--teal-700)' : palette(keys)(k); },
        width: function (k) { return k === 'Pakistan' ? 3 : 1.8; },
        lede: 'Each force’s reported cases as an index of its own ' + base + ' figure. '
            + 'The thick black line is the national total.',
        foot: 'An index hides size: Islamabad’s doubling is 24,573 cases; Punjab’s '
            + 'is 1.12 million. See the per-head and stacked charts for the levels. '
            + '† marks a force whose own yearbook differs from PBS.' });
  }

  /* The forces stacked, which is how Punjab's share is visible at all. */
  function renderCrimeForce() {
    var years = crimeYears();
    var fs = forces();
    var last = {};
    fs.forEach(function (f) { last[f] = (crime.filter(function (d) { return d.region === f; }).pop() || {}).value || 0; });
    fs.sort(function (a, b) { return last[b] - last[a]; });
    crimeCover([fs.length + ' forces', 'reported offences, not crimes committed']);
    var data = years.map(function (yv) {
      var o = { year: yv, total: 0 };
      fs.forEach(function (f) {
        var d = crime.filter(function (r) { return r.region === f && r.year === yv; })[0];
        o[f] = d ? d.value : 0; o.total += o[f];
      });
      return o;
    });
    stackedArea(data, fs, {
      fmt: shortNum, axis: 'reported cases', label: function (f) { return forceLabel(f) + dagger(f); },
      lede: 'Reported cases by force, stacked to the national total. Punjab is the '
          + 'wide band; the rise from 786,000 to 1.51 million is almost entirely Punjab’s.',
      foot: 'The stack adds the eight forces PBS compiles. Legend figures are the latest year. '
          + '† marks a force whose own yearbook differs from PBS.',
    });
  }

  /* Then-and-now by offence. Two of the eleven are not a six-year series:
     'Others' is reported in 2019 and never again; 'M.V. Theft/ Snatching'
     starts in 2022. Each is compared over the years it has, and says so. */
  function renderCrimeOffence() {
    var rows = offences.filter(function (d) { return d.region === 'Pakistan'; });
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    var keys = Array.from(new Set(rows.map(function (d) { return d.offence; })));
    var out = keys.map(function (k) {
      var s = rows.filter(function (d) { return d.offence === k; }).sort(function (a, b) { return a.year - b.year; });
      var f = s[0], l = s[s.length - 1];
      var partial = f.year !== years[0] || l.year !== years[years.length - 1];
      return { name: k, a: f.value, b: l.value === f.value && s.length === 1 ? null : l.value,
               aYear: f.year, bYear: l.year, partial: partial,
               sub: f.year + ' to ' + l.year + (partial ? ' only' : '') };
    });
    var partial = out.filter(function (r) { return r.partial; }).map(function (r) { return r.name; });
    crimeCover([keys.length + ' named offences', partial.length + ' not reported every year']);
    dumbbell(out, {
      aLabel: String(years[0]), bLabel: String(years[years.length - 1]), upIsBad: true,
      fmt: shortNum, log: true,
      delta: function (r) { return r.b == null ? 'one year only' : signed(100 * (r.b - r.a) / r.a, 0) + '%'; },
      lede: 'Reported cases by offence across Pakistan, first year against last, on a ratio '
          + 'scale so a doubling is the same distance everywhere.',
      foot: 'An offence not reported in every year is compared over the years it has: '
          + partial.join('; ') + '. The eleven do not add to a force’s total.',
    });
  }

  function renderCrimeAjk() {
    var rows = crimeDistricts.filter(function (d) { return d.measure === 'reported_crime_total'; });
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    var keys = Array.from(new Set(rows.map(function (d) { return d.district; })));
    var out = keys.map(function (k) {
      var s = rows.filter(function (d) { return d.district === k; }).sort(function (a, b) { return a.year - b.year; });
      return { name: k, a: s[0].value, b: s[s.length - 1].value, sub: s[0].year + ' to ' + s[s.length - 1].year };
    });
    crimeCover([keys.length + ' districts', 'all reported crime', 'Azad Jammu & Kashmir']);
    dumbbell(out, {
      aLabel: String(years[0]), bLabel: String(years[years.length - 1]), upIsBad: true,
      fmt: function (v) { return v.toLocaleString(); },
      delta: function (r) { return signed(100 * (r.b - r.a) / r.a, 0) + '%'; },
      lede: 'Reported crime by district in Azad Jammu & Kashmir, first year against last.',
      foot: 'The only force publishing a district total. These ten add to the territory’s own '
          + 'figure, which differs from PBS’s by a few dozen cases a year. On the map: '
          + 'Places has these districts.',
    });
  }

  /* Khyber Pakhtunkhwa publishes seven named offences across 37 districts and
     no total. Summed across the province they come to 5,971 cases in 2024
     against a provincial total of 216,872, so they are drawn as the seven
     offences they are and never labelled a total. */
  function renderCrimeKp() {
    var kp = crimeDistricts.filter(function (d) { return d.region === 'KP'; });
    var byOff = d3.rollup(kp, function (v) { return d3.sum(v, function (d) { return d.value; }); },
      function (d) { return d.offence; }, function (d) { return d.year; });
    var years = Array.from(new Set(kp.map(function (d) { return d.year; }))).sort(d3.ascending);
    var out = Array.from(byOff, function (e) {
      var ys = Array.from(e[1].keys()).sort(d3.ascending);
      return { name: e[0], a: e[1].get(ys[0]), b: e[1].get(ys[ys.length - 1]), sub: ys[0] + ' to ' + ys[ys.length - 1] };
    });
    var places = new Set(kp.map(function (d) { return d.district; })).size;
    var last = years[years.length - 1];
    var tot = d3.sum(kp.filter(function (d) { return d.year === last; }), function (d) { return d.value; });
    cover([places + ' districts', out.length + ' named offences',
           tot.toLocaleString() + ' cases in ' + last, 'not a provincial total']);
    dumbbell(out, {
      aLabel: String(years[0]), bLabel: String(last), upIsBad: true, log: true,
      fmt: function (v) { return v.toLocaleString(); },
      delta: function (r) { return signed(100 * (r.b - r.a) / r.a, 0) + '%'; },
      lede: 'Seven serious offences summed across ' + places + ' districts of Khyber Pakhtunkhwa, '
          + 'first year against last, on a ratio scale.',
      foot: 'These are not the province’s crime total, which was 216,872 in 2024 — '
          + 'thirty-six times this. Districts are on the map under Places.',
    });
  }

  /* ── Sindh's own tables ──────────────────────────────────────────────── */
  function sindhProv() { return sindhCrime.filter(function (d) { return d.level === 'province'; }); }

  /* The groups partition the categories, so here — and only here in the
     crime theme — a stack is honest. */
  function renderSindhGroup() {
    var prov = sindhProv();
    var years = Array.from(new Set(prov.map(function (d) { return d.year; }))).sort(d3.ascending);
    var byG = d3.rollup(prov, function (v) { return d3.sum(v, function (d) { return d.value; }); },
      function (d) { return d.group; }, function (d) { return d.year; });
    var groups = Array.from(byG.keys()).sort(function (a, b) {
      return (byG.get(b).get(years[years.length - 1]) || 0) - (byG.get(a).get(years[years.length - 1]) || 0);
    });
    var data = years.map(function (yv) {
      var o = { year: yv, total: 0 };
      groups.forEach(function (g) { o[g] = byG.get(g).get(yv) || 0; o.total += o[g]; });
      return o;
    });
    var f = data[0], l = data[data.length - 1];
    cover([years[0] + ' to ' + years[years.length - 1], groups.length + ' category groups',
           shortNum(l.total) + ' cases in ' + l.year + ' (' + signed(100 * (l.total - f.total) / f.total, 0) + '% on ' + f.year + ')',
           'Sindh police, on its own schema']);
    stackedBars(data, groups, {
      fmt: shortNum, axis: 'cases reported',
      label: function (g) { return g.replace(/&/g, '&').toLowerCase().replace(/^./, function (c) { return c.toUpperCase(); }); },
      lede: 'Cases reported in Sindh by category group, stacked to the provincial total.',
      foot: 'Categories as Sindh groups them. The groups partition the 44 categories, so they add '
          + 'to the total; 2019 comes from the 2020 report’s comparison column.',
    });
  }

  function renderSindhChange(mode) {
    var use = mode === 'range'
      ? sindhCrime.filter(function (d) { return d.level === 'range'; }) : sindhProv();
    var by = mode === 'range' ? 'place' : 'category';
    var roll = d3.rollup(use, function (v) { return d3.sum(v, function (d) { return d.value; }); },
      function (d) { return d[by]; }, function (d) { return d.year; });
    var years = Array.from(new Set(use.map(function (d) { return d.year; }))).sort(d3.ascending);
    var out = Array.from(roll, function (e) {
      var ys = Array.from(e[1].keys()).sort(d3.ascending);
      return { name: e[0], a: e[1].get(ys[0]), b: e[1].get(ys[ys.length - 1]),
               sub: ys[0] + ' to ' + ys[ys.length - 1], key: e[0] };
    }).sort(function (a, b) { return b.b - a.b; });
    var nCat = new Set(sindhProv().map(function (d) { return d.category; })).size;
    if (mode === 'category') out = out.slice(0, 12);
    out.forEach(function (r) { r.name = r.name.toLowerCase().replace(/(^|\s)\S/g, function (c) { return c.toUpperCase(); }); });
    cover([years[0] + ' to ' + years[years.length - 1],
           mode === 'range' ? out.length + ' police ranges' : out.length + ' of ' + nCat + ' categories',
           'Sindh police, on its own schema']);
    dumbbell(out, {
      aLabel: String(years[0]), bLabel: String(years[years.length - 1]), upIsBad: true, log: true,
      fmt: shortNum,
      delta: function (r) { return signed(100 * (r.b - r.a) / r.a, 0) + '%'; },
      lede: mode === 'range'
        ? 'Cases reported by police range, first year against last, on a ratio scale. Karachi '
          + 'Range is more than half of the province.'
        : 'The twelve largest of ' + nCat + ' categories, first year against last, on a ratio scale.',
      foot: mode === 'range'
        ? 'Police ranges are not districts and do not nest inside the census geography.'
        : 'Sindh publishes on its own schema, so these do not line up with the national compilation.',
    });
  }

  /* Eight weeks of 2025, not a year. ytd is a running total and is drawn as
     one; daily_firs is the flow and is the only column that may be summed. */
  function renderFirs() {
    var prov = firs.filter(function (d) { return d.level === 'province'; })
      .sort(function (a, b) { return a.date < b.date ? -1 : 1; });
    var dist = firs.filter(function (d) { return d.level === 'police_district'; });
    var day0 = new Date(prov[0].date), dayN = new Date(prov[prov.length - 1].date);
    var span = Math.round((dayN - day0) / 86400000) + 1;
    cover([niceSpan(prov[0].date, prov[prov.length - 1].date),
           prov.length + ' of ' + span + ' days observed',
           new Set(dist.map(function (d) { return d.place; })).size + ' districts',
           'bulletins, not a daily series']);
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 16, right: 64, bottom: 30, left: 56 }, W);
    var x = d3.scaleTime()
      .domain(d3.extent(prov, function (d) { return new Date(d.date); }))
      .range([m.left, W - m.right]);
    var y = d3.scaleLinear().domain([0, d3.max(prov, function (d) { return d.daily; })])
      .nice().range([H - m.bottom, m.top]);
    var mean = d3.mean(prov, function (d) { return d.daily; });
    /* No line. Drawn as one the six-week gap between 24 September and 6
       November became a long diagonal that reads as a slow decline actually
       measured. Eleven observations are eleven dots. */
    svg.append('line').attr('x1', m.left).attr('x2', W - m.right).attr('y1', y(mean)).attr('y2', y(mean))
      .attr('stroke', muted()).attr('stroke-dasharray', '3 3');
    svg.append('text').attr('x', W - m.right + 4).attr('y', y(mean) + 4).attr('font-size', 10)
      .attr('fill', muted()).text('mean ' + Math.round(mean));
    svg.selectAll('circle').data(prov).join('circle')
      .attr('cx', function (d) { return x(new Date(d.date)); })
      .attr('cy', function (d) { return y(d.daily); })
      .attr('r', 5).attr('fill', DDPalette.accent())
      .append('title').text(function (d) {
        return d.date + '\n' + d.daily.toLocaleString() + ' FIRs that day'
             + '\n' + d.ytd.toLocaleString() + ' so far this year';
      });
    svg.selectAll('text.v').data(prov).join('text').attr('class', 'v')
      .attr('x', function (d) { return x(new Date(d.date)); })
      .attr('y', function (d) { return y(d.daily) - 9; })
      .attr('text-anchor', 'middle').attr('font-size', 10).attr('fill', ink())
      .text(function (d) { return d.daily; });
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x).ticks(6)).attr('color', muted()).attr('font-size', 11);
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(5)).attr('color', muted()).attr('font-size', 11);
    host.say('First information reports registered across Sindh on each day a bulletin exists.',
             'The days are not joined: 43 of the 54 have no bulletin. The year-to-date column '
           + 'in this table is a running total and is never summed; only the daily figure is drawn.');
  }

  /* ── power plants ────────────────────────────────────────────────────── */
  /* THE UNION OF EIGHT REPORTS IS NOT A YEAR. The payload carries the
     observations themselves, one row per plant per fiscal year, and these
     charts name the year they draw. Presence is read, not inferred. */
  var PLANT_YEARS = (D.plants && D.plants.years) || [];
  var PLANT_COVER = (D.plants && D.plants.cover) || {};
  var plantFy = PLANT_YEARS[PLANT_YEARS.length - 1];

  function clearCtl() {
    var old = $('card').querySelectorAll('.ctl-row');
    Array.prototype.forEach.call(old, function (e) { e.remove(); });
  }
  function ctlRow() {
    var bar = document.createElement('div');
    bar.className = 'ctl-row';
    $('card').insertBefore(bar, $('chart'));
    return bar;
  }
  function seg(bar, label, options, value, onPick) {
    var lab = document.createElement('span');
    lab.className = 'ctl-lbl';
    lab.textContent = label;
    bar.appendChild(lab);
    options.forEach(function (o) {
      var b = document.createElement('button');
      b.type = 'button';
      b.className = 'seg';
      b.textContent = o[1];
      b.setAttribute('aria-pressed', o[0] === value ? 'true' : 'false');
      b.onclick = function () { onPick(o[0]); };
      bar.appendChild(b);
    });
  }

  function plantYearPicker() {
    var bar = ctlRow();
    seg(bar, 'Report year', PLANT_YEARS.map(function (fy) { return [fy, fy]; }), plantFy,
        function (v) { plantFy = v; render(); });
  }

  function plantsIn(fy) {
    return plants.filter(function (d) { return d.fy === fy; });
  }

  var FAMILY_ORDER = ['Hydro', 'Nuclear', 'Coal', 'Gas', 'Oil and mixed thermal',
                      'Wind', 'Solar', 'Bagasse and biogas'];
  var FAMILY_COLOUR = { Hydro: '#2f5d7c', Nuclear: '#6b4c7a', Coal: '#7b5a3c',
                        Gas: '#b5651d', 'Oil and mixed thermal': '#a8452f',
                        Wind: '#3f9aa3', Solar: '#d4a017', 'Bagasse and biogas': '#5c6b3a' };

  /* Capacity by fuel family in every report year, stacked: the mix and its
     movement in one chart, which one year at a time cannot show. */
  function renderPlantsMix() {
    var data = PLANT_YEARS.map(function (fy) {
      var o = { year: fy, total: 0 };
      plantsIn(fy).forEach(function (d) {
        if (d.mw == null) return;
        o[d.family] = (o[d.family] || 0) + d.mw; o.total += d.mw;
      });
      FAMILY_ORDER.forEach(function (f) { o[f] = o[f] || 0; });
      return o;
    });
    var f = data[0], l = data[data.length - 1];
    cover([PLANT_YEARS[0] + ' to ' + PLANT_YEARS[PLANT_YEARS.length - 1],
           Math.round(f.total).toLocaleString() + ' MW reported in ' + f.year,
           Math.round(l.total).toLocaleString() + ' MW in ' + l.year,
           'nameplate capacity, not generation']);
    stackedBars(data, FAMILY_ORDER.filter(function (k) { return data.some(function (d) { return d[k] > 0; }); }), {
      fmt: function (v) { return Math.round(v / 1000) + 'k'; }, axis: 'megawatts',
      colour: function (k) { return FAMILY_COLOUR[k]; },
      valueFmt: function (v) { return Math.round(v).toLocaleString() + ' MW'; },
      lede: 'Installed capacity NEPRA reported, by fuel family, for each report year. Each bar '
          + 'is one report; the same plant appears in every bar it was reported in.',
      foot: 'Coal and nuclear are the additions; oil-fired capacity has been leaving the reports. '
          + 'Plants listed without a capacity add nothing here — 11 of 108 in 2017-18. '
          + 'Nameplate, not generation: the fuel mix of capacity is not the mix of electricity.',
    });
  }

  /* Capacity is what could run; generation is what did. The ratio, on
     nameplate over 8,760 hours, is the capacity factor. Plants that report
     no generation are dropped from both sides rather than counted idle. */
  function useOf(rows) {
    var mw = 0, gwh = 0, n = 0;
    rows.forEach(function (d) {
      if (d.mw == null || d.gwh == null || !d.mw) return;
      mw += d.mw; gwh += d.gwh; n += 1;
    });
    return { mw: mw, gwh: gwh, n: n, pct: mw ? 100 * gwh / (mw * 8.76) : null };
  }

  function renderPlantsUsed() {
    state.mode = 'used';
    plantYearPicker();
    var yr = plantsIn(plantFy), all = useOf(yr);
    var fam = FAMILY_ORDER.map(function (f) {
      var u = useOf(yr.filter(function (d) { return d.family === f; }));
      u.family = f; return u;
    }).filter(function (u) { return u.n; })
      .sort(function (a, b) { return b.pct - a.pct; });
    cover([plantFy + ' report',
           'all plants ran at ' + all.pct.toFixed(0) + '% of capacity',
           Math.round(all.gwh).toLocaleString() + ' GWh from '
             + Math.round(all.mw).toLocaleString() + ' MW',
           'CPPA-G system, not K-Electric']);
    barChart(fam, function (d) { return d.family; }, function (d) { return d.pct; },
      'Electricity generated as a share of what the capacity could have produced '
        + 'running all year, ' + plantFy + '. All plants together: '
        + all.pct.toFixed(1) + ' per cent.',
      'Nuclear runs almost flat out; oil-fired plants, most of them paid a capacity '
        + 'charge whether they run or not, produced a fraction of what they could. '
        + 'Hydro and wind are held down by water and weather as well as by demand. '
        + 'Plants that report no generation are left out, not counted as idle.',
      function (d) {
        return Math.round(d.gwh).toLocaleString() + ' GWh from '
             + Math.round(d.mw).toLocaleString() + ' MW · ' + d.n + ' plants';
      },
      function (d) { return d.pct.toFixed(0) + '% · ' + Math.round(d.mw).toLocaleString() + ' MW'; },
      function (d) { return FAMILY_COLOUR[d.family] || DDPalette.accent(); });
  }

  function renderPlantsUseTrend() {
    var ALL = 'All plants', rows = [], idx = {};
    PLANT_YEARS.forEach(function (fy, i) { idx[fy] = i; });
    var years = PLANT_YEARS.map(function (fy, i) { return i; });
    PLANT_YEARS.forEach(function (fy) {
      var yr = plantsIn(fy), all = useOf(yr);
      rows.push({ k: ALL, year: idx[fy], value: all.pct });
      FAMILY_ORDER.forEach(function (f) {
        var u = useOf(yr.filter(function (d) { return d.family === f; }));
        if (u.n) rows.push({ k: f, year: idx[fy], value: u.pct });
      });
    });
    // Wind, solar and bagasse run on weather and harvest, not dispatch, and
    // together are under 7 per cent of capacity; they stay on the one-year
    // chart and out of this one, which is about plants that could be called on.
    var DISPATCH = ['Nuclear', 'Coal', 'Hydro', 'Gas', 'Oil and mixed thermal'];
    var keys = [ALL].concat(DISPATCH.filter(function (f) {
      return rows.some(function (d) { return d.k === f; }); }));
    rows = rows.filter(function (d) { return keys.indexOf(d.k) >= 0; });
    var sys = rows.filter(function (d) { return d.k === ALL; });
    cover([PLANT_YEARS[0] + ' to ' + PLANT_YEARS[PLANT_YEARS.length - 1],
           sys[0].value.toFixed(0) + '% of capacity used in ' + PLANT_YEARS[0],
           sys[sys.length - 1].value.toFixed(0) + '% in ' + PLANT_YEARS[PLANT_YEARS.length - 1],
           'CPPA-G system, not K-Electric']);
    lineChart(rows, keys, years, 'value', '% of capacity used',
      function (v) { return v.toFixed(0) + '%'; },
      { colour: function (k) { return k === ALL ? 'var(--teal-700)' : FAMILY_COLOUR[k]; },
        width: function (k) { return k === ALL ? 3.2 : 1.6; },
        tick: function (i) { return PLANT_YEARS[i]; },
        lede: 'Electricity generated as a share of what the reported capacity could '
            + 'have produced running all year. Capacity grew by 9,000 MW over these '
            + 'years; generation did not keep up, so each megawatt ran less.',
        foot: 'Gas and oil fell furthest, from 47 and 41 per cent in 2017-18 to 27 and '
            + '22 in 2024-25. A falling share is idle capacity that consumers still '
            + 'pay for through capacity charges. Wind, solar and bagasse are on the '
            + 'one-year chart. Nameplate capacity, CPPA-G plants only.' });
  }

  /* What each report covered, from the reports themselves. */
  function renderPlantsReports() {
    var rows = PLANT_YEARS.map(function (fy) {
      var live = plantsIn(fy), c = PLANT_COVER[fy] || {};
      return { fy: fy, n: live.length, rated: c.with_mw != null ? c.with_mw : live.length,
               mw: d3.sum(live, function (d) { return d.mw || 0; }) };
    });
    var last = rows[rows.length - 1];
    var ever = new Set(plants.map(function (d) { return d.id; }));
    var inLast = new Set(plantsIn(PLANT_YEARS[PLANT_YEARS.length - 1])
      .map(function (d) { return d.id; }));
    var gone = 0;
    ever.forEach(function (id) { if (!inLast.has(id)) gone += 1; });
    cover([PLANT_YEARS[0] + ' to ' + PLANT_YEARS[PLANT_YEARS.length - 1],
           last.n + ' plants in ' + last.fy,
           ever.size + ' ever reported, ' + gone + ' not in the last report',
           'the reporting universe, not commissioning']);
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 14, right: 74, bottom: 34, left: 58 }, W);
    var x = d3.scalePoint().domain(PLANT_YEARS).range([m.left, W - m.right]).padding(.4);
    var y = d3.scaleLinear().domain([0, d3.max(rows, function (d) { return d.mw; })])
      .nice().range([H - m.bottom, m.top]);
    svg.append('path').datum(rows).attr('fill', 'none')
      .attr('stroke', '#1e6b3e').attr('stroke-width', 2)
      .attr('d', d3.line().x(function (d) { return x(d.fy); })
                          .y(function (d) { return y(d.mw); }));
    svg.selectAll('circle').data(rows).join('circle')
      .attr('cx', function (d) { return x(d.fy); })
      .attr('cy', function (d) { return y(d.mw); })
      .attr('r', 3.5).attr('fill', '#1e6b3e')
      .append('title').text(function (d) {
        return d.fy + '\n' + d.n + ' plants listed'
             + (d.rated < d.n ? ', ' + d.rated + ' with a capacity' : '')
             + '\n' + Math.round(d.mw).toLocaleString() + ' MW as reported';
      });
    svg.selectAll('text.n').data(rows).join('text').attr('class', 'n')
      .attr('x', function (d) { return x(d.fy); })
      .attr('y', function (d) { return y(d.mw) - 9; })
      .attr('text-anchor', 'middle').attr('font-size', 10.5).attr('fill', muted())
      .text(function (d) { return d.rated < d.n ? d.rated + '/' + d.n : d.n; });
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x)).attr('color', muted()).attr('font-size', 10)
      .selectAll('text').attr('transform', 'rotate(-30)').attr('text-anchor', 'end');
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(5).tickFormat(shortNum))
      .attr('color', muted()).attr('font-size', 11);
    host.say('Megawatts NEPRA reported for each fiscal year, summed from the '
           + 'plants that report names, with the number of plants above each '
           + 'point.',
             'This is what the reports covered, not what was commissioned or '
           + 'retired. A plant leaving the series has dropped out of the '
           + 'reporting, which is not the same as having closed, and where '
           + 'two numbers are shown the first is how many plants carry a '
           + 'capacity figure. Summing every year together would count the '
           + 'same plant up to eight times.');
  }

  function renderPlants(mode) {
    state.mode = mode;
    if (mode === 'reports') return renderPlantsReports();
    plantYearPicker();
    var yr = plantsIn(plantFy).filter(function (d) { return d.mw != null; });
    var c = PLANT_COVER[plantFy] || {};
    var mw = d3.sum(yr, function (d) { return d.mw; });
    cover([plantFy + ' report',
           (c.plants || yr.length) + ' plants listed'
             + (c.with_mw != null && c.with_mw < c.plants
                ? ', ' + c.with_mw + ' with a capacity' : ''),
           Math.round(mw).toLocaleString() + ' MW as reported',
           'nameplate capacity, not generation']);
    if (mode === 'largest') {
      var top = yr.slice().sort(function (a, b) { return b.mw - a.mw; }).slice(0, 18);
      return barChart(top, function (d) { return d.plant; }, function (d) { return d.mw; },
                      'Installed megawatts in ' + plantFy
                        + ' · the 18 largest of ' + yr.length,
                      'Capacity a plant could produce in the year NEPRA '
                        + 'reported it, not what it generated.',
                      function (d) { return d.technology + ' · ' + d.fuel; },
                      null, function (d) { return FAMILY_COLOUR[d.family] || DDPalette.accent(); });
    }
    var fam = Array.from(d3.rollup(yr,
      function (v) {
        return { mw: d3.sum(v, function (d) { return d.mw; }), n: v.length };
      }, function (d) { return d.family; }),
      function (e) { return { family: e[0], mw: e[1].mw, n: e[1].n }; })
      .sort(function (a, b) { return b.mw - a.mw; });
    barChart(fam, function (d) { return d.family; }, function (d) { return d.mw; },
             'Installed megawatts by fuel, ' + plantFy,
             'Nameplate capacity as reported that year. The figure after each '
               + 'bar is how many plants it holds.',
             function (d) { return d.n + (d.n === 1 ? ' plant' : ' plants'); },
             function (d) { return Math.round(d.mw).toLocaleString() + ' MW · ' + d.n; },
             function (d) { return FAMILY_COLOUR[d.family] || DDPalette.accent(); });
  }

  /* ── bills paid ──────────────────────────────────────────────────────── */
  /* Recovery is rupees, not units: a bill issued and never paid. The T&D
     loss charts count that unit as sold. */
  var REC_YEARS = Array.from(new Set(recovery.map(function (d) { return d.fy; }))).sort();
  var recFy = REC_YEARS[REC_YEARS.length - 1];

  function renderDiscoRecovery() {
    var bar = ctlRow();
    seg(bar, 'Year', REC_YEARS.map(function (fy) { return [fy, fy]; }), recFy,
        function (v) { recFy = v; render(); });
    var rows = recovery.filter(function (d) {
      return d.fy === recFy && d.unit !== 'ALL' && d.pct != null;
    }).sort(function (a, b) { return a.pct - b.pct; });
    var sys = recovery.filter(function (d) { return d.fy === recFy && d.unit === 'ALL'; })[0];
    var bn = function (v) { return 'Rs ' + Math.round(v / 1000).toLocaleString() + ' bn'; };
    cover([recFy, rows.length + ' companies',
           sys ? 'system ' + sys.pct.toFixed(1) + '% collected' : '',
           sys ? bn(sys.billed_mn - sys.collected_mn) + ' billed and not collected' : '']
          .filter(Boolean));
    barChart(rows, function (d) { return discoArea(d.unit); },
      function (d) { return d.pct; },
      'Rupees collected for every hundred rupees billed, ' + recFy + ', worst first. '
        + 'This is bills not paid, separate from the units lost before a bill was ever issued.',
      'Above 100 means arrears from earlier years were collected on top of the year’s '
        + 'bills. Hover for the government and private split: in Quetta the '
        + 'private customers paid ' + (function () {
          var q = rows.filter(function (d) { return d.unit === 'QESCO'; })[0];
          return q && q.pvt_pct != null ? q.pvt_pct.toFixed(0) + ' rupees in a hundred' : 'least';
        })() + '.',
      function (d) {
        return bn(d.billed_mn) + ' billed, ' + bn(d.collected_mn) + ' collected'
             + (d.govt_pct != null ? '\ngovernment ' + d.govt_pct.toFixed(0) + '%' : '')
             + (d.pvt_pct != null ? ' · private ' + d.pvt_pct.toFixed(0) + '%' : '');
      },
      function (d) { return d.pct.toFixed(1) + '%'; },
      function (d) { return d.pct < 90 ? 'var(--rust)' : DDPalette.accent(); });
  }

  /* ── electricity distribution losses ─────────────────────────────────── */
  var DISCO_AREA = (D.discos && D.discos.areas) || {};

  function discoArea(u) {
    return DISCO_AREA[u] ? u + ' · ' + DISCO_AREA[u] : u;
  }
  function discoYears() {
    return Array.from(new Set(discos.map(function (d) { return d.fy_end; })))
      .sort(d3.ascending);
  }
  /* K-Electric generates most of what it sells, so its "purchased" column is
     grid imports alone and the three quantities do not decompose. It keeps its
     loss rate and is left out of anything that adds them up. */
  function discoUnitsOnly() {
    return discos.filter(function (d) {
      return d.unit !== 'ALL' && d.unit !== 'K-Electric';
    });
  }
  function discoCover(extra) {
    var ys = discoYears(), last = ys[ys.length - 1];
    var n = new Set(discos.filter(function (d) { return d.unit !== 'ALL'; })
      .map(function (d) { return d.unit; })).size;
    cover([n + ' companies', ys[0] - 1 + '-' + String(ys[0]).slice(2)
           + ' to ' + (last - 1) + '-' + String(last).slice(2)]
          .concat(extra || []));
  }

  /* Each company's first reported rate against its latest. Eighteen years,
     and the worst three have barely moved: that is the chart. */
  function renderDiscoChange() {
    var units = Array.from(new Set(discos.filter(function (d) { return d.unit !== 'ALL'; })
      .map(function (d) { return d.unit; })));
    var out = units.map(function (u) {
      var s = discos.filter(function (d) { return d.unit === u && d.loss_pct != null; })
        .sort(function (a, b) { return a.fy_end - b.fy_end; });
      var f = s[0], l = s[s.length - 1];
      return { name: discoArea(u), a: f.loss_pct, b: l.loss_pct, sub: f.fy + ' to ' + l.fy,
               aFy: f.fy, bFy: l.fy };
    });
    var early = out.filter(function (r) { return r.aFy !== out[0].aFy; });
    discoCover(['first reported year against ' + out[0].bFy]);
    dumbbell(out, {
      aLabel: 'first year', bLabel: 'latest', upIsBad: true,
      fmt: function (v) { return pct(v, 1); },
      delta: function (r) { return signed(r.b - r.a, 1) + ' pts'; },
      lede: 'T&D losses as a share of units entering the system: each company’s first '
          + 'reported year against its latest, worst first.',
      foot: 'Most companies start in 2006-07; SEPCO and TESCO enter the tables later and are '
          + 'compared over the years they have. K-Electric’s rate is NEPRA’s published '
          + 'one on its own available energy, because its purchased column is grid imports only.',
    });
  }

  function renderDiscoLosses() {
    var ys = discoYears();
    var rows = discos.filter(function (d) {
      return d.unit !== 'ALL' && d.loss_pct != null;
    }).map(function (d) {
      return { k: d.unit, year: d.fy_end, value: d.loss_pct };
    });
    var keys = Array.from(new Set(rows.map(function (d) { return d.k; })))
      .sort(function (a, b) {
        var la = rows.filter(function (d) { return d.k === a; }).pop();
        var lb = rows.filter(function (d) { return d.k === b; }).pop();
        return (lb ? lb.value : 0) - (la ? la.value : 0);
      });
    var sys = discos.filter(function (d) {
      return d.unit === 'ALL' && d.loss_pct != null;
    }).sort(function (a, b) { return a.fy_end - b.fy_end; }).pop();
    discoCover(['worst ' + rows.filter(function (d) {
      return d.year === ys[ys.length - 1];
    }).reduce(function (a, b) { return b.value > a.value ? b : a; }).value.toFixed(1)
      + '%', sys ? 'system ' + sys.loss_pct.toFixed(1) + '% in ' + sys.fy : '']
      .filter(Boolean));
    lineChart(rows, keys, ys, 'value',
      'T&D losses, % of units entering the system',
      function (v) { return v.toFixed(0) + '%'; },
      { gaps: true,
        lede: 'The share of the electricity entering each company’s system '
            + 'that never reaches a billed meter — lost in the wires, or '
            + 'taken off them. Bills issued and not paid are a different '
            + 'measure, which NEPRA reports in rupees; those units were '
            + 'billed, so they count as sold here.',
        foot: 'Computed from the units bought and units sold printed in the '
            + 'same row, not from the percentage column beside them: for '
            + 'PESCO in 2006-07 to 2009-10 that column reads 54 to 64 per '
            + 'cent where its own GWh give 32 to 35, and the 2015 edition '
            + 'restates 2010-11 at 37.96 where 2011 printed 62.35. '
            + 'Denominators differ by company and the axis says so: for '
            + 'every DISCO it is units purchased, but K-Electric generates '
            + 'most of what it sells and its purchased column is grid '
            + 'imports alone — 1,083 GWh in 2024-25 against 15,249 GWh sold '
            + '— so purchased minus sold is not its loss. Its rate is '
            + 'NEPRA’s published one, on its own available energy.' });
  }

  function renderDiscoLatest() {
    var ys = discoYears(), last = ys[ys.length - 1];
    var rows = discos.filter(function (d) {
      return d.unit !== 'ALL' && d.fy_end === last && d.loss_pct != null;
    }).sort(function (a, b) { return b.loss_pct - a.loss_pct; });
    if (!rows.length) return renderDiscoLosses();
    var fy = rows[0].fy;
    discoCover([fy, rows.length + ' companies reporting']);
    barChart(rows, function (d) { return discoArea(d.unit); },
      function (d) { return d.loss_pct; },
      'T&D losses in ' + fy + ', % of units entering the system',
      'The spread is the story: the same regulator, the same tariff, and a '
      + 'range from under nine per cent to nearly forty. Bars are ordered worst first.',
      function (d) {
        return d.loss_gwh != null
          ? d.loss_gwh.toLocaleString() + ' GWh lost' : '';
      },
      function (d) { return d.loss_pct.toFixed(1) + '%'; });
  }

  function renderDiscoUnits() {
    var rows = discoUnitsOnly().filter(function (d) {
      return d.purchased != null && d.sold != null && d.loss_gwh != null;
    });
    var ys = Array.from(new Set(rows.map(function (d) { return d.fy_end; })))
      .sort(d3.ascending);
    /* Summed across the companies rather than read off NEPRA's own system
       row, which stops in 2022-23. Where both exist the two agree to within
       0.13 per cent, which is the check on the whole crosswalk. But the set
       of companies is not constant: SEPCO first reports in 2011-12 and
       TESCO in 2010-11, so short years are drawn faded and say so. */
    var full = 0, by = {};
    rows.forEach(function (d) {
      var t = by[d.fy_end] || (by[d.fy_end] = { fy: d.fy, year: d.fy_end,
                                                sold: 0, lost: 0, n: 0 });
      t.sold += d.sold; t.lost += d.loss_gwh; t.n += 1;
      if (t.n > full) full = t.n;
    });
    var years = ys.map(function (y) { return by[y]; });
    var last = years[years.length - 1];
    var short = years.filter(function (d) { return d.n < full; });
    discoCover([last.fy + ': ' + Math.round(last.sold + last.lost).toLocaleString()
                + ' GWh bought',
                Math.round(last.lost).toLocaleString() + ' GWh lost',
                'excludes K-Electric']);

    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 16, right: 130, bottom: 26, left: 58 }, W);
    var x = d3.scaleBand().domain(ys).range([m.left, W - m.right]).padding(.18);
    var y = d3.scaleLinear()
      .domain([0, d3.max(years, function (d) { return d.sold + d.lost; })]).nice()
      .range([H - m.bottom, m.top]);
    var LAYERS = [
      { k: 'sold', label: 'billed', c: 'var(--pine)' },
      { k: 'lost', label: 'never billed (T&D loss)', c: 'var(--rust)' }];
    var acc = {};
    LAYERS.forEach(function (L) {
      svg.append('g').attr('fill', L.c).selectAll('rect')
        .data(years).join('rect')
        .attr('x', function (d) { return x(d.year); })
        .attr('width', x.bandwidth())
        .attr('y', function (d) {
          var base = acc[d.year] || 0;
          return y(base + d[L.k]);
        })
        .attr('height', function (d) {
          var base = acc[d.year] || 0;
          return Math.max(0, y(base) - y(base + d[L.k]));
        })
        .attr('opacity', function (d) { return d.n < full ? 0.45 : 1; })
        .append('title').text(function (d) {
          return d.fy + '\n' + L.label + ': '
               + Math.round(d[L.k]).toLocaleString() + ' GWh\n'
               + (100 * d.lost / (d.sold + d.lost)).toFixed(1) + '% lost overall\n'
               + d.n + ' of ' + full + ' companies reporting';
        });
      years.forEach(function (d) {
        acc[d.year] = (acc[d.year] || 0) + d[L.k];
      });
    });
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x).tickValues(ys.filter(function (v, i) {
        return i % Math.ceil(ys.length / 8) === 0;
      })).tickFormat(function (v) { return (v - 1) + '-' + String(v).slice(2); }))
      .attr('color', muted()).attr('font-size', 11);
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(6).tickFormat(function (v) {
        return (v / 1000).toFixed(0) + 'k';
      })).attr('color', muted()).attr('font-size', 11);
    svg.append('text').attr('x', m.left).attr('y', m.top - 2)
      .attr('font-size', 10.5).attr('fill', muted()).text('GWh bought');
    legend(svg, LAYERS.map(function (L) { return L.label; }),
      d3.scaleOrdinal().domain(LAYERS.map(function (L) { return L.label; }))
        .range(LAYERS.map(function (L) { return L.c; })), W, m);
    host.say('Every unit the distribution companies bought, split into the '
      + 'units they billed and the units they did not.',
      'Faded bars are years when fewer than ' + full + ' companies reported — '
      + (short.length
          ? short[0].fy + ' to ' + short[short.length - 1].fy + ', before '
            + 'SEPCO and TESCO appear in the tables — so part of the step up '
            + 'in 2010-11 is a company arriving rather than demand growing. '
          : '')
      + 'K-Electric is excluded throughout because it generates most of what '
      + 'it sells, so its purchased column is grid imports rather than '
      + 'supply. Where NEPRA prints its own system total, these companies '
      + 'add up to it to within 0.13 per cent.');
  }

  /* ── disaster alerts ─────────────────────────────────────────────────── */
  function eventRows() {
    return events.map(function (e) {
      return Object.assign({}, e, { year: +String(e.start).slice(0, 4) });
    }).filter(function (e) { return e.year; });
  }
  function hazardLabel(h) {
    return h.replace(/_/g, ' ').replace(/^./, function (c) { return c.toUpperCase(); });
  }
  function renderEvents() {
    var rows = eventRows();
    var hazards = Array.from(new Set(rows.map(function (d) { return d.hazard; })));
    cover([rows.length + ' alerts',
           d3.min(rows, function (d) { return d.year; }) + ' to '
             + d3.max(rows, function (d) { return d.year; }),
           hazards.length + ' hazard types',
           'alert levels, not losses']);
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 14, right: 20, bottom: 30, left: 132 }, W);
    var x = d3.scaleLinear().domain(d3.extent(rows, function (d) { return d.year; }))
      .nice().range([m.left, W - m.right]);
    var y = d3.scalePoint().domain(hazards).range([m.top + 14, H - m.bottom - 14]).padding(.6);
    var alerts = ['Orange', 'Red'];
    var colour = d3.scaleOrdinal().domain(alerts).range(['#d4a017', '#9a2c1f']);
    var jitter = {};
    svg.selectAll('circle').data(rows).join('circle')
      .attr('cx', function (d) { return x(d.year); })
      .attr('cy', function (d) {
        var k = d.hazard + d.year;
        jitter[k] = (jitter[k] || 0) + 1;
        return y(d.hazard) + (jitter[k] - 1) * 11 - 4;
      })
      .attr('r', 6.5)
      .attr('fill', function (d) { return colour(d.alert); })
      .attr('stroke', 'white').attr('stroke-width', 1).attr('opacity', .9)
      .append('title')
      .text(function (d) {
        return d.title.trim() + '\n' + d.start + ' to ' + d.end
             + '\n' + d.alert + ' alert';
      });
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x).ticks(8).tickFormat(d3.format('d')))
      .attr('color', muted()).attr('font-size', 11);
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).tickFormat(hazardLabel)).attr('color', muted()).attr('font-size', 11);
    legend(svg, alerts, colour, W, { top: 2, right: 130 }, function (a) {
      return a + ' alert  ' + rows.filter(function (d) { return d.alert === a; }).length;
    });
    host.say('One circle per alert. GDACS grades how severe an event looks as it '
           + 'happens; it does not count losses.', '');
  }

  function renderEventsCount() {
    var rows = eventRows();
    var hazards = Array.from(new Set(rows.map(function (d) { return d.hazard; })));
    var y0 = d3.min(rows, function (d) { return d.year; }), y1 = d3.max(rows, function (d) { return d.year; });
    var years = d3.range(y0, y1 + 1);
    var data = years.map(function (yv) {
      var o = { year: yv, total: 0 };
      hazards.forEach(function (h) {
        o[h] = rows.filter(function (d) { return d.year === yv && d.hazard === h; }).length; o.total += o[h];
      });
      return o;
    });
    cover([rows.length + ' alerts', y0 + ' to ' + y1, hazards.length + ' hazard types',
           'alert levels, not losses']);
    stackedBars(data, hazards, {
      fmt: function (v) { return String(v); }, axis: 'alerts issued', label: hazardLabel,
      colour: d3.scaleOrdinal().domain(hazards).range(['#2f5d7c', '#c98b2e', '#3f9aa3']),
      integer: true,
      lede: 'Alerts GDACS issued for Pakistan in each year, by hazard.',
      foot: 'A year with no bar had no alert. Grades are on the timeline; this chart counts them.',
    });
  }

  /* ── monsoon impacts ─────────────────────────────────────────────────── */
  var IMPACT_LABEL = {
    deaths_total: 'Deaths', injured_total: 'Injured',
    houses_destroyed: 'Houses destroyed',
    houses_partially_damaged: 'Houses partly damaged',
    houses_damaged_total: 'Houses damaged, total',
    livestock_perished: 'Livestock perished',
    roads_damaged_km: 'Roads damaged (km)', bridges_damaged: 'Bridges damaged',
    deaths_male: 'Deaths, male', deaths_female: 'Deaths, female',
    deaths_children: 'Deaths, children', injured_male: 'Injured, male',
    injured_female: 'Injured, female', injured_children: 'Injured, children',
  };
  var IMPACT_TITLE = {
    houses_damaged_total: 'Houses damaged', deaths_total: 'Deaths',
    injured_total: 'Injured', livestock_perished: 'Livestock perished',
  };
  var MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July',
                'August', 'September', 'October', 'November', 'December'];

  function niceSpan(a, b) {
    var p = String(a).split('-'), q = String(b).split('-');
    var from = +p[2] + ' ' + MONTHS[+p[1] - 1];
    return from + (p[0] === q[0] ? '' : ' ' + p[0])
         + ' to ' + (+q[2]) + ' ' + MONTHS[+q[1] - 1] + ' ' + q[0];
  }

  function impactsCover() {
    var nat = {};
    impacts.forEach(function (d) { if (d.level === 'country') nat[d.metric] = d.value; });
    cover([niceSpan(D.impacts.start, D.impacts.end),
           Math.round(nat.deaths_total) + ' deaths',
           Math.round(nat.injured_total) + ' injured',
           'cumulative for the season, not a daily count']);
    return nat;
  }

  /* Four measures, one season, one panel each. */
  function renderImpactsPanel() {
    var nat = impactsCover();
    var metrics = ['deaths_total', 'injured_total', 'houses_damaged_total', 'livestock_perished'];
    var host = chartHost();
    panels(host, metrics, function (g, w, h, metric) {
      var rows = impacts.filter(function (d) { return d.metric === metric && d.level !== 'country'; })
        .sort(function (a, b) { return b.value - a.value; });
      var y = d3.scaleBand().domain(rows.map(function (d) { return d.place; }))
        .range([26, h - 6]).padding(.22);
      var x = d3.scaleLinear().domain([0, d3.max(rows, function (d) { return d.value; }) || 1])
        .range([80, w - 46]);
      g.append('text').attr('x', 6).attr('y', 14).attr('font-size', 11.5).attr('font-weight', 700)
        .attr('fill', ink()).text((IMPACT_TITLE[metric] || metric) + ' · '
                                  + Math.round(nat[metric]).toLocaleString() + ' nationally');
      g.selectAll('rect').data(rows).join('rect')
        .attr('x', 80).attr('y', function (d) { return y(d.place); })
        .attr('width', function (d) { return Math.max(1, x(d.value) - 80); })
        .attr('height', y.bandwidth()).attr('fill', DDPalette.accent()).attr('rx', 2)
        .append('title').text(function (d) { return d.place + ': ' + d.value.toLocaleString(); });
      g.selectAll('text.v').data(rows).join('text').attr('class', 'v')
        .attr('x', function (d) { return x(d.value) + 4; })
        .attr('y', function (d) { return y(d.place) + y.bandwidth() / 2 + 3.5; })
        .attr('font-size', 10).attr('fill', ink()).text(function (d) { return Math.round(d.value).toLocaleString(); });
      g.selectAll('text.k').data(rows).join('text').attr('class', 'k')
        .attr('x', 76).attr('y', function (d) { return y(d.place) + y.bandwidth() / 2 + 3.5; })
        .attr('text-anchor', 'end').attr('font-size', 10).attr('fill', muted())
        .text(function (d) { return d.place; });
    }, { cols: 2, minH: 150 });
    host.say('NDMA situation reports for the 2026 monsoon, by province, four measures.',
             'Cumulative to the latest report. A missing province is one no report covered, not a zero.');
  }

  function renderImpacts() {
    var nat = impactsCover();
    var metric = current.ind;
    var rows = impacts.filter(function (d) {
      return d.metric === metric && d.level !== 'country';
    }).sort(function (a, b) { return b.value - a.value; });
    barChart(rows, function (d) { return d.place; }, function (d) { return d.value; },
             (IMPACT_TITLE[metric] || IMPACT_LABEL[metric] || metric)
               + ' by province · '
               + Math.round(nat[metric]).toLocaleString() + ' nationally',
             'NDMA situation reports. A missing province is one no report covered, '
               + 'not a zero.');
  }

  /* ── shared chart furniture ──────────────────────────────────────────── */
  function ink() {
    return getComputedStyle(document.documentElement).getPropertyValue('--body').trim();
  }
  function muted() {
    return getComputedStyle(document.documentElement).getPropertyValue('--muted').trim();
  }
  /* The shared sixteen, so a nine-force chart does not run out after seven
     and start repeating. DDPalette cycles rather than returning undefined. */
  function palette(domain) {
    var pick = window.DDPalette
      ? window.DDPalette.ordinal(domain)
      : d3.scaleOrdinal().domain(domain)
          .range(['#0c3a1e', '#d4a017', '#0f6e78', '#9a2c1f', '#4d8a62']);
    return function (k) { return pick(k); };
  }
  function shortNum(v) {
    if (v >= 1e6) return (v / 1e6).toFixed(1) + 'm';
    if (v >= 1e3) return (v / 1e3).toFixed(0) + 'k';
    return String(Math.round(v));
  }
  function fmtBn(v) {
    if (v == null) return '—';
    if (Math.abs(v) >= 1000) return (v / 1000).toFixed(v >= 10000 ? 1 : 2) + ' tn';
    return Math.round(v).toLocaleString() + ' bn';
  }
  /* The chart box is a lede, an svg and a foot. Captions live in the two divs
     rather than as svg <text>, because svg text does not wrap: on a phone the
     footnotes ran past the viewBox and were simply cut off. */
  function chartHost(minH) {
    var host = $('chart');
    host.className = 'chart';
    host.innerHTML = '';
    host.style.minHeight = '';
    var lede = document.createElement('div');
    lede.className = 'chart-lede';
    var foot = document.createElement('div');
    foot.className = 'chart-foot';
    host.appendChild(lede);
    var box = host.getBoundingClientRect();
    var W = Math.max(300, box.width), H = Math.max(minH || 280, box.height - 40);
    var svg = d3.select(host).append('svg').attr('viewBox', '0 0 ' + W + ' ' + H);
    host.appendChild(foot);
    return { w: W, h: H, svg: svg,
             say: function (a, b) { lede.textContent = a || ''; foot.textContent = b || ''; } };
  }
  /* A chart's left gutter holds category names and its right gutter holds a
     legend, both sized for a desktop card. In a narrow pane the two together
     can exceed the whole width, which inverts the scale and asks SVG for
     negative bar widths. Shrink both to fit, keeping at least 90px of plot. */
  function fit(m, W) {
    var want = m.left + m.right, room = W - 90;
    if (want > room && want > 0) {
      var k = Math.max(0.2, room / want);
      m.left = Math.round(m.left * k);
      m.right = Math.round(m.right * k);
    }
    return m;
  }

  function cover(bits) {
    $('coverage').innerHTML = '<b>Coverage</b>'
      + bits.map(function (b, i) {
          return '<span' + (i === bits.length - 1 && bits.length > 2
            ? ' style="margin-left:auto"' : '') + '>' + b + '</span>';
        }).join('');
  }
  function axes(svg, x, y, m, W, H, fmt, label, log, tick) {
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(tick ? d3.axisBottom(x).tickValues(x.domain()[0] === x.domain()[1] ? [x.domain()[0]]
                     : d3.range(x.domain()[0], x.domain()[1] + 1)).tickFormat(tick)
                 : d3.axisBottom(x).ticks(6).tickFormat(d3.format('d')))
      .attr('color', muted()).attr('font-size', 11);
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(log ? d3.axisLeft(y).ticks(5, fmt) : d3.axisLeft(y).ticks(6).tickFormat(fmt))
      .attr('color', muted()).attr('font-size', 11);
    svg.append('text').attr('x', m.left).attr('y', m.top - 2)
      .attr('font-size', 10.5).attr('fill', muted()).text(label);
  }
  function legend(svg, keys, colour, W, m, labelFn) {
    var g = svg.append('g').attr('transform', 'translate(' + (W - m.right + 10) + ',' + (m.top + 4) + ')');
    keys.forEach(function (k, i) {
      var row = g.append('g').attr('transform', 'translate(0,' + i * 17 + ')');
      row.append('rect').attr('width', 10).attr('height', 10).attr('rx', 2)
        .attr('fill', colour(k));
      row.append('text').attr('x', 15).attr('y', 9).attr('font-size', 11.5)
        .attr('fill', ink()).text(labelFn ? labelFn(k) : k);
    });
  }
  /* An invisible column per year that surfaces the year's figures on hover,
     for stacks where a title per layer would name one band at a time. */
  function hoverYears(svg, rows, x, m, W, H, textOf) {
    if (rows.length < 2) return;
    var step = (x(rows[1].year) - x(rows[0].year)) || 10;
    svg.append('g').selectAll('rect').data(rows).join('rect')
      .attr('x', function (d) { return x(d.year) - step / 2; }).attr('y', m.top)
      .attr('width', step).attr('height', Math.max(0, H - m.bottom - m.top))
      .attr('fill', 'transparent')
      .append('title').text(textOf);
  }
  function barChart(rows, keyOf, valOf, label, foot, subOf, labelOf, colourOf) {
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var names = rows.map(keyOf);
    var wide = d3.max(names, function (n) { return n.length; }) || 10;
    var m = fit({ top: 8, right: 96, bottom: 8,
                  left: Math.min(Math.round(W * 0.42), 20 + wide * 6.4) }, W);
    var y = d3.scaleBand().domain(names).range([m.top, H - m.bottom]).padding(.22);
    var x = d3.scaleLinear().domain([0, d3.max(rows, valOf) || 1]).nice()
      .range([m.left, W - m.right]);
    svg.selectAll('rect').data(rows).join('rect')
      .attr('x', m.left).attr('y', function (d) { return y(keyOf(d)); })
      .attr('width', function (d) { return Math.max(1, x(valOf(d)) - m.left); })
      .attr('height', y.bandwidth())
      .attr('fill', colourOf || DDPalette.accent()).attr('rx', 2)
      .append('title').text(function (d) {
        return keyOf(d) + '\n' + valOf(d).toLocaleString()
             + (subOf ? '\n' + subOf(d) : '');
      });
    svg.selectAll('text.v').data(rows).join('text').attr('class', 'v')
      .attr('x', function (d) { return x(valOf(d)) + 6; })
      .attr('y', function (d) { return y(keyOf(d)) + y.bandwidth() / 2 + 4; })
      .attr('font-size', 11).attr('fill', ink())
      .text(labelOf || function (d) { return valOf(d).toLocaleString(); });
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).tickSize(0)).attr('color', muted()).attr('font-size', 11)
      .call(function (g) { g.select('.domain').attr('stroke', 'none'); });
    host.say(label, foot);
  }

  function lineChart(rows, keys, years, field, label, fmt, opt) {
    opt = opt || {};
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 16, right: 200, bottom: 26, left: 58 }, W);
    var x = d3.scaleLinear().domain(d3.extent(years)).range([m.left, W - m.right]);
    var y;
    if (opt.log) {
      var lo = d3.min(rows, function (d) { return d[field] || undefined; });
      y = d3.scaleLog().domain([Math.max(1, lo * 0.8),
                                d3.max(rows, function (d) { return d[field]; })])
        .range([H - m.bottom, m.top]);
    } else {
      y = d3.scaleLinear()
        .domain([opt.base != null ? opt.base : 0, d3.max(rows, function (d) { return d[field]; })]).nice()
        .range([H - m.bottom, m.top]);
    }
    var colour = palette(keys);
    var col = function (k) { return opt.colour ? opt.colour(k) : colour(k); };
    keys.forEach(function (k) {
      var series = rows.filter(function (d) { return d.k === k; })
        .sort(function (a, b) { return a.year - b.year; });
      if (!series.length) return;
      // With gaps on, a year the source did not report breaks the line rather
      // than being bridged: a continuous line across it would draw a number
      // nobody published.
      if (opt.gaps) {
        var at = {};
        series.forEach(function (d) { at[d.year] = d; });
        series = years.map(function (yr) { return at[yr] || { year: yr, k: k }; });
      }
      var line = d3.line()
        .defined(function (d) { return d[field] !== undefined && d[field] !== null; })
        .x(function (d) { return x(d.year); })
        .y(function (d) { return y(d[field]); });
      svg.append('path').datum(series).attr('fill', 'none')
        .attr('stroke', col(k))
        .attr('stroke-width', opt.width ? opt.width(k) : 2)
        .attr('stroke-dasharray', opt.dash ? opt.dash(k) : null)
        .attr('d', line);
      svg.selectAll('circle.p-' + keys.indexOf(k))
        .data(series.filter(function (d) { return d[field] != null; }))
        .join('circle').attr('class', 'p-' + keys.indexOf(k))
        .attr('cx', function (d) { return x(d.year); })
        .attr('cy', function (d) { return y(d[field]); })
        .attr('r', 2.4).attr('fill', col(k))
        .append('title').text(function (d) {
          return k + '\n' + (opt.tick ? opt.tick(d.year) : d.year) + ': ' + fmt(d[field])
               + (d.raw != null ? ' (' + d.raw.toLocaleString() + ')' : '');
        });
    });
    if (opt.base != null) {
      svg.append('line').attr('x1', m.left).attr('x2', W - m.right)
        .attr('y1', y(100)).attr('y2', y(100)).attr('stroke', muted()).attr('stroke-dasharray', '3 3');
    }
    axes(svg, x, y, m, W, H, fmt, opt.axis || label, opt.log, opt.tick);
    host.say(opt.lede || '', opt.foot || '');
    legend(svg, keys, col, W, m, function (k) {
      var last = rows.filter(function (d) { return d.k === k; }).pop();
      return k + (opt.trail ? opt.trail(k) : '')
           + (last ? '  ' + fmt(last[field]) : '');
    });
  }

  /* Then and now. One row per name; a grey dot at the first value, a full
     dot at the latest, joined by a line coloured by direction. Sorted by
     the latest value, largest first, with the change written beside it. */
  function dumbbell(rows, opt) {
    var host = chartHost(Math.max(280, 26 * rows.length + 60));
    var W = host.w, H = host.h, svg = host.svg;
    rows = rows.slice().sort(function (a, b) {
      return (b.b == null ? b.a : b.b) - (a.b == null ? a.a : a.b);
    });
    var names = rows.map(function (r) { return r.name; });
    var wide = d3.max(names, function (n) { return n.length; }) || 10;
    var m = fit({ top: 48, right: 120, bottom: 8,
                  left: Math.min(Math.round(W * 0.4), 16 + wide * 6.6) }, W);
    var y = d3.scaleBand().domain(names).range([m.top, H - m.bottom]).padding(.35);
    var all = [];
    rows.forEach(function (r) { if (r.a != null) all.push(r.a); if (r.b != null) all.push(r.b); });
    var x = opt.log
      ? d3.scaleLog().domain([Math.max(1, d3.min(all) * 0.8), d3.max(all) * 1.05]).range([m.left, W - m.right])
      : d3.scaleLinear().domain([0, d3.max(all)]).nice().range([m.left, W - m.right]);
    var up = DDPalette.negative, down = DDPalette.positive;
    if (!opt.upIsBad) { up = DDPalette.positive; down = DDPalette.negative; }
    var grey = '#b9bfc6';
    rows.forEach(function (r) {
      var cy = y(r.name) + y.bandwidth() / 2;
      var g = svg.append('g');
      if (r.a != null && r.b != null) {
        g.append('line').attr('x1', x(r.a)).attr('x2', x(r.b)).attr('y1', cy).attr('y2', cy)
          .attr('stroke', r.b > r.a ? up : r.b < r.a ? down : grey).attr('stroke-width', 3)
          .attr('opacity', .8);
      }
      if (r.a != null) {
        g.append('circle').attr('cx', x(r.a)).attr('cy', cy).attr('r', 5)
          .attr('fill', '#faf7ef').attr('stroke', grey).attr('stroke-width', 2);
      }
      if (r.b != null) {
        g.append('circle').attr('cx', x(r.b)).attr('cy', cy).attr('r', 5.5)
          .attr('fill', r.a == null ? grey : r.b > r.a ? up : r.b < r.a ? down : grey);
      }
      var end = r.b != null ? r.b : r.a;
      g.append('text').attr('x', x(Math.max(r.a == null ? 0 : r.a, end)) + 10).attr('y', cy + 4)
        .attr('font-size', 11).attr('fill', ink())
        .text(r.b == null ? opt.fmt(r.a) + '  ' + opt.aLabel + ' only'
              : r.a == null ? opt.fmt(r.b) + '  ' + opt.bLabel + ' only'
              : opt.fmt(r.b) + (opt.delta ? '  ' + opt.delta(r) : ''));
      g.append('title').text(r.name + '\n' + opt.aLabel + ': ' + (r.a == null ? 'not reported' : opt.fmt(r.a))
        + '\n' + opt.bLabel + ': ' + (r.b == null ? 'not reported' : opt.fmt(r.b))
        + (r.sub ? '\n' + r.sub : ''));
    });
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).tickSize(0)).attr('color', muted()).attr('font-size', 11.5)
      .call(function (g) { g.select('.domain').attr('stroke', 'none'); });
    var axis = d3.axisTop(x).tickSize(0).tickFormat(function (v) { return opt.fmt(v); });
    if (opt.log) {
      // Ratio scale: ticks at 1, 2 and 5 in each decade, not every integer.
      axis.tickValues(x.ticks(20).filter(function (v) {
        var mant = v / Math.pow(10, Math.floor(Math.log10(v)));
        return Math.abs(mant - 1) < 1e-9 || Math.abs(mant - 2) < 1e-9 || Math.abs(mant - 5) < 1e-9;
      }));
    } else axis.ticks(5);
    svg.append('g').attr('transform', 'translate(0,' + (m.top - 6) + ')')
      .call(axis).attr('color', muted()).attr('font-size', 10)
      .call(function (g) { g.select('.domain').attr('stroke', 'none'); });
    // key, one row across the top
    var kx = m.left, ky = 12;
    svg.append('circle').attr('cx', kx + 5).attr('cy', ky).attr('r', 5).attr('fill', '#faf7ef')
      .attr('stroke', grey).attr('stroke-width', 2);
    svg.append('text').attr('x', kx + 16).attr('y', ky + 4).attr('font-size', 11).attr('fill', ink()).text(opt.aLabel);
    kx += 26 + opt.aLabel.length * 6.5;
    svg.append('circle').attr('cx', kx + 5).attr('cy', ky).attr('r', 5.5).attr('fill', up);
    svg.append('text').attr('x', kx + 16).attr('y', ky + 4).attr('font-size', 11).attr('fill', ink())
      .text(opt.bLabel + ', higher');
    kx += 26 + (opt.bLabel.length + 8) * 6.5;
    svg.append('circle').attr('cx', kx + 5).attr('cy', ky).attr('r', 5.5).attr('fill', down);
    svg.append('text').attr('x', kx + 16).attr('y', ky + 4).attr('font-size', 11).attr('fill', ink())
      .text(opt.bLabel + ', lower');
    host.say(opt.lede || '', opt.foot || '');
  }

  /* Small multiples: a grid of panels, each handed its own <g>, width and
     height. The card grows with the grid rather than squeezing it. */
  function panels(host, keys, draw, opt) {
    opt = opt || {};
    var W = host.w, cols = Math.max(1, Math.min(opt.cols || 3, Math.floor(W / 220)));
    var rowsN = Math.ceil(keys.length / cols);
    var pw = Math.floor(W / cols), ph = Math.max(opt.minH || 170, Math.floor((host.h) / rowsN));
    var H = ph * rowsN;
    host.svg.attr('viewBox', '0 0 ' + W + ' ' + H);
    $('chart').style.minHeight = (H + 60) + 'px';
    keys.forEach(function (k, i) {
      var g = host.svg.append('g')
        .attr('transform', 'translate(' + (i % cols) * pw + ',' + Math.floor(i / cols) * ph + ')');
      g.append('rect').attr('x', 2).attr('y', 2).attr('width', pw - 4).attr('height', ph - 4)
        .attr('fill', 'none').attr('stroke', 'var(--line)').attr('rx', 6);
      draw(g.append('g').attr('transform', 'translate(6,2)'), pw - 12, ph - 4, k);
    });
  }

  /* Diverging bars, grouped: one group per key on the x axis, one bar per
     year inside it, drawn up or down from a zero line. */
  function divergingBars(rows, keys, years, opt) {
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 22, right: 20, bottom: 48, left: 58 }, W);
    var x0 = d3.scaleBand().domain(keys).range([m.left, W - m.right]).paddingInner(.25);
    var x1 = d3.scaleBand().domain(years).range([0, x0.bandwidth()]).padding(.12);
    var ext = d3.max(rows, function (r) { return Math.abs(r.value); }) || 1;
    var y = d3.scaleLinear().domain([-ext, ext]).nice().range([H - m.bottom, m.top]);
    var up = DDPalette.negative, down = DDPalette.positive;
    svg.selectAll('rect').data(rows).join('rect')
      .attr('x', function (r) { return x0(r.k) + x1(r.year); })
      .attr('width', x1.bandwidth())
      .attr('y', function (r) { return r.value >= 0 ? y(r.value) : y(0); })
      .attr('height', function (r) { return Math.abs(y(r.value) - y(0)); })
      .attr('fill', function (r) { return r.value >= 0 ? up : down; }).attr('rx', 1.5)
      .append('title').text(opt.title);
    svg.selectAll('text.yr').data(rows).join('text').attr('class', 'yr')
      .attr('x', function (r) { return x0(r.k) + x1(r.year) + x1.bandwidth() / 2; })
      .attr('y', H - m.bottom + 12).attr('text-anchor', 'middle').attr('font-size', 9)
      .attr('fill', muted()).text(function (r) { return String(r.year).slice(2); });
    svg.selectAll('text.v').data(rows).join('text').attr('class', 'v')
      .attr('x', function (r) { return x0(r.k) + x1(r.year) + x1.bandwidth() / 2; })
      .attr('y', function (r) { return r.value >= 0 ? y(r.value) - 3 : y(r.value) + 10; })
      .attr('text-anchor', 'middle').attr('font-size', 9.5).attr('fill', ink())
      .text(function (r) { return opt.fmt(r.value); });
    svg.append('line').attr('x1', m.left).attr('x2', W - m.right).attr('y1', y(0)).attr('y2', y(0))
      .attr('stroke', ink()).attr('stroke-width', 1);
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom + 18) + ')')
      .call(d3.axisBottom(x0).tickSize(0)).attr('color', muted()).attr('font-size', 11.5)
      .call(function (g) { g.select('.domain').attr('stroke', 'none'); });
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(6).tickFormat(function (v) { return opt.fmt(v); }))
      .attr('color', muted()).attr('font-size', 10.5);
    svg.append('text').attr('x', m.left).attr('y', m.top - 8).attr('font-size', 10.5)
      .attr('fill', muted()).text(opt.axis);
    host.say(opt.lede || '', opt.foot || '');
  }

  /* Vertical stacked bars, one per year, with a legend of the latest year. */
  function stackedBars(data, keys, opt) {
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 16, right: 210, bottom: 30, left: 52 }, W);
    var colour = opt.colour || palette(keys);
    var x = d3.scaleBand().domain(data.map(function (d) { return d.year; }))
      .range([m.left, W - m.right]).padding(.18);
    var y = d3.scaleLinear().domain([0, d3.max(data, function (d) { return d.total; })]).nice()
      .range([H - m.bottom, m.top]);
    var acc = {};
    keys.forEach(function (k) {
      svg.append('g').attr('fill', colour(k)).selectAll('rect').data(data).join('rect')
        .attr('x', function (d) { return x(d.year); }).attr('width', x.bandwidth())
        .attr('y', function (d) { return y((acc[d.year] || 0) + d[k]); })
        .attr('height', function (d) { return Math.max(0, y(acc[d.year] || 0) - y((acc[d.year] || 0) + d[k])); })
        .append('title').text(function (d) {
          return d.year + '\n' + (opt.label ? opt.label(k) : k) + ': '
               + (opt.valueFmt ? opt.valueFmt(d[k]) : d[k].toLocaleString())
               + ' (' + pct(100 * d[k] / d.total, 0) + ')\ntotal '
               + (opt.valueFmt ? opt.valueFmt(d.total) : d.total.toLocaleString());
        });
      data.forEach(function (d) { acc[d.year] = (acc[d.year] || 0) + d[k]; });
    });
    svg.selectAll('text.t').data(data).join('text').attr('class', 't')
      .attr('x', function (d) { return x(d.year) + x.bandwidth() / 2; })
      .attr('y', function (d) { return y(d.total) - 4; })
      .attr('text-anchor', 'middle').attr('font-size', 9.5).attr('fill', muted())
      .text(function (d) { return d.total ? (opt.integer ? d.total : opt.fmt(d.total)) : ''; });
    var dom = x.domain();
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x).tickValues(dom.filter(function (v, i) {
        return i % Math.ceil(dom.length / 10) === 0; })))
      .attr('color', muted()).attr('font-size', 10.5);
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(6).tickFormat(opt.fmt)).attr('color', muted()).attr('font-size', 11);
    svg.append('text').attr('x', m.left).attr('y', m.top - 3).attr('font-size', 10.5)
      .attr('fill', muted()).text(opt.axis || '');
    var last = data[data.length - 1];
    legend(svg, keys.slice().reverse(), colour, W, m, function (k) {
      var t = opt.label ? opt.label(k) : k;
      return (t.length > 20 ? t.slice(0, 19) + '…' : t) + '  '
           + (opt.valueFmt ? opt.valueFmt(last[k]) : opt.fmt(last[k]));
    });
    host.say(opt.lede || '', opt.foot || '');
  }

  /* A stacked area over years, with hover by year. */
  function stackedArea(data, keys, opt) {
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 16, right: 200, bottom: 26, left: 58 }, W);
    var colour = opt.colour || palette(keys);
    var years = data.map(function (d) { return d.year; });
    var x = d3.scaleLinear().domain(d3.extent(years)).range([m.left, W - m.right]);
    var y = d3.scaleLinear().domain([0, d3.max(data, function (d) { return d.total; })]).nice()
      .range([H - m.bottom, m.top]);
    var series = d3.stack().keys(keys)(data);
    svg.selectAll('path.area').data(series).join('path').attr('class', 'area')
      .attr('fill', function (d) { return colour(d.key); }).attr('opacity', .92)
      .attr('d', d3.area().x(function (d) { return x(d.data.year); })
        .y0(function (d) { return y(d[0]); }).y1(function (d) { return y(d[1]); }))
      .append('title').text(function (d) { return opt.label ? opt.label(d.key) : d.key; });
    hoverYears(svg, data, x, m, W, H, function (d) {
      var lines = [d.year + ' · ' + opt.fmt(d.total)];
      keys.slice().reverse().forEach(function (k) {
        lines.push((opt.label ? opt.label(k) : k) + ': ' + d[k].toLocaleString()
                   + ' (' + pct(100 * d[k] / d.total, 0) + ')');
      });
      return lines.join('\n');
    });
    axes(svg, x, y, m, W, H, opt.fmt, opt.axis || '');
    var last = data[data.length - 1];
    legend(svg, keys, colour, W, m, function (k) {
      return (opt.label ? opt.label(k) : k) + '  ' + opt.fmt(last[k]);
    });
    host.say(opt.lede || '', opt.foot || '');
  }

  /* The source panel is generated from the provenance record, the same one
     the economy page uses, so what this chart is made of is written down once
     and the CSV carries the same words. */
  function renderSrc(chart) {
    var card = $('card');
    if (!card) return;
    var el = card.querySelector('.src');
    if (!el) { el = document.createElement('div'); el.className = 'src';
               card.appendChild(el); }
    var r = window.DDProv && window.DDProv.of('state:' + chart);
    el.innerHTML = r ? window.DDProv.panel(r) : '';
    // The budget exports its own grouped payload through budgetCsv(), so it
    // has a table even though it is in no CSV_BLOCK; the button said
    // "Catalogue" and then downloaded a CSV.
    var btn = $('csvBtn'), has = (current.topic === 'budget' && !!BUDGET)
                             || !!(CSV_BLOCK[chart] && D[CSV_BLOCK[chart]]
                                    && D[CSV_BLOCK[chart]].cols);
    if (btn) {
      btn.textContent = has ? 'CSV' : 'Catalogue';
      btn.title = has ? 'Download this chart’s data as CSV'
        : 'This chart has no table of its own on this page; the catalogue has the rows';
    }
  }

  function csvCell(v) {
    if (v === null || v === undefined) return '';
    v = String(v);
    return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
  }

  /* The CSV serves the block the current indicator actually draws, which is
     not always the block named after its topic. */
  var CSV_BLOCK = {
    crimeOffence: 'offences', crimeAjk: 'crimeDistricts',
    crimeKp: 'crimeDistricts', crimeForce: 'crime', crimeRate: 'crime',
    crimeIndexed: 'crime',
    sindhGroup: 'sindhCrime', sindhCategory: 'sindhCrime',
    sindhRange: 'sindhCrime', firsDaily: 'firs',
    impactsMetric: 'impacts', impactsPanel: 'impacts',
    eventsTimeline: 'events', eventsCount: 'events',
    judgesVacancy: 'judges', judgesComposition: 'judges',
    courtsDistricts: 'courtDistricts', judgesProvince: 'judgesProvince',
    courtsPending: 'courts', courtsClearance: 'courts',
    courtsNet: 'courts', courtsCategory: 'courts',
    taxStack: 'tax', taxGdp: 'tax', taxShare: 'tax', taxShift: 'tax',
    plantsMix: 'plants', plantsFuel: 'plants', plantsLargest: 'plants',
    plantsReports: 'plants', plantsUsed: 'plants', plantsUseTrend: 'plants',
    discoRecovery: 'recovery',
    discoChange: 'discos', discoLosses: 'discos', discoLatest: 'discos',
    discoUnits: 'discos',
    /* The budget draws from its own payload, which is grouped rather than
       the warehouse rows; its CSV is the group totals per year. */
  };

  /* A CSV that is the whole block when the chart drew a slice of it is not
     this chart's data. Where the chart narrows, the export narrows with it,
     and what it narrowed to is written into the file's header. Where the
     chart divides by a derived denominator, the denominator rides along. */
  var VIEW_FILTER = {
    discoLatest: function (rows, cols) {
      var i = cols.indexOf('fy'), last = rows.reduce(function (m, r) {
        return r[i] > m ? r[i] : m; }, '');
      return { rows: rows.filter(function (r) { return r[i] === last; }),
               view: { 'fiscal year': last } };
    },
    impactsMetric: function (rows, cols) {
      var i = cols.indexOf('metric'), m = state.ind;
      return { rows: rows.filter(function (r) { return r[i] === m; }),
               view: { metric: m } };
    },
    courtsDistricts: function (rows, cols) {
      var i = cols.indexOf('year');
      return { rows: rows.filter(function (r) { return r[i] === cdYear; }),
               view: { year: cdYear,
                       'rates': 'per_100k uses the 2023 census for every year; '
                              + 'per_judge = pending_end / working_judges' } };
    },
    plantsUsed: function (rows, cols) {
      var f = cols.indexOf('fy');
      return { rows: rows.filter(function (r) { return r[f] === plantFy; }),
               view: { 'report year': plantFy,
                       'derived': 'share used = gwh / (mw x 8.76); plants with an '
                                + 'empty mw or gwh are left out of both sides' } };
    },
    plantsUseTrend: function (rows) {
      return { rows: rows,
               view: { 'derived': 'share used = gwh / (mw x 8.76), summed by fiscal '
                                + 'year; plants with an empty mw or gwh are left out' } };
    },
    discoRecovery: function (rows, cols) {
      var f = cols.indexOf('fy');
      return { rows: rows.filter(function (r) { return r[f] === recFy; }),
               view: { 'fiscal year': recFy,
                       'derived': 'pct = collected_mn / billed_mn x 100' } };
    },
    plantsFuel: function (rows, cols) {
      var f = cols.indexOf('fy');
      return { rows: rows.filter(function (r) { return r[f] === plantFy; }),
               view: { 'report year': plantFy } };
    },
    plantsLargest: function (rows, cols) {
      var i = cols.indexOf('mw'), f = cols.indexOf('fy');
      return { rows: rows.filter(function (r) {
                 return r[f] === plantFy && r[i] != null; })
                 .sort(function (a, b) { return b[i] - a[i]; }).slice(0, 18),
               view: { 'report year': plantFy,
                       'shown': 'the 18 largest by capacity' } };
    },
    taxGdp: function (rows, cols) {
      var i = cols.indexOf('fy_end');
      return { rows: rows.map(function (r) { return r.concat([GDP[r[i]] || '', GDP_SPLICED[r[i]] ? 1 : 0]); }),
               cols: cols.concat(['gdp_mp_pkr_mn', 'gdp_spliced']),
               view: { 'derived': 'gdp_mp_pkr_mn is GDP at current market prices (PBS); '
                                  + 'gdp_spliced=1 marks years spliced onto the 2015-16 base' } };
    },
    crimeRate: function (rows, cols) {
      var i = cols.indexOf('region');
      return { rows: rows.map(function (r) { return r.concat([POP[r[i]] || '']); }),
               cols: cols.concat(['population_2023']),
               view: { 'derived': 'population_2023 is the Census 2023 total; empty where the '
                                  + 'census does not cover the force' } };
    },
  };

  function budgetCsv() {
    var years = budgetYears(), labels = [];
    years.forEach(function (y) { budgetGroups(bSide, y).forEach(function (g) {
      if (labels.indexOf(g.label) < 0) labels.push(g.label); }); });
    var rows = [['budget_year', 'side', 'group', 'pkr_bn', 'gdp_mp_pkr_bn']];
    years.forEach(function (y) {
      budgetGroups(bSide, y).forEach(function (g) {
        rows.push([y, bSide, g.label, d3.sum(g.children, function (c) { return c.bn; }).toFixed(2),
                   GDP[fyEndOf(y)] ? (GDP[fyEndOf(y)] / 1000).toFixed(1) : '']);
      });
    });
    var head = ['# Data Darbar - the federal budget, ' + bSide + ', grouped; budget estimates, not outturn',
                '# gdp_mp_pkr_bn is GDP at current market prices for the budget year (PBS), derived', '#'];
    var csv = head.concat(rows.map(function (r) { return r.map(csvCell).join(','); })).join('\n');
    save(csv, 'data_darbar_budget_' + bSide + '.csv');
  }
  function save(csv, name) {
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    a.download = name;
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  function downloadCsv() {
    if (current.topic === 'budget' && BUDGET) return budgetCsv();
    var name = CSV_BLOCK[current.chart];
    var block = name && D[name];
    if (!block || !block.cols) {
      window.location.href = 'datasets/' + current.ds.replace(/_/g, '-') + '/';
      return;
    }
    var rows = block.rows, cols = block.cols, view = {};
    var f = VIEW_FILTER[current.chart];
    if (f) { var r = f(block.rows, block.cols); rows = r.rows; view = r.view; if (r.cols) cols = r.cols; }
    var head = [];
    if (window.DDProv) {
      head = window.DDProv.header('state:' + current.chart, view)
        .map(function (r) { return r.map(csvCell).join(','); });
      if (head.length) head.push('#');
    }
    var csv = head.concat([cols].concat(rows).map(function (r) {
      return r.map(csvCell).join(','); })).join('\n');
    save(csv, 'data_darbar_' + current.ds + '.csv');
  }

  /* The tree and the rail are peers. A tree click sets the topic and takes
     that topic's first indicator; the rail then re-fills itself and reports
     the row it settled on, which is what gets drawn. The URL carries all
     three so a chart can be linked to. */
  function writeUrl() {
    var q = new URLSearchParams({ t: state.topic, i: state.ind });
    history.replaceState(null, '', location.pathname + '?' + q);
  }

  function readUrl() {
    var q = new URLSearchParams(location.search);
    var t = q.get('t'), i = q.get('i');
    var row = D.index.filter(function (r) {
      return (!t || r.topic === t) && (!i || r.ind === i);
    })[0];
    if (row) { state.ds = row.ds; state.topic = row.topic; state.ind = row.ind; }
  }

  function boot() {
    readUrl();
    rail = window.DDExplorer.mount({
      el: $('rail'), index: D.index, state: state,
      levels: ['topic', 'ds', 'ind'], nav: true,
      listLevel: 'ind', listEl: $('chartList'), searchEl: $('xFind'),
      labels: { ind: 'Chart' }, moreEl: $('more'),
      onChange: function (row) { render(row); writeUrl(); },
    });
    render(rail.sync(false));
    $('csvBtn').onclick = downloadCsv;
    var t;
    window.addEventListener('resize', function () {
      clearTimeout(t);
      t = setTimeout(function () { render(current); }, 150);
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
