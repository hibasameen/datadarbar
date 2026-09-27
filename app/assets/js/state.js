/*
   state.js — the State explorer.

   All five themes have data. They were built years ago and never published -
   they sit in the desktop warehouse under ljcp/, regional_police/,
   sindh_police/, sindh_fir/ and climate_events/, with the nepra tables at its
   top level - so a search of app/data found nothing and this page briefly said
   "not collected" about four things that had been collected.

   What each theme does NOT cover is said on the theme rather than left for the
   reader to infer, because every one of these series is patchy in a way that
   changes what it can be read to mean:

     judges    Balochistan only, and its own working/vacant columns do not add
               to sanctioned - a post filled by an ex-cadre officer is neither,
               so the remainder is drawn grey and named rather than hidden.
     crime     eight forces on PBS's definition, drawn on a log scale because
               Punjab is three hundred times Gilgit-Baltistan. AJK appears
               twice in the source and the two figures differ; the chart uses
               PBS and marks the force with a dagger. Only AJK publishes a
               district total: KP publishes seven named offences, which come to
               5,971 cases in 2024 against a provincial 216,872, so they are
               never summed into anything called a total.
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
  var crime = asObjects(D.crime), offences = asObjects(D.offences);
  var crimeDistricts = asObjects(D.crimeDistricts);
  var sindhCrime = asObjects(D.sindhCrime), firs = asObjects(D.firs);
  var plants = asObjects(D.plants), events = asObjects(D.events);
  var impacts = asObjects(D.impacts);

  var TOPICS = {
    tax: {
      theme: 'Public money',
      title: 'What the state collects',
      dek: 'Federal tax collection by head, as FBR reports it. The last year, '
         + '2023–24, comes to 9.3 trillion rupees, of which income tax is '
         + '4.46 trillion.',
      note: 'Direct and indirect tax nest inside their heads, so the chart '
          + 'stacks heads within a type rather than adding both. Collection is '
          + 'in nominal rupees and is not adjusted for inflation — much of the '
          + 'rise after 2021 is prices, not base.',
    },
    budget: {
      theme: 'Public money',
      title: 'The federal budget',
      dek: 'Eighteen years of budget documents are in the warehouse. They are '
         + 'not yet a series.',
      note: '',
    },
    courts: {
      theme: 'Justice',
      title: 'Case flows and pendency',
      dek: 'What was pending, what came in and what was decided, by province, '
         + '2020 to 2024. A clearance rate above 100 per cent means the court '
         + 'decided more than it received that year and the backlog fell.',
      note: 'Categories nest: \u2018all\u2019 contains civil and criminal, so the three '
          + 'are not added together. Where opening plus instituted minus '
          + 'disposed does not equal the closing figure, transfers between '
          + 'courts usually explain it; the table carries the residual.',
    },
    judges: {
      theme: 'Justice',
      title: 'Judges and vacancies',
      dek: 'Sanctioned posts against the judges actually sitting in them, by '
         + 'rank. Of 337 posts in 2024, 235 were filled and 91 stood vacant \u2014 '
         + 'more than a quarter of the bench.',
      note: 'Balochistan only, for 2023 and 2024. The other provinces\u2019 '
          + 'strength tables have not been extracted, so this is not a national '
          + 'picture and should not be read as one. Working and vacant do not '
          + 'add to sanctioned: the source counts as working only judges '
          + 'sitting in their own cadre, so a post filled by an officer on '
          + 'deputation is neither, and the grey remainder is those posts. The '
          + 'shortfall sits almost entirely in the district and sessions rank.',
    },
    crime: {
      theme: 'Crime & policing',
      title: 'Reported offences',
      dek: 'Cases reported to police by force and year, 2019 to 2024. The '
         + 'national figure nearly doubles over the six years, from 786,000 to '
         + '1.51 million, and almost all of the rise is Punjab.',
      note: 'Offences reported, not crimes committed \u2014 the two move for '
          + 'different reasons, and reporting rises with confidence in the '
          + 'police as much as with crime. The forces are drawn on a log scale '
          + 'because Punjab is three hundred times Gilgit-Baltistan. Azad Jammu '
          + '& Kashmir appears twice in the source, once in PBS\u2019s '
          + 'compilation and once in its own yearbooks, and the two disagree by '
          + 'a few dozen cases a year; the chart uses PBS, which is the only '
          + 'series covering all nine forces on one definition. The eleven '
          + 'named offences are a partial breakdown and do not add to a '
          + 'force\u2019s total.',
    },
    sindhCrime: {
      theme: 'Crime & policing',
      title: 'Sindh\u2019s own crime tables',
      dek: 'Sindh police publish on their own schema \u2014 54 offence categories '
         + 'in seven groups, by police range, 2019 to 2025 \u2014 which is a '
         + 'longer and finer series than the national compilation carries.',
      note: 'These figures do not line up with the national compilation and '
          + 'should not be spliced onto it: the categories are Sindh\u2019s own, and '
          + 'the geography is police ranges, which are not districts and do '
          + 'not nest inside the census frame. Prior-year comparison columns '
          + 'printed in the source are excluded, so each year is counted once. '
          + 'Within Sindh the groups do partition the categories, so those add '
          + 'up; the twelve shown are the largest of the 54.',
    },
    firs: {
      theme: 'Crime & policing',
      title: 'First information reports',
      dek: 'Every FIR registered across Sindh, day by day, through the autumn '
         + 'of 2025.',
      note: 'Eight weeks, not a year, and the only complete daily series any '
          + 'Pakistani force publishes. The table also carries a year-to-date '
          + 'column, which is a running total: adding those numbers up would '
          + 'count the same reports once for every day that remained in the '
          + 'year. Only the daily figure drawn here is a flow.',
    },
    discos: {
      theme: 'Energy',
      title: 'Electricity distribution',
      dek: 'Nineteen years of NEPRA\u2019s distribution tables for 25 companies, '
         + 'published and queryable but not yet a series.',
      note: 'The year lives in a row label that is a calendar year in some '
          + 'editions and a fiscal year in others, and the consumer categories '
          + 'drift between editions \u2014 \u2018Agricultural\u2019 in one table and '
          + '\u2018Agricu- ltural\u2019 in the next, where a column header wrapped in '
          + 'the PDF. Charting it needs a label crosswalk across the editions, '
          + 'the same work the budget documents need.',
    },
    plants: {
      theme: 'Energy',
      title: 'Power plants and capacity',
      dek: 'The 133 plants on the national grid, what they burn and how much '
         + 'they could produce.',
      note: 'Installed capacity is nameplate \u2014 what a plant could produce, not '
          + 'what it does, so the fuel mix here is not the generation mix. '
          + 'NEPRA spells the same fuel more than one way across its tables '
          + '(\u2018Coal\u2019 and \u2018THERMAL- COAL\u2019 are both coal), so the bars group '
          + 'its technology and fuel strings into families; each plant keeps '
          + 'the original two, visible on hover.',
    },
    events: {
      theme: 'Public services',
      title: 'Disaster alerts',
      dek: 'Every flood, drought and cyclone alert GDACS has issued for '
         + 'Pakistan since 2001, graded orange or red.',
      note: 'These are alerts, not losses. GDACS grades how severe an event '
          + 'looks as it happens and carries no casualty figures at all \u2014 '
          + 'deaths, people affected and damage are empty in all 31 records, '
          + 'which is an absence of measurement rather than an absence of harm. '
          + 'For impact figures see the monsoon reports, which cover one '
          + 'season and do not correspond to these events. An event\u2019s '
          + 'footprint may cross borders; it is listed here because Pakistan '
          + 'was affected.',
    },
    impacts: {
      theme: 'Public services',
      title: 'Monsoon impacts',
      dek: 'What the 2026 monsoon did, as NDMA\u2019s situation reports counted '
         + 'it: 181 deaths, 515 injured and 1,729 houses damaged by 5 '
         + 'September.',
      note: 'Cumulative for the season to the date of the latest report, not a '
          + 'daily count, so these figures cannot be added to earlier ones or '
          + 'to each other. The province rows add to the national row for every '
          + 'measure except damaged roads, where NDMA rounds 34.56 km to 35. A '
          + 'missing province is one no report covered rather than a province '
          + 'with nothing to report. This is one season \u2014 the only one '
          + 'extracted \u2014 and is not a series.',
    },
  };

  /* State is an index of 27 indicators over 10 datasets. The subject tree and
     the three dropdowns are two ways into the same row; whichever the reader
     uses, `current` is that row and `CHART[row.chart]` draws it. */
  var state = { ds: 'fbr_tax_collection', topic: 'tax', ind: 'stack' };
  var current = D.index[0];
  var rail;

  function $(id) { return document.getElementById(id); }

  function fmtTn(v) { return (v / 1e6).toFixed(1); }

  var CHART = {
    taxStack: function () { renderTax('stack'); },
    taxLines: function () { renderTax('lines'); },
    taxShare: function () { renderTax('share'); },
    budgetPanel: renderBudget,
    courtsPending: function () { renderCourts('pending'); },
    courtsClearance: function () { renderCourts('clearance'); },
    courtsFlow: function () { renderCourts('flow'); },
    courtsCategory: renderCourtsCategory,
    judgesComposition: renderJudges,
    judgesTrend: renderJudgesTrend,
    crimeForce: renderCrimeForce,
    crimeOffence: renderCrimeOffence,
    crimeAjk: renderCrimeAjk,
    crimeKp: renderCrimeKp,
    sindhGroup: function () { renderSindh('group'); },
    sindhCategory: function () { renderSindh('category'); },
    sindhRange: function () { renderSindh('range'); },
    firsDaily: renderFirs,
    plantsFuel: function () { renderPlants('fuel'); },
    plantsLargest: function () { renderPlants('largest'); },
    plantsReports: function () { renderPlants('reports'); },
    discoPanel: renderDiscoPanel,
    eventsTimeline: renderEvents,
    impactsMetric: renderImpacts,
  };

  function render(row) {
    current = row || current;
    var t = TOPICS[current.topic] || {};
    $('paneTheme').textContent = current.theme;
    $('paneTitle').textContent = t.title || current.topicLabel;
    $('paneDek').textContent = t.dek || '';
    $('cardNote').textContent = t.note || '';
    (CHART[current.chart] || renderBudget)();
  }

  /* ── what the state collects ─────────────────────────────────────────── */
  function renderTax(mode) {
    state.mode = mode;
    var years = Array.from(new Set(tax.map(function (d) { return d.fyEnd; }))).sort(d3.ascending);
    $('coverage').innerHTML = '<b>Coverage</b>'
      + '<span>' + tax[0].fy + ' to ' + tax[tax.length - 1].fy + '</span>'
      + '<span>' + years.length + ' fiscal years</span>'
      + '<span>' + new Set(tax.map(function (d) { return d.head; })).size + ' heads</span>'
      + '<span style="margin-left:auto">Nominal rupees, not inflation-adjusted</span>';

    var heads = Array.from(new Set(tax.map(function (d) { return d.head; })));
    var byYear = d3.rollup(tax, function (v) {
      var o = {};
      v.forEach(function (d) { o[d.head] = (o[d.head] || 0) + d.mn; });
      return o;
    }, function (d) { return d.fyEnd; });
    var data = years.map(function (y) {
      var o = { year: y };
      heads.forEach(function (h) { o[h] = (byYear.get(y) || {})[h] || 0; });
      o.total = d3.sum(heads, function (h) { return o[h]; });
      return o;
    });

    draw(data, heads, years);
  }

  function draw(data, heads, years) {
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 12, right: 132, bottom: 26, left: 52 }, W);
    host.say('', state.mode === 'share'
      ? 'Each head as a share of the year\u2019s total collection.'
      : 'Trillions of rupees collected, as FBR reports them.');

    var x = d3.scaleLinear().domain(d3.extent(years)).range([m.left, W - m.right]);
    var colour = d3.scaleOrdinal().domain(heads)
      .range(['#0c3a1e', '#1e6b3e', '#4d8a62', '#7aa88c', '#b5860b', '#d4a017', '#9dbfa9']);

    var share = state.mode === 'share';
    var series, y;
    if (state.mode === 'lines') {
      y = d3.scaleLinear()
        .domain([0, d3.max(data, function (d) { return d3.max(heads, function (h) { return d[h]; }); })])
        .nice().range([H - m.bottom, m.top]);
      heads.forEach(function (h) {
        svg.append('path').datum(data)
          .attr('fill', 'none').attr('stroke', colour(h)).attr('stroke-width', 2)
          .attr('d', d3.line().x(function (d) { return x(d.year); })
                              .y(function (d) { return y(d[h]); }));
      });
    } else {
      var stack = d3.stack().keys(heads)
        .offset(share ? d3.stackOffsetExpand : d3.stackOffsetNone);
      series = stack(data);
      y = d3.scaleLinear()
        .domain([0, share ? 1 : d3.max(data, function (d) { return d.total; })])
        .nice().range([H - m.bottom, m.top]);
      svg.selectAll('path.area').data(series).join('path')
        .attr('class', 'area').attr('fill', function (d) { return colour(d.key); })
        .attr('opacity', .92)
        .attr('d', d3.area().x(function (d) { return x(d.data.year); })
                            .y0(function (d) { return y(d[0]); })
                            .y1(function (d) { return y(d[1]); }));
    }

    var ink = getComputedStyle(document.documentElement).getPropertyValue('--muted').trim();
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x).ticks(8).tickFormat(d3.format('d')))
      .attr('color', ink).attr('font-size', 11);
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(6).tickFormat(
        share ? d3.format('.0%') : function (v) { return fmtTn(v); }))
      .attr('color', ink).attr('font-size', 11);
    svg.append('text').attr('x', m.left).attr('y', m.top - 1)
      .attr('font-size', 10.5).attr('fill', ink)
      .text(share ? 'share of collection' : 'trillion rupees');

    var last = data[data.length - 1];
    var order = heads.slice().sort(function (a, b) { return last[b] - last[a]; });
    var lg = svg.append('g').attr('transform', 'translate(' + (W - m.right + 10) + ',' + (m.top + 4) + ')');
    order.forEach(function (h, i) {
      var g = lg.append('g').attr('transform', 'translate(0,' + i * 17 + ')');
      g.append('rect').attr('width', 10).attr('height', 10).attr('rx', 2).attr('fill', colour(h));
      g.append('text').attr('x', 15).attr('y', 9).attr('font-size', 11.5)
        .attr('fill', getComputedStyle(document.documentElement).getPropertyValue('--body').trim())
        .text(h + '  ' + fmtTn(last[h]));
    });
  }

  /* ── courts ──────────────────────────────────────────────────────────── */
  function renderCourts(mode) {
    state.mode = mode;
    var rows = courts.filter(function (d) { return d.category === 'all'; });
    var provs = Array.from(new Set(rows.map(function (d) { return d.province; }))).sort();
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    cover(['2020 to 2024', provs.length + ' provinces',
           'all courts', 'civil and criminal also available']);
    var key = state.mode === 'clearance' ? 'clearance_pct' : 'pending_end';
    if (state.mode === 'flow') return flowChart(rows, provs, years);
    lineChart(rows.map(function (d) {
                return { k: d.province, year: d.year,
                         pending_end: d.pending_end, clearance_pct: d.clearance_pct };
              }), provs, years, key,
              state.mode === 'clearance' ? 'per cent' : 'cases pending',
              state.mode === 'clearance' ? d3.format('.0f') : shortNum);
  }

  function flowChart(rows, provs, years) {
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 14, right: 130, bottom: 26, left: 58 }, W);
    var x = d3.scaleLinear().domain(d3.extent(years)).range([m.left, W - m.right]);
    var y = d3.scaleLinear()
      .domain([0, d3.max(rows, function (d) { return Math.max(d.instituted, d.disposed); })])
      .nice().range([H - m.bottom, m.top]);
    var colour = palette(provs);
    provs.forEach(function (p) {
      var series = rows.filter(function (d) { return d.province === p; })
        .sort(function (a, b) { return a.year - b.year; });
      [['instituted', '4 3'], ['disposed', null]].forEach(function (pair) {
        svg.append('path').datum(series).attr('fill', 'none')
          .attr('stroke', colour(p)).attr('stroke-width', 2)
          .attr('stroke-dasharray', pair[1])
          .attr('d', d3.line().x(function (d) { return x(d.year); })
                              .y(function (d) { return y(d[pair[0]]); }));
      });
    });
    axes(svg, x, y, m, W, H, shortNum, 'cases');
    legend(svg, provs, colour, W, m, function (p) {
      var last = rows.filter(function (d) { return d.province === p; }).pop();
      return p + '  ' + shortNum(last.disposed);
    });
    host.say('', 'Dashed: cases instituted. Solid: cases disposed.');
  }

  /* Civil and criminal nest inside 'all', so they are never added — they are
     drawn as two lines against each other, which is what the split is for. */
  function renderCourtsCategory() {
    var rows = courts.filter(function (d) { return d.category !== 'all'; });
    var provs = Array.from(new Set(rows.map(function (d) { return d.province; }))).sort();
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    var series = rows.map(function (d) {
      return { k: d.province + ' \u00b7 ' + d.category, year: d.year,
               value: d.pending_end, cat: d.category, prov: d.province };
    });
    var keys = [];
    provs.forEach(function (p) {
      ['civil', 'criminal'].forEach(function (c) { keys.push(p + ' \u00b7 ' + c); });
    });
    keys = keys.filter(function (k) {
      return series.some(function (d) { return d.k === k; });
    });
    cover([years[0] + ' to ' + years[years.length - 1], provs.length + ' provinces',
           'civil against criminal', 'pending at year end']);
    var colour = d3.scaleOrdinal().domain(provs)
      .range(['#0c3a1e', '#1e6b3e', '#b5860b', '#4d8a62', '#d4a017']);
    lineChart(series, keys, years, 'value', 'cases pending, log scale', shortNum,
      { log: true, gaps: true,
        colour: function (k) { return colour(k.split(' \u00b7 ')[0]); },
        dash: function (k) { return /civil$/.test(k) ? '4 3' : null; },
        foot: 'Dashed: civil. Solid: criminal. These two are the parts of the '
            + '\u2018all courts\u2019 figure, so they are never added to it.' });
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
    cover(['Balochistan only', tot('sanctioned') + ' posts sanctioned',
           tot('working') + ' filled',
           Math.round(100 * tot('vacant') / tot('sanctioned')) + ' per cent vacant']);

    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 10, right: 120, bottom: 10, left: 196 }, W);
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
      ? '\u2018Neither\u2019 is ' + tot('unaccounted') + ' posts counted as neither working '
        + 'nor vacant \u2014 the table excludes ex-cadre officers'
      : '';
    host.say('Sanctioned posts, by what fills them \u00b7 ' + year, foot);
    legend(svg, parts.map(function (p) { return p[1]; }),
           d3.scaleOrdinal().domain(parts.map(function (p) { return p[1]; }))
             .range(parts.map(function (p) { return p[2]; })),
           W, { top: 26, right: m.right },
           function (k) {
             var f = parts.filter(function (p) { return p[1] === k; })[0][0];
             return k + '  ' + tot(f);
           });
  }

  /* Two years is a short series, but it is a series: the ranks move in
     different directions, which a single-year bar chart cannot show. */
  function renderJudgesTrend() {
    var years = Array.from(new Set(judges.map(function (d) { return d.year; })))
      .sort(d3.ascending);
    var rows = [];
    judges.forEach(function (d) {
      rows.push({ k: (TIER_LABEL[d.tier] || d.tier) + ' \u00b7 working',
                  year: d.year, value: d.working });
      rows.push({ k: (TIER_LABEL[d.tier] || d.tier) + ' \u00b7 vacant',
                  year: d.year, value: d.vacant });
    });
    var tiers = Array.from(new Set(judges.map(function (d) {
      return TIER_LABEL[d.tier] || d.tier;
    })));
    var keys = rows.map(function (d) { return d.k; })
      .filter(function (k, i, a) { return a.indexOf(k) === i; });
    var colour = palette(tiers);
    var last = years[years.length - 1];
    cover(['Balochistan only', years.join(' and '),
           d3.sum(judges.filter(function (d) { return d.year === last; }),
                  function (d) { return d.vacant; }) + ' posts vacant in ' + last,
           tiers.length + ' ranks']);
    lineChart(rows, keys, years, 'value', 'posts', function (v) { return String(v); },
      { colour: function (k) { return colour(k.split(' \u00b7 ')[0]); },
        dash: function (k) { return /vacant$/.test(k) ? '4 3' : null; },
        foot: 'Dashed: vacant. Solid: working. Balochistan only, and the two '
            + 'do not add to sanctioned strength \u2014 an ex-cadre officer\u2019s post '
            + 'counts as neither.' });
  }

  /* ── crime ───────────────────────────────────────────────────────────── */
  function crimeCover(extra) {
    var years = Array.from(new Set(crime.map(function (d) { return d.year; }))).sort(d3.ascending);
    var nat = crime.filter(function (d) { return d.region === 'Pakistan'; })
      .sort(function (a, b) { return a.year - b.year; });
    cover([years[0] + ' to ' + years[years.length - 1],
           shortNum(nat[nat.length - 1].value) + ' cases in ' + nat[nat.length - 1].year]
          .concat(extra || []));
  }

  function renderCrimeForce() {
    var forces = Array.from(new Set(crime.map(function (d) { return d.region; })))
      .filter(function (r) { return r !== 'Pakistan'; }).sort();
    var years = Array.from(new Set(crime.map(function (d) { return d.year; }))).sort(d3.ascending);
    crimeCover([forces.length + ' forces', 'reported offences, not crimes committed']);
    var rows = crime.filter(function (d) { return d.region !== 'Pakistan'; })
      .map(function (d) { return { k: d.region, year: d.year, value: d.value }; });
    lineChart(rows, forces, years, 'value', 'reported cases, log scale', shortNum,
      { log: true,
        foot: crime.some(function (d) { return d.own_value && d.own_value !== d.value; })
          ? '\u2020 the force\u2019s own yearbook gives a different total from PBS for that year.'
          : '',
        trail: function (k) {
          return crime.some(function (d) {
            return d.region === k && d.own_value && d.own_value !== d.value;
          }) ? ' \u2020' : '';
        } });
  }

  /* Two of the eleven offences are not a six-year series. 'Others' is reported
     in 2019 and never again; 'M.V. Theft/ Snatching' starts in 2022. Those are
     reclassifications, so the lines break rather than joining across the gap —
     a continuous line there would invent a number PBS never published. */
  function renderCrimeOffence() {
    var rows = offences.filter(function (d) { return d.region === 'Pakistan'; })
      .map(function (d) { return { k: d.offence, year: d.year, value: d.value }; });
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    var keys = Array.from(d3.rollup(rows, function (v) {
      return d3.max(v, function (d) { return d.value; });
    }, function (d) { return d.k; }))
      .sort(function (a, b) { return b[1] - a[1]; })
      .map(function (e) { return e[0]; });
    var partial = keys.filter(function (k) {
      return new Set(rows.filter(function (d) { return d.k === k; })
        .map(function (d) { return d.year; })).size < years.length;
    });
    crimeCover([keys.length + ' named offences',
                partial.length + ' of them not reported every year']);
    lineChart(rows, keys, years, 'value', 'cases reported, log scale', shortNum,
      { log: true, gaps: true,
        foot: 'A line breaks where PBS stopped or started naming that offence: '
            + partial.join(' and ') + '. The eleven do not add to a force\u2019s total.' });
  }

  function renderCrimeAjk() {
    var rows = crimeDistricts
      .filter(function (d) { return d.measure === 'reported_crime_total'; })
      .map(function (d) { return { k: d.district, year: d.year, value: d.value }; });
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    var keys = Array.from(new Set(rows.map(function (d) { return d.k; }))).sort();
    crimeCover([keys.length + ' districts', 'all reported crime']);
    lineChart(rows, keys, years, 'value', 'reported cases', shortNum,
      { gaps: true,
        foot: 'Azad Jammu & Kashmir is the only force publishing a district '
            + 'total. These ten add to the territory\u2019s own figure.' });
  }

  /* Khyber Pakhtunkhwa publishes seven named offences across 37 districts and
     no total. Summed across the province they come to 5,971 cases in 2024
     against a provincial total of 216,872, so they are drawn as the seven
     offences they are and never labelled a total. */
  function renderCrimeKp() {
    var kp = crimeDistricts.filter(function (d) { return d.region === 'KP'; });
    var rows = Array.from(d3.rollup(kp,
      function (v) { return d3.sum(v, function (d) { return d.value; }); },
      function (d) { return d.offence; }, function (d) { return d.year; }))
      .flatMap(function (e) {
        return Array.from(e[1], function (y) {
          return { k: e[0], year: y[0], value: y[1] };
        });
      });
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    var keys = Array.from(d3.rollup(rows, function (v) {
      return d3.max(v, function (d) { return d.value; });
    }, function (d) { return d.k; }))
      .sort(function (a, b) { return b[1] - a[1]; }).map(function (e) { return e[0]; });
    var places = new Set(kp.map(function (d) { return d.district; })).size;
    var tot = d3.sum(rows.filter(function (d) { return d.year === years[years.length - 1]; }),
                     function (d) { return d.value; });
    cover([places + ' districts', keys.length + ' named offences',
           tot.toLocaleString() + ' cases in ' + years[years.length - 1],
           'not a provincial total']);
    lineChart(rows, keys, years, 'value', 'cases reported, log scale', shortNum,
      { log: true, gaps: true,
        foot: 'Seven serious offences summed across ' + places + ' districts. '
            + 'They are not Khyber Pakhtunkhwa\u2019s crime total, which was '
            + '216,872 in 2024 \u2014 thirty-six times this.' });
  }

  /* ── Sindh's own tables ──────────────────────────────────────────────── */
  function renderSindh(mode) {
    var prov = sindhCrime.filter(function (d) { return d.level === 'province'; });
    var use = mode === 'range'
      ? sindhCrime.filter(function (d) { return d.level === 'range'; })
      : prov;
    var by = mode === 'group' ? 'group' : mode === 'range' ? 'place' : 'category';
    var rows = Array.from(d3.rollup(use,
      function (v) { return d3.sum(v, function (d) { return d.value; }); },
      function (d) { return d[by]; }, function (d) { return d.year; }))
      .flatMap(function (e) {
        return Array.from(e[1], function (y) {
          return { k: e[0], year: y[0], value: y[1] };
        });
      });
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    var keys = Array.from(d3.rollup(rows, function (v) {
      return d3.max(v, function (d) { return d.value; });
    }, function (d) { return d.k; }))
      .sort(function (a, b) { return b[1] - a[1]; }).map(function (e) { return e[0]; });
    if (mode === 'category') keys = keys.slice(0, 12);
    var shown = rows.filter(function (d) { return keys.indexOf(d.k) >= 0; });
    cover([years[0] + ' to ' + years[years.length - 1],
           mode === 'range' ? keys.length + ' police ranges'
             : keys.length + (mode === 'group' ? ' category groups' : ' of '
                 + new Set(prov.map(function (d) { return d.category; })).size
                 + ' categories'),
           'Sindh police, on its own schema']);
    lineChart(shown, keys, years, 'value', 'cases reported, log scale', shortNum,
      { log: true, gaps: true,
        foot: mode === 'category'
          ? 'The twelve largest of '
            + new Set(prov.map(function (d) { return d.category; })).size
            + ' categories. Sindh publishes on its own schema, so these do not '
            + 'line up with the national compilation.'
          : mode === 'range'
            ? 'Police ranges, which are not districts and do not nest inside '
              + 'the census geography.'
            : 'Categories as Sindh groups them. The groups partition the '
              + 'categories, so they do add to the provincial total.' });
  }

  /* Eight weeks of 2025, not a year. ytd is a running total and is drawn as
     one; daily_firs is the flow and is the only column that may be summed. */
  function renderFirs() {
    var prov = firs.filter(function (d) { return d.level === 'province'; })
      .sort(function (a, b) { return a.date < b.date ? -1 : 1; });
    var dist = firs.filter(function (d) { return d.level === 'police_district'; });
    cover([niceSpan(prov[0].date, prov[prov.length - 1].date),
           prov.length + ' daily reports',
           new Set(dist.map(function (d) { return d.place; })).size + ' districts',
           'eight weeks, not a year']);
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 16, right: 64, bottom: 30, left: 56 }, W);
    var x = d3.scaleTime()
      .domain(d3.extent(prov, function (d) { return new Date(d.date); }))
      .range([m.left, W - m.right]);
    var y = d3.scaleLinear().domain([0, d3.max(prov, function (d) { return d.daily; })])
      .nice().range([H - m.bottom, m.top]);
    svg.append('path').datum(prov).attr('fill', 'none')
      .attr('stroke', '#1e6b3e').attr('stroke-width', 2)
      .attr('d', d3.line().x(function (d) { return x(new Date(d.date)); })
                          .y(function (d) { return y(d.daily); }));
    svg.selectAll('circle').data(prov).join('circle')
      .attr('cx', function (d) { return x(new Date(d.date)); })
      .attr('cy', function (d) { return y(d.daily); })
      .attr('r', 2.5).attr('fill', '#1e6b3e')
      .append('title').text(function (d) {
        return d.date + '\n' + d.daily.toLocaleString() + ' FIRs that day'
             + '\n' + d.ytd.toLocaleString() + ' so far this year';
      });
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x).ticks(5)).attr('color', muted()).attr('font-size', 11);
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(5)).attr('color', muted()).attr('font-size', 11);
    host.say('First information reports registered each day across Sindh.',
             'The year-to-date column in this table is a running total and is '
           + 'never summed; only the daily figure drawn here is a flow.');
  }

  /* ── power plants ────────────────────────────────────────────────────── */
  var FY = ['2017-18', '2018-19', '2019-20', '2020-21', '2021-22', '2022-23',
            '2023-24', '2024-25'];

  /* first_fy and last_fy are the first and last fiscal year a plant appears in
     NEPRA's reports, NOT when it was commissioned: 108 of the 133 carry
     2017-18, which is simply where the report series begins. Drawn as
     cumulative capacity it would show 37,853 MW springing into existence in
     one year. So this counts what is IN the reports each year, and says so. */
  function renderPlantsReports() {
    var rows = FY.map(function (fy) {
      var live = plants.filter(function (d) {
        return d.first_fy <= fy && fy <= d.last_fy;
      });
      return { fy: fy, n: live.length, mw: d3.sum(live, function (d) { return d.mw; }) };
    });
    var gone = plants.filter(function (d) { return d.last_fy < FY[FY.length - 1]; });
    cover([FY[0] + ' to ' + FY[FY.length - 1],
           rows[rows.length - 1].n + ' plants in the latest report',
           gone.length + ' have dropped out',
           'report coverage, not commissioning']);
    var host = chartHost();
    var W = host.w, H = host.h, svg = host.svg;
    var m = fit({ top: 14, right: 74, bottom: 34, left: 58 }, W);
    var x = d3.scalePoint().domain(FY).range([m.left, W - m.right]).padding(.4);
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
        return d.fy + '\n' + d.n + ' plants\n'
             + Math.round(d.mw).toLocaleString() + ' MW';
      });
    svg.selectAll('text.n').data(rows).join('text').attr('class', 'n')
      .attr('x', function (d) { return x(d.fy); })
      .attr('y', function (d) { return y(d.mw) - 9; })
      .attr('text-anchor', 'middle').attr('font-size', 10.5).attr('fill', muted())
      .text(function (d) { return d.n; });
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x)).attr('color', muted()).attr('font-size', 10)
      .selectAll('text').attr('transform', 'rotate(-30)').attr('text-anchor', 'end');
    svg.append('g').attr('transform', 'translate(' + m.left + ',0)')
      .call(d3.axisLeft(y).ticks(5).tickFormat(shortNum))
      .attr('color', muted()).attr('font-size', 11);
    host.say('Installed megawatts present in NEPRA\u2019s report for each fiscal '
           + 'year, with the number of plants above each point.',
             'This is report coverage, not commissioning. The fiscal years on '
           + 'each plant are the first and last it appears in, and 108 of the '
           + '133 begin at 2017-18 because that is where the series starts. '
           + 'The ' + gone.length + ' plants whose last year is earlier have '
           + 'dropped out of the reports, which is not the same as having '
           + 'closed.');
  }

  function renderDiscoPanel() {
    cover(['25 distribution companies', '19 fiscal years', '20,689 rows',
           'not a series yet']);
    var host = $('chart');
    host.className = 'chart panel';
    host.innerHTML =
      '<h2>Nineteen years of distribution tables, and no series in them yet</h2>'
      + '<p>NEPRA\u2019s state-of-industry reports carry 20,689 rows for 25 '
      + 'distribution companies, and they cannot be charted as they stand. The '
      + 'year sits in a row label that is sometimes a calendar year '
      + '(<code>2016</code>) and sometimes a fiscal one (<code>2022-23</code>), '
      + 'so the same company\u2019s history is split across two spellings of '
      + 'time. The consumer categories drift between editions as well \u2014 '
      + '<code>Agricultural</code> in one table and <code>Agricu- ltural</code> '
      + 'in the next, where a column header wrapped in the PDF.</p>'
      + '<p>Charting it needs a label crosswalk across the editions, the same '
      + 'work the budget documents need and the census districts needed. '
      + 'Inventing one here would produce a line that looks continuous and is '
      + 'not, so the table is downloadable and documented and this page does '
      + 'not draw it.</p>'
      + '<p><a href="/datasets/nepra-disco-annual/">Dataset page and field '
      + 'definitions</a></p>';
  }

  function renderPlants(mode) {
    state.mode = mode;
    if (mode === 'reports') return renderPlantsReports();
    var mw = d3.sum(plants, function (d) { return d.mw; });
    cover([plants.length + ' plants',
           Math.round(mw).toLocaleString() + ' MW installed',
           'nameplate capacity, not generation']);
    if (mode === 'largest') {
      var top = plants.slice(0, 18);
      return barChart(top, function (d) { return d.plant; }, function (d) { return d.mw; },
                      'Installed megawatts \u00b7 the 18 largest of ' + plants.length,
                      'Capacity a plant could produce, not what it does.',
                      function (d) { return d.technology + ' \u00b7 ' + d.fuel; });
    }
    var fam = Array.from(d3.rollup(plants,
      function (v) {
        return { mw: d3.sum(v, function (d) { return d.mw; }), n: v.length };
      }, function (d) { return d.family; }),
      function (e) { return { family: e[0], mw: e[1].mw, n: e[1].n }; })
      .sort(function (a, b) { return b.mw - a.mw; });
    barChart(fam, function (d) { return d.family; }, function (d) { return d.mw; },
             'Installed megawatts by fuel',
             'Nameplate capacity. The figure after each bar is how many plants it holds.',
             function (d) { return d.n + (d.n === 1 ? ' plant' : ' plants'); },
             function (d) { return Math.round(d.mw).toLocaleString() + ' MW \u00b7 ' + d.n; });
  }

  /* ── disaster alerts ─────────────────────────────────────────────────── */
  function renderEvents() {
    var rows = events.map(function (e) {
      return Object.assign({}, e, { year: +String(e.start).slice(0, 4) });
    }).filter(function (e) { return e.year; });
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
      .call(d3.axisLeft(y).tickFormat(function (h) {
        return h.replace(/_/g, ' ').replace(/^./, function (c) { return c.toUpperCase(); });
      })).attr('color', muted()).attr('font-size', 11);
    legend(svg, alerts, colour, W, { top: 2, right: 130 }, function (a) {
      return a + ' alert  ' + rows.filter(function (d) { return d.alert === a; }).length;
    });
    host.say('One circle per alert. GDACS grades how severe an event looks as it '
           + 'happens; it does not count losses.', '');
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

  var IMPACT_ORDER = ['deaths_total', 'injured_total', 'houses_destroyed',
    'houses_partially_damaged', 'livestock_perished', 'roads_damaged_km',
    'bridges_damaged'];

  function renderImpacts() {
    var nat = {};
    impacts.forEach(function (d) { if (d.level === 'country') nat[d.metric] = d.value; });
    cover([niceSpan(D.impacts.start, D.impacts.end),
           Math.round(nat.deaths_total) + ' deaths',
           Math.round(nat.injured_total) + ' injured',
           'cumulative for the season, not a daily count']);
    var metric = current.ind;
    var rows = impacts.filter(function (d) {
      return d.metric === metric && d.level !== 'country';
    }).sort(function (a, b) { return b.value - a.value; });
    barChart(rows, function (d) { return d.place; }, function (d) { return d.value; },
             (IMPACT_TITLE[metric] || IMPACT_LABEL[metric] || metric)
               + ' by province \u00b7 '
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
  function palette(domain) {
    return d3.scaleOrdinal().domain(domain)
      .range(['#0c3a1e', '#1e6b3e', '#b5860b', '#4d8a62', '#d4a017', '#7aa88c',
              '#886608', '#9dbfa9', '#0f6e78']);
  }
  function shortNum(v) {
    if (v >= 1e6) return (v / 1e6).toFixed(1) + 'm';
    if (v >= 1e3) return (v / 1e3).toFixed(0) + 'k';
    return String(Math.round(v));
  }
  /* The chart box is a lede, an svg and a foot. Captions live in the two divs
     rather than as svg <text>, because svg text does not wrap: on a phone the
     footnotes ran past the viewBox and were simply cut off. */
  function chartHost() {
    var host = $('chart');
    host.className = 'chart';
    host.innerHTML = '';
    var lede = document.createElement('div');
    lede.className = 'chart-lede';
    var foot = document.createElement('div');
    foot.className = 'chart-foot';
    host.appendChild(lede);
    var box = host.getBoundingClientRect();
    var W = Math.max(300, box.width), H = Math.max(280, box.height - 40);
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
  function axes(svg, x, y, m, W, H, fmt, label, log) {
    svg.append('g').attr('transform', 'translate(0,' + (H - m.bottom) + ')')
      .call(d3.axisBottom(x).ticks(6).tickFormat(d3.format('d')))
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
  function barChart(rows, keyOf, valOf, label, foot, subOf, labelOf) {
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
      .attr('height', y.bandwidth()).attr('fill', '#1e6b3e').attr('rx', 2)
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
    var m = fit({ top: 16, right: 150, bottom: 26, left: 58 }, W);
    var x = d3.scaleLinear().domain(d3.extent(years)).range([m.left, W - m.right]);
    var y;
    if (opt.log) {
      var lo = d3.min(rows, function (d) { return d[field] || undefined; });
      y = d3.scaleLog().domain([Math.max(1, lo * 0.8),
                                d3.max(rows, function (d) { return d[field]; })])
        .range([H - m.bottom, m.top]);
    } else {
      y = d3.scaleLinear()
        .domain([0, d3.max(rows, function (d) { return d[field]; })]).nice()
        .range([H - m.bottom, m.top]);
    }
    var colour = palette(keys);
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
        .attr('stroke', opt.colour ? opt.colour(k) : colour(k))
        .attr('stroke-width', 2)
        .attr('stroke-dasharray', opt.dash ? opt.dash(k) : null)
        .attr('d', line);
      if (opt.gaps) {
        svg.selectAll('circle.p-' + keys.indexOf(k))
          .data(series.filter(function (d) { return d[field] != null; }))
          .join('circle').attr('class', 'p-' + keys.indexOf(k))
          .attr('cx', function (d) { return x(d.year); })
          .attr('cy', function (d) { return y(d[field]); })
          .attr('r', 2.2).attr('fill', opt.colour ? opt.colour(k) : colour(k))
          .append('title').text(function (d) {
            return k + '\n' + d.year + ': ' + d[field].toLocaleString();
          });
      }
    });
    axes(svg, x, y, m, W, H, fmt, opt.axis || label, opt.log);
    host.say(opt.lede || '', opt.foot || '');
    legend(svg, keys, opt.colour ? d3.scaleOrdinal().domain(keys)
             .range(keys.map(opt.colour)) : colour, W, m, function (k) {
      var last = rows.filter(function (d) { return d.k === k; }).pop();
      return k + (opt.trail ? opt.trail(k) : '')
           + (last ? '  ' + fmt(last[field]) : '');
    });
  }

  /* ── the budget, and why it is not a chart ───────────────────────────── */
  function renderBudget() {
    var b = D.budget;
    $('coverage').innerHTML = '<b>Coverage</b>'
      + '<span>' + b.first + ' to ' + b.last + '</span>'
      + '<span>' + b.docs + ' budget documents</span>'
      + '<span>' + b.rows.toLocaleString() + ' lines</span>'
      + '<span>' + b.items.toLocaleString() + ' distinct item labels</span>';
    $('chart').className = 'chart panel';
    $('chart').innerHTML =
      '<div class="notice"><p><b>' + b.docs + ' years of budget documents are in the '
      + 'warehouse, and they are not yet a series.</b></p>'
      + '<p>The item labels drift between documents — <i>EXPENDITURE (I + II)</i> one '
      + 'year, <i>Current Exp. on Revenue Receipts</i> another, some carrying figures '
      + 'inside the label — so no item name spans more than five of the eighteen '
      + 'years. Charting them as they stand would draw a line that looks continuous '
      + 'and is not.</p>'
      + '<p>What it needs is an item crosswalk across the documents, checked the way '
      + 'the census districts were. Until then the table is published and queryable: '
      + '<a href="datasets/budget-lines/">budget_lines</a> in the catalogue, or '
      + '<a href="query.html">query it directly</a>.</p></div>';
    $('cardNote').textContent = '';
  }

  var CSV_NAME = {
    tax: 'fbr_tax_collection', courts: 'ljcp_case_flows',
    judges: 'ljcp_judicial_strength', crime: 'police_reported_offences',
    plants: 'nepra_power_plants', events: 'climate_events',
    offences: 'police_reported_offences_by_type',
    crimeDistricts: 'police_crime_by_district', impacts: 'ndma_monsoon_impacts',
  };

  function csvCell(v) {
    if (v === null || v === undefined) return '';
    v = String(v);
    return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
  }

  /* The CSV serves the block the current indicator actually draws, which is
     not always the block named after its topic. */
  var CSV_BLOCK = {
    crimeOffence: 'offences', crimeAjk: 'crimeDistricts',
    crimeKp: 'crimeDistricts', crimeForce: 'crime',
    sindhGroup: 'sindhCrime', sindhCategory: 'sindhCrime',
    sindhRange: 'sindhCrime', firsDaily: 'firs',
    impactsMetric: 'impacts', eventsTimeline: 'events',
    judgesTrend: 'judges', judgesComposition: 'judges',
    courtsPending: 'courts', courtsClearance: 'courts',
    courtsFlow: 'courts', courtsCategory: 'courts',
    taxStack: 'tax', taxLines: 'tax', taxShare: 'tax',
    plantsFuel: 'plants', plantsLargest: 'plants', plantsReports: 'plants',
  };

  function downloadCsv() {
    var name = CSV_BLOCK[current.chart];
    var block = name && D[name];
    if (!block) {
      window.location.href = '/datasets/'
        + current.ds.replace(/_/g, '-') + '/';
      return;
    }
    if (!block || !block.cols) return;
    var rows = [block.cols].concat(block.rows);
    var csv = rows.map(function (r) { return r.map(csvCell).join(','); }).join('\n');
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    a.download = 'data_darbar_' + current.ds + '.csv';
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  /* The tree and the rail are peers. A tree click sets the topic and takes
     that topic's first indicator; the rail then re-fills itself and reports
     the row it settled on, which is what gets drawn. The URL carries all
     three so a chart can be linked to. */
  function pick(topic) {
    var first = D.index.filter(function (r) { return r.topic === topic; })[0];
    if (!first) return;
    state.ds = first.ds;
    state.topic = first.topic;
    state.ind = first.ind;
    render(rail.sync(false));
    writeUrl();
  }

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
      levels: ['topic', 'ds', 'ind'],
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
