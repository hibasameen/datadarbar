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

  var state = { topic: 'tax', mode: 'stack' };

  function $(id) { return document.getElementById(id); }

  function fmtTn(v) { return (v / 1e6).toFixed(1); }

  function render() {
    var t = TOPICS[state.topic];
    $('paneTheme').textContent = t.theme;
    $('paneTitle').textContent = t.title;
    $('paneDek').textContent = t.dek;
    $('cardNote').textContent = t.note;
    document.querySelectorAll('.tree-item[data-topic]').forEach(function (b) {
      b.classList.toggle('is-on', b.dataset.topic === state.topic);
    });
    var fn = {
      budget: renderBudget, courts: renderCourts, judges: renderJudges,
      crime: renderCrime, plants: renderPlants, events: renderEvents,
      impacts: renderImpacts,
    }[state.topic];
    (fn || renderTax)();
  }

  /* ── what the state collects ─────────────────────────────────────────── */
  function renderTax() {
    var years = Array.from(new Set(tax.map(function (d) { return d.fyEnd; }))).sort(d3.ascending);
    $('coverage').innerHTML = '<b>Coverage</b>'
      + '<span>' + tax[0].fy + ' to ' + tax[tax.length - 1].fy + '</span>'
      + '<span>' + years.length + ' fiscal years</span>'
      + '<span>' + new Set(tax.map(function (d) { return d.head; })).size + ' heads</span>'
      + '<span style="margin-left:auto">Nominal rupees, not inflation-adjusted</span>';

    if (['stack', 'lines', 'share'].indexOf(state.mode) < 0) state.mode = 'stack';
    $('cardControls').innerHTML =
      ['stack', 'lines'].map(function (m) {
        return '<button class="seg" type="button" data-mode="' + m + '" aria-pressed="'
             + (state.mode === m) + '">' + (m === 'stack' ? 'Stacked' : 'By head') + '</button>';
      }).join('')
      + '<button class="seg" type="button" data-mode="share" aria-pressed="'
      + (state.mode === 'share') + '">Share of total</button>';
    $('cardControls').querySelectorAll('.seg').forEach(function (b) {
      b.onclick = function () { state.mode = b.dataset.mode; render(); };
    });

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
  function renderCourts() {
    var rows = courts.filter(function (d) { return d.category === 'all'; });
    var provs = Array.from(new Set(rows.map(function (d) { return d.province; }))).sort();
    var years = Array.from(new Set(rows.map(function (d) { return d.year; }))).sort(d3.ascending);
    cover(['2020 to 2024', provs.length + ' provinces',
           'all courts', 'civil and criminal also available']);
    seg([['pending', 'Pending'], ['clearance', 'Clearance rate'],
         ['flow', 'Instituted vs disposed']]);

    var key = state.mode === 'clearance' ? 'clearance_pct' : 'pending_end';
    if (state.mode === 'flow') return flowChart(rows, provs, years);
    lineChart(rows, provs, years, key,
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
    seg(years.map(function (y) { return [String(y), String(y)]; }));
    var year = +state.mode;
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

  /* ── crime ───────────────────────────────────────────────────────────── */
  function renderCrime() {
    var regions = Array.from(new Set(crime.map(function (d) { return d.region; })));
    var forces = regions.filter(function (r) { return r !== 'Pakistan'; }).sort();
    var years = Array.from(new Set(crime.map(function (d) { return d.year; }))).sort(d3.ascending);
    var nat = crime.filter(function (d) { return d.region === 'Pakistan'; })
      .sort(function (a, b) { return a.year - b.year; });
    cover([years[0] + ' to ' + years[years.length - 1],
           forces.length + ' forces',
           shortNum(nat[nat.length - 1].value) + ' cases in '
             + nat[nat.length - 1].year,
           'reported offences, not crimes committed']);
    seg([['trend', 'By force'], ['offences', 'By offence'],
         ['districts', 'By district']]);
    if (state.mode === 'offences') return offenceChart(years);
    if (state.mode === 'districts') return districtCrime();

    var rows = crime.filter(function (d) { return d.region !== 'Pakistan'; })
      .map(function (d) { return { province: d.region, year: d.year, value: d.value }; });
    lineChart(rows, forces, years, 'value', 'reported cases, log scale', shortNum,
              { log: true,
                axis: 'reported cases, log scale',
                foot: crime.some(function (d) {
                        return d.own_value && d.own_value !== d.value;
                      })
                  ? '\u2020 the force\u2019s own yearbook gives a different total from '
                    + 'PBS for that year.'
                  : '',
                trail: function (k) {
                  var own = crime.filter(function (d) {
                    return d.region === k && d.own_value
                        && d.own_value !== d.value;
                  });
                  return own.length ? ' \u2020' : '';
                } });
  }

  function offenceChart(years) {
    var latest = d3.max(offences, function (d) { return d.year; });
    var rows = offences.filter(function (d) {
      return d.year === latest && d.region === 'Pakistan';
    }).sort(function (a, b) { return b.value - a.value; });
    barChart(rows, function (d) { return d.offence; }, function (d) { return d.value; },
             'Cases reported across Pakistan, ' + latest,
             'The eleven offences PBS names. They do not add to a force\u2019s total.');
  }

  function districtCrime() {
    var ajk = crimeDistricts.filter(function (d) {
      return d.measure === 'reported_crime_total';
    });
    var latest = d3.max(ajk, function (d) { return d.year; });
    var rows = ajk.filter(function (d) { return d.year === latest; })
      .sort(function (a, b) { return b.value - a.value; });
    barChart(rows, function (d) { return d.district; }, function (d) { return d.value; },
             'All reported crime by district, Azad Jammu & Kashmir, ' + latest,
             'The only force publishing a district total. Khyber Pakhtunkhwa '
               + 'publishes seven named offences for 37 districts, not a total.');
  }

  /* ── power plants ────────────────────────────────────────────────────── */
  function renderPlants() {
    var mw = d3.sum(plants, function (d) { return d.mw; });
    cover([plants.length + ' plants',
           Math.round(mw).toLocaleString() + ' MW installed',
           'nameplate capacity, not generation']);
    seg([['fuel', 'By fuel'], ['largest', 'Largest plants']]);

    if (state.mode === 'largest') {
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
    seg([]);
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
    seg([['deaths_total', 'Deaths'], ['injured_total', 'Injured'],
         ['houses_damaged_total', 'Houses damaged'],
         ['livestock_perished', 'Livestock']]);
    var metric = state.mode;
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
  function seg(modes) {
    if (!modes.length) { $('cardControls').innerHTML = ''; return; }
    if (!modes.some(function (m) { return m[0] === state.mode; })) state.mode = modes[0][0];
    $('cardControls').innerHTML = modes.map(function (m) {
      return '<button class="seg" type="button" data-mode="' + m[0]
           + '" aria-pressed="' + (state.mode === m[0]) + '">' + m[1] + '</button>';
    }).join('');
    $('cardControls').querySelectorAll('.seg').forEach(function (b) {
      b.onclick = function () { state.mode = b.dataset.mode; render(); };
    });
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
      row.append('rect').attr('width', 10).attr('height', 10).attr('rx', 2).attr('fill', colour(k));
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
      var lo = d3.min(rows, function (d) { return d[field]; });
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
      var series = rows.filter(function (d) { return d.province === k; })
        .sort(function (a, b) { return a.year - b.year; });
      if (!series.length) return;
      svg.append('path').datum(series).attr('fill', 'none')
        .attr('stroke', colour(k)).attr('stroke-width', 2)
        .attr('d', d3.line().x(function (d) { return x(d.year); })
                            .y(function (d) { return y(d[field]); }));
    });
    axes(svg, x, y, m, W, H, fmt, opt.axis || label, opt.log);
    host.say(opt.lede || '', opt.foot || '');
    legend(svg, keys, colour, W, m, function (k) {
      var last = rows.filter(function (d) { return d.province === k; }).pop();
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
    $('cardControls').innerHTML = '';
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

  var CSV_BLOCK = {
    offences: 'offences', districts: 'crimeDistricts',
  };

  function downloadCsv() {
    var name = state.topic === 'crime' && CSV_BLOCK[state.mode]
      ? CSV_BLOCK[state.mode] : state.topic;
    var block = D[name];
    if (!block || !block.cols) return;
    var rows = [block.cols].concat(block.rows);
    var csv = rows.map(function (r) { return r.map(csvCell).join(','); }).join('\n');
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    a.download = 'data_darbar_' + (CSV_NAME[name] || name) + '.csv';
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  function boot() {
    document.querySelectorAll('.tree-item[data-topic]').forEach(function (b) {
      b.onclick = function () {
        state.topic = b.dataset.topic;
        state.mode = state.topic === 'tax' ? 'stack' : '';
        render();
      };
    });
    $('csvBtn').onclick = downloadCsv;
    render();
    window.addEventListener('resize', function () {
      if (state.topic !== 'budget') render();
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
