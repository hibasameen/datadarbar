/*
   state.js — the State explorer.

   Five themes, one with data. The page says which rather than showing four
   empty tabs: the Justice and policing pipelines exist under etl/ and publish
   nothing yet, and nothing has been collected on energy or weather disasters.
   A tab that looks live and is empty is worse than one labelled "not
   collected".
*/
(function () {
  'use strict';

  var D = window.DD_STATE;
  if (!D || !window.d3) return;

  var tax = D.tax.rows.map(function (r) {
    return { fy: r[0], fyEnd: r[1], type: r[2], head: r[3], mn: r[4] };
  });

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
    if (state.topic === 'budget') return renderBudget();
    renderTax();
  }

  /* ── what the state collects ─────────────────────────────────────────── */
  function renderTax() {
    var years = Array.from(new Set(tax.map(function (d) { return d.fyEnd; }))).sort(d3.ascending);
    $('coverage').innerHTML = '<b>Coverage</b>'
      + '<span>' + tax[0].fy + ' to ' + tax[tax.length - 1].fy + '</span>'
      + '<span>' + years.length + ' fiscal years</span>'
      + '<span>' + new Set(tax.map(function (d) { return d.head; })).size + ' heads</span>'
      + '<span style="margin-left:auto">Nominal rupees, not inflation-adjusted</span>';

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
    var host = $('chart');
    host.innerHTML = '';
    var box = host.getBoundingClientRect();
    var W = Math.max(320, box.width), H = Math.max(300, box.height);
    var m = { top: 12, right: 132, bottom: 26, left: 52 };
    var svg = d3.select(host).append('svg').attr('viewBox', '0 0 ' + W + ' ' + H);

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

  function downloadCsv() {
    if (state.topic !== 'tax') return;
    var rows = [['fy', 'fy_end', 'tax_type', 'head', 'collection_pkr_mn']];
    tax.forEach(function (d) { rows.push([d.fy, d.fyEnd, d.type, d.head, d.mn]); });
    var csv = rows.map(function (r) { return r.join(','); }).join('\n');
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    a.download = 'data_darbar_fbr_tax_collection.csv';
    a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); }, 1000);
  }

  function boot() {
    document.querySelectorAll('.tree-item[data-topic]').forEach(function (b) {
      b.onclick = function () { state.topic = b.dataset.topic; render(); };
    });
    $('csvBtn').onclick = downloadCsv;
    render();
    window.addEventListener('resize', function () {
      if (state.topic === 'tax') render();
    });
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
