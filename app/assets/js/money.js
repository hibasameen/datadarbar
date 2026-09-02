/* Data Darbar — Monetary & External
   Charts SBP monetary and external series. Payload: data/money_data.js (window.DD_MONEY).

   Conventions follow finance.js: full redraw on every change (no D3 update pattern),
   one global #tip tooltip, sidebar topics with #hash deep links, and 'all' present in
   TOPICS but deliberately absent from TOPIC_DRAWS.

   Two things specific to this page:
   - every multi-series chart has a chips row (series selection), and its end labels go
     through placeLabels(), which pushes colliding labels apart and adds a leader line;
   - the external balance is a two-sided treemap (dollars in / dollars out) whose panel
     widths are proportional to the totals, so a deficit is visible as a wider right box. */

/* ---------------- helpers ---------------- */
const tip = d3.select('#tip');
const showTip = (h, e) => { tip.html(h).style('opacity', 1); moveTip(e); };
const moveTip = e => { const p = 12; let x = e.clientX + p, y = e.clientY + p,
  w = tip.node().offsetWidth, hh = tip.node().offsetHeight;
  if (x + w > innerWidth) x = e.clientX - w - p;
  if (y + hh > innerHeight) y = e.clientY - hh - p;
  tip.style('left', x + 'px').style('top', y + 'px'); };
const hideTip = () => tip.style('opacity', 0);

const MON = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
const dt = s => new Date(s + 'T00:00:00');
const mLbl = s => MON[+s.slice(5, 7) - 1] + '-' + s.slice(0, 4);
const num = (v, d = 1) => v == null ? '—' : v.toLocaleString(undefined,
  { minimumFractionDigits: d, maximumFractionDigits: d });
const pct = (v, d = 1) => v == null ? '—' : (v >= 0 ? '+' : '') + v.toFixed(d) + '%';
const bn = v => v == null ? '—' : Math.abs(v) >= 1000
  ? (v / 1000).toFixed(1) + ' tn' : Math.round(v).toLocaleString() + ' bn';
const usdm = v => v == null ? '—' : (Math.abs(v) >= 1000 ? '$' + (v / 1000).toFixed(1) + 'bn' : '$' + Math.round(v) + 'm');
const fyOf = d => { const y = +d.slice(0, 4), m = +d.slice(5, 7); const f = m >= 7 ? y : y - 1;
  return `${f}-${String(f + 1).slice(2)}`; };

/* ---------------- series catalogue for the page ----------------
   key -> [label, colour]. Chart definitions below pick from these. */
const SER = {
  usd:['PKR per US$','#c0392b'], reer:['Real (REER)','#3d6db5'], neer:['Nominal (NEER)','#9ca3af'],
  cpi_nat:['National CPI','#0c3a1e'], cpi_urb:['Urban CPI','#3d6db5'], cpi_rur:['Rural CPI','#c9862b'],
  spi:['SPI (sensitive prices)','#7a5195'], wpi:['WPI (wholesale)','#9ca3af'],
  cpi_urbf:['Urban food','#3d6db5'], cpi_rurf:['Rural food','#22804a'],
  cpi_urbnf:['Urban non-food','#85b7eb'], cpi_rurnf:['Rural non-food','#97c459'],
  cpi_urbc:['Urban core','#185fa5'], cpi_rurc:['Rural core','#3b6d11'],
  pol_target:['Policy rate','#c0392b'], pol_rev:['Reverse repo (ceiling)','#5f5e5a'], pol_repo:['Repo (floor)','#9ca3af'],
  /* tenors run light->dark with maturity, but nothing lighter than mid-grey: the
     first cut used #d3d1c7 for 1 week and its label vanished against white */
  kib_1w:['1 week','#9ca3af'], kib_2w:['2 weeks','#888780'], kib_1m:['1 month','#5f5e5a'],
  kib_3m:['3 months','#85b7eb'], kib_6m:['6 months','#3d6db5'], kib_9m:['9 months','#185fa5'],
  kib_1y:['1 year','#c9862b'], kib_2y:['2 years','#993c1d'], kib_3y:['3 years','#c0392b'],
  lend:['Lending','#c0392b'], depo:['Deposits','#3d6db5'],
  res_sbp:['SBP','#186636'], res_banks:['Banks','#9ca3af'], res_gold:['Gold','#d4a017'], res_imf:['IMF position','#7a5195'],
  gx:['Goods exports','#22804a'], gm:['Goods imports','#c0392b'], sx:['Services exports','#97c459'],
  sm:['Services imports','#f09595'], ca:['Current account','#3d6db5'], remit_bop:['Remittances','#0f6e56'],
  m1:['M1','#c9862b'], m2:['M2','#186636'], m3:['M3','#3d6db5'], notes:['Notes in circulation','#9ca3af'],
  npl_ratio:['NPL ratio','#c0392b']
};
const lbl = k => SER[k] ? SER[k][0] : k, col = k => SER[k] ? SER[k][1] : '#888';

/* chart -> {avail: keys offered as chips, def: default selection} */
const CH = {
  reer:   { avail:['reer','neer'], def:['reer','neer'] },
  cpi:    { avail:['cpi_nat','cpi_urb','cpi_rur','spi','wpi'], def:['cpi_nat','cpi_urb','cpi_rur'] },
  food:   { avail:['cpi_urbf','cpi_rurf','cpi_urbnf','cpi_rurnf','cpi_urbc','cpi_rurc'], def:['cpi_urbf','cpi_rurf'] },
  policy: { avail:['pol_target','pol_rev','pol_repo'], def:['pol_target','pol_rev','pol_repo'] },
  kibor:  { avail:['kib_1w','kib_2w','kib_1m','kib_3m','kib_6m','kib_9m','kib_1y','kib_2y','kib_3y'],
            def:['kib_1w','kib_3m','kib_6m','kib_1y','kib_3y'] },
  spread: { avail:['lend','depo'], def:['lend','depo'] },
  res:    { avail:['res_sbp','res_banks','res_gold','res_imf'], def:['res_sbp','res_banks'] },
  bop:    { avail:['gx','gm','sx','sm','remit_bop','ca'], def:['gx','gm','sx','sm','ca'] },
  money:  { avail:['m1','m2','m3','notes'], def:['m1','m2','m3'] }
};
const SEL = {}; Object.keys(CH).forEach(c => SEL[c] = new Set(CH[c].def));
const isDefault = c => { const d = CH[c].def; return SEL[c].size === d.length && d.every(k => SEL[c].has(k)); };

let D, S, M, scale = 'log', fy = null, fys = [], bopView = 'tree';
let topic = 'rupee', applyingHash = false, _lastNavHash = '';

/* ---------------- topics ---------------- */
const TOPICS = [
 {k:'all', label:'Everything',
  desc:'Every topic on one page, top to bottom.',
  meta:'All series come from the State Bank of Pakistan’s EasyData portal. Full sources are listed at the foot of the page.'},
 {k:'rupee', label:'The rupee',
  desc:'The exchange rate since 1947, and whether the rupee is over-valued in real terms.',
  meta:'SBP bank floating average exchange rates (monthly, from Aug-1947) and the nominal/real effective exchange rate indices, base 2010 (from Jul-2001).'},
 {k:'prices', label:'Prices',
  desc:'Consumer price inflation, its components, and the gap between town and country.',
  meta:'PBS consumer price index (2015-16 base), sensitive price indicator and wholesale price index, distributed through SBP EasyData. Year-on-year, from Jul-2016.'},
 {k:'rates', label:'Interest rates',
  desc:'The policy rate, the interbank curve, and what banks pay and charge.',
  meta:'SBP structure of interest rates (policy rates from Jan-1956, KIBOR from Jun-2005) and weighted average lending and deposit rates (from Jan-2004).'},
 {k:'external', label:'External balance',
  desc:'Where the dollars come from and where they go; reserves; remittances.',
  meta:'SBP gold and FX reserves (from Jun-1948), the BPM6 monthly balance of payments summary (from Jul-2013), and country-wise workers’ remittances (from Jul-1972).'},
 {k:'money', label:'Money & banks',
  desc:'The money supply and the banking system’s bad loans.',
  meta:'SBP monetary aggregates M3 monthly profile (from Jun-2006) and segment-wise advances and non-performing loans (quarterly, to Jun-2025).'}];

const TOPIC_DRAWS = {
  rupee:    () => { drawUsd(); drawReer(); },
  prices:   () => { drawCpi(); drawFood(); },
  rates:    () => { drawPolicy(); drawKibor(); drawSpread(); },
  external: () => { drawRes(); drawBop(); drawRemit(); },
  money:    () => { drawMoney(); drawNpl(); }
};
const TOPIC_GROUPS = [
 {label:null, keys:['all']},
 {label:'Prices & the rupee', keys:['rupee','prices']},
 {label:'Rates & money', keys:['rates','money']},
 {label:'The outside world', keys:['external']}];
const drawAll = () => Object.values(TOPIC_DRAWS).forEach(f => f());

/* ---------------- chart scaffolding ---------------- */
function frame(sel, hFrac, m, fallbackH) {
  const el = d3.select(sel); el.selectAll('*').remove();
  const W = el.node() ? (el.node().clientWidth || 1100) : 1100;
  const H = fallbackH || Math.max(260, Math.min(360, W * hFrac));
  const svg = el.append('svg').attr('width', W).attr('height', H).style('display', 'block');
  return { el, W, H, m, svg, labels: [] };
}
const xTime = (f, dom) => d3.scaleTime().domain(dom).range([f.m.l, f.W - f.m.r]);
function axes(f, x, y, yFmt, xTicks) {
  f.svg.append('g').attr('transform', `translate(0,${f.H - f.m.b})`).attr('class', 'axis')
    .call(d3.axisBottom(x).ticks(xTicks || Math.floor((f.W - f.m.r) / 90)));
  f.svg.append('g').attr('transform', `translate(${f.m.l},0)`).attr('class', 'axis')
    .call(d3.axisLeft(y).ticks(6).tickFormat(yFmt || null))
    .call(g => g.selectAll('.tick line').clone().attr('x2', f.W - f.m.r - f.m.l).attr('class', 'gl'));
}
const zeroLine = (f, x, y) => f.svg.append('line').attr('x1', f.m.l).attr('x2', f.W - f.m.r)
  .attr('y1', y(0)).attr('y2', y(0)).attr('stroke', 'var(--slate-300)');
const linePath = (f, pts, x, y, colour, w, dash) => f.svg.append('path').datum(pts)
  .attr('fill', 'none').attr('stroke', colour).attr('stroke-width', w || 2)
  .attr('stroke-dasharray', dash || null)
  .attr('d', d3.line().defined(p => p[1] != null).x(p => x(dt(p[0]))).y(p => y(p[1])));

/* Queue an end-of-line label; placeLabels() lays them all out at the end. */
function endLabel(f, x, y, p, colour, text) {
  if (!p) return;
  f.labels.push({ x: x(dt(p[0])), y: y(p[1]), colour, text });
}
/* Lay out the end-of-line labels so none overlap.

   One-dimensional dodge: sort by y, sweep forward pushing each label at least GAP
   below the previous one, then re-centre the whole stack on where the line ends
   actually are. Without the re-centring every label in a cluster gets pushed
   downward and the leaders fan out below the lines like a plunge in the data —
   which is exactly how the first version read. Leaders are faint elbows so they
   register as annotation, not as series. */
function placeLabels(f) {
  const L = f.labels; if (!L.length) return;
  const GAP = 13, top = f.m.t + 6, bot = f.H - f.m.b - 4;
  L.sort((a, b) => a.y - b.y);
  L.forEach(l => l.ty = l.y);
  for (let i = 1; i < L.length; i++) if (L[i].ty - L[i - 1].ty < GAP) L[i].ty = L[i - 1].ty + GAP;
  const shift = d3.mean(L, l => l.ty) - d3.mean(L, l => l.y);         // re-centre on the cluster
  L.forEach(l => l.ty -= shift);
  const over = L[L.length - 1].ty - bot, under = top - L[0].ty;         // then respect the plot edges
  if (over > 0) L.forEach(l => l.ty -= over);
  if (under > 0) L.forEach(l => l.ty += under);
  L.forEach(l => {
    const lx = l.x + 3, tx = l.x + 9;
    if (Math.abs(l.ty - l.y) > 2.5)
      f.svg.append('path').attr('fill', 'none').attr('stroke', l.colour).attr('stroke-width', .9).attr('opacity', .45)
        .attr('d', `M${lx},${l.y} H${lx + 3} V${l.ty} H${tx - 2}`);
    f.svg.append('text').attr('x', tx).attr('y', l.ty + 4).attr('font-size', 11)
      .attr('font-weight', 700).attr('fill', l.colour).text(l.text);
  });
}
/* Transparent overlay reporting the nearest date on mousemove. */
function hoverLayer(f, x, dates, render) {
  f.svg.append('rect').attr('x', f.m.l).attr('y', f.m.t)
    .attr('width', Math.max(0, f.W - f.m.r - f.m.l)).attr('height', Math.max(0, f.H - f.m.b - f.m.t))
    .attr('fill', 'transparent')
    .on('mousemove', function (e) {
      const mx = x.invert(d3.pointer(e, this)[0]);
      let best = null, bd = Infinity;
      dates.forEach(d => { const gap = Math.abs(dt(d) - mx); if (gap < bd) { bd = gap; best = d; } });
      if (best) showTip(render(best), e);
    })
    .on('mouseleave', hideTip);
}
const nodata = (sel, msg) => { const el = d3.select(sel); el.selectAll('*').remove();
  el.append('div').attr('class', 'nodata').text(msg); };
const at = (k, d) => { const p = (S[k] || []).find(q => q[0] === d); return p ? p[1] : null; };
const fmtPct = v => v == null ? '—' : v.toFixed(2) + '%';

/* ---------------- chips (series selection) ---------------- */
function chips(chart, onChange) {
  const box = d3.select('#chips-' + chart); if (box.empty()) return;
  box.selectAll('*').remove();
  CH[chart].avail.forEach(k => {
    const on = SEL[chart].has(k);
    box.append('button').attr('class', 'chip' + (on ? ' on' : '')).attr('data-k', k)
      .style('background', on ? col(k) : null).style('border-color', on ? col(k) : null)
      .html(`<span style="display:inline-block;width:8px;height:8px;border-radius:2px;background:${on ? '#fff' : col(k)};margin-right:6px;vertical-align:middle"></span>${lbl(k)}`)
      .on('click', () => {
        if (on && SEL[chart].size === 1) return;          // never empty a chart
        if (on) SEL[chart].delete(k); else SEL[chart].add(k);
        onChange(); writeHash(false);
      });
  });
}
/* Generic multi-line time chart used by most panels. */
function multiLine(chart, sel, opts) {
  const keys = CH[chart].avail.filter(k => SEL[chart].has(k) && S[k] && S[k].length);
  if (!keys.length) return nodata(sel, 'Pick at least one series.');
  const f = frame(sel, opts.hFrac || 0.27, opts.m || { t: 16, r: 110, b: 28, l: 46 });
  const all = keys.flatMap(k => S[k]);
  const x = xTime(f, d3.extent(all, p => dt(p[0])));
  const lo = opts.zero === false ? d3.min(all, p => p[1]) * 0.94 : Math.min(0, d3.min(all, p => p[1]));
  const y = d3.scaleLinear().domain([lo, d3.max(all, p => p[1]) * 1.08]).nice().range([f.H - f.m.b, f.m.t]);
  axes(f, x, y, opts.yFmt);
  if (opts.refLine != null) {
    f.svg.append('line').attr('x1', f.m.l).attr('x2', f.W - f.m.r).attr('y1', y(opts.refLine)).attr('y2', y(opts.refLine))
      .attr('stroke', 'var(--slate-300)').attr('stroke-dasharray', '3 3');
    if (opts.refText) f.svg.append('text').attr('x', f.m.l + 4).attr('y', y(opts.refLine) - 5)
      .attr('font-size', 10).attr('fill', 'var(--slate-400)').text(opts.refText);
  } else if (lo < 0) zeroLine(f, x, y);
  if (opts.before) opts.before(f, x, y, keys);
  keys.forEach(k => {
    linePath(f, S[k], x, y, col(k), opts.emph && opts.emph === k ? 2.6 : 1.8);
    endLabel(f, x, y, S[k][S[k].length - 1], col(k), lbl(k));
  });
  placeLabels(f);
  const dates = [...new Set(all.map(p => p[0]))].sort();
  hoverLayer(f, x, dates, d => `<b>${mLbl(d)}</b><br>` +
    keys.map(k => `${lbl(k)} ${opts.fmt ? opts.fmt(at(k, d)) : num(at(k, d), 2)}`).join('<br>'));
  return f;
}

/* ---------------- charts: the rupee ---------------- */
function drawUsd() {
  const pts = S.usd; if (!pts) return nodata('#chUsd', 'No exchange-rate data.');
  const f = frame('#chUsd', 0.30, { t: 16, r: 92, b: 28, l: 52 });
  const x = xTime(f, d3.extent(pts, p => dt(p[0])));
  const vals = pts.map(p => p[1]);
  const y = (scale === 'log'
      ? d3.scaleLog().domain([d3.min(vals) * 0.9, d3.max(vals) * 1.15])
      : d3.scaleLinear().domain([0, d3.max(vals) * 1.08]).nice())
    .range([f.H - f.m.b, f.m.t]);
  axes(f, x, y, d => scale === 'log' ? (d >= 1 ? d3.format('~s')(d) : d) : d3.format('~s')(d));
  linePath(f, pts, x, y, col('usd'), 2);
  const last = pts[pts.length - 1];
  endLabel(f, x, y, last, col('usd'), 'Rs ' + num(last[1], 0)); placeLabels(f);
  hoverLayer(f, x, pts.map(p => p[0]), d => `<b>${mLbl(d)}</b><br>Rs ${num(at('usd', d), 2)} per US$`);
}
function drawReer() {
  chips('reer', drawReer);
  multiLine('reer', '#chReer', { zero: false, refLine: 100, refText: '2010 = 100', fmt: v => num(v, 1),
    m: { t: 16, r: 120, b: 28, l: 44 } });
}

/* ---------------- charts: prices ---------------- */
function drawCpi() {
  chips('cpi', drawCpi);
  multiLine('cpi', '#chCpi', { yFmt: d => d + '%', fmt: fmtPct, emph: 'cpi_nat', m: { t: 16, r: 140, b: 28, l: 44 } });
}
function drawFood() {
  chips('food', drawFood);
  multiLine('food', '#chFood', { yFmt: d => d + '%', fmt: fmtPct, m: { t: 16, r: 120, b: 28, l: 44 },
    /* shade the urban-rural food gap when both are shown; green = rural worse */
    before: (f, x, y, keys) => {
      if (!(keys.includes('cpi_urbf') && keys.includes('cpi_rurf'))) return;
      const u = {}; S.cpi_urbf.forEach(p => u[p[0]] = p[1]);
      const pair = S.cpi_rurf.filter(p => u[p[0]] != null).map(p => ({ d: p[0], u: u[p[0]], r: p[1] }));
      f.svg.append('path').datum(pair).attr('fill', col('cpi_rurf')).attr('opacity', .14)
        .attr('d', d3.area().x(p => x(dt(p.d))).y0(p => y(p.u)).y1(p => y(p.r)));
    } });
}

/* ---------------- charts: rates ---------------- */
function drawPolicy() {
  chips('policy', drawPolicy);
  const keys = CH.policy.avail.filter(k => SEL.policy.has(k) && S[k]);
  if (!keys.length) return nodata('#chPolicy', 'Pick at least one series.');
  const f = frame('#chPolicy', 0.28, { t: 16, r: 150, b: 28, l: 44 });
  const all = keys.flatMap(k => S[k]);
  const x = xTime(f, d3.extent(all, p => dt(p[0])));
  const y = d3.scaleLinear().domain([0, d3.max(all, p => p[1]) * 1.08]).nice().range([f.H - f.m.b, f.m.t]);
  axes(f, x, y, d => d + '%');
  /* as-needed data: the rate holds until it is changed, so step, never interpolate */
  const step = d3.line().curve(d3.curveStepAfter).x(p => x(dt(p[0]))).y(p => y(p[1]));
  const today = all.reduce((a, p) => p[0] > a ? p[0] : a, '');
  keys.forEach(k => {
    const pts = S[k].concat([[today, S[k][S[k].length - 1][1]]]);   // hold the last rate to the latest date
    f.svg.append('path').datum(pts).attr('fill', 'none').attr('stroke', col(k))
      .attr('stroke-width', k === 'pol_target' ? 2.6 : 1.6).attr('d', step);
    endLabel(f, x, y, [today, S[k][S[k].length - 1][1]], col(k), lbl(k));
  });
  placeLabels(f);
  const held = (k, d) => { let v = null; for (const p of S[k] || []) { if (p[0] <= d) v = p[1]; else break; } return v; };
  hoverLayer(f, x, [...new Set(all.map(p => p[0]))].sort(), d => `<b>${mLbl(d)}</b><br>` +
    keys.map(k => `${lbl(k)} ${fmtPct(held(k, d))}`).join('<br>'));
}
function drawKibor() {
  chips('kibor', drawKibor);
  multiLine('kibor', '#chKibor', { yFmt: d => d + '%', fmt: fmtPct, m: { t: 16, r: 90, b: 28, l: 44 } });
}
function drawSpread() {
  chips('spread', drawSpread);
  multiLine('spread', '#chSpread', { yFmt: d => d + '%', fmt: fmtPct, m: { t: 16, r: 90, b: 28, l: 44 },
    before: (f, x, y, keys) => {
      if (!(keys.includes('lend') && keys.includes('depo'))) return;
      const l = {}; S.lend.forEach(p => l[p[0]] = p[1]);
      const pair = S.depo.filter(p => l[p[0]] != null).map(p => ({ d: p[0], l: l[p[0]], dp: p[1] }));
      f.svg.append('path').datum(pair).attr('fill', col('lend')).attr('opacity', .10)
        .attr('d', d3.area().x(p => x(dt(p.d))).y0(p => y(p.dp)).y1(p => y(p.l)));
    } });
}

/* ---------------- charts: external ---------------- */
function importCover() {
  /* months of goods imports the SBP's reserves would pay for, on a trailing
     12-month average of imports (a single month is far too noisy) */
  if (!S.res_sbp || !S.gm) return [];
  const imp = {}; S.gm.forEach(p => imp[p[0]] = p[1]);
  const dates = S.gm.map(p => p[0]).sort(), roll = {};
  dates.forEach((d, i) => {
    const win = dates.slice(Math.max(0, i - 11), i + 1).map(k => imp[k]).filter(v => v != null);
    if (win.length >= 6) roll[d] = win.reduce((a, b) => a + b, 0) / win.length;
  });
  return S.res_sbp.filter(p => roll[p[0]]).map(p => [p[0], p[1] / roll[p[0]]]);
}
const usdBn = d => '$' + (d / 1000) + 'bn';
function drawRes() {
  chips('res', drawRes);
  const cover = importCover();
  /* the import-cover line gets its own right-hand axis rather than borrowing the
     dollar one — a reader looking at "3 months" against a $-scale is being misled */
  multiLine('res', '#chRes', { yFmt: usdBn, fmt: v => usdm(v), hFrac: 0.30,
    m: { t: 16, r: 150, b: 28, l: 56 },
    before: (f, x, y) => {
      if (!cover.length) return;
      const y2 = d3.scaleLinear().domain([0, Math.max(9, d3.max(cover, p => p[1]))]).nice().range([f.H - f.m.b, f.m.t]);
      const ax = f.svg.append('g').attr('transform', `translate(${f.W - f.m.r + 44},0)`).attr('class', 'axis')
        .call(d3.axisRight(y2).ticks(5).tickFormat(d => d + ' m'));
      ax.selectAll('text').attr('fill', '#b8860b');
      ax.select('.domain').attr('stroke', '#e8b92e');
      f.svg.append('text').attr('x', f.W - f.m.r + 44).attr('y', f.m.t - 4).attr('font-size', 9.5)
        .attr('fill', '#b8860b').text('months of imports');
      f.svg.append('path').datum(cover).attr('fill', 'none').attr('stroke', '#d4a017')
        .attr('stroke-width', 1.6).attr('stroke-dasharray', '4 3')
        .attr('d', d3.line().x(p => x(dt(p[0]))).y(p => y2(p[1])));
      f.svg.append('line').attr('x1', x(dt(cover[0][0]))).attr('x2', f.W - f.m.r).attr('y1', y2(3)).attr('y2', y2(3))
        .attr('stroke', '#d4a017').attr('opacity', .5).attr('stroke-dasharray', '2 4');
      f.svg.append('text').attr('x', x(dt(cover[0][0])) + 4).attr('y', y2(3) - 4).attr('font-size', 9.5)
        .attr('fill', '#b8860b').text('3 months’ cover');
      endLabel(f, x, y2, cover[cover.length - 1], '#b8860b', cover[cover.length - 1][1].toFixed(1) + ' mths');
    } });
}

/* The balance of payments, two ways: a two-sided treemap of one fiscal year, or the
   monthly trend. Treemap panel widths are proportional to totals, so the deficit is
   literally the extra width of the right-hand box. */
const CREDITS = [['gx', 'Goods exports'], ['sx', 'Services exports'], ['remit_bop', 'Remittances'],
                 ['sic_other', 'Other transfers in'], ['pic', 'Income received']];
const DEBITS  = [['gm', 'Goods imports'], ['sm', 'Services imports'], ['pid', 'Income paid abroad'],
                 ['sid', 'Transfers out']];
const CRED_COL = { gx:'#186636', sx:'#3b6d11', remit_bop:'#0f6e56', sic_other:'#5dcaa5', pic:'#97c459' };
const DEB_COL  = { gm:'#a32d2d', sm:'#d85a30', pid:'#e24b4a', sid:'#f09595' };
const fySum = (k, f) => (S[k] || []).filter(p => fyOf(p[0]) === f).reduce((a, p) => a + p[1], 0);
const bopMonths = f => (D.bop_months && D.bop_months[f]) || 0;

/* Drill-down. D.drill.nodes[k] holds a tree under each top-level tile (goods to
   four levels, services by type, remittances by source, income paid by sector);
   node values are FY totals aligned to D.drill.fys. bopPath = [] is the two-sided
   overview; ['gx', 1, 3] is goods exports > 2nd group > 4th member. Hash: bz=gx.1.3 */
let bopPath = [];
const TOP_NAME = Object.fromEntries([...CREDITS, ...DEBITS]);
const drillRoot = k => D.drill && D.drill.nodes && D.drill.nodes[k];
function drillNode(path) {
  let nd = drillRoot(path[0]);
  for (const i of path.slice(1)) nd = nd && nd.ch && nd.ch[i];
  return nd;
}
const fyIdx = () => D.drill ? D.drill.fys.indexOf(fy) : -1;
const nodeVal = (nd, i) => (nd.v && nd.v[i] != null) ? nd.v[i] : null;

/* one treemap panel; items are {n, v, has (children?), onClick, colour} */
function tilePanel(svg, items, x0, y0, w, h, title, total, subtitle, dark) {
  const root = d3.hierarchy({ children: items }).sum(d => d.v).sort((a, b) => b.value - a.value);
  d3.treemap().size([w, h]).paddingInner(3).paddingOuter(0).round(true)(root);
  const g = svg.append('g').attr('transform', `translate(${x0},${y0})`);
  if (title) {
    svg.append('text').attr('x', x0).attr('y', 16).attr('font-size', 13).attr('font-weight', 800)
      .attr('fill', 'var(--green-900)').text(title);
    svg.append('text').attr('x', x0).attr('y', 32).attr('font-size', 11.5).attr('font-weight', 600)
      .attr('fill', 'var(--slate-500)').text(subtitle);
  }
  const cell = g.selectAll('g').data(root.leaves()).join('g').attr('transform', d => `translate(${d.x0},${d.y0})`);
  cell.append('rect').attr('width', d => d.x1 - d.x0).attr('height', d => d.y1 - d.y0).attr('rx', 3)
    .attr('fill', d => d.data.colour).attr('stroke', '#fff').attr('stroke-width', 1)
    .attr('class', d => d.data.has ? 'zoom' : null)
    .on('mousemove', (e, d) => showTip(`<b>${d.data.n}</b><br>${usdm(d.data.v)} in ${fy}<br>${(100 * d.data.v / total).toFixed(1)}% of ${(title || 'this').toLowerCase()}`
      + (d.data.has ? '<br><i>Click to break down</i>' : ''), e))
    .on('mouseleave', hideTip)
    .on('click', (e, d) => { if (d.data.has) { hideTip(); d.data.onClick(); } });
  /* text is clipped to its tile: a width estimate is only ever an estimate, and the
     first cut let "Services imports" run past its box */
  cell.each(function (d, i) {
    const w = d.x1 - d.x0, h = d.y1 - d.y0, c = d3.select(this);
    if (w < 58 || h < 30) return;
    const id = `clip-${(title || 'z').replace(/\W/g, '')}-${i}`;
    c.append('clipPath').attr('id', id).append('rect').attr('width', Math.max(0, w - 6)).attr('height', h);
    const t = c.append('g').attr('clip-path', `url(#${id})`).style('pointer-events', 'none');
    const fg = dark(d.data.colour) ? '#fff' : '#1a1a1a', fg2 = dark(d.data.colour) ? 'rgba(255,255,255,.85)' : 'rgba(0,0,0,.65)';
    /* conservative width estimate; drop trailing words until it fits, so "Other
       exports (land-borne…)" shortens to "Other exports", not to "Other" */
    const words = d.data.n.replace(/\s*\(.*$/, '').split(' ');
    let label = d.data.n;
    for (let k = words.length; k >= 1 && label.length * 8.4 > w - 12; k--) label = words.slice(0, k).join(' ');
    t.append('text').attr('x', 7).attr('y', 16).attr('class', 'cl').attr('fill', fg).text(label);
    if (h > 44) t.append('text').attr('x', 7).attr('y', 31).attr('class', 'cv').attr('fill', fg2)
      .text(usdm(d.data.v) + (w > 130 ? ` · ${(100 * d.data.v / total).toFixed(0)}%` : ''));
    if (d.data.has && w > 40 && h > 20) t.append('text').attr('x', w - 9).attr('y', h - 6).attr('text-anchor', 'end')
      .attr('font-size', 11).attr('fill', fg2).text('▸');
  });
}
const isDark = c => d3.hsl(c).l < 0.62;

function drawBop() {
  d3.selectAll('#bopSeg button').classed('on', function () { return this.dataset.bv === bopView; });
  d3.select('#chips-bop').style('display', bopView === 'trend' ? null : 'none');
  d3.select('#bopCrumb').style('display', 'none');
  if (bopView === 'trend') return drawBopTrend();
  const el = d3.select('#chBop'); el.selectAll('*').remove();
  if (!bopMonths(fy)) return nodata('#chBop', `No balance-of-payments detail for ${fy} — the BPM6 monthly series begins in July 2013.`);
  const partial = bopMonths(fy) < 12;
  d3.select('#bopNote').style('display', partial ? null : 'none')
    .text(`${fy} covers ${bopMonths(fy)} month${bopMonths(fy) === 1 ? '' : 's'} so far — not comparable with full years.`);
  if (bopPath.length && drillNode(bopPath)) return drawBopZoom(el);
  bopPath = [];
  const cred = CREDITS.map(([k, n]) => ({ k, n, v: k === 'sic_other' ? fySum('sic', fy) - fySum('remit_bop', fy) : fySum(k, fy),
    colour: CRED_COL[k], has: !!drillRoot(k), onClick: () => zoomTo([k]) })).filter(d => d.v > 0);
  const deb = DEBITS.map(([k, n]) => ({ k, n, v: fySum(k, fy), colour: DEB_COL[k], has: !!drillRoot(k),
    onClick: () => zoomTo([k]) })).filter(d => d.v > 0);
  const tc = d3.sum(cred, d => d.v), td = d3.sum(deb, d => d.v), bal = tc - td;

  const W = el.node().clientWidth || 1100, H = Math.max(300, Math.min(400, W * 0.34));
  const gap = 34, hdr = 42;
  const wc = Math.round((W - gap) * tc / (tc + td)), wd = W - gap - wc;
  const svg = el.append('svg').attr('width', W).attr('height', H).style('display', 'block');
  const sub = t => usdm(t) + (partial ? ` · ${bopMonths(fy)} months` : '');
  tilePanel(svg, cred, 0, hdr, wc, H - hdr, 'Dollars in', tc, sub(tc), isDark);
  tilePanel(svg, deb, wc + gap, hdr, wd, H - hdr, 'Dollars out', td, sub(td), isDark);
  /* the balance, in the gap */
  const gx = wc + gap / 2;
  svg.append('line').attr('x1', gx).attr('x2', gx).attr('y1', hdr).attr('y2', H).attr('stroke', 'var(--slate-200)');
  const bt = svg.append('text').attr('x', gx).attr('y', H / 2 + hdr / 2).attr('text-anchor', 'middle')
    .attr('transform', `rotate(-90 ${gx} ${H / 2 + hdr / 2})`).attr('font-size', 11).attr('font-weight', 800)
    .attr('fill', bal < 0 ? '#a32d2d' : '#186636');
  bt.text(`${bal < 0 ? 'Deficit' : 'Surplus'} ${usdm(Math.abs(bal))}`);
}

function zoomTo(path) { bopPath = path; drawBop(); writeHash(true); }

function drawBopZoom(el) {
  const k = bopPath[0], nd = drillNode(bopPath), fi = fyIdx();
  const base = CRED_COL[k] || DEB_COL[k];
  const items = (nd.ch || []).map((c, i) => ({ i, n: c.n, v: nodeVal(c, fi), has: !!(c.ch && c.ch.length),
    onClick: () => zoomTo([...bopPath, i]) })).filter(d => d.v > 0).sort((a, b) => b.v - a.v);
  /* breadcrumb: every level above the current one is a link */
  const crumb = d3.select('#bopCrumb').style('display', null).html('');
  const names = [['Current account', []]];
  bopPath.forEach((p, i) => names.push([i === 0 ? TOP_NAME[p] : drillNode(bopPath.slice(0, i + 1)).n, bopPath.slice(0, i + 1)]));
  names.forEach(([n, path], i) => {
    if (i) crumb.append('span').attr('class', 'sep').text('›');
    if (i < names.length - 1) crumb.append('a').text(n).on('click', () => zoomTo(path));
    else crumb.append('span').attr('class', 'here').text(n);
  });
  const root = drillRoot(k);
  if (bopPath.length === 1 && root.basis) crumb.append('span').attr('class', 'basis').text('Basis: ' + root.basis);
  if (!items.length) return nodata('#chBop', `No breakdown of ${names[names.length - 1][0].toLowerCase()} for ${fy}.`);

  const total = d3.sum(items, d => d.v);
  items.forEach((d, i) => { d.colour = d3.interpolateRgb(base, '#fff')(items.length === 1 ? 0 : 0.6 * i / (items.length - 1)); });
  const W = el.node().clientWidth || 1100, H = Math.max(300, Math.min(420, W * 0.36)), hdr = 42;
  const svg = el.append('svg').attr('width', W).attr('height', H).style('display', 'block');
  const parentV = bopPath.length === 1 ? fySum(k === 'sic_other' ? 'sic' : k, fy) - (k === 'sic_other' ? fySum('remit_bop', fy) : 0)
    : nodeVal(drillNode(bopPath), fi);
  let sub = usdm(total) + (bopMonths(fy) < 12 ? ` · ${bopMonths(fy)} months` : '');
  if (bopPath.length === 1 && root.adj && root.adj.v[fi] != null && Math.abs(total - parentV) > 1)
    sub += ` before ${root.adj.n.toLowerCase()} of ${usdm(root.adj.v[fi])}; ${usdm(parentV)} in the balance of payments`;
  tilePanel(svg, items, 0, hdr, W, H - hdr, names[names.length - 1][0], total, sub, isDark);
  svg.append('text').attr('x', W).attr('y', 16).attr('text-anchor', 'end').attr('font-size', 11.5).attr('font-weight', 600)
    .attr('fill', 'var(--green-700)').style('cursor', 'pointer').text('← back')
    .on('click', () => zoomTo(bopPath.slice(0, -1)));
}
function drawBopTrend() {
  chips('bop', drawBopTrend);
  d3.select('#bopNote').style('display', 'none');
  multiLine('bop', '#chBop', { yFmt: usdBn, fmt: v => usdm(v), hFrac: 0.30,
    m: { t: 16, r: 130, b: 28, l: 56 } });
}

function drawRemit() {
  if (!D.remit || !D.remit.length) return nodata('#chRemit', 'No remittance data.');
  const rows = D.remit.filter(r => r[1] === fy).sort((a, b) => b[2] - a[2]);
  if (!rows.length) return nodata('#chRemit', 'No remittances for ' + fy + '.');
  d3.select('#remitNote').style('display', fyPartial(fy) ? null : 'none')
    .text(`${fy} is not a complete year — these totals cover ${fyMonths(fy)} month` +
          `${fyMonths(fy) === 1 ? '' : 's'} only, so they are not comparable with earlier years.`);
  const el = d3.select('#chRemit'); el.selectAll('*').remove();
  const W = el.node().clientWidth || 1100;
  const H = Math.max(240, rows.length * 24 + 40), m = { t: 10, r: 96, b: 24, l: 190 };
  const svg = el.append('svg').attr('width', W).attr('height', H).style('display', 'block');
  const x = d3.scaleLinear().domain([0, d3.max(rows, r => r[2]) * 1.06]).range([m.l, W - m.r]);
  const y = d3.scaleBand().domain(rows.map(r => r[0])).range([m.t, H - m.b]).padding(.24);
  svg.append('g').attr('transform', `translate(0,${H - m.b})`).attr('class', 'axis')
    .call(d3.axisBottom(x).ticks(Math.floor((W - m.r) / 110)).tickFormat(d3.format('~s')));
  svg.append('g').attr('transform', `translate(${m.l},0)`).attr('class', 'axis').call(d3.axisLeft(y));
  const tot = d3.sum(rows, r => r[2]);
  svg.selectAll('rect.b').data(rows).join('rect').attr('class', 'b')
    .attr('x', m.l).attr('y', r => y(r[0])).attr('height', y.bandwidth())
    .attr('width', r => Math.max(0, x(r[2]) - m.l)).attr('fill', '#186636').attr('rx', 3)
    .on('mousemove', (e, r) => showTip(`<b>${r[0]}</b><br>${usdm(r[2])} in ${r[1]}<br>${(100 * r[2] / tot).toFixed(1)}% of total`, e))
    .on('mouseleave', hideTip);
  svg.selectAll('text.v').data(rows).join('text').attr('class', 'v cv')
    .attr('x', r => x(r[2]) + 6).attr('y', r => y(r[0]) + y.bandwidth() / 2 + 3)
    .attr('fill', 'var(--slate-500)').text(r => usdm(r[2]));
}

/* ---------------- charts: money & banks ---------------- */
function drawMoney() {
  chips('money', drawMoney);
  multiLine('money', '#chMoney', { yFmt: d => d3.format('~s')(d / 1000), fmt: v => 'Rs ' + bn(v / 1000),
    m: { t: 16, r: 130, b: 28, l: 58 },
    before: f => f.svg.append('text').attr('x', f.m.l).attr('y', f.m.t - 4).attr('font-size', 10)
      .attr('fill', 'var(--slate-400)').text('Rs trillion') });
}
function drawNpl() {
  if (!S.npl_ratio) return nodata('#chNpl', 'No non-performing loan data.');
  const f = frame('#chNpl', 0.24, { t: 16, r: 70, b: 28, l: 44 });
  const pts = S.npl_ratio;
  const x = xTime(f, d3.extent(pts, p => dt(p[0])));
  const y = d3.scaleLinear().domain([0, d3.max(pts, p => p[1]) * 1.12]).nice().range([f.H - f.m.b, f.m.t]);
  axes(f, x, y, d => d + '%');
  f.svg.append('path').datum(pts).attr('fill', col('npl_ratio')).attr('opacity', .12)
    .attr('d', d3.area().x(p => x(dt(p[0]))).y0(y(0)).y1(p => y(p[1])));
  linePath(f, pts, x, y, col('npl_ratio'), 2.2);
  endLabel(f, x, y, pts[pts.length - 1], col('npl_ratio'), pts[pts.length - 1][1].toFixed(1) + '%'); placeLabels(f);
  const lvl = {}; (S.npl_level || []).forEach(p => lvl[p[0]] = p[1]);
  hoverLayer(f, x, pts.map(p => p[0]), d => `<b>${mLbl(d)}</b><br>${at('npl_ratio', d).toFixed(2)}% of gross advances` +
    (lvl[d] ? `<br>Rs ${bn(lvl[d] / 1000)} outstanding` : ''));
}

/* ---------------- KPIs ---------------- */
function kpi(l, val, sub, cls) {
  return `<div class="kpi"><div class="lbl">${l}</div><div class="val">${val}</div>` +
    (sub ? `<div class="sub${cls ? ' ' + cls : ''}">${sub}</div>` : '') + '</div>';
}
function last(k) { const s = S[k]; return s && s.length ? s[s.length - 1] : null; }
function yoy(k) {
  const s = S[k]; if (!s || s.length < 13) return null;
  const l = s[s.length - 1], prev = s.find(p => p[0].slice(0, 7) === (+l[0].slice(0, 4) - 1) + l[0].slice(4, 7));
  return prev && prev[1] ? 100 * (l[1] / prev[1] - 1) : null;
}
const fyMonths = f => (D.remit_months && D.remit_months[f]) || 12;
const fyPartial = f => fyMonths(f) < 12;
function drawKpis() {
  const u = last('usd'), c = last('cpi_nat'), p = last('pol_target'), r = last('res_sbp');
  const cov = importCover(), cv = cov.length ? cov[cov.length - 1] : null;
  const dep = yoy('usd');
  const remFy = D.remit.filter(x => x[1] === fy).reduce((a, b) => a + b[2], 0);
  d3.select('#kpis').html([
    kpi('Rupees per US$', u ? num(u[1], 1) : '—', u ? mLbl(u[0]) + (dep != null ? ` · ${pct(dep)} in a year` : '') : '', dep > 0 ? 'neg' : 'pos'),
    kpi('Inflation', c ? c[1].toFixed(1) + '%' : '—', c ? 'National CPI, ' + mLbl(c[0]) : ''),
    kpi('Policy rate', p ? p[1].toFixed(2) + '%' : '—', p ? 'set ' + mLbl(p[0]) : ''),
    kpi('SBP reserves', r ? usdm(r[1]) : '—', cv ? cv[1].toFixed(1) + ' months of imports' : '', cv && cv[1] < 3 ? 'neg' : ''),
    kpi('Remittances', remFy ? usdm(remFy) : '—', fy ? fy + (fyPartial(fy) ? ` · ${fyMonths(fy)} mths only` : '') : '')
  ].join(''));
}

/* ---------------- CSV ---------------- */
function pairTable(keys) {
  const dates = new Set();
  keys.forEach(k => (S[k] || []).forEach(p => dates.add(p[0])));
  const idx = {}; keys.forEach(k => { idx[k] = {}; (S[k] || []).forEach(p => idx[k][p[0]] = p[1]); });
  return [['date', ...keys], ...[...dates].sort().map(d => [d, ...keys.map(k => idx[k][d] ?? '')])];
}
/* the breakdown on screen, every fiscal year, one row per node with its depth */
function drillCSV() {
  const nd = drillNode(bopPath), fys = D.drill.fys, rows = [['level', 'item', ...fys.map(f => f + '_mn_usd')]];
  const walk = (n, depth) => { rows.push([depth, n.n, ...(n.v || fys.map(() => null))]); (n.ch || []).forEach(c => walk(c, depth + 1)); };
  (nd.ch || []).forEach(c => walk(c, 1));
  const name = bopPath.map((p, i) => i ? drillNode(bopPath.slice(0, i + 1)).n : TOP_NAME[p]).join('-')
    .toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '');
  return [name, rows];
}
const CSV = {
  usd:    () => ['pkr-usd', [['date', 'pkr_per_usd'], ...S.usd]],
  reer:   () => ['effective-exchange-rates', pairTable(CH.reer.avail)],
  cpi:    () => ['inflation-yoy', pairTable(CH.cpi.avail)],
  food:   () => ['inflation-components-yoy', pairTable(CH.food.avail)],
  policy: () => ['policy-rates', pairTable(CH.policy.avail)],
  kibor:  () => ['kibor-month-end', pairTable(CH.kibor.avail)],
  spread: () => ['lending-deposit-rates', pairTable(CH.spread.avail)],
  res:    () => ['fx-reserves', pairTable(CH.res.avail)],
  bop:    () => bopView === 'tree' && bopPath.length ? drillCSV()
                : ['balance-of-payments-monthly', pairTable(['gx','gm','sx','sm','pic','pid','sic','remit_bop','sid','ca'])],
  remit:  () => ['remittances-by-source', [['source', 'fiscal_year', 'mn_usd'], ...D.remit]],
  money:  () => ['monetary-aggregates', pairTable(CH.money.avail)],
  npl:    () => ['non-performing-loans', pairTable(['npl_ratio', 'npl_level'])]
};
const toCSV = rows => rows.map(r => r.map(v => { const s = v == null ? '' : String(v);
  return /[",\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s; }).join(',')).join('\n');
function downloadCSV(name, rows) {
  const b = new Blob([toCSV(rows)], { type: 'text/csv;charset=utf-8' });
  const a = document.createElement('a'); a.href = URL.createObjectURL(b); a.download = name + '.csv';
  document.body.appendChild(a); a.click(); document.body.removeChild(a);
  setTimeout(() => URL.revokeObjectURL(a.href), 1000);
}
function initCsv() {
  d3.selectAll('.csvbtn').on('click', function () {
    const f = CSV[this.dataset.csv]; if (!f) return; const [name, rows] = f(); downloadCSV(name, rows);
  });
}

/* ---------------- share ---------------- */
function initShare() {
  document.querySelectorAll('.card[id]').forEach(card => {
    const b = document.createElement('button');
    b.className = 'sharebtn'; b.textContent = 'Share';
    b.style.right = card.querySelector('.csvbtn') ? '76px' : '14px';
    b.addEventListener('click', () => {
      const p = new URLSearchParams(location.hash.replace(/^#/, '')); p.set('at', card.id);
      const url = location.origin + location.pathname + '#' + p.toString();
      navigator.clipboard && navigator.clipboard.writeText(url);
      b.textContent = 'Copied'; b.classList.add('ok');
      setTimeout(() => { b.textContent = 'Share'; b.classList.remove('ok'); }, 1400);
    });
    card.appendChild(b);
  });
}

/* ---------------- topics, hash, wiring ---------------- */
const TOPIC_CHARTS = { rupee:['reer'], prices:['cpi','food'], rates:['policy','kibor','spread'],
                       external:['res','bop'], money:['money'] };
function applyTopic(k, push) {
  topic = k;
  const t = TOPICS.find(x => x.k === k);
  d3.selectAll('#topicList .topic-item').classed('on', function () { return this.dataset.k === k; });
  d3.select('#topicDesc').text(t.desc);
  d3.select('#topicMeta').html('<b>Sources.</b> ' + t.meta);
  const all = k === 'all';
  /* per-topic sidebar controls — every test needs the ||all branch */
  d3.select('#sideScale').style('display', (k === 'rupee' || all) ? null : 'none');
  d3.select('#sideFy').style('display', (k === 'external' || all) ? null : 'none');
  d3.selectAll('[data-topic]').classed('topic-hidden', function () {
    return !all && this.dataset.topic !== k && this.dataset.topic !== 'all';
  });
  if (!applyingHash) writeHash(push);
  drawKpis();
  if (all) drawAll(); else if (TOPIC_DRAWS[k]) TOPIC_DRAWS[k]();
  if (push) window.scrollTo({ top: 0, behavior: 'smooth' });
}
function setScale(v) { scale = v;
  d3.selectAll('#scaleSeg button').classed('on', function () { return this.dataset.sc === v; });
  drawUsd(); writeHash(false); }
function setFy(v) { fy = v; d3.select('#fySelect').property('value', v); syncFySliders();
  drawRemit(); drawBop(); drawKpis(); writeHash(false); }
/* the in-card year sliders mirror the sidebar select; fys is oldest -> newest */
function syncFySliders() {
  d3.selectAll('.fySlider').property('value', fys.indexOf(fy));
  d3.selectAll('.yr-lbl[data-for]').text(fy + (fyPartial(fy) ? ` · ${fyMonths(fy)} mth${fyMonths(fy) === 1 ? '' : 's'}` : ''));
}
function initFySliders() {
  d3.selectAll('.fySlider').attr('min', 0).attr('max', fys.length - 1).attr('step', 1)
    .on('input', function () { setFy(fys[+this.value]); });
  /* the treemap has nothing before the BPM6 series starts (Jul-2013); its slider begins there */
  const firstBop = fys.findIndex(f => bopMonths(f) > 0);
  if (firstBop > 0) d3.select('#bopYr').attr('min', firstBop);
  syncFySliders();
}
function setBopView(v) { bopView = v; drawBop(); writeHash(false); }

function writeHash(push) {
  if (applyingHash) return;
  const p = new URLSearchParams(); p.set('t', topic);
  const all = topic === 'all';
  if ((topic === 'rupee' || all) && scale !== 'log') p.set('sc', scale);
  if ((topic === 'external' || all) && fy && fys.length && fy !== defaultFy()) p.set('fy', fy);
  if ((topic === 'external' || all) && bopView !== 'tree') p.set('bv', bopView);
  if ((topic === 'external' || all) && bopView === 'tree' && bopPath.length) p.set('bz', bopPath.join('.'));
  const charts = all ? Object.values(TOPIC_CHARTS).flat() : (TOPIC_CHARTS[topic] || []);
  charts.forEach(c => { if (!isDefault(c)) p.set('c_' + c, [...SEL[c]].join('.')); });
  const hv = '#' + p.toString(); _lastNavHash = hv;
  try { if (push && history.pushState) history.pushState(null, '', hv);
        else if (history.replaceState) history.replaceState(null, '', hv);
        else location.hash = hv; } catch (e) { location.hash = hv; }
}
function readHashRaw() { let h = '';
  try { h = decodeURIComponent(location.hash.replace(/^#/, '')); } catch (e) { h = location.hash.replace(/^#/, ''); }
  return h.includes('=') ? h : (h ? 't=' + h : ''); }
function readHash() {
  const raw = readHashRaw(); if (!raw) return { t: 'rupee' };
  const o = {}; new URLSearchParams(raw).forEach((v, k) => o[k] = v);
  if (!o.t) o.t = 'rupee'; return o;
}
function applyStateFromHash() {
  if (_lastNavHash && '#' + new URLSearchParams(readHashRaw()).toString() === _lastNavHash) return;
  const o = readHash();
  const k = TOPICS.some(t => t.k === o.t) ? o.t : 'rupee';
  applyingHash = true;
  Object.keys(CH).forEach(c => {
    const v = o['c_' + c];
    SEL[c] = new Set(v ? v.split('.').filter(x => CH[c].avail.includes(x)) : CH[c].def);
    if (!SEL[c].size) SEL[c] = new Set(CH[c].def);
  });
  if (o.bv === 'trend' || o.bv === 'tree') bopView = o.bv;
  bopPath = [];
  if (o.bz) { const p = o.bz.split('.').map((x, i) => i ? +x : x);
    if (drillRoot(p[0]) && p.slice(1).every(Number.isInteger) && drillNode(p)) bopPath = p; }
  applyTopic(k, false);
  if (o.sc) setScale(o.sc);
  if (o.fy && fys.includes(o.fy)) setFy(o.fy);
  applyingHash = false;
  writeHash(false);
  if (o.at) { const el = document.getElementById(o.at);
    if (el) setTimeout(() => el.scrollIntoView({ behavior: 'smooth', block: 'start' }), 140); }
}
function initTopics() {
  const list = d3.select('#topicList'); list.selectAll('*').remove();
  TOPIC_GROUPS.forEach((g, gi) => {
    if (g.label) list.append('div').attr('class', 'topic-group' + (gi === 0 ? ' first' : '')).text(g.label);
    g.keys.forEach(k => {
      const t = TOPICS.find(x => x.k === k); if (!t) return;
      list.append('button').attr('class', 'topic-item' + (k === 'all' ? ' all' : '')).attr('data-k', k)
        .html(`<span class="t-dot"></span>${t.label}`).on('click', () => applyTopic(k, true));
    });
  });
  d3.selectAll('#scaleSeg button').on('click', function () { setScale(this.dataset.sc); });
  d3.selectAll('#bopSeg button').on('click', function () { setBopView(this.dataset.bv); });
  d3.select('#fySelect').on('change', function () { setFy(this.value); });
  d3.select(window).on('keydown.bop', e => { if (e.key === 'Escape' && bopPath.length) zoomTo(bopPath.slice(0, -1)); });
  window.addEventListener('hashchange', () => { if (!applyingHash) applyStateFromHash(); });
  window.addEventListener('popstate', () => { if (!applyingHash) applyStateFromHash(); });
  initShare();
  applyStateFromHash();
}
/* Default to the last COMPLETE fiscal year. The newest one is normally partial — at
   the time of writing FY2026-27 holds a single month — and a one-month total labelled
   as a year reads as a 90% collapse in remittances. */
function defaultFy() { const c = fys.filter(f => !fyPartial(f)); return c.length ? c[c.length - 1] : fys[fys.length - 1]; }
function buildFySelect() {
  fys = [...new Set(D.remit.map(r => r[1]))].sort();
  fy = defaultFy();
  d3.select('#fySelect').selectAll('option').data(fys.slice().reverse()).join('option')
    .attr('value', d => d)
    .text(d => d + (fyPartial(d) ? ` (${fyMonths(d)} mth${fyMonths(d) === 1 ? '' : 's'} so far)` : ''));
  d3.select('#fySelect').property('value', fy);
  initFySliders();
}
function buildFoot() {
  d3.select('#pageFoot').html(
    '<b>Sources.</b> Every series on this page comes from the State Bank of Pakistan’s ' +
    '<a href="https://easydata.sbp.org.pk/" target="_blank" rel="noopener">EasyData</a> portal, retrieved ' +
    'through its public API. Exchange rates, reserves, the balance of payments and remittances are SBP’s own ' +
    'compilations; the price indices are produced by the ' +
    '<a href="https://www.pbs.gov.pk/" target="_blank" rel="noopener">Pakistan Bureau of Statistics</a> and ' +
    'redistributed by SBP. Trade figures here are on a <em>payments</em> basis and will not match the ' +
    'customs-basis figures on the Trade Atlas — the two measure different things. Full detail, including every ' +
    'series not charted here, lives in the project’s DuckDB warehouse (<code>sbp_series_catalog</code>, ' +
    '<code>sbp_observations</code>) and can be queried from the <a href="query.html">SQL console</a>.');
}

/* ---------------- boot ---------------- */
function start() {
  if (!window.DD_MONEY) { return setTimeout(start, 30); }
  D = window.DD_MONEY; S = D.series; M = D.meta;
  buildFySelect(); buildFoot(); initCsv();
  initTopics();          // last: it triggers the first render via applyStateFromHash()
  let rt;
  window.addEventListener('resize', () => { clearTimeout(rt); rt = setTimeout(() => {
    if (topic === 'all') drawAll(); else if (TOPIC_DRAWS[topic]) TOPIC_DRAWS[topic]();
    drawKpis();
  }, 150); });
}
if (document.readyState !== 'loading') start();
else document.addEventListener('DOMContentLoaded', start);
