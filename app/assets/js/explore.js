/*
   explore.js - put any two of these series on one axis, and refuse the
   comparisons that would be wrong.

   Every chart on the Economy and State pages is a fixed selection: inflation
   on one, tax heads on another, and nothing that lets a reader set the policy
   rate beside manufacturing output, or one distribution company's losses
   beside another's. This is the workspace for the rest.

   The hard part is not drawing lines together, it is knowing when not to.
   A series here carries its unit, its kind and whether its year is fiscal or
   calendar, and those decide which views are offered:

     Overlay   needs one shared unit. Two series in rupees and per cent on one
               axis is not a comparison, it is a coincidence of scale, and the
               usual fix - a second y-axis - makes the crossing point an
               artefact of where you put the zeroes. Aligned panels instead.
     Indexed   needs a level, and a positive observed value at the reference
               period for EVERY series. You cannot index an index, and a rate
               rebased to 100 means nothing. The reference is chosen and shown.
     Change    is year-on-year. For a rate that is a change in PERCENTAGE
               POINTS, for a level it is per cent, and the two are labelled
               differently because they are different quantities.
     Panels    always valid, one scale each, one shared cursor. The fallback,
               never an automatic second axis.

   Fiscal years are plotted at the year they end and say so. A fiscal year and
   a calendar year are never silently treated as the same point.

   Missing observations are gaps, not interpolations: defined() breaks every
   line, so a series that stopped looks stopped.
*/
(function () {
  'use strict';

  var DATA = window.DD_EXPLORE || { index: [], series: {} };
  var IDX = DATA.index, SER = DATA.series;
  var BY = {}; IDX.forEach(function (s) { BY[s.key] = s; });

  /* Colours are assigned on add and kept: changing the period, the view or
     the topic must never repaint a series the reader has learned. */
  var PALETTE = ['#1e6b3e', '#b5502a', '#145228', '#d4a017', '#3d7ea6',
                 '#7a3d8f', '#0c3a1e', '#8a6d1f'];
  var sel = [], colourOf = {}, used = 0;
  var mode = 'auto', fromY = null, toY = null, q = '', pageFilter = '';

  var $ = function (id) { return document.getElementById(id); };
  var esc = function (s) {
    return String(s == null ? '' : s).replace(/&/g, '&amp;')
      .replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  };
  var dt = function (s) { return new Date(s + 'T00:00:00'); };
  var yearOf = function (s) { return +String(s).slice(0, 4); };

  /* ---------- what the current selection allows ---------- */
  function units() {
    return Array.from(new Set(sel.map(function (k) { return BY[k].unit; })));
  }
  function kinds() {
    return Array.from(new Set(sel.map(function (k) { return BY[k].kind; })));
  }
  function bases() {
    return Array.from(new Set(sel.map(function (k) { return BY[k].basis; })));
  }

  function modeRules() {
    var u = units(), kd = kinds(), out = {};
    out.overlay = sel.length === 0 ? null
      : u.length === 1 ? true
      : 'the selection mixes ' + u.length + ' units (' + u.join(', ')
        + '), which cannot share one axis';
    var nonLevel = sel.filter(function (k) { return BY[k].kind !== 'level'; });
    if (!sel.length) out.indexed = null;
    else if (nonLevel.length) out.indexed = 'indexing needs level series; '
      + nonLevel.map(function (k) { return BY[k].label; }).join(', ')
      + ' ' + (nonLevel.length === 1 ? 'is' : 'are') + ' already a rate or an index';
    else out.indexed = true;
    out.change = sel.length ? true : null;
    out.panels = sel.length ? true : null;
    return out;
  }

  function effectiveMode() {
    var r = modeRules();
    if (mode !== 'auto' && r[mode] === true) return mode;
    if (r.overlay === true) return 'overlay';
    return 'panels';
  }

  /* ---------- observations, trimmed to the chosen period ---------- */
  function pts(k) {
    var raw = SER[k] || [];
    return raw.filter(function (p) {
      var y = yearOf(p[0]);
      return (fromY == null || y >= fromY) && (toY == null || y <= toY);
    }).map(function (p) { return { t: dt(p[0]), iso: p[0], v: p[1] }; });
  }

  function allYears() {
    var ys = new Set();
    sel.forEach(function (k) {
      (SER[k] || []).forEach(function (p) { ys.add(yearOf(p[0])); });
    });
    return Array.from(ys).sort(function (a, b) { return a - b; });
  }

  /* ---------- transformations ---------- */
  function indexed(series, refYear) {
    return series.map(function (s) {
      var base = null;
      s.pts.forEach(function (p) {
        if (base == null && yearOf(p.iso) === refYear && p.v > 0) base = p.v;
      });
      return { key: s.key, label: s.label, base: base,
               pts: base == null ? []
                 : s.pts.map(function (p) {
                     return { t: p.t, iso: p.iso,
                              v: p.v == null ? null : 100 * p.v / base };
                   }) };
    });
  }

  function yoy(series) {
    return series.map(function (s) {
      var rate = BY[s.key].kind !== 'level';
      var by = {};
      s.pts.forEach(function (p) { by[p.iso] = p.v; });
      var out = [];
      s.pts.forEach(function (p) {
        /* the same period a year earlier, matched on the date rather than on
           position: a series with a gap must not compare across it */
        var prev = new Date(p.t); prev.setFullYear(prev.getFullYear() - 1);
        var key = prev.toISOString().slice(0, 10);
        var b = by[key];
        if (b == null) { out.push({ t: p.t, iso: p.iso, v: null }); return; }
        out.push({ t: p.t, iso: p.iso,
                   v: rate ? p.v - b : (b === 0 ? null : 100 * (p.v - b) / b) });
      });
      return { key: s.key, label: s.label, pts: out, rate: rate };
    });
  }

  /* ---------- drawing ---------- */
  function lineChart(host, series, yLabel, W, H) {
    var m = { t: 14, r: 118, b: 26, l: 58 };
    var all = series.reduce(function (a, s) { return a.concat(s.pts); }, [])
      .filter(function (p) { return p.v != null; });
    if (!all.length) {
      host.append('div').attr('class', 'xp-empty')
        .text('No observations in this period.');
      return;
    }
    var svg = host.append('svg').attr('width', W).attr('height', H)
      .style('display', 'block');
    var x = d3.scaleTime().domain(d3.extent(all, function (p) { return p.t; }))
      .range([m.l, W - m.r]);
    var ext = d3.extent(all, function (p) { return p.v; });
    var y = d3.scaleLinear().domain([Math.min(0, ext[0]), ext[1]]).nice()
      .range([H - m.b, m.t]);
    var ink = 'var(--muted)';
    svg.append('g').attr('transform', 'translate(0,' + (H - m.b) + ')')
      .call(d3.axisBottom(x).ticks(6)).attr('color', ink).attr('font-size', 10.5);
    svg.append('g').attr('transform', 'translate(' + m.l + ',0)')
      .call(d3.axisLeft(y).ticks(5)).attr('color', ink).attr('font-size', 10.5)
      .call(function (g) {
        g.selectAll('.tick line').clone().attr('x2', W - m.r - m.l)
          .attr('stroke', 'var(--line)');
      });
    if (ext[0] < 0) svg.append('line').attr('x1', m.l).attr('x2', W - m.r)
      .attr('y1', y(0)).attr('y2', y(0)).attr('stroke', 'var(--line-strong)');
    svg.append('text').attr('x', m.l).attr('y', m.t - 2).attr('font-size', 10.5)
      .attr('fill', ink).text(yLabel);
    /* defined() is the whole difference between a gap and a straight line
       drawn through years nobody published. */
    var line = d3.line().defined(function (p) { return p.v != null; })
      .x(function (p) { return x(p.t); }).y(function (p) { return y(p.v); });
    series.forEach(function (s) {
      svg.append('path').datum(s.pts).attr('fill', 'none')
        .attr('stroke', colourOf[s.key]).attr('stroke-width', 2)
        .attr('d', line);
      var lp = s.pts.filter(function (p) { return p.v != null; }).pop();
      if (lp) svg.append('text').attr('x', x(lp.t) + 6).attr('y', y(lp.v) + 4)
        .attr('font-size', 10.5).attr('font-weight', 700)
        .attr('fill', colourOf[s.key])
        .text(s.label.length > 17 ? s.label.slice(0, 16) + '…' : s.label);
    });
  }

  function draw() {
    var host = d3.select('#xpChart');
    host.selectAll('*').remove();
    if (!sel.length) {
      host.append('div').attr('class', 'xp-empty').text(
        'Nothing selected yet. Search on the left and choose a series — '
        + 'inflation, a policy rate, a tax head, a distribution company’s '
        + 'losses, cases pending in one province. Add a second and this page '
        + 'will offer only the comparisons that are valid for the two.');
      $('xpWhy').innerHTML = '';
      $('xpSrc').innerHTML = '';
      return;
    }
    var W = host.node().clientWidth || 900;
    var eff = effectiveMode();
    var series = sel.map(function (k) {
      return { key: k, label: BY[k].label, pts: pts(k) };
    });

    if (eff === 'indexed') {
      var ys = allYears();
      var ref = fromY != null ? fromY : ys[0];
      var ix = indexed(series, ref);
      var failed = ix.filter(function (s) { return s.base == null; });
      if (failed.length) {
        $('xpWhy').innerHTML = '<b>Cannot index on ' + ref + '.</b> '
          + failed.map(function (s) { return esc(BY[s.key].label); }).join(', ')
          + (failed.length === 1 ? ' has' : ' have')
          + ' no positive observation that year, and a base of nothing is not '
          + 'a base. Showing panels instead — Common period will narrow to '
          + 'the years every series covers.';
        eff = 'panels';
      } else {
        lineChart(host, ix, ref + ' = 100', W, 380);
        $('xpWhy').innerHTML = 'Each series set to 100 in <b>' + ref
          + '</b> and shown as relative movement since. Indexing makes sizes '
          + 'comparable; it does not make the definitions equivalent.';
      }
    }
    if (eff === 'change') {
      var ch = yoy(series);
      var mixed = Array.from(new Set(ch.map(function (s) { return s.rate; })));
      lineChart(host, ch, mixed.length > 1 ? 'change on a year earlier'
        : (ch[0].rate ? 'change, percentage points' : 'change, per cent'),
        W, 380);
      $('xpWhy').innerHTML = 'Change on the same period a year earlier.'
        + (mixed.length > 1 ? ' <b>Mixed quantities:</b> the rate series '
          + 'change in percentage points and the level series in per cent, '
          + 'so these lines are not the same measure.' : '');
    }
    if (eff === 'overlay') {
      lineChart(host, series, units()[0], W, 380);
      $('xpWhy').innerHTML = 'One axis, one unit: <b>' + esc(units()[0])
        + '</b>.';
    }
    if (eff === 'panels') {
      var h = sel.length > 3 ? 150 : 190;
      series.forEach(function (s) {
        var p = host.append('div').attr('class', 'xp-panel');
        p.append('div').attr('class', 'xp-panel-lbl')
          .style('color', colourOf[s.key])
          .text(s.label + ' · ' + BY[s.key].unit);
        lineChart(p, [s], '', W, h);
      });
      if (!$('xpWhy').innerHTML) {
        var r = modeRules();
        $('xpWhy').innerHTML = typeof r.overlay === 'string'
          ? '<b>Separate panels:</b> ' + esc(r.overlay)
            + ', so each keeps its own scale rather than being forced onto a '
            + 'second y-axis where the crossing point would be an artefact.'
          : 'Separate panels, each on its own scale.';
      }
    }
    if (bases().length > 1) {
      $('xpWhy').innerHTML += ' <b>Mixed year bases:</b> this selection has '
        + 'both fiscal and calendar years. Fiscal years are plotted at the '
        + 'year they end; they are not the same twelve months.';
    }
    renderSrc();
  }

  function renderSrc() {
    $('xpSrc').innerHTML = sel.map(function (k) {
      var s = BY[k];
      return '<div><b>' + esc(s.label) + '</b> · ' + esc(s.unit)
        + ' · ' + esc(s.freq) + ' · ' + esc(s.basis) + ' years · '
        + s.from + ' to ' + s.to + ' · <a href="datasets/'
        + s.source.replace(/_/g, '-') + '/">' + esc(s.source) + '</a>'
        + (s.note ? ' — ' + esc(s.note) : '') + '</div>';
    }).join('');
  }

  /* ---------- chrome ---------- */
  function renderChips() {
    $('xpChips').innerHTML = sel.map(function (k) {
      return '<span class="xp-chip"><i style="background:' + colourOf[k]
        + '"></i>' + esc(BY[k].label)
        + '<button type="button" data-drop="' + esc(k)
        + '" aria-label="Remove">×</button></span>';
    }).join('');
    Array.prototype.forEach.call(
      $('xpChips').querySelectorAll('[data-drop]'), function (b) {
        b.onclick = function () { toggle(b.dataset.drop); };
      });
  }

  function renderModes() {
    var r = modeRules(), eff = effectiveMode();
    var opts = [['overlay', 'Overlay'], ['indexed', 'Indexed'],
                ['change', 'Year on year'], ['panels', 'Panels']];
    $('xpMode').innerHTML = opts.map(function (o) {
      var ok = r[o[0]] === true;
      return '<button type="button" class="seg" data-m="' + o[0] + '"'
        + (ok ? '' : ' disabled title="' + esc(r[o[0]] || 'add a series') + '"')
        + ' aria-pressed="' + (eff === o[0]) + '">' + o[1] + '</button>';
    }).join('');
    Array.prototype.forEach.call($('xpMode').querySelectorAll('button'),
      function (b) {
        b.onclick = function () { mode = b.dataset.m; sync(); };
      });
  }

  function renderRange() {
    var ys = allYears();
    [['xpFrom', fromY], ['xpTo', toY]].forEach(function (pair) {
      var el = $(pair[0]);
      el.innerHTML = ys.map(function (y) {
        return '<option value="' + y + '"' + (y === pair[1] ? ' selected' : '')
          + '>' + y + '</option>';
      }).join('');
      el.disabled = !ys.length;
    });
  }

  function renderList() {
    var needle = q.trim().toLowerCase();
    var rows = IDX.filter(function (s) {
      if (pageFilter && s.page !== pageFilter) return false;
      if (!needle) return true;
      return (s.label + ' ' + s.topic + ' ' + s.unit + ' ' + s.source + ' '
        + (s.note || '')).toLowerCase().indexOf(needle) >= 0;
    });
    $('xpCount').textContent = rows.length + ' of ' + IDX.length + ' series';
    var out = [], lastGrp = null;
    rows.forEach(function (s) {
      var g = (s.page === 'economy' ? 'Economy' : 'State') + ' · ' + s.topic;
      if (g !== lastGrp) { out.push('<div class="xp-grp">' + esc(g) + '</div>'); lastGrp = g; }
      out.push('<button type="button" class="xp-item" data-k="' + esc(s.key)
        + '" aria-pressed="' + (sel.indexOf(s.key) >= 0) + '"><b>'
        + esc(s.label) + '</b><span class="xp-meta">' + esc(s.unit) + ' · '
        + esc(s.freq) + ' · ' + s.from.slice(0, 4) + '–'
        + s.to.slice(0, 4) + '</span></button>');
    });
    $('xpList').innerHTML = out.join('') ||
      '<div class="xp-empty">Nothing matches that.</div>';
    Array.prototype.forEach.call($('xpList').querySelectorAll('.xp-item'),
      function (b) { b.onclick = function () { toggle(b.dataset.k); }; });
  }

  function toggle(k) {
    var i = sel.indexOf(k);
    if (i >= 0) { sel.splice(i, 1); }
    else {
      if (!colourOf[k]) colourOf[k] = PALETTE[used++ % PALETTE.length];
      sel.push(k);
      /* Adding must not move the window the reader set. Only an empty
         selection gets a default range. */
      if (fromY == null) { var ys = allYears(); fromY = ys[0]; toY = ys[ys.length - 1]; }
    }
    if (!sel.length) { fromY = toY = null; }
    sync();
  }

  function commonPeriod() {
    if (!sel.length) return;
    var lo = -Infinity, hi = Infinity;
    sel.forEach(function (k) {
      lo = Math.max(lo, yearOf(BY[k].from));
      hi = Math.min(hi, yearOf(BY[k].to));
    });
    if (lo > hi) {
      $('xpWhy').innerHTML = '<b>No common period.</b> These series do not '
        + 'overlap at all, so there is nothing to narrow to.';
      return;
    }
    fromY = lo; toY = hi; sync();
  }

  function csv() {
    if (!sel.length) return;
    var rows = {}, cols = sel.slice();
    sel.forEach(function (k) {
      pts(k).forEach(function (p) {
        (rows[p.iso] = rows[p.iso] || {})[k] = p.v;
      });
    });
    var head = ['period'].concat(cols.map(function (k) {
      return BY[k].label + ' (' + BY[k].unit + ')';
    }));
    var body = Object.keys(rows).sort().map(function (t) {
      return [t].concat(cols.map(function (k) {
        return rows[t][k] == null ? '' : rows[t][k];
      }));
    });
    var meta = [['# view', effectiveMode()],
                ['# period', (fromY || '') + ' to ' + (toY || '')]]
      .concat(sel.map(function (k) {
        var s = BY[k];
        return ['# series', s.label + ' | ' + s.unit + ' | ' + s.freq + ' | '
          + s.basis + ' years | ' + s.source + ' | ' + s.from + ' to ' + s.to];
      }))
      .concat([['# retrieved', new Date().toISOString().slice(0, 10)],
               ['# terms', 'Derived data CC BY 4.0 - darbar.adaad.org']]);
    var cell = function (v) {
      v = String(v == null ? '' : v);
      return /[",\n]/.test(v) ? '"' + v.replace(/"/g, '""') + '"' : v;
    };
    var text = meta.map(function (r) { return r.map(cell).join(','); }).join('\n')
      + '\n#\n' + [head].concat(body)
        .map(function (r) { return r.map(cell).join(','); }).join('\n');
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([text], { type: 'text/csv' }));
    a.download = 'data_darbar_compare.csv';
    document.body.appendChild(a); a.click();
    setTimeout(function () { URL.revokeObjectURL(a.href); a.remove(); }, 200);
  }

  /* ---------- url ---------- */
  function writeUrl() {
    var p = new URLSearchParams();
    if (sel.length) p.set('s', sel.join('~'));
    if (mode !== 'auto') p.set('v', mode);
    if (fromY != null) p.set('a', fromY);
    if (toY != null) p.set('b', toY);
    history.replaceState(null, '', p.toString() ? '?' + p : location.pathname);
  }
  function readUrl() {
    var p = new URLSearchParams(location.search);
    (p.get('s') || '').split('~').filter(Boolean).forEach(function (k) {
      if (BY[k] && sel.indexOf(k) < 0) {
        colourOf[k] = colourOf[k] || PALETTE[used++ % PALETTE.length];
        sel.push(k);
      }
    });
    var v = p.get('v'); if (v) mode = v;
    if (p.get('a')) fromY = +p.get('a');
    if (p.get('b')) toY = +p.get('b');
    if (sel.length && fromY == null) {
      var ys = allYears(); fromY = ys[0]; toY = ys[ys.length - 1];
    }
  }

  function sync() {
    renderChips(); renderModes(); renderRange(); renderList();
    $('xpWhy').innerHTML = '';
    draw(); writeUrl();
  }

  function init() {
    $('xpFilters').innerHTML =
      [['', 'All'], ['economy', 'Economy'], ['state', 'State']]
        .map(function (f) {
          return '<button type="button" class="seg" data-p="' + f[0]
            + '" aria-pressed="' + (pageFilter === f[0]) + '">' + f[1]
            + '</button>';
        }).join('');
    Array.prototype.forEach.call($('xpFilters').querySelectorAll('button'),
      function (b) {
        b.onclick = function () {
          pageFilter = b.dataset.p;
          Array.prototype.forEach.call($('xpFilters').querySelectorAll('button'),
            function (o) { o.setAttribute('aria-pressed', o === b); });
          renderList();
        };
      });
    $('xpQ').oninput = function () { q = this.value; renderList(); };
    $('xpFrom').onchange = function () { fromY = +this.value; sync(); };
    $('xpTo').onchange = function () { toY = +this.value; sync(); };
    $('xpCommon').onclick = commonPeriod;
    $('xpCsv').onclick = csv;
    $('xpShare').onclick = function () {
      writeUrl();
      var t = $('xpShare');
      navigator.clipboard && navigator.clipboard.writeText(location.href)
        .then(function () {
          t.textContent = 'Copied';
          setTimeout(function () { t.textContent = 'Share'; }, 1400);
        });
    };
    var rt;
    window.addEventListener('resize', function () {
      clearTimeout(rt); rt = setTimeout(draw, 160);
    });
    readUrl();
    sync();
  }

  if (document.readyState !== 'loading') init();
  else document.addEventListener('DOMContentLoaded', init);
})();
