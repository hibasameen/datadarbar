/* Data Darbar - real and nominal rupees.

   One deflator for every page that offers a "Real" view: the GDP deflator by
   fiscal year (data/deflator_data.js, built by etl/economy/build_deflator.py
   from PBS's national accounts, 1960-61 onwards). Real figures are in the
   rupees of the latest complete fiscal year, so a reader sees amounts they
   recognise rather than 2015-16 prices.

     DDDeflate.factor(fy)       multiply a nominal amount for that fiscal year by
                                this to get it in base-year rupees
     DDDeflate.factorDate(d)    the same for a date ('2019-03-31'): the
                                deflator is interpolated between fiscal-year
                                midpoints, so a monthly line does not step
                                every July
     DDDeflate.fyOfEnd(2024)    '2023-24'
     DDDeflate.isEstimate(fy)   true past the last published year, where the
                                deflator is extended at its last growth rate
     DDDeflate.base, .label, .note
*/
window.DDDeflate = (function () {
  var D = window.DD_DEFLATOR || { fy: {}, real_base: null };
  var idx = D.fy || {};
  var known = Object.keys(idx).sort();
  var base = D.real_base || known[known.length - 1];

  function startOf(fy) { return +String(fy).slice(0, 4); }
  function fyOfStart(y) { return y + '-' + String((y + 1) % 100).padStart(2, '0'); }
  function fyOfEnd(y) { return fyOfStart(+y - 1); }

  /* The index for any fiscal year: published where it is, carried at the
     last year's growth beyond it, and at the first year's before it. */
  function index(fy) {
    if (idx[fy] != null) return idx[fy];
    if (!known.length) return null;
    var y = startOf(fy), lo = startOf(known[0]), hi = startOf(known[known.length - 1]);
    if (y > hi) {
      var g = idx[known[known.length - 1]] / idx[known[known.length - 2]];
      return idx[known[known.length - 1]] * Math.pow(g, y - hi);
    }
    if (y < lo) {
      var g0 = idx[known[1]] / idx[known[0]];
      return idx[known[0]] / Math.pow(g0, lo - y);
    }
    return null;
  }
  function factor(fy) {
    var i = index(fy), b = index(base);
    return i && b ? b / i : null;
  }
  /* A date's deflator, interpolated geometrically between the midpoints of
     the fiscal years either side (1 January). */
  function factorDate(d) {
    var t = new Date(String(d).slice(0, 10) + 'T00:00:00Z');
    if (isNaN(t)) return null;
    var y = t.getUTCFullYear();
    var mid = function (endYear) { return Date.UTC(endYear, 0, 1); };
    var e0 = t.getTime() >= mid(y) ? y : y - 1;       // the midpoint at or before
    var i0 = index(fyOfEnd(e0)), i1 = index(fyOfEnd(e0 + 1));
    if (!i0 || !i1) return null;
    var w = (t.getTime() - mid(e0)) / (mid(e0 + 1) - mid(e0));
    var i = i0 * Math.pow(i1 / i0, w), b = index(base);
    return b ? b / i : null;
  }
  function fyOfDate(d) {
    var t = String(d), y = +t.slice(0, 4), m = +t.slice(5, 7);
    return m >= 7 ? fyOfStart(y) : fyOfStart(y - 1);
  }
  function isEstimate(fy) {
    if (idx[fy] != null) return false;
    return startOf(fy) > startOf(known[known.length - 1]);
  }

  return {
    available: known.length > 1,
    base: base,
    first: known[0], last: known[known.length - 1],
    index: index, factor: factor, factorDate: factorDate,
    fyOfEnd: fyOfEnd, fyOfDate: fyOfDate, isEstimate: isEstimate,
    label: 'Real ' + base,
    unit: base + ' rupees',
    note: 'Real: in ' + base + ' rupees, deflated by the GDP deflator (PBS national '
        + 'accounts; before 1999-00 the 1980-81 series, linked).',
  };
})();
