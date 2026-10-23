/*
   palette.js — the one set of chart colours. Spec: the redesign canvas,
   artboard "Chart palette" (2026-10-23).

   Before this, each page brought its own. finance.js used 38 colours outside
   the design system — #9b59b6 violet, #3d6db5 blue, #e07b39 orange, #16a085
   teal — a generic categorical set with no relation to the green-and-gold the
   rest of the site is built from; money.js and trade.js carried near-copies of
   it, and every one of the three drew its negatives in #c0392b, an alert red
   that sits badly on a cream ground.

   The design system says the ground is cream, the chrome is green and gold,
   and the three explorers are told apart by accent alone. So the categorical
   ramp is built from that family: it walks green -> teal -> gold -> brick,
   alternating light and dark so neighbouring series stay apart at a glance
   rather than relying on hue alone. Ten steps, because past ten a legend is
   the wrong tool and the chart wants a different shape.

   Ordered data is a different problem and gets its own ramps. Nothing here is
   picked for prettiness: a reader should be able to tell two lines apart, and
   should not have to learn a new colour language on each page.
*/
(function () {
  'use strict';

  /* Eight, in a fixed order, each checked rather than eyeballed.

     The sixteen before these were muted on purpose, to sit beside the green
     header - and five of them were muted past the point where a hue still
     does identity work: pine, slate blue, plum, teal and olive all read as
     grey under a chroma check. Olive and sienna were 4.8 apart under
     protanopia, which is the same colour; and the brand gold reached 2.2:1
     on cream, which fails for a line.

     These eight pass the lightness band, the chroma floor, adjacent-pair
     separation under protan, deutan and tritan simulation, and 3:1 contrast
     on #faf7ef (2026-10-23, dataviz validator). Only the first three are safe
     in EVERY pairing - maps, scatter and small multiples where any two marks
     can touch - so from slot four on, assignment is in order and a ninth
     series folds into "Other". The order never changes: it is what makes
     the neighbours distinguishable, so a filter that drops a series must not
     repaint the survivors. */
  var CATEGORICAL = [
    '#1e6b3e', // pine    - the site's green; the single-series default
    '#b8860b', // gold    - a step darker than the chrome's #d4a017, for 3:1
    '#2f6fb0', // blue
    '#b5402a', // brick
    '#7a4f9c', // plum
    '#0f9aab', // teal
    '#d96a26', // orange
    '#8c4a7e', // mauve
  ];

  /* Colours with a fixed meaning, the same on every chart on every page, so
     a reader carries them from the structure chart to the budget to the IO
     grid. The three sectors are the fills the structure chart has always
     used (white labels sit on them, so 3:1 against cream does not apply);
     flows in and out take slots one and two. */
  var MEANING = {
    agri: '#9dbfa9', ind: '#d4a017', serv: '#0c3a1e',
    inflow: '#1e6b3e', outflow: '#b8860b',
    state: '#0f6e78',      // State's single-series accent, from its card
    reference: '#9aa39c',  // a total behind parts, the 100% line, "Other"
    shade: '#e9ece6',      // era bands, COVID, floods, the backcast years
    absent: null,          // not reported: outline only, never a fill
  };

  /* Brick <- grey -> green. The midpoint is grey, never a hue, so a value
     at the baseline is not read as belonging to either side. */
  var DIVERGING = ['#9a2c1f', '#b5402a', '#d58a74', '#ecc9bd', '#d9d8d0',
                   '#bcd8c5', '#7aa88c', '#2a7d4c', '#145228'];

  var GREEN = ['#f0f9f4', '#d6e8dc', '#9dbfa9', '#7aa88c', '#4d8a62',
               '#2a7d4c', '#1e6b3e', '#145228', '#0c3a1e'];
  var GOLD = ['#fef6dc', '#f0cc5a', '#e8b92e', '#d4a017', '#b5860b',
              '#8a6a0d', '#6b5311'];

  function ramp(stops) {
    return function (n) {
      if (n <= 1) return [stops[stops.length - 2]];
      var out = [];
      for (var i = 0; i < n; i++) {
        out.push(stops[Math.round(i * (stops.length - 1) / (n - 1))]);
      }
      return out;
    };
  }

  /* A page's own accent, read from the token so CSS stays the single source. */
  function accent() {
    var v = getComputedStyle(document.documentElement)
      .getPropertyValue('--accent').trim();
    return v || '#1e6b3e';
  }

  function token(name, fallback) {
    var v = getComputedStyle(document.documentElement)
      .getPropertyValue('--' + name).trim();
    return v || fallback;
  }

  window.DDPalette = {
    categorical: CATEGORICAL.slice(),
    /* d3.scaleOrdinal over the categorical ramp, cycling past ten rather than
       running out and handing every later series the same undefined. */
    ordinal: function (domain) {
      var d = domain || [];
      return function (k) {
        var i = d.indexOf(k);
        return CATEGORICAL[(i < 0 ? 0 : i) % CATEGORICAL.length];
      };
    },
    green: ramp(GREEN),
    gold: ramp(GOLD),
    diverging: ramp(DIVERGING),
    divergingStops: DIVERGING.slice(),
    meaning: MEANING,
    greenStops: GREEN.slice(),
    goldStops: GOLD.slice(),
    positive: '#2a7d4c',
    negative: '#9a2c1f',
    accent: accent,
    token: token,
  };
})();
