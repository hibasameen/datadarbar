/*
   palette.js — the one set of chart colours.

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

  /* Sixteen, spread across hue families rather than drawn from the chrome.

     The first attempt built the ramp out of the brand colours and read as
     what it was: ten of sixteen were green or gold, with one blue and one
     purple between them, so a chart of eight series looked like eight shades
     of the same thing. Reordering would not have fixed that - the set was
     unbalanced, not badly sorted.

     This is roughly even across green, blue, gold, red, purple and teal, and
     the first five land in five different families, which is what matters:
     most charts never reach slot six. Every hue is held at an editorial
     saturation so it still sits on cream beside the green header - these are
     muted cousins of the default web palette, not the palette itself. */
  var CATEGORICAL = [
    '#1b5e4a', // pine
    '#d4a017', // gold
    '#2f5d7c', // slate blue
    '#a8452f', // rust
    '#6b4c7a', // plum
    '#3f9aa3', // teal
    '#b5651d', // sienna
    '#5c6b3a', // olive
    '#7b9ec9', // sky
    '#9a2c1f', // brick
    '#a3789e', // mauve
    '#c98b2e', // amber
    '#4d8a62', // sage
    '#6e7f95', // grey blue
    '#8fbfc4', // pale teal
    '#d8b48a', // sand
  ];

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
    greenStops: GREEN.slice(),
    goldStops: GOLD.slice(),
    positive: '#2a7d4c',
    negative: '#9a2c1f',
    accent: accent,
    token: token,
  };
})();
