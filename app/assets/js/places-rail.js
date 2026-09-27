/*
   places-rail.js — Topic, then Dataset, then Indicator.

   The order matters and is not the same as Economy's. Someone coming to
   Places has a subject in mind and wants to know who measured it:

     Education  ->  Census 2023  ->  Literacy rate, 10+
     Education  ->  PSLM 2019-20 ->  Literacy rate, 10+

   The same indicator name genuinely exists under several datasets, measured
   differently and years apart, and the old picker made that collision almost
   impossible to see: it listed 5,801 indicators grouped by topic with the
   source only in small print, so three datasets' literacy rates sat next to
   each other looking like three unrelated rows. Naming the dataset as its own
   step makes choosing between them deliberate.

   It is also what makes Economic Census 2023 (8 indicators), Mouza Census 2020
   (54) and the agriculture pull (324) reachable at all. All three were in the
   index the whole time and could only be found by typing the right word.

   The search box and the grouped list stay exactly as they were. This is
   another way in, not a replacement, and both end at the same choose(row).
*/
(function () {
  'use strict';

  function build(IX, N, col, level) {
    var index = [], seen = {};
    for (var i = 0; i < N; i++) {
      if (col('level', i) !== level) continue;
      var topic = col('topic', i);
      var ds = col('dataset', i) || 'Unattributed';
      var label = col('label', i) || col('indicator', i);
      // Row number is the indicator's value, so two identically named
      // indicators under one dataset stay distinct rather than collapsing.
      var key = topic + '\u001f' + ds + '\u001f' + i;
      if (seen[key]) continue;
      seen[key] = 1;
      index.push({
        topic: topic, topicLabel: col('topic_label', i) || topic,
        ds: ds, dsLabel: ds,
        ind: String(i), label: label,
        key: col('indicator', i),
        rows: col('shapes', i) ? col('shapes', i) + ' places' : '',
      });
    }
    index.sort(function (a, b) {
      return a.topicLabel.localeCompare(b.topicLabel)
          || a.dsLabel.localeCompare(b.dsLabel)
          || a.label.localeCompare(b.label);
    });
    return index;
  }

  /* Matched against the indicator KEY, not the display label. Labels are
     ambiguous in both directions: "POPULATION - 2017 / ALL SEXES" also reads
     as an all-sexes population row, and table 3's household-size brackets are
     labelled "1,000-1,999 - Population 2023 - All Sexes". Two earlier attempts
     opened the national map on the 2017 census and then on a household-size
     bracket. The key names the table, so it cannot mean two things. */
  var OPENERS = [
    /^1\|POPULATION[\s-]*2023 \/ ALL SEXES\|POPULATION[\s-]*2023 \/ ALL SEXES$/i,
    /^1\|POPULATION[\s-]*2023 \/ ALL SEXES\|/i,
    /^total_population$/i,
  ];

  function prefer(index) {
    for (var p = 0; p < OPENERS.length; p++) {
      var hit = index.filter(function (r) { return OPENERS[p].test(r.key || ''); })[0];
      if (hit) return hit;
    }
    // Nothing recognised: take the topic with the most indicators rather than
    // whatever sorts first alphabetically.
    var byTopic = {};
    index.forEach(function (r) { byTopic[r.topic] = (byTopic[r.topic] || 0) + 1; });
    var best = Object.keys(byTopic).sort(function (a, b) {
      return byTopic[b] - byTopic[a];
    })[0];
    return index.filter(function (r) { return r.topic === best; })[0] || null;
  }

  window.DDPlacesRail = {
    mount: function (opts) {
      var host = opts.el;
      if (!host || !window.DDExplorer) return null;
      var index = build(opts.IX, opts.N, opts.col, opts.level);
      if (!index.length) return null;

      /* Alphabetically first is "Agriculture / Almond, area in thousand
         hectares", which is a poor thing to open a national map on. Open on
         population the way the live map does, and fall back by preference
         rather than to whatever sorts first. */
      var first = prefer(index) || index[0];
      var st = { topic: first.topic, ds: first.ds, ind: first.ind };
      if (opts.row != null) {
        var cur = index.filter(function (r) {
          return r.ind === String(opts.row);
        })[0];
        if (cur) { st.topic = cur.topic; st.ds = cur.ds; st.ind = cur.ind; }
      }

      var rail = window.DDExplorer.mount({
        el: host, index: index, state: st,
        levels: ['topic', 'ds', 'ind'],
        labels: { topic: 'Topic', ds: 'Dataset', ind: 'Indicator' },
        onChange: function (r) { if (r) opts.onChange(Number(r.ind)); },
      });
      rail.sync(false);

      return {
        /* Draw whatever the rail is currently showing. boot() calls this when
           the URL carried no indicator, so the page opens on a real map
           instead of a rail pointing at an empty one. */
        fire: function () {
          var r = index.filter(function (x) { return x.ind === st.ind; })[0];
          if (r) opts.onChange(Number(r.ind));
        },
        /* The picker list and the rail are peers, so choosing from one moves
           the other. Without this they drift and the page shows a rail that
           disagrees with the indicator actually on the map. */
        follow: function (row) {
          var r = index.filter(function (x) { return x.ind === String(row); })[0];
          if (!r) return;
          st.topic = r.topic; st.ds = r.ds; st.ind = r.ind;
          rail.sync(false);
        },
        rebuild: function (level, row) {
          index = build(opts.IX, opts.N, opts.col, level);
          var r = index.filter(function (x) { return x.ind === String(row); })[0]
               || index[0];
          if (!r) return;
          st.topic = r.topic; st.ds = r.ds; st.ind = r.ind;
          rail = window.DDExplorer.mount({
            el: host, index: index, state: st,
            levels: ['topic', 'ds', 'ind'],
            labels: { topic: 'Topic', ds: 'Dataset', ind: 'Indicator' },
            onChange: function (x) { if (x) opts.onChange(Number(x.ind)); },
          });
          rail.sync(false);
        },
      };
    },
  };
})();
