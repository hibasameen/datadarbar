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

  function build(IX, N, col, level, list) {
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
      /* The measure is the indicator and the breakdown is the metric, so
         "Total Population" holds its 103 age bands instead of standing as 103
         separate indicators. A cell with no breakdown gets one metric, "All",
         rather than an empty dropdown. */
      var measure = col('measure', i) || label;
      var metric = col('metric', i) || '';
      index.push({
        topic: topic, topicLabel: col('topic_label', i) || topic,
        ds: ds, dsLabel: ds,
        ind: measure, label: measure,
        metric: String(i), metricLabel: metric || 'All',
        key: col('indicator', i), groupKey: col('group_key', i),
        years: (list ? list('years', i) : []).join('/'),
        row: i, fullLabel: label,
        rows: col('shapes', i) ? col('shapes', i) + ' places' : '',
      });
    }
    /* "Total Population" is published in several tables, each with its own
       age bands, so the metric list showed "0-4" three times over. Where a
       metric repeats within one measure, the table it came from is what
       separates them - the same qualifier the full label uses. */
    var byMeasure = {};
    index.forEach(function (r) {
      var k = r.topic + '\u001f' + r.ds + '\u001f' + r.ind + '\u001f' + r.metricLabel;
      (byMeasure[k] = byMeasure[k] || []).push(r);
    });
    Object.keys(byMeasure).forEach(function (k) {
      var group = byMeasure[k];
      if (group.length < 2) return;
      group.forEach(function (r) {
        var t = /^census_t(.+)$/.exec(r.groupKey || '');
        if (t) r.metricLabel += '\u2002\u00b7\u2002table ' + t[1];
      });
      // One table can still give two rows with one name, because the two
      // censuses spell the same band differently and the labels normalise to
      // the same string. The census separates those, as it does for labels.
      var still = {};
      group.forEach(function (r) {
        (still[r.metricLabel] = still[r.metricLabel] || []).push(r);
      });
      Object.keys(still).forEach(function (lab) {
        if (still[lab].length < 2) return;
        still[lab].forEach(function (r) {
          if (r.years) r.metricLabel += '\u2002\u00b7\u2002' + r.years;
        });
      });
    });

    index.sort(function (a, b) {
      return a.topicLabel.localeCompare(b.topicLabel)
          || a.dsLabel.localeCompare(b.dsLabel)
          || a.label.localeCompare(b.label)
          || a.metricLabel.localeCompare(b.metricLabel, undefined, { numeric: true });
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

  function hold(st, r) {
    st.topic = r.topic; st.ds = r.ds; st.ind = r.ind; st.metric = r.metric;
  }

  /* The row the four held values resolve to, falling back up the cascade the
     same way the rail itself does when a level's value did not survive. */
  function pick(index, st) {
    return index.filter(function (r) {
      return r.topic === st.topic && r.ds === st.ds
          && r.ind === st.ind && r.metric === st.metric;
    })[0]
    || index.filter(function (r) {
      return r.topic === st.topic && r.ds === st.ds && r.ind === st.ind;
    })[0]
    || index.filter(function (r) { return r.topic === st.topic; })[0];
  }

  window.DDPlacesRail = {
    mount: function (opts) {
      var host = opts.el;
      if (!host || !window.DDExplorer) return null;
      var index = build(opts.IX, opts.N, opts.col, opts.level, opts.list);
      if (!index.length) return null;

      /* Alphabetically first is "Agriculture / Almond, area in thousand
         hectares", which is a poor thing to open a national map on. Open on
         population the way the live map does, and fall back by preference
         rather than to whatever sorts first. */
      var first = prefer(index) || index[0];
      var st = {};
      hold(st, first);
      if (opts.row != null) {
        var cur = index.filter(function (r) { return r.row === opts.row; })[0];
        if (cur) hold(st, cur);
      }

      var rail = window.DDExplorer.mount({
        el: host, index: index, state: st,
        levels: ['topic', 'ds', 'ind', 'metric'],
        labels: { topic: 'Topic', ds: 'Dataset', ind: 'Indicator',
                  metric: 'Metric' },
        onChange: function (r) { if (r) opts.onChange(r.row); },
      });
      rail.sync(false);

      return {
        /* Draw whatever the rail is currently showing. boot() calls this when
           the URL carried no indicator, so the page opens on a real map
           instead of a rail pointing at an empty one. */
        fire: function () {
          var r = pick(index, st);
          if (r) opts.onChange(r.row);
        },
        /* The picker list and the rail are peers, so choosing from one moves
           the other. Without this they drift and the page shows a rail that
           disagrees with the indicator actually on the map. */
        follow: function (row) {
          var r = index.filter(function (x) { return x.row === row; })[0];
          if (!r) return;
          hold(st, r);
          rail.sync(false);
        },
        /* Switching district <-> tehsil rebuilds the index, because the two
           levels carry different indicators. Two things were wrong here: it
           re-mounted the rail but never fired, so the map went blank and
           stayed blank; and having no held row it took index[0], which sorts
           alphabetically - so the country opened on "Almond, area in thousand
           hectares" or on "Bank".

           Now it keeps the indicator you were looking at if the new level has
           it, falls back to the preferred opener if not, and always draws. */
        rebuild: function (level, key) {
          var prev = pick(index, st);
          index = build(opts.IX, opts.N, opts.col, level, opts.list);
          if (!index.length) return;

          var same = prev && index.filter(function (x) {
            return x.key === prev.key;
          })[0];
          var next = same || prefer(index) || index[0];
          hold(st, next);

          rail = window.DDExplorer.mount({
            el: host, index: index, state: st,
            levels: ['topic', 'ds', 'ind', 'metric'],
            labels: { topic: 'Topic', ds: 'Dataset', ind: 'Indicator',
                      metric: 'Metric' },
            onChange: function (x) { if (x) opts.onChange(x.row); },
          });
          rail.sync(false);
          var drawn = pick(index, st) || next;
          opts.onChange(drawn.row);
        },
      };
    },
  };
})();
