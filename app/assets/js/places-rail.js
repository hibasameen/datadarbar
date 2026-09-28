/*
   places-rail.js — Topic, subtopic, family, source, indicator, metric.

   The hierarchy is the one proposed in places-hierarchy-proposal.md and
   joined onto the index by etl/places/build_payload.py on the compound source
   key. Eleven topics, 49 subtopics, 76 families over the same 5,201 entries;
   nothing is merged, renamed or dropped, and every original series keeps its
   own row.

   Why a subject tree and not the flat topic list it replaces: the old rail
   asked for a topic and then a dataset, and between them they had to carry
   1,391 population entries or 1,040 education ones. "Education & schools"
   is a shelf, not a choice - what a reader wants is attainment, or
   attendance, or distance to a school, and those were only reachable by
   scrolling a list of several hundred or by already knowing the wording.

     Education & schools -> Educational attainment -> Highest education
       attained -> Population Census -> Matric -> Female, urban

   Source stays a step rather than becoming a filter in small print, because
   the collision it was added for is still real: three datasets publish a
   literacy rate for the same district, measured differently and years apart.
   It sits after the family now, which is where the choice actually arises -
   only 24 of the 76 families have more than one source, and in the other 52
   the step collapses rather than asking a question with one answer.

   Levels that offer no choice are hidden, not greyed. Thirty-five of the 50
   subtopics hold exactly one family; six visible dropdowns would be five
   more than most of the index needs.

   The search box and the grouped list stay exactly as they were. This is
   another way in, not a replacement, and both end at the same choose(row).
*/
(function () {
  'use strict';

  /* Words that mean the number is already relative to something. Checked on
     the measure and the breakdown, because either half can carry it. */
  var RATE_WORDS = /%|\brates?\b|\bratios?\b|\bper\b|averag|\bavg\b|\bmedian\b|\bmean\b|\bindex\b|proportion|\bshare\b|\bpct\b|per cent|percent|\bdensity\b/i;

  /* The population itself, which is the denominator and cannot be normalised
     by itself. Only the WHOLE population: an earlier rule excluded anything
     starting with "Population", which silently took out every mother tongue -
     they are published as "Population by Mother Tongue - Balochi" - and those
     are exactly the counts a share is wanted for. Male and female populations
     stay in, because the share of a district that is female is a real
     question; the derived columns beside them (sex ratio, density, urban
     proportion, household size) are caught as rates by their own names. */
  var WHOLE_POP = /^population[\s\-\u2013\u2014]*(20\d\d)?(\s*[\u2014-]\s*all sexes)?$/i;

  var NORMS = [
    { id: 'p', mode: 'pct', label: '% of population' },
    { id: 'n', mode: 'per1000', label: 'per 1,000 people' },
  ];

  /* A heading per table, not per title. PBS retitled its tables between the
     censuses - table 6 is "Population 15+ by marital status" in 2017 and
     "Population 15 years and above by age group, sex, marital status and
     rural/urban" in 2023 - so grouping on the title gave 22 headings for 8
     tables, the same table listed twice under two names. Keyed on the number,
     with the shortest title winning so the heading stays scannable, and our
     own notes about what changed between censuses left off: they belong on
     the chart, not in a dropdown heading. */
  var HEADING = {};

  function heading(groupKey, groupLabel, topic) {
    var t = /^census_t(.+)$/.exec(groupKey || '');
    if (!t) return groupLabel || '';
    var subject = String(groupLabel || '')
      .replace(/^Table\s+\S+\s*[\u2014-]\s*/, '')
      .replace(/^20\d\d:\s*/, '');
    // Cut at the first comma, but not into nonsense: "Area, population by
    // sex, sex ratio, density..." became "Area", which names the wrong
    // column. Keep going until there are words enough to recognise it by.
    var cut = subject.split(/[,.]/)[0].trim();
    if (cut.split(/\s+/).length < 3) {
      cut = subject.split(/\s+/).slice(0, 7).join(' ').replace(/[,.]$/, '');
    }
    // Keyed on the topic as well as the number: PBS reused its numbers, so
    // table 22 is homelessness in 2017 and cooking fuel in 2023 - one
    // dictionary per number let the 2023 title label a Demographics group.
    var key = 'census_t' + t[1] + '\u001f' + (topic || '');
    // Our notes read on from "2017:", so stripping it leaves a lower-case
    // first word in a heading.
    cut = cut.charAt(0).toUpperCase() + cut.slice(1);
    if (cut && (!HEADING[key] || cut.length < HEADING[key].length)) {
      HEADING[key] = cut;
    }
    return key;
  }

  /* Named once every row has been seen, so the shortest title has had its
     chance to win. */
  function nameHeadings(index) {
    index.forEach(function (r) {
      if (HEADING[r.tableLabel]) {
        r.tableLabel = 'Table ' + r.tableLabel.split('\u001f')[0]
                         .replace('census_t', '')
                     + ' \u2014 ' + HEADING[r.tableLabel];
      }
    });
  }

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
      /* A count can be read per head; a rate already is one.

         For census rows dp carries the index's own is_rate flag - null for a
         count, 2 for a rate - and that is trustworthy. For every other source
         dp is a decimal-places hint rather than a claim about the measure:
         "Annual Growth Rate (%)" has no dp and is plainly a rate. So those
         are tested on the name as well, which is what the name is for.

         Population and area are excluded even though they are counts:
         "population per 1,000 people" is 1,000 everywhere, and land per head
         is a different question from the one this control asks. */
      var dp = col('dp', i);
      var census = col('source', i) === 'census';
      // dp is is_rate only for census rows. Elsewhere it is decimal places:
      // crop area carries dp 1 and is a count of hectares, not a rate.
      var isRate = (census && dp > 0)
        || RATE_WORDS.test(measure) || RATE_WORDS.test(metric || '');
      var countable = !isRate && !WHOLE_POP.test(measure) && !/^area\b/i.test(measure);

      /* The hierarchy's topic, not the index's own. The original topic
         column stays on the row as `srcTopic` because heading() keys census
         table numbers on it - PBS reused number 22 for homelessness in 2017
         and cooking fuel in 2023 - and because the map colour scales fall
         back to it. Navigation moved; identity did not. */
      var hTopic = col('h_topic', i) || col('topic_label', i) || topic;
      var base = {
        topic: hTopic, topicLabel: hTopic,
        sub: col('h_sub', i) || hTopic, subLabel: col('h_sub', i) || hTopic,
        fam: col('h_family', i) || '', famLabel: col('h_family', i) || '',
        mform: col('h_metric', i) || '',
        review: col('h_review', i) === 1,
        srcTopic: topic,
        ds: ds, dsLabel: ds,
        ind: measure, label: measure,
        key: col('indicator', i), groupKey: col('group_key', i),
        tableLabel: heading(col('group_key', i), col('group_label', i), topic),
        years: (list ? list('years', i) : []).join('/'),
        row: i, fullLabel: label,
        rows: col('shapes', i) ? col('shapes', i) + ' places' : '',
      };
      index.push(Object.assign({}, base, {
        metric: String(i), metricLabel: metric || 'All',
      }));
      /* Two ways to read a count against the population, because they suit
         different sizes. A share is the natural reading of mother tongue or
         religion, where the categories partition the population; per 1,000
         is the natural reading of something rare, where the share would be
         0.006% and unreadable. Both use the same denominator. */
      NORMS.forEach(function (n) {
        if (!countable) return;
        index.push(Object.assign({}, base, {
          metric: n.id + i,
          metricLabel: (metric || 'All') + '\u2002\u00b7\u2002' + n.label,
          norm: n.mode, fullLabel: label + ', ' + n.label,
        }));
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

    nameHeadings(index);

    /* Topics in the order the proposal gives - general subjects first, so a
       reader opening the list meets Population before Agriculture - and the
       order travels in the payload rather than being inferred here from
       whichever topic happens to hold the first row. Anything the payload
       does not name sorts after the named ones rather than first. */
    var ORDER = (IX.h_topic_order || []);
    var rank = function (t) {
      var i = ORDER.indexOf(t);
      return i < 0 ? ORDER.length : i;
    };
    index.sort(function (a, b) {
      return rank(a.topic) - rank(b.topic)
          || a.topic.localeCompare(b.topic)
          || a.subLabel.localeCompare(b.subLabel)
          || a.famLabel.localeCompare(b.famLabel)
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

  /* The source defaults to all of them, so opening a family shows every
     indicator in it whoever measured it - and picking one narrows rather than
     being the price of entry. */
  var ALL = function () { return window.DDExplorer.ALL; };

  /* An "All" the reader chose survives a redraw; everything else follows the
     row on screen. follow() runs after every draw, and an earlier version
     rewrote topic and dataset from the drawn row - so picking All topics
     lasted exactly one redraw before snapping back to the topic of whatever
     was showing. Subtopic and family behave the same way, because with All
     topics chosen they are the only levels left to browse across. */
  function hold(st, r) {
    var keep = function (held, next) {
      return held === ALL() || held === undefined ? ALL() : next;
    };
    st.topic = st.topic === ALL() ? ALL() : r.topic;
    st.sub = st.sub === ALL() ? ALL() : r.sub;
    st.fam = st.fam === ALL() ? ALL() : r.fam;
    st.ds = keep(st.ds, r.ds);
    st.ind = r.ind; st.metric = r.metric;
  }

  /* The row the four held values resolve to, falling back up the cascade the
     same way the rail itself does when a level's value did not survive. */
  function pick(index, st) {
    var at = function (lv, r) { return st[lv] === ALL() || r[lv] === st[lv]; };
    var inScope = function (r) {
      return at('topic', r) && at('sub', r) && at('fam', r) && at('ds', r);
    };
    return index.filter(function (r) {
      return inScope(r) && r.ind === st.ind && r.metric === st.metric;
    })[0]
    || index.filter(function (r) { return inScope(r) && r.ind === st.ind; })[0]
    || index.filter(inScope)[0];
  }


  /* One config, used by both the first mount and the rebuild that follows a
     district/tehsil switch. They were two copies of the same object literal,
     which is two places to forget a level. */
  var LEVELS = ['topic', 'sub', 'fam', 'ds', 'ind', 'metric'];

  function railCfg(host, index, st, onRow) {
    return {
      el: host, index: index, state: st,
      levels: LEVELS,
      /* Topic, subtopic, family and source can all be left at All, so a
         reader who knows only the source - or only the wording - can get to a
         series without first guessing which subject tree it was filed under.
         Indicator and metric are the series itself and always resolve. */
      optional: { topic: true, sub: true, fam: true, ds: true },
      /* ...and the three middle ones disappear when they offer one answer.
         35 of the 50 subtopics hold a single family, and 52 of the 76
         families have a single source. */
      hideSingle: { sub: true, fam: true, ds: true },
      groupOf: { ind: 'tableLabel', fam: 'subLabel' },
      labelOf: { sub: 'subLabel', fam: 'famLabel' },
      labels: { topic: 'Topic', sub: 'Subtopic', fam: 'Indicator family',
                ds: 'Source', ind: 'Indicator', metric: 'Metric' },
      allLabel: { topic: 'All topics', sub: 'All subtopics',
                  fam: 'All families', ds: 'All sources' },
      onChange: onRow,
    };
  }

  window.DDPlacesRail = {
    /* Exposed because the totals strip needs the same answer. It once used dp
       alone and so totalled a contraceptive-prevalence rate across 128
       districts to 3,635 - a number that means nothing. One test, one
       answer. */
    isRate: function (census, dp, measure, metric) {
      return (census && dp > 0)
          || RATE_WORDS.test(measure || '') || RATE_WORDS.test(metric || '');
    },

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

      var emit = function (r) { if (r) opts.onChange(r.row, r.norm || ''); };
      var rail = window.DDExplorer.mount(railCfg(host, index, st, emit));
      rail.sync(false);

      return {
        /* Draw whatever the rail is currently showing. boot() calls this when
           the URL carried no indicator, so the page opens on a real map
           instead of a rail pointing at an empty one. */
        fire: function () {
          var r = pick(index, st);
          if (r) opts.onChange(r.row, r.norm || '');
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

          rail = window.DDExplorer.mount(railCfg(host, index, st, emit));
          rail.sync(false);
          var drawn = pick(index, st) || next;
          opts.onChange(drawn.row, drawn.norm || '');
        },
      };
    },
  };
})();
