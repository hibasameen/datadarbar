/*
   places-rail.js — search, topics, featured measures, and the full library.

   THE CASCADE IS GONE. The picker used to be six dropdowns - topic, subtopic,
   family, source, indicator, metric - and a reader had to understand what
   separates a subtopic from a family before a map appeared. Education opened
   on "% Below Primary" behind 78 choices; the whole map opened on the 0-4 age
   band. The hierarchy was right and showing every level of it as a control
   was wrong.

   Three routes now lead to the same selection (the review of 29 September):

     Explore a topic    open it and a map appears at once - the topic's own
                        opening measure - with the handful most people want
                        listed beneath it.
     Browse the library every measure in the topic, under the subtopic and
                        family headings that used to be dropdowns, or the
                        same collection by source table. Nothing is capped.
     Search             across every topic, grouped by measure, with every
                        match reachable by "Load more".

   Each ends at the same choose(row), so a search result, a featured link, a
   library entry and a shared link draw exactly the same map.

   A MEASURE IS NOT A ROW. The index holds a row per published cell - the
   0-4 band, the 5-9 band - and those are breakdowns of one measure, not 103
   measures. Rows are grouped by topic, family, source, table and measure
   name. Grouping stops at the table on purpose: "Total Population" is
   published by several tables with different age bands, and folding them
   together would put two different 0-4 bands under one name. Where two
   measures in a family share a name, the table says which is which.

   Featured measures and each topic's opening view are resolved in the build
   (etl/places/build_payload.py), which fails if a key stops matching. The
   last opener lived here as a regular expression over an indicator key, and
   a merge rewrote the key without anything noticing.
*/
(function () {
  'use strict';

  var PAGE = 40;       // library groups per "Load more", not a ceiling
  var NOUN = { district: 'district', tehsil: 'tehsil' };

  function esc(t) {
    return String(t == null ? '' : t).replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  /* The years as a phrase. Census pairs carry a stored or computable change;
     crops run to 39 fiscal years and read as a span. */
  function yearsText(ys) {
    if (!ys.length) return '';
    var plain = ys.filter(function (y) { return !/^Δ/.test(y); });
    var change = plain.length < ys.length;
    if (plain.length > 3) return plain[0] + ' to ' + plain[plain.length - 1];
    return plain.join(', ') + (change ? ' and change' : '');
  }

  /* The index sometimes appends its own disambiguation - "Total Population ·
     2017/2023/Δ2017-23" - which is a note for a dropdown, not a name. */
  function tidy(s) {
    return String(s || '').replace(/\s*·\s*(19|20)\d\d[\/0-9Δ\-]*$/, '');
  }

  /* SOURCE TOTALS STAY FINDABLE. 463 rows are a table's own total or column
     heading - usually the denominator its other rows are shares of - so they
     are kept out of the subject headings, where they would stand at the top
     of every family as a duplicate of the population. But a denominator must
     not disappear for looking like a total: they sit under their own table's
     topic in a section named for what they are, and search finds them. Their
     topic is the one the rest of their table is filed under; every one of the
     463 has such a sibling. */
  var TOTALS = 'Source totals and denominators';

  function tableTopics(N, col, level) {
    var count = {};
    for (var i = 0; i < N; i++) {
      if (col('level', i) !== level || col('redundant', i) === 1) continue;
      var k = col('group_key', i), t = col('h_topic', i);
      if (!t) continue;
      var c = count[k] = count[k] || {};
      c[t] = (c[t] || 0) + 1;
    }
    var out = {};
    Object.keys(count).forEach(function (k) {
      out[k] = Object.keys(count[k]).sort(function (a, b) {
        return count[k][b] - count[k][a];
      })[0];
    });
    return out;
  }

  function build(IX, N, col, list, level) {
    var measures = [], byKey = {}, byRow = {};
    var tt = tableTopics(N, col, level);
    for (var i = 0; i < N; i++) {
      if (col('level', i) !== level) continue;
      var red = col('redundant', i) === 1;
      var topic = red ? (tt[col('group_key', i)] || col('topic_label', i))
                      : (col('h_topic', i) || col('topic_label', i) || col('topic', i));
      var fam = red ? (col('group_label', i) || '') : (col('h_family', i) || '');
      var ds = col('dataset', i) || 'Unattributed';
      var name = tidy(col('measure', i) || col('label', i));
      var gk = col('group_key', i);
      var k = [topic, fam, ds, gk, name, red ? 't' : ''].join('\u001f');
      var m = byKey[k];
      if (!m) {
        m = byKey[k] = {
          id: measures.length, topic: topic,
          sub: red ? TOTALS : (col('h_sub', i) || topic),
          fam: fam, ds: ds, groupKey: gk, groupLabel: col('group_label', i) || '',
          name: name, label: name, rows: [], years: [], shapes: 0,
          source: col('source', i), review: false, total: red,
        };
        measures.push(m);
      }
      var metric = col('metric', i) || '';
      m.rows.push({ row: i, metric: metric || 'All' });
      list('years', i).forEach(function (y) {
        if (m.years.indexOf(y) < 0) m.years.push(y);
      });
      m.shapes = Math.max(m.shapes, col('shapes', i) || 0);
      if (col('h_review', i) === 1) m.review = true;
      byRow[i] = m;
    }
    /* Two measures with one name in one family are two tables. Say which. */
    var clash = {};
    measures.forEach(function (m) {
      var c = m.topic + '\u001f' + m.fam + '\u001f' + m.ds + '\u001f' + m.name;
      (clash[c] = clash[c] || []).push(m);
    });
    Object.keys(clash).forEach(function (c) {
      if (clash[c].length < 2) return;
      var sameLabel = clash[c].every(function (m) {
        return m.groupLabel === clash[c][0].groupLabel;
      });
      clash[c].forEach(function (m) {
        var t = /^census_t(.+)$/.exec(m.groupKey || '');
        // Two curated groups can publish one measure under one group title,
        // and then the title separates nothing; how many places it reaches
        // does.
        m.label = m.name + (t ? ' \u00b7 table ' + t[1]
          : sameLabel ? ' \u00b7 ' + m.shapes + ' places'
          : ' \u00b7 ' + m.groupLabel);
      });
    });
    measures.forEach(function (m) {
      m.years.sort();
      // Breakdowns in the order a reader expects: "All" first, then bands in
      // numeric order rather than "10-14" before "5-9".
      m.rows.sort(function (a, b) {
        if (a.metric === 'All') return -1;
        if (b.metric === 'All') return 1;
        return a.metric.localeCompare(b.metric, undefined, { numeric: true });
      });
    });
    return { measures: measures, byRow: byRow };
  }

  /* PLAIN WORDS FIND SOURCE WORDS. Searching "power", "language", "jobless"
     or "oosc" found nothing, because no published label uses them; "out of
     school" found 5 entries where "out-of-school" found 108, and "sanitation"
     found 1 where "toilet" found 273. Each group below is searched as one:
     the reader's own word still ranks first, and the source's wording stays
     searchable exactly as printed. "Schooling" is deliberately broad - it
     could mean attendance, enrolment or attainment, and pretending it names
     one of them would hide the other two. */
  var ALIASES = [
    ['literacy', 'literate', 'illiterate'],
    ['out of school', 'oosc', 'never attended'],
    ['electricity', 'power', 'lighting'],
    ['mother tongue', 'language'],
    ['sanitation', 'toilet', 'washroom', 'open defecation'],
    ['immunisation', 'immunization', 'immunised', 'vaccination', 'vaccine'],
    ['poverty', 'mpi', 'headcount', 'deprived'],
    ['unemployment', 'unemployed', 'jobless', 'seeking work'],
    ['migration', 'migrant', 'emigrant', 'emigrants'],
    ['schooling', 'attendance', 'attended', 'enrolment', 'attainment'],
    ['internet', 'online'],
    ['phone', 'mobile', 'smartphone'],
    ['housing', 'house', 'dwelling'],
    ['water', 'drinking water', 'piped'],
  ];
  function norm(s) { return String(s).toLowerCase().replace(/[-\u2013\u2014]/g, ' ').replace(/\s+/g, ' '); }
  function expand(q) {
    var out = [q];
    ALIASES.forEach(function (g) {
      if (g.some(function (w) { return q.indexOf(w) >= 0; })) {
        g.forEach(function (w) {
          g.forEach(function (from) {
            if (q.indexOf(from) < 0) return;
            var alt = q.replace(from, w);
            if (out.indexOf(alt) < 0) out.push(alt);
          });
        });
      }
    });
    return out;
  }

  /* Relevance for search. The census outnumbers everything else forty to one,
     so an unranked "literacy" opens on "10 -- 14 · FORMAL". */
  function score(m, q, alts) {
    var name = norm(m.label), at = name.indexOf(q), s = 0;
    // An alias match counts, but the reader's own word first.
    if (at < 0 && alts) {
      alts.some(function (a) { at = name.indexOf(a); return at >= 0; });
      if (at >= 0) s -= 15;
    }
    if (name === q) s += 200;
    if (at === 0) s += 100;
    else if (at > 0) s += /[^a-z0-9]/.test(name.charAt(at - 1)) ? 70 : 50;
    else if (m.fam.toLowerCase().indexOf(q) >= 0) s += 30;
    if (m.source !== 'census') s += 25;          // one of ~300 chosen by hand
    s -= Math.min(20, name.length / 6);           // the thing itself, not a cell in it
    s += Math.min(10, m.shapes / 15);             // coverage breaks ties, never buries
    if (m.total) s -= 40;                         // a denominator, after the measures
    return s;
  }

  function matches(m, q, col) {
    var alts = Array.isArray(q) ? q : [q];
    var hay = norm(m.label + ' ' + m.fam + ' ' + m.sub + ' ' + m.topic + ' '
                   + m.ds + ' ' + m.groupLabel);
    if (alts.some(function (a) { return hay.indexOf(a) >= 0; })) return true;
    // The source's own spelling stays searchable: someone who knows a series
    // by what PBS printed must still find it by typing that.
    return m.rows.some(function (r) {
      var raw = norm(col('indicator', r.row) + ' ' + col('label', r.row));
      return alts.some(function (a) { return raw.indexOf(a) >= 0; });
    });
  }

  window.DDPlacesRail = {
    // For tests/test_places_preservation.py, which checks that every row the
    // index holds is reachable from the library.
    _build: build,
    canTotal: function (row) { return row === 1 || row === true; },

    mount: function (opts) {
      var IX = opts.IX, N = opts.N, col = opts.col, list = opts.list;
      var host = opts.el, lib = opts.libraryEl, search = opts.searchEl;
      var level = opts.level;
      var built = build(IX, N, col, list, level);
      var other = build(IX, N, col, list, level === 'district' ? 'tehsil' : 'district');
      var order = IX.h_topic_order || [];
      var open = null, current = null, libState = null, invoker = null;
      var pending = null;

      function featured(topic) {
        var rows = ((IX.featured || {})[level] || {})[topic] || [];
        var seen = {}, out = [];
        rows.forEach(function (r) {
          var m = built.byRow[r];
          if (m && !seen[m.id]) { seen[m.id] = 1; out.push({ m: m, row: r }); }
        });
        return out;
      }

      function opener(topic) {
        var f = featured(topic)[0];
        if (f) return f.row;
        var m = built.measures.filter(function (x) { return x.topic === topic; })[0];
        return m ? m.rows[0].row : null;
      }

      // Measures a reader browses; the source totals are counted in the
      // library, where they sit in their own section.
      function count(topic, b) {
        return b.measures.filter(function (m) {
          return m.topic === topic && !m.total;
        }).length;
      }

      /* ── the topic list ──────────────────────────────────────────────── */
      function drawTopics() {
        var h = '<h2 class="rail-cap">Topics</h2><ul class="topics">';
        order.forEach(function (t) {
          var n = count(t, built), elsewhere = count(t, other);
          if (!n && !elsewhere) return;
          var isOpen = open === t;
          h += '<li class="topic' + (isOpen ? ' is-open' : '') + '">'
             + '<button type="button" class="topic-btn" data-topic="' + esc(t) + '"'
             + ' aria-expanded="' + isOpen + '">'
             + '<span class="topic-name">' + esc(t) + '</span>'
             + '<span class="topic-n">' + (n ? n : NOUN[level === 'district'
                 ? 'tehsil' : 'district'] + 's only') + '</span></button>';
          if (isOpen) h += drawOpen(t, n);
          h += '</li>';
        });
        h += '</ul>';
        host.innerHTML = h;
        host.querySelectorAll('.topic-btn').forEach(function (b) {
          b.onclick = function () {
            var t = b.dataset.topic;
            if (open === t) { open = null; drawTopics(); return; }
            open = t;
            var r = opener(t);
            if (r != null) opts.onChange(r, '');
            else drawTopics();     // measured at the other level only
          };
        });
        host.querySelectorAll('[data-row]').forEach(function (b) {
          b.onclick = function () { opts.onChange(Number(b.dataset.row), ''); };
        });
        host.querySelectorAll('[data-browse]').forEach(function (b) {
          b.onclick = function () {
            openLibrary({ topic: b.dataset.browse, q: '' }, b);
          };
        });
        host.querySelectorAll('[data-switch]').forEach(function (b) {
          b.onclick = function () {
            // The reader asked for this topic, so the other level opens on it
            // rather than on whatever was on screen before.
            pending = b.dataset.topic || null;
            opts.onLevel(b.dataset.switch);
          };
        });
      }

      function drawOpen(t, n) {
        if (!n) {
          var lv = level === 'district' ? 'tehsil' : 'district';
          return '<div class="topic-body"><p class="topic-note">'
               + esc(t) + ' is measured for ' + lv + 's, not ' + level + 's.</p>'
               + '<button type="button" class="topic-link" data-switch="' + lv
               + '" data-topic="' + esc(t) + '">Open the ' + lv + ' map</button></div>';
        }
        var f = featured(t), h = '<div class="topic-body"><ul class="featured">';
        f.forEach(function (x) {
          var on = current && built.byRow[current] === x.m;
          h += '<li><button type="button" class="feat' + (on ? ' is-on' : '') + '"'
             + ' data-row="' + x.row + '"' + (on ? ' aria-current="true"' : '') + '>'
             + '<span class="feat-name">' + esc(x.m.name) + '</span>'
             + '<span class="feat-meta">' + esc(shortSource(x.m)) + '</span>'
             + '</button></li>';
        });
        h += '</ul>';
        // The measure on screen, when it came from the library, is shown here
        // too - a list that hides the thing you are looking at loses your place.
        var cm = current != null ? built.byRow[current] : null;
        if (cm && cm.topic === t && !f.some(function (x) { return x.m === cm; })) {
          h += '<p class="feat-also">Showing <b>' + esc(cm.label) + '</b></p>';
        }
        h += '<button type="button" class="topic-link" data-browse="' + esc(t) + '">'
           + 'Browse all ' + n.toLocaleString() + ' measures in this topic \u2192'
           + '</button></div>';
        return h;
      }

      /* The source as a reader would name it. The full dataset name - with
         "rural households only" and the retrieval date - is in the line under
         the measure once it is on the map, where it qualifies the number; in
         a list it only has to tell two entries apart. */
      function shortSource(m) {
        var y = yearsText(m.years);
        var ds = m.ds;
        var rural = /rural households only/i.test(ds);
        ds = ds.split(/ \u00b7 |, via |, latest |, pull of |, rural households only| \(/)[0]
               .replace(/^Population Census$/, 'Census')
               .replace(/^Bureau of Emigration & Overseas Employment$/, 'Bureau of Emigration')
               .replace(/^Adaad school layer.*/, 'School locations')
               .replace(/^VIIRS DNB$/, 'VIIRS night lights');
        if (rural) ds += ', rural';
        return ds + (y && ds.indexOf(y) < 0 ? ' \u00b7 ' + y : '');
      }

      /* ── the library ─────────────────────────────────────────────────── */
      function openLibrary(st, from) {
        libState = Object.assign({ view: 'subject', shown: PAGE, topic: open, q: '' },
                                 libState && libState.keep ? libState : {}, st);
        invoker = from || invoker;
        lib.hidden = false;
        drawLibrary();
        var box = lib.querySelector('.lib-q');
        if (box && !st.keepFocus) {
          box.focus();
          box.setSelectionRange(box.value.length, box.value.length);
        }
      }

      function closeLibrary() {
        lib.hidden = true;
        if (search && libState && libState.q) search.value = '';
        libState = null;
        if (invoker && invoker.focus && document.contains(invoker)) invoker.focus();
      }

      function libRows() {
        var q = norm((libState.q || '').trim());
        var pool = built.measures.filter(function (m) {
          return !libState.topic || m.topic === libState.topic;
        });
        if (!q) return { list: pool, q: '' };
        var alts = expand(q);
        libState.alts = alts.slice(1);
        var hits = pool.filter(function (m) { return matches(m, alts, col); })
          .map(function (m) { return { m: m, s: score(m, q, alts) }; })
          .sort(function (a, b) { return b.s - a.s; })
          .map(function (x) { return x.m; });
        return { list: hits, q: q };
      }

      function drawLibrary() {
        var got = libRows(), ms = got.list, q = got.q;
        var scope = libState.topic || 'All topics';
        var series = ms.reduce(function (a, m) { return a + m.rows.length; }, 0);
        var h = '<header class="lib-head">'
          + '<div class="lib-title"><h2>' + esc(q ? 'Search' : scope) + '</h2>'
          + '<button type="button" class="lib-close" aria-label="Close">×</button></div>'
          + '<label class="lib-search"><input class="lib-q" type="search" value="'
          + esc(libState.q) + '" placeholder="Search '
          + esc(libState.topic ? 'within ' + libState.topic : 'every topic')
          + '…" aria-label="Search measures"/></label>'
          + '<div class="lib-ctl">'
          + '<span class="lib-scope">'
          + (libState.topic
              ? '<button type="button" data-scope="topic" aria-pressed="true">'
                + esc(libState.topic) + '</button>'
                + '<button type="button" data-scope="all" aria-pressed="false">All topics</button>'
              : '<button type="button" data-scope="all" aria-pressed="true">All topics</button>'
                + (open ? '<button type="button" data-scope="topic" aria-pressed="false">'
                          + esc(open) + '</button>' : ''))
          + '</span>'
          + (q ? '' : '<span class="lib-view">'
              + '<button type="button" data-view="subject" aria-pressed="'
              + (libState.view === 'subject') + '">By subject</button>'
              + '<button type="button" data-view="source" aria-pressed="'
              + (libState.view === 'source') + '">By source table</button></span>')
          + '</div>'
          + '<p class="lib-count">' + (ms.length
              ? ms.length.toLocaleString() + ' measure' + (ms.length === 1 ? '' : 's')
                + (series > ms.length ? ' · ' + series.toLocaleString()
                   + ' detailed series' : '')
                + (q ? ' match \u201c' + esc(libState.q) + '\u201d'
                     + (libState.alts && libState.alts.length
                        ? ' (also searching ' + esc(libState.alts.slice(0, 4).join(', ')) + ')'
                        : '') : '')
                + ' · ' + level + ' map'
              : (q ? 'Nothing here matches “' + esc(libState.q) + '”.' : ''))
          + '</p></header><div class="lib-body">';

        /* Sorted before it is paged, so "Load more" continues the list rather
           than dropping entries into sections already read. Sections run in
           the order a reader looks for them: the one holding the topic's
           opening measure first, then the other featured ones, then the rest
           alphabetically - and reference populations last, where someone
           looking for a denominator will look. */
        var a = libState.view === 'source' ? 'ds' : 'sub';
        var b = libState.view === 'source' ? 'groupLabel' : 'fam';
        if (!q) {
          var feat = featured(libState.topic || open || '').map(function (x) { return x.m; });
          var rankOf = {};
          feat.forEach(function (m, k) {
            if (!(m[a] in rankOf)) rankOf[m[a]] = k;
          });
          var last = function (v) {
            return v === TOTALS ? 2 : /population bases|reference/i.test(v) ? 1 : 0;
          };
          ms = ms.slice().sort(function (x, y) {
            var rx = x[a] in rankOf ? rankOf[x[a]] : 999;
            var ry = y[a] in rankOf ? rankOf[y[a]] : 999;
            return last(x[a]) - last(y[a]) || rx - ry
                || String(x[a]).localeCompare(String(y[a]))
                || String(x[b]).localeCompare(String(y[b]))
                || x.label.localeCompare(y.label, undefined, { numeric: true });
          });
        }
        var shown = ms.slice(0, libState.shown);
        if (q) {
          h += shown.map(item).join('');
        } else {
          // Headings are the old dropdowns: subtopic and family by subject,
          // dataset and table by source. They are headings now, not steps.
          var lastA = null, lastB = null;
          shown.forEach(function (m) {
            if (m[a] !== lastA) {
              if (lastA !== null) h += '</section>';
              h += '<section class="lib-sec"><h3>' + esc(m[a]) + '</h3>';
              lastA = m[a]; lastB = null;
            }
            if (m[b] && m[b] !== lastB) {
              h += '<h4>' + esc(m[b]) + '</h4>';
              lastB = m[b];
            }
            h += item(m);
          });
          if (lastA !== null) h += '</section>';
        }
        if (ms.length > shown.length) {
          h += '<button type="button" class="lib-more">Load '
             + Math.min(PAGE, ms.length - shown.length) + ' more of '
             + (ms.length - shown.length).toLocaleString() + ' remaining</button>';
        }
        /* A measure that exists only at the other geography is not absent -
           it is on the other map. Say so rather than letting the level make
           it look like it does not exist. */
        if (q) {
          var elsewhere = other.measures.filter(function (m) {
            return (!libState.topic || m.topic === libState.topic)
                && matches(m, expand(q), col);
          }).filter(function (m) {
            return !built.measures.some(function (x) {
              return x.groupKey === m.groupKey && x.name === m.name;
            });
          });
          if (elsewhere.length) {
            var lv = level === 'district' ? 'tehsil' : 'district';
            h += '<section class="lib-sec lib-else"><h3>Also on the ' + lv + ' map</h3>'
               + elsewhere.slice(0, 12).map(function (m) {
                   return '<div class="lib-item is-else"><span class="lib-name">'
                     + esc(m.label) + '</span><span class="lib-meta">'
                     + esc(m.topic + ' · ' + shortSource(m)) + '</span></div>';
                 }).join('')
               + '<button type="button" class="topic-link" data-switch="' + lv
               + '">Open the ' + lv + ' map</button></section>';
          }
        }
        h += '</div>';
        lib.innerHTML = h;
        wireLibrary();
      }

      function item(m) {
        var on = current != null && built.byRow[current] === m;
        var n = m.rows.length;
        var meta = [shortSource(m), m.shapes.toLocaleString() + ' '
                    + NOUN[level] + 's with data'];
        if (!libState.topic || libState.q) meta.unshift(m.topic);
        var h = '<div class="lib-item' + (on ? ' is-on' : '') + '">'
          + '<button type="button" class="lib-pick" data-row="' + m.rows[0].row + '"'
          + (on ? ' aria-current="true"' : '') + '>'
          + '<span class="lib-name">' + esc(m.label)
          + (m.review ? ' <span class="ind-flag" title="The source label for this'
              + ' series is ambiguous and has not been resolved. It is kept exactly'
              + ' as published rather than guessed at.">definition needs review</span>'
              : '')
          + '</span><span class="lib-meta">' + esc(meta.join(' · ')) + '</span>'
          + '</button>';
        if (n > 1) {
          // Every published band stays one click away, including the
          // overlapping ones, without standing as a measure of its own.
          h += '<details class="lib-bands"><summary>' + n + ' breakdowns</summary>'
             + '<div class="lib-band-list">'
             + m.rows.map(function (r) {
                 return '<button type="button" data-row="' + r.row + '"'
                   + (current === r.row ? ' aria-current="true"' : '') + '>'
                   + esc(r.metric) + '</button>';
               }).join('') + '</div></details>';
        }
        return h + '</div>';
      }

      function wireLibrary() {
        lib.querySelector('.lib-close').onclick = closeLibrary;
        var box = lib.querySelector('.lib-q');
        box.oninput = function () {
          libState.q = box.value;
          libState.shown = PAGE;
          if (search) search.value = libState.topic ? '' : box.value;
          var pos = box.selectionStart;
          drawLibrary();
          var nb = lib.querySelector('.lib-q');
          nb.focus();
          nb.setSelectionRange(pos, pos);
        };
        lib.querySelectorAll('[data-scope]').forEach(function (b) {
          b.onclick = function () {
            libState.topic = b.dataset.scope === 'topic' ? (libState.topic || open) : null;
            libState.shown = PAGE;
            drawLibrary();
          };
        });
        lib.querySelectorAll('[data-view]').forEach(function (b) {
          b.onclick = function () { libState.view = b.dataset.view; drawLibrary(); };
        });
        var more = lib.querySelector('.lib-more');
        if (more) more.onclick = function () {
          libState.shown += PAGE;
          drawLibrary();
          var items = lib.querySelectorAll('.lib-item');
          var nxt = items[libState.shown - PAGE];
          if (nxt) { var bt = nxt.querySelector('button'); if (bt) bt.focus(); }
        };
        lib.querySelectorAll('[data-row]').forEach(function (b) {
          b.onclick = function () {
            opts.onChange(Number(b.dataset.row), '');
            closeLibrary();
          };
        });
        lib.querySelectorAll('[data-switch]').forEach(function (b) {
          b.onclick = function () {
            var q = libState.q;
            closeLibrary();
            opts.onLevel(b.dataset.switch);
            if (q) openLibrary({ topic: null, q: q });
          };
        });
      }

      lib.addEventListener('keydown', function (e) {
        if (e.key === 'Escape') { e.preventDefault(); closeLibrary(); }
      });
      if (search) {
        search.addEventListener('input', function () {
          var q = search.value;
          if (q.trim().length < 2) {
            if (libState && !libState.topic) closeLibrary();
            return;
          }
          openLibrary({ topic: null, q: q, shown: PAGE, keepFocus: true }, search);
          search.focus();
        });
        search.addEventListener('keydown', function (e) {
          if (e.key === 'Enter' && search.value.trim().length >= 2) {
            var box = lib.querySelector('.lib-q');
            if (box) box.focus();
          }
        });
      }

      drawTopics();

      var api = {
        /* What the opening map shows. */
        fire: function () {
          open = 'Population & households';
          var r = opener(open);
          if (r == null) {
            open = order.filter(function (t) { return count(t, built); })[0];
            r = opener(open);
          }
          if (r != null) opts.onChange(r, '');
        },
        follow: function (row) {
          current = row;
          var m = built.byRow[row];
          if (m) open = m.topic;
          drawTopics();
          if (libState && !lib.hidden) drawLibrary();
        },
        /* The measure a row belongs to, for the breakdown control beside the
           legend: the ages, the tables' own columns. */
        measureOf: function (row) { return built.byRow[row] || null; },
        rebuild: function (lv, prevKey, prevGroup) {
          level = lv;
          built = build(IX, N, col, list, level);
          other = build(IX, N, col, list, level === 'district' ? 'tehsil' : 'district');
          var keep = null;
          if (prevKey) {
            for (var i = 0; i < N; i++) {
              if (col('level', i) === lv && col('indicator', i) === prevKey
                  && col('group_key', i) === prevGroup && col('redundant', i) !== 1) {
                keep = i; break;
              }
            }
          }
          if (pending) { open = pending; keep = null; pending = null; }
          var r = keep != null ? keep : (open && opener(open));
          if (r == null) { api.fire(); return; }
          opts.onChange(r, '');
        },
        openLibrary: function (topic) { openLibrary({ topic: topic || open, q: '' }); },
      };
      return api;
    },
  };
})();
