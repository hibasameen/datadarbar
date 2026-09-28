/*
   explorer.js — the cascading dropdowns shared by Places, Economy and State.

   One index, a list of levels, and a select per level. The ORDER of the levels
   is the page's choice, because the three explorers are asked different
   questions:

     Places   topic -> dataset -> indicator
              "child mortality ... who measured it ... which cut"
     Economy  topic -> chart          (+ a series search across everything)
     State    topic -> chart

   Each select is filled from the rows the levels ABOVE it allow, so the rail
   can never offer a dead combination. Each is filled from `state`, never from
   a row resolved first: an earlier version looked the row up before filling,
   and because that lookup matched on topic ahead of dataset, choosing a new
   dataset was silently overridden by the topic still held from the old one.

   An index row carries at least {topic, topicLabel, ds, dsLabel, ind, label}
   plus whatever the page needs (chart, years, rows, theme).
*/
(function () {
  'use strict';

  var CAPTION = { topic: 'Topic', ds: 'Dataset', ind: 'Indicator',
                  metric: 'Metric' };
  var ALL_LABEL = { topic: 'All topics', ds: 'All datasets' };
  var LABEL_OF = { topic: 'topicLabel', ds: 'dsLabel', ind: 'label',
                   metric: 'metricLabel' };

  /* A level whose name is not one of the four above brings its own label
     field and caption: Places added subtopic, family and source when it took
     the proposed hierarchy, and a level with no entry here would have shown
     its raw key as every option. */
  function labelKey(lv, cfg) {
    return (cfg.labelOf && cfg.labelOf[lv]) || LABEL_OF[lv] || lv;
  }

  function uniq(rows, key, labelKey, groupKey) {
    var seen = {}, out = [];
    rows.forEach(function (r) {
      if (!(r[key] in seen)) {
        seen[r[key]] = 1;
        out.push({ value: r[key], label: r[labelKey] || r[key],
                   theme: groupKey ? r[groupKey] : r.theme });
      }
    });
    return out;
  }

  function option(o) {
    var el = document.createElement('option');
    el.value = o.value;
    el.textContent = o.label;
    return el;
  }

  function fill(sel, opts, value, group) {
    sel.innerHTML = '';
    if (group && opts.some(function (o) { return o.theme; })) {
      var themes = [];
      opts.forEach(function (o) {
        var t = o.theme || 'Other';
        if (themes.indexOf(t) < 0) themes.push(t);
      });
      themes.forEach(function (t) {
        var g = document.createElement('optgroup');
        g.label = t;
        opts.filter(function (o) { return (o.theme || 'Other') === t; })
          .forEach(function (o) { g.appendChild(option(o)); });
        sel.appendChild(g);
      });
    } else {
      opts.forEach(function (o) { sel.appendChild(option(o)); });
    }
    sel.value = value;
    // A value that did not survive the narrowing falls back to the first.
    if (sel.selectedIndex < 0 && opts.length) sel.value = opts[0].value;
    return sel.value;
  }

  function field(caption, id) {
    var wrap = document.createElement('label');
    wrap.className = 'xf';
    var cap = document.createElement('span');
    cap.className = 'xf-label';
    cap.textContent = caption;
    var sel = document.createElement('select');
    sel.className = 'xf-select';
    sel.id = id;
    wrap.appendChild(cap);
    wrap.appendChild(sel);
    return { wrap: wrap, sel: sel, cap: cap };
  }

  function mount(cfg) {
    var index = cfg.index, state = cfg.state, el = cfg.el;
    var levels = cfg.levels || ['ds', 'topic', 'ind'];
    var names = cfg.labels || {};

    el.innerHTML = '';
    el.classList.add('xrail');

    /* A level can render as a list instead of a select. Economy and State put
       the topic in a dropdown and then show that topic's charts as a list,
       because the list is the thing you browse - a chart is a page you might
       want, not a value you set. Places keeps all three as selects: 725
       indicators under one dataset is not a list anyone reads. */
    var listLevel = cfg.listLevel;

    var fields = levels.filter(function (lv) { return lv !== listLevel; })
      .map(function (lv) {
        var f = field(names[lv] || CAPTION[lv] || lv,
                      'x' + lv.charAt(0).toUpperCase() + lv.slice(1));
        f.level = lv;
        el.appendChild(f.wrap);
        return f;
      });

    function fieldFor(lv) {
      return fields.filter(function (f) { return f.level === lv; })[0];
    }

    var meta = document.createElement('p');
    meta.className = 'xrail-meta';
    el.appendChild(meta);

    /* A level marked optional gets an "All" entry that does not narrow. The
       cascade was hiding most of a topic behind a choice nobody knew they had
       to make: Women & gender holds 18 indicators over three surveys, and the
       dataset step showed whichever sorted first, so you saw DHS's five and
       had no sign the other thirteen existed. */
    var ALL = window.DDExplorer.ALL;
    var optional = cfg.optional || {};
    var groupOf = cfg.groupOf || {};
    var hideSingle = cfg.hideSingle || {};
    /* "All families", not "All familys". The fallback pluralises the caption,
       which is right for topics and datasets and wrong for anything else, so
       a page with its own levels names its own. */
    function allLabel(lv) {
      return (cfg.allLabel && cfg.allLabel[lv]) || ALL_LABEL[lv]
          || ('All ' + (names[lv] || CAPTION[lv] || lv).toLowerCase() + 's');
    }

    function sync(fire) {
      var rows = index;
      levels.forEach(function (lv, i) {
        /* A level can group its options under headings. 180 census indicators
           in one flat list is unreadable; the same 180 under the eight tables
           that published them is a list you can scan. */
        var opts = uniq(rows, lv, labelKey(lv, cfg), groupOf[lv]);
        /* Whether this level is worth a row of the rail, decided on the real
           options before All is added - otherwise every optional level has
           two entries and none ever collapses. */
        var forced = !!hideSingle[lv] && opts.length < 2;
        // Always, not only when there is a choice. With the All option
        // dropped on a single-dataset topic, fill() fell back to that one
        // dataset and wrote it into state - so an All that the reader never
        // cancelled was lost the moment they passed through such a topic,
        // and every topic after it showed one table's charts.
        // A forced level is the exception: an All nobody can see is an All
        // nobody can cancel, and with one option it means the same thing.
        if (optional[lv] && !forced) {
          opts = [{ value: ALL, label: allLabel(lv) }].concat(opts);
        }
        if (lv === listLevel) {
          state[lv] = drawList(rows, lv, state[lv]);
        } else {
          var f = fieldFor(lv);
          state[lv] = fill(f.sel, opts, state[lv], i === 0 || !!groupOf[lv]);
          // A select with one option stays visible but disabled, so the rail
          // keeps its shape as you move between datasets.
          f.sel.disabled = opts.length < 2;
          /* Unless the page asks for it to go. A level that offers no choice
             is a row of furniture on Economy, where the rail is three fields
             deep; on Places it is six deep and 35 of the 50 subtopics hold
             exactly one family, so the Family row would sit there forced and
             greyed for most of the index. It still sets state - the cascade
             is unchanged - it just does not take a line. */
          f.wrap.hidden = forced;
          f.cap.textContent = (names[lv] || CAPTION[lv] || lv)
            + (opts.length > 1 ? '\u2002\u00b7\u2002' + opts.length : '');
        }
        if (state[lv] !== ALL) {
          rows = rows.filter(function (r) { return r[lv] === state[lv]; });
        }
      });

      var chosen = rows[0] || index[0];
      meta.textContent = chosen
        ? [chosen.dsLabel, chosen.years, chosen.rows].filter(Boolean).join(' · ')
        : '';
      more(chosen);
      if (fire && cfg.onChange) cfg.onChange(chosen);
      return chosen;
    }

    /* The charts in the current topic, as a list. The one on screen is marked
       rather than removed: a list that hides the thing you are looking at
       makes you lose your place in it. */
    function drawList(rows, lv, held) {
      var host = cfg.listEl;
      var opts = uniq(rows, lv, labelKey(lv, cfg));
      var value = opts.some(function (o) { return o.value === held; })
        ? held : (opts[0] && opts[0].value);
      if (!host) return value;
      host.innerHTML = '';
      opts.forEach(function (o) {
        var row = rows.filter(function (r) { return r[lv] === o.value; })[0] || {};
        var b = document.createElement('button');
        b.type = 'button';
        b.className = 'xlist-item' + (o.value === value ? ' is-on' : '');
        b.setAttribute('aria-current', o.value === value ? 'true' : 'false');
        b.innerHTML = '<span class="xlist-name"></span>'
                    + '<span class="xlist-meta"></span>';
        b.querySelector('.xlist-name').textContent = o.label;
        b.querySelector('.xlist-meta').textContent =
          [row.dsLabel, row.years].filter(Boolean).join(' \u00b7 ');
        b.onclick = function () { state[lv] = o.value; sync(true); };
        host.appendChild(b);
      });
      return value;
    }

    /* Type-to-find across every row, whatever topic it sits under, because
       someone who knows a series by name should not have to know which topic
       it was filed under first. */
    function wireSearch() {
      var box = cfg.searchEl;
      if (!box) return;
      var out = document.createElement('div');
      out.className = 'xfind';
      out.hidden = true;
      box.parentNode.insertBefore(out, box.nextSibling);

      box.addEventListener('input', function () {
        var q = box.value.trim().toLowerCase();
        if (q.length < 2) { out.hidden = true; out.innerHTML = ''; return; }
        var hits = index.filter(function (r) {
          return (r.label + ' ' + r.topicLabel + ' ' + r.dsLabel)
            .toLowerCase().indexOf(q) >= 0;
        }).slice(0, 12);
        out.hidden = !hits.length;
        out.innerHTML = '';
        hits.forEach(function (r) {
          var b = document.createElement('button');
          b.type = 'button';
          b.className = 'xfind-item';
          b.innerHTML = '<span class="xfind-name"></span>'
                      + '<span class="xfind-meta"></span>';
          b.querySelector('.xfind-name').textContent = r.label;
          b.querySelector('.xfind-meta').textContent =
            r.topicLabel + ' \u00b7 ' + r.dsLabel;
          b.onclick = function () {
            levels.forEach(function (lv) { state[lv] = r[lv]; });
            box.value = '';
            out.hidden = true;
            out.innerHTML = '';
            sync(true);
          };
          out.appendChild(b);
        });
      });
      box.addEventListener('blur', function () {
        setTimeout(function () { out.hidden = true; }, 160);
      });
    }

    /* "Also in this topic", under the chart. A topic is a subject rather than
       a table, so the siblings are everything filed under this topic whatever
       dataset it comes from. Rendered by the same sync that fills the selects,
       so the strip and the dropdowns cannot disagree. */
    function more(chosen) {
      var host = cfg.moreEl;
      if (!host || !chosen) return;
      var sibs = index.filter(function (r) {
        return r.topic === chosen.topic && r.ind !== chosen.ind;
      });
      if (!sibs.length) { host.innerHTML = ''; host.hidden = true; return; }
      host.hidden = false;
      host.innerHTML = '<div class="xmore-label">Also in this topic</div>'
                     + '<div class="xmore-list"></div>';
      var list = host.querySelector('.xmore-list');
      sibs.slice(0, cfg.moreMax || 6).forEach(function (r) {
        var b = document.createElement('button');
        b.type = 'button';
        b.className = 'xmore-item';
        b.innerHTML = '<span class="xmore-spark" aria-hidden="true"></span>'
                    + '<span class="xmore-text"><span class="xmore-name"></span>'
                    + '<span class="xmore-ds"></span></span>';
        b.querySelector('.xmore-spark').innerHTML = spark(r.chart || r.ind);
        b.querySelector('.xmore-name').textContent = r.label;
        b.querySelector('.xmore-ds').textContent = r.note || r.dsLabel;
        b.onclick = function () {
          levels.forEach(function (lv) { state[lv] = r[lv]; });
          sync(true);
        };
        list.appendChild(b);
      });
    }

    /* A three-bar glyph per sibling card, as the design shows. It is
       decoration, not data - it never claims to be the shape of that series. */
    function spark(seed) {
      var s = String(seed), h = 0;
      for (var i = 0; i < s.length; i++) h = (h * 31 + s.charCodeAt(i)) % 997;
      var bars = [0, 1, 2].map(function (i) { return 7 + ((h >> (i * 3)) % 12); });
      var fills = ['var(--green-400)', 'var(--gold-500)', 'var(--green-800)'];
      return '<svg viewBox="0 0 26 22" width="26" height="22">'
        + bars.map(function (b, i) {
            return '<rect x="' + (i * 9) + '" y="' + (21 - b) + '" width="7" height="'
                 + b + '" rx="1" fill="' + fills[i] + '"/>';
          }).join('')
        + '</svg>';
    }

    fields.forEach(function (f) {
      f.sel.onchange = function () { state[f.level] = f.sel.value; sync(true); };
    });
    wireSearch();

    return {
      sync: sync,
      levels: levels,
      selects: fields.reduce(function (o, f) { o[f.level] = f.sel; return o; }, {}),
    };
  }

  window.DDExplorer = { mount: mount, ALL: '\u0000all' };
})();
