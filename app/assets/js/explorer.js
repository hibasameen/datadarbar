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

  var CAPTION = { topic: 'Topic', ds: 'Dataset', ind: 'Indicator' };
  var LABEL_OF = { topic: 'topicLabel', ds: 'dsLabel', ind: 'label' };

  function uniq(rows, key, labelKey) {
    var seen = {}, out = [];
    rows.forEach(function (r) {
      if (!(r[key] in seen)) {
        seen[r[key]] = 1;
        out.push({ value: r[key], label: r[labelKey] || r[key], theme: r.theme });
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

    var fields = levels.map(function (lv) {
      var f = field(names[lv] || CAPTION[lv] || lv,
                    'x' + lv.charAt(0).toUpperCase() + lv.slice(1));
      el.appendChild(f.wrap);
      return f;
    });

    var meta = document.createElement('p');
    meta.className = 'xrail-meta';
    el.appendChild(meta);

    function sync(fire) {
      var rows = index;
      levels.forEach(function (lv, i) {
        var opts = uniq(rows, lv, LABEL_OF[lv]);
        state[lv] = fill(fields[i].sel, opts, state[lv], i === 0);
        // A select with one option stays visible but disabled, so the rail
        // keeps its shape as you move between datasets.
        fields[i].sel.disabled = opts.length < 2;
        fields[i].cap.textContent =
          (names[lv] || CAPTION[lv] || lv)
          + (opts.length > 1 ? ' · ' + opts.length : '');
        rows = rows.filter(function (r) { return r[lv] === state[lv]; });
      });

      var chosen = rows[0] || index[0];
      meta.textContent = chosen
        ? [chosen.dsLabel, chosen.years, chosen.rows].filter(Boolean).join(' · ')
        : '';
      more(chosen);
      if (fire && cfg.onChange) cfg.onChange(chosen);
      return chosen;
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

    fields.forEach(function (f, i) {
      f.sel.onchange = function () { state[levels[i]] = f.sel.value; sync(true); };
    });

    return {
      sync: sync,
      levels: levels,
      selects: fields.reduce(function (o, f, i) { o[levels[i]] = f.sel; return o; }, {}),
    };
  }

  window.DDExplorer = { mount: mount };
})();
