/*
   explorer.js — the dropdown rail shared by State, Economy and Places.

   Three selects over one index: dataset, topic, indicator. They are peers
   rather than a strict hierarchy, because people arrive from both directions —
   some want "the courts data", some want "the table LJCP publishes". Picking a
   dataset narrows the topics to the ones it feeds; picking a topic narrows the
   datasets to the ones behind it; the indicator select always lists what the
   current pair can actually draw, so it can never offer a dead combination.

   The subject tree stays as it is. These controls are another way in, not a
   replacement, and both drive the same state object.

   An index row is:
     {ds, dsLabel, topic, topicLabel, theme, ind, label, chart, years, note}

   window.DDExplorer.mount({el, index, state, onChange}) returns {sync}.
*/
(function () {
  'use strict';

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

  function fill(sel, opts, value, groupByTheme) {
    sel.innerHTML = '';
    if (groupByTheme && opts.some(function (o) { return o.theme; })) {
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
    // A value that survived a narrowing may no longer be on the list.
    if (sel.selectedIndex < 0 && opts.length) sel.value = opts[0].value;
    return sel.value;
  }

  function option(o) {
    var el = document.createElement('option');
    el.value = o.value;
    el.textContent = o.label;
    return el;
  }

  function field(label, id) {
    var wrap = document.createElement('label');
    wrap.className = 'xf';
    var cap = document.createElement('span');
    cap.className = 'xf-label';
    cap.textContent = label;
    var sel = document.createElement('select');
    sel.className = 'xf-select';
    sel.id = id;
    wrap.appendChild(cap);
    wrap.appendChild(sel);
    return { wrap: wrap, sel: sel };
  }

  function mount(cfg) {
    var index = cfg.index, state = cfg.state, el = cfg.el;
    el.innerHTML = '';
    el.className = 'xrail';

    /* Places picks a measured series and calls it an indicator. Economy and
       State pick a whole chart - a stacked area, a drill-down, a matrix - and
       calling those indicators overstates what they are, so the third field
       is named by the page. */
    var names = cfg.labels || {};
    var ds = field(names.ds || 'Dataset', 'xDataset');
    var tp = field(names.topic || 'Topic', 'xTopic');
    var ind = field(names.ind || 'Indicator', 'xIndicator');
    [ds, tp, ind].forEach(function (f) { el.appendChild(f.wrap); });

    var meta = document.createElement('p');
    meta.className = 'xrail-meta';
    el.appendChild(meta);

    /* Dataset is the top level and always lists every one, because it is the
       way in for a reader who knows the table and not the subject. Topic lists
       what the chosen dataset feeds, and indicator what the chosen pair can
       actually draw — so the rail can never offer a dead combination.

       Each select is filled from `state`, never from a row resolved first: an
       earlier version looked the row up before filling, and because that
       lookup matched on topic ahead of dataset, choosing a new dataset was
       silently overridden by the topic still held from the old one. */
    function sync(fire) {
      state.ds = fill(ds.sel, uniq(index, 'ds', 'dsLabel'), state.ds);

      var topics = uniq(index.filter(function (r) { return r.ds === state.ds; }),
                        'topic', 'topicLabel');
      state.topic = fill(tp.sel, topics, state.topic, true);

      var pair = index.filter(function (r) {
        return r.ds === state.ds && r.topic === state.topic;
      });
      state.ind = fill(ind.sel, pair.map(function (r) {
        return { value: r.ind, label: r.label };
      }), state.ind);

      var chosen = pair.filter(function (r) { return r.ind === state.ind; })[0]
                || pair[0] || index[0];
      meta.textContent = chosen
        ? [chosen.dsLabel, chosen.years, chosen.rows].filter(Boolean).join(' \u00b7 ')
        : '';
      // Disabled rather than hidden: a rail that drops from three fields to
      // two as you move between datasets reads as something having broken.
      tp.sel.disabled = topics.length < 2;
      ind.sel.disabled = pair.length < 2;
      more(chosen);
      if (fire && cfg.onChange) cfg.onChange(chosen);
      return chosen;
    }


    /* "Other charts in this topic", under the chart. A topic is a subject, not
       a table, so the siblings are everything filed under this topic whatever
       dataset it comes from - moving between them is the common case, and
       making the reader go back up to a dropdown for it is the wrong shape.
       Rendered from the same sync that fills the selects, so the strip and the
       dropdowns cannot disagree about what else is here. */
    function more(chosen) {
      var el = cfg.moreEl;
      if (!el || !chosen) return;
      var sibs = index.filter(function (r) {
        return r.topic === chosen.topic && r.ind !== chosen.ind;
      });
      if (!sibs.length) { el.innerHTML = ''; el.hidden = true; return; }
      el.hidden = false;
      el.innerHTML = '<div class="xmore-label">Other charts in this topic</div>'
        + '<div class="xmore-list"></div>';
      var list = el.querySelector('.xmore-list');
      sibs.forEach(function (r) {
        var b = document.createElement('button');
        b.type = 'button';
        b.className = 'xmore-item';
        b.innerHTML = '<span class="xmore-name"></span>'
                    + '<span class="xmore-ds"></span>';
        b.querySelector('.xmore-name').textContent = r.label;
        b.querySelector('.xmore-ds').textContent = r.dsLabel;
        b.onclick = function () {
          state.ds = r.ds; state.topic = r.topic; state.ind = r.ind;
          sync(true);
        };
        list.appendChild(b);
      });
    }

    /* fill() falls back to a list's first option when the held value did not
       survive the narrowing, so each handler only has to set and re-sync. */
    ds.sel.onchange = function () { state.ds = ds.sel.value; sync(true); };
    tp.sel.onchange = function () { state.topic = tp.sel.value; sync(true); };
    ind.sel.onchange = function () { state.ind = ind.sel.value; sync(true); };

    return { sync: sync, selects: { ds: ds.sel, topic: tp.sel, ind: ind.sel } };
  }

  window.DDExplorer = { mount: mount };
})();
