/*
   provenance.js - renders each chart's source panel, and stamps its CSV.

   The panel is NOT written in the page. It is generated from window.DD_PROV,
   which etl/build_provenance.py generates from the warehouse catalogue, so a
   chart cannot claim a publisher, a table or a vintage that the warehouse
   does not have. Before this, the same fact lived in the rail's dataset map,
   in hand-written prose under the chart and in a CSV function, and the three
   disagreed: the share charts named a constant-price table for a current-price
   chart, and two cards pointed at a table that does not contain them.

   Two things follow from the record and are worth stating plainly on the page.
   A chart drawn from a pre-warehouse extract says so, because following its
   catalogue link will not reproduce it. A chart with no warehouse table at all
   says that too, rather than borrowing the name of one that exists.
*/
(function () {
  'use strict';

  var P = window.DD_PROV || {};

  function cap(s) { return s.charAt(0).toUpperCase() + s.slice(1); }

  function esc(s) {
    return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;');
  }

  /* Sentences, not fields. A reader should be able to read the panel; the
     machine-readable version is the record itself, which the CSV carries. */
  function panel(r) {
    var h = '<b class="src-lbl">Source.</b> ' + esc(r.publisher) + ', '
          + esc(r.publication);
    if (r.vintage) h += ' (warehouse release ' + esc(r.vintage) + ')';
    h += '.';
    if (r.series_refresh) h += ' Series refreshed ' + esc(r.series_refresh)
      + '.';
    if (r.selection) h += '<span class="src-line">This chart: '
      + esc(r.selection) + '.</span>';
    /* These read as their own sentences on their own lines, so they start
       like sentences; the record stores them as written, not pre-capitalised,
       because the CSV header lists them as fields. */
    if (r.calc) h += '<span class="src-line">' + cap(esc(r.calc)) + '.</span>';
    if (r.note) h += '<span class="src-line">' + cap(esc(r.note)) + '</span>';
    /* origin already says there is no warehouse table and why, so a chart
       that carries one does not also get the generic line below. */
    if (r.artefact) h += '<span class="src-art">' + esc(r.origin) + '</span>';
    if (r.table) {
      h += '<span class="src-line"><b>Full table:</b> <a href="' + r.catalogue
        + '">' + esc(r.table) + '</a>'
        + (r.rows ? ' (' + r.rows.toLocaleString() + ' rows)' : '')
        + ' · <a href="query.html">query it</a></span>';
    } else if (!r.origin) {
      h += '<span class="src-line"><b>No table:</b> this chart has no '
        + 'warehouse source and cannot be queried or re-derived here.</span>';
    }
    return h;
  }

  function render() {
    Object.keys(P).forEach(function (id) {
      var card = document.getElementById(id);
      if (!card) return;
      var el = card.querySelector(':scope > .src');
      if (!el) {
        el = document.createElement('div');
        el.className = 'src';
        card.appendChild(el);
      }
      el.innerHTML = panel(P[id]);
    });
  }

  /* The rows a chart exports say what they are. view is whatever the chart
     narrowed itself to - years, price basis, filters, units - and it is the
     half a footer can never carry, because it changes as the reader clicks. */
  function header(id, view) {
    var r = P[id];
    if (!r) return [];
    var L = [['# chart', r.title]];
    if (view) Object.keys(view).forEach(function (k) {
      if (view[k] != null && view[k] !== '') L.push(['# ' + k, String(view[k])]);
    });
    L.push(['# publisher', r.publisher], ['# publication', r.publication]);
    if (r.series_refresh) L.push(['# series refreshed', r.series_refresh]);
    if (r.selection) L.push(['# selection', r.selection]);
    if (r.calc) L.push(['# calculation', r.calc]);
    if (r.note) L.push(['# note', r.note]);
    if (r.artefact) L.push(['# artefact', r.origin]);
    L.push(['# source table', r.table || 'none - no warehouse source']);
    if (r.vintage) L.push(['# warehouse release', r.vintage]);
    L.push(['# retrieved', new Date().toISOString().slice(0, 10)],
           ['# terms', 'Derived data CC BY 4.0 - darbar.adaad.org']);
    return L;
  }

  /* The sidebar's "Sources" line was a fifth copy of the same fact, written
     per topic in three separate modules' TOPICS tables. A topic can span
     several tables, so a hand-written sentence there goes stale the moment a
     chart is repointed - which is how the share charts came to name a
     constant-price table. Built from the records instead, and a topic whose
     charts have no warehouse source says so rather than naming one. */
  function topicMeta(topic) {
    var byPub = {}, order = [], art = false;
    Object.keys(P).forEach(function (id) {
      var r = P[id];
      if (r.topic !== topic) return;
      if (r.artefact) art = true;
      if (!byPub[r.publisher]) { byPub[r.publisher] = []; order.push(r.publisher); }
      if (byPub[r.publisher].indexOf(r.publication) < 0)
        byPub[r.publisher].push(r.publication);
    });
    if (!order.length) return null;
    var out = order.map(function (pub) {
      return '<b>' + esc(pub) + '</b> \u2014 ' + byPub[pub].map(esc).join('; ');
    }).join('. ') + '.';
    if (art) out += ' Some charts here are drawn from pre-warehouse extracts; '
      + 'each says so under it.';
    return out;
  }

  window.DDProv = { of: function (id) { return P[id]; }, header: header,
                    render: render, topicMeta: topicMeta, panel: panel };

  if (document.readyState !== 'loading') render();
  else document.addEventListener('DOMContentLoaded', render);
})();
