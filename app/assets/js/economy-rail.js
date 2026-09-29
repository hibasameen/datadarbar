/*
   economy-rail.js — the dataset / topic / indicator dropdowns for finance,
   money and trade.

   The three pages already mark every chart as
     <div class="card" data-topic="…" id="sec-…"><h2>…</h2>
   so the cards ARE the indicators and the index is read off the page rather
   than duplicated in a second list that would drift from it. Only one thing
   cannot be read from the DOM — which warehouse table feeds a card — so that
   is the single map below, and it is asserted against the cards actually
   present at boot: a card with no dataset, or a dataset naming a card that
   does not exist, fails loudly rather than producing a rail with a hole in it.

   The rail does not replace the topic nav; it drives it. Choosing a topic
   clicks the page's own topic control, so all the existing show/hide logic
   runs exactly as before, and choosing an indicator scrolls to that card.
   Nothing on these pages was removed to make room for it.
*/
(function () {
  'use strict';

  /* The dataset map used to live here, in parallel with the prose under each
     chart and the CSV function beside it, and the three drifted apart: the
     share charts named gva_by_activity_annual for a Table 7b current-price
     chart, and two cards named a table that does not contain them. There is
     now one record, generated from the warehouse catalogue by
     etl/build_provenance.py, and the rail reads it like everything else.
     A chart with no warehouse table says so rather than borrowing a name. */
  var DATASET = (function () {
    var P = window.DD_PROV || {}, out = {};
    Object.keys(P).forEach(function (id) {
      var r = P[id];
      out[id] = [r.table || '\u2014',
                 r.table ? r.publication : 'No warehouse table'];
    });
    return out;
  })();

  /* Cards that are drill-downs of another card rather than charts in their own
     right. sec-country only exists once a partner has been clicked, so listing
     it in the rail would offer a choice that lands on nothing. The card itself
     stays on the page and is reached the way it always was.

     Note the selector below takes .card[data-topic][id], and finance.html puts
     four of its charts inside grid2 wrappers as bare <div class="card">. Those
     four - the year breakdown, growth engines, the monthly QIM and the index
     weights - were on the page and in no dropdown, because only the wrapper
     carried an id. They now carry their own, and the wrapper keeps its: the
     page hides by wrapper, the rail scrolls to the card. */
  var DRILLDOWN = { 'sec-country': 1 };

  var TOPIC_LABEL = {
    structure: ['Structural change', 'National accounts'],
    growth: ['Composition of growth', 'National accounts'],
    industry: ['Industry & factories', 'Industry'],
    censuses: ['Manufacturing censuses', 'Industry'],
    linkages: ['Sector linkages', 'National accounts'],
    budget: ['State & budget', 'Public money'],
    rupee: ['The rupee', 'Money & prices'],
    prices: ['Prices', 'Money & prices'],
    rates: ['Interest rates', 'Money & prices'],
    external: ['External balance', 'External'],
    money: ['Money & banks', 'Money & prices'],
    basket: ['What we trade', 'Trade'],
    movers: ['What’s growing', 'Trade'],
    overtime: ['Trade over time', 'Trade'],
    partners: ['Trading partners', 'Trade'],
  };

  /* A card's <h2> reads as a headline with its units after a dash. The rail
     wants the headline only — the units are already on the chart. */
  function shortLabel(card) {
    var h = card.querySelector('h2');
    var t = h ? h.textContent.replace(/\s+/g, ' ').trim() : card.id;
    return t.split(/\s+—\s+/)[0].replace(/\s+—\s*$/, '').trim() || card.id;
  }

  function build() {
    var cards = Array.prototype.slice.call(
      document.querySelectorAll('.card[data-topic][id]'));
    if (!cards.length) return null;

    var index = [], missing = [];
    cards.forEach(function (card) {
      if (DRILLDOWN[card.id]) return;
      var ds = DATASET[card.id];
      if (!ds) { missing.push(card.id); return; }
      var tl = TOPIC_LABEL[card.dataset.topic]
            || [card.dataset.topic, 'Other'];
      index.push({
        ds: ds[0], dsLabel: ds[0],
        topic: card.dataset.topic, topicLabel: tl[0], theme: tl[1],
        ind: card.id, label: shortLabel(card), chart: card.id,
        years: '', rows: ds[1],
      });
    });
    if (missing.length) {
      // Loud, because a silently missing card is a chart nobody can reach.
      console.warn('economy-rail: no dataset mapped for', missing.join(', '));
    }
    return index;
  }

  /* ── the topic list ─────────────────────────────────────────────────────
     A TOPIC IS A PLACE TO GO, NOT A VALUE TO SET. This page used to put its
     fifteen topics in a dropdown, add a dataset dropdown under it and a list
     of charts under that, so the year slider and the sector focus were pushed
     down the sidebar and a reader could not scan what the page covered
     without opening a select. The topics are a visible list again, grouped by
     subject, each named for what it answers rather than for the table behind
     it - and the dataset selector is gone: which table a chart comes from is
     in the source panel under that chart, where it qualifies the number.

     The federal budget's home is the State page now. It is listed here as a
     link there, so a reader who looks for it under Economy still finds it,
     and an old #t=budget link still opens the card on this page. */
  var NAV = [
    { group: 'GDP & growth', items: [
      { t: 'structure', label: 'GDP, income and sector shares' },
      { t: 'growth', label: 'What drove growth' },
      { t: 'linkages', label: 'How sectors feed each other' } ] },
    { group: 'Industry', items: [
      { t: 'industry', label: 'Factory output' },
      { t: 'censuses', label: 'Manufacturing censuses' } ] },
    { group: 'Trade', items: [
      { t: 'overtime', label: 'Exports and imports over time' },
      { t: 'basket', label: 'What Pakistan trades' },
      { t: 'partners', label: 'Trading partners' },
      { t: 'movers', label: 'What is growing and shrinking' } ] },
    { group: 'Prices & interest rates', items: [
      { t: 'prices', label: 'Inflation' },
      { t: 'rates', label: 'Interest rates' } ] },
    { group: 'Money & banking', items: [
      { t: 'money', label: 'Money supply and bad loans' } ] },
    { group: 'Rupee & external balance', items: [
      { t: 'rupee', label: 'The rupee' },
      { t: 'external', label: 'Reserves and the balance of payments' },
      { t: 'external', label: 'Remittances', card: 'sec-remit' } ] },
    { group: 'Elsewhere', items: [
      { href: 'state.html#t=budget', label: 'Government budget & tax', note: 'on State' },
      { href: 'explore.html', label: 'Compare any two series', note: 'Compare' } ] },
  ];

  /* A chart with series in the explorer opens there with them chosen, so a
     reader starts comparing from what they were looking at. */
  var COMPARE = {
    'sec-macro': 'na:gdp_tn~na:gva_tn', 'sec-qimonth': 'qim:overall',
    'sec-usd': 'sbp:usd', 'sec-reer': 'sbp:reer~sbp:neer',
    'sec-cpi': 'sbp:cpi_nat~sbp:cpi_urb~sbp:cpi_rur',
    'sec-food': 'sbp:cpi_urbf~sbp:cpi_rurf', 'sec-policy': 'sbp:pol_target',
    'sec-kibor': 'sbp:kib_6m~sbp:kib_1y', 'sec-spread': 'sbp:lend~sbp:depo',
    'sec-res': 'sbp:res_sbp~sbp:res_banks', 'sec-bop': 'sbp:gx~sbp:gm',
    'sec-m': 'sbp:m2', 'sec-npl': 'sbp:npl_ratio',
    'sec-totals': 'trade:export~trade:import',
  };

  function esc(t) {
    return String(t == null ? '' : t).replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  function mount() {
    var index = build();
    if (!index || !index.length) return;
    var side = document.querySelector('.eco-side');
    var wrap = document.querySelector('.wrap');
    if (!side || !wrap) return;

    var topicHash = function () {
      return new URLSearchParams(location.hash.replace(/^#/, '')).get('t');
    };
    var labelOf = {};
    NAV.forEach(function (g) {
      g.items.forEach(function (it) {
        if (it.t && !it.card && !labelOf[it.t]) labelOf[it.t] = it.label;
      });
    });

    /* ── the controls move to the charts they drive ─────────────────────
       The year slider, sector focus, scale, fiscal year, country and detail
       level were panels down the sidebar under the navigation. They now sit
       in a bar above the topic's charts that stays in view as they scroll.
       The elements are moved, not rebuilt, so every script that shows, hides
       or reads them by id keeps working. */
    var bar = document.createElement('section');
    bar.className = 'topic-bar';
    bar.setAttribute('aria-label', 'This topic');
    var head = document.createElement('div');
    head.className = 'topic-head';
    head.innerHTML = '<h1 class="topic-title" id="topicTitle"></h1>';
    var desc = document.getElementById('topicDesc');
    if (desc) {
      var holder = desc.parentNode;
      head.appendChild(desc);
      if (holder && holder.classList.contains('eco-panel') && !holder.children.length) {
        holder.remove();
      }
    }
    bar.appendChild(head);
    var ctl = document.createElement('div');
    ctl.className = 'topic-ctl';
    side.querySelectorAll('.eco-panel[id^="side"]').forEach(function (p) {
      ctl.appendChild(p);
    });
    bar.appendChild(ctl);
    var meta = document.getElementById('topicMeta');
    if (meta) bar.appendChild(meta);

    // The long "Getting around" banner goes: the topic list says what is
    // here, and each control says what it does where it is.
    var help = wrap.querySelector('.navhelp');
    if (help) help.remove();
    wrap.insertBefore(bar, wrap.firstChild);

    /* ── the sidebar: search, then the topics ─────────────────────────── */
    var nav = document.createElement('nav');
    nav.className = 'eco-nav';
    nav.setAttribute('aria-label', 'Economy topics');
    var find = document.createElement('label');
    find.className = 'xsearch';
    find.innerHTML =
      '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
      + 'stroke="currentColor" stroke-width="1.8" aria-hidden="true">'
      + '<circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5 L14 14"/></svg>'
      + '<input id="xFind" type="search" placeholder="Find a chart…" '
      + 'aria-label="Find a chart" autocomplete="off"/>';
    var list = document.createElement('div');
    list.className = 'eco-topics';
    var hits = document.createElement('div');
    hits.className = 'xfind';
    hits.hidden = true;
    nav.appendChild(find);
    nav.appendChild(hits);
    nav.appendChild(list);
    var toggle = side.querySelector('.mobile-topic-toggle');
    side.insertBefore(nav, toggle ? toggle.nextSibling : side.firstChild);

    function draw() {
      var cur = topicHash() || index[0].topic;
      var h = '';
      NAV.forEach(function (g) {
        h += '<div class="eco-group"><h2 class="eco-group-h">' + esc(g.group) + '</h2><ul>';
        g.items.forEach(function (it) {
          if (it.href) {
            h += '<li><a class="topic-item is-link" href="' + esc(it.href) + '">'
               + esc(it.label) + ' <span class="eco-note">' + esc(it.note)
               + ' →</span></a></li>';
            return;
          }
          var on = it.t === cur && !it.card;
          var sub = '';
          if (on) {
            var charts = index.filter(function (r) { return r.topic === it.t; });
            if (charts.length > 1) {
              sub = '<ul class="eco-charts">' + charts.map(function (r) {
                return '<li><button type="button" data-card="' + esc(r.ind)
                     + '" data-t="' + esc(r.topic) + '">' + esc(r.label)
                     + '</button></li>';
              }).join('') + '</ul>';
            }
          }
          h += '<li><button type="button" class="topic-item' + (on ? ' on' : '') + '"'
             + ' data-t="' + esc(it.t) + '"' + (it.card ? ' data-card="' + esc(it.card) + '"' : '')
             + (on ? ' aria-current="page"' : '') + '>' + esc(it.label) + '</button>'
             + sub + '</li>';
        });
        h += '</ul></div>';
      });
      list.innerHTML = h;
      list.querySelectorAll('button[data-t]').forEach(function (b) {
        b.onclick = function () { go(b.dataset.t, b.dataset.card); };
      });
      var title = document.getElementById('topicTitle');
      if (title) title.textContent = labelOf[cur] || '';
      // A chart scrolled to must land below the sticky bar, not under it.
      requestAnimationFrame(function () {
        var sticky = getComputedStyle(bar).position === 'sticky';
        document.documentElement.style.scrollPaddingTop =
          (sticky ? 56 + bar.offsetHeight + 10 : 10) + 'px';
      });
    }

    /* Through the hash, which every module on this page already reads:
       t= switches the topic and at= scrolls to a card once the module that
       owns it has drawn it. A timer of our own guessed at when that would be
       and lost the race whenever a topic fetched its data first. */
    function go(topic, card) {
      var q = new URLSearchParams(location.hash.replace(/^#/, ''));
      q.set('t', topic);
      if (card) q.set('at', card); else q.delete('at');
      var next = '#' + q.toString();
      if (next !== location.hash) location.hash = next;
      else if (card) {
        var el = document.getElementById(card);
        if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
      }
      if (!card) window.scrollTo({ top: 0, behavior: 'smooth' });
      draw();
    }

    /* Type to find any chart on the page, whichever topic it is filed under. */
    var box = find.querySelector('input');
    box.addEventListener('input', function () {
      var q = box.value.trim().toLowerCase();
      if (q.length < 2) { hits.hidden = true; hits.innerHTML = ''; return; }
      var found = index.filter(function (r) {
        return (r.label + ' ' + (labelOf[r.topic] || '') + ' ' + r.dsLabel)
          .toLowerCase().indexOf(q) >= 0;
      });
      hits.hidden = false;
      hits.innerHTML = found.length
        ? found.map(function (r) {
            return '<button type="button" class="xfind-item" data-t="' + esc(r.topic)
                 + '" data-card="' + esc(r.ind) + '"><span class="xfind-name">'
                 + esc(r.label) + '</span><span class="xfind-meta">'
                 + esc(labelOf[r.topic] || r.topicLabel) + '</span></button>';
          }).join('')
        : '<p class="xfind-none">No chart matches. The Compare page searches '
          + 'every series.</p>';
      hits.querySelectorAll('button').forEach(function (b) {
        b.onclick = function () {
          box.value = ''; hits.hidden = true; hits.innerHTML = '';
          go(b.dataset.t, b.dataset.card);
        };
      });
    });

    /* Compare beside the chart. */
    Object.keys(COMPARE).forEach(function (id) {
      var card = document.getElementById(id);
      if (!card || card.querySelector('.cmpbtn')) return;
      var a = document.createElement('a');
      a.className = 'cmpbtn';
      a.href = 'explore.html?s=' + encodeURIComponent(COMPARE[id]).replace(/%3A/g, ':')
                                                                   .replace(/%7E/g, '~');
      a.textContent = 'Compare';
      a.title = 'Open these series in Compare, to set them against any other';
      var csv = card.querySelector('.csvbtn');
      if (csv) csv.parentNode.insertBefore(a, csv); else card.insertBefore(a, card.firstChild);
    });

    window.addEventListener('hashchange', draw);
    draw();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mount);
  } else { mount(); }
})();
