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

  var DATASET = {
    // finance
    'sec-arc': ['gva_by_activity_annual', 'Value added by sector, annual'],
    'sec-mix': ['gva_by_activity_annual', 'Value added by sector, annual'],
    'sec-contrib': ['gdp_growth', 'GDP growth by sector since 1951'],
    'sec-lsm': ['lsm_qim', 'Large-scale manufacturing index'],
    'sec-cmi': ['national_accounts', 'National accounts and GDP tables'],
    'sec-io': ['national_accounts', 'National accounts and GDP tables'],
    'sec-budget': ['budget_lines', 'Federal budget line items'],
    // money
    'sec-usd': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-reer': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-cpi': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-food': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-policy': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-kibor': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-spread': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-res': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-bop': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-remit': ['diaspora_remittances_monthly', 'Remittances to Pakistan by month'],
    'sec-m': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-npl': ['sbp_observations', 'SBP monetary and external statistics'],
    // trade
    'sec-drill': ['trade_hs8', 'Imports and exports by HS8 product and country'],
    'sec-products': ['trade_by_group', 'Imports and exports by commodity group'],
    'sec-movers': ['trade_by_group', 'Imports and exports by commodity group'],
    'sec-totals': ['trade_monthly_totals', 'Monthly trade totals since 2003'],
    'sec-partners': ['trade_by_country', 'Imports and exports by trading partner'],
  };

  /* Cards that are drill-downs of another card rather than charts in their own
     right. sec-country only exists once a partner has been clicked, so listing
     it in the rail would offer a choice that lands on nothing. The card itself
     stays on the page and is reached the way it always was. */
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
    if (!cards.length || !window.DDExplorer) return null;

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

  function mount() {
    var index = build();
    if (!index || !index.length) return;

    /* The sidebar these pages already have gets the search box, the topic
       dropdown and the list of that topic's charts, in place of its own
       nav. The charts themselves are untouched. */
    var side = document.querySelector('.eco-side');
    var host = document.getElementById('rail');
    if (!host) {
      host = document.createElement('div');
      host.id = 'rail';
      host.className = 'xrail';
    }
    var search, list;
    if (side) {
      side.classList.add('xside');
      side.innerHTML = '';
      var lab = document.createElement('label');
      lab.className = 'xsearch';
      lab.innerHTML =
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        + 'stroke="currentColor" stroke-width="1.8" aria-hidden="true">'
        + '<circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5 L14 14"/></svg>'
        + '<input id="xFind" type="search" placeholder="Find a series\u2026" '
        + 'aria-label="Find a series" autocomplete="off"/>';
      side.appendChild(lab);
      side.appendChild(host);
      list = document.createElement('div');
      list.className = 'xlist';
      list.id = 'chartList';
      side.appendChild(list);
      search = lab.querySelector('#xFind');
    } else {
      var anchor = document.querySelector('.card[data-topic]');
      if (!anchor || !anchor.parentNode) return;
      anchor.parentNode.insertBefore(host, anchor);
    }

    /* "Also in this topic" belongs under the chart, not in the sidebar. It was
       inserted next to the rail, and once the rail moved into the sidebar it
       went with it - sitting above the list of the very charts it repeats. */
    var moreEl = document.getElementById('more');
    if (!moreEl) {
      moreEl = document.createElement('div');
      moreEl.className = 'xmore';
      moreEl.id = 'more';
      moreEl.hidden = true;
      var cards = document.querySelectorAll('.card[data-topic]');
      var last = cards[cards.length - 1];
      if (last && last.parentNode) {
        last.parentNode.insertBefore(moreEl, last.nextSibling);
      }
    }

    var state = { ds: index[0].ds, topic: index[0].topic, ind: index[0].ind };
    var rail = window.DDExplorer.mount({
      el: host, index: index, state: state,
      levels: ['topic', 'ds', 'ind'],
      listLevel: list ? 'ind' : undefined, listEl: list, searchEl: search,
      labels: { ind: 'Chart' }, moreEl: moreEl,
      onChange: function (row) { go(row); },
    });

    /* Drive the page's own topic switch rather than reimplementing it, so
       every show/hide rule it already has keeps working. All three pages
       carry their state in the hash (#t=structure&y=2025-26) and listen for
       hashchange, so setting t= is the supported way in. An earlier version
       clicked a [data-topic] button; those buttons carry no data-topic, so
       nothing switched and five of the seven cards stayed hidden. */
    function setTopic(topic) {
      var q = new URLSearchParams(location.hash.replace(/^#/, ''));
      if (q.get('t') === topic) return false;
      q.set('t', topic);
      location.hash = q.toString();
      return true;
    }

    function go(row) {
      var moved = setTopic(row.topic);
      var show = function () {
        var card = document.getElementById(row.ind);
        if (!card || card.offsetParent === null) return;
        card.scrollIntoView({ behavior: 'smooth', block: 'start' });
        card.classList.add('is-picked');
        setTimeout(function () { card.classList.remove('is-picked'); }, 1400);
      };
      // The page redraws on hashchange, so wait a frame for the card to exist.
      if (moved) setTimeout(show, 90); else show();
    }

    rail.sync(false);

    /* And the other way: a hash change from anywhere else - a shared link, the
       back button - moves the rail with it, so the two can never disagree
       about what is on screen. */
    window.addEventListener('hashchange', function () {
      var t = new URLSearchParams(location.hash.replace(/^#/, '')).get('t');
      if (!t || t === state.topic) return;
      var first = index.filter(function (r) { return r.topic === t; })[0];
      if (!first) return;                 // 'all' and anything not in the index
      state.ds = first.ds;
      state.topic = first.topic;
      state.ind = first.ind;
      rail.sync(false);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mount);
  } else { mount(); }
})();
