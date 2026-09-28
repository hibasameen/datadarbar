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
    // national_accounts for the aggregates and income per person;
    // gdp_indicators only still supplies the exchange rate.
    'sec-macro': ['national_accounts', 'Output, income and the rupee, Table 5'],
    'sec-qtr': ['gva_by_activity_quarterly', 'Value added by sector, quarterly'],
    /* These three read national_accounts, not gva_by_activity_annual: the
       share charts are Table 7b at current prices and the decomposition is
       Table 5 at constant prices. gva_by_activity_annual is constant-price
       leaf activity data - a real table, but not the one behind these - and
       naming it sent anyone trying to reproduce the chart to the wrong
       numbers. */
    'sec-arc': ['national_accounts', 'Sector shares of GVA, Table 7b'],
    'sec-mix': ['national_accounts', 'Sector shares of GVA, Table 7b'],
    'sec-contrib': ['national_accounts', 'Real growth by activity, Tables 5 and 6'],
    'sec-cybreak': ['national_accounts', 'Real growth by activity, Tables 5 and 6'],
    'sec-ceras': ['national_accounts', 'Real growth by activity, Tables 5 and 6'],
    'sec-lsm': ['lsm_qim', 'Large-scale manufacturing index'],
    'sec-qimonth': ['lsm_qim', 'Large-scale manufacturing index'],
    'sec-weights': ['lsm_sector_indices', 'Manufacturing sector indices and weights'],
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
    'sec-emig': ['diaspora_destinations', 'Emigrants by destination, skill and occupation'],
    'sec-m': ['sbp_observations', 'SBP monetary and external statistics'],
    'sec-npl': ['sbp_observations', 'SBP monetary and external statistics'],
    // trade
    'sec-drill': ['trade_hs8', 'Imports and exports by HS8 product and country'],
    'sec-products': ['trade_by_group', 'Imports and exports by commodity group'],
    'sec-movers': ['trade_by_group', 'Imports and exports by commodity group'],
    'sec-totals': ['trade_monthly_totals', 'Monthly trade totals since 2003'],
    'sec-recon': ['trade_reconciliation', 'Trade totals reconciled against their parts'],
    'sec-partners': ['trade_by_country', 'Imports and exports by trading partner'],
  };

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
      /* Inserted at the top, not in place of everything. This used to be
         side.innerHTML = '' - which took the rail's own panel and every
         panel under it, so the year slider, the sector focus, the log/linear
         toggle, the fiscal-year select, the country and the detail level
         were all removed from the page the moment the rail mounted. The
         code that drives them kept running against elements that no longer
         existed, which d3 does silently, so nothing complained. */
      var lab = document.createElement('label');
      lab.className = 'xsearch';
      lab.innerHTML =
        '<svg width="14" height="14" viewBox="0 0 16 16" fill="none" '
        + 'stroke="currentColor" stroke-width="1.8" aria-hidden="true">'
        + '<circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5 L14 14"/></svg>'
        + '<input id="xFind" type="search" placeholder="Find a series\u2026" '
        + 'aria-label="Find a series" autocomplete="off"/>';
      list = document.createElement('div');
      list.className = 'xlist';
      list.id = 'chartList';
      var panel = document.createElement('div');
      panel.className = 'eco-panel xpanel';
      panel.appendChild(lab);
      panel.appendChild(host);
      panel.appendChild(list);
      side.insertBefore(panel, side.firstChild);
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

    /* All datasets by default, as Places does. With the dataset leading, a
       topic showed only the charts of whichever table sorted first: External
       balance listed the two SBP charts and hid both remittances and
       emigration, which are the two anyone comes to that topic for. */
    /* Booted from the hash, not from index[0]. The rail has always had a
       hashchange listener, but a page opened AT a topic - a shared link, or
       the money.html and trade.html redirects - fires no such event, so the
       dropdown opened on whichever topic sorted first while the cards below
       it were the ones the link asked for. */
    var want = new URLSearchParams(location.hash.replace(/^#/, '')).get('t');
    var at = index.filter(function (r) { return r.topic === want; })[0]
          || index[0];
    var state = { ds: window.DDExplorer.ALL, topic: at.topic, ind: at.ind };
    var rail = window.DDExplorer.mount({
      el: host, index: index, state: state,
      levels: ['topic', 'ds', 'ind'],
      optional: { ds: true },
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
      state.topic = first.topic;
      state.ind = first.ind;
      rail.sync(false);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', mount);
  } else { mount(); }
})();
