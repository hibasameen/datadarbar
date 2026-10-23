/*
   economy-nav.js — the Economy explorer's rail, tabs and overview.

   ONE VIEW AT A TIME, like Places and State. finance.html holds every chart
   of the three Economy modules (finance.js, money.js, trade.js) as a
   <div class="card" data-topic="…" id="sec-…">, and until now showed a
   topic's cards as a stack to scroll; the sidebar listed 25 rows, taller than
   the screen, and the sub-items under a topic scrolled to a card here while
   on State they swapped the chart. Now the rail is six groups with only the
   current one open, a topic's charts are tabs above one chart, and a chart's
   linked view - the year's sub-sector mix beside the structure chart, the
   partner's profile beside the partners chart - sits in a panel on the right
   instead of a second card further down.

   THE MODULES ARE UNTOUCHED. They still read the hash (t= is THEIR topic,
   at= a card), hide the cards of other topics, draw on hashchange and on
   resize. So this file speaks to them only through the hash: a tab click
   writes t=<the card's module topic>&at=<card>, which makes the module that
   owns it redraw its topic with that card the one on screen. The page's own
   topics (PAGE below) are a grouping over cards and need not match the
   modules' - "Factories" is finance.js's industry and censuses, "Remittances
   and migration" is half of money.js's external - which is what lets the
   boundary agreed on 2026-10-23 be drawn without reopening three modules.

   The modules rewrite the hash without at= once they have applied it
   (replaceState, no event), so the card on screen is remembered here, not
   read back from the URL; the Share button puts it back.

   The budget, the long-run fiscal series and public debt moved to State.
   Their cards stay in the document, always hidden, because finance.js and
   money.js initialise against their ids; a link to them goes to State.
*/
(function () {
  'use strict';

  var PAGE = [
    { theme: 'Output & growth', topics: [
      { k: 'structure', label: 'Size and shape of the economy',
        dek: 'Seven decades of sector shares and the economy’s composition in any year. Services overtook agriculture in 1965-66; industry has sat near a fifth since the 1980s. Output, income and the rupee since 1999-2000, and the only quarterly series.',
        cards: ['sec-arc', 'sec-macro', 'sec-qtr'], side: { 'sec-arc': 'sec-mix' } },
      { k: 'growth', label: 'What drove growth',
        dek: 'Each year’s real GDP growth split into the percentage points each sector supplied, and the average engines of growth era by era.',
        cards: ['sec-contrib', 'sec-ceras'], side: { 'sec-contrib': 'sec-cybreak' } },
      { k: 'linkages', label: 'How sectors feed each other',
        dek: 'Who buys from whom: the 2015-16 input-output flows between twelve broad sectors, the only IO table PBS has published.',
        cards: ['sec-io'] },
      { k: 'factories', label: 'Factories',
        dek: 'Large-scale manufacturing: two decades of factory output by product, the monthly quantum index, what weighs what in it, and how registered manufacturing changed between the 2005-06 and 2015-16 censuses.',
        cards: ['sec-lsm', 'sec-qimonth', 'sec-weights', 'sec-cmi'] } ] },
    { theme: 'Trade', topics: [
      { k: 'overtime', label: 'Exports and imports over time',
        dek: 'Exports, imports and the trade balance year by year, 2003-04 to 2025-26, and how much of the total the product detail accounts for.',
        cards: ['sec-totals', 'sec-recon'] },
      { k: 'basket', label: 'What Pakistan trades',
        dek: 'Every traded product grouped by sector, as a treemap you can drill into, and the largest single commodities.',
        cards: ['sec-drill', 'sec-products'] },
      { k: 'partners', label: 'Trading partners',
        dek: 'The largest countries Pakistan trades with. Pick one for its own profile: its trade over time and the goods that make it up.',
        cards: ['sec-partners'], side: { 'sec-partners': 'sec-country' } },
      { k: 'movers', label: 'What is growing and shrinking',
        dek: 'The biggest risers and fallers in the trade basket, over the decade or year on year, at the detail level you choose.',
        cards: ['sec-movers'] } ] },
    { theme: 'Prices & money', topics: [
      { k: 'prices', label: 'Inflation',
        dek: 'Consumer price inflation, its components, and the gap between town and country.',
        cards: ['sec-cpi', 'sec-food'] },
      { k: 'rates', label: 'Interest rates',
        dek: 'The policy rate, the interbank curve, and what banks pay depositors and charge borrowers.',
        cards: ['sec-policy', 'sec-kibor', 'sec-spread'] },
      { k: 'money', label: 'Money supply and bad loans',
        dek: 'The money supply and the banking system’s non-performing loans.',
        cards: ['sec-m', 'sec-npl'] },
      { k: 'rupee', label: 'The rupee',
        dek: 'The exchange rate since 1947, and how the rupee has moved against its trading partners in real terms.',
        cards: ['sec-usd', 'sec-reer'] } ] },
    { theme: 'External', topics: [
      { k: 'external', label: 'Reserves and the balance of payments',
        dek: 'Foreign exchange reserves in dollars and months of imports, and where the dollars come from and where they go.',
        cards: ['sec-res', 'sec-bop'] },
      { k: 'remittances', label: 'Remittances and migration',
        dek: 'Money sent home by Pakistanis abroad, now worth more than goods exports, by the country it comes from; and where registered emigrants go.',
        cards: ['sec-remit', 'sec-emig'] } ] },
    { theme: 'Land & resources', topics: [
      { k: 'resources', label: 'Mining, crop prices and emissions',
        dek: 'What comes out of the ground since the 1960s, what crops fetch at market, and where the greenhouse gases come from.',
        cards: ['sec-mining', 'sec-cropprice', 'sec-emissions'] } ] },
    { theme: 'The long view', topics: [
      { k: 'longrun', label: 'Since 1950: GDP, money and prices',
        dek: 'Seventy-five years: the size of the economy at 2005-06 prices, and money and prices since independence. The state’s finances and debt over the same span are on State.',
        cards: ['sec-gdplong', 'sec-moneylong'] },
      { k: 'peers', label: 'Pakistan and its peers',
        dek: 'Pakistan beside its neighbours and comparable economies, on the World Bank’s figures.',
        cards: ['sec-peers'] } ] },
  ];

  /* Cards that stay in the document for the modules' sake and go nowhere
     in the nav: their subject lives on State now. */
  var GONE = { 'sec-budget': 'state.html?t=budget', 'sec-fiscal': 'state.html?t=debt&i=fiscal',
               'sec-debt': 'state.html?t=debt&i=debt' };

  function $(id) { return document.getElementById(id); }
  function esc(t) {
    return String(t == null ? '' : t).replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }
  /* A card's <h2> reads as a headline with its units after a dash. The tab
     wants the headline only - the units are on the chart. */
  function shortLabel(card) {
    var h = card.querySelector('h2');
    var t = h ? h.textContent.replace(/\s+/g, ' ').trim() : card.id;
    return t.split(/\s+[—–-]\s+/)[0].replace(/\s+[—–-]\s*$/, '').trim() || card.id;
  }

  var index = [], byCard = {}, topicOf = {}, state = { topic: 'overview', ind: null };
  var rail, cards, side, ctl;

  function build() {
    var missing = [];
    PAGE.forEach(function (g) {
      g.topics.forEach(function (t) {
        topicOf[t.k] = t;
        t.cards.forEach(function (id) {
          var card = $(id);
          if (!card) { missing.push(id); return; }
          var row = { topic: t.k, topicLabel: t.label, theme: g.theme, ind: id, label: shortLabel(card),
                      chart: id, mt: card.dataset.topic, sideCard: t.side && t.side[id] || null };
          index.push(row); byCard[id] = row;
        });
      });
    });
    if (missing.length) console.warn('economy-nav: cards missing from the page:', missing.join(', '));
    // A card on the page that no topic lists is a chart nobody can reach.
    var orphan = [];
    document.querySelectorAll('#cards .card[data-topic][id]').forEach(function (c) {
      if (!byCard[c.id] && !GONE[c.id] && !isSide(c.id)) orphan.push(c.id);
    });
    if (orphan.length) console.warn('economy-nav: cards in no topic:', orphan.join(', '));
  }
  function isSide(id) {
    return index.some(function (r) { return r.sideCard === id; });
  }

  /* ── the hash: read what the modules left, write what they understand ── */
  function readHashObj() {
    var h = '';
    try { h = decodeURIComponent(location.hash.replace(/^#/, '')); } catch (e) { h = location.hash.replace(/^#/, ''); }
    if (!h) return {};
    if (h.indexOf('=') < 0) return { t: h };
    var o = {};
    new URLSearchParams(h).forEach(function (v, k) { o[k] = v; });
    return o;
  }
  function rowFromHash() {
    var o = readHashObj();
    if (!o.t || o.t === 'overview') return null;
    if (o.at && GONE[o.at]) { location.replace(GONE[o.at]); return null; }
    if (o.t === 'budget') { location.replace(GONE['sec-budget']); return null; }
    if (o.at && byCard[o.at]) return byCard[o.at];
    // The module topic alone: keep the card on screen if it belongs to it.
    if (state.ind && byCard[state.ind] && byCard[state.ind].mt === o.t) return byCard[state.ind];
    // A page topic by name (links from elsewhere on the site), else the
    // first card of that module topic.
    var mine = index.filter(function (r) { return r.topic === o.t; })[0];
    if (mine) return mine;
    return index.filter(function (r) { return r.mt === o.t; })[0] || null;
  }
  function writeHash(row) {
    var o = readHashObj();
    var p = new URLSearchParams();
    if (!row) { p.set('t', 'overview'); }
    else {
      // Keep the module's own state (year, sector, view) when staying in
      // its topic; it is meaningless across topics.
      if (o.t === row.mt) Object.keys(o).forEach(function (k) { if (k !== 'at') p.set(k, o[k]); });
      p.set('t', row.mt);
      p.set('at', row.ind);
    }
    var next = '#' + p.toString();
    if (location.hash !== next) location.hash = next;   // hashchange: the modules draw
  }

  /* ── the view ────────────────────────────────────────────────────────── */
  function show(row) {
    var over = $('overview'), body = $('paneBody');
    if (!row) {
      state.topic = 'overview'; state.ind = null;
      $('paneTheme').textContent = 'Economy';
      $('paneTitle').textContent = 'What the economy produces, trades and prices';
      setDek(PAGE.length + ' groups, ' + index.reduce(function (n, r, i, a) {
        return n + (a.findIndex(function (x) { return x.topic === r.topic; }) === i ? 1 : 0); }, 0)
        + ' topics. Each opens on one chart; the rest of the topic sits in tabs above it.');
      $('coverage').innerHTML = '';
      body.hidden = true;
      drawOverview();
      over.hidden = false;
      return;
    }
    over.hidden = true;
    body.hidden = false;
    var t = topicOf[row.topic];
    $('paneTheme').textContent = row.theme;
    $('paneTitle').textContent = t.label;
    setDek(t.dek || '');
    // Which card is on screen. Every other card is off, including the ghosts.
    document.querySelectorAll('#cards .card[data-topic][id], #paneSide .card').forEach(function (c) {
      c.classList.toggle('xc-off', c.id !== row.ind && c.id !== row.sideCard);
    });
    // The linked view moves beside the chart, or back when another tab opens.
    document.querySelectorAll('#paneSide .card').forEach(function (c) {
      if (c.id !== row.sideCard) cards.appendChild(c);
    });
    if (row.sideCard && $(row.sideCard)) {
      side.appendChild($(row.sideCard));
      side.hidden = false;
      sideHint(row);
    } else side.hidden = true;
    // Sources sit under each chart already (the provenance panel); the
    // coverage line stays empty here so the chart starts higher.
    $('coverage').innerHTML = '';
  }

  /* The partner profile is display:none until a partner is picked (trade.js
     sets it), so the panel would stand empty: say what fills it, and watch
     the card for the moment it does. */
  var hint, hintObs;
  function sideHint(row) {
    if (!hint) { hint = document.createElement('p'); hint.className = 'side-hint'; }
    side.insertBefore(hint, side.firstChild);
    var card = $(row.sideCard);
    var HINT = { 'sec-country': 'Pick a partner on the chart, or in the Country control above it, to see its trade over time and the goods that make it up.' };
    function sync() {
      var off = card.style.display === 'none';
      hint.hidden = !off || !HINT[row.sideCard];
      hint.textContent = HINT[row.sideCard] || '';
    }
    if (hintObs) hintObs.disconnect();
    hintObs = new MutationObserver(sync);
    hintObs.observe(card, { attributes: true, attributeFilter: ['style'] });
    sync();
  }

  var dekOpen = false;
  function setDek(text) {
    var el = $('paneDek'), more = $('dekMore');
    el.textContent = text;
    dekOpen = false;
    el.classList.add('is-clamped');
    more.textContent = 'Read more';
    requestAnimationFrame(function () { more.hidden = !(el.scrollHeight > el.clientHeight + 2); });
  }

  /* ── the overview: one tile per group, a headline where the payloads
     give one cheaply, nothing invented where they do not ─────────────── */
  function headline(theme) {
    try {
      if (theme === 'Output & growth' && window.DD_GROWTH && window.DD_GROWTH.headline) {
        var hl = window.DD_GROWTH.headline, last = hl[hl.length - 1];
        if (last && last.value != null) return { n: (last.value > 0 ? '+' : '') + last.value.toFixed(1) + '%', u: 'real GDP growth, ' + last.year,
          p: 'Growth in gross value added at basic prices, from the national accounts, and what each sector added to it.' };
      }
      if (theme === 'Trade' && window.ECON && window.ECON.totals) {
        var T = window.ECON.totals, ys = Object.keys(T).sort(), y = ys[ys.length - 1];
        var ex = T[y] && (T[y].exp || T[y].export || T[y].exports);
        if (ex) return { n: 'Rs ' + (ex / 1000).toFixed(1) + 'tn', u: 'exports, ' + y,
          p: 'Goods exports as PBS publishes them; imports, the balance, every product and partner, and what is growing and shrinking.' };
      }
      if (theme === 'Prices & money' && window.DD_MONEY && window.DD_MONEY.series && window.DD_MONEY.series.cpi_nat) {
        var c = window.DD_MONEY.series.cpi_nat, lc = c[c.length - 1];
        if (lc) return { n: lc[1].toFixed(1) + '%', u: 'inflation, ' + lc[0].slice(0, 7),
          p: 'Consumer prices on a year earlier; the policy rate and what banks charge; money and bad loans; the rupee.' };
      }
      if (theme === 'External' && window.DD_MONEY && window.DD_MONEY.series && window.DD_MONEY.series.res_total) {
        var r = window.DD_MONEY.series.res_total, lr = r[r.length - 1];
        if (lr) return { n: '$' + (lr[1] / 1000).toFixed(1) + 'bn', u: 'reserves, ' + lr[0].slice(0, 7),
          p: 'Foreign exchange reserves and the balance of payments; remittances by the country they come from; where emigrants go.' };
      }
    } catch (e) { /* a headline is a courtesy, never a reason to fail */ }
    return null;
  }
  var OVERVIEW_P = {
    'Output & growth': 'Sector shares since 1951-52, what drove growth each year, how sectors feed each other, and factory output.',
    'Trade': 'Exports and imports over time, every product and partner, and what is growing and shrinking.',
    'Prices & money': 'Inflation, interest rates, money and bad loans, and the rupee.',
    'External': 'Reserves, the balance of payments, remittances and migration.',
    'Land & resources': 'Mining since 1960, wholesale crop prices, and greenhouse gas emissions by sector.',
    'The long view': 'The economy, money and prices since 1950, and Pakistan beside its peers.',
  };
  function drawOverview() {
    var host = $('overview');
    host.innerHTML = '';
    PAGE.forEach(function (g) {
      var h = headline(g.theme);
      var b = document.createElement('button');
      b.type = 'button';
      b.className = 'ovtile';
      b.innerHTML = '<span class="eyebrow"></span>'
        + (h ? '<span class="ovtile-n"><span class="n"></span><small></small></span>' : '')
        + '<p class="ovtile-p"></p><span class="ovtile-topics"></span>';
      b.querySelector('.eyebrow').textContent = g.theme;
      if (h) { b.querySelector('.n').textContent = h.n; b.querySelector('small').textContent = h.u; }
      b.querySelector('.ovtile-p').textContent = h ? h.p : OVERVIEW_P[g.theme] || '';
      var tl = b.querySelector('.ovtile-topics');
      g.topics.forEach(function (t) {
        var sp = document.createElement('span'); sp.textContent = t.label; tl.appendChild(sp);
      });
      b.onclick = function () { go(index.filter(function (r) { return r.topic === g.topics[0].k; })[0]); };
      host.appendChild(b);
    });
  }

  function go(row) {
    state.topic = row ? row.topic : 'overview';
    state.ind = row ? row.ind : null;
    rail.sync(false);
    show(row);
    writeHash(row);
    toTop();
  }
  /* The modules scroll the at= card into view 140ms after a hash change,
     which puts the head and the tabs under the header. The head is the
     page's orientation, so the page goes back to the top after them. */
  function toTop() {
    window.scrollTo({ top: 0 });
    setTimeout(function () { window.scrollTo({ top: 0, behavior: 'smooth' }); }, 360);
  }

  /* ── the controls move beside the chart they drive ───────────────────
     The year slider, sector focus, scale, fiscal year, country and detail
     level were panels down the sidebar. They sit in a row above the chart
     now. Moved, not rebuilt, so every module that shows, hides or reads
     them by id keeps working. */
  function moveControls() {
    document.querySelectorAll('#sidePanels .eco-panel[id^="side"]').forEach(function (p) { ctl.appendChild(p); });
  }

  function mount() {
    cards = $('cards'); side = $('paneSide'); ctl = $('topicCtl');
    if (!cards || !$('rail')) return;
    build();
    if (!index.length) return;
    moveControls();
    var help = document.querySelector('.navhelp');
    if (help) help.remove();

    rail = window.DDExplorer.mount({
      el: $('rail'), index: index, state: state, nav: true,
      tabsEl: $('tabs'), overview: { label: 'Overview' }, searchEl: $('xFind'),
      links: [{ href: 'state.html', label: 'Budget, tax, debt and public services', note: 'on State' }],
      onChange: function (row) { show(row); writeHash(row); toTop(); },
    });

    // Where the hash says we are; an empty hash is the overview.
    var first = rowFromHash();
    if (first) { state.topic = first.topic; state.ind = first.ind; }
    rail.sync(false);
    show(first);
    if (!location.hash) history.replaceState(null, '', location.pathname + location.search + '#t=overview');
    // A link by page topic (#t=factories) names no module topic; say it in
    // the module's terms so the module draws it.
    if (first && readHashObj().t !== first.mt) writeHash(first);
    if (first) toTop();

    window.addEventListener('hashchange', function () {
      var row = rowFromHash();
      if ((row ? row.ind : null) === state.ind && (row ? row.topic : 'overview') === state.topic) return;
      state.topic = row ? row.topic : 'overview'; state.ind = row ? row.ind : null;
      rail.sync(false);
      show(row);
    });

    window.DDExplorer.mobileToggle(document.querySelector('.eco-side'), function () {
      return state.ind && topicOf[state.topic] ? topicOf[state.topic].label : 'Overview';
    });

    $('shareBtn').onclick = function () {
      var btn = this;
      var p = new URLSearchParams(location.hash.replace(/^#/, ''));
      if (state.ind) { p.set('t', byCard[state.ind].mt); p.set('at', state.ind); }
      var url = location.origin + location.pathname + location.search + '#' + p.toString();
      var done = function () { btn.textContent = 'Copied'; setTimeout(function () { btn.textContent = 'Share'; }, 1400); };
      if (navigator.clipboard && navigator.clipboard.writeText) navigator.clipboard.writeText(url).then(done, function () { window.prompt('Copy this link:', url); });
      else window.prompt('Copy this link:', url);
    };
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', mount);
  else mount();
})();
