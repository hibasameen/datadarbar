/*
   econ-shell.js — the one fact three explorers must agree on.

   Economy is one section and was three pages: finance.html held output,
   industry and the budget, money.html the rupee, prices, rates, remittances
   and the external balance, trade.html products and partners. Two of the
   three were reachable from nowhere - not the header, not the landing cards,
   not each other - so the landing page promised "every traded product and
   partner, the rupee, prices, interest rates and the external balance" and
   linked to the page holding none of them.

   They are one page now. What made that awkward is that the three scripts
   were each written as a whole page: all three declare TOPICS, TOPIC_DRAWS,
   start, applyTopic, writeHash, tip, showTip and twenty-odd more at top
   level, so loading them together in one document throws on the first
   duplicate declaration and nothing runs.

   Twenty-nine names collide and only four are the same code. tip, showTip,
   moveTip and hideTip are identical bar whitespace; toCSV, downloadCSV and
   writeHash genuinely differ - money serialises rows as arrays where finance
   and trade use objects with a header, and each writes its own state keys
   into the hash. Unifying them by hand would have meant reconciling three
   sets of behaviour nobody asked to change.

   So they are ES modules instead. Module scope is not global scope, so all
   twenty-nine keep their own copy and nothing had to be renamed or
   reconciled. Each one answers hash changes exactly as it did when it had a
   page to itself; the module that does not own the topic on screen hides its
   own cards and its own sidebar panels and stands down.

   Navigation is the rail's (economy-rail.js): its Topic dropdown is built
   from the cards present on the page, which is now all fifteen topics,
   grouped by theme. So this file draws nothing. It holds the single answer
   to "which topic is showing", because three modules each deciding that for
   themselves is exactly what went wrong: with an empty hash every one of
   them fell back to its own first topic and believed it owned the page.
*/
(function () {
  'use strict';

  var topics = {};

  function currentTopic() {
    var h = '';
    try { h = decodeURIComponent(location.hash.replace(/^#/, '')); }
    catch (e) { h = location.hash.replace(/^#/, ''); }
    var k = window.DDEcon.defaultTopic;
    if (h) k = h.indexOf('=') < 0 ? h : (new URLSearchParams(h).get('t') || k);
    /* Each page used to offer "Everything", meaning everything on that page.
       Across all three it would mean 33 cards drawn at once, and a link to it
       would be claimed by all three modules at the same time. Old links still
       work; they open the default topic rather than nothing. */
    return k === 'all' ? window.DDEcon.defaultTopic : k;
  }

  window.DDEcon = {
    /* Which topic an empty hash means. One answer, for the reason above. */
    defaultTopic: 'structure',

    /* Modules announce their topics on boot. Nothing is drawn from it - it is
       what lets a module ask whether the topic on screen is one of its own. */
    register: function (mod) {
      (mod.topics || []).forEach(function (t) { topics[t.k] = t.label; });
    },
    label: function (k) { return topics[k]; },
    owns: function (keys) { return keys.indexOf(currentTopic()) >= 0; },
    current: currentTopic,
  };
})();
