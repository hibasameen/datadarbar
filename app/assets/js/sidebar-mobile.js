/* Data Darbar — collapsible sidebar on small screens.
   The chart pages (GDP & Budget, Trade Atlas, Monetary & External) keep their
   topic list and controls in a fixed left sidebar. Below 1000px that sidebar
   becomes a block above the content, and on a phone it filled the whole first
   screen. This adds a toggle bar — "Topic · <current> ▾" — that keeps the panels
   collapsed until tapped, and collapses them again once a topic is chosen. The
   CSS lives in each page under .mobile-topic-toggle; on desktop the bar is
   display:none and nothing here has any effect. */
(function () {
  var side = document.querySelector('.eco-side');
  if (!side || side.querySelector('.mobile-topic-toggle')) return;

  var btn = document.createElement('button');
  btn.type = 'button';
  btn.className = 'mobile-topic-toggle';
  btn.setAttribute('aria-expanded', 'false');
  btn.innerHTML = '<span class="lbl">Topic</span><span class="cur"></span>' +
    '<svg class="chev" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3"><polyline points="6 9 12 15 18 9"/></svg>';
  side.insertBefore(btn, side.firstChild);

  var isMobile = function () { return getComputedStyle(btn).display !== 'none'; };
  var current = function () {
    var on = side.querySelector('.topic-item.on') || side.querySelector('.topic-item.active');
    return on ? on.textContent.trim() : '';
  };
  var sync = function () { btn.querySelector('.cur').textContent = current(); };
  var set = function (open) {
    side.classList.toggle('open', open);
    btn.setAttribute('aria-expanded', open ? 'true' : 'false');
  };

  btn.addEventListener('click', function () { set(!side.classList.contains('open')); });
  side.addEventListener('click', function (e) {
    if (e.target.closest('.topic-item') && isMobile()) {
      setTimeout(function () { sync(); set(false); }, 0);
    }
  });
  window.addEventListener('hashchange', function () { setTimeout(sync, 0); });
  // the topic list is rendered by the page script after this runs
  var tries = 0, t = setInterval(function () { sync(); if (current() || ++tries > 40) clearInterval(t); }, 100);
})();
