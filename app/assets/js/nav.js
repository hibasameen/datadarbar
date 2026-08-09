/*
   Data Darbar — shared "Data" nav dropdown
   ----------------------------------------
   Collapses the four product links (District Map, Poverty Metrics, Trade Atlas,
   Economy & Budget) into a single "Data" tab that opens a menu.

   Included on every page. Self-contained like modals.js: injects its own <style>
   with literal hex colours so it works regardless of which stylesheet the page
   uses (map.html/poverty.html link styles.css; index/trade/finance carry their
   own inline CSS). No fetch(), so it also works under file://.

   Markup contract — the page supplies:
     desktop:  <div class="dd-nav" data-dd-nav> … </div>   inside .header-nav
     mobile:   <div class="dd-mnav-group"> … </div>        inside #mobileNav
   Active state is resolved at runtime from location, so the markup is identical
   on every page and can't drift out of sync.
*/
(function () {
  'use strict';

  var VIEWS = [
    { href: 'map.html',     title: 'District Map',     desc: 'Census, PSLM, labour force and household indicators across 141 districts' },
    { href: 'poverty.html', title: 'Poverty Metrics',  desc: 'Multidimensional poverty, relative wealth, population and night-lights' },
    { href: 'trade.html',   title: 'Trade Atlas',      desc: 'Every 8-digit product and trading partner, as a treemap' },
    { href: 'finance.html', title: 'Economy & Budget', desc: 'GDP by sector, input-output flows and the federal budget' }
  ];

  var CSS = ''
    + '.dd-nav{position:relative;display:inline-flex}'
    + '.dd-nav-trigger{font-size:12.5px;font-weight:600;color:rgba(255,255,255,.65);background:none;'
    +   'border:1px solid rgba(255,255,255,.15);border-radius:6px;padding:6px 12px;cursor:pointer;'
    +   'font-family:inherit;display:inline-flex;align-items:center;gap:5px;white-space:nowrap;transition:.15s}'
    + '.dd-nav-trigger:hover{color:#f0cc5a;border-color:#d4a017;background:rgba(255,255,255,.05)}'
    + '.dd-nav-trigger .dd-chev{transition:transform .18s}'
    + '.dd-nav[data-open="1"] .dd-nav-trigger{color:#0c3a1e;background:#d4a017;border-color:#d4a017}'
    + '.dd-nav[data-open="1"] .dd-chev{transform:rotate(180deg)}'
    + '.dd-nav-trigger.dd-current{color:#0c3a1e;background:#d4a017;border-color:#d4a017}'
    + '.dd-nav-menu{position:absolute;top:calc(100% + 8px);right:0;min-width:330px;background:#fff;'
    +   'border:1px solid #e2e5ea;border-radius:12px;box-shadow:0 12px 34px rgba(12,58,30,.20);'
    +   'padding:6px;z-index:400;opacity:0;visibility:hidden;transform:translateY(-6px);'
    +   'transition:opacity .16s,transform .16s,visibility .16s}'
    + '.dd-nav[data-open="1"] .dd-nav-menu{opacity:1;visibility:visible;transform:translateY(0)}'
    + '.dd-nav-menu::before{content:"";position:absolute;top:-6px;right:20px;width:11px;height:11px;'
    +   'background:#fff;border-left:1px solid #e2e5ea;border-top:1px solid #e2e5ea;transform:rotate(45deg)}'
    + '.dd-nav-item{display:block;position:relative;padding:9px 12px 9px 14px;border-radius:8px;'
    +   'text-decoration:none;color:#17301f;border-left:3px solid transparent;transition:background .12s}'
    + '.dd-nav-item:hover{background:#f0f9f4;text-decoration:none}'
    + '.dd-nav-item b{display:block;font-size:13.5px;font-weight:700;color:#0c3a1e;letter-spacing:-.15px}'
    + '.dd-nav-item span{display:block;font-size:11.5px;color:#6b7280;line-height:1.4;margin-top:1px}'
    + '.dd-nav-item.dd-current{background:#fef6dc;border-left-color:#d4a017}'
    + '.dd-nav-item.dd-current b{color:#145228}'
    /* mobile: a labelled group rather than a nested dropdown — the menu is already a list */
    + '.dd-mnav-label{padding:10px 12px 4px;font-size:11px;font-weight:700;letter-spacing:.08em;'
    +   'text-transform:uppercase;color:rgba(255,255,255,.42)}'
    /* .mobile-nav is a flex column and these links used to be its direct children;
       inside a wrapper they'd otherwise flow inline, so re-establish the column here */
    + '.dd-mnav-group{display:flex;flex-direction:column;gap:2px;'
    +   'border-left:2px solid rgba(255,255,255,.12);margin:2px 0 6px 10px}'
    + '.dd-mnav-group .mobile-nav-link{padding-left:24px}'
    + '@media(max-width:820px){.dd-nav{display:none}}';

  function menuHtml() {
    var h = '<button type="button" class="dd-nav-trigger" data-dd-trigger aria-haspopup="true" aria-expanded="false">'
          + 'Data <svg class="dd-chev" width="9" height="9" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="3.5"><polyline points="6 9 12 15 18 9"/></svg>'
          + '</button><div class="dd-nav-menu" role="menu">';
    VIEWS.forEach(function (v) {
      h += '<a class="dd-nav-item" role="menuitem" href="' + v.href + '"><b>' + v.title + '</b><span>' + v.desc + '</span></a>';
    });
    return h + '</div>';
  }

  function currentFile() {
    var p = location.pathname.split('/').pop();
    return (!p || p === '') ? 'index.html' : p;
  }

  function boot() {
    if (document.getElementById('ddNavStyle')) return;
    var st = document.createElement('style');
    st.id = 'ddNavStyle';
    st.textContent = CSS;
    document.head.appendChild(st);

    var here = currentFile();
    var onAView = VIEWS.some(function (v) { return v.href === here; });

    // ── desktop dropdown ──
    var host = document.querySelector('[data-dd-nav]');
    if (host) {
      host.className = 'dd-nav';
      host.innerHTML = menuHtml();
      var trigger = host.querySelector('[data-dd-trigger]');
      if (onAView) trigger.classList.add('dd-current');
      host.querySelectorAll('.dd-nav-item').forEach(function (a) {
        if (a.getAttribute('href') === here) a.classList.add('dd-current');
      });

      var open = function (on) {
        host.setAttribute('data-open', on ? '1' : '0');
        trigger.setAttribute('aria-expanded', on ? 'true' : 'false');
        window.__ddNavOpen = !!on;
      };
      open(false);

      // Click to toggle — deliberately NOT hover-to-open. Combining the two means a
      // pointer user who hovers (opening it) and then clicks immediately closes it again,
      // and hover menus are awkward on touch and for keyboard users besides.
      trigger.addEventListener('click', function (e) {
        e.preventDefault(); e.stopPropagation();
        open(host.getAttribute('data-open') !== '1');
      });
      document.addEventListener('click', function (e) { if (!host.contains(e.target)) open(false); });
      // capture phase + stopPropagation so Escape closing the menu doesn't also
      // trigger the map page's own Escape handler (zoom-out)
      document.addEventListener('keydown', function (e) {
        if (e.key === 'Escape' && host.getAttribute('data-open') === '1') {
          e.stopPropagation(); open(false); trigger.focus();
        }
      }, true);
    }

    // ── mobile group ──
    var mg = document.querySelector('.dd-mnav-group');
    if (mg) {
      var mh = '';
      VIEWS.forEach(function (v) {
        mh += '<a href="' + v.href + '" class="mobile-nav-link' + (v.href === here ? ' active' : '') + '">' + v.title + '</a>';
      });
      mg.innerHTML = mh;
    }
  }

  if (document.readyState === 'loading') document.addEventListener('DOMContentLoaded', boot);
  else boot();
})();
