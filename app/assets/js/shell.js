/*
   Data Darbar — the shell's behaviour.
   -----------------------------------
   The header and footer markup is written by etl/apply_shell.py and styled by
   assets/css/shell.css; this is the third part of the same thing, and it owns
   exactly one behaviour: the mobile menu.

   It replaces assets/js/nav.js, which existed to build the "Data ▾" dropdown
   over six products that are now three explorers and a shelf. The toggle had
   been bound five different ways — once in app.js, once inline on each of six
   pages, and not at all on about, methodology or the old methods page, where
   the button was there and did nothing.

   Binding is guarded: if the button is already wired, this leaves it alone,
   because two handlers on one button toggle twice and the menu never opens.
*/
(function () {
  'use strict';

  function boot() {
    var btn = document.getElementById('mobileMenuBtn');
    var nav = document.getElementById('mobileNav');
    if (!btn || !nav || btn.dataset.ddBound === '1') return;
    btn.dataset.ddBound = '1';

    function set(open) {
      nav.classList.toggle('hidden', !open);
      btn.setAttribute('aria-expanded', String(open));
    }
    set(false);

    btn.addEventListener('click', function (e) {
      e.preventDefault();
      set(nav.classList.contains('hidden'));
    });

    // Escape closes it, and does not travel on: the map page has its own
    // Escape handler that zooms out, and closing a menu should not do that too.
    document.addEventListener('keydown', function (e) {
      if (e.key === 'Escape' && !nav.classList.contains('hidden')) {
        e.stopPropagation();
        set(false);
        btn.focus();
      }
    }, true);

    // A link inside the drawer navigates; leaving it open behind the new page
    // is only visible for a moment, but it looks like a bug.
    nav.addEventListener('click', function (e) {
      if (e.target.closest('a')) set(false);
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', boot);
  } else {
    boot();
  }
})();
