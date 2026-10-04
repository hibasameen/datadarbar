"""Write the shared header and footer into every page, from one definition.

The chrome had drifted: six pages carried their own copy of the header CSS
inline, two more had their own stylesheet, and of the 90 rules that appeared on
three or more pages, 31 had different bodies depending on the page. The same
header rendered differently depending on where you were standing.

Keeping the markup in the HTML rather than building it at runtime matters for
crawlers - the nav is how the site's pages are found - so the fix is not to move
it into JavaScript but to stop writing it by hand. This script owns the markup
between the header and footer markers; `assets/css/shell.css` owns its styling;
neither is edited per page.

Run after changing NAV or the footer:
    python3 etl/apply_shell.py            # rewrite every page
    python3 etl/apply_shell.py --check    # fail if any page is out of date
"""
import argparse, pathlib, re, sys
import hashlib

APP = pathlib.Path(__file__).resolve().parent.parent / 'app'

# The three explorers, then the analysts' shelf. `soon` marks a destination that
# does not exist yet: it is shown, because the structure is the point, and it is
# not a link, because a link to nothing is worse than a label.
NAV = [
    {'href': 'places.html',   'label': 'Places',    'match': ('places.html',)},
    {'href': 'finance.html',  'label': 'Economy',   'match': ('finance.html', 'money.html', 'trade.html', 'economy.html')},
    {'href': 'state.html',    'label': 'State',     'match': ('state.html',)},
]
SHELF = [
    # Compare was retired on 29 September 2026: explore.html redirects to
    # Economy, and the dictionary folded into the catalogue's field search.
    {'href': 'datasets/',     'label': 'Catalogue', 'match': ('datasets/', 'dictionary.html')},
    {'href': 'query.html',    'label': 'Query',     'match': ('query.html',)},
    {'href': 'methods.html',  'label': 'Methods',   'match': ('methods.html', 'methodology.html')},
    # About was folded into Methods in the redesign and came back as its own
    # page on 30 September 2026: who built this and why is not a method.
    {'href': 'about.html',    'label': 'About',     'match': ('about.html',)},
]

BURGER = ('<svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" '
          'stroke-width="2.5" aria-hidden="true"><line x1="3" y1="6" x2="21" y2="6"/>'
          '<line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>')

BEGIN_H, END_H = '<!-- dd:header -->', '<!-- /dd:header -->'
BEGIN_F, END_F = '<!-- dd:footer -->', '<!-- /dd:footer -->'


def link(item, here, cls, root=''):
    on = here in item['match']
    active = ' active' if on else ''
    if not item['href']:
        return (f'<span class="{cls}{active} soon" aria-disabled="true" '
                f'title="Not collected yet">{item["label"]}</span>')
    aria = ' aria-current="page"' if on else ''
    return (f'<a href="{root}{item["href"]}" class="{cls}{active}"{aria}>'
            f'{item["label"]}</a>')


def header(here, root=''):
    """The shared header. `root` prefixes every link and asset path, so a page
    under /datasets/<slug>/ passes '/' and gets absolute paths."""
    nav = ''.join(link(i, here, 'header-link', root) for i in NAV)
    nav += '<span class="header-sep"></span>'
    nav += ''.join(link(i, here, 'header-link shelf', root) for i in SHELF)
    mob = ''.join(link(i, here, 'mobile-nav-link', root) for i in NAV)
    mob += '<div class="mobile-nav-label">For analysts</div>'
    mob += ''.join(link(i, here, 'mobile-nav-link', root) for i in SHELF)
    return (
        f'{BEGIN_H}\n'
        '<header class="site-header">\n'
        f'  <a class="header-brand" href="{root}index.html">'
        f'<img src="{root}assets/img/logo.svg" class="header-logo" alt=""/>'
        '<span class="header-text"><span class="header-title">Data Darbar</span>'
        '<span class="header-tagline">Pakistan in numbers</span></span></a>\n'
        f'  <nav class="header-nav" aria-label="Sections">{nav}</nav>\n'
        f'  <button id="mobileMenuBtn" class="mobile-menu-btn" aria-label="Menu" '
        f'aria-expanded="false" aria-controls="mobileNav">{BURGER}</button>\n'
        '</header>\n'
        f'<nav id="mobileNav" class="mobile-nav hidden" aria-label="Sections">{mob}</nav>\n'
        f'{END_H}')


def footer(root=''):
    """The live site's bar, text for text.

    Measured off darbar.adaad.org rather than rewritten: one centred row, the
    licence first and the outbound links after, separated by middots. The only
    change is that it is fixed to the bottom of the viewport, which is what
    the design asks for.
    """
    sep = '<span class="footer-sep">&middot;</span>'
    return (
        f'{BEGIN_F}\n'
        '<footer class="site-footer">\n'
        '  <span>&copy; 2026 Hiba Sameen</span>' + sep +
        '<span>Data: <a href="https://www.pbs.gov.pk/" target="_blank" '
        'rel="noopener">Pakistan Bureau of Statistics</a>, <a href="https://www.sbp.org.pk/" '
        'target="_blank" rel="noopener">State Bank of Pakistan</a> and the sources '
        '<a href="' + root + 'about.html#acknowledgements">acknowledged</a></span>' + sep +
        '<span>Code: <a href="https://opensource.org/licenses/MIT" '
        'target="_blank" rel="noopener">MIT Licence</a> &middot; Derived data: '
        '<a href="https://creativecommons.org/licenses/by/4.0/" target="_blank" '
        'rel="noopener">CC BY 4.0</a> unless <a href="' + root + 'datasets/#licences">'
        'stated otherwise</a></span>' + sep +
        '<span><a href="' + root + 'datasets/">Data Catalogue</a> &middot; '
        '<a href="' + root + 'districts/">District Profiles</a> &middot; '
        '<a href="' + root + 'methods.html">Methods</a> &middot; '
        '<a href="https://adaad.org/" target="_blank" rel="noopener">Adaad</a> '
        '&middot; <a href="https://aiwan.adaad.org/" target="_blank" '
        'rel="noopener">Aiwan-e-Jamhoor</a></span>\n'
        '</footer>\n'
        f'{END_F}')


def drop_stray_mobile_nav(text):
    """Remove a page's own #mobileNav from outside the managed header block.

    The header block carries one now. A page that had its own ended up with two
    elements sharing an id, which breaks the toggle and is invalid besides.
    """
    m = re.search(re.escape(BEGIN_H) + r'.*?' + re.escape(END_H), text, re.S)
    if not m:
        return text, 0
    head, tail = text[:m.start()], text[m.end():]
    pat = re.compile(r'\s*<nav[^>]*id="mobileNav".*?</nav>', re.S)
    n = len(pat.findall(head)) + len(pat.findall(tail))
    return pat.sub('', head) + m.group(0) + pat.sub('', tail), n


def splice(text, begin, end, block, anchor_re, where):
    """Replace a marked block, or insert one if the page has none yet."""
    marked = re.compile(re.escape(begin) + r'.*?' + re.escape(end), re.S)
    if marked.search(text):
        return marked.sub(lambda _: block, text, count=1), 'replaced'
    m = re.search(anchor_re, text, re.S | re.I)
    if not m:
        return text, 'no anchor'
    if where == 'over':                      # replace the element found
        return text[:m.start()] + block + text[m.end():], 'converted'
    return text[:m.start()] + block + '\n' + text[m.start():], 'inserted'


# Selectors shell.css owns. A page's inline <style> loads after the stylesheet
# link, so any copy left behind would win - which is how the header came to
# render differently on different pages in the first place. Page-specific
# selectors are left exactly as they are; only the chrome is taken.
CHROME_HEADS = (
    '.site-header', '.header-', '.mobile-menu-btn', '.mobile-nav',
    '.site-footer', '.footer-', '.foot',
)


def is_chrome(sel):
    """True when every selector in a comma list belongs to the shell.

    Conservative on purpose: a list that mixes chrome with anything else stays
    where it is, and so does a selector that merely contains a chrome class
    further along (`.hero .header-link` is the page's, not the shell's).
    """
    parts = [x.strip() for x in sel.split(',') if x.strip()]
    return bool(parts) and all(
        any(one.startswith(h) for h in CHROME_HEADS) for one in parts)


def split_rules(css):
    """[(selector, body, whole)] for top-level rules, @media blocks included."""
    out, depth, buf = [], 0, ''
    for ch in css:
        buf += ch
        if ch == '{':
            depth += 1
        elif ch == '}':
            depth -= 1
            if depth == 0:
                m = re.match(r'\s*([^{]+)\{(.*)\}\s*$', buf, re.S)
                if m:
                    out.append((' '.join(m.group(1).split()), m.group(2), buf))
                buf = ''
    if buf.strip():
        out.append((None, None, buf))
    return out


def strip_chrome(css):
    """Remove the rules shell.css owns; recurse into @media blocks."""
    kept, dropped = [], 0
    for sel, body, whole in split_rules(css):
        if sel is None:
            kept.append(whole)
            continue
        if sel.startswith('@media'):
            inner, n = strip_chrome(body)
            dropped += n
            if inner.strip():
                kept.append(sel + '{' + inner + '}')
            continue
        if is_chrome(sel):
            dropped += 1
            continue
        kept.append(whole)
    return ''.join(kept), dropped


def dechrome(text):
    """Rewrite every inline <style> block, dropping the chrome rules."""
    total = 0

    def sub(m):
        nonlocal total
        inner, n = strip_chrome(m.group(2))
        total += n
        return m.group(1) + inner + m.group(3)

    text = re.sub(r'(<style[^>]*>)(.*?)(</style>)', sub, text, flags=re.S)
    return text, total


# The one-line binding, wherever it sits: in a <script> of its own, or as a
# statement inside a larger one (dictionary.html keeps it next to its download
# handler). Matched as a statement so only the statement goes.
INLINE_TOGGLE = re.compile(
    r'[ \t]*document\.getElementById\(["\']mobileMenuBtn["\']\)'
    r'(?:\s*&&\s*document\.getElementById\(["\']mobileMenuBtn["\']\))?'
    r'\.addEventListener\(["\']click["\'],\s*function\s*\(\)\s*\{'
    r'\s*document\.getElementById\(["\']mobileNav["\']\)'
    r'\.classList\.toggle\(["\']hidden["\']\)\s*\}\);?[ \t]*\n?')


def drop_empty_scripts(text):
    """A <script> left holding nothing but whitespace after the binding went."""
    return re.sub(r'\s*<script>\s*</script>', '', text)


def ensure_script(text):
    """assets/js/shell.js owns the header's behaviour; nav.js is retired.

    The toggle used to be bound in app.js, inline on six pages, and nowhere at
    all on three others. One owner, one binding.
    """
    changed = False
    if 'js/nav.js' in text:
        text = re.sub(r'<script src="/?assets/js/nav\.js"\s*>\s*</script>',
                      '<script src="assets/js/shell.js"></script>', text)
        changed = True
    # drop a page's own one-line binding, keeping any config statement with it
    new = drop_empty_scripts(INLINE_TOGGLE.sub('', text))
    if new != text:
        text, changed = new, True
    # A page that had nav.js and also got shell.js appended on an earlier run
    # ends up loading it twice. Harmless - the binding is guarded - but keep one.
    tag = '<script src="assets/js/shell.js"></script>'
    if text.count(tag) > 1:
        first = text.index(tag)
        text = text[:first + len(tag)] + text[first + len(tag):].replace(tag, '')
        changed = True
    if 'assets/js/shell.js' not in text:
        m = re.search(r'</body>', text, re.I)
        if m:
            text = (text[:m.start()]
                    + '<script src="assets/js/shell.js"></script>\n'
                    + text[m.start():])
            changed = True
    return text, changed


# Stylesheets and scripts are stamped with a hash of their own contents, so a
# reader who has the old file gets the new one. Without this, changing a token
# in shell.css reaches nobody who has visited before - which is exactly what
# happened when the design-system tokens landed and --teal-700 came back empty
# in a browser holding yesterday's copy.
def asset_version(root):
    """Hash EVERY asset the stamp is applied to, not a chosen few.

    The first cut listed eight files by name. Because one hash is stamped on
    every asset link, editing anything outside that list - finance.js, say -
    left the stamp unchanged, so browsers kept serving the old file and the
    edit reached nobody. That is worse than no versioning at all: it looks
    like cache-busting while silently doing nothing.

    data/ is in here for the same reason and a sharper one. The payloads under
    it were stamped by nothing at all, so a returning reader kept whichever
    copy they had. A stale number would be bad; a stale payload is worse,
    because the code that reads it is versioned and the payload is not. Adding
    the distribution-loss block to state_data.js meant state.js began reading
    D.discos, and a reader holding yesterday's payload would have got a
    TypeError and an empty page rather than an out-of-date chart. The two are
    one deployable and now share one hash.
    """
    h = hashlib.sha256()
    files = []
    for d in ('assets/css', 'assets/js', 'data'):
        base = root / d
        if base.is_dir():
            files.extend(sorted(base.rglob('*.css')) + sorted(base.rglob('*.js')))
    for f in sorted(set(files)):
        h.update(f.relative_to(root).as_posix().encode())
        h.update(f.read_bytes())
    return h.hexdigest()[:8]


def stamp_assets(text, ver):
    """Put ?v=<hash> on our own css and js, never on anything off-site."""
    def sub(m):
        url = m.group(2)
        if '//' in url:
            return m.group(0)
        url = re.sub(r'\?v=[0-9a-f]+', '', url)
        return m.group(1) + url + '?v=' + ver + m.group(3)
    before = text
    text = re.sub(r'(href=")((?:\.\./)*assets/css/[^"]+?)(")', sub, text)
    text = re.sub(r'(src=")((?:\.\./)*assets/js/[^"]+?)(")', sub, text)
    # The ?v= has to be inside the match or the stamp freezes. Anchored on
    # \.js alone this matched data/money_data.js once, and never again -
    # data/money_data.js?v=abc123 does not end in .js - so every payload kept
    # whatever stamp it was first given while the assets beside it moved on.
    # The assets pattern above has no such anchor, which is why it was right.
    text = re.sub(r'(src=")((?:\.\./)*data/[^"?]+?\.js(?:\?v=[0-9a-f]+)?)(")',
                  sub, text)
    return text, text != before


def ensure_stylesheet(text):
    if 'assets/css/shell.css' in text:
        return text, False
    tag = '<link rel="stylesheet" href="assets/css/shell.css"/>'
    m = re.search(r'<link[^>]*fonts\.googleapis[^>]*>', text, re.I)
    if m:
        return text[:m.end()] + '\n' + tag + text[m.end():], True
    m = re.search(r'</title>', text, re.I)
    if m:
        return text[:m.end()] + '\n' + tag + text[m.end():], True
    return text, False


# The two page stylesheets carry chrome of their own - 31 rules in styles.css,
# 18 in research.css - and both load after shell.css, so they would win.
SHEETS = ['assets/css/styles.css', 'assets/css/research.css']


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--check', action='store_true',
                    help='report drift without writing')
    a = ap.parse_args()

    pages = sorted(p for p in APP.glob('*.html')
                   if 'http-equiv="refresh"' not in p.read_text())
    ver = asset_version(APP)
    stale, report = [], []
    for p in pages:
        before = p.read_text()
        text, h = splice(before, BEGIN_H, END_H, header(p.name),
                         r'<header\b.*?</header>', 'over')
        text, f = splice(text, BEGIN_F, END_F, footer(),
                         r'<footer\b.*?</footer>', 'over')
        text, css = ensure_stylesheet(text)
        text, n = dechrome(text)
        text, dup = drop_stray_mobile_nav(text)
        n += dup
        text, js = ensure_script(text)
        text, stamped = stamp_assets(text, ver)
        if text != before:
            stale.append(p.name)
            if not a.check:
                p.write_text(text)
        report.append((p.name, h, f,
                       ('linked' if css else '-') + ('+js' if js else ''), n))

    for name in SHEETS:
        f = APP / name
        before = f.read_text()
        text, n = strip_chrome(before)
        if n:
            stale.append(name)
            if not a.check:
                f.write_text(text)
        report.append((name, '-', '-', 'is a sheet', n))

    w = max(len(r[0]) for r in report)
    for name, h, f, css, n in report:
        print(f'  {name:{w}s}  header:{h:<10s} footer:{f:<10s} '
              f'shell.css:{css:<7s} chrome rules dropped:{n}')
    if a.check and stale:
        print(f'\n{len(stale)} page(s) out of date: {", ".join(stale)}')
        sys.exit(1)
    print(f'\n{len(stale)} of {len(pages)} pages written' if not a.check
          else f'\nall {len(pages)} pages up to date')


if __name__ == '__main__':
    main()
