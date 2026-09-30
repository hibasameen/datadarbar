"""Fold About and Methodology into one Methods page, without retyping either.

The methodology page is 23 KB of provenance: which release a figure came from,
which districts a survey's frame predates, why two food-insecurity series are
not the same series. Retyping any of that by hand risks changing a claim about
the data, so this moves the content verbatim and only regroups the headings from
the old six-product structure onto the new three-explorer one.

    python3 etl/build_methods_page.py
"""
import pathlib, re
from html import escape

APP = pathlib.Path(__file__).resolve().parent.parent / 'app'

# Old section heading -> new one, keyed on the heading's plain text. Everything
# under each heading is untouched; only the label moves onto the new structure.
# The headings carry a coloured <span class="dd-tag">, which is kept - only the
# text inside it is replaced, and any trailing <small> qualifier is dropped
# because the new name already says what it qualified.
RENAME = {
    'District & Tehsil Map': 'Places — the map',
    'Poverty & Wealth (on the District & Tehsil Map)':
        'Places — poverty, satellite and rural facilities',
    'Trade Atlas': 'Economy — trade',
    'GDP & Budget': 'Economy — output, and the federal budget',
    'Monetary & External': 'Economy — prices, money and the external balance',
}


def plain(html):
    """Heading text as a reader sees it."""
    t = re.sub(r'<[^>]+>', '', html)
    for a, b in (('&amp;', '&'), ('&nbsp;', ' '), ('&#8211;', '-'), ('&middot;', '·')):
        t = t.replace(a, b)
    return ' '.join(t.split())


def content(name):
    """The inner HTML of the page's .method-content block."""
    s = (APP / name).read_text()
    m = re.search(r'<div class="method-content">(.*?)\n</div>', s, re.S)
    if not m:
        m = re.search(r'<div class="method-content">(.*)</div>\s*</main>', s, re.S)
    if not m:
        raise SystemExit(f'{name}: could not find .method-content')
    return m.group(1)


def retitle(html):
    def sub(m):
        inner = m.group(1)
        new = RENAME.get(plain(inner))
        if not new:
            return m.group(0)
        tag = re.search(r'(<span class="dd-tag"[^>]*>)(.*?)(</span>)', inner, re.S)
        if not tag:
            return f'<h2>{new}</h2>'
        return f'<h2>{tag.group(1)}{new}{tag.group(3)}</h2>'
    return re.sub(r'<h2[^>]*>(.*?)</h2>', sub, html, flags=re.S)


def slug(t):
    return re.sub(r'[^a-z0-9]+', '-', plain(t).lower()).strip('-')


def anchor(html):
    """Give every h2 an id, and collect them for the contents list."""
    found = []
    def sub(m):
        inner = m.group(1).strip()
        s = slug(inner)
        found.append((s, plain(inner)))
        return f'<h2 id="{s}">{inner}</h2>'
    return re.sub(r'<h2[^>]*>(.*?)</h2>', sub, html, flags=re.S), found


# The order sections appear in, by slug. Anything not listed keeps its place at
# the end, so a new section in either source page still shows up.
ORDER = ['the-project', 'the-four-views', 'sources', 'built-by',
         'places-the-map', 'places-poverty-satellite-and-rural-facilities',
         'economy-output-and-the-federal-budget', 'economy-trade',
         'economy-prices-money-and-the-external-balance', 'limitations']


def reorder(html):
    """Split on h2 and put the sections in ORDER; keep anything before the first."""
    parts = re.split(r'(?=<h2\b)', html)
    head = parts[0] if not parts[0].lstrip().startswith('<h2') else ''
    secs = parts[1:] if head else parts
    def key(sec):
        m = re.match(r'<h2[^>]*id="([^"]+)"', sec)
        sid = m.group(1) if m else ''
        return (ORDER.index(sid) if sid in ORDER else len(ORDER))
    return head + ''.join(sorted(secs, key=key))


def main():
    about = content('about.html')
    method = retitle(content('methodology.html'))
    body, _ = anchor(about + '\n' + method)
    body = reorder(body)
    heads = [(m.group(1), plain(m.group(2)))
             for m in re.finditer(r'<h2[^>]*id="([^"]+)"[^>]*>(.*?)</h2>', body, re.S)]
    toc = ''.join(f'<li><a href="#{s}">{escape(t)}</a></li>' for s, t in heads)

    page = f'''<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"/>
<title>Methods and sources — Data Darbar</title>
<meta name="description" content="What Data Darbar is, where every figure comes \
from, how districts are matched across boundary changes, and what each source \
will and will not support."/>
<link rel="canonical" href="https://darbar.adaad.org/methods.html"/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&amp;family=JetBrains+Mono:wght@400;500&amp;display=swap" rel="stylesheet"/>
<link rel="stylesheet" href="assets/css/shell.css"/>
<link rel="stylesheet" href="assets/css/research.css"/>
</head>
<body>
<!-- dd:header --><!-- /dd:header -->
<main class="wrap">
  <div class="hero">
    <h1>Methods and sources</h1>
    <p>What this is, where every figure comes from, and what each source will and
    will not support. Previously split across About and Methodology.</p>
  </div>
  <nav class="method-toc" aria-label="On this page">
    <h2 class="method-toc-title">On this page</h2>
    <ul>{toc}</ul>
  </nav>
  <div class="method-content">
{body}
  </div>
</main>
<!-- dd:footer --><!-- /dd:footer -->
<script src="assets/js/nav.js"></script>
<script src="assets/js/modals.js"></script>
</body></html>
'''
    (APP / 'methods.html').write_text(page)
    print(f'methods.html written — {len(page):,} bytes, {len(heads)} sections:')
    for s, t in heads:
        print(f'   #{s:48s} {t[:56]}')


if __name__ == '__main__':
    main()
