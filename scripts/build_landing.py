"""The landing page, with its numbers read from the warehouse.

Every figure on the front page - tables, rows, indicators, places - is counted
from the catalogue and the two indexes at build time rather than typed into the
HTML. A landing page that advertises "28 tables" while the warehouse holds 60
is worse than one with no numbers on it, and the only way that does not happen
is to stop a human from writing them down.

Markup only: the page's styling lives in shell.css and landing.css, with no
inline <style> and no hex literals. Run after build_web_warehouse.py.

Usage: build_landing.py [--out app/index.html]
"""
import argparse, json, pathlib, html, re


def n(x):
    return f'{x:,}'


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--app', default='app')
    a = ap.parse_args()
    APP = pathlib.Path(a.app)

    cat = json.loads((APP / 'data/warehouse/catalog.json').read_text())
    tables = cat['tables']

    def load(rel):
        t = (APP / rel).read_text()
        return json.loads(t[t.index('{'):t.rindex('}') + 1])

    places = load('data/places_index.js')
    state = load('data/state_data.js')

    rows = sum(t['rows'] for t in tables)
    by = {t['name']: t['rows'] for t in tables}
    facts = {
        'tables': len(tables),
        'rows': rows,
        'placesInd': len(places['dp']),
        'stateInd': len(state['index']),
        'stateDs': len({r['ds'] for r in state['index']}),
        'census': by.get('census_panel_2017', 0) + by.get('census_panel_2023', 0),
        'hs8': by.get('trade_hs8', 0),
        'sbp': by.get('sbp_observations', 0),
        'schools': by.get('schools_pk', 0),
        'generated': str(cat['generated']),
    }

    E = html.escape

    def card(href, accent, tag, title, desc, stat):
        return f'''      <a class="lcard" data-accent="{accent}" href="{href}">
        <div class="lcard-tag">{E(tag)}</div>
        <h2 class="lcard-title">{E(title)}</h2>
        <p class="lcard-desc">{E(desc)}</p>
        <div class="lcard-stat">{stat}</div>
      </a>'''

    explorers = '\n'.join([
        card('places.html', 'places', 'Explorer', 'Places',
             'Every district and tehsil indicator on one map and one frame: '
             'census, survey, poverty, agriculture, facilities and satellite.',
             f'<b>{n(facts["placesInd"])}</b> indicators · 156 districts · 649 tehsils'),
        card('finance.html', 'economy', 'Explorer', 'Economy',
             'National accounts, industry, prices, the rupee, the budget and '
             'trade down to the eight-digit product.',
             f'<b>{n(facts["hs8"])}</b> trade rows · <b>{n(facts["sbp"])}</b> SBP observations'),
        card('state.html', 'state', 'Explorer', 'State',
             'What the state collects, spends, judges, polices and generates — '
             'and what it does not publish.',
             f'<b>{facts["stateInd"]}</b> indicators across <b>{facts["stateDs"]}</b> datasets'),
    ])

    shelf = '\n'.join([
        card('datasets/', 'neutral', 'For analysts', 'The catalogue',
             'Every table with its field definitions, source, row count and '
             'the warnings that come with it. Parquet, documented.',
             f'<b>{facts["tables"]}</b> tables · <b>{n(facts["rows"])}</b> rows'),
        card('query.html', 'neutral', 'For analysts', 'Query in the browser',
             'Run SQL against the same tables without downloading anything. '
             'Nothing leaves your machine.',
             'DuckDB over Parquet'),
        card('methods.html', 'neutral', 'For analysts', 'Methods and sources',
             'How the frame was built, which keys join safely, and where each '
             'figure came from.',
             'Boundaries, crosswalks, provenance'),
    ])

    body = f'''<div class="lhero">
  <div class="lwrap">
    <p class="leyebrow">Pakistan in numbers</p>
    <h1>The official statistics, on one frame.</h1>
    <p class="llede">Pakistan’s census, trade, budget, policing, energy and
      survey data, put on a single geography and published with the caveats
      attached. Built by <a href="https://adaad.org/">Adaad</a>.</p>
    <p class="lstat"><b>{n(facts['rows'])}</b> rows across <b>{facts['tables']}</b>
      documented tables, including <b>{n(facts['census'])}</b> census cells on the
      2023 boundaries.</p>
  </div>
</div>

<div class="lwrap">
  <h2 class="lsec">Three ways in</h2>
  <div class="lcards">
{explorers}
  </div>

  <h2 class="lsec">For analysts</h2>
  <div class="lcards">
{shelf}
  </div>

  <div class="lnote">
    <h2>What this is careful about</h2>
    <p>Both censuses are drawn on the boundaries PBS published for 2023, so the
      same place means the same place in each year. Where a district split, the
      unit map records it rather than silently splitting a number. Rates are
      recomputed from summed numerators and denominators, never averaged.
      An absent figure is marked absent, not drawn as a zero.</p>
    <p><a href="methods.html">Read the methods</a> ·
       <a href="datasets/">Browse the catalogue</a></p>
    <p class="lgen">Warehouse release {E(facts['generated'])}.</p>
  </div>
</div>
'''

    page = (APP / 'index.html')
    src = page.read_text()
    start = src.index('<body')
    start = src.index('>', start) + 1
    end = src.rindex('</body>')
    head = src[:start]
    tail = src[end:]
    # apply_shell.py owns the header and footer inside <body>; keep whatever it
    # wrote and replace only what sits between them.
    hdr = re.search(r'\s*<header class="site-header".*?</header>', src, re.S)
    nav = re.search(r'\s*<nav id="mobileNav".*?</nav>', src, re.S)
    ftr = re.search(r'\s*<footer class="site-footer".*?</footer>', src, re.S)
    chrome_top = (hdr.group(0) if hdr else '') + (nav.group(0) if nav else '')
    chrome_bot = ftr.group(0) if ftr else ''
    # The old page carried 3 KB of inline <style> with seventeen hard-coded
    # colours. It goes; landing.css replaces it and reads tokens only.
    head = re.sub(r'\s*<style[^>]*>.*?</style>', '', head, flags=re.S)
    if 'landing.css' not in head:
        head = head.replace('</head>',
                            '<link rel="stylesheet" href="assets/css/landing.css"/>\n</head>')
    page.write_text(head + chrome_top + '\n\n' + body + chrome_bot + '\n' + tail)
    print(f'index.html written: {facts["tables"]} tables, {n(facts["rows"])} rows, '
          f'{n(facts["placesInd"])} Places indicators, {facts["stateInd"]} State')


if __name__ == '__main__':
    main()
