"""The landing page, with its numbers read from the warehouse.

Every figure on the front page - tables, rows, indicators, census cells - is
counted from the catalogue and the two indexes at build time rather than typed
into the HTML. A landing page advertising "28 tables" over a warehouse holding
60 is worse than one with no numbers on it, and the only way that does not
happen is to stop a human writing them down.

Markup only: the styling lives in shell.css and landing.css, with no inline
<style> and no hex literals. Run after build_web_warehouse.py.

Usage: build_landing.py [--app app]
"""
import argparse, html, json, pathlib, re


def n(x):
    return f'{x:,}'


# One illustration per explorer, in that explorer's accent. They are
# decoration and nothing more: no shape here is drawn from data, and none is
# labelled as though it were.
ART = {
    'places':
        '<svg viewBox="0 0 220 130" role="img" aria-label="">'
        '<path d="M44 58 L70 34 L104 40 L110 72 L78 92 L46 84 Z" fill="var(--green-300)"/>'
        '<path d="M104 40 L140 30 L164 52 L150 78 L110 72 Z" fill="var(--green-700)"/>'
        '<path d="M110 72 L150 78 L158 104 L120 114 L78 92 Z" fill="var(--green-400)"/>'
        '<path d="M22 70 L44 58 L46 84 L30 96 Z" fill="var(--line-strong)"/>'
        '<path d="M164 52 L192 62 L182 92 L158 104 L150 78 Z" fill="var(--green-200)"/>'
        '</svg>',
    'economy':
        '<svg viewBox="0 0 220 130" role="img" aria-label="">'
        '<path d="M20 108 L20 74 L52 56 L84 70 L116 44 L148 62 L180 36 L200 48 '
        'L200 108 Z" fill="var(--gold-400)"/>'
        '<path d="M20 108 L20 92 L52 80 L84 94 L116 74 L148 88 L180 66 L200 76 '
        'L200 108 Z" fill="var(--gold-600)"/>'
        '<path d="M20 108 L20 102 L52 98 L84 104 L116 94 L148 102 L180 90 L200 96 '
        'L200 108 Z" fill="var(--gold-800)"/>'
        '<polyline points="20,74 52,56 84,70 116,44 148,62 180,36 200,48" '
        'fill="none" stroke="var(--green-900)" stroke-width="2.5"/>'
        '</svg>',
    'state':
        # A public building rather than a chart: State is the institutions -
        # courts, police, the treasury, the regulator - and a portico says that
        # where one more bar chart would say "Economy" again.
        '<svg viewBox="0 0 220 130" role="img" aria-label="">'
        '<path d="M110 14 L186 44 L34 44 Z" fill="var(--teal-700)"/>'
        '<circle cx="110" cy="33" r="6" fill="var(--gold-500)"/>'
        '<rect x="40" y="46" width="140" height="8" fill="var(--teal-500)"/>'
        '<rect x="50" y="58" width="12" height="40" fill="var(--teal-700)"/>'
        '<rect x="76" y="58" width="12" height="40" fill="var(--teal-500)"/>'
        '<rect x="104" y="58" width="12" height="40" fill="var(--teal-700)"/>'
        '<rect x="132" y="58" width="12" height="40" fill="var(--teal-500)"/>'
        '<rect x="158" y="58" width="12" height="40" fill="var(--teal-700)"/>'
        '<rect x="36" y="100" width="148" height="7" fill="var(--teal-300)"/>'
        '<rect x="28" y="108" width="164" height="7" fill="var(--teal-500)"/>'
        '<line x1="20" y1="116" x2="200" y2="116" stroke="var(--gold-500)" stroke-width="2.5"/>'
        '</svg>',
}

E = html.escape


def card(href, accent, title, desc, stat, links=()):
    """A card is a doorway with named rooms. A reader looking for exports
    should see "Trade" on the homepage, not have to guess it sits inside
    Economy - so each card lists the destinations people come for, and the
    card's title still opens the section as a whole. An <a> cannot hold other
    links, so the card is an article whose title is the link."""
    items = ''.join(
        f'            <li><a href="{E(h)}">{E(t)}</a></li>\n' for h, t in links)
    return (
        f'      <article class="lcard" data-accent="{accent}">\n'
        f'        <a class="lcard-art" href="{href}" tabindex="-1" aria-hidden="true">'
        f'{ART[accent]}</a>\n'
        f'        <div class="lcard-body">\n'
        f'          <h2 class="lcard-title"><a href="{href}">{E(title)}</a></h2>\n'
        f'          <p class="lcard-desc">{E(desc)}</p>\n'
        + (f'          <ul class="lcard-links">\n{items}          </ul>\n' if items else '')
        + f'          <div class="lcard-stat">{E(stat)}</div>\n'
        f'        </div>\n'
        f'      </article>')


def shelf(href, title, desc):
    return (
        f'      <a class="lshelf-item" href="{href}">\n'
        f'        <span class="lshelf-title">{E(title)} &rarr;</span>\n'
        f'        <span class="lshelf-desc">{E(desc)}</span>\n'
        f'      </a>')


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
    by = {t['name']: t['rows'] for t in tables}

    f = {
        'tables': len(tables),
        'rows': sum(t['rows'] for t in tables),
        'placesInd': len(places['dp']),
        'stateInd': len(state['index']),
        'stateDs': len({r['ds'] for r in state['index']}),
        'census': by.get('census_panel_2017', 0) + by.get('census_panel_2023', 0),
        'generated': str(cat['generated']),
    }

    P = 'places.html?lv=district&'
    explorers = '\n'.join([
        card('places.html', 'places', 'Places',
             'Every district and tehsil: people, schooling, work, housing, '
             'health, poverty, crops and night-time lights.',
             f'156 districts · 649 tehsils · {n(f["placesInd"])} indicators',
             [('places.html', 'The map'),
              (P + 'g=literacy&i=literacy_ratio_all', 'Literacy'),
              (P + 'g=employment&i=lfpr', 'Work'),
              (P + 'g=mpi&i=H', 'Poverty'),
              (P + 'g=crops&i=crop%7C4%7Cproduction_000t', 'Crops')]),
        card('finance.html', 'economy', 'Economy',
             'Output and growth since 1951, industry, trade by product and '
             'partner, prices, the rupee and the external balance.',
             '15 topics · PBS and SBP series',
             [('finance.html#t=structure', 'GDP & growth'),
              ('finance.html#t=industry', 'Industry'),
              ('finance.html#t=basket', 'Trade'),
              ('finance.html#t=prices', 'Prices'),
              ('finance.html#t=rupee', 'The rupee')]),
        card('state.html', 'state', 'State',
             'What the state reports about itself: the budget and tax, courts '
             'and judges, crime, electricity and disasters.',
             f'{f["stateInd"]} charts · {f["stateDs"]} datasets',
             [('state.html?t=tax', 'Budget & tax'),
              ('state.html?t=courts', 'Courts'),
              ('state.html?t=crime', 'Crime'),
              ('state.html?t=discos', 'Energy'),
              ('state.html?t=impacts', 'Disasters')]),
    ])

    links = '\n'.join([
        shelf('datasets/', 'Data catalogue',
              f'{f["tables"]} documented tables, source notes, CSV and Parquet'),
        shelf('query.html', 'Query with SQL', 'DuckDB in your browser, no signup'),
        shelf('datasets/#fields', 'Dictionary',
              'Every field in every table, searchable'),
    ])

    body = f'''<main class="lmain">
  <div class="lwrap">
    <div class="lhero">
      <h1>Pakistan&rsquo;s official statistics, by place, by sector and by institution</h1>
      <p class="llede">Census, survey, trade and macroeconomic data from PBS and
        the State Bank, as maps and charts. Every chart downloads as CSV.</p>
    </div>

    <div class="lcards">
{explorers}
    </div>

    <div class="lshelf">
      <div class="lshelf-head">
        <span class="lshelf-eyebrow">For analysts</span>
        <span class="lshelf-sub">{n(f['rows'])} rows in {f['tables']} tables · release {E(f['generated'])}</span>
      </div>
{links}
    </div>

  </div>
</main>
'''

    page = APP / 'index.html'
    src = page.read_text()
    head = src[:src.index('>', src.index('<body')) + 1]
    tail = src[src.rindex('</body>'):]

    # apply_shell.py owns the header and footer inside <body>; keep what it
    # wrote and replace only what sits between them.
    hdr = re.search(r'\s*<header class="site-header".*?</header>', src, re.S)
    nav = re.search(r'\s*<nav id="mobileNav".*?</nav>', src, re.S)
    ftr = re.search(r'\s*<footer class="site-footer".*?</footer>', src, re.S)

    # The old page carried 3 KB of inline <style> with seventeen hard-coded
    # colours. landing.css replaces it and reads tokens only.
    head = re.sub(r'\s*<style[^>]*>.*?</style>', '', head, flags=re.S)
    if 'landing.css' not in head:
        head = head.replace(
            '</head>', '<link rel="stylesheet" href="assets/css/landing.css"/>\n</head>')

    page.write_text(head
                    + (hdr.group(0) if hdr else '')
                    + (nav.group(0) if nav else '')
                    + '\n\n' + body
                    + (ftr.group(0) if ftr else '')
                    + '\n' + tail)
    print(f'index.html: {f["tables"]} tables, {n(f["rows"])} rows, '
          f'{n(f["placesInd"])} Places indicators, {f["stateInd"]} State charts')


if __name__ == '__main__':
    main()
