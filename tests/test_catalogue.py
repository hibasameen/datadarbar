"""The catalogue documents every table, and nothing links to a retired page.

The exit test the rebuild plan set for the analysts' shelf: every table on the
site has a catalogue entry a stranger could join from. So every table in
catalog.json must sit on a shelf, appear in the index, have its own page that
links its file, and say what place keys it carries.
"""
import json
import pathlib
import re

APP = pathlib.Path(__file__).resolve().parent.parent / 'app'
CAT = json.loads((APP / 'data/warehouse/catalog.json').read_text())


def test_every_table_is_filed_listed_and_has_a_page():
    kinds = {k['key'] for k in CAT['kinds']}
    index = (APP / 'datasets/index.html').read_text()
    for t in CAT['tables']:
        assert t.get('kind') in kinds, f'{t["name"]} has no shelf'
        assert isinstance(t.get('keys'), list), f'{t["name"]} has no keys list'
        assert f'data-table="{t["name"]}"' in index, f'{t["name"]} is not in the index'
        page = APP / 'datasets' / t['name'].replace('_', '-') / 'index.html'
        assert page.is_file(), f'{t["name"]} has no page'
        assert f'/data/warehouse/{t["file"]}' in page.read_text(), f'{t["name"]} page does not link its file'


def test_boundaries_are_downloadable_geojson():
    for name, n in (('pbs_districts_2023.geojson', 156), ('pbs_tehsils_2023.geojson', 649)):
        g = json.loads((APP / 'data/boundaries' / name).read_text())
        assert g['type'] == 'FeatureCollection' and len(g['features']) == n, name
        assert not any(len(k) < 3 for f in g['features'] for k in f['properties']), \
            f'{name} still carries the payload\'s one-letter property names'


def test_nothing_links_to_retired_pages():
    retired = re.compile(r'href="/?(?:explore|dictionary)\.html')
    for p in APP.rglob('*.html'):
        if p.name in ('explore.html', 'dictionary.html') and p.parent == APP:
            continue
        assert not retired.search(p.read_text(errors='ignore')), f'{p.relative_to(APP)} links a retired page'


def test_every_table_names_its_owner():
    """About says each catalogue page names the owner of the data. A source
    line that names a script or another table names nobody."""
    for t in CAT['tables']:
        src = (t.get('source') or '').strip()
        assert src, f'{t["name"]} has no source'
        assert not re.match(r'(Built by|Derived from)\b', src), \
            f'{t["name"]} names a process, not an owner: {src[:60]}'


def test_every_table_states_its_licence():
    """A table without stated terms would inherit the site-wide CC BY, which
    can be freer than its source allows (SBP is non-commercial, OSM is
    share-alike). Every table carries its own, and the strictest wins."""
    order = ['open', 'unstated', 'share-alike', 'non-commercial']
    for t in CAT['tables']:
        lic = t.get('licence')
        assert lic and lic.get('terms'), f'{t["name"]} states no licence'
        assert lic['reuse'] in order, f'{t["name"]}: unknown reuse {lic["reuse"]}'
    sbp = [t for t in CAT['tables'] if t['name'].startswith('sbp_')]
    assert sbp and all(t['licence']['reuse'] == 'non-commercial' for t in sbp)
    osm = next(t for t in CAT['tables'] if t['name'] == 'healthsites_osm_2019')
    assert osm['licence']['reuse'] == 'share-alike'
