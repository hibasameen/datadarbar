"""The provenance record and the export contract, held in place.

Both of these regressed silently once. The rail's dataset map drifted from
the charts until four of them named a table that could not produce them, and
the State CSV button fell through to a catalogue redirect for three charts
because nobody noticed they were missing from a lookup. Neither failure was
visible on the page: the chart still drew, and the button still did
something. These are the two checks that would have caught them.
"""
import json
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parents[1]
APP = ROOT / 'app'

# The budget charts export from their own grouped payload (DD_BUDGET) through
# budgetCsv(), not from a warehouse block, since the budget moved to this page
# on 29 September. They are checked separately below rather than exempted.
BUDGET_TOPIC = 'budget'


def _state_data():
    s = (APP / 'data/state_data.js').read_text()
    return json.loads(s[s.index('{'):s.rindex(';')])


def test_every_state_chart_exports_its_table():
    js = (APP / 'assets/js/state.js').read_text()
    body = re.search(r'var CSV_BLOCK = \{(.*?)\n  \};', js, re.S).group(1)
    body = re.sub(r'/\*.*?\*/', '', body, flags=re.S)
    block = dict(re.findall(r"(\w+):\s*'(\w+)'", body))

    D = _state_data()
    charts = {r['chart'] for r in D['index'] if r['topic'] != BUDGET_TOPIC}
    budget = {r['chart'] for r in D['index'] if r['topic'] == BUDGET_TOPIC}
    # The budget's route: state.js hands the whole topic to budgetCsv(), which
    # must exist and read the budget payload the page loads.
    assert budget, 'the budget topic has no charts'
    assert "current.topic === 'budget' && BUDGET) return budgetCsv()" in js
    assert 'function budgetCsv()' in js
    assert 'window.DD_BUDGET=' in (APP / 'data/budget_data.js').read_text()
    for c in sorted(charts):
        b = block.get(c)
        assert b, f'{c} is in the State index but in no CSV block'
        assert isinstance(D.get(b), dict) and 'cols' in D[b], (
            f'{c} maps to {b}, which is not a table')


def test_provenance_builds_and_covers_every_chart():
    out = ROOT / 'app/data/provenance.js'
    r = subprocess.run(
        [sys.executable, str(ROOT / 'etl/build_provenance.py'),
         '--app', str(APP), '--out', str(out)],
        capture_output=True, text=True)
    assert r.returncode == 0, r.stderr

    s = out.read_text()
    P = json.loads(s[s.index('{'):s.rindex(';')])
    charts = {r['chart'] for r in _state_data()['index']}
    for c in charts:
        assert 'state:' + c in P, f'{c} has no provenance record'

    # A record names a real table or says plainly that it has none. Borrowing
    # the name of a table that does not contain the chart is the failure this
    # whole file exists for.
    cat = json.loads((APP / 'data/warehouse/catalog.json').read_text())
    tables = {t['name'] for t in cat['tables']}
    for cid, rec in P.items():
        assert rec.get('publisher') and rec.get('publication'), cid
        if rec.get('table'):
            assert rec['table'] in tables, f'{cid} names {rec["table"]}'
        else:
            assert rec.get('origin'), f'{cid} has no table and does not say so'


