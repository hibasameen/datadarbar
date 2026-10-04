"""The long-run Economy charts break where the comparison would mislead.

Money growth computed across a change of definition, or across 1971-72 when
East Pakistan leaves the figures, is a border or a redefinition rather than a
change in money; inflation computed across a rebasing of the CPI is the change
of base. history_data.js carries a NULL at each of those points so the line
breaks, and these checks keep it that way.
"""
import json
import pathlib

APP = pathlib.Path(__file__).resolve().parent.parent / 'app'


def payload():
    s = (APP / 'data' / 'history_data.js').read_text()
    return json.loads(s[s.index('{'):s.rstrip().rstrip(';').rfind('}') + 1])


def gaps(series):
    return {p[0][:4] for p in series if p[1] is None}


def test_money_growth_breaks_at_1972_and_each_redefinition():
    S = payload()['series']
    for k in ('m2_growth', 'm0_growth'):
        g = gaps(S[k])
        assert '1972' in g, f'{k} joins across 1971-72'
        assert {'1987', '1991', '2005'} <= g, f'{k} joins across a change of definition: {sorted(g)}'


def test_inflation_breaks_at_each_rebasing():
    assert len(gaps(payload()['series']['cpi_infl'])) >= 7


def test_peers_compare_with_pakistan():
    P = payload()['peers']
    assert P['indicators'] and all('PAK' in P['data'][i['code']] for i in P['indicators'])
