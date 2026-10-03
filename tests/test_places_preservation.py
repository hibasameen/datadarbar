"""Every indicator the index holds must still have a route a reader can take.

The picker was rebuilt around topics, featured measures and a library on 29
September, and the first version quietly dropped the 463 source totals and
denominators from every route but SQL - the old list search had carried them,
the new library did not. Nothing on the page looked wrong. These checks run
the picker's own grouping over the payload the page ships and fail if any row
at either level is unreachable, if a featured measure points nowhere, or if a
link shared before cells were merged can no longer be resolved.
"""
import json
import pathlib
import subprocess

ROOT = pathlib.Path(__file__).resolve().parents[1]
APP = ROOT / 'app'

JS = r"""
const fs = require('fs');
global.window = {};
eval(fs.readFileSync(process.argv[1], 'utf8'));          // places_index.js
eval(fs.readFileSync(process.argv[2], 'utf8'));          // places-rail.js
const IX = window.DD_PLACES_IX, N = IX.level.i.length;
const col = (n, r) => { const c = IX[n]; return c.v ? c.v[c.i[r]] : c[r]; };
const list = (n, r) => { const s = col(n, r); return s ? s.split('\u001f') : []; };
const out = {};
for (const lv of ['district', 'tehsil']) {
  const b = window.DDPlacesRail._build(IX, N, col, list, lv);
  const seen = {};
  b.measures.forEach(m => m.rows.forEach(r => { seen[r.row] = (seen[r.row] || 0) + 1; }));
  const rows = [];
  for (let i = 0; i < N; i++) if (col('level', i) === lv) rows.push(i);
  out[lv] = {
    rows: rows.length,
    unreached: rows.filter(i => !seen[i]),
    twice: rows.filter(i => seen[i] > 1),
    totals: b.measures.filter(m => m.total).reduce((a, m) => a + m.rows.length, 0),
    topicless: b.measures.filter(m => !m.topic).length,
  };
}
console.log(JSON.stringify(out));
"""


def _run():
    r = subprocess.run(['node', '-e', JS, str(APP / 'data/places_index.js'),
                        str(APP / 'assets/js/places-rail.js')],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return json.loads(r.stdout)


def _payload():
    s = (APP / 'data/places_index.js').read_text()
    return json.loads(s[s.index('{'):s.rstrip().rfind('}') + 1])


def test_every_row_is_in_exactly_one_library_entry():
    got = _run()
    for lv, g in got.items():
        assert not g['unreached'], f'{lv}: {len(g["unreached"])} rows no route reaches'
        assert not g['twice'], f'{lv}: {len(g["twice"])} rows listed under two measures'
        assert not g['topicless'], f'{lv}: {g["topicless"]} measures with no topic'
    # The source totals are there, in their own section, not dropped. 463 until
    # 3 October 2026, when 32 of them turned out to be one workbook's misspelt
    # copy of a total every other district prints - Kohistan's ALL rows, five
    # districts' POPULATION for POPULATION - 2017 - and folded back into it.
    assert got['district']['totals'] + got['tehsil']['totals'] == 431


def test_featured_measures_point_at_real_rows():
    ix = _payload()
    n = len(ix['dp'])
    for lv, topics in ix['featured'].items():
        for topic, rows in topics.items():
            assert rows, f'{lv}/{topic} has no opening measure'
            for r in rows:
                assert 0 <= r < n
                lvl = ix['level']['v'][ix['level']['i'][r]]
                assert lvl == lv, f'{lv}/{topic} features a {lvl} row'


def test_links_shared_before_the_merge_still_resolve():
    # A merged row's indicator is "2017:female=<cell>\x1f2023:all=<cell>". A
    # link that named one of those cells is resolved by finding the row that
    # holds it, so each cell must be held by exactly one row per level.
    ix = _payload()
    dec = lambda c: [ix[c]['v'][i] for i in ix[c]['i']]
    level, ind = dec('level'), dec('indicator')
    import re
    merged = re.compile(r'^((?:19|20)\d\d)(?::([a-z]+))?=')
    holders = {}
    for r, s in enumerate(ind):
        if not merged.match(s):
            continue        # "Literate >=10" has an "=" and is not a merge
        for part in s.split('\x1f'):
            m = merged.match(part)
            holders.setdefault((level[r], part[m.end():]), set()).add(r)
    assert holders, 'no merged rows - the check is not checking anything'
    shared = {k: v for k, v in holders.items() if len(v) > 1}
    assert not shared, f'{len(shared)} old cells resolve to two rows, e.g. {next(iter(shared))}'
