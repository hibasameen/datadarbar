#!/usr/bin/env python3
"""Splice a rebuilt industry.json into app/assets/js/econ_data.js.

econ_data.js (`window.ECON={...}`, ~2.9 MB) is a committed artefact that no
script assembles; finance.js reads LSM/QIM from `window.ECON.industry`. This
replaces ONLY the value of the top-level "industry" key and leaves every other
byte of the file as it was. It also rewrites app/data/econ_industry.json (the
same block as a standalone file; not loaded by the page).

    python3 etl/splice_econ_industry.py [path/to/industry.json]

Default input: <Data Darbar>/data_darbar_warehouse/econ_data/industry.json,
written by etl/build_industry.py.
"""
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
JS = REPO / 'app' / 'assets' / 'js' / 'econ_data.js'
JSON_OUT = REPO / 'app' / 'data' / 'econ_industry.json'
DEFAULT_SRC = REPO.parent / 'data_darbar_warehouse' / 'econ_data' / 'industry.json'
PREFIX = 'window.ECON='
KEY = 'industry'


def top_level_spans(text):
    """{key: (start, end)} of each top-level value in `window.ECON={...}`."""
    if not text.startswith(PREFIX) or text[len(PREFIX)] != '{':
        sys.exit(f'{JS}: does not start with {PREFIX}{{')
    dec = json.JSONDecoder()
    i, spans = len(PREFIX) + 1, {}
    while True:
        key, j = dec.raw_decode(text, i)
        if text[j] != ':':
            sys.exit(f'unexpected {text[j]!r} after key {key!r}')
        _, end = dec.raw_decode(text, j + 1)
        spans[key] = (j + 1, end)
        if text[end] == '}':
            return spans
        if text[end] != ',':
            sys.exit(f'unexpected {text[end]!r} after value of {key!r}')
        i = end + 1


def main():
    src = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_SRC
    new = json.loads(src.read_text(encoding='utf-8'))
    text = JS.read_text(encoding='utf-8')
    spans = top_level_spans(text)
    if KEY not in spans:
        sys.exit(f'{JS}: no top-level "{KEY}" key')
    a, b = spans[KEY]
    old = json.loads(text[a:b])
    # same compact style the existing file uses
    seg = json.dumps(new, separators=(',', ':'), ensure_ascii=False)
    out = text[:a] + seg + text[b:]

    # every other key must be byte-identical
    spans2 = top_level_spans(out)
    assert list(spans2) == list(spans)
    for k, (x, y) in spans.items():
        if k != KEY:
            x2, y2 = spans2[k]
            assert text[x:y] == out[x2:y2], k

    tmp = JS.with_suffix('.js.tmp')
    tmp.write_text(out, encoding='utf-8')
    tmp.replace(JS)
    JSON_OUT.write_text(json.dumps(new), encoding='utf-8')   # build_industry.py's own style

    tr_old, tr_new = old.get('trend', []), new.get('trend', [])
    print(f'{JS.relative_to(REPO)}: {len(text):,} -> {len(out):,} bytes; '
          f'industry {b - a:,} -> {len(seg):,} bytes; other keys unchanged: '
          f'{", ".join(k for k in spans if k != KEY)}')
    print(f'trend {tr_old[-1]["month"] if tr_old else "-"} -> {tr_new[-1]["month"] if tr_new else "-"}; '
          f'new_base FYs {sorted(old.get("new_base", {}))[-1:]} -> {sorted(new.get("new_base", {}))[-2:]}')
    print(f'wrote {JSON_OUT.relative_to(REPO)}')


if __name__ == '__main__':
    main()
