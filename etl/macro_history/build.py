"""Build the long-run macro tables from the capture made by fetch.py.

Three tables, each from a source whose terms allow it to be republished:

  sbp_handbook_series   SBP Handbook of Statistics on Pakistan Economy 2020, the
                        tables that fill the gaps: money since 1950 (4.1), real
                        GDP since FY50 (1.5), price indices since FY50 (2.8) and
                        consolidated public finance since FY76 (3.7). Values are
                        as SBP printed them - SBP's terms ask that the data not
                        be changed - with the block, base year and footnote
                        marker each figure was printed under.
  imf_pakistan_fiscal   IMF fiscal history for Pakistan: Public Finances in
                        Modern History (from 1950), the Global Debt Database,
                        the Fiscal Monitor and four WEO series.
  wdi_comparators       World Development Indicators for Pakistan beside South
                        Asia and comparable economies.

Nothing is spliced. Where two sources cover the same years both are kept and
labelled, because choosing between them is a judgement the reader should see.

    python3 etl/macro_history/build.py --capture ../raw_data/macro_history/<date> \
        --out ../data_darbar_warehouse/macro_history/<date>
"""
import argparse, json, pathlib, re, warnings

import duckdb
import openpyxl

warnings.filterwarnings('ignore', category=UserWarning)

# Which Handbook tables, and how they are laid out. 'rows' = one period per
# row; 'cols' = one period per column.
HANDBOOK = {
    '4.1': (4, 'rows', 'Monetary statistics: currency, reserve money (M0), narrow money (M1), '
                       'broad money (M2)'),
    '1.5': (1, 'rows', 'Gross domestic product at constant factor cost, long series'),
    '2.8': (2, 'rows', 'Price indices (CPI, WPI, SPI) and GDP deflator'),
    '3.7': (3, 'cols', 'Summary of public finance, consolidated federal and provincial'),
}

PERIOD = re.compile(r'^\s*(FY\s*\d{2}|\d{4})\s*(\S*)\s*$')
MONTH = {'jun': 6, 'dec': 12, 'mar': 3, 'sep': 9}
BASE = re.compile(r'\(\s*(\d{4}\s*-\s*\d{2,4})\s*=\s*100\s*\)')


def text(v):
    return '' if v is None else re.sub(r'\s+', ' ', str(v)).strip()


def number(v):
    """(value, marker). SBP prints a dash for nil or not available, and
    occasionally a footnote star or an unbalanced bracket on a figure; the
    figure is read, the mark is kept, nothing is guessed."""
    if v is None:
        return None, ''
    if isinstance(v, (int, float)):
        return float(v), ''
    s = text(v)
    if s in ('', '-', '--', '–', '—', '...', '..', 'n.a', 'n.a.', 'NA'):
        return None, s
    mark = ''.join(ch for ch in s if ch in '*()#')
    core = re.sub(r'[*#,\s]', '', s)
    if core.startswith('(') and not core.endswith(')'):
        # "(28828" - a negative printed with its closing bracket missing.
        # Not read: the sign is a guess. Kept as text.
        return None, s
    core = core.strip('()') if core.startswith('(') and core.endswith(')') else core
    try:
        val = float(core)
    except ValueError:
        return None, s
    if s.startswith('(') and s.endswith(')'):
        val = -val
    return val, mark


def fiscal_year(label):
    """FY50 -> 1950 (the year the fiscal year ends, July to June)."""
    m = re.match(r'FY\s*(\d{2})', label)
    if not m:
        return None
    yy = int(m.group(1))
    return (1900 if yy >= 47 else 2000) + yy


def sheet_notes(rows):
    src, notes = '', []
    for r in rows:
        line = ' '.join(text(v) for v in r if text(v))
        if not line:
            continue
        if line.lower().startswith('source'):
            src = line
        elif re.match(r'^(\d+\.|\*|note)', line, re.I) and len(line) > 12:
            notes.append(line)
    return src, ' '.join(notes)


def parse_rows(table, rows):
    """Periods down the side. Header lines are read from 'Period' to the
    numbering row (1 2 3 ...) or the first period, and joined per column, so a
    label printed over four lines reads as one."""
    out, block, labels, header, base = [], 0, {}, None, {}
    year, src, notes = None, *sheet_notes(rows)
    for r in rows:
        cells = list(r)
        first = text(cells[1]) if len(cells) > 1 else ''
        if first.lower().startswith('period'):
            block += 1
            header, labels, base, year = [cells], {}, {}, None
            continue
        if header is not None and not PERIOD.match(first):
            nums = [text(c) for c in cells[3:] if text(c)]
            if nums and all(re.fullmatch(r'\d+', n) for n in nums) and not first:
                labels['__numbered'] = {i: text(c) for i, c in enumerate(cells) if text(c)}
                continue
            if any(BASE.search(text(c)) for c in cells) and not first:
                pass                        # a base line under the header
            else:
                header.append(cells)
                continue
        if header is not None and not labels.get('__done'):
            width = max(len(h) for h in header)
            for i in range(2, width):
                parts = [text(h[i]) for h in header if i < len(h) and text(h[i])]
                if parts:
                    labels[i] = ' '.join(parts)
            labels['__done'] = True
            # The header is finished. Left open, every half-year row that
            # prints its month but not its year ("  | Dec. | ...") read as
            # another header line and the December figures were lost.
            header = None
        # A base-year line: applies to the columns it sits over, until the next.
        if not first and any(BASE.search(text(c)) for c in cells):
            for i, c in enumerate(cells):
                m = BASE.search(text(c))
                if m:
                    base[i] = re.sub(r'\s+', '', m.group(1))
            continue
        m = PERIOD.match(first)
        if first and m:
            year = m.group(1).replace(' ', '')
            marker = m.group(2)
        elif first:
            continue                            # Source:, notes, titles
        elif not year:
            continue
        else:
            marker = ''
        month = text(cells[2]).rstrip('.').lower()[:3] if len(cells) > 2 else ''
        if not labels.get('__done'):
            continue
        for i, c in enumerate(cells):
            if not isinstance(i, int) or i < 2 or i not in labels:
                continue
            if i == 2 and month in MONTH:
                continue
            val, mark = number(c)
            raw = text(c)
            if val is None and not raw:
                continue
            fy = fiscal_year(year)
            out.append({
                'table_id': table, 'block': block,
                'series_no': labels.get('__numbered', {}).get(i, ''),
                'series': labels[i],
                'period': year + (' ' + month.title() if month in MONTH else ''),
                'year': fy if fy else int(year),
                'year_basis': 'fiscal (July-June, year it ends)' if fy else 'calendar',
                'month': MONTH.get(month),
                'value': val, 'value_text': raw if val is None or mark else '',
                'mark': (marker + ' ' + mark).strip(), 'base': base.get(i, ''),
                'source_line': src, 'notes': notes})
    return out


def parse_cols(table, rows):
    """Periods across the top (FY76 FY77 ...), items down the side - and the
    sheet may hold several such panels side by side, each with its own Item
    column. Table 3.7 prints FY76-FY99 and FY00-FY20 as two panels whose rows
    are NOT the same: the second has no autonomous-bodies line, so reading the
    second panel's figures against the first panel's labels shifts every item
    by a row."""
    out, panels = [], []
    src, notes = sheet_notes(rows)
    sections = {}
    for r in rows:
        cells = list(r)
        items = [i for i, c in enumerate(cells) if text(c).lower() == 'item']
        if items:
            panels = []
            for k, at in enumerate(items):
                stop = items[k + 1] if k + 1 < len(items) else len(cells)
                fys = {i: text(cells[i]) for i in range(at + 1, stop)
                       if re.match(r'FY\d\d', text(cells[i]))}
                panels.append((at, fys))
                sections[at] = 'Million Rupees'
            continue
        for at, fys in panels:
            first = text(cells[at]) if at < len(cells) else ''
            if not first:
                continue
            if first.startswith('(') and 'percent' in first.lower():
                sections[at] = 'Percent of GDP'
                continue
            if first.lower().startswith('source'):
                continue
            pct = sections[at] != 'Million Rupees'
            for i, lab in fys.items():
                if i >= len(cells):
                    continue
                val, mark = number(cells[i])
                if val is None and not text(cells[i]):
                    continue
                out.append({
                    'table_id': table, 'block': 2 if pct else 1,
                    'series_no': '', 'series': first + (' (% of GDP)' if pct else ''),
                    'period': lab, 'year': fiscal_year(lab),
                    'year_basis': 'fiscal (July-June, year it ends)', 'month': None,
                    'value': val, 'value_text': text(cells[i]) if val is None or mark else '',
                    'mark': mark, 'base': '', 'source_line': src, 'notes': notes})
    return out


def handbook(capture):
    rows_out = []
    for table, (ch, layout, title) in HANDBOOK.items():
        wb = openpyxl.load_workbook(capture / 'sbp_handbook_2020' / f'Ch-{ch}.xlsx',
                                    read_only=True, data_only=True)
        rows = list(wb[table].iter_rows(values_only=True))
        unit = next((text(r[1]) for r in rows[:6] if len(r) > 1
                     and text(r[1]).startswith('(')), '').strip('()')
        got = (parse_rows if layout == 'rows' else parse_cols)(table, rows)
        for g in got:
            g.update({'chapter': ch, 'table_title': title,
                      'unit': unit if table != '3.7' or g['block'] == 1 else 'Percent of GDP'})
        assert got, f'Handbook table {table} parsed to nothing'
        rows_out += got
        print(f'  {table}: {len(got):,} figures, {len({g["series"] for g in got})} series')
    return rows_out


def imf(capture):
    meta = json.loads((capture / 'imf' / 'indicators.json').read_text())['indicators']
    man = json.loads((capture / 'retrieval_manifest.json').read_text())
    out = []
    for f in man['files']:
        if f.get('source') != 'IMF' or not f.get('indicator'):
            continue
        code = f['indicator']
        d = json.loads((capture / f['path']).read_text())
        vals = d.get('values', {}).get(code, {}).get('PAK', {})
        m = meta.get(code, {})
        for y, v in vals.items():
            out.append({'dataset': f['dataset'], 'dataset_name': m.get('source') or '',
                        'indicator': code, 'label': m.get('label') or code,
                        'unit': m.get('unit') or '', 'year': int(y), 'value': float(v),
                        # WEO and the Fiscal Monitor are the April 2026 editions;
                        # from 2026 their figures are the IMF's projections.
                        'is_projection': f['dataset'] in ('WEO', 'FM') and int(y) >= 2026})
    return out


def wdi(capture):
    man = json.loads((capture / 'retrieval_manifest.json').read_text())
    out = []
    for f in man['files']:
        if f.get('source') != 'WB' or '/meta/' in f['path']:
            continue
        payload = json.loads((capture / f['path']).read_text())
        meta = json.loads((capture / 'wdi' / 'meta' / f"{f['indicator']}.json").read_text())
        src = (meta[1][0].get('sourceOrganization') or '').strip() if len(meta) > 1 else ''
        if len(payload) < 2 or not payload[1]:
            continue
        for r in payload[1]:
            if r['value'] is None:
                continue
            out.append({'country_code': r['countryiso3code'] or r['country']['id'],
                        'country': r['country']['value'],
                        'indicator': r['indicator']['id'], 'label': r['indicator']['value'],
                        'year': int(r['date']), 'value': float(r['value']),
                        'original_source': src})
    return out


def write(rows, path):
    con = duckdb.connect()
    tmp = path.with_suffix('.json')
    tmp.write_text(json.dumps(rows))
    con.execute(f"COPY (SELECT * FROM read_json_auto('{tmp}', maximum_object_size=200000000)) "
                f"TO '{path}' (FORMAT PARQUET)")
    tmp.unlink()
    print(f'  {path.name}: {len(rows):,} rows')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--capture', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    cap, out = pathlib.Path(a.capture), pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    print('SBP Handbook…')
    write(handbook(cap), out / 'sbp_handbook_series.parquet')
    print('IMF…')
    write(imf(cap), out / 'imf_pakistan_fiscal.parquet')
    print('WDI…')
    write(wdi(cap), out / 'wdi_comparators.parquet')
    (out / 'capture.txt').write_text(str(cap.resolve()) + '\n')


if __name__ == '__main__':
    main()
