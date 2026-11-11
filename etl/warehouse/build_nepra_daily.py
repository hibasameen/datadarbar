#!/usr/bin/env python3
"""NEPRA daily plant generation PDFs -> plant x day table.

Input : the ~11.5k one-page PDFs linked from the monthly sheets (one per plant-month),
        "Details of Generation Capacity (MW), Daily Energy Generation (GWh) and % age
        Plant utilization", FY2017-18 -> FY2024-25.  Raw: $NEPRA_RAW/{Detail of Generation,SOI Data}/**/*.pdf
        The join key to nepra_plant_month is the relative PDF path (nepra_plant_month.daily_pdf).
Output: $DD_WAREHOUSE/nepra_plant_day.parquet   one row per plant x day
        $DD_WAREHOUSE/nepra_plant_day_files.parquet  one row per PDF (parse status, header text) for QA

Row columns: daily_pdf, fiscal_year, plant, month, month_date, day, date,
             installed_mw, dependable_mw, load_max_mw, load_min_mw, generation_gwh, utilization_pct
Notes
  * Numbers '--', '-', 'N/A' -> NULL.  Some plants report only 5 numeric cols (no min load): handled.
  * The 'Total' line is NOT stored here (it equals the monthly sheet value; use nepra_plant_month).
  * The 'Sum YYYY-YY.pdf' annual files have a different (monthly) layout and are skipped.
"""
import os, re, sys, glob, calendar
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor
import pandas as pd

from dd_paths import NEPRA_RAW as RAW, WAREHOUSE as WH
MONTHS = ['Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun']
MONTH_RE = re.compile(r'(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s*,?\s*(20\d\d)', re.I)

def num(tok):
    t = tok.replace(',', '')
    if t in ('-', '--', '---', 'N/A', 'NA', 'nil', 'Nil'):
        return None
    try:
        return float(t)
    except ValueError:
        return 'BAD'

def lines_from_pdf(path):
    import pymupdf
    doc = pymupdf.open(path)
    out = []
    for page in doc:
        words = page.get_text('words')  # x0,y0,x1,y1,word,block,line,wordno
        words.sort(key=lambda w: (round(w[1] / 4), w[0]))
        rows, cur, cur_y = [], [], None
        for w in words:
            y = (w[1] + w[3]) / 2
            if cur_y is None or abs(y - cur_y) <= 4:
                cur.append(w); cur_y = y if cur_y is None else (cur_y + y) / 2
            else:
                rows.append(cur); cur = [w]; cur_y = y
        if cur: rows.append(cur)
        for r in rows:
            r.sort(key=lambda w: w[0])
            # some PDFs double-render every glyph (overlapping duplicate text objects): drop a word
            # that repeats the previous word at (almost) the same x position
            ded = []
            for w in r:
                if ded and w[4] == ded[-1][4] and abs(w[0] - ded[-1][0]) < 3:
                    continue
                ded.append(w)
            out.append(' '.join(w[4] for w in ded))
    doc.close()
    return out

def parse_one(rel):
    path = RAW / rel
    rec = dict(daily_pdf=rel, status='ok', header='', month_txt=None, year_txt=None, n_days=0, note='')
    rows = []
    try:
        lines = lines_from_pdf(path)
    except Exception as e:
        rec['status'] = 'err:' + str(e)[:80]
        return rec, rows
    txt = '\n'.join(lines)
    rec['header'] = ' | '.join(l for l in lines[:8])[:300]
    m = MONTH_RE.search(txt)
    if m:
        rec['month_txt'] = m.group(1)[:3].title(); rec['year_txt'] = int(m.group(2))
    for l in lines:
        toks = l.split()
        if not toks or not re.fullmatch(r'\d{1,2}', toks[0]):
            continue
        day = int(toks[0])
        if not 1 <= day <= 31:
            continue
        vals = [num(t) for t in toks[1:]]
        # a stray word like 'OK' in a numeric column -> NULL; more than 6 columns -> not a day row
        vals = [None if v == 'BAD' else v for v in vals]
        if len(vals) < 5 or len(vals) > 6:
            continue
        if len(vals) == 5:  # no min-load column
            inst, dep, mx, gwh, pct = vals; mn = None
        else:
            inst, dep, mx, mn, gwh, pct = vals
        rows.append(dict(daily_pdf=rel, day=day, installed_mw=inst, dependable_mw=dep,
                         load_max_mw=mx, load_min_mw=mn, generation_gwh=gwh, utilization_pct=pct))
    rec['n_days'] = len(rows)
    if not rows:
        rec['status'] = 'nodays'
    return rec, rows

def main():
    month = pd.read_parquet(WH / 'nepra_plant_month.parquet')
    keys = month[month.month != 'FY'].dropna(subset=['daily_pdf'])[['daily_pdf','fiscal_year','plant','month','month_date']].drop_duplicates('daily_pdf')
    def resolve(rel):
        """the sheets mix folder case (Wapda/WAPDA); macOS merged them, Linux mounts do not"""
        if (RAW / rel).exists():
            return rel
        cur = RAW; parts = Path(rel).parts
        for p in parts:
            if (cur / p).exists():
                cur = cur / p; continue
            hit = next((d for d in cur.iterdir() if d.name.lower() == p.lower()), None) if cur.exists() else None
            if hit is None:
                return None
            cur = hit
        return str(cur.relative_to(RAW))
    fixed = {r: resolve(r) for r in keys.daily_pdf}
    keys = keys.assign(daily_pdf=keys.daily_pdf.map(lambda r: fixed[r] or r))
    rels = [r for r in keys.daily_pdf if (RAW / r).exists() and (RAW / r).stat().st_size > 0]
    missing = len(keys) - len(rels)
    print(f'{len(rels)} daily PDFs to parse ({missing} referenced but not on disk)', file=sys.stderr)
    recs, rows = [], []
    with ProcessPoolExecutor(max_workers=os.cpu_count() or 4) as ex:
        for i, (rec, rr) in enumerate(ex.map(parse_one, rels, chunksize=20)):
            recs.append(rec); rows += rr
            if i % 1000 == 0:
                print(f'  {i}/{len(rels)}', file=sys.stderr)
    files = pd.DataFrame(recs).merge(keys, on='daily_pdf', how='left')
    day = pd.DataFrame(rows).merge(keys, on='daily_pdf', how='left')
    day['month_date'] = pd.to_datetime(day['month_date'])
    # calendar-validate the day
    dim = day.month_date.dt.days_in_month
    bad = day.day > dim
    if bad.any():
        print(f'  dropping {bad.sum()} rows with day > days_in_month', file=sys.stderr)
        day = day[~bad]
    day['date'] = day.month_date + pd.to_timedelta(day.day - 1, unit='D')
    day = day[['daily_pdf','fiscal_year','plant','month','month_date','day','date','installed_mw','dependable_mw',
               'load_max_mw','load_min_mw','generation_gwh','utilization_pct']].sort_values(['fiscal_year','plant','date'])
    day.to_parquet(WH / 'nepra_plant_day.parquet', index=False)
    # QA: sum of the daily GWh vs the monthly figure in the sheet (relative error; NULL if no daily rows)
    sheet = month[month.month != 'FY'][['daily_pdf', 'generation_gwh']].rename(columns={'generation_gwh': 'sheet_gwh'})
    sheet['daily_pdf'] = sheet.daily_pdf.map(lambda r: fixed.get(r, r) or r)
    dsum = day.groupby('daily_pdf').generation_gwh.sum().rename('daily_sum_gwh').reset_index()
    files = files.merge(sheet.drop_duplicates('daily_pdf'), on='daily_pdf', how='left').merge(dsum, on='daily_pdf', how='left')
    files['sum_vs_sheet_relerr'] = (files.daily_sum_gwh - files.sheet_gwh).abs() / files.sheet_gwh.abs().clip(lower=0.05)
    files.to_parquet(WH / 'nepra_plant_day_files.parquet', index=False)
    ok = files.sum_vs_sheet_relerr.dropna()
    print(f'  daily-sum vs sheet: {(ok < 0.01).mean():.1%} within 1%, {(ok < 0.05).mean():.1%} within 5% (n={len(ok)})', file=sys.stderr)
    print(files.status.str.slice(0, 6).value_counts().to_string(), file=sys.stderr)
    print(f'wrote nepra_plant_day.parquet rows={len(day)}', file=sys.stderr)

if __name__ == '__main__':
    main()
