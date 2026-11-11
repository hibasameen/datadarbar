#!/usr/bin/env python3
"""NEPRA State-of-Industry data repository -> plant x month generation table.

Input : the Excel-as-HTML sheets NEPRA publishes alongside each SOI report
        ("Details of Installed & Dependable Capacity (MW), Monthly Energy
        Generation (GWh) and Plant Utilization (%)"), FY2017-18 -> FY2024-25.
        Raw location: $NEPRA_RAW/html/**/sheet001.htm  (default raw dir below).
Output: $DD_WAREHOUSE/nepra_plant_month.parquet  (one row per plant x month, + a 'FY' total row)

Columns
  fiscal_year   '2017-18' ... '2024-25'
  sno           S.No in the published sheet (per-year ordering, NOT a stable id)
  plant         plant name as published (whitespace-normalised)
  company_group folder group taken from the daily-PDF link (WAPDA / Thermal / Wind ...), may be NULL
  technology    THERMAL / HYDEL-(WAPDA) / WIND / SOLAR / NUCLEAR / BAGASSE / Coal ...
  fuel          RFO / Natural Gas / HYDEL / ...
  installed_mw, dependable_mw
  month         'Jul'..'Jun' or 'FY' (annual total / annual utilisation)
  month_date    first day of the calendar month (NULL for 'FY')
  utilization_pct  plant utilisation factor (%) w.r.t. dependable capacity
  generation_gwh
  daily_pdf     relative path (inside raw dir) of the daily-detail PDF for that plant-month
"""
import os, re, sys, glob
from pathlib import Path
from urllib.parse import unquote
import pandas as pd
from bs4 import BeautifulSoup

from dd_paths import NEPRA_RAW as RAW, WAREHOUSE
OUT = WAREHOUSE / 'nepra_plant_month.parquet'
MONTHS = ['Jul','Aug','Sep','Oct','Nov','Dec','Jan','Feb','Mar','Apr','May','Jun']

def num(s):
    s = (s or '').replace(',', '').replace('\xa0', '').strip()
    if s in ('', '-', '--', 'N/A', 'NA'):
        return None
    try:
        return float(s)
    except ValueError:
        return None

def norm(s):
    return re.sub(r'\s+', ' ', (s or '').replace('\xa0', ' ')).strip()

def month_date(fy, m):
    y0 = int(fy[:4])
    i = MONTHS.index(m)
    year = y0 if i < 6 else y0 + 1
    return pd.Timestamp(year=year, month=((i + 6) % 12) + 1, day=1)

def parse_sheet(path, fy):
    html = open(path, 'rb').read().decode('cp1252', errors='replace')
    soup = BeautifulSoup(html, 'lxml')
    sheet_dir = Path(path).parent  # links are relative to the sheet's own dir
    rows = []
    for tr in soup.find_all('tr'):
        tds = tr.find_all(['td', 'th'])
        cells = [norm(td.get_text(' ', strip=True)) for td in tds]
        if not cells or not re.fullmatch(r'\d+', cells[0]):
            continue
        # plant rows: sno, name, tech, fuel, inst, dep, then (pct, gwh) x 13
        if len(cells) < 6 + 26:
            continue
        sno = int(cells[0]); plant = cells[1]; tech = cells[2]; fuel = cells[3]
        inst = num(cells[4]); dep = num(cells[5])
        links = {}
        for j, td in enumerate(tds):
            a = td.find('a')
            if a and a.get('href'):
                links[j] = a['href']
        group = None
        for m_i, m in enumerate(MONTHS + ['FY']):
            c = 6 + 2 * m_i
            pct = num(cells[c]); gwh = num(cells[c + 1])
            href = links.get(c) or links.get(c + 1)
            rel = None
            if href:
                p = (sheet_dir / unquote(href)).resolve()
                try:
                    rel = str(p.relative_to(RAW.resolve()))
                except ValueError:
                    rel = unquote(href)
                # sheets live under html/<same tree>; the PDFs were saved without the html/ prefix
                if rel.startswith('html/'):
                    rel = rel[5:]
                if group is None:
                    parts = Path(rel).parts
                    # e.g. Detail of Generation/2017-2018/Thermal/Lal pir/Jul.pdf -> Thermal
                    group = parts[-3] if len(parts) >= 3 else None
            rows.append(dict(fiscal_year=fy, sno=sno, plant=plant, company_group=None,
                             technology=tech, fuel=fuel, installed_mw=inst, dependable_mw=dep,
                             month=m, month_date=(month_date(fy, m) if m != 'FY' else pd.NaT),
                             utilization_pct=pct, generation_gwh=gwh, daily_pdf=rel))
        for r in rows[-13:]:
            r['company_group'] = group
    return rows

def main():
    sheets = []
    for p in glob.glob(str(RAW / 'html' / '**' / 'List of Companies Genenration wise *_files' / 'sheet001.htm'), recursive=True):
        fy = re.search(r'wise (\d{4}-\d{2})', p).group(1)
        sheets.append((fy, p))
    sheets.sort()
    all_rows = []
    for fy, p in sheets:
        rows = parse_sheet(p, fy)
        n_plants = len({r['sno'] for r in rows})
        tot = sum(r['generation_gwh'] or 0 for r in rows if r['month'] == 'FY')
        print(f'{fy}: {n_plants} plants, FY total {tot:,.0f} GWh', file=sys.stderr)
        all_rows += rows
    df = pd.DataFrame(all_rows)
    df['month_date'] = pd.to_datetime(df['month_date'])
    df.to_parquet(OUT, index=False)
    print(f'wrote {OUT} rows={len(df)}', file=sys.stderr)

if __name__ == '__main__':
    main()
