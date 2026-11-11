#!/usr/bin/env python3
"""Join the two halves of the 8-digit trade table.

build_trade.py parses the years PBS published as text or Excel; build_trade_pdf.py parses
2021-22 to 2023-24, which exist only as PDF. The warehouse table is the two stacked, with
the PDF rows given a source_file. That join was done by hand when the PDF years were added
and no script was kept; this one was written afterwards (2026-11) and checked to give
the same 1,134,442 rows as the warehouse's trade_hs8.parquet (row order differs).

Input : $DD_WORK/trade_hs8.parquet        from build_trade.py
        $DD_WORK/trade_pdf_2124.parquet   from build_trade_pdf.py
Output: $DD_WORK/trade_hs8.parquet        rewritten with the PDF years appended
Safe to rerun: PDF years already present are not added twice.
"""
import pandas as pd
from dd_paths import WORK

base = pd.read_parquet(WORK / 'trade_hs8.parquet')
pdf = pd.read_parquet(WORK / 'trade_pdf_2124.parquet')
pdf['source_file'] = pdf.direction.str.capitalize() + '_' + pdf.fiscal_year + '.pdf'
have = set(base.source_file.unique())
add = pdf[~pdf.source_file.isin(have)]
out = pd.concat([base, add[base.columns]], ignore_index=True)
out.to_parquet(WORK / 'trade_hs8.parquet', index=False)
print(f'{len(base)} existing rows + {len(add)} PDF rows = {len(out)}')
print(out.groupby(['fiscal_year', 'direction']).size().unstack().fillna(0).astype(int).to_string())
