#!/usr/bin/env python3
"""Federal budget PDFs -> the layout-preserving text the detailed budget builders read.

build_budget_expenditure.py and build_budget_receipts.py do not open the PDFs; they read
`pdftotext -layout` output, one text file per fiscal year. That step was typed at a shell
when the builders were first written and never saved. This is it.

Input : $DD_RAW/budget_documents/<fy>/Budget_in_Brief.pdf
        $DD_RAW/budget_documents/<fy>/Explanatory_Memorandum_Federal_Receipts.pdf
Output: $DD_WORK/bud/bib_<fy>.txt   (read by build_budget_expenditure.py)
        $DD_WORK/bud/em_<fy>.txt    (read by build_budget_receipts.py)

Needs `pdftotext` (poppler-utils) on the PATH.
Usage : python3 etl/warehouse/extract_budget_text.py [fy ...]      e.g. 2025-26; default every year
"""
import re, subprocess, sys
from dd_paths import RAW, BUD

DOCS = {'bib': 'Budget_in_Brief.pdf', 'em': 'Explanatory_Memorandum_Federal_Receipts.pdf'}


def main():
    want = set(sys.argv[1:])
    base = RAW / 'budget_documents'
    years = sorted(d.name for d in base.iterdir() if d.is_dir() and re.fullmatch(r'\d{4}-\d{2}', d.name))
    if not years:
        sys.exit(f'no fiscal-year folders under {base}')
    for fy in years:
        if want and fy not in want:
            continue
        for prefix, name in DOCS.items():
            pdf = base / fy / name
            if not pdf.exists():
                print(f'{fy}: no {name}', file=sys.stderr)
                continue
            out = BUD / f'{prefix}_{fy}.txt'
            subprocess.run(['pdftotext', '-layout', str(pdf), str(out)], check=True)
            print(f'{fy}: {name} -> {out.name} ({out.stat().st_size // 1024} KB)')


if __name__ == '__main__':
    main()
