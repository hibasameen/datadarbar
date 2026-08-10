# Industry & structure build scripts

These two scripts produce the JSON behind the Economy & Budget page
(`app/data/econ_industry.json` and `app/data/econ_structure.json`).

## build_industry.py — QIM / LSM / CMI
Parses PBS industry statistics into the warehouse and the app bundle:

- **QIM**: monthly Quantum Index of Manufacturing — `Trend-sheet.xlsx` (Jul-2016 →),
  plus monthly sector indices (Table-2/Table-3 xlsx for recent years).
- **LSM sector indices**: annual Table-2 PDFs on both bases —
  2015-16 base (2015-16 → 2023-24) and 2005-06 base (2005-06 → 2021-22).
  The app splices old-base onto new-base at the 2015-16 overlap (dashed = linked).
- **CMI**: Census of Manufacturing Industries 2015-16 report (summary tables
  3.2/3.3/3.5/3.13) and the 2005-06 comparison — the only *measured* observations
  of small-scale/registered manufacturing between benchmarks. This is what the
  "censuses" panel uses instead of the annual SSM series.

Inputs are the raw PBS files (not committed — see https://www.pbs.gov.pk/industry-2/);
expected layout is documented in the script docstring. PDF tables are extracted with
`pdftotext -layout`; numbers can contain stray spaces ("1, 093, 235") which the
parser strips.

## build_structure.py — long-arc sector series
Builds growth/shares/backcast from the national-accounts warehouse table
(PBS 2015-16-base workbook): real growth 1951-52 → (Table-1), GVA shares
1999-00 → incl. LSM/SSM sub-sectors (Table 7b), and a pre-1999 backcast from
official real growth rates anchored at 1999-00 (indicative only).

**Note on SSM**: small-scale manufacturing in the national accounts is imputed
at a near-constant assumed growth rate (~8-9% p.a.) between CMI benchmarks, not
surveyed annually. Treat the annual SSM series accordingly; the CMI censuses are
the hard observations.

Both scripts read from a local DuckDB/Parquet warehouse built from the raw PBS
downloads; they're included for provenance and reproducibility of the published JSON.
