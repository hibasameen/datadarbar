# Warehouse builders

The scripts that turn raw downloads into the local DuckDB/Parquet warehouse
(`data_darbar_warehouse/`) that the Economy and State pages, the query console and
`etl/economy/`, `etl/build_money.py` and `etl/build_web_warehouse.py` read from.

They were written in working sessions between August and October 2026 and kept beside the
warehouse, outside this repository. They are published here as they were run. The only
changes are that the folder locations are no longer typed into each script (see
`dd_paths.py`), one print statement in `finalize.py` was fixed for current DuckDB, and two
steps that had only ever been done by hand are written down (`extract_budget_text.py`,
`merge_trade.py`).

## Settings

| Variable | What it is | Default |
|---|---|---|
| `DD_RAW` | raw downloads (`budget_documents/`, `trade_data_8digit/`, `national_accounts/`) | `../raw_data` beside the checkout |
| `DD_WAREHOUSE` | the finished warehouse | `../data_darbar_warehouse` beside the checkout |
| `DD_WORK` | scratch for intermediates | `<system temp>/dd_warehouse` |
| `NEPRA_RAW` | the NEPRA collection | see `dd_paths.py` |
| `NEPRA_CACHE` | NEPRA per-report parse cache | `$DD_WORK/nepra/cache` |
| `SBP_API_KEY` | SBP EasyData key, for `build_sbp.py` only | none; never stored |

`pip install -r requirements.txt`, plus `pdftotext` from poppler-utils. Run the scripts from
this folder.

The raw files are not in the repository and the downloads are not scripted. Every file is a
public release of PBS, the Finance Division, NEPRA or the State Bank.

## Run order

**NEPRA** (details, schema and caveats in `NEPRA.md`). Writes straight to `DD_WAREHOUSE`.

```bash
python3 build_nepra_month.py
python3 build_nepra_daily.py
python3 build_nepra_soi.py            # or --from-cells to re-tidy without the PDFs
python3 build_nepra_per.py
python3 build_nepra_finalize.py       # plant crosswalk, DISCO series, DuckDB registration
```

**SBP EasyData** (details in `SBP_EASYDATA.md`). Writes straight to `DD_WAREHOUSE`.

```bash
python3 -m unittest test_build_sbp test_sbp_state     # offline
python3 build_sbp.py catalog
python3 build_sbp.py observations --select --tier headline core
python3 build_sbp.py load
```

**Trade, national accounts, budget.** These build into `$DD_WORK` so that a run cannot
damage the live warehouse; copy the Parquet files across once checked.

```bash
python3 build_trade.py                # text and Excel years  -> $DD_WORK/trade_hs8.parquet
python3 build_trade_pdf.py            # 2021-22 to 2023-24, PDF only
python3 merge_trade.py                # stacks the two
python3 build_na.py "$DD_RAW/national_accounts/annual_tables_2015-16_base/<workbook>.xlsx"
python3 build_budget.py               # every Budget in Brief table, line by line
python3 assemble.py                   # -> $DD_WORK/warehouse/parquet/*.parquet + file catalogue
cp "$DD_WORK"/warehouse/parquet/*.parquet "$DD_WORK"/warehouse/
python3 finalize.py                   # -> $DD_WORK/warehouse/data_darbar.duckdb
```

**Detailed budget trees** (the receipts and current-expenditure treemaps).

```bash
python3 extract_budget_text.py        # pdftotext -layout -> $DD_WORK/bud/{bib,em}_<fy>.txt
python3 build_budget_expenditure.py   # -> $DD_WORK/bud/exp_groups.json
python3 build_budget_receipts.py      # -> $DD_WORK/bud/receipts_tree.json
```

**Chart JSON.**

```bash
python3 build_io.py                   # 2015-16 input-output table, 12 sectors -> io_2015.json
python3 build_trade_extra.py          # country detail and movers; reads DD_WAREHOUSE
```

## Checked on 2026-11-11

Run against the raw folders and compared with what is in the warehouse and on the site:

| Output | Result |
|---|---|
| `trade_hs8` | same 1,134,442 rows (row order differs) |
| `national_accounts` (Sep 2026 workbook) | identical |
| `budget_lines` | identical |
| `file_catalog` | identical apart from the 24 NEPRA rows, which `build_nepra_finalize.py` adds, and one newer national-accounts manifest row |
| `exp_groups.json` vs `app/data/budget_expenditure_detailed.json` | identical, all 18 years |
| `receipts_tree.json` vs `app/data/budget_receipts_detailed.json` | identical, all 18 years |
| `io_2015.json` | matrix matches the block in `app/assets/js/econ_data.js` |
| `trade_extra.json` | identical |
| `nepra_soi_long` via `--from-cells` | identical |
| SBP | 37 offline tests pass |

Not rerun: the NEPRA extraction from PDFs and sheets (the raw collection was not to hand)
and the SBP pull (needs a key and about an hour of rate-limited requests).

## Superseded, kept for the record

`build_econ.py`, `build_econ2.py`, `build_ind.py`, `build_budget_tree.py` and
`build_budget_tree2.py` produced the first Economy payload in August 2026. They still run,
but the site's trade and macro series are now built by `etl/economy/` from the warehouse,
and the budget treemaps by the detailed builders above. `build_econ2.py` replaced
`build_econ.py`, and `build_budget_tree2.py` replaced `build_budget_tree.py`.

## Still not scripted

- Placing `exp_groups.json`, `receipts_tree.json` and `io_2015.json` inside
  `app/assets/js/econ_data.js` and the two `app/data/budget_*_detailed.json` files.
- The trade gap fill (exports 2017-18 from UN Comtrade; import totals for 2018-19 and
  2019-20 from the Economic Survey), which was done in a browser session.
- The district MPI build from PSLM 2019-20 microdata. The results are in the repository;
  the script is not.
- Downloading the raw files.
