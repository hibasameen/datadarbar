"""Where the warehouse builders read and write.

These scripts were first run in throwaway sandbox sessions with the paths typed in. The four
locations they need are named once here, each overridable by an environment variable:

  DD_RAW        the raw downloads: budget_documents/, trade_data_8digit/, national_accounts/ ...
                default  <workspace>/raw_data
  DD_WAREHOUSE  the finished warehouse (Parquet files + data_darbar.duckdb)
                default  <workspace>/data_darbar_warehouse
  DD_WORK       scratch for intermediates (stage Parquet, pdftotext output, a staging copy of
                the warehouse, the econ_data JSON)     default  <system temp>/dd_warehouse
  NEPRA_RAW     the NEPRA collection, which is kept outside DD_RAW
  NEPRA_CACHE   per-report parse cache for the NEPRA PDF builders   default  $DD_WORK/nepra/cache

<workspace> is the folder that holds this repository's checkout, which is how the other
pipelines in etl/ find data_darbar_warehouse/ too.
"""
import os, tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WORKSPACE = REPO.parent


def _env(name, default):
    return Path(os.environ.get(name) or default).expanduser()


RAW = _env('DD_RAW', WORKSPACE / 'raw_data')
WAREHOUSE = _env('DD_WAREHOUSE', WORKSPACE / 'data_darbar_warehouse')
WORK = _env('DD_WORK', Path(tempfile.gettempdir()) / 'dd_warehouse')
STAGE = WORK / 'warehouse'      # assemble.py / finalize.py build here; copy into WAREHOUSE once checked
ECON = WORK / 'econ_data'       # chart JSON written by build_econ*, build_ind, build_io, build_budget_tree*
BUD = WORK / 'bud'              # pdftotext output of the budget PDFs (extract_budget_text.py)
NEPRA_RAW = _env('NEPRA_RAW', Path.home() / 'Library/Mobile Documents/com~apple~CloudDocs/Adaad')
NEPRA_CACHE = _env('NEPRA_CACHE', WORK / 'nepra' / 'cache')
