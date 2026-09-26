"""Guard: no output column may be a SQL reserved word.

`table`, `row`, `col` and `column` all had to be renamed after they broke a
DuckDB query. The panel is published to a public SQL console, so a reserved
word here is a bug for every user, not just the build.
"""
import unittest
import duckdb
from build_panel import TABLES  # noqa: F401  (import check)

PANEL_COLUMNS = ['province', 'table_id', 'district', 'unit', 'unit_source', 'unit_type',
                 'locality', 'sex', 'indicator', 'col_label', 'value', 'missing',
                 'source_file', 'sheet', 'src_row', 'src_col', 'region', 'missing_source',
                 'is_rate', 'renderings_disagree', 'dds_id', 'adm3_pcode', 'dd_id', 'geo_method']
CROSSWALK_COLUMNS = ['dds_id', 'province', 'district', 'unit', 'unit_type', 'adm3_pcode',
                     'adm3_name', 'dd_id', 'polygon', 'method', 'evidence']


class TestColumnNames(unittest.TestCase):
    def test_no_reserved_words(self):
        con = duckdb.connect()
        bad = []
        for c in PANEL_COLUMNS + CROSSWALK_COLUMNS:
            try:
                con.execute(f'SELECT 1 AS {c}')
            except Exception:
                bad.append(c)
        self.assertEqual(bad, [], f'reserved words used as column names: {bad}')

    def test_no_duplicates(self):
        for cols in (PANEL_COLUMNS, CROSSWALK_COLUMNS):
            self.assertEqual(len(cols), len(set(cols)))


if __name__ == '__main__':
    unittest.main()
