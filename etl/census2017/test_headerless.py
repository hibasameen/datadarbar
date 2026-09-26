"""Regression tests for the headerless-workbook recovery.

The distinction these protect is the one that matters: a file with no header and
real data must be recovered, and a file with no header and no data must be left
alone. Repairing the second kind would invent localities for districts that have
none.
"""
import sys, unittest, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from headerless import synth_columns, is_empty, reason_text


class TestSynthColumns(unittest.TestCase):
    def test_recovers_positions_from_the_widest_data_row(self):
        # Swabi's table 1, abridged: sparse grid, unit label at column 2
        rows = [
            [None] * 24,
            [None, None, 'SWABI DISTRICT', None, 1543, None, 1625477, 815828,
             None, 809550, None, 99],
            [None] * 12,
            [None, None, 'RURAL', None, None, None, 1349513, 679142, None, 670297, None, 74],
        ]
        stub, cols = synth_columns(rows)
        self.assertEqual(stub, 2)
        self.assertEqual(cols, [4, 6, 7, 9, 11])

    def test_prefers_the_earliest_row_when_widths_tie(self):
        # the unit's own total row comes first; a later tehsil row must not win
        rows = [
            [None, 'DISTRICT', 10, 20, 30],
            [None, 'TEHSIL A', 40, 50, 60],
        ]
        stub, cols = synth_columns(rows)
        self.assertEqual(stub, 1)
        self.assertEqual(cols, [2, 3, 4])

    def test_the_stub_is_the_text_cell_left_of_the_first_number(self):
        rows = [[None, 'IGNORED HEADING', None, 'DERA BUGTI DISTRICT', None, 7, 8, 9]]
        stub, cols = synth_columns(rows)
        self.assertEqual(stub, 3)
        self.assertEqual(cols, [5, 6, 7])

    def test_a_row_too_narrow_to_be_a_total_row_is_not_recovered(self):
        # one stray number is what the empty locality tables contain
        self.assertEqual(synth_columns([[None, 'LAHORE IS URBANIZED.', None, 43270]]), (None, []))

    def test_no_numbers_at_all_is_not_recovered(self):
        self.assertEqual(synth_columns([['TABLE 25'], ['SOME TEXT']]), (None, []))


class TestIsEmpty(unittest.TestCase):
    def test_a_file_with_real_data_is_not_empty(self):
        self.assertFalse(is_empty([[None, 'BUNER DISTRICT', 100, 200, 300]]))

    def test_an_empty_locality_table_is_empty(self):
        self.assertTrue(is_empty([['TABLE 26 - SELECTED HOUSING STATISTICS'],
                                  [None, None, 'SHERANI IS RURAL.'], [None, 43270]]))


class TestReasonText(unittest.TestCase):
    def test_finds_the_note_pbs_wrote(self):
        rows = [['TABLE 23'], [None, None, 'LAHORE IS URBANIZED.'], [None, 43270]]
        self.assertEqual(reason_text(rows), 'LAHORE IS URBANIZED.')

    def test_returns_none_when_pbs_wrote_no_note(self):
        self.assertIsNone(reason_text([['TABLE 25'], [None, 43270]]))


if __name__ == '__main__':
    unittest.main(verbosity=2)
