"""Regression tests for the Stage 2 reader.

Every case here is a defect that was actually found in the PBS corpus and
silently produced wrong data before it was fixed.
"""
import unittest
from read_workbook import (read, anchor, label_column, header, unit_type, cell,
                           validate_spec, numbered_columns, row_unit)
from unit_aliases import apply as apply_alias

A = 'ABBOTTABAD DISTRICT'


def grid(*rows):
    return [list(r) for r in rows]


class TestAnchoring(unittest.TestCase):
    def test_finds_column_number_row(self):
        rows = grid(('TABLE 1 : X', None, None), ('NAME', 'AREA', 'POP'),
                    (1, 2, 3), (A, 5, 9))
        self.assertEqual(anchor(rows), 2)

    def test_columns_come_from_the_numbered_row(self):
        # Table 7's Punjab workbook carries 256 columns of which 249 are empty.
        # PBS numbers the real ones, so that row is the authority.
        rows = grid(('TABLE 7', None, None, None, None, None),
                    ('NAME', 'A', 'B', None, None, None),
                    (1, 2, 3, None, None, None))
        stub, data = numbered_columns(rows, 2)
        self.assertEqual(stub, 0)
        self.assertEqual(data, [1, 2])

    def test_unit_banner_may_sit_outside_the_stub_column(self):
        # Table 10 centres its district banner in the middle of the data region.
        row = [None, None, None, None, A, None]
        label, col = row_unit(row, 0)
        self.assertEqual(label, A)
        self.assertEqual(col, 4)


class TestAnchorAcrossCensuses(unittest.TestCase):
    """The column-number row is detected differently in each census year."""

    def test_accepts_float_formatted_numbers(self):
        # The 2017 workbooks are .xls and store the row as 1.0, 2.0, 3.0.
        rows = grid(('TABLE 1', None, None), ('NAME', 'A', 'B'), (1.0, 2.0, 3.0), (A, 5, 9))
        self.assertEqual(anchor(rows), 2)

    def test_prefers_the_row_that_numbers_the_stub(self):
        # 2017 tables 29, 30 and 32 print two numbered rows: the first numbers
        # only the data columns, the second includes the stub. Taking the first
        # treats the stub as data and no unit is ever found.
        rows = grid(('TABLE 29', None, None, None),
                    ('HOUSING UNITS', 'BY ROOMS', None, None),
                    (None, 1.0, 2.0, 3.0),
                    (1.0, 2.0, 3.0, 4.0),
                    (A, None, None, None))
        self.assertEqual(anchor(rows), 3)
        self.assertEqual(numbered_columns(rows, 3)[0], 0)

    def test_a_data_row_stubbed_01_is_not_a_column_number_row(self):
        # Table 4's single-year-age rows are stubbed "01", "02" and carry
        # counts. Read as a column-number row, the whole table shifts a column.
        rows = grid(('TABLE 4', None, None), ('SEX/AGE', 'ALL', 'MALE'),
                    (1, 2, 3), (A, None, None),
                    ('ALL AGES', 1397587, 701906),
                    ('01', 31371, 15647))
        self.assertEqual(anchor(rows), 2)


class TestHeaders(unittest.TestCase):
    def test_merged_span_does_not_leak_past_its_range(self):
        # Forward-filling gave column 4 the label
        # "POPULATION 2017 / AVERAGE H.HOLD SIZE" - two unrelated columns.
        rows = grid(('TABLE 1 : X', None, None, None),
                    ('NAME', 'POP-2023', None, 'POPULATION 2017'),
                    ('NAME', 'MALE', 'FEMALE', None),
                    (1, 2, 3, 4))
        merges = [(1, 1, 1, 2), (1, 3, 2, 3)]
        h = header(rows, 3, 0, merges)
        self.assertEqual(h[1], 'POP-2023 / MALE')
        self.assertEqual(h[2], 'POP-2023 / FEMALE')
        self.assertEqual(h[3], 'POPULATION 2017')

    def test_title_banner_is_not_a_column_label(self):
        rows = grid(('TABLE 1 : AREA, POPULATION', None), ('NAME', 'AREA'), (1, 2))
        h = header(rows, 2, 0, [(0, 0, 0, 1)])
        self.assertNotIn('TABLE', h[1].upper())


class TestUnits(unittest.TestCase):
    def test_de_excluded_area_is_a_unit(self):
        # Punjab's DE-EXCLUDED AREA RAJANPUR carries no keyword, holds 41,741
        # people, and Rajanpur does not close without it.
        self.assertEqual(unit_type('DE-EXCLUDED AREA \nRAJANPUR'), 'other')

    def test_province_row_is_not_a_unit(self):
        self.assertIsNone(unit_type('KHYBER PAKHTUNKHWA'))

    def test_sub_district_vocabulary_by_province(self):
        self.assertEqual(unit_type('BADIN TALUKA'), 'tehsil')
        self.assertEqual(unit_type('AWARAN SUB-DIVISION'), 'sub_division')
        self.assertEqual(unit_type('YAK MACHH SUB-TEHSIL'), 'sub_tehsil')


class TestAliases(unittest.TestCase):
    def test_all_deleted_from_six_names_in_table_1(self):
        for bad, good in [('AI TEHSIL', 'ALLAI TEHSIL'),
                          ('KAR KAHAR TEHSIL', 'KALLAR KAHAR TEHSIL'),
                          ('TANDO AHYAR DISTRICT', 'TANDO ALLAHYAR DISTRICT'),
                          ('KAG SUB-TEHSIL', 'KALLAG SUB-TEHSIL')]:
            fixed, why = apply_alias('1', bad)
            self.assertEqual(fixed, good)
            self.assertIn('ALL', why)

    def test_alias_only_applies_to_the_affected_table(self):
        self.assertEqual(apply_alias('9', 'AI TEHSIL')[0], 'AI TEHSIL')

    def test_malakand_resolved_to_district_label(self):
        self.assertEqual(apply_alias('9', 'MALAKAND PROTECTED AREA')[0], 'MALAKAND DISTRICT')


class TestMissingness(unittest.TestCase):
    def test_printed_dash_is_missing_not_zero(self):
        for d in ('-', ' - ', '–'):
            self.assertEqual(cell(d), (None, 'dash'))

    def test_zero_stays_zero(self):
        self.assertEqual(cell(0), (0, 'number'))

    def test_blank_is_distinct_from_dash(self):
        self.assertEqual(cell(None), (None, 'blank'))


class TestSpec(unittest.TestCase):
    def test_shape_mismatch_is_reported(self):
        # table 1 is declared stub_unit; a banner-style sheet must fail.
        rows = grid(('TABLE 1', None, None), ('NAME', 'POP', 'M'), (1, 2, 3),
                    (A, None, None), ('RURAL', 5, 3))
        self.assertIn('shape mismatch', validate_spec(rows, '1'))


class TestEndToEnd(unittest.TestCase):
    def test_stub_unit_table_emits_locality_rows(self):
        rows = grid(('TABLE 1 : X', None, None), ('NAME', 'AREA', 'POP'), (1, 2, 3),
                    (A, 1967, 1419072), ('RURAL', None, 1086757), ('URBAN', None, 332315))
        got = list(read(rows, 'KHYBER PAKHTUNKHWA', '1'))
        locs = {(o['locality'], o['indicator']): o['value'] for o in got}
        self.assertEqual(locs[('all', 'POP')], 1419072)
        self.assertEqual(locs[('rural', 'POP')], 1086757)
        self.assertEqual(locs[('urban', 'POP')], 332315)

    def test_wrapped_label_is_normalised(self):
        rows = grid(('TABLE 1 : X', None, None), ('NAME', 'POP', 'M'), (1, 2, 3),
                    ('CHOWK SARWAR \nSHAHEED TEHSIL', 414578, 210000))
        got = list(read(rows, 'PUNJAB', '1'))
        self.assertEqual(got[0]['unit'], 'CHOWK SARWAR SHAHEED TEHSIL')


if __name__ == '__main__':
    unittest.main()

