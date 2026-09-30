"""Regression tests for recovering unit labels PBS omitted mid-workbook.

The costly failure mode is the false positive: treating a labelled block as
unnamed shifts every later name by one, so the tests fix both that a genuinely
blank block is repaired and that an unusually named one is left alone.
"""
import sys, unittest, pathlib
HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent / 'stage2'))
from recover_units import opener_label, scan, assign, plan

ROSTER = ['BHALWAL TEHSIL', 'BHERA TEHSIL', 'SARGODHA TEHSIL', 'SHAHPUR TEHSIL']
# A roster that exactly accounts for one named block plus one unnamed one, which
# is the only case `plan` will act on.
PAIR = ['BHALWAL TEHSIL', 'BHERA TEHSIL']


def sheet(*blocks):
    """A sheet: a numbered row, then one block per (name, ) with OVERALL inside."""
    rows = [['1', '2']]
    for name in blocks:
        rows.append([name, None])
        rows.append(['OVERALL', None])
        rows.append(['ALL SEXES', 10])
    return rows


class TestOpener(unittest.TestCase):
    def test_finds_the_label_that_begins_each_block(self):
        rows = sheet('BHALWAL TEHSIL', 'BHERA TEHSIL')
        self.assertEqual(opener_label(rows, 0, 0), 'OVERALL')

    def test_returns_none_when_no_unit_is_labelled(self):
        self.assertIsNone(opener_label([['1'], ['OVERALL'], ['ALL SEXES']], 0, 0))


class TestScan(unittest.TestCase):
    def test_a_blank_block_is_unnamed(self):
        rows = sheet('BHALWAL TEHSIL', None)
        openers, unnamed = scan(rows, 0, 0, 'OVERALL')
        self.assertEqual(len(openers), 2)
        self.assertEqual(len(unnamed), 1)

    def test_every_block_labelled_means_nothing_unnamed(self):
        rows = sheet('BHALWAL TEHSIL', 'BHERA TEHSIL', 'SARGODHA TEHSIL')
        openers, unnamed = scan(rows, 0, 0, 'OVERALL')
        self.assertEqual(len(openers), 3)
        self.assertEqual(unnamed, [])

    def test_a_unit_in_another_column_is_still_a_unit(self):
        # Attock's table 14 puts the name in column 3 of a blank-stub row, and of
        # the row that also opens the block; missing either makes named blocks
        # look unnamed and shifts every later name by one
        rows = [['1', '2'],
                [None, None, 'ATTOCK TEHSIL'],
                ['OVERALL', None],
                ['ALL SEXES', 10],
                ['OVERALL', None, None, 'FATEH JANG TEHSIL'],
                ['ALL SEXES', 20]]
        openers, unnamed = scan(rows, 0, 0, 'OVERALL')
        self.assertEqual(len(openers), 2)
        self.assertEqual(unnamed, [], 'both blocks are named, just not in the stub')

    def test_a_unit_carrying_no_keyword_is_still_a_unit(self):
        # DE-EXCLUDED AREA RAJANPUR is the one such unit in the country, and a
        # private copy of the vocabulary missed it
        rows = sheet('DE-EXCLUDED AREA RAJANPUR')
        openers, unnamed = scan(rows, 0, 0, 'OVERALL')
        self.assertEqual(unnamed, [], 'a labelled block must not be called unnamed')


class TestAssign(unittest.TestCase):
    def test_fills_gaps_with_the_names_left_over_in_order(self):
        got = assign(['BHALWAL TEHSIL', 'BHERA TEHSIL', None, None], ROSTER)
        self.assertEqual(got, ['SARGODHA TEHSIL', 'SHAHPUR TEHSIL'])

    def test_a_gap_before_a_labelled_block_still_takes_the_leftover(self):
        got = assign([None, 'SHAHPUR TEHSIL'], ['BHALWAL TEHSIL', 'SHAHPUR TEHSIL'])
        self.assertEqual(got, ['BHALWAL TEHSIL'])

    def test_no_roster_left_yields_none_rather_than_a_guess(self):
        self.assertEqual(assign([None, None], ['BHALWAL TEHSIL']), ['BHALWAL TEHSIL', None])


class TestPlan(unittest.TestCase):
    def test_maps_the_opener_row_to_the_recovered_name(self):
        rows = sheet('BHALWAL TEHSIL', None)
        at, rep = plan(rows, 0, 0, PAIR)
        self.assertEqual(list(at.values()), ['BHERA TEHSIL'])
        self.assertTrue(rep[0]['applied'])

    def test_leaves_the_sheet_untouched(self):
        rows = sheet('BHALWAL TEHSIL', None)
        before = [list(r) for r in rows]
        plan(rows, 0, 0, PAIR)
        self.assertEqual([list(r) for r in rows], before,
                         'src_row must keep pointing at the workbook row')

    def test_does_nothing_when_every_block_is_labelled(self):
        rows = sheet('BHALWAL TEHSIL', 'BHERA TEHSIL')
        self.assertEqual(plan(rows, 0, 0, ROSTER), ({}, []))

    def test_refuses_the_file_when_the_counts_do_not_match(self):
        # two unnamed blocks but only one roster name unused: the opener is not
        # one-per-unit, which is how Kohistan's table 9 came to have its
        # district's rural and urban sub-blocks named as four sub-divisions
        rows = sheet('BHALWAL TEHSIL', None, None)
        at, rep = plan(rows, 0, 0, PAIR)
        self.assertEqual(at, {}, 'no assignment may be applied')
        self.assertTrue(all(not r['applied'] for r in rep))
        self.assertIn('not one opener', rep[0]['why'])

    def test_refuses_when_there_are_more_roster_names_than_blanks(self):
        rows = sheet('BHALWAL TEHSIL', None)
        at, rep = plan(rows, 0, 0, ROSTER)
        self.assertEqual(at, {})


if __name__ == '__main__':
    unittest.main(verbosity=2)
