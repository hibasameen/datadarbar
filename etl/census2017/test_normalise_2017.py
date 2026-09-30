"""Regression tests for the Census 2017 normalisation.

The test that matters most is that two columns which co-occur are never merged.
Collapsing table 20's RURAL / TOTAL / URBAN blocks, or table 8's SON and GRAND
SON, would silently destroy data while leaving the panel looking complete.
"""
import sys, unittest, pathlib
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from normalise_2017 import canon_map, last_segment, split_unit, fix_unit


class TestLastSegment(unittest.TestCase):
    def test_takes_the_innermost_segment(self):
        self.assertEqual(last_segment('MARITAL STATUS / NEVER MARRIED'), 'NEVER MARRIED')

    def test_leaves_an_unprefixed_label_alone(self):
        self.assertEqual(last_segment('MALE'), 'MALE')

    def test_collapses_newlines_and_runs_of_space(self):
        self.assertEqual(last_segment('AREA\n(SQ.  KM.)'), 'AREA (SQ. KM.)')

    def test_empty_is_none(self):
        self.assertIsNone(last_segment('   '))
        self.assertIsNone(last_segment(None))


class TestCanonMap(unittest.TestCase):
    def test_merges_variants_that_never_share_a_file(self):
        obs = {('15', 'EDUCATIONAL ATTAINMENT / TOTAL'): {'a'},
               ('15', 'LITERATE POPULATION BY EDUCATIONAL ATTAINMENT / TOTAL'): {'b', 'c'}}
        cmap, merged, kept = canon_map(obs)
        self.assertEqual(set(cmap.values()), {'TOTAL'})
        self.assertEqual(len(merged), 1)
        self.assertEqual(kept, [])

    def test_keeps_columns_that_share_a_file(self):
        # table 20's three locality blocks all end in BOTH SEXES
        obs = {('20', 'RURAL / BOTH SEXES'): {'a', 'b'},
               ('20', 'URBAN / BOTH SEXES'): {'a', 'b'},
               ('20', 'TOTAL / BOTH SEXES'): {'a', 'b'}}
        cmap, merged, kept = canon_map(obs)
        self.assertEqual(len(set(cmap.values())), 3, 'three columns must stay three series')
        self.assertEqual(merged, [])
        self.assertEqual(len(kept), 1)

    def test_son_and_grand_son_stay_apart(self):
        obs = {('8', 'SON / DAUGHTER'): {'f'}, ('8', 'GRAND SON / DAUGHTER'): {'f'}}
        cmap, _, _ = canon_map(obs)
        self.assertEqual(len(set(cmap.values())), 2)

    def test_majority_spelling_wins(self):
        obs = {('15', 'INTERMEDIATE'): set('abcdefgh'), ('15', 'INTER-MEDIATE'): {'z'}}
        cmap, _, _ = canon_map(obs)
        self.assertEqual(set(cmap.values()), {'INTERMEDIATE'})

    def test_punctuation_only_differences_merge(self):
        obs = {('1', 'AREA (SQ. KM.)'): set('abcdef'), ('1', 'AREA (SQ KM)'): {'z'}}
        cmap, _, _ = canon_map(obs)
        self.assertEqual(set(cmap.values()), {'AREA (SQ. KM.)'})

    def test_an_explicit_alias_is_applied(self):
        # differs in the segment itself, so co-occurrence cannot resolve it
        obs = {('1', 'SEX RATIO ALL AGES'): {'z'}}
        cmap, _, _ = canon_map(obs)
        self.assertEqual(cmap[('1', 'SEX RATIO ALL AGES')], 'SEX RATIO')


class TestUnits(unittest.TestCase):
    def test_splits_a_locality_folded_into_the_unit_name(self):
        self.assertEqual(split_unit('KOHISTAN DISTRICT - RURAL'), ('KOHISTAN DISTRICT', 'rural'))
        self.assertEqual(split_unit('KOHISTAN DISTRICT-RURAL'), ('KOHISTAN DISTRICT', 'rural'))
        self.assertEqual(split_unit('KOHISTAN DISTRICT - URBAN'), ('KOHISTAN DISTRICT', 'urban'))

    def test_leaves_an_ordinary_district_alone(self):
        self.assertEqual(split_unit('ABBOTTABAD DISTRICT'), ('ABBOTTABAD DISTRICT', None))

    def test_a_tehsil_named_rural_something_is_not_split(self):
        self.assertEqual(split_unit('RURAL TEHSIL'), ('RURAL TEHSIL', None))

    def test_corrects_the_two_misspelled_districts(self):
        self.assertEqual(fix_unit('SHEIKUPURA DISTRICT')[0], 'SHEIKHUPURA DISTRICT')
        self.assertEqual(fix_unit('KILLA ABDULLAB DISTRICT')[0], 'KILLA ABDULLAH DISTRICT')

    def test_leaves_correct_names_alone(self):
        self.assertEqual(fix_unit('LAHORE DISTRICT'), ('LAHORE DISTRICT', None))



class TestGroupNesting(unittest.TestCase):
    """The spec options that fix tables 27 and 37."""

    def setUp(self):
        sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / 'stage2'))
        from read_workbook import read
        self.read = read

    def _rows(self, stub_labels, width=2):
        rows = [['1'] + [str(i + 2) for i in range(width)]]
        for lab, vals in stub_labels:
            rows.append([lab] + list(vals) + [None] * (width - len(vals)))
        return rows

    def test_a_numberless_heading_becomes_part_of_the_indicator_path(self):
        rows = self._rows([
            ('ABBOTTABAD DISTRICT', []),
            ('ALL LOCALITIES', []),
            ('KITCHEN', []),
            ('NONE', [22852]),
            ('BATHROOM', []),
            ('NONE', [19154]),
        ])
        spec = dict(levels=['locality', 'indicator'], header=['category'],
                    group_indicator=True)
        got = {(o['indicator'], o['value']) for o in
               self.read(rows, 'KP', '37', spec=spec) if o['value'] is not None}
        self.assertIn(('KITCHEN / NONE', 22852), got)
        self.assertIn(('BATHROOM / NONE', 19154), got)

    def test_without_the_option_the_two_collapse(self):
        rows = self._rows([
            ('ABBOTTABAD DISTRICT', []),
            ('ALL LOCALITIES', []),
            ('KITCHEN', []),
            ('NONE', [22852]),
            ('BATHROOM', []),
            ('NONE', [19154]),
        ])
        spec = dict(levels=['locality', 'indicator'], header=['category'])
        got = {o['indicator'] for o in self.read(rows, 'KP', '37', spec=spec)}
        self.assertEqual(got, {'NONE'}, 'this is the defect the option fixes')

    def test_a_group_exit_closes_the_group(self):
        rows = self._rows([
            ('ABBOTTABAD DISTRICT', []),
            ('ALL LOCALITIES', []),
            ('LATRINE', []),
            ('NONE', [13593]),
            ('TOTAL :', [214133]),
        ])
        spec = dict(levels=['locality', 'indicator'], header=['category'],
                    group_indicator=True, group_exits={'TOTAL :'})
        got = {(o['indicator'], o['value']) for o in
               self.read(rows, 'KP', '37', spec=spec) if o['value'] is not None}
        self.assertIn(('TOTAL :', 214133), got)
        self.assertNotIn(('LATRINE / TOTAL :', 214133), got)

    def test_not_locality_keeps_total_out_of_the_locality_slot(self):
        rows = self._rows([
            ('ABBOTTABAD DISTRICT', []),
            ('Rural', []),
            ('REGULAR', [1028123]),
            ('TOTAL', [1039104]),
        ])
        spec = dict(levels=['locality', 'indicator'], header=['category'],
                    not_locality={'TOTAL'})
        got = {(o['locality'], o['indicator'], o['value']) for o in
               self.read(rows, 'KP', '27', spec=spec) if o['value'] is not None}
        self.assertIn(('rural', 'TOTAL', 1039104), got)
        self.assertNotIn(('all', 'TOTAL', 1039104), got)

if __name__ == '__main__':
    unittest.main(verbosity=2)
