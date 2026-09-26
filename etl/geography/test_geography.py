"""Checks for allocation, identity, source changes and unsafe partial comparisons."""
import copy
import json
import tempfile
import unittest
from pathlib import Path

from build_geography import (HERE, WORKSPACE, SOURCE_RELEASE, aggregate_indicator,
                             count, digest, parse_2023_table1, read_csv, strict_sum,
                             validate_registry, verify_inputs)
from locality_evidence import administrative_rows, label_key, pair_row, parse_localities


class GeographyTests(unittest.TestCase):
    def setUp(self):
        self.config = json.loads((HERE / 'registry.json').read_text())
        self.roster = read_csv(WORKSPACE / SOURCE_RELEASE / 'source_units.csv')

    def test_full_partition_and_explicit_tando_alias(self):
        units, groups, aliases = validate_registry(self.config, self.roster)
        self.assertEqual((len(units), len(groups), len(aliases)), (271, 127, 542))
        self.assertEqual(aliases['census2023_population', 'census2023:sindh:tando_ahyar']['unit_version_id'],
                         aliases['census2023_education', 'census2023:sindh:tando_allahyar']['unit_version_id'])
        self.assertEqual(sum(g['approval'] == 'supported_tabular' for g in groups.values()), 127)

    def test_rates_use_combined_counts(self):
        value, num, den = aggregate_indicator([{'total': 100, 'never_attended': 10},
                                               {'total': 900, 'never_attended': 450}], 'pct_never_attended')
        self.assertEqual((value, num, den), (46, 460, 1000))
        self.assertNotEqual(value, (10 + 50) / 2)

    def test_missing_member_withholds_aggregate(self):
        self.assertIsNone(strict_sum([22, None]))
        self.assertIsNone(aggregate_indicator([{'pop_transgender': 22}, {'pop_transgender': None}], 'pop_transgender')[0])
        self.assertIsNone(aggregate_indicator([{'total': 100, 'never_attended': None}], 'pct_never_attended')[0])

    def test_printed_zero_remains_zero(self):
        self.assertEqual(count('0'), 0)
        self.assertEqual(strict_sum([0, 0]), 0)
        self.assertIsNone(count('-'))
        self.assertIsNone(count(''))
        self.assertIsNone(aggregate_indicator([{'total': 0, 'never_attended': 0}], 'pct_never_attended')[0])

    def test_duplicate_module_alias_is_rejected(self):
        self.config['aliases'].append(copy.deepcopy(self.config['aliases'][0]))
        with self.assertRaisesRegex(ValueError, 'Duplicate source alias'):
            validate_registry(self.config, self.roster)

    def test_two_source_units_cannot_collapse_into_one_version(self):
        self.config['aliases'][1]['unit_version_id'] = self.config['aliases'][0]['unit_version_id']
        with self.assertRaisesRegex(ValueError, 'Duplicate module'):
            validate_registry(self.config, self.roster)

    def test_new_source_name_needs_explicit_review(self):
        self.roster[0]['source_name'] = 'NEW UNREVIEWED LABEL'
        with self.assertRaisesRegex(ValueError, 'Changed source label'):
            validate_registry(self.config, self.roster)

    def test_unresolved_link_cannot_be_marked_supported(self):
        self.config['groups'][0]['relationship'] = 'partial'
        with self.assertRaisesRegex(ValueError, 'Unresolved link cannot enter panel'):
            validate_registry(self.config, self.roster)

    def test_exact_cannot_hide_a_split(self):
        next(g for g in self.config['groups'] if g['relationship'] == 'aggregated')['relationship'] = 'exact'
        with self.assertRaisesRegex(ValueError, 'Exact link is not one to one'):
            validate_registry(self.config, self.roster)

    def test_legal_date_not_inferred_from_census_year(self):
        self.config['units'][0]['legal_valid_from'] = '2017-01-01'
        with self.assertRaisesRegex(ValueError, 'no reviewed legal effective dates'):
            validate_registry(self.config, self.roster)

    def test_fata_is_preserved_as_2017_reporting_area(self):
        fata = [u for u in self.config['units'] if u['province_at_census'] == 'FATA']
        self.assertEqual(len(fata), 13)
        self.assertTrue(all(u['year'] == 2017 for u in fata))

    def test_changed_input_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            path = root / 'source.txt'
            path.write_text('original')
            lock = {'files': [{'path': path.name, 'bytes': path.stat().st_size, 'sha256': digest(path)}]}
            verify_inputs(root, lock)
            path.write_text('modified')
            with self.assertRaisesRegex(ValueError, 'Changed input'):
                verify_inputs(root, lock)

    def test_input_cannot_escape_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, 'escapes root'):
                verify_inputs(Path(tmp), {'files': [{'path': '../outside.txt'}]})

    def test_wrapped_district_and_non_district_rows(self):
        raw = '1,410 280,162 140,000 140,162 - 99.88 198.70 0.0 7.0 274,923 0.32'
        text = 'KOLAI PALAS KOHISTAN\n' + raw + '\nDISTRICT\nA TEHSIL ' + raw
        rows = parse_2023_table1(text)
        self.assertEqual(list(rows), ['KOLAI PALAS KOHISTAN DISTRICT'])
        self.assertEqual(count(rows['KOLAI PALAS KOHISTAN DISTRICT'][0][-2]), 274923)
        with self.assertRaisesRegex(ValueError, 'Duplicate Table 1'):
            parse_2023_table1(text + '\f' + text)

    def test_four_districts_resolved_only_as_joint_areas(self):
        retired = {g['comparison_id']: g['superseded_by'] for g in self.config['retired_groups']}
        self.assertEqual(retired, {'DDG-0053': 'DDG-0130', 'DDG-0054': 'DDG-0130',
                                   'DDG-0110': 'DDG-0131', 'DDG-0111': 'DDG-0131'})
        for gid in ['DDG-0130', 'DDG-0131']:
            members = [u for u in self.config['units'] if u['comparison_id'] == gid]
            self.assertEqual(len(members), 4)
            self.assertEqual(sorted(u['year'] for u in members), [2017, 2017, 2023, 2023])
        self.assertIn('individual full-indicator historical allocations remain unresolved', self.config['resolution_scope'])

    def test_retired_ids_cannot_be_reused(self):
        self.config['groups'].append(self.config['retired_groups'][0])
        with self.assertRaisesRegex(ValueError, 'Retired comparison ID reused'):
            validate_registry(self.config, self.roster)

    def test_locality_match_preserves_numbers(self):
        self.assertEqual(label_key('CHAK NO. 324/ G.B'), label_key('CHAK NO 324/G.B'))
        self.assertNotEqual(label_key('CHAK 003'), label_key('CHAK 3'))
        self.assertEqual(label_key('NORTH'), 'NORTH')

    def test_locality_parser_excludes_headers_and_pc_totals(self):
        raw = ' '.join(['-'] * 22 + ['1488'])
        text = ' '.join(str(x) for x in range(1, 26)) + '\n762/G.B.PC ' + raw + '\nCHAK NO. 324/G.B ' + raw
        rows = parse_localities(text, 'fixture.pdf', 2023, 'TOBA TEK SINGH', 1, 1)
        self.assertEqual([r['kind'] for r in rows], ['pc', 'village'])
        self.assertIsNone(rows[1]['population'])
        self.assertEqual(rows[1]['population_raw'], '-')
        self.assertEqual(rows[1]['hadbast_code'], '')
        changed = {**rows[1], 'area_acres': 1489}
        with self.assertRaisesRegex(ValueError, 'acreage changed'):
            pair_row(rows[1], changed, 'test')

    def test_wrapped_subdistrict_retrospective_cell(self):
        rows = administrative_rows('DERA MURAD JAMALI\n281 265822 135942 129871 9 104.67 945.99 40.23 6.7 230775 2.39\nSUB-DIVISION', 2023)
        self.assertEqual(rows[0]['name'], 'DERA MURAD JAMALI SUB-DIVISION')
        self.assertEqual(rows[0]['population'], 265822)
        self.assertEqual(rows[0]['retrospective_2017'], 230775)


if __name__ == '__main__':
    unittest.main()
