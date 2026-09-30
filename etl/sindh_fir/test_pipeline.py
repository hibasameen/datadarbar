import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('sindh_fir_pipeline', HERE / 'pipeline.py')
p = importlib.util.module_from_spec(spec)
spec.loader.exec_module(p)


class TestFIRExtraction(unittest.TestCase):
    def fixture(self, name):
        return (HERE / 'fixtures' / name).read_text()

    def test_markdown_source_and_missing_semantics(self):
        rows = p.parse_indexed(self.fixture('2025-09-19.txt'))
        self.assertEqual(len(rows), 38)
        lookup = {r['geography_name']: r for r in rows if r['geography_level'] == 'police_district'}
        self.assertEqual((lookup['SOUTH KARACHI']['daily_firs'], lookup['SOUTH KARACHI']['ytd_firs']), (27, 5526))
        self.assertIsNone(lookup['JAMSHORO']['daily_firs'])
        self.assertEqual(lookup['SHIKARPUR']['daily_firs'], 0)
        self.assertEqual(lookup['TANDO ALLAH YAR']['ytd_firs'], 154)  # Preserve anomalous published value.

    def test_html_source_with_malformed_nested_row(self):
        rows = p.parse_indexed(self.fixture('2025-11-07.txt'))
        self.assertEqual(len(rows), 38)
        east = next(r for r in rows if r['geography_name'] == 'EAST KARACHI')
        self.assertEqual((east['daily_firs'], east['ytd_firs']), (28, 13284))
        self.assertEqual(east['report_date'], '2025-11-07')
        self.assertEqual(rows[-1]['daily_firs'], 311)
        self.assertEqual(rows[-1]['ytd_firs'], 106723)

    def test_truncated_and_unexpected_district_rejected(self):
        text = self.fixture('2025-09-19.txt')
        with self.assertRaises(ValueError):
            p.parse_indexed(text[:text.index('G R A N D T O T A L')])
        with self.assertRaises(ValueError):
            p.parse_indexed(text.replace('CITY KARACHI', 'UNKNOWN DISTRICT'))

    def test_other_section_and_wrong_year_rejected(self):
        text = self.fixture('2025-09-19.txt')
        with self.assertRaises(ValueError):
            p.parse_indexed(text.replace(p.TITLE, '3- CALLS RECEIVED ON EMERGENCY -15 & FIRs REGISTERED'))
        with self.assertRaises(ValueError):
            p.parse_indexed(text.replace('01-01-2025', '01-01-2024'))

    def test_missing_children_cannot_pass_total_check(self):
        rows = p.parse_indexed(self.fixture('2025-09-19.txt'))
        checks, _ = p.qa(rows)
        check = next(r for r in checks if r['geography_name'] == 'Hyderabad' and r['metric'] == 'daily_firs')
        self.assertEqual(check['status'], 'not_checkable_missing_values')
        self.assertIsNone(check['residual'])
        self.assertEqual(check['missing_children'], 5)
        checks, _ = p.qa(p.parse_indexed(self.fixture('2025-11-07.txt')))
        self.assertTrue(all(r['status'] == 'pass' for r in checks))

    def test_discrepancy_detected_without_correction(self):
        rows = p.parse_indexed(self.fixture('2025-11-07.txt'))
        rows[-1]['daily_firs'] += 1
        checks, _ = p.qa(rows)
        self.assertEqual(sum(r['status'] == 'mismatch' for r in checks), 2)
        self.assertEqual(rows[-1]['daily_firs'], 312)

    def test_temporal_checks_respect_gaps_and_year_boundaries(self):
        rows = p.parse_indexed(self.fixture('2025-11-07.txt'))
        previous = [dict(r, report_date='2025-11-06') for r in rows]
        _, flags = p.qa(previous + rows)
        self.assertTrue(any(f['geography_name'] == 'EAST KARACHI' and f['change_minus_daily'] == -28 for f in flags))
        previous = [dict(r, report_date='2025-11-05') for r in rows]
        self.assertEqual(p.qa(previous + rows)[1], [])  # No daily inference across a gap.
        previous = [dict(r, report_date='2024-12-31', ytd_start_date='2024-01-01') for r in rows]
        self.assertEqual(p.qa(previous + rows)[1], [])
        self.assertEqual(next(r for r in rows if r['geography_name'] == 'EAST KARACHI')['daily_firs'], 28)

    def test_ytd_decrease_is_flagged_even_across_gap(self):
        rows = p.parse_indexed(self.fixture('2025-11-07.txt'))
        previous = [dict(r, report_date='2025-11-05', ytd_firs=r['ytd_firs']+100) for r in rows]
        _, flags = p.qa(previous + rows)
        self.assertEqual(len(flags), 38)
        self.assertTrue(all(f['flag'] == 'ytd_decrease' and f['change_minus_daily'] is None for f in flags))

    def test_build_integrity_and_deterministic_rebuild(self):
        with tempfile.TemporaryDirectory() as temp:
            raw, out = Path(temp) / 'raw', Path(temp) / 'out'
            discovery = raw / 'discovery'
            discovery.mkdir(parents=True)
            (discovery / 'search_0.txt').write_text(self.fixture('2025-09-19.txt'))
            p.recover(raw)
            p.build(raw, out)
            first = (out / 'sindh_fir_district_daily_ytd.csv').read_bytes()
            p.build(raw, out)
            self.assertEqual(first, (out / 'sindh_fir_district_daily_ytd.csv').read_bytes())
            source = json.loads((raw / 'indexed_sources.json').read_text())[0]
            with (raw / source['local_file']).open('a') as f:
                f.write('altered')
            with self.assertRaisesRegex(ValueError, 'integrity'):
                p.build(raw, out)


if __name__ == '__main__':
    unittest.main()
