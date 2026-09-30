import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import pipeline as p


class RegionalPoliceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.out = Path(cls.tmp.name)
        cls.report = p.build(out=cls.out)
        cls.rows = json.loads((cls.out/'crime_annual.json').read_text())

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def row(self, **conditions):
        result = [r for r in self.rows if all(r[k]==v for k,v in conditions.items())]
        self.assertEqual(len(result),1,conditions)
        return result[0]

    def test_pbs_all_five_annual_coverage(self):
        totals = json.loads((self.out/'regional_totals.json').read_text())
        self.assertEqual({(r['region'],r['year']) for r in totals},
                         {(g,y) for g in p.REGIONS for y in range(2019,2025)})
        self.assertEqual(self.row(source_family='pbs_national_police_bureau',region='ICT',year=2024,
                                  offence_id='total')['value'],24573)
        self.assertEqual(self.row(source_family='pbs_national_police_bureau',region='GB',year=2019,
                                  offence_id='total')['value'],1983)

    def test_kp_blank_does_not_shift_adjacent_year(self):
        old=self.row(source_family='kp_police_yearbooks',geography='Battagram',year=2023,offence_id='car_snatching')
        new=self.row(source_family='kp_police_yearbooks',geography='Battagram',year=2024,offence_id='car_snatching')
        self.assertIsNone(old['value'])
        self.assertEqual(old['value_status'],'blank')
        self.assertEqual(new['value'],0)
        self.assertEqual(new['value_status'],'reported')

    def test_kp_uses_column_years_not_stale_title(self):
        row=self.row(source_family='kp_police_yearbooks',geography='Abbottabad',year=2022,offence_id='murder')
        self.assertEqual(row['value'],61)
        self.assertEqual(row['source_id'],'kp_2024')

    def test_replaced_kp_geographies_not_double_counted(self):
        rows=[r for r in self.rows if r['source_family']=='kp_police_yearbooks' and r['year']==2021 and r['geography_level']=='district']
        self.assertEqual(len({r['geography'] for r in rows}),35)
        self.assertNotIn('Chitral',{r['geography'] for r in rows})
        self.assertIn('Chitral Lower',{r['geography'] for r in rows})

    def test_selected_offences_never_become_all_fir_totals(self):
        kp=[r for r in self.rows if r['source_family']=='kp_police_yearbooks']
        self.assertTrue(all(r['measure']=='reported_offence_count' for r in kp))
        b=[r for r in self.rows if r['source_family']=='baloch_home_department']
        self.assertTrue(all(r['measure']!='reported_crime_total' for r in b))
        self.assertEqual(len([r for r in b if r['measure']=='selected_offences_and_accidents_total']),10)

    def test_ajk_page_boundary_and_order(self):
        rows=[r for r in self.rows if r['source_family']=='ajk_police_yearbooks' and r['year']==2024 and r['geography_level']=='district' and r['offence_id']=='total']
        self.assertEqual(len(rows),10)
        vals={r['geography']:r['value'] for r in rows}
        self.assertEqual(vals['Bagh'],1099)
        self.assertEqual(vals['Kotli'],2465)
        self.assertEqual(vals['Mirpur'],1663)
        self.assertEqual(sum(vals.values()),9962)

    def test_source_disagreements_are_preserved(self):
        differences=json.loads((self.out/'cross_source_differences.json').read_text())
        self.assertEqual(len(differences),6)
        d=next(d for d in differences if d['year']==2024)
        self.assertEqual((d['pbs_total'],d['provincial_total']),(9871,9962))
        self.assertIn('pbs_and_ajk_annual_totals_differ',self.row(source_family='ajk_police_yearbooks',geography='Bagh',year=2024,offence_id='total')['quality_flags'])

    def test_source_arithmetic_errors_are_not_corrected(self):
        checks=json.loads((self.out/'total_checks.json').read_text())
        failures=[c for c in checks if c['status']=='mismatch']
        self.assertEqual(len(failures),2)
        self.assertTrue(all(c['source_id']=='pbs_2022' and c['year']==2019 for c in failures))

    def test_gaps_and_dashes_not_filled(self):
        self.assertFalse(any(r['year']==2025 for r in self.rows))
        self.assertTrue(all(r['frequency']=='annual' for r in self.rows))
        self.assertEqual(p.numeric('-'),None)
        self.assertEqual(p.numeric(''),None)
        self.assertEqual(p.numeric('0'),0)
        coverage=json.loads((self.out/'year_coverage.json').read_text())
        self.assertTrue(all(r['district_daily_fir_status']=='not_recovered' for r in coverage))

    def test_parquet_preserves_null_numeric_cells(self):
        import duckdb
        con=duckdb.connect(':memory:')
        relation=con.read_parquet(str(self.out/'district_crime_annual.parquet'))
        self.assertEqual(relation.filter('value IS NULL').count('*').fetchone()[0],535)
        con.close()

    def test_rebuild_is_deterministic(self):
        before=(self.out/'crime_annual.json').read_bytes()
        p.build(out=self.out)
        self.assertEqual(before,(self.out/'crime_annual.json').read_bytes())


if __name__=='__main__':
    unittest.main()
