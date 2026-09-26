"""Meaningful release checks against retained reports and geography semantics.

Run after run.py: python -m unittest discover -s etl/ljcp -p 'test_*.py'
"""
import json
from pathlib import Path
import tempfile
import unittest
import duckdb
from build import strict_sum,ratio
from extract import WORKSPACE,integer,FIELDS
from fetch_sources import fetch_one

OUT=WORKSPACE/'data_darbar_warehouse/ljcp'

def data(name):return json.loads((OUT/f'{name}.json').read_text())

class PipelineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw=data('source_observations');cls.annual=data('annual_sessions');cls.policy=data('district_policy_indicators')

    def row(self,year,province,name,category='all'):
        rows=[r for r in self.annual if (r['year'],r['province'],r['session_division'],r['category'])==(year,province,name,category)]
        self.assertEqual(len(rows),1);return rows[0]

    def test_missing_is_not_zero_and_denominators_are_safe(self):
        self.assertIsNone(integer('--'));self.assertEqual(integer('0'),0)
        self.assertEqual(integer('1,203'),1203)
        with self.assertRaises(ValueError):integer('1x3')
        self.assertIsNone(strict_sum([4,None]));self.assertIsNone(ratio(12,0))

    def test_verified_pdf_cells_and_grid_recovery(self):
        r=self.row(2024,'Punjab','attock')
        self.assertEqual([r[f] for f in FIELDS],[20723,35426,15616,11776,35723,24266])
        self.assertEqual(self.row(2024,'Balochistan','uthal','civil')['pending_start'],244)
        self.assertEqual(self.row(2023,'Khyber Pakhtunkhwa','kolai pallas')['pending_start'],149)

    def test_reviewed_scan_and_source_mismatch_retention(self):
        self.assertEqual(self.row(2021,'Punjab','attock')['instituted'],33307)
        self.assertEqual(self.row(2021,'Sindh','badin')['transfers_in'],8)
        self.assertEqual(self.row(2021,'Khyber Pakhtunkhwa','tor ghar')['transfers_out'],89)
        mismatches={(r['source_id'],r['province'],r['section'],r['metric'],r['difference']) for r in data('table_validation') if r['status']=='source_mismatch'}
        self.assertEqual(mismatches,{('annual_2021','Khyber Pakhtunkhwa','5.34','transfers_in',9),('annual_2021','Khyber Pakhtunkhwa','5.34','transfers_out',9),('annual_2024','Balochistan','6.33','sanctioned',1)})

    def test_punjab_tiers_are_added_once_and_categories_keep_scope(self):
        a=self.row(2023,'Punjab','attock')
        self.assertEqual(a['pending_end'],18336+2387)
        self.assertEqual(a['method'],'sum_court_tiers')
        self.assertEqual(self.row(2020,'Punjab','attock','civil')['court_tier'],'civil_courts')

    def test_balochistan_bad_printed_criminal_table_is_not_released_as_criminal(self):
        r=self.row(2020,'Balochistan','quetta','criminal')
        self.assertEqual(r['method'],'all_minus_civil');self.assertEqual(r['pending_end'],6047-3784)
        self.assertEqual(sum(x['pending_end'] for x in self.annual if x['year']==2020 and x['province']=='Balochistan' and x['category']=='criminal'),7796)

    def test_ict_population_and_judges_are_not_duplicated(self):
        rows=[r for r in self.policy if r['year']==2024 and r['adm2_key']=='islamabad']
        self.assertEqual(len(rows),1);r=rows[0]
        self.assertEqual(r['pending_end'],47854);self.assertEqual(r['working_judges'],68)
        self.assertEqual(set(r['sessions_included']),{'islamabad east','islamabad west'})
        self.assertAlmostEqual(r['pending_per_100k_population'],47854/r['population']*100000)

    def test_complete_groups_and_uncertain_geographies(self):
        r=next(r for r in self.policy if r['year']==2024 and r['adm2_key']=='kohistan')
        self.assertEqual(len(r['sessions_included']),3)
        self.assertTrue(all(r['population_year']==2023 for r in self.policy))
        self.assertFalse(any(r['year']==2023 and r['province']=='Balochistan' for r in self.policy))

    def policy_row(self,year,adm):
        rows=[r for r in self.policy if (r['year'],r['adm2_key'])==(year,adm)];self.assertEqual(len(rows),1);return rows[0]

    def test_balochistan_seats_fold_into_map_districts(self):
        q=self.policy_row(2024,'quetta')
        self.assertEqual(set(q['sessions_included']),{'quetta','sariab'});self.assertEqual(q['pending_end'],7419+1435)
        self.assertEqual(q['crosswalk_status'],'aggregated_to_existing_map_unit')
        k=self.policy_row(2024,'kech');self.assertEqual(k['sessions_included'],['turbat']);self.assertEqual(k['crosswalk_status'],'seat_to_district')
        self.assertEqual(self.policy_row(2024,'lasbela')['pending_end'],self.row(2024,'Balochistan','hub')['pending_end']+self.row(2024,'Balochistan','uthal')['pending_end'])
        self.assertEqual(self.policy_row(2020,'lasbela')['sessions_included'],['hub'])
        self.assertFalse(any(r['adm2_key'] in ('sariab','turbat','hub','chaman','usta muhammad','surab') for r in self.policy))

    def test_hosted_districts_enter_host_population_only_when_they_have_no_seat(self):
        pop=json.loads((WORKSPACE/'datadarbar/app/data/districts.json').read_text())
        e=self.policy_row(2024,'karachi east')
        self.assertEqual(e['population_keys'],['karachi east','korangi']);self.assertEqual(e['population'],int(pop['karachi east']['t1_2023_pop_total']+pop['korangi']['t1_2023_pop_total']))
        self.assertFalse(any(r['adm2_key'] in ('korangi','keamari') for r in self.policy))
        self.assertEqual(self.policy_row(2022,'thatta')['population_keys'],['thatta','sajawal'])
        self.assertEqual(self.policy_row(2024,'thatta')['population_keys'],['thatta']);self.assertEqual(self.policy_row(2024,'sajawal')['population_keys'],['sajawal'])
        self.assertEqual(self.policy_row(2020,'kharan')['hosted_districts'],['washuk']);self.assertEqual(self.policy_row(2022,'kharan')['hosted_districts'],[])
        hosted={(r['year'],r['district']) for r in data('hosted_districts')}
        self.assertIn((2020,'harnai'),hosted);self.assertNotIn((2021,'harnai'),hosted)
        # Every district population is counted at most once per year.
        for year in {r['year'] for r in self.policy}:
            keys=[k for r in self.policy if r['year']==year for k in r['population_keys']]
            self.assertEqual(len(keys),len(set(keys)))

    def test_category_reader_agrees_with_curated_series(self):
        checks=data('category_crosscheck')
        self.assertGreater(sum(c['status']=='matches' for c in checks),1300)
        # The only permitted differences are the 2020 Balochistan printed criminal table,
        # which repeats the all-cases table and is replaced by all-minus-civil in the curated series.
        self.assertTrue(all(c['status']=='matches' or (c['year'],c['province'],c['category'])==(2020,'Balochistan','criminal') for c in checks))
        cats=data('category_sessions')
        row=next(r for r in cats if (r['year'],r['province'],r['session_division'],r['category'])==(2024,'Punjab','attock','family'))
        self.assertEqual([row[f] for f in FIELDS],[1939,2633,1131,889,2694,2120]);self.assertEqual(row['stock_flow_check'],'balanced')
        # Sindh 2024 "4.28" is civil revisions despite its printed title (continuity with 2023 4.24).
        rev=next(r for r in cats if (r['year'],r['province'],r['session_division'],r['category'])==(2024,'Sindh','karachi south','civil_revisions'))
        prev=next(r for r in cats if (r['year'],r['province'],r['session_division'],r['category'])==(2023,'Sindh','karachi south','civil_revisions'))
        self.assertEqual(rev['pending_start'],prev['pending_end']);self.assertEqual(rev['section'],'4.28')
        dc=data('district_category_indicators')
        self.assertFalse(any(r['category'] in ('all','civil','criminal') for r in dc))
        self.assertTrue(all(r['population_keys'] for r in dc))

    def test_balochistan_staffing_summed_from_ranks(self):
        js=[r for r in data('judicial_strength') if r['province']=='Balochistan' and r['year']==2024]
        q=next(r for r in js if r['session_division']=='quetta')
        self.assertEqual(set(q['staff_units_included']),{'kuchlak','quetta','sariab quetta'});self.assertEqual(q['rank_coverage'],'sum_of_four_reported_ranks')
        p=self.policy_row(2024,'quetta');self.assertEqual(p['working_judges'],q['working_judges']);self.assertEqual(p['judge_coverage_status'],'matched')
        self.assertTrue(all(r['working_judges'] is None for r in self.policy if r['year']==2021))

    def test_interpolated_population_is_bounded_by_the_censuses(self):
        pop=json.loads((WORKSPACE/'datadarbar/app/data/districts.json').read_text())
        for r in self.policy:
            if r['population_interpolation_basis']!='geometric_2017_2023':continue
            p17=sum(pop[k]['t1_2017_pop_total'] for k in r['population_keys']);p23=sum(pop[k]['t1_2023_pop_total'] for k in r['population_keys'])
            lo,hi=sorted([p17,p23]);self.assertGreaterEqual(r['population_interpolated'],lo*0.99)
            if r['year']<2023:self.assertLessEqual(r['population_interpolated'],hi)
        self.assertEqual(self.policy_row(2024,'karachi west')['population_interpolated'],self.policy_row(2024,'karachi west')['population'])

    def test_annual_and_halfyear_coverage_do_not_mix(self):
        self.assertEqual({r['year'] for r in self.annual},{2020,2021,2022,2023,2024})
        self.assertFalse(any(r['year']==2023 and r['province']=='Balochistan' for r in self.annual))
        self.assertTrue(any(r['year']==2023 and r['province']=='Balochistan' for r in data('annual_provinces')))
        half=data('halfyear_provinces');self.assertEqual(len(half),72)
        self.assertTrue(all(r['period_type']=='half_year' for r in half))
        self.assertTrue(any(r['period_end']=='2025-06-30' for r in half))

    def test_unique_keys_and_provenance(self):
        keys=[(r['year'],r['province'],r['session_division'],r['category']) for r in self.annual]
        self.assertEqual(len(keys),len(set(keys)));self.assertEqual(len(keys),1848)
        keys=[(r['year'],r['adm2_key']) for r in self.policy];self.assertEqual(len(keys),len(set(keys)))
        ids={r['record_id'] for r in self.raw};self.assertEqual(len(ids),len(self.raw))
        self.assertTrue(all(r['source_record_ids'] and set(r['source_record_ids'])<=ids for r in self.annual))

    def test_staffing_reference_dates_are_respected(self):
        p=[r for r in self.policy if r['year']==2022 and r['province']=='Punjab']
        self.assertTrue(p);self.assertTrue(all(r['working_judges'] is None for r in p))

    def test_exports_agree(self):
        con=duckdb.connect()
        for name in ['annual_sessions','annual_provinces','halfyear_provinces','judicial_strength','judicial_strength_by_rank','district_policy_indicators','sessions_crosswalk']:
            count=len(data(name));pq=str(OUT/f'{name}.parquet').replace("'","''");csv=str(OUT/f'{name}.csv').replace("'","''")
            self.assertEqual(con.execute(f"select count(*) from read_parquet('{pq}')").fetchone()[0],count)
            self.assertEqual(con.execute(f"select count(*) from read_csv('{csv}',header=true)").fetchone()[0],count)
        con.close()

    def test_tampered_cache_cannot_be_silently_reused(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'pdfs').mkdir();(root/'pdfs/test.pdf').write_bytes(b'%PDF-not-the-pinned-file')
            result=fetch_one({'id':'test','source_url':'https://invalid.example/test.pdf','archive_url':'https://invalid.example/test.pdf','sha256':'0'*64},root)
            self.assertEqual(result['status'],'failed')

if __name__=='__main__':unittest.main()
