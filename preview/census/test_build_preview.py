import copy
import csv
import json
import tempfile
import unittest
from pathlib import Path

from shapely.geometry import shape
from shapely.ops import unary_union
from build_preview import (HERE, GEOMETRY, DEFAULT_RELEASE, TOTALS, build, build_geometry,
                           digest, read_csv, verify_release)


class PreviewBuildTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory(prefix='dd-census-preview-')
        cls.out = Path(cls.temp.name) / 'first'
        cls.result = build(out=cls.out)
        text = (cls.out / 'census-preview-data.js').read_text()
        cls.data = json.loads(text.removeprefix('window.DD_CENSUS_PREVIEW=').removesuffix(';\n'))
        cls.groups = {r['comparison_id']: r for r in read_csv(DEFAULT_RELEASE / 'comparison_geographies.csv')}
        cls.registry = json.loads((HERE / 'boundary_mapping.json').read_text())
        cls.geometry = json.loads(GEOMETRY.read_text())

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_fresh_build_is_byte_identical(self):
        repeat = Path(self.temp.name) / 'second'
        build(out=repeat)
        first = {str(p.relative_to(self.out)): digest(p) for p in self.out.rglob('*') if p.is_file()}
        second = {str(p.relative_to(repeat)): digest(p) for p in repeat.rglob('*') if p.is_file()}
        self.assertEqual(first, second)

    def test_every_area_counted_once_and_national_totals_preserved(self):
        areas = self.data['areas']
        self.assertEqual(len(areas), 127)
        self.assertEqual(len({a['id'] for a in areas}), 127)
        for year, total in TOTALS.items():
            self.assertEqual(sum(a['values'][year]['pop_total']['value'] for a in areas), total)

    def test_joint_areas_replace_four_individual_ids(self):
        areas = {a['id']: a for a in self.data['areas']}
        self.assertFalse({'DDG-0053','DDG-0054','DDG-0110','DDG-0111'} & areas.keys())
        for cid, populations in [('DDG-0130',(4934128,5589683)), ('DDG-0131',(797779,1005989))]:
            for year, value in zip(('2017','2023'),populations):
                self.assertEqual(areas[cid]['values'][year]['pop_total']['value'],value)
                self.assertEqual(len(areas[cid]['members'][year]),2)

    def test_joined_outline_is_union_not_duplicate_shading(self):
        for cid, names in [('DDG-0130',['Jhang','Toba Tek Singh']),('DDG-0131',['Kachhi','Nasirabad']),
                           ('DDG-0089',['Karachi West','Keamari'])]:
            expected=unary_union([shape(f['geometry']) for f in self.geometry['features'] if f['properties']['districts'] in names])
            actual=shape(next(f['geometry'] for f in self.data['geometry']['features'] if f['properties']['comparison_id']==cid))
            self.assertTrue(expected.equals(actual))

    def test_frontier_region_extents_remain_unverified(self):
        ids={a['id'] for a in self.data['areas'] if a['map_status']=='unverified_frontier_extent'}
        self.assertEqual(ids,{'DDG-0001','DDG-0002','DDG-0003','DDG-0004','DDG-0013','DDG-0018'})
        self.assertFalse(self.result['boundary_certification'])

    def test_duplicate_polygon_assignment_fails(self):
        bad=copy.deepcopy(self.registry)
        bad['areas'][1]['polygon_names']=bad['areas'][0]['polygon_names']
        with self.assertRaisesRegex(ValueError,'assigned more than once'):
            build_geometry(self.groups,bad,self.geometry)

    def test_missing_mapping_fails(self):
        bad=copy.deepcopy(self.registry)
        bad['areas'].pop()
        with self.assertRaisesRegex(ValueError,'Incomplete boundary mapping'):
            build_geometry(self.groups,bad,self.geometry)

    def test_cross_province_join_fails(self):
        bad=copy.deepcopy(self.registry)
        bad['areas'][0]['polygon_names']=['Lahore']
        with self.assertRaisesRegex(ValueError,'Province mismatch'):
            build_geometry(self.groups,bad,self.geometry)

    def test_missing_source_cells_are_not_zero(self):
        missing=[r for a in self.data['areas'] for year in a['values'].values() for r in year.values() if r['value'] is None]
        self.assertEqual(len(missing),5)
        self.assertTrue(all(r['status']=='missing_source_component' for r in missing))

    def test_all_source_links_resolve_to_verified_archives(self):
        for a in self.data['areas']:
            for year in a['sources'].values():
                for sources in year.values():
                    self.assertTrue(sources)
                    for s in sources:
                        self.assertEqual(digest(self.out / s['archived']), s['sha256'])

    def test_full_panel_download_is_exact_release(self):
        self.assertEqual(digest(self.out/'downloads/panel.csv'),digest(DEFAULT_RELEASE/'panel.csv'))

    def test_builder_rejects_deployment_output(self):
        from build_preview import REPO
        with self.assertRaisesRegex(ValueError,'outside the published app'):
            build(out=REPO/'app/census-preview')

    def test_education_changes_disabled(self):
        self.assertIs(self.data['meta']['education_change_enabled'],False)


if __name__ == '__main__':
    unittest.main()
