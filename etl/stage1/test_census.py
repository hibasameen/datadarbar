"""Regression tests for failures that would change census meaning or outputs."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import build_census as census
from common import sha256, verify_files


class CensusTests(unittest.TestCase):
    def test_wrapped_2017_attainment_uses_overall_not_later_male_row(self):
        text = "SOUTH WAZIRISTAN AGENCY\nOVERALL\nALL SEXES\n5 AND ABOVE\n554,377 362,389 71,685 44,866 34,995 23,031 8,325 4,395 2,736 216 1,739\nMALE\n5 AND ABOVE 100 50 10 10 10 10 2 2 2 2 2"
        values, _ = census.parse_education_pdf(text)
        self.assertEqual(census.number(values["total"]), 554377)
        self.assertEqual(census.number(values["intermediate"]), 8325)

    def test_population_wrapped_name_and_frontier_region(self):
        for text in ["SOUTH WAZIRISTAN AGENCY\n6,620 675,215 355,611 319,554 50 111.28 102.00 - 7.98 429,841 2.4",
                     "FR LAKKI MARWAT 132 26,394 13,685 12,709 - 107.68 199.95 - 7.84 6,987 7.23"]:
            values, _, _ = census.parse_population_pdf(text)
            self.assertIn("pop_total", values)
        self.assertIsNone(census.number(values["pop_transgender"]))

    def test_zero_is_not_missing_or_a_guessed_dash(self):
        self.assertEqual(census.number("0"), 0)
        self.assertIsNone(census.number("-"))
        self.assertIsNone(census.number(""))
        for value in ["nan", "inf", "-1", "1.5", "broken"]:
            with self.assertRaises(ValueError):
                census.number(value)

    def test_education_rejects_missing_sex_or_overall_context(self):
        with self.assertRaises(ValueError):
            census.parse_education_pdf("RURAL\nALL SEXES\n5 AND ABOVE 10 1 1 1 1 1 1 1 1 1 1")

    def test_2023_rejects_duplicate_summary(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"source.csv"
            path.write_text("title\nEXAMPLE DISTRICT\nALL LOCALITIES\nALL SEXES\n5 & ABOVE,"+",".join(["100"]+["1"]*12)+"\n5 & ABOVE,"+",".join(["100"]+["1"]*12)+"\n")
            with self.assertRaisesRegex(ValueError, "Duplicate"):
                list(census.parse_2023(path, "13"))

    def test_2023_protected_area_and_excludes_tehsil(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp)/"source.csv"
            path.write_text("1,2,3,4,5,6,7,8,9,10,11,12\nMALAKAND PROTECTED AREA,1,100,50,50,0,1,1,1,1,1,1\nOTHER TEHSIL,1,80,40,40,0,1,1,1,1,1,1\n")
            records = list(census.parse_2023(path, "1"))
            self.assertEqual(len(records), 1)
            self.assertEqual(records[0][1]["pop_total"], "100")

    def test_2023_pdf_wrapped_population_does_not_take_rural_count(self):
        text = "NORTH WAZIRISTAN\n4,707 693,332 354,636 338,681 15 104.71 147.30 0.60 6.9 540,546 4.25\nDISTRICT\nRURAL 689,201 352,371 336,816 14 104.62 6.9 536,182 4.29"
        records = list(census.parse_2023_pdf(text, "1"))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0][0], "NORTH WAZIRISTAN DISTRICT")
        self.assertEqual(records[0][1]["pop_total"], "693,332")

    def test_2023_pdf_ict_label_and_all_sexes(self):
        row = "5 & ABOVE " + " ".join(["100"] + ["1"]*12)
        records = list(census.parse_2023_pdf("ISLAMABAD CAPITAL TERRITORY\nALL LOCALITIES\nALL SEXES\n" + row + "\nMALE\n" + row, "13"))
        self.assertEqual(len(records), 1)
        self.assertEqual(census.bare_name(records[0][0]), "ISLAMABAD")
        self.assertEqual(records[0][1]["never_attended"], "1")

    def test_2023_pdf_duplicate_and_truncated_summary_stop(self):
        title = "GHOTKI DISTRICT\nALL LOCALITIES\nALL SEXES\n"
        with self.assertRaisesRegex(ValueError, "expected 13"):
            list(census.parse_2023_pdf(title + "5 & ABOVE 100 10", "13"))
        row = "5 & ABOVE " + " ".join(["100"] + ["1"]*12)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            list(census.parse_2023_pdf(title + row + "\n" + row, "13"))

    def test_missing_component_withholds_rate(self):
        meta = {"source_dataset":"census2023_education"}
        raw = dict.fromkeys(census.EDU23, "1")
        raw.update(total="20", intermediate="")
        rows = {r["indicator"]:r for r in census.observations(meta, raw)}
        self.assertIsNone(rows["pct_matric_plus"]["value"])
        self.assertEqual(rows["pct_matric_plus"]["status"], "missing_component")
        self.assertEqual(rows["pct_never_attended"]["denominator"], 20)

    def test_duplicate_observations_stop_validation(self):
        row = dict(source_dataset="x", source_unit_id="x", indicator="x", value=1)
        with self.assertRaisesRegex(ValueError, "Duplicate"):
            census.validate([row, row])

    def test_inconsistent_count_is_explicit_issue(self):
        rows = [dict(source_dataset="census2023_population", source_unit_id="x", indicator=k, value=v)
                for k,v in zip(census.POP, [100, 50, 40, 0])]
        checks, issues = census.validate(rows)
        self.assertEqual(issues[0]["residual"], 10)
        self.assertEqual(checks[0]["status"], "source_count_discrepancy")

    def test_inconsistent_education_withholds_derived_values(self):
        rows = census.observations(dict(source_dataset="census2023_education", source_unit_id="x"),
                                   {k:"100" if k == "total" else "1" for k in census.EDU23})
        census.validate(rows)
        rate = next(r for r in rows if r["indicator"] == "pct_never_attended")
        self.assertEqual(rate["status"], "withheld_source_count_discrepancy")
        self.assertIsNone(rate["value"])
        self.assertEqual(rate["denominator"], 100)

    def test_input_hash_change_and_missing_file_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);p=root/"input.csv";p.write_text("abc")
            entry=dict(path=p.name, bytes=p.stat().st_size, sha256=sha256(p))
            verify_files(root,[entry])
            p.write_text("abd")
            with self.assertRaisesRegex(ValueError,"Changed input"):
                verify_files(root,[entry])
            p.unlink()
            with self.assertRaisesRegex(ValueError,"Missing required"):
                verify_files(root,[entry])

    def test_input_lock_cannot_escape_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError,"escapes"):
                verify_files(Path(tmp),[dict(path="../outside", bytes=0, sha256="")])

    def test_existing_output_not_replaced(self):
        with tempfile.TemporaryDirectory() as tmp:
            out=Path(tmp)/"release";out.mkdir();sentinel=out/"keep";sentinel.write_text("same")
            with self.assertRaisesRegex(ValueError,"already exists"):
                census.build(Path(tmp)/"raw",out,Path(tmp)/"absent-lock",Path(tmp)/"base",Path(tmp)/"code")
            self.assertEqual(sentinel.read_text(),"same")

    def test_missing_input_build_never_creates_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/"raw";source.mkdir();out=root/"release"
            lock=root/"lock.json"
            lock.write_text(json.dumps(dict(schema_version=census.SCHEMA_VERSION,
                                           files=[dict(path="absent.pdf",bytes=0,sha256="")],baseline_files=[])))
            with mock.patch.object(census.shutil,"which",return_value="pdftotext"):
                with self.assertRaisesRegex(ValueError,"Missing required input"):
                    census.build(source,out,lock,root/"base",root/"legacy.py")
            self.assertFalse(out.exists())

    def test_failed_parquet_write_cleans_staging_and_preserves_baseline(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);source=root/"raw";source.mkdir();base=root/"base";base.mkdir()
            panel=base/"districts.json";panel.write_text('{"example": {}}')
            (base/"pakistan_districts_province_boundries.geojson").write_text(json.dumps(dict(features=[dict(properties=dict(districts="example"))])))
            legacy=root/"legacy.py";legacy.write_text("CROSSWALK = {}")
            lock=root/"lock.json";lock.write_text(json.dumps(dict(schema_version=census.SCHEMA_VERSION,
                files=[],baseline_files=[],legacy_crosswalk_sha256=sha256(legacy))))
            meta=dict(source_dataset="census2023_population",source_unit_id="example",source_name="EXAMPLE DISTRICT",year=2023)
            rows=census.observations(meta,dict(zip(census.POP,["100","50","50","0"])))
            with mock.patch.object(census.shutil,"which",return_value="pdftotext"), \
                 mock.patch.object(census,"source_rows",return_value=rows), \
                 mock.patch.object(census,"parquet",side_effect=ValueError("write failure")):
                with self.assertRaisesRegex(ValueError,"write failure"):
                    census.build(source,root/"release",lock,base,legacy)
            self.assertFalse((root/"release").exists())
            self.assertFalse(list(root.glob(".census-build-*")))
            self.assertEqual(panel.read_text(),'{"example": {}}')


if __name__ == "__main__":
    unittest.main()
