"""Regressions for source errors and their dependent website displays."""
import copy
import json
import sys
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "etl"))
import census_corrections as correction


class CensusCorrectionTests(unittest.TestCase):
    def setUp(self):
        self.data = json.loads((REPO / "app/data/districts.json").read_text())

    def test_shifted_ghotki_cells_and_rates_are_repaired(self):
        self.data["ghotki"].update(t_edu_2023_never_attended=77551, t_edu_2023_matric=None,
                                   t_edu_2023_pct_never_attended=5.24)
        row = correction.corrected(self.data)["ghotki"]
        self.assertEqual(row["t_edu_2023_never_attended"], 908928)
        self.assertEqual(row["t_edu_2023_matric"], 77551)
        self.assertEqual(row["t_edu_2023_pct_never_attended"], round(908928 / 1481137 * 100, 2))
        self.assertEqual(row["t_edu_diff_pct_never_attended"], round(row["t_edu_2023_pct_never_attended"] - row["t_edu_2017_pct_never_attended"], 4))

    def test_south_waziristan_uses_overall_row_and_repairs_change(self):
        self.data["south waziristan agency"]["t_edu_2017_total"] = 50
        row = correction.corrected(self.data)["south waziristan agency"]
        self.assertEqual(row["t_edu_2017_total"], 554377)
        self.assertEqual(row["t_edu_2017_never_attended"], 362389)
        self.assertEqual(row["t_edu_2017_intermediate"], 8325)
        self.assertEqual(row["t_edu_diff_total"], 736393 - 554377)

    def test_karachi_combines_counts_with_education_denominator(self):
        result = correction.corrected(self.data)
        c, p = result["keamari"], result["karachi west"]
        expected = round(100 * (c["t_edu_2023_never_attended"] + p["t_edu_2023_never_attended"]) /
                         (c["t_edu_2023_total"] + p["t_edu_2023_total"]) -
                         100 * p["t_edu_2017_never_attended"] / p["t_edu_2017_total"], 4)
        self.assertEqual(c["t_edu_diff_pct_never_attended"], expected)
        self.assertEqual(p["t_edu_diff_pct_never_attended"], expected)
        self.assertEqual(c["t_edu_diff_middle"], p["t_edu_diff_middle"])
        # Arbitrary all-age population weights must not affect education rates.
        other = copy.deepcopy(self.data)
        other["keamari"]["t1_2023_pop_total"] = 1
        self.assertEqual(correction.corrected(other)["keamari"]["t_edu_diff_pct_never_attended"], expected)

    def test_missing_chitral_counts_remove_false_change(self):
        self.data["chitral"]["t1_2023_pop_transgender"] = 0
        self.data["chitral"]["t1_diff_pop_transgender"] = -22
        result = correction.corrected(self.data)
        self.assertIsNone(result["chitral"]["t1_2023_pop_transgender"])
        self.assertNotIn("t1_diff_pop_transgender", result["chitral"])

    def test_unrelated_districts_and_fields_are_unchanged_and_idempotent(self):
        original = copy.deepcopy(self.data)
        result = correction.corrected(self.data)
        allowed = {x["district_key"] for x in json.loads(correction.SOURCE.read_text())["education"]} | {"karachi west"}
        for row in correction.changes(self.data, result):
            self.assertTrue(row["district"] in allowed and row["field"].startswith("t_edu_") or
                            row["district"] == "chitral" and row["field"] in ("t1_2023_pop_transgender", "t1_diff_pop_transgender"))
        self.assertEqual(self.data, original)
        self.assertEqual(correction.corrected(result), result)

    def test_incomplete_source_or_destination_stops_without_mutation(self):
        source = json.loads(correction.SOURCE.read_text())["education"][0]
        source["counts"]["total"] = 1
        with self.assertRaisesRegex(ValueError, "does not reconcile"):
            correction.source_counts(source)
        del self.data["ghotki"]
        original = copy.deepcopy(self.data)
        with self.assertRaisesRegex(ValueError, "Required census row absent"):
            correction.apply_verified_census_corrections(self.data)
        self.assertEqual(self.data, original)


if __name__ == "__main__":
    unittest.main()
