import contextlib
import hashlib
import io
import json
import tempfile
import unittest
from pathlib import Path

import pipeline as p


def report(name="edition", priority=2023, year=2023, values=(3, 4, 7)):
    return {"report_id": name, "source_url": "https://example.test/report.pdf", "reporting_year": year,
            "period_start": f"{year}-01-01", "period_end": f"{year}-12-31", "is_full_year": True,
            "source_priority": priority, "source_type": "test_fixture", "category_partition_complete": True,
            "_path": "/tmp/test-report.json", "_hash": name,
            "observations": [{"year": year, "geography_name": "Sindh Province", "geography_level": "province",
                              "crime_category_raw": label, "cases_reported": value, "value_raw": str(value),
                              "row_type": "total" if label == "Total" else "detail"}
                             for label, value in zip(("Murder", "Other Theft", "Total"), values)]}


class ParsingTests(unittest.TestCase):
    def test_missing_dashes_are_not_zero(self):
        for value in (None, "", "—", "-", "N/A"):
            self.assertEqual(p.parse_cases(value), (None, None))
        self.assertEqual(p.parse_cases("0"), (0, None))
        self.assertEqual(p.parse_cases("12,345"), (12345, None))

    def test_invalid_counts_are_rejected(self):
        for value in (True, -1, "4.5", "1,00", "zero", "=1+2"):
            self.assertIsNotNone(p.parse_cases(value)[1])

    def test_full_year_requires_matching_dates(self):
        self.assertTrue(p.annual_period(2023, "2023-01-01", "2023-12-31", True))
        self.assertFalse(p.annual_period(2023, "2023-01-01", "2023-11-30", True))
        self.assertFalse(p.annual_period(2022, "2023-01-01", "2023-12-31", True))
        self.assertFalse(p.annual_period(2023, "2023-01-01", "2023-12-31", "true"))

    def test_original_labels_retained_canonical_keys_match(self):
        r = report()
        r["observations"][0]["crime_category_raw"] = "Grevious Hurt"
        r["observations"][-1]["crime_category_key"] = "t_o_t_a_l"
        rows, _, _ = p.normalise_reports([r])
        self.assertEqual(rows[0]["crime_category_raw"], "Grevious Hurt")
        self.assertEqual(rows[0]["crime_category_key"], "grievous_hurt")
        self.assertEqual(rows[-1]["crime_category_key"], "total")
        self.assertEqual(p.geography_key("S.B.ABAD RANGE", "range"), "range:shaheed_benazirabad")

    def test_raw_count_disagreement_does_not_enter_preferred(self):
        r = report()
        r["observations"][0]["cases_reported"] = 99
        rows, _, issues = p.normalise_reports([r])
        selected, _ = p.select_annual(rows, 2019, 2025)
        self.assertTrue(any(i["issue"] == "raw_parsed_count_mismatch" for i in issues))
        self.assertEqual(len(selected), 2)


class SelectionTests(unittest.TestCase):
    def test_explicit_priority_keeps_all_versions_and_selects_latest(self):
        rows, _, _ = p.normalise_reports([report("old", 10), report("new", 20, values=(5, 4, 9))])
        selected, conflicts = p.select_annual(rows, 2019, 2025)
        self.assertEqual(len(rows), 6)
        self.assertEqual(len(selected), 3)
        self.assertEqual({r["report_id"] for r in selected}, {"new"})
        self.assertEqual(conflicts, [])

    def test_equal_priority_conflicts_are_withheld(self):
        rows, _, _ = p.normalise_reports([report("a", 10), report("b", 10, values=(5, 4, 9))])
        selected, conflicts = p.select_annual(rows, 2019, 2025)
        self.assertEqual(len(conflicts), 2)
        self.assertEqual(len(selected), 1)

    def test_newer_partial_report_cannot_replace_complete_data(self):
        newer = report("partial", 20, values=(5, 4, 9))
        newer["selection_eligible"] = False
        rows, _, _ = p.normalise_reports([report("complete", 10), newer])
        selected, _ = p.select_annual(rows, 2019, 2025)
        self.assertEqual({r["report_id"] for r in selected}, {"complete"})

    def test_sparse_annual_period_does_not_pass_cohort_coverage(self):
        rows, _, _ = p.normalise_reports([report()])
        checks = p.cohort_checks(rows)
        self.assertFalse(checks[0]["complete_45_rows_by_7_geographies"])
        selected, _ = p.select_annual(rows, 2019, 2025)
        self.assertEqual(selected, [])

    def test_total_mismatch_flagged_without_correction(self):
        r = report(values=(3, 4, 99))
        rows, _, _ = p.normalise_reports([r])
        check = p.total_checks(rows, [r])[0]
        self.assertEqual(check["status"], "mismatch")
        self.assertEqual(check["reported"], 99)
        self.assertEqual(check["calculated"], 7)
        self.assertEqual(rows[-1]["cases_reported"], 99)


class ArchiveTests(unittest.TestCase):
    def test_identical_json_import_is_deduplicated_by_payload_hash(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            body = json.dumps(report()).encode()
            (root / "original.json").write_bytes(body)
            copy = root / "extracts" / "copy.json"
            copy.parent.mkdir()
            copy.write_bytes(body)
            reports, issues = p.load_reports(root)
            self.assertEqual(len(reports), 1)
            self.assertEqual(len(reports[0]["_snapshot_aliases"]), 1)
            self.assertEqual(issues, [])

    def test_declared_source_pdf_and_evidence_checksums_are_enforced(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            evidence = root / "evidence.json"
            evidence.write_text("original")
            r = report()
            r["source_document_path"] = str(root / "missing.pdf")
            r["source_document_sha256"] = "bad-hash"
            r["raw_evidence"] = [{"path": str(evidence), "sha256": "bad-hash"}]
            rows, _, issues = p.normalise_reports([r])
            self.assertIn("source_document_missing", {i["issue"] for i in issues})
            self.assertIn("raw_evidence_checksum_mismatch", {i["issue"] for i in issues})
            self.assertEqual(p.select_annual(rows, 2019, 2025)[0], [])

    def test_cloudflare_html_at_pdf_url_is_archived_but_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            data = b"<!doctype html><html>Cloudflare access denied</html>"
            with self.assertRaisesRegex(ValueError, "No PDF or observations produced"):
                p.archive(root, data, "https://example.test/a.pdf", expected_pdf=True)
            entry = json.loads((root / "manifest.jsonl").read_text())
            self.assertEqual(entry["kind"], "html")
            self.assertEqual(entry["status"], "rejected_non_pdf_or_http_error")
            self.assertEqual(Path(entry["path"]).read_bytes(), data)

    def test_cache_reuse_checks_hash_and_never_overwrites_corruption(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d)
            data = b"%PDF-1.7 test"
            first = p.archive(root, data, "https://example.test/a.pdf", expected_pdf=True)
            p.archive(root, data, "https://example.test/a.pdf", expected_pdf=True)
            target = Path(first["path"])
            target.write_bytes(b"corrupted")
            with self.assertRaisesRegex(ValueError, "checksum mismatch"):
                p.archive(root, data, "https://example.test/a.pdf", expected_pdf=True)
            self.assertEqual(target.read_bytes(), b"corrupted")

    def test_empty_build_has_typed_tables_and_explicit_missing_years(self):
        with tempfile.TemporaryDirectory() as d, contextlib.redirect_stdout(io.StringIO()):
            root = Path(d)
            quality = p.build(root / "raw", root / "out")
            self.assertEqual(quality["missing_years"], list(range(2019, 2026)))
            output = root / "out" / "sindh_crime_annual.json"
            before = hashlib.sha256(output.read_bytes()).hexdigest()
            p.build(root / "raw", root / "out")
            self.assertEqual(before, hashlib.sha256(output.read_bytes()).hexdigest())
            self.assertIn("cases_reported", (root / "out" / "sindh_crime_annual.csv").read_text())


if __name__ == "__main__":
    unittest.main()
