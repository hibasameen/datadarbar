import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pipeline as p


META = dict(url="https://example.org/source", sha256="abc", retrieved_at="2026-09-06T00:00:00Z")


def feature(iso="PAK", eventtype="FL", **overrides):
    props = dict(iso3=iso, eventtype=eventtype, eventid=123, episodeid=1,
                 name="Flood", fromdate="2022-06-14T01:00:00", todate="2022-08-31T01:00:00",
                 datemodified="2022-09-01", affectedcountries=[])
    props.update(overrides)
    return {"type": "Feature", "geometry": {"type": "Point", "coordinates": [75, 36]}, "properties": props}


class Sheet:
    def __init__(self, rows):
        self.values = rows


class Workbook:
    def __init__(self, sheets):
        self.sheets = sheets
        self.sheetnames = list(sheets)

    def __getitem__(self, key):
        return Sheet(self.sheets[key])

    def __iter__(self):
        return iter(Sheet(v) for v in self.sheets.values())

    def close(self):
        pass


class PipelineTests(unittest.TestCase):
    def rows(self, *features):
        return p.gdacs_rows(dict(type="FeatureCollection", features=list(features)), META)

    def test_pakistan_filter_and_nonclimate_exclusion(self):
        rows = self.rows(feature(), feature("IND"), feature(eventtype="EQ"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["record_id"], "gdacs:FL:123:PAK")
        self.assertIsNone(rows[0]["deaths"])
        self.assertNotIn("district_key", rows[0])

    def test_crossborder_event_included(self):
        self.assertEqual(len(self.rows(feature("IND", affectedcountries=[{"iso3": "PAK"}]))), 1)

    def test_bad_api_payload_fails(self):
        with self.assertRaises(ValueError):
            p.gdacs_rows({"error": "rate limited"}, META)

    def test_revision_dedupe_not_maximum_deaths(self):
        old = self.rows(feature())[0]
        new = dict(old, source_modified="2022-09-02", deaths=8)
        old["deaths"] = 10
        self.assertEqual(p.dedupe_events([new, old, old]), [new])

    def test_sources_never_automerged(self):
        one = self.rows(feature())[0]
        two = dict(one, record_id="emdat:2022-1-PAK", source="emdat")
        self.assertEqual(len(p.dedupe_events([one, two])), 2)

    def test_missing_is_not_zero(self):
        for value in (None, "-", "", "N/A"):
            self.assertIsNone(p.number(value))
        self.assertEqual(p.number(0), 0)
        self.assertEqual(p.number("1,000.5"), 1000.5)
        for value in ("NaN", "inf", -1):
            with self.assertRaises(ValueError):
                p.number(value)

    def test_partial_dates_not_fabricated(self):
        self.assertEqual(p.emdat_date({"Start Year": 2022}, "Start"), (None, "year"))
        self.assertEqual(p.emdat_date({"Start Year": 2022, "Start Month": 6}, "Start"), (None, "month"))

    def test_emdat_header_filter_and_dollar_conversion(self):
        header = ["DisNo.", "ISO", "Disaster Type", "Start Year", "Total Damage ('000 US$)", "Total Deaths"]
        book = Workbook({"Data": [["Introductory note"], header,
            ["2022-1-PAK", "PAK", "Flood", 2022, 1.5, 0],
            ["2022-2-IND", "IND", "Flood", 2022, 9, 9],
            ["2022-3-PAK", "PAK", "Earthquake", 2022, 9, 9]]})
        with tempfile.TemporaryDirectory() as d, patch("openpyxl.load_workbook", return_value=book):
            file = Path(d) / "export.xlsx"
            file.write_bytes(b"mocked workbook")
            rows = p.import_emdat(file, p.Store(Path(d) / "raw"))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["damage_usd"], 1500)
        self.assertEqual(rows[0]["deaths"], 0)
        self.assertIsNone(rows[0]["start_date"])

    def test_unosat_one_sheet_nulls_and_summary_levels(self):
        header = ["Province/ District", "Area of Province/District", "Analyzed area in cloud free zones (km2)",
                  "Percentage of analyzed area", "Maximum flood water extent (km2)",
                  "Total population in Province/ District", "Total population in cloud free area",
                  "Population potentially exposed", "Population potentially exposed (%)"]
        values = [header, ["Federal Capital Territory", 1, 1, 1, "-", 100, 100, "-", "-"],
                  ["Islamabad", 1, 1, 1, 0, 100, 100, 0, 0]]
        book = Workbook({"Statistics the whole country": values, "Duplicate sheet": values})
        with patch("openpyxl.load_workbook", return_value=book):
            rows = p.exposure_rows(b"", {"id": 3343}, META, {"islamabad"})
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["admin_level"], "province")
        self.assertIsNone(rows[0]["population_exposed"])
        self.assertEqual(rows[1]["population_exposed"], 0)
        self.assertEqual(rows[1]["district_key"], "islamabad")

    def test_unosat_unknown_layout_fails(self):
        with patch("openpyxl.load_workbook", return_value=Workbook({"New layout": []})):
            with self.assertRaisesRegex(ValueError, "Unknown UNOSAT"):
                p.exposure_rows(b"", {"id": 3343}, META, set())

    def ndma_fixture(self):
        header = "Cumulative Casualties and Injuries - (From 26 June 2026 to 5 September 2026)\n"
        table = "Male Female Children Total Male Female Children Total\n"
        table += "\n".join(f"{region} 1 2 3 6 1 2 3 6" for region in ["Punjab", "KP", "Sindh", "Balochistan", "GB", "AJ&K", "ICT"])
        table += "\nGrand T otal 7 14 21 42 7 14 21 42\n"
        meta = dict(report_id="ndma:test", raw_sha256="test", source_url="https://ndma.gov.pk/test.pdf")
        return [dict(meta, page=3, text="Last 24 Hours\n" + header), dict(meta, page=4, text=table)]

    def test_ndma_split_page_and_cumulative_period(self):
        rows = p.ndma_impacts(self.ndma_fixture())
        self.assertEqual(len(rows), 64)
        self.assertTrue(all(r["source_page"] == 4 and r["period_type"] == "cumulative" for r in rows))
        national = [r for r in rows if r["metric"] == "deaths_total" and r["location_name"] == "Pakistan"][0]
        self.assertEqual(national["value"], 42)
        self.assertEqual(national["period_start"], "2026-06-26")

    def test_ndma_bad_totals_rejected(self):
        pages = self.ndma_fixture()
        pages[1]["text"] = pages[1]["text"].replace("Punjab 1 2 3 6", "Punjab 1 2 3 7")
        with self.assertRaisesRegex(ValueError, "subtotal"):
            p.ndma_impacts(pages)

    def test_ndma_missing_region_rejected(self):
        pages = self.ndma_fixture()
        pages[1]["text"] = pages[1]["text"].replace("GB 1 2 3 6 1 2 3 6\n", "")
        with self.assertRaisesRegex(ValueError, "missing regions"):
            p.ndma_impacts(pages)

    def test_ndma_chart_title_touching_last_cell(self):
        pages = self.ndma_fixture()
        pages[1]["text"] = pages[1]["text"].replace("Punjab 1 2 3 6 1 2 3 6\n",
            "Punjab 1 2 3 6 1 2 3 6Cumulative House Damaged\n")
        self.assertEqual(len(p.ndma_impacts(pages)), 64)

    def test_no_event_response_is_explicit(self):
        class FakeStore:
            def get(self, *args):
                return b"", dict(META, http_status=204)
        with patch("builtins.print"):
            self.assertEqual(p.fetch_gdacs(FakeStore(), p.date(2000, 1, 1), p.date(2000, 12, 31)), [])

    def test_asset_paths(self):
        links = dict(p.unosat_assets(dict(id=3343, pdf_name="report.pdf",
                     excel_table="/unosat_filesystem/3343/data.xlsx", shp_link="/static/3343/data.zip", kml_link="None")))
        self.assertEqual(links["xlsx"], "https://unosat.org/static/unosat_filesystem/3343/data.xlsx")
        self.assertEqual(links["pdf"], "https://unosat.org/static/unosat_filesystem/3343/report.pdf")
        self.assertNotIn("kml", links)

    def test_ndma_deduplicates_pdf_links(self):
        html = '<a href="/r.pdf">Report</a><a href="/r.pdf">View</a><a href="?page=2">Next</a>'
        self.assertEqual(p.ndma_links(html, "https://ndma.gov.pk/sitreps"), [("https://ndma.gov.pk/r.pdf", "View")])

    def test_exact_match_only(self):
        self.assertEqual(p.district_match("Dera Bugti", {"dera bugti"}), "dera bugti")
        self.assertIsNone(p.district_match("Karachi City", {"karachi east", "karachi west"}))

    def test_cache_immutable_and_checksum_verified(self):
        with tempfile.TemporaryDirectory() as d:
            s = p.Store(d)
            _, a = s.record("https://test/a.json", b"old", "test", "application/json")
            _, b = s.record("https://test/a.json", b"new", "test", "application/json")
            self.assertNotEqual(a["path"], b["path"])
            self.assertEqual((Path(d) / a["path"]).read_bytes(), b"old")
            offline = p.Store(d, offline=True)
            self.assertEqual(offline.get("https://test/a.json", "test")[0], b"new")
            (Path(d) / b["path"]).write_bytes(b"bad")
            with self.assertRaises(ValueError):
                offline.get("https://test/a.json", "test")

    def test_offline_miss(self):
        with tempfile.TemporaryDirectory() as d:
            with self.assertRaises(FileNotFoundError):
                p.Store(d, offline=True).get("https://test/missing", "test")

    def test_repeated_pagination_rejected(self):
        class FakeStore:
            def get(self, *args):
                return json.dumps(dict(type="FeatureCollection", features=[feature()] * 100)), META
        with self.assertRaisesRegex(ValueError, "repeated a page"):
            p.fetch_gdacs(FakeStore(), p.date(2022, 1, 1), p.date(2022, 12, 31))

    def test_empty_tables_typed_and_build_repeatable(self):
        import duckdb
        with tempfile.TemporaryDirectory() as d:
            raw, out = Path(d) / "raw", Path(d) / "out"
            p.write_json(raw / "runs/a.json", dict(source="test", events=self.rows(feature())))
            with patch("builtins.print"):
                first = p.build(raw, out)
                csv = (out / "climate_events.csv").read_bytes()
                second = p.build(raw, out)
            self.assertEqual(first["row_counts"], second["row_counts"])
            self.assertEqual(csv, (out / "climate_events.csv").read_bytes())
            table = duckdb.read_parquet(str(out / "climate_exposures.parquet"))
            self.assertEqual(table.count("*").fetchone()[0], 0)
            self.assertEqual(str(table.types[table.columns.index("population_exposed")]), "DOUBLE")


if __name__ == "__main__":
    unittest.main()
