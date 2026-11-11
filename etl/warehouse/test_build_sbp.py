#!/usr/bin/env python3
"""Offline tests for build_sbp.py -- no network, no real API key.

    python3 test_build_sbp.py

Every HTTP call is faked, so this verifies parsing, pacing, backoff, resume
and the DuckDB load without spending any of the 2,000/day API budget.
"""

from __future__ import annotations

import io
import json
import sys
import tempfile
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_sbp as B  # noqa: E402


# ---------------------------------------------------------------- fake server

class FakeHTTP:
    """Stands in for urllib.request.urlopen."""

    def __init__(self, datasets: dict, series: dict, fail_429_after: int | None = None):
        self.datasets = datasets
        self.series = series
        self.calls: list[str] = []
        self.fail_429_after = fail_429_after
        self._429_fired = 0

    def __call__(self, req, timeout=None):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        self.calls.append(url)
        path = url.split("/api/v1/", 1)[1].split("?", 1)[0]

        if (self.fail_429_after is not None
                and len(self.calls) > self.fail_429_after
                and self._429_fired < 1):
            self._429_fired += 1
            raise urllib.error.HTTPError(url, 429, "Too Many Requests", {}, None)

        if path.startswith("dataset/"):
            code = path.split("/")[1]
            if code not in self.datasets:
                raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)
            body = {
                "columns": ["Reference Dataset Name", "Series Code", "Series Name",
                            "Series Description", "Data Frequency", "Unit",
                            "Variable Type", "Available Since", "Available Upto",
                            "Last Refresh Date"],
                "rows": self.datasets[code],
            }
        elif path.startswith("series/"):
            key = path.split("/")[1]
            body = {
                "columns": ["Dataset Name", "Series Key", "Series Name",
                            "Observation Date", "Observation Value", "Unit",
                            "Observation Status", "Status Comments"],
                "rows": self.series.get(key, []),
            }
        else:
            raise urllib.error.HTTPError(url, 404, "Not Found", {}, None)

        return _Resp(json.dumps(body).encode())


class _Resp(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *a):
        self.close()
        return False


DATASETS = {
    "TS_GP_BOP_WR_M": [
        ["Country-wise Workers' Remittances", "TS_GP_BOP_WR_M.WR0010",
         "Total Cash inflow", "desc", "Monthly", "USD", "Flow",
         "31-Jul-1972", "31-Jul-2026", "10-Aug-2026"],
        ["Country-wise Workers' Remittances", "TS_GP_BOP_WR_M.WR0020",
         "USA remittances", "desc", "Monthly", "USD", "Flow",
         "31-Jul-1972", "31-Jul-2026", "10-Aug-2026"],
    ],
    "TS_GP_PF_ZAKATRCT_Y": [
        ["Zakat Receipts", "TS_GP_PF_ZAKATRCT_Y.Z001", "Zakat", "desc",
         "Annual", "PKR", "Flow", "30-Jun-1988", "30-Jun-2025", "12-Dec-2025"],
    ],
}

SERIES = {
    "TS_GP_BOP_WR_M.WR0010": [
        ["Country-wise Workers' Remittances", "TS_GP_BOP_WR_M.WR0010", "Total",
         "2026-07-31", "3631.044564", "Million USD", "Normal", ""],
        ["Country-wise Workers' Remittances", "TS_GP_BOP_WR_M.WR0010", "Total",
         "1972-07-31", "9.5", "Million USD", "Normal", ""],
    ],
    "TS_GP_BOP_WR_M.WR0020": [
        ["Country-wise Workers' Remittances", "TS_GP_BOP_WR_M.WR0020", "USA",
         "2026-07-31", "1,234.5", "Million USD", "Normal", ""],
        ["Country-wise Workers' Remittances", "TS_GP_BOP_WR_M.WR0020", "USA",
         "2026-06-30", "-", "Million USD", "Not available", "suppressed"],
    ],
    "TS_GP_PF_ZAKATRCT_Y.Z001": [
        ["Zakat Receipts", "TS_GP_PF_ZAKATRCT_Y.Z001", "Zakat",
         "30-Jun-2025", "1000", "Million PKR", "Normal", ""],
    ],
}


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self._saved = {k: getattr(B, k) for k in
                       ("WAREHOUSE", "STATE_DIR", "CATALOG_JSON", "OBS_DIR", "CHECKPOINT", "HERE")}
        B.WAREHOUSE = root
        B.STATE_DIR = root / "sbp_state"
        B.CATALOG_JSON = B.STATE_DIR / "series_catalog.json"
        B.OBS_DIR = B.STATE_DIR / "obs"
        B.CHECKPOINT = B.STATE_DIR / "checkpoint.json"
        B.HERE = root
        (root / "sbp_dataset_codes.json").write_text(json.dumps({
            "prefix": "TS_GP_",
            "codes": ["BOP_WR_M", "PF_ZAKATRCT_Y"],
        }))
        self._urlopen = urllib.request.urlopen
        import os
        os.environ["SBP_API_KEY"] = "TESTKEY"

    def tearDown(self):
        urllib.request.urlopen = self._urlopen
        for k, v in self._saved.items():
            setattr(B, k, v)
        self.tmp.cleanup()

    def args(self, **kw):
        base = dict(rate=100000.0, burst=50, daily_budget=1900, refresh=False,
                    subject_area=None, dataset=None, frequency=None, match=None,
                    since=None, all=False, start=None, end=None,
                    select=None, tier=None)
        base.update(kw)
        return type("A", (), base)()


# ---------------------------------------------------------------- unit tests

class TestParsing(Base):
    def test_rows_to_dicts_normalises_keys(self):
        payload = {"columns": ["Series Code", "Available Since", "Last Refresh Date"],
                   "rows": [["K1", "31-Jul-1972", "10-Aug-2026"]]}
        d = B.rows_to_dicts(payload)[0]
        self.assertEqual(d["series_code"], "K1")
        self.assertEqual(d["available_since"], "31-Jul-1972")
        self.assertEqual(d["last_refresh_date"], "10-Aug-2026")

    def test_rows_to_dicts_empty(self):
        self.assertEqual(B.rows_to_dicts({}), [])
        self.assertEqual(B.rows_to_dicts({"columns": ["A"], "rows": []}), [])

    def test_parse_num(self):
        self.assertEqual(B.parse_num("1,234.5"), 1234.5)
        self.assertEqual(B.parse_num(".5"), 0.5)          # EasyData emits bare ".5"
        self.assertEqual(B.parse_num("0"), 0.0)
        for bad in (None, "", "-", "NA", "N/A", "abc"):
            self.assertIsNone(B.parse_num(bad), bad)

    def test_parse_date_both_formats(self):
        self.assertEqual(B.parse_date("2026-07-31"), "2026-07-31")   # data endpoint
        self.assertEqual(B.parse_date("31-Jul-1972"), "1972-07-31")  # meta endpoint
        self.assertEqual(B.parse_date("2024"), "2024-12-31")         # bare year
        self.assertIsNone(B.parse_date("garbage"))
        self.assertIsNone(B.parse_date(None))

    def test_subject_area_mapping(self):
        self.assertEqual(B.subject_area("TS_GP_BOP_WR_M"), "External Sector")
        self.assertEqual(B.subject_area("TS_GP_PF_ZAKATRCT_Y"), "Public Finance")
        self.assertEqual(B.subject_area("TS_GP_RLS_CPI0708_M"), "Real Sector")
        self.assertEqual(B.subject_area("TS_GP_IR_SIRPR_AH"), "Interest Rates")
        self.assertEqual(B.subject_area("TS_GP_MFS_EPUI_M"), "Monetary and Financial Sector")
        self.assertEqual(B.subject_area("TS_GP_PDL_NSSOA_M"), "Pakistan's Debt Profile")

    def test_every_real_dataset_code_maps_to_a_subject(self):
        seed = json.loads((Path(__file__).resolve().parent /
                           "sbp_dataset_codes.json").read_text())
        unmapped = [c for c in seed["codes"]
                    if B.subject_area(seed["prefix"] + c) == "Other"]
        self.assertEqual(unmapped, [], f"unmapped dataset prefixes: {unmapped}")


class TestMalformedJSON(Base):
    def test_raw_control_characters_are_tolerated(self):
        # Real failure mode: TS_GP_RL_CCSIND_M returns a description containing a
        # literal newline, which strict JSON rejects.
        raw = ('{"columns": ["Series Code", "Series Description"],'
               ' "rows": [["K1", "line one\nline two\ttabbed"]]}').encode()
        with self.assertRaises(json.JSONDecodeError):
            json.loads(raw.decode())
        payload = B.decode_payload(raw)
        self.assertEqual(payload["rows"][0][0], "K1")
        self.assertIn("line two", payload["rows"][0][1])

    def test_unescaped_inner_quotes_are_repaired(self):
        # Real failure mode: SBP's CPI moving-average series emit a status comment
        # containing unescaped quotes -- '... is missing "MIS" due to ...'.
        # strict=False does NOT help here; only the quote repair does.
        raw = ('{"columns": ["Series Key", "Status Comments"],'
               ' "rows": [["K1", "status is missing "MIS" due to trimming"]]}').encode()
        with self.assertRaises(json.JSONDecodeError):
            json.loads(raw.decode(), strict=False)
        payload = B.decode_payload(raw)
        self.assertEqual(payload["rows"][0][0], "K1")
        self.assertIn('"MIS"', payload["rows"][0][1])

    def test_repair_leaves_valid_json_untouched(self):
        good = '{"a": "he said \\"hi\\"", "b": ["x", "y"], "c": {"d": "e"}}'
        self.assertEqual(json.loads(B.repair_inner_quotes(good)), json.loads(good))

    def test_repair_handles_both_faults_together(self):
        raw = ('{"columns": ["A"], "rows": [["line\nbreak and "quoted" text"]]}').encode()
        payload = B.decode_payload(raw)
        self.assertIn("quoted", payload["rows"][0][0])

    def test_client_does_not_burn_retries_on_control_chars(self):
        calls = []

        def server(req, timeout=None):
            calls.append(req.full_url)
            return _Resp(b'{"columns": ["Series Code"], "rows": [["K\n1"]]}')

        urllib.request.urlopen = server
        c = B.Client("K", B.RateLimiter(100000, 50), daily_budget=100)
        out = c.get("dataset/X/meta")
        self.assertEqual(len(calls), 1, "malformed JSON should not trigger retries")
        self.assertEqual(len(out["rows"]), 1)


class TestSelectionFile(Base):
    def _catalog(self):
        urllib.request.urlopen = FakeHTTP(DATASETS, SERIES)
        B.cmd_catalog(self.args())

    def _write_sel(self, spec):
        p = B.HERE / "sel.json"
        p.write_text(json.dumps(spec))
        return "sel.json"

    def test_exact_series_and_dataset_entries(self):
        self._catalog()
        sel = self._write_sel({"tiers": {"t1": {"entries": [
            {"series": ["TS_GP_BOP_WR_M.WR0020"]},
            {"dataset": "TS_GP_PF_ZAKATRCT_Y"},
        ]}}})
        rows = B.flat_catalog()
        got = {r["series_key"] for r in
               B.select_series(self.args(select=sel, tier=None), rows)}
        self.assertEqual(got, {"TS_GP_BOP_WR_M.WR0020", "TS_GP_PF_ZAKATRCT_Y.Z001"})

    def test_include_regex(self):
        self._catalog()
        sel = self._write_sel({"tiers": {"t1": {"entries": [
            {"dataset": "TS_GP_BOP_WR_M", "include": "^USA"},
        ]}}})
        rows = B.flat_catalog()
        got = [r["series_key"] for r in
               B.select_series(self.args(select=sel, tier=None), rows)]
        self.assertEqual(got, ["TS_GP_BOP_WR_M.WR0020"])

    def test_tier_filter_and_dedup(self):
        self._catalog()
        sel = self._write_sel({"tiers": {
            "a": {"entries": [{"dataset": "TS_GP_BOP_WR_M"}]},
            "b": {"entries": [{"series": ["TS_GP_BOP_WR_M.WR0010"]},
                              {"dataset": "TS_GP_PF_ZAKATRCT_Y"}]},
        }})
        rows = B.flat_catalog()
        only_a = B.select_series(self.args(select=sel, tier=["a"]), rows)
        self.assertEqual(len(only_a), 2)
        both = B.select_series(self.args(select=sel, tier=["a", "b"]), rows)
        self.assertEqual(len(both), 3, "overlapping tiers must not duplicate series")

    def test_unknown_tier_is_fatal(self):
        self._catalog()
        sel = self._write_sel({"tiers": {"a": {"entries": []}}})
        with self.assertRaises(SystemExit):
            B.select_series(self.args(select=sel, tier=["nope"]), B.flat_catalog())

    def test_shipped_selection_file_is_valid(self):
        """The real sbp_selection.json must parse and only name real datasets."""
        build_dir = Path(__file__).resolve().parent
        spec = json.loads((build_dir / "sbp_selection.json").read_text())
        seed = json.loads((build_dir / "sbp_dataset_codes.json").read_text())
        known = {seed["prefix"] + c for c in seed["codes"]}

        self.assertTrue(spec["tiers"], "no tiers defined")
        for tname, tier in spec["tiers"].items():
            self.assertIn("description", tier, f"tier {tname} lacks a description")
            self.assertTrue(tier.get("entries"), f"tier {tname} has no entries")
            for entry in tier["entries"]:
                if "series" in entry:
                    for key in entry["series"]:
                        ds = key.split(".")[0]
                        self.assertIn(ds, known, f"{tname}: unknown dataset {ds}")
                        self.assertIn(".", key, f"{tname}: {key} is not a series key")
                if "dataset" in entry:
                    self.assertIn(entry["dataset"], known,
                                  f"{tname}: unknown dataset {entry['dataset']}")
                if entry.get("include"):
                    import re as _re
                    _re.compile(entry["include"])          # must be a valid regex


class TestRateLimiter(Base):
    def test_sustained_rate_is_respected(self):
        rl = B.RateLimiter(rate_per_min=600, burst=1)   # 0.1s apart
        t0 = time.monotonic()
        for _ in range(5):
            rl.wait()
        elapsed = time.monotonic() - t0
        self.assertGreater(elapsed, 0.25, "limiter let requests through too fast")

    def test_burst_then_throttle(self):
        rl = B.RateLimiter(rate_per_min=60, burst=3)
        t0 = time.monotonic()
        for _ in range(3):
            rl.wait()
        self.assertLess(time.monotonic() - t0, 0.2, "burst should be immediate")

    def test_on_429_backs_off_and_adds_penalty(self):
        rl = B.RateLimiter(rate_per_min=100000, burst=10)
        B.time.sleep = lambda s: None            # don't actually nap
        try:
            nap1 = rl.on_429()
            nap2 = rl.on_429()
            self.assertGreater(nap2, nap1, "backoff should grow")
            self.assertGreater(rl.penalty, 0)
            rl.on_success()
            self.assertLess(rl.penalty, 2.0)
        finally:
            B.time.sleep = time.sleep


class TestClient(Base):
    def test_retries_on_429_then_succeeds(self):
        fake = FakeHTTP(DATASETS, SERIES, fail_429_after=1)
        urllib.request.urlopen = fake
        B.time.sleep = lambda s: None
        try:
            c = B.Client("K", B.RateLimiter(100000, 50), daily_budget=100)
            c.get("dataset/TS_GP_BOP_WR_M/meta")
            out = c.get("dataset/TS_GP_PF_ZAKATRCT_Y/meta")
            self.assertEqual(len(out["rows"]), 1)
            self.assertEqual(c.spent, 3, "the retried request should be counted")
        finally:
            B.time.sleep = time.sleep

    def test_401_is_fatal_and_explains_rotation(self):
        def boom(req, timeout=None):
            raise urllib.error.HTTPError(req.full_url, 401, "Unauthorized", {}, None)
        urllib.request.urlopen = boom
        c = B.Client("K", B.RateLimiter(100000, 50), daily_budget=100)
        with self.assertRaises(SystemExit) as cm:
            c.get("dataset/X/meta")
        self.assertIn("90 days", str(cm.exception))

    def test_daily_budget_stops_before_spending(self):
        urllib.request.urlopen = FakeHTTP(DATASETS, SERIES)
        c = B.Client("K", B.RateLimiter(100000, 50), daily_budget=2)
        c.get("dataset/TS_GP_BOP_WR_M/meta")
        c.get("dataset/TS_GP_PF_ZAKATRCT_Y/meta")
        with self.assertRaises(B.BudgetExhausted):
            c.get("dataset/TS_GP_BOP_WR_M/meta")
        self.assertEqual(c.spent, 2)


class TestCatalog(Base):
    def test_catalog_and_resume(self):
        fake = FakeHTTP(DATASETS, SERIES)
        urllib.request.urlopen = fake
        B.cmd_catalog(self.args())
        cat = json.loads(B.CATALOG_JSON.read_text())
        self.assertEqual(len(cat), 2)
        self.assertEqual(len(cat["TS_GP_BOP_WR_M"]), 2)
        s = cat["TS_GP_BOP_WR_M"][0]
        self.assertEqual(s["series_key"], "TS_GP_BOP_WR_M.WR0010")
        self.assertEqual(s["available_since"], "1972-07-31")
        self.assertEqual(s["frequency"], "Monthly")

        # rerun: nothing further should be fetched
        before = len(fake.calls)
        B.cmd_catalog(self.args())
        self.assertEqual(len(fake.calls), before, "resume re-fetched completed datasets")

    def test_catalog_records_failures_without_aborting(self):
        bad = dict(DATASETS)
        fake = FakeHTTP({"TS_GP_BOP_WR_M": bad["TS_GP_BOP_WR_M"]}, SERIES)
        urllib.request.urlopen = fake
        B.cmd_catalog(self.args())
        cp = json.loads(B.CHECKPOINT.read_text())
        self.assertIn("TS_GP_PF_ZAKATRCT_Y", cp["catalog_failed"])
        self.assertIn("TS_GP_BOP_WR_M", cp["catalog_done"])


class TestObservations(Base):
    def _catalog(self):
        urllib.request.urlopen = FakeHTTP(DATASETS, SERIES)
        B.cmd_catalog(self.args())

    def test_requires_explicit_scope(self):
        self._catalog()
        with self.assertRaises(SystemExit):
            B.cmd_observations(self.args())

    def test_fetch_all_and_values(self):
        self._catalog()
        fake = FakeHTTP(DATASETS, SERIES)
        urllib.request.urlopen = fake
        B.cmd_observations(self.args(all=True))
        obs = json.loads((B.OBS_DIR / "TS_GP_BOP_WR_M.json").read_text())
        self.assertEqual(len(obs), 2)
        wr10 = obs["TS_GP_BOP_WR_M.WR0010"]
        self.assertEqual([o[0] for o in wr10], ["1972-07-31", "2026-07-31"],
                         "observations should be sorted ascending")
        self.assertAlmostEqual(wr10[-1][1], 3631.044564)
        wr20 = obs["TS_GP_BOP_WR_M.WR0020"]
        self.assertAlmostEqual(wr20[-1][1], 1234.5, msg="thousands separator not parsed")
        self.assertIsNone(wr20[0][1], "suppressed '-' should become NULL, not 0")
        self.assertEqual(wr20[0][2], "Not available")

    def test_filters_narrow_selection(self):
        self._catalog()
        fake = FakeHTTP(DATASETS, SERIES)
        urllib.request.urlopen = fake
        B.cmd_observations(self.args(subject_area=["Public Finance"]))
        self.assertTrue((B.OBS_DIR / "TS_GP_PF_ZAKATRCT_Y.json").exists())
        self.assertFalse((B.OBS_DIR / "TS_GP_BOP_WR_M.json").exists())
        self.assertEqual(len([c for c in fake.calls if "/series/" in c]), 1)

    def test_resume_skips_completed_series(self):
        self._catalog()
        fake = FakeHTTP(DATASETS, SERIES)
        urllib.request.urlopen = fake
        B.cmd_observations(self.args(all=True))
        n = len([c for c in fake.calls if "/series/" in c])
        B.cmd_observations(self.args(all=True))
        self.assertEqual(len([c for c in fake.calls if "/series/" in c]), n,
                         "resume re-fetched already-stored series")

    def test_budget_exhaustion_is_recoverable(self):
        self._catalog()
        fake = FakeHTTP(DATASETS, SERIES)
        urllib.request.urlopen = fake
        cp = B.load_checkpoint()
        B.record_spend(cp, 1899)          # one request left today
        B.save_json(B.CHECKPOINT, cp)
        B.cmd_observations(self.args(all=True))
        cp = json.loads(B.CHECKPOINT.read_text())
        self.assertEqual(len(cp["obs_done"]), 1, "should have stopped after one series")
        # next 'day'
        B.record_spend(cp, 0)
        cp["spend"] = {}
        B.save_json(B.CHECKPOINT, cp)
        B.cmd_observations(self.args(all=True))
        cp = json.loads(B.CHECKPOINT.read_text())
        self.assertEqual(len(cp["obs_done"]), 3, "resume did not finish the remainder")


class TestLoad(Base):
    def test_load_builds_duckdb(self):
        urllib.request.urlopen = FakeHTTP(DATASETS, SERIES)
        B.cmd_catalog(self.args())
        B.cmd_observations(self.args(all=True))
        B.cmd_load(self.args())

        import duckdb
        con = duckdb.connect(str(B.WAREHOUSE / "data_darbar.duckdb"))
        try:
            n_cat = con.execute("SELECT count(*) FROM sbp_series_catalog").fetchone()[0]
            n_obs = con.execute("SELECT count(*) FROM sbp_observations").fetchone()[0]
            self.assertEqual(n_cat, 3)
            self.assertEqual(n_obs, 5)
            v = con.execute("""
                SELECT value FROM sbp_series
                WHERE series_key='TS_GP_BOP_WR_M.WR0010' AND obs_date='1972-07-31'
            """).fetchone()[0]
            self.assertAlmostEqual(v, 9.5)
            subj = con.execute("""
                SELECT DISTINCT subject_area FROM sbp_series_catalog ORDER BY 1
            """).fetchall()
            self.assertEqual([s[0] for s in subj],
                             ["External Sector", "Public Finance"])
            nulls = con.execute(
                "SELECT count(*) FROM sbp_observations WHERE value IS NULL").fetchone()[0]
            self.assertEqual(nulls, 1, "suppressed observation should persist as NULL")

            # FY labelling: EasyData stamps FY2024-25 as 2025-06-30.
            fy = con.execute("""
                SELECT fiscal_year FROM sbp_annual_fy
                WHERE series_key='TS_GP_PF_ZAKATRCT_Y.Z001'
            """).fetchone()[0]
            self.assertEqual(fy, "2024-25")
            # monthly series must not get an FY label
            mfy = con.execute("""
                SELECT DISTINCT fiscal_year FROM sbp_annual_fy
                WHERE series_key='TS_GP_BOP_WR_M.WR0010'
            """).fetchall()
            self.assertEqual(mfy, [(None,)])
        finally:
            con.close()

    def test_load_is_rerunnable(self):
        """`load` must be idempotent.

        Regression: the first run created sbp_series_catalog as a TABLE, and the
        second run's `DROP VIEW IF EXISTS sbp_series_catalog` raised
        "Existing object ... is of type Table, trying to drop type View".
        So load worked once and failed forever after.
        """
        urllib.request.urlopen = FakeHTTP(DATASETS, SERIES)
        B.cmd_catalog(self.args())
        B.cmd_observations(self.args(all=True))
        B.cmd_load(self.args())
        B.cmd_load(self.args())          # must not raise
        B.cmd_load(self.args())

        import duckdb
        con = duckdb.connect(str(B.WAREHOUSE / "data_darbar.duckdb"))
        try:
            self.assertEqual(con.execute("SELECT count(*) FROM sbp_observations").fetchone()[0], 5)
            self.assertEqual(con.execute("SELECT count(*) FROM sbp_series_catalog").fetchone()[0], 3)
            # and the relation types are still what downstream expects
            kinds = dict(con.execute(
                "SELECT view_name, 'view' FROM duckdb_views() WHERE NOT internal "
                "UNION ALL SELECT table_name, 'table' FROM duckdb_tables()").fetchall())
            self.assertEqual(kinds.get("sbp_series_catalog"), "table")
            self.assertEqual(kinds.get("sbp_observations"), "view")
        finally:
            con.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
