#!/usr/bin/env python3
"""Tests for sbp_state_from_raw.py — the raw -> pipeline-state rebuild.

    python3 test_sbp_state.py

No network, no API key. Covers the v2 scaled-integer delta codec and the
"empty series must not count as fetched" rule, which is the subtle one: a pack
carries catalogue rows for series whose observations were never downloaded, and
marking those as done would make build_sbp.py skip them permanently.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_sbp as B  # noqa: E402
import sbp_state_from_raw as S  # noqa: E402


class TestV2Codec(unittest.TestCase):
    def test_scaled_integer_roundtrip(self):
        for raw, dp in [("59441.24515", 5), ("-4428765.458", 3), ("0", 0),
                        ("278.2380682", 7), ("1.503665069", 9)]:
            neg = raw.startswith("-")
            digits = raw.lstrip("-").replace(".", "")
            frac = raw.split(".")[1] if "." in raw else ""
            scaled = int(digits + "0" * (dp - len(frac)))
            if neg:
                scaled = -scaled
            self.assertEqual(float(S._from_int(scaled, dp)), float(raw), raw)

    def test_delta_decode_matches_browser_fixture(self):
        # captured from the browser encoder on real SBP central-government-debt data
        dates = ["2026-06-30", "2026-05-31", "2026-04-30", "2026-03-31"]
        got = S._unpack_v2(5, "0,1,1,1",
                           "5944124515,-132876546,-1792310,-52287075", "", dates)
        self.assertEqual([r[0] for r in got], dates)
        for row, exp in zip(got, [59441.24515, 58112.47969, 58094.55659, 57571.68584]):
            self.assertAlmostEqual(row[1], exp, places=6)

    def test_missing_value_does_not_advance_the_delta_base(self):
        # '~' emits NULL; the next delta is still relative to the last real number
        got = S._unpack_v2(2, "0,1,1,1", "100,~,5,-5", "1:Missing value",
                           ["2020-01-31", "2020-02-29", "2020-03-31", "2020-04-30"])
        self.assertEqual([r[1] for r in got], [1.0, None, 1.05, 1.0])
        self.assertEqual(got[1][2], "Missing value")
        self.assertEqual(got[0][2], "Normal")

    def test_negative_deltas_and_dp_zero(self):
        got = S._unpack_v2(0, "0,1,1", "10,-3,-3", "", ["2020-01-31", "2020-02-29", "2020-03-31"])
        self.assertEqual([r[1] for r in got], [10.0, 7.0, 4.0])


class TestEmptySeriesRule(unittest.TestCase):
    """A catalogue-only series must not be recorded as fetched."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self._saved = {k: getattr(B, k) for k in
                       ("STATE_DIR", "CATALOG_JSON", "OBS_DIR", "CHECKPOINT")}
        B.STATE_DIR = root / "sbp_state"
        B.CATALOG_JSON = B.STATE_DIR / "series_catalog.json"
        B.OBS_DIR = B.STATE_DIR / "obs"
        B.CHECKPOINT = B.STATE_DIR / "checkpoint.json"
        S.RAW = B.STATE_DIR / "raw"
        S.RAW.mkdir(parents=True)

    def tearDown(self):
        for k, v in self._saved.items():
            setattr(B, k, v)
        self.tmp.cleanup()

    def _pack(self, series):
        return {"v": 2, "descs": ["d"], "dates": ["2020-01-31", "2020-02-29"],
                "datasets": [{"ds": "TS_GP_X", "dataset_name": "X",
                              "mrows": [[s[0], "name", 0, "Monthly", "PKR", "Stock",
                                         "31-Jan-2020", "29-Feb-2020", "01-Mar-2020"]
                                        for s in series],
                              "series": series}]}

    def test_series_with_no_observations_is_not_marked_done(self):
        (S.RAW / "cond__T.json").write_text(json.dumps(self._pack([
            ["HAS", 2, "0,1", "100,5", ""],
            ["EMPTY", 0, "", "", ""],
        ])))
        S.main()
        cp = json.loads(B.CHECKPOINT.read_text())
        self.assertIn("TS_GP_X.HAS", cp["obs_done"])
        self.assertNotIn("TS_GP_X.EMPTY", cp["obs_done"],
                         "empty series marked done -> build_sbp.py would skip it forever")
        # but it must still appear in the catalogue so `plan` can see it as outstanding
        cat = json.loads(B.CATALOG_JSON.read_text())
        self.assertEqual({r["series_key"] for r in cat["TS_GP_X"]},
                         {"TS_GP_X.HAS", "TS_GP_X.EMPTY"})

    def test_obs_done_is_rebuilt_not_unioned(self):
        B.STATE_DIR.mkdir(parents=True, exist_ok=True)
        B.save_json(B.CHECKPOINT, {"catalog_done": [], "catalog_failed": {},
                                   "obs_done": ["TS_GP_X.STALE"], "obs_failed": {},
                                   "spend": {}})
        (S.RAW / "cond__T.json").write_text(json.dumps(self._pack([
            ["HAS", 2, "0,1", "100,5", ""],
        ])))
        S.main()
        cp = json.loads(B.CHECKPOINT.read_text())
        self.assertEqual(cp["obs_done"], ["TS_GP_X.HAS"],
                         "a stale entry survived; raw/ must be the source of truth")


if __name__ == "__main__":
    unittest.main(verbosity=2)
