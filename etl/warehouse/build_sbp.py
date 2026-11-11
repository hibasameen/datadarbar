#!/usr/bin/env python3
"""
SBP EasyData -> Data Darbar warehouse.

Pulls the State Bank of Pakistan EasyData catalogue and observations into
DuckDB/Parquet alongside trade_hs8, national_accounts and budget_lines.

    https://easydata.sbp.org.pk/apex/f?p=10:210     (catalogue)
    https://easydata.sbp.org.pk/api/v1/...          (API, docs under Help > Developer Guide)

WHY IT LOOKS LIKE THIS
----------------------
The API has no bulk-data endpoint: one HTTP request per *series*, and the
account is capped at 2,000 requests/day and (documented) 250/hour. Observed
behaviour on 2026-08-16 is tighter still -- a burst of ~35 requests in ~30s
returned HTTP 429, recovering after ~50s. So this script is built around
being interrupted and resumed rather than around finishing in one go:

  * a token-bucket paced at --rate requests/minute (default 4/min = 240/hr,
    just under the documented hourly cap and far under the burst threshold)
  * adaptive exponential backoff on 429
  * a JSON checkpoint after every request, so Ctrl-C / crash / daily-cap exit
    all resume exactly where they left off
  * a hard --daily-budget stop so it can be driven by a daily cron/scheduled task

One request returns a series' entire history when start_date/end_date are
given (verified: 649 monthly obs back to Jul-1972 in a single call), so cost
is flat at 1 request per series regardless of how long the series is.

USAGE
-----
    export SBP_API_KEY=...                    # never hardcode; rotates every 90 days
    python3 build_sbp.py catalog              # ~226 requests, ~1 hour at default rate
    python3 build_sbp.py plan                 # cost the observation pull, no requests
    python3 build_sbp.py observations --subject-area "External Sector"
    python3 build_sbp.py observations --all --daily-budget 1900
    python3 build_sbp.py load                 # parquet + attach into data_darbar.duckdb

Re-running `observations` only fetches series not already complete, and with
--refresh only fetches observations newer than what is already stored.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import OrderedDict
from datetime import date, datetime
from pathlib import Path

BASE = "https://easydata.sbp.org.pk/api/v1"
PREFIX = "TS_GP_"

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
from dd_paths import WAREHOUSE
STATE_DIR = WAREHOUSE / "sbp_state"
CATALOG_JSON = STATE_DIR / "series_catalog.json"
OBS_DIR = STATE_DIR / "obs"          # one .json per dataset, keeps memory flat
CHECKPOINT = STATE_DIR / "checkpoint.json"

# Earliest plausible observation in EasyData is 1947 (exchange rates from Aug-1947).
EARLIEST = "1900-01-01"

# ---------------------------------------------------------------- rate limiting


class RateLimiter:
    """Token bucket + adaptive backoff.

    `rate_per_min` is the sustained rate. `burst` lets a small number of
    requests go out back-to-back after an idle period, which is safe because
    the observed burst threshold is ~35.
    """

    def __init__(self, rate_per_min: float = 4.0, burst: int = 3):
        self.interval = 60.0 / rate_per_min
        self.burst = max(1, burst)
        self.allowance = float(self.burst)
        self.last = time.monotonic()
        self.penalty = 0.0        # extra seconds added after a 429
        self.n_429 = 0

    def wait(self) -> None:
        while True:
            now = time.monotonic()
            self.allowance += (now - self.last) / self.interval
            self.last = now
            if self.allowance > self.burst:
                self.allowance = float(self.burst)
            if self.allowance >= 1.0:
                self.allowance -= 1.0
                if self.penalty:
                    time.sleep(self.penalty)
                return
            time.sleep((1.0 - self.allowance) * self.interval)

    def on_429(self) -> float:
        """Back off exponentially; returns the sleep taken."""
        self.n_429 += 1
        nap = min(300.0, 30.0 * (2 ** (self.n_429 - 1))) * (0.8 + 0.4 * random.random())
        # permanently slow down a little after repeated throttling
        self.penalty = min(10.0, self.penalty + 1.0)
        self.allowance = 0.0
        time.sleep(nap)
        return nap

    def on_success(self) -> None:
        if self.n_429:
            self.n_429 = max(0, self.n_429 - 1)
        if self.penalty:
            self.penalty = max(0.0, self.penalty - 0.25)


class BudgetExhausted(Exception):
    pass


class Client:
    def __init__(self, api_key: str, limiter: RateLimiter, daily_budget: int,
                 spent: int = 0, timeout: int = 60, verbose: bool = True):
        self.api_key = api_key
        self.limiter = limiter
        self.daily_budget = daily_budget
        self.spent = spent
        self.timeout = timeout
        self.verbose = verbose

    def get(self, path: str, params: dict | None = None, _tries: int = 0) -> dict:
        if self.spent >= self.daily_budget:
            raise BudgetExhausted(f"daily budget of {self.daily_budget} requests reached")
        q = dict(params or {})
        q["api_key"] = self.api_key
        url = f"{BASE}/{path}?{urllib.parse.urlencode(q)}"
        self.limiter.wait()
        req = urllib.request.Request(url, headers={"Accept": "application/json",
                                                   "User-Agent": "data-darbar/1.0"})
        try:
            self.spent += 1
            with urllib.request.urlopen(req, timeout=self.timeout) as r:
                payload = decode_payload(r.read())
            self.limiter.on_success()
            return payload
        except urllib.error.HTTPError as e:
            if e.code == 429 and _tries < 6:
                nap = self.limiter.on_429()
                if self.verbose:
                    print(f"    429 throttled, slept {nap:.0f}s, retrying", flush=True)
                return self.get(path, params, _tries + 1)
            if e.code == 401:
                raise SystemExit(
                    "401 Unauthorized -- SBP_API_KEY is missing, wrong, or expired.\n"
                    "Keys last 90 days: regenerate at My Data Basket > My Account."
                )
            if e.code in (500, 502, 503, 504) and _tries < 3:
                time.sleep(5 * (_tries + 1))
                return self.get(path, params, _tries + 1)
            raise
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as e:
            if _tries < 3:
                time.sleep(5 * (_tries + 1))
                return self.get(path, params, _tries + 1)
            raise RuntimeError(f"{path}: {e}") from e


# ---------------------------------------------------------------- parsing

def repair_inner_quotes(text: str) -> str:
    """Escape unescaped double quotes that appear *inside* EasyData string values.

    SBP emits status comments like:
        "The observation status is missing "MIS" due to trimmed inflation ..."
    The inner quotes are not escaped, which no conforming JSON parser accepts
    (and `strict=False` does not help — that only relaxes control characters).
    Seen on the CPI moving-average series, e.g. TS_GP_PT_CPI_M.P00161516.

    A quote inside a string is treated as the real terminator only when the next
    non-space character is one of , ] } : — otherwise it is literal and gets
    escaped. That is enough for this feed, where the corruption is confined to
    the free-text final column.
    """
    out, in_str, esc = [], False, False
    i, n = 0, len(text)
    while i < n:
        ch = text[i]
        if not in_str:
            out.append(ch)
            if ch == '"':
                in_str = True
        elif esc:
            out.append(ch)
            esc = False
        elif ch == "\\":
            out.append(ch)
            esc = True
        elif ch == '"':
            j = i + 1
            while j < n and text[j] in " \t\r\n":
                j += 1
            if j < n and text[j] in ",]}:":
                out.append('"')
                in_str = False
            else:
                out.append('\\"')
        else:
            out.append(ch)
        i += 1
    return "".join(out)


def decode_payload(raw: bytes) -> dict:
    """Parse an EasyData response, working around two ways the feed emits bad JSON.

    1. Raw control characters inside string literals (TS_GP_RL_CCSIND_M fails with
       "Bad control character in string literal" around position 39,602).
       `strict=False` tolerates those.
    2. Unescaped double quotes inside string values (the CPI moving-average
       series) — see repair_inner_quotes.

    Escalating through the repairs keeps a malformed response from costing four
    wasted requests and a dead dataset.
    """
    text = raw.decode("utf-8", "replace")
    try:
        return json.loads(text, strict=False)
    except json.JSONDecodeError:
        pass
    cleaned = "".join(" " if ch < " " else ch for ch in text)
    try:
        return json.loads(cleaned, strict=False)
    except json.JSONDecodeError:
        return json.loads(repair_inner_quotes(cleaned), strict=False)


def rows_to_dicts(payload: dict) -> list[dict]:
    """EasyData returns {"columns":[...], "rows":[[...], ...]}. Normalise keys."""
    cols = payload.get("columns") or []
    keys = [c.strip().lower().replace(" ", "_").replace("/", "_").replace("(", "")
            .replace(")", "").replace(".", "") for c in cols]
    out = []
    for row in payload.get("rows") or []:
        d = dict(zip(keys, row))
        if len(row) > len(keys):          # defensive: unexpected extra columns
            d["_extra"] = row[len(keys):]
        out.append(d)
    return out


def parse_num(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "")
    if s in ("", "-", "--", "NA", "N/A", "null", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


_DATE_FORMATS = ("%Y-%m-%d", "%d-%b-%Y", "%d-%B-%Y", "%Y/%m/%d", "%d/%m/%Y")


def parse_date(v):
    """EasyData mixes ISO (data endpoint) and DD-Mon-YYYY (meta endpoint)."""
    if v is None:
        return None
    s = str(v).strip()
    if not s or s in ("-", "NA"):
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    # bare year, e.g. "2024"
    if s.isdigit() and len(s) == 4:
        return f"{s}-12-31"
    return None


# ---------------------------------------------------------------- state

def load_json(path: Path, default):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except json.JSONDecodeError:
            print(f"warning: {path.name} is corrupt, ignoring", file=sys.stderr)
    return default


def save_json(path: Path, obj) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False))
    tmp.replace(path)          # atomic, so a kill mid-write can't corrupt state


def today_str() -> str:
    return date.today().isoformat()


def load_checkpoint() -> dict:
    cp = load_json(CHECKPOINT, {})
    cp.setdefault("catalog_done", [])
    cp.setdefault("catalog_failed", {})
    cp.setdefault("obs_done", [])
    cp.setdefault("obs_failed", {})
    cp.setdefault("spend", {})
    return cp


def spend_today(cp: dict) -> int:
    return int(cp.get("spend", {}).get(today_str(), 0))


def record_spend(cp: dict, n: int) -> None:
    cp.setdefault("spend", {})[today_str()] = n
    # keep the ledger small
    for k in sorted(cp["spend"])[:-14]:
        del cp["spend"][k]


def dataset_codes() -> list[str]:
    seed = load_json(HERE / "sbp_dataset_codes.json", None)
    if not seed:
        raise SystemExit("sbp_dataset_codes.json missing next to build_sbp.py")
    prefix = seed.get("prefix", PREFIX)
    return [prefix + c if not c.startswith(prefix) else c for c in seed["codes"]]


def get_key() -> str:
    key = os.environ.get("SBP_API_KEY", "").strip()
    if not key:
        raise SystemExit(
            "Set SBP_API_KEY first, e.g.\n"
            "    export SBP_API_KEY=your_key_here\n"
            "Generate one at https://easydata.sbp.org.pk -> My Data Basket -> My Account."
        )
    return key


def make_client(args, cp) -> Client:
    already = spend_today(cp)
    limiter = RateLimiter(rate_per_min=args.rate, burst=args.burst)
    return Client(get_key(), limiter, daily_budget=args.daily_budget, spent=already)


# ---------------------------------------------------------------- phase: catalog

def cmd_catalog(args) -> None:
    cp = load_checkpoint()
    client = make_client(args, cp)
    codes = dataset_codes()
    catalog = load_json(CATALOG_JSON, {})
    done = set(cp["catalog_done"])
    if args.refresh:
        done, catalog = set(), {}

    todo = [c for c in codes if c not in done]
    print(f"catalog: {len(done)} done, {len(todo)} to fetch "
          f"(spent {client.spent}/{client.daily_budget} today)")

    try:
        for i, code in enumerate(todo, 1):
            try:
                payload = client.get(f"dataset/{code}/meta")
            except BudgetExhausted:
                raise
            except Exception as e:
                cp["catalog_failed"][code] = str(e)[:200]
                print(f"  [{i}/{len(todo)}] {code}: FAILED {e}", file=sys.stderr)
                continue
            series = []
            for d in rows_to_dicts(payload):
                key = d.get("series_code") or d.get("series_key")
                if not key:
                    continue
                series.append({
                    "series_key": key,
                    "dataset_code": code,
                    "dataset_name": d.get("reference_dataset_name"),
                    "series_name": d.get("series_name"),
                    "series_short_name": d.get("series_short_name"),
                    "description": d.get("series_description"),
                    "frequency": d.get("data_frequency"),
                    "unit": d.get("unit"),
                    "variable_type": d.get("variable_type"),
                    "available_since": parse_date(d.get("available_since")),
                    "available_upto": parse_date(d.get("available_upto")),
                    "last_refresh": parse_date(d.get("last_refresh_date")),
                })
            catalog[code] = series
            done.add(code)
            cp["catalog_done"] = sorted(done)
            cp["catalog_failed"].pop(code, None)
            record_spend(cp, client.spent)
            save_json(CATALOG_JSON, catalog)
            save_json(CHECKPOINT, cp)
            print(f"  [{i}/{len(todo)}] {code}: {len(series)} series", flush=True)
    except (BudgetExhausted, KeyboardInterrupt) as e:
        print(f"\nstopping: {e or 'interrupted'} -- rerun to resume", file=sys.stderr)
    finally:
        record_spend(cp, client.spent)
        save_json(CHECKPOINT, cp)

    total = sum(len(v) for v in catalog.values())
    print(f"\ncatalog: {len(catalog)}/{len(codes)} datasets, {total:,} series")
    if cp["catalog_failed"]:
        print(f"failed: {len(cp['catalog_failed'])} -- {list(cp['catalog_failed'])[:5]}")


# ---------------------------------------------------------------- selection

SUBJECT_BY_PREFIX = OrderedDict([
    ("ES_", "External Sector"), ("BOP_", "External Sector"), ("ER_", "External Sector"),
    ("FI_", "External Sector"), ("EXT_", "External Sector"),
    ("IR_", "Interest Rates"),
    ("BAM_", "Monetary and Financial Sector"), ("MFS_", "Monetary and Financial Sector"),
    ("BS_", "Monetary and Financial Sector"),
    ("PDL_", "Pakistan's Debt Profile"), ("ED_", "Pakistan's Debt Profile"),
    ("PKDP_", "Pakistan's Debt Profile"),
    ("PF_", "Public Finance"),
    ("RL_", "Real Sector"), ("RLS_", "Real Sector"), ("RS_", "Real Sector"),
    ("PT_", "Real Sector"), ("FSA_", "Real Sector"), ("GA_", "Real Sector"),
])


def subject_area(dataset_code: str) -> str:
    stem = dataset_code[len(PREFIX):] if dataset_code.startswith(PREFIX) else dataset_code
    for pre, subj in SUBJECT_BY_PREFIX.items():
        if stem.startswith(pre):
            return subj
    return "Other"


def flat_catalog() -> list[dict]:
    catalog = load_json(CATALOG_JSON, {})
    out = []
    for code, series in catalog.items():
        for s in series:
            s = dict(s)
            s["subject_area"] = subject_area(code)
            out.append(s)
    return out


def apply_selection_file(args, rows: list[dict]) -> list[dict] | None:
    """Resolve --select/--tier against sbp_selection.json.

    Each tier is a list of entries, any of:
        {"dataset": "TS_GP_X"}                      -> every series in it
        {"dataset": "TS_GP_X", "include": "regex"}  -> matching series names
        {"series": ["TS_GP_X.Y", ...]}              -> exact keys
    """
    if not getattr(args, "select", None):
        return None
    path = Path(args.select)
    if not path.is_absolute():
        path = HERE / path
    spec = load_json(path, None)
    if spec is None:
        raise SystemExit(f"selection file not found: {path}")

    tiers = spec.get("tiers", {})
    wanted_tiers = args.tier or list(tiers)
    unknown = [t for t in wanted_tiers if t not in tiers]
    if unknown:
        raise SystemExit(f"unknown tier(s) {unknown}; available: {list(tiers)}")

    by_key = {r["series_key"]: r for r in rows}
    by_dataset: dict[str, list[dict]] = {}
    for r in rows:
        by_dataset.setdefault(r["dataset_code"], []).append(r)

    picked: dict[str, dict] = {}
    missing: list[str] = []
    for tname in wanted_tiers:
        for entry in tiers[tname].get("entries", []):
            if "series" in entry:
                for k in entry["series"]:
                    if k in by_key:
                        picked[k] = by_key[k]
                    else:
                        missing.append(k)
                continue
            code = entry["dataset"]
            if not code.startswith(PREFIX):
                code = PREFIX + code
            members = by_dataset.get(code)
            if not members:
                missing.append(code)
                continue
            if entry.get("include"):
                rx = re.compile(entry["include"], re.I)
                members = [m for m in members
                           if rx.search((m.get("series_name") or ""))]
            if entry.get("exclude"):
                rx = re.compile(entry["exclude"], re.I)
                members = [m for m in members
                           if not rx.search((m.get("series_name") or ""))]
            for m in members:
                picked[m["series_key"]] = m

    if missing:
        print(f"warning: {len(missing)} selection entries not in the catalogue "
              f"(is `catalog` complete?): {missing[:5]}", file=sys.stderr)
    return list(picked.values())


def select_series(args, rows: list[dict]) -> list[dict]:
    from_file = apply_selection_file(args, rows)
    if from_file is not None:
        rows = from_file
    sel = rows
    if args.subject_area:
        want = {s.lower() for s in args.subject_area}
        sel = [r for r in sel if r["subject_area"].lower() in want]
    if args.dataset:
        want = {d.upper() for d in args.dataset}
        want |= {PREFIX + d.upper() for d in args.dataset}
        sel = [r for r in sel if r["dataset_code"].upper() in want]
    if args.frequency:
        want = {f.lower() for f in args.frequency}
        sel = [r for r in sel if (r.get("frequency") or "").lower().split(",")[0] in want]
    if args.match:
        needles = [m.lower() for m in args.match]
        sel = [r for r in sel
               if any(n in ((r.get("series_name") or "") + " " +
                            (r.get("dataset_name") or "")).lower() for n in needles)]
    if args.since:
        sel = [r for r in sel if (r.get("available_upto") or "") >= args.since]
    return sel


def cmd_plan(args) -> None:
    rows = flat_catalog()
    if not rows:
        raise SystemExit("no catalogue yet -- run `build_sbp.py catalog` first")
    cp = load_checkpoint()
    done = set(cp["obs_done"])
    sel = select_series(args, rows)
    todo = [r for r in sel if r["series_key"] not in done]

    by_subj: dict[str, list] = {}
    for r in sel:
        by_subj.setdefault(r["subject_area"], []).append(r)
    print(f"{'subject area':<32}{'series':>9}{'to fetch':>10}")
    print("-" * 51)
    for subj in sorted(by_subj, key=lambda s: -len(by_subj[s])):
        n = len(by_subj[subj])
        nt = sum(1 for r in by_subj[subj] if r["series_key"] not in done)
        print(f"{subj:<32}{n:>9,}{nt:>10,}")
    print("-" * 51)
    print(f"{'TOTAL':<32}{len(sel):>9,}{len(todo):>10,}")
    days = (len(todo) + args.daily_budget - 1) // max(1, args.daily_budget)
    hours = len(todo) / max(1e-9, args.rate * 60)
    print(f"\nat {args.rate}/min and {args.daily_budget}/day: "
          f"~{hours:.1f} hours of runtime, ~{days} day(s) of API budget")


# ---------------------------------------------------------------- phase: observations

def obs_path(dataset_code: str) -> Path:
    return OBS_DIR / f"{dataset_code}.json"


def cmd_observations(args) -> None:
    rows = flat_catalog()
    if not rows:
        raise SystemExit("no catalogue yet -- run `build_sbp.py catalog` first")
    if not (args.all or args.subject_area or args.dataset or args.match or args.select):
        raise SystemExit("refusing to fetch everything implicitly: pass --all or a filter "
                         "(--select / --subject-area / --dataset / --match). "
                         "Run `plan` first to cost it.")

    cp = load_checkpoint()
    client = make_client(args, cp)
    done = set(cp["obs_done"])
    sel = select_series(args, rows)
    todo = [r for r in sel if args.refresh or r["series_key"] not in done]
    print(f"observations: {len(sel):,} selected, {len(todo):,} to fetch "
          f"(spent {client.spent}/{client.daily_budget} today)")

    OBS_DIR.mkdir(parents=True, exist_ok=True)
    buffers: dict[str, dict] = {}

    def flush(code: str) -> None:
        if code in buffers:
            save_json(obs_path(code), buffers[code])

    try:
        for i, r in enumerate(todo, 1):
            code, key = r["dataset_code"], r["series_key"]
            if code not in buffers:
                buffers[code] = load_json(obs_path(code), {})
            params = {"start_date": args.start or EARLIEST,
                      "end_date": args.end or today_str()}
            try:
                payload = client.get(f"series/{key}/data", params)
            except BudgetExhausted:
                raise
            except Exception as e:
                cp["obs_failed"][key] = str(e)[:200]
                print(f"  [{i}/{len(todo)}] {key}: FAILED {e}", file=sys.stderr)
                continue

            obs = []
            for d in rows_to_dicts(payload):
                dt = parse_date(d.get("observation_date"))
                if dt is None:
                    continue
                obs.append([dt,
                            parse_num(d.get("observation_value")),
                            d.get("observation_status") or None,
                            d.get("status_comments") or None])
            obs.sort(key=lambda x: x[0])
            buffers[code][key] = obs
            done.add(key)
            cp["obs_done"] = sorted(done)
            cp["obs_failed"].pop(key, None)
            record_spend(cp, client.spent)

            if i % 25 == 0 or i == len(todo):
                flush(code)
                save_json(CHECKPOINT, cp)
                print(f"  [{i}/{len(todo)}] {key}: {len(obs)} obs "
                      f"(spent {client.spent}/{client.daily_budget})", flush=True)
            else:
                flush(code)
                save_json(CHECKPOINT, cp)
    except (BudgetExhausted, KeyboardInterrupt) as e:
        print(f"\nstopping: {e or 'interrupted'} -- rerun to resume", file=sys.stderr)
    finally:
        for code in buffers:
            flush(code)
        record_spend(cp, client.spent)
        save_json(CHECKPOINT, cp)
        print(f"done for now: {len(cp['obs_done']):,} series stored, "
              f"{len(cp['obs_failed'])} failed")


# ---------------------------------------------------------------- phase: load

def _drop_relation(con, name: str) -> None:
    """Drop `name` whether it is a table or a view.

    `DROP VIEW IF EXISTS x` is not a no-op when x exists as a TABLE -- DuckDB
    raises "Existing object x is of type Table, trying to drop type View".
    So the naive drop-view-then-drop-table pair works the first time (nothing
    exists) and fails on every re-run. Look up the actual type instead.
    """
    kind = con.execute(
        "SELECT 'view' FROM duckdb_views() WHERE view_name = ? AND NOT internal "
        "UNION ALL SELECT 'table' FROM duckdb_tables() WHERE table_name = ?",
        [name, name]).fetchone()
    if kind:
        con.execute(f"DROP {kind[0].upper()} IF EXISTS {name}")


CAT_COLUMNS = ["series_key", "dataset_code", "dataset_name", "subject_area",
               "series_name", "series_short_name", "frequency", "unit",
               "variable_type", "available_since", "available_upto",
               "last_refresh", "description"]

OBS_COLUMNS = ["series_key", "dataset_code", "obs_date", "value", "status", "comment"]


def cmd_load(args) -> None:
    # DuckDB writes the parquet itself rather than going through pandas.to_parquet.
    # That path needs pyarrow at a version pandas approves of, and on a conda base
    # env pip and the interpreter can disagree about which pyarrow is installed
    # (pip reporting 23.x while `import pyarrow` gets 11.x, which pandas 3 rejects).
    # DuckDB is already a hard dependency here and has its own parquet writer, so
    # this drops pandas and pyarrow from the requirements entirely.
    import duckdb

    rows = flat_catalog()
    if not rows:
        raise SystemExit("no catalogue yet -- run `build_sbp.py catalog` first")

    cat_rows = [tuple(r.get(c) for c in CAT_COLUMNS) for r in rows]

    obs_rows = []
    for path in sorted(OBS_DIR.glob("*.json")) if OBS_DIR.exists() else []:
        code = path.stem
        for key, obs in load_json(path, {}).items():
            for dt, val, status, comment in obs:
                obs_rows.append((key, code, dt, val, status, comment))

    os.chdir(WAREHOUSE)
    cat_pq = WAREHOUSE / "sbp_series_catalog.parquet"
    obs_pq = WAREHOUSE / "sbp_observations.parquet"

    w = duckdb.connect()
    try:
        w.execute("CREATE TABLE cat (" + ", ".join(f'"{c}" VARCHAR' for c in CAT_COLUMNS) + ")")
        if cat_rows:
            w.executemany(f"INSERT INTO cat VALUES ({','.join('?' * len(CAT_COLUMNS))})", cat_rows)
        # dates land as DATE, values as DOUBLE, so the parquet types match what
        # query.html expects rather than everything arriving as text
        w.execute("""CREATE TABLE obs (series_key VARCHAR, dataset_code VARCHAR,
                     obs_date DATE, value DOUBLE, status VARCHAR, comment VARCHAR)""")
        if obs_rows:
            w.executemany("INSERT INTO obs VALUES (?,?,?,?,?,?)", obs_rows)
        for col in ("available_since", "available_upto", "last_refresh"):
            w.execute(f'ALTER TABLE cat ALTER "{col}" TYPE DATE')
        w.execute(f"COPY cat TO '{cat_pq.name}' (FORMAT PARQUET)")
        w.execute(f"COPY obs TO '{obs_pq.name}' (FORMAT PARQUET)")
        n_cat = w.execute("SELECT count(*) FROM cat").fetchone()[0]
        n_obs = w.execute("SELECT count(*) FROM obs").fetchone()[0]
    finally:
        w.close()

    print(f"wrote {cat_pq.name}: {n_cat:,} series")
    print(f"wrote {obs_pq.name}: {n_obs:,} observations")

    # The parquet-backed view uses a relative path so the warehouse folder stays
    # portable, and DuckDB resolves it at CREATE VIEW time -- so we must be
    # inside the folder here (os.chdir already done above, same reason
    # finalize.py chdirs).
    db = WAREHOUSE / "data_darbar.duckdb"
    con = duckdb.connect(str(db))
    try:
        for name in ("sbp_annual_fy", "sbp_series", "sbp_observations",
                     "sbp_series_catalog"):
            _drop_relation(con, name)
        con.execute("CREATE TABLE sbp_series_catalog AS "
                    f"SELECT * FROM read_parquet('{cat_pq.as_posix()}')")
        # observations stay parquet-backed: the file will outgrow the 20MB
        # device_commit cap that forced the same choice for trade_hs8.
        con.execute("CREATE VIEW sbp_observations AS "
                    "SELECT * FROM read_parquet('sbp_observations.parquet')")
        con.execute("""
            CREATE VIEW sbp_series AS
            SELECT o.series_key, c.dataset_name, c.subject_area, c.series_name,
                   c.frequency, c.unit, o.obs_date, o.value, o.status
            FROM sbp_observations o
            JOIN sbp_series_catalog c USING (series_key)
        """)
        # EasyData stamps a fiscal year with its END date: FY2023-24 -> 2024-06-30.
        # Verified against national_accounts: GDP at constant basic prices 2015-16
        # for 2023-24 is 39,777,011.25 Mn PKR in both sources.
        # Only June-ending annual observations get an FY label; annual series on a
        # calendar basis (population, literacy, ...) are deliberately left NULL.
        _drop_relation(con, "sbp_annual_fy")
        con.execute("""
            CREATE VIEW sbp_annual_fy AS
            SELECT *,
                   CASE WHEN frequency ILIKE 'Annual%'
                             AND month(obs_date) = 6 AND day(obs_date) = 30
                        THEN (year(obs_date) - 1) || '-' ||
                             right(CAST(year(obs_date) AS VARCHAR), 2)
                   END AS fiscal_year
            FROM sbp_series
        """)
        n = con.execute("SELECT count(*) FROM sbp_observations").fetchone()[0]
        print(f"duckdb: sbp_series_catalog (table), sbp_observations (view, {n:,} rows), "
              f"sbp_series (joined view)")
    finally:
        con.close()
    print(f"\nNOTE: open duckdb from inside {WAREHOUSE.name}/ -- "
          "sbp_observations is a relative-path parquet view, like trade_hs8.")


# ---------------------------------------------------------------- cli

def main(argv=None) -> None:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    def common(sp):
        sp.add_argument("--rate", type=float, default=4.0,
                        help="sustained requests per minute (default 4 = 240/hr)")
        sp.add_argument("--burst", type=int, default=3, help="burst allowance")
        sp.add_argument("--daily-budget", type=int, default=1900,
                        help="stop after N requests today (account cap is 2000)")

    def filters(sp):
        sp.add_argument("--subject-area", nargs="+")
        sp.add_argument("--dataset", nargs="+")
        sp.add_argument("--frequency", nargs="+")
        sp.add_argument("--match", nargs="+", help="substring match on series/dataset name")
        sp.add_argument("--since", help="only series with data at/after YYYY-MM-DD")
        sp.add_argument("--select", nargs="?", const="sbp_selection.json",
                        help="curated selection file (default sbp_selection.json)")
        sp.add_argument("--tier", nargs="+",
                        help="tiers within --select (default: all of them)")

    sp = sub.add_parser("catalog", help="fetch series metadata for all datasets")
    common(sp)
    sp.add_argument("--refresh", action="store_true", help="re-fetch from scratch")
    sp.set_defaults(func=cmd_catalog)

    sp = sub.add_parser("plan", help="cost an observation pull without making requests")
    common(sp)
    filters(sp)
    sp.set_defaults(func=cmd_plan)

    sp = sub.add_parser("observations", help="fetch observations for selected series")
    common(sp)
    filters(sp)
    sp.add_argument("--all", action="store_true", help="every series in the catalogue")
    sp.add_argument("--refresh", action="store_true", help="re-fetch even if stored")
    sp.add_argument("--start", help="start_date (default 1900-01-01)")
    sp.add_argument("--end", help="end_date (default today)")
    sp.set_defaults(func=cmd_observations)

    sp = sub.add_parser("load", help="write parquet and attach into data_darbar.duckdb")
    sp.set_defaults(func=cmd_load)

    args = p.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
