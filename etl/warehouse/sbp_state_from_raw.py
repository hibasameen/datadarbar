#!/usr/bin/env python3
"""Rebuild sbp_state/{series_catalog.json, obs/*.json, checkpoint.json} from raw API responses.

Raw files live in sbp_state/raw/ and come in three shapes:
  meta__{DATASET}.json   verbatim /dataset/{code}/meta response
  data__{SERIES}.json    verbatim /series/{key}/data response
  data__{SERIES}__wN.json  windowed responses for one long series (merged on date)
  bulk__{NAME}.json      {"series_key": "<verbatim /data response text>", ...} —
                         produced by the browser bulk-transfer recipe, one entry
                         per series, values are the exact response bodies.
  cond__{NAME}.json      condensed payload for datasets too large to move
                         verbatim through the browser->sandbox channel. Same
                         information, de-duplicated: descriptions and dates are
                         factored into lookup tables and observations packed as
                         "dateIdx:value|status" tokens. See COND_SHAPE below.

Why cond__ exists: the browser->sandbox transfer is hand-copied text, so payload
size is the binding constraint (and every transfer so far has needed a sha256
gate to catch single-character corruption). Verbatim responses repeat the dataset
name and series name on every observation row; factoring those out cut one
dataset from 1.9 MB to 114 KB with no information loss.

Dates are merged per series (windows overlap harmlessly; last write wins on a
date), sorted ascending, and stored in build_sbp.py's obs format
[date, value, status, comment]. Run `build_sbp.py load` afterwards.
"""

import datetime
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import build_sbp as B  # noqa: E402

RAW = B.STATE_DIR / "raw"


def meta_rows_to_series(code: str, payload: dict) -> list[dict]:
    out = []
    for d in B.rows_to_dicts(payload):
        key = d.get("series_code") or d.get("series_key")
        if not key:
            continue
        out.append({
            "series_key": key, "dataset_code": code,
            "dataset_name": d.get("reference_dataset_name"),
            "series_name": d.get("series_name"),
            "series_short_name": d.get("series_short_name"),
            "description": d.get("series_description"),
            "frequency": d.get("data_frequency"),
            "unit": d.get("unit"),
            "variable_type": d.get("variable_type"),
            "available_since": B.parse_date(d.get("available_since")),
            "available_upto": B.parse_date(d.get("available_upto")),
            "last_refresh": B.parse_date(d.get("last_refresh_date")),
        })
    return out


def obs_from_payload(payload: dict) -> dict:
    store = {}
    for d in B.rows_to_dicts(payload):
        dt = B.parse_date(d.get("observation_date"))
        if dt is None:
            continue
        store[dt] = [dt, B.parse_num(d.get("observation_value")),
                     d.get("observation_status") or None,
                     d.get("status_comments") or None]
    return store


COND_SHAPE = """
{
  "ds":         "TS_GP_...",              dataset code
  "dataset_name": "...",                  optional; falls back to DATASET_NAMES
  "descs":      ["...", ...],             unique Series Description values
  "mrows":      [[suffix, series_name, descIdx, frequency, unit,
                  variable_type, since, upto, refresh], ...],
  "dates":      ["2001-06-30", ...],      observation date vocabulary
  "series":     [[suffix, "0:1234.5,1:2345.6|Not available", ...], ...]
}
Each observation token is "dateIdx:value" with an optional "|status" suffix;
an omitted status means 'Normal', matching how the API reports the common case.
"""

# Dataset names for cond__ payloads built before the field was carried through.
DATASET_NAMES = {
    "TS_GP_BAM_ADVDEP_HY":
        "Province-wise Advances and Deposits by Borrowers/Category of Deposit Holders",
}


def _from_int(n: int, dp: int) -> str:
    """Inverse of the browser's scaled-integer encoding, done in exact integer arithmetic."""
    neg, n = n < 0, abs(n)
    s = str(n)
    if dp > 0:
        s = s.rjust(dp + 1, "0")
        s = s[:len(s) - dp] + "." + s[len(s) - dp:]
    return ("-" if neg else "") + s


def _unpack_v2(dp: int, idxs: str, vals: str, stats: str, dates: list):
    """Decode one v2-packed series.

    v2 stores date indices and values as deltas, with values first scaled to
    integers by 10**dp so the arithmetic is exact (no float drift). A '~' token
    is a missing observation: it emits NULL and does not advance the value base,
    so the following delta is still relative to the last real number.
    """
    I = idxs.split(",") if idxs else []
    V = vals.split(",") if vals else []
    status = {}
    for tok in (stats.split(";") if stats else []):
        if ":" in tok:
            pos, _, st = tok.partition(":")
            status[int(pos)] = st

    out, ci, prev, first = [], 0, 0, True
    for i, tok in enumerate(V):
        ci = int(I[i]) if i == 0 else ci + int(I[i])
        dt = B.parse_date(dates[ci])
        st = status.get(i, "Normal")
        if tok == "~":
            out.append([dt, None, st, None])
            continue
        n = int(tok) if first else prev + int(tok)
        prev, first = n, False
        out.append([dt, B.parse_num(_from_int(n, dp)), st, None])
    return out


def _expand_one(ds, name, mrows, series, descs, dates, version=1):
    catalog_rows = []
    for suffix, series_name, di, freq, unit, vtype, since, upto, refresh in mrows:
        catalog_rows.append({
            "series_key": f"{ds}.{suffix}", "dataset_code": ds,
            "dataset_name": name, "series_name": series_name,
            "series_short_name": None,
            "description": descs[di] if di is not None and di < len(descs) else None,
            "frequency": freq, "unit": unit, "variable_type": vtype,
            "available_since": B.parse_date(since),
            "available_upto": B.parse_date(upto),
            "last_refresh": B.parse_date(refresh),
        })

    # NB: a pack may carry catalogue rows for series whose observations were never
    # fetched (they encode as an empty string). Those must NOT produce an entry
    # here — an empty store would land in checkpoint['obs_done'] and the pipeline
    # would then skip the series forever, silently leaving a hole.
    stores: dict[str, dict] = {}
    for entry in series:
        if version >= 2:
            suffix, dp, idxs, vals, stats = entry
            if not vals:
                continue
            store = stores.setdefault(f"{ds}.{suffix}", {})
            for row in _unpack_v2(dp, idxs, vals, stats, dates):
                if row[0] is not None:
                    store[row[0]] = row
            continue
        suffix, packed = entry
        if not packed:
            continue
        store = stores.setdefault(f"{ds}.{suffix}", {})
        for tok in packed.split(","):
            head, _, status = tok.partition("|")
            idx, _, raw = head.partition(":")
            dt = B.parse_date(dates[int(idx)])
            if dt is None:
                continue
            store[dt] = [dt, B.parse_num(raw), status or "Normal", None]
    return catalog_rows, stores


def read_cond(path: Path) -> tuple[dict[str, list[dict]], dict[str, dict]]:
    """Expand a condensed payload into {dataset_code: catalog rows} + per-series obs.

    Two shapes are accepted. The original single-dataset shape has `ds`/`mrows`/
    `series` at the top level; the later multi-dataset shape shares one `descs`
    and `dates` vocabulary across a `datasets` list, which packs several small
    datasets into one transfer.
    """
    c = B.decode_payload(path.read_bytes())
    descs, dates = c.get("descs", []), c["dates"]
    version = c.get("v", 1)

    if "datasets" in c:
        entries = [(d["ds"], d.get("dataset_name"), d["mrows"], d["series"])
                   for d in c["datasets"]]
    else:
        entries = [(c["ds"], c.get("dataset_name") or DATASET_NAMES.get(c["ds"]),
                    c["mrows"], c["series"])]

    catalogs: dict[str, list[dict]] = {}
    all_stores: dict[str, dict] = {}
    for ds, name, mrows, series in entries:
        rows, stores = _expand_one(ds, name or DATASET_NAMES.get(ds),
                                   mrows, series, descs, dates, version)
        catalogs[ds] = rows
        all_stores.update(stores)
    return catalogs, all_stores


def main() -> None:
    if not RAW.exists():
        raise SystemExit(f"no raw folder at {RAW}")

    catalog: dict[str, list] = {}
    for p in sorted(RAW.glob("meta__*.json")):
        code = p.stem.replace("meta__", "")
        catalog[code] = meta_rows_to_series(code, B.decode_payload(p.read_bytes()))
        print(f"  meta {code}: {len(catalog[code])} series")

    # per-series stores keyed by date, merged across data__ and bulk__ files
    stores: dict[str, dict] = {}

    for p in sorted(RAW.glob("data__*.json")):
        key = p.stem.replace("data__", "").split("__w")[0]
        stores.setdefault(key, {}).update(obs_from_payload(B.decode_payload(p.read_bytes())))

    for p in sorted(RAW.glob("cond__*.json")):
        catalogs, stores_ds = read_cond(p)
        for ds, rows in catalogs.items():
            # a meta__ file carries series_short_name too, so it wins if present
            if ds not in catalog:
                catalog[ds] = rows
        for key, store in stores_ds.items():
            stores.setdefault(key, {}).update(store)
        nobs = sum(len(s) for s in stores_ds.values())
        print(f"  cond {p.name}: {len(catalogs)} dataset(s), "
              f"{sum(len(r) for r in catalogs.values())} series, {nobs:,} obs")

    for p in sorted(RAW.glob("bulk__*.json")):
        bulk = B.decode_payload(p.read_bytes())
        n = 0
        for key, body in bulk.items():
            payload = body if isinstance(body, dict) else B.decode_payload(
                body.encode() if isinstance(body, str) else body)
            stores.setdefault(key, {}).update(obs_from_payload(payload))
            n += 1
        print(f"  bulk {p.name}: {n} series")

    by_ds: dict[str, dict] = {}
    total = 0
    for key, store in stores.items():
        ds = key.split(".")[0]
        obs = sorted(store.values(), key=lambda x: x[0])
        by_ds.setdefault(ds, {})[key] = obs
        total += len(obs)
    for ds, series_map in by_ds.items():
        B.save_json(B.OBS_DIR / f"{ds}.json", series_map)

    B.save_json(B.CATALOG_JSON, catalog)

    cp = B.load_checkpoint()
    cp["catalog_done"] = sorted(set(cp["catalog_done"]) | set(catalog))
    # obs_done is REBUILT, not unioned: raw/ is the source of truth for this script,
    # and it rewrites obs/*.json wholesale. Unioning would let a series that carries
    # no observations stay marked as fetched, and build_sbp.py would skip it forever.
    cp["obs_done"] = sorted(k for k, v in stores.items() if v)
    B.save_json(B.CHECKPOINT, cp)

    ncat = sum(len(v) for v in catalog.values())
    print(f"\nstate: {len(catalog)} datasets ({ncat} catalog series), "
          f"{len(stores)} series with obs, {total:,} observations")
    missing = sorted({k.split('.')[0] for k in stores} - set(catalog))
    if missing:
        print(f"WARNING: obs exist for datasets with no meta (won't join): {missing}")


if __name__ == "__main__":
    main()
