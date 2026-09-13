"""Re-parse Census 2023 Table 13 (educational attainment, t_edu_2023_*) into
app/data/districts.json without a full build_dataset.py run.

WHY. `_parse_edu_hierarchical_format` used to key its "already read" set on
the crosswalked district, so for the five districts the boundary file has
not split (Chitral, Kohistan, Kalat, Killa Abdullah, Loralai) only the first
successor's block was read: one successor's counts sat over a combined
population, and every t_edu_2023_pct_* for those districts was that
successor's rate. The parser now sums successors. A full build re-reads every
microdata layer, which is hours for a fix that touches one table; this runs
only the Table 13 loader, derives the percentages the same way the full
build does (compute_education_pcts), and merges the result.

Usage:
    python etl/rebuild_education_2023.py                  # reads "<repo>/../PBS data/Census 2023/census2023_all_tables/table_13"
    python etl/rebuild_education_2023.py --raw <dir>      # or point at the folder holding table_13_*.csv
    ... [--districts PATH] [--dry-run]

Then:
    python etl/inline_districts.py
    python etl/build_web_warehouse.py --src <desktop warehouse>
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_dataset import (PBS, compute_education_pcts,  # noqa: E402
                           load_education_2023_raw, _BOUNDARY_PAIRS, _MERGED_DISTRICTS)

PREFIX = "t_edu_2023_"
DEFAULT_RAW = PBS / "Census 2023" / "census2023_all_tables" / "table_13"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raw", type=Path, default=DEFAULT_RAW,
                    help="folder holding the per-province table_13_*.csv files")
    ap.add_argument("--districts", type=Path,
                    default=Path(__file__).resolve().parent.parent / "app" / "data" / "districts.json")
    ap.add_argument("--dry-run", action="store_true", help="report, do not write")
    args = ap.parse_args()

    if not args.raw.exists() or not any(args.raw.glob("table_13_*.csv")):
        sys.exit(f"no table_13_*.csv under {args.raw} — pass --raw")

    parsed = load_education_2023_raw(args.raw)
    districts = json.loads(args.districts.read_text())

    changed, orphans = [], []
    for key, counts in parsed.items():
        rec = districts.get(key)
        if rec is None:
            orphans.append(key)
            continue
        fresh = {f: v for f, v in counts.items() if v is not None}
        if all(rec.get(f) == v for f, v in fresh.items()):
            continue   # counts unchanged: leave the record, its percentages and diffs alone
        if key in _BOUNDARY_PAIRS or key in _BOUNDARY_PAIRS.values():
            # Keamari / Karachi West difference against a shared 2017 area
            # (_fix_boundary_change_pairs); that needs the full build.
            print(f"  {key}: counts changed but this district is a boundary pair — run build_dataset.py")
            continue
        before_total = rec.get(f"{PREFIX}total")
        # Drop the old counts and derived percentages for this year, then
        # write the fresh counts and derive again.
        for f in list(rec):
            if f.startswith(PREFIX):
                del rec[f]
        rec.update(fresh)
        compute_education_pcts(rec, "2023")
        # The 2017-2023 diffs for this family follow the 2023 side.
        for f in [f for f in rec if f.startswith("t_edu_diff_")]:
            del rec[f]
        for k17 in [f for f in rec if f.startswith("t_edu_2017_")]:
            suffix = k17.split("_2017_", 1)[1]
            k23 = f"{PREFIX}{suffix}"
            if rec.get(k17) is not None and rec.get(k23) is not None:
                rec[f"t_edu_diff_{suffix}"] = round(rec[k23] - rec[k17], 4)
        changed.append((key, before_total, rec.get(f"{PREFIX}total")))

    print(f"parsed {len(parsed)} districts; {len(changed)} changed")
    for key, b, a in sorted(changed):
        tag = "  (merged)" if key in _MERGED_DISTRICTS else ""
        print(f"  {key:<22} total {b!s:>12} -> {a!s:>12}{tag}")
    if orphans:
        print(f"  {len(orphans)} parsed keys have no entry in districts.json: "
              + ", ".join(sorted(orphans)))

    if args.dry_run:
        return
    args.districts.write_text(json.dumps(districts, indent=2) + "\n")
    print(f"wrote {args.districts}")
    print("next: python etl/inline_districts.py && python etl/build_web_warehouse.py --src <desktop warehouse>")


if __name__ == "__main__":
    main()
