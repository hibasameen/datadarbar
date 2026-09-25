"""Reviewed census source corrections, applied after the legacy aggregation.

The adjacent dated JSON retains complete original rows, PDF locations and hashes.
Only the verified source rows and their dependent website fields are changed.
Run directly to repair districts.json, then inline it and rebuild the district
warehouse table. The full and education-only builders also call this module.
"""
from __future__ import annotations

import argparse
import copy
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
SOURCE = HERE / "census_corrections/2026-09-25.json"
LEVELS = ("below_primary", "primary", "middle", "matric", "intermediate", "graduate", "masters_above")
CORE = ("total", "never_attended") + LEVELS
RATE_NUMERATORS = {"pct_" + name: (name,) for name in LEVELS + ("never_attended",)}
RATE_NUMERATORS["pct_matric_plus"] = ("matric", "intermediate", "graduate", "masters_above")


def source_counts(entry):
    counts = entry["counts"].copy()
    if any(type(v) is not int or v < 0 for v in counts.values()):
        raise ValueError("Correction counts must be nonnegative integers")
    if counts["total"] <= 0 or sum(v for k, v in counts.items() if k != "total") != counts["total"]:
        raise ValueError("Correction source row does not reconcile")
    if entry["year"] == "2023":
        counts["graduate"] = counts["graduate_2yr"] + counts["graduate_4yr"]
        counts["masters_above"] = counts["masters"] + counts["mphil_phd"]
    if not set(CORE) <= counts.keys():
        raise ValueError("Incomplete education correction")
    return counts


def rate(record, year, numerator):
    prefix = f"t_edu_{year}_"
    values = [record.get(prefix + name) for name in numerator]
    total = record.get(prefix + "total")
    if not total or any(v is None for v in values):
        return None
    return 100 * sum(values) / total


def education_diffs(record):
    for field in list(record):
        if field.startswith("t_edu_diff_"):
            del record[field]
    for field, old in list(record.items()):
        if field.startswith("t_edu_2017_"):
            suffix = field.removeprefix("t_edu_2017_")
            new = record.get("t_edu_2023_" + suffix)
            if old is not None and new is not None:
                record["t_edu_diff_" + suffix] = round(new - old, 4)


def combined_karachi_education(data):
    """Existing shared-area comparison, using each education table's denominator."""
    child, parent = data["keamari"], data["karachi west"]
    for record in (child, parent):
        for field in list(record):
            if field.startswith("t_edu_diff_"):
                del record[field]
    combined = {}
    for name in CORE:
        a, b = child.get("t_edu_2023_" + name), parent.get("t_edu_2023_" + name)
        if a is None or b is None:
            raise ValueError("Cannot recompute the shared Karachi education comparison from incomplete counts")
        combined["t_edu_2023_" + name] = a + b
        old = parent.get("t_edu_2017_" + name)
        if old is not None:
            for record in (child, parent):
                record["t_edu_diff_" + name] = round(a + b - old, 4)
    for indicator, numerator in RATE_NUMERATORS.items():
        old, new = rate(parent, "2017", numerator), rate(combined, "2023", numerator)
        if old is not None and new is not None:
            for record in (child, parent):
                record["t_edu_diff_" + indicator] = round(new - old, 4)
    child["t_edu_boundary_change"] = "karachi west"
    parent["t_edu_boundary_change"] = "keamari"


def corrected(data):
    """Return a corrected copy; fail before mutation if required data is absent."""
    source = json.loads(SOURCE.read_text())
    result = copy.deepcopy(data)
    touched = set()
    for entry in source["education"]:
        key, year = entry["district_key"], entry["year"]
        record = result.get(key)
        prefix = f"t_edu_{year}_"
        if record is None or not record.get(prefix + "total"):
            raise ValueError(f"Required census row absent: {key}/{year}")
        counts = source_counts(entry)
        for field in list(record):
            if field.startswith(prefix + "pct_"):
                del record[field]
        # Keep the established public schema; full source categories remain in
        # the correction evidence and the separate Stage 1 source-native release.
        names = CORE + (("diploma_certificate",) if year == "2017" else ())
        record.update({prefix + name: counts[name] for name in names})
        for indicator, numerator in RATE_NUMERATORS.items():
            record[prefix + indicator] = round(rate(record, year, numerator), 2)
        touched.add(key)
    for key in touched:
        education_diffs(result[key])
    combined_karachi_education(result)
    if "chitral" not in result or "t1_2023_pop_transgender" not in result["chitral"]:
        raise ValueError("Chitral population record missing")
    symbols = source["chitral_missing_population"]
    if len(symbols) != 2 or any(x["raw_value"] != "-" for x in symbols):
        raise ValueError("Chitral missing-value evidence changed")
    result["chitral"]["t1_2023_pop_transgender"] = None
    result["chitral"].pop("t1_diff_pop_transgender", None)
    return result


def apply_verified_census_corrections(data):
    result = corrected(data)
    data.clear()
    data.update(result)


def changes(before, after):
    return [dict(district=key, field=field, before=before[key].get(field), after=after[key].get(field),
                 action="removed" if field not in after[key] else "added" if field not in before[key] else "updated")
            for key in sorted(before) for field in sorted(set(before[key]) | set(after[key]))
            if before[key].get(field) != after[key].get(field) or (field in before[key]) != (field in after[key])]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--districts", type=Path, default=HERE.parent / "app/data/districts.json")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    before = json.loads(args.districts.read_text())
    after = corrected(before)
    diff = changes(before, after)
    if args.report:
        args.report.write_text(json.dumps(diff, indent=2) + "\n")
    if args.write:
        args.districts.write_text(json.dumps(after, ensure_ascii=False, indent=2) + "\n")
    print(f"{len(diff)} changed fields across {len({r['district'] for r in diff})} districts; "
          + ("written" if args.write else "dry run"))
