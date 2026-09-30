#!/usr/bin/env python3
"""Versioned Sindh Police public crime-report extracts -> local CSV/JSON/Parquet.

The build never invents annual coverage from report titles. Full-year observations
require explicit 1 January–31 December dates plus is_full_year=true. Original
source observations remain separate from the selected annual analytical table.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import subprocess
import sys
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

WORKSPACE = Path(__file__).resolve().parents[3]
DEFAULT_RAW = WORKSPACE / "raw_data" / "sindh_police"
DEFAULT_OUT = WORKSPACE / "data_darbar_warehouse" / "sindh_police"
MISSING = {"", "-", "--", "—", "–", "n/a", "na", "null", "none"}

# Reviewed spelling-only aliases. Substantive category reclassification must be
# supplied in an extract rather than guessed from similar crime descriptions.
ALIASES = {
    "t_o_t_a_l": "total", "grand_total": "total",
    "motorcycle_theft": "motor_cycle_theft",
    "motorcycle_snatched": "motor_cycle_snatched",
    "motor_cycle_snatching": "motor_cycle_snatched",
    "attempt_to_murder": "attempt_to_murder",
    "prohibition_ordinance": "prohibition_ord",
    "non_fatal": "non_fatal", "nonfatal": "non_fatal",
    "fatal_accidents": "fatal", "non_fatal_accidents": "non_fatal",
    "grevious_hurt": "grievous_hurt", "other_roberry": "other_robbery",
    "reeceiving_stolen_property": "receiving_stolen_property_s_411_ppc",
    "receiving_stolen_property": "receiving_stolen_property_s_411_ppc",
    "other_local_andf_special_laws": "other_local_and_special_laws",
}

OBS_FIELDS = {
    "observation_id": "string", "report_id": "string", "source_snapshot_sha256": "string",
    "source_snapshot_path": "string", "source_url": "string", "source_type": "string",
    "extraction_method": "string", "source_priority": "int64", "reporting_year": "int64",
    "year": "int64", "period_start": "string", "period_end": "string",
    "period_label_raw": "string", "is_full_year": "bool", "comparison_column": "string",
    "geography_level": "string", "geography_name": "string", "geography_key": "string",
    "crime_category_raw": "string", "category_group": "string", "crime_category_key": "string",
    "row_type": "string", "metric": "string", "value_raw": "string", "cases_reported": "int64",
    "source_page": "int64", "source_row": "string", "source_column": "string",
    "validation_status": "string", "selection_status": "string",
    "selection_eligible": "bool", "source_document_path": "string", "source_document_sha256": "string",
    "source_archive_url": "string", "evidence_path": "string", "evidence_sha256": "string",
    "observation_metadata_json": "string",
}
SOURCE_FIELDS = {
    "report_id": "string", "title": "string", "source_url": "string", "reporting_year": "int64",
    "period_start": "string", "period_end": "string", "is_full_year": "bool",
    "source_type": "string", "extraction_method": "string", "source_priority": "int64",
    "source_snapshot_path": "string", "source_snapshot_sha256": "string",
    "retrieved_at": "string", "observation_count": "int64", "category_partition_complete": "bool",
    "selection_eligible": "bool", "source_document_path": "string", "source_document_sha256": "string",
    "source_archive_url": "string", "source_metadata_json": "string",
    "source_snapshot_aliases_json": "string",
}


def slug(text):
    return re.sub(r"[^a-z0-9]+", "_", str(text).lower()).strip("_")


def category_key(label, group=""):
    key = slug(label)
    return ALIASES.get(key, key)


def geography_key(name, level):
    key = slug(name)
    key = re.sub(r"_(range|province)$", "", key)
    if key in {"s_b_abad", "s_b_abad_range", "shaheed_benazirabad", "shaheed_benazir_abad", "sba"}:
        key = "shaheed_benazirabad"
    return f"{level}:{key}"


def digest(data):
    return hashlib.sha256(data).hexdigest()


def timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(data, encoding="utf-8")
    temporary.replace(path)


def parse_cases(value):
    """Return (nullable integer, error). A dash is missing, never imputed zero."""
    if value is None:
        return None, None
    if isinstance(value, bool):
        return None, "boolean_is_not_count"
    s = str(value).strip()
    if s.lower() in MISSING:
        return None, None
    if not re.fullmatch(r"(?:\d+|\d{1,3}(?:,\d{3})+)", s):
        return None, "invalid_nonnegative_integer"
    return int(s.replace(",", "")), None


def iso_date(value):
    try:
        return date.fromisoformat(str(value)).isoformat()
    except (TypeError, ValueError):
        return None


def integer(value):
    if isinstance(value, bool):
        return None
    try:
        s = str(value)
        return int(s) if re.fullmatch(r"\d+", s) else None
    except (TypeError, ValueError):
        return None


def annual_period(year, start, end, declared_full):
    return bool(year and declared_full is True and start == f"{year}-01-01"
                and end == f"{year}-12-31")


def load_reports(raw):
    reports, diagnostics = [], []
    seen = {}
    imported_origins = {}
    if not raw.exists():
        return reports, diagnostics
    manifest = raw / "manifest.jsonl"
    if manifest.exists():
        for line in manifest.read_text(encoding="utf-8").splitlines():
            try:
                entry = json.loads(line)
                if entry.get("original_local_path"):
                    imported_origins[entry["sha256"]] = str(Path(entry["original_local_path"]).parent)
            except (ValueError, KeyError):
                continue
    # Raw web responses also use JSON; only the explicit report contract is built.
    for path in sorted(raw.rglob("*.json"), key=lambda pp: ("extracts" in pp.relative_to(raw).parts, str(pp))):
        if "objects" in path.relative_to(raw).parts:
            continue
        try:
            payload = path.read_bytes()
            obj = json.loads(payload)
        except (OSError, ValueError) as exc:
            diagnostics.append({"path": str(path), "issue": "unreadable_json", "detail": str(exc)})
            continue
        if not isinstance(obj, dict) or "observations" not in obj or not obj.get("report_id"):
            continue
        if not isinstance(obj["observations"], list):
            diagnostics.append({"path": str(path), "issue": "observations_not_list"})
            continue
        obj = dict(obj)
        obj["_path"] = str(path.resolve())
        obj["_hash"] = digest(payload)
        if "extracts" in path.relative_to(raw).parts and obj["_hash"] in imported_origins:
            obj["_evidence_base"] = imported_origins[obj["_hash"]]
        if obj["_hash"] in seen:
            seen[obj["_hash"]].setdefault("_snapshot_aliases", []).append(obj["_path"])
            continue
        seen[obj["_hash"]] = obj
        reports.append(obj)
    return reports, diagnostics


def normalise_reports(reports):
    observations, sources, issues = [], [], []
    for report in reports:
        report_id = str(report["report_id"])
        snapshot_hash = report["_hash"]
        integrity_issues = []
        evidence_base = Path(report.get("_evidence_base", Path(report["_path"]).parent))
        declared_assets = []
        document = report.get("source_document_path", report.get("source_pdf_path"))
        document_hash = report.get("source_document_sha256", report.get("source_pdf_sha256"))
        if document:
            declared_assets.append(("source_document", document, document_hash))
        for evidence in report.get("raw_evidence", []) + report.get("source_snapshots", []):
            if isinstance(evidence, dict) and evidence.get("path"):
                declared_assets.append(("raw_evidence", evidence["path"], evidence.get("sha256")))
        for kind, location, expected_hash in declared_assets:
            asset = Path(location)
            if not asset.is_absolute():
                asset = evidence_base / asset
            if not asset.is_file():
                integrity_issues.append(f"{kind}_missing")
            elif not expected_hash:
                integrity_issues.append(f"{kind}_checksum_missing")
            elif digest(asset.read_bytes()) != expected_hash:
                integrity_issues.append(f"{kind}_checksum_mismatch")
        integrity_issues = sorted(set(integrity_issues))
        base = {
            "report_id": report_id, "source_snapshot_sha256": snapshot_hash,
            "source_snapshot_path": report["_path"], "source_url": report.get("source_url"),
            "source_type": report.get("source_type", "unspecified_extract"),
            "extraction_method": report.get("extraction_method", "unspecified"),
            "source_priority": integer(report.get("source_priority", 0)) or 0,
            "reporting_year": integer(report.get("reporting_year")),
            "selection_eligible": report.get("selection_eligible", True) is True,
            "source_document_path": report.get("source_document_path", report.get("source_pdf_path")),
            "source_document_sha256": report.get("source_document_sha256", report.get("source_pdf_sha256")),
            "source_archive_url": report.get("source_archive_url", report.get("archive_url")),
            "source_metadata_json": json.dumps({k: v for k, v in report.items() if k != "observations" and not k.startswith("_")}, ensure_ascii=False, sort_keys=True),
        }
        sources.append({**base, "title": report.get("title"),
                        "period_start": iso_date(report.get("period_start")),
                        "period_end": iso_date(report.get("period_end")),
                        "is_full_year": report.get("is_full_year") is True,
                        "retrieved_at": report.get("retrieved_at"),
                        "observation_count": len(report["observations"]),
                        "source_snapshot_aliases_json": json.dumps(report.get("_snapshot_aliases", [])),
                        "category_partition_complete": report.get("category_partition_complete") is True})
        for index, raw in enumerate(report["observations"]):
            row_issues = list(integrity_issues)
            if not isinstance(raw, dict):
                issues.append({"report_id": report_id, "source_row": index + 1, "issue": "observation_not_object"})
                continue
            year = integer(raw.get("year", report.get("reporting_year")))
            start = iso_date(raw.get("period_start", report.get("period_start")))
            end = iso_date(raw.get("period_end", report.get("period_end")))
            full = annual_period(year, start, end, raw.get("is_full_year", report.get("is_full_year")))
            if not year or not start or not end or start > end:
                row_issues.append("invalid_or_missing_period")
            if raw.get("is_full_year", report.get("is_full_year")) is True and not full:
                row_issues.append("full_year_dates_mismatch")
            raw_value = raw.get("value_raw", raw.get("cases_reported"))
            value, error = parse_cases(raw_value)
            if error:
                row_issues.append(error)
            if "cases_reported" in raw:
                supplied, supplied_error = parse_cases(raw["cases_reported"])
                if supplied_error or supplied != value:
                    row_issues.append("raw_parsed_count_mismatch")
            label = str(raw.get("crime_category_raw", "")).strip()
            group = str(raw.get("category_group") or "")
            geo = str(raw.get("geography_name", "")).strip()
            level = raw.get("geography_level", "unknown")
            row_type = raw.get("row_type", "detail")
            if not label or not geo:
                row_issues.append("missing_category_or_geography")
            if row_type not in ("detail", "subtotal", "total"):
                row_issues.append("invalid_row_type")
            page = integer(raw.get("source_page"))
            if page is not None and page < 1:
                row_issues.append("source_page_must_be_one_based")
            supplied_key = raw.get("crime_category_key")
            key = ALIASES.get(supplied_key, supplied_key) if supplied_key else category_key(label, group)
            evidence = raw.get("recovery_evidence_file")
            evidence_path = (evidence_base / evidence) if evidence else None
            if evidence_path and not evidence_path.exists():
                row_issues.append("missing_recovery_evidence_file")
            row = {**base, "observation_id": digest(f"{snapshot_hash}:{index}".encode()),
                   "year": year, "period_start": start, "period_end": end,
                   "period_label_raw": raw.get("period_label_raw", report.get("period_label_raw")),
                   "is_full_year": full, "comparison_column": raw.get("comparison_column"),
                   "geography_level": level, "geography_name": geo,
                   "geography_key": raw.get("geography_key") or geography_key(geo, level),
                   "crime_category_raw": label, "category_group": group,
                   "crime_category_key": str(key), "row_type": row_type,
                   "metric": "cases_reported", "value_raw": None if raw_value is None else str(raw_value),
                   "cases_reported": value, "source_page": page,
                   "source_row": str(raw.get("source_row", raw.get("source_row_index", index + 1))),
                   "source_column": str(raw.get("source_column", raw.get("comparison_column", ""))),
                   "validation_status": "ok" if not row_issues else "|".join(row_issues),
                   "selection_status": "not_full_year" if not full else "not_selected",
                   "evidence_path": str(evidence_path.resolve()) if evidence_path else None,
                   "evidence_sha256": digest(evidence_path.read_bytes()) if evidence_path and evidence_path.exists() else None,
                   "observation_metadata_json": json.dumps(raw, ensure_ascii=False, sort_keys=True)}
            observations.append(row)
            for issue in row_issues:
                issues.append({"observation_id": row["observation_id"], "report_id": report_id,
                               "source_row": row["source_row"], "issue": issue})
    return observations, sources, issues


def cohort_checks(observations):
    """Annual completeness belongs to a report edition, even when split into files."""
    cohorts = defaultdict(list)
    for row in observations:
        if row["is_full_year"]:
            cohorts[(row["source_url"], row["reporting_year"], row["year"])].append(row)
    checks = []
    expected_geographies = {"province:sindh", "range:karachi", "range:hyderabad", "range:mirpurkhas",
                            "range:shaheed_benazirabad", "range:sukkur", "range:larkana"}
    for (url, edition, year), rows in sorted(cohorts.items(), key=lambda kv: str(kv[0])):
        by_geo = defaultdict(list)
        for row in rows:
            by_geo[row["geography_key"]].append(row)
        key_sets = [{r["crime_category_key"] for r in rr} for rr in by_geo.values()]
        complete = (set(by_geo) == expected_geographies and all(len(s) == 45 for s in key_sets)
                    and all(s == key_sets[0] for s in key_sets)
                    and all(len(rr) == 45 and sum(r["row_type"] == "total" for r in rr) == 1
                            and sum(r["row_type"] == "detail" for r in rr) == 44 for rr in by_geo.values())
                    and all(r["validation_status"] == "ok" for r in rows))
        declared = all(r["selection_eligible"] for r in rows)
        if not complete or not declared:
            for row in rows:
                row["selection_eligible"] = False
        checks.append({"source_url": url, "reporting_year": edition, "year": year,
                       "complete_45_rows_by_7_geographies": complete, "selection_eligible": complete and declared,
                       "rows": len(rows), "geographies": sorted(by_geo),
                       "categories_per_geography": {g: len({r["crime_category_key"] for r in rr}) for g, rr in sorted(by_geo.items())}})
    return checks


def fact_key(row):
    return (row["year"], row["period_start"], row["period_end"], row["geography_key"],
            row["crime_category_key"], row["row_type"], row["metric"])


def select_annual(observations, start_year, end_year):
    """Priority is explicit in report metadata. Equal-priority disagreements fail closed."""
    groups, conflicts, selected = defaultdict(list), [], []
    for row in observations:
        if row["validation_status"] != "ok":
            row["selection_status"] = "invalid_observation"
        elif not row["selection_eligible"]:
            row["selection_status"] = "incomplete_or_ineligible_source_cohort"
        elif row["is_full_year"] and start_year <= row["year"] <= end_year:
            groups[fact_key(row)].append(row)
        elif row["is_full_year"]:
            row["selection_status"] = "outside_requested_years"
    for key, candidates in sorted(groups.items()):
        priority = max(r["source_priority"] for r in candidates)
        best = [r for r in candidates if r["source_priority"] == priority]
        values = {r["cases_reported"] for r in best}
        if len(values) > 1:
            conflicts.append({"key": list(key), "source_priority": priority,
                              "observations": [{"observation_id": r["observation_id"],
                                                "report_id": r["report_id"], "value": r["cases_reported"]} for r in best]})
            for row in candidates:
                row["selection_status"] = "conflicting_preferred_sources" if row in best else "lower_priority"
            continue
        chosen = sorted(best, key=lambda r: (r["report_id"], r["source_snapshot_sha256"], r["source_row"]))[0]
        for row in candidates:
            row["selection_status"] = ("selected" if row is chosen else
                                       "same_value_alternative" if row in best else "lower_priority")
        selected.append(dict(chosen))
    return selected, conflicts


def total_checks(observations, reports):
    checks = []
    by_hash = {r["_hash"]: r for r in reports}
    groups = defaultdict(list)
    for row in observations:
        groups[(row["source_snapshot_sha256"], row["year"], row["period_start"], row["period_end"], row["geography_key"])].append(row)
    for (source_hash, year, start, end, geo), rows in sorted(groups.items(), key=lambda v: str(v[0])):
        report = by_hash[source_hash]
        base = {"report_id": report["report_id"], "source_snapshot_sha256": source_hash,
                "year": year, "period_start": start, "period_end": end, "geography_key": geo}
        if report.get("category_partition_complete") is True:
            totals = [r for r in rows if r["row_type"] == "total"]
            details = [r for r in rows if r["row_type"] == "detail"]
            keys = [r["crime_category_key"] for r in details]
            if len(totals) != 1 or not details or len(set(keys)) != len(keys) or any(r["cases_reported"] is None or r["validation_status"] != "ok" for r in totals + details):
                checks.append({**base, "check": "category_details_sum_to_total", "status": "incomplete"})
            else:
                calculated = sum(r["cases_reported"] for r in details)
                stated = totals[0]["cases_reported"]
                checks.append({**base, "check": "category_details_sum_to_total", "status": "pass" if calculated == stated else "mismatch",
                               "reported": stated, "calculated": calculated, "difference": calculated - stated})
        else:
            checks.append({**base, "check": "category_details_sum_to_total", "status": "not_asserted",
                           "reason": "extract does not declare a complete disjoint category partition"})
        for rule in report.get("validation_rules", []):
            lookup = defaultdict(list)
            for row in rows:
                lookup[row["crime_category_key"]].append(row)
            needed = [rule["target"]] + rule["components"]
            if any(len(lookup[k]) != 1 or lookup[k][0]["cases_reported"] is None for k in needed):
                checks.append({**base, "check": rule.get("name", rule["target"]), "status": "incomplete"})
                continue
            stated = lookup[rule["target"]][0]["cases_reported"]
            calculated = sum(lookup[k][0]["cases_reported"] for k in rule["components"])
            checks.append({**base, "check": rule.get("name", rule["target"]), "status": "pass" if stated == calculated else "mismatch",
                           "reported": stated, "calculated": calculated, "difference": calculated - stated})
    return checks


def geography_checks(observations):
    groups, checks = defaultdict(list), []
    for row in observations:
        groups[(row["source_url"], row["reporting_year"], row["year"], row["period_start"],
                row["period_end"], row["crime_category_key"])].append(row)
    for key, rows in sorted(groups.items(), key=lambda kv: str(kv[0])):
        province = [r for r in rows if r["geography_key"] == "province:sindh"]
        ranges = [r for r in rows if r["geography_level"] == "range"]
        if len(province) != 1 or len(ranges) != 6 or len({r["geography_key"] for r in ranges}) != 6:
            continue
        if any(r["cases_reported"] is None or r["validation_status"] != "ok" for r in province + ranges):
            continue
        calculated = sum(r["cases_reported"] for r in ranges)
        stated = province[0]["cases_reported"]
        checks.append({"source_url": key[0], "reporting_year": key[1], "year": key[2],
                       "crime_category_key": key[5], "check": "six_ranges_sum_to_province",
                       "status": "pass" if stated == calculated else "mismatch",
                       "reported": stated, "calculated": calculated, "difference": calculated - stated})
    return checks


def export_table(out, name, rows, fields):
    rows = [{key: row.get(key) for key in fields} for row in rows]
    write_json(out / f"{name}.json", rows)
    with (out / f"{name}.csv").open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(fields))
        writer.writeheader()
        writer.writerows(rows)
    parquet = False
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
        types = {"string": pa.string(), "int64": pa.int64(), "bool": pa.bool_()}
        schema = pa.schema([(key, types[typ]) for key, typ in fields.items()])
        table = pa.Table.from_pylist(rows, schema=schema)
        pq.write_table(table, out / f"{name}.parquet", compression="zstd")
        parquet = True
    except (ImportError, ValueError):
        # A host can have a binary-incompatible Arrow installation. DuckDB's
        # native writer provides the same explicitly typed Parquet without pandas.
        try:
            import duckdb
            con = duckdb.connect()
            sql_types = {"string": "VARCHAR", "int64": "BIGINT", "bool": "BOOLEAN"}
            columns = ", ".join(f'"{key}" {sql_types[typ]}' for key, typ in fields.items())
            con.execute(f"CREATE TABLE result ({columns})")
            if rows:
                con.executemany(f"INSERT INTO result VALUES ({', '.join('?' for _ in fields)})",
                                [[row[key] for key in fields] for row in rows])
            con.execute("COPY result TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(out / f"{name}.parquet")])
            con.close()
            parquet = True
        except ImportError:
            pass
    return {"name": name, "rows": len(rows), "csv": f"{name}.csv", "json": f"{name}.json",
            "parquet": f"{name}.parquet" if parquet else None,
            "columns": [{"name": key, "type": typ} for key, typ in fields.items()]}


def build(raw, out, start_year=2019, end_year=2025):
    reports, input_issues = load_reports(raw)
    observations, sources, issues = normalise_reports(reports)
    cohorts = cohort_checks(observations)
    selected, conflicts = select_annual(observations, start_year, end_year)
    counts = Counter((r["source_snapshot_sha256"],) + fact_key(r) for r in observations)
    duplicates = [{"key": list(key), "count": n} for key, n in counts.items() if n > 1]
    totals = total_checks(observations, reports)
    geo_totals = geography_checks(observations)
    geographies = sorted({r["geography_key"] for r in observations})
    category_union = {g: {r["crime_category_key"] for r in observations if r["geography_key"] == g} for g in geographies}
    coverage = []
    for year in range(start_year, end_year + 1):
        for geo in geographies or ["province:sindh_province"]:
            rows = [r for r in selected if r["year"] == year and r["geography_key"] == geo]
            present = {r["crime_category_key"] for r in rows}
            coverage.append({"year": year, "geography_key": geo, "preferred_rows": len(rows),
                             "preferred_nonmissing_values": sum(r["cases_reported"] is not None for r in rows),
                             "has_reported_total": any(r["row_type"] == "total" and r["cases_reported"] is not None for r in rows),
                             "missing_categories_relative_to_observed_union": sorted(category_union.get(geo, set()) - present)})
    covered = sorted({r["year"] for r in selected if r["cases_reported"] is not None})
    out.mkdir(parents=True, exist_ok=True)
    tables = [export_table(out, "sindh_crime_observations", observations, OBS_FIELDS),
              export_table(out, "sindh_crime_annual", selected, OBS_FIELDS),
              export_table(out, "sindh_crime_sources", sources, SOURCE_FIELDS)]
    quality = {
        "requested_years": list(range(start_year, end_year + 1)), "covered_years": covered,
        "missing_years": sorted(set(range(start_year, end_year + 1)) - set(covered)),
        "source_reports": len(sources), "source_observations": len(observations), "preferred_annual_observations": len(selected),
        "input_issues": input_issues, "observation_issues": issues,
        "duplicate_source_facts": duplicates, "preferred_source_conflicts": conflicts,
        "total_checks": totals, "coverage": coverage,
        "source_cohort_checks": cohorts,
        "geography_total_checks": geo_totals,
        "selected_category_total_checks": total_checks(selected, reports),
        "selected_geography_total_checks": geography_checks(selected),
        "parquet_written": all(t["parquet"] is not None for t in tables),
        "notes": [
            "Missing categories are relative to the union recovered at each geography, not an assertion that the source published every category every year.",
            "Full-year eligibility requires explicit matching calendar-year dates and is_full_year=true; missing counts remain null.",
            "Police ranges, regions and province rows overlap geographically. Do not sum geography levels.",
            "Category subtotal/total rows overlap detail rows. Do not sum all row types.",
            "Search-index extracts are preserved snapshots of indexed official reports, not verified downloads of original PDF bytes.",
            "Equal-priority source disagreements are retained in observations and withheld from preferred annual rows until an explicit priority resolves them.",
            "Preferred annual selection requires a complete report edition with 45 matching category rows (44 detail plus total) across Sindh province and six police ranges. Partial report editions stay in source observations only.",
        ],
    }
    write_json(out / "quality_report.json", quality)
    write_json(out / "catalog.json", {"name": "Sindh Police reported crime", "version": 1,
                                     "requested_years": [start_year, end_year], "tables": tables,
                                     "quality_report": "quality_report.json", "notes": quality["notes"]})
    print(json.dumps({"output": str(out.resolve()), "reports": len(sources), "observations": len(observations),
                      "annual_rows": len(selected), "covered_years": covered, "missing_years": quality["missing_years"],
                      "conflicts": len(conflicts), "total_mismatches": sum(c["status"] == "mismatch" for c in totals),
                      "parquet": quality["parquet_written"]}, indent=2))
    return quality


def artifact_kind(data):
    stripped = data.lstrip()
    if stripped.startswith(b"%PDF-"):
        return "pdf"
    if stripped[:500].lower().startswith((b"<!doctype html", b"<html")) or b"<html" in stripped[:500].lower():
        return "html"
    try:
        json.loads(data)
        return "json"
    except (ValueError, UnicodeError):
        return "bin"


def archive(raw, data, source_url, status=200, content_type=None, expected_pdf=False, original_local_path=None):
    kind = artifact_kind(data)
    sha = digest(data)
    target = raw / "objects" / f"{sha}.{kind}"
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        if digest(target.read_bytes()) != sha:
            raise ValueError(f"Cached artifact checksum mismatch: {target}")
    else:
        target.write_bytes(data)
    ok = 200 <= status < 300 and (not expected_pdf or kind == "pdf")
    record = {"source_url": source_url, "retrieved_at": timestamp(), "http_status": status,
              "content_type": content_type, "sha256": sha, "bytes": len(data),
              "path": str(target.resolve()), "kind": kind,
              "status": "downloaded" if ok else "rejected_non_pdf_or_http_error"}
    if original_local_path:
        record["original_local_path"] = str(Path(original_local_path).resolve())
    with (raw / "manifest.jsonl").open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")
    if not ok:
        raise ValueError(f"Download rejected (HTTP {status}, detected {kind}); diagnostic snapshot saved at {target}. No PDF or observations produced.")
    return record


def fetch(raw, url, timeout=45):
    if urllib.parse.urlsplit(url).scheme not in ("http", "https"):
        raise ValueError("Use an explicit HTTP(S) URL; import-file handles local files")
    request = urllib.request.Request(url, headers={"User-Agent": "DataDarbar/1.0 public-report archival", "Accept": "application/pdf,application/json,text/html;q=0.5"})
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            data, status, content_type = response.read(), response.status, response.headers.get("Content-Type")
    except urllib.error.HTTPError as exc:
        data, status, content_type = exc.read(), exc.code, exc.headers.get("Content-Type")
    return archive(raw, data, url, status, content_type,
                   expected_pdf=urllib.parse.urlsplit(url).path.lower().endswith(".pdf"))


def import_file(raw, path, source_url=None):
    data = path.read_bytes()
    record = archive(raw, data, source_url or path.resolve().as_uri(), expected_pdf=path.suffix.lower() == ".pdf",
                     original_local_path=path)
    if record["kind"] == "json":
        obj = json.loads(data)
        if isinstance(obj, dict) and obj.get("report_id") and isinstance(obj.get("observations"), list):
            # Content-addressed imports preserve all versions; they are never overwritten.
            target = raw / "extracts" / f"{record['sha256']}.json"
            target.parent.mkdir(parents=True, exist_ok=True)
            if not target.exists():
                target.write_bytes(data)
    return record


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--raw", type=Path, default=DEFAULT_RAW)
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    commands = ap.add_subparsers(dest="command", required=True)
    bp = commands.add_parser("build", help="Rebuild normalized datasets offline from recovered report JSON")
    bp.add_argument("--start-year", type=int, default=2019)
    bp.add_argument("--end-year", type=int, default=2025)
    commands.add_parser("rebuild", help="Re-extract retained PDF/index evidence, then build all local tables offline")
    fp = commands.add_parser("fetch", help="Archive explicitly supplied public report URLs")
    fp.add_argument("urls", nargs="+")
    fp.add_argument("--timeout", type=int, default=45)
    ip = commands.add_parser("import-file", help="Archive a local PDF or import a structured report JSON")
    ip.add_argument("path", type=Path)
    ip.add_argument("--source-url")
    args = ap.parse_args(argv)
    try:
        if args.command == "build":
            if args.start_year > args.end_year:
                raise ValueError("start-year must be <= end-year")
            result = build(args.raw, args.out, args.start_year, args.end_year)
            return int(bool(result["input_issues"] or result["observation_issues"] or result["duplicate_source_facts"]))
        if args.command == "rebuild":
            extracts = args.out / "recovered_source_extracts"
            here = Path(__file__).resolve().parent
            for script, source_dir in [
                ("extract_archived_pdfs.py", args.raw),
                ("extract_2023_recovery.py", args.raw / "recovery_2019_2022"),
                ("extract_2024_2025_recovery.py", args.raw / "recovery_2023_2025"),
            ]:
                subprocess.run([sys.executable, str(here / script), "--raw-dir", str(source_dir.resolve()),
                                "--out-dir", str(extracts.resolve())], check=True)
            result = build(extracts, args.out)
            return int(bool(result["input_issues"] or result["observation_issues"] or result["duplicate_source_facts"]))
        if args.command == "fetch":
            failures = 0
            for url in args.urls:
                try:
                    print(json.dumps(fetch(args.raw, url, args.timeout), indent=2))
                except (OSError, ValueError) as exc:
                    failures += 1
                    print(str(exc), file=sys.stderr)
            return int(bool(failures))
        print(json.dumps(import_file(args.raw, args.path, args.source_url), indent=2))
        return 0
    except (OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
