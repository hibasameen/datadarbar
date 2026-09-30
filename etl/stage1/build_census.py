#!/usr/bin/env python3
"""Offline, source-native population/attainment build. Never writes app/data.

2017: first overall summary in each Table 1/15 PDF, with publication identifiers
from the archived download manifests. 2023: official provincial Table 1/13 PDFs.
Historical local CSV extracts are comparison inputs only.
"""
from __future__ import annotations

import argparse
import ast
import collections
import csv
import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from common import (BASELINE, REPO, WORKSPACE, code_fingerprint, contained,
                    json_bytes, read_csv, sha256, verify_files, write_csv, write_json)

SCHEMA_VERSION = "census-source-v0.1"
EDU = ["total", "never_attended", "below_primary", "primary", "middle", "matric",
       "intermediate", "graduate", "masters_above", "diploma_certificate", "others"]
EDU23 = ["total", "never_attended", "below_primary", "primary", "middle", "matric",
         "intermediate", "graduate_2yr", "graduate_4yr", "masters", "mphil_phd",
         "diploma_certificate", "others"]
POP = ["pop_total", "pop_male", "pop_female", "pop_transgender"]
OBS_FIELDS = ["source_unit_id", "source_dataset", "year", "source_name", "source_code",
              "code_type", "source_province", "source_geography", "indicator", "value",
              "status", "raw_value", "numerator", "denominator", "universe", "unit",
              "source_file", "source_locator", "source_url", "source_status", "validation_status"]


def norm(value):
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def bare_name(value):
    if value.strip().upper() == "ISLAMABAD CAPITAL TERRITORY":
        return "ISLAMABAD"
    return re.sub(r"\s+(DISTRICT|PROTECTED AREA)$", "", value.strip(), flags=re.I)


def number(raw):
    text = str(raw).strip()
    if text in ("", "-", "—", "NA", "N/A"):
        return None
    value = float(text.replace(",", "").replace(" ", ""))
    if not (value >= 0 and value < float("inf") and value.is_integer()):
        raise ValueError(f"Invalid census count: {raw!r}")
    return int(value)


def pdf_text(path, first_page=True):
    return subprocess.run(["pdftotext"] + (["-f", "1", "-l", "1"] if first_page else []) + ["-layout", str(path), "-"],
                          check=True, capture_output=True, text=True, timeout=30).stdout


def parse_2023_pdf(text, table):
    """District summaries only, retaining page/line evidence and printed labels."""
    seen = set()
    district = locality = sex = None
    for page_no, page in enumerate(text.split("\f"), 1):
        lines = page.splitlines()
        for i, line in enumerate(lines):
            label = " ".join(line.split())
            raw = None
            if table == "1":
                parts = line.split()
                if len(parts) < 11 or not all(re.fullmatch(r"[\d,.\-]+", v) for v in parts[-11:]):
                    continue
                label = " ".join(parts[:-11])
                if not label and i > 0:
                    # Long names wrap around a line containing the 11 cells.
                    label = " ".join(lines[i - 1].split())
                    if i + 1 < len(lines) and lines[i + 1].strip() == "DISTRICT":
                        label += " DISTRICT"
                if not re.search(r"\s(DISTRICT|PROTECTED AREA)$", label):
                    continue
                raw = dict(zip(POP, parts[-10:-6]))
            else:
                if re.search(r"\s(DISTRICT|PROTECTED AREA)$", label) or label == "ISLAMABAD CAPITAL TERRITORY":
                    district, locality, sex = label, None, None
                elif re.search(r"\s(TEHSIL|TALUKA|SUB-DIVISION|SUB DIVISION)$", label):
                    district = None
                elif label in ("ALL LOCALITIES", "RURAL", "URBAN", "RURAL LOCALITIES", "URBAN LOCALITIES"):
                    locality = label
                elif label in ("ALL SEXES", "MALE", "FEMALE", "TRANSGENDER"):
                    sex = label
                else:
                    match = re.match(r"^5\s*&\s*ABOVE\s+(.+)$", label)
                    if match and district and locality == "ALL LOCALITIES" and sex == "ALL SEXES":
                        values = match.group(1).split()
                        if len(values) != len(EDU23):
                            raise ValueError(f"Table 13: expected 13 cells at page {page_no}, line {i+1}")
                        label, raw = district, dict(zip(EDU23, values))
            if raw is not None:
                if label in seen:
                    raise ValueError(f"Duplicate PDF district summary: {label}")
                seen.add(label)
                yield label, raw, f"PDF page {page_no}; text line {i + 1}"


def parse_population_pdf(text):
    """First administrative summary, with exactly 11 numeric cells.

    Do not use a broad digit search: table column numbers, rural rows and tehsils
    are not district summaries. Handles wrapped names and frontier-region labels.
    """
    lines = text.splitlines()
    for i, line in enumerate(lines):
        candidates = [line.strip()]
        if i + 1 < len(lines) and not re.search(r"\d", line):
            candidates.append(line.strip() + " " + lines[i + 1].strip())
        for candidate in candidates:
            parts = candidate.split()
            if len(parts) < 12 or not all(re.fullmatch(r"[\d,.\-]+", x) for x in parts[-11:]):
                continue
            label = " ".join(parts[:-11])
            if not (re.search(r"\b(DISTRICT|AGENCY|AREA|ISLAMABAD)\b", label)
                    or label.startswith("FR ")):
                continue
            return dict(zip(POP, parts[-10:-6])), f"PDF page 1; text line {i + 1}", label
    raise ValueError("No unambiguous Table 1 administrative summary")


def parse_education_pdf(text):
    lines = text.splitlines()
    before = []
    for i, line in enumerate(lines, 1):
        match = re.match(r"^\s*5\s+(?:AND|&)\s+ABOVE\s*(.*)$", line, re.I)
        if match:
            if not any(x.strip() == "ALL SEXES" for x in before):
                raise ValueError("Table 15 summary is not under ALL SEXES")
            if not any("OVERALL" in x for x in before):
                raise ValueError("Table 15 summary is not under OVERALL")
            value_text = match.group(1).strip()
            if not value_text and i < len(lines):
                value_text = lines[i].strip()
            values = value_text.split()
            if len(values) != len(EDU):
                raise ValueError(f"Table 15: expected 11 cells, got {len(values)}")
            return dict(zip(EDU, values)), f"PDF page 1; text line {i}"
        before.append(line)
    raise ValueError("No Table 15 overall 5+ row")


def parse_2023(path: Path, table: str):
    """Yield source label, raw indicator cells and physical CSV line number."""
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle)
        first = next(reader)
        if table == "1":
            if first != [str(i) for i in range(1, 13)]:
                raise ValueError(f"Unexpected Table 1 header: {path.name}")
            for row in reader:
                if row and re.search(r"\s(DISTRICT|PROTECTED AREA)$", row[0].strip(), re.I):
                    if len(row) != 12:
                        raise ValueError(f"Wrong Table 1 width in {path.name}:{reader.line_num}")
                    yield row[0].strip(), dict(zip(POP, row[2:6])), reader.line_num
        elif first[0] == "DISTRICT":
            required = ["DISTRICT", "LOCALITY", "SEX", "SEX/ AGE GROUP (IN YEARS)", "TOTAL",
                        "NEVER ATTENDED", "BELOW PRIMARY", "PRIMARY", "MIDDLE", "MATRIC",
                        "INTERMEDIATE", "GRADUATE (2 YEARS)", "GRADUATE (4 YEARS)", "MASTERS",
                        "M.Phil/Ph.D", "DIPLOMA/ CERTIFICATE", "OTHERS"]
            if first[:17] != required:
                raise ValueError(f"Unexpected Sindh Table 13 columns: {path.name}")
            for row in reader:
                if len(row) >= 17 and row[1:3] == ["ALL LOCALITIES", "ALL SEXES"] and re.fullmatch(r"5\s*&\s*ABOVE", row[3].strip()):
                    yield row[0], dict(zip(EDU23, row[4:17])), reader.line_num
        else:
            district = None
            locality = sex = None
            seen = set()
            for row in reader:
                if not row:
                    continue
                label = row[0].strip()
                if re.search(r"\s(DISTRICT|PROTECTED AREA)$", label, re.I):
                    district, locality, sex = label, None, None
                elif re.search(r"\s(TEHSIL|TALUKA|SUB-DIVISION|SUB DIVISION)$", label, re.I):
                    district = None
                elif label in ("ALL LOCALITIES", "RURAL", "URBAN", "RURAL LOCALITIES", "URBAN LOCALITIES"):
                    locality = label
                elif label in ("ALL SEXES", "MALE", "FEMALE", "TRANSGENDER"):
                    sex = label
                elif district and locality == "ALL LOCALITIES" and sex == "ALL SEXES" and re.fullmatch(r"5\s*&\s*ABOVE", label):
                    if district in seen:
                        raise ValueError(f"Duplicate district summary in {path.name}: {district}")
                    if len(row) != 14:
                        raise ValueError(f"Wrong Table 13 width at {path.name}:{reader.line_num}")
                    seen.add(district)
                    yield district, dict(zip(EDU23, row[1:14])), reader.line_num


def observations(meta, raw):
    parsed = {k: number(v) for k, v in raw.items()}
    result = []
    def emit(indicator, value, raw_value, status=None, numerator=None, denominator=None):
        result.append({**meta, "indicator": indicator, "value": value,
                       "status": status or ("observed" if value is not None else "source_symbol_or_blank"),
                       "raw_value": raw_value, "numerator": numerator, "denominator": denominator,
                       "unit": "percent" if indicator.startswith("pct_") else "persons"})
    for k, value in parsed.items():
        emit(k, value, str(raw[k]).strip())
    if meta["source_dataset"].endswith("education"):
        for name, components in [("graduate", ["graduate_2yr", "graduate_4yr"]),
                                 ("masters_above", ["masters", "mphil_phd"])]:
            if name not in parsed:
                parsed[name] = sum(parsed[x] for x in components) if all(parsed.get(x) is not None for x in components) else None
                emit(name, parsed[name], "+".join(components), "derived" if parsed[name] is not None else "missing_component")
        matric = ["matric", "intermediate", "graduate", "masters_above"]
        parsed["matric_plus"] = sum(parsed[x] for x in matric) if all(parsed.get(x) is not None for x in matric) else None
        emit("matric_plus", parsed["matric_plus"], "+".join(matric), "derived" if parsed["matric_plus"] is not None else "missing_component")
        for name in ["never_attended", "matric_plus"]:
            n, d = parsed[name], parsed["total"]
            value = round(n / d * 100, 6) if n is not None and d else None
            emit("pct_" + name, value, f"100*{name}/total", "derived" if value is not None else "missing_component",
                 numerator=n, denominator=d)
    return result


def source_rows(source_root: Path, lock):
    output, checks = [], []
    for table, module in [("01", "population"), ("15", "education")]:
        folder = source_root / f"Census 2017/pbs_2017_table{table}"
        manifest = read_csv(folder / "manifest.csv")
        if len(manifest) != lock["expected_source_rows"][f"census2017_{module}"]:
            raise ValueError("2017 manifest coverage changed")
        for entry in manifest:
            filename = Path(entry["file_path"]).name
            path = folder / filename
            text = pdf_text(path)
            if table == "01":
                raw, locator, label = parse_population_pdf(text)
            else:
                raw, locator = parse_education_pdf(text)
            meta = dict(source_unit_id=f"census2017:{entry['code']}", source_dataset=f"census2017_{module}",
                        year=2017, source_name=entry["district_name"], source_code=entry["code"],
                        code_type="PBS_publication_code_not_longitudinal_id", source_province="",
                        source_geography="district_or_agency_or_frontier_region_as_published",
                        universe="all_ages_all_sexes" if module == "population" else "age_5_plus_all_sexes",
                        source_file=path.relative_to(source_root).as_posix(), source_locator=locator,
                        source_url=entry["attempted_url"], source_status="archived_official_pdf")
            output.extend(observations(meta, raw))
    for table, module in [("1", "population"), ("13", "education")]:
        entries = [x for x in lock["files"] if x.get("role") == f"census2023_{module}"]
        for entry in entries:
            path = source_root / entry["path"]
            province = entry["province"]
            for label, raw, locator in parse_2023_pdf(pdf_text(path, first_page=False), table):
                # IDs identify a source name within one census/province. Spelling
                # discrepancies across tables remain explicit for Step 3 review.
                slug = norm(bare_name(label)).replace(" ", "_")
                meta = dict(source_unit_id=f"census2023:{province}:{slug}", source_dataset=f"census2023_{module}",
                            year=2023, source_name=label, source_code="", code_type="generated_source_label_key",
                            source_province=province, source_geography="district_as_published",
                            universe="all_ages_including_headcount_only" if module == "population" else "age_5_plus_detailed_enumeration",
                            source_file=entry["path"], source_locator=locator,
                            source_url=entry["source_url"], source_status="archived_official_pdf")
                output.extend(observations(meta, raw))
    output.sort(key=lambda r: (r["source_dataset"], r["source_unit_id"], r["indicator"]))
    counts = collections.Counter()
    units = set()
    for r in output:
        units.add((r["source_dataset"], r["source_unit_id"]))
    for module, _ in units:
        counts[module] += 1
    if dict(counts) != lock["expected_source_rows"]:
        raise ValueError(f"Unexpected source coverage: {dict(counts)} != {lock['expected_source_rows']}")
    return output


def compare_local_extracts(rows, source_root, lock):
    originals = {(r["source_dataset"], r["source_unit_id"], r["indicator"]): r for r in rows}
    result = []
    for entry in lock["files"]:
        if not entry["role"].startswith("legacy_census2023_"):
            continue
        dataset = entry["role"].removeprefix("legacy_")
        table = "1" if dataset.endswith("population") else "13"
        for label, raw, line in parse_2023(source_root / entry["path"], table):
            uid = f"census2023:{entry['province']}:{norm(bare_name(label)).replace(' ', '_')}"
            for indicator, old_raw in raw.items():
                new = originals.get((dataset, uid, indicator))
                old = number(old_raw)
                status = "no_source_label_match" if new is None else "match" if old == new["value"] else "different"
                result.append(dict(source_dataset=dataset, source_unit_id=uid, source_name=label,
                                   indicator=indicator, official_value=new["value"] if new else None,
                                   legacy_extract_value=old, legacy_raw_value=old_raw, status=status,
                                   official_file=new["source_file"] if new else "",
                                   official_locator=new["source_locator"] if new else "",
                                   legacy_file=entry["path"], legacy_locator=f"CSV physical line {line}"))
    return sorted(result, key=lambda r:(r["source_dataset"], r["source_unit_id"], r["indicator"]))


def validate(rows):
    keys = [(r["source_dataset"], r["source_unit_id"], r["indicator"]) for r in rows]
    if len(keys) != len(set(keys)):
        raise ValueError("Duplicate observation key")
    grouped = collections.defaultdict(dict)
    for row in rows:
        grouped[(row["source_dataset"], row["source_unit_id"])][row["indicator"]] = row["value"]
    issues, checks = [], []
    for (dataset, uid), values in sorted(grouped.items()):
        if dataset.endswith("population"):
            total, parts = "pop_total", POP[1:]
        else:
            total, parts = "total", (EDU23 if dataset.startswith("census2023") else EDU)[1:]
        if values[total] is None or values[total] <= 0:
            raise ValueError(f"Missing/zero total: {dataset}/{uid}")
        missing = [k for k in parts if values.get(k) is None]
        residual = values[total] - sum(values.get(k) or 0 for k in parts)
        # A source-table discrepancy is an audit finding; retain the source
        # counts, mark the unit, and exclude it from the research-ready subset.
        status = "pass" if not missing and residual == 0 else "source_count_discrepancy" if residual != 0 else "source_symbol_present"
        check = dict(source_dataset=dataset, source_unit_id=uid, check="category_sum", status=status,
                     residual=residual, missing_components="|".join(missing))
        checks.append(check)
        if status != "pass":
            issues.append(check)
        for indicator, value in values.items():
            if value is not None and indicator.startswith("pct_") and not 0 <= value <= 100:
                raise ValueError(f"Rate outside 0–100: {dataset}/{uid}/{indicator}")
    statuses = {(r["source_dataset"], r["source_unit_id"]): r["status"] for r in checks}
    for row in rows:
        row["validation_status"] = statuses[(row["source_dataset"], row["source_unit_id"])]
        if row["validation_status"] == "source_count_discrepancy" and row.get("status") == "derived":
            row["value"] = None
            row["status"] = "withheld_source_count_discrepancy"
    return checks, issues


def legacy_crosswalk(path):
    tree = ast.parse(path.read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == "CROSSWALK" for t in node.targets):
            return ast.literal_eval(node.value)
    raise ValueError("Cannot locate the baseline CROSSWALK")


def candidates(rows, baseline_root, legacy_path):
    """Diagnostic current-map aggregation only; no longitudinal equivalence claim."""
    geo = json.loads((baseline_root / "pakistan_districts_province_boundries.geojson").read_text())
    geo_keys = {norm(f["properties"].get("districts") or f["properties"].get("district_agency") or "") for f in geo["features"]}
    mapping = legacy_crosswalk(legacy_path)
    groups, links = collections.defaultdict(list), {}
    for row in rows:
        key = norm(bare_name(row["source_name"]))
        # Agency suffix is part of the legacy map's keys.
        key = mapping.get(key, key)
        matched = key in geo_keys
        links[(row["source_dataset"], row["source_unit_id"])] = dict(
            source_dataset=row["source_dataset"], source_unit_id=row["source_unit_id"],
            source_name=row["source_name"], district_key=key if matched else "",
            status="legacy_mapping_unreviewed" if matched else "unresolved",
            evidence="baseline build_dataset.py CROSSWALK and map names; not a boundary-equivalence proof")
        if matched:
            groups[(key, row["source_dataset"], row["indicator"])].append(row)
    result = []
    for (key, dataset, indicator), group in sorted(groups.items()):
        missing = any(r["value"] is None for r in group)
        numerator = denominator = None
        if indicator.startswith("pct_"):
            if not missing:
                numerator = sum(r["numerator"] for r in group)
                denominator = sum(r["denominator"] for r in group)
            value = round(numerator / denominator * 100, 6) if denominator else None
        else:
            value = None if missing else sum(r["value"] for r in group)
        year = group[0]["year"]
        field = f"{'t1' if dataset.endswith('population') else 't_edu'}_{year}_{indicator}"
        result.append(dict(district_key=key, source_dataset=dataset, year=year, indicator=indicator,
                           field=field, value=value, numerator=numerator, denominator=denominator,
                           status="missing_component" if missing else "legacy_mapping_unreviewed",
                           source_units="|".join(sorted(r["source_unit_id"] for r in group))))
    return result, [links[k] for k in sorted(links)]


def compare(candidate_rows, baseline_root):
    baseline = json.loads((baseline_root / "districts.json").read_text())
    result = []
    for row in candidate_rows:
        field, key, value = row["field"], row["district_key"], row["value"]
        old = baseline.get(key, {}).get(field)
        if field not in baseline.get(key, {}):
            status = "not_in_baseline"
        elif value is None or old is None:
            status = "match" if value is old else "different_missingness"
        else:
            tolerance = 0.005001 if row["indicator"].startswith("pct_") else 0
            status = "match" if abs(value - old) <= tolerance else "different"
        result.append(dict(district_key=key, field=field, rebuilt_value=value, baseline_value=old,
                           status=status, source_units=row["source_units"]))
    return result


def parquet(path, rows, fields):
    con = duckdb.connect()
    numeric = {"value", "numerator", "denominator", "rebuilt_value", "baseline_value"}
    definition = ",".join(f'"{f}" {"DOUBLE" if f in numeric else "BIGINT" if f == "year" else "VARCHAR"}' for f in fields)
    con.execute(f"CREATE TABLE output ({definition})")
    con.executemany(f"INSERT INTO output VALUES ({','.join('?' for _ in fields)})", [[r.get(f) for f in fields] for r in rows])
    con.execute("COPY output TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(path)])
    actual = con.execute("SELECT * FROM read_parquet(?)", [str(path)]).fetchall()
    expected = [tuple(float(r.get(f)) if f in numeric and r.get(f) is not None else r.get(f) if f == 'year' else str(r[f]) if r.get(f) is not None else None for f in fields) for r in rows]
    if actual != expected:
        raise ValueError(f"Parquet read-back differs: {path.name}")
    con.close()


def build(source_root, out, lock_path, baseline_root, legacy_path):
    out = out.resolve()
    if out.exists():
        raise ValueError(f"Output already exists; choose a new release folder: {out}")
    if (contained(out, source_root) or contained(out, REPO) or contained(source_root, out)
            or contained(baseline_root, out)):
        raise ValueError("Output must be outside source inputs and the application repository")
    if not shutil.which("pdftotext"):
        raise ValueError("Required external dependency pdftotext is missing")
    lock = json.loads(lock_path.read_text())
    if lock["schema_version"] != SCHEMA_VERSION:
        raise ValueError("Incompatible input lock schema")
    verify_files(source_root, lock["files"])
    verify_files(baseline_root, lock["baseline_files"])
    if sha256(legacy_path) != lock["legacy_crosswalk_sha256"]:
        raise ValueError("Legacy crosswalk changed; refresh inventory after review")
    rows = source_rows(source_root, lock)
    checks, issues = validate(rows)
    extract_comparison = compare_local_extracts(rows, source_root, lock)
    candidate_rows, links = candidates(rows, baseline_root, legacy_path)
    comparison = compare(candidate_rows, baseline_root)
    out.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".census-build-", dir=out.parent))
    try:
        write_csv(staging / "source_observations.csv", rows, OBS_FIELDS)
        parquet(staging / "source_observations.parquet", rows, OBS_FIELDS)
        write_csv(staging / "source_units.csv", [dict(zip(
            ["source_dataset", "source_unit_id", "source_name", "source_code", "code_type", "source_province", "source_geography"], k))
            for k in sorted({tuple(r[f] for f in ["source_dataset", "source_unit_id", "source_name", "source_code", "code_type", "source_province", "source_geography"]) for r in rows})])
        write_csv(staging / "legacy_map_crosswalk.csv", links)
        write_csv(staging / "legacy_map_candidates.csv", candidate_rows)
        parquet(staging / "legacy_map_candidates.parquet", candidate_rows, list(candidate_rows[0]))
        write_csv(staging / "baseline_comparison.csv", comparison)
        write_csv(staging / "legacy_extract_comparison.csv", extract_comparison)
        write_csv(staging / "validation_checks.csv", checks)
        write_csv(staging / "issues.csv", issues, list(checks[0]))
        dictionary = []
        for dataset, indicator in sorted({(r["source_dataset"], r["indicator"]) for r in rows}):
            example = next(r for r in rows if r["source_dataset"] == dataset and r["indicator"] == indicator)
            dictionary.append(dict(source_dataset=dataset, indicator=indicator, unit=example["unit"],
                                   universe=example["universe"], aggregation="sum numerator and denominator, then divide" if indicator.startswith("pct_") else "sum counts only across disjoint verified units",
                                   cross_year_comparability="not_certified_pending_geography_and_definition_review",
                                   formula=example["raw_value"] if example["status"] in ("derived", "missing_component") else "source cell"))
        write_csv(staging / "indicator_dictionary.csv", dictionary)
        validation = dict(observations=len(rows), source_units_by_dataset=dict(collections.Counter(r["source_dataset"] for r in checks)),
                          category_checks=dict(collections.Counter(r["status"] for r in checks)),
                          baseline_comparison=dict(collections.Counter(r["status"] for r in comparison)),
                          legacy_extract_comparison=dict(collections.Counter(r["status"] for r in extract_comparison)),
                          crosswalk_status=dict(collections.Counter(r["status"] for r in links)),
                          duplicate_observation_keys=0, parquet_readback="pass", release_status="audit_build_not_harmonised_research_release",
                          caveats=["2017 and 2023 counts are parsed from checksum-locked official PDFs; legacy CSVs are comparison inputs only.",
                                   "2017 publication codes and 2023 source-label IDs are not cross-year identifiers.",
                                   "No 2017 values are copied to Keamari and no cross-year changes are computed.",
                                   "2023 Table 1 includes headcount-only population; education uses its own 5+ denominator.",
                                   "Legacy map aggregation is diagnostic, not approved geographic equivalence."])
        write_json(staging / "validation_report.json", validation)
        write_json(staging / "input_lock.json", lock)
        versions = dict(python=".".join(map(str, sys.version_info[:3])), duckdb=duckdb.__version__,
                        pdftotext=subprocess.run(["pdftotext", "-v"], capture_output=True, text=True).stderr.splitlines()[0])
        fingerprint = dict(inputs=lock, code=code_fingerprint(), runtime=versions)
        release_id = "census-audit-" + hashlib.sha256(json_bytes(fingerprint)).hexdigest()[:16]
        manifest = dict(release_id=release_id, schema_version=SCHEMA_VERSION, baseline_commit=BASELINE,
                        **fingerprint, outputs=[dict(path=p.name, bytes=p.stat().st_size, sha256=sha256(p)) for p in sorted(staging.iterdir())])
        write_json(staging / "build_manifest.json", manifest)
        write_json(staging / "run.json", dict(captured_at_utc=datetime.now(timezone.utc).isoformat(),
                                             source_root=str(source_root.resolve()), baseline_root=str(baseline_root.resolve()),
                                             output_root=str(out), release_id=release_id))
        # Directory is only made visible after parsing, validation and read-back.
        # Refuse replacement even if another process created the destination.
        if out.exists():
            raise ValueError("Output appeared during build; refusing replacement")
        staging.rename(out)
        return validation, release_id
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=WORKSPACE / "raw_data/pbs")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--input-lock", type=Path, default=REPO / "docs/stage1/inventory/census_inputs.lock.json")
    parser.add_argument("--baseline-root", type=Path, default=REPO / "app/data")
    parser.add_argument("--legacy-crosswalk", type=Path, default=REPO / "etl/build_dataset.py")
    args = parser.parse_args()
    try:
        report, release = build(args.source_root, args.out, args.input_lock, args.baseline_root, args.legacy_crosswalk)
    except (ValueError, OSError, subprocess.SubprocessError) as exc:
        parser.exit(1, f"Census build stopped without publishing outputs: {exc}\n")
    print(json.dumps(dict(release_id=release, output=str(args.out.resolve()), **report), indent=2))


if __name__ == "__main__":
    main()
