#!/usr/bin/env python3
"""Inventory all registered sources and published assets without running ETL.

Only approved dataset roots are read. No broad scan of user documents, secret
files, database/WAL contents, or microdata records. Non-census raw files are
inventoried by name/size; only the census build closure is SHA-256 locked.
"""
from __future__ import annotations

import argparse
import collections
import json
import re
import subprocess
from pathlib import Path

from common import BASELINE, REPO, WORKSPACE, read_csv, sha256, write_csv, write_json
from source_registry import SOURCES

EXCLUDED = {".DS_Store", "__pycache__", ".ipynb_checkpoints", ".git", "node_modules"}


def expand(workspace, pattern):
    if any(c in pattern for c in "*?["):
        return sorted(workspace.glob(pattern))
    path = workspace / pattern
    return [path] if path.exists() else []


def allowed(path):
    return not any(p in EXCLUDED for p in path.parts) and not re.search(
        r"(?i)(api[ _-]?key|open[ _-]?ai[ _-]?key|credential|secret|\.env$|token)", path.name)


def files_for(paths):
    result = set()
    for path in paths:
        if path.is_file() and allowed(path):
            result.add(path)
        elif path.is_dir():
            result.update(p for p in path.rglob("*") if p.is_file() and allowed(p))
    return sorted(result)


def lock_census(workspace, repo):
    root = workspace / "raw_data/pbs"
    files = []
    def add(path, role, **meta):
        if not path.is_file():
            raise ValueError(f"Required census input missing: {path}")
        files.append(dict(path=path.relative_to(root).as_posix(), bytes=path.stat().st_size,
                          sha256=sha256(path), role=role, **meta))
    for table, module in [("01", "population"), ("15", "education")]:
        folder = root / f"Census 2017/pbs_2017_table{table}"
        manifest = folder / "manifest.csv"
        add(manifest, f"census2017_{module}_manifest")
        entries = read_csv(manifest)
        if len(entries) != 135 or len({x["code"] for x in entries}) != 135:
            raise ValueError("Expected 135 unique 2017 publication codes; review inventory changes")
        for entry in entries:
            add(folder / Path(entry["file_path"]).name, f"census2017_{module}", source_url=entry["attempted_url"],
                publication_code=entry["code"], source_status="archived_pdf_url_from_local_manifest")
    official = root / "stage1_sources/2026-09-25-official"
    retrieval = json.loads((official / "retrieval_manifest.json").read_text())
    add(official / "retrieval_manifest.json", "census2023_retrieval_manifest")
    add(official / "source_page.html", "census2023_source_page")
    official_files = {x["path"]: x for x in retrieval["files"]}
    for table, module in [("1", "population"), ("13", "education")]:
        for province in ["balochistan", "islamabad", "kp", "punjab", "sindh"]:
            filename = f"table_{table}_{province}{'' if province == 'islamabad' else '_districts'}.csv"
            add(root / f"Census 2023/census2023_all_tables/table_{table}" / filename,
                f"legacy_census2023_{module}", province=province, source_url="https://www.pbs.gov.pk/census/",
                source_status="local_extract; exact_original_retrieval_url_and_extraction_history_unverified")
            pdf_name = filename.replace(".csv", ".pdf")
            downloaded = official_files[pdf_name]
            if sha256(official / pdf_name) != downloaded["sha256"]:
                raise ValueError(f"Downloaded original changed: {pdf_name}")
            add(official / pdf_name, f"census2023_{module}", province=province,
                source_url=downloaded["url"], retrieved_at_utc=downloaded["retrieved_at_utc"],
                source_status="official_pdf_archived_with_retrieval_manifest")
    baseline_paths = ["districts.json", "census_data.js", "pakistan_districts_province_boundries.geojson",
                      "warehouse/district_indicators.parquet", "warehouse/catalog.json"]
    baseline_files = []
    for rel in baseline_paths:
        p = repo / "app/data" / rel
        baseline_files.append(dict(path=rel, bytes=p.stat().st_size, sha256=sha256(p)))
    return dict(schema_version="census-source-v0.1", baseline_commit=BASELINE,
                expected_source_rows={"census2017_population":135, "census2017_education":135,
                                      "census2023_population":136, "census2023_education":136},
                files=sorted(files, key=lambda x:x["path"]), baseline_files=baseline_files,
                legacy_crosswalk_sha256=sha256(repo / "etl/build_dataset.py"),
                limitations=["2017 publication codes are not longitudinal IDs.",
                             "2023 CSVs are legacy comparison inputs; source observations come directly from archived official PDFs.",
                             "No current-file date is used as a source release or retrieval date."])


def inventory(workspace, repo, out):
    catalog = json.loads((repo / "app/data/warehouse/catalog.json").read_text())
    source_rows, dependency_rows, file_rows = [], [], {}
    output_sources = collections.defaultdict(set)
    for spec in SOURCES:
        available_files = set()
        missing = []
        for role in ["inputs", "pipeline", "outputs"]:
            for pattern in spec[role]:
                matches = expand(workspace, pattern)
                local_files = files_for(matches)
                if not matches:
                    missing.append(pattern)
                dependency_rows.append(dict(dataset_id=spec["dataset_id"], role=role, path_or_pattern=pattern,
                                            availability="present" if matches else "missing", file_count=len(local_files),
                                            evidence=spec["evidence"]))
                for p in local_files:
                    # External project directories are inventory metadata only;
                    # census observations and baseline are the hashed closure.
                    rel = str(p.relative_to(workspace)) if p.is_relative_to(workspace) else str(p)
                    item = file_rows.setdefault(rel, dict(path=rel, bytes=p.stat().st_size, sha256="",
                                                         identity_status="metadata_only", dataset_ids=set(), roles=set()))
                    item["dataset_ids"].add(spec["dataset_id"])
                    item["roles"].add(role)
                    if role == "inputs":
                        available_files.add(rel)
                    if role == "outputs":
                        output_sources[rel].add(spec["dataset_id"])
        row = {k:v for k,v in spec.items() if k not in ["inputs", "pipeline", "outputs"]}
        row.update(input_paths=" | ".join(spec["inputs"]), transformation_paths=" | ".join(spec["pipeline"]),
                   published_or_local_outputs=" | ".join(spec["outputs"]),
                   input_files_found=len(available_files), missing_paths=" | ".join(missing),
                   source_retrieval_date="2026-09-25 UTC; official PDF capture; exact times in retrieval manifest" if spec["dataset_id"] in ("census2023_population", "census2023_education") else "not_established; inspect individual manifests",
                   stage1_membership="census_build" if spec["reproducibility_status"].startswith("stage1_") else "inventory_only")
        source_rows.append(row)
    lock = lock_census(workspace, repo)
    for entry in lock["files"]:
        rel = "raw_data/pbs/"+entry["path"]
        if rel in file_rows:
            file_rows[rel].update(sha256=entry["sha256"], identity_status="census_build_locked")
    # Completeness is checked against both the catalogue and the published tree.
    assets = []
    asset_paths = sorted(p for p in (repo / "app/data").rglob("*") if p.is_file() and allowed(p))
    asset_paths += [repo / "app/assets/js/econ_data.js"]
    tables = {t["file"]: t for t in catalog["tables"]}
    table_rows = []
    for p in asset_paths:
        rel = "datadarbar/"+p.relative_to(repo).as_posix()
        sources = output_sources.get(rel, set())
        if p.name == "catalog.json":
            sources = {"file_catalog"}
        # census_data also embeds boundary geometry, in addition to panel values.
        if p.name == "census_data.js":
            sources = sources | {"boundaries"}
        assets.append(dict(path=rel, bytes=p.stat().st_size, sha256=sha256(p),
                           datasets="|".join(sorted(sources)), lineage_status="registered" if sources else "unresolved"))
        if p.name in tables:
            t = tables[p.name]
            table_rows.append(dict(table=t["name"], path=rel, rows=t["rows"],
                                   catalog_bytes=t["bytes"], actual_bytes=p.stat().st_size,
                                   source=t.get("source", ""), unit=t.get("unit") or "see columns/notes",
                                   notes=t.get("notes", ""), dataset_ids="|".join(sorted(sources)),
                                   status="registered" if sources else "unresolved"))
    if len(table_rows) != len(catalog["tables"]) or any(not r["dataset_ids"] for r in table_rows):
        raise ValueError("A published catalogue table is missing its inventory/source association")
    if any(not r["datasets"] for r in assets):
        raise ValueError("A published asset has no source inventory entry")
    source_rows.sort(key=lambda r:r["dataset_id"])
    out.mkdir(parents=True, exist_ok=True)
    write_csv(out / "source_inventory.csv", source_rows)
    write_csv(out / "dependency_inventory.csv", dependency_rows)
    final_files = [{**v, "dataset_ids":"|".join(sorted(v["dataset_ids"])), "roles":"|".join(sorted(v["roles"]))} for _,v in sorted(file_rows.items())]
    write_csv(out / "file_inventory.csv", final_files)
    write_csv(out / "published_assets.csv", assets)
    write_csv(out / "warehouse_tables.csv", sorted(table_rows, key=lambda r:r["table"]))
    write_json(out / "census_inputs.lock.json", lock)
    report = dict(dataset_families=len(source_rows), catalogue_tables=len(table_rows),
                  published_assets=len(assets), inventoried_files=len(file_rows),
                  census_locked_files=len(lock["files"]), unresolved_published_assets=0,
                  missing_dependency_paths=sum(x["availability"]=="missing" for x in dependency_rows),
                  completion_scope="All 25 catalogue tables, all app/data assets and econ_data.js; registered local extensions and their declared dependencies.",
                  limitations=["Complete inventory coverage does not mean complete raw provenance or reproducibility.",
                               "Only the census build inputs and published assets are hashed; other files are metadata-only.",
                               "External Adaad dependencies are inventoried where explicitly referenced; no pipelines are executed.",
                               "Credentials, cache directories and database/WAL contents are excluded."])
    write_json(out / "inventory_report.json", report)
    lines = ["# Data Darbar source inventory", "", "Generated from the registry and local file availability. Census inputs are checksum-locked; other sources retain explicit blockers.", "",
             f"Coverage: {len(source_rows)} dataset families, {len(table_rows)} public warehouse tables, {len(assets)} published data assets, {len(file_rows)} dependency files.", "",
             "| Dataset | Period | Build status | Outstanding work |", "|---|---|---|---|"]
    for r in source_rows:
        lines.append(f"| {r['dataset_id']} | {r['observation_period']} | {r['reproducibility_status']} | {r['blocker']} |")
    lines += ["", "The detailed CSVs retain source geography, covered population, URLs, inputs, scripts, outputs, reuse conditions, evidence and availability. Both census years build directly from archived official PDFs. Historical CSVs are comparison inputs only. Inventory coverage is complete for the declared scope; unresolved lineage and missing dependencies remain explicit.", ""]
    (out / "README.md").write_text("\n".join(lines))
    return report


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--workspace", type=Path, default=WORKSPACE)
    p.add_argument("--repo", type=Path, default=REPO)
    p.add_argument("--out", type=Path, default=REPO / "docs/stage1/inventory")
    a = p.parse_args()
    print(json.dumps(inventory(a.workspace, a.repo, a.out), indent=2))
