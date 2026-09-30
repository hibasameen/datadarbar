#!/usr/bin/env python3
"""Verify artifact integrity and byte equality between two offline builds."""
import argparse
import hashlib
import json
from pathlib import Path

from common import code_fingerprint, contained, json_bytes, sha256, verify_files, write_json


def verify(release):
    manifest = json.loads((release / "build_manifest.json").read_text())
    verify_files(release, manifest["outputs"])
    fingerprint = {k: manifest[k] for k in ["inputs", "code", "runtime"]}
    expected_id = "census-audit-" + hashlib.sha256(json_bytes(fingerprint)).hexdigest()[:16]
    if expected_id != manifest["release_id"]:
        raise ValueError("Release identity does not match its fingerprint")
    if json.loads((release / "input_lock.json").read_text()) != manifest["inputs"]:
        raise ValueError("Input lock does not match build manifest")
    if manifest["code"] != code_fingerprint():
        raise ValueError("Current Stage 1 code differs from the archived build fingerprint")
    names = {x["path"] for x in manifest["outputs"]} | {"build_manifest.json", "run.json"}
    if {p.name for p in release.iterdir()} != names:
        raise ValueError("Release directory contains unrecorded or missing files")
    return manifest


def compare_builds(release, repeat):
    first, second = verify(release), verify(repeat)
    names = [x["path"] for x in first["outputs"]] + ["build_manifest.json"]
    if first["release_id"] != second["release_id"]:
        raise ValueError("Builds have different release identities")
    mismatched = [name for name in names if sha256(release / name) != sha256(repeat / name)]
    if mismatched:
        raise ValueError(f"Builds differ: {mismatched}")
    return dict(release_id=first["release_id"], result="pass", byte_identical_files=len(names),
                excluded_files={"run.json":"Execution time and absolute output location are expected to differ"},
                output_integrity="pass", current_code_fingerprint="pass", runtime=first["runtime"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--repeat", type=Path, required=True)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()
    try:
        if args.report and any(contained(args.report, root) for root in [args.release, args.repeat]):
            raise ValueError("Write the verification report outside the immutable release directories")
        result = compare_builds(args.release, args.repeat)
        if args.report:
            write_json(args.report, result)
    except (ValueError, OSError, KeyError) as exc:
        parser.exit(1, f"Verification failed: {exc}\n")
    print(json.dumps(result, indent=2))
