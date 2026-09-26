"""Small, side-effect-free helpers for the Stage 1 audit and census build."""
from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
WORKSPACE = REPO.parent
BASELINE = "df174cf0e4c56db1162bb6c942db4c649dbd55c4"


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_bytes(value) -> bytes:
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, indent=2,
                       allow_nan=False) + "\n").encode()


def write_json(path: Path, value):
    path.write_bytes(json_bytes(value))


def write_csv(path: Path, rows: list[dict], fields: list[str] | None = None):
    fields = fields or list(rows[0])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def read_csv(path: Path):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def contained(path: Path, parent: Path) -> bool:
    return path.resolve().is_relative_to(parent.resolve())


def verify_files(root: Path, entries: list[dict]):
    errors = []
    for item in entries:
        path = root / item["path"]
        if not contained(path, root):
            errors.append(f"Input escapes its root: {item['path']}")
        elif not path.is_file():
            errors.append(f"Missing required input: {item['path']}")
        elif path.stat().st_size != item["bytes"] or sha256(path) != item["sha256"]:
            errors.append(f"Changed input (review before refreshing the lock): {item['path']}")
    if errors:
        raise ValueError("\n".join(errors))


def code_fingerprint():
    return {p.name: sha256(p) for p in sorted(Path(__file__).parent.iterdir())
            if p.suffix == ".py" or p.name == "requirements.txt"}
