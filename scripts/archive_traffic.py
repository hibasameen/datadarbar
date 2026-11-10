#!/usr/bin/env python3
"""Archive GitHub traffic figures, which GitHub only keeps for 14 days.

Run daily by .github/workflows/traffic-archive.yml. Writes three CSVs into
the directory given as the first argument:

  daily.csv      one row per UTC day: views, unique visitors, clones,
                 unique cloners. Days already archived are kept; days still
                 inside GitHub's 14-day window are refreshed, because the
                 current day's count keeps growing.
  referrers.csv  top referring sites over the trailing 14 days, one
                 snapshot per run.
  paths.csv      most visited pages over the trailing 14 days, one
                 snapshot per run.

"Unique" counts are unique within a day. They cannot be summed across days
to give unique visitors for a longer period.

Needs GH_TOKEN (a token allowed to read the repository's traffic) and
GITHUB_REPOSITORY (owner/name), both set by the workflow.
"""
import csv
import json
import os
import sys
import urllib.request
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

API = "https://api.github.com"
DAILY_FIELDS = ["date", "views", "unique_visitors", "clones", "unique_cloners"]


def get(repo, endpoint, token):
    request = urllib.request.Request(
        f"{API}/repos/{repo}/traffic/{endpoint}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def read_rows(path):
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_rows(path, fields, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def merge_daily(existing, views, clones, today):
    """Return daily rows with the fresh 14-day window laid over the archive.

    GitHub leaves out days with no activity. Inside the window those days
    are real zeros, so they are written as zeros rather than left missing.
    """
    by_day = {row["date"]: row for row in existing}

    fresh = {}
    for item in views:
        day = item["timestamp"][:10]
        fresh.setdefault(day, {})["views"] = item["count"]
        fresh[day]["unique_visitors"] = item["uniques"]
    for item in clones:
        day = item["timestamp"][:10]
        fresh.setdefault(day, {})["clones"] = item["count"]
        fresh[day]["unique_cloners"] = item["uniques"]

    if fresh:
        day = date.fromisoformat(min(fresh))
        while day <= today:
            fresh.setdefault(day.isoformat(), {})
            day += timedelta(days=1)

    for day, counts in fresh.items():
        by_day[day] = {
            "date": day,
            "views": counts.get("views", 0),
            "unique_visitors": counts.get("unique_visitors", 0),
            "clones": counts.get("clones", 0),
            "unique_cloners": counts.get("unique_cloners", 0),
        }
    return [by_day[day] for day in sorted(by_day)]


def add_snapshot(path, fields, new_rows, snapshot):
    """Append this run's snapshot, replacing any earlier one from today."""
    kept = [row for row in read_rows(path) if row["snapshot_date"] != snapshot]
    write_rows(path, fields, kept + new_rows)


def main():
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "traffic")
    out.mkdir(parents=True, exist_ok=True)
    repo = os.environ["GITHUB_REPOSITORY"]
    token = os.environ["GH_TOKEN"]
    today = datetime.now(timezone.utc).date()
    snapshot = today.isoformat()

    views = get(repo, "views", token)["views"]
    clones = get(repo, "clones", token)["clones"]
    daily_path = out / "daily.csv"
    daily = merge_daily(read_rows(daily_path), views, clones, today)
    write_rows(daily_path, DAILY_FIELDS, daily)

    referrers = [
        {
            "snapshot_date": snapshot,
            "referrer": item["referrer"],
            "views_14d": item["count"],
            "unique_visitors_14d": item["uniques"],
        }
        for item in get(repo, "popular/referrers", token)
    ]
    add_snapshot(
        out / "referrers.csv",
        ["snapshot_date", "referrer", "views_14d", "unique_visitors_14d"],
        referrers,
        snapshot,
    )

    paths = [
        {
            "snapshot_date": snapshot,
            "path": item["path"],
            "title": item["title"],
            "views_14d": item["count"],
            "unique_visitors_14d": item["uniques"],
        }
        for item in get(repo, "popular/paths", token)
    ]
    add_snapshot(
        out / "paths.csv",
        ["snapshot_date", "path", "title", "views_14d", "unique_visitors_14d"],
        paths,
        snapshot,
    )

    print(f"{len(daily)} days archived, {daily[0]['date']} to {daily[-1]['date']}"
          if daily else "No traffic recorded yet")


if __name__ == "__main__":
    main()
