#!/usr/bin/env python3
"""Capture the ten selected public PBS 2023 district PDFs, separately from builds.

Discover actual links on the official page. Preserve response bytes and dated
retrieval metadata. Existing snapshots are never replaced. The offline builder
does not invoke this utility or require network access.
"""
import argparse
import concurrent.futures
import hashlib
import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path

from common import write_json

PAGE = "https://www.pbs.gov.pk/census/"
EXPECTED = {f"table_{table}_{region}{'' if region=='islamabad' else '_districts'}.pdf"
            for table in [1, 13] for region in ["kp", "punjab", "sindh", "balochistan", "islamabad"]}


class Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = {}
    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = dict(attrs).get("href", "")
        url = urllib.parse.urljoin(PAGE, href)
        name = Path(urllib.parse.urlparse(url).path).name
        if name in EXPECTED and urllib.parse.urlparse(url).hostname == "www.pbs.gov.pk":
            self.urls[name] = url


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    if args.out.exists():
        parser.error("Snapshot folder already exists; choose a new capture folder")
    response = urllib.request.urlopen(PAGE, timeout=45)
    page = response.read()
    links = Links()
    links.feed(page.decode("utf-8"))
    if set(links.urls) != EXPECTED:
        parser.error("The official page does not expose all ten expected PDF links")
    args.out.mkdir(parents=True)
    (args.out / "source_page.html").write_bytes(page)
    def fetch(item):
        name, url = item
        with urllib.request.urlopen(url, timeout=45) as response:
            content = response.read()
            if not content.startswith(b"%PDF"):
                raise ValueError(f"Not a PDF: {name}")
            (args.out / name).write_bytes(content)
            return dict(path=name, url=url, retrieved_at_utc=datetime.now(timezone.utc).isoformat(),
                        sha256=hashlib.sha256(content).hexdigest(), bytes=len(content),
                        content_type=response.headers.get("Content-Type"))
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        records = list(pool.map(fetch, sorted(links.urls.items())))
    write_json(args.out / "retrieval_manifest.json", dict(source_page=PAGE, files=records,
               source_page_sha256=hashlib.sha256(page).hexdigest()))
    print(json.dumps(dict(files=len(records), bytes=sum(r["bytes"] for r in records), output=str(args.out)), indent=2))


if __name__ == "__main__":
    main()
