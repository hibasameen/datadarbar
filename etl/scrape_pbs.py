"""
Scrape PBS Census 2023 district tables (12 & 13) and save PDFs locally.
Run: python scrape_pbs.py --out ./downloads
"""
import argparse, os, re, sys, time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

PBS_BASE = "https://www.pbs.gov.pk/"
CENSUS_BASE = "https://www.pbs.gov.pk/census-2023-district-tables"  # example hub page; adjust if needed

def get(url):
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    return r

def find_pdf_links(html):
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.lower().endswith(".pdf") and ("table_12" in href.lower() or "table_13" in href.lower()):
            links.append(urljoin(PBS_BASE, href))
    return sorted(set(links))

def main(out_dir):
    os.makedirs(out_dir, exist_ok=True)
    print("Fetching:", CENSUS_BASE)
    html = get(CENSUS_BASE).text
    pdfs = find_pdf_links(html)
    if not pdfs:
        print("No PDFs found. The site structure may have changed. Open the page and inspect table links.", file=sys.stderr)
        sys.exit(1)
    print(f"Found {len(pdfs)} PDFs")
    for i, url in enumerate(pdfs, 1):
        fn = os.path.join(out_dir, os.path.basename(url))
        print(f"[{i}/{len(pdfs)}] downloading {url} -> {fn}")
        with get(url) as r:
            with open(fn, "wb") as f:
                f.write(r.content)
        time.sleep(0.5)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="./downloads")
    args = ap.parse_args()
    main(args.out)
