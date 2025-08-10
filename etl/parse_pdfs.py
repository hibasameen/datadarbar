"""
Parse PBS district tables (12 & 13) PDFs into tidy CSV using pdfplumber (layout-agnostic).
Run: python parse_pdfs.py --indir ./downloads --out data.csv
"""
import argparse, os, re, json, csv
from collections import defaultdict

try:
    import pdfplumber
except Exception as e:
    raise SystemExit("This script requires pdfplumber. Install: pip install pdfplumber")

def norm(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "") .lower()).strip()

def parse_table_text(text):
    """
    Very heuristic parser. You will likely need to tweak the regexes below for the exact PBS layout.
    Returns list of dicts with fields: province, district, literacy_total, literacy_male, literacy_female, school_attendance, disability_rate, no_health_facility_5km
    """
    rows = []
    province = None
    for line in text.splitlines():
        # Province header
        m_prov = re.search(r"Province\s*:\s*(.+)$", line, re.I)
        if m_prov:
            province = m_prov.group(1).strip()
            continue

        # District lines like: "Lahore  76.5 80.2 72.4  68.1  1.9  14.7"
        m = re.match(r"([A-Za-z\-\.\s']+)\s+(\d{1,3}\.?\d*)\s+(\d{1,3}\.?\d*)\s+(\d{1,3}\.?\d*)\s+(\d{1,3}\.?\d*)\s+(\d{1,3}\.?\d*)\s+(\d{1,3}\.?\d*)$", line.strip())
        if m:
            district = m.group(1).strip()
            vals = [float(m.group(i)) for i in range(2, 8)]
            rows.append({
                "province": province,
                "district": district,
                "literacy_total": vals[0],
                "literacy_male": vals[1],
                "literacy_female": vals[2],
                "school_attendance": vals[3],
                "disability_rate": vals[4],
                "no_health_facility_5km": vals[5]
            })
    return rows

def parse_pdf(path):
    rows = []
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            text = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
            rows.extend(parse_table_text(text))
    return rows

def main(indir, out_csv):
    all_rows = []
    for fn in sorted(os.listdir(indir)):
        if fn.lower().endswith(".pdf"):
            rows = parse_pdf(os.path.join(indir, fn))
            all_rows.extend(rows)

    # de-dup by province+district (last wins)
    keyf = lambda r: (norm(r["province"]), norm(r["district"]))
    dedup = {}
    for r in all_rows:
        dedup[keyf(r)] = r
    rows = list(dedup.values())

    # write CSV
    cols = ["province","district","literacy_total","literacy_male","literacy_female","school_attendance","disability_rate","no_health_facility_5km"]
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Wrote {len(rows)} rows -> {out_csv}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--indir", default="./downloads")
    ap.add_argument("--out", default="./district_indicators.csv")
    args = ap.parse_args()
    main(args.indir, args.out)
