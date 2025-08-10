"""
Join parsed CSV with district names to build a JSON keyed by normalized district for the web app.
Run: python build_dataset.py --csv ./district_indicators.csv --out ../app/data/pbs_district_indicators.json
"""
import argparse, csv, json, re, os

def norm(s):
    return re.sub(r"[^a-z0-9]+", " ", (s or "") .lower()).strip()

def main(csv_path, out_json):
    data = {}
    with open(csv_path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            key = norm(row["district"])
            data[key] = {
                "literacy_total": _to_num(row.get("literacy_total")),
                "literacy_male": _to_num(row.get("literacy_male")),
                "literacy_female": _to_num(row.get("literacy_female")),
                "school_attendance": _to_num(row.get("school_attendance")),
                "disability_rate": _to_num(row.get("disability_rate")),
                "no_health_facility_5km": _to_num(row.get("no_health_facility_5km"))
            }
    os.makedirs(os.path.dirname(out_json), exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"Wrote {len(data)} districts -> {out_json}")

def _to_num(v):
    try:
        return float(v) if v not in (None, '', 'NA', 'N/A', '-') else None
    except:
        return None

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="./district_indicators.csv")
    ap.add_argument("--out", default="../app/data/pbs_district_indicators.json")
    args = ap.parse_args()
    main(args.csv, args.out)
