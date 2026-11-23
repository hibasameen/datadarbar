#!/usr/bin/env python3
"""District headline indicators from the 2010 long table (Tables 1.1, 1.2, 6.1; areas in acres).

Usage: python3 build_district_headline.py <derived_dir>
"""
import csv, gzip, sys, collections
from pathlib import Path

d = Path(sys.argv[1])
cell = {}
for r in csv.DictReader(gzip.open(d / "agri_census_2010_long.csv.gz", "rt")):
    if r["scope"].startswith("PAKISTAN") or r["table"] not in ("1.1", "1.2", "6.1") or r["qc"] == "count_mismatch":
        continue
    k = (r["scope"], r["unit"], r["table"], r["row_label"], int(r["col"]))
    if r["qc"] == "dup_label" and r["value"] != "" and cell.get(k, "") != "":
        cell[k] = str(float(cell[k]) + float(r["value"]))   # a class label printed twice (Mohmand, T1.1): keep both rows' figures
    else:
        cell.setdefault(k, r["value"])
units = list(dict.fromkeys((s, u) for (s, u, *_ ) in cell if u))
TOT = "PRIVATE FARMS - TOTAL"
SMALL = ["UNDER 1.0", "1.0 TO UNDER 2.5", "2.5 TO UNDER 5.0"]
LARGE = ["25.0 TO UNDER 50.0", "50.0 TO UNDER 100.0", "100.0 TO UNDER 150.0", "150.0 AND ABOVE"]


def g(s, u, t, row, col):
    v = cell.get((s, u, t, row, col), "")
    if v == "":
        return None
    v = float(v)
    return int(v) if v == int(v) else v


def pct(a, b):
    return round(100 * a / b, 2) if a is not None and b else ""


out = []
for s, u in units:
    farms, area, cult = (g(s, u, "1.1", TOT, c) for c in (2, 4, 6))
    small = sum(g(s, u, "1.1", r, 2) or 0 for r in SMALL)
    large_area = sum(g(s, u, "1.1", r, 4) or 0 for r in LARGE)
    own, oct_, ten = (g(s, u, "1.2", TOT, c) for c in (3, 4, 5))
    out.append(dict(
        province=s, unit=u,
        unit_type="province" if "PROVINCE" in u else "agency" if "AGENCY" in u else "frontier_region" if u.startswith("F.R") else
                  "special_area" if u.startswith("DE-EX") or u.startswith("CHOLISTAN") else "district",
        farms=farms, farm_area_ac=area, cultivated_area_ac=cult,
        avg_farm_size_ac=round(area / farms, 2) if farms else "",
        share_cultivated=pct(cult, area), share_farms_under_5ac=pct(small, farms), share_area_25ac_plus=pct(large_area, area),
        owner_farms=own, owner_cum_tenant_farms=oct_, tenant_farms=ten,
        share_farms_owner=pct(own, farms), share_farms_tenant=pct(ten, farms),
        cropped_area_ac=g(s, u, "6.1", TOT, 2), kharif_area_ac=g(s, u, "6.1", TOT, 3), rabi_area_ac=g(s, u, "6.1", TOT, 5),
        orchard_area_ac=g(s, u, "6.1", TOT, 7),
        cropping_intensity_pct=pct(g(s, u, "6.1", TOT, 2), cult),
    ))
with open(d / "agri_census_2010_district_headline.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(out[0])); w.writeheader(); w.writerows(out)
print(len(out), "units")
