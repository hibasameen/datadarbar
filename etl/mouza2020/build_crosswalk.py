#!/usr/bin/env python3
"""PBS Mouza Census tehsil -> Data Darbar ADM3 dd_id crosswalk.

The join is deliberately MANY-TO-ONE. PBS enumerates 595 tehsils against the
boundary file's 553 because PBS carries sub-tehsils created after the polygons
were drawn (ex-FATA, Balochistan, the Cholistan units). Every Mouza Census
figure is a count of mouzas, so summing several PBS tehsils into one polygon is
exactly the right operation and loses nothing.
"""
import collections
import csv
import difflib
import json
import os
import re
import unicodedata

HERE = os.path.dirname(os.path.abspath(__file__))
MOUZA = os.environ.get("MOUZA_CSV", os.path.join(HERE, "pk-mouza-2020-tehsil.csv"))
OUT = os.path.join(HERE, "mouza2020_tehsil_crosswalk.csv")


def load_dd_tehsils():
    """Pull the tehsil records straight out of the generated poverty payload so
    this folder does not carry a second copy of them."""
    path = os.path.join(HERE, "..", "..", "app", "data", "poverty_data.js")
    s = open(path).read()
    i = s.index("window.DD_POV=") + len("window.DD_POV=")
    return json.loads(s[i:s.index("\n", i)].rstrip(";"))["tehsils"]

# PBS district -> boundary-file district. geoBoundaries ADM3 predates the 2018
# FATA merger, the Chitral and Kohistan splits, and the newer Balochistan
# districts, so several PBS districts fold into one polygon set.
DISTRICT_ALIAS = {
    "ASTORE": "ASTOR",
    "BAJAUR": "BAJAURAGENCY",
    "BALTISTAN": "SKARDU",
    "CHAGAI": "CHAGHI",
    "CHITRALLOWER": "CHITRAL",
    "CHITRALUPPER": "CHITRAL",
    "DUKI": "LORALAI",
    "GHANCHE": "GHANCHI",
    "HATTIANBALA": "HATTIAN",
    "HUNZA": "HUNZANAGAR",
    "NAGAR": "HUNZANAGAR",
    "KARACHIWEST": "KARACHI",
    "MALIR": "KARACHI",
    "KHARMANG": "SKARDU",
    "SHIGAR": "SKARDU",
    "KHYBER": "KHYBERAGENCY",
    "KOLAIPALASKOHISTAN": "KOHISTAN",
    "LOWERKOHISTAN": "KOHISTAN",
    "KURRAM": "KURRAMAGENCY",
    "MALAKANDPROTECTEDAREA": "MALAKAND",
    "MOHMAND": "MOHMANDAGENCY",
    "MUSAKHEL": "MUSAKHAIL",
    "NAUSHAHROFEROZE": "NAUSHEHROFEROZE",
    "NORTHWAZIRISTAN": "NORTHWAZIRISTANAGENCY",
    "ORAKZAI": "ORAKZAIAGENCY",
    "SOUTHWAZIRISTAN": "SOUTHWAZIRISTANAGENCY",
    "SUDHNOTI": "SUDHNUTTI",
    "SUJAWAL": "SAJAWAL",
}

# PBS tehsil code -> boundary district, where the PBS district has no
# counterpart and its tehsils land in different polygons.
TEHSIL_DISTRICT_OVERRIDE = {
    "0071": "BAHAWALNAGAR",   # Fort Abbas (Cholistan)
    "0079": "BAHAWALPUR",     # Yazman (Cholistan)
    "0081": "RAHIMYARKHAN",   # Liaquatpur (Cholistan)
}

from manual_map import MANUAL  # noqa: E402  (reviewed by hand, see file)

SUFFIX = (r"\b(TEHSIL|TALUKA|TALUKO|SUB-?TEHSIL|SUB-?DIVISION|DISTRICT|"
          r"MUNICIPAL|CORPORATION|TOWN|CITY)\b")


def norm(x):
    x = unicodedata.normalize("NFKD", x or "").upper()
    x = re.sub(SUFFIX, " ", x)
    x = re.sub(r"[^A-Z0-9]", " ", x)
    return re.sub(r"\s+", " ", x).strip()


def sq(x):
    return norm(x).replace(" ", "")


def main():
    teh = load_dd_tehsils()
    dd = [dict(dd_id=k, name=v["name"], dk=v["dk"], prov=v["prov"])
          for k, v in teh.items()]
    by_district = collections.defaultdict(list)
    for d in dd:
        by_district[sq(d["dk"])].append(d)

    rows = list(csv.DictReader(open(MOUZA)))
    out, unresolved = [], []

    for r in rows:
        pdk = sq(r["district"])
        pdk = TEHSIL_DISTRICT_OVERRIDE.get(r["tehsil_code"],
                                           DISTRICT_ALIAS.get(pdk, pdk))
        cands = by_district.get(pdk, [])
        rec = {
            "tehsil_code": r["tehsil_code"], "tehsil": r["tehsil"],
            "district_code": r["district_code"], "district": r["district"],
            "province": r["province"], "mouzas": r["TotalMauzaCount"],
            "dd_id": "", "dd_name": "", "dd_district": pdk, "match": "",
            "dd_candidates": "",
        }

        if r["tehsil_code"] in MANUAL:
            dist_ovr, want_name, conf = MANUAL[r["tehsil_code"]]
            pool = by_district[sq(dist_ovr)] if dist_ovr else cands
            want = sq(want_name)
            hit = next((c for c in pool if sq(c["name"]) == want), None)
            if hit:
                rec.update(dd_id=hit["dd_id"], dd_name=hit["name"], match=conf)
                out.append(rec)
                continue
            rec["match"] = "MANUAL_TARGET_MISSING"
            rec["dd_candidates"] = want_name
            unresolved.append(rec)
            out.append(rec)
            continue

        if not cands:
            rec["match"] = "no_district"
            unresolved.append(rec)
            out.append(rec)
            continue

        if len(cands) == 1:
            c = cands[0]
            rec.update(dd_id=c["dd_id"], dd_name=c["name"],
                       match="only_tehsil_in_district")
            out.append(rec)
            continue

        want = sq(r["tehsil"])
        hit = next((c for c in cands if sq(c["name"]) == want), None)
        if hit:
            rec.update(dd_id=hit["dd_id"], dd_name=hit["name"], match="exact_name")
            out.append(rec)
            continue

        contained = [c for c in cands
                     if sq(c["name"]) and (sq(c["name"]) in want or want in sq(c["name"]))]
        if len(contained) == 1:
            c = contained[0]
            rec.update(dd_id=c["dd_id"], dd_name=c["name"], match="contained")
            out.append(rec)
            continue

        names = [c["name"] for c in cands]
        sqmap = {sq(c["name"]): c for c in cands}
        close_sq = difflib.get_close_matches(want, list(sqmap), n=1, cutoff=0.84)
        if close_sq:
            c = sqmap[close_sq[0]]
            rec.update(dd_id=c["dd_id"], dd_name=c["name"], match="fuzzy_squashed")
            out.append(rec)
            continue

        close = difflib.get_close_matches(r["tehsil"].title(), names, n=1, cutoff=0.82)
        if close:
            c = next(c for c in cands if c["name"] == close[0])
            rec.update(dd_id=c["dd_id"], dd_name=c["name"], match="fuzzy")
            out.append(rec)
            continue

        rec["match"] = "UNRESOLVED"
        rec["dd_candidates"] = "|".join(names)
        unresolved.append(rec)
        out.append(rec)

    cols = ["tehsil_code", "tehsil", "district_code", "district", "province",
            "mouzas", "dd_id", "dd_name", "dd_district", "match", "dd_candidates"]
    with open(OUT, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(out)

    counts = collections.Counter(r["match"] for r in out)
    print("match methods:", dict(counts))
    lost = sum(int(r["mouzas"] or 0) for r in out if not r["dd_id"])
    total = sum(int(r["mouzas"] or 0) for r in out)
    print(f"unresolved tehsils: {len(unresolved)}  mouzas unassigned: {lost} of {total}")
    for r in unresolved:
        if int(r["mouzas"] or 0) > 0:
            print(f"  {r['tehsil_code']:>5} {r['district'][:24]:24} {r['tehsil'][:30]:30} "
                  f"m={r['mouzas']:>4} cands=[{r['dd_candidates'][:95]}]")


if __name__ == "__main__":
    main()
