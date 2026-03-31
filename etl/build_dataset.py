"""
Build a unified district-level JSON dataset from all available PBS sources:
  - Census 2017 & 2023 (Tables 1, 5, 12, 13, 14, 15, 16)
  - PSLM 2019-20 microdata (education, employment, housing, WASH, ICT)
  - Economic Census (establishments & workforce by industry)
  - LFS 2020-21 microdata (labour force, employment, industry)

Normalises district names, aggregates multi-district units (Karachi 7,
Kohistan 3, Chitral 2), computes diffs, and outputs a single JSON for
the web app.

Run:  python build_dataset.py
Output: ../app/data/districts.json
"""

import csv, json, os, re, sys
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
PBS  = HERE.parent.parent / "PBS data"
OUT  = HERE.parent / "app" / "data" / "districts.json"

# ── helpers ──────────────────────────────────────────────────────────────────

def norm(s):
    """Normalise a district name for matching."""
    s = (s or "").strip()
    s = re.sub(r"\(.*?\)", "", s)          # drop parenthetical
    s = re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
    return s

def to_num(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    v = str(v).strip().replace(",", "")
    if v in ("", "-", "NA", "N/A", "…", ".."):
        return None
    try:
        return float(v)
    except ValueError:
        return None

def read_csv(path):
    """Read a CSV and return list of dicts. Handles leading blank lines."""
    with open(path, newline="", encoding="utf-8-sig") as f:
        # Skip leading blank lines before the header
        lines = f.readlines()
    clean = [l for l in lines if l.strip()]
    import io
    return list(csv.DictReader(io.StringIO("".join(clean))))


def accumulate(out, key, vals):
    """Add vals into out[key], summing numeric fields when key already exists.

    This handles Karachi (7 sub-districts), Kohistan (3), and Chitral (2)
    where multiple CSV rows map to the same GeoJSON polygon.
    For count fields: sum.  For rate/derived fields: they will be recomputed
    after accumulation by each loader's post-processing step.
    """
    if key not in out:
        out[key] = dict(vals)
        return
    existing = out[key]
    for k, v in vals.items():
        if v is None:
            continue
        if existing.get(k) is None:
            existing[k] = v
        elif isinstance(v, (int, float)) and isinstance(existing[k], (int, float)):
            existing[k] += v
        # else: keep first (for non-numeric)

# ── District name crosswalk ──────────────────────────────────────────────────
# Maps normalised CSV names → normalised GeoJSON names where they differ.
# Built manually by comparing the two name lists.
CROSSWALK = {
    "bajaur":                  "bajaur agency",
    "chagai":                  "chaghi",
    # FR districts merged into parent settled districts (post-25th Amendment)
    "fr peshawar":             "peshawar",
    "fr kohat":                "kohat",
    "fr bannu":                "bannu",
    "fr lakki marwat":         "lakki marwat",
    "fr di khan":              "dera ismail khan",
    "fr dikhan":               "dera ismail khan",
    "fr tank":                 "tank",
    "kambar shahdad kot":      "kambar shahdadkot",
    "kashmor":                 "kashmore",
    "khyber":                  "khyber agency",
    "kurram":                  "kurram agency",
    "malakand protected area": "malakand",
    "mirpur khas":             "mirpurkhas",
    "mohmand":                 "mohmand agency",
    "musakhel":                "musakhail",
    "naushahro feroze":        "naushehro feroze",
    "north waziristan":        "north waziristan agency",
    "orakzai":                 "orakzai agency",
    "south waziristan":        "south waziristan agency",
    "sujawal":                 "sajawal",
    "tando allahyar":          "tando allah yar",
    "torghar":                 "tor ghar",
    "umer kot":                "umerkot",
    "bhakhar":                 "bhakkar",
    # Karachi sub-districts → single Karachi polygon in GeoJSON
    "karachi central":         "karachi",
    "karachi east":            "karachi",
    "karachi south":           "karachi",
    "karachi west":            "karachi",
    "korangi":                 "karachi",
    "malir":                   "karachi",
    "keamari":                 "karachi",
    # Additional name variants
    "tando ahyar":             "tando allah yar",
    "washuk district washuk":  "washuk",
    "hafizabad district hafizabad": "hafizabad",
    "buner district buner":    "buner",
    "kolai palas kohistan":    "kohistan",
    "lower kohistan":          "kohistan",
    "upper kohistan":          "kohistan",
    "lower chitral":           "chitral",
    "upper chitral":           "chitral",
    # Qilla / Killa variants
    "qilla abdullah":          "killa abdullah",
    "qilla saifullah":         "killa saifullah",
    # Lehri — new district carved from Sibi, merged back into Sibi polygon
    "lehri sub division":      "sibi",
    "lehri":                   "sibi",
    # Economic Census / LFS spelling variants
    "abbotabad":               "abbottabad",
    "batgram":                 "batagram",
    "battagram":               "batagram",
    "gawadar":                 "gwadar",
    "jehlum":                  "jhelum",
    "jhung":                   "jhang",
    "north wazirstan":         "north waziristan agency",
    "south wazirstan":         "south waziristan agency",
    "nowshehra":               "nowshera",
    "sawabi":                  "swabi",
    "sawat":                   "swat",
    "sohbatour":               "sohbatpur",
    "surab":                   "kalat",   # sub-district of Kalat
    "zohb":                    "zhob",
    "duki":                    "loralai",  # sub-district of Loralai
    "chaman":                  "killa abdullah",  # part of Killa Abdullah district
    "shaheed sikandar abad":   "sherani",
    # LFS 2024-25 S4C16 label variants
    "bahwalnagar":             "bahawalnagar",
    "d i khan":                "dera ismail khan",
    "der ghazi khan":          "dera ghazi khan",
    "kachhi bolan":            "kachhi",
    "kambar shahadad kot":     "kambar shahdadkot",
    "kech turbat":             "kech",
    "tando muhammad khan":     "tando muhammad khan",
    "south baziristan":        "south waziristan agency",
    "kemari":                  "karachi",
}

# GeoJSON keys that receive multiple CSV rows (need aggregation, not overwrite)
_MERGED_DISTRICTS = {"karachi", "kohistan", "chitral", "killa abdullah", "kalat", "loralai",
                      "peshawar", "kohat", "bannu", "lakki marwat", "dera ismail khan", "tank", "sibi",
                      "chaghi", "nushki"}  # chaghi/nushki may appear under both div 41 & 47 (Rakhshan)

# Names to skip entirely (not real district entries)
SKIP_NAMES = {
    "table", "table 12 balochistan districts csv", "table 12 islamabad csv",
    "table 12 kp districts csv", "table 12 punjab districts csv",
    "table 12 sindh districts csv", "tribal area adj", "tribal area adj bannu",
    "tribal area adj dera ismail khan", "tribal area adj kohat",
    "tribal area adj peshawar", "tribal area adj tank",
}

def apply_crosswalk(key):
    """Map a normalised CSV district name to its GeoJSON equivalent. Returns None for skip names."""
    if key in SKIP_NAMES:
        return None
    return CROSSWALK.get(key, key)

# ── GeoJSON district name index ──────────────────────────────────────────────

def load_geojson_names():
    """Return dict: normalised_name -> display_name from the GeoJSON."""
    gj_path = HERE.parent.parent / "web" / "pakistan_districts_province_boundries.geojson"
    if not gj_path.exists():
        # try alternate location
        gj_path = HERE.parent / "app" / "data" / "pakistan_districts_province_boundries.geojson"
    with open(gj_path) as f:
        gj = json.load(f)
    index = {}
    for feat in gj["features"]:
        p = feat["properties"]
        name = p.get("districts") or p.get("district_agency") or ""
        province = p.get("province_territory", "")
        key = norm(name)
        index[key] = {"display": name, "province": province}
    return index

# ── Survey adjustment helpers ────────────────────────────────────────────────
# Minimum sample-size threshold and post-stratification to census totals.

MIN_SAMPLE_SIZE = 30  # suppress district estimates with fewer observations

def _load_census_pop(tables_so_far):
    """Extract 2023 census population by sex for post-stratification.

    Returns: { norm_district: { 'pop_total': N, 'pop_male': N, 'pop_female': N } }
    Pulls from data already loaded by Table 1 (2023).
    """
    pop = {}
    for dk, vals in tables_so_far.items():
        total = vals.get("t1_2023_pop_total")
        male = vals.get("t1_2023_pop_male")
        female = vals.get("t1_2023_pop_female")
        if total and male and female:
            pop[dk] = {"pop_total": total, "pop_male": male, "pop_female": female}
    return pop


def _poststratify_sex(grp, weight_col, sex_col, census_male, census_female):
    """Compute sex-ratio post-stratification adjustment factors.

    Reweights survey observations so that the weighted male/female totals in
    this district match the 2023 census male/female population counts.

    Returns a Series of adjusted weights (same index as grp).
    """
    import pandas as pd

    w = grp[weight_col].copy()
    sex = grp[sex_col]

    w_male = w[sex == 1].sum()
    w_female = w[sex == 2].sum()
    w_total = w_male + w_female

    if w_total <= 0 or w_male <= 0 or w_female <= 0:
        return w  # can't adjust, return original weights

    census_total = census_male + census_female
    # Survey sex proportions vs census sex proportions
    adj_male = (census_male / census_total) / (w_male / w_total)
    adj_female = (census_female / census_total) / (w_female / w_total)

    adj = pd.Series(1.0, index=grp.index)
    adj[sex == 1] = adj_male
    adj[sex == 2] = adj_female
    # For transgender/other (very rare in LFS), keep original weight
    return w * adj


def _suppress_low_n(out, prefix, n_obs, threshold=MIN_SAMPLE_SIZE):
    """For a district output dict, set all indicators to None if n < threshold.

    Always stores the observation count and a low_n flag.
    """
    out[f"{prefix}n_obs"] = n_obs
    if n_obs < threshold:
        out[f"{prefix}low_n"] = True
        # Null out all indicators (keep n_obs and low_n)
        for k in list(out.keys()):
            if k.startswith(prefix) and k not in (f"{prefix}n_obs", f"{prefix}low_n"):
                out[k] = None
    else:
        out[f"{prefix}low_n"] = False


# ── Table loaders ────────────────────────────────────────────────────────────
# Each returns: { norm_district: { indicator: value, ... } }

def load_table1(path, year):
    """Table 1: area, population, density, urbanisation, household size, growth."""
    prefix = f"t1_{year}_"
    rows = read_csv(path)
    out = {}
    for r in rows:
        key = apply_crosswalk(norm(r.get("district", "")))
        if not key:
            continue
        vals = {
            f"{prefix}area_sq_km":           to_num(r.get("area_sq_km")),
            f"{prefix}pop_total":            to_num(r.get("population_all") or r.get("population_2023") or r.get("population_2017")),
            f"{prefix}pop_male":             to_num(r.get("population_male")),
            f"{prefix}pop_female":           to_num(r.get("population_female")),
            f"{prefix}pop_transgender":      to_num(r.get("population_transgender")),
            # Rate fields — stored per-row, will be recomputed for merged districts
            f"{prefix}avg_household_size":   round(v, 2) if (v := to_num(r.get("avg_household_size"))) is not None else None,
            f"{prefix}annual_growth_rate":   round(v, 2) if (v := to_num(r.get("annual_growth_rate"))) is not None else None,
            f"{prefix}urban_proportion":     round(v, 2) if (v := to_num(r.get("urban_proportion"))) is not None else None,
        }
        accumulate(out, key, vals)

    # Recompute derived fields from summed counts
    for key, d in out.items():
        pop = d.get(f"{prefix}pop_total")
        male = d.get(f"{prefix}pop_male")
        female = d.get(f"{prefix}pop_female")
        area = d.get(f"{prefix}area_sq_km")
        d[f"{prefix}sex_ratio"] = round(male / female * 100, 2) if female and male else None
        d[f"{prefix}density_per_sq_km"] = round(pop / area, 2) if area and pop else None
        # For merged districts (Karachi, Kohistan, etc.), rate fields from accumulate
        # are sums of rates which is wrong.  Null them out; urban_proportion is
        # recomputed from Table 5 later.  Household size and growth rate stay as-is
        # for non-merged districts (the first row's value is kept by accumulate).
        if key in _MERGED_DISTRICTS:
            d[f"{prefix}urban_proportion"] = None  # recomputed from T5
            d[f"{prefix}avg_household_size"] = None  # not recomputable
            d[f"{prefix}annual_growth_rate"] = None  # not recomputable
    return out

def load_table5(path, year):
    """Table 5: population by urban/rural."""
    prefix = f"t5_{year}_"
    rows = read_csv(path)
    out = {}
    for r in rows:
        key = apply_crosswalk(norm(r.get("district", "")))
        if not key:
            continue
        vals = {
            f"{prefix}total_all":       to_num(r.get("total_all_sexes")),
            f"{prefix}total_male":      to_num(r.get("total_male")),
            f"{prefix}total_female":    to_num(r.get("total_female")),
            f"{prefix}rural_all":       to_num(r.get("rural_all_sexes")),
            f"{prefix}rural_male":      to_num(r.get("rural_male")),
            f"{prefix}rural_female":    to_num(r.get("rural_female")),
            f"{prefix}urban_all":       to_num(r.get("urban_all_sexes")),
            f"{prefix}urban_male":      to_num(r.get("urban_male")),
            f"{prefix}urban_female":    to_num(r.get("urban_female")),
        }
        accumulate(out, key, vals)
    return out

def load_table12_2017(path):
    """Table 12 (2017): literacy rates."""
    prefix = "t12_2017_"
    rows = read_csv(path)
    out = {}
    for r in rows:
        key = apply_crosswalk(norm(r.get("district", "")))
        if not key:
            continue
        vals = {
            f"{prefix}literate_all":          to_num(r.get("literate_all")),
            f"{prefix}illiterate_all":        to_num(r.get("illiterate_all")),
            f"{prefix}pop_all":               to_num(r.get("population_all")),
            f"{prefix}literate_male":         to_num(r.get("literate_male")),
            f"{prefix}pop_male":              to_num(r.get("population_male")),
            f"{prefix}literate_female":       to_num(r.get("literate_female")),
            f"{prefix}pop_female":            to_num(r.get("population_female")),
        }
        accumulate(out, key, vals)

    # Recompute rates from summed counts
    for key, d in out.items():
        pop = d.get(f"{prefix}pop_all")
        lit = d.get(f"{prefix}literate_all")
        d[f"{prefix}literacy_ratio_all"] = round(lit / pop * 100, 2) if pop and lit is not None else None
        pop_m = d.get(f"{prefix}pop_male")
        lit_m = d.get(f"{prefix}literate_male")
        d[f"{prefix}literacy_ratio_male"] = round(lit_m / pop_m * 100, 2) if pop_m and lit_m is not None else None
        pop_f = d.get(f"{prefix}pop_female")
        lit_f = d.get(f"{prefix}literate_female")
        d[f"{prefix}literacy_ratio_female"] = round(lit_f / pop_f * 100, 2) if pop_f and lit_f is not None else None
        # Clean up intermediate fields
        for k in [f"{prefix}pop_all", f"{prefix}literate_male", f"{prefix}pop_male",
                   f"{prefix}literate_female", f"{prefix}pop_female"]:
            d.pop(k, None)
    return out

def load_table12_2023(path):
    """Table 12 (2023): long-format with tehsil-level data nested under DISTRICT header rows.
    We aggregate literacy data from the first sub-row after each DISTRICT row that contains
    'literacy ratio' or 'literate'/'illiterate' data."""
    prefix = "t12_2023_"
    rows = read_csv(path)
    # First pass: collect per raw-district data (before crosswalk merges)
    raw_out = {}  # raw_district_name -> {indicators}
    current_district_raw = None
    current_district_key = None

    for r in rows:
        indicator = (r.get("indicator") or "").strip()
        indicator_lower = indicator.lower()

        # Detect district header rows (e.g. "ABBOTTABAD DISTRICT")
        if "DISTRICT" in indicator:
            name = re.sub(r"\s+DISTRICT\s*$", "", indicator, flags=re.I).strip()
            current_district_key = apply_crosswalk(norm(name))
            current_district_raw = norm(name)
            if current_district_raw and current_district_raw not in raw_out:
                raw_out[current_district_raw] = {"_key": current_district_key}
            continue

        if not current_district_raw or not current_district_key:
            continue

        d = raw_out.get(current_district_raw, {})

        # Look for literacy % row — only keep the FIRST occurrence per raw district
        if indicator_lower in ("literate %", "literacy ratio", "literacy rate", "literacy %"):
            if f"{prefix}literacy_ratio_all" not in d:
                v_all = to_num(r.get("total_all_sexes"))
                v_m = to_num(r.get("male"))
                v_f = to_num(r.get("female"))
                d[f"{prefix}literacy_ratio_all"]    = round(v_all, 2) if v_all is not None else None
                d[f"{prefix}literacy_ratio_male"]   = round(v_m, 2) if v_m is not None else None
                d[f"{prefix}literacy_ratio_female"] = round(v_f, 2) if v_f is not None else None
        elif indicator_lower.startswith("literate") and ">=" in indicator_lower:
            if f"{prefix}literate_all" not in d:
                d[f"{prefix}literate_all"] = to_num(r.get("total_all_sexes"))
                d[f"{prefix}literate_male"] = to_num(r.get("male"))
                d[f"{prefix}literate_female"] = to_num(r.get("female"))
        elif indicator_lower == "population >=10":
            if f"{prefix}pop_10plus" not in d:
                d[f"{prefix}pop_10plus"] = to_num(r.get("total_all_sexes"))
                d[f"{prefix}pop_10plus_male"] = to_num(r.get("male"))
                d[f"{prefix}pop_10plus_female"] = to_num(r.get("female"))
        elif indicator_lower.startswith("never to school") and "all" in indicator_lower:
            if f"{prefix}never_school_all" not in d:
                d[f"{prefix}never_school_all"] = to_num(r.get("total_all_sexes"))
        elif indicator_lower.startswith("out of school"):
            if f"{prefix}out_of_school_5_16" not in d:
                d[f"{prefix}out_of_school_5_16"] = to_num(r.get("total_all_sexes"))
        elif indicator_lower == "ever attended":
            if f"{prefix}ever_attended" not in d:
                d[f"{prefix}ever_attended"] = to_num(r.get("total_all_sexes"))

    # Accumulate into merged districts
    out = {}
    for raw_name, d in raw_out.items():
        key = d.pop("_key", None)
        if not key or not d:
            continue
        accumulate(out, key, d)

    # For merged districts, literacy_ratio fields got summed (wrong for rates).
    # Recompute from aggregated counts: literate / pop_10plus * 100
    for key in _MERGED_DISTRICTS:
        if key in out:
            lit = out[key].get(f"{prefix}literate_all")
            pop10 = out[key].get(f"{prefix}pop_10plus")
            if lit is not None and pop10 and pop10 > 0:
                out[key][f"{prefix}literacy_ratio_all"] = round(lit / pop10 * 100, 2)
            else:
                out[key].pop(f"{prefix}literacy_ratio_all", None)
            # Recompute male/female ratios from aggregated gender counts
            lit_m = out[key].get(f"{prefix}literate_male")
            pop10_m = out[key].get(f"{prefix}pop_10plus_male")
            if lit_m is not None and pop10_m and pop10_m > 0:
                out[key][f"{prefix}literacy_ratio_male"] = round(lit_m / pop10_m * 100, 2)
            else:
                out[key].pop(f"{prefix}literacy_ratio_male", None)
            lit_f = out[key].get(f"{prefix}literate_female")
            pop10_f = out[key].get(f"{prefix}pop_10plus_female")
            if lit_f is not None and pop10_f and pop10_f > 0:
                out[key][f"{prefix}literacy_ratio_female"] = round(lit_f / pop10_f * 100, 2)
            else:
                out[key].pop(f"{prefix}literacy_ratio_female", None)

    # Compute illiterate_all = pop_10plus - literate_all
    for key, vals in out.items():
        pop10 = vals.get(f"{prefix}pop_10plus")
        lit = vals.get(f"{prefix}literate_all")
        if pop10 is not None and lit is not None:
            vals[f"{prefix}illiterate_all"] = round(pop10 - lit)
        # Clean up intermediate fields (not needed in final output)
        for tmp in (f"{prefix}pop_10plus", f"{prefix}pop_10plus_male", f"{prefix}pop_10plus_female",
                    f"{prefix}literate_male", f"{prefix}literate_female"):
            vals.pop(tmp, None)

    out = {k: v for k, v in out.items() if v}
    return out

def load_education_table_clean(path, year):
    """Load education from a clean combined CSV (2017 format: one row per district)."""
    prefix = f"t_edu_{year}_"
    rows = read_csv(path)
    out = {}
    for r in rows:
        key = apply_crosswalk(norm(r.get("district", "")))
        if not key:
            continue
        vals = {
            f"{prefix}total":         to_num(r.get("total_literate") or r.get("total_population")),
            f"{prefix}below_primary": to_num(r.get("below_primary")),
            f"{prefix}primary":       to_num(r.get("primary")),
            f"{prefix}middle":        to_num(r.get("middle")),
            f"{prefix}matric":        to_num(r.get("matric")),
            f"{prefix}intermediate":  to_num(r.get("intermediate")),
            f"{prefix}graduate":      to_num(r.get("graduate")),
            f"{prefix}masters_above": to_num(r.get("masters_above")),
        }
        accumulate(out, key, vals)
    return out

def _parse_edu_sindh_format(fpath, prefix):
    """Parse Sindh-style Table 13: DISTRICT, LOCALITY, SEX, AGE GROUP columns."""
    out = {}
    rows = read_csv(fpath)
    for r in rows:
        district = r.get("DISTRICT", "").strip()
        locality = r.get("LOCALITY", "").strip()
        sex = r.get("SEX", "").strip()
        age = r.get("SEX/ AGE GROUP (IN YEARS)", "").strip()

        if locality != "ALL LOCALITIES" or sex != "ALL SEXES":
            continue
        if "5 &" not in age and "5 AND" not in age.upper():
            continue

        key = apply_crosswalk(norm(district))
        if not key:
            continue

        grad2 = to_num(r.get("GRADUATE (2 YEARS)"))
        grad4 = to_num(r.get("GRADUATE (4 YEARS)"))
        masters = to_num(r.get("MASTERS"))
        mphil = to_num(r.get("M.Phil/Ph.D"))
        graduate = (grad2 or 0) + (grad4 or 0) if (grad2 is not None or grad4 is not None) else None
        masters_above = (masters or 0) + (mphil or 0) if (masters is not None or mphil is not None) else None

        out[key] = {
            f"{prefix}total":         to_num(r.get("TOTAL")),
            f"{prefix}never_attended":to_num(r.get("NEVER ATTENDED")),
            f"{prefix}below_primary": to_num(r.get("BELOW PRIMARY")),
            f"{prefix}primary":       to_num(r.get("PRIMARY")),
            f"{prefix}middle":        to_num(r.get("MIDDLE")),
            f"{prefix}matric":        to_num(r.get("MATRIC")),
            f"{prefix}intermediate":  to_num(r.get("INTERMEDIATE")),
            f"{prefix}graduate":      graduate,
            f"{prefix}masters_above": masters_above,
        }
    return out

def _parse_edu_hierarchical_format(fpath, prefix):
    """Parse hierarchical Table 13: DISTRICT header rows, then sub-rows.
    Structure per district:
      DISTRICT_NAME DISTRICT,,,,,,...
      ALL LOCALITIES,,,,,,...
      ALL SEXES,,,,,,...
      5 &  ABOVE, total, never_attended, below_primary, primary, middle, matric, intermediate, grad2, grad4, masters, mphil, diploma, others
    Numbers may have commas and quotes: ' 11,151,025 '
    """
    import csv as _csv
    out = {}
    # Use csv.reader to properly handle quoted fields with commas in numbers
    with open(fpath, newline="", encoding="utf-8-sig") as f:
        raw_lines = f.readlines()

    # Find data start (after "1,2,3,..." row)
    data_start = 0
    for i, line in enumerate(raw_lines):
        if line.strip().startswith("1,2,"):
            data_start = i + 1
            break

    current_district = None
    in_all_localities = False
    in_all_sexes = False
    got = set()

    for line in raw_lines[data_start:]:
        # Parse with csv.reader to handle quoted fields like " 11,151,025 "
        parsed = list(_csv.reader([line]))[0]
        first = (parsed[0] or "").strip()
        first_upper = first.upper()

        # District header
        if first_upper.endswith("DISTRICT") or first_upper.endswith("PROTECTED AREA"):
            name = re.sub(r"\s+(DISTRICT|PROTECTED AREA)\s*$", "", first, flags=re.I).strip()
            current_district = apply_crosswalk(norm(name))
            in_all_localities = False
            in_all_sexes = False
            continue

        # Tehsil/taluka = stop
        if any(kw in first_upper for kw in ("TEHSIL", "TALUKA", "SUB-DIVISION", "SUB DIVISION")):
            current_district = None
            in_all_localities = False
            in_all_sexes = False
            continue

        if not current_district or current_district in got:
            continue

        # Track locality/sex context
        if first_upper == "ALL LOCALITIES":
            in_all_localities = True
            in_all_sexes = False
            continue
        if first_upper == "ALL SEXES" and in_all_localities:
            in_all_sexes = True
            continue
        if first_upper in ("MALE", "FEMALE", "TRANSGENDER"):
            in_all_sexes = False
            continue
        if first_upper in ("RURAL", "URBAN"):
            in_all_localities = False
            in_all_sexes = False
            continue

        # The row we want: "5 & ABOVE" under ALL LOCALITIES + ALL SEXES
        if in_all_localities and in_all_sexes and "5 &" in first_lower(first):
            # Columns: 0=age, 1=total, 2=never, 3=below_primary, 4=primary, 5=middle,
            #          6=matric, 7=intermediate, 8=grad2, 9=grad4, 10=masters, 11=mphil, 12=diploma, 13=others
            def _g(idx):
                return to_num(parsed[idx]) if len(parsed) > idx else None

            grad2 = _g(8); grad4 = _g(9)
            masters = _g(10); mphil = _g(11)
            graduate = (grad2 or 0) + (grad4 or 0) if (grad2 is not None or grad4 is not None) else None
            masters_above = (masters or 0) + (mphil or 0) if (masters is not None or mphil is not None) else None

            out[current_district] = {
                f"{prefix}total":         _g(1),
                f"{prefix}never_attended":_g(2),
                f"{prefix}below_primary": _g(3),
                f"{prefix}primary":       _g(4),
                f"{prefix}middle":        _g(5),
                f"{prefix}matric":        _g(6),
                f"{prefix}intermediate":  _g(7),
                f"{prefix}graduate":      graduate,
                f"{prefix}masters_above": masters_above,
            }
            got.add(current_district)
            in_all_sexes = False

    return out

def first_lower(s):
    """Helper: lowercase version of a string."""
    return s.lower()

def load_education_2023_raw(raw_dir):
    """Table 13 (2023): Read all raw per-province CSVs.
    Sindh has a different format (flat with DISTRICT/LOCALITY/SEX/AGE columns),
    while other provinces use a hierarchical format."""
    prefix = "t_edu_2023_"
    out = {}
    for fpath in sorted(raw_dir.glob("table_13_*.csv")):
        # Detect format: Sindh has "DISTRICT,LOCALITY,SEX" header; others have a table title
        with open(fpath, encoding="utf-8-sig") as f:
            first_line = f.readline()

        if "LOCALITY" in first_line.upper():
            parsed = _parse_edu_sindh_format(fpath, prefix)
        else:
            parsed = _parse_edu_hierarchical_format(fpath, prefix)

        # Use accumulate instead of dict.update to handle merged districts
        for key, vals in parsed.items():
            accumulate(out, key, vals)
        print(f"    {fpath.name}: {len(parsed)} districts")
    return out

def load_employment_table_clean(path, year):
    """Load employment from a clean combined CSV (2017 format: one row per district)."""
    prefix = f"t_emp_{year}_"
    rows = read_csv(path)
    out = {}
    for r in rows:
        key = apply_crosswalk(norm(r.get("district", "")))
        if not key:
            continue
        vals = {
            f"{prefix}total":         to_num(r.get("total_population")),
            f"{prefix}worked":        to_num(r.get("worked")),
            f"{prefix}seeking_work":  to_num(r.get("seeking_work")),
            f"{prefix}student":       to_num(r.get("student")),
            f"{prefix}house_keeping": to_num(r.get("house_keeping")),
        }
        accumulate(out, key, vals)
    return out

def load_employment_2023_raw(raw_dir):
    """Table 14 (2023): Read raw per-province CSVs directly.
    Format: Hierarchical — 'NAME DISTRICT' header row, then indicator rows
    like 'Population', 'Employed', 'Unemployed', 'Not L.F & Stud'.
    Columns: indicator, total, male, female, trans, rural_total, ..., urban_total, ..."""
    prefix = "t_emp_2023_"
    out = {}
    for fpath in sorted(raw_dir.glob("table_14_*.csv")):
        with open(fpath, newline="", encoding="utf-8-sig") as f:
            raw_lines = f.readlines()

        # Use a raw_name tracker so each raw sub-district (e.g. "karachi central")
        # is parsed once, then accumulated into the merged key
        current_raw = None
        current_key = None
        got_raw = set()   # track raw names, not merged keys
        pending = {}      # raw_name -> {indicator vals}

        for line in raw_lines:
            line = line.strip()
            if not line or line.startswith("1,2,"):
                continue
            parts = [p.strip().strip('"') for p in line.split(",")]
            first = parts[0]

            if first.upper().endswith("DISTRICT"):
                # Flush previous district
                if current_raw and current_raw in pending:
                    accumulate(out, current_key, pending[current_raw])
                name = re.sub(r"\s+DISTRICT\s*$", "", first, flags=re.I).strip()
                current_raw = norm(name)
                current_key = apply_crosswalk(current_raw)
                if current_raw not in got_raw:
                    pending[current_raw] = {}
                continue

            if any(kw in first.upper() for kw in ("TEHSIL", "TALUKA", "SUB-DIVISION", "SUB DIVISION")):
                if current_raw and current_raw in pending:
                    accumulate(out, current_key, pending[current_raw])
                    got_raw.add(current_raw)
                current_raw = None
                current_key = None
                continue

            if not current_raw or not current_key or current_raw in got_raw:
                continue

            indicator = first.lower().strip()
            total_val = to_num(parts[1]) if len(parts) > 1 else None

            if indicator == "population":
                pending[current_raw][f"{prefix}total"] = total_val
            elif indicator == "employed":
                pending[current_raw][f"{prefix}worked"] = total_val
            elif indicator == "unemployed":
                pending[current_raw][f"{prefix}seeking_work"] = total_val
            elif "not l.f" in indicator and "stud" in indicator:
                pending[current_raw][f"{prefix}student"] = total_val
                accumulate(out, current_key, pending[current_raw])
                got_raw.add(current_raw)

        # Flush any remaining
        if current_raw and current_raw in pending and current_raw not in got_raw:
            accumulate(out, current_key, pending[current_raw])

        print(f"    {fpath.name}: parsed")

    out = {k: v for k, v in out.items() if v}
    return out

def load_table15_2017(path):
    """Table 15 (2017): education attainment (population 5+ by highest level completed)."""
    prefix = "t15_2017_"
    rows = read_csv(path)
    out = {}
    for r in rows:
        key = apply_crosswalk(norm(r.get("district", "")))
        if not key:
            continue
        vals = {
            f"{prefix}total":              to_num(r.get("total_population")),
            f"{prefix}never_attended":     to_num(r.get("never_attended")),
            f"{prefix}below_primary":      to_num(r.get("below_primary")),
            f"{prefix}primary":            to_num(r.get("primary")),
            f"{prefix}middle":             to_num(r.get("middle")),
            f"{prefix}matric":             to_num(r.get("matric")),
            f"{prefix}intermediate":       to_num(r.get("intermediate")),
            f"{prefix}graduate":           to_num(r.get("graduate")),
            f"{prefix}masters_above":      to_num(r.get("masters_above")),
            f"{prefix}diploma_certificate":to_num(r.get("diploma_certificate")),
        }
        accumulate(out, key, vals)

    # Recompute derived rates from summed counts
    for key, d in out.items():
        total = d.get(f"{prefix}total")
        never = d.get(f"{prefix}never_attended")
        d[f"{prefix}pct_never_attended"] = round(never / total * 100, 2) if total and never is not None else None
        matric_plus = sum(filter(None, [
            d.get(f"{prefix}matric"), d.get(f"{prefix}intermediate"),
            d.get(f"{prefix}graduate"), d.get(f"{prefix}masters_above"),
            d.get(f"{prefix}diploma_certificate"),
        ]))
        d[f"{prefix}pct_matric_plus"] = round(matric_plus / total * 100, 2) if total else None
    return out


def load_table16_2017(path):
    """Table 16 (2017): economic activity (population 10+ by usual activity)."""
    prefix = "t16_2017_"
    rows = read_csv(path)
    out = {}
    for r in rows:
        key = apply_crosswalk(norm(r.get("district", "")))
        if not key:
            continue
        vals = {
            f"{prefix}total":              to_num(r.get("total_population")),
            f"{prefix}worked":             to_num(r.get("worked")),
            f"{prefix}seeking_work":       to_num(r.get("seeking_work")),
            f"{prefix}student":            to_num(r.get("student")),
            f"{prefix}house_keeping":      to_num(r.get("house_keeping")),
            f"{prefix}others":             to_num(r.get("others")),
        }
        accumulate(out, key, vals)

    # Recompute derived rates from summed counts
    for key, d in out.items():
        total = d.get(f"{prefix}total")
        worked = d.get(f"{prefix}worked")
        seeking = d.get(f"{prefix}seeking_work")
        labour_force = (worked or 0) + (seeking or 0) if (worked is not None or seeking is not None) else None
        d[f"{prefix}lfpr"] = round(labour_force / total * 100, 2) if total and labour_force is not None else None
        d[f"{prefix}unemployment_rate"] = round(seeking / labour_force * 100, 2) if labour_force and seeking is not None else None
        d[f"{prefix}employment_ratio"] = round(worked / total * 100, 2) if total and worked is not None else None
    return out


# ── PSLM 2019-20 aggregation ────────────────────────────────────────────────

# Crosswalk: PSLM district label → normalised GeoJSON name
PSLM_DISTRICT_CROSSWALK = {
    "bajur": "bajaur agency",
    "bunair": "buner",
    "charsada": "charsadda",
    "d. i. khan": "dera ismail khan",
    "tor garh": "tor ghar",
    "d. g. khan": "dera ghazi khan",
    "jehlum": "jhelum",
    "t.t. singh": "toba tek singh",
    "muzaffar garh": "muzaffargarh",
    "mir pur khas": "mirpurkhas",
    "nowshero feroze": "naushehro feroze",
    "shahdadkot": "kambar shahdadkot",
    "shaheed banazir abad": "shaheed benazirabad",
    "sujawal": "sajawal",
    "umer kot": "umerkot",
    "kachhi/ bolan": "kachhi",
    "kech/turbat": "kech",
    "nasirabad/ tamboo": "nasirabad",
    "musa khel": "musakhail",
    "shaheed sikandar abad": "sherani",
    "sibbi": "sibi",
    "bhakhar": "bhakkar",
    # Karachi sub-districts → single Karachi polygon
    "karachi central": "karachi",
    "karachi east": "karachi",
    "karachi malir": "karachi",
    "karachi south": "karachi",
    "karachi west": "karachi",
    "korangi": "karachi",
}


def load_pslm(pslm_dir):
    """Aggregate PSLM 2019-20 microdata to district-level indicators.

    Key design: map district codes to GeoJSON keys BEFORE groupby, so
    Karachi sub-districts (7), Kohistan (3), Chitral (2) are naturally
    aggregated with proper survey weights.

    Returns: { norm_district: { indicator: value, ... } }
    """
    import pandas as pd

    prefix = "pslm_"
    out = {}

    # ── Load weight file ───────────────────────────────────────────
    weight_path = pslm_dir / "weight_file.dta"
    wdf = pd.read_stata(str(weight_path), convert_categoricals=False)
    wdf["psu"] = wdf["psu"].astype(int)
    weights = wdf.set_index("psu")["weights"].to_dict()

    # ── Load district labels & build code→geojson_key map ──────────
    with pd.io.stata.StataReader(str(pslm_dir / "roster.dta")) as reader:
        dist_labels = reader.value_labels().get("district", {})

    dist_code_to_key = {}
    for code, label in dist_labels.items():
        label_lower = label.strip().lower()
        if label_lower in PSLM_DISTRICT_CROSSWALK:
            dist_code_to_key[code] = PSLM_DISTRICT_CROSSWALK[label_lower]
        else:
            dist_code_to_key[code] = apply_crosswalk(norm(label_lower))

    # ── 1. Literacy, numeracy, school attendance (secc1 + roster) ──
    print("  Loading PSLM education (secc1 + roster)...")
    roster = pd.read_stata(str(pslm_dir / "roster.dta"), convert_categoricals=False,
                           columns=["hhcode", "psu", "district", "idc", "age", "sb1q4"])
    roster["psu_int"] = roster["psu"].astype(int)
    roster["w"] = roster["psu_int"].map(weights)
    # Map district code to merged GeoJSON key
    roster["dk"] = roster["district"].map(dist_code_to_key)

    edu = pd.read_stata(str(pslm_dir / "secc1.dta"), convert_categoricals=False,
                        columns=["hhcode", "psu", "district", "idc",
                                 "sc1q1a", "sc1q2a", "sc1q3a", "sc1q01", "sc1q03", "sc1q11"])
    edu = edu.merge(roster[["hhcode", "idc", "age", "sb1q4", "w", "dk"]], on=["hhcode", "idc"], how="left")

    # Group by GeoJSON key (merged districts grouped together)
    agg_edu = {}
    for dk, grp in edu.groupby("dk"):
        if not dk:
            continue
        g10 = grp[grp["age"] >= 10]
        g5_16 = grp[(grp["age"] >= 5) & (grp["age"] <= 16)]
        currently_attending = grp[grp["sc1q01"] == 3]

        w10 = g10["w"].sum()
        w5_16 = g5_16["w"].sum()

        d = {}
        if w10 > 0:
            literate = g10[(g10["sc1q1a"] == 1) & (g10["sc1q2a"] == 1)]["w"].sum()
            d[f"{prefix}literacy_rate"] = round(literate / w10 * 100, 2)
            numerate = g10[g10["sc1q3a"] == 1]["w"].sum()
            d[f"{prefix}numeracy_rate"] = round(numerate / w10 * 100, 2)
            never = g10[g10["sc1q01"] == 1]["w"].sum()
            d[f"{prefix}pct_never_attended"] = round(never / w10 * 100, 2)

        if w5_16 > 0:
            attending = g5_16[g5_16["sc1q01"] == 3]["w"].sum()
            d[f"{prefix}net_enrolment_rate"] = round(attending / w5_16 * 100, 2)

        w_att = currently_attending["w"].sum()
        if w_att > 0:
            govt = currently_attending[currently_attending["sc1q11"] == 1]["w"].sum()
            private = currently_attending[currently_attending["sc1q11"].isin([2, 7])]["w"].sum()
            d[f"{prefix}pct_govt_school"] = round(govt / w_att * 100, 2)
            d[f"{prefix}pct_private_school"] = round(private / w_att * 100, 2)

        agg_edu[dk] = d

    # ── 2. Employment (sece) ───────────────────────────────────────
    # seaq01: "did … do any work for pay, profit or family gain during the
    #          last month at least for one hour?"  1=yes, 2=no
    # This is the ILO broad definition — includes unpaid family/subsistence work.
    # We split by gender because the male-female gap is huge and drives most of
    # the cross-district variation (rural female subsistence work vs urban norms).
    print("  Loading PSLM employment (sece)...")
    emp = pd.read_stata(str(pslm_dir / "sece.dta"), convert_categoricals=False,
                        columns=["hhcode", "psu", "district", "idc", "seaq01"])
    emp = emp.merge(roster[["hhcode", "idc", "age", "sb1q4", "w", "dk"]], on=["hhcode", "idc"], how="left")

    agg_emp = {}
    for dk, grp in emp.groupby("dk"):
        if not dk:
            continue
        g10 = grp[grp["age"] >= 10]
        w10 = g10["w"].sum()
        if w10 > 0:
            employed = g10[g10["seaq01"] == 1]["w"].sum()
            d = {f"{prefix}work_participation_rate": round(employed / w10 * 100, 2)}

            # Male (sb1q4==1)
            g10m = g10[g10["sb1q4"] == 1]
            w10m = g10m["w"].sum()
            if w10m > 0:
                emp_m = g10m[g10m["seaq01"] == 1]["w"].sum()
                d[f"{prefix}work_participation_male"] = round(emp_m / w10m * 100, 2)

            # Female (sb1q4==2)
            g10f = g10[g10["sb1q4"] == 2]
            w10f = g10f["w"].sum()
            if w10f > 0:
                emp_f = g10f[g10f["seaq01"] == 1]["w"].sum()
                d[f"{prefix}work_participation_female"] = round(emp_f / w10f * 100, 2)

            agg_emp[dk] = d

    # ── 3. Housing & ICT (secf1) ──────────────────────────────────
    print("  Loading PSLM housing & ICT (secf1)...")
    housing = pd.read_stata(str(pslm_dir / "secf1.dta"), convert_categoricals=False,
                            columns=["hhcode", "psu", "district", "sf1q11_1a", "sf1q11_1b"])
    housing["psu_int"] = housing["psu"].astype(int)
    housing["w"] = housing["psu_int"].map(weights)
    housing["dk"] = housing["district"].map(dist_code_to_key)

    agg_ict = {}
    for dk, grp in housing.groupby("dk"):
        if not dk:
            continue
        wtot = grp["w"].sum()
        if wtot > 0:
            internet = grp[grp["sf1q11_1a"] == 1]["w"].sum()
            mobile = grp[grp["sf1q11_1b"] == 1]["w"].sum()
            agg_ict[dk] = {
                f"{prefix}pct_internet": round(internet / wtot * 100, 2),
                f"{prefix}pct_mobile": round(mobile / wtot * 100, 2),
            }

    # ── 4. Water & sanitation (secf2) ─────────────────────────────
    print("  Loading PSLM water & sanitation (secf2)...")
    wash = pd.read_stata(str(pslm_dir / "secf2.dta"), convert_categoricals=False,
                         columns=["hhcode", "psu", "district", "sf2q01", "sf2q11"])
    wash["psu_int"] = wash["psu"].astype(int)
    wash["w"] = wash["psu_int"].map(weights)
    wash["dk"] = wash["district"].map(dist_code_to_key)

    agg_wash = {}
    for dk, grp in wash.groupby("dk"):
        if not dk:
            continue
        wtot = grp["w"].sum()
        if wtot > 0:
            piped = grp[grp["sf2q01"].isin([1, 8])]["w"].sum()
            flush = grp[grp["sf2q11"].isin([2, 3, 4, 5])]["w"].sum()
            no_toilet = grp[grp["sf2q11"] == 1]["w"].sum()
            agg_wash[dk] = {
                f"{prefix}pct_piped_water": round(piped / wtot * 100, 2),
                f"{prefix}pct_flush_toilet": round(flush / wtot * 100, 2),
                f"{prefix}pct_no_toilet": round(no_toilet / wtot * 100, 2),
            }

    # ── 5. Household size (roster) ────────────────────────────────
    print("  Loading PSLM household size...")
    hh_sizes = roster.groupby(["hhcode", "dk"]).agg(
        members=("idc", "count"),
        w=("w", "first")
    ).reset_index()

    agg_hh = {}
    for dk, grp in hh_sizes.groupby("dk"):
        if not dk:
            continue
        wtot = grp["w"].sum()
        if wtot > 0:
            avg_size = (grp["members"] * grp["w"]).sum() / wtot
            agg_hh[dk] = {f"{prefix}avg_hh_size": round(avg_size, 2)}

    # ── Combine all PSLM indicators ──────────────────────────────
    all_dists = set(agg_edu) | set(agg_emp) | set(agg_ict) | set(agg_wash) | set(agg_hh)
    for dk in all_dists:
        if dk not in out:
            out[dk] = {}
        for src in (agg_edu, agg_emp, agg_ict, agg_wash, agg_hh):
            if dk in src:
                out[dk].update(src[dk])

    print(f"  PSLM: {len(out)} districts with indicators")
    return out


# ── Economic Census ────────────────────────────────────────────────────────

def load_economic_census(ec_dir):
    """Parse Economic Census PSIC (Table 1) xlsx files.

    Extracts total establishments, workforce, and sector composition
    per district.  Returns: { norm_district: { ec_*: value, … } }
    """
    import pandas as pd

    prefix = "ec_"
    TABLE1_FILES = {
        "PUNJAB":      "PUNJAB-DISTRICTS-PSIC-wise-1.xlsx",
        "SINDH":       "SINDH-DISTRICTS-PSIC-WISE.xlsx",
        "KPK":         "KPK-DISTRICTPSIC-WISE.xlsx",
        "BALOCHISTAN":  "BALOCHISTAN-DISTRICTPSIC-WISE.xlsx",
    }

    raw = {}  # norm_name -> accum dict

    for province, fname in TABLE1_FILES.items():
        fpath = ec_dir / fname
        if not fpath.exists():
            print(f"    WARNING: {fname} not found, skipping")
            continue

        df = pd.read_excel(str(fpath), header=None)
        current = None
        i = 0

        while i < len(df):
            val0 = df.iloc[i, 0]

            # District header: text in col 0, next row has "Sr" in col 0
            if pd.notna(val0) and isinstance(val0, str):
                s = str(val0).strip()
                # skip file-level title rows
                if any(kw in s for kw in ("Table 1", "Table-1", "Districts with")):
                    i += 1; continue
                if i + 1 < len(df):
                    nxt = df.iloc[i + 1, 0]
                    if pd.notna(nxt) and isinstance(nxt, str) and "Sr" in str(nxt):
                        key = apply_crosswalk(norm(s))
                        if key:
                            current = key
                            if current not in raw:
                                raw[current] = {
                                    f"{prefix}total_establishments": 0,
                                    f"{prefix}total_workforce": 0,
                                    "_agriculture": 0, "_manufacturing": 0,
                                    "_trade": 0, "_construction": 0, "_services": 0,
                                }
                        i += 1; continue

            # Data row: col 0 numeric Sr.No, col 2 PSIC code, col 3 establishments, col 4 workforce
            if current and pd.notna(df.iloc[i, 2] if df.shape[1] > 2 else None):
                try:
                    sr = int(df.iloc[i, 0]) if pd.notna(df.iloc[i, 0]) else None
                except (ValueError, TypeError):
                    sr = None

                if sr is not None:
                    psic = str(df.iloc[i, 2]).strip().replace(" to ", "-").replace(" ", "")
                    est = to_num(df.iloc[i, 3]) or 0
                    wf  = to_num(df.iloc[i, 4]) or 0

                    raw[current][f"{prefix}total_establishments"] += est
                    raw[current][f"{prefix}total_workforce"] += wf

                    if psic in ("01-03", "1-3", "0103"):
                        raw[current]["_agriculture"] += est
                    elif psic in ("10-33", "1033"):
                        raw[current]["_manufacturing"] += est
                    elif psic in ("45-47", "4547"):
                        raw[current]["_trade"] += est
                    elif psic in ("41-43", "4143"):
                        raw[current]["_construction"] += est
                    else:
                        raw[current]["_services"] += est
            i += 1

        print(f"    {fname}: parsed")

    # Compute derived indicators and clean up temp keys
    out = {}
    for key, d in raw.items():
        total = d[f"{prefix}total_establishments"]
        vals = {
            f"{prefix}total_establishments": total,
            f"{prefix}total_workforce": d[f"{prefix}total_workforce"],
            f"{prefix}avg_workers_per_est": round(d[f"{prefix}total_workforce"] / total, 2) if total else None,
            f"{prefix}pct_agriculture": round(d["_agriculture"] / total * 100, 2) if total else None,
            f"{prefix}pct_manufacturing": round(d["_manufacturing"] / total * 100, 2) if total else None,
            f"{prefix}pct_trade": round(d["_trade"] / total * 100, 2) if total else None,
            f"{prefix}pct_construction": round(d["_construction"] / total * 100, 2) if total else None,
            f"{prefix}pct_services": round(d["_services"] / total * 100, 2) if total else None,
        }
        accumulate(out, key, vals)

    print(f"  Economic Census: {len(out)} districts")
    return out


# ── LFS 2020-21 ───────────────────────────────────────────────────────────

LFS_DISTRICT_CROSSWALK = {
    "east": None,       # fragment — skip
    "west": None,       # fragment — skip
    "mardan": "mardan",  # fix duplicate "Mardan" / "MARDAN"
}

def load_lfs_2021(lfs_dir, census_pop=None):
    """Aggregate LFS 2020-21 microdata to district-level labour indicators.

    Key design: reads in chunks (file is ~1 GB), maps district text names
    through crosswalk, computes weighted employment and LFPR indicators.

    Post-stratification: if census_pop is provided, reweights observations
    so that male/female weighted totals match 2023 census proportions.

    Minimum sample-size: districts with < MIN_SAMPLE_SIZE observations
    have all indicators suppressed (set to None) with a low_n flag.

    Returns: { norm_district: { lfs21_*: value, … } }
    """
    import pandas as pd

    prefix = "lfs21_"
    fpath = lfs_dir / "LFS2020-21.dta"
    if not fpath.exists():
        print(f"  WARNING: {fpath} not found")
        return {}

    columns = ["District", "Province", "Weights", "S4C5", "S4C6",
               "S5C1", "S5C2", "S5C3", "S5C7"]

    print("  Reading LFS 2020-21 in chunks…")
    chunks = []
    for chunk in pd.read_stata(str(fpath), convert_categoricals=False,
                               chunksize=100000, columns=columns):
        # Map district names through crosswalk
        chunk["dk"] = chunk["District"].apply(
            lambda x: None if pd.isna(x) else apply_crosswalk(norm(str(x)))
        )
        # Drop fragments
        chunk.loc[chunk["dk"].isin(LFS_DISTRICT_CROSSWALK) &
                  chunk["dk"].map(lambda k: LFS_DISTRICT_CROSSWALK.get(k) is None), "dk"] = None
        chunks.append(chunk)

    df = pd.concat(chunks, ignore_index=True)
    print(f"    {len(df):,} rows, {df['dk'].nunique()} mapped districts")

    n_poststrat = 0
    n_suppressed = 0
    out = {}
    for dk, grp in df.groupby("dk"):
        if not dk:
            continue

        n_obs = len(grp)
        d = {}

        g10 = grp[grp["S4C6"] >= 10]

        # ── Post-stratification: adjust weights by sex ratio ──
        wt_col = "Weights"
        if census_pop and dk in census_pop:
            cp = census_pop[dk]
            g10 = g10.copy()
            g10["_adj_wt"] = _poststratify_sex(
                g10, wt_col, "S4C5", cp["pop_male"], cp["pop_female"]
            )
            wt_col = "_adj_wt"
            n_poststrat += 1

        w10 = g10[wt_col].sum()
        if w10 <= 0:
            continue

        # Employed = S5C1==1 OR (S5C1==2 AND S5C2==1)  (ILO broad)
        employed = g10[(g10["S5C1"] == 1) | ((g10["S5C1"] == 2) & (g10["S5C2"] == 1))]
        w_emp = employed[wt_col].sum()

        # Looking for work = S5C1==2, S5C2==2, S5C3==1
        looking = g10[(g10["S5C1"] == 2) & (g10["S5C2"] == 2) & (g10["S5C3"] == 1)]
        w_look = looking[wt_col].sum()
        w_lf = w_emp + w_look

        d[f"{prefix}employment_ratio"] = round(w_emp / w10 * 100, 2)
        d[f"{prefix}lfpr"] = round(w_lf / w10 * 100, 2)
        d[f"{prefix}unemployment_rate"] = round(w_look / w_lf * 100, 2) if w_lf else None

        # By gender (S4C5: 1=male, 2=female — NOT RSex which is respondent sex)
        for code, label in [(1, "male"), (2, "female")]:
            gs = g10[g10["S4C5"] == code]
            ws = gs[wt_col].sum()
            if ws > 0:
                emp_s = gs[(gs["S5C1"] == 1) | ((gs["S5C1"] == 2) & (gs["S5C2"] == 1))][wt_col].sum()
                look_s = gs[(gs["S5C1"] == 2) & (gs["S5C2"] == 2) & (gs["S5C3"] == 1)][wt_col].sum()
                lf_s = emp_s + look_s
                d[f"{prefix}employment_ratio_{label}"] = round(emp_s / ws * 100, 2)
                d[f"{prefix}lfpr_{label}"] = round(lf_s / ws * 100, 2)

        # Youth (15-24) — subset of g10 which already has adjusted weights
        gy = g10[(g10["S4C6"] >= 15) & (g10["S4C6"] <= 24)]
        wy = gy[wt_col].sum()
        if wy > 0:
            emp_y_mask = (gy["S5C1"] == 1) | ((gy["S5C1"] == 2) & (gy["S5C2"] == 1))
            emp_y = gy.loc[emp_y_mask, wt_col].sum()
            d[f"{prefix}youth_employment_ratio"] = round(emp_y / wy * 100, 2)

        # Industry composition (S5C7: 1=agri, 3=manuf, 6=trade, rest=services)
        if w_emp > 0:
            agri = employed[employed["S5C7"] == 1][wt_col].sum()
            manuf = employed[employed["S5C7"] == 3][wt_col].sum()
            trade = employed[employed["S5C7"] == 6][wt_col].sum()
            d[f"{prefix}pct_agriculture"] = round(agri / w_emp * 100, 2)
            d[f"{prefix}pct_manufacturing"] = round(manuf / w_emp * 100, 2)
            d[f"{prefix}pct_trade"] = round(trade / w_emp * 100, 2)

        # ── Sample-size filter ──
        _suppress_low_n(d, prefix, n_obs)
        if d.get(f"{prefix}low_n"):
            n_suppressed += 1

        out[dk] = d

    print(f"  LFS 2020-21: {len(out)} districts, {n_poststrat} post-stratified, {n_suppressed} suppressed (n<{MIN_SAMPLE_SIZE})")
    return out


# ── LFS 2024-25 ───────────────────────────────────────────────────────────

def _build_lfs25_crosswalk(lfs_dir):
    """Build two crosswalks for LFS 2024-25 district identification.

    In LFS 2024-25, PCode position III = 0 means the stratum is a whole
    division (not a district).  ~42 % of observations are coded this way —
    notably ALL of Balochistan.  For these rows, PCode alone cannot identify
    the district.

    Strategy:
      1. S4C16 value labels give every LFS district code → name (authoritative).
      2. For district-level PCode (pos 3 ≠ 0): map PCode[:3] → name via S4C16.
      3. For divisional PCode (pos 3 = 0): use Census Enumeration-Block code
         (EBCode[:3]) cross-referenced with LFS 2020-21, which has text District
         names for every observation.

    Returns: (pd3_xwalk, eb_xwalk)
      pd3_xwalk: {str 3-digit code → norm name or None}  (district-level)
      eb_xwalk:  {(int province, str eb3) → norm name or None}  (via Census EB)
    """
    import pandas as pd

    fpath_21 = lfs_dir / "LFS2020-21.dta"
    fpath_25 = lfs_dir / "LFS 2024-25.sav web.dta"

    # ── S4C16 value labels from 2024-25 (authoritative district codes) ──
    with pd.io.stata.StataReader(str(fpath_25)) as reader:
        value_labels = reader.value_labels()
    s4c16 = value_labels.get("S4C16", {})
    lfs25_code_to_name = {k: v.strip().upper() for k, v in s4c16.items() if k < 900}

    # ── pd3_xwalk: PCode[:3] → normalised GeoJSON name ──
    pd3_xwalk = {}
    for code, name in lfs25_code_to_name.items():
        code_str = str(code)
        geo = apply_crosswalk(norm(name))
        if geo:
            pd3_xwalk[code_str] = geo

    mapped_pd3 = sum(1 for v in pd3_xwalk.values() if v)
    print(f"  LFS 2024-25 pd3 crosswalk: {mapped_pd3} codes → GeoJSON names")

    # ── Census EB mapping from LFS 2020-21 ──
    # In 2020-21, every obs has text District + EBCode.  Build
    # (province, EBCode[:3]) → district_name from that data.
    census_eb_to_dist = {}  # (int prov, str eb3) → UPPER district name
    for chunk in pd.read_stata(str(fpath_21), convert_categoricals=False,
                               chunksize=100000,
                               columns=["Province", "EBCode", "District"]):
        valid = chunk[chunk["EBCode"].notna() & chunk["District"].notna()
                      & chunk["Province"].notna()]
        for _, row in valid.iterrows():
            prov = int(row["Province"])
            eb3 = str(int(row["EBCode"]))[:3]
            dist = row["District"].strip().upper()
            key = (prov, eb3)
            if key not in census_eb_to_dist:
                census_eb_to_dist[key] = dist
            elif census_eb_to_dist[key] != dist:
                census_eb_to_dist[key] = None  # ambiguous → drop
    census_eb_to_dist = {k: v for k, v in census_eb_to_dist.items() if v}

    # Bridge census district names → S4C16 codes → GeoJSON names
    name_to_lfs25 = {}
    for code, name in lfs25_code_to_name.items():
        name_to_lfs25[name] = code
    # Manual fixes for spelling differences between 2020-21 and 2024-25
    _NAME_FIXES = {
        "DERA GHAZI KHAN": 281,
        "DERA ISMAIL KHAN": 171,
        "KAMBAR SHAHDAD KOT": 315,
        "TANDO MUHAMMAD KHAN": 335,
        "KACHHI": 451,
        "KECH": 471,
        "SHAHEED SIKANDAR ABAD": 466,  # now SURAB
        "BAHAWALNAGAR": 292,
    }

    eb_xwalk = {}  # (int prov, str eb3) → norm GeoJSON name
    for (prov, eb3), census_name in census_eb_to_dist.items():
        lfs_code = None
        if census_name in name_to_lfs25:
            lfs_code = name_to_lfs25[census_name]
        elif census_name in _NAME_FIXES:
            lfs_code = _NAME_FIXES[census_name]
        else:
            # Try partial match
            for lfs_name, lfs_c in name_to_lfs25.items():
                if census_name.replace(" ", "") == lfs_name.replace(" ", "").replace(".", "").replace("/", ""):
                    lfs_code = lfs_c
                    break
        if lfs_code and str(lfs_code) in pd3_xwalk:
            eb_xwalk[(prov, eb3)] = pd3_xwalk[str(lfs_code)]

    print(f"  LFS 2024-25 Census-EB crosswalk: {len(eb_xwalk)} (prov,EB[:3]) → GeoJSON")
    return pd3_xwalk, eb_xwalk


def load_lfs_2025(lfs_dir, census_pop=None):
    """Aggregate LFS 2024-25 microdata to district-level labour indicators.

    Employment (ILO broad): S5C1==1 OR S5C2==1 OR S5C3==1
      These are skip-pattern questions — S5C2 only asked if S5C1!=1,
      S5C3 only if S5C2!=1 — so NaN means the person was already captured
      by an earlier question.

    Industry (ISIC Rev 4, column S5C13): division 01-03 = Agriculture,
      10-33 = Manufacturing, 45-47 = Wholesale & Retail Trade.

    District identification uses TWO methods:
      - PCode[:3] for district-level strata (position 3 ≠ 0)
      - Census EBCode[:3] for divisional strata (position 3 = 0)

    Post-stratification: if census_pop is provided, reweights by sex ratio.
    Minimum sample-size: districts with < MIN_SAMPLE_SIZE obs are suppressed.

    Returns: { norm_district: { lfs25_*: value, … } }
    """
    import pandas as pd

    prefix = "lfs25_"
    fpath = lfs_dir / "LFS 2024-25.sav web.dta"
    if not fpath.exists():
        print(f"  WARNING: {fpath} not found")
        return {}

    pd3_xwalk, eb_xwalk = _build_lfs25_crosswalk(lfs_dir)

    columns = ["PCode", "EBCode", "Province", "Weights", "S4C5", "S4C6",
               "S5C1", "S5C2", "S5C3", "S9C1", "S5C13"]

    print("  Reading LFS 2024-25 in chunks…")
    chunks = []
    for chunk in pd.read_stata(str(fpath), convert_categoricals=False,
                               chunksize=100000, columns=columns):
        chunk = chunk.copy()
        chunk["pd3"] = chunk["PCode"].astype(str).str[:3]
        is_div = chunk["pd3"].str.endswith("0")

        # District-level: map via PCode[:3]
        chunk.loc[~is_div, "dk"] = chunk.loc[~is_div, "pd3"].map(pd3_xwalk)

        # Divisional-level: map via (Province, EBCode[:3])
        div_mask = is_div & chunk["EBCode"].notna() & chunk["Province"].notna()
        if div_mask.any():
            div_rows = chunk.loc[div_mask]
            eb3 = div_rows["EBCode"].astype(int).astype(str).str[:3]
            prov = div_rows["Province"].astype(int)
            chunk.loc[div_mask, "dk"] = [
                eb_xwalk.get((p, e)) for p, e in zip(prov, eb3)
            ]

        # Drop unmapped rows
        chunk = chunk[chunk["dk"].notna()]
        chunks.append(chunk)

    df = pd.concat(chunks, ignore_index=True)
    print(f"    {len(df):,} rows, {df['dk'].nunique()} mapped districts")

    n_poststrat = 0
    n_suppressed = 0
    out = {}
    for dk, grp in df.groupby("dk"):
        if not dk:
            continue

        n_obs = len(grp)
        d = {}

        # Age 10+ population for denominator
        g10 = grp[grp["S4C6"] >= 10]

        # ── Post-stratification: adjust weights by sex ratio ──
        wt_col = "Weights"
        if census_pop and dk in census_pop:
            cp = census_pop[dk]
            g10 = g10.copy()
            g10["_adj_wt"] = _poststratify_sex(
                g10, wt_col, "S4C5", cp["pop_male"], cp["pop_female"]
            )
            wt_col = "_adj_wt"
            n_poststrat += 1

        w10 = g10[wt_col].sum()
        if w10 <= 0:
            continue

        # ── Employment (ILO broad) ──
        # S5C1/2/3 are skip-pattern: NaN means "not asked because already
        # captured by prior question", so treat NaN as False (not matching 1).
        employed = (
            (g10["S5C1"] == 1) |
            (g10["S5C2"] == 1) |
            (g10["S5C3"] == 1)
        ).fillna(False)

        emp_rows = g10[employed]
        w_emp = emp_rows[wt_col].sum()

        # Looking for work: S9C1==1  (asked only to non-employed in LFS 2024-25)
        looking = (~employed & (g10["S9C1"] == 1)).fillna(False)
        w_look = g10.loc[looking, wt_col].sum()
        w_lf = w_emp + w_look

        d[f"{prefix}employment_ratio"] = round(w_emp / w10 * 100, 2)
        d[f"{prefix}lfpr"] = round(w_lf / w10 * 100, 2)
        d[f"{prefix}unemployment_rate"] = round(w_look / w_lf * 100, 2) if w_lf else None

        # ── By gender ──
        for code, label in [(1, "male"), (2, "female")]:
            gs = g10[g10["S4C5"] == code]
            ws = gs[wt_col].sum()
            if ws > 0:
                emp_mask_s = ((gs["S5C1"] == 1) | (gs["S5C2"] == 1) | (gs["S5C3"] == 1)).fillna(False)
                emp_s = gs[emp_mask_s][wt_col].sum()
                look_s = gs[~emp_mask_s & (gs["S9C1"] == 1).fillna(False)][wt_col].sum()
                lf_s = emp_s + look_s
                d[f"{prefix}employment_ratio_{label}"] = round(emp_s / ws * 100, 2)
                d[f"{prefix}lfpr_{label}"] = round(lf_s / ws * 100, 2)

        # ── Youth (15-24) — subset of g10 which already has adjusted weights ──
        gy = g10[(g10["S4C6"] >= 15) & (g10["S4C6"] <= 24)]
        wy = gy[wt_col].sum()
        if wy > 0:
            emp_y_mask = ((gy["S5C1"] == 1) | (gy["S5C2"] == 1) | (gy["S5C3"] == 1)).fillna(False)
            emp_y = gy.loc[emp_y_mask, wt_col].sum()
            d[f"{prefix}youth_employment_ratio"] = round(emp_y / wy * 100, 2)

        # ── Industry composition (S5C13 = ISIC Rev 4, stored without leading zero)
        # Codes like 111 = ISIC 0111 (agriculture), 4520 = ISIC 4520 (trade)
        # Zero-pad to 4 digits, then take first 2 as ISIC division.
        if w_emp > 0:
            isic = emp_rows["S5C13"].dropna().astype(int)
            div_series = isic.apply(lambda x: int(str(x).zfill(4)[:2]))

            w_isic = emp_rows.loc[isic.index, wt_col]
            agri = w_isic[div_series.between(1, 3)].sum()
            manuf = w_isic[div_series.between(10, 33)].sum()
            trade = w_isic[div_series.between(45, 47)].sum()
            d[f"{prefix}pct_agriculture"] = round(agri / w_emp * 100, 2)
            d[f"{prefix}pct_manufacturing"] = round(manuf / w_emp * 100, 2)
            d[f"{prefix}pct_trade"] = round(trade / w_emp * 100, 2)

        # ── Sample-size filter ──
        _suppress_low_n(d, prefix, n_obs)
        if d.get(f"{prefix}low_n"):
            n_suppressed += 1

        out[dk] = d

    print(f"  LFS 2024-25: {len(out)} districts, {n_poststrat} post-stratified, {n_suppressed} suppressed (n<{MIN_SAMPLE_SIZE})")
    return out


# ── HIES 2024-25 ──────────────────────────────────────────────────────────

# HIES prcode: 8 digits = division(2) + district(2) + PSU(4).
# District code = first 4 digits.  Within-division suffixes: 02, 11, 21, 31, …
_HIES_CODE_SUFFIXES = [2, 11, 21, 31, 41, 51, 61, 71, 81, 91]

# Division → [normalised GeoJSON district names, in coding-scheme order]
# Built from HIES coding scheme XLS (BIFF8 extraction) and administrative geography.
_HIES_DIVISION_DISTRICTS = {
    # KP
    11: ["chitral", "upper dir", "lower dir", "swat", "shangla", "buner", "malakand", "bajaur agency",
         "chitral", None],  # pos 9=Upper Chitral→same polygon, pos 10=unknown
    12: ["kohistan", "mansehra", "batagram", "abbottabad", "haripur", "tor ghar",
         "kohistan", "kohistan", None, None],  # 7-8 = Lower/Upper Kohistan → same polygon
    13: ["mardan", "swabi", None],
    14: ["charsadda", "peshawar", "nowshera", "khyber agency", "mohmand agency", None],
    15: ["kohat", "hangu", "karak", "kurram agency", "orakzai agency", None],
    16: ["bannu", "lakki marwat", "north waziristan agency"],
    17: ["dera ismail khan", "tank", "south waziristan agency"],
    # Punjab
    21: ["attock", "rawalpindi", "jhelum", "chakwal", None],
    22: ["sargodha", "bhakkar", "khushab", "mianwali", None],
    23: ["faisalabad", "chiniot", "jhang", "toba tek singh", None],
    24: ["gujranwala", "hafizabad", "gujrat", "mandi bahauddin", "sialkot", "narowal", None],
    25: ["lahore", "kasur", "sheikhupura", "nankana sahib", None],
    26: ["okara", "sahiwal", "pakpattan", None],
    27: ["vehari", "multan", "lodhran", "khanewal", None],
    28: ["dera ghazi khan", "rajanpur", "layyah", "muzaffargarh", None],
    29: ["bahawalpur", "bahawalnagar", "rahim yar khan", None],
    # ICT
    61: ["islamabad", "islamabad"],  # rural Islamabad → same polygon
    # Sindh
    31: ["jacobabad", "kashmore", "shikarpur", "larkana", "kambar shahdadkot", None],
    32: ["sukkur", "ghotki", "khairpur", None],
    33: ["dadu", "jamshoro", "hyderabad", "tando allah yar", "tando muhammad khan",
         "matiari", "badin", "thatta", "sajawal", None],
    34: ["mirpurkhas", "umerkot", "tharparkar", "sanghar"],
    35: ["karachi", "karachi", "karachi", "karachi", "karachi", "karachi"],
    36: ["naushehro feroze", "shaheed benazirabad", "sanghar", None],
    # Balochistan
    41: ["quetta", "pishin", "killa abdullah", "chaghi", "nushki"],
    42: ["loralai", "barkhan", "musakhail", "killa saifullah", "zhob", "sherani", None],
    43: ["sibi", "harnai", "ziarat", "kohlu", "dera bugti", "sibi"],  # pos 5 = Lehri → merged into Sibi
    44: ["kachhi", "jaffarabad", "nasirabad", "jhal magsi", "sohbatpur", None],
    # NOTE: If HIES uses post-reorganisation codes, kharan & washuk moved to div 47
    # (Rakhshan), so div 45 positions may have shifted.  Keeping old mapping for
    # backward compatibility; if lasbela still missing after div 47 fix, try moving
    # lasbela to position 4 (code 4541) in this list.
    45: ["kalat", "mastung", "khuzdar", "awaran", "kharan", "washuk", "lasbela", None, None],
    46: ["kech", "gwadar", "panjgur", None],
    # Division 47 = Rakhshan Division (est. 2017, carved from Kalat + Quetta divs)
    # Districts: Kharan (HQ), Washuk, Chagai, Nushki
    # NOTE: Chagai & Nushki may ALSO appear under div 41 (old Quetta coding).
    # The pipeline deduplicates via _MERGED_DISTRICTS or last-write-wins.
    # District ordering within division is a best guess — verify against
    # HIES coding scheme XLS if available.
    47: ["kharan", "washuk", "chaghi", "nushki", None],
    # Division 48 = Loralai Division (est. 2021, carved from Zhob div)
    # Districts: Loralai (HQ), Barkhan, Musakhail, Duki (→ merged into Loralai polygon)
    # NOTE: Loralai may ALSO appear under div 42 (old Zhob coding).
    48: ["loralai", "barkhan", "musakhail", "loralai", None],  # pos 3 = Duki → merged into Loralai
}

def _build_hies_crosswalk():
    """Build HIES 4-digit district code → normalised GeoJSON name crosswalk."""
    xwalk = {}
    for div_code, districts in _HIES_DIVISION_DISTRICTS.items():
        for i, dist_name in enumerate(districts):
            if i < len(_HIES_CODE_SUFFIXES):
                hies_code = div_code * 100 + _HIES_CODE_SUFFIXES[i]
                xwalk[hies_code] = dist_name  # None = skip
    return xwalk


def load_hies(hies_dir, census_pop=None):
    """Aggregate HIES 2024-25 to district-level household welfare indicators.

    Reads consumption expenditure, FIES food insecurity, water/sanitation,
    housing characteristics, and roster data.  Joins on weight file for
    population-representative estimates.

    Post-stratification: if census_pop is provided, calibrates household
    weights so that weighted household counts scale proportionally to
    2023 census population totals per district.

    Minimum sample-size: districts with < MIN_SAMPLE_SIZE households
    have all indicators suppressed (set to None) with a low_n flag.

    Returns: { norm_district: { hies_*: value, … } }
    """
    import pandas as pd
    import numpy as np

    prefix = "hies_"
    xwalk = _build_hies_crosswalk()
    mapped = sum(1 for v in xwalk.values() if v is not None)
    print(f"  HIES crosswalk: {mapped} codes mapped")

    # ── Load weights ──
    w = pd.read_stata(str(hies_dir / "weight.dta"), convert_categoricals=False)
    w["dist_code"] = w["prcode"].astype(str).str[:4].astype(int)
    w["dk"] = w["dist_code"].map(xwalk)

    # ── Diagnostic: show all district codes and mapping status ──
    all_codes = sorted(w["dist_code"].unique())
    unmapped = [c for c in all_codes if c not in xwalk]
    mapped_none = [c for c in all_codes if xwalk.get(c) is None and c in xwalk]
    print(f"  HIES district codes found: {len(all_codes)}")
    if unmapped:
        print(f"  ⚠ UNMAPPED codes (not in crosswalk): {unmapped}")
    if mapped_none:
        print(f"  ⚠ Codes mapped to None (skipped): {mapped_none}")
    # Show per-division summary
    div_counts = {}
    for c in all_codes:
        div = c // 100
        div_counts.setdefault(div, []).append(c)
    for div in sorted(div_counts):
        codes = div_counts[div]
        names = [xwalk.get(c, '???') for c in codes]
        print(f"    Div {div}: codes {codes} → {names}")

    # ── Roster: household size ──
    roster = pd.read_stata(str(hies_dir / "plist_roster.dta"), convert_categoricals=False,
                           columns=["prcode", "hhno", "weight"])
    roster["dist_code"] = roster["prcode"].astype(str).str[:4].astype(int)
    roster["dk"] = roster["dist_code"].map(xwalk)
    hh_size = roster.groupby(["prcode", "hhno"]).size().reset_index(name="hh_size")
    hh_size["dist_code"] = hh_size["prcode"].astype(str).str[:4].astype(int)
    hh_size["dk"] = hh_size["dist_code"].map(xwalk)

    # ── Consumption expenditure ──
    exp = pd.read_stata(str(hies_dir / "sec_6a_consum_exp.dta"), convert_categoricals=False,
                        columns=["prcode", "hhno", "itc", "v1"])
    exp["dist_code"] = exp["prcode"].astype(str).str[:4].astype(int)
    exp["dk"] = exp["dist_code"].map(xwalk)
    # Food (itc=1000), Non-food frequent (itc=2000), Non-food infrequent (itc=5000)
    food_exp = exp[exp["itc"] == 1000].groupby(["prcode", "hhno"])["v1"].sum().reset_index(name="food_monthly")
    nonfood_exp = exp[exp["itc"] == 2000].groupby(["prcode", "hhno"])["v1"].sum().reset_index(name="nonfood_monthly")
    annual_exp = exp[exp["itc"] == 5000].groupby(["prcode", "hhno"])["v1"].sum().reset_index(name="annual_items")

    hh_exp = food_exp.merge(nonfood_exp, on=["prcode", "hhno"], how="outer")
    hh_exp = hh_exp.merge(annual_exp, on=["prcode", "hhno"], how="outer")
    hh_exp = hh_exp.merge(hh_size[["prcode", "hhno", "hh_size", "dk"]], on=["prcode", "hhno"], how="left")
    hh_exp = hh_exp.merge(w[["prcode", "weight"]], on="prcode", how="left")
    hh_exp["food_monthly"] = hh_exp["food_monthly"].fillna(0)
    hh_exp["nonfood_monthly"] = hh_exp["nonfood_monthly"].fillna(0)
    hh_exp["annual_items"] = hh_exp["annual_items"].fillna(0)
    # Total annual expenditure = (food + nonfood_frequent) × 12 + annual_items
    hh_exp["total_annual"] = (hh_exp["food_monthly"] + hh_exp["nonfood_monthly"]) * 12 + hh_exp["annual_items"]
    hh_exp["food_annual"] = hh_exp["food_monthly"] * 12
    hh_exp["per_capita_monthly"] = (hh_exp["food_monthly"] + hh_exp["nonfood_monthly"]) / hh_exp["hh_size"].clip(lower=1)

    # ── FIES food insecurity (8 questions, yes=1) ──
    fies = pd.read_stata(str(hies_dir / "sec_05m4_fies.dta"), convert_categoricals=False,
                         columns=["prcode", "hhno", "q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8"])
    fies["dist_code"] = fies["prcode"].astype(str).str[:4].astype(int)
    fies["dk"] = fies["dist_code"].map(xwalk)
    fies = fies.merge(w[["prcode", "weight"]], on="prcode", how="left")
    # FIES raw score = count of "yes" (1) answers out of 8
    for q in ["q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8"]:
        fies[q] = (fies[q] == 1).astype(int)
    fies["fies_score"] = fies[["q1", "q2", "q3", "q4", "q5", "q6", "q7", "q8"]].sum(axis=1)
    # Moderate-or-severe insecurity: score >= 4 (standard threshold)
    fies["food_insecure"] = (fies["fies_score"] >= 4).astype(int)

    # ── Water source ──
    ws = pd.read_stata(str(hies_dir / "sec_05m2_watersanitation.dta"), convert_categoricals=False,
                       columns=["prcode", "hhno", "s5m2q01"])
    ws["dist_code"] = ws["prcode"].astype(str).str[:4].astype(int)
    ws["dk"] = ws["dist_code"].map(xwalk)
    ws = ws.merge(w[["prcode", "weight"]], on="prcode", how="left")
    ws["piped_water"] = (ws["s5m2q01"] == 1).astype(int)  # 1=piped water

    # ── Housing: electricity ──
    housing = pd.read_stata(str(hies_dir / "sec_05m1_housingchar.dta"), convert_categoricals=False,
                            columns=["prcode", "hhno", "s5m1q01", "s5m1q10"])
    housing["dist_code"] = housing["prcode"].astype(str).str[:4].astype(int)
    housing["dk"] = housing["dist_code"].map(xwalk)
    housing = housing.merge(w[["prcode", "weight"]], on="prcode", how="left")
    # Electricity from main grid (1) or grid+solar (2)
    housing["has_electricity"] = housing["s5m1q10"].isin([1, 2]).astype(int)
    # Owner-occupied (1 or 2)
    housing["owner_occupied"] = housing["s5m1q01"].isin([1, 2]).astype(int)

    # ── Post-stratification: compute district-level calibration factors ──
    # For HIES (household-level), we calibrate so weighted household counts
    # scale proportionally to census population per district.
    hies_cal = {}  # dk → calibration factor
    if census_pop:
        # Compute weighted population per district from roster
        for dk_val in roster["dk"].dropna().unique():
            if not dk_val or dk_val not in census_pop:
                continue
            dk_roster = roster[roster["dk"] == dk_val]
            w_pop = dk_roster["weight"].sum()
            if w_pop > 0:
                census_total = census_pop[dk_val]["pop_total"]
                hies_cal[dk_val] = census_total / w_pop
        print(f"  HIES post-stratification: {len(hies_cal)} districts calibrated to census pop")

    # ── Aggregate to district ──
    print("  Aggregating HIES to district level…")
    n_suppressed = 0
    out = {}

    for dk in set(hh_exp["dk"].dropna().unique()) | set(fies["dk"].dropna().unique()):
        if not dk:
            continue
        d = {}

        # Count households for sample-size check
        n_hh = len(hh_exp[hh_exp["dk"] == dk])

        # Calibration factor for this district (1.0 if not available)
        cal = hies_cal.get(dk, 1.0)

        # Expenditure
        de = hh_exp[hh_exp["dk"] == dk]
        if len(de) > 0:
            wt = de["weight"].fillna(1) * cal
            d[f"{prefix}median_monthly_percapita"] = round(de["per_capita_monthly"].median(), 0)
            d[f"{prefix}mean_monthly_percapita"] = round(
                (de["per_capita_monthly"] * wt).sum() / wt.sum(), 0)
            total_food = (de["food_annual"] * wt).sum()
            total_exp = (de["total_annual"] * wt).sum()
            d[f"{prefix}food_share"] = round(total_food / total_exp * 100, 1) if total_exp else None
            d[f"{prefix}avg_hh_size"] = round((de["hh_size"] * wt).sum() / wt.sum(), 1)

        # FIES
        df_fies = fies[fies["dk"] == dk]
        if len(df_fies) > 0:
            wt = df_fies["weight"].fillna(1) * cal
            d[f"{prefix}food_insecurity_pct"] = round(
                (df_fies["food_insecure"] * wt).sum() / wt.sum() * 100, 1)
            d[f"{prefix}avg_fies_score"] = round(
                (df_fies["fies_score"] * wt).sum() / wt.sum(), 1)

        # Water
        dw = ws[ws["dk"] == dk]
        if len(dw) > 0:
            wt = dw["weight"].fillna(1) * cal
            d[f"{prefix}pct_piped_water"] = round(
                (dw["piped_water"] * wt).sum() / wt.sum() * 100, 1)

        # Housing
        dh = housing[housing["dk"] == dk]
        if len(dh) > 0:
            wt = dh["weight"].fillna(1) * cal
            d[f"{prefix}pct_electricity"] = round(
                (dh["has_electricity"] * wt).sum() / wt.sum() * 100, 1)
            d[f"{prefix}pct_owner_occupied"] = round(
                (dh["owner_occupied"] * wt).sum() / wt.sum() * 100, 1)

        if d:
            # ── Sample-size filter ──
            _suppress_low_n(d, prefix, n_hh)
            if d.get(f"{prefix}low_n"):
                n_suppressed += 1

            # Use accumulate for merged districts (Karachi, Kohistan, etc.)
            if dk in _MERGED_DISTRICTS:
                accumulate(out, dk, d)
            else:
                out[dk] = d

    # Recompute per-capita for merged districts (averages, not sums)
    # The accumulate() function sums all fields, which is wrong for rates/averages.
    # For merged districts, the rates from the LAST sub-district will dominate via
    # the accumulate logic. Since these are weighted averages already, just keep them.

    print(f"  HIES 2024-25: {len(out)} districts, {n_suppressed} suppressed (n<{MIN_SAMPLE_SIZE})")
    return out


# ── Main builder ─────────────────────────────────────────────────────────────

def merge_into(target, source):
    """Merge source dict-of-dicts into target dict-of-dicts."""
    for key, vals in source.items():
        if key not in target:
            target[key] = {}
        target[key].update(vals)

def compute_diffs(data):
    """For each pair of 2017/2023 indicators with the same suffix, add a _diff field."""
    for district, vals in data.items():
        keys_2017 = [k for k in vals if "_2017_" in k]
        for k17 in keys_2017:
            suffix = k17.split("_2017_", 1)[1]
            # Find matching 2023 key
            k23 = k17.replace("_2017_", "_2023_")
            if k23 in vals and vals[k17] is not None and vals[k23] is not None:
                diff_key = k17.split("_2017_")[0] + "_diff_" + suffix
                vals[diff_key] = round(vals[k23] - vals[k17], 4)

def main():
    geo_index = load_geojson_names()
    data = {}  # norm_name -> { indicators }

    # ── Load all tables ──────────────────────────────────────────────────
    tables = []

    # Table 1
    t1_2017 = PBS / "Census 2017" / "final tables" / "table1_combined_2017.csv"
    t1_2023 = PBS / "Census 2023" / "final tables" / "table1_combined_2023.csv"
    if t1_2017.exists():
        tables.append(("Table 1 (2017)", load_table1(t1_2017, "2017")))
    if t1_2023.exists():
        tables.append(("Table 1 (2023)", load_table1(t1_2023, "2023")))

    # Table 5
    t5_2017 = PBS / "Census 2017" / "final tables" / "table05_combined_2017.csv"
    t5_2023 = PBS / "Census 2023" / "final tables" / "table05_combined_2023.csv"
    if t5_2017.exists():
        tables.append(("Table 5 (2017)", load_table5(t5_2017, "2017")))
    if t5_2023.exists():
        tables.append(("Table 5 (2023)", load_table5(t5_2023, "2023")))

    # Table 12 (literacy)
    t12_2017 = PBS / "Census 2017" / "final tables" / "table12_combined_2017.csv"
    t12_2023 = PBS / "Census 2023" / "final tables" / "table12_combined_2023.csv"
    if t12_2017.exists():
        tables.append(("Table 12 (2017)", load_table12_2017(t12_2017)))
    if t12_2023.exists():
        tables.append(("Table 12 (2023)", load_table12_2023(t12_2023)))

    # Education (2017): Use Table 15 (total pop 5+, includes never-attended)
    # instead of Table 14 (literate-only), so 2017 is comparable to 2023 Table 13.
    # We map Table 15 output to t_edu_2017_* keys for unified Education group.
    t15_2017 = PBS / "Census 2017" / "final tables" / "table15_combined_2017.csv"
    if t15_2017.exists():
        t15_data = load_table15_2017(t15_2017)
        # Remap t15_2017_* keys → t_edu_2017_* keys
        remapped = {}
        for dist, vals in t15_data.items():
            remapped[dist] = {k.replace("t15_2017_", "t_edu_2017_"): v for k, v in vals.items()}
        tables.append(("Education 2017 (Table 15 → t_edu)", remapped))

    # Employment: table16 (2017)
    t16_2017 = PBS / "Census 2017" / "final tables" / "table16_combined_2017csv.csv"
    if t16_2017.exists():
        tables.append(("Employment (2017)", load_employment_table_clean(t16_2017, "2017")))

    # Table 16: Economic activity (2017)
    t16_2017 = PBS / "Census 2017" / "final tables" / "table16_combined_2017csv.csv"
    if t16_2017.exists():
        tables.append(("Table 16 - Econ Activity (2017)", load_table16_2017(t16_2017)))

    # Census 2023 Education (Table 13) — try raw per-province first (includes Sindh/Karachi),
    # fall back to combined CSV
    t13_raw_dir = PBS / "Census 2023" / "census2023_all_tables" / "table_13"
    t13_2023 = PBS / "Census 2023" / "final tables" / "table13_combined_2023.csv"
    if t13_raw_dir.exists() and any(t13_raw_dir.glob("table_13_*.csv")):
        tables.append(("Education 2023 (raw per-province)", load_education_2023_raw(t13_raw_dir)))
    elif t13_2023.exists():
        tables.append(("Education 2023 (combined)", load_education_table_clean(t13_2023, "2023")))

    # Census 2023 Employment (Table 14) — combined CSV (preferred over raw per-province)
    t14_2023 = PBS / "Census 2023" / "final tables" / "table14_combined_2023.csv"
    if t14_2023.exists():
        tables.append(("Employment 2023 (combined)", load_employment_table_clean(t14_2023, "2023")))

    # PSLM 2019-20 microdata
    pslm_dir = PBS / "Microdata" / "PSLM 2019-20" / "stata data"
    if pslm_dir.exists():
        try:
            tables.append(("PSLM 2019-20", load_pslm(pslm_dir)))
        except Exception as e:
            print(f"  WARNING: PSLM failed: {e}")

    # Economic Census
    ec_dir = PBS / "Economic Census"
    if ec_dir.exists():
        try:
            tables.append(("Economic Census", load_economic_census(ec_dir)))
        except Exception as e:
            print(f"  WARNING: Economic Census failed: {e}")

    # ── Build census population reference for post-stratification ──
    # Merge census tables loaded so far to extract 2023 pop by sex
    _census_data = {}
    for name, tbl in tables:
        merge_into(_census_data, tbl)
    census_pop = _load_census_pop(_census_data)
    print(f"  Census pop for post-stratification: {len(census_pop)} districts")

    # LFS 2020-21
    lfs_dir = PBS / "Microdata" / "LFS"
    if lfs_dir.exists():
        try:
            tables.append(("LFS 2020-21", load_lfs_2021(lfs_dir, census_pop=census_pop)))
        except Exception as e:
            print(f"  WARNING: LFS 2020-21 failed: {e}")

    # LFS 2024-25
    if lfs_dir.exists():
        try:
            tables.append(("LFS 2024-25", load_lfs_2025(lfs_dir, census_pop=census_pop)))
        except Exception as e:
            print(f"  WARNING: LFS 2024-25 failed: {e}")

    # HIES 2024-25
    hies_dir = PBS / "Microdata" / "HEIS"
    if hies_dir.exists():
        try:
            tables.append(("HIES 2024-25", load_hies(hies_dir, census_pop=census_pop)))
        except Exception as e:
            print(f"  WARNING: HIES 2024-25 failed: {e}")

    # ── Merge ────────────────────────────────────────────────────────────
    print(f"Loaded {len(tables)} table-year combinations:")
    for name, tbl in tables:
        merge_into(data, tbl)
        print(f"  {name}: {len(tbl)} districts")

    # ── Compute education percentages from raw counts ──────────────────
    for key, vals in data.items():
        for year in ("2017", "2023"):
            total = vals.get(f"t_edu_{year}_total")
            if total and total > 0:
                for level in ("below_primary", "primary", "middle", "matric",
                              "intermediate", "graduate", "masters_above"):
                    count = vals.get(f"t_edu_{year}_{level}")
                    if count is not None:
                        vals[f"t_edu_{year}_pct_{level}"] = round(count / total * 100, 2)
                # Compute % never attended
                # If we have an explicit never_attended count, use it
                never = vals.get(f"t_edu_{year}_never_attended")
                if never is not None:
                    vals[f"t_edu_{year}_pct_never_attended"] = round(never / total * 100, 2)
                else:
                    # Residual method: total - sum(levels) = never attended
                    attended_sum = sum(vals.get(f"t_edu_{year}_{lvl}", 0) or 0
                                       for lvl in ("below_primary", "primary", "middle", "matric",
                                                   "intermediate", "graduate", "masters_above"))
                    residual = total - attended_sum
                    # Only use residual if it's a significant portion (>5%) — otherwise
                    # it's just rounding noise
                    if attended_sum > 0 and residual / total > 0.05:
                        vals[f"t_edu_{year}_pct_never_attended"] = round(residual / total * 100, 2)
                # Compute % matric or above
                matric_plus = sum(filter(None, [
                    vals.get(f"t_edu_{year}_matric"),
                    vals.get(f"t_edu_{year}_intermediate"),
                    vals.get(f"t_edu_{year}_graduate"),
                    vals.get(f"t_edu_{year}_masters_above"),
                ]))
                if matric_plus > 0:
                    vals[f"t_edu_{year}_pct_matric_plus"] = round(matric_plus / total * 100, 2)

    # ── Recompute urban proportion from T5 for merged districts ─────────
    for key, vals in data.items():
        for year in ("2017", "2023"):
            total = vals.get(f"t5_{year}_total_all")
            urban = vals.get(f"t5_{year}_urban_all")
            if total and urban is not None and vals.get(f"t1_{year}_urban_proportion") is None:
                vals[f"t1_{year}_urban_proportion"] = round(urban / total * 100, 2)

    # ── Compute diffs ────────────────────────────────────────────────────
    compute_diffs(data)

    # ── Add GeoJSON metadata & match report ──────────────────────────────
    matched, unmatched_csv, unmatched_geo = 0, [], []

    output = {}
    for key, vals in data.items():
        geo = geo_index.get(key)
        if geo:
            matched += 1
            vals["_display_name"] = geo["display"]
            vals["_province"] = geo["province"]
        else:
            unmatched_csv.append(key)
            # Try fuzzy: find closest geo key
            best, best_score = None, 0
            for gk, gv in geo_index.items():
                # simple overlap score
                common = len(set(key.split()) & set(gk.split()))
                if common > best_score:
                    best_score = common
                    best = (gk, gv)
            if best and best_score > 0:
                vals["_display_name"] = best[1]["display"]
                vals["_province"] = best[1]["province"]
                vals["_fuzzy_match"] = best[0]
            else:
                vals["_display_name"] = key.title()
                vals["_province"] = "Unknown"
        output[key] = vals

    # Districts in GeoJSON but not in any CSV
    for gk in geo_index:
        if gk not in data:
            unmatched_geo.append(gk)

    print(f"\n── Match report ──")
    print(f"  CSV districts:     {len(data)}")
    print(f"  GeoJSON districts: {len(geo_index)}")
    print(f"  Matched:           {matched}")
    print(f"  CSV-only:          {len(unmatched_csv)}")
    if unmatched_csv:
        for u in sorted(unmatched_csv):
            print(f"    {u}")
    print(f"  GeoJSON-only:      {len(unmatched_geo)}")
    if unmatched_geo:
        for u in sorted(unmatched_geo):
            print(f"    {u}")

    # ── Sanitise NaN / Infinity (invalid in JSON) ─────────────────────────
    import math
    for vals in output.values():
        for k, v in list(vals.items()):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                vals[k] = None

    # ── Write output ─────────────────────────────────────────────────────
    os.makedirs(OUT.parent, exist_ok=True)
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print(f"\nWrote {len(output)} districts -> {OUT}")

if __name__ == "__main__":
    main()
