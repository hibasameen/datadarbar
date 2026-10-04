#!/usr/bin/env python3
"""
Build the *web* warehouse: the Parquet files + catalog that app/query.html
loads into DuckDB-WASM so anyone can run SQL against Data Darbar in their browser.

Design note
-----------
The whole point of this layout is that there is no server. DuckDB-WASM runs in the
user's tab; the Parquet files are ordinary static assets on the CDN. So the build's
job is only to (a) produce tidy, well-typed, well-compressed Parquet, and (b) emit a
catalog.json describing every table well enough that the page can render a schema
browser and the user can write SQL without reading the ETL code.

Inputs  (all local, no network):
  <icloud>/data_darbar_warehouse/*.parquet   trade, national accounts, budget, LSM, file catalog
  app/data/districts.json                    district indicator panel (built by build_dataset.py)
  app/data/poverty_data.js                   MPI (district) + RWI/pop/night-lights (tehsil)
  etl/mouza2020/*.csv                        Mouza Census 2020 counts + the ADM3 crosswalk
  app/assets/js/app.js                       INDICATOR_GROUPS -> human labels for district fields

Output: app/data/warehouse/{*.parquet, catalog.json}

Usage:  python3 etl/build_web_warehouse.py [--src /path/to/data_darbar_warehouse]
"""

from __future__ import annotations

import argparse
import collections
import json
import os
import re
import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
APP = REPO / "app"
OUT = APP / "data" / "warehouse"

DEFAULT_SRC = Path(
    os.path.expanduser(
        "~/Library/Mobile Documents/com~apple~CloudDocs/Data Darbar/data_darbar_warehouse"
    )
)

# Parquet knobs. ZSTD-12 buys ~15% over snappy on these dictionary-heavy string
# columns for no read-side cost that matters in WASM. Row groups of ~120k rows keep
# per-group statistics selective enough that a filtered query over trade only has to
# fetch a few MB when the browser can do HTTP range requests.
PQ = "(FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 12, ROW_GROUP_SIZE 122880)"
# An absolute path into a local checkout, up to and including the project folder.
LOCAL_PATH = r'/Users/[^"]*?/Data Darbar/'


# ─────────────────────────────────────────────────────────────────────────────
# app.js INDICATOR_GROUPS -> {field_name: {label, group, dataset, year}}
# ─────────────────────────────────────────────────────────────────────────────
def _js_object_to_json(src: str) -> str:
    """Convert the (simple, literal-only) INDICATOR_GROUPS object literal to JSON.

    Only handles what actually appears in app.js: line/block comments, bare keys,
    single-quoted strings, trailing commas. Deliberately not a JS parser — if the
    object ever grows expressions this will fail loudly rather than silently mangle.
    """
    out, i, n = [], 0, len(src)
    while i < n:
        ch = src[i]
        if ch in "\"'":  # copy string literals verbatim (converting quote style)
            quote = ch
            j = i + 1
            buf = []
            while j < n:
                if src[j] == "\\":
                    buf.append(src[j : j + 2])
                    j += 2
                    continue
                if src[j] == quote:
                    break
                buf.append(src[j])
                j += 1
            body = "".join(buf)
            if quote == "'":
                body = body.replace('"', '\\"')
            out.append('"' + body + '"')
            i = j + 1
            continue
        if src.startswith("//", i):
            i = src.find("\n", i)
            if i < 0:
                break
            continue
        if src.startswith("/*", i):
            i = src.find("*/", i) + 2
            continue
        out.append(ch)
        i += 1
    s = "".join(out)
    s = re.sub(r"([{,]\s*)([A-Za-z_$][\w$]*)\s*:", r'\1"\2":', s)  # bare keys
    s = re.sub(r",(\s*[}\]])", r"\1", s)  # trailing commas
    return s


def load_indicator_groups(_unused: Path = None) -> dict:
    """The curated indicator vocabulary, from the ETL's own copy.

    This used to parse app.js, because that is where the vocabulary lived. It
    moved to etl/places/indicator_groups.json when map.html was retired: the
    app is meant to read its labels from the warehouse, not the warehouse from
    the app, and a build that parses a page's JavaScript breaks the moment the
    page does.
    """
    src = REPO / "etl" / "places" / "indicator_groups.json"
    return json.loads(src.read_text(encoding="utf-8"))


def field_dictionary(groups: dict) -> dict:
    """Reconstruct the districts.json field names each group owns, with labels.

    Mirrors how app.js resolves a field: `{prefix}_{year}_{indicator}` normally, but a
    group with `mixedKeys` (housing quality, digital access) spells out the real key
    per indicator because those groups straddle two prefixes.
    """
    dic = {}
    for gkey, g in groups.items():
        prefix = g.get("prefix")
        if not prefix:
            continue
        mixed = g.get("mixedKeys") or {}
        years = g.get("years") or (["2017", "2023"] if g.get("hasYears") else [None])
        for ind, label in (g.get("indicators") or {}).items():
            for yr in years:
                if ind in mixed:
                    field = mixed[ind]
                else:
                    field = f"{prefix}_{yr}_{ind}" if yr else f"{prefix}_{ind}"
                dic[field] = {
                    "group": gkey,
                    "group_label": g.get("label"),
                    "dataset": g.get("dataset"),
                    "indicator": ind,
                    "label": label,
                    "year": yr,
                }
    return dic


# Fields the ETL writes alongside the indicators: sampling quality and provenance.
_QUALITY_SUFFIX = {
    "_n_obs": ("Sample size (observations)", "quality"),
    "_low_n": ("Small-sample flag (1 = unreliable)", "quality"),
    "_coverage": ("Districts covered by this source", "quality"),
    "_inherited_from": ("District whose estimate was borrowed", "provenance"),
    "_boundary_change": ("District this one shares its pre-split 2017 figures with", "provenance"),
    "_year": ("Reference year of the source", "provenance"),
    "_basis": ("Definition / basis used", "provenance"),
}


def classify_extra(field: str):
    for suf, (label, kind) in _QUALITY_SUFFIX.items():
        if field.endswith(suf):
            return label, kind, field[: -len(suf)]
    return None, "indicator", field.split("_")[0]


def derive_meta(field: str, dic: dict, prefixes: list[str]) -> dict | None:
    """Best-effort metadata for a field app.js never charts.

    Two cases matter. `{prefix}_diff_{ind}` is the 2017→2023 change the ETL
    precomputes — it inherits the 2023 field's label. Everything else gets a
    prettified label and the longest known prefix, which is better than NULL for
    someone browsing the table but is flagged by leaving `dataset` NULL.
    """
    m = re.match(r"^(.*)_diff_(.+)$", field)
    if m:
        pre, ind = m.groups()
        base = dic.get(f"{pre}_2023_{ind}") or dic.get(f"{pre}_2017_{ind}")
        if base:
            return {**base, "label": f"{base['label']} — change 2017→2023", "year": "Δ2017-23"}
    pre = max((p for p in prefixes if field.startswith(p + "_")), key=len, default=None)
    ind = field[len(pre) + 1:] if pre else field
    ind = re.sub(r"^(2017|2023)_", "", ind)
    return {
        "group": pre or field.split("_")[0],
        "group_label": None,
        "dataset": None,
        "indicator": ind,
        "label": ind.replace("_", " ").replace("pct ", "% ").strip().capitalize(),
        "year": "2017" if "_2017_" in field else ("2023" if "_2023_" in field else None),
    }


# ─────────────────────────────────────────────────────────────────────────────
# poverty_data.js  ->  dicts  (it is `window.DD_POV={...};` — take the JSON slice)
# ─────────────────────────────────────────────────────────────────────────────
def load_dd_pov(path: Path) -> dict:
    s = path.read_text(encoding="utf-8")
    start = s.index("{", s.index("window.DD_POV"))
    depth, j, instr, esc = 0, start, False, False
    while j < len(s):
        c = s[j]
        if instr:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                instr = False
        elif c == '"':
            instr = True
        elif c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                j += 1
                break
        j += 1
    return json.loads(s[start:j])


# ─────────────────────────────────────────────────────────────────────────────
# ── census → map geometry -----------------------------------------------------
# The map colours the 2015 district layer inlined in app/data/census_data.js,
# whose key is normName(properties.districts). Resolving a census district onto
# it is name work, and only two rules are allowed: an exact match after
# normalisation, and a reviewed spelling difference listed below. A census unit
# is never merged into a *different* unit to find it a polygon — Lower Chitral
# and Upper Chitral would both land on 2015 Chitral and paint one district twice
# with two different numbers, and FR Bannu is not Bannu. Units with no polygon
# of their own stay unmapped and the map says how many there are.
# Words that mark a figure as something other than a count, and so as something
# that must not be added when two units are combined. 2023 carries PBS's own
# is_rate; 2017 does not, so its labels are read for these.
RATE_WORDS = ('RATIO|RATE|PER CENT|PERCENT|PROPORTION|AVERAGE|DENSITY'
              '|PER SQ|HOUSEHOLD SIZE|PERSONS PER')

def _norm_name(x: str) -> str:
    """app.js normName(): lowercase, non-alphanumerics to single spaces."""
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", (x or "").lower())).strip()


def _load_module(path: Path, name: str):
    """Load one ETL module by path, without putting its folder on sys.path."""
    import importlib.util
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def census_table_titles(repo: Path) -> dict:
    """{(year, table_id): PBS's own title}.

    The titles are already written down twice in the ETL — table_spec.TITLES for
    2023 and table_map.MAP for 2017 — so the map reuses them rather than
    inventing a third set. They are not interchangeable between years: 2017's
    table 23 is a locality table and 2023's is drinking water.
    """
    out = {}
    try:
        t23 = _load_module(repo / "etl" / "stage2" / "table_spec.py", "_dd_spec23")
        out.update({(2023, k): v for k, v in t23.TITLES.items()})
    except Exception as e:
        print(f"    (no 2023 table titles: {e})")
    try:
        t17 = _load_module(repo / "etl" / "census2017" / "table_map.py", "_dd_map17")
        out.update({(2017, k): v[2] for k, v in t17.MAP.items()})
    except Exception as e:
        print(f"    (no 2017 table titles: {e})")
    return out


# The 2015 district layer and its aliases lived here, matched by name against
# census_data.js. Both went with map.html: districts key on PBS’s own code
# now, through etl/census2017/census_unit_map.csv.



def build(src: Path, district_only: bool = False, schools_only: bool = False,
          health_only: bool = False) -> None:
    if health_only:
        from health_facilities.register_health import update_warehouse as update_health
        update_health(OUT)
        return
    if schools_only:
        from schools.update_punjab import update_warehouse
        update_warehouse(OUT)
        return
    OUT.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    tables: list[dict] = []

    def register(name, desc, notes, columns, source, sql=None, unit=None):
        """Copy a query to Parquet and record it in the catalog."""
        path = OUT / f"{name}.parquet"
        con.sql(f"COPY ({sql}) TO '{path.as_posix()}' {PQ}")
        rows = con.sql(f"SELECT count(*) FROM '{path.as_posix()}'").fetchone()[0]
        schema = con.sql(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()
        tables.append(
            {
                "name": name,
                "file": f"{name}.parquet",
                "bytes": path.stat().st_size,
                "rows": rows,
                "description": desc,
                "notes": notes,
                "unit": unit,
                "source": source,
                "columns": [
                    {"name": c[0], "type": c[1], "description": columns.get(c[0], "")}
                    for c in schema
                ],
            }
        )
        print(f"  {name:<22} {rows:>9,} rows  {path.stat().st_size/1e6:6.2f} MB")

    # ── 1. district indicator panel (long format) ────────────────────────────
    print("districts…")
    groups = load_indicator_groups()
    dic = field_dictionary(groups)
    prefixes = sorted({g["prefix"] for g in groups.values() if g.get("prefix")})
    districts = json.loads((APP / "data" / "districts.json").read_text())

    rows, unknown = [], set()
    for key, rec in districts.items():
        name = rec.get("_display_name") or key.title()
        prov = rec.get("_province")
        for field, val in rec.items():
            if field.startswith("_") or val is None or isinstance(val, (dict, list)):
                continue
            meta = dic.get(field)
            kind = "indicator"
            if meta is None:
                # Sampling-quality companions (*_n_obs, *_low_n) and provenance fields
                # the ETL writes but the app never charts. Keep them — they are how a
                # user decides whether a district's survey estimate is worth using.
                label, kind, gk = classify_extra(field)
                if kind == "indicator":
                    unknown.add(field)
                    meta = derive_meta(field, dic, prefixes)
                else:
                    meta = {
                        "group": gk,
                        "group_label": None,
                        "dataset": None,
                        "indicator": field,
                        "label": label,
                        "year": None,
                    }
            rows.append(
                {
                    "district_key": key,
                    "district": name,
                    "province": prov,
                    "dataset": meta["dataset"],
                    "group_key": meta["group"],
                    "group_label": meta["group_label"],
                    "indicator": meta["indicator"],
                    "label": meta["label"],
                    "year": meta["year"],
                    "kind": kind,
                    "field": field,
                    "value": float(val) if isinstance(val, (int, float)) else None,
                    "value_text": None if isinstance(val, (int, float)) else str(val),
                }
            )
    con.register("df_district", _to_arrow(rows))
    register(
        "district_indicators",
        "Every district-level indicator in Data Darbar, one row per district × indicator × year.",
        "Long format so heterogeneous sources share one table. Filter `kind = 'indicator'` "
        "for the measures themselves; kind='quality' rows carry the sample size and small-n "
        "flag for the same prefix, and kind='provenance' rows record borrowed estimates. "
        "Survey groups suppress cells with n<30 upstream, so an absent row can mean "
        "'suppressed' as well as 'not collected'. DOUBLE-COUNTING TRAP: where a district "
        "was split after Census 2017 (see the *_boundary_change provenance rows) both "
        "halves carry the SAME 2017 figure for the combined pre-split area, so summing a "
        "2017 count across all districts overstates the total. Filter those rows out, or "
        "sum the 2023 year instead. Census education source corrections dated 2026-09-25 "
        "use original PBS Table 15 (2017) and Table 13 (2023); see the website methodology. "
        "Missing source counts are unavailable, not zero.",
        {
            "district_key": "normalised join key used across Data Darbar",
            "district": "display name",
            "province": "province / territory",
            "dataset": "source dataset (Census 2017/23, PSLM 2019-20, LFS, HIES, PDHS…)",
            "group_key": "indicator group key in app.js",
            "group_label": "human label for the group",
            "indicator": "indicator key within the group",
            "label": "human label for the indicator",
            "year": "census year for two-year census groups, else NULL",
            "kind": "'indicator' | 'quality' (n_obs, low_n) | 'provenance'",
            "field": "raw field name in districts.json",
            "value": "numeric value (units implied by the label)",
            "value_text": "non-numeric value, if any",
        },
        "PBS Census 2017 & 2023, Economic Census 2023, PSLM 2019-20, LFS 2020-21/2024-25, HIES 2024-25; PDHS 2017-18 (NIPS and ICF); MICS district rounds (Punjab 2017-18, Sindh 2018-19, Khyber Pakhtunkhwa 2019, Balochistan 2019-20, Gilgit-Baltistan 2016-17, Azad Jammu & Kashmir 2020-21; the provincial and regional bureaus of statistics and planning departments, with UNICEF)",
        "SELECT * FROM df_district ORDER BY district_key, dataset, group_key, indicator, year",
    )
    if unknown:
        print(f"    ({len(unknown)} fields had no app.js label, e.g. {sorted(unknown)[:3]})")

    if district_only:
        import hashlib
        catalog_path = OUT / "catalog.json"
        catalog = json.loads(catalog_path.read_text())
        if sum(t["name"] == "district_indicators" for t in catalog["tables"]) != 1:
            raise ValueError("Expected one district table in the existing catalogue")
        catalog["tables"] = [tables[0] if t["name"] == "district_indicators" else t for t in catalog["tables"]]
        digest = hashlib.sha256((OUT / "district_indicators.parquet").read_bytes()).hexdigest()[:12]
        catalog["generated"] = _today()
        catalog["revision"] = f"{_today()}-census-{digest}"
        catalog_path.write_text(json.dumps(catalog, indent=1) + "\n")
        con.close()
        print("Updated the district table and catalogue; other warehouse tables retained")
        return

    # ── 2. poverty: district MPI + tehsil satellite ──────────────────────────
    print("poverty…")
    pov = load_dd_pov(APP / "data" / "poverty_data.js")

    mpi = [
        {"district_key": k, **{kk: v.get(kk) for kk in
                               ("name", "prov", "mpi", "H", "A", "rank", "n_obs", "low_n",
                                "c_schooling", "c_attendance", "c_electricity", "c_cooking_fuel",
                                "c_sanitation", "c_water", "c_housing")}}
        for k, v in pov["districts"].items()
    ]
    con.register("df_mpi", _to_arrow(mpi))
    register(
        "mpi_districts",
        "Alkire-Foster multidimensional poverty index by district, from PSLM 2019-20 microdata.",
        "M0 = H × A. Censored headcounts c_* are the share of people who are both poor and "
        "deprived in that indicator (%). low_n=1 marks districts whose sample is too small to "
        "be reliable — filter them out for rankings.",
        {
            "district_key": "join key to district_indicators.district_key",
            "name": "district name", "prov": "province",
            "mpi": "M0, adjusted headcount ratio (0-1)",
            "H": "headcount ratio — % of people who are MPI-poor",
            "A": "intensity — average share of weighted deprivations among the poor (%)",
            "rank": "1 = poorest", "n_obs": "households in the PSLM sample",
            "low_n": "1 if sample below the reliability threshold",
            "c_schooling": "censored headcount: years of schooling (%)",
            "c_attendance": "censored headcount: school attendance (%)",
            "c_electricity": "censored headcount: electricity (%)",
            "c_cooking_fuel": "censored headcount: cooking fuel (%)",
            "c_sanitation": "censored headcount: sanitation (%)",
            "c_water": "censored headcount: drinking water (%)",
            "c_housing": "censored headcount: housing (%)",
        },
        "PSLM/HIES 2019-20 microdata (PBS), Alkire-Foster method",
        "SELECT * FROM df_mpi ORDER BY rank",
    )

    # `nl` arrives as {year: radiance}. Keeping it as a STRUCT would force every user
    # to learn DuckDB struct syntax to touch the light series, so split it: a scalar
    # latest-year column on the main table, and the full series as its own long table.
    teh, lights = [], []
    for k, v in pov["tehsils"].items():
        nl = v.get("nl") or {}
        years = sorted(nl.keys())
        for y in years:
            if nl[y] is not None:
                lights.append({"tehsil_id": k, "year": int(y), "radiance": float(nl[y])})
        row = {kk: vv for kk, vv in v.items() if kk != "nl"}
        row["nl_latest"] = nl.get(years[-1]) if years else None
        row["nl_year"] = int(years[-1]) if years else None
        teh.append({"tehsil_id": k, **row})
    con.register("df_teh", _to_arrow(teh))
    con.register("df_lights", _to_arrow(lights))
    register(
        "tehsil_satellite",
        "Tehsil-level (ADM3) satellite measures: relative wealth, population, night-lights.",
        "RWI is Meta's Relative Wealth Index — note it USES night-time lights as one of its "
        "own inputs, so rwi and the light columns are NOT independent measurements; a "
        "correlation between them is partly mechanical. Lights are June VIIRS radiance, "
        "population-weighted. The full year-by-year series is in tehsil_nightlights.",
        {
            "tehsil_id": "GADM/ADM3 identifier — joins to tehsil_nightlights.tehsil_id",
            "name": "tehsil name",
            "dk": "district_key of the parent district (joins to district_indicators)",
            "prov": "province",
            "area": "area, km²", "rwi": "Meta Relative Wealth Index (mean, population-weighted)",
            "rwi_pct": "percentile of rwi within Pakistan",
            "pop": "population (WorldPop 2020, UN-adjusted)", "popdens": "people per km²",
            "nl_latest": "radiance in the most recent June (VIIRS, nW/cm²/sr)",
            "nl_year": "the year nl_latest refers to",
            "nl_growth": "% change in radiance, first to last available year",
            "nl_lowc": "1 if radiance is near the noise floor (treat growth as unreliable)",
        },
        "Meta Data for Good Relative Wealth Index (Chi et al. 2022, PNAS 119(3)), via the Humanitarian Data Exchange; WorldPop 2020 UN-adjusted 1 km population (WorldPop, University of Southampton; CC BY 4.0); VIIRS day/night band monthly composites, Earth Observation Group, Payne Institute for Public Policy, Colorado School of Mines (Elvidge et al. 2013, 2017), from NOAA/NASA VIIRS",
        "SELECT * FROM df_teh ORDER BY prov, dk, name",
    )

    register(
        "tehsil_nightlights",
        "Night-time light radiance by tehsil and year (June VIIRS composites).",
        "One row per tehsil × year. Population-weighted mean radiance. Tehsils flagged "
        "nl_lowc in tehsil_satellite sit near the sensor's noise floor — their year-on-year "
        "movements are mostly noise.",
        {"tehsil_id": "joins to tehsil_satellite.tehsil_id", "year": "calendar year (June composite)",
         "radiance": "nW/cm²/sr, population-weighted mean"},
        "VIIRS day/night band monthly composites, Earth Observation Group, Payne Institute for Public Policy, Colorado School of Mines (Elvidge et al. 2013, 2017), from NOAA/NASA VIIRS",
        "SELECT * FROM df_lights ORDER BY tehsil_id, year",
    )

    # ── 4. Mouza Census 2020 ─────────────────────────────────────────────────
    # The raw PBS counts rather than the shares the map draws. Shares bake in a
    # denominator choice the source does not publish (see notes); counts let
    # anyone recompute with their own.
    print("mouza…")
    mz_dir = REPO / "etl" / "mouza2020"
    mz_csv = (mz_dir / "pk-mouza-2020-tehsil.csv").as_posix()
    xw_csv = (mz_dir / "mouza2020_tehsil_crosswalk.csv").as_posix()

    def mouza_col_desc(name):
        """230 columns is too many to hand-write, and the families are regular."""
        fams = [
            ("EducationFacility_", "mouzas with / without this school type (Existance / NotExistance pair)"),
            ("HealthFacility_", "mouzas reporting this health facility (multiple response)"),
            ("SourceOfDrinkingWater_", "mouzas reporting this drinking-water source (multiple response)"),
            ("StatusTypeOfStreets_", "mouzas reporting this street surface (multiple response)"),
            ("ElectrictiyAvailability_", "mouzas by how much of the settlement has electricity (exclusive; PBS spelling)"),
            ("AlternateEnergySource_", "mouzas reporting this alternative energy source"),
            ("FuelAvailability_", "mouzas reporting this domestic fuel (multiple response)"),
            ("CommunityInfrastructure_", "mouzas with / without this facility"),
            ("CreditSource_", "mouzas reporting this credit source, post office or police station"),
            ("IndustryAndSourceOfEmployment_", "mouzas by industry scale, and by how much of the workforce is in each sector"),
            ("NaturalDisaster_", "mouzas exposed to natural disaster, and to each type"),
            ("HousingConstruction_", "mouzas by predominant house construction material (exclusive)"),
            ("MauzaStatus_", "mouzas by settlement status (exclusive; sums to TotalMauzaCount)"),
            ("Livestock_", "mouzas with / without this veterinary facility"),
            ("WholesaleMarket_", "mouzas with this wholesale market"),
            ("DepoAgencyShop_", "mouzas with this agricultural input supplier"),
            ("MediaSource_", "mouzas reached by this medium (multiple response)"),
        ]
        for pre, txt in fams:
            if name.startswith(pre):
                return txt
        return {
            "province_code": "PBS province code", "province": "province name",
            "division_code": "PBS division code", "division": "division name",
            "district_code": "PBS district code (999 = the Cholistan pseudo-district)",
            "district": "district name", "tehsil_code": "PBS tehsil code, unique nationally",
            "tehsil": "tehsil name as PBS writes it",
            "TotalMauzaCount": "mouzas enumerated in the tehsil",
            "CompletedMouzaCount": "of which completed",
            "CompletedWithErrorMouzaCount": "of which flagged with an error at source",
            "RuralPopulatedMouzaCount": "mouzas that are rural and populated",
            "TotalArea": "total area, acres", "CultivatedArea": "cultivated area, acres",
            "NonCultivatedArea": "non-cultivated area, acres",
            "PopulatedArea": "built-up area, acres",
            "AvgDepthOfWater": "mean water-table depth, feet",
            "MinDepthOfWater": "minimum water-table depth, feet",
            "MaxDepthOfWater": "maximum water-table depth, feet",
        }.get(name, "count of mouzas reporting this")

    mz_schema = con.sql(f"DESCRIBE SELECT * FROM read_csv_auto('{mz_csv}')").fetchall()
    register(
        "mouza_tehsil",
        "Mouza Census 2020 facility counts, one row per PBS tehsil.",
        "Every column is a COUNT OF MOUZAS (revenue villages), never of people or households — "
        "a mouza of 12,000 and a mouza of 300 each count once. PBS publishes numerators without "
        "a denominator, and its own indicator blocks disagree about how many mouzas answered: "
        "only 33 of the 544 enumerated tehsils give a single consistent base, and blocks can "
        "differ by up to 181. Take each block's own row sum as its base rather than "
        "TotalMauzaCount. Existance/NotExistance pairs are exclusive; drinking water, health "
        "facility type, fuel, street surface and media are multiple response, so they can sum "
        "past the base. The frame is rural — cities are not revenue villages — but mouzas that "
        "urbanise stay in it (4.9% urban, 2.4% partly urban). 51 tehsils return all zeros: AJK "
        "and GB were not enumerated, nor were Mand and Tump in Kech or Kallag in Panjgur. Join "
        "to the ADM3 geography through mouza_crosswalk. Column names keep PBS's own casing and "
        "spelling; DuckDB matches them case-insensitively.",
        {c[0]: mouza_col_desc(c[0]) for c in mz_schema},
        "PBS Mouza Census 2020 (mc2020.pbos.gov.pk)",
        f"SELECT * FROM read_csv_auto('{mz_csv}') ORDER BY province_code, district_code, tehsil_code",
    )

    register(
        "mouza_crosswalk",
        "Maps each PBS tehsil to the ADM3 polygon Data Darbar maps it on.",
        "MANY-TO-ONE by design: PBS enumerates 595 tehsils against the boundary file's 553, "
        "because it carries sub-tehsils created after the polygons were drawn. Since every "
        "Mouza Census figure is a count of mouzas, summing several PBS tehsils into one polygon "
        "is the correct operation — group by tehsil_id and sum before taking any share. "
        "match records how each row was resolved: exact_name, variant (same place spelled "
        "differently), contained, fuzzy, only_tehsil_in_district, parent (a sub-tehsil folded "
        "into the unit it was carved from), and approx (no polygon exists for the area, so it "
        "was placed in a neighbour — treat those tehsils' placement as a judgement call). "
        "All 48,738 mouzas are assigned. The 10 unresolved rows all have zero mouzas.",
        {
            "tehsil_code": "PBS tehsil code — joins to mouza_tehsil.tehsil_code",
            "tehsil": "PBS tehsil name",
            "district_code": "PBS district code", "district": "PBS district name",
            "province": "province name", "mouzas": "mouzas enumerated (0 = not enumerated)",
            "dd_id": "ADM3 identifier — joins to tehsil_satellite.tehsil_id",
            "dd_name": "boundary-file tehsil name", "dd_district": "boundary-file district key",
            "match": "how the row was matched (see notes)",
            "dd_candidates": "boundary-file names considered, where the match failed",
        },
        "PBS Mouza Census 2020 frame × geoBoundaries PAK ADM3 (Runfola et al. 2020, PLoS ONE 15(4); CC BY 4.0)",
        f"SELECT * FROM read_csv_auto('{xw_csv}', all_varchar=true) "
        f"ORDER BY province, district, tehsil",
    )

    # ── 2b. government school layer (Adaad, "How far is the girls' school?") ─
    # One row per listed government school, the district access statistics
    # computed from it, the coverage ledger, and the external-validity tests
    # against the Mouza Census 2020. Inputs are frozen in etl/schools/ by
    # release; see etl/schools/README.md for how they were built.
    print("schools…")
    sc_dir = REPO / "etl" / "schools"
    sc_release = "2026-09"
    SCHOOLS_SOURCE = (
        "Provincial school registers: SED Balochistan open-data portal; Sindh SELD Institution Checker "
        "and Distance Checker with RSU district GIS pins; KP Education Monitoring Authority school locator "
        "and the JSiMS mirror of KP EMIS; Punjab School Information System; GB EMIS; Mirpur and Kotli "
        "exam boards; OpenStreetMap for Islamabad. Compiled for Adaad, September 2026."
        " The 130 Islamabad rows are \u00a9 OpenStreetMap contributors (ODbL 1.0); 232 Punjab positions were geocoded against GeoNames (CC BY 4.0) and OpenStreetMap."
    )
    SCHOOLS_PORTAL_NOTE = (
        "Every position in this table is either published by a provincial education department "
        "(Balochistan, KP, and Sindh's pins), solved from distances the Sindh department's own public "
        "distance checker returns, or geocoded from a settlement name in a public roster. The table adds "
        "nothing a department has not put online: no staff names, no contact details. "
    )
    schools_csv = (sc_dir / f"schools_pk_{sc_release}.csv.gz").as_posix()
    from schools.update_punjab import register_table
    register_table(register, sc_dir)

    register(
        "school_access_district",
        "Distance to the nearest government school by sex, enrolment by sex, and school counts, one row per district (132).",
        "all_km, boys_km, girls_km are POPULATION-WEIGHTED MEDIAN straight-line distances (Lambert conformal conic, "
        "km) from every populated 1 km cell to the nearest school of that network; *_min are walking minutes on the "
        "Malaria Atlas friction surface. gap_km = girls_km − boys_km (positive = girls further). Networks are by name "
        "designation; for Sindh the same medians under the attendance and girls-or-mixed definitions are in "
        "girls_km_attend / girls_km_mixed (the designation gap of 0.8 km falls to zero under attendance). "
        "tt_caveat flags districts where the friction surface is unreliable (GB) or mountain/desert cells make "
        "minutes an upper bound. *_in_school_pct_5_16 are Census 2023 Table 13(b) enrolment rates (enrolled / "
        "population aged 5–16) with successor districts summed into parents, so 123 of the 132 rows carry them. "
        "living_standards_deprivation_pslm is the (censored) MPI component; material_deprivation_pslm is an "
        "uncensored index from PSLM 2019-20 (no toilet, no flush, no piped water, food insecurity). "
        "*_n_schools count schools whose POINT falls in the district polygon (the basis of the piece's Figure 1); "
        "schools_listed / _with_coords / _in_analysis count rows by the SOURCE's district. The two bases differ "
        "where geocoded points cross a boundary. Distances depend on the whole network, not the district's own "
        "schools, so a district with few schools can still be close to the next district's.",
        {
            "region": "analysis region", "district_key": "Data Darbar district key", "district": "display name",
            "all_km": "median km to nearest school, either sex", "boys_km": "median km to nearest boys' school",
            "girls_km": "median km to nearest girls' school", "gap_km": "girls_km − boys_km",
            "all_min": "median walking minutes, any school", "boys_min": "median walking minutes, boys' school",
            "girls_min": "median walking minutes, girls' school", "gap_min": "girls_min − boys_min",
            "tt_caveat": "travel-time caveat, if any",
            "girls_in_school_pct_5_16": "Census 2023 Table 13(b): girls 5–16 enrolled / girls 5–16, %",
            "boys_in_school_pct_5_16": "Census 2023 Table 13(b): boys 5–16 enrolled / boys 5–16, %",
            "enrol_gap_pp_boys_minus_girls": "boys_in_school_pct_5_16 − girls_in_school_pct_5_16, percentage points",
            "living_standards_deprivation_pslm": "MPI living-standards deprivation share (censored), PSLM 2019-20",
            "material_deprivation_pslm": "uncensored material deprivation index, PSLM 2019-20",
            "schools_listed": "rows in schools_pk whose source district is this district",
            "schools_with_coords": "of which positioned", "schools_in_analysis": "of which in the analysis",
            "girls_primary_median_km": "median km to nearest girls' school, any level", "boys_primary_median_km": "as girls_, boys",
            "girls_middle_median_km": "median km to nearest girls' middle-or-above school", "boys_middle_median_km": "as girls_, boys",
            "girls_high_median_km": "median km to nearest girls' high-or-above school", "boys_high_median_km": "as girls_, boys",
            "girls_primary_share_over_5km": "share of population more than 5 km from a girls' school, any level",
            "boys_primary_share_over_5km": "as girls_, boys", "girls_middle_share_over_5km": "share more than 5 km from a girls' middle-plus school",
            "boys_middle_share_over_5km": "as girls_, boys", "girls_high_share_over_5km": "share more than 5 km from a girls' high-plus school",
            "boys_high_share_over_5km": "as girls_, boys",
            "girls_primary_n_schools": "girls' schools (any level) whose point is inside the district polygon",
            "boys_primary_n_schools": "as girls_, boys", "girls_middle_n_schools": "girls' middle-plus schools inside the polygon (Figure 1)",
            "boys_middle_n_schools": "as girls_, boys", "girls_high_n_schools": "girls' high-plus schools inside the polygon",
            "boys_high_n_schools": "as girls_, boys",
            "girls_km_mixed": "Sindh only: girls_km when Mixed schools join the girls' network", "boys_km_mixed": "Sindh only: boys_km, boys-or-mixed",
            "gap_km_mixed": "Sindh only: gap under the mixed definition",
            "girls_km_attend": "Sindh only: girls_km when any school enrolling girls counts", "boys_km_attend": "Sindh only: boys_km by attendance",
            "gap_km_attend": "Sindh only: gap under the attendance definition", "release": "release tag",
        },
        "Adaad school layer (schools_pk) × WorldPop 2020 × Malaria Atlas friction surface; PBS Census 2023 Table 13(b); PSLM 2019-20",
        f"SELECT * FROM read_csv_auto('{(sc_dir / 'school_access_district.csv').as_posix()}') ORDER BY region, district",
        unit="km, walking minutes, per cent",
    )

    register(
        "school_access_tehsil",
        "Distance to the nearest government school by sex and level, and school counts, one row per tehsil (498 of 553 ADM3 polygons).",
        "The tehsil version of school_access_district, by the same method: population-weighted median straight-line "
        "km (Lambert conformal conic, 1 km WorldPop 2020 cells) to the nearest school of that sex and level in the "
        "cell's own REGION network — a tehsil's residents can use the next tehsil's schools, so its distance is not a "
        "function of its own school count. gap_*_km = girls − boys (positive = girls further). *_over5km_pct is the "
        "population share more than 5 km away. *_schools count schools whose point falls in the polygon. "
        "coverage_pct is the share of the polygon's population that lay inside the analysed region; rows under 50% "
        "(slivers of the merged districts and of AJK's other districts inside a neighbouring district polygon) are "
        "dropped, so 498 tehsils carry data and 55 do not. The 553 polygons are the Mouza Census ADM3 frame "
        "(mouza_crosswalk.dd_id) rather than PBS's 2023 tehsils. Read coord_tier before comparing across provinces: "
        "Punjab's positions are geocoded (half at settlement precision, the rest at markaz or tehsil centroids), so "
        "Punjab tehsil values are a district-scale picture, not a local one. Sindh networks are by name designation; "
        "the designated gap is an upper bound (see school_access_district's mixed and attendance columns). "
        "This is the table the map's Education → Distance to School layer draws.",
        {
            "dd_id": "ADM3 identifier — joins mouza_crosswalk.dd_id, tehsil_satellite.tehsil_id", "tehsil": "tehsil name in the ADM3 frame", "district_key": "Data Darbar district key",
            "region": "analysis region", "coord_tier": "coverage grade of the region's positions (A to C)",
            "pop": "population of the analysed cells (WorldPop 2020)", "coverage_pct": "share of the polygon's population analysed",
            "girls_primary_km": "median km to nearest girls' school, any level", "boys_primary_km": "as girls_, boys", "gap_primary_km": "girls − boys, any level",
            "girls_middle_km": "median km to nearest girls' middle-or-above school", "boys_middle_km": "as girls_, boys", "gap_middle_km": "girls − boys, middle",
            "girls_high_km": "median km to nearest girls' high-or-above school", "boys_high_km": "as girls_, boys", "gap_high_km": "girls − boys, high",
            "girls_middle_over5km_pct": "population more than 5 km from a girls' middle-plus school, %", "boys_middle_over5km_pct": "as girls_, boys",
            "girls_primary_schools": "girls' schools (any level) inside the polygon", "boys_primary_schools": "as girls_, boys",
            "girls_middle_schools": "girls' middle-plus schools inside the polygon", "boys_middle_schools": "as girls_, boys",
            "girls_high_schools": "girls' high-plus schools inside the polygon", "boys_high_schools": "as girls_, boys",
            "girls_share_middle_pct": "girls' share of middle-plus schools inside the polygon, %", "release": "release tag",
        },
        "Adaad school layer (schools_pk) × WorldPop 2020 × Data Darbar ADM3 polygons",
        f"SELECT * FROM read_csv_auto('{(sc_dir / 'school_access_tehsil.csv').as_posix()}') ORDER BY region, district_key, dd_id",
        unit="km, per cent",
    )

    register(
        "school_distance_stats",
        "Distance-to-school distributions by region and district, sex and school level (primary, middle-plus, high-plus).",
        "One row per geography × sex × level. geography_type = 'region' rows are the seven analysis regions; "
        "'district' rows carry district_key. mean_km and median_km are population-weighted over 1 km cells; "
        "share_over_2km/5km/10km are population shares beyond that straight-line distance. network_schools is the "
        "size of the REGION's network for that sex and level — the same number repeats on every district row of a "
        "region, because a district's residents can use the next district's schools. Per-district school counts "
        "are in school_access_district. tier and sector repeat the coverage ledger's grade and the 'government "
        "schools only' scope.",
        {
            "region": "analysis region", "geography_type": "region | district", "district_key": "Data Darbar key for district rows",
            "sex": "girls | boys", "level": "primary (any school) | middle (middle-plus) | high (high-plus)",
            "network_schools": "schools in the region's network for this sex and level", "pop": "population of the geography (WorldPop 2020 sum)",
            "mean_km": "population-weighted mean km", "median_km": "population-weighted median km",
            "share_over_2km": "population share more than 2 km away", "share_over_5km": "more than 5 km",
            "share_over_10km": "more than 10 km", "tier": "coverage grade", "sector": "scope", "release": "release tag",
        },
        "Adaad school layer × WorldPop 2020",
        f"SELECT * FROM read_csv_auto('{(sc_dir / 'school_distance_stats.csv').as_posix()}') ORDER BY region, geography_type DESC, district_key, sex, level",
        unit="km, population shares",
    )

    register(
        "school_layer_coverage",
        "Coverage ledger: how many government schools each region lists, how many the layer positions, and where the positions come from.",
        "schools_known_to_exist is the department's own count (roster or annual census); schools_in_analysis is what "
        "the piece used; rows_in_schools_pk / rows_with_coords / rows_in_analysis are the same counts read back from "
        "the released table, so any difference is visible here (Sindh: 39,745 functional positioned schools, 39,741 "
        "with a sex designation). Two rows carry no positions at all: the ex-FATA merged districts (6,394 schools "
        "per KP's annual census, no public locations) and eight of AJK's ten districts (no public list). Islamabad's "
        "~420 is the size of the federal network, not a roster count. quality_tier grades positional quality, not "
        "completeness: Punjab is 95% geocoded but only half of it at settlement precision.",
        {
            "region": "region", "sector": "which schools the row covers", "schools_in_analysis": "schools the piece used",
            "schools_known_to_exist": "schools the department lists", "source": "register", "gps_origin": "how positions were obtained",
            "quality_tier": "A (GPS at source) to C (partial geocoding)", "coverage_note": "caveats", "mapped": "yes | partial | no",
            "schools_pk_region": "region label in schools_pk", "rows_in_schools_pk": "rows in the released table",
            "rows_with_coords": "of which positioned", "rows_in_analysis": "of which in the analysis", "release": "release tag",
        },
        "Adaad, coverage ledger of the school layer, September 2026",
        f"SELECT * FROM read_csv_auto('{(sc_dir / 'school_layer_coverage.csv').as_posix()}', all_varchar=false)",
    )

    register(
        "school_validation_district",
        "External-validity test of the school layer against the Mouza Census 2020, one row per mouza district (147): count floors and village-reported distances.",
        "The Mouza Census asked every rural mouza whether an institution for boys and one for girls exists at "
        "each level and, if not, how far the nearest is. It is sector-blind and counts villages, so it cannot audit "
        "the layer line by line; it puts a FLOOR under the count (a village with one has at least one) and gives an "
        "INDEPENDENT distance. ratio_* = schools in the layer ÷ mouzas reporting an institution of that sex and "
        "level: well under 1 means schools are missing or misclassified; above 1 is expected. The floor test is "
        "diagnostic only where private and co-educational provision is scarce — at primary level in Punjab a low "
        "ratio is private schools, and in Sindh 'girls undercounted relative to boys' fires by construction because "
        "designated boys' schools include the Mixed majority. status = 'absent from harvest' marks the 15 mouza "
        "districts the layer does not cover (7 merged districts, 8 AJK). village_* columns are means and medians of "
        "the villages' reported km; model_* are the layer's population-weighted medians and shares; *_gap_* are "
        "girls minus boys. Compare ranks and signs, not levels: village distances are unweighted, by road, any "
        "sector. Run on the released table (in_analysis rows), so these figures describe schools_pk, not the "
        "raw registers.",
        {
            "region": "region", "district_key": "Data Darbar key", "mouza_district": "district(s) as PBS names them (successors joined with +)",
            "layer_district": "district name in the layer", "status": "matched | absent from harvest | harvest only (no mouza rows: the layer has schools but PBS enumerated no rural mouza there, e.g. Karachi's urban districts)", "rural_mouzas": "rural mouzas in the district",
            "flag": "test outcomes: boys/girls middle-plus or primary under half; girls undercounted relative to boys",
            "release": "release tag",
        } | {f"mouzas_with_{x}_{l}": f"mouzas reporting a {'boys' if x=='B' else 'girls'}' {l} institution" for x in 'BG' for l in ('primary', 'middle', 'high')}
          | {f"coll_{x}_{l}": f"{'boys' if x=='B' else 'girls'}' {l} schools in the layer" for x in 'BG' for l in ('primary', 'middle', 'high')}
          | {f"coll_midplus_{x}": f"{'boys' if x=='B' else 'girls'}' middle-plus schools in the layer" for x in 'BG'}
          | {f"ratio_{k}_{x}": f"{'boys' if x=='B' else 'girls'}' {k} ratio: layer ÷ mouza floor" for k in ('midplus', 'high', 'primary') for x in 'BG'}
          | {"girls_to_boys_ratio_midplus": "ratio_midplus_G ÷ ratio_midplus_B"}
          | {f"village_{s}_km_{x}_{l}": f"villages' reported km to nearest {'boys' if x=='B' else 'girls'}' {l} institution, {s}" for s in ('mean', 'median') for x in 'BG' for l in ('primary', 'middle', 'high')}
          | {f"village_share_over_5km_{x}_{l}": f"share of villages more than 5 km from a {'boys' if x=='B' else 'girls'}' {l} institution" for x in 'BG' for l in ('primary', 'middle', 'high')}
          | {f"model_{v}_{s}_{c}": f"layer: {v.replace('_', ' ')} for {s}, {c.replace('_plus', '-plus')}" for v in ('median_km', 'share_over_5km') for s in ('girls', 'boys') for c in ('primary_plus', 'middle_plus', 'high_plus')}
          | {f"village_gap_km_{l}": f"village mean km, girls − boys, {l}" for l in ('primary', 'middle', 'high')}
          | {f"model_gap_km_{l}": f"layer median km, girls − boys, {l}" for l in ('primary', 'middle', 'high')}
          | {f"village_gap_share5_{l}": f"village share beyond 5 km, girls − boys, {l}" for l in ('primary', 'middle', 'high')}
          | {f"model_gap_share5_{l}": f"layer share beyond 5 km, girls − boys, {l}" for l in ('primary', 'middle', 'high')},
        "PBS Mouza Census 2020 microdata (Form-11 Part IV) × schools_pk × school_distance_stats",
        f"SELECT * FROM read_csv_auto('{(sc_dir / 'school_validation_district.csv').as_posix()}') ORDER BY region, district_key",
    )

    register(
        "school_validation_tehsil",
        "The same count-floor test by tehsil, for the provinces whose registers carry a tehsil (Sindh, Punjab, Balochistan, GB).",
        "Locates holes inside districts. 'absent from harvest' here is mostly a NAME mismatch rather than a hole: "
        "PBS enumerates sub-tehsils the registers do not carry (88 of Balochistan's 139), and the Karachi "
        "sub-divisions do not exist in the SELD roster. Sindh matches 104 of its 110 rural talukas. Read flags "
        "with the same caveats as school_validation_district, and with min_m = 5 mouzas rather than 10.",
        {
            "region": "region", "district_key": "Data Darbar key", "tehsil_key": "normalised tehsil name used to match",
            "mouza_tehsil": "tehsil as PBS names it", "layer_tehsil": "tehsil as the register names it",
            "status": "matched | absent from harvest", "rural_mouzas": "rural mouzas", "flag": "test outcomes", "release": "release tag",
        } | {f"mouzas_with_{x}_{l}": f"mouzas reporting a {'boys' if x=='B' else 'girls'}' {l} institution" for x in 'BG' for l in ('primary', 'middle', 'high')}
          | {f"coll_{x}_{l}": f"{'boys' if x=='B' else 'girls'}' {l} schools in the layer" for x in 'BG' for l in ('primary', 'middle', 'high')}
          | {f"coll_midplus_{x}": f"{'boys' if x=='B' else 'girls'}' middle-plus schools in the layer" for x in 'BG'}
          | {f"ratio_{k}_{x}": f"{'boys' if x=='B' else 'girls'}' {k} ratio: layer ÷ mouza floor" for k in ('midplus', 'high', 'primary') for x in 'BG'}
          | {"girls_to_boys_ratio_midplus": "ratio_midplus_G ÷ ratio_midplus_B"},
        "PBS Mouza Census 2020 microdata × schools_pk",
        f"SELECT * FROM read_csv_auto('{(sc_dir / 'school_validation_tehsil.csv').as_posix()}') ORDER BY region, district_key, tehsil_key",
    )

    register(
        "school_validation_summary",
        "Rank agreement between the layer's district distances and the villages' own reports, by region and level.",
        "Spearman rank correlations across matched districts between the layer's population-weighted median "
        "distance to the nearest girls' school and the villages' reported distance (rank_corr_level_girls), between "
        "the two shares beyond 5 km (rank_corr_share5_girls), and between the two girls-minus-boys gaps "
        "(rank_corr_gap); sign_agree_gap is the share of districts where both sources agree on the SIGN of the gap. "
        "Read it as a scorecard: the ordering of districts by how far girls are from school is confirmed (0.65–0.77 "
        "nationally at middle and high level); the ordering by the SIZE of the gap is confirmed at primary and "
        "middle and only weakly at high level (Balochistan 0.05); the sign of Punjab's gap is not supported "
        "(0.31–0.57), because both sources put it within a kilometre of zero. GB has seven districts — treat its "
        "rows as indicative.",
        {
            "region": "All or region", "level": "primary | middle | high", "n": "districts compared",
            "rank_corr_level_girls": "Spearman, girls' median km: layer v villages", "rank_corr_share5_girls": "Spearman, share beyond 5 km",
            "rank_corr_gap": "Spearman, girls − boys km gap", "sign_agree_gap": "share of districts agreeing on the gap's sign",
            "rank_corr_gap_share5": "Spearman, girls − boys share beyond 5 km", "release": "release tag",
        },
        "PBS Mouza Census 2020 microdata × school_distance_stats",
        f"SELECT * FROM read_csv_auto('{(sc_dir / 'school_validation_summary.csv').as_posix()}')",
    )

    register(
        "census_enrolment_5_16_by_sex",
        "Census 2023 Table 13(b): population and enrolment aged 5–16 by sex and rural/urban, one row per district on the 2017 boundary frame.",
        "Successor districts are summed into their 2017 parents (constituents lists them), so rates are on the "
        "same frame as school_access_district; 130 rows. *_in_school_pct = enrolled ÷ population aged 5–16. "
        "*_never and *_out_of_school follow PBS's definitions (never attended; never attended plus dropped out). "
        "This is the age-matched outcome the piece uses; Table 12's all-ages complement runs about 0.7 points higher.",
        {"district": "district (parent frame)", "constituents": "census districts summed into the row", "district_key": "Data Darbar key",
         "enrol_gap_pp": "boys_in_school_pct − girls_in_school_pct", "release": "release tag"}
        | {f"{s}{a}_{m}_5_16": f"{s}{' ' + a.strip('_') if a else ''} {m.replace('_', ' ')}, ages 5–16" for s in ('girls', 'boys') for a in ('', '_rural', '_urban') for m in ('pop', 'enrolled', 'never', 'out_of_school')}
        | {f"{s}{a}_in_school_pct": f"{s}{' ' + a.strip('_') if a else ''} enrolled ÷ population 5–16, %" for s in ('girls', 'boys') for a in ('', '_rural', '_urban')},
        "PBS Population and Housing Census 2023, district Table 13(b), via github.com/fahad-mirza/pakistan_census_2023_tables",
        f"SELECT * FROM read_csv_auto('{(sc_dir / 'census_enrolment_5_16_by_sex.csv').as_posix()}') ORDER BY district",
        unit="persons; per cent",
    )
    EXAMPLES.extend(SCHOOL_EXAMPLES)

    # ── 2c. travel time to care (Adaad, "The unequal road to care") ──────────
    print("health access…")
    ha_dir = REPO / "etl" / "health_access"
    HA_SOURCE = ("Malaria Atlas Project accessibility surfaces (Weiss et al. 2020, Nature Medicine 26: motorised 2019, "
                 "walking-only 2020), clipped to Pakistan; WorldPop 2020 UN-adjusted 1 km population; Data Darbar boundaries. "
                 "Built for Adaad's September 2026 issue.")
    HA_NOTE = ("Travel time is MODELLED: the cost of crossing each 1 km cell on the friction surface, summed along the "
               "least-cost path to the nearest mapped health facility. The facility set behind the surfaces is OpenStreetMap "
               "and Google Maps hospitals and clinics, public and private together, with no information on staffing, opening "
               "hours or quality — so this measures geographic access to a mapped point, not to a working service. Motorised "
               "assumes a vehicle is available; walking-only assumes none. Population weights are WorldPop 2020, which puts "
               "Gilgit-Baltistan at about 1.1 million against 1.7 million in the 2023 census, so the north's headcounts are "
               "understated. Thresholds are strictly greater than 30, 60 or 120 minutes. ")
    register(
        "health_access_tehsil",
        "Travel time to the nearest health facility by tehsil (553 ADM3 polygons): population-weighted means and shares of people beyond 30, 60 and 120 minutes, motorised and walking.",
        HA_NOTE + "mot_/wal_ = motorised / walking-only surface; *_mean is the unweighted mean of valid cells, *_popw_mean the "
        "population-weighted mean; *_pct_pop_gtN the share of people more than N minutes away. Manora Cantonment has no valid "
        "cells: its blanks are missing estimates, not zero. The tehsil grid holds 1,175,119 cells and 219.6 million people, "
        "4,671 cells fewer than the district build because the two boundary files rasterise differently, so tehsil rows do "
        "not recombine exactly to health_access_district (22.10 v 22.13 minutes nationally). The seven ex-FATA merged "
        "districts are Khyber Pakhtunkhwa and carry merged_district = 1. This is the table the map's Health → Travel Time "
        "to Care (tehsil) layer draws.",
        {
            "dd_id": "ADM3 identifier — joins mouza_crosswalk.dd_id, tehsil_satellite.tehsil_id, school_access_tehsil.dd_id",
            "tehsil": "tehsil name", "district_key": "Data Darbar district key", "province": "province or territory",
            "merged_district": "1 for the seven ex-FATA merged districts", "n_px": "valid 1 km cells", "pop_2020": "WorldPop 2020 population on those cells",
            "mot_mean": "motorised minutes, unweighted mean of cells", "mot_popw_mean": "motorised minutes, population-weighted mean",
            "wal_mean": "walking minutes, unweighted mean of cells", "wal_popw_mean": "walking minutes, population-weighted mean",
            "mot_pct_pop_gt30": "% of people more than 30 motorised minutes from care", "mot_pct_pop_gt60": "% more than 60 motorised minutes",
            "mot_pct_pop_gt120": "% more than 120 motorised minutes", "wal_pct_pop_gt30": "% more than 30 walking minutes",
            "wal_pct_pop_gt60": "% more than 60 walking minutes", "wal_pct_pop_gt120": "% more than 120 walking minutes", "release": "release tag",
        },
        HA_SOURCE,
        f"SELECT * FROM read_csv_auto('{(ha_dir / 'travel_time_tehsils_2026-09.csv').as_posix()}') ORDER BY province, district_key, tehsil",
        unit="minutes; per cent",
    )
    register(
        "health_access_district",
        "Travel time to the nearest health facility by district (147): population-weighted medians and means and shares of people beyond 30, 60 and 120 minutes, motorised and walking.",
        HA_NOTE + "*_popw_median is the population-weighted median (the piece's headline measure: 22 minutes motorised, 128 walking "
        "nationally); *_median the unweighted median of cells. Shares were fractions in the piece's file and are per cent here. "
        "Join mpi_districts on district_key for the poverty gradient (Spearman 0.76 between MPI and motorised time; the "
        "poorest MPI quintile is 47 minutes from care motorised against 5 for the least poor). The seven ex-FATA merged "
        "districts are Khyber Pakhtunkhwa and carry merged_district = 1.",
        {
            "district_key": "Data Darbar district key — joins district_indicators, mpi_districts, school_access_district", "district": "district name",
            "province": "province or territory", "merged_district": "1 for the seven ex-FATA merged districts", "n_px": "valid 1 km cells",
            "pop_2020": "WorldPop 2020 population on those cells",
            "mot_mean": "motorised minutes, unweighted mean of cells", "mot_median": "motorised minutes, unweighted median of cells",
            "mot_popw_mean": "motorised minutes, population-weighted mean", "mot_popw_median": "motorised minutes, population-weighted median",
            "wal_mean": "walking minutes, unweighted mean", "wal_median": "walking minutes, unweighted median",
            "wal_popw_mean": "walking minutes, population-weighted mean", "wal_popw_median": "walking minutes, population-weighted median",
            "mot_pct_pop_gt30": "% of people more than 30 motorised minutes from care", "mot_pct_pop_gt60": "% more than 60 motorised minutes",
            "mot_pct_pop_gt120": "% more than 120 motorised minutes", "wal_pct_pop_gt30": "% more than 30 walking minutes",
            "wal_pct_pop_gt60": "% more than 60 walking minutes", "wal_pct_pop_gt120": "% more than 120 walking minutes", "release": "release tag",
        },
        HA_SOURCE,
        f"SELECT * FROM read_csv_auto('{(ha_dir / 'travel_time_districts_2026-09.csv').as_posix()}') ORDER BY province, district",
        unit="minutes; per cent",
    )
    # Facility points: ALHASAN (CC0) and OpenStreetMap via healthsites.io (ODbL).
    from health_facilities.register_health import register_tables as register_health
    register_health(register)
    EXAMPLES.extend(HEALTH_EXAMPLES)

    # ── 3. macro tables lifted from the desktop warehouse ────────────────────
    print("macro…")
    t = (src / "trade_hs8.parquet").as_posix()
    register(
        "trade_hs8",
        "8-digit HS imports and exports, by commodity and partner country, FY2015-16 → FY2024-25.",
        "Values are THOUSAND rupees. fy_* are full fiscal-year (Jul–Jun) cumulative figures; "
        "month_* are June alone. country IS NULL marks the commodity total row — country rows "
        "sum to it, so filter one or the other or you will double-count. Years missing here "
        "exist only as PDFs upstream (see file_catalog).",
        {
            "direction": "'import' or 'export'", "fiscal_year": "e.g. '2020-21' (Jul–Jun)",
            "hs8": "8-digit HS code", "commodity": "commodity description as published",
            "country": "partner country; NULL = all-countries total for that HS8",
            "unit": "quantity unit", "month_qty": "June quantity",
            "month_value_kpkr": "June value, thousand Rs",
            "fy_qty": "fiscal-year cumulative quantity",
            "fy_value_kpkr": "fiscal-year cumulative value, thousand Rs",
        },
        "PBS External Trade Statistics (annual fixed-width TXT + D-10 workbooks)",
        f"""SELECT direction, fiscal_year, hs8, commodity, country, unit,
                   month_qty, month_value_kpkr, fy_qty, fy_value_kpkr
            FROM '{t}' ORDER BY direction, fiscal_year, hs8, country NULLS FIRST""",
        unit="thousand Rs",
    )

    na = (src / "national_accounts.parquet").as_posix()
    register(
        "national_accounts",
        "National accounts / GDP series, 1951-52 → 2025-26 (PBS 2015-16 base).",
        "Units vary BY TABLE: levels are Rs million (tables 2–5, 8–11), growth rates and shares "
        "are percentages (tables 6, 7a/b). Always read table_name before aggregating. "
        "Item labels on the Macro/Main-Aggregate sheets are best-effort.",
        {
            "table_sheet": "sheet name in the PBS workbook", "table_name": "published table title",
            "price_basis": "constant or current prices", "base_year": "price base",
            "item": "sector / indicator label as published",
            "year": "fiscal year, e.g. '2019-20'", "value": "value — unit depends on the table",
        },
        "PBS National Accounts annual tables (2015-16 base)",
        f"SELECT * FROM '{na}' ORDER BY table_sheet, item, year",
    )

    bl = (src / "budget_lines.parquet").as_posix()
    register(
        "budget_lines",
        "Federal budget line items from the Budget in Brief documents, FY2009-10 → FY2026-27.",
        "Rs million. Each printed row carried 1–4 numeric columns; is_own_year_be = TRUE marks "
        "the document's own-year Budget Estimate, which is the only column safe to string into "
        "a time series. The low-numbered 'Budget at a Glance' tables extract noisily and item "
        "wording drifts between years — match with ILIKE and sanity-check.",
        {
            "doc_fy": "fiscal year of the source document", "table_no": "table number in the PDF",
            "table_title": "table title", "item": "line item as printed",
            "col_index": "0-based column position in the printed row",
            "n_cols": "how many numeric columns that row had",
            "col_label": "inferred column meaning", "value_rs_mn": "value, Rs million",
            "is_own_year_be": "TRUE = own-year Budget Estimate (the reliable column)",
        },
        "Finance Division, Budget in Brief (PDF)",
        f"SELECT * FROM '{bl}' ORDER BY doc_fy, table_no, item, col_index",
        unit="Rs million",
    )

    for nm, desc, notes, cols, srcname in [
        ("lsm_qim",
         "Monthly Quantum Index of Manufacturing (large-scale manufacturing).",
         "Index; mom/yoy/cum_chg are percentages.",
         {"month": "YYYY-MM", "qim": "index level", "mom": "% change on previous month",
          "yoy": "% change on same month a year earlier", "cum_qim": "fiscal-year-to-date index",
          "cum_chg": "% change in the FYTD index"},
         "PBS Quantum Index of Manufacturing"),
        ("lsm_sector_indices",
         "LSM indices by manufacturing sector, annual and monthly, with CMI weights.",
         "weight is the sector's share in the index (per the stated base year).",
         {"base": "index base year", "fy": "fiscal year", "sector": "manufacturing sector",
          "weight": "weight in the overall index", "annual_index": "annual index level",
          "month": "YYYY-MM (monthly rows)", "monthly_index": "monthly index level"},
         "PBS LSM / Census of Manufacturing Industries"),
        ("file_catalog",
         "Index of every source file collected for the warehouse, with its upstream URL.",
         "parsed_into_db = FALSE means the file is catalogued but its contents are not in any "
         "table here (mostly scanned PDFs). Use this to check coverage before concluding data "
         "is missing.",
         {"dataset": "collection it belongs to", "category": "sub-category", "period": "period covered",
          "item": "what the file contains", "filename": "file name", "format": "PDF/TXT/xlsx",
          "source_url": "upstream URL at PBS / Finance Division", "status": "download status",
          "notes": "free text", "parsed_into_db": "TRUE if its contents are in a table here"},
         "PBS, Finance Division"),
    ]:
        p = (src / f"{nm}.parquet").as_posix()
        register(nm, desc, notes, cols, srcname, f"SELECT * FROM '{p}'")

    # ── 4. SBP EasyData (optional — appears once build_sbp.py has run) ───────
    sbp_cat = src / "sbp_series_catalog.parquet"
    sbp_obs = src / "sbp_observations.parquet"
    if sbp_cat.exists() and sbp_obs.exists():
        print("sbp…")
        # Two tables rather than one denormalised view on purpose: the observation
        # table is long and the series names/descriptions are verbatim SBP prose, so
        # repeating them per observation would multiply the download for no
        # analytical gain. Users join on series_key — see the examples.
        register(
            "sbp_series_catalog",
            "Every SBP EasyData series held here: name, unit, frequency, coverage, method note.",
            "One row per series — browse this first, then join to sbp_observations on "
            "series_key. DATE CONVENTION: observations are stamped on the LAST day of their "
            "period, and annual series on the last day of the FISCAL year, so FY2023-24 "
            "appears as 2024-06-30. Not every annual series is fiscal though — population "
            "and literacy are calendar years. `available_upto` is SBP's own claim and "
            "several datasets are stale (province-wise banking stops at Jun-2023), so check "
            "it before presenting a series as current.",
            {
                "series_key": "join key to sbp_observations.series_key",
                "dataset_code": "EasyData dataset the series belongs to",
                "dataset_name": "published dataset title",
                "subject_area": "External Sector | Monetary and Financial Sector | Real Sector | "
                                "Public Finance | Interest Rates | Pakistan's Debt Profile",
                "series_name": "published series title",
                "series_short_name": "abbreviated title",
                "frequency": "Daily | Weekly | Monthly | Quarterly | Half-yearly | Annual | As-Needed",
                "unit": "unit as published (PKR, USD, Percent, Index…)",
                "variable_type": "Flow | Stock (level) Variable | ratio",
                "available_since": "first observation date claimed by SBP",
                "available_upto": "last observation date claimed by SBP",
                "last_refresh": "when SBP last revised the series",
                "description": "SBP's own methodological note",
            },
            "State Bank of Pakistan, EasyData API (easydata.sbp.org.pk)",
            f"SELECT * FROM '{sbp_cat.as_posix()}' ORDER BY subject_area, dataset_code, series_key",
        )
        # The data dictionary lists which SBP datasets actually carry observations
        # (the catalogue inventories all 226; only the fetched ones have data).
        ds = con.sql(f"""
            SELECT c.dataset_code, any_value(c.dataset_name) AS name, any_value(c.subject_area) AS subject,
                   count(DISTINCT o.series_key) AS series,
                   strftime(min(o.obs_date), '%Y-%m') AS since, strftime(max(o.obs_date), '%Y-%m') AS upto
            FROM '{sbp_obs.as_posix()}' o JOIN '{sbp_cat.as_posix()}' c USING (series_key)
            WHERE o.value IS NOT NULL GROUP BY 1 ORDER BY subject, series DESC, 1""").fetchall()
        for t in tables:
            if t["name"] == "sbp_series_catalog":
                t["datasets"] = [{"code": r[0], "name": r[1], "subject": r[2], "series": r[3],
                                  "since": r[4], "upto": r[5]} for r in ds]
        register(
            "sbp_observations",
            "The macro time-series panel: every observation of every SBP series held here.",
            "One row per series × date. UNITS ARE PER-SERIES — join to sbp_series_catalog and "
            "read `unit` before summing anything, because this table mixes rupees, dollars, "
            "percentages and index levels in one `value` column. value IS NULL where SBP "
            "suppressed or has not published the figure and `status` says which ('Normal', "
            "'Missing value', …) — a NULL is not a zero. Frequencies are mixed, so filter on "
            "the catalogue's `frequency` before resampling or averaging across series. "
            "DOUBLE-COUNTING TRAP (country-wise remittances, TS_GP_BOP_WR_M): the country series "
            "are hierarchical, so summing them all overstates the total by ~42% (FY2024-25: "
            "54,384 vs the published 38,299 Mn USD). 'U.A.E.' already contains Dubai, Abu Dhabi, "
            "Sharjah and 'Other four U.A.E.'s States'; 'Other GCC Countries excluding Saudi Arabia "
            "& U.A.E.' already contains Bahrain, Kuwait, Oman and Qatar; 'ten European Countries' "
            "already contains Belgium, Denmark, France, Germany, Greece, Ireland, Italy, "
            "Netherland, Spain and Sweden — Norway and Switzerland are reported separately. "
            "The partition that reconciles exactly to the published total is: Saudi Arabia, "
            "U.A.E., U.K., U.S.A., Other GCC, ten European Countries, Norway, Switzerland, "
            "Australia, Canada, Japan, Malaysia, South Africa, South Korea, Other Countries. "
            "The same shape applies to province-wise banking (TS_GP_BAM_ADVDEP_HY), where "
            "'all Pakistan' is the total of the regional series.",
            {
                "series_key": "joins to sbp_series_catalog.series_key",
                "dataset_code": "EasyData dataset code, denormalised for cheap filtering",
                "obs_date": "observation date — END of the period (see catalogue notes)",
                "value": "the observation; unit depends on the series",
                "status": "SBP observation status; anything but 'Normal' needs care",
                "comment": "SBP status comment, usually empty",
            },
            "State Bank of Pakistan, EasyData API (easydata.sbp.org.pk)",
            f"SELECT * FROM '{sbp_obs.as_posix()}' ORDER BY series_key, obs_date",
        )
        EXAMPLES.extend(SBP_EXAMPLES)
    else:
        print("sbp…  skipped (run build_sbp.py catalog / observations / load first)")

    # ── 4b. long-run macro history: the gaps the Global Macro Database exposed ──
    # The GMD's terms forbid republishing it, so these come from the sources it
    # compiles: SBP's own Handbook, the IMF and the World Bank. Nothing is
    # spliced; where two sources cover a year both are kept, labelled.
    mh = (sorted(src.glob("macro_history/*/sbp_handbook_series.parquet")) or [None])[-1]
    if mh:
        print("macro history…")
        d = mh.parent
        register(
            "sbp_handbook_series",
            "Pakistan’s long macro series from SBP’s Handbook of Statistics 2020: money "
            "since 1950, real GDP and prices since FY50, consolidated public finance since FY76.",
            "Four Handbook tables, every figure as SBP printed it: 4.1 monetary statistics "
            "(currency, reserve money M0, narrow money M1, broad money M2, from 1950), 1.5 GDP "
            "at constant factor cost (FY50 on, SBP’s own splice to the 2005-06 base), 2.8 "
            "price indices and GDP deflator (FY50 on) and 3.7 the consolidated federal and "
            "provincial budget (FY76 to FY20). BLOCKS ARE NOT ONE SERIES: table 4.1 prints three "
            "blocks under different definitions - 1950-85 annual, 1986-2007 with foreign-currency "
            "deposits in M2, 2003-20 on the current definition - and they overlap, so filter on "
            "block. Up to 1971 the money figures are for Pakistan with its east wing - currency in circulation falls from 8,157 to 5,173 million rupees between 1971 and 1972 - and from 1971 the net foreign assets are former West Pakistan\u2019s (footnote 5); "
            "annual rows before 1991 do not state their month. Index bases change down the "
            "column in 2.8 and are carried in base. A dash is NULL with the dash kept in "
            "value_text; a figure printed with a footnote star or a damaged bracket keeps its "
            "mark. Fiscal years are named by the year they end (FY50 = 1949-50 = 1950).",
            {"chapter": "Handbook chapter (1 national income, 2 prices, 3 public finance, 4 money)",
             "table_id": "Handbook table number, e.g. 4.1",
             "table_title": "what the table holds",
             "unit": "unit as printed (Million Rupees, index, percent of GDP)",
             "block": "the printed block within the table; definitions differ between blocks",
             "series_no": "column number SBP prints under the heading, where it prints one",
             "series": "column or row heading as printed, footnote digits included",
             "period": "period as printed: 1950, 1991 Jun, FY76",
             "year": "calendar year, or for a fiscal year the year it ends",
             "year_basis": "calendar, or fiscal (July-June, year it ends)",
             "month": "6 or 12 for the half-yearly rows; NULL where SBP gives a year only",
             "value": "the figure as printed",
             "value_text": "the cell as printed where it is not a plain number (a dash, a star)",
             "mark": "footnote marker printed with the period or the figure",
             "base": "index base year in force for this figure (table 2.8)",
             "source_line": "the table’s own Source line",
             "notes": "the table’s footnotes, verbatim"},
            "State Bank of Pakistan, Handbook of Statistics on Pakistan Economy 2020 "
            "(archive.sbp.org.pk); SBP terms: reuse with reference to the source, non-commercial, "
            "unchanged",
            f"SELECT * FROM '{(d / 'sbp_handbook_series.parquet').as_posix()}' "
            "ORDER BY chapter, table_id, block, series_no, series, year, month",
            unit="as printed per table: million rupees, index points or percent of GDP",
        )
        register(
            "imf_pakistan_fiscal",
            "Pakistan’s public finances since 1950 from the IMF: revenue, spending, "
            "interest, primary balance and debt as a share of GDP.",
            "Four IMF datasets, kept apart: Public Finances in Modern History (FPP, Mauro et al. "
            "2015; 1950-2024), the Global Debt Database (GDD, central government debt from 1951), "
            "the Fiscal Monitor (FM, general government from the 1990s) and four World Economic "
            "Outlook series (WEO: unemployment, population, inflation, current account, from "
            "1980). The coverage differs - FPP and GDD are central or general government by "
            "period, FM is general government - so the same year can carry two different debt "
            "ratios; that is the sources disagreeing about scope, not an error. From 2026 the WEO "
            "and FM figures are projections (is_projection). For Pakistan’s own consolidated "
            "budget in rupees see sbp_handbook_series table 3.7.",
            {"dataset": "FPP, GDD, FM or WEO",
             "dataset_name": "the IMF database and edition",
             "indicator": "IMF indicator code",
             "label": "IMF’s label",
             "unit": "unit as the IMF gives it",
             "year": "year as the IMF reports it",
             "value": "the IMF’s figure",
             "is_projection": "TRUE where the figure is an IMF projection, not an outturn"},
            "International Monetary Fund: Public Finances in Modern History, Global Debt Database, "
            "Fiscal Monitor and World Economic Outlook (DataMapper)",
            f"SELECT * FROM '{(d / 'imf_pakistan_fiscal.parquet').as_posix()}' "
            "ORDER BY dataset, indicator, year",
            unit="percent of GDP, percent, millions of people",
        )
        register(
            "wdi_comparators",
            "Pakistan beside its neighbours and peers: 27 World Bank indicators since 1960.",
            "World Development Indicators for Pakistan, India, Bangladesh, Sri Lanka, Nepal, "
            "Afghanistan, Iran, Egypt, Indonesia, Nigeria and Türkiye, with South Asia, "
            "lower-middle income and the world as context: population, growth, income per head, "
            "inflation, unemployment, trade, investment, remittances, tax, literacy, life "
            "expectancy, fertility, child mortality, poverty and electricity. Several are "
            "modelled estimates rather than national figures - unemployment is the ILO’s "
            "model (SL.UEM.TOTL.ZS); SL.UEM.TOTL.NE.ZS is the national estimate - and "
            "original_source names who produced each. For Pakistan’s own series prefer the "
            "PBS and SBP tables; this is for comparison.",
            {"country_code": "ISO3, or the World Bank’s code for an aggregate",
             "country": "country or aggregate name",
             "indicator": "WDI indicator code",
             "label": "WDI indicator name, unit included",
             "year": "calendar year",
             "value": "the figure",
             "original_source": "the organisation WDI credits for the series"},
            "World Bank, World Development Indicators (CC BY 4.0)",
            f"SELECT * FROM '{(d / 'wdi_comparators.parquet').as_posix()}' "
            "ORDER BY indicator, country_code, year",
            unit="as given in each indicator’s label",
        )

    # ── 4c. the 1998 census, and population back to 1951 ─────────────────────
    # What PBS still serves of 1998 is the summary layer; the District Census
    # Reports are print-only. Both tables are read from text PDFs PBS exported
    # from spreadsheets (etl/census1998), figures as printed.
    c98 = (sorted(src.glob("census1998/*/census_admin_units_1951_1998.parquet")) or [None])[-1]
    if c98:
        print("census 1998…")
        register(
            "census_admin_units_1951_1998",
            "Population and area of every province, district, sub-division, tehsil and town at "
            "the 1951, 1961, 1972, 1981 and 1998 censuses, rural and urban.",
            "PBS\u2019s Area & Population of Administrative Units (1998). One row per unit, "
            "locality and census. The units are the 1998 frame, with each earlier census restated "
            "onto it, so a district created after 1951 has no total for 1951 (a dash, NULL here) "
            "although its towns may already be counted; summing districts at an early census "
            "therefore falls short - use the province row. Levels: country, province, district "
            "(districts, tribal agencies and frontier regions), sub_division and tehsil (in "
            "Balochistan tehsils sit inside sub-divisions, elsewhere they stand alone - do not add "
            "both), and urban_locality (municipal and town committees, cantonments; urban "
            "population only). Peshawar is printed twice over, as Towns 1-4 and as the city\u2019s "
            "rural and urban parts: the parts are district_part and must not be added to the "
            "towns. Checked: Pakistan equals its provinces at all five censuses, and every "
            "district equals its sub-units in 1998. Two inconsistencies are PBS\u2019s and are left "
            "as printed: Tharparkar\u2019s rural and urban do not add to its total in 1972 and "
            "1981. Footnote marks are kept in mark; the footnotes themselves are in the source "
            "PDF. FATA, Islamabad and all four provinces are included; AJK and Gilgit-Baltistan "
            "were not in the census.",
            {"table_no": "table in the publication (1 Pakistan, 2 NWFP, 3 FATA, 4 Punjab, "
                         "5 Sindh, 6 Balochistan, 7 Islamabad)",
             "province": "province or area as printed",
             "district": "district, agency or frontier region the unit belongs to",
             "sub_division": "sub-division the unit belongs to, where it has one",
             "tehsil": "tehsil, taluka or town the unit belongs to, where it has one",
             "unit_type": "country, province, district, district_part, sub_division, tehsil "
                          "or urban_locality",
             "unit": "name as printed",
             "locality": "all, rural or urban",
             "census_year": "1951, 1961, 1972, 1981 or 1998",
             "population": "persons; NULL where PBS prints a dash",
             "mark": "footnote mark printed beside the figure",
             "area_sq_km": "area in square kilometres, on the unit\u2019s all-locality rows"},
            "Pakistan Bureau of Statistics, Population Census 1998: Area & Population of "
            "Administrative Units (pbs.gov.pk census archive)",
            f"SELECT * FROM '{c98.as_posix()}' ORDER BY table_no, census_year",
            unit="persons; area in square kilometres",
        )
        register(
            "census1998_district_glance",
            "The 1998 census district by district: population by sex, urban share, density, "
            "household size, literacy by sex, growth since 1981 and housing amenities.",
            "PBS\u2019s District at a Glance (1998), one sheet per district, long format: about "
            "twenty indicators each. These sheets are on the districts as they stood after the "
            "2000-01 changes - Umerkot apart from Mirpur Khas, City District Karachi as one - so "
            "they do not match census_admin_units_1951_1998, which keeps the 1998 frame (Mirpur "
            "Khas there includes Umerkot\u2019s 663,095). Charsadda\u2019s sheet is linked by PBS "
            "but the file is refused by its server, so Charsadda is absent; its population is in "
            "the admin units table. District names are as PBS titled them, misspellings included "
            "(SHAIWAL, JACCOBABAD). share_pct is the percentage PBS prints beside a figure. A "
            "figure that cannot be what its label says is not read: Ghotki\u2019s household size "
            "is printed 505, so value is NULL and flag says why.",
            {"district": "district as titled on the sheet",
             "title": "the sheet\u2019s full title",
             "file": "the PBS file it was read from",
             "indicator": "machine name of the indicator",
             "label": "label as printed",
             "value": "the figure",
             "share_pct": "percentage printed beside it, e.g. urban population (82.44 %)",
             "unit": "what the figure counts",
             "as_printed": "the figure and its percentage as printed",
             "flag": "why a printed figure was not read"},
            "Pakistan Bureau of Statistics, Population Census 1998: District at a Glance "
            "(pbs.gov.pk census archive)",
            f"SELECT * FROM '{(c98.parent / 'census1998_district_glance.parquet').as_posix()}' "
            "ORDER BY district, indicator",
            unit="per indicator: persons, percent, units, sq km",
        )

    # ── 5. census panels: the 2017 and 2023 unit tables ──────────────────────
    # Two tables, deliberately not one. The years cannot be stacked yet: only 50
    # of 2017's 377 indicator labels appear verbatim among 2023's 201, and most of
    # the difference is cosmetic rather than real ("00 - 04" vs "00 -- 04"), so a
    # stacked table would look comparable while silently splitting age bands into
    # separate rows. The crosswalk that would make a cross-year join safe is the
    # geography register work, which is not done.
    def _latest(pattern):
        hits = sorted(src.glob(pattern))
        return hits[-1] if hits else None

    CENSUS_SHARED = {
        "census_year": "2017 or 2023",
        "province_area": "province, or FATA / Islamabad Capital Territory — the census frame has "
                         "four provinces and two federal areas, not six provinces",
        "table_id": "PBS table number WITHIN that census. Numbering is not comparable across "
                    "years: 2017's table 23 is a locality table, 2023's is drinking water",
        "district": "district the unit sits in; equal to unit on district rows",
        "unit": "the published unit's own name",
        "unit_type": "district, tehsil, sub_tehsil, sub_division or other. Rows of DIFFERENT "
                     "unit_type are nested, so summing across them double-counts",
        "locality": "all, rural or urban. rural + urban = all, so pick one",
        "sex": "all, male, female or transgender. The three sexes sum to all, so pick one",
        "indicator": "the indicator as PBS labels it, joined with / down the header hierarchy",
        "col_label": "the column heading the value sat under, kept because the same indicator "
                     "can appear under several columns",
        "value": "the published figure — unit depends on the indicator, see notes",
        "missing": "the cell was printed as a dash rather than a number",
    }

    # Both censuses are drawn on one frame - PBS's Digital Census 2023 - and the
    # unit map says, per unit, what that means: which shape it is, and whether a
    # 2017 figure belongs on a 2023 shape at all. It is built by
    # etl/census2017/build_unit_map_2023.py from the crosswalk, which is itself
    # checked against PBS's own restatement of the 2017 population. Joining a
    # reviewed table here, rather than matching names at build time, means the
    # eight real boundary changes are handled by a decision instead of a
    # near-miss.
    UNIT_MAP = (REPO / 'etl' / 'census2017' / 'census_unit_map.csv').as_posix()

    def _map_join(year):
        return (f"LEFT JOIN read_csv('{UNIT_MAP}', header=true, quote='\"', escape='\"',\n"
                "                   types={'census_year': 'INTEGER', 'map_key': 'VARCHAR',\n"
                "                          'weight': 'DOUBLE'}) AS m\n"
                f"  ON m.census_year = {year}\n"
                " AND m.unit_type = CASE WHEN p.unit_type = 'district'\n"
                "                        THEN 'district' ELSE 'tehsil' END\n"
                " AND m.district = p.district AND m.unit = p.unit")

    MAP_COLS = """nullif(m.map_key, '') AS map_key,
                       m.relation AS map_relation,
                       coalesce(m.comparable, 'no') AS map_comparable,
                       nullif(m.note, '') AS map_note, m.weight AS map_weight"""

    _MAP_DOCS = {
        "map_key": "the PBS Digital Census 2023 shape this row is drawn on \u2014 the district "
                   "code on district rows, dds_id below that. NULL where no 2023 shape can "
                   "carry the figure, which for 2017 means the unit was split or redrawn",
        "map_relation": "how this unit relates to the 2023 frame: exact, renamed, merged, "
                        "split, boundary transfer, or restructured below the district",
        "map_comparable": "yes where the figure belongs on the 2023 shape as published; "
                          "combined where two 2017 units share one 2023 shape and must be "
                          "added, or averaged on map_weight for a rate; flagged where the "
                          "district persists but its territory changed; no where nothing can "
                          "honestly be drawn",
        "map_note": "the reason, in words, for anything other than a straight match \u2014 "
                    "written to be shown to a reader, not parsed",
        "map_weight": "2017 population, the weight for averaging a rate across units that "
                      "combine into one 2023 shape",
    }

    # A WHOLLY RURAL UNIT HAS AN URBAN PROPORTION OF NIL, AND PBS PRINTS A DASH.
    # The dash arrived as NULL, so 203 of the 591 tehsils in 2023 (16 of 536 in
    # 2017) were blank on the urban-proportion map rather than at zero - a
    # fifth of the country missing from a featured measure. Only where the
    # unit has no urban population anywhere in table 1 is the dash read as 0;
    # missing stays TRUE, as it does for 2017's recovered dashes, so the
    # printed dash is still on record.
    def _rural_join(src):
        return (f"LEFT JOIN (SELECT DISTINCT coalesce(district, '') AS d, unit AS u, unit_type AS ut\n"
                f"           FROM '{src}' WHERE table_id = '1' AND locality = 'urban'\n"
                f"             AND value > 0) AS urb\n"
                "  ON urb.d = coalesce(p.district, '') AND urb.u = p.unit AND urb.ut = p.unit_type")

    RURAL_ZERO = """CASE WHEN p.value IS NULL AND p.missing AND p.table_id = '1'
                             AND p.locality = 'all' AND urb.u IS NULL
                             AND upper(p.indicator) LIKE '%URBAN PROPORTION%'
                        THEN 0 ELSE p.value END AS value"""

    p17 = _latest("census2017/*/panel/panel_2017.parquet")
    if p17:
        print("census 2017…")
        register(
            "census_panel_2017",
            "Population and Housing Census 2017 unit tables: 35 of 40 tables at district, "
            "tehsil, sub-tehsil and sub-division level.",
            "Do not sum across unit_type, locality or sex — each is a nested hierarchy and "
            "adding the levels together double- or triple-counts. Tables are not uniform about the sex column: in table 1, 5,997 rows carry the sex split in the indicator label while sex stays 'all', so filter on the indicator there rather than on sex. Many indicators are RATES or "
            "PERCENTAGES (literacy, sex ratio, growth) and must never be summed; read the "
            "indicator label before aggregating. missing = TRUE marks a cell PBS printed as a "
            "dash; in this panel 899,800 of those 901,438 rows carry a recovered value of 0, "
            "read back from the combined district PDFs, so the zero is real rather than absent "
            "— unlike 2023, where a dash is left NULL. series_ambiguous = TRUE on 31,963 rows "
            "flags a series whose key is not unique within its table, so a filter on indicator "
            "and col_label alone may return more than one series there. Tables 23–26 are the "
            "individual-locality tables and are not in this panel. Comparing to "
            "census_panel_2023 by table_id or indicator is unsafe: the numbering differs and "
            "only 50 indicator labels match verbatim.",
            {**CENSUS_SHARED, **_MAP_DOCS,
             "is_rate": "the indicator reads as a rate, ratio, average, density or proportion "
                        "and must not be summed. PBS publishes no such flag for 2017, unlike "
                        "2023, so this is read off the label here: a guide, not the census\u2019s "
                        "own statement",
             "series_ambiguous": "the series key is not unique within this table (31,963 rows)"},
            "PBS Population and Housing Census 2017, per-district Excel tables (135 districts × 40 tables)",
            f"""SELECT p.census_year, p.province_area, p.table_id, p.district, p.unit,
                       p.unit_type, {MAP_COLS},
                       p.locality, p.sex, p.indicator, p.col_label, {RURAL_ZERO}, p.missing,
                       regexp_matches(upper(p.indicator || ' ' || p.col_label),
                                      '{RATE_WORDS}') AS is_rate,
                       p.series_ambiguous
                FROM '{p17.as_posix()}' AS p
                {_map_join(2017)}
                {_rural_join(p17.as_posix())}
                ORDER BY p.table_id, p.unit_type, p.province_area, p.district, p.unit,
                         p.locality, p.sex, p.indicator, p.col_label""",
            unit="persons, households or housing units; rates and percentages where the indicator says so",
        )

    p23 = _latest("stage2/optionB-*/warehouse/census2023_observations.parquet")
    if p23:
        print("census 2023…")
        register(
            "census_panel_2023",
            "Population and Housing Census 2023 unit tables: 27 tables at district, tehsil, "
            "sub-tehsil and sub-division level.",
            "Do not sum across unit_type, locality or sex — each is a nested hierarchy and "
            "adding the levels together double- or triple-counts. Tables are not uniform about the sex column: in tables 1, 3, 21 and 25, 15,141 rows carry the sex split in the indicator label while sex stays 'all', so filter on the indicator there rather than on sex. Table 1 also republishes a POPULATION 2017 column beside the 2023 figure — that is PBS's own comparison and is safe to use, unlike joining this panel to census_panel_2017. is_rate = TRUE marks the "
            "624,219 rows that are rates or percentages and must never be summed. missing = "
            "TRUE on 1,565,334 rows marks a cell PBS printed as a dash, and unlike the 2017 "
            "panel the value is left NULL rather than recovered as zero, so a count of "
            "non-missing cells is not comparable between the two years. renderings_disagree = "
            "TRUE on 27,033 rows flags a figure where PBS's two published renderings of the "
            "same table do not agree. adm3_pcode and dd_id are the geographic join keys and are "
            "incomplete by design: both are NULL on every district row, and dd_id — the key the "
            "site's own tehsil geometry uses — is present on 1,919,489 of 2,069,879 tehsil rows. "
            "Comparing to census_panel_2017 by table_id or indicator is unsafe: the numbering "
            "differs and only 50 indicator labels match verbatim. PBS's own spelling is kept, "
            "including 'EDUCATOINAL ATTAINMENT'.",
            {**CENSUS_SHARED,
             **_MAP_DOCS,
             "dds_id": "Data Darbar sub-district identifier, present on every row",
             "dd_id": "identifier used by the site's tehsil geometry; NULL on district rows",
             "adm3_pcode": "COD-AB ADM3 code; NULL on district rows and on units without a match",
             "is_rate": "the value is a rate or percentage and must not be summed",
             "value_corrected": "the figure was corrected against the other rendering",
             "renderings_disagree": "PBS's two renderings of this table disagree here",
             "unit_source": "the district workbook this row was read from"},
            "PBS Population and Housing Census 2023, Excel tables (both published renderings)",
            f"""-- Sub-district units key on dds_id, the census's own unit id,
                -- because PBS's Digital Census 2023 layer draws one polygon per
                -- unit under that id. The map used to key on dd_id, a 2017
                -- boundary, and 51 units shared 23 of those shapes: Lahore City,
                -- Model Town, Raiwind and Shalimar all landed on one 2017 Lahore
                -- polygon, so none of them could be drawn without choosing
                -- arbitrarily between them. All 591 are now drawable. Districts
                -- key on PBS's own district code, which reaches all 136; the 2015
                -- polygon layer this used to match names against reached 128.
                SELECT 2023 AS census_year, p.province_area, p.table_id, p.dds_id, p.dd_id,
                       p.adm3_pcode, p.district, p.unit, p.unit_type, {MAP_COLS},
                       p.locality, p.sex, p.indicator, p.col_label,
                       {RURAL_ZERO}, p.missing, p.is_rate, p.value_corrected,
                       p.renderings_disagree, p.unit_source
                FROM '{p23.as_posix()}' AS p
                {_map_join(2023)}
                {_rural_join(p23.as_posix())}
                ORDER BY p.table_id, p.unit_type, p.province_area, p.district, p.unit,
                         p.locality, p.sex, p.indicator, p.col_label""",
            unit="persons, households or housing units; rates and percentages where is_rate is TRUE",
        )

    p98 = (sorted(src.glob("census1998/*/panel_1998.parquet")) or [None])[-1]
    if p98:
        print("census 1998 panel…")
        register(
            "census_panel_1998",
            "The 1998 census on PBS\u2019s 2023 boundaries: population for every district and "
            "tehsil, and the district indicators PBS published for 1998.",
            "Two layers, from what PBS still publishes. table_id '1': POPULATION - 1998, all / "
            "rural / urban, for every district and every sub-district (tehsil, taluka, "
            "sub-division, sub-tehsil) - PBS\u2019s own restatement of 1998 onto the 2017 units, "
            "printed in the 2017 census\u2019s table 1; it sums to 132,352,279, the published "
            "1998 total, at both tiers, and its map_key reaches all 136 districts and all 591 "
            "tehsils of the 2023 frame through the same unit map as the 2017 panel. table_id "
            "'glance': the District at a Glance indicators - population by sex, sex ratio, "
            "density, household size, literacy by sex, 1981 population and growth since, "
            "housing units and their amenities - for districts only. Each glance district is "
            "linked to the 2017 districts it became in groups whose 1998 populations balance "
            "against PBS\u2019s restatement to the person (etl/census1998/glance_crosswalk.py); "
            "86 groups balance, and Bannu, Nawabshah and Upper Dir, which do not, carry no "
            "map_key rather than a wrong one. Charsadda has no glance sheet. A glance district "
            "drawn across several 2023 shapes is map_comparable 'combined': add counts, and "
            "average rates on map_weight, the district\u2019s 1998 population. Below the "
            "district nothing but population survives online: the District Census Reports "
            "are print-only. Do not sum across unit_type, locality or sex.",
            {**CENSUS_SHARED, **_MAP_DOCS,
             "table_id": "'1' for population (PBS\u2019s restatement in the 2017 census) or "
                         "'glance' for the District at a Glance indicators",
             "unit": "the 2017 unit the 1998 population is restated on (table 1), or the "
                     "glance district as PBS titled it",
             "map_weight": "the unit\u2019s own 1998 population, the weight for averaging a "
                           "rate across units drawn on one shape",
             "is_rate": "the value is a rate, ratio or share and must not be summed",
             "published_in": "the PBS publication the figure was read from",
             "districts_2017": "glance rows: the 2017 districts this glance district\u2019s "
                               "balanced group became, joined by ' + '",
             "glance_group": "glance rows: the glance districts in that group"},
            "Pakistan Bureau of Statistics, Population Census 1998 (District at a Glance) and "
            "Census 2017 table 1 (POPULATION 1998, PBS\u2019s restatement)",
            f"SELECT * FROM '{p98.as_posix()}' ORDER BY table_id, unit_type, district, unit, "
            "locality, sex, indicator, col_label",
            unit="persons; rates and shares where is_rate is TRUE",
        )

    ph = (sorted(src.glob("census1998/*/panel_1951_1981.parquet")) or [None])[-1]
    if ph:
        print("census 1951-1981 panel…")
        register(
            "census_panel_1951_1981",
            "Pakistan\u2019s population at the 1951, 1961, 1972 and 1981 censuses, on PBS\u2019s "
            "2023 districts.",
            "From PBS\u2019s Area & Population of Administrative Units 1951-1998, which restates "
            "the earlier censuses on the 115 districts and agencies of 1998. Each 1998 district "
            "is linked to the 2017 districts it became only where its 1998 population equals "
            "PBS\u2019s restated 1998 population of those districts to the person - all 115 do "
            "(etl/census1998/glance_crosswalk.py) - and through them to the 2023 shapes. A row "
            "is a footprint: one 1998 district, or several drawn as one where they share a "
            "2023 shape (a Frontier Region and its host district) or where PBS counted one "
            "inside another at that census (the table\u2019s footnotes: Lower Dir in Upper Dir "
            "in 1951 and 1961, Kohistan and Batagram in Mansehra, much of Balochistan in 1951). "
            "Bolan, Jafarabad and Jhal Magsi are blank in 1951 with no footnote; they are drawn "
            "with Sibi and Kalat and the note says so. Every year sums to PBS\u2019s published "
            "national total. Where a district printed no total, its printed urban or rural part "
            "is counted and the rest is in the district that counted it. The schema is "
            "census_panel_1998\u2019s; select one census_year.",
            {**CENSUS_SHARED, **_MAP_DOCS,
             "census_year": "1951, 1961, 1972 or 1981",
             "unit": "the 1998 district or districts the footprint is made of, joined by ' + '",
             "map_weight": "the footprint\u2019s population at that census",
             "published_in": "the PBS publication the figure was read from"},
            "Pakistan Bureau of Statistics, Area & Population of Administrative Units by "
            "Rural/Urban: 1951-1998 Censuses",
            f"SELECT * FROM '{ph.as_posix()}' ORDER BY census_year, map_key, locality",
            unit="persons",
        )
    hist = (sorted(src.glob("census1998/*/population_history.parquet")) or [None])[-1]
    if hist:
        register(
            "census_population_history",
            "Population at every census from 1951 to 2023, for Pakistan, each province and "
            "each district, on boundaries that hold still.",
            "One series per place for a chart of population over time. Pakistan and the "
            "provinces as PBS prints them (1951-1998, table 1 of the administrative units "
            "table). Districts as footprints on the 2023 frame that are the same ground in "
            "every year shown: a 1998 district with the 2017 districts it became, joined to "
            "its neighbour where the two cannot be told apart - Lahore and Kasur, which "
            "exchanged ground between 1998 and 2017, are one series. 1951 and 1961 appear "
            "only where PBS counted the footprint on its own that year. 1998 and earlier "
            "are PBS\u2019s restatement on 1998 districts; 2017 is table 1 of the 2017 census; "
            "2023 is table 1 of the 2023 census. Each year\u2019s district rows sum to the "
            "published national total.",
            {"level": "country, province or district",
             "map_key": "the 2023 district shapes the footprint covers (districts only)",
             "place": "the 1998 district or districts of the footprint, or the province",
             "today": "the 2017 districts the footprint is today (2023 splits aside)",
             "province": "the province of the footprint\u2019s largest district",
             "census_year": "1951, 1961, 1972, 1981, 1998, 2017 or 2023",
             "population": "persons enumerated",
             "relation": "exact for one district on one shape; combined otherwise"},
            "Pakistan Bureau of Statistics: Administrative Units 1951-1998; Census 2017 and "
            "2023 table 1",
            f"SELECT * FROM '{hist.as_posix()}' ORDER BY level, place, census_year",
            unit="persons",
        )

    if p17 or p23:
        # The picker needs to know what can be mapped without loading 9 MB to
        # find out. One row per selectable series, with the count of units that
        # actually carry a value and a polygon, so a series that would colour
        # four districts can be shown as such instead of looking empty.
        titles = census_table_titles(REPO)
        _title_values = ", ".join(
            "(" + str(y) + ", '" + t + "', '" + ttl.replace("'", "''") + "')"
            for (y, t), ttl in sorted(titles.items()))
        parts = []
        if p17:
            parts.append(f"""
              SELECT 2017 AS census_year, table_id,
                     CASE WHEN unit_type = 'district' THEN 'district'
                          ELSE 'tehsil' END AS unit_type, indicator, col_label,
                     locality, sex, bool_or(is_rate) AS is_rate,
                     -- map_key can name several shapes at once, where a unit
                     -- that was later split is drawn across its successors, so
                     -- the shapes are counted after expanding it rather than by
                     -- counting keys.
                     length(list_distinct(flatten(list(str_split(map_key, ' '))
                       FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)))) AS mappable_units,
                     count(DISTINCT district || '|' || unit) FILTER (WHERE value IS NOT NULL) AS units_with_value,
                     min(value) AS min_value, max(value) AS max_value
              FROM '{(OUT / 'census_panel_2017.parquet').as_posix()}'
              -- Every unit below the district sits on one layer: PBS publishes
              -- some as tehsils, some as sub-divisions and some as sub-tehsils,
              -- and its own 2023 boundary file draws all 591 in a single set.
              -- They are one geography for the map, whatever PBS calls them.
              GROUP BY ALL
              -- The guarantee is one value per census unit, which is what makes
              -- a series safe to draw. It is deliberately not one value per
              -- shape: six 2017 districts absorbed an FR, so two 2017 units
              -- legitimately share one 2023 shape and are combined when drawn.
              -- Testing shapes instead of units dropped 13,553 series that are
              -- perfectly well defined.
              HAVING count(DISTINCT map_key) FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL) > 0
                 AND count(*) FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)
                   = count(DISTINCT district || '|' || unit)
                       FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)""")
        if p23:
            parts.append(f"""
              SELECT 2023 AS census_year, table_id,
                     CASE WHEN unit_type = 'district' THEN 'district'
                          ELSE 'tehsil' END AS unit_type, indicator, col_label,
                     locality, sex, bool_or(is_rate) AS is_rate,
                     -- map_key can name several shapes at once, where a unit
                     -- that was later split is drawn across its successors, so
                     -- the shapes are counted after expanding it rather than by
                     -- counting keys.
                     length(list_distinct(flatten(list(str_split(map_key, ' '))
                       FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)))) AS mappable_units,
                     count(DISTINCT district || '|' || unit) FILTER (WHERE value IS NOT NULL) AS units_with_value,
                     min(value) AS min_value, max(value) AS max_value
              FROM '{(OUT / 'census_panel_2023.parquet').as_posix()}'
              -- Every unit below the district sits on one layer: PBS publishes
              -- some as tehsils, some as sub-divisions and some as sub-tehsils,
              -- and its own 2023 boundary file draws all 591 in a single set.
              -- They are one geography for the map, whatever PBS calls them.
              GROUP BY 1, 2, 3, 4, 5, locality, sex
              HAVING count(DISTINCT map_key) FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL) > 0
                 AND count(*) FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)
                   = count(DISTINCT district || '|' || unit)
                       FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)""")
        if p98:
            parts.append(f"""
              SELECT 1998 AS census_year, table_id,
                     CASE WHEN unit_type = 'district' THEN 'district'
                          ELSE 'tehsil' END AS unit_type, indicator, col_label,
                     locality, sex, bool_or(is_rate) AS is_rate,
                     length(list_distinct(flatten(list(str_split(map_key, ' '))
                       FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)))) AS mappable_units,
                     count(DISTINCT district || '|' || unit) FILTER (WHERE value IS NOT NULL) AS units_with_value,
                     min(value) AS min_value, max(value) AS max_value
              FROM '{(OUT / 'census_panel_1998.parquet').as_posix()}'
              GROUP BY 1, 2, 3, 4, 5, locality, sex
              HAVING count(DISTINCT map_key) FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL) > 0
                 AND count(*) FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)
                   = count(DISTINCT district || '|' || unit)
                       FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)""")
            _title_values += (", (1998, '1', 'Population 1998, restated by PBS on the 2017 units "
                              "(Census 2017 table 1)'), (1998, 'glance', 'District at a Glance (1998)')")
        if ph:
            parts.append(f"""
              SELECT census_year, table_id, 'district' AS unit_type, indicator, col_label,
                     locality, sex, bool_or(is_rate) AS is_rate,
                     length(list_distinct(flatten(list(str_split(map_key, ' '))
                       FILTER (WHERE value IS NOT NULL AND map_key IS NOT NULL)))) AS mappable_units,
                     count(DISTINCT unit) FILTER (WHERE value IS NOT NULL) AS units_with_value,
                     min(value) AS min_value, max(value) AS max_value
              FROM '{(OUT / 'census_panel_1951_1981.parquet').as_posix()}'
              GROUP BY ALL""")
            _title_values += "".join(
                f", ({y}, '1', 'Population {y}, restated by PBS on the districts of 1998')"
                for y in (1951, 1961, 1972, 1981))
        register(
            "census_series_index",
            "One row per census series that the district and tehsil map can colour, "
            "for both census years.",
            "This is a guide to the two census panels, not data in its own right: every row "
            "points at a series in census_panel_2017 or census_panel_2023, which is where the "
            "values are. mappable_units counts the units that have both a value and a boundary, "
            "so it is the number of shapes that would actually be coloured — a series with a "
            "low count is thin, not broken. is_rate marks series that must not be summed. "
            "Series are listed per census year and must not be compared across years on "
            "table_id or indicator; see either panel's notes.",
            {
                "census_year": "1951, 1961, 1972, 1981, 1998, 2017 or 2023",
                "table_id": "PBS table number within that census",
                "table_title": "the table\u2019s published title, as the ETL already records it. Titles are not interchangeable between years: 2017\u2019s table 23 is a locality table and 2023\u2019s is drinking water",
                "unit_type": "district or tehsil — the geography this series can be drawn on",
                "indicator": "the indicator as PBS labels it",
                "col_label": "the column heading the values sat under",
                "locality": "all, rural or urban",
                "sex": "all, male, female or transgender",
                "is_rate": "the series is a rate or percentage",
                "units_with_value": "census units carrying a value, whether or not they can be drawn \u2014 larger than mappable_units where a unit was split or redrawn and so has no 2023 shape",
                "mappable_units": "shapes this series colours \u2014 shapes, not units: a 2017 unit that was later split is drawn across all of its successors, so it contributes several. Shapes with both a value and a boundary — the number of shapes this series colours. Every series listed resolves to exactly one value per shape; series that do not are left out rather than drawn from an arbitrary row",
                "min_value": "smallest value in the series",
                "max_value": "largest value in the series",
            },
            "Pakistan Bureau of Statistics, Population and Housing Censuses 2017 and 2023; derived from census_panel_2017 and census_panel_2023",
            "WITH ix AS (" + " UNION ALL ".join(parts) + "), tt(ty, tid, title) AS (VALUES "
            + _title_values + ") "
            + """SELECT ix.* EXCLUDE (table_id), ix.table_id, tt.title AS table_title
                 FROM ix LEFT JOIN tt ON tt.ty = ix.census_year AND tt.tid = ix.table_id
                 ORDER BY census_year, table_id, unit_type, indicator, col_label, locality, sex""",
            unit="counts of units; the values themselves live in the panels",
        )

    # ── geography: the frames, the crosswalks, and which key joins what ──────
    print("geography\u2026")
    XW = REPO / "etl" / "census2017"
    PL = REPO / "etl" / "places"
    CSV_OPTS = "header=true, quote='\"', escape='\"'"

    if (XW / "district_crosswalk_2017_2023.csv").exists():
        register(
            "district_crosswalk_2017_2023",
            "Every district-level relationship between Census 2017 and Census 2023, "
            "as groups that cover the same ground.",
            "A group is the smallest set of 2017 and 2023 units covering the same "
            "territory, so a merge has two 2017 units and one 2023 unit and a split "
            "has one and several. What makes this checkable rather than argued is the "
            "balances column: Census 2023 table 1 prints PBS\u2019s own restatement of the "
            "2017 population on 2023 boundaries, and for every group the 2017 units\u2019 "
            "published population equals the 2023 units\u2019 restated figure. All 127 "
            "groups balance. A group that did not would mean the relation asserted for "
            "it is wrong.",
            {"relation": "exact, renamed, merged, split, or boundary transfer",
             "units_2017": "the 2017 district(s) in this group, joined by +",
             "units_2023": "the 2023 district(s) in this group, joined by +",
             "province_area": "province or area, as the census names it",
             "population_2017": "the 2017 units\u2019 published 2017 population",
             "restated_2017": "the 2023 units\u2019 POPULATION 2017, restated by PBS",
             "population_2023": "the 2023 units\u2019 2023 population",
             "balances": "yes where the two 2017 figures agree to the person"},
            "Pakistan Bureau of Statistics, Population and Housing Censuses 2017 and 2023; derived from census_panel_2017 and census_panel_2023; checked against "
            "PBS\u2019s own restatement",
            f"SELECT * FROM read_csv('{(XW / 'district_crosswalk_2017_2023.csv').as_posix()}', {CSV_OPTS})",
            unit="districts and people",
        )
        register(
            "subdistrict_crosswalk_2017_2023",
            "The same thing one tier down: 510 groups over the 537 sub-districts of "
            "2017 and the 591 of 2023.",
            "Pairs are matched by name within a district group, or \u2014 where the names "
            "give nothing \u2014 by a population that is identical and uniquely so within "
            "the group; matched_by records which. Dera Bugti\u2019s Phelawagh Tehsil and "
            "Qadirabad Sub-Division are one place under two names and only the 28,054 "
            "says so. Where several 2017 units became several 2023 ones, the group is "
            "marked restructured and the correspondence inside it is left open rather "
            "than guessed: the group balances, but which unit became which does not "
            "follow from that. The tier word is not a hierarchy \u2014 a unit published "
            "as a tehsil in 2017 is often a sub-division in 2023 and the same place.",
            {"district_group": "the district group this sits inside",
             "relation": "exact, renamed, or restructured",
             "matched_by": "name, population, or blank for a restructured group",
             "district_2017": "the district the 2017 unit belongs to",
             "district_2023": "the district the 2023 unit belongs to",
             "units_2017": "the 2017 unit(s), joined by +",
             "units_2023": "the 2023 unit(s), joined by +",
             "population_2017": "published 2017 population",
             "restated_2017": "PBS\u2019s POPULATION 2017 for the 2023 units",
             "balances": "yes where the two agree"},
            "Pakistan Bureau of Statistics, Population and Housing Censuses 2017 and 2023; derived from census_panel_2017 and census_panel_2023",
            f"SELECT * FROM read_csv('{(XW / 'subdistrict_crosswalk_2017_2023.csv').as_posix()}', {CSV_OPTS})",
            unit="sub-districts and people",
        )
    if (XW / "census_unit_map.csv").exists():
        register(
            "census_unit_map",
            "One row per census unit saying which PBS 2023 shape it is drawn on, and "
            "\u2014 where that is not a straight match \u2014 why.",
            "This is the crosswalk turned into a decision. A crosswalk says what "
            "happened to a unit; this says what a figure for it means when drawn on "
            "the 2023 frame. comparable is the field to read: yes where the figure "
            "belongs on the shape as published; combined where two 2017 units share "
            "one 2023 shape and must be added, or averaged on weight for a rate; "
            "parent where a unit was split and its figure is drawn across every "
            "successor at once rather than divided between them; flagged where the "
            "district persists but its territory changed; no where nothing can "
            "honestly be drawn. map_key can therefore name several shapes, "
            "space-separated. Anything that totals or ranks must count units and not "
            "shapes: a parent drawn across its successors is visited once per shape, "
            "which turns a national total of 207,684,626 into 213.4 million.",
            {"census_year": "2017 or 2023",
             "unit_type": "district, or tehsil for everything below it",
             "district": "the district the unit sits in",
             "unit": "the unit, as the census names it",
             "map_key": "the PBS 2023 shape(s) it is drawn on, space-separated",
             "relation": "how it relates to the 2023 frame",
             "comparable": "yes, combined, parent, flagged, or no",
             "note": "the reason, in words written for a reader",
             "weight": "2017 population, for averaging a rate across combined units"},
            "Pakistan Bureau of Statistics, Population and Housing Censuses 2017 and 2023 and the Digital Census 2023 layer; derived from the 2017\u21922023 crosswalks",
            f"SELECT * FROM read_csv('{(XW / 'census_unit_map.csv').as_posix()}', {CSV_OPTS})",
            unit="census units",
        )

    # ── the State: courts, policing, energy, disasters ──────────────────────
    # These were built long ago and never published. They live in the desktop
    # warehouse under their own folders, which is why a search of app/data found
    # nothing and the State page said "not collected" about four themes that
    # were in fact collected.
    print("courts, policing, energy and disasters\u2026")

    def src_table(name, rel, desc, notes, cols, source, unit):
        f = src / rel
        if not f.exists():
            print(f"  {name:<28} skipped (no {rel})")
            return
        # Provenance columns record where a file sat when it was parsed. That
        # is an absolute path on the machine that ran the parse, which says
        # nothing to a reader and publishes a home directory. Keep the part
        # below the project folder.
        text = [c[0] for c in con.sql(f"DESCRIBE SELECT * FROM '{f.as_posix()}'").fetchall()
                if c[1] == "VARCHAR"]
        leaky = [c for c in text if con.sql(
            f"SELECT count(*) FROM '{f.as_posix()}' WHERE \"{c}\" LIKE '%/Users/%'").fetchone()[0]]
        scrub = ("" if not leaky else " REPLACE (" + ", ".join(
            f"regexp_replace(\"{c}\", '{LOCAL_PATH}', '', 'g') AS \"{c}\"" for c in leaky) + ")")
        register(name, desc, notes, cols, source,
                 f"SELECT *{scrub} FROM '{f.as_posix()}'", unit=unit)

    src_table(
        "ljcp_case_flows", "ljcp/annual_provinces.parquet",
        "Cases pending, instituted and disposed by province, court tier and "
        "category, 2020 to 2024.",
        "The stock and the flow together: pending at the start, what came in, "
        "what was decided, and what was left. clearance_rate_pct is disposals "
        "over institutions. Above 100 means more cases were decided than "
        "filed that year; it does NOT establish that the backlog fell, "
        "which depends on the opening stock and is answered by backlog_change. "
        "stock_flow_check records whether opening plus instituted minus disposed "
        "actually equals the closing figure PBS prints; where it does not, "
        "transfers between courts usually explain it, and the residual columns "
        "say by how much. Categories nest: \u2018all\u2019 contains civil and criminal, so "
        "do not add the three together.",
        {"year": "calendar year", "province": "province or area",
         "category": "all, civil or criminal \u2014 these nest",
         "court_tier": "which courts are counted",
         "pending_start": "cases pending at the start of the year",
         "instituted": "cases filed during the year",
         "disposed": "cases decided during the year",
         "pending_end": "cases pending at the end",
         "clearance_rate_pct": "disposals as a percentage of institutions",
         "backlog_change": "pending at the end minus pending at the start",
         "stock_flow_check": "whether the stock and flow figures reconcile"},
        "Law & Justice Commission of Pakistan, annual judicial statistics",
        "cases")

    src_table(
        "ljcp_judicial_strength", "ljcp/judicial_strength_by_rank.parquet",
        "Sanctioned, working and vacant judicial posts by rank and session "
        "division.",
        "Balochistan only, for 2023 and 2024 \u2014 the other provinces\u2019 strength "
        "tables have not been extracted, so this is not a national picture and "
        "should not be read as one. Within Balochistan it is complete: 337 posts "
        "sanctioned in 2024 against 235 working.",
        {"year": "calendar year", "province": "province",
         "session_division": "the session division",
         "sanctioned_judges": "posts on the establishment",
         "working_judges": "posts filled", "vacant_judges": "posts unfilled",
         "court_tier": "which courts", "rank_coverage": "which ranks are counted"},
        "Law & Justice Commission of Pakistan", "judicial posts")

    # District courts and consolidated staffing, all provinces. Selected
    # columns only: the list-valued provenance fields stay in the desktop
    # warehouse, and the lists that explain a unit are joined into text.
    f = src / "ljcp/district_policy_indicators.parquet"
    if f.exists():
        register(
            "ljcp_court_districts",
            "Cases pending in each district's courts at year end, with the "
            "population and the working judges beside them, 2020 to 2024.",
            "One all-cases row per year and map district. Rates use the 2023 "
            "census count for every year, not a population estimate for the "
            "year. Sessions divisions are keyed to districts by name, "
            "provisionally; two-seat Balochistan districts and Islamabad are "
            "summed first, and a district with no court seat of its own is "
            "added to its host's population (hosted_districts). Working judges "
            "are matched in 384 of 585 rows: none in 2021, none for KP in 2024, "
            "and Punjab 2022 is withheld because its staffing is dated 2021. A "
            "low rate in a district with few courts can mean cases are not "
            "filed there, not that they are decided quickly; the staffing "
            "comparison is descriptive, not causal.",
            {"year": "calendar year", "province": "province or area",
             "district": "map district (ADM2 key)",
             "sessions_included": "the sessions divisions summed into it",
             "hosted_districts": "districts without a seat, counted in its population",
             "pending_start": "cases pending at the start of the year",
             "instituted": "cases filed", "disposed": "cases decided",
             "pending_end": "cases pending at the end of the year",
             "clearance_rate_pct": "disposals as a percentage of institutions",
             "population_2023": "Census 2023 population of the district and any hosted",
             "pending_per_100k": "pending_end per 100,000 people, 2023 census",
             "pending_per_100k_interpolated": "the same on a population grown between censuses",
             "working_judges": "judges in post, where matched",
             "sanctioned_judges": "posts sanctioned, where matched",
             "pending_per_working_judge": "pending_end / working_judges",
             "crosswalk_status": "how the sessions division was keyed",
             "judge_coverage_status": "whether staffing was matched",
             "stock_flow_check": "whether the stock and flow figures reconcile"},
            "Law & Justice Commission of Pakistan, annual judicial statistics",
            f"""SELECT year, province, adm2_key AS district,
                       array_to_string(sessions_included, '; ') AS sessions_included,
                       array_to_string(hosted_districts, '; ') AS hosted_districts,
                       pending_start, instituted, disposed, pending_end,
                       round(clearance_rate_pct, 2) AS clearance_rate_pct,
                       population AS population_2023,
                       round(pending_per_100k_population, 2) AS pending_per_100k,
                       round(pending_per_100k_interpolated, 2) AS pending_per_100k_interpolated,
                       working_judges, sanctioned_judges,
                       round(pending_per_working_judge, 2) AS pending_per_working_judge,
                       crosswalk_status, judge_coverage_status, stock_flow_check
                FROM '{f.as_posix()}' ORDER BY year, province, district""",
            unit="district-years")

    f = src / "ljcp/judicial_strength.parquet"
    if f.exists():
        register(
            "ljcp_judges_province",
            "Judicial posts sanctioned, filled and vacant in the district "
            "judiciary, by province and session division, 2020 to 2024.",
            "Consolidated strength, all ranks together, as each report prints "
            "it. Not every year is there: no 2021 edition table, KP 2024 not "
            "found, and Islamabad prints working judges only. Punjab's 2022 "
            "edition is dated 31 December 2021 (strength_date_status). "
            "Balochistan's figures sum four rank tables and, from 2022, exclude "
            "ex-cadre posts, so its 2020 total is not strictly comparable.",
            {"year": "report year", "as_of_date": "date the strength refers to",
             "province": "province or area", "session_division": "the session division",
             "sanctioned_judges": "posts on the establishment",
             "working_judges": "posts filled", "vacant_judges": "posts unfilled",
             "strength_date_status": "same_year, or the edition's date differs",
             "rank_coverage": "how the ranks were counted",
             "working_definition": "what 'working' includes"},
            "Law & Justice Commission of Pakistan",
            f"""SELECT year, as_of_date, province, session_division,
                       sanctioned_judges, working_judges, vacant_judges,
                       strength_date_status, rank_coverage, working_definition
                FROM '{f.as_posix()}' ORDER BY year, province, session_division""",
            unit="judicial posts")

    src_table(
        "police_crime_annual", "regional_police/crime_annual.parquet",
        "Reported offences by province, range and year, 2019 to 2024.",
        "Nine reporting regions, and the geography is not uniform between them: "
        "Khyber Pakhtunkhwa reports 38 places and Azad Jammu & Kashmir 11, while "
        "Punjab, Sindh, Balochistan, ICT, Gilgit-Baltistan and the Railways "
        "police report one figure each. So a district map of this covers KP and "
        "AJK and nothing else. measure says what is counted \u2014 mostly "
        "reported_offence_count, which is offences reported to police and not "
        "crimes committed.",
        {"source_family": "which force reported it", "region": "province or force",
         "geography": "the place, where the force reports one",
         "geography_level": "province, range, district or national",
         "year": "calendar year", "measure": "what is counted",
         "value": "the count"},
        "Police returns as published by the Khyber Pakhtunkhwa Bureau of Statistics (Development Statistics of Khyber Pakhtunkhwa), the Planning & Development Department of Azad Jammu & Kashmir (AJK Statistical Year Book), the Bureau of Statistics, Balochistan (Development Statistics of Balochistan) and the Pakistan Bureau of Statistics (crime by type; Pakistan Statistical Year Book). The source_url column names each row's publication.", "offences")

    src_table(
        "police_crime_district", "regional_police/district_crime_annual.parquet",
        "The same, at district level where a force publishes it.",
        "Only Khyber Pakhtunkhwa and Azad Jammu & Kashmir publish district "
        "figures; everywhere else the province is the finest grain available. "
        "Joining this to a district map leaves most of the country empty, which "
        "is a fact about police reporting rather than about crime.",
        {"region": "province or force", "geography": "district",
         "geography_id": "the force\u2019s own identifier",
         "year": "calendar year", "measure": "what is counted",
         "value": "the count"},
        "Police returns as published by the Khyber Pakhtunkhwa Bureau of Statistics (Development Statistics of Khyber Pakhtunkhwa) and the Planning & Development Department of Azad Jammu & Kashmir (AJK Statistical Year Book). The source_url column names each row's publication.", "offences")

    src_table(
        "sindh_crime_annual", "sindh_police/sindh_crime_annual.parquet",
        "Sindh police reported crime by category and year, 2019 to 2025.",
        "Sindh reports in more detail than the other provinces and separately "
        "from the national compilation, so it is kept as its own table rather "
        "than folded in. Every row carries the source document and how it was "
        "extracted.",
        {"reporting_year": "the year as the report labels it",
         "year": "calendar year", "source_url": "the report it came from",
         "extraction_method": "how the figure was read off the page"},
        "Sindh Police", "offences")

    src_table(
        "sindh_fir_daily", "sindh_fir/sindh_fir_observations.parquet",
        "First information reports registered in Sindh, daily and year to date, "
        "by district and range.",
        "A daily operational series rather than an annual statistical one, so it "
        "moves for reasons that are about reporting as much as about crime. "
        "ytd_firs is the running total from the start of the year, so it is not "
        "additive across dates.",
        {"report_date": "the date reported", "geography_name": "district or range",
         "police_range": "the police range", "daily_firs": "FIRs that day",
         "ytd_firs": "FIRs so far that year \u2014 a running total, do not add"},
        "Sindh Police daily FIR reports", "reports")

    src_table(
        "nepra_plants", "nepra_plants.parquet",
        "Power plants on the national grid, with fuel, technology and installed "
        "capacity.",
        "133 plants. Hydel is the largest block at 11,890 MW, then coal at 7,260, "
        "furnace oil at 5,440 and nuclear at 3,635; wind has the most plants, 37, "
        "for 1,885 MW. Installed capacity is nameplate and not what a plant "
        "actually generates \u2014 see nepra_disco_annual for what was dispatched.",
        {"plant_id": "NEPRA\u2019s own identifier", "plant_name": "the plant",
         "name_variants": "other spellings in the source documents",
         "technology": "how it generates", "fuel": "what it burns or uses",
         "installed_mw": "nameplate capacity, megawatts",
         "first_fy": "first year it appears", "last_fy": "last year it appears"},
        "NEPRA State of Industry and performance reports", "megawatts")

    # ONE ROW PER PLANT WAS NOT ENOUGH. nepra_plants collapses the reports to
    # a single row per plant with installed_mw taken as the MAXIMUM across
    # every year it appears and a first/last fiscal year around it. Anything
    # built from that is a union of eight reports, not a year: 133 plants and
    # 45,405 MW, against 118 plants and 41,440 MW actually reported for
    # 2024-25. It also cannot answer which year a capacity belongs to, and 22
    # plants are revised between years, so the single figure is right for at
    # most one of them.
    #
    # These are the observations themselves, one row per plant per fiscal
    # year, so a chart can name the year it is drawing instead of inferring
    # presence from a span.
    if (src / "nepra_plant_month.parquet").exists():
        register(
            "nepra_plant_years",
            "Every plant in NEPRA\u2019s reports, by fiscal year: what it burns "
            "and what it was rated at in that year.",
            "One row per plant per fiscal year as reported, 2017-18 to "
            "2024-25 \u2014 the observations behind nepra_plants, which collapses "
            "them to one row per plant at its maximum capacity. Use this "
            "table when the year matters: 22 plants have their capacity "
            "revised between reports, and the union of all eight years is "
            "133 plants and 45,405 MW against 118 and 41,440 MW reported for "
            "2024-25. Presence here is observed, not inferred: one plant is "
            "absent from a year inside its own first-to-last span. Rows with "
            "no installed_mw are kept rather than dropped, because a plant "
            "the report lists without a capacity is still a plant it listed: "
            "11 of the 108 in 2017-18 are like this, falling to none from "
            "2021-22. This is the reporting universe NEPRA published, not a "
            "register of every plant in the country.",
            {"plant_id": "NEPRA\u2019s own identifier",
             "plant": "the plant as that report names it",
             "fiscal_year": "the report\u2019s fiscal year",
             "technology": "how it generates", "fuel": "what it burns or uses",
             "installed_mw": "nameplate capacity as reported THAT year",
             "dependable_mw": "dependable capacity as reported that year",
             "generation_gwh": "generation that year, where reported"},
            "NEPRA State of Industry and performance reports",
            f"""SELECT plant_id, plant, fiscal_year, technology, fuel,
                       installed_mw, dependable_mw, generation_gwh
                FROM '{(src / "nepra_plant_month.parquet").as_posix()}'
                WHERE month = 'FY'
                ORDER BY fiscal_year, installed_mw DESC NULLS LAST""",
            unit="megawatts")

    src_table(
        "nepra_disco_annual", "nepra_disco_annual.parquet",
        "Distribution company performance by year, 2006\u201307 to 2024\u201325.",
        "Read straight off NEPRA\u2019s tables, which is why row_label and col_label "
        "are carried as printed rather than normalised: the tables change shape "
        "between editions and a single schema across nineteen years would have "
        "to invent correspondences. value_raw is the text as printed; filter on "
        "series, table_no and row_label to pull one measure.",
        {"series": "which NEPRA publication", "report_year": "the edition",
         "table_no": "table within it", "title": "the table\u2019s title",
         "disco": "distribution company", "fy": "fiscal year the row is about",
         "row_label": "the row as printed", "col_label": "the column as printed",
         "value_raw": "the cell as printed"},
        "NEPRA State of Industry reports", "varies by row")

    src_table(
        "climate_events", "climate_events/climate_events.parquet",
        "Flood, drought and cyclone events affecting Pakistan, 2001 to 2025.",
        "31 events: 23 floods, 4 droughts, 4 tropical cyclones. An event is a "
        "named episode with a start and end, and date_precision says how well "
        "the dates are known. These events do NOT join to climate_impacts: "
        "the impact records carry no event key, only their own report_id, so "
        "the two are separate universes and attributing an impact row to an "
        "event here would be an inference, not a lookup. An earlier note "
        "said climate_impacts carries what each one did, which invited a "
        "join that cannot be made.",
        {"record_id": "the event", "source": "who recorded it (gdacs)",
         "hazard": "flood, drought or tropical cyclone", "subtype": "finer type",
         "title": "how the source names it", "start_date": "when it began",
         "end_date": "when it ended",
         "date_precision": "how precisely the dates are known"},
        "GDACS (Global Disaster Alert and Coordination System, European Commission Joint Research Centre), Pakistan flood, drought and cyclone alerts, 2001 to 2025; CC BY 4.0", "events")

    src_table(
        "climate_impacts", "climate_events/climate_impacts.parquet",
        "Impacts reported by disaster reporting: people affected, killed, "
        "displaced, and assets damaged, by place and reporting period.",
        "336 observations, keyed by report and place, not by event. There "
        "is no event key in this table and none that joins to "
        "climate_events, so an impact cannot be attributed to a named event "
        "by lookup - only by reading the period and the place, which is an "
        "inference. The metric column says what is counted and the figures "
        "come from whichever report covered that place, so coverage is "
        "uneven between places and between periods. Do not read a missing "
        "row as a zero.",
        {"observation_id": "the observation", "report_id": "the report it came from",
         "location_name": "the place as the report names it",
         "admin_level": "how fine the place is", "metric": "what is counted",
         "value": "the figure", "period_start": "start of the period covered",
         "period_end": "end of the period covered"},
        "NDMA (National Disaster Management Authority) monsoon situation reports, September 2026, tables read off the PDFs; each row carries its report and page", "people, assets")

    # ── the national series: Economy and State ──────────────────────────────
    print("national accounts, trade and tax\u2026")
    PULL = REPO.parent / "raw_data" / "pbs_insight_explorer"
    NA = PULL / "national_accounts_2026-09-27"
    TR = PULL / "trade_2026-09-27"
    CSV = "header=true, quote='\"', escape='\"'"

    def csv_table(name, path, desc, notes, cols, source, unit):
        if not path.exists():
            print(f"  {name:<28} skipped (no {path.name})")
            return
        register(name, desc, notes, cols, source,
                 f"SELECT * FROM read_csv('{path.as_posix()}', {CSV})", unit=unit)

    csv_table(
        "gdp_growth", NA / "gdp_growth_1952_2025.csv",
        "Real GDP growth and its sectoral components, 1951\u201352 to 2024\u201325.",
        "Seventy-four fiscal years, the longest series on the site. "
        "government_label names the government of the day, which PBS\u2019s own "
        "dashboard carries and which is useful for reading the series but is a "
        "political attribution rather than a statistical one \u2014 a growth rate in a "
        "government\u2019s first months reflects decisions taken before it.",
        {"fy": "fiscal year, e.g. 2024-25", "fy_end": "calendar year it ends in",
         "gdp_growth_pct": "real GDP growth, per cent",
         "agriculture_growth_pct": "agriculture, per cent",
         "industry_growth_pct": "industry, per cent",
         "services_growth_pct": "services, per cent",
         "commodity_producing_sector_growth_pct": "agriculture and industry together",
         "government_label": "the government in office that year, as PBS labels it"},
        "PBS national accounts (na.data.gov.pk), pull of 2026-09-27", "per cent")

    csv_table(
        "gdp_indicators", NA / "gdp_indicators_annual_2000_2026.csv",
        "GDP, national product, per-capita income and the exchange rate, "
        "1999\u20132000 to 2025\u201326.",
        "Constant prices on PBS\u2019s own base. status marks a year PBS flags as "
        "provisional or revised; the latest years are usually provisional and "
        "move.",
        {"fy": "fiscal year", "fy_end": "calendar year it ends in",
         "gdp_constant_pkr_mn": "GDP at constant prices, million rupees",
         "npi_constant_pkr_mn": "net national product, million rupees",
         "per_capita_income_constant_rs": "per-capita income, rupees",
         "exchange_rate_pkr_per_usd": "rupees per US dollar",
         "status": "PBS\u2019s own flag: provisional, revised or final"},
        "PBS national accounts, pull of 2026-09-27", "million rupees, rupees")

    csv_table(
        "gva_by_activity_annual", NA / "gdp_by_activity_annual_constant_2000_2026.csv",
        "Gross value added by sector and sub-sector, annual, at constant prices.",
        "One row per leaf activity, 22 a year, with sector and subsector as "
        "the path to it rather than rows of their own. Summing the rows is "
        "therefore correct and reproduces the published total: it equals "
        "Table 5\u2019s GVA at basic prices to the rupee in 26 of the 27 years, "
        "the exception being 2023-24, where the activity file and the annual "
        "table are different vintages and differ by Rs15.9bn. An earlier note "
        "here said the rows were three nested levels and that adding them "
        "double-counts, which is not what the file contains.",
        {"fy": "fiscal year", "fy_end": "calendar year it ends in",
         "sector": "the broad sector", "subsector": "within the sector",
         "category": "within the sub-sector",
         "gva_constant_pkr_mn": "gross value added, million rupees, constant prices",
         "status": "PBS\u2019s own flag"},
        "PBS national accounts, pull of 2026-09-27", "million rupees")

    csv_table(
        "gva_by_activity_quarterly", NA / "gdp_by_activity_quarterly_constant_2016_2025.csv",
        "The same, quarterly, from 2015\u201316.",
        "Quarterly national accounts are newer and thinner than the annual "
        "series and are revised more. The grain is the same as the annual "
        "file: one row per leaf activity, with sector and subsector as the "
        "path, so the rows of one quarter add up rather than double-count.",
        {"fy": "fiscal year", "fy_end": "calendar year it ends in",
         "quarter": "Q1 is July\u2013September", "sector": "the broad sector",
         "subsector": "within the sector", "category": "within the sub-sector",
         "gva_constant_pkr_mn": "gross value added, million rupees, constant prices",
         "status": "PBS\u2019s own flag"},
        "PBS national accounts, pull of 2026-09-27", "million rupees")

    csv_table(
        "fbr_tax_collection", NA / "fbr_tax_collection_by_head_1992_2024.csv",
        "Federal tax collection by head and sub-head, 1991\u201392 to 2023\u201324.",
        "Direct and indirect tax by the heads FBR reports. tax_type, head and "
        "subhead nest, so filter to one level before summing.",
        {"fy": "fiscal year", "fy_end": "calendar year it ends in",
         "tax_type": "direct or indirect", "head": "the tax head",
         "subhead": "within the head",
         "collection_pkr_mn": "collection, million rupees",
         "status": "PBS\u2019s own flag"},
        "FBR via PBS national accounts, pull of 2026-09-27", "million rupees")

    csv_table(
        "trade_by_country", TR / "trade_by_country_fy_period.csv",
        "Imports and exports by trading partner, fiscal year and period, "
        "2003\u201304 onwards.",
        "231 partners. Read the period column before using a total: "
        "FY_from_quarters is the four quarters added, and is what to use for a "
        "year \u2014 the portal\u2019s own whole-year rows are excluded because they "
        "overstate badly, 38.3 against a published 32.1 US dollars billion of "
        "exports for 2024-25. The quarterly rows still run a little over the "
        "totals endpoint, about 3 per cent on Q4 and 2 per cent on Q2; "
        "trade_reconciliation puts every period beside the totals so the gap can "
        "be seen. The dollar columns are zero before 2013-14 on every endpoint. "
        "This is aggregate trade and does not replace trade_hs8, which has the "
        "8-digit product detail.",
        {"fy": "fiscal year", "period": "Q1\u2013Q4, M01\u2013M12, or FY_from_quarters",
         "country": "partner as the portal names it", "iso2": "ISO 3166 alpha-2",
         "iso3": "ISO 3166 alpha-3", "continent": "continent",
         "pbs_country_code": "PBS\u2019s own code",
         "imports_pkr": "imports, rupees", "exports_pkr": "exports, rupees",
         "imports_usd": "imports, US dollars", "exports_usd": "exports, US dollars"},
        "PBS National Trade Database (trade.data.gov.pk), pull of 2026-09-27",
        "rupees and US dollars")

    csv_table(
        "trade_by_group", TR / "trade_by_commodity_group_fy_period.csv",
        "Imports and exports by commodity group, fiscal year and period.",
        "Ten groups, numbered 0 to 9. The portal does not label them; by their "
        "values they are the one-digit SITC sections, but PBS does not say so and "
        "they are carried as numbers rather than given names they may not have. "
        "The period caution for trade_by_country applies here too.",
        {"fy": "fiscal year", "period": "Q1\u2013Q4, M01\u2013M12, or FY_from_quarters",
         "group": "commodity group 0\u20139, unlabelled by the portal",
         "imports_pkr": "imports, rupees", "exports_pkr": "exports, rupees",
         "imports_usd": "imports, US dollars", "exports_usd": "exports, US dollars"},
        "PBS National Trade Database, pull of 2026-09-27", "rupees and US dollars")

    csv_table(
        "trade_monthly_totals", TR / "trade_monthly_totals_2003_2026.csv",
        "Total imports and exports by calendar month, 2003\u201304 onwards.",
        "The totals endpoint, which the country and group tables are reconciled "
        "against. The portal\u2019s month list has no June, so June appears only "
        "inside Q4.",
        {"fy": "fiscal year", "year": "calendar year", "month": "calendar month",
         "imports_pkr": "imports, rupees", "exports_pkr": "exports, rupees",
         "imports_usd": "imports, US dollars", "exports_usd": "exports, US dollars"},
        "PBS National Trade Database, pull of 2026-09-27", "rupees and US dollars")

    csv_table(
        "trade_reconciliation", TR / "trade_reconciliation_fy_period.csv",
        "Each period\u2019s totals beside the sum of its country rows and its group "
        "rows.",
        "Read this before quoting a trade total. Monthly country and group rows "
        "match the totals endpoint to within 0.01 US dollars billion in every "
        "month except April 2021. Quarterly rows overshoot on exports, most often "
        "in Q4 \u2014 3.3 per cent on average \u2014 so a year built from quarters is "
        "close but not exact.",
        {"fy": "fiscal year", "period": "the period compared",
         "totals_imports_pkr": "from the totals endpoint",
         "totals_exports_pkr": "from the totals endpoint",
         "countries_imports_pkr": "the country rows added",
         "countries_exports_pkr": "the country rows added",
         "groups_imports_pkr": "the group rows added",
         "groups_exports_pkr": "the group rows added"},
        "PBS National Trade Database, pull of 2026-09-27", "rupees and US dollars")

    # ── the diaspora country files ──────────────────────────────────────────
    DIA_NOTE = ("The portal serves this keyed by country and it is not country "
                "data: 198 country keys return 21 distinct series and not one "
                "country has a series of its own, with 141 of them returning the "
                "same one. The national series is taken instead and the country "
                "dimension dropped, because publishing one number against 141 "
                "countries would present it as variation.")
    for name, desc, notes, cols, unit in [
        ("diaspora_remittances_monthly",
         "Remittances to Pakistan by month, 2018 to 2025.",
         DIA_NOTE + " The value is as the portal gives it, which its magnitude "
         "puts in millions of US dollars \u2014 34,662 for 2024 against roughly 30 "
         "billion published \u2014 but the portal does not label the unit, so treat "
         "the level with care and the shape as sound.",
         {"year": "calendar year", "month": "calendar month",
          "value": "remittances, unit as the portal gives it"},
         "unlabelled by the source; magnitude suggests million US dollars"),
        ("diaspora_emigrants_by_skill",
         "Registered emigrants by skill level and year.",
         DIA_NOTE + " Skill levels are the Bureau of Emigration\u2019s own bands. "
         "Total is the sum of the others, so do not add it to them.",
         {"year": "calendar year", "mode": "the portal\u2019s own breakdown mode",
          "skill_level": "highly qualified, highly skilled, skilled, "
                         "semi-skilled, unskilled, or total",
          "emigrants": "people"},
         "people"),
        ("diaspora_destinations",
         "Registered emigrants by destination country and year, 2011 to 2024.",
         "This one is genuinely by country, unlike the remittance and skill files "
         "from the same portal: 199 countries in 2024 with 102 distinct values. "
         "The totals match the Bureau of Emigration\u2019s published figures \u2014 "
         "859,740 in 2023 and 725,587 in 2024. Saudi Arabia takes about six in "
         "ten.",
         {"year": "calendar year", "country": "destination as the portal names it",
          "iso2": "ISO 3166 alpha-2", "iso3": "ISO 3166 alpha-3",
          "continent": "continent code", "emigrants": "people"},
         "people"),
        ("diaspora_occupations",
         "Registered emigrants by occupation, 2024.",
         "Forty occupation categories for one year. Labourer is the largest at "
         "364,574, about half of all emigration that year.",
         {"year": "calendar year", "occupation": "category as the portal names it",
          "emigrants": "people"},
         "people"),
    ]:
        f = OUT / f"{name}.parquet"
        if f.exists():
            register(name, desc, notes, cols,
                     "Bureau of Emigration & Overseas Employment via PBS\u2019s "
                     "diaspora portal, pull of 2026-09-27",
                     f"SELECT * FROM '{f.as_posix()}'", unit=unit)

    f = OUT / "census_entities.parquet"
    if f.exists():
        register(
            "census_entities",
            "Census 2023\u2019s count of enumerated structures \u2014 24 kinds, from schools "
            "and hospitals to factories, mosques and police stations \u2014 by district "
            "and tehsil.",
            "The only table here that covers the whole frame Data Darbar draws: 156 "
            "districts and 649 tehsils, every district of Azad Jammu & Kashmir and "
            "Gilgit-Baltistan included. Neither census panel has a row for either, so "
            "for those 58 tehsils these are the only values on the site. Two "
            "cautions. A missing row is not a zero \u2014 a district with no jail has no "
            "jail row \u2014 so coverage varies by kind: hostels, hotels and hospitals "
            "reach all 156 districts, universities 74, orphanages 66. And \u2018Home\u2019 is "
            "returned for 9 districts of 156; the portal\u2019s own documentation says to "
            "treat it as unavailable, so it is here but deliberately not offered as "
            "a map indicator. The district and tehsil aggregations agree exactly for "
            "all 24 kinds.",
            {"level": "district or tehsil",
             "area_code": "PBS\u2019s own district or tehsil code",
             "map_key": "the shape this is drawn on \u2014 district code, or dds_id "
                        "below that, or PBS-<code> for a tehsil outside the census",
             "unit_id": "PBS\u2019s own id for the kind of structure, 1\u201324",
             "unit_type": "what the structure is",
             "area": "the area\u2019s name as the portal gives it",
             "count": "structures enumerated"},
            "PBS Digital Census 2023 via economic.data.gov.pk, pull of 2026-09-27",
            f"SELECT * FROM '{f.as_posix()}'",
            unit="structures",
        )

    f = OUT / "crops_district_fy.parquet"
    if f.exists():
        register(
            "crops_district_fy",
            "Area, production and yield by crop, district and fiscal year, "
            "1981\u201382 to 2024\u201325.",
            "108 crops over 123 districts and 44 fiscal years. All 123 district "
            "codes join the PBS 2023 layer exactly, but the frame is older than "
            "that layer: 33 of the 156 districts Data Darbar draws have no crop "
            "rows at all \u2014 every district of Azad Jammu & Kashmir and "
            "Gilgit-Baltistan, which have their own agricultural authorities, six "
            "of Karachi\u2019s seven, and the most recently created districts "
            "(Chaman, Duki, Surab, Sohbatpur, Upper Chitral, Upper and Lower "
            "Kohistan, Keamari). Karachi appears once, under Karachi Central\u2019s "
            "code, and is vestigial rather than city-wide: six crops and nothing "
            "at all in recent years. Two crops are unlabelled on PBS\u2019s portal "
            "and are carried as \u2018Unnamed crop (portal id 124/125)\u2019 rather than "
            "dropped \u2014 125 is not small, at 85,000 hectares in 2024\u201325. Coverage "
            "varies by crop: the majors run from 1981\u201382, most vegetables and "
            "fruit from 2008\u201309.",
            {"district_code": "PBS 2023 district code, joining to geography_keys",
             "district": "district name as PBS\u2019s 2023 layer gives it",
             "province": "province or area",
             "crop_id": "PBS\u2019s own crop id",
             "crop": "crop name, or a placeholder where the portal gives none",
             "fy": "fiscal year, e.g. 2024-25",
             "area_000ha": "thousand hectares sown",
             "production_000t": "thousand tonnes produced",
             "yield_t_per_ha": "tonnes per hectare; production divided by area"},
            "PBS Agriculture Statistics via the national accounts portal, pull of "
            "2026-09-27",
            f"SELECT * FROM '{f.as_posix()}'",
            unit="thousand hectares, thousand tonnes, tonnes per hectare",
        )

    f = OUT / "diaspora_emigrants_district.parquet"
    if f.exists():
        register(
            "diaspora_emigrants_district",
            "Registered emigrants by district of origin, 2011\u20132024, from the Bureau "
            "of Emigration & Overseas Employment.",
            "Two things to know before using it. The year 'overall' is NOT the sum of "
            "2011\u20132024: it is 9,858,937 against 8,663,198, because the Bureau\u2019s "
            "register goes back well before 2011. is_cumulative marks it, and it is a "
            "separate indicator in place_indicators for the same reason. And the frame "
            "is partial \u2014 the file keys on PBS district codes and all 146 join "
            "cleanly, but eleven PBS districts have no row of their own: Chaman, Duki, "
            "Surab, Kharmang, Nagar, Shigar, Upper and Lower Kohistan, Upper Chitral "
            "and Keamari. The register has followed some recent splits and not others "
            "\u2014 Kolai Palas Kohistan and Lower Chitral appear, their siblings do not "
            "\u2014 so those eleven sit inside a parent\u2019s figure rather than being absent, "
            "and the parent is flagged in place_indicators.",
            {"district_code": "PBS 2023 district code, joining to geography_keys",
             "district": "district name as PBS\u2019s 2023 layer gives it",
             "division": "division, as the register gives it",
             "province": "province or area",
             "year": "2011\u20132024, or 'overall' for the whole register",
             "emigrants_registered": "people registered as emigrating in that period",
             "is_cumulative": "TRUE on the 'overall' row \u2014 do not add it to the years"},
            "Bureau of Emigration & Overseas Employment, read through PBS\u2019s diaspora "
            "portal, pull of 2026-09-27",
            f"SELECT * FROM '{f.as_posix()}'",
            unit="people",
        )

    f = OUT / "geography_keys.parquet"
    if f.exists():
        register(
            "geography_keys",
            "Which identifier names a Pakistani place, and which of them can safely "
            "be joined on.",
            "Six identifiers name places in this warehouse and they are not "
            "interchangeable; unique_per_place is the column that decides whether a "
            "join is safe. dd_id is the trap: 591 census sub-districts share 471 of "
            "them, so joining on it and keeping one row per polygon returns a "
            "plausible answer with units missing. Every count here is measured from "
            "the boundary files rather than asserted.",
            {"key": "the column name as it appears in the tables",
             "level": "district or sub-district",
             "what": "what the identifier is",
             "distinct_values": "how many distinct values exist, measured",
             "unique_per_place": "FALSE means one value can name several places \u2014 "
                                 "do not join on it alone",
             "frame": "the boundary set it belongs to",
             "notes": "what to do about it"},
            "Measured from the PBS Digital Census 2023 layers and the warehouse tables",
            f"SELECT * FROM '{f.as_posix()}'",
            unit="identifiers",
        )

    # Who owns the data behind the Places tables. They mix sources, so each
    # is named here and each indicator row carries its own in `dataset`.
    PLACES_SOURCE = ("Pakistan Bureau of Statistics (Population Censuses 2017 and 2023, Mouza Census 2020, Economic Census 2023, PSLM, HIES, LFS, agriculture statistics); MICS district rounds (Punjab 2017-18, Sindh 2018-19, Khyber Pakhtunkhwa 2019, Balochistan 2019-20, Gilgit-Baltistan 2016-17, Azad Jammu & Kashmir 2020-21; the provincial and regional bureaus of statistics and planning departments, with UNICEF); PDHS 2017-18 (NIPS and ICF); Bureau of Emigration & Overseas Employment; Malaria Atlas Project; WorldPop; Meta Data for Good; the Earth Observation Group, Colorado School of Mines (VIIRS night-lights); Adaad school layer. The dataset column names each indicator's own source.")

    PLACE_COLS = {
        "place_indicators": {
            "level": "district or tehsil",
            "map_key": "the PBS 2023 shape(s) this row is drawn on, space-separated "
                       "where a unit was later split",
            "source_key": "the identifier the source table used \u2014 a district slug, a "
                          "dd_id, or a PBS tehsil code",
            "relation": "how the unit relates to the 2023 frame: exact, alias, split, "
                        "or shared",
            "note": "the reason, in words, for anything other than a straight match",
            "group_key": "the indicator group, joining to place_indicator_index",
            "indicator": "the indicator id within that group",
            "year": "the year, where the series has one; NULL where it does not",
            "value": "the figure \u2014 unit depends on the indicator, see the index",
        },
        "place_indicator_index": {
            "level": "district or tehsil \u2014 the geography this indicator draws on",
            "topic": "the topic it browses under",
            "topic_label": "that topic, as a reader sees it",
            "group_key": "the group, joining to place_indicators.group_key",
            "group_label": "that group, as a reader sees it",
            "dataset": "the source dataset and release",
            "indicator": "the indicator id, joining to place_indicators.indicator",
            "label": "the indicator, as a reader sees it",
            "dp": "decimal places to show; NULL for a count",
            "source": "where the values are: place_indicators, or a census panel",
            "years": "how many years the series has",
            "shapes": "PBS 2023 shapes this indicator colours",
            "units": "census units carrying a value \u2014 differs from shapes wherever a "
                     "unit is drawn across the shapes that replaced it",
            "min_value": "smallest value",
            "max_value": "largest value",
        },
    }
    for name, desc, notes, src in [
        ("place_indicators",
         "Every curated district and tehsil indicator, on PBS\u2019s 2023 frame.",
         "Values only \u2014 the words belong to place_indicator_index, because "
         "repeating a label 90,000 times is most of what makes a payload large. "
         "map_key names the PBS 2023 shape(s) a row is drawn on and follows the same "
         "convention as census_unit_map: a unit that was later split names all of its "
         "successors, so count units and not shapes when totalling. 73 of the fields "
         "here are provenance rather than indicators \u2014 dhs_coverage, "
         "hies_inherited_from, the *_n_obs and *_low_n flags \u2014 which is why they have "
         "no index entry.",
         "place_indicators"),
        ("place_indicator_index",
         "One row per indicator Places can draw, curated and census alike.",
         "The picker reads this and nothing else: 296 curated indicators beside "
         "37,971 census series, with the words a reader sees and the number of shapes "
         "behind each. source says where the values are \u2014 place_indicators, or a "
         "census panel \u2014 which is what lets one list cover both without copying 17 MB "
         "of census into a table it does not fit. shapes counts shapes and units "
         "counts census units; they differ wherever a unit is drawn across its "
         "successors.",
         "place_indicator_index"),
    ]:
        f = OUT / f"{src}.parquet"
        if f.exists():
            register(name, desc, notes, PLACE_COLS.get(name, {}),
                     PLACES_SOURCE,
                     f"SELECT * FROM '{f.as_posix()}'",
                     unit="indicator values" if src == "place_indicators" else "indicators")

    if p17 or p23:
        EXAMPLES.extend(CENSUS_PANEL_EXAMPLES)
    else:
        print("census panels…  skipped (no panel release found under --src)")

    # ── catalogue metadata: shelf, period covered, place keys, sample rows ──
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from catalog_meta import (KIND, KINDS, TIME_COLS, KEY_COLS, LICENCE, licence_of,
                              PROVENANCE, provenance_of)
    unfiled = [t["name"] for t in tables if t["name"] not in KIND]
    if unfiled:
        raise SystemExit(f"catalog_meta.KIND does not file: {', '.join(unfiled)}")
    # A table without stated terms would fall back to the site-wide CC BY,
    # which may be freer than its source allows - so it fails instead.
    unlicensed = [t["name"] for t in tables if t["name"] not in LICENCE]
    if unlicensed:
        raise SystemExit(f"catalog_meta.LICENCE does not cover: {', '.join(unlicensed)}")
    # Nor without saying whether its figures are the publisher's or ours.
    unsourced = [t["name"] for t in tables if t["name"] not in PROVENANCE]
    if unsourced:
        raise SystemExit(f"catalog_meta.PROVENANCE does not cover: {', '.join(unsourced)}")
    samples = {}
    for t in tables:
        path = (OUT / t["file"]).as_posix()
        cols = [c["name"] for c in t["columns"]]
        t["kind"] = KIND[t["name"]]
        t["licence"] = licence_of(t["name"])
        t["provenance"] = provenance_of(t["name"], cols)
        for c in t["columns"]:
            if c["name"] in t["provenance"]["derived_columns"]:
                c["derived"] = True
        tc = next((c for c in TIME_COLS if c in cols), None)
        if tc:
            lo, hi = con.sql(f'SELECT min("{tc}")::VARCHAR, max("{tc}")::VARCHAR '
                             f"FROM '{path}' WHERE \"{tc}\"::VARCHAR NOT IN ('overall', '-1') "
                             f"AND \"{tc}\"::VARCHAR NOT LIKE 'Δ%'").fetchone()
            t["span"] = {"column": tc, "from": lo, "to": hi}
        t["keys"] = [k for k in KEY_COLS if k in cols]
        # Five rows, as text, so a reader sees the shape before downloading.
        rows = con.sql(f"SELECT * FROM '{path}' LIMIT 5").fetchall()
        samples[t["name"]] = [[None if v is None else str(v)[:80] for v in r] for r in rows]
    (OUT / "catalog_samples.json").write_text(
        json.dumps(samples, ensure_ascii=False, separators=(",", ":")))

    # ── catalog ──────────────────────────────────────────────────────────────
    catalog = {
        "name": "Data Darbar",
        "version": 1,
        "generated": _today(),
        "license": "Data Darbar\u2019s derived data CC BY 4.0 unless a table states other "
                   "terms (each table\u2019s licence field) · code MIT",
        "kinds": [{"key": k, "label": l, "about": a} for k, l, a in KINDS],
        "tables": sorted(tables, key=lambda t: t["name"]),
        "examples": EXAMPLES,
    }
    # Written as the committed file is: characters as themselves, not escaped,
    # so a rebuild's diff shows what changed rather than every dash and arrow.
    (OUT / "catalog.json").write_text(json.dumps(catalog, indent=1, ensure_ascii=False))
    total = sum(t["bytes"] for t in tables)
    eager = sum(t["bytes"] for t in tables if t["bytes"] < 2_000_000)
    print(f"\ncatalog.json written · {len(tables)} tables · {total/1e6:.1f} MB total "
          f"({eager/1e6:.1f} MB loaded eagerly)")


EXAMPLES = [
    # Ten, deliberately: one or two per table family, each answering a question a
    # reader would actually ask, and each showing one habit the table needs
    # (filter low_n, country IS NULL, is_own_year_be, kind='indicator').
    {"title": "Ten poorest districts",
     "sql": ("SELECT rank, name, prov, mpi, H, A\n"
             "FROM mpi_districts\n"
             "WHERE low_n = 0   -- drop districts whose sample is too small to rank\n"
             "ORDER BY rank LIMIT 10;")},
    {"title": "Female literacy, 2017 vs 2023",
     "sql": ("SELECT district, province,\n"
             "       max(value) FILTER (year = '2017') AS lit_2017,\n"
             "       max(value) FILTER (year = '2023') AS lit_2023,\n"
             "       round(max(value) FILTER (year = '2023')\n"
             "           - max(value) FILTER (year = '2017'), 1) AS change\n"
             "FROM district_indicators\n"
             "WHERE indicator = 'literacy_ratio_female'\n"
             "GROUP BY 1, 2\n"
             "ORDER BY change DESC;")},
    {"title": "Urbanisation vs multidimensional poverty",
     "sql": ("SELECT d.district, d.province, d.value AS pct_urban_2023, m.mpi\n"
             "FROM district_indicators d\n"
             "JOIN mpi_districts m USING (district_key)\n"
             "WHERE d.indicator = 'urban_proportion' AND d.year = '2023'\n"
             "  AND m.low_n = 0\n"
             "ORDER BY m.mpi DESC;")},
    {"title": "What Pakistan exports most, FY2024-25",
     "sql": ("-- country IS NULL rows are the commodity totals; country rows are the\n"
             "-- partner split of the same money. Never add both.\n"
             "SELECT hs8, any_value(commodity) AS commodity,\n"
             "       round(sum(fy_value_kpkr) / 1e6, 1) AS rs_bn\n"
             "FROM trade_hs8\n"
             "WHERE direction = 'export' AND fiscal_year = '2024-25'\n"
             "  AND country IS NULL\n"
             "GROUP BY 1 ORDER BY rs_bn DESC LIMIT 20;")},
    {"title": "Top import partners, FY2024-25 (Rs bn)",
     "sql": ("SELECT country, round(sum(fy_value_kpkr) / 1e6, 1) AS rs_bn\n"
             "FROM trade_hs8\n"
             "WHERE direction = 'import' AND fiscal_year = '2024-25'\n"
             "  AND country IS NOT NULL\n"
             "GROUP BY 1 ORDER BY rs_bn DESC LIMIT 15;")},
    {"title": "Real GDP growth by year",
     "sql": ("SELECT year, round(value, 2) AS growth_pct\n"
             "FROM national_accounts\n"
             "WHERE table_sheet = 'Table 6' AND item LIKE 'D GDP%'\n"
             "ORDER BY year;")},
    {"title": "Federal defence budget over time",
     "sql": ("-- is_own_year_be keeps the document's own-year budget estimate,\n"
             "-- the only column that strings into a clean time series.\n"
             "SELECT doc_fy, item, value_rs_mn\n"
             "FROM budget_lines\n"
             "WHERE is_own_year_be AND item ILIKE '%defence%'\n"
             "ORDER BY doc_fy;")},
    {"title": "Rural electrification, worst tehsils (Mouza Census)",
     "sql": ("-- Shares are of MOUZAS, not people. Aggregate to the polygon first:\n"
             "-- several PBS tehsils can share one boundary.\n"
             "SELECT x.dd_name AS tehsil, m.province,\n"
             "       sum(m.\"ElectrictiyAvailability_NoneMouzas\") AS no_electricity,\n"
             "       sum(m.\"ElectrictiyAvailability_AllMouzas\"\n"
             "         + m.\"ElectrictiyAvailability_MostlyMouzas\"\n"
             "         + m.\"ElectrictiyAvailability_SomeMouzas\"\n"
             "         + m.\"ElectrictiyAvailability_NoneMouzas\") AS base,\n"
             "       round(100.0 * sum(m.\"ElectrictiyAvailability_NoneMouzas\")\n"
             "             / nullif(sum(m.\"ElectrictiyAvailability_AllMouzas\"\n"
             "               + m.\"ElectrictiyAvailability_MostlyMouzas\"\n"
             "               + m.\"ElectrictiyAvailability_SomeMouzas\"\n"
             "               + m.\"ElectrictiyAvailability_NoneMouzas\"), 0), 1) AS pct_dark\n"
             "FROM mouza_tehsil m\n"
             "JOIN mouza_crosswalk x USING (tehsil_code)\n"
             "WHERE m.TotalMauzaCount > 0\n"
             "GROUP BY 1, 2 HAVING base >= 50\n"
             "ORDER BY pct_dark DESC LIMIT 20;")},
]


# The school layer: each example teaches the one thing the table needs — read
# coord_precision before mapping, count by boundary or by source, and that a
# Sindh "boys'" school is usually mixed.
SCHOOL_EXAMPLES = [
    {"title": "Girls' middle-plus schools by district (Figure 1 of the girls' school piece)",
     "sql": ("-- Count by the polygon the point falls in (analysis_district_key_boundary), which is\n"
             "-- how the piece counts; district_key is the register's own district.\n"
             "SELECT analysis_district_key_boundary AS district,\n"
             "       count(*) FILTER (analysis_sex = 'G') AS girls_schools,\n"
             "       count(*) FILTER (analysis_sex = 'B') AS boys_schools\n"
             "FROM schools_pk\n"
             "WHERE in_analysis AND middle_plus AND analysis_district_key_boundary IS NOT NULL\n"
             "GROUP BY 1 ORDER BY girls_schools DESC;")},
    {"title": "How precise are the positions, by province?",
     "sql": ("-- Never map Punjab's tehsil-centroid rows as if they were GPS fixes.\n"
             "SELECT province, coord_precision, count(*) AS schools,\n"
             "       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY province), 1) AS pct\n"
             "FROM schools_pk\n"
             "GROUP BY 1, 2 ORDER BY province, schools DESC;")},
    {"title": "Sindh: designated boys' schools that enrol girls",
     "sql": ("-- In Sindh the name says GB (boys) but SEMIS calls most of them Mixed and the\n"
             "-- roster shows girls enrolled. This is why the piece reports the Sindh gap\n"
             "-- under three network definitions.\n"
             "SELECT district, count(*) AS boys_designated,\n"
             "       count(*) FILTER (gender_official = 'Mixed') AS officially_mixed,\n"
             "       count(*) FILTER (girls_enrolled > 0) AS enrolling_girls,\n"
             "       sum(girls_enrolled) AS girls_in_them\n"
             "FROM schools_pk\n"
             "WHERE province = 'Sindh' AND gender = 'Boys' AND functional AND middle_plus\n"
             "GROUP BY 1 ORDER BY girls_in_them DESC;")},
    {"title": "Where girls are furthest from a middle school, and how many are in school",
     "sql": ("SELECT district, region, girls_middle_median_km, boys_middle_median_km, gap_km,\n"
             "       girls_in_school_pct_5_16, boys_in_school_pct_5_16, tt_caveat\n"
             "FROM school_access_district\n"
             "ORDER BY girls_middle_median_km DESC LIMIT 20;")},
    {"title": "Does the Mouza Census agree? Districts where the layer fails its floor test",
     "sql": ("-- ratio < 1 means fewer schools in the layer than villages reporting one:\n"
             "-- schools are missing or misclassified. Sindh's girls-v-boys flag fires by\n"
             "-- construction (see the notes); read the girls' ratio itself.\n"
             "SELECT region, mouza_district, ratio_midplus_G, ratio_midplus_B, ratio_high_G, flag\n"
             "FROM school_validation_district\n"
             "WHERE status = 'matched' AND ratio_midplus_G < 0.9\n"
             "ORDER BY ratio_midplus_G;")},
    {"title": "Tehsils where girls are furthest from a middle school",
     "sql": ("-- Distances depend on the whole region's network, not the tehsil's own\n"
             "-- schools; Punjab rows are geocoded, so compare them at district scale.\n"
             "SELECT tehsil, district_key, region, girls_middle_km, boys_middle_km, gap_middle_km,\n"
             "       girls_middle_schools, girls_middle_over5km_pct\n"
             "FROM school_access_tehsil\n"
             "WHERE coord_tier = 'A'   -- GPS-quality positions only\n"
             "ORDER BY girls_middle_km DESC LIMIT 25;")},
    {"title": "Sindh multilateration: solved positions that disagree with the RSU pin",
     "sql": ("-- 24% of RSU pins sit more than 300 m from the position solved from SELD's own\n"
             "-- distance checker; the table keeps both so the choice can be audited.\n"
             "SELECT district, name, level, coord_method, coord_resid_m, pin_vs_solved_m, lat, lng\n"
             "FROM schools_pk\n"
             "WHERE province = 'Sindh' AND pin_vs_solved_m > 5000\n"
             "ORDER BY pin_vs_solved_m DESC LIMIT 25;")},
]


HEALTH_EXAMPLES = [
    {"title": "Travel time to care by poverty quintile",
     "sql": ("-- Districts in fifths by MPI (equal numbers of districts), each fifth weighted by\n"
             "-- population. The piece's 47 v 5 minutes uses population quintiles and medians;\n"
             "-- the gradient is the same either way.\n"
             "WITH q AS (\n"
             "  SELECT h.*, ntile(5) OVER (ORDER BY m.mpi) AS mpi_quintile\n"
             "  FROM health_access_district h JOIN mpi_districts m USING (district_key)\n"
             "  WHERE m.low_n = 0)\n"
             "SELECT mpi_quintile, count(*) AS districts,\n"
             "       round(sum(mot_popw_mean * pop_2020) / sum(pop_2020), 1) AS motorised_min,\n"
             "       round(sum(wal_popw_mean * pop_2020) / sum(pop_2020), 1) AS walking_min,\n"
             "       round(sum(mot_pct_pop_gt60 * pop_2020) / sum(pop_2020), 1) AS pct_over_60_min_motorised\n"
             "FROM q GROUP BY 1 ORDER BY 1;")},
    {"title": "Tehsils where most people are over an hour from care even with a vehicle",
     "sql": ("SELECT tehsil, district_key, province, pop_2020, mot_popw_mean, mot_pct_pop_gt60, wal_pct_pop_gt120\n"
             "FROM health_access_tehsil\n"
             "WHERE mot_pct_pop_gt60 > 50\n"
             "ORDER BY pop_2020 DESC LIMIT 25;")},
]

# The State Bank tables are long: one row per series x date, and the series are
# identified by name in sbp_series_catalog. Every example therefore starts from
# the catalogue, by name, so it reads as English rather than as a code.
CENSUS_PANEL_EXAMPLES = [
    {"title": "Census 2023 population by district — the safe filter",
     "sql": ("-- The panel nests levels: district rows and the tehsil rows inside them both\n"
             "-- exist, and locality sums to its own total, so pin both.\n"
             "-- Then watch the sex column. In tables 1, 3, 21 and 25 the sex split is\n"
             "-- carried in the INDICATOR label while sex stays 'all', so sex = 'all' here\n"
             "-- would still hand back the male, female and transgender rows. Name the\n"
             "-- indicator exactly; ILIKE '%POPULATION%' also catches POPULATION 2017.\n"
             "SELECT province_area, unit AS district, value AS population\n"
             "FROM census_panel_2023\n"
             "WHERE table_id = '1' AND unit_type = 'district'\n"
             "  AND indicator = 'POPULATION-2023 / ALL SEXES'\n"
             "  AND locality = 'all' AND NOT missing\n"
             "ORDER BY population DESC;\n"
             "-- 136 rows adding to 241,499,431 — PBS's published national total.")},
    {"title": "Check a filter before you trust it",
     "sql": ("-- Worth doing on any table_id you have not used before: if a single unit\n"
             "-- returns more than one row, your filter is not yet a series.\n"
             "SELECT indicator, col_label, locality, sex, count(*) AS rows_, sum(value) AS total\n"
             "FROM census_panel_2023\n"
             "WHERE table_id = '1' AND unit = 'LAHORE DISTRICT'\n"
             "GROUP BY 1, 2, 3, 4\n"
             "ORDER BY rows_ DESC, total DESC\n"
             "LIMIT 20;")},
    {"title": "Tehsil populations joined to the map geometry",
     "sql": ("-- dd_id is the key the site's tehsil layer uses. It is NULL on district rows\n"
             "-- and missing for about 150,000 tehsil rows, so an inner join silently drops\n"
             "-- units; count what you lose before mapping.\n"
             "SELECT count(*) AS tehsil_rows,\n"
             "       count(dd_id) AS joinable_to_geometry,\n"
             "       count(*) - count(dd_id) AS would_be_dropped\n"
             "FROM census_panel_2023\n"
             "WHERE table_id = '1' AND unit_type = 'tehsil'\n"
             "  AND locality = 'all' AND sex = 'all' AND indicator ILIKE '%POPULATION%';")},
    {"title": "Why 2017 and 2023 cannot simply be joined",
     "sql": ("-- Both panels use the same column names, which makes a cross-year join look\n"
             "-- easy. It is not: the indicator vocabularies barely overlap, and most of the\n"
             "-- difference is punctuation rather than meaning. Until a crosswalk exists,\n"
             "-- read this before comparing anything across the two censuses.\n"
             "WITH a AS (SELECT DISTINCT indicator FROM census_panel_2017),\n"
             "     b AS (SELECT DISTINCT indicator FROM census_panel_2023)\n"
             "SELECT (SELECT count(*) FROM a) AS labels_2017,\n"
             "       (SELECT count(*) FROM b) AS labels_2023,\n"
             "       (SELECT count(*) FROM a SEMI JOIN b USING (indicator)) AS identical_labels;")},
    {"title": "A dash means different things in the two panels",
     "sql": ("-- 2017 recovered its dashes from the printed PDFs and stored them as a real 0;\n"
             "-- 2023 left them NULL. So a count of populated cells is not comparable across\n"
             "-- the years, and neither is an average taken over missing rows.\n"
             "SELECT 2017 AS census_year, missing, count(*) AS rows_, count(value) AS with_a_value\n"
             "FROM census_panel_2017 GROUP BY 1, 2\n"
             "UNION ALL\n"
             "SELECT 2023, missing, count(*), count(value)\n"
             "FROM census_panel_2023 GROUP BY 1, 2\n"
             "ORDER BY census_year, missing;")},
]

SBP_EXAMPLES = [
    {"title": "Find a State Bank series by name",
     "sql": ("-- sbp_observations is one row per series x date. Start here: find the\n"
             "-- series, note its unit and frequency, then join on series_key.\n"
             "SELECT series_key, series_name, unit, frequency, available_since, available_upto\n"
             "FROM sbp_series_catalog\n"
             "WHERE series_name ILIKE '%remittance%'\n"
             "ORDER BY dataset_code, series_key;")},
    {"title": "The rupee against the dollar since 1947",
     "sql": ("SELECT o.obs_date, o.value AS pkr_per_usd\n"
             "FROM sbp_observations o\n"
             "JOIN sbp_series_catalog c USING (series_key)\n"
             "WHERE c.dataset_code = 'TS_GP_ER_FAERPKR_M'   -- bank floating average rates, monthly\n"
             "  AND c.series_name = 'Average Exchange rate of Pak Rupees per U.S. Dollar'\n"
             "  -- (a sibling series, 'App (+) / Dep (-) ...', holds the monthly % change)\n"
             "ORDER BY o.obs_date;")},
    {"title": "Where remittances come from, FY2024-25",
     "sql": ("-- The country series are hierarchical (U.A.E. contains Dubai/Abu Dhabi/Sharjah,\n"
             "-- 'Other GCC' contains Bahrain/Kuwait/Oman/Qatar, 'ten European Countries'\n"
             "-- contains Belgium..Sweden). Summing all of them overstates the total by 42%;\n"
             "-- these fifteen add up exactly to SBP's published figure.\n"
             "SELECT replace(c.series_name, 'Workers'' remittances received from ', '') AS source,\n"
             "       round(sum(o.value)) AS mn_usd\n"
             "FROM sbp_observations o JOIN sbp_series_catalog c USING (series_key)\n"
             "WHERE c.dataset_code = 'TS_GP_BOP_WR_M'\n"
             "  AND o.obs_date BETWEEN DATE '2024-07-01' AND DATE '2025-06-30'\n"
             "  AND replace(c.series_name, 'Workers'' remittances received from ', '') IN (\n"
             "    'Saudi Arabia', 'U.A.E.', 'U.K.', 'U.S.A.',\n"
             "    'Other GCC Countries excluding Saudi Arabia & U.A.E.', 'ten European Countries',\n"
             "    'Norway', 'Switzerland', 'Australia', 'Canada', 'Japan', 'Malaysia',\n"
             "    'South Africa', 'South Korea', 'Other Countries')\n"
             "GROUP BY 1 ORDER BY mn_usd DESC;")},
]


def _to_arrow(rows):
    import pyarrow as pa

    if not rows:
        return pa.table({})
    keys = list(rows[0].keys())
    return pa.table({k: [r.get(k) for r in rows] for k in keys})


def _today():
    import datetime

    return datetime.date.today().isoformat()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=Path, default=DEFAULT_SRC,
                    help="folder holding the desktop warehouse Parquet files")
    ap.add_argument("--only", choices=["district_indicators", "schools_pk", "health"],
                    help="rebuild only one table family into the existing warehouse")
    a = ap.parse_args()
    if not a.only and not (a.src / "trade_hs8.parquet").exists():
        sys.exit(f"desktop warehouse not found at {a.src} — pass --src")
    build(a.src, district_only=a.only == "district_indicators", schools_only=a.only == "schools_pk",
          health_only=a.only == "health")
