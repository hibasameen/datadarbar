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


def load_indicator_groups(app_js: Path) -> dict:
    s = app_js.read_text(encoding="utf-8")
    i = s.index("const INDICATOR_GROUPS")
    start = s.index("{", i)
    depth, j = 0, start
    while j < len(s):
        if s[j] == "{":
            depth += 1
        elif s[j] == "}":
            depth -= 1
            if depth == 0:
                j += 1
                break
        j += 1
    return json.loads(_js_object_to_json(s[start:j]))


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
def build(src: Path) -> None:
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
    groups = load_indicator_groups(APP / "assets" / "js" / "app.js")
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
        "sum the 2023 year instead.",
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
        "PBS Census 2017 & 2023, PSLM 2019-20, LFS 2020-21/2024-25, HIES 2024-25, PDHS 2017-18",
        "SELECT * FROM df_district ORDER BY district_key, dataset, group_key, indicator, year",
    )
    if unknown:
        print(f"    ({len(unknown)} fields had no app.js label, e.g. {sorted(unknown)[:3]})")

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
        "Meta/Data for Good RWI, WorldPop 2020, NOAA VIIRS DNB monthly composites",
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
        "NOAA VIIRS DNB monthly composites",
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
        "PBS Mouza Census 2020 frame × geoBoundaries PAK ADM3",
        f"SELECT * FROM read_csv_auto('{xw_csv}', all_varchar=true) "
        f"ORDER BY province, district, tehsil",
    )

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

    # ── catalog ──────────────────────────────────────────────────────────────
    catalog = {
        "name": "Data Darbar",
        "version": 1,
        "generated": _today(),
        "license": "Derived data CC BY 4.0 · code MIT",
        "tables": sorted(tables, key=lambda t: t["name"]),
        "examples": EXAMPLES,
    }
    (OUT / "catalog.json").write_text(json.dumps(catalog, indent=1))
    total = sum(t["bytes"] for t in tables)
    eager = sum(t["bytes"] for t in tables if t["bytes"] < 2_000_000)
    print(f"\ncatalog.json written · {len(tables)} tables · {total/1e6:.1f} MB total "
          f"({eager/1e6:.1f} MB loaded eagerly)")


EXAMPLES = [
    {"title": "Ten poorest districts",
     "sql": "SELECT rank, name, prov, mpi, H, A\nFROM mpi_districts\nWHERE low_n = 0\nORDER BY rank\nLIMIT 10;"},
    {"title": "Female literacy, 2017 vs 2023",
     "sql": ("SELECT district, province,\n"
             "       max(value) FILTER (year = '2017') AS lit_2017,\n"
             "       max(value) FILTER (year = '2023') AS lit_2023,\n"
             "       round(max(value) FILTER (year = '2023')\n"
             "           - max(value) FILTER (year = '2017'), 1) AS change\n"
             "FROM district_indicators\n"
             "WHERE indicator = 'literacy_ratio_female'\n"
             "GROUP BY 1, 2\nORDER BY change DESC;")},
    {"title": "Top import partners, FY2024-25 (Rs bn)",
     "sql": ("SELECT country, round(sum(fy_value_kpkr) / 1e6, 1) AS rs_bn\n"
             "FROM trade_hs8\n"
             "WHERE direction = 'import' AND fiscal_year = '2024-25'\n"
             "  AND country IS NOT NULL\n"
             "GROUP BY 1\nORDER BY rs_bn DESC\nLIMIT 15;")},
    {"title": "What Pakistan exports most (FY2024-25)",
     "sql": ("SELECT hs8, any_value(commodity) AS commodity,\n"
             "       round(sum(fy_value_kpkr) / 1e6, 1) AS rs_bn\n"
             "FROM trade_hs8\n"
             "WHERE direction = 'export' AND fiscal_year = '2024-25'\n"
             "  AND country IS NULL\n"
             "GROUP BY 1\nORDER BY rs_bn DESC\nLIMIT 20;")},
    {"title": "Real GDP growth by year",
     "sql": ("SELECT year, round(value, 2) AS growth_pct\n"
             "FROM national_accounts\n"
             "WHERE table_sheet = 'Table 6' AND item LIKE 'D GDP%'\n"
             "ORDER BY year;")},
    {"title": "Federal defence budget over time",
     "sql": ("SELECT doc_fy, item, value_rs_mn\n"
             "FROM budget_lines\n"
             "WHERE is_own_year_be AND item ILIKE '%defence%'\n"
             "ORDER BY doc_fy;")},
    {"title": "Do lights track wealth? (tehsils, by province)",
     "sql": ("-- Note: Meta's RWI uses night-lights as an input, so this correlation\n"
             "-- is partly mechanical. Read it as consistency, not as validation.\n"
             "SELECT prov,\n"
             "       count(*) AS tehsils,\n"
             "       round(corr(rwi, ln(nl_latest + 0.01)), 3) AS corr_rwi_lognl\n"
             "FROM tehsil_satellite\n"
             "WHERE nl_lowc = 0\nGROUP BY 1\nHAVING count(*) >= 5\nORDER BY 3 DESC;")},
    {"title": "Night-lights, tehsil trajectories since 2020",
     "sql": ("SELECT s.name AS tehsil, s.prov,\n"
             "       max(l.radiance) FILTER (l.year = 2020) AS y2020,\n"
             "       max(l.radiance) FILTER (l.year = 2026) AS y2026,\n"
             "       s.pop\n"
             "FROM tehsil_nightlights l\n"
             "JOIN tehsil_satellite s USING (tehsil_id)\n"
             "WHERE s.nl_lowc = 0 AND s.pop > 200000\n"
             "GROUP BY 1, 2, 5\nORDER BY y2026 / nullif(y2020, 0) DESC\nLIMIT 20;")},
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
    {"title": "Do brighter tehsils have more girls' schools?",
     "sql": ("-- Facilities are places, lights are satellite: this asks whether the\n"
             "-- two agree, not whether either causes the other.\n"
             "SELECT s.prov,\n"
             "       count(*) AS tehsils,\n"
             "       round(corr(g.pct_girls_primary, ln(s.nl_latest + 0.01)), 3) AS corr\n"
             "FROM (SELECT x.dd_id,\n"
             "             100.0 * sum(m.\"EducationFacility_FemalePrimaryExistanceCount\")\n"
             "             / nullif(sum(m.\"EducationFacility_FemalePrimaryExistanceCount\"\n"
             "               + m.\"EducationFacility_FemalePrimaryNotExistanceCount\"), 0)\n"
             "               AS pct_girls_primary\n"
             "      FROM mouza_tehsil m JOIN mouza_crosswalk x USING (tehsil_code)\n"
             "      WHERE m.TotalMauzaCount > 0 GROUP BY 1) g\n"
             "JOIN tehsil_satellite s ON s.tehsil_id = g.dd_id\n"
             "WHERE s.nl_lowc = 0\n"
             "GROUP BY 1 HAVING count(*) >= 5 ORDER BY 3;")},
    {"title": "Urbanisation vs multidimensional poverty",
     "sql": ("SELECT d.district, d.province, d.value AS pct_urban_2023, m.mpi\n"
             "FROM district_indicators d\n"
             "JOIN mpi_districts m USING (district_key)\n"
             "WHERE d.indicator = 'urban_proportion' AND d.year = '2023'\n"
             "  AND m.low_n = 0\nORDER BY m.mpi DESC;")},
]


SBP_EXAMPLES = [
    {"title": "The rupee since 1947",
     "sql": ("SELECT obs_date, value AS pkr_per_usd\n"
             "FROM sbp_observations\n"
             "WHERE series_key = 'TS_GP_ER_FAERPKR_M.E00220'\n"
             "ORDER BY obs_date;")},
    {"title": "The real policy rate",
     "sql": ("-- The policy rate only moves when SBP meets, so it cannot be joined\n"
             "-- month-to-month against CPI. ASOF JOIN carries the last rate forward.\n"
             "SELECT c.obs_date,\n"
             "       p.value AS policy_rate,\n"
             "       c.value AS cpi_yoy,\n"
             "       round(p.value - c.value, 2) AS real_rate\n"
             "FROM sbp_observations c\n"
             "ASOF JOIN (SELECT obs_date, value FROM sbp_observations\n"
             "           WHERE series_key = 'TS_GP_IR_SIRPR_AH.SBPOL0030') p\n"
             "  ON p.obs_date <= c.obs_date\n"
             "WHERE c.series_key = 'TS_GP_PT_CPI_M.P00011516'\n"
             "ORDER BY c.obs_date;")},
    {"title": "Where remittances come from, FY2024-25",
     "sql": ("-- The country series are hierarchical: U.A.E. contains Dubai/Abu Dhabi/Sharjah,\n"
             "-- 'Other GCC' contains Bahrain/Kuwait/Oman/Qatar, and 'ten European Countries'\n"
             "-- contains Belgium..Sweden. Summing all of them overstates the total by ~42%,\n"
             "-- so this restricts to the partition that reconciles to the published figure.\n"
             "SELECT replace(c.series_name, 'Workers'' remittances received from ', '') AS source,\n"
             "       round(sum(o.value), 0) AS mn_usd\n"
             "FROM sbp_observations o\n"
             "JOIN sbp_series_catalog c USING (series_key)\n"
             "WHERE c.dataset_code = 'TS_GP_BOP_WR_M'\n"
             "  AND o.obs_date BETWEEN DATE '2024-07-01' AND DATE '2025-06-30'\n"
             "  AND c.series_name LIKE '%received from%'\n"
             "  AND c.series_name NOT ILIKE '%Dubai%'    AND c.series_name NOT ILIKE '%Abu Dhabi%'\n"
             "  AND c.series_name NOT ILIKE '%Sharjah%'  AND c.series_name NOT ILIKE '%four U.A.E%'\n"
             "  AND c.series_name NOT ILIKE '%Bahrain%'  AND c.series_name NOT ILIKE '%Kuwait%'\n"
             "  AND c.series_name NOT ILIKE '%Oman%'     AND c.series_name NOT ILIKE '%Qatar%'\n"
             "  AND c.series_name NOT ILIKE '%Belgium%'  AND c.series_name NOT ILIKE '%Denmark%'\n"
             "  AND c.series_name NOT ILIKE '%France%'   AND c.series_name NOT ILIKE '%Germany%'\n"
             "  AND c.series_name NOT ILIKE '%Greece%'   AND c.series_name NOT ILIKE '%Ireland%'\n"
             "  AND c.series_name NOT ILIKE '%Italy%'    AND c.series_name NOT ILIKE '%Netherland%'\n"
             "  AND c.series_name NOT ILIKE '%Spain%'    AND c.series_name NOT ILIKE '%Sweden%'\n"
             "GROUP BY 1\nORDER BY mn_usd DESC;")},
    {"title": "Pakistan's fiscal balance, and whether it adds up",
     "sql": ("-- Revenue - Expenditure = Fiscal Balance, and + Interest = Primary Balance.\n"
             "-- Both identities hold exactly in the source data.\n"
             "SELECT o.obs_date,\n"
             "       round(max(o.value) FILTER (c.series_name LIKE 'Total Revenue%') / 1e6, 2) AS revenue_trn,\n"
             "       round(max(o.value) FILTER (c.series_name LIKE 'Total Expenditure (%') / 1e6, 2) AS spending_trn,\n"
             "       round(max(o.value) FILTER (c.series_name LIKE 'Fiscal Balance%') / 1e6, 2) AS fiscal_bal_trn,\n"
             "       round(100 * max(o.value) FILTER (c.series_name LIKE 'Fiscal Balance%')\n"
             "                 / max(o.value) FILTER (c.series_name LIKE 'Gross Domestic Product%'), 2) AS pct_of_gdp\n"
             "FROM sbp_observations o\n"
             "JOIN sbp_series_catalog c USING (series_key)\n"
             "WHERE c.dataset_code = 'TS_GP_PF_SPF_Y'\n"
             "GROUP BY 1\nORDER BY 1;")},
    {"title": "Reserves cover: months of imports",
     "sql": ("WITH res AS (\n"
             "  SELECT o.obs_date, o.value AS sbp_reserves\n"
             "  FROM sbp_observations o JOIN sbp_series_catalog c USING (series_key)\n"
             "  WHERE c.series_name = 'Total SBP Reserves'),\n"
             "imp AS (\n"
             "  SELECT o.obs_date, o.value AS goods_imports\n"
             "  FROM sbp_observations o JOIN sbp_series_catalog c USING (series_key)\n"
             "  WHERE c.series_name = 'Goods - Import (FOB)')\n"
             "SELECT r.obs_date, round(r.sbp_reserves, 0) AS reserves_mn_usd,\n"
             "       round(r.sbp_reserves / nullif(avg(i.goods_imports) OVER (\n"
             "            ORDER BY r.obs_date ROWS BETWEEN 11 PRECEDING AND CURRENT ROW), 0), 1) AS months_cover\n"
             "FROM res r JOIN imp i USING (obs_date)\n"
             "ORDER BY r.obs_date DESC\nLIMIT 24;")},
    {"title": "Urban vs rural food inflation",
     "sql": ("SELECT o.obs_date,\n"
             "       max(o.value) FILTER (c.series_name ILIKE 'Urban Food%') AS urban_food,\n"
             "       max(o.value) FILTER (c.series_name ILIKE 'Rural Food%') AS rural_food\n"
             "FROM sbp_observations o\n"
             "JOIN sbp_series_catalog c USING (series_key)\n"
             "WHERE c.dataset_code = 'TS_GP_PT_CPI_M'\n"
             "  AND c.series_name ILIKE '%Year-on-Year%'\n"
             "GROUP BY 1\nHAVING urban_food IS NOT NULL\nORDER BY 1;")},
    {"title": "Bank deposits by province (stops Jun-2023)",
     "sql": ("-- The only sub-national SBP data. Stops Jun-2023.\n"
             "SELECT c.series_name, o.obs_date, round(o.value / 1000, 1) AS rs_bn\n"
             "FROM sbp_observations o\n"
             "JOIN sbp_series_catalog c USING (series_key)\n"
             "WHERE c.dataset_code = 'TS_GP_BAM_ADVDEP_HY'\n"
             "  AND c.series_name ILIKE 'Total Outstanding Deposits%'\n"
             "  AND o.obs_date = (SELECT max(obs_date) FROM sbp_observations\n"
             "                    WHERE dataset_code = 'TS_GP_BAM_ADVDEP_HY')\n"
             "ORDER BY rs_bn DESC;")},
    {"title": "What SBP series are actually current?",
     "sql": ("SELECT subject_area, frequency,\n"
             "       count(*) AS series,\n"
             "       max(available_upto) AS latest\n"
             "FROM sbp_series_catalog\n"
             "GROUP BY 1, 2\nORDER BY 1, series DESC;")},
    {"title": "SBP vs PBS: two measures of the same GDP",
     "sql": ("-- Should agree: same PBS source, different distribution channel.\n"
             "SELECT n.year, n.value AS pbs_workbook,\n"
             "       s.value AS sbp_easydata\n"
             "FROM national_accounts n\n"
             "JOIN sbp_observations s\n"
             "  ON s.series_key = 'TS_GP_RLS_PAKGDP15_Y.GDP00150000'\n"
             " AND CAST(left(n.year, 4) AS INT) + 1 = year(s.obs_date)\n"
             "WHERE n.table_sheet = 'Table 5' AND n.item LIKE 'D GDP%'\n"
             "ORDER BY n.year;")},
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
    a = ap.parse_args()
    if not (a.src / "trade_hs8.parquet").exists():
        sys.exit(f"desktop warehouse not found at {a.src} — pass --src")
    build(a.src)
