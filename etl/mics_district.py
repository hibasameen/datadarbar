"""
District-level indicators from Pakistan's Multiple Indicator Cluster Surveys,
aligned to the app's district crosswalk.

WHY MICS, GIVEN THE SITE ALREADY CARRIES PDHS
---------------------------------------------
The PDHS 2017-18 is representative at region level, not district: its median
district holds about 88 women, which is why every dhs_* field on this site is
labelled indicative and suppressed below n=30. MICS is different. Every round
used here was *designed* to estimate for districts, and the median district
holds 1,000 to 2,000 women. Where the two overlap — maternal care, nutrition,
immunisation — MICS is the series to quote and PDHS is the fallback.

The exception is Islamabad, and it is worth stating because it inverts the
rule. ICT has never had a MICS: the surveys are commissioned by provincial and
territorial Bureaus of Statistics out of their own development programmes, and
Islamabad has no such bureau. It does not need one. ICT is simultaneously a
single district and a PDHS reporting domain, so the PDHS figures there rest on
1,072 women rather than a thin district cut. Islamabad is blank on every mics_
field by design and its dhs_* fields are the right source.

SOURCES, AND WHY THE VINTAGES DIFFER
------------------------------------
Each region gets the most recent round that produces district estimates:

  Punjab              MICS 2017-18   microdata
  Sindh               MICS 2018-19   microdata
  Khyber Pakhtunkhwa  MICS 2019      microdata
  Balochistan         MICS 2019-20   microdata
  Azad Jammu&Kashmir  MICS 2020-21   published Survey Findings Report tables
  Gilgit-Baltistan    MICS 2016-17   microdata (MICS5)

Newer rounds exist for Punjab (2024) and GB (2024-25) and neither can be used
yet: both have published only a Key Findings Report, and neither KFR contains
a single district table. There is no newer district-level substitute either —
PBS's most recent PSLM District Level round is still 2019-20.

That leaves a 2016-17 to 2020-21 spread, which is why `mics_survey_year` is
published as an indicator in its own right rather than buried in a tooltip. A
reader comparing Gilgit-Baltistan with AJK is comparing measurements four
years apart, and the map should let them see that.

AJK IS THE ONE SOURCE THAT IS NOT MICRODATA. Its 2020-21 round has released no
datasets, so the figures are transcribed from the Survey Findings Report's
district tables. They cannot be recomputed, re-based or given standard errors;
the report's definitions are the definitions. GB's round is MICS5, which
predates four MICS6 modules outright, so E. coli water quality and Washington
Group child functioning are structurally absent there rather than missing.

Upstream build scripts, validation and the full list of construction traps live
in the Adaad repo at data/pk-mics-districts (build_mics6_indicators.py,
build_ajk_panel.py, parse_ajk_sfr.py, build_gb_panel.py). Ten of eleven
checkable indicators reproduce the published Sindh SFR figures to within 0.4,
eight of them exactly. Handwashing and child labour were built, failed
validation against the published totals, and were deliberately dropped rather
than shipped wrong.

Run standalone:  python mics_district.py /path/to/pk-mics-districts
"""
import json
import re
import sys
from pathlib import Path

# Indicators to publish, mapped to their field name on the site. The prefix
# groups them the way app.js expects: <group prefix>_<indicator key>.
INDICATORS = {
    "inst_delivery_pct":         "mics_mat_inst_delivery",
    "skilled_attendance_pct":    "mics_mat_skilled_attendance",
    "anc4_pct":                  "mics_mat_anc4",
    "stunting_pct":              "mics_nut_stunting",
    "wasting_pct":               "mics_nut_wasting",
    "underweight_pct":           "mics_nut_underweight",
    "careseek_fever_pct":        "mics_ch_careseek_fever",
    "ecoli_household_pct":       "mics_wash_ecoli_household",
    "ecoli_source_pct":          "mics_wash_ecoli_source",
    "open_defecation_pct":       "mics_wash_open_defecation",
    "birth_registered_pct":      "mics_prot_birth_registered",
    "violent_discipline_pct":    "mics_prot_violent_discipline",
    "child_func_difficulty_pct": "mics_eq_child_func_difficulty",
    "women_literate_pct":        "mics_wom_literate",
    "married_before_18_pct":     "mics_wom_married_before_18",
    "mcpr_pct":                  "mics_wom_mcpr",
    "cpr_any_pct":               "mics_wom_cpr_any",
    "dv_any_justified_pct":      "mics_wom_dv_justified",
}

SURVEY_YEAR = {
    "MICS 2016-17": 2016, "MICS 2017-18": 2017, "MICS 2018-19": 2018,
    "MICS 2019": 2019, "MICS 2019-20": 2019, "MICS 2020-21": 2020,
}

# MICS district label (normalised) -> normalised GeoJSON name. Only entries the
# base crosswalk in build_dataset.py does not already cover.
#
# Gilgit-Baltistan needs genuine aggregation because the boundary set is a 2015
# vintage: Baltistan, Kharmang and Shigar all fall inside Skardu, and Hunza and
# Nagar inside Hunza Nagar. That is done upstream in build_gb_panel.py, weighted
# by summed survey weight rather than by observation count.
#
# Sujawal is NOT such a case, despite looking like one. It was split from
# Thatta in 2013 and the polygon set does carry it — spelled "sajawal", which
# is why a search for "sujawal" turns up nothing and invites the conclusion
# that it must be folded into Thatta. Doing that would put two districts'
# households under one name and leave a real polygon blank.
CROSSWALK = {
    # Punjab
    "ry khan": "rahim yar khan", "dg khan": "dera ghazi khan",
    "tt singh": "toba tek singh",
    # Sindh
    "shahdad kot": "kambar shahdadkot", "naushahro feroze": "naushehro feroze",
    "tando allahyar": "tando allah yar",
    "tando muhmmad khan": "tando muhammad khan",
    "mirpur khas": "mirpurkhas", "umer kot": "umerkot",
    "karachi korangi": "korangi", "karachi malir": "malir",
    "sujawal": "sajawal",
    # Khyber Pakhtunkhwa
    "abbotabad": "abbottabad", "hari pur": "haripur",
    "laki marwat": "lakki marwat", "nowshehra": "nowshera",
    "torghar": "tor ghar", "bajor": "bajaur agency",
    "khyber": "khyber agency", "kuram": "kurram agency",
    "mohmind": "mohmand agency", "orakzai": "orakzai agency",
    "north waziristan": "north waziristan agency",
    "south waziristan": "south waziristan agency",
    # Balochistan
    "kachhi bolan": "kachhi", "kech turbat": "kech",
    "musakhel": "musakhail", "sheerani": "sherani", "sibbi": "sibi",
    # Gilgit-Baltistan / AJK
    "astore": "astor", "diamer": "diamir", "baltistan": "skardu",
    "ghanche": "ghanchi", "hattian bala": "hattian",
    "sudhnoti": "sudhnutti", "sudhonti": "sudhnutti",
}

# Lehri (Balochistan) has no polygon: it was created after the 2015 boundary
# vintage. Guessing a parent would put a real district's figures under another
# district's name, so it is dropped and named here rather than silently lost.
NO_POLYGON = {"lehri"}


def norm(s):
    s = re.sub(r"[^a-z0-9 ]", " ", str(s).strip().lower())
    return re.sub(r"\s+", " ", s).strip()


def geo_key(label):
    n = norm(label)
    if n in NO_POLYGON:
        return None
    return CROSSWALK.get(n, n)


def compute(panel_dir):
    """Read the three built panels and return {district_key: {field: value}}.

    Values are combined weighted means where two MICS districts share one
    polygon. The weight is the summed survey weight behind the indicator, not
    the observation count: MICS samples are not self-weighting and are often
    allocated roughly equally across districts of very different size, so
    counting observations would let a small district pull as hard as a large
    one.
    """
    import pandas as pd

    panel_dir = Path(panel_dir)
    sources = [
        ("mics6_district_indicators.csv", True),
        ("gb_boundary_panel.csv", True),
        ("ajk_district_panel.csv", False),   # published tables, no weights
    ]

    acc, meta = {}, {}
    for fname, has_weights in sources:
        path = panel_dir / fname
        if not path.exists():
            raise FileNotFoundError(f"{path} — run the Adaad build scripts first")
        df = pd.read_csv(path, index_col=0)
        for label, row in df.iterrows():
            key = geo_key(label)
            if key is None:
                continue
            survey = row.get("survey")
            meta.setdefault(key, survey)
            for src_col, field in INDICATORS.items():
                if src_col not in df.columns:
                    continue
                val = row[src_col]
                if pd.isna(val):
                    continue
                wcol = f"wt_{src_col}"
                w = row[wcol] if (has_weights and wcol in df.columns
                                  and not pd.isna(row[wcol])) else 1.0
                acc.setdefault(key, {}).setdefault(field, []).append((float(val), float(w)))

    out = {}
    for key, fields in acc.items():
        rec = {}
        for field, pairs in fields.items():
            tot = sum(w for _, w in pairs)
            rec[field] = round(sum(v * w for v, w in pairs) / tot, 1) if tot else None
        survey = meta.get(key)
        rec["mics_survey"] = survey
        rec["mics_survey_year"] = SURVEY_YEAR.get(survey)
        out[key] = rec
    return out


if __name__ == "__main__":
    d = compute(sys.argv[1] if len(sys.argv) > 1 else ".")
    here = Path(__file__).parent
    with open(here / "mics_district_indicators.json", "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=1, sort_keys=True)
    filled = {f: sum(1 for r in d.values() if r.get(f) is not None)
              for f in sorted(INDICATORS.values())}
    print(f"{len(d)} districts")
    for f, n in filled.items():
        print(f"  {f:34} {n:3d}")
