"""Census 2023 Table 12: schooling by district and sex.

Data Darbar already carried three fields from this table — `t12_2023_ever_attended`,
`t12_2023_never_school_all` and `t12_2023_out_of_school_5_16` — as totals with no
sex split, and as orphans: no app.js group, so no label, no dataset and no place
on the district map. This module adds the sex splits and the accompanying group.

WHY THE CENSUS. For enrolment the census beats PSLM 2019-20: a full enumeration
rather than a sample, no small-district suppression, and it reaches the merged
tribal districts and the five Balochistan districts PSLM never enumerated at
all. Across the districts both cover, the two sources rank-correlate at 0.92.

THE PARSE TRAP. In `table_12_districts_combined_with_headers.csv` the geography
names are not a column. They sit as rows inside `indicator` ("BADIN DISTRICT",
"MATLI TALUKA"), heading the block of indicator rows beneath them. Read naively
the file looks like it holds five districts — the five source PDFs. Forward-
filling the geography header down its block recovers 135.

THE MEASURE. The census publishes enrolment as counts by level plus an explicit
out-of-school count for ages 5-16, and no 5-16 population denominator. So the
rate here is

    in school (%) = (primary + middle + matric)
                    / (primary + middle + matric + out-of-school 5-16)

Primary through matric is the 5-16 span; intermediate and above sit past 16 and
are excluded. This is close to, but not the same as, PSLM's "currently
attending, ages 5-16" — do not expect the two series to agree in level.

NO 2017 COUNTERPART. Census 2017's district tables are mother tongue (11),
literacy by age (12), literacy by sex (13) and educational attainment of the
*literate* population (14). None reports current school attendance or
out-of-school children, so these fields carry a 2023 value only and no
`t12_diff_*` is computed. The sex-split trend that does exist across both
censuses is literacy: `t12_2017_literacy_ratio_female` against
`t12_2023_literacy_ratio_female`, with the diff already built.

Usage:
    python etl/census2023_schooling_by_sex.py --census "<PBS Census 2023 dir>" \
        [--write]      merge the fields into app/data/districts.json
        [--csv PATH]   also write a flat CSV
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_dataset import apply_crosswalk, norm  # noqa: E402

TABLE = "table_12_districts_combined_with_headers.csv"
PREFIX = "t12_2023_"

INDICATORS = {
    "Population >=5", "Population >=10", "Literate >=10", "Literate %",
    "Ever Attended", "Enrolment Primary", "Enrolment Middle",
    "Enrolment Matric", "Enrolment Intermidiate", "Enrolment Graduation above",
    "Never to School (all)", "Drop Out (5 - 16)", "Never to School (5-16)",
    "Out of School Children (5-16)",
}
# Primary through matric spans ages 5-16; intermediate and above sit past it.
SCHOOL_AGE = ["Enrolment Primary", "Enrolment Middle", "Enrolment Matric"]


def load_long(census_dir: Path) -> pd.DataFrame:
    """Table 12 as one row per geography x indicator, with the sex splits."""
    d = pd.read_csv(census_dir / TABLE)
    d["geo"] = d.indicator.where(~d.indicator.isin(INDICATORS)).ffill()
    dist = d[d.geo.str.contains(" DISTRICT$", na=False)
             & d.indicator.isin(INDICATORS)].copy()
    dist["name"] = dist.geo.str.replace(" DISTRICT$", "", regex=True).str.title()
    # Reuse Data Darbar's own crosswalk so these keys match every other layer.
    dist["district_key"] = dist["name"].map(lambda s: apply_crosswalk(norm(s.lower())))
    return dist


def build(census_dir: Path) -> dict[str, dict]:
    dist = load_long(census_dir)
    w = dist.pivot_table(index="district_key", columns="indicator",
                         values=["male", "female"], aggfunc="first")

    def rate(sex):
        enr = sum(w[(sex, c)] for c in SCHOOL_AGE)
        oos = w[(sex, "Out of School Children (5-16)")]
        return enr / (enr + oos) * 100

    girls, boys = rate("female"), rate("male")
    out: dict[str, dict] = {}
    for key in w.index:
        if pd.isna(girls.get(key)) or pd.isna(boys.get(key)):
            continue
        rec = {
            f"{PREFIX}in_school_5_16_female": round(float(girls[key]), 2),
            f"{PREFIX}in_school_5_16_male": round(float(boys[key]), 2),
            f"{PREFIX}schooling_gender_gap": round(float(boys[key] - girls[key]), 2),
        }
        for field, col in [
            ("out_of_school_5_16_female", ("female", "Out of School Children (5-16)")),
            ("out_of_school_5_16_male", ("male", "Out of School Children (5-16)")),
            ("never_school_5_16_female", ("female", "Never to School (5-16)")),
            ("never_school_5_16_male", ("male", "Never to School (5-16)")),
        ]:
            v = w[col].get(key)
            if pd.notna(v):
                rec[f"{PREFIX}{field}"] = float(v)
        out[key] = rec
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--census", required=True, type=Path,
                    help="directory holding " + TABLE)
    ap.add_argument("--districts", type=Path,
                    default=Path(__file__).resolve().parent.parent
                    / "app" / "data" / "districts.json")
    ap.add_argument("--csv", type=Path)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    data = build(args.census)
    print(f"built: {len(data)} districts from Census 2023 Table 12")

    gaps = sorted(((v[f"{PREFIX}schooling_gender_gap"], k) for k, v in data.items()),
                  reverse=True)
    print("  widest gaps (pp, boys minus girls): "
          + ", ".join(f"{k} {g:.1f}" for g, k in gaps[:4]))
    print(f"  districts where girls are ahead: {sum(1 for g, _ in gaps if g < 0)}")

    if args.csv:
        pd.DataFrame([{"district_key": k, **v} for k, v in sorted(data.items())]
                     ).to_csv(args.csv, index=False)
        print(f"  wrote {args.csv}")

    if args.write:
        districts = json.loads(args.districts.read_text())
        matched = [k for k in data if k in districts]
        for k in matched:
            districts[k].update(data[k])
        args.districts.write_text(json.dumps(districts, indent=2) + "\n")
        missing = sorted(set(data) - set(districts))
        print(f"  merged into {args.districts} ({len(matched)} districts)")
        if missing:
            print(f"  {len(missing)} census districts have no entry in districts.json "
                  f"(created after the boundary file): {', '.join(missing)}")


if __name__ == "__main__":
    main()
