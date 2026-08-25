"""PSLM 2019-20 education indicators split by sex, at district level.

Data Darbar already carries `pslm_net_enrolment_rate` and
`pslm_pct_never_attended` as district totals (built in build_dataset.py,
`load_pslm`).  Neither is disaggregated by sex, so the warehouse cannot
answer "how many girls are in school here?" — the only sex-split education
field is *literacy*, which is a stock measure of adults, not current
schooling of children.

This module produces the same two measures separately for boys and girls,
using the identical definitions, sample restrictions and weights as
`load_pslm`, so that the male and female series decompose the totals
already published rather than sitting alongside them as a second opinion:

    net_enrolment_rate      share of ages 5-16 currently attending (sc1q01 == 3)
    pct_never_attended      share of ages 10+ who never attended (sc1q01 == 1)

Post-stratification is deliberately omitted.  `load_pslm` multiplies every
weight in a district by a single district-level constant, which cancels in
a ratio; reproducing the totals bit-for-bit without it (see `verify()`)
confirms this.

Emitted fields, per district:

    pslm_net_enrolment_rate_female    %
    pslm_net_enrolment_rate_male      %
    pslm_enrolment_gender_gap         male - female, percentage points
    pslm_pct_never_attended_female    %
    pslm_pct_never_attended_male      %
    pslm_edusex_n_obs                 smallest of the four sex cells
    pslm_edusex_low_n                 True if that is under MIN_SAMPLE_SIZE

Usage:
    python etl/pslm_sex_education.py --pslm "<path to PSLM stata data>" \
        [--write]        merge the fields into app/data/districts.json
        [--csv PATH]     also write a flat CSV
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_dataset import (  # noqa: E402
    MIN_SAMPLE_SIZE,
    PSLM_DISTRICT_CROSSWALK,
    _SUCCESSOR_DISTRICTS,
    apply_crosswalk,
    norm,
)

PREFIX = "pslm_"
SEXES = {1: "male", 2: "female"}


def district_key_map(pslm_dir: Path) -> dict[int, str]:
    """PSLM district code -> Data Darbar district_key, exactly as load_pslm does."""
    with pd.io.stata.StataReader(str(pslm_dir / "roster.dta")) as reader:
        labels = reader.value_labels().get("district", {})
    out = {}
    for code, label in labels.items():
        low = label.strip().lower()
        out[code] = PSLM_DISTRICT_CROSSWALK.get(low) or apply_crosswalk(norm(low))
    return out


def load(pslm_dir: Path) -> pd.DataFrame:
    """Person-level education frame with weights, district key and sex."""
    wdf = pd.read_stata(str(pslm_dir / "weight_file.dta"), convert_categoricals=False)
    wdf["psu"] = wdf["psu"].astype(int)
    weights = wdf.set_index("psu")["weights"].to_dict()

    code_to_key = district_key_map(pslm_dir)

    roster = pd.read_stata(
        str(pslm_dir / "roster.dta"),
        convert_categoricals=False,
        columns=["hhcode", "psu", "district", "idc", "age", "sb1q4"],
    )
    roster["w"] = roster["psu"].astype(int).map(weights)
    roster["dk"] = roster["district"].map(code_to_key)

    edu = pd.read_stata(
        str(pslm_dir / "secc1.dta"),
        convert_categoricals=False,
        columns=["hhcode", "psu", "district", "idc", "sc1q01"],
    )
    edu = edu.merge(
        roster[["hhcode", "idc", "age", "sb1q4", "w", "dk"]],
        on=["hhcode", "idc"],
        how="left",
    )
    return edu


def _rates(grp: pd.DataFrame) -> dict:
    """Net enrolment (5-16) and never-attended (10+) for one cell."""
    g5_16 = grp[(grp["age"] >= 5) & (grp["age"] <= 16)]
    g10 = grp[grp["age"] >= 10]
    out = {"n_5_16": len(g5_16), "n_10": len(g10)}
    w5_16 = g5_16["w"].sum()
    if w5_16 > 0 and len(g5_16) >= MIN_SAMPLE_SIZE:
        out["ner"] = round(g5_16[g5_16["sc1q01"] == 3]["w"].sum() / w5_16 * 100, 2)
    w10 = g10["w"].sum()
    if w10 > 0 and len(g10) >= MIN_SAMPLE_SIZE:
        out["never"] = round(g10[g10["sc1q01"] == 1]["w"].sum() / w10 * 100, 2)
    return out


def build(pslm_dir: Path) -> dict[str, dict]:
    edu = load(pslm_dir)
    out: dict[str, dict] = {}
    for dk, grp in edu.groupby("dk"):
        if not dk:
            continue
        rec: dict = {}
        cells = []
        for code, sex in SEXES.items():
            r = _rates(grp[grp["sb1q4"] == code])
            cells += [r["n_5_16"], r["n_10"]]
            if "ner" in r:
                rec[f"{PREFIX}net_enrolment_rate_{sex}"] = r["ner"]
            if "never" in r:
                rec[f"{PREFIX}pct_never_attended_{sex}"] = r["never"]
        m = rec.get(f"{PREFIX}net_enrolment_rate_male")
        f = rec.get(f"{PREFIX}net_enrolment_rate_female")
        if m is not None and f is not None:
            rec[f"{PREFIX}enrolment_gender_gap"] = round(m - f, 2)
        if rec:
            n_min = min(cells)
            rec[f"{PREFIX}edusex_n_obs"] = int(n_min)
            rec[f"{PREFIX}edusex_low_n"] = bool(n_min < MIN_SAMPLE_SIZE)
            out[dk] = rec
    return out


def verify(pslm_dir: Path, districts_json: Path) -> pd.DataFrame:
    """Reproduce the published totals from the same code path.

    If the pooled (both-sex) figures computed here match the published
    `pslm_net_enrolment_rate` and `pslm_pct_never_attended`, then the sex
    split is a decomposition of what Data Darbar already ships, not a
    parallel estimate on a different basis.
    """
    edu = load(pslm_dir)
    pub = json.loads(districts_json.read_text())
    rows = []
    for dk, grp in edu.groupby("dk"):
        if not dk or dk not in pub:
            continue
        r = _rates(grp)
        rows.append(
            {
                "district_key": dk,
                "ner_recomputed": r.get("ner"),
                "ner_published": pub[dk].get(f"{PREFIX}net_enrolment_rate"),
                "never_recomputed": r.get("never"),
                "never_published": pub[dk].get(f"{PREFIX}pct_never_attended"),
            }
        )
    df = pd.DataFrame(rows)
    df["ner_diff"] = (df["ner_recomputed"] - df["ner_published"]).abs()
    df["never_diff"] = (df["never_recomputed"] - df["never_published"]).abs()
    return df


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pslm", required=True, type=Path)
    ap.add_argument(
        "--districts",
        type=Path,
        default=Path(__file__).resolve().parent.parent / "app" / "data" / "districts.json",
    )
    ap.add_argument("--csv", type=Path)
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    chk = verify(args.pslm, args.districts)
    matched = chk.dropna(subset=["ner_recomputed", "ner_published"])
    print(
        f"verification: {len(matched)} districts compared, "
        f"max |NER diff| = {matched['ner_diff'].max():.3f} pp, "
        f"max |never-attended diff| = {chk['never_diff'].max():.3f} pp"
    )
    if matched["ner_diff"].max() > 0.02:
        print("  WARNING: totals do not reproduce; the split is not a decomposition.")

    data = build(args.pslm)
    print(f"built: {len(data)} districts with sex-split education indicators")
    low = sum(1 for v in data.values() if v[f"{PREFIX}edusex_low_n"])
    print(f"  {low} flagged low_n (smallest sex cell under {MIN_SAMPLE_SIZE})")

    if args.csv:
        pd.DataFrame(
            [{"district_key": k, **v} for k, v in sorted(data.items())]
        ).to_csv(args.csv, index=False)
        print(f"  wrote {args.csv}")

    if args.write:
        districts = json.loads(args.districts.read_text())
        n = 0
        for dk, rec in data.items():
            if dk in districts:
                districts[dk].update(rec)
                n += 1
        # Districts carved out after PSLM was fielded already carry their
        # parent's pslm_ figures (build_dataset._inherit_from_parent_district).
        # That routine fills a family only when the child has *nothing* for it,
        # so it will not backfill these new fields on a later run — do it here,
        # under the inheritance flag the parent's figures already travel with.
        for child, parent in _SUCCESSOR_DISTRICTS.items():
            if child not in districts or parent not in data:
                continue
            if districts[child].get(f"{PREFIX}inherited_from") != parent:
                continue
            if f"{PREFIX}net_enrolment_rate_female" in districts[child]:
                continue
            districts[child].update(data[parent])
            n += 1
            print(f"  {child}: inherited sex-split education from {parent}")
        args.districts.write_text(json.dumps(districts, indent=2) + "\n")
        print(f"  merged into {args.districts} ({n} districts)")


if __name__ == "__main__":
    main()
