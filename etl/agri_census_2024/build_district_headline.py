#!/usr/bin/env python3
"""District headline indicators from the Agricultural Census 2024 long table.

Reads  derived/agri_census_2024_long.csv.gz (from parse_tables.py)
Writes derived/agri_census_2024_district_headline.csv  — one row per unit (district / Cholistan / MPA / ICT /
       division / province / Pakistan), raw counts + derived shares, plus a `check` column set.
Columns are matched by header text (normalised) because a few tables carry extra columns in some provinces.
"""
import os, re, sys
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
DER = os.path.abspath(os.path.join(HERE, "..", "..", "..", "raw_data", "pbs", "agricultural_census_2024", "derived"))

def N(s):
    s = str(s).upper()
    s = s.replace("TANENT", "TENANT").replace("HOPUSEHOLD", "HOUSEHOLD").replace("FREGMENT", "FRAGMENT").replace("AVERAGAE", "AVERAGE")
    return re.sub(r"[^A-Z0-9|]", "", s)

# indicator: (table, column-regex on normalised header, category or None for flat tables)
SPEC = {
    "farms":                 ("T1.1", r"^NUMBEROFFARMS\|TOTAL$", "TOTAL"),
    "farm_area_ac":          ("T1.1", r"^FARMAREA\|TOTAL$", "TOTAL"),
    "cultivated_area_ac":    ("T1.1", r"^CULTIVATEDAREA\|TOTAL$", "TOTAL"),
    "farms_under1":          ("T1.1", r"^NUMBEROFFARMS\|TOTAL$", "UNDER 1.0"),
    "farms_1_to_2_5":        ("T1.1", r"^NUMBEROFFARMS\|TOTAL$", "1.0 TO UNDER 2.5"),
    "farm_area_25_plus_ac":  ("T1.1", r"^FARMAREA\|TOTAL$", ["25.0 TO UNDER 50.0", "50.0 TO UNDER 100.0", "100.0 AND ABOVE"]),
    "farms_owner":           ("T1.3", r"^NUMBEROFFARMS\|OWNER$", "TOTAL"),
    "farms_owner_cum_tenant":("T1.3", r"^NUMBEROFFARMS\|OWNERCUMTENANT$", "TOTAL"),
    "farms_tenant":          ("T1.3", r"^NUMBEROFFARMS\|TENANT$", "TOTAL"),
    "farm_area_tenant_ac":   ("T1.3", r"^AREAOFFARMS\|TENANT$", "TOTAL"),
    "farms_female":          ("T1.9", r"^NUMBEROFFARMS\|FEMALE$", "TOTAL"),
    "farms_fragmented":      ("T2.1", r"^NUMBEROFFRAGMENTEDFARMS\|TOTAL$", "TOTAL"),
    "irrigated_area_ac":     ("T4.2", r"ACTUALLYIRRIGATED\|TOTAL$", "TOTAL"),
    "irrig_canal_only_ac":   ("T4.2", r"ACTUALLYIRRIGATED\|CANALONLY$", "TOTAL"),
    "irrig_tubewell_only_ac":("T4.2", r"ACTUALLYIRRIGATED\|TUBEWELLONLY$", "TOTAL"),
    "cropping_intensity_pct":("T5.3", r"^AVERAGECROPPINGINTENSITY\|ALLFARMS$", ["TOTAL", "PRIVATE-FARMS TOTAL"]),
    "cropped_area_ac":       ("T6.1", r"^TOTALCROPPEDAREA$", "TOTAL"),
    "kharif_area_ac":        ("T6.1", r"^KHARIFCROPSAREA\|TOTAL$", "TOTAL"),
    "rabi_area_ac":          ("T6.1", r"^RABICROPSAREA\|TOTAL$", "TOTAL"),
    "orchard_area_ac":       ("T6.1", r"^ORCHARDAREA\|TOTAL$", "TOTAL"),
    "wheat_area_ac":         ("T6.9", r"^AREAUNDERWHEAT\|TOTAL$", "TOTAL"),
    "wheat_farms":           ("T6.9", r"^FARMSREPORTINGWHEAT\|NUMBER$", "TOTAL"),
    "paddy_area_ac":         ("T6.10", r"^FARMSREPORTINGPADDY\|ANYVARIETY\|AREA$", "TOTAL"),
    "cotton_area_ac":        ("T6.11", r"^AREAUNDERCOTTON$", "TOTAL"),
    "maize_area_ac":         ("T6.12", r"^AREAUNDERMAIZE\|TOTAL$", "TOTAL"),
    "sugarcane_area_ac":     ("T6.13", r"^AREAUNDERSUGARCANE$", "TOTAL"),
    "tunnel_farms":          ("T6.19", r"^FARMSREPORTINGUSETUNNELFARMING\|NUMBER$", "TOTAL"),
    "cattle":                ("T7.1", r"^NUMBEROFANIMALS\|CATTLE$", None),
    "buffaloes":             ("T7.1", r"^NUMBEROFANIMALS\|BUFFALOES$", None),
    "sheep":                 ("T7.1", r"^NUMBEROFANIMALS\|SHEEP$", None),
    "goats":                 ("T7.1", r"^NUMBEROFANIMALS\|GOATS$", None),
    "camels":                ("T7.1", r"^NUMBEROFANIMALS\|CAMELS$", None),
    "asses":                 ("T7.1", r"^NUMBEROFANIMALS\|ASSES$", None),
    "tractors":              ("T8.1", r"^ALLTRACTORS$", None),
    "tubewells_liftpumps":   ("T8.11", r"^TOTALTUBEWELLS/?LIFTPUMPS$|^TOTALTUBEWELLSLIFTPUMPS$", None),
    "tubewells":             ("T8.11", r"^NUMBEROFTUBEWELLS\|TOTAL$", None),
    "tubewells_solar":       ("T8.11", r"^NUMBEROFTUBEWELLS\|SOLAR$", None),
    "tubewells_electric":    ("T8.11", r"^NUMBEROFTUBEWELLS\|ELECTRIC$", None),
    "tubewells_diesel":      ("T8.11", r"^NUMBEROFTUBEWELLS\|DIESEL$", None),
}

def main():
    d = pd.read_csv(os.path.join(DER, "agri_census_2024_long.csv.gz"), low_memory=False)
    d["ncol"] = d["column"].map(N)
    d["cat"] = d["category"].fillna("").str.upper().str.strip()
    # drop the national file's province-level duplicates? keep: national gives PAKISTAN + provinces; provincial files give the rest
    d = d[(d.prov_file != "national") | (d.unit_level == "country")]
    d = d[~((d.prov_file == "punjab") & (d.unit == "ISLAMABAD CAPITAL TERRITORY") & False)]
    frames, matchlog = [], []
    for ind, (tid, rx, cat) in SPEC.items():
        t = d[d.table_id == tid]
        cols = sorted(set(t.ncol[t.ncol.str.contains(rx, regex=True)]))
        for p in sorted(t.prov_file.unique()):
            pc = sorted(set(t[t.prov_file == p].ncol[t[t.prov_file == p].ncol.str.contains(rx, regex=True)]))
            matchlog.append((ind, p, pc))
            if len(pc) != 1:
                print(f"WARN {ind} {p}: {len(pc)} column matches {pc}", file=sys.stderr)
        sel = t[t.ncol.str.contains(rx, regex=True)]
        if cat is not None:
            cats = [cat] if isinstance(cat, str) else cat
            sel = sel[sel.cat.isin([c.upper() for c in cats])]
        s = sel.groupby(["prov_file", "unit", "unit_level"], as_index=False)["value"].sum().rename(columns={"value": ind})
        frames.append(s)
    out = frames[0]
    for f in frames[1:]:
        out = out.merge(f, on=["prov_file", "unit", "unit_level"], how="outer")
    # Washuk is missing from Balochistan Table 1.1 (acres) but present in 1.2 (hectares): fill from 1.2
    h = d[(d.table_id == "T1.2") & (d.cat.isin(["TOTAL", "UNDER 1.0", "1.0 TO UNDER 2.5", "25.0 TO UNDER 50.0", "50.0 TO UNDER 100.0", "100.0 AND ABOVE", "UNDER 0.4", "0.4 TO UNDER 1.0", "10.1 TO UNDER 20.2", "20.2 TO UNDER 40.5", "40.5 AND ABOVE"]))]
    out["filled_from_T1_2"] = False
    for i, r in out[out.farms.isna()].iterrows():
        hh = h[(h.prov_file == r.prov_file) & (h.unit == r.unit)]
        if hh.empty: continue
        g = lambda rx, cats: hh[hh.ncol.str.contains(rx, regex=True) & hh.cat.isin(cats)].value.sum()
        out.loc[i, "farms"] = g(r"^NUMBEROFFARMS\|TOTAL$", ["TOTAL"])
        out.loc[i, "farm_area_ac"] = g(r"^FARMAREA\|TOTAL$", ["TOTAL"]) * 2.47105
        out.loc[i, "cultivated_area_ac"] = g(r"^CULTIVATEDAREA\|TOTAL$", ["TOTAL"]) * 2.47105
        out.loc[i, "filled_from_T1_2"] = True
        print("filled from T1.2:", r.prov_file, r.unit, round(out.loc[i, "farms"]), file=sys.stderr)
    # derived shares
    o = out
    o["avg_farm_size_ac"] = o.farm_area_ac / o.farms
    o["share_farms_under_2_5ac"] = (o.farms_under1 + o.farms_1_to_2_5) / o.farms
    o["share_area_25ac_plus"] = o.farm_area_25_plus_ac / o.farm_area_ac
    o["share_farms_tenant"] = o.farms_tenant / o.farms
    o["share_farms_owner"] = o.farms_owner / o.farms
    o["share_farms_female"] = o.farms_female / o.farms
    o["share_farms_fragmented"] = o.farms_fragmented / o.farms
    o["share_cultivated"] = o.cultivated_area_ac / o.farm_area_ac
    o["share_irrigated"] = o.irrigated_area_ac / o.cultivated_area_ac
    o["share_irrig_tubewell_only"] = o.irrig_tubewell_only_ac / o.irrigated_area_ac
    o["share_wheat_of_cropped"] = o.wheat_area_ac / o.cropped_area_ac
    o["cattle_buffalo_per_farm"] = (o.cattle + o.buffaloes) / o.farms
    o["tractors_per_1000_farms"] = 1000 * o.tractors / o.farms
    o["cultivated_ac_per_tractor"] = o.cultivated_area_ac / o.tractors
    o["share_tubewells_solar"] = o.tubewells_solar / o.tubewells
    o["district_name"] = o.unit.str.replace(r"\s+DISTRICT$", "", regex=True).str.title()
    o = o.sort_values(["prov_file", "unit_level", "unit"])
    o.to_csv(os.path.join(DER, "agri_census_2024_district_headline.csv"), index=False)
    pd.DataFrame(matchlog, columns=["indicator", "prov_file", "matched_columns"]).to_csv(os.path.join(DER, "headline_column_matches.csv"), index=False)
    print(o.unit_level.value_counts().to_dict())
    print(o[o.unit_level == "district"][["prov_file", "district_name", "farms", "avg_farm_size_ac", "share_irrigated", "cropping_intensity_pct", "goats", "tractors", "share_tubewells_solar"]].head(8).to_string(index=False))

if __name__ == "__main__":
    main()
