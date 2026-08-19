#!/usr/bin/env python3
"""Aggregate the Mouza Census 2020 tehsil counts onto Data Darbar's ADM3
polygons and emit the window.DD_POV_MOUZA payload.

Counts are summed across every PBS tehsil that maps to a polygon, then shares
are taken from the summed numerator and summed denominator — not averaged over
tehsils, which would weight a 7-mouza sub-tehsil the same as a 597-mouza one.

Denominators follow the raw dataset's README: each exclusive block supplies its
own base, because the dashboard's blocks disagree about how many mouzas answered.
Multiple-response blocks are divided by the modal base and can exceed 100.
"""
import collections
import csv
import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
MOUZA = os.environ.get("MOUZA_CSV", os.path.join(HERE, "pk-mouza-2020-tehsil.csv"))
XW = os.path.join(HERE, "mouza2020_tehsil_crosswalk.csv")
OUT = os.path.join(HERE, "..", "..", "app", "data", "mouza_data.js")


def load_dd_tehsils():
    """Pull the tehsil records straight out of the generated poverty payload so
    this folder does not carry a second copy of them."""
    path = os.path.join(HERE, "..", "..", "app", "data", "poverty_data.js")
    s = open(path).read()
    i = s.index("window.DD_POV=") + len("window.DD_POV=")
    return json.loads(s[i:s.index("\n", i)].rstrip(";"))["tehsils"]

# key -> (numerator columns, denominator spec)
#   ("block", [cols])   share of that exclusive block's own sum
#   ("pair", other)     share of numerator / (numerator + other)
#   ("base",)           share of the modal base; multiple response, may exceed 100
E = "EducationFacility_"
H = "HealthFacility_"
W = "SourceOfDrinkingWater_"
C = "CommunityInfrastructure_"
S = "StatusTypeOfStreets_"
F = "FuelAvailability_"
N = "NaturalDisaster_"
R = "CreditSource_"

ELEC = ["ElectrictiyAvailability_AllMouzas", "ElectrictiyAvailability_MostlyMouzas",
        "ElectrictiyAvailability_SomeMouzas", "ElectrictiyAvailability_NoneMouzas"]
HOUSE = ["HousingConstruction_Bricked", "HousingConstruction_MudMade",
         "HousingConstruction_BricksAndMud", "HousingConstruction_Others"]
STATUS = ["MauzaStatus_RuralCount", "MauzaStatus_UrbanCount",
          "MauzaStatus_PartiallyUrbanCount", "MauzaStatus_ForestCount",
          "MauzaStatus_UnPopulatedCount"]

IND = {
    # settlement
    "rural":        (["MauzaStatus_RuralCount"], ("block", STATUS)),
    "urbanish":     (["MauzaStatus_UrbanCount", "MauzaStatus_PartiallyUrbanCount"], ("block", STATUS)),
    "unpop":        (["MauzaStatus_UnPopulatedCount"], ("block", STATUS)),
    # electricity
    "elec_none":    (["ElectrictiyAvailability_NoneMouzas"], ("block", ELEC)),
    "elec_partial": (["ElectrictiyAvailability_MostlyMouzas", "ElectrictiyAvailability_SomeMouzas"], ("block", ELEC)),
    "elec_all":     (["ElectrictiyAvailability_AllMouzas"], ("block", ELEC)),
    "solar":        (["AlternateEnergySource_SolarEnergy"], ("base",)),
    "alt_none":     (["AlternateEnergySource_None"], ("base",)),
    # water
    "w_piped":      ([W + "GovtPipedSupply"], ("base",)),
    "w_pump":       ([W + "PersonalPump", W + "NearbyPump"], ("base",)),
    "w_well":       ([W + "Well"], ("base",)),
    "w_surface":    ([W + "RiverCanal", W + "PondToba"], ("base",)),
    "w_karez":      ([W + "SpringRavineKarez"], ("base",)),
    "w_treated":    ([W + "WaterFiltrationPlant", W + "ROPlant"], ("base",)),
    # streets
    "st_dirt":      ([S + "DirtRoad"], ("base",)),
    "st_metaled":   ([S + "Metaled"], ("base",)),
    "st_paved":     ([S + "Cemented", S + "Bricked"], ("base",)),
    # housing
    "h_bricked":    (["HousingConstruction_Bricked"], ("block", HOUSE)),
    "h_mud":        (["HousingConstruction_MudMade"], ("block", HOUSE)),
    # education
    "sch_pri_m":    ([E + "MalePrimaryExistanceCount"], ("pair", E + "MalePrimaryNotExistanceCount")),
    "sch_pri_f":    ([E + "FemalePrimaryExistanceCount"], ("pair", E + "FemalePrimaryNotExistanceCount")),
    "sch_mid_m":    ([E + "MaleMiddleExistanceCount"], ("pair", E + "MaleMiddleNotExistanceCount")),
    "sch_mid_f":    ([E + "FemaleMiddleExistanceCount"], ("pair", E + "FemaleMiddleNotExistanceCount")),
    "sch_high_m":   ([E + "MaleHHSchoolExistanceCount"], ("pair", E + "MaleHHSchoolNotExistanceCount")),
    "sch_high_f":   ([E + "FemaleHHSchoolExistanceCount"], ("pair", E + "FemaleHHSchoolNotExistanceCount")),
    "col_m":        ([E + "MaleCollegeExistanceCount"], ("pair", E + "MaleCollegeNotExistanceCount")),
    "col_f":        ([E + "FemaleCollegeExistanceCount"], ("pair", E + "FemaleCollegeNotExistanceCount")),
    "madrasa":      ([E + "MaleDeniMudarsaExistanceCount"], ("pair", E + "MaleDeniMudarsaNotExistanceCount")),
    # health
    "hf_bhu":       ([H + "BHU"], ("base",)),
    "hf_rhc":       ([H + "RHC"], ("base",)),
    "hf_hosp":      ([H + "HospitalDispensary"], ("base",)),
    "hf_private":   ([H + "PrivateDoctorMBBS"], ("base",)),
    "hf_midwife":   ([H + "FacilityOfMidwife"], ("base",)),
    "hf_mch":       ([H + "ChildMotherCareCentre"], ("base",)),
    # connectivity
    "road_metaled": (["MetaledRoadsCount"], ("base",)),
    "transport":    ([C + "TransportFacilityAvailableCount"], ("pair", C + "TransportFacilityNotAvailableCount")),
    "mobile":       ([C + "TelephoneFacility_MobileSignalsAvailableCount"], ("pair", C + "TelephoneFacility_MobileSignalsNotAvailableCount")),
    "net_mobile":   ([C + "InternetFacility_MobileNetAvailableCount"], ("pair", C + "InternetFacility_MobileNetNotAvailableCount")),
    "net_dsl":      ([C + "InternetFacility_FixedlineDSLAvailableCount"], ("pair", C + "InternetFacility_FixedlineDSLNotAvailableCount")),
    "post":         ([R + "PostOfficeExistance"], ("pair", R + "PostOfficeNotExistance")),
    "police":       ([R + "PoliceStationExistance"], ("pair", R + "PoliceStationNotExistance")),
    # fuel
    "fuel_gas":     ([F + "SuiGas"], ("base",)),
    "fuel_lpg":     ([F + "LPG"], ("base",)),
    "fuel_wood":    ([F + "Wood"], ("base",)),
    "fuel_dung":    ([F + "AnimalDungCake"], ("base",)),
    # economy
    "bazar":        (["BazarAvailabilityCount"], ("base",)),
    "mkt_grain":    (["WholesaleMarket_GrainsCount"], ("base",)),
    "bank_online":  ([R + "OnlineCommercialBankExistance"], ("pair", R + "OnlineCommercialBankNotExistance")),
    "credit_mfi":   ([R + "MicrofinanceBank"], ("base",)),
    "ind_none":     (["IndustryAndSourceOfEmployment_None"], ("base",)),
    # disasters
    "dis_any":      ([N + "ExistanceCount"], ("pair", N + "NotExistanceCount")),
    "dis_flood":    ([N + "FloodCount"], ("base",)),
    "dis_drought":  ([N + "DroughtCount"], ("base",)),
}

PAIR_BLOCKS = [
    (E + "MalePrimaryExistanceCount", E + "MalePrimaryNotExistanceCount"),
    (E + "FemaleCollegeExistanceCount", E + "FemaleCollegeNotExistanceCount"),
    (C + "TransportFacilityAvailableCount", C + "TransportFacilityNotAvailableCount"),
    (R + "PostOfficeExistance", R + "PostOfficeNotExistance"),
]


def n(r, c):
    v = r.get(c, "")
    return int(v) if v not in ("", None) else 0


def main():
    xw = {r["tehsil_code"]: r for r in csv.DictReader(open(XW))}
    rows = list(csv.DictReader(open(MOUZA)))
    teh = load_dd_tehsils()

    groups = collections.defaultdict(list)
    approx = collections.defaultdict(bool)
    for r in rows:
        x = xw.get(r["tehsil_code"])
        if not x or not x["dd_id"]:
            continue
        groups[x["dd_id"]].append(r)
        if x["match"] == "approx":
            approx[x["dd_id"]] = True

    payload = {}
    for dd_id, rs in groups.items():
        tot = sum(n(r, "TotalMauzaCount") for r in rs)
        rec = {"m": tot, "np": len(rs)}
        if approx[dd_id]:
            rec["apx"] = 1
        if tot == 0:
            rec["cov"] = 0
            payload[dd_id] = rec
            continue
        rec["cov"] = 1
        rec["rp"] = sum(n(r, "RuralPopulatedMouzaCount") for r in rs)

        # modal base for multiple-response blocks, aggregated
        bases = []
        for cols in (ELEC, HOUSE):
            bases.append(sum(n(r, c) for r in rs for c in cols))
        for a, b in PAIR_BLOCKS:
            bases.append(sum(n(r, a) + n(r, b) for r in rs))
        bases = [b for b in bases if b]
        base = collections.Counter(bases).most_common(1)[0][0] if bases else tot
        rec["base"] = base
        rec["bsp"] = round((max(bases) - min(bases)) / base * 100, 1) if bases and base else 0

        for key, (nums, den) in IND.items():
            num = sum(n(r, c) for r in rs for c in nums)
            if den[0] == "block":
                d = sum(n(r, c) for r in rs for c in den[1])
            elif den[0] == "pair":
                d = num + sum(n(r, den[1]) for r in rs)
            else:
                d = base
            rec[key] = round(num / d * 100, 1) if d else None

        # water table depth, mouza-weighted mean of the tehsil averages
        num = sum(n(r, "AvgDepthOfWater") * n(r, "TotalMauzaCount") for r in rs)
        rec["wdepth"] = round(num / tot, 0) if tot else None
        payload[dd_id] = rec

    # polygons with no PBS tehsil at all: the Mouza Census frame does not reach them
    for k in teh:
        payload.setdefault(k, {"cov": 0, "m": 0, "np": 0})

    with open(OUT, "w") as f:
        f.write("/* Data Darbar - Mouza Census 2020 rural-facility payload "
                "(generated by etl/mouza2020; do not edit by hand) */\n")
        f.write("window.DD_POV_MOUZA=")
        json.dump(payload, f, separators=(",", ":"))
        f.write(";\n")
    covered = sum(1 for v in payload.values() if v.get("cov"))
    print(f"polygons: {len(payload)}  with data: {covered}  indicators: {len(IND)}")
    print(f"payload bytes: {len(open(OUT).read()):,}")
    print("mouzas carried:", sum(v.get("m", 0) for v in payload.values()))


if __name__ == "__main__":
    main()
