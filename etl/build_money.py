#!/usr/bin/env python3
"""
Build the payload for app/money.html — the Money & Prices page.

Design note
-----------
This emits a *slim* generated bundle (app/data/money_data.js, window.DD_MONEY),
following the poverty_data.js convention rather than the 2.87 MB econ_data.js
one. Two things it deliberately does NOT do:

  * It does not pull the whole SBP warehouse. Of 1,336 series only ~60 are
    charted, so the page ships those and nothing else.
  * It does not read parquet in the browser. query.html loads DuckDB-WASM from
    jsDelivr for that, which is several MB of engine before a single pixel is
    drawn — fine for a SQL console, absurd for a line chart. A plain <script>
    payload also keeps the page working under file://, like every other
    chart page here.

Daily series (KIBOR) are thinned to month-end before shipping: the page draws a
20-year curve where 5,516 daily points per tenor would be invisible detail at
~1,200px wide. The SQL console still has the full daily data.

Input : <icloud>/data_darbar_warehouse/sbp_{series_catalog,observations}.parquet
Output: app/data/money_data.js

Usage : python3 etl/build_money.py [--src /path/to/data_darbar_warehouse]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "app" / "data" / "money_data.js"

DEFAULT_SRC = Path(os.path.expanduser(
    "~/Library/Mobile Documents/com~apple~CloudDocs/Data Darbar/data_darbar_warehouse"))

P = "TS_GP_"

# ── series selection ────────────────────────────────────────────────────────
# key -> (series_key, short label). Everything the five topics draw.
SERIES = {
    # the rupee
    "usd":        (f"{P}ER_FAERPKR_M.E00220",     "PKR per US$"),
    "reer":       (f"{P}ER_REERNEER_M.R00010",    "REER"),
    "neer":       (f"{P}ER_REERNEER_M.N00010",    "NEER"),
    # prices (CPI year-on-year) -- every cut, so the page can offer selection
    "cpi_nat":    (f"{P}PT_CPI_M.P00011516",      "National"),
    "cpi_urb":    (f"{P}PT_CPI_M.P00021516",      "Urban"),
    "cpi_rur":    (f"{P}PT_CPI_M.P00031516",      "Rural"),
    "cpi_urbf":   (f"{P}PT_CPI_M.P00041516",      "Urban food"),
    "cpi_rurf":   (f"{P}PT_CPI_M.P00051516",      "Rural food"),
    "cpi_urbnf":  (f"{P}PT_CPI_M.P00061516",      "Urban non-food"),
    "cpi_rurnf":  (f"{P}PT_CPI_M.P00071516",      "Rural non-food"),
    "cpi_urbc":   (f"{P}PT_CPI_M.P00121516",      "Urban core (NFNE)"),
    "cpi_rurc":   (f"{P}PT_CPI_M.P00131516",      "Rural core (NFNE)"),
    "spi":        (f"{P}PT_CPI_M.P00111516",      "SPI (sensitive prices)"),
    "wpi":        (f"{P}PT_CPI_M.P00081516",      "WPI (wholesale)"),
    # policy corridor
    "pol_target": (f"{P}IR_SIRPR_AH.SBPOL0030",   "Policy (target) rate"),
    "pol_rev":    (f"{P}IR_SIRPR_AH.SBPOL0010",   "Reverse repo"),
    "pol_repo":   (f"{P}IR_SIRPR_AH.SBPOL0020",   "Repo"),
    # KIBOR curve (offer), every tenor
    "kib_1w":     (f"{P}BAM_SIRKIBOR_D.1KIBOR1W", "1 week"),
    "kib_2w":     (f"{P}BAM_SIRKIBOR_D.2KIBOR2W", "2 weeks"),
    "kib_1m":     (f"{P}BAM_SIRKIBOR_D.KIBOR0010", "1 month"),
    "kib_3m":     (f"{P}BAM_SIRKIBOR_D.KIBOR0020", "3 months"),
    "kib_6m":     (f"{P}BAM_SIRKIBOR_D.KIBOR0030", "6 months"),
    "kib_9m":     (f"{P}BAM_SIRKIBOR_D.6KIBOR9M",  "9 months"),
    "kib_1y":     (f"{P}BAM_SIRKIBOR_D.7KIBOR12M", "1 year"),
    "kib_2y":     (f"{P}BAM_SIRKIBOR_D.8KIBOR2Y",  "2 years"),
    "kib_3y":     (f"{P}BAM_SIRKIBOR_D.9KIBOR3Y",  "3 years"),
    # Bank rates on OUTSTANDING stock, which is what the lending-deposit spread
    # is conventionally read off. Note 65 of the 145 WALDR series are published
    # by SBP with no values at all -- including the plain "All banks" headline
    # ones -- so these "incl. 0 Markup incl. Interbank" variants are the
    # broadest cuts that actually carry data.
    "lend":       (f"{P}BAM_WALDR_M.WALD01270000", "Lending (outstanding loans)"),
    "depo":       (f"{P}BAM_WALDR_M.WALD01410000", "Deposits (outstanding)"),
    # external: reserves
    "res_sbp":    (f"{P}EXT_PAKRES_M.Z00020",     "SBP reserves"),
    "res_banks":  (f"{P}EXT_PAKRES_M.Z00040",     "Bank reserves"),
    "res_gold":   (f"{P}EXT_PAKRES_M.Z00010",     "Gold"),
    "res_imf":    (f"{P}EXT_PAKRES_M.Z00015",     "IMF reserve position"),
    # external: the current account, from the BPM6 monthly summary. These are the
    # building blocks of the "dollars in / dollars out" treemap, and they satisfy
    # credits - debits = current account balance exactly (verified FY2024-25:
    # 82,698 - 80,860 = 1,838), which the build asserts below.
    "gx":         (f"{P}BOP_BPM6SUM_M.P00030",    "Goods exports"),
    "gm":         (f"{P}BOP_BPM6SUM_M.P00040",    "Goods imports"),
    "sx":         (f"{P}BOP_BPM6SUM_M.P00060",    "Services exports"),
    "sm":         (f"{P}BOP_BPM6SUM_M.P00070",    "Services imports"),
    "pic":        (f"{P}BOP_BPM6SUM_M.P00100",    "Income received"),
    "pid":        (f"{P}BOP_BPM6SUM_M.P00110",    "Income paid (profits, interest)"),
    "sic":        (f"{P}BOP_BPM6SUM_M.P00140",    "Transfers received"),
    "remit_bop":  (f"{P}BOP_BPM6SUM_M.P00190",    "Workers' remittances"),
    "sid":        (f"{P}BOP_BPM6SUM_M.P00220",    "Transfers paid"),
    "ca":         (f"{P}BOP_BPM6SUM_M.P00010",    "Current account balance"),
    "rem":        (f"{P}BOP_WR_M.WR0010",         "Workers' remittances"),
    # money & banks
    "m1":         (f"{P}BAM_M3_M.MA3001300",      "M1"),
    "m2":         (f"{P}BAM_M3_M.MA3001700",      "M2"),
    "m3":         (f"{P}BAM_M3_M.MA3002100",      "M3"),
    "notes":      (f"{P}BAM_M3_M.MA3001100",      "Notes in circulation"),
    "npl_ratio":  (f"{P}MFS_SGADVNPL_Q.TOTAL30",  "NPLs / gross advances"),
    "npl_level":  (f"{P}MFS_SGADVNPL_Q.TOTAL20",  "Non-performing loans"),
}

# Remittances: the hierarchy-safe partition. Summing every country series
# overstates the total by ~42% because U.A.E. contains Dubai/Abu Dhabi/Sharjah,
# "Other GCC" contains Bahrain/Kuwait/Oman/Qatar, and "ten European Countries"
# contains Belgium..Sweden. See catalog.json for the full note.
REMIT_PARTITION = [
    "Saudi Arabia", "U.A.E.", "U.K.", "U.S.A.",
    "Other GCC Countries excluding Saudi Arabia & U.A.E.",
    "ten European Countries", "Norway", "Switzerland", "Australia", "Canada",
    "Japan", "Malaysia", "South Africa", "South Korea", "Other Countries",
]

# Daily series are thinned to month-end; anything else is kept whole.
THIN_TO_MONTH_END = {k for k in SERIES if k.startswith("kib_")}


def fetch_series(con, key: str) -> list:
    rows = con.execute(
        "SELECT strftime(obs_date, '%Y-%m-%d'), value FROM sbp_observations "
        "WHERE series_key = ? AND value IS NOT NULL ORDER BY obs_date", [key]).fetchall()
    return [[d, v] for d, v in rows]


def month_end_only(points: list) -> list:
    """Keep the last observation of each month (daily -> monthly)."""
    keep = {}
    for d, v in points:
        keep[d[:7]] = [d, v]
    return [keep[m] for m in sorted(keep)]


# ── drill-down (treemap zoom) ───────────────────────────────────────────────
# Every "dollars in / dollars out" tile can be broken down one or more levels:
#   goods      -> commodity groups -> commodities (up to 4 levels, SBP XRECCG/MRECCG)
#   services   -> BPM6 service types (XMGS)
#   remittances-> source country (WR, hierarchy-safe partition)
#   transfers  -> official / other personal / other current (BPM6SUM)
#   income paid-> repatriated profits by sector (REPATSEC) + interest & other
# Values are fiscal-year totals in million USD, aligned to drill["fys"].

FY_LBL = lambda fy0: f"{fy0}-{str(fy0 + 1)[2:]}"


def fy_totals(con, dataset: str, fys: list, scale: float = 1.0) -> dict:
    """{suffix: (name, [total per FY or None])} for every series in a dataset."""
    rows = con.execute(f"""
        SELECT c.series_key, c.series_name,
               CASE WHEN month(o.obs_date) >= 7 THEN year(o.obs_date) ELSE year(o.obs_date) - 1 END AS fy0,
               sum(o.value)
        FROM sbp_observations o JOIN sbp_series_catalog c USING (series_key)
        WHERE c.dataset_code = ? AND o.value IS NOT NULL GROUP BY 1, 2, 3""", [dataset]).fetchall()
    out = {}
    for key, name, fy0, v in rows:
        suf = key.split(".")[1]
        if suf not in out:
            out[suf] = (name, [None] * len(fys))
        lbl = FY_LBL(fy0)
        if lbl in fys:
            out[suf][1][fys.index(lbl)] = v * scale
    return out


def bridge_totals(src: Path, name: str, dataset: str, fys: list, scale: float) -> dict | None:
    """Same shape as fy_totals, from sbp_state/raw/fytot__<name>.json — FY totals
    exported straight from the browser for datasets whose monthly series have not
    been loaded into the parquet yet (build_sbp.py observations --tier granular
    makes this file redundant)."""
    p = src / "sbp_state" / "raw" / f"fytot__{name}.json"
    if not p.exists():
        return None
    b = json.loads(p.read_text())
    ds = b["datasets"].get(dataset)
    if not ds:
        return None
    out = {}
    for suf, (nm, vals) in ds["series"].items():
        arr = [None] * len(fys)
        for f, v in zip(b["fys"], vals):
            if f in fys:
                arr[fys.index(f)] = v * scale
        out[suf] = (nm, arr)
    return out


def vsum(*arrs):
    return [None if any(a[i] is None for a in arrs) else sum(a[i] for a in arrs)
            for i in range(len(arrs[0]))]


def close(a, b, tol):
    return all((x is None and y is None) or (x is not None and y is not None and abs(x - y) <= tol)
               for x, y in zip(a, b))


def infer_tree(keys: list, S: dict, tol: float) -> list:
    """Recover the commodity hierarchy from the numbers alone.

    SBP lists groups and their members in document order with no level marker:
    "Food Group, Rice, Basmati rice, rice Other than Basmati, Fish, ...". A series
    is a parent when the top-level items that follow it sum to it in every fiscal
    year. Recursive descent with memoisation (parse(i) depends only on i; without
    the cache it is exponential). Verified against the names for all 147 goods
    series: Transport Group > Road Motor Vehicles > CKD > Motor Cars (CKD)."""
    import functools
    n = len(keys)

    @functools.lru_cache(None)
    def parse(i):
        name, tgt = S[keys[i]]
        ch, j, acc = [], i + 1, [0.0] * len(tgt)
        while j < n:
            child, j2 = parse(j)
            cv = S[child["k"]][1]
            acc2 = [None if (a is None or b is None) else a + b for a, b in zip(acc, cv)]
            t = tol * (len(ch) + 2)
            if all(a is None or tg is None or abs(a - tg) <= t for a, tg in zip(acc2, tgt)) \
                    and any(a is not None for a in acc2):
                return {"k": keys[i], "n": name, "ch": ch + [child]}, j2
            if any(a is not None and tg is not None and a > tg + t for a, tg in zip(acc2, tgt)):
                break
            ch, acc, j = ch + [child], acc2, j2
        return {"k": keys[i], "n": name, "ch": []}, i + 1

    out, i = [], 0
    while i < n:
        nd, i = parse(i)
        out.append(nd)
    return out


def with_values(nodes, S, r=1):
    """attach rounded values; drop the internal key from the payload"""
    return [{"n": nd["n"], "v": [None if v is None else round(v, r) for v in S[nd["k"]][1]],
             **({"ch": with_values(nd["ch"], S, r)} if nd["ch"] else {})} for nd in nodes]


def tidy(name: str) -> str:
    fixes = {"Vegetablees/Leguminous Vegetablees": "Vegetables", "Jewellary": "Jewellery",
             "Machi.nery": "Machinery", "Instr.ument": "Instruments", "Mach.": "Machinery",
             "Equip..": "Equipment", "Madeup Articles(incl.Other Tex)": "Made-up articles",
             "Art;Silk & Synthetic Textile": "Art silk & synthetic textiles",
             "Tents;Canvas & Tarpaulin": "Tents, canvas & tarpaulin", "Carpets;Rugs & Mats": "Carpets, rugs & mats",
             "Oil Seeds; Nuts and Kernels": "Oil seeds, nuts & kernels", "Buses;Trucks & Oth. Heavy Vehicle": "Buses, trucks & heavy vehicles",
             "Aircrafts; Ships and Boats": "Aircraft, ships & boats", "Natural Gas; Liquified": "LNG",
             "Petroleum Gas; Liquified": "LPG", "Agri. & Other Chemical": "Agricultural & other chemicals",
             "Rubber Crude Incl. Synth/Reclaim": "Crude rubber", "Paper & Paper Board & Manf.": "Paper & paperboard",
             "Milk and Cream including for Infants": "Milk & cream", "Office Machinery Incl. Data Pros. Equipment": "Office & data-processing equipment",
             "Completely Built Unit (CBU)": "Built-up vehicles (CBU)", "Completely Knock Down (CKD)": "Kits for local assembly (CKD)"}
    for a, b in fixes.items():
        name = name.replace(a, b)
    return name.replace(";", ",").strip()


def goods_drill(S: dict, side: str, bop: list, fys: list) -> dict:
    """side 'EXP' or 'IMP'. Returns the drill node for goods exports/imports and
    checks the accounting chain: groups == through banks; banks - freight == fob;
    fob + other == BOP total == the BPM6 headline the top-level tile draws."""
    tol = 0.01  # million USD; the bridge is rounded to the thousand
    keys = list(S)
    for k in keys:
        S[k] = (tidy(S[k][0]), S[k][1])
    banks = next(k for k in keys if "hrough Banks" in S[k][0])
    i = keys.index(banks)
    freight, fob, other, total = keys[i + 1], keys[i + 2], keys[i + 3], keys[i + 4]
    tree = infer_tree(keys[:i], S, tol)
    top = vsum(*[S[nd["k"]][1] for nd in tree])
    if not close(top, S[banks][1], 0.5 * len(tree)):
        raise SystemExit(f"{side}: commodity groups do not sum to receipts through banks")
    if not close(vsum(S[banks][1], [-v if v is not None else None for v in S[freight][1]]), S[fob][1], 1.0):
        raise SystemExit(f"{side}: banks - freight != fob")
    if not close(vsum(S[fob][1], S[other][1]), S[total][1], 1.0):
        raise SystemExit(f"{side}: fob + other != BOP total")
    if not close(S[total][1], bop, 1.5):
        raise SystemExit(f"{side}: BOP total from the commodity table != BPM6 headline\n  {S[total][1]}\n  {bop}")
    if len(tree) < 4 or sum(len(nd["ch"]) for nd in tree) < 20:
        raise SystemExit(f"{side}: hierarchy inference found too little structure — check the bridge")
    what = "receipts" if side == "EXP" else "payments"
    other_n = "Other exports (land-borne, samples, rebates)" if side == "EXP" else "Other imports (not through banks)"
    return {"ch": with_values(tree, S) + [{"n": other_n, "v": [None if v is None else round(v, 1) for v in S[other][1]]}],
            "basis": f"{what} through banks plus {other_n.split(' (')[0].lower()}",
            "adj": {"n": "Freight" + (" & insurance" if side == "IMP" else ""),
                    "v": [None if v is None else round(v, 1) for v in S[freight][1]]}}


def build_drill(con, src: Path, out: dict, fys: list) -> dict:
    drill = {"fys": fys, "nodes": {}}
    fyv = lambda k: [round(sum(v for d, v in out["series"][k] if fyOf(d) == f), 3) if any(fyOf(d) == f for d, v in out["series"][k]) else None
                     for f in fys]

    # goods: parquet if the granular tier has been loaded, else the browser bridge
    for side, ds, k in (("EXP", "BOP_XRECCG_M", "gx"), ("IMP", "BOP_MRECCG_M", "gm")):
        S = fy_totals(con, f"{P}{ds}", fys, 1e-3)
        S = S if S else bridge_totals(src, "goods", ds, fys, 1e-3)
        if not S:
            print(f"  note: no {ds} data (parquet or bridge) — goods drill-down skipped")
            continue
        drill["nodes"][k] = goods_drill(S, side, fyv(k), fys)

    # services by type
    X = fy_totals(con, f"{P}BOP_XMGS_M", fys)
    if X:
        def svc(lo, hi, total_suf, k, prefix):
            items = [(s, X[s]) for s in sorted(X) if lo <= s <= hi and "of which" not in X[s][0]]
            tot = vsum(*[v for s, (n, v) in items])
            if not close(tot, X[total_suf][1], 1.0) or not close(X[total_suf][1], fyv(k), 1.0):
                raise SystemExit(f"services {k}: types do not sum to the total")
            drill["nodes"][k] = {"ch": [{"n": n.split("-", 1)[1].replace("n.i.e.", "").replace("; ", ", ").strip(),
                                         "v": [None if x is None else round(x, 1) for x in v]}
                                        for s, (n, v) in items if any(x for x in v)]}
        svc("P00040", "P00160", "P00030", "sx", "Exports of Services-")
        svc("P00200", "P00310", "P00190", "sm", "Imports of Services-")

    # transfers: remittances by source; other transfers by kind
    B = fy_totals(con, f"{P}BOP_BPM6SUM_M", fys)
    if B:
        parts = vsum(B["P00150"][1], B["P00190"][1], B["P00200"][1], B["P00210"][1])
        if not close(parts, B["P00140"][1], 1.5):
            raise SystemExit("secondary income credit components do not sum to the total")
        rem = {}
        for s, f, v in out["remit"]:
            rem.setdefault(s, [None] * len(fys))
            if f in fys:
                rem[s][fys.index(f)] = v
        rem_tot = vsum(*rem.values())
        # WR (cash remittances through banks) vs the BPM6 line: same concept, tiny
        # timing/coverage differences; refuse anything beyond 2%
        for a, b in zip(rem_tot, B["P00190"][1]):
            if a is not None and b is not None and abs(a - b) > 0.02 * b:
                raise SystemExit(f"remittances by source ({a:,.0f}) differ from the BPM6 line ({b:,.0f})")
        drill["nodes"]["remit_bop"] = {"ch": [{"n": s.replace("Other GCC Countries excluding Saudi Arabia & U.A.E.", "Other GCC")
                                              .replace("ten European Countries", "EU (ten countries)"), "v": v}
                                             for s, v in rem.items()],
                                       "basis": "cash remittances received through banks (SBP WR table)"}
        r1 = lambda arr: [None if x is None else round(x, 1) for x in arr]
        drill["nodes"]["sic_other"] = {"ch": [
            {"n": "Official transfers", "v": r1(B["P00150"][1]), "ch": [
                {"n": "Current international cooperation", "v": r1(B["P00160"][1])},
                {"n": "Other official transfers", "v": r1(B["P00170"][1])}]},
            {"n": "Other personal transfers", "v": r1(B["P00200"][1])},
            {"n": "Other current transfers", "v": r1(B["P00210"][1])}]}

    # income paid abroad: repatriated profits by sector + interest & other
    R = fy_totals(con, f"{P}FI_REPATSEC_M", fys)
    R = R if R else bridge_totals(src, "repat", "FI_REPATSEC_M", fys, 1.0)
    if R and "PRS0010" in R:
        # FI (= FDI + FPI) per sector: every third series from PRS0040. The sector
        # list is nested like the goods tables (Power = Thermal + Hydel + Coal,
        # Communications > IT > Software/Hardware/IT Service): summing all 50
        # overstates the total by 28%, so the hierarchy is inferred the same way.
        keys = [s for s in sorted(R) if s.startswith("PRS") and s >= "PRS0040" and (int(s[3:]) - 40) % 30 == 0]
        S = {s: (R[s][0].split(" by ", 1)[-1].replace(" Sector", "").replace("Transport Equipment(Automobiles)", "Automobiles")
                 .replace("Buses;Trucks;Vans & Trail", "Buses, trucks & vans").strip(), R[s][1]) for s in keys}
        tree = infer_tree(keys, S, 0.06)
        tot = vsum(*[S[nd["k"]][1] for nd in tree])
        if not close(tot, R["PRS0010"][1], 0.06 * (len(tree) + 1)):
            raise SystemExit("repatriation by sector does not sum to the published total\n"
                             f"  {[round(x, 1) if x is not None else None for x in tot]}\n  {R['PRS0010'][1]}")
        pid = fyv("pid")
        resid = [None if (a is None or b is None) else a - b for a, b in zip(pid, R["PRS0010"][1])]
        if any(v is not None and v < -1 for v in resid):
            raise SystemExit("repatriated profits exceed primary income debit — hierarchy is wrong")
        drill["nodes"]["pid"] = {"ch": [
            {"n": "Interest & other investment income", "v": [None if v is None else round(v, 1) for v in resid]},
            {"n": "Repatriated profits & dividends", "v": [None if v is None else round(v, 1) for v in R["PRS0010"][1]],
             "ch": with_values(tree, S)}],
            "basis": "interest & other is the BPM6 primary-income debit less SBP's repatriation table"}
    else:
        print("  note: no FI_REPATSEC_M data (parquet or bridge) — income-paid drill-down skipped")
    return drill


def fyOf(d: str) -> str:
    y, m = int(d[:4]), int(d[5:7])
    return FY_LBL(y if m >= 7 else y - 1)


def build(src: Path) -> None:
    con = duckdb.connect()
    for t in ("sbp_series_catalog", "sbp_observations"):
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM read_parquet('{(src / (t + '.parquet')).as_posix()}')")

    meta = {r[0]: {"name": r[1], "unit": r[2], "freq": r[3]} for r in con.execute(
        "SELECT series_key, series_name, unit, frequency FROM sbp_series_catalog").fetchall()}

    out = {"series": {}, "meta": {}}
    missing, empty = [], []
    for k, (skey, label) in SERIES.items():
        pts = fetch_series(con, skey)
        if not pts:
            # distinguish "no such series" from "series exists but SBP publishes
            # no values for it" -- 65 of the 145 WALDR series are the latter, and
            # conflating them sends you hunting for a typo that isn't there
            n = con.execute("SELECT count(*) FROM sbp_observations WHERE series_key = ?",
                            [skey]).fetchone()[0]
            (empty if n else missing).append((k, skey))
            continue
        if k in THIN_TO_MONTH_END:
            pts = month_end_only(pts)
        out["series"][k] = pts
        m = meta.get(skey, {})
        out["meta"][k] = {"label": label, "key": skey,
                          "unit": m.get("unit"), "freq": m.get("freq"),
                          "name": m.get("name")}

    # remittances by source, fiscal-year totals, hierarchy-safe
    like = " OR ".join(["c.series_name = 'Workers'' remittances received from ' || ?"] * len(REMIT_PARTITION))
    rem = con.execute(f"""
        SELECT replace(c.series_name, 'Workers'' remittances received from ', '') AS src,
               CASE WHEN month(o.obs_date) >= 7 THEN year(o.obs_date) ELSE year(o.obs_date) - 1 END AS fy0,
               sum(o.value) AS v
        FROM sbp_observations o JOIN sbp_series_catalog c USING (series_key)
        WHERE c.dataset_code = '{P}BOP_WR_M' AND ({like})
        GROUP BY 1, 2 HAVING sum(o.value) IS NOT NULL ORDER BY 2, 3 DESC
    """, REMIT_PARTITION).fetchall()
    out["remit"] = [[s, f"{fy}-{str(fy + 1)[2:]}", round(v, 1)] for s, fy, v in rem]

    # How many months each fiscal year actually covers. The newest FY is nearly
    # always partial (the data ends mid-year), and a one-month total shown as a
    # year is badly misleading -- the page uses this to default to the last
    # COMPLETE year and to mark the partial one in the picker.
    months = con.execute(f"""
        SELECT CASE WHEN month(o.obs_date) >= 7 THEN year(o.obs_date) ELSE year(o.obs_date) - 1 END AS fy0,
               count(DISTINCT date_trunc('month', o.obs_date)) AS n
        FROM sbp_observations o JOIN sbp_series_catalog c USING (series_key)
        WHERE c.series_key = '{P}BOP_WR_M.WR0010' GROUP BY 1 ORDER BY 1
    """).fetchall()
    out["remit_months"] = {f"{fy}-{str(fy + 1)[2:]}": n for fy, n in months}

    # reconciliation guard: the partition must match the published total
    tot = con.execute(f"""
        SELECT sum(o.value) FROM sbp_observations o JOIN sbp_series_catalog c USING (series_key)
        WHERE c.series_name LIKE 'Total inflow%'
          AND o.obs_date BETWEEN DATE '2024-07-01' AND DATE '2025-06-30'""").fetchone()[0]
    part = sum(v for s, fy, v in out["remit"] if fy == "2024-25")
    if abs(part - tot) > max(2.0, 0.001 * tot):
        raise SystemExit(f"remittance partition does not reconcile: {part:,.0f} vs published {tot:,.0f}")

    # Current-account identity guard for the treemap: the tiles it draws must add
    # up to the balance SBP publishes, or the picture is wrong by construction.
    fy_sum = lambda k: sum(v for d, v in out["series"][k]
                           if "2024-07-01" <= d <= "2025-06-30")
    credits = fy_sum("gx") + fy_sum("sx") + fy_sum("pic") + fy_sum("sic")
    debits = fy_sum("gm") + fy_sum("sm") + fy_sum("pid") + fy_sum("sid")
    ca_pub = fy_sum("ca")
    if abs((credits - debits) - ca_pub) > 2.0:
        raise SystemExit(f"current account does not reconcile: credits {credits:,.0f} - debits "
                         f"{debits:,.0f} = {credits - debits:,.0f}, published {ca_pub:,.0f}")
    # remittances must sit inside "transfers received", not alongside it
    if fy_sum("remit_bop") > fy_sum("sic") + 1:
        raise SystemExit("remittances exceed secondary income credit -- hierarchy is wrong")

    # month coverage per fiscal year for the BOP series (the treemap's year picker
    # must know which years are partial, same reason as remit_months)
    bop_months = con.execute(f"""
        SELECT CASE WHEN month(obs_date) >= 7 THEN year(obs_date) ELSE year(obs_date) - 1 END AS fy0,
               count(DISTINCT date_trunc('month', obs_date)) AS n
        FROM sbp_observations WHERE series_key = '{P}BOP_BPM6SUM_M.P00030' AND value IS NOT NULL
        GROUP BY 1 ORDER BY 1""").fetchall()
    out["bop_months"] = {f"{fy}-{str(fy + 1)[2:]}": n for fy, n in bop_months}

    out["drill"] = build_drill(con, src, out, list(out["bop_months"]))

    out["generated"] = con.execute("SELECT max(last_refresh) FROM sbp_series_catalog").fetchone()[0].isoformat()
    out["reconciled"] = {"remit_fy2024_25": round(part, 1), "published_total": round(tot, 1),
                         "ca_fy2024_25": {"credits": round(credits), "debits": round(debits),
                                          "balance": round(credits - debits), "published": round(ca_pub)}}

    if missing or empty:
        msg = []
        if missing:
            msg.append(f"series not in the warehouse (check the key): {missing}")
        if empty:
            msg.append(f"series exist but SBP publishes no values for them: {empty}")
        raise SystemExit("\n".join(msg))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text("/* Data Darbar - Money & Prices payload (generated by "
                   "etl/build_money.py; do not edit by hand) */\nwindow.DD_MONEY="
                   + json.dumps(out, separators=(",", ":")) + ";\n")

    npts = sum(len(v) for v in out["series"].values())
    print(f"wrote {OUT.relative_to(REPO)}")
    print(f"  {len(out['series'])} series, {npts:,} points, "
          f"{len(out['remit'])} remittance rows, {OUT.stat().st_size / 1024:.0f} KB")
    print(f"  remittance partition FY2024-25 {part:,.0f} == published {tot:,.0f}")
    cnt = lambda nd: 1 + sum(cnt(c) for c in nd.get("ch", []))
    print("  drill-down: " + ", ".join(f"{k} {cnt(v) - 1} nodes" for k, v in out["drill"]["nodes"].items()))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--src", type=Path, default=DEFAULT_SRC)
    a = ap.parse_args()
    if not (a.src / "sbp_observations.parquet").exists():
        sys.exit(f"SBP parquet not found in {a.src} — run build_sbp.py load first")
    build(a.src)
