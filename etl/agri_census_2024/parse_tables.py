#!/usr/bin/env python3
"""Parse the PBS Agricultural Census 2024 Excel tables into one long table.

Input : raw_data/pbs/agricultural_census_2024/<prov>/excel/*.xlsx  (prov = national, punjab, sindh, kpk, balochistan)
Output: raw_data/pbs/agricultural_census_2024/derived/
          agri_census_2024_long.csv.gz   one row per (file, unit, category, column) cell; table_id is 'T6.10' (string)
          tables_catalog.csv             one row per (file, table) with layout + counts
          parse_log.txt

Two layouts occur:
  block : an administrative-unit header row (name only), then category rows under it
          (farm-size classes, tenure classes, ...). Nested category headers inside a block
          (e.g. "OWNER FARMS") are kept in `group`.
  flat  : one row per administrative unit (e.g. 7.1 livestock by administrative unit).

Values are PBS's weighted estimates (non-integer), kept as floats.
"""
import glob, os, re, sys, csv, gzip
import openpyxl

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", "..", "raw_data", "pbs", "agricultural_census_2024"))
OUT = os.path.join(ROOT, "derived")
os.makedirs(OUT, exist_ok=True)

PROVS = ["national", "punjab", "sindh", "kpk", "balochistan"]
UNIT_RE = re.compile(r"(DIVISION|DISTRICT|AREA|TERRITORY|AGENCY|TRIBAL|REGION|^PAKISTAN$|^PUNJAB$|^SINDH$|KHYBER|^K\.?P\.?K?$|BALOCHISTAN|GILGIT|AZAD|KASHMIR|ISLAMABAD|FATA|^ICT$)", re.I)
PROV_RE = re.compile(r"^(PAKISTAN|PUNJAB|SINDH|KHYBER PAKHTUNKHWA|KHYBER PAKHTUNKHAWA|K\.?P\.?K?|BALOCHISTAN|GILGIT.*|AZAD.*|ISLAMABAD.*|ICT)$", re.I)
NOTE_RE = re.compile(r"^\s*\(.*\)\s*$")

def clean(s):
    if s is None: return None
    s = str(s).replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s or None

def is_num(v):
    return isinstance(v, (int, float)) and not isinstance(v, bool)

def unit_level(name):
    u = name.upper()
    if u == "PAKISTAN": return "country"
    if PROV_RE.match(u): return "province"
    if "DIVISION" in u: return "division"
    if "DISTRICT" in u: return "district"
    if "AGENCY" in u or "TRIBAL" in u: return "district"
    return "other"

def parse_sheet(ws):
    rows = [list(r) for r in ws.iter_rows(values_only=True)]
    # trim trailing empty columns
    ncol = 0
    for r in rows:
        for j, v in enumerate(r):
            if v is not None and clean(v) not in (None, ""): ncol = max(ncol, j + 1)
    rows = [r[:ncol] + [None] * (ncol - len(r)) for r in rows]
    # title row: first row containing 'TABLE'
    title, title_i = None, None
    for i, r in enumerate(rows[:8]):
        txt = " ".join(clean(v) for v in r if clean(v))
        if re.search(r"\bTABLES?\s*\.?\s*\d+(\.\d+)?\s*:?", txt, re.I):
            title, title_i = txt, i
            break
    if title_i is None:
        raise ValueError("no title row")
    # numbering row: col A == 1 and col B == 2
    num_i = None
    for i in range(title_i + 1, min(title_i + 14, len(rows))):
        r = rows[i]
        nums = [v for v in r if is_num(v)]
        if len(nums) >= 2 and nums[0] == 1 and nums[1] == 2 and all(not isinstance(v, str) or not v.strip() for v in r):
            num_i = i; break
    if num_i is None:
        raise ValueError("no numbering row")
    # header rows between title and numbering row
    notes, hdr_rows = [], []
    for r in rows[title_i + 1:num_i]:
        vals = [clean(v) for v in r]
        nonnull = [v for v in vals if v]
        if not nonnull: continue
        if len(nonnull) == 1 and NOTE_RE.match(nonnull[0]):
            notes.append(nonnull[0]); continue
        if len(nonnull) == 1 and re.match(r"^(PAKISTAN|PUNJAB|SINDH|KHYBER.*|BALOCHISTAN|GILGIT.*|AZAD.*|ISLAMABAD.*)$", nonnull[0], re.I):
            notes.append("scope:" + nonnull[0]); continue
        hdr_rows.append(vals)
    # forward-fill each header row across columns (merged cells), but a lower header row
    # only inherits leftwards within the span of the group above it.
    cols = []
    for j in range(ncol):
        parts = []
        for k, hr in enumerate(hdr_rows):
            v = hr[j]
            if v is None:
                for k0 in range(j - 1, -1, -1):
                    if any(hdr_rows[m][k0 + 1] for m in range(k)) and k0 + 1 <= j and any(hdr_rows[m][x] for m in range(k) for x in range(k0 + 1, j + 1)):
                        break
                    if hr[k0]:
                        v = hr[k0]; break
            if v and (not parts or parts[-1] != v):
                parts.append(v)
        cols.append(" | ".join(parts) if parts else f"col{j+1}")
    # data rows
    out = []
    layout = "flat"
    unit, group = None, None
    pending = []
    for i in range(num_i + 1, len(rows)):
        r = rows[i]
        a = clean(r[0])
        rest = r[1:]
        nums = [(j + 1, v) for j, v in enumerate(rest) if is_num(v)]
        texts = [(j + 1, clean(v)) for j, v in enumerate(rest) if isinstance(v, str) and clean(v)]
        if a is None and texts and any("\n" in str(v) for v in r if isinstance(v, str)):
            raw = [v for v in r if isinstance(v, str) and "\n" in v][0]
            pending = [clean(x) for x in raw.split("\n") if clean(x)]
            if not nums: continue
        if a is None and not nums: continue
        if a is None and texts and len(texts) == 1 and not pending:
            a = texts[0][1]  # label sits in a later column
        if a is None and nums:
            a = pending.pop(0) if pending else "(unlabelled)"
        if a and not nums and all(clean(v) is None for v in rest):
            if UNIT_RE.search(a):
                unit, group, layout = a, None, "block"
            else:
                group = a
            continue
        if a and not nums:
            # text-only row with other text cells: ignore (notes)
            continue
        if layout == "block" and unit is not None:
            u, cat = unit, a
        else:
            u, cat = a, None
        for j, v in nums:
            out.append((u, unit_level(u), group, cat, j + 1, cols[j] if j < ncol else f"col{j+1}", float(v)))
    return title, "; ".join(notes), cols, layout, out

def table_id(fn):
    m = re.search(r"(\d+)\.(\d+)", fn)
    return f"{int(m.group(1))}.{m.group(2)}" if m else fn

def main():
    log = open(os.path.join(OUT, "parse_log.txt"), "w")
    gz = gzip.open(os.path.join(OUT, "agri_census_2024_long.csv.gz"), "wt", newline="")
    w = csv.writer(gz)
    w.writerow(["prov_file", "table_id", "chapter", "table_no", "table_title", "notes", "unit", "unit_level", "group", "category", "col_index", "column", "value"])
    cat = open(os.path.join(OUT, "tables_catalog.csv"), "w", newline="")
    cw = csv.writer(cat)
    cw.writerow(["prov_file", "table_id", "file", "title", "notes", "layout", "n_cols", "n_units", "n_rows", "columns"])
    n_all = 0
    for prov in PROVS:
        files = sorted(glob.glob(os.path.join(ROOT, prov, "excel", "*.xls*")), key=lambda f: [float(x) for x in re.findall(r"\d+\.\d+", os.path.basename(f))[:1]] or [999])
        for f in files:
            tid = table_id(os.path.basename(f))
            try:
                wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
                ws = wb[wb.sheetnames[0]]
                if len(wb.sheetnames) > 3:
                    want = tid.replace(".", r"\s*\.\s*")
                    cands = [n for n in wb.sheetnames if re.search(r"TABLES?\s*\.?\s*" + want + r"\s*$", n, re.I)]
                    if not cands: raise ValueError("multi-sheet workbook, no sheet for " + tid)
                    ws = wb[cands[0]]
                title, notes, cols, layout, rows = parse_sheet(ws)
            except Exception as e:
                log.write(f"FAIL {prov} {os.path.basename(f)}: {e}\n"); print("FAIL", prov, os.path.basename(f), e); continue
            units = {r[0] for r in rows}
            for r in rows:
                ch, tn = tid.split(".") if "." in tid else (tid, "0")
                w.writerow([prov, "T" + tid, int(ch), int(tn), title, notes, *r])
            cw.writerow([prov, "T" + tid, os.path.basename(f), title, notes, layout, len(cols), len(units), len(rows), " || ".join(cols)])
            n_all += len(rows)
            log.write(f"ok   {prov} {tid:>5} {layout:5} units={len(units):3} rows={len(rows):6} {title[:70]}\n")
    gz.close(); cat.close(); log.close()
    print("rows:", n_all)

if __name__ == "__main__":
    main()
