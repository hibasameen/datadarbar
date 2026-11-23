#!/usr/bin/env python3
"""Agricultural Census 2010 (PBS / Agricultural Census Organization) -> long CSV.

Input : the province tabulation PDFs (text layer) in raw_data/pbs/agricultural_census_2010/
Output: derived/agri_census_2010_long.csv.gz   one row per printed cell
        derived/tables_catalog.csv             table x file: title, unit note, columns, header guess
        derived/parse_log.txt                  rows whose value count != printed column count, etc.

Usage: python3 parse_tables.py <raw_dir> <out_dir>      (needs poppler's pdftotext)
"""
import csv, gzip, re, subprocess, sys, collections
from pathlib import Path

FILES = {  # file -> (scope, area unit of the file)
    "Tabulation-TABLES-of-Punjab-Province.pdf": "PUNJAB",
    "Tabulation-Sindh-Province.pdf": "SINDH",
    "Tabulation-of-KPK-Province.pdf": "KHYBER PAKHTUNKHWA",
    "Tables-Pakistan-In-Hectares.pdf": "PAKISTAN (hectares file)",
}
VAL = re.compile(r"^(?:-|\*+|\+|X|x|-?\d[\d,]*(?:\.\d+)?(?:E\+\d+)?|\.\d+)$")
TABLE = re.compile(r"^\s*TABLE\s*:?\s+(\d+\.\d+)\s*(.*)$")
PROVS = {"PUNJAB", "SINDH", "KHYBER PAKHTUNKHWA", "BALOCHISTAN", "PAKISTAN"}
UNIT = re.compile(r"^(?:.*(PROVINCE|DISTRICT|AGENCY|DIVISION)(?:\s*\(.*\))?|F\.\s?R\.?\s?.+|PAKISTAN|PUNJAB|SINDH|KHYBER PAKHTUNKHWA|BALOCHISTAN|ISLAMABAD)$")
AREA = re.compile(r"\((?:AREA|QUANTITY|VALUE|AMOUNT)[^)]*\)", re.I)
SKIP = re.compile(r"NOT APPLICABLE|VALUE LESS THAN|Cont|^\s*\*+\s*[A-Za-z(]|^\s*NOTE|^\s*Note|^\s*\d+\s*$|^\s*[-–]\s*\d+\s*[-–]\s*$")
DOTS = re.compile(r"(?:\.\s?){2,}")


def num(tok):
    if tok == "-":
        return 0.0, "nil"
    if tok.startswith("*") or tok == "+":
        return None, "lt_half"       # value less than 0.5
    if tok in ("X", "x"):
        return None, "na"
    return float(tok.replace(",", "")), ("sci_rounded" if "E+" in tok else "")


def colline(line):
    """The printed column-number row. PBS skips numbers in a few tables (11.3 prints 1 2 4 5 6 7 9 10 11)."""
    toks = line.split()
    if len(toks) >= 3 and all(t.isdigit() for t in toks):
        n = [int(t) for t in toks]
        if n[0] == 1 and all(b > a for a, b in zip(n, n[1:])) and n[-1] <= len(n) + 4:
            return n, [(m.start() + m.end()) / 2 for m in re.finditer(r"\d+", line)]
    return None


def header_guess(hlines, centers):
    out = collections.defaultdict(list)
    for ln in hlines:
        for m in re.finditer(r"\S+(?: \S+)*", ln):
            c = (m.start() + m.end()) / 2
            j = min(range(len(centers)), key=lambda k: abs(centers[k] - c))
            out[j].append(m.group())
    return {k: " ".join(v) for k, v in out.items()}


def parse(pdf, scope, w, catalog, log):
    txt = subprocess.run(["pdftotext", "-layout", str(pdf), "-"], capture_output=True, text=True).stdout
    stats = collections.Counter(); canon = {}
    for pno, page in enumerate(txt.split("\f"), 1):
        table = title = None; ncol = None; cols = None; unit = None; group = ""; pending = None; orphan = None; last = None
        hlines = []; in_header = False; area_note = ""
        if page.strip() and not re.search(r"(?m)^\s*TABLE\s*:?\s+\d", page):
            log.append(f"{pdf.name} p{pno}: page has no TABLE line, skipped")
        for raw in page.split("\n"):
            line = raw.rstrip()
            if not line.strip():
                continue
            m = TABLE.match(line)
            if m:
                table, title = m.group(1), m.group(2)
                a = AREA.search(title)
                if a:
                    area_note = a.group(); title = title.replace(a.group(), "")
                title = re.sub(r"\s+", " ", title).strip()
                if title in PROVS:
                    title = ""
                in_header = True; hlines = []; ncol = None; cols = None
                continue
            if table is None:
                continue
            if in_header:
                cl = colline(line)
                key = (scope, table)
                if cl:
                    cols = cl[0][1:]; ncol = len(cl[0]); in_header = False
                    if key not in catalog:
                        hg = header_guess(hlines, cl[1])
                        catalog[key] = dict(scope=scope, file=pdf.name, table=table, title=title, unit_note=area_note, cols=cols,
                                            first_page=pno, headers={c: hg.get(i + 1, "") for i, c in enumerate(cols)})
                    continue
                u = re.sub(r"\s+", " ", line.strip())
                if key in catalog and UNIT.match(u) and not DOTS.search(u):
                    # page printed without its column-number row: take the columns from the table's first page
                    cols = catalog[key]["cols"]; ncol = len(cols) + 1; in_header = False
                    log.append(f"{pdf.name} p{pno} T{table}: no column-number row on page, columns taken from p{catalog[key]['first_page']}")
                if in_header:
                    a = AREA.search(line)
                    if a:
                        area_note = a.group(); line = line.replace(a.group(), " " * len(a.group()))
                    if not title and line.strip() and not hlines:
                        title = re.sub(r"\s+", " ", line).strip(); continue
                    hlines.append(line)
                    continue
            if SKIP.search(line):
                continue
            s = line.strip()
            toks = s.split()
            # all-value line: continuation of a wrapped label
            pv = [t for t in toks if VAL.match(t)]
            if pending is not None and (len(pv) == len(toks) or (len(pv) == ncol - 1 and len(toks) - len(pv) <= 2)):
                if len(pv) != len(toks):
                    log.append(f"{pdf.name} p{pno} T{table}: dropped stray text {[t for t in toks if t not in pv]} in: {pending}")
                label, vals = pending, pv; pending = None
            else:
                pending = None
                d = list(DOTS.finditer(s))
                if d:
                    label, rest = s[:d[-1].start()].strip(), s[d[-1].end():].split()
                    stray = [t for t in rest if re.fullmatch(r"[A-WYZa-wyz]", t)]
                    if stray:                # source typos such as a lone 'R' or 'v' among the figures
                        log.append(f"{pdf.name} p{pno} T{table}: dropped stray character(s) {stray} in: {label}")
                        rest = [t for t in rest if t not in stray]
                    if not rest:
                        if orphan:           # figures printed on the line above their label
                            rest, orphan = orphan, None
                        else:
                            pending = label; continue
                    vals = rest
                    if not all(VAL.match(t) for t in vals):
                        log.append(f"{pdf.name} p{pno} T{table}: non-value after dots: {s[:160]}"); stats["bad"] += 1; continue
                else:
                    k = 0
                    while k < len(toks) and VAL.match(toks[-1 - k]):
                        k += 1
                    if k == 0:
                        u = re.sub(r"\s+", " ", s)
                        if UNIT.match(u):
                            unit, group = u, ""
                        else:
                            group = u; stats["group_lines"] += 1
                        continue
                    take = min(k, ncol - 1)
                    if take < ncol - 1 or take == len(toks):
                        if take == len(toks):
                            orphan = toks; continue
                    label, vals = " ".join(toks[:len(toks) - take]), toks[len(toks) - take:]
            label = re.sub(r"\s+", " ", label).strip(" .")
            label = re.sub(r"\s*-\s*TOTAL$", " - TOTAL", label)      # "FARMS-TOTAL" / "FARMS - TOTAL"
            if UNIT.match(label):            # flat layout: one row per administrative unit
                unit, group, label = label, "", ""
            ok = len(vals) == ncol - 1
            dup = label != "" and (unit, label) == last
            last = (unit, label)
            if dup:
                log.append(f"{pdf.name} p{pno} T{table} [{unit}] {label}: label printed twice in a row (source typo; second row is probably the next class)")
            if not ok:
                log.append(f"{pdf.name} p{pno} T{table} [{unit}] {label}: {len(vals)} values for {ncol - 1} columns"); stats["count_mismatch"] += 1
            if unit:                         # one spelling per unit ("GHOTKI (AT MIRPUR MATHELO)DISTRICT" etc.)
                unit = canon.setdefault(re.sub(r"\s+", "", unit), unit)
            if unit is None:
                log.append(f"{pdf.name} p{pno} T{table}: data row before any unit: {label}"); stats["no_unit"] += 1
            for i, t in enumerate(vals):
                v, flag = num(t)
                j = cols[i] if ok else i + 2
                w.writerow([scope, table, unit or "", group, label, j, t, "" if v is None else (int(v) if v == int(v) else v),
                            flag, "" if ok and not dup else ("dup_label" if ok else "count_mismatch"), pno])
            stats["rows"] += 1; stats["cells"] += len(vals)
    return stats


def main(raw, out):
    raw, out = Path(raw), Path(out); out.mkdir(parents=True, exist_ok=True)
    catalog, log = {}, []
    with gzip.open(out / "agri_census_2010_long.csv.gz", "wt", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["scope", "table", "unit", "group", "row_label", "col", "value_raw", "value", "flag", "qc", "pdf_page"])
        for fn, scope in FILES.items():
            st = parse(raw / fn, scope, w, catalog, log)
            print(fn, dict(st)); log.insert(0, f"SUMMARY {fn}: {dict(st)}")
    with open(out / "tables_catalog.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(["scope", "file", "table", "title", "unit_note", "n_cols", "first_page", "col", "header_guess"])
        for c in catalog.values():
            for j in c["cols"]:
                w.writerow([c["scope"], c["file"], c["table"], c["title"], c["unit_note"], len(c["cols"]) + 1, c["first_page"], j, c["headers"].get(j, "")])
    (out / "parse_log.txt").write_text("\n".join(log) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:3])
