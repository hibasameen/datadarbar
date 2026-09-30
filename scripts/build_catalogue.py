"""The data catalogue: an index of every published table, a page per table,
and a page for the boundaries everything joins on.

Called by build_seo.py, which owns the metadata, the chrome and the sitemap.
Everything here reads catalog.json, which build_web_warehouse.py writes with
each table's shelf (kind), the period it covers (span) and the place keys it
carries (keys), plus catalog_samples.json for the first rows of each.

The design is the redesign's Catalogue board: kind filters, Geography pinned
first because every other table joins on it, and a dataset page that leads
with what an analyst needs before downloading - how big, which years, what it
joins on, what to watch for - then the dictionary and a sample.
"""
import html
import json
import re
from pathlib import Path
from urllib.parse import quote

E = html.escape
ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app"
W = APP / "data" / "warehouse"
BOUNDS = APP / "data" / "boundaries"

# Conventions that apply across tables. They were the dictionary page's
# preamble; the dictionary folded into the catalogue and they came with it.
CONVENTIONS = [
    ("Join on a code, not a name",
     "Districts join on <code>district_code</code> (PBS 2023) or "
     "<code>district_key</code> (the older 147-district slug); census units on "
     "<code>dds_id</code>. <code>dd_id</code> is not unique: 591 census units share "
     "471 of them. <a href=\"/datasets/geography-keys/\">Which key is safe</a>."),
    ("Units are per table",
     "Trade is in thousand rupees, the budget in Rs million, national accounts vary "
     "by table, and every State Bank series carries its own unit in "
     "<code>sbp_series_catalog</code>. Read the unit before you sum."),
    ("Long tables",
     "<code>place_indicators</code>, <code>district_indicators</code> and "
     "<code>sbp_observations</code> hold one row per place or series, indicator and "
     "period. Filter to one indicator first, then <code>PIVOT</code>."),
    ("Fiscal years",
     "Pakistan's fiscal year runs July to June and is written <code>2024-25</code>. "
     "State Bank series are stamped on the last day of the period, so FY2023-24 "
     "appears as <code>2024-06-30</code>."),
    ("NULL is not zero",
     "A NULL is suppressed or unpublished. Survey districts with too few respondents "
     "are flagged (<code>low_n</code>) and should be left out of rankings."),
    ("Two trade bases",
     "<code>trade_hs8</code> is PBS customs data by product; the State Bank's "
     "balance-of-payments series are on a payments basis. They will not reconcile."),
]

ICON = {
    "search": '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5 14 14"/></svg>',
    "down": '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M8 2v9M4 7l4 4 4-4M3 14h10"/></svg>',
    "sql": '<svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><path d="M5 4 1.5 8 5 12M11 4l3.5 4-3.5 4"/></svg>',
}


def slug(name):
    return name.replace("_", "-")


def mb(b):
    return f"{b/1e6:.1f} MB" if b >= 1e6 else f"{max(1, round(b/1e3))} KB"


def num(n):
    return f"{n:,}"


def span_text(sp):
    if not sp:
        return ""
    a, b = sp["from"], sp["to"]
    # Dates are shown to the month; a range that starts and ends in one period
    # is that period.
    if re.match(r"\d{4}-\d\d-\d\d", a or ""):
        a, b = a[:7], b[:7]
    return a if a == b else f"{a} to {b}"


def short_title(title):
    """The SEO titles open with 'Pakistan' for search; in a list of Pakistani
    tables it is noise."""
    t = re.sub(r"^Pakistan(?:'s)?\s+", "", title)
    return t[:1].upper() + t[1:]


def query_href(sql):
    return "/query.html#q=" + quote(sql, safe="")


def starter_sql(t):
    return f"SELECT *\nFROM {t['name']}\nLIMIT 100;"


def btn(href, label, kind="ghost", icon=None, download=False, title=None):
    return (f'<a class="cbtn {kind}" href="{E(href)}"'
            + (' download' if download else '')
            + (f' title="{E(title, quote=True)}"' if title else '')
            + '>' + (ICON[icon] if icon else '') + f'<span>{E(label)}</span></a>')


# ── boundaries ───────────────────────────────────────────────────────────────

def _read_js_geo(path):
    js = path.read_text()
    return json.loads(js[js.index("=") + 1:].strip().rstrip(";"))


# Short property names the map payload uses, spelled out for a download.
PROPS = {"code": "district_code", "n": "name", "c": "census_name", "d": "district",
         "p": "province", "nc": "outside_census_frame"}


def write_boundaries():
    """Plain GeoJSON of the 2023 frame, from the same geometry the map draws.

    The map ships these as JavaScript assignments, which no GIS tool opens.
    Properties are renamed from the payload's one-letter keys; geometry is
    untouched, so what an analyst joins to is exactly what the site draws."""
    BOUNDS.mkdir(parents=True, exist_ok=True)
    out = []
    for src, name, level in (("districts_2023_geo.js", "pbs_districts_2023.geojson", "district"),
                             ("tehsils_2023_geo.js", "pbs_tehsils_2023.geojson", "tehsil")):
        g = _read_js_geo(APP / "data" / src)
        for f in g["features"]:
            f["properties"] = {PROPS.get(k, k): v for k, v in f["properties"].items()}
        dest = BOUNDS / name
        dest.write_text(json.dumps(g, separators=(",", ":")))
        out.append({"file": name, "level": level, "n": len(g["features"]),
                    "outside": sum(1 for f in g["features"] if f["properties"].get("outside_census_frame")),
                    "bytes": dest.stat().st_size,
                    "props": sorted({k for f in g["features"] for k in f["properties"]})})
    return out


def mini_map(max_w=240, max_h=200):
    """The districts as one small SVG: equirectangular, points thinned to what
    a 240px drawing can show. Decoration that is also the real outline."""
    g = _read_js_geo(APP / "data" / "districts_2023_geo.js")
    import math
    k = math.cos(math.radians(30))
    rings = []
    for f in g["features"]:
        geom = f["geometry"]
        polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
        for poly in polys:
            rings.append(poly[0])
    xs = [p[0] * k for r in rings for p in r]
    ys = [p[1] for r in rings for p in r]
    x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
    s = min(max_w / (x1 - x0), max_h / (y1 - y0))
    w, h = (x1 - x0) * s, (y1 - y0) * s
    paths = []
    for r in rings:
        pts, last = [], None
        for lon, lat in r:
            p = (round((lon * k - x0) * s, 1), round((y1 - lat) * s, 1))
            if last is None or abs(p[0] - last[0]) + abs(p[1] - last[1]) >= 1.6:
                pts.append(p)
                last = p
        if len(pts) >= 3:
            paths.append("M" + "L".join(f"{a:g} {b:g}" for a, b in pts) + "Z")
    return (f'<svg class="minimap" viewBox="0 0 {w:.0f} {h:.0f}" role="img" '
            f'aria-label="The 156 districts of the PBS 2023 frame">'
            f'<path d="{"".join(paths)}"/></svg>')


# ── shared pieces ────────────────────────────────────────────────────────────

def facts(items):
    return '<div class="cfacts">' + "".join(
        f'<div class="cfact"><span class="clabel">{E(k)}</span>'
        f'<span class="cfact-v">{v}</span>'
        + (f'<span class="cfact-s">{s}</span>' if s else '') + '</div>'
        for k, v, s in items) + '</div>'


def key_chips(keys, safety):
    out = []
    for k in keys:
        safe = safety.get(k)
        cls = "" if safe is None else (" safe" if safe else " unsafe")
        tip = ("" if safe is None else
               ("One value per place: safe to join on." if safe else
                "Not unique per place: do not join on it alone."))
        if k in ("district", "province"):
            tip = "A name, not a code: match with care."
            cls = " name"
        out.append(f'<code class="ckey{cls}" title="{E(tip, quote=True)}">{E(k)}</code>')
    return " ".join(out)


def page_shell(path, title, description, body, extra, seo):
    """The generated-page frame, with the catalogue's own stylesheet."""
    target = APP / path.strip("/") / "index.html"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(f'''<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover"/>
<meta name="theme-color" content="#0c3a1e"/>
<link rel="icon" type="image/png" sizes="32x32" href="/assets/img/favicon-32.png"/><link rel="apple-touch-icon" href="/assets/img/apple-touch-icon.png"/>
<link rel="preconnect" href="https://fonts.googleapis.com"/><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<link rel="stylesheet" href="/assets/css/shell.css"/>
<link rel="stylesheet" href="/assets/css/catalogue.css"/>
{seo.metadata(f"{title} — {seo.SITE_NAME}", description, path, extra, seo.SHARE_TAGS)}
</head><body class="cpage">
<a class="skip" href="#main">Skip to content</a>
{seo._shell.header(path, root="/")}
<main id="main" class="cwrap">
{body}
</main>
{seo._shell.footer(root="/")}
<script src="/assets/js/export.js"></script>
<script src="/assets/js/catalogue.js"></script>
<script src="/assets/js/modals.js"></script>
<script src="/assets/js/analytics.js"></script>
<script src="/assets/js/shell.js"></script>
</body></html>''')


# ── the index ────────────────────────────────────────────────────────────────

def table_row(t, name_of, safety):
    title = short_title(name_of[t["name"]])
    href = f"/datasets/{slug(t['name'])}/"
    sp = span_text(t.get("span"))
    return (f'<li class="crow" data-kind="{t["kind"]}" data-table="{E(t["name"])}">'
            f'<div class="crow-main"><a class="crow-title" href="{href}">{E(title)}</a>'
            f'<code class="ctbl">{E(t["name"])}</code>'
            f'<p class="crow-desc">{E(t["description"])}</p>'
            f'<div class="crow-fields" hidden></div></div>'
            f'<div class="crow-span">{E(sp) if sp else "<span class=cnil>—</span>"}</div>'
            f'<div class="crow-rows">{num(t["rows"])}</div>'
            f'<div class="crow-keys">{key_chips(t["keys"], safety) or "<span class=cnil>—</span>"}</div>'
            f'<div class="crow-act">'
            f'<a class="cicon" href="{query_href(starter_sql(t))}" title="Query {E(t["name"])} in the browser" aria-label="Query {E(t["name"])}">{ICON["sql"]}</a>'
            f'<a class="cicon" href="/data/warehouse/{E(t["file"])}" download title="Download Parquet, {mb(t["bytes"])}" aria-label="Download {E(t["name"])} as Parquet, {mb(t["bytes"])}">{ICON["down"]}</a>'
            f'</div></li>')


def row_head():
    return ('<li class="crow chead" aria-hidden="true"><div>Table</div><div>Period</div>'
            '<div class="r">Rows</div><div>Joins on</div><div></div></li>')


def index_page(cat, name_of, safety, bounds, seo):
    tables = cat["tables"]
    kinds = cat["kinds"]
    by = {k["key"]: [t for t in tables if t["kind"] == k["key"]] for k in kinds}
    n_fields = sum(len(t["columns"]) for t in tables)

    pills = [f'<button type="button" class="cpill" data-kind="all" aria-pressed="true">All <span>{len(tables)}</span></button>']
    for k in kinds:
        if by[k["key"]]:
            pills.append(f'<button type="button" class="cpill" data-kind="{k["key"]}" aria-pressed="false">'
                         f'{E(k["label"])} <span>{len(by[k["key"]])}</span></button>')

    d = next(b for b in bounds if b["level"] == "district")
    tt = next(b for b in bounds if b["level"] == "tehsil")
    geo = by["geography"]
    geo_list = "".join(
        f'<a class="cgeo-item" href="/datasets/{slug(t["name"])}/" data-table="{E(t["name"])}">'
        f'<span><b>{E(short_title(name_of[t["name"]]))}</b> '
        f'<span class="cmuted">{num(t["rows"])} rows</span></span>'
        f'<code class="ctbl">{E(t["name"])}</code></a>' for t in geo)

    sections = [f'''<section class="csec" data-kind="geography" id="geography">
  <div class="csec-head"><h2>Geography</h2><p>{E(kinds[0]["about"])}</p></div>
  <div class="cgeo">
    <a class="cgeo-card" href="/datasets/boundaries/">
      {mini_map(120, 104)}
      <span class="cgeo-text"><b>Boundaries, Census 2023</b>
        <span>PBS Digital Census 2023 polygons as the site draws them: {d["n"]} districts and {tt["n"]} tehsils, as GeoJSON.</span>
        <span class="cmuted">Keys: <code class="ckey safe">district_code</code> <code class="ckey safe">dds_id</code></span></span>
    </a>
    <div class="cgeo-list">{geo_list}</div>
  </div>
  <ul class="ctable" hidden>{"".join(table_row(t, name_of, safety) for t in geo)}</ul>
</section>''']
    for k in kinds[1:]:
        ts = by[k["key"]]
        if not ts:
            continue
        sections.append(
            f'<section class="csec" data-kind="{k["key"]}" id="{k["key"]}">'
            f'<div class="csec-head"><h2>{E(k["label"])}</h2><p>{E(k["about"])}</p></div>'
            f'<ul class="ctable">{row_head()}'
            + "".join(table_row(t, name_of, safety) for t in sorted(ts, key=lambda t: name_of[t["name"]]))
            + '</ul></section>')

    conv = "".join(f'<div class="cconv"><b>{E(h)}</b><p>{b}</p></div>' for h, b in CONVENTIONS)
    body = f'''<div class="chero">
  <div><h1>Data catalogue</h1>
  <p>Every table behind the explorers, documented: fields, source, period, the keys it joins on and what to watch for. {len(tables)} tables, {num(n_fields)} fields. Start with the geography: every other table joins on one of its keys.</p></div>
  <label class="csearch">{ICON["search"]}<input type="search" id="cq" placeholder="Find a table or field…" aria-label="Find a table or field" autocomplete="off"/></label>
</div>
<div class="cbar"><div class="cpills" role="group" aria-label="Kind">{"".join(pills)}</div>
<p class="cstat" id="cstat" aria-live="polite"></p></div>
<p class="cempty" id="cempty" hidden>Nothing matches. Try a column name such as <code>district_code</code>, or a subject such as <code>literacy</code>.</p>
{"".join(sections)}
<section class="csec" id="conventions"><div class="csec-head"><h2>Before you combine tables</h2><p>Six habits the tables need.</p></div>
<div class="cconvs">{conv}</div></section>
<section class="csec cmach" id="machines"><div class="csec-head"><h2>For code</h2><p>The files are static Parquet: query them over HTTP without downloading.</p></div>
<div class="cmach-grid"><pre class="ccode"><code>import duckdb
duckdb.sql("INSTALL httpfs; LOAD httpfs;")
duckdb.sql("""SELECT district, pending_end, pending_per_100k
  FROM '{seo.ORIGIN}/data/warehouse/ljcp_court_districts.parquet'
  WHERE year = 2024 ORDER BY pending_per_100k DESC LIMIT 10""").show()</code></pre>
<div class="cmach-links">{btn("/data/warehouse/catalog.json", "catalog.json", icon="down")}
<button type="button" class="cbtn ghost" id="cdict">{ICON["down"]}<span>Whole dictionary, Markdown</span></button>
{btn("/query.html", "Open the SQL console", "primary", icon="sql")}</div></div></section>'''
    extra = {"@type": "DataCatalog", "name": "Data Darbar Pakistan open data catalogue",
             "url": seo.ORIGIN + "/datasets/",
             "dataset": [{"@type": "Dataset", "name": name_of[t["name"]],
                          "url": seo.ORIGIN + "/datasets/" + slug(t["name"]) + "/",
                          "description": t["description"]} for t in tables]}
    page_shell("/datasets/", "Pakistan open data catalogue",
               "Every Pakistan census, survey, trade, budget, State Bank, court and energy "
               "table Data Darbar publishes, with field definitions, periods, join keys, "
               "sample rows and Parquet downloads.", body, extra, seo)


# ── a table's page ───────────────────────────────────────────────────────────

def dataset_page(t, cat, name_of, safety, samples, seo):
    title = name_of[t["name"]]
    kinds = {k["key"]: k["label"] for k in cat["kinds"]}
    path = f"/datasets/{slug(t['name'])}/"
    parquet = f"/data/warehouse/{t['file']}"
    sp = t.get("span")
    keys = t["keys"]
    head = f'''<nav class="ccrumb"><a href="/datasets/">&larr; All datasets</a> · <a href="/datasets/#{t["kind"]}">{E(kinds[t["kind"]])}</a></nav>
<div class="dhero"><div>
  <h1>{E(short_title(title))}</h1>
  <p class="dlede">{E(t["description"])}</p>
  <div class="dacts">{btn(parquet, f"Download Parquet · {mb(t['bytes'])}", "primary", "down", download=True)}
    {btn(query_href(starter_sql(t)), "Query in the browser", icon="sql")}
    <button type="button" class="cbtn ghost" data-copy="{E(t["name"])}"><code>{E(t["name"])}</code><span class="cmuted">copy</span></button></div>
</div></div>'''
    fx = facts([
        ("Rows", num(t["rows"]), f'{len(t["columns"])} fields'),
        ("Period", E(span_text(sp)) if sp else '<span class="cnil">No time column</span>',
         f'from <code>{E(sp["column"])}</code>' if sp else ""),
        ("Joins on", key_chips(keys, safety) if keys else '<span class="cnil">No place key</span>',
         '<a href="/datasets/geography-keys/">which keys are safe</a>' if keys else ""),
        ("Source", f'<span class="cfact-long">{E(t["source"] or "—")}</span>',
         f'unit: {E(t["unit"])}' if t.get("unit") else ""),
    ])
    notes = (f'<section class="dnote"><h2>Read before using</h2><p>{E(t["notes"])}</p></section>'
             if t.get("notes") else "")

    keyset = set(keys)
    dic = ('<section class="dsec"><div class="csec-head"><h2>Fields</h2>'
           f'<p>{len(t["columns"])} columns, in file order.</p></div>'
           '<div class="dscroll" tabindex="0" role="region" aria-label="Fields"><table class="dtab">'
           '<thead><tr><th>Field</th><th>Type</th><th>Definition</th></tr></thead><tbody>'
           + "".join(f'<tr><td><code class="cfield">{E(c["name"])}</code>'
                     + (' <span class="dtag">key</span>' if c["name"] in keyset else '')
                     + (' <span class="dtag">period</span>' if sp and c["name"] == sp["column"] else '')
                     + f'</td><td class="dty">{E(c["type"].lower())}</td>'
                     f'<td>{E(c["description"]) or "<span class=cnil>—</span>"}</td></tr>'
                     for c in t["columns"])
           + '</tbody></table></div></section>')

    rows = samples.get(t["name"]) or []
    cols = [c["name"] for c in t["columns"]]
    sample = ('<section class="dsec"><div class="csec-head"><h2>First rows</h2>'
              f'<p>{len(rows)} of {num(t["rows"])}, as stored. Long text is cut at 80 characters.</p></div>'
              '<div class="dscroll" tabindex="0" role="region" aria-label="Sample rows"><table class="dtab dsample">'
              '<thead><tr>' + "".join(f'<th>{E(c)}</th>' for c in cols) + '</tr></thead><tbody>'
              + "".join('<tr>' + "".join(
                  '<td class="cnil">NULL</td>' if v is None else
                  (f'<td class="n">{E(v)}</td>' if re.fullmatch(r"-?[\d.]+(e-?\d+)?", v) else f'<td>{E(v)}</td>')
                  for v in r) + '</tr>' for r in rows)
              + '</tbody></table></div></section>') if rows else ""

    sbp = ""
    if t.get("datasets"):
        sbp = ('<section class="dsec"><div class="csec-head"><h2>State Bank datasets</h2>'
               f'<p>{len(t["datasets"])} datasets, {num(sum(d["series"] for d in t["datasets"]))} series.</p></div>'
               '<div class="dscroll" tabindex="0" role="region" aria-label="State Bank datasets"><table class="dtab">'
               '<thead><tr><th>dataset_code</th><th>Dataset</th><th>Subject</th><th class="r">Series</th><th>Span</th></tr></thead><tbody>'
               + "".join(f'<tr><td><code class="cfield">{E(d["code"])}</code></td><td>{E(d["name"])}</td>'
                         f'<td>{E(d["subject"])}</td><td class="n">{d["series"]}</td>'
                         f'<td class="dty">{E(d["since"])} to {E(d["upto"])}</td></tr>' for d in t["datasets"])
               + '</tbody></table></div></section>')

    exs = [x for x in cat.get("examples", []) if re.search(r"\b" + re.escape(t["name"]) + r"\b", x["sql"])]
    ex_html = ""
    if exs:
        ex_html = ('<div class="dside-box"><h2>Example queries</h2><ul class="dex">'
                   + "".join(f'<li><a href="{query_href(x["sql"])}">{E(x["title"])}</a></li>' for x in exs)
                   + '</ul></div>')
    url = seo.ORIGIN + parquet
    code = (f'-- DuckDB, in the browser console or the CLI\n'
            f"SELECT * FROM '{url}' LIMIT 10;\n\n"
            f'# Python\nimport duckdb\nduckdb.sql("INSTALL httpfs; LOAD httpfs;")\n'
            f'df = duckdb.sql("SELECT * FROM \'{url}\'").df()\n\n'
            f'# R\narrow::read_parquet("{url}")')
    related = [x for x in cat["tables"] if x["kind"] == t["kind"] and x["name"] != t["name"]]
    rel = ('<div class="dside-box"><h2>Also in ' + E(kinds[t["kind"]]) + '</h2><ul class="drel">'
           + "".join(f'<li><a href="/datasets/{slug(x["name"])}/">{E(short_title(name_of[x["name"]]))}</a>'
                     f'<code class="ctbl">{E(x["name"])}</code></li>' for x in related[:8])
           + '</ul></div>') if related else ""
    # A table redistributed from a share-alike source keeps that source's
    # licence: ODbL data cannot be relicensed as CC BY, so the page and its
    # JSON-LD say ODbL and the cite line says so too.
    # A table that only carries some OpenStreetMap rows among its own is not
    # relicensed wholesale, but those rows keep their owners' terms and the
    # cite line has to say so.
    src = t.get("source") or ""
    odbl = src.startswith("\u00a9 OpenStreetMap")
    part_odbl = not odbl and "ODbL" in src
    lic_url = "https://opendatacommons.org/licenses/odbl/1-0/" if odbl else seo.LICENSE
    lic_txt = ("Redistributed under the Open Database Licence (ODbL) 1.0, share-alike; "
               "\u00a9 OpenStreetMap contributors" if odbl
               else "Derived data CC BY 4.0, except the rows taken from OpenStreetMap, which "
                    "stay \u00a9 OpenStreetMap contributors under the Open Database Licence "
                    "(ODbL) 1.0; cite the original source above" if part_odbl
               else "Derived data CC BY 4.0; cite the original source above")
    cite = (f'<div class="dside-box"><h2>Cite</h2><p class="dcite">Hiba Sameen / Data Darbar. '
            f'{E(title)}. {seo.ORIGIN}{path}. Release {E(str(cat["generated"]))}. '
            f'{lic_txt}.</p></div>')

    body = (head + fx + notes
            + '<div class="dgrid"><div class="dmain">' + dic + sample + sbp + '</div>'
            + '<aside class="dside">'
            + f'<div class="dside-box"><h2>Use it in code</h2><pre class="ccode"><code>{E(code)}</code></pre>'
            + f'<button type="button" class="cbtn ghost sm" data-copy-code>Copy</button></div>'
            + ex_html + rel + cite + '</aside></div>')
    schema = {"@type": "Dataset", "name": title, "alternateName": t["name"],
              "description": t["description"] + " " + (t.get("notes") or ""),
              "url": seo.ORIGIN + path, "license": lic_url, "creator": seo.PERSON,
              "publisher": seo.PUBLISHER, "spatialCoverage": {"@type": "Place", "name": "Pakistan"},
              "variableMeasured": [{"@type": "PropertyValue", "name": c["name"],
                                    "description": c["description"]} for c in t["columns"]],
              "distribution": [{"@type": "DataDownload",
                                "encodingFormat": "application/vnd.apache.parquet",
                                "contentUrl": url}],
              "includedInDataCatalog": {"@type": "DataCatalog", "name": "Data Darbar",
                                        "url": seo.ORIGIN + "/datasets/"}}
    if sp:
        schema["temporalCoverage"] = f'{sp["from"]}/{sp["to"]}'
    page_shell(path, title, t["description"], body, schema, seo)


# ── the boundaries page ──────────────────────────────────────────────────────

YES, NO = '<span class="dok">Yes</span>', '<span class="dno">No</span>'


def boundaries_page(cat, bounds, safety, key_rows, seo):
    path = "/datasets/boundaries/"
    dl = "".join(btn(f"/data/boundaries/{b['file']}",
                     f"{b['level'].title()}s · GeoJSON · {mb(b['bytes'])}",
                     "primary" if i == 0 else "ghost", "down", download=True)
                 for i, b in enumerate(bounds))
    d = next(b for b in bounds if b["level"] == "district")
    tt = next(b for b in bounds if b["level"] == "tehsil")
    keyed = {}
    for t in cat["tables"]:
        for k in t["keys"]:
            keyed.setdefault(k, []).append(t["name"])
    rows = "".join(
        f'<tr><td><code class="cfield">{E(k)}</code></td><td>{E(lvl)}</td><td>{E(what)}</td>'
        f'<td class="n">{num(n)}</td>'
        f'<td>{YES if uniq else NO}</td>'
        f'<td>{E(notes)}'
        + (('<div class="dkeyed">' + " ".join(f'<a href="/datasets/{slug(x)}/"><code>{E(x)}</code></a>'
                                              for x in keyed.get(k, []) + (keyed.get("dk", []) if k == "district_key" else [])
                                              + (keyed.get("tehsil_id", []) if k == "dd_id" else []))
            + '</div>') if keyed.get(k) or k in ("district_key", "dd_id") else '')
        + '</td></tr>'
        for k, lvl, what, n, uniq, frame, notes in key_rows)
    props = lambda b: " ".join(f'<code class="cfield">{E(p)}</code>' for p in b["props"])
    body = f'''<nav class="ccrumb"><a href="/datasets/">&larr; All datasets</a> · <a href="/datasets/#geography">Geography</a></nav>
<div class="dhero withmap"><div>
  <h1>Boundaries, Census 2023</h1>
  <p class="dlede">The PBS Digital Census 2023 polygons, exactly as the Places map draws them: {d["n"]} districts and {tt["n"]} tehsils. Districts carry PBS's own <code>district_code</code>; tehsils carry the census unit id <code>dds_id</code>, the only sub-district key with one polygon per census unit. Azad Jammu &amp; Kashmir and Gilgit-Baltistan are included and flagged where the census does not reach them.</p>
  <div class="dacts">{dl}</div>
</div>
<figure class="dmap">{mini_map()}<figcaption>{d["n"]} districts. Simplified for the web; drawn from the downloadable file.</figcaption></figure></div>
{facts([("Source", "PBS Digital Census 2023", "pulled 27 September 2026"),
        ("Coordinates", "WGS 84", "EPSG:4326, longitude and latitude"),
        ("Licence", "PBS terms", "derived keys CC BY 4.0"),
        ("Replaces", "geoBoundaries", "ADM2 (147) and ADM3 (553)")])}
<section class="dsec"><div class="csec-head"><h2>Which key joins what</h2><p>Measured from the boundary files, not asserted. From <a href="/datasets/geography-keys/"><code>geography_keys</code></a>.</p></div>
<div class="dscroll" tabindex="0" role="region" aria-label="Keys"><table class="dtab dkeys"><thead><tr><th>Key</th><th>Level</th><th>What it is</th><th class="r">Values</th><th>One per place</th><th>Notes and tables keyed on it</th></tr></thead><tbody>{rows}</tbody></table></div></section>
<div class="dtwo">
<section class="dside-box"><h2>What is in each file</h2>
<p><b>Districts</b> {props(d)}</p><p><b>Tehsils</b> {props(tt)}</p>
<p class="cmuted"><code>outside_census_frame</code> is 1 on the {tt["outside"]} tehsils the 2023 census did not enumerate; their areas are drawn and have no census figures.</p></section>
<section class="dside-box"><h2>Crosswalks that go with these files</h2><ul class="drel">
{"".join(f'<li><a href="/datasets/{slug(t["name"])}/">{E(short_title(n))}</a><code class="ctbl">{E(t["name"])}</code></li>' for t, n in ((t, seo.DATASET_NAMES[t["name"]]) for t in cat["tables"] if t["kind"] == "geography" and t["name"] != "geography_keys"))}
</ul></section></div>'''
    schema = {"@type": "Dataset", "name": "Pakistan administrative boundaries, Census 2023",
              "description": "PBS Digital Census 2023 district and tehsil polygons as GeoJSON.",
              "url": seo.ORIGIN + path, "license": seo.LICENSE, "creator": seo.PERSON,
              "publisher": seo.PUBLISHER,
              "distribution": [{"@type": "DataDownload", "encodingFormat": "application/geo+json",
                                "contentUrl": seo.ORIGIN + "/data/boundaries/" + b["file"]} for b in bounds]}
    page_shell(path, "Pakistan district and tehsil boundaries, Census 2023",
               f"GeoJSON of Pakistan's {d['n']} districts and {tt['n']} tehsils on the PBS "
               "Digital Census 2023 frame, with the identifiers that join them to every table.",
               body, schema, seo)


def build_catalogue(seo):
    import duckdb
    cat = json.loads((W / "catalog.json").read_text())
    samples = json.loads((W / "catalog_samples.json").read_text())
    name_of = seo.DATASET_NAMES
    missing = [t["name"] for t in cat["tables"] if t["name"] not in name_of]
    assert not missing, f"no title in DATASET_NAMES for {missing}"
    key_rows = duckdb.sql(f"SELECT * FROM '{(W / 'geography_keys.parquet').as_posix()}'").fetchall()
    safety = {r[0]: bool(r[4]) for r in key_rows}
    safety.update({"dk": safety.get("district_key"), "tehsil_id": safety.get("dd_id")})
    bounds = write_boundaries()
    index_page(cat, name_of, safety, bounds, seo)
    for t in cat["tables"]:
        dataset_page(t, cat, name_of, safety, samples, seo)
    boundaries_page(cat, bounds, safety, key_rows, seo)
    return len(cat["tables"])
