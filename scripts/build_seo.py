#!/usr/bin/env python3
"""Build readable research pages from the same published data as the explorer.

Run from any directory. No network, third-party packages or data reconstruction.
"""
import csv
import html
import json
import re
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app"
ORIGIN = "https://darbar.adaad.org"
ESC = html.escape
LICENSE = "https://creativecommons.org/licenses/by/4.0/"
PROFILES = ["lahore", "faisalabad", "rawalpindi", "multan", "gujranwala", "bahawalpur", "islamabad", "karachi central", "karachi east", "karachi south", "hyderabad", "sukkur", "larkana", "peshawar", "abbottabad", "swat", "mardan", "quetta", "gwadar"]
FIELDS = [
    ("t1_2023_pop_total", "Population", "people"),
    ("t1_2023_pop_male", "Male population", "people"),
    ("t1_2023_pop_female", "Female population", "people"),
    ("t1_2023_pop_transgender", "Transgender population", "people"),
    ("t1_2023_area_sq_km", "Area", "km²"),
    ("t1_2023_density_per_sq_km", "Population density", "people per km²"),
    ("t1_2023_urban_proportion", "Urban population share", "%"),
    ("t1_2023_avg_household_size", "Average household size", "people per household"),
    ("t12_2023_literacy_ratio_all", "Literacy, ages 10+", "%"),
    ("t12_2023_literacy_ratio_male", "Male literacy, ages 10+", "%"),
    ("t12_2023_literacy_ratio_female", "Female literacy, ages 10+", "%"),
    ("t12_2023_out_of_school_5_16", "Out-of-school children, ages 5–16", "children"),
]
DATASET_NAMES = {
    "budget_lines": "Pakistan federal budget line items",
    "district_indicators": "Pakistan district census and survey indicators",
    "file_catalog": "Pakistan statistical source-file catalogue",
    "lsm_qim": "Pakistan large-scale manufacturing index",
    "lsm_sector_indices": "Pakistan manufacturing sector indices",
    "mouza_crosswalk": "Pakistan Mouza Census geographic crosswalk",
    "mouza_tehsil": "Pakistan rural facilities by tehsil, Mouza Census 2020",
    "mpi_districts": "Pakistan district multidimensional poverty estimates",
    "national_accounts": "Pakistan national accounts and GDP tables",
    "sbp_observations": "Pakistan monetary and external statistics, SBP series",
    "sbp_series_catalog": "State Bank of Pakistan series catalogue",
    "tehsil_nightlights": "Pakistan tehsil night-time lights",
    "tehsil_satellite": "Pakistan tehsil wealth, population and satellite indicators",
    "trade_hs8": "Pakistan imports and exports by HS8 product and country",
}
BASE_PAGES = {
    "index.html": ("Data Darbar — Pakistan Census, Trade & Economic Data", "Data Darbar by Adaad brings Pakistan's official census, trade, budget and economic statistics together, with maps, downloadable datasets and source notes."),
    "map.html": ("Pakistan Census 2023 District Data & Maps — Data Darbar", "Explore Pakistan's district population, literacy and census indicators, alongside separately labelled survey estimates and tehsil rural facilities."),
    "trade.html": ("Pakistan Exports & Imports by Product and Country — Data Darbar", "Explore Pakistan's imports and exports by 8-digit HS product, trading partner and fiscal year. Read source definitions and download the underlying trade data."),
    "finance.html": ("Pakistan GDP & Federal Budget Data — Data Darbar", "Explore Pakistan's GDP, sector shares, federal budget receipts and spending, with definitions and downloadable data."),
    "money.html": ("Pakistan Inflation, Remittances & Monetary Data — Data Darbar", "Explore State Bank of Pakistan series on inflation, remittances, exchange rates, reserves, interest rates and banking, with source notes."),
    "poverty.html": ("Pakistan Poverty, Wealth & Night-time Lights — Data Darbar", "Explore Pakistan's multidimensional poverty, relative wealth and night-time lights. Compare sources with their distinct definitions and limitations."),
    "query.html": ("Download & Query Pakistan Open Data — Data Darbar", "Query Pakistan census, trade, budget and State Bank data in your browser, or download the documented tables for your own analysis."),
    "dictionary.html": ("Pakistan Open Data Dictionary — Data Darbar", "Read field definitions, units, source coverage and limitations for Data Darbar's downloadable Pakistan research datasets."),
}


def norm(value):
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def number(value):
    if value is None:
        return "Unavailable"
    if isinstance(value, (float, int)):
        return f"{value:,.2f}".rstrip("0").rstrip(".") if isinstance(value, float) else f"{value:,}"
    return str(value)


def ld(value):
    return json.dumps(value, ensure_ascii=False).replace("<", "\\u003c")


def metadata(title, description, path, extra=None):
    url = ORIGIN + path
    graph = [{"@type": "WebPage", "@id": url + "#page", "url": url, "name": title, "description": description, "inLanguage": "en", "isPartOf": {"@id": ORIGIN + "/#website"}}]
    if path == "/":
        graph.append({"@type": "WebSite", "@id": ORIGIN + "/#website", "url": url, "name": "Data Darbar", "alternateName": "Data Darbar by Adaad", "description": description, "creator": {"@type": "Person", "name": "Hiba Sameen", "url": "https://adaad.org/about/"}, "publisher": {"@type": "Organization", "name": "Adaad", "url": "https://adaad.org/"}})
    if extra:
        graph.extend(extra if isinstance(extra, list) else [extra])
    return f'''<!-- SEO:START -->
<title>{ESC(title)}</title>
<meta name="description" content="{ESC(description, quote=True)}">
<link rel="canonical" href="{url}">
<meta property="og:title" content="{ESC(title, quote=True)}">
<meta property="og:description" content="{ESC(description, quote=True)}">
<meta property="og:url" content="{url}">
<meta name="twitter:title" content="{ESC(title, quote=True)}">
<meta name="twitter:description" content="{ESC(description, quote=True)}">
<script type="application/ld+json">{ld({"@context": "https://schema.org", "@graph": graph})}</script>
<!-- SEO:END -->'''


def patch_metadata(file, title, description, path):
    text = file.read_text()
    text = re.sub(r"<!-- SEO:START -->.*?<!-- SEO:END -->\n?", "", text, flags=re.S)
    text = re.sub(r"<title>.*?</title>\s*", "", text, flags=re.S | re.I)
    text = re.sub(r'<meta\b[^>]*(?:name|property)=["\'](?:description|og:title|og:description|og:url|twitter:title|twitter:description)["\'][^>]*>\s*', "", text, flags=re.I)
    text = re.sub(r'<link\b[^>]*rel=["\']canonical["\'][^>]*>\s*', "", text, flags=re.I)
    text = text.replace("</head>", metadata(title, description, path) + "\n</head>", 1)
    # The standalone URLs remain useful to crawlers and to readers without JS.
    text = re.sub(r'href="#"\s+data-modal="(about|methodology)"', lambda m: f'href="{m[1]}.html"', text)
    if 'data-research-links' not in text:
        links = '<span data-research-links><a href="/districts/">District profiles</a> · <a href="/datasets/">Data catalogue</a> · <a href="https://adaad.org/">Adaad</a></span>'
        text = text.replace("</footer>", links + "</footer>", 1)
    file.write_text(text)


def page(path, title, description, body, extra=None):
    target = APP / path.strip("/") / "index.html" if path.endswith("/") else APP / path.strip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(f'''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
{metadata(title + " — Data Darbar", description, path, extra)}
<link rel="icon" href="/assets/img/favicon-32.png"><link rel="stylesheet" href="/assets/css/research.css">
</head><body><a class="skip" href="#main">Skip to content</a>
<header><a class="brand" href="/">Data Darbar <small>by Adaad</small></a><nav aria-label="Main"><a href="/map.html">Map</a><a href="/districts/">Districts</a><a href="/datasets/">Datasets</a><a href="/about.html">About</a><a href="/methodology.html">Methods</a></nav></header>
<main id="main"><p class="eyebrow">Pakistan · Data · Sources</p><h1>{ESC(title)}</h1><p class="intro">{ESC(description)}</p>{body}</main>
<footer><p>Hiba Sameen · <a href="https://adaad.org/">Adaad</a> · <a href="https://aiwan.adaad.org/elections/">Pakistan election results</a></p><p>Derived data: <a href="{LICENSE}">CC BY 4.0</a>. Original sources and limitations are identified on each page.</p></footer>
<script src="/assets/js/modals.js" defer></script><script src="/assets/js/analytics.js" defer></script></body></html>''')


def table(headers, rows, caption):
    return '<div class="table-wrap" role="region" tabindex="0" aria-label="' + ESC(caption, quote=True) + '"><table><caption>' + ESC(caption) + '</caption><thead><tr>' + ''.join('<th scope="col">' + ESC(h) + '</th>' for h in headers) + '</tr></thead><tbody>' + ''.join('<tr>' + ''.join('<td>' + ESC(str(v)) + '</td>' for v in row) + '</tr>' for row in rows) + '</tbody></table></div>'


def modal_content(variable):
    # Reuse the full, maintained method notes rather than a second summary.
    source = (APP / "assets/js/modals.js").read_text()
    match = re.search(r"\bvar\s+" + variable + r"\s*=\s*`([^`]*)`\s*;", source)
    assert match and "${" not in match[1] and chr(92) not in match[1], "Unsupported modal template"
    content = match[1].replace("https://hibasameen.github.io/datadarbar/", ORIGIN + "/")
    content = re.sub(r"<(\/?)h([3-6])", lambda m: "<" + m[1] + "h" + str(int(m[2]) - 1), content)
    return '<div class="method-content">' + content + '</div>'


def build():
    districts = json.loads((APP / "data/districts.json").read_text())
    geo = json.loads((APP / "data/pakistan_districts_province_boundries.geojson").read_text())
    labels = {norm(f["properties"]["districts"]): f["properties"] for f in geo["features"]}
    catalog = json.loads((APP / "data/warehouse/catalog.json").read_text())
    links = []
    for key in PROFILES:
        d = districts[key]
        assert d.get("t1_2023_pop_total") and d.get("t12_2023_literacy_ratio_all") is not None, key
        assert not any(v for k, v in d.items() if k.endswith("_boundary_change")), key
        name = labels[key]["districts"].title()
        province = labels[key]["province_territory"]
        slug = key.replace(" ", "-")
        path = f"/districts/{slug}/"
        title = f"{name} district: Census 2023 population and literacy"
        description = f"{name} district, {province}, recorded {number(d['t1_2023_pop_total'])} people in Census 2023. Read its population, literacy, urban share and schooling counts, with definitions and a CSV download."
        rows = [(label, number(d.get(field)), unit) for field, label, unit in FIELDS]
        csv_path = APP / path.strip("/") / "census-2023.csv"
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        with csv_path.open("w", newline="") as stream:
            writer = csv.writer(stream)
            writer.writerow(["district", "province", "year", "indicator", "value", "unit", "source_field"])
            for field, label, unit in FIELDS:
                writer.writerow([name, province, 2023, label, "" if d.get(field) is None else d[field], unit, field])
        notes = "These figures describe the district, not necessarily the city of the same name. Literacy covers ages 10 and over; out-of-school counts cover ages 5–16. Missing values are unavailable, not zero. This profile uses 2023 figures only: boundary changes make some 2017 comparisons unsafe. School attendance and literacy are different measures."
        body = f'<p><a href="/districts/">All district profiles</a> · <a href="/map.html">Explore the interactive district map</a> · <a href="census-2023.csv" download>Download this profile (CSV)</a></p>'
        body += table(["Indicator", "2023 value", "Unit"], rows, f"{name} district — Census 2023")
        body += f'<aside><h2>How to read this profile</h2><p>{notes}</p></aside><h2>Source and method</h2><p>Pakistan Bureau of Statistics, Population and Housing Census 2023, Tables 1 and 12; processed by Data Darbar. The table and CSV are generated from the same district record used by the explorer.</p><p><a href="https://www.pbs.gov.pk/census/">PBS census publications</a> · <a href="/datasets/district-indicators/">Data dictionary and full district dataset</a> · <a href="/methodology.html">Methodology</a></p>'
        body += f'<h2>Cite this profile</h2><p>Data Darbar / Hiba Sameen. {ESC(title)}. {ORIGIN}{path}. Derived data licensed CC BY 4.0; cite PBS as the original source.</p>'
        schema = {"@type": "Dataset", "name": title, "description": description + " " + notes, "url": ORIGIN + path, "creator": {"@type": "Person", "name": "Hiba Sameen", "url": "https://adaad.org/about/"}, "license": LICENSE, "temporalCoverage": "2023", "spatialCoverage": {"@type": "Place", "name": f"{name} district, {province}, Pakistan"}, "isBasedOn": "https://www.pbs.gov.pk/census/", "distribution": [{"@type": "DataDownload", "encodingFormat": "text/csv", "contentUrl": ORIGIN + path + "census-2023.csv"}]}
        page(path, title, description, body, schema)
        links.append(f'<li><a href="{path}">{ESC(name)} district</a><span>{ESC(str(province))} · Population {number(d["t1_2023_pop_total"])}</span></li>')
    page("/districts/", "Pakistan district profiles: Census 2023", "Population, literacy and schooling figures for selected districts of Pakistan, with readable tables, source definitions and CSV downloads.", '<p>These initial profiles use districts with available population and literacy data and no explicit boundary-change flag in the source record. More places and survey indicators remain available in the <a href="/map.html">district map</a>.</p><ul class="cards">' + ''.join(links) + '</ul><p><a href="/datasets/district-indicators/">Download the full district indicator dataset</a>.</p>')
    dataset_links = []
    for d in catalog["tables"]:
        slug = d["name"].replace("_", "-")
        path = f"/datasets/{slug}/"
        title = DATASET_NAMES[d["name"]]
        assert (APP / "data/warehouse" / d["file"]).is_file(), d["file"]
        download = "/data/warehouse/" + d["file"]
        body = f'<p><a href="/datasets/">All datasets</a> · <a href="{download}" download>Download Parquet</a> · <a href="/query.html">Query this table in the browser</a></p><dl><dt>Source</dt><dd>{ESC(d["source"])}</dd><dt>Rows in this release</dt><dd>{number(d["rows"])}</dd><dt>Units</dt><dd>{ESC(d["unit"] or "Vary by field or series; see definitions below.")}</dd><dt>Catalogue generated</dt><dd>{ESC(str(catalog["generated"]))}</dd></dl><aside><h2>Definitions and limitations</h2><p>{ESC(d["notes"])}</p></aside>'
        body += table(["Field", "Type", "Definition"], [(c["name"], c["type"], c["description"]) for c in d["columns"]], title + " — data dictionary")
        body += f'<h2>Reuse and citation</h2><p>Data Darbar / Hiba Sameen. {ESC(title)}. {ORIGIN}{path}. Cite the original source listed above and the catalogue release when reusing this table.</p><p><a href="/methodology.html">Methodology</a> · <a href="https://adaad.org/datasets/">Data behind Adaad articles</a></p>'
        schema = {"@type": "Dataset", "name": title, "alternateName": d["name"], "description": d["description"] + " " + d["notes"], "url": ORIGIN + path, "license": LICENSE, "creator": {"@type": "Person", "name": "Hiba Sameen", "url": "https://adaad.org/about/"}, "spatialCoverage": {"@type": "Place", "name": "Pakistan"}, "variableMeasured": [{"@type": "PropertyValue", "name": c["name"], "description": c["description"]} for c in d["columns"]], "distribution": [{"@type": "DataDownload", "encodingFormat": "application/vnd.apache.parquet", "contentUrl": ORIGIN + download}], "includedInDataCatalog": {"@type": "DataCatalog", "name": "Data Darbar", "url": ORIGIN + "/datasets/"}}
        page(path, title, d["description"], body, schema)
        dataset_links.append(f'<li><a href="{path}">{ESC(title)}</a><span>{ESC(d["description"])}</span></li>')
    page("/datasets/", "Pakistan open data catalogue", "Download documented census, trade, budget, national accounts, poverty and State Bank of Pakistan datasets. Each table has field definitions, source information and limitations.", '<ul class="cards">' + ''.join(dataset_links) + '</ul><p>The <a href="/query.html">browser query tool</a> opens these same tables. Read the units and warnings before combining or summing rows.</p>', {"@type": "DataCatalog", "name": "Data Darbar Pakistan open data catalogue", "url": ORIGIN + "/datasets/", "dataset": [{"@type": "Dataset", "name": DATASET_NAMES[d["name"]], "url": ORIGIN + "/datasets/" + d["name"].replace("_", "-") + "/", "description": d["description"]} for d in catalog["tables"]]})
    page("/about.html", "About Data Darbar", "Data Darbar by Adaad is an independent explorer of Pakistan’s official statistics, built by economist and data scientist Hiba Sameen.", modal_content("ABOUT"))
    page("/methodology.html", "Data Darbar methodology and sources", "Complete sources, processing methods, coverage and limitations for Pakistan census, survey, trade, budget, monetary and satellite datasets.", modal_content("METH"))
    for file, (title, description) in BASE_PAGES.items():
        patch_metadata(APP / file, title, description, "/" if file == "index.html" else "/" + file)
    sitemap = Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
    paths = ["/" if p.name == "index.html" and p.parent == APP else "/" + str(p.relative_to(APP)).removesuffix("index.html") if p.name == "index.html" else "/" + str(p.relative_to(APP)) for p in APP.rglob("*.html") if p.name != "economy.html"]
    for path in sorted(set(paths)):
        SubElement(SubElement(sitemap, "url"), "loc").text = ORIGIN + path
    (APP / "sitemap.xml").write_bytes(tostring(sitemap, encoding="utf-8", xml_declaration=True))
    (APP / "robots.txt").write_text(f"User-agent: *\nAllow: /\n\nSitemap: {ORIGIN}/sitemap.xml\n")
    print(f"Built {len(PROFILES)} district profiles, {len(catalog['tables'])} dataset pages and {len(set(paths))} sitemap entries.")


if __name__ == "__main__":
    build()
