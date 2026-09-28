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

# The header and footer belong to etl/apply_shell.py, which writes them into
# every page under app/. These pages are generated rather than edited, so they
# import the same definition instead of carrying a copy - a copy is how the
# chrome came to render differently on different pages before.
import sys
sys.path.insert(0, str(ROOT / "etl"))
import apply_shell as _shell
ORIGIN = "https://darbar.adaad.org"
ESC = html.escape
LICENSE = "https://creativecommons.org/licenses/by/4.0/"
SITE_NAME = "Data Darbar"
TAGLINE = "Pakistan in Numbers"
OG_IMAGE = ORIGIN + "/assets/img/og-preview.png"
ADAAD = "https://adaad.org/"
# The same @ids adaad.org declares, so the three sites resolve to one publisher and one author.
PUBLISHER = {"@type": "Organization", "@id": ADAAD + "#publisher", "name": "Adaad", "url": ADAAD}
PERSON = {"@type": "Person", "@id": ADAAD + "about/#hiba-sameen", "name": "Hiba Sameen", "url": ADAAD + "about/", "sameAs": ["https://github.com/hibasameen", "https://www.linkedin.com/in/hiba-sameen-86750819/", "https://scholar.google.com/citations?user=FaZDLEUAAAAJ"]}
# Share-card tags for generated pages; the hand-written pages keep their own og:image.
SHARE_TAGS = f'<meta property="og:type" content="website"><meta property="og:site_name" content="{SITE_NAME}"><meta property="og:image" content="{OG_IMAGE}"><meta property="og:image:width" content="1200"><meta property="og:image:height" content="630"><meta name="twitter:card" content="summary_large_image"><meta name="twitter:image" content="{OG_IMAGE}">'
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
    "census_panel_2017": "Pakistan Census 2017 district and tehsil tables",
    "census_series_index": "Pakistan census map series index",
    "census_panel_2023": "Pakistan Census 2023 district and tehsil tables",
    "district_indicators": "Pakistan district census and survey indicators",
    "file_catalog": "Pakistan statistical source-file catalogue",
    # geography: the frames, the crosswalks, and which key joins what
    "district_crosswalk_2017_2023": "Pakistan district boundary changes, Census 2017 to 2023",
    "subdistrict_crosswalk_2017_2023": "Pakistan tehsil boundary changes, Census 2017 to 2023",
    "census_unit_map": "Pakistan census units mapped to 2023 boundaries",
    "geography_keys": "Pakistan place identifiers and which ones join safely",
    "diaspora_emigrants_district": "Pakistan registered emigrants by district of origin",
    "crops_district_fy": "Pakistan crop area, production and yield by district",
    "census_entities": "Pakistan buildings and facilities counted by Census 2023",
    "gdp_growth": "Pakistan GDP growth by sector since 1951",
    "gdp_indicators": "Pakistan GDP, per-capita income and exchange rate",
    "gva_by_activity_annual": "Pakistan value added by sector, annual",
    "gva_by_activity_quarterly": "Pakistan value added by sector, quarterly",
    "fbr_tax_collection": "Pakistan federal tax collection by head since 1992",
    "trade_by_country": "Pakistan imports and exports by trading partner",
    "trade_by_group": "Pakistan imports and exports by commodity group",
    "trade_monthly_totals": "Pakistan monthly trade totals since 2003",
    "trade_reconciliation": "Pakistan trade totals reconciled against their parts",
    "diaspora_remittances_monthly": "Remittances to Pakistan by month",
    "diaspora_emigrants_by_skill": "Pakistan emigrants by skill level",
    "diaspora_destinations": "Pakistan emigrants by destination country",
    "diaspora_occupations": "Pakistan emigrants by occupation",
    "place_indicators": "Pakistan district and tehsil indicators on 2023 boundaries",
    "place_indicator_index": "Index of every Pakistan district and tehsil indicator",
    "lsm_qim": "Pakistan large-scale manufacturing index",
    "lsm_sector_indices": "Pakistan manufacturing sector indices",
    "mouza_crosswalk": "Pakistan Mouza Census geographic crosswalk",
    "mouza_tehsil": "Pakistan rural facilities by tehsil, Mouza Census 2020",
    "mpi_districts": "Pakistan district multidimensional poverty estimates",
    "national_accounts": "Pakistan national accounts and GDP tables",
    "sbp_observations": "Pakistan monetary and external statistics, SBP series",
    "sbp_series_catalog": "State Bank of Pakistan series catalogue",
    "schools_pk": "Pakistan government schools with positions and provenance",
    "school_access_district": "Pakistan district distance to the nearest girls' and boys' school",
    "school_access_tehsil": "Pakistan tehsil distance to the nearest girls' and boys' school",
    "school_distance_stats": "Pakistan distance-to-school distributions by sex and level",
    "school_layer_coverage": "Pakistan school layer coverage ledger",
    "school_validation_district": "Pakistan school layer validation against the Mouza Census, by district",
    "school_validation_tehsil": "Pakistan school layer validation against the Mouza Census, by tehsil",
    "school_validation_summary": "Pakistan school layer validation scorecard",
    "census_enrolment_5_16_by_sex": "Pakistan district enrolment aged 5–16 by sex, Census 2023",
    "health_access_tehsil": "Pakistan tehsil travel time to the nearest health facility",
    "health_access_district": "Pakistan district travel time to the nearest health facility",
    "tehsil_nightlights": "Pakistan tehsil night-time lights",
    "tehsil_satellite": "Pakistan tehsil wealth, population and satellite indicators",
    "trade_hs8": "Pakistan imports and exports by HS8 product and country",
    # justice, policing, energy and disasters - published with the State
    # explorer rather than held back in the desktop warehouse
    "ljcp_case_flows": "Pakistan court case flows and pendency by province",
    "ljcp_judicial_strength": "Pakistan judicial posts, filled and vacant",
    "police_crime_annual": "Pakistan reported offences by police force and year",
    "police_crime_district": "Pakistan reported offences by district",
    "sindh_crime_annual": "Sindh reported crime by category and year",
    "sindh_fir_daily": "Sindh first information reports, daily running totals",
    "nepra_plants": "Pakistan power plants, fuel and installed capacity",
    "nepra_disco_annual": "Pakistan electricity distribution companies, annual",
    "climate_events": "Pakistan flood, drought and cyclone alerts since 2001",
    "climate_impacts": "Pakistan monsoon deaths, injuries and damage, NDMA reports",
}
BASE_PAGES = {
    "index.html": ("Data Darbar — Pakistan Census, Trade & Economic Data", "Data Darbar by Adaad brings Pakistan's official census, trade, budget and economic statistics together, with maps, downloadable datasets and source notes."),
    "state.html": ("Pakistan Federal Tax Collection & Public Money \u2014 Data Darbar", "Pakistan's federal tax collection by head since 1991-92, and what the courts, police and energy regulators publish."),
    "places.html": ("Pakistan District & Tehsil Indicators Map \u2014 Data Darbar", "Search 5,801 indicators for Pakistan's 156 districts and 649 tehsils: census, survey, poverty, agriculture, facilities and satellite data on one map."),
    # trade.html and money.html were their own pages and are redirects now:
    # Economy is one page, and _retired() keeps them out of the sitemap. The
    # title below has to cover all fifteen topics, because it is the only
    # page any of them has.
    "finance.html": ("Pakistan GDP, Trade, Inflation & Budget Data — Data Darbar", "Pakistan's economy in one place: GDP and sector shares since 1951, large-scale manufacturing, the federal budget, exports and imports by product and partner, the rupee, inflation, interest rates, remittances and the external balance."),
    "query.html": ("Download & Query Pakistan Open Data — Data Darbar", "Query Pakistan census, trade, budget and State Bank data in your browser, or download the documented tables for your own analysis."),
    "dictionary.html": ("Pakistan Open Data Dictionary — Data Darbar", "Read field definitions, units, source coverage and limitations for Data Darbar's downloadable Pakistan research datasets."),
    "methods.html": ("Methods and Sources — Data Darbar", "What Data Darbar is, where every figure comes from, how districts are matched across boundary changes, and what each source will and will not support."),
}


def _retired(p: Path) -> bool:
    """A page that only redirects somewhere else does not belong in a sitemap.

    Naming them one by one drifts - economy.html and poverty.html were listed,
    then map.html, about.html and methodology.html retired and were not. The
    page says what it is: a redirect carries noindex.
    """
    try:
        head = p.read_text(encoding="utf-8")[:2000]
    except OSError:
        return True
    return 'name="robots" content="noindex"' in head or 'http-equiv="refresh"' in head


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


def metadata(title, description, path, extra=None, tags=""):
    url = ORIGIN + path
    graph = [{"@type": "WebPage", "@id": url + "#page", "url": url, "name": title, "description": description, "inLanguage": "en", "isPartOf": {"@id": ORIGIN + "/#website"}}]
    if path == "/":
        graph.append({"@type": "WebSite", "@id": ORIGIN + "/#website", "url": url, "name": SITE_NAME, "alternateName": [TAGLINE, "ڈیٹا دربار"], "description": description, "inLanguage": "en", "isPartOf": {"@id": ADAAD + "#website"}, "creator": PERSON, "publisher": PUBLISHER})
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
{tags}<script type="application/ld+json">{ld({"@context": "https://schema.org", "@graph": graph})}</script>
<!-- SEO:END -->'''


def patch_metadata(file, title, description, path):
    text = file.read_text()
    text = re.sub(r"<!-- SEO:START -->.*?<!-- SEO:END -->\n?", "", text, flags=re.S)
    text = re.sub(r"<title>.*?</title>\s*", "", text, flags=re.S | re.I)
    text = re.sub(r'<meta\b[^>]*(?:name|property)=["\'](?:description|og:title|og:description|og:url|twitter:title|twitter:description)["\'][^>]*>\s*', "", text, flags=re.I)
    text = re.sub(r'<link\b[^>]*rel=["\']canonical["\'][^>]*>\s*', "", text, flags=re.I)
    tags = "" if "og:site_name" in text else f'<meta property="og:site_name" content="{SITE_NAME}">\n'
    text = text.replace("</head>", metadata(title, description, path, tags=tags) + "\n</head>", 1)
    # One copyright line across the three sites; the data is CC BY, so nothing is "all rights reserved".
    text = text.replace("&copy; 2026 Hiba Sameen. All rights reserved.", "&copy; 2026 Hiba Sameen")
    # The standalone URLs remain useful to crawlers and to readers without JS.
    text = re.sub(r'href="#"\s+data-modal="(about|methodology)"', lambda m: f'href="{m[1]}.html"', text)
    text = re.sub(r'<span data-research-links>.*?</span>', "", text, flags=re.S)
    links = f'<span data-research-links><a href="/districts/">District Profiles</a> · <a href="/datasets/">Data Catalogue</a> · <a href="{ADAAD}">Adaad</a> · <a href="https://aiwan.adaad.org/">Aiwan-e-Jamhoor</a></span>'
    text = text.replace("</footer>", links + "</footer>", 1)
    file.write_text(text)


# The site chrome, as index.html and dictionary.html carry it: the same header,
# The header, mobile menu and footer come from etl/apply_shell.py with
# root-absolute paths, so a page two directories deep resolves them. The active
# entry is baked in by that module rather than resolved at runtime.


def page(path, title, description, body, extra=None, heading=None):
    target = APP / path.strip("/") / "index.html" if path.endswith("/") else APP / path.strip("/")
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(f'''<!DOCTYPE html><html lang="en"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover"/>
<meta name="theme-color" content="#0c3a1e"/>
<link rel="icon" type="image/png" sizes="32x32" href="/assets/img/favicon-32.png"/><link rel="apple-touch-icon" href="/assets/img/apple-touch-icon.png"/>
<link rel="preconnect" href="https://fonts.googleapis.com"/><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<link rel="stylesheet" href="/assets/css/shell.css"/>\n<link rel="stylesheet" href="/assets/css/research.css"/>
{metadata(f"{title} — {SITE_NAME}", description, path, extra, SHARE_TAGS)}
</head><body>
<a class="skip" href="#main">Skip to content</a>
{_shell.header(path, root="/")}
<main id="main" class="wrap">
<div class="hero"><h1>{ESC(heading or title)}</h1><p>{ESC(description)}</p></div>
{body}
</main>
{_shell.footer(root="/")}
<script src="/assets/js/modals.js"></script>
<script src="/assets/js/analytics.js"></script>
<script src="/assets/js/shell.js"></script>\n</body></html>''')


def table(headers, rows, caption):
    return '<div class="table-scroll" role="region" tabindex="0" aria-label="' + ESC(caption, quote=True) + '"><table class="cols"><caption>' + ESC(caption) + '</caption><thead><tr>' + ''.join('<th scope="col">' + ESC(h) + '</th>' for h in headers) + '</tr></thead><tbody>' + ''.join('<tr>' + ''.join('<td>' + ESC(str(v)) + '</td>' for v in row) + '</tr>' for row in rows) + '</tbody></table></div>'


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
        body += f'<aside><h2>How to read this profile</h2><p>{notes}</p></aside><h2>Source and method</h2><p>Pakistan Bureau of Statistics, Population and Housing Census 2023, Tables 1 and 12; processed by Data Darbar. The table and CSV are generated from the same district record used by the explorer.</p><p><a href="https://www.pbs.gov.pk/census/">PBS census publications</a> · <a href="/datasets/district-indicators/">Data dictionary and full district dataset</a> · <a href="/methods.html">Methodology</a></p>'
        body += f'<h2>Cite this profile</h2><p>Hiba Sameen / Data Darbar. {ESC(title)}. {ORIGIN}{path}. Derived data licensed CC BY 4.0; cite PBS as the original source.</p>'
        schema = {"@type": "Dataset", "name": title, "description": description + " " + notes, "url": ORIGIN + path, "creator": PERSON, "publisher": PUBLISHER, "license": LICENSE, "temporalCoverage": "2023", "spatialCoverage": {"@type": "Place", "name": f"{name} district, {province}, Pakistan"}, "isBasedOn": "https://www.pbs.gov.pk/census/", "distribution": [{"@type": "DataDownload", "encodingFormat": "text/csv", "contentUrl": ORIGIN + path + "census-2023.csv"}]}
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
        body += f'<h2>Reuse and citation</h2><p>Hiba Sameen / Data Darbar. {ESC(title)}. {ORIGIN}{path}. Cite the original source listed above and the catalogue release when reusing this table.</p><p><a href="/methods.html">Methodology</a> · <a href="https://adaad.org/datasets/">Adaad research datasets</a></p>'
        schema = {"@type": "Dataset", "name": title, "alternateName": d["name"], "description": d["description"] + " " + d["notes"], "url": ORIGIN + path, "license": LICENSE, "creator": PERSON, "publisher": PUBLISHER, "spatialCoverage": {"@type": "Place", "name": "Pakistan"}, "variableMeasured": [{"@type": "PropertyValue", "name": c["name"], "description": c["description"]} for c in d["columns"]], "distribution": [{"@type": "DataDownload", "encodingFormat": "application/vnd.apache.parquet", "contentUrl": ORIGIN + download}], "includedInDataCatalog": {"@type": "DataCatalog", "name": "Data Darbar", "url": ORIGIN + "/datasets/"}}
        page(path, title, d["description"], body, schema)
        dataset_links.append(f'<li><a href="{path}">{ESC(title)}</a><span>{ESC(d["description"])}</span></li>')
    page("/datasets/", "Pakistan open data catalogue", "Download documented census, trade, budget, national accounts, poverty and State Bank of Pakistan datasets. Each table has field definitions, source information and limitations.", '<ul class="cards">' + ''.join(dataset_links) + '</ul><p>The <a href="/query.html">browser query tool</a> opens these same tables. Read the units and warnings before combining or summing rows.</p>', {"@type": "DataCatalog", "name": "Data Darbar Pakistan open data catalogue", "url": ORIGIN + "/datasets/", "dataset": [{"@type": "Dataset", "name": DATASET_NAMES[d["name"]], "url": ORIGIN + "/datasets/" + d["name"].replace("_", "-") + "/", "description": d["description"]} for d in catalog["tables"]]})
    # About and Methodology merged into Methods when the redesign collapsed
    # them. They stay as redirects rather than as pages, so an old link still
    # lands and the same text is not published at three URLs - which is both a
    # duplicate-content problem and a way for two copies to drift.
    for path, title, what, frag in (
        ("/about.html", "About", "About Data Darbar", "#the-project"),
        ("/methodology.html", "Methodology", "Methodology and sources",
         "#places-the-map"),
    ):
        (APP / path.strip("/")).write_text(
            '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"/>\n'
            f'<title>Data Darbar \u2014 {title}</title>\n'
            '<meta name="robots" content="noindex"/>\n'
            f'<link rel="canonical" href="{ORIGIN}/methods.html"/>\n'
            f'<meta http-equiv="refresh" content="0; url=methods.html{frag}"/>\n'
            f"<script>location.replace('methods.html{frag}' + location.hash);</script>\n"
            '</head><body style="font-family:system-ui;padding:40px;'
            'background:#faf7ef;color:#17301f">\n'
            f'{what} now lives on the <a href="methods.html{frag}">Methods</a> '
            'page. Redirecting&hellip;\n</body></html>\n')

    for file, (title, description) in BASE_PAGES.items():
        patch_metadata(APP / file, title, description, "/" if file == "index.html" else "/" + file)
    sitemap = Element("urlset", xmlns="http://www.sitemaps.org/schemas/sitemap/0.9")
    paths = ["/" if p.name == "index.html" and p.parent == APP else "/" + str(p.relative_to(APP)).removesuffix("index.html") if p.name == "index.html" else "/" + str(p.relative_to(APP)) for p in APP.rglob("*.html") if not _retired(p)]
    for path in sorted(set(paths)):
        SubElement(SubElement(sitemap, "url"), "loc").text = ORIGIN + path
    (APP / "sitemap.xml").write_bytes(tostring(sitemap, encoding="utf-8", xml_declaration=True))
    (APP / "robots.txt").write_text(f"User-agent: *\nAllow: /\n\nSitemap: {ORIGIN}/sitemap.xml\n")
    print(f"Built {len(PROFILES)} district profiles, {len(catalog['tables'])} dataset pages and {len(set(paths))} sitemap entries.")


if __name__ == "__main__":
    build()
