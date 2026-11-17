"""District profiles: a page for each of PBS's 156 districts, and the index.

Called by build_seo.py, which owns the metadata, the chrome and the sitemap,
the way build_catalogue.py is. Everything here reads
app/data/district_profiles.json, which etl/places/build_profiles.py writes
from the warehouse, so the deploy renders the pages without DuckDB and a
profile cannot say what the map and the catalogue do not.

What a profile is for. These pages are where a search for a district lands,
so each one answers the question the searcher had - how many people, how
literate, how poor, how hot - in the first screen, then gives the full table
by subject, each figure beside the median district in the province and in
Pakistan and its rank, so a number can be read without a chart. Every row
links to the same indicator on the map, so the page is a door into Places,
not a dead end. The tehsils, the neighbouring districts and the sources are
at the bottom, and the whole table is one CSV.
"""
import csv
import html
import json
import math
import re
from pathlib import Path

E = html.escape
ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app"
PROFILES = APP / "data" / "district_profiles.json"

# The page, in reading order. Each section names the ids it shows; an id a
# district has no value for is left out, and a section with nothing is too.
SECTIONS = [
    ("people", "People", "Census 2023, with 2017 beside it",
     ["pop", "pop_2017", "pop_m", "pop_f", "trans", "growth", "area", "density", "urban", "urban_pop",
      "rural_pop", "hh_size", "sex_ratio", "under_15", "age_15_64", "age_60_plus"]),
    ("education", "Education", "Census 2023 and PSLM 2019-20",
     ["lit", "lit_f", "lit_m", "literate", "in_school_g", "in_school_b", "oos", "oos_g", "oos_b",
      "never_school", "matric_plus", "graduate", "ner", "ner_g", "ner_b", "pslm_lit", "numeracy",
      "private_school"]),
    ("work", "Work", "Census 2023, Labour Force Survey 2024-25 and Economic Census 2023",
     ["lfpr", "lfpr_f", "lfpr_m", "unemp", "unemp_f", "unemp_m", "employed", "paid_emp", "self_emp",
      "unpaid_fam", "lfs_lfpr", "lfs_unemp", "lfs_agri", "lfs_manu", "lfs_trade", "lfs_youth",
      "estab", "workforce", "workers_per_est", "est_manu", "est_trade", "est_services"]),
    ("health", "Health and children", "MICS, DHS 2017-18, PSLM 2019-20 and travel-time surfaces",
     ["stunting", "underweight", "wasting", "inst_delivery", "anc4", "sba", "birth_reg", "violent_disc",
      "careseek", "child_func", "ceb", "child_loss", "morbidity", "sought_tx", "govt_facility",
      "tt_mot", "tt_walk", "tt_mot_60", "tt_walk_60"]),
    ("women", "Women", "DHS 2017-18, MICS and HIES 2024-25",
     ["cpr", "cpr_modern", "unmet_need", "married_u18", "women_lit", "dv_justified",
      "not_permitted_work", "health_consulted"]),
    ("living", "Poverty and living standards", "PSLM 2019-20, MICS, HIES 2024-25 and FAO DIEM",
     ["mpi", "mpi_h", "mpi_a", "dep_electricity", "dep_water", "dep_sanitation", "dep_fuel", "dep_housing",
      "dep_schooling", "dep_attendance", "piped_water", "flush_toilet", "no_toilet", "open_def",
      "ecoli_source", "internet", "internet_f", "internet_m", "smartphone", "hh_mobile", "food_insecure",
      "r_electricity", "r_pucca", "r_katcha", "r_owner", "r_internet", "r_smartphone", "r_food_insecure",
      "diem_poor_fc", "diem_skipped", "diem_shock"]),
    ("agriculture", "Agriculture", "Agricultural Census 2024 and PBS crop statistics",
     ["farms", "farm_area", "cultivated", "farm_size", "small_farms", "large_area", "tenant", "owner_farms",
      "female_farms", "irrigated", "crop_intensity", "wheat_share", "cattle", "buffaloes", "sheep", "goats",
      "tractors", "tractors_per_farm", "tubewells", "solar_tubewells"]),
    ("environment", "Climate and environment", "ERA5-Land, CHIRPS, satellite land cover and emissions",
     ["tmean", "tanom", "tmax", "tmin", "rain", "rain_normal", "monsoon", "hot_days", "hot_days_normal",
      "warm_nights", "warm_spells", "pm25", "lc_crop", "lc_built", "lc_tree", "lc_short", "lc_bare", "lc_water",
      "grassland", "forest_loss_total", "water_stress", "water_extreme", "flood_peak", "flood_weeks",
      "ghg_km2", "ghg_agri", "ghg", "fires_rice", "fires_wheat"]),
    ("facilities", "Buildings and facilities", "Structures enumerated by Census 2023",
     ["f_school", "f_college", "f_university", "f_madrisah", "f_hospital", "f_mosque", "f_bank", "f_post",
      "f_police", "f_govt", "f_factory", "f_shop", "f_hotel"]),
    ("state", "Migration, courts and policing", "Bureau of Emigration, Law and Justice Commission, police yearbooks",
     ["emig", "emig_total", "pending", "instituted", "disposed", "clearance", "pending_100k", "judges", "crime"]),
]

# The tiles under the title: the eight figures a searcher most often wants.
TILES = ["pop", "growth", "area", "density", "urban", "lit", "mpi_h", "tt_walk"]
TILES_OUTSIDE = ["tmean", "tanom", "hot_days", "rain", "tt_walk", "pm25", "lc_tree", "f_school"]

# Ids whose value is read "lower is better" when a rank is explained. The
# rank itself is always 1 = highest; this only chooses the words.
LOWER_BETTER = {"unemp", "unemp_f", "unemp_m", "oos", "never_school", "stunting", "underweight", "wasting",
                "violent_disc", "child_loss", "morbidity", "tt_mot", "tt_walk", "tt_walk_60", "tt_mot_60",
                "unmet_need", "married_u18", "dv_justified", "not_permitted_work", "mpi", "mpi_h", "mpi_a",
                "no_toilet", "open_def", "ecoli_source", "food_insecure", "r_katcha", "r_food_insecure",
                "diem_poor_fc", "diem_skipped", "diem_shock", "pm25", "pending", "pending_100k", "crime"}

# Where a row's figure lives on the map: the picker group and indicator.
# Rows derived here (age shares, normals, courts) have no map twin.
MAP = {}


def fmt(v, kind):
    if v is None:
        return "—"
    if kind == "int":
        return f"{round(v):,}"
    if kind == "pct":
        return f"{v:,.1f}%"
    if kind == "pct0":
        return f"{round(v):,}%"
    if kind == "n1":
        return f"{v:,.1f}"
    if kind == "n2":
        return f"{v:,.2f}"
    if kind == "n3":
        return f"{v:,.3f}"
    if kind == "c":
        return f"{v:,.1f}°C"
    if kind == "c2":
        return f"{v:+,.2f}°C"
    if kind == "mm":
        return f"{round(v):,} mm"
    if kind == "min":
        return f"{round(v):,} min"
    if kind == "ha":
        return f"{round(v):,} ha"
    return f"{v:,}"


def short(v):
    """13,004,135 -> 13.0 million; 305,160 -> 305,160."""
    if v is None:
        return "—"
    if v >= 1e6:
        return f"{v/1e6:.1f} million"
    return f"{round(v):,}"


def ordinal(n):
    return f"{n}{'th' if 10 <= n % 100 <= 20 else {1: 'st', 2: 'nd', 3: 'rd'}.get(n % 10, 'th')}"


# ── drawings: small, static, inline ─────────────────────────────────────────

def svg_bars(pts, fmt_kind="int", w=320, h=96, cls="dp-bars", label=None):
    """Population at each census: a bar per census, labelled at both ends."""
    if not pts:
        return ""
    vals = [v for _, v in pts]
    top = max(vals) or 1
    n = len(pts)
    pad_b, pad_t = 18, 14
    gap = 6
    bw = (w - gap * (n - 1)) / n
    out = [f'<svg class="{cls}" viewBox="0 0 {w} {h}" role="img" aria-label="{E(label or "", quote=True)}">']
    for i, (y, v) in enumerate(pts):
        bh = (h - pad_b - pad_t) * v / top
        x = i * (bw + gap)
        out.append(f'<rect x="{x:.1f}" y="{h - pad_b - bh:.1f}" width="{bw:.1f}" height="{bh:.1f}" rx="2"/>')
        out.append(f'<text x="{x + bw/2:.1f}" y="{h - 4}" text-anchor="middle" class="dp-ax">{y}</text>')
        if i in (0, n - 1):
            # anchored inward at the ends, so a label never leaves the drawing
            anchor = "start" if i == 0 else "end"
            tx = x if i == 0 else x + bw
            out.append(f'<text x="{tx:.1f}" y="{h - pad_b - bh - 4:.1f}" text-anchor="{anchor}" class="dp-val">{short(v) if fmt_kind == "int" else fmt(v, fmt_kind)}</text>')
    out.append("</svg>")
    return "".join(out)


def svg_line(pts, normal=None, w=320, h=90, cls="dp-line", label=None, unit=""):
    """A yearly series as a line, with the 1981–2010 average dashed across it
    and the first and last years labelled. Enough to see a trend; not a chart
    to read values from, which is what the map is for."""
    if not pts or len(pts) < 3:
        return ""
    ys = [y for y, _ in pts]
    vs = [v for _, v in pts]
    lo, hi = min(vs + ([normal] if normal is not None else [])), max(vs + ([normal] if normal is not None else []))
    if hi == lo:
        hi = lo + 1
    pad_b, pad_t, pad_x = 16, 10, 4
    def X(y): return pad_x + (w - 2 * pad_x) * (y - ys[0]) / max(1, ys[-1] - ys[0])
    def Y(v): return pad_t + (h - pad_b - pad_t) * (1 - (v - lo) / (hi - lo))
    path = "M" + "L".join(f"{X(y):.1f} {Y(v):.1f}" for y, v in pts)
    out = [f'<svg class="{cls}" viewBox="0 0 {w} {h}" role="img" aria-label="{E(label or "", quote=True)}">']
    if normal is not None:
        out.append(f'<line class="dp-normal" x1="{pad_x}" x2="{w - pad_x}" y1="{Y(normal):.1f}" y2="{Y(normal):.1f}"/>')
    out.append(f'<path d="{path}"/>')
    out.append(f'<circle cx="{X(ys[-1]):.1f}" cy="{Y(vs[-1]):.1f}" r="2.6"/>')
    out.append(f'<text x="{pad_x}" y="{h - 3}" class="dp-ax">{ys[0]}</text>')
    out.append(f'<text x="{w - pad_x}" y="{h - 3}" text-anchor="end" class="dp-ax">{ys[-1]}</text>')
    out.append("</svg>")
    return "".join(out)


def svg_anomaly(pts, w=320, h=90, cls="dp-anom", label=None):
    """Temperature against the normal: bars above and below a zero line,
    warm years one colour and cool years another."""
    if not pts:
        return ""
    ys = [y for y, _ in pts]
    vs = [v for _, v in pts]
    m = max(abs(min(vs)), abs(max(vs)), 0.5)
    pad_b, pad_t = 16, 6
    mid = pad_t + (h - pad_b - pad_t) / 2
    scale = (h - pad_b - pad_t) / 2 / m
    bw = (w - 4) / len(pts)
    out = [f'<svg class="{cls}" viewBox="0 0 {w} {h}" role="img" aria-label="{E(label or "", quote=True)}">']
    for i, (y, v) in enumerate(pts):
        x = 2 + i * bw
        bh = abs(v) * scale
        out.append(f'<rect class="{"warm" if v >= 0 else "cool"}" x="{x:.1f}" y="{(mid - bh if v >= 0 else mid):.1f}" width="{max(bw - 0.6, 0.8):.1f}" height="{max(bh, 0.4):.1f}"/>')
    out.append(f'<line class="dp-zero" x1="2" x2="{w - 2}" y1="{mid:.1f}" y2="{mid:.1f}"/>')
    out.append(f'<text x="2" y="{h - 3}" class="dp-ax">{ys[0]}</text>')
    out.append(f'<text x="{w - 2}" y="{h - 3}" text-anchor="end" class="dp-ax">{ys[-1]}</text>')
    out.append("</svg>")
    return "".join(out)


def comp_bars(items, limit=6, cls="dp-comp"):
    """Shares as labelled horizontal bars: mother tongue, religion."""
    items = [it for it in items if it[1] is not None]
    if not items:
        return ""
    top = items[:limit]
    rest = sum(v for _, v in items[limit:])
    if rest > 0.05:
        top = top + [["Others", round(rest, 2)]]
    mx = max(v for _, v in top) or 1
    rows = "".join(
        f'<div class="dp-comp-row"><span class="dp-comp-k">{E(k)}</span>'
        f'<span class="dp-comp-bar"><i style="width:{max(1.5, 100*v/mx):.1f}%"></i></span>'
        f'<span class="dp-comp-v">{v:.1f}%</span></div>' for k, v in top)
    return f'<div class="{cls}">{rows}</div>'


# ── the locator map ─────────────────────────────────────────────────────────

KARACHI = ["146", "147", "148", "149", "150", "154", "166"]   # Karachi division's seven
KARACHI_CITY = ["146", "147", "149", "150", "154", "166"]       # the six the city is; Malir is the hinterland
SMALL_PX = 9          # a district narrower than this on the national map gets an inset


def _geo():
    js = (APP / "data" / "districts_2023_geo.js").read_text()
    return json.loads(js[js.index("{"):js.rindex("}") + 1])


class Locator:
    """Every district projected once, at the national map's scale, so the
    same coordinates draw the small national locator, the zoomed inset a
    small district needs, and the index's clickable map.

    Karachi Central is two pixels wide on a 230-pixel Pakistan, and twenty
    other districts are under nine. For those the page shows the country
    with a rectangle on it, and under that the rectangle's contents at a
    size where the district has a shape and neighbours."""

    def __init__(self, max_w=230, max_h=250, step=3.2):
        g = _geo()
        k = math.cos(math.radians(30))
        raw = []
        for f in g["features"]:
            geom = f["geometry"]
            polys = geom["coordinates"] if geom["type"] == "MultiPolygon" else [geom["coordinates"]]
            for poly in polys:
                raw.append((f["properties"]["code"], poly[0]))
        xs = [p[0] * k for _, r in raw for p in r]
        ys = [p[1] for _, r in raw for p in r]
        x0, x1, y0, y1 = min(xs), max(xs), min(ys), max(ys)
        s = min(max_w / (x1 - x0), max_h / (y1 - y0))
        self.w, self.h = (x1 - x0) * s, (y1 - y0) * s
        self.rings = [(code, [((lon * k - x0) * s, (y1 - lat) * s) for lon, lat in r]) for code, r in raw]
        self.bbox = {}
        for code, pts in self.rings:
            bx = [x for x, _ in pts]
            by = [y for _, y in pts]
            b = self.bbox.get(code)
            self.bbox[code] = (min(bx), min(by), max(bx), max(by)) if b is None else \
                (min(b[0], min(bx)), min(b[1], min(by)), max(b[2], max(bx)), max(b[3], max(by)))
        self.step = step
        self.paths = {}
        for code, pts in self.rings:
            d = self._path(pts, step)
            if d:
                self.paths[code] = self.paths.get(code, "") + d

    @staticmethod
    def _thin(pts, step, dp):
        out, last = [], None
        for x, y in pts:
            q = (round(x, dp), round(y, dp))
            if last is None or abs(q[0] - last[0]) + abs(q[1] - last[1]) >= step:
                out.append(q)
                last = q
        return out

    def _path(self, pts, step, dp=0):
        """One ring as a path. Thinning that leaves fewer than three points
        has eaten a district whole - Karachi Central and South are a few
        pixels at this size - so such a ring is drawn from its full outline
        at a tenth of a pixel instead of dropped."""
        q = self._thin(pts, step, dp)
        if len(q) < 3:
            q = self._thin(pts, 0, 1)
        if len(q) < 3:
            return ""
        d = f"M{q[0][0]:g} {q[0][1]:g}"
        for (a, b), (c, e) in zip(q, q[1:]):
            d += f"l{round(c - a, 1):g} {round(e - b, 1):g}"
        return d + "Z"

    def size(self, code):
        b = self.bbox[code]
        return max(b[2] - b[0], b[3] - b[1])

    def window(self, code):
        """The rectangle an inset shows, in national coordinates, or None
        when the district is large enough to be seen as it is. Karachi's
        districts share one window: the division, which is the city."""
        if code in KARACHI:
            # framed on the six urban districts: Malir is nine times their
            # size and would shrink them to the corner of their own inset
            bs = [self.bbox[c] for c in KARACHI_CITY]
            x0, y0, x1, y1 = min(b[0] for b in bs), min(b[1] for b in bs), max(b[2] for b in bs), max(b[3] for b in bs)
            pad = 0.12 * max(x1 - x0, y1 - y0)
            return (x0 - pad, y0 - pad, x1 + pad, y1 + pad)
        if self.size(code) >= SMALL_PX:
            return None
        b = self.bbox[code]
        cx, cy = (b[0] + b[2]) / 2, (b[1] + b[3]) / 2
        half = max(5 * self.size(code), 24) / 2
        return (cx - half, cy - half, cx + half, cy + half)

    def _zoom_paths(self, win, codes, out_w):
        x0, y0, x1, y1 = win
        f = out_w / (x1 - x0)
        out = {}
        for code, pts in self.rings:
            if codes is not None and code not in codes:
                continue
            b = self.bbox[code]
            if b[2] < x0 or b[0] > x1 or b[3] < y0 or b[1] > y1:
                continue
            z = [((x - x0) * f, (y - y0) * f) for x, y in pts]
            d = self._path(z, 2.5, 1)
            if d:
                out[code] = out.get(code, "") + d
        return out, out_w, (y1 - y0) * f

    def svg(self, code, name):
        """The national locator; with the inset's rectangle drawn on it
        when there is an inset."""
        others = "".join(d for c, d in self.paths.items() if c != code)
        win = self.window(code)
        rect = ""
        if win:
            rect = (f'<rect class="dp-loc-win" x="{win[0]:.1f}" y="{win[1]:.1f}" '
                    f'width="{win[2] - win[0]:.1f}" height="{win[3] - win[1]:.1f}"/>')
        return (f'<svg class="dp-locator" viewBox="0 0 {self.w:.0f} {self.h:.0f}" role="img" '
                f'aria-label="{E(name, quote=True)} district within Pakistan">'
                f'<path class="dp-loc-all" d="{others}"/>'
                f'<path class="dp-loc-this" d="{self.paths.get(code, "")}"/>{rect}</svg>')

    def inset(self, code, name, by_code):
        """The zoomed inset, or an empty string."""
        win = self.window(code)
        if not win:
            return ""
        only = KARACHI if code in KARACHI else None
        paths, w, h = self._zoom_paths(win, only, 230)
        others = "".join(d for c, d in paths.items() if c != code)
        # label the neighbours that have room for a word
        labels = []
        for c, d in paths.items():
            if c == code or c not in by_code:
                continue
            b = self.bbox[c]
            f = 230 / (win[2] - win[0])
            lx, ly = ((b[0] + b[2]) / 2 - win[0]) * f, ((b[1] + b[3]) / 2 - win[1]) * f
            nm = by_code[c]["name"]
            room = min(b[2] - b[0], b[3] - b[1]) * f
            # a word is about four pixels a letter at this size; a name that
            # would run past its district is left off rather than clipped
            if 6 < lx < w - 6 and 6 < ly < h - 4 and room > 28 and len(nm) * 4 < max(room, (b[2] - b[0]) * f) + 12:
                labels.append(f'<text x="{lx:.0f}" y="{ly:.0f}">{E(nm)}</text>')
        cap = ("Karachi\u2019s districts; Malir runs beyond the frame" if code in KARACHI else f"{name} and its neighbours")
        return (f'<svg class="dp-locator dp-inset" viewBox="0 0 {w:.0f} {h:.0f}" role="img" '
                f'aria-label="{E(cap, quote=True)}; {E(name, quote=True)} highlighted">'
                f'<path class="dp-loc-all" d="{others}"/>'
                f'<path class="dp-loc-this" d="{paths.get(code, "")}"/>{"".join(labels)}</svg>'
                f'<p class="dp-map-cap">{cap}</p>')

    def hero(self, code, name, by_code):
        """The map beside the title. A district large enough to see gets the
        country with itself filled in. A small one gets the zoom as the map
        and the country as a thumbnail beside it, marked where the zoom is.

        The two were stacked at first, country over zoom, which made the map
        column twice the height of the words beside it and left 200 pixels
        of nothing under the buttons. Side by side the zoom is the same
        height as the plain locator, so every profile's top is one size."""
        zoom = self.inset(code, name, by_code)
        if not zoom:
            return f'<div class="dp-hero-map">{self.svg(code, name)}</div>'
        return (f'<div class="dp-hero-map dp-hero-zoom"><div class="dp-zoom-nat">{self.svg(code, name)}'
                f'<p class="dp-map-cap">In Pakistan</p></div><div class="dp-zoom-in">{zoom}</div></div>')

    def index_map(self, districts):
        """The index's map: every district a link to its profile, and
        Karachi again at a size where its districts can be told apart."""
        by = {d["code"]: d for d in districts}

        def link(code, d):
            dd = by.get(code)
            if not dd:
                return ""
            return (f'<a href="/districts/{dd["slug"]}/" data-n="{E(dd["name"] + ", " + dd["province"], quote=True)}" '
                    f'aria-label="{E(dd["name"], quote=True)}, {E(dd["province"], quote=True)}"><path d="{d}"/></a>')
        parts = [f'<svg class="dp-pkmap" viewBox="0 0 {self.w:.0f} {self.h:.0f}" role="img" '
                 f'aria-label="Pakistan\u2019s districts; choose one to open its profile">']
        parts += [link(c, d) for c, d in self.paths.items()]
        # Karachi, top left, where the map has only Afghanistan
        win = self.window(KARACHI[0])
        kp, kw, kh = self._zoom_paths(win, KARACHI, 62)
        ox, oy = 4, 4
        parts.append(f'<g class="dp-pkmap-inset" transform="translate({ox} {oy})">'
                     f'<rect x="-3" y="-3" width="{kw + 6:.0f}" height="{kh + 16:.0f}" rx="4"/>'
                     + "".join(link(c, d) for c, d in kp.items())
                     + f'<text x="{kw / 2:.0f}" y="{kh + 10:.0f}">Karachi</text></g>')
        parts.append("</svg>")
        return "".join(parts)


# ── a profile ───────────────────────────────────────────────────────────────

def lede(d, I):
    v = d["values"]
    n, prov = d["name"], d["province"]
    s = []
    if "pop" in v:
        t = f"{n} district, {prov}, had {fmt(v['pop']['v'], 'int')} people at the 2023 census"
        if "growth" in v:
            t += f", growing {v['growth']['v']:.2f}% a year since 2017"
        r, n_ = v['pop']['r'], I['pop']['n']
        t += ", the most populous of the " if r == 1 else f", the {ordinal(r)} most populous of the "
        t += f"{n_} districts with a 2023 figure."
        s.append(t)
        bits = []
        if "urban" in v:
            bits.append(f"{v['urban']['v']:.0f}% of people live in urban areas")
        if "lit" in v:
            bits.append(f"{v['lit']['v']:.1f}% of those aged 10 and over can read and write")
        if bits:
            b = " and ".join(bits)
            s.append(b[:1].upper() + b[1:] + ".")
    else:
        s.append(f"{n} district, {prov}, lies outside the tables PBS has published from Census 2023, so this profile draws on surveys and satellite sources.")
    if "mpi_h" in v:
        s.append(f"{v['mpi_h']['v']:.1f}% of people were multidimensionally poor in 2019-20.")
    if "tt_walk" in v:
        m = v['tt_walk']['v']
        hours = f" (about {m/60:.1f} hours)" if m >= 90 else ""
        s.append(f"The median resident is {round(m):,} minutes{hours} on foot from the nearest health facility.")
    if "tmean" in v and "tanom" in v:
        t = f"In 2025 the mean temperature was {v['tmean']['v']:.1f}°C, {abs(v['tanom']['v']):.2f}°C {'above' if v['tanom']['v'] >= 0 else 'below'} the 1981–2010 normal"
        if "hot_days" in v:
            t += f", with {int(v['hot_days']['v'])} days at 40°C or more"
        s.append(t + ".")
    return " ".join(s)


def description(d, I):
    v = d["values"]
    parts = []
    if "pop" in v:
        parts.append(f"population {short(v['pop']['v'])}")
    if "lit" in v:
        parts.append(f"literacy {v['lit']['v']:.1f}%")
    if "mpi_h" in v:
        parts.append(f"poverty {v['mpi_h']['v']:.1f}%")
    if "tt_walk" in v:
        parts.append(f"{fmt(v['tt_walk']['v'], 'min')} walk to care")
    if "tmean" in v:
        parts.append(f"mean temperature {v['tmean']['v']:.1f}°C")
    head = f"{d['name']} district, {d['province']}: " + ", ".join(parts) + "." if parts else f"{d['name']} district, {d['province']}."
    return head + " Census 2023, PSLM, MICS, labour force, agriculture, climate and satellite figures with ranks, tehsils and a CSV download."


def row_html(iid, rec, I, d, census_cols):
    ind = I[iid]
    kind = ind["fmt"]
    label = E(ind["label"])
    m = MAP.get(iid)
    if m:
        g, i, y = m
        q = f"/places.html?lv=district&g={g}&i={i}" + (f"&y={y}" if y else "") + f"&p={d['code']}"
        label = f'<a href="{E(q)}" title="See this indicator on the map">{label}</a>'
    flag = ' <abbr class="dp-lown" title="Small survey sample: read with caution">†</abbr>' if rec.get("low_n") else ""
    if rec.get("fp"):
        flag += f' <abbr class="dp-lown" title="One figure for {E(", ".join(rec["fp"]), quote=True)} together: the source published it for the district before it was divided">‡</abbr>'
    cells = [f"<th scope=\"row\">{label}</th>", f'<td class="dp-v">{fmt(rec["v"], kind)}{flag}</td>']
    if census_cols:
        cells.append(f'<td class="dp-prev">{fmt(rec.get("prev"), kind) if rec.get("prev") is not None else ""}</td>')
    cells.append(f'<td class="dp-rank">{rec["r"]} <span>of {ind["n"]}</span></td>')
    pm = ind["prov_median"].get(d["province"])
    cells.append(f'<td class="dp-med">{fmt(pm, kind) if pm is not None else "—"}</td>')
    cells.append(f'<td class="dp-med">{fmt(ind["median"], kind)}</td>')
    # the id is what the page's search scrolls to and what a shared link names
    return f'<tr id="i-{iid}">' + "".join(cells) + "</tr>"


def section_html(sec, d, I):
    key, title, sub, ids = sec
    ids = [i for i in ids if i in d["values"]]
    if not ids:
        return ""
    census_cols = key in ("people", "education", "work") and any(d["values"][i].get("prev") is not None for i in ids)
    head = ("<thead><tr><th scope=\"col\">Indicator</th><th scope=\"col\">" + E(d["name"]) + "</th>"
            + ("<th scope=\"col\">2017</th>" if census_cols else "")
            + "<th scope=\"col\"><abbr title=\"1 = the highest value among the districts with a figure\">Rank</abbr></th>"
            + f"<th scope=\"col\"><abbr title=\"The median district in {E(d['province'], quote=True)}\">{E(d['province'])}</abbr></th>"
            + "<th scope=\"col\"><abbr title=\"The median district in Pakistan\">Pakistan</abbr></th></tr></thead>")
    rows = "".join(row_html(i, d["values"][i], I, d, census_cols) for i in ids)
    extra = ""
    if key == "people":
        figs = []
        h = d["series"].get("population")
        if h and h.get("pts"):
            cap = "Population at each census" + (f", {E(h['label'])} together: PBS cannot separate them in every census" if h["relation"] == "combined" else "")
            figs.append(f'<figure class="dp-fig"><figcaption>{cap}</figcaption>{svg_bars(h["pts"], label=cap)}</figure>')
        if d["tongues"]:
            figs.append(f'<figure class="dp-fig"><figcaption>Mother tongue, Census 2023</figcaption>{comp_bars(d["tongues"])}</figure>')
        if d["religions"]:
            figs.append(f'<figure class="dp-fig"><figcaption>Religion, Census 2023</figcaption>{comp_bars(d["religions"], limit=5)}</figure>')
        if figs:
            extra = '<div class="dp-figs">' + "".join(figs) + "</div>"
    if key == "environment":
        figs = []
        s = d["series"]
        if s.get("tanom"):
            figs.append(f'<figure class="dp-fig"><figcaption>Temperature against the 1981–2010 normal, 1950–2025</figcaption>{svg_anomaly(s["tanom"], label="Yearly temperature anomaly")}</figure>')
        if s.get("rain"):
            nrm = d["values"].get("rain_normal", {}).get("v")
            figs.append(f'<figure class="dp-fig"><figcaption>Rainfall each year, 1981–2025, with the 1981–2010 average</figcaption>{svg_line(s["rain"], nrm, label="Yearly rainfall")}</figure>')
        if s.get("hot_days"):
            nrm = d["values"].get("hot_days_normal", {}).get("v")
            figs.append(f'<figure class="dp-fig"><figcaption>Days reaching 40°C each year, 1981–2025, with the 1981–2010 average</figcaption>{svg_line(s["hot_days"], nrm, label="Days at 40°C or more")}</figure>')
        if figs:
            extra = '<div class="dp-figs">' + "".join(figs) + "</div>"
    if key == "agriculture" and d["crops"]:
        extra += crops_table(d["crops"])
    if key == "state" and d["series"].get("emigrants"):
        extra = f'<div class="dp-figs"><figure class="dp-fig"><figcaption>Emigrants registered each year, 2011–2024</figcaption>{svg_line(d["series"]["emigrants"], label="Registered emigrants by year")}</figure></div>'
    return (f'<section class="dp-sec" id="{key}"><div class="dp-sec-head"><h2>{E(title)}</h2><p>{E(sub)}</p></div>'
            + extra
            + f'<div class="dp-table"><table><caption class="visually-hidden">{E(title)}: {E(d["name"])} against the median district</caption>{head}<tbody>{rows}</tbody></table></div></section>')


CROPS_FY = None


def row_slug(s):
    return re.sub(r"[^a-z0-9]+", "-", str(s).lower()).strip("-")


def crops_table(crops):
    def num(v, dp):
        return "—" if v is None else f"{v:,.{dp}f}"
    # an address per row, like the indicator rows: the page's search writes
    # it to the URL, and a row without one left the previous row's there
    rows = "".join(
        f'<tr id="i-crop-{row_slug(c["crop"])}"><th scope="row">{E(c["crop"])}</th><td class="dp-v">{num(c["area_000ha"], 1)}</td>'
        f'<td class="dp-v">{num(c["production_000t"], 1)}</td><td class="dp-v">{num(c["yield_t_ha"], 2)}</td></tr>'
        for c in crops)
    return (f'<div class="dp-table dp-table-narrow" data-find="Main crops, area sown"><table><caption>Main crops by area sown, {E(str(CROPS_FY))} (PBS crop statistics)</caption>'
            '<thead><tr><th scope="col">Crop</th><th scope="col">Area (thousand ha)</th>'
            '<th scope="col">Production (thousand t)</th><th scope="col">Yield (t/ha)</th></tr></thead>'
            f'<tbody>{rows}</tbody></table></div>')


def tiles_html(d, I):
    v = d["values"]
    ids = [i for i in (TILES_OUTSIDE if d["outside_census"] else TILES) if i in v]
    if len(ids) < 4:
        ids = [i for i in TILES + TILES_OUTSIDE if i in v][:8]
    out = []
    for i in ids:
        rec, ind = v[i], I[i]
        rank = f"{ordinal(rec['r'])} of {ind['n']}"
        if i in LOWER_BETTER:
            rank += " (1 = highest)"
        out.append(f'<div class="dp-tile"><span class="dp-tile-v">{fmt(rec["v"], ind["fmt"])}</span>'
                   f'<span class="dp-tile-k">{E(ind["label"])}</span><span class="dp-tile-s">{rank}</span></div>')
    return '<div class="dp-tiles">' + "".join(out) + "</div>"


def tehsils_html(d):
    if not d["tehsils"]:
        return ""
    has_pop = any(t["pop"] is not None for t in d["tehsils"])
    rows = "".join(
        f'<tr id="i-tehsil-{row_slug(t["id"])}"><th scope="row"><a href="/places.html?lv=tehsil&g=demographics&i=pop_total&p={E(t["id"])}">{E(t["name"])}</a></th>'
        f'<td>{E(t["kind"])}</td>' + (f'<td class="dp-v">{fmt(t["pop"], "int")}</td>' if has_pop else "") + "</tr>"
        for t in d["tehsils"])
    n = len(d["tehsils"])
    return (f'<section class="dp-sec" id="tehsils"><div class="dp-sec-head"><h2>Tehsils</h2><p>The {n} sub-district unit{"s" if n != 1 else ""} PBS drew for the 2023 census' + (", with their population" if has_pop else "") + '. Each opens on the tehsil map.</p></div>'
            f'<div class="dp-table dp-table-narrow"><table><thead><tr><th scope="col">Name</th><th scope="col">Unit</th>' + ('<th scope="col">Population 2023</th>' if has_pop else "") + f'</tr></thead><tbody>{rows}</tbody></table></div></section>')


def nearby_html(d, districts):
    sib = sorted((x for x in districts if x["province"] == d["province"] and x["code"] != d["code"]), key=lambda x: x["name"])
    if not sib:
        return ""
    chips = "".join(f'<a class="dp-chip" href="/districts/{x["slug"]}/">{E(x["name"])}</a>' for x in sib)
    return (f'<section class="dp-sec" id="nearby"><div class="dp-sec-head"><h2>Other districts in {E(d["province"])}</h2></div>'
            f'<div class="dp-chips">{chips}</div></section>')


def notes_html(d, I):
    used = {I[i]["group"] for i in d["values"] if I[i]["group"]}
    notes = list(d["notes"])
    # the warehouse writes one note per way of combining a shape; the reader
    # needs the fact once
    seen, merged = set(), []
    for n in notes:
        key = n.split(";")[0]
        if key not in seen:
            seen.add(key)
            merged.append(n if ";" not in n else key + "; their figures are combined here")
    low = any(r.get("low_n") for r in d["values"].values())
    items = []
    if d["outside_census"]:
        items.append("PBS has not published Census 2023 district tables for Gilgit-Baltistan or Azad Jammu and Kashmir, so the census rows are absent and the surveys that reach here are shown instead.")
    items.append("Rank 1 is the highest value among the districts that have a figure, whichever way is better. The province and Pakistan columns are the median district, not a population-weighted total.")
    items.append("Survey estimates (PSLM, MICS, DHS, LFS, HIES) are indicative at district level: the surveys were designed for provinces, and Data Darbar post-stratifies and suppresses small samples." + (" Rows marked † rest on a small sample." if low else ""))
    if any(r.get("fp") for r in d["values"].values()):
        items.append("Rows marked ‡ are one figure the source published for the district before it was divided, shown for each of its successors; it counts once in the ranks and medians.")
    items.append("Climate figures are district means of ERA5-Land and CHIRPS grids; the normal is 1981–2010. Travel times are from the Malaria Atlas accessibility surfaces weighted by where people live.")
    items += merged
    return ('<section class="dp-sec" id="notes"><div class="dp-sec-head"><h2>How to read this profile</h2></div><ul class="dp-notes">'
            + "".join(f"<li>{E(n)}</li>" for n in items) + "</ul></section>")


def sources_html(d, I, S):
    groups = sorted({I[i]["group"] for i in d["values"] if I[i]["group"] and I[i]["group"] in S})
    seen, items = set(), []
    for g in groups:
        s = S[g]
        key = (s["dataset"], s["source"])
        if key in seen:
            continue
        seen.add(key)
        items.append(f"<li><b>{E(s['dataset'])}</b> — {E(s['source'])}</li>")
    items.append("<li><b>Population since 1951</b> — PBS censuses, restated on footprints PBS can separate in every census.</li>")
    if "pending" in d["values"]:
        items.append("<li><b>District courts</b> — Law and Justice Commission of Pakistan, annual judicial statistics, matched to the 2023 district by name.</li>")
    if "crime" in d["values"]:
        items.append("<li><b>Reported offences</b> — Azad Jammu and Kashmir Statistical Year Book, police tables.</li>")
    return ('<section class="dp-sec" id="sources"><div class="dp-sec-head"><h2>Sources</h2><p>Every figure is read from the tables in the <a href="/datasets/">catalogue</a>; the <a href="/datasets/place-indicators/">place_indicators</a> table holds most of them on the 2023 frame.</p></div>'
            f'<ul class="dp-sources">{"".join(items)}</ul>'
            f'<p class="dp-cite">Cite as: Hiba Sameen / Data Darbar, “{E(d["name"])} district profile”, {ORIGIN}/districts/{d["slug"]}/. Derived data CC BY 4.0; cite the original sources named above. <a href="/methods.html">Methods</a>.</p></section>')


ORIGIN = "https://darbar.adaad.org"


def write_csv(path, d, I):
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["district", "district_code", "province", "section", "indicator", "label", "year", "value",
                    "value_2017", "rank", "districts_with_value", "median_province", "median_pakistan", "small_sample", "shared_with"])
        for key, title, _, ids in SECTIONS:
            for i in ids:
                if i not in d["values"]:
                    continue
                r, ind = d["values"][i], I[i]
                w.writerow([d["name"], d["code"], d["province"], title, i, ind["label"], ind["year"] or "",
                            r["v"], r.get("prev", ""), r["r"], ind["n"], ind["prov_median"].get(d["province"], ""),
                            ind["median"], "yes" if r.get("low_n") else "", "; ".join(r.get("fp", []))])
        for t in d["tehsils"]:
            w.writerow([d["name"], d["code"], d["province"], "Tehsils", t["id"], t["name"] + " (" + t["kind"] + ")", "2023",
                        "" if t["pop"] is None else t["pop"], "", "", "", "", "", ""])
        for c in d["crops"]:
            w.writerow([d["name"], d["code"], d["province"], "Crops", "area_000ha", c["crop"] + " area (thousand ha)", CROPS_FY, c["area_000ha"], "", "", "", "", "", ""])
        for name, share in d["tongues"]:
            w.writerow([d["name"], d["code"], d["province"], "Mother tongue", "share", name + " (%)", "2023", share, "", "", "", "", "", ""])
        for name, share in d["religions"]:
            w.writerow([d["name"], d["code"], d["province"], "Religion", "share", name + " (%)", "2023", share, "", "", "", "", "", ""])


def profile_page(d, P, loc, seo):
    I, S = P["indicators"], P["sources"]
    path = f"/districts/{d['slug']}/"
    title = f"{d['name']} district: population, literacy, poverty, work and climate" if not d["outside_census"] \
        else f"{d['name']} district: people, health access, land and climate"
    desc = description(d, I)
    toc_ids = [(k, t) for k, t, _, ids in SECTIONS if any(i in d["values"] for i in ids)]
    if d["tehsils"]:
        toc_ids.append(("tehsils", "Tehsils"))
    toc_ids.append(("sources", "Sources"))
    # Find an indicator: completes over this page's own rows (districts.js
    # reads them from the tables), so it is hidden until the script runs.
    find = ('<div class="dp-find" role="search" hidden><label class="dp-find-box">'
            '<span class="visually-hidden">Find an indicator on this page</span>'
            '<svg width="15" height="15" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8" aria-hidden="true"><circle cx="7" cy="7" r="4.5"/><path d="M10.5 10.5 14 14"/></svg>'
            f'<input type="search" id="dpInd" data-code="{E(d["code"], quote=True)}" placeholder="Find an indicator: literacy, rainfall…" '
            'autocomplete="off" spellcheck="false" role="combobox" aria-label="Find an indicator on this page" '
            'aria-autocomplete="list" aria-controls="dpIndList" aria-expanded="false"/><kbd aria-hidden="true">/</kbd></label>'
            '<ul class="dp-find-list" id="dpIndList" role="listbox" hidden></ul></div>')
    toc = ('<div class="dp-toc">' + find + '<nav class="dp-toc-links" aria-label="On this page">'
           + "".join(f'<a href="#{k}">{E(t)}</a>' for k, t in toc_ids) + "</nav></div>")
    map_q = f"/places.html?lv=district&g=demographics&i=pop_total&p={d['code']}" if not d["outside_census"] \
        else f"/places.html?lv=district&g=climateTmean&i=tmean_c&y=2025&p={d['code']}"
    body = (
        f'<nav class="dp-crumb" aria-label="Breadcrumb"><a href="/districts/">Districts</a> › '
        f'<a href="/districts/#{E(prov_id(d["province"]))}">{E(d["province"])}</a> › <span>{E(d["name"])}</span></nav>'
        f'<header class="dp-hero"><div class="dp-hero-text"><p class="dp-kicker">District profile · {E(d["province"])}</p>'
        f'<h1>{E(d["name"])} district</h1><p class="dp-lede">{E(lede(d, I))}</p>'
        f'<div class="dp-actions"><a class="dp-btn primary" href="{E(map_q)}">Open in the map</a>'
        f'<a class="dp-btn" href="profile.csv" download>Download the profile (CSV)</a>'
        f'<a class="dp-btn" href="/query.html">Query the tables</a></div></div>'
        + loc.hero(d["code"], d["name"], {x["code"]: x for x in P["districts"]}) + '</header>'
        + tiles_html(d, I) + toc
        + "".join(section_html(sec, d, I) for sec in SECTIONS)
        + tehsils_html(d) + nearby_html(d, P["districts"]) + notes_html(d, I) + sources_html(d, I, S))
    schema = [
        {"@type": "Dataset", "name": f"{d['name']} district profile", "description": desc,
         "url": seo.ORIGIN + path, "creator": seo.PERSON, "publisher": seo.PUBLISHER, "license": seo.LICENSE,
         "temporalCoverage": "1951/2025",
         "spatialCoverage": {"@type": "AdministrativeArea", "name": f"{d['name']} District",
                             "containedInPlace": {"@type": "AdministrativeArea", "name": d["province"],
                                                  "containedInPlace": {"@type": "Country", "name": "Pakistan"}}},
         "isBasedOn": ["https://www.pbs.gov.pk/census/", seo.ORIGIN + "/datasets/place-indicators/"],
         "variableMeasured": [{"@type": "PropertyValue", "name": I[i]["label"], "value": d["values"][i]["v"]}
                              for i in (TILES if not d["outside_census"] else TILES_OUTSIDE) if i in d["values"]],
         "distribution": [{"@type": "DataDownload", "encodingFormat": "text/csv", "contentUrl": seo.ORIGIN + path + "profile.csv"}]},
        {"@type": "BreadcrumbList", "itemListElement": [
            {"@type": "ListItem", "position": 1, "name": "Districts", "item": seo.ORIGIN + "/districts/"},
            {"@type": "ListItem", "position": 2, "name": d["province"], "item": seo.ORIGIN + "/districts/#" + prov_id(d["province"])},
            {"@type": "ListItem", "position": 3, "name": d["name"] + " district", "item": seo.ORIGIN + path}]},
    ]
    page_shell(path, title, desc, body, schema, seo)
    write_csv(APP / path.strip("/") / "profile.csv", d, I)
    return title


def prov_id(p):
    return re.sub(r"[^a-z0-9]+", "-", p.lower()).strip("-")


def index_page(P, loc, seo):
    I = P["indicators"]
    ds = P["districts"]
    provs = []
    for p in ["Punjab", "Sindh", "Khyber Pakhtunkhwa", "Balochistan", "Islamabad Capital Territory", "Gilgit-Baltistan", "Azad Jammu & Kashmir"]:
        items = sorted((d for d in ds if d["province"] == p), key=lambda d: -(d["values"].get("pop", {}).get("v") or 0))
        if items:
            provs.append((p, items))
    total_pop = sum(d["values"]["pop"]["v"] for d in ds if "pop" in d["values"])
    sections = []
    for p, items in provs:
        cards = []
        for d in items:
            v = d["values"]
            bits = []
            if "pop" in v:
                bits.append(f"{short(v['pop']['v'])} people")
            if "lit" in v:
                bits.append(f"literacy {v['lit']['v']:.0f}%")
            if "mpi_h" in v:
                bits.append(f"poverty {v['mpi_h']['v']:.0f}%")
            if not bits and "tmean" in v:
                bits.append(f"mean temperature {v['tmean']['v']:.1f}°C")
            cards.append(f'<li data-name="{E(d["name"].lower(), quote=True)}"><a href="/districts/{d["slug"]}/">{E(d["name"])}</a><span>{E(" · ".join(bits))}</span></li>')
        sections.append(f'<section class="dp-sec dp-prov" id="{prov_id(p)}"><div class="dp-sec-head"><h2>{E(p)}</h2><p>{len(items)} districts</p></div><ul class="dp-cards">{"".join(cards)}</ul></section>')
    body = (
        f'<header class="dp-hero"><div class="dp-hero-text"><p class="dp-kicker">Districts</p><h1>Pakistan’s districts, one profile each</h1>'
        f'<p class="dp-lede">A page for every one of the {len(ds)} districts on the 2023 census frame: population since 1951, literacy and schooling, work, health, poverty, farms, climate and what the census counted, each figure ranked and set beside the median district in its province and in Pakistan. Choose a district on the map, search for one, or browse by province.</p>'
        f'<label class="dp-search"><span class="visually-hidden">Find a district</span><input type="search" id="dpFind" placeholder="Find a district…" autocomplete="off"></label></div>'
        f'<div class="dp-hero-map dp-hero-map-wide">{loc.index_map(ds)}<p class="dp-map-name" id="dpMapName" aria-live="polite">Hover over or tap a district</p></div></header>'
        + "".join(sections)
        + '<section class="dp-sec" id="about"><div class="dp-sec-head"><h2>About these profiles</h2></div>'
        '<p class="dp-p">The profiles are generated from the same tables as the <a href="/places.html">Places map</a> and the <a href="/datasets/">catalogue</a>, on the Pakistan Bureau of Statistics’ 2023 district frame. Census 2023 does not publish district tables for Gilgit-Baltistan or Azad Jammu and Kashmir, so their pages carry the surveys and satellite sources that reach them. Each page has a CSV of its figures; the full tables are in the <a href="/datasets/place-indicators/">place_indicators</a> dataset. <a href="/methods.html">Methods</a>.</p></section>'
        '<script>(function(){var m=document.getElementById("dpMapName");if(m){[].forEach.call(document.querySelectorAll(".dp-pkmap a"),function(a){function show(){m.textContent=a.getAttribute("data-n");}a.addEventListener("mouseenter",show);a.addEventListener("focus",show);});}var f=document.getElementById("dpFind");if(!f)return;var items=[].slice.call(document.querySelectorAll(".dp-cards li"));var secs=[].slice.call(document.querySelectorAll(".dp-prov"));function apply(){var q=f.value.trim().toLowerCase();items.forEach(function(li){li.hidden=q&&li.getAttribute("data-name").indexOf(q)<0;});secs.forEach(function(s){s.hidden=q&&!s.querySelector(".dp-cards li:not([hidden])");});}f.addEventListener("input",apply);var q0=new URLSearchParams(location.search).get("q");if(q0){f.value=q0;apply();f.focus();}})();</script>')
    schema = {"@type": "CollectionPage", "name": "Pakistan district profiles", "url": seo.ORIGIN + "/districts/",
              "hasPart": [{"@type": "Dataset", "name": f"{d['name']} district profile", "url": seo.ORIGIN + f"/districts/{d['slug']}/"} for d in ds]}
    page_shell("/districts/", "Pakistan district profiles: population, literacy, poverty and climate for every district",
               f"Profiles of all {len(ds)} districts of Pakistan on the 2023 census frame: population, literacy, schooling, work, health, poverty, farms and climate, ranked and compared, with tehsils and CSV downloads.",
               body, schema, seo)


def page_shell(path, title, description, body, extra, seo):
    target = APP / path.strip("/") / "index.html"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(f'''<!DOCTYPE html><html lang="en" data-explorer="places"><head>
<meta charset="UTF-8"/><meta name="viewport" content="width=device-width, initial-scale=1.0, viewport-fit=cover"/>
<meta name="theme-color" content="#0c3a1e"/>
<link rel="icon" type="image/png" sizes="32x32" href="/assets/img/favicon-32.png"/><link rel="apple-touch-icon" href="/assets/img/apple-touch-icon.png"/>
<link rel="preconnect" href="https://fonts.googleapis.com"/><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin/>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet"/>
<link rel="stylesheet" href="{seo.asset("/assets/css/shell.css")}"/>
<link rel="stylesheet" href="{seo.asset("/assets/css/districts.css")}"/>
{seo.metadata(f"{title} — {seo.SITE_NAME}", description, path, extra, seo.SHARE_TAGS)}
</head><body class="dp-page">
<a class="skip" href="#main">Skip to content</a>
{seo._shell.header("districts/", root="/")}
<main id="main" class="dp-wrap">
{body}
</main>
{seo._shell.footer(root="/")}
<script src="{seo.asset("/assets/js/districts.js")}"></script>
<script src="{seo.asset("/assets/js/modals.js")}"></script>
<script src="{seo.asset("/assets/js/analytics.js")}"></script>
<script src="{seo.asset("/assets/js/shell.js")}"></script>
</body></html>''')


def build_districts(seo):
    global CROPS_FY, ORIGIN
    P = json.loads(PROFILES.read_text())
    CROPS_FY = P["crops_fy"]
    ORIGIN = seo.ORIGIN
    # the map twin of each row, from the spec the data builder used
    import sys
    sys.path.insert(0, str(ROOT / "etl" / "places"))
    from build_profiles import SPEC
    MAP.clear()
    for iid, g, ind, y, _, _ in SPEC:
        MAP[iid] = (g, ind, y)
    loc = Locator()
    # stale pages from an earlier frame or an earlier file name do not linger
    keep = {d["slug"] for d in P["districts"]}
    root = APP / "districts"
    if root.exists():
        for child in root.iterdir():
            if child.is_dir():
                if child.name not in keep:
                    for f in child.iterdir():
                        f.unlink()
                    child.rmdir()
                else:
                    for f in child.iterdir():
                        if f.name not in ("index.html", "profile.csv"):
                            f.unlink()
    for d in P["districts"]:
        profile_page(d, P, loc, seo)
    index_page(P, loc, seo)
    return len(P["districts"])
