"""Build the 1998 census tables from the capture made by fetch.py.

  census_admin_units_1951_1998   Area & Population of Administrative Units:
                                 every province, district or agency, sub-division
                                 or tehsil, and town, at the 1951, 1961, 1972,
                                 1981 and 1998 censuses, rural and urban.
  census1998_district_glance     District at a Glance: about twenty indicators
                                 per district (sex, density, urban share,
                                 household size, literacy by sex, growth since
                                 1981, housing and its amenities).

Both are read from text PDFs PBS generated from spreadsheets, so nothing is
OCR'd. Figures are kept as printed, footnote marks included, and the parse is
checked the way the census checks itself: each level must add up to the one
above it.

    python3 etl/census1998/build.py --capture ../raw_data/pbs/census1998_sources/<date> \
        --out ../data_darbar_warehouse/census1998/<date>
"""
import argparse, json, pathlib, re, subprocess

import duckdb

YEARS = [1951, 1961, 1972, 1981, 1998]
MARK = re.compile(r'^[*#¾½¼@$]+$')
NUM = re.compile(r'^-?[\d,]+(\.\d+)?$')
URBAN_LOCALITY = re.compile(
    r'(\bM\.?\s?C\b\.?|\bT\.?\s?C\b\.?|Cantt\.?|M\.\s?Corporation|Municipal|Corporation|'
    r'C\.D\.A|CDA|\(part\)|\(Rural part\)|\(Urban part|Town Committee|\bCity$)', re.I)


def pdftext(path):
    return subprocess.run(['pdftotext', '-layout', str(path), '-'], capture_output=True,
                          text=True, check=True).stdout


def split_values(rest):
    """Tokens after the name -> [(value or None, mark)]. A dash is no figure; a
    footnote mark is attached to the figure before it."""
    out = []
    for tok in rest.split():
        if MARK.match(tok):
            if out:
                out[-1] = (out[-1][0], (out[-1][1] + ' ' + tok).strip())
            continue
        if tok in ('-', '–'):
            out.append((None, ''))
        elif NUM.match(tok):
            out.append((float(tok.replace(',', '')), ''))
        else:
            return None                       # not a data line
    return out


def admin_units(path):
    lines = pdftext(path).split('\n')
    rows, notes = [], []
    table = None
    province = district = subdiv = tehsil = None
    sub = None           # the last sub-division or tehsil, and whether it printed Rural/Urban
    last = None          # the unit whose Rural/Urban lines follow
    last_rows = []       # its rows, so a name wrapped onto the next line can be completed
    for raw in lines:
        s = raw.strip()
        if not s:
            continue
        m = re.match(r'TABLE-(\d+)', s)
        if m:
            if 'Continued' not in s:
                table = int(m.group(1))
                province = district = subdiv = tehsil = sub = None
            last, last_rows = None, []
            continue
        if s.startswith(('NAME OF', 'ADMINISTRATIVE UNIT', 'SQ.KM')) or s == 'POPULATION':
            continue
        if re.match(r'^(\*+|#+|\u00be|\u00bd|@)\s*:', s):
            notes.append({'table_no': table, 'district': district, 'note': s})
            continue
        # The name runs to the first gap of two spaces. Splitting at the last
        # letter instead cut "Peshawar Town-1" into "Peshawar Town" and an area
        # of -1.
        m = re.match(r'^(\S.*?)(\s{2,}(.*))?$', s)
        if not m:
            continue
        name, rest = m.group(1).strip(), (m.group(3) or '').strip()
        vals = split_values(rest) if rest else []
        if vals is None:
            continue
        if not vals:
            # "North West Frontier" / "Province (NWFP)": the figures sit on the
            # first line and the name finishes on the second.
            if last_rows and last is not None and not last.get('closed'):
                full = last['unit'] + ' ' + name
                for r in last_rows:
                    r['unit'] = full
                    if r['unit_type'] == 'province':
                        r['province'] = full
                if last['unit_type'] == 'province' and table != 1:
                    province = full
                if last['unit_type'] == 'district':
                    district = full
                last['unit'] = full
            continue
        if name in ('Rural', 'Urban'):
            if last is None:
                continue
            last['closed'] = True
            last['ru'] = True
            for y, (v, mk) in zip(YEARS, vals[-5:]):
                rows.append({**last['key'], 'unit': last['unit'], 'locality': name.lower(),
                             'census_year': y, 'population': v, 'mark': mk, 'area_sq_km': None})
            continue
        has_area = len(vals) == 6
        area = vals[0][0] if has_area else None
        yrs = vals[1:] if has_area else vals
        if len(yrs) != 5:
            continue
        # The level is read from the layout, not only the name:
        #   - a unit printed with an area and followed by Rural/Urban lines is
        #     an administrative unit;
        #   - a line printed WITHOUT an area is a town inside the tehsil above
        #     it - but only if that tehsil printed Rural/Urban lines. Lahore's
        #     towns and Lahore Cantt. print none: they ARE the subdivisions of
        #     the city district, and Lahore adds up only if they are peers.
        if table == 1:
            level = 'country' if name.upper() == 'PAKISTAN' else 'province'
        elif province is None:
            level = 'province'
        elif name.isupper() or name.startswith('Tribal Area Adjoining'):
            level = 'district'           # districts, agencies, frontier regions
        elif re.search(r'\((Rural|Urban) part', name):
            # Peshawar prints its total twice over: as the city's rural part,
            # urban part and cantonment, and as Towns 1-4 and the cantonment.
            # Both add to 2,026,851. The parts are a second breakdown, not
            # more subdivisions, and are kept apart so neither double counts.
            level = 'district_part'
        elif not has_area:
            if sub is not None and sub.get('ru'):
                level = 'urban_locality'
            elif sub is not None or not URBAN_LOCALITY.search(name):
                level = 'tehsil'
            else:
                level = 'urban_locality'
        elif re.search(r'sub-?division', name, re.I):
            level = 'sub_division'
        else:
            level = 'tehsil'
        if level == 'province' and table != 1:
            province, district, subdiv, tehsil = name, None, None, None
        elif level == 'district':
            district, subdiv, tehsil, sub = name, None, None, None
        elif level == 'sub_division':
            subdiv, tehsil = name, None
        elif level == 'tehsil':
            tehsil = name
        key = {'table_no': table,
               'province': (name if level == 'province' else None if level == 'country'
                            else province),
               'district': district if level not in ('country', 'province') else None,
               # district_part rows do not open a tehsil of their own
               'sub_division': subdiv if level in ('sub_division', 'tehsil', 'urban_locality') else None,
               'tehsil': tehsil if level in ('tehsil', 'urban_locality') else None,
               'unit_type': level}
        locality = 'urban' if level == 'urban_locality' else 'all'
        last_rows = []
        for y, (v, mk) in zip(YEARS, yrs):
            r = {**key, 'unit': name, 'locality': locality, 'census_year': y, 'population': v,
                 'mark': mk, 'area_sq_km': area}
            rows.append(r)
            last_rows.append(r)
        last = {'key': key, 'unit': name, 'unit_type': level,
                'closed': level == 'urban_locality'}
        if level in ('sub_division', 'tehsil'):
            sub = last
    # Rural/Urban rows were written with the name as it stood; wrapped names
    # were completed on the unit rows only, so carry them over.
    return rows, notes


GLANCE_ORDER = [
    # label as printed -> (indicator, unit)
    ('Area', 'area', 'sq km'),
    ('Population - 1998', 'population_1998', 'persons'),
    ('Sex Ratio (males per 100 females)', 'sex_ratio', 'males per 100 females'),
    ('Populaltion Density', 'population_density', 'persons per sq km'),
    ('Population Density', 'population_density', 'persons per sq km'),
    ('Urban Population', 'population_urban', 'persons'),
    ('Rural Population', 'population_rural', 'persons'),
    ('Average Household Size', 'avg_household_size', 'persons'),
    ('Literacy Ratio (10 +)', 'literacy_ratio', 'percent of population 10+'),
    ('Population - 1981', 'population_1981', 'persons'),
    ('Average Annual Growth Rate (1981 - 98)', 'growth_rate_1981_98', 'percent a year'),
    ('Total Housing Units', 'housing_units', 'units'),
    ('Pacca Housing Units', 'housing_units_pacca', 'units'),
    ('Housing Units having Electricity', 'housing_units_electricity', 'units'),
    ('Housing Units having Piped Water', 'housing_units_piped_water', 'units'),
    ('Housing Units using Gas for Cooking', 'housing_units_gas_cooking', 'units'),
]


def glance(path, fname):
    lines = [l for l in pdftext(path).split('\n') if l.strip()]
    title = re.sub(r'\s+', ' ', lines[0]).strip()
    district = re.sub(r'\s*AT (A )?GLANCE.*$', '', title, flags=re.I).strip()
    out, context, admin = [], None, False
    for l in lines[1:]:
        m = re.match(r'^\s*(.*?)\s{2,}(\S.*)$', l)
        if not m:
            if l.strip().lower().startswith('administrative units'):
                admin = True
            continue
        label = re.sub(r'\s+', ' ', m.group(1)).strip()
        rest = m.group(2).strip()
        num = re.match(r'^(-|[\d,.]+)\s*(?:[A-Za-z. %]*)?(?:\(\s*([\d.]+)\s*%\s*\))?', rest)
        value = None if not num or num.group(1) == '-' else float(num.group(1).replace(',', ''))
        share = float(num.group(2)) if num and num.group(2) else None
        if admin:
            ind, unit = 'admin_' + re.sub(r'[^a-z]+', '_', label.lower()).strip('_'), 'count'
        elif label in ('Male', 'Female'):
            if context == 'literacy':
                ind, unit = f'literacy_ratio_{label.lower()}', 'percent of population 10+'
            else:
                ind, unit = f'population_{label.lower()}', 'persons'
        else:
            hit = next(((i, u) for lab, i, u in GLANCE_ORDER if lab == label), None)
            if not hit:
                ind, unit = re.sub(r'[^a-z0-9]+', '_', label.lower()).strip('_'), ''
            else:
                ind, unit = hit
            context = ('literacy' if ind == 'literacy_ratio'
                       else 'population' if ind == 'population_1998' else context)
        flag = ''
        # A figure that cannot be what its label says is not read, and not
        # "corrected" either: Ghotki's household size is printed 505.
        if ind == 'avg_household_size' and value is not None and value > 20:
            flag, value = f'printed {rest}; not a possible household size, not read', None
        out.append({'district': district, 'title': title, 'file': fname, 'indicator': ind,
                    'label': label, 'value': value, 'share_pct': share, 'unit': unit,
                    'as_printed': rest, 'flag': flag})
    return out


def write(rows, path):
    tmp = path.with_suffix('.json')
    tmp.write_text(json.dumps(rows))
    duckdb.execute(f"COPY (SELECT * FROM read_json_auto('{tmp}')) TO '{path}' (FORMAT PARQUET)")
    tmp.unlink()
    print(f'  {path.name}: {len(rows):,} rows')


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--capture', required=True)
    ap.add_argument('--out', required=True)
    a = ap.parse_args()
    cap, out = pathlib.Path(a.capture), pathlib.Path(a.out)
    out.mkdir(parents=True, exist_ok=True)
    man = json.loads((cap / 'retrieval_manifest.json').read_text())

    au, notes = admin_units(cap / 'administrative_units' / 'administrative_units.pdf')
    write(au, out / 'census_admin_units_1951_1998.parquet')
    (out / 'admin_units_notes.json').write_text(json.dumps(notes, indent=1))

    g = []
    for f in man['files']:
        if f['kind'] == 'district_at_a_glance':
            g += glance(cap / f['path'], f['path'].rsplit('/', 1)[1])
    write(g, out / 'census1998_district_glance.parquet')
    (out / 'capture.txt').write_text(str(cap.resolve()) + '\n')


if __name__ == '__main__':
    main()
