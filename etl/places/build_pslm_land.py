"""Rural land ownership from PSLM 2019-20 (Section G) as a Places group.

The district figures are made from the microdata by
etl/pslm_land/build_district_land.py and kept in etl/pslm2019_district_land.csv;
this puts them on PBS's 2023 district frame.

PSLM 2019-20 is on the 2019 districts. Five of them have since been divided
(Chitral, Kohistan, Killa Abdullah, Karachi West; and Kalat and Loralai, whose
successors Surab - Shaheed Sikandarabad in the PSLM file - and Duki were already
sampled on their own). A divided district's figure is the old district's, shown
across its successors and said to be, as the other PSLM groups do.

Only what the survey measures soundly is drawn: who owns land at all, how much
the typical owner has, and who rents. The Gini among owners is in the CSV for
anyone who wants it but is not a Places indicator: a few dozen households
reporting in an unlabelled unit move it by 0.3 in some districts.

Usage:
  build_pslm_land.py --src etl/pslm2019_district_land.csv --geo <districts_2023_geo.js> --out-places <parquet>
"""
import argparse, csv, json, pathlib, re

GROUP = 'pslmLand'
# indicator -> (label, csv column, dp, needs 30 owners?, subtopic, metric form)
IND = {
    'landless_pct': ('Rural households owning no agricultural land (%)', 'landless_pct', 1,
                     'Published percentage / proportion'),
    'owners_under_2_5ac_pct': ('Land-owning rural households with under 2.5 acres (%)', 'owners_under_2_5ac_pct', 1,
                               'Published percentage / proportion'),
    'median_owned_ac': ('Median land owned by rural owning households (acres)', 'median_owned_ac', 2,
                        'Average / source-defined score'),
    'rent_in_pct': ('Rural households renting land in (%)', 'rent_in_pct', 1,
                    'Published percentage / proportion'),
    'rent_out_pct': ('Rural households renting land out (%)', 'rent_out_pct', 1,
                     'Published percentage / proportion'),
}
SUBTOPIC = 'Land ownership'
SUM_OK = {}
HREVIEW = ('Data Darbar estimate from PSLM 2019-20 microdata (Section G), rural households, PSU weights; '
           'districts with fewer than 30 sampled rural households (or 30 owners, for holding size) are not drawn')
SE_NOTE_FROM = 5.0   # percentage points: say so when the landlessness estimate is this loose

# PSLM district label -> name(s) in the 2023 geography layer, where they differ
ALIAS = {
    'bajur': ['BAJAUR'], 'bunair': ['BUNER'], 'charsada': ['CHARSADDA'], 'd. i. khan': ['DERA ISMAIL KHAN'],
    'malakand': ['MALAKAND PROTECTED AREA'], 'tor garh': ['TORGHAR'], 'bhakhar': ['BHAKKAR'],
    'd. g. khan': ['DERA GHAZI KHAN'], 'jehlum': ['JHELUM'], 'muzaffar garh': ['MUZAFFARGARH'],
    't.t. singh': ['TOBA TEK SINGH'], 'karachi malir': ['MALIR'], 'mir pur khas': ['MIRPUR KHAS'],
    'nowshero feroze': ['NAUSHAHRO FEROZE'], 'shahdadkot': ['KAMBAR SHAHDAD KOT'],
    'shaheed banazir abad': ['SHAHEED BENAZIRABAD'], 'tando allah yar': ['TANDO ALLAHYAR'],
    'kachhi/ bolan': ['KACHHI'], 'kech/turbat': ['KECH'], 'musa khel': ['MUSAKHEL'],
    'nasirabad/ tamboo': ['NASIRABAD'], 'panjgoor': ['PANJGUR'], 'qilla saifullah': ['KILLA SAIFULLAH'],
    'sibbi': ['SIBI'], 'shaheed sikandar abad': ['SURAB'],
    # divided since 2019: the old district's figure, across its successors
    'chitral': ['LOWER CHITRAL', 'UPPER CHITRAL'],
    'kohistan': ['KOLAI PALAS KOHISTAN', 'UPPER KOHISTAN', 'LOWER KOHISTAN'],
    'qilla abdullah': ['KILLA ABDULLAH', 'CHAMAN'],
    'karachi west': ['KARACHI WEST', 'KEAMARI'],
}
_norm = lambda s: re.sub(r'[^a-z]', '', s.lower().replace('district', ''))


def vocab():
    """Same shape as a curated_groups.json entry; read by extra_groups.py."""
    return [{
        'group_key': GROUP, 'topic': 'agriculture', 'topic_label': 'Agriculture',
        'group_label': 'Land ownership — PSLM 2019-20 (rural households)',
        'dataset': 'PSLM 2019-20 microdata, Section G, rural households (Data Darbar estimates)',
        'dp': {k: v[2] for k, v in IND.items()},
        'indicators': {k: v[0] for k, v in IND.items()},
    }]


def mapping(countable):
    """Entries for build_hierarchy, in the crosswalk's own shape."""
    out = []
    for k, (label, col, dp, form) in IND.items():
        o = dict(level='district', group_key=GROUP, indicator=k, label=label,
                 measure=label, metric='', dp=dp, source='place')
        out.append(dict(
            proposed_topic='Agriculture', proposed_subtopic=SUBTOPIC,
            indicator_family='Land ownership (PSLM 2019-20, rural households)',
            metric_form=form, suggested_controls='District', definition_review=HREVIEW,
            legacy_modes=['Source value'], original=o))
    return out


def main():
    import duckdb
    ap = argparse.ArgumentParser()
    ap.add_argument('--src', required=True)
    ap.add_argument('--geo', required=True)
    ap.add_argument('--out-places', required=True)
    a = ap.parse_args()

    js = pathlib.Path(a.geo).read_text()
    gj = json.loads(js[js.index('=') + 1:].rstrip().rstrip(';'))
    code = {_norm(f['properties']['n']): f['properties']['code'] for f in gj['features']}
    name = {f['properties']['code']: f['properties']['n'].replace(' DISTRICT', '').title() for f in gj['features']}

    rows, unmatched = [], []
    for r in csv.DictReader(open(a.src)):
        targets = ALIAS.get(r['pslm_district'], [r['pslm_district']])
        codes = [code.get(_norm(t)) for t in targets]
        if None in codes:
            unmatched.append(r['pslm_district']); continue
        split = len(codes) > 1
        base = None
        if split:
            old = r['pslm_district'].replace('qilla', 'killa').title()
            base = (f'{old} is now {", ".join(name[c] for c in codes)}. The figure is for the whole of the '
                    'old district and is shown across all of them, not divided between them')
        for k, (label, col, dp, form) in IND.items():
            if r[col] in ('', 'nan'):
                continue
            note = base
            if k == 'landless_pct' and float(r['landless_se']) >= SE_NOTE_FROM:
                se = (f'From {int(float(r["n_rural_hh"])):,} sampled rural households in {int(float(r["n_psu"]))} '
                      f'sampling units; the standard error is about {float(r["landless_se"]):.0f} percentage points')
                note = f'{base}. {se}' if base else se
            rows.append(('district', ' '.join(codes), r['pslm_district'], 'split' if split else 'exact',
                         note, GROUP, k, None, float(r[col])))
    if unmatched:
        raise SystemExit(f'PSLM districts not in the geography layer: {unmatched}')

    con = duckdb.connect()
    con.execute("""CREATE TABLE places(level TEXT, map_key TEXT, source_key TEXT, relation TEXT, note TEXT,
                   group_key TEXT, indicator TEXT, year TEXT, value DOUBLE)""")
    con.executemany('INSERT INTO places VALUES (?,?,?,?,?,?,?,?,?)', rows)
    p = pathlib.Path(a.out_places); p.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY places TO '{p.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    print(con.sql("""SELECT indicator, count(*) AS districts, round(min(value),1) AS min_,
                     round(median(value),1) AS median_, round(max(value),1) AS max_
                     FROM places GROUP BY 1 ORDER BY 1""").df().to_string(index=False))


if __name__ == '__main__':
    main()
