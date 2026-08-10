#!/usr/bin/env python3
"""Long-arc structural series for the economy dashboard.

From the national_accounts warehouse table:
  - growth: real growth of Agriculture / Manufacturing / Services / GDP, 1951-52 -> 2025-26 (Table-1)
  - shares: sectoral shares in GVA 1999-00 -> 2025-26 (Table 7b), incl. sub-sectors
  - shares_backcast: 1951-52 -> 1998-99, backcast from real growth rates anchored
    at 1999-00 shares. Indicative only (ignores relative-price shifts & rebasing).

Output: econ_data/structure.json
"""
import json
from pathlib import Path
import duckdb

WH = Path(__file__).resolve().parents[1]
OUT = WH / 'econ_data'
con = duckdb.connect(str(WH / 'data_darbar.duckdb'), read_only=True)

def fy_sort(y):
    return int(y[:4])

# ---- growth series (Table-1 Macro Economic Indicators) ----
g = con.sql("""
  select item, year, value from national_accounts
  where table_sheet='Table-1' and item in ('Agriculture','Manufacturing','Services Sector','GDP')
""").df()
growth = {}
for item, key in [('Agriculture', 'agri'), ('Manufacturing', 'mfg'), ('Services Sector', 'serv'), ('GDP', 'gdp')]:
    s = g[g.item == item].sort_values('year', key=lambda c: c.map(fy_sort))
    growth[key] = [dict(year=r.year, value=round(r.value, 2)) for r in s.itertuples() if r.value is not None]

# ---- shares 1999-00 -> 2025-26 (Table 7b) ----
sh = con.sql("select item, year, value from national_accounts where table_sheet='Table 7b'").df()
SHARE_ITEMS = {
  'A. Agriculture, Forestry and Fishing     ( 1 to 4 )': ('agri', 'Agriculture'),
  'B. Industrial Activities ( 1 to 4 )': ('ind', 'Industry'),
  'C. Services ( 1 to 10)': ('serv', 'Services'),
  '1. Crops ( i+ii+iii)': ('crops', 'Crops'),
  '2.  Livestock': ('livestock', 'Livestock'),
  '2.  Manufacturing ( i+ii+iii)': ('mfg', 'Manufacturing'),
  'i)    Large Scale': ('lsm', 'Large-scale manufacturing'),
  'ii)   Small Scale': ('ssm', 'Small-scale manufacturing'),
  '4.  Construction': ('constr', 'Construction'),
  '1.  Mining and Quarrying': ('mining', 'Mining & quarrying'),
  '3   Electricity, Gas and Water supply': ('utilities', 'Electricity, gas & water'),
  '1.  Wholesale & Retail trade': ('trade', 'Wholesale & retail trade'),
  '2. Transportation & Storage': ('transport', 'Transport & storage'),
  '4. Information and Communication': ('ict', 'Information & communication'),
  '5.  Financial and Insurance Activities': ('finance', 'Finance & insurance'),
  '6.  Real Estate Activities (OD)': ('realestate', 'Real estate'),
  '7.  Public Administration and Social Security (General Government)': ('public', 'Public administration'),
  '8. Education': ('education', 'Education'),
  '9. Human Health and Social Work Activities': ('health', 'Health'),
}
shares = {}
labels = {}
for item, (key, label) in SHARE_ITEMS.items():
    s = sh[sh.item == item].sort_values('year', key=lambda c: c.map(fy_sort))
    pts = [dict(year=r.year, value=round(r.value, 2)) for r in s.itertuples() if r.value is not None]
    if pts:
        shares[key] = pts
        labels[key] = label

# ---- backcast 1951-52 -> 1998-99 ----
# share_s(t-1) = share_s(t) * (1+g_gdp(t)) / (1+g_s(t))   (real, anchored at 1999-00)
anchor_year = '1999-00'
anchors = {k: next((p['value'] for p in shares[k] if p['year'] == anchor_year), None)
           for k in ('agri', 'serv', 'mfg')}
gmap = {k: {p['year']: p['value'] for p in growth[k]} for k in growth}
years_g = sorted(gmap['gdp'], key=fy_sort)
back_years = [y for y in years_g if fy_sort(y) < 1999]
backcast = {k: [] for k in ('agri', 'serv', 'mfg')}
cur = dict(anchors)
for y in sorted(back_years, key=fy_sort, reverse=True):
    # growth in year y+1 links y -> y+1; use growth at the LATER year to step back
    later = f'{fy_sort(y)+1}-{str(fy_sort(y)+2)[2:]:0>2}'
    for k in backcast:
        gk, gg = gmap[k].get(later), gmap['gdp'].get(later)
        if cur[k] is not None and gk is not None and gg is not None and gk > -100:
            cur[k] = cur[k] * (1 + gg / 100) / (1 + gk / 100)
        backcast[k].append(dict(year=y, value=round(cur[k], 2) if cur[k] is not None else None))
for k in backcast:
    backcast[k] = sorted(backcast[k], key=lambda p: fy_sort(p['year']))

# ---- contributions to real GDP growth: contrib_s(t) = share_s(t-1) * growth_s(t) / 100 ----
# Growth rates: Table 6 (real growth by sector). Shares: Table 7b (% of GVA at basic prices).
GROWTH_ITEMS = {
  '1. Crops ( i+ii+iii)': ('crops', 'Crops', 'agri'),
  '2.  Livestock': ('livestock', 'Livestock', 'agri'),
  '3.  Forestry': ('forestry', 'Forestry', 'agri'),
  '4.  Fishing': ('fishing', 'Fishing', 'agri'),
  '1.  Mining and Quarrying': ('mining', 'Mining & quarrying', 'ind'),
  'i)    Large Scale': ('lsm', 'Large-scale manufacturing', 'ind'),
  'ii)   Small Scale': ('ssm', 'Small-scale manufacturing', 'ind'),
  'iii)  Slaughtering': ('slaughter', 'Slaughtering', 'ind'),
  'iii) Cotton Ginning': ('ginning', 'Cotton ginning', 'agri'),
  '4.  Construction': ('constr', 'Construction', 'ind'),
  '3   Electricity, Gas and Water supply': ('utilities', 'Electricity, gas & water', 'ind'),
  '1.  Wholesale & Retail trade': ('trade', 'Wholesale & retail trade', 'serv'),
  '2. Transportation & Storage': ('transport', 'Transport & storage', 'serv'),
  '3. Accommodation and Food Services Activities (Hotels & Restaurants)': ('hotels', 'Hotels & restaurants', 'serv'),
  '4. Information and Communication': ('ict', 'Information & communication', 'serv'),
  '5.  Financial and Insurance Activities': ('finance', 'Finance & insurance', 'serv'),
  '6.  Real Estate Activities (OD)': ('realestate', 'Real estate', 'serv'),
  '7.  Public Administration and Social Security (General Government)': ('public', 'Public administration', 'serv'),
  '8. Education': ('education', 'Education', 'serv'),
  '9. Human Health and Social Work Activities': ('health', 'Health & social work', 'serv'),
  '10.  Other Private Services': ('otherserv', 'Other private services', 'serv'),
}
gr = con.sql("select item, year, value from national_accounts where table_sheet='Table 6'").df()
grmap = {}
for item, (key, label, parent) in GROWTH_ITEMS.items():
    s = gr[gr.item == item]
    grmap[key] = {r.year: r.value for r in s.itertuples() if r.value is not None}
gdp_growth_t6 = {r.year: r.value for r in gr[gr.item == 'D GDP {Total of GVA at bp (A+B+C)'].itertuples()
                 if r.value is not None}
share_by_key = {}
for item, (key, label, parent) in GROWTH_ITEMS.items():
    # shares for the same key: prefer Table 7b entry with the identical item wording
    s = sh[sh.item == item]
    share_by_key[key] = {r.year: r.value for r in s.itertuples() if r.value is not None}
# 'hotels' and 'otherserv' and forestry/fishing exist in 7b under slightly different labels
EXTRA_SHARE = {'forestry': '3.  Forestry', 'fishing': '4.  Fishing',
               'slaughter': 'iii)  Slaughtering', 'ginning': 'iii) Cotton Ginning',
               'hotels': '3. Accommodation and Food Services Activities (Hotels & Restaurants)',
               'otherserv': '10.  Other Private Services'}
for k, item in EXTRA_SHARE.items():
    s = sh[sh.item == item]
    if len(s):
        share_by_key[k] = {r.year: r.value for r in s.itertuples() if r.value is not None}

cyears = sorted([y for y in gdp_growth_t6 if fy_sort(y) >= 2000], key=fy_sort)
contrib = {}
for key, (label, parent) in {v[0]: (v[1], v[2]) for v in GROWTH_ITEMS.values()}.items():
    pts = []
    for y in cyears:
        prev = f'{fy_sort(y)-1}-{str(fy_sort(y))[2:]}'
        s0, g1 = share_by_key.get(key, {}).get(prev), grmap.get(key, {}).get(y)
        if s0 is not None and g1 is not None:
            pts.append(dict(year=y, value=round(s0 * g1 / 100, 3)))
    if pts:
        contrib[key] = dict(label=label, parent=parent, points=pts)
# validation: contributions should sum close to headline GDP growth
check = []
for y in cyears:
    tot = sum(next((p['value'] for p in c['points'] if p['year'] == y), 0) for c in contrib.values())
    if y in gdp_growth_t6:
        check.append((y, round(tot, 2), round(gdp_growth_t6[y], 2), round(tot - gdp_growth_t6[y], 2)))
print('contribution check (year, sum, published GDP growth, gap):')
for row in check[-8:]:
    print('  ', row)
gaps = [abs(r[3]) for r in check]
print(f'   max |gap| {max(gaps):.2f} pp, mean {sum(gaps)/len(gaps):.2f} pp')

# ---- subsector (and macro-industry) real-growth series, from Table 6 ----
# lets the growth chart show any sector or subsector, not just the 3 macro aggregates
growth_sub = {}
growth_sub_labels = {}
growth_sub_parent = {}
for item, (key, label, parent) in GROWTH_ITEMS.items():
    pts = sorted([dict(year=y, value=round(v, 2)) for y, v in grmap.get(key, {}).items()],
                 key=lambda p: fy_sort(p['year']))
    if pts:
        growth_sub[key] = pts
        growth_sub_labels[key] = label
        growth_sub_parent[key] = parent
# macro Industry growth (proper, not the manufacturing proxy)
ind_g = {r.year: r.value for r in gr[gr.item == 'B Industrial Activities ( 1 to 4 )'].itertuples() if r.value is not None}
if ind_g:
    growth['ind'] = [dict(year=y, value=round(v, 2)) for y, v in sorted(ind_g.items(), key=lambda kv: fy_sort(kv[0]))]

out = dict(growth=growth, shares=shares, labels=labels, backcast=backcast,
           growth_sub=growth_sub, growth_sub_labels=growth_sub_labels, growth_sub_parent=growth_sub_parent,
           contrib=contrib, contrib_gdp=[dict(year=y, value=round(gdp_growth_t6[y], 2)) for y in cyears],
           meta=dict(
             growth_src='PBS National Accounts, Macro Economic Indicators (Table-1), real growth %',
             shares_src='PBS National Accounts 2015-16 base, Sectoral Shares in GDP (Table 7b), % of GVA',
             backcast_note='Backcast from official real growth rates anchored at 1999-00 shares; indicative only',
             contrib_src='Contribution to real GDP growth, percentage points = previous-year share of GVA x this-year real growth (PBS Tables 6 and 7b)'))
OUT.mkdir(exist_ok=True)
(OUT / 'structure.json').write_text(json.dumps(out))
print('growth years:', {k: (v[0]['year'], v[-1]['year'], len(v)) for k, v in growth.items()})
print('share keys:', sorted(shares))
print('backcast 1951-52:', {k: backcast[k][0] for k in backcast})
print('wrote structure.json', (OUT / 'structure.json').stat().st_size // 1024, 'KB')
