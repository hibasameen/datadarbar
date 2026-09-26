"""How the 2017 tables correspond to the 2023 ones.

Table numbers do not carry over. 2017 publishes 40 tables; 2023 publishes 1-26
with 13(a) and 13(b), plus the locality tables 31-35. The first eleven align,
the locality tables are renumbered by eight, and the education and housing
blocks were restructured between censuses.

`match` states how much weight a cross-year comparison can bear:

  exact    same subject, same universe, same breakdown - comparable directly
  close    same subject, wording or breakdown differs - comparable with a note
  partial  2017 and 2023 divide the subject differently - needs indicator-level
           work before any comparison, not a table-level join
  none     published in one census and not the other
"""

# 2017 table -> (2023 table or None, match, note)
MAP = {
    '1':  ('1',   'exact',   'Area, population by sex, sex ratio, density, urban proportion, household size, growth rate'),
    '2':  ('2',   'exact',   'Urban localities by population size'),
    '3':  ('3',   'exact',   'Rural localities by population size'),
    '4':  ('4',   'exact',   'Population by single year of age'),
    '5':  ('5',   'exact',   'Population by selected age group'),
    '6':  ('6',   'exact',   'Population 15+ by marital status'),
    '7':  ('7',   'exact',   'Population 15+ by relationship to head of household'),
    '8':  ('8',   'exact',   'Population by relationship to head and age group'),
    '9':  ('9',   'exact',   'Population by religion'),
    '10': ('10',  'exact',   'Population by nationality'),
    '11': ('11',  'exact',   'Population by mother tongue'),

    # Literacy and education: 2017 spreads this over four tables keyed on
    # literacy and attainment; 2023 reorganises it around enrolment and
    # out-of-school population, and splits the age-bracket detail into 13(a)
    # and 13(b). No table-level join is defensible.
    '12': ('13a', 'partial', '2017: population 10+ by literacy, sex and age group. 2023 13(a) carries the age brackets but reorganises the columns'),
    '13': ('13',  'close',   '2017: population 10+ by literacy and sex, no age detail. Closest to 2023 table 13, which is district-only'),
    '14': ('13b', 'partial', '2017: literate population 10+ by level of educational attainment. 2023 folds attainment into 13(b)'),
    '15': ('13b', 'partial', '2017: population 5+ by level of educational attainment. Different universe (5+ not 10+) from 2017 table 14'),

    '16': ('14',  'close',   '2017: population 10+ by usual activity. 2023 table 14 reports employment status; category lists differ'),
    '17': ('16',  'close',   '2017: disabled population by sex and age group. 2023 reports disability and functional limitation separately'),
    '18': ('17',  'partial', '2017: disabled population 5+ by educational attainment'),
    '19': ('17',  'partial', '2017: disabled population 10+ by activity'),

    '20': ('18',  'partial', '2017: persons living abroad. 2023 table 18 covers migration generally, with "migration from abroad" as one category'),
    '21': (None,  'none',    'Pakistani citizens 18+ holding a CNIC. Not published in 2023'),
    '22': ('21',  'partial', '2017: homeless population in its own table. 2023 carries HOMELESS as a category inside table 21'),

    # Locality tables: renumbered by eight, otherwise the same four.
    '23': ('31',  'exact',   'Selected population statistics of individual rural localities'),
    '24': ('32',  'exact',   'Selected housing statistics of individual rural localities'),
    '25': ('33',  'exact',   'Selected population statistics of urban localities'),
    '26': ('34',  'exact',   'Selected housing statistics of urban localities'),

    # Housing: 2017 uses fourteen tables, many crossed with period of
    # construction; 2023 consolidates the same ground into seven and drops the
    # construction-period dimension. Every one of these needs indicator-level
    # work.
    '27': ('20',  'close',   '2017: types of housing units with population. 2023 table 20 is type of housing unit by region'),
    '28': ('21',  'partial', '2017: households by number of persons'),
    '29': ('25',  'partial', '2017: housing units by household size and number of rooms'),
    '30': ('25',  'partial', '2017: housing units by number of rooms and tenure'),
    '31': ('25',  'partial', '2017: owned housing units by sex of ownership. 2023 has no sex-of-ownership breakdown'),
    '32': ('25',  'partial', '2017: owned units by period of construction and rooms. 2023 drops period of construction'),
    '33': ('24',  'partial', '2017: units by tenure and material of walls and roofs'),
    '34': ('24',  'partial', '2017: owned units by period of construction and materials'),
    '35': ('23',  'partial', '2017: units by ownership, drinking water, lighting and cooking fuel. 2023 splits water (23) from fuel and lighting (22)'),
    '36': ('23',  'partial', '2017: owned units by period of construction and services'),
    '37': ('24',  'close',   '2017: units by tenure, kitchen, bathroom and latrine. 2023 table 24 covers toilet and washroom'),
    '38': ('22',  'partial', '2017: owned units by period of construction and housing facilities'),
    '39': ('24',  'partial', '2017: owned units by period of construction and materials, in more detail than table 34'),
    '40': (None,  'none',    'Households by source of information. A data-collection field, not published in 2023'),
}

# 2023 tables with no 2017 counterpart at all.
NO_2017 = {
    '13b': 'Split of table 13 introduced in 2023',
    '15':  'Employment for special age groups. 2017 carries age detail inside its table 16 instead',
    '19':  'Migration for special age groups. 2017 publishes only persons living abroad',
    '26':  'Total structures by type. 2017 reports housing units, not structures',
    '35':  'Urban locality housing, second part. Not published as Excel in 2023 either',
}


def by_2023():
    """{2023 table: [(2017 table, match, note), ...]} — many 2017 tables map to one."""
    out = {}
    for src, (dst, match, note) in MAP.items():
        if dst:
            out.setdefault(dst, []).append((src, match, note))
    return out


def comparable(level='exact'):
    """2017 tables safe to compare at the stated confidence or better."""
    order = {'exact': 0, 'close': 1, 'partial': 2, 'none': 3}
    return {s: d for s, (d, m, _) in MAP.items() if d and order[m] <= order[level]}
