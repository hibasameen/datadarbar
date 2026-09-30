"""Per-table layout declarations for the Census 2023 cross-tabulated tables.

`shape`  stub_unit    the unit label carries values on its own row
         banner_unit  the unit label is a banner with no values

`levels` the stub rows nested under a unit, outermost first. 'locality' and
         'sex' match fixed vocabularies; 'indicator' matches any other label.
         A row emits whenever it carries numbers, tagged with whatever levels
         are set at that point.

`header` what a column header means once locality and sex have been taken out
         of it, so every observation ends up keyed the same way regardless of
         which layout it came from.

Verified against the KP workbook for each table; `validate_spec` re-checks the
shape per file, and a mismatch fails the build rather than producing wrong data.
"""

SPEC = {
    # unit row carries its own values
    '1':   dict(shape='stub_unit',   levels=['locality'],                     header=['measure']),
    '3':   dict(shape='stub_unit',   levels=['indicator'],                    header=['measure']),

    # banner unit, locality only
    '20':  dict(shape='banner_unit', levels=['locality'],                     header=['measure']),
    '22':  dict(shape='banner_unit', levels=['locality'],                     header=['measure']),
    '23':  dict(shape='banner_unit', levels=['locality'],                     header=['measure']),
    '24':  dict(shape='banner_unit', levels=['locality'],                     header=['measure']),
    '25':  dict(shape='banner_unit', levels=['locality'],                     header=['measure']),
    '26':  dict(shape='banner_unit', levels=['locality'],                     header=['measure']),

    # banner unit, locality then sex; columns are categories
    '9':   dict(shape='banner_unit', levels=['locality', 'sex'],              header=['category']),
    '11':  dict(shape='banner_unit', levels=['locality', 'sex'],              header=['category']),

    # banner unit, locality then sex then indicator
    '6':   dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),
    '8':   dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),
    '13':  dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),
    '13a': dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['age']),
    '13b': dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['age']),

    # banner unit, locality then indicator; columns carry the sex split
    '15':  dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['sex']),
    '17':  dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['sex']),
    '19':  dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['sex']),
    '21':  dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category']),

    # banner unit, locality then indicator then sex (table 7 nests all three)
    '7':   dict(shape='banner_unit', levels=['locality', 'indicator', 'sex'], header=['category']),

    # banner unit, indicator only; locality and sex live in the column header
    '4':   dict(shape='banner_unit', levels=['indicator'],                    header=['locality', 'sex']),
    '5':   dict(shape='banner_unit', levels=['indicator'],                    header=['locality', 'sex']),
    '12':  dict(shape='banner_unit', levels=['indicator'],                    header=['locality', 'sex']),
    '14':  dict(shape='banner_unit', levels=['indicator'],                    header=['locality', 'sex']),
    '16':  dict(shape='banner_unit', levels=['indicator'],                    header=['locality', 'sex']),
    '18':  dict(shape='banner_unit', levels=['indicator'],                    header=['locality', 'sex']),

    # banner unit, sex then indicator; locality and nationality in the header
    '10':  dict(shape='banner_unit', levels=['sex', 'indicator'],             header=['locality', 'category']),

    # table 2 lists named urban localities under a size class, with their
    # parent tehsil in a column. Its rows are places, not administrative
    # units, so it is read but excluded from the unit panel.
    '2':   dict(shape='banner_unit', levels=['indicator'],                    header=['measure'],
                locality_list=True),
}

DISTRICT_ONLY = {'6', '7', '8', '10', '13'}

TITLES = {
    '1': 'Area, population by sex, sex ratio, density, urban proportion, household size, growth rate',
    '2': 'Urban localities by population size and their population by sex, growth rate and household size',
    '3': 'Number of rural localities by population size and their population by sex',
    '4': 'Population by single year age, sex and rural/urban',
    '5': 'Population by selected age group, sex and rural/urban',
    '6': 'Population 15 years and above by age group, sex, marital status and rural/urban',
    '7': 'Population 15 years and above by relationship to head of household, sex, marital status and rural/urban',
    '8': 'Population by sex, age group, relationship to head of household and rural/urban',
    '9': 'Population by sex, religion and rural/urban',
    '10': 'Population by nationality, age group, sex and rural/urban',
    '11': 'Population by mother tongue, sex and rural/urban',
    '12': 'Literacy rate, enrolments and out-of-school population by sex and rural/urban',
    '13': 'Population and literacy rate for special age groups by rural/urban and sex',
    '13a': 'Population and literacy rate for special age groups by rural/urban and sex (a)',
    '13b': 'Population and literacy rate for special age groups by rural/urban and sex (b)',
    '14': 'Population and employment by gender and rural/urban',
    '15': 'Population and employment for special age groups by rural/urban',
    '16': 'Disability and functional limitation by region and gender',
    '17': 'Disability and functional limitation for special age groups by rural/urban',
    '18': 'Migration and its reasons by region and gender',
    '19': 'Migration and its reasons for special age groups by rural/urban',
    '20': 'Type of housing unit by region',
    '21': 'Types of households by population and sex, rural/urban',
    '22': 'Housing characteristics, facilities used for fuel, lighting and kitchen by region',
    '23': 'Housing facilities by source of drinking water by region',
    '24': 'Housing characteristics by housing structure, type of toilet and washroom by region',
    '25': 'Housing characteristics by residential status, ownership and number of rooms by region',
    '26': 'Total number of structures by type and rural/urban',
}
