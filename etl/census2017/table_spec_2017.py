"""Per-table layout declarations for the Census 2017 district tables.

Same contract as `etl/stage2/table_spec.py` — `shape`, `levels`, `header` — so
the shared reader handles both census years. Shapes were read off the Abbottabad
workbooks and are re-checked per file by `validate_spec`.

What a spec declares is a LAYOUT, not a comparability claim: it says how to
read a workbook, not that its indicators line up with a 2023 table. Some tables
declared here map to 2023 only `partial` (12, 14, 15, 20) - they are still worth
extracting on their own terms, and the cross-year join is `table_map.py`'s
judgement to make, not this file's. Tables whose layout has not been profiled
are simply absent.

Note the vocabulary: 2017 writes OVERALL where 2023 writes ALL LOCALITIES, and
ALL DISABLED where the universe is the disabled population. Both are handled in
the shared reader's LOCALITY map rather than here.
"""

SPEC_2017 = {
    # unit row carries its own values
    '1':  dict(shape='stub_unit',   levels=['locality'],                     header=['measure']),
    # header: DISABLED POPULATION over the four sexes
    # stub: unit, then the population-size classes; header: sex
    '3':  dict(shape='stub_unit',   levels=['indicator'],                    header=['sex']),
    # one row per unit; both locality and sex are in the column header
    # (TOTAL / BOTH SEXES, RURAL / MALE, ...), not in the stub
    '20': dict(shape='stub_unit',   levels=[],                               header=['locality', 'sex']),

    # banner unit, indicator stub, locality and sex in the column header
    '4':  dict(shape='banner_unit', levels=['indicator'],                    header=['locality', 'sex']),
    '5':  dict(shape='banner_unit', levels=['indicator'],                    header=['locality', 'sex']),

    # banner unit, locality then sex
    '9':  dict(shape='banner_unit', levels=['locality', 'sex'],              header=['category']),
    '11': dict(shape='banner_unit', levels=['locality', 'sex'],              header=['category']),
    '13': dict(shape='banner_unit', levels=['locality', 'sex'],              header=['category']),

    # banner unit, locality then sex then indicator
    '6':  dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),
    '8':  dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),
    '12': dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),
    '14': dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),
    '15': dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),
    '16': dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),

    # banner unit, locality then indicator then sex (table 7 nests all three,
    # as 2023's table 7 does)
    '7':  dict(shape='banner_unit', levels=['locality', 'indicator', 'sex'], header=['category']),

    # banner unit, sex then indicator; locality in the header
    '10': dict(shape='banner_unit', levels=['sex', 'indicator'],             header=['locality', 'category']),

    # Disability. ALL DISABLED marks the universe, not a locality - see 18 and 19.
    '17': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['sex'],
               not_locality={'ALL DISABLED'}),

    # types of housing units, closest to 2023 table 20. The stub nests two deep:
    # Overall / Rural / Urban, then REGULAR, INSTITUTIONAL, HOMELESS, TOTAL and
    # PERCENT beneath each. Declaring only the type flattens the two into one slot,
    # where the type row overwrites the locality row and all three localities are
    # reported as the district total.
    '27': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['sex', 'category'],
               # TOTAL here sums the housing types within a locality; without
               # this it is read as "all localities" and Rural's and Urban's
               # totals are both filed as the district total.
               not_locality={'TOTAL'}),

    # tenure, kitchen, bathroom and latrine; 2023 table 24 covers toilet and washroom.
    # Three stub tiers below the unit: locality, then the facility (KITCHEN,
    # BATHROOM, LATRINE), then its state (SEPARATE, SHARED, NONE, ...). The
    # facility rows carry no figures, which is how they are recognised as
    # headings; "TOTAL :" and "PERCENT :" are leaves at the facility's own level
    # and close the group rather than sitting inside LATRINE.
    '37': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               group_indicator=True, group_exits={'TOTAL :', 'PERCENT :'}),
    # ---- profiled 26 September 2026. All map to 2023 only `partial`, so they are
    # ---- extracted on their own terms and excluded from cross-year joins.

    # unit row, then the locality rows carry the figures themselves
    '28': dict(shape='stub_unit',   levels=['locality'],                     header=['category']),
    '40': dict(shape='stub_unit',   levels=['locality'],                     header=['category']),
    # one row per unit; locality and sex are both in the column header
    '31': dict(shape='stub_unit',   levels=[],                               header=['locality', 'sex']),

    # unit -> locality -> an age or category stub
    # Sex is a tier here, not a column: the age block repeats under ALL DISABLED,
    # then MALE, then FEMALE. Without it the sex blocks collapse onto each other.
    #
    # ALL DISABLED marks the universe, not a locality. It is in the locality
    # vocabulary because it stands where a locality would, but it recurs inside
    # every locality block - OVERALL, then ALL DISABLED, then the ages; RURAL,
    # then ALL DISABLED again - so read as a locality it resets RURAL and URBAN
    # back to 'all' and their figures land on the district total.
    '18': dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'],  header=['category'],
               not_locality={'ALL DISABLED'}),
    '19': dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'],  header=['category'],
               not_locality={'ALL DISABLED'}),
    '21': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['sex', 'category']),
    '22': dict(shape='banner_unit', levels=['locality', 'sex', 'indicator'], header=['category']),
    # TOTAL and PERCENT close each locality block and sum the categories above
    # them, as in table 27; without this they reset the locality instead.
    '29': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               not_locality={'TOTAL'}),
    '32': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               not_locality={'TOTAL'}),
    # TOTAL here sums the tenure categories within a locality, as in table 27
    '30': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               not_locality={'TOTAL'}),

    # unit -> locality -> a heading carrying no figures -> its categories. The
    # heading is recognised by having no numbers of its own, the same signal
    # table 37 uses for KITCHEN, BATHROOM and LATRINE.
    '33': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               group_indicator=True),
    '34': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               group_indicator=True),
    '35': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               group_indicator=True),
    '36': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               group_indicator=True),
    '38': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               group_indicator=True),
    '39': dict(shape='banner_unit', levels=['locality', 'indicator'],        header=['category'],
               group_indicator=True),
}

# Read through the locality reader (`etl/stage2/read_localities.py`), not this
# spec: their rows are places, not administrative units.
LOCALITY_TABLES_2017 = {
    '23': ('31', 'rural population'),
    '24': ('32', 'rural housing'),
    '25': ('33', 'urban population'),
    '26': ('34', 'urban housing'),
}

# Table 2 lists named urban localities under a population-size class, as 2023's
# table 2 does — a place list, not a unit table.
LOCALITY_LIST_2017 = {'2'}


# PBS does not publish every table for every district, and the absences are
# substantive rather than retrieval failures. Recorded so the extract can tell
# "absent by publication" from "we failed to fetch it".
KNOWN_ABSENT = {
    # Kohistan's population is 100% rural - urban population is exactly 0 in its
    # own table 1 - so there are no urban localities to report.
    ('KOHISTAN', '25'): 'district has no urban population',
    ('KOHISTAN', '26'): 'district has no urban population',
    # These two are absent from the spreadsheet release; table 23 is in the
    # combined PDF, table 24 is in neither rendering.
    ('KOHISTAN', '23'): 'absent from the spreadsheet release; present in the combined PDF',
    ('KOHISTAN', '24'): 'absent from both renderings',

}
