"""Which subject each census table measures, keyed on the census AND the table.

PBS renumbered between the two censuses. Of the 21 table ids that appear in
both, 16 carry a different subject in each - table 16 is "population 10+ by
usual activity" in 2017 and "Disability and functional limitation" in 2023;
tables 18 and 19 are disability in 2017 and migration in 2023; table 22 is
homelessness in 2017 and cooking fuel in 2023.

An earlier version of this file keyed on table_id alone, which quietly filed
1,000,095 cells - 22% of the 2023 panel - under the wrong subject: 2023's
migration tables sat in Health, its employment tables in Education, its
cooking-fuel table in Demographics. The map is keyed on (census_year,
table_id) now, and a table id missing for a census raises rather than falling
back to the other census's meaning.

Assignments follow each table's own published title, which travels with the
row in census_series_index.parquet. Labels live in topics.py, which both
halves of the index read; keeping a second copy here is what once let "Health"
and "Health & children" coexist as separate topics.
"""

# (census_year, table_id) -> topic key in topics.TOPICS
TOPIC_OF_TABLE = {}


def _set(year, topic, *tables):
    for t in tables:
        TOPIC_OF_TABLE[(str(year), t)] = topic


# ── 2017 ────────────────────────────────────────────────────────────────────
_set(2017, 'demographics',
     '1',    # area, population, density, growth
     '3',    # rural localities by population size
     '4', '5',      # age
     '6',    # marital status
     '7', '8',      # relationship to head
     '9',    # religion
     '10',   # nationality
     '11',   # mother tongue
     '21',   # CNIC holders 18+
     '22')   # homeless population
_set(2017, 'education', '12', '13', '14', '15')
_set(2017, 'employment', '16')          # population 10+ by usual activity
_set(2017, 'health', '17', '18', '19')  # disabled population
_set(2017, 'migration', '20')           # persons living abroad
_set(2017, 'welfare', '28', '40')       # household size; source of information
_set(2017, 'housing',
     '27',   # types of housing unit
     '29', '30',    # rooms, tenure
     '31',   # sex of ownership
     '32',   # period of construction and rooms
     '33', '34', '39')   # wall and roof materials
_set(2017, 'infrastructure',
     '35',   # drinking water, lighting, cooking fuel
     '36',   # services by period of construction
     '37',   # kitchen, bathroom, latrine
     '38')   # housing facilities

# ── 2023 ────────────────────────────────────────────────────────────────────
_set(2023, 'demographics',
     '1', '3', '4', '5', '6', '7', '8', '9', '10', '11')
_set(2023, 'education', '12', '13', '13a', '13b')
_set(2023, 'employment', '14', '15')
_set(2023, 'health', '16', '17')        # disability and functional limitation
_set(2023, 'migration', '18', '19')     # migration and its reasons
_set(2023, 'housing',
     '20',   # type of housing unit
     '21',   # types of household
     '25',   # residential status, ownership, rooms
     '26')   # structures by type
_set(2023, 'infrastructure',
     '22',   # fuel, lighting, kitchen
     '23',   # drinking water
     '24')   # toilet and washroom


def topic_for(census_year, table_id):
    """The subject of one table in one census.

    Raises rather than guessing: a table that appears in a new census release
    and is not listed here is a table nobody has read the title of, and
    silently filing it under the other census's meaning is how 22% of a panel
    ended up in the wrong place.
    """
    key = (str(census_year), str(table_id))
    if key not in TOPIC_OF_TABLE:
        raise KeyError(
            f'census {census_year} table {table_id} has no topic. Read its '
            f'table_title in census_series_index.parquet and add it above - '
            f'do not assume it means what the same number meant last census.')
    return TOPIC_OF_TABLE[key]
