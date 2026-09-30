"""Reviewed decisions for the 2017 -> 2023 district crosswalk.

Only the relations that are not a plain identity are written here. Everything
else is resolved by name and checked, so this file stays short enough to read.

The check is not our own arithmetic. Census 2023's table 1 prints a
POPULATION 2017 column beside the 2023 figure: PBS's own restatement of the 2017
count on 2023 boundaries. It sums across the 136 districts to 207,684,626, which
is the 2017 total to the person, so every relation asserted below can be tested
against the publisher rather than argued for.

The administrative history, as the two censuses record it:

* FATA was dissolved into Khyber Pakhtunkhwa in 2018. Its seven agencies became
  districts under the same name; its six Frontier Regions did not survive as
  units and were absorbed into the settled district each adjoins.
* Chitral and Kohistan were split, into two and three districts.
* Four districts were created out of one neighbour each: Chaman from Killa
  Abdullah, Duki from Loralai, Surab from Kalat, Keamari from Karachi West.
"""

# 2017 unit -> 2023 unit, where the pair is one unit under two names.
RENAMED = {
    'BAJAUR AGENCY': 'BAJAUR DISTRICT',
    'KHYBER AGENCY': 'KHYBER DISTRICT',
    'KURRAM AGENCY': 'KURRAM DISTRICT',
    'MOHMAND AGENCY': 'MOHMAND DISTRICT',
    'ORAKZAI AGENCY': 'ORAKZAI DISTRICT',
    'NORTH WAZIRISTAN AGENCY': 'NORTH WAZIRISTAN DISTRICT',
    'SOUTH WAZIRISTAN AGENCY': 'SOUTH WAZIRISTAN DISTRICT',
}

# 2017 unit -> the 2023 district that absorbed it. The host keeps its name, so
# the group is {host 2017, absorbed 2017} -> {host 2023}.
MERGED_INTO = {
    'FR BANNU': 'BANNU DISTRICT',
    'FR D.I.KHAN': 'DERA ISMAIL KHAN DISTRICT',
    'FR KOHAT': 'KOHAT DISTRICT',
    'FR LAKKI MARWAT': 'LAKKI MARWAT DISTRICT',
    'FR PESHAWAR': 'PESHAWAR DISTRICT',
    'FR TANK': 'TANK DISTRICT',
}

# 2017 unit -> the 2023 units it became. A district that kept its name and lost
# territory to a new neighbour is a split too, and is listed with itself first.
SPLIT_INTO = {
    'CHITRAL DISTRICT': ['LOWER CHITRAL DISTRICT', 'UPPER CHITRAL DISTRICT'],
    'KOHISTAN DISTRICT': ['LOWER KOHISTAN DISTRICT', 'UPPER KOHISTAN DISTRICT',
                          'KOLAI PALAS KOHISTAN DISTRICT'],
    'KILLA ABDULLAH DISTRICT': ['KILLA ABDULLAH DISTRICT', 'CHAMAN DISTRICT'],
    'LORALAI DISTRICT': ['LORALAI DISTRICT', 'DUKI DISTRICT'],
    'KALAT DISTRICT': ['KALAT DISTRICT', 'SURAB DISTRICT'],
    'KARACHI WEST DISTRICT': ['KARACHI WEST DISTRICT', 'KEAMARI DISTRICT'],
}


# Pairs of districts that kept their names and exchanged territory. Neither is a
# rename, a merge or a split, and each looks like an ordinary identity until its
# population is checked: Jhang's restated 2017 count is 893 higher than its
# published one and Toba Tek Singh's is 893 lower, Nasirabad's is 1,932 higher
# and Kachhi's 1,932 lower. Taken as a pair each balances exactly, so the two
# are comparable together and neither is comparable alone.
#
# These were found by the check rather than looked up: nothing in the names
# suggests a boundary moved.
TRANSFERRED = [
    ['JHANG DISTRICT', 'TOBA TEK SINGH DISTRICT'],
    ['NASIRABAD DISTRICT', 'KACHHI DISTRICT'],
]
