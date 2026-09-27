"""How the curated layers' district names land on PBS's 2023 district codes.

`district_indicators` is keyed on a slug of the district's name, drawn from a
2015 boundary set with 147 districts. PBS's Digital Census 2023 layer draws 157.
135 of the slugs match a PBS district on name alone; the twelve below do not,
and each was checked rather than fuzzy-matched into place.

Nine are spellings. PBS and the survey frames transliterate the same place
differently - Astore against Astor, Chagai against Chaghi - and the fuzzy score
for each is high and unambiguous.

One is a second name rather than a spelling: AJK's Jhelum Valley district is
universally called Hattian, after Hattian Bala, its headquarters. No string
similarity would find that pair, and the best fuzzy candidate for it was Thatta,
in a different province - which is the reason this file exists rather than a
similarity threshold.

Seven are real boundary changes, where one district in the older frame is
several in PBS's. They are handled the way the census crosswalk handles a split:
the figure is drawn across every successor at once, because the successors
together are the ground the parent covered, and it is never divided between
them.
"""

# slug -> PBS district code. A spelling, or a different name for the same place.
ALIAS = {
    'astor':            '131',   # ASTORE
    'chaghi':           '099',   # CHAGAI
    'diamir':           '132',   # DIAMER
    'ghanchi':          '130',   # GHANCHE
    'musakhail':        '112',   # MUSAKHEL
    'naushehro feroze': '082',   # NAUSHAHRO FEROZE
    'sajawal':          '152',   # SUJAWAL
    'sudhnutti':        '145',   # SUDHNOTI
    'hattian':          '139',   # JHELUM VALLEY - Hattian Bala is its seat
}

# slug -> the PBS districts it became. One value, drawn across all of them.
# The first four are the same splits the census crosswalk found, arrived at
# from the other side: there, PBS's restated 2017 population proved the group
# balanced; here, the older frame simply has the parent and PBS has the
# children. The last three are Gilgit-Baltistan and Khyber Pakhtunkhwa
# reorganisations that the census does not cover at all.
SPLIT = {
    'killa abdullah': ['100', '167'],         # Killa Abdullah and Chaman
    'loralai':        ['111', '165'],         # Loralai and Duki
    'kalat':          ['086', '164'],         # Kalat and Surab
    'chitral':        ['015', '170'],         # Lower and Upper Chitral
    'kohistan':       ['008', '168', '169'],  # Kolai Palas, Upper and Lower Kohistan
    'hunza nagar':    ['135', '157'],         # Hunza and Nagar
    'skardu':         ['129', '155', '156'],  # Skardu, Kharmang and Shigar
}

# How a unit relates to the 2023 frame, in the words the map shows a reader.
NOTE = {
    'alias': '{old} is {new} in PBS’s 2023 district list',
    'split': '{old} is now {new}. The figure is for the whole of the old '
             'district and is shown across all of them, not divided between them',
}
