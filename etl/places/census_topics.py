"""Which subject topic each PBS census table belongs under.

The redesign's Places picker browses by subject - Demographics, Education,
Housing - and "Census" is a source, not a subject. Filing all 37,971 census
series under a topic called "census" reproduces exactly the source-oriented
structure the redesign exists to remove, and buries the census under one
heading while PSLM and LFS indicators sit in the open.

So every table is placed under one of the ten topics the design names. The
assignment is by what the table measures, not by which census published it.

A note on the year, because the design assumes more of it than the data
supports. It says the facets of a chosen indicator - census year, rural/urban,
sex - are picked afterwards. Locality and sex are real facets: most definitions
carry three or nine variants. The year is not. Of 4,039 district cell
definitions, **34 exist in both censuses** - 58 if table numbering is ignored.
For the other 99 per cent the year is part of what the indicator *is*, not a
choice about it, because the two censuses did not publish the same tables. The
index therefore records which years a definition actually has, and the picker
must not offer a year that does not exist.
"""

TOPIC_OF_TABLE = {}
for _t in ('1', '3', '4', '5', '6', '7', '8', '9', '10', '11', '20', '21', '22'):
    TOPIC_OF_TABLE[_t] = 'demographics'          # area, age, sex, religion,
                                                 # language, nationality, CNIC,
                                                 # living abroad, homelessness
for _t in ('12', '13', '13a', '13b', '14', '15'):
    TOPIC_OF_TABLE[_t] = 'education'             # literacy and attainment
for _t in ('16',):
    TOPIC_OF_TABLE[_t] = 'employment'            # usual activity
for _t in ('28', '40'):
    TOPIC_OF_TABLE[_t] = 'welfare'               # household size, information
for _t in ('17', '18', '19'):
    TOPIC_OF_TABLE[_t] = 'health'                # disability
for _t in ('23', '24', '25', '26', '27', '29', '30', '31', '32', '33',
           '34', '35', '36', '37', '38', '39'):
    TOPIC_OF_TABLE[_t] = 'housing'               # structures, tenure,
                                                 # materials, water, fuel

# Labels live in topics.py, which both halves of the index read. Keeping a
# second copy here is what let "Health" and "Health & children" coexist.
