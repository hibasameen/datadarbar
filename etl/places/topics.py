"""The one topic vocabulary Places browses by.

The design names ten topics and they are subjects, not sources. Two things had
drifted from that:

  "Census" was a topic of its own, which files 37,971 series under one heading
  and reproduces the source-oriented structure the redesign exists to remove.
  Census tables are assigned by what they measure, in census_topics.py.

  The curated side carried app.js's twelve topics, whose labels nearly but not
  quite matched - "Health" and "Children" against "Health & children",
  "Housing & Infrastructure" against "Housing & infrastructure". Near-matching
  labels do not merge, so the picker showed both.

One vocabulary, used by both halves. An eleventh topic, Migration, was added
when the emigration series arrived: the design's ten were drawn before that
data existed.
"""

TOPICS = {
    'demographics': 'Demographics',
    'education':    'Education',
    'employment':   'Employment',
    'welfare':      'Household welfare',
    'poverty':      'Poverty & wealth',
    'housing':      'Housing & infrastructure',
    'health':       'Health & children',
    'access':       'Schools & access to care',
    'rural':        'Rural facilities (Mouza 2020)',
    'satellite':    'Satellite & environment',
    'migration':    'Migration',
}

# The order the picker lists them in: the big subject topics first, then the
# ones that are about a particular source or instrument.
ORDER = ['demographics', 'education', 'employment', 'welfare', 'poverty',
         'housing', 'health', 'migration', 'access', 'rural', 'satellite']

# Curated group -> topic. Where a group sat under an app.js topic that the
# design does not have, it moves to the nearest one the design does:
# Economic Activity into Employment, ICT & Digital into Household welfare,
# Women's Empowerment and Children into Health & children.
GROUP_TOPIC = {
    'demographics': 'demographics', 'urbanRural': 'demographics',
    'migration': 'migration',

    'literacy': 'education', 'censusSchooling': 'education',
    'education': 'education', 'pslmEducation': 'education',

    'employment': 'employment', 'pslmEmployment': 'employment',
    'lfs': 'employment', 'lfs25': 'employment', 'econCensus': 'employment',

    'pslmFies': 'welfare', 'hies': 'welfare',
    'pslmDigital': 'welfare', 'hiesIct': 'welfare',

    'mpi': 'poverty',

    'micsWash': 'housing', 'pslmWash': 'housing',
    'hiesHousing': 'housing', 'hiesWaste': 'housing',

    'micsMaternal': 'health', 'micsChildHealth': 'health',
    'micsNutrition': 'health', 'pslmHealth': 'health',
    'dhsFamilyPlanning': 'health', 'dhsFertility': 'health',
    'dhsMaternal': 'health', 'dhsImmunisation': 'health', 'dhsNutrition': 'health',
    'micsWomen': 'health', 'hiesDecisions': 'health',
    'micsProtection': 'health', 'micsEquity': 'health',

    # Distance and travel time are about reaching a service, not about the
    # service itself, so they form their own topic as the design has it.
    'schoolAccess': 'access', 'healthAccess': 'access',
    'healthAccessDistrict': 'access',

    # Satellite-derived measures sit together whatever they are about: they
    # share a method and its caveats rather than a subject.
    'rwi': 'satellite', 'satPop': 'satellite', 'nightlights': 'satellite',
}
for _g in ('electricity_energy', 'drinking_water', 'streets_roads', 'housing',
           'schools', 'health', 'connectivity', 'cooking_fuel',
           'markets_credit', 'hazards', 'settlement'):
    GROUP_TOPIC['mouza_' + _g] = 'rural'
