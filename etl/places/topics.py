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
when the emigration series arrived, and a twelfth, Agriculture, with the crop
series: the design's ten were drawn before either existed.
"""

TOPICS = {
    'demographics': 'Demographics',
    'education':    'Education',
    'employment':   'Employment',
    'welfare':      'Household welfare',
    'poverty':      'Poverty & wealth',
    'housing':      'Housing',
    'infrastructure': 'Infrastructure & utilities',
    'economic':     'Economic activity',
    'health':       'Health & children',
    'access':       'Schools & access to care',

    'satellite':    'Satellite & environment',
    'migration':    'Migration',
    'agriculture':  'Agriculture',
    # Premises, not utilities: the census counted structures and classified
    # them, so a shop, a mosque and a cattle shed are one measurement. They
    # stay together, but the topic no longer carries its source in its name -
    # the same reason "Rural facilities (Mouza 2020)" was retired.
    'facilities':   'Buildings & premises',
}

# The order the picker lists them in: the big subject topics first, then the
# ones that are about a particular source or instrument.
ORDER = ['demographics', 'education', 'employment', 'economic', 'welfare',
         'poverty', 'housing', 'infrastructure', 'health', 'agriculture',
         'migration', 'access', 'facilities', 'satellite']

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
    'lfs': 'employment', 'lfs25': 'employment',

    # Who works, and what the work is, are different questions. Employment is
    # about people; Economic activity is about establishments and the places
    # that serve them.
    'econCensus': 'economic',

    'pslmFies': 'welfare', 'hies': 'welfare',
    'pslmDigital': 'welfare', 'hiesIct': 'welfare',

    'mpi': 'poverty',

    'hiesHousing': 'housing',
    'micsWash': 'infrastructure', 'pslmWash': 'infrastructure',
    'hiesWaste': 'infrastructure',

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
# The Mouza census was its own topic, "Rural facilities (Mouza 2020)", which
# is a source wearing a subject's clothes - the thing this file exists to stop.
# Its groups file by what they measure, like every other source, and what they
# mostly measure is infrastructure.
for _g in ('electricity_energy', 'drinking_water', 'streets_roads',
           'connectivity', 'cooking_fuel', 'hazards'):
    GROUP_TOPIC['mouza_' + _g] = 'infrastructure'
GROUP_TOPIC['mouza_housing'] = 'housing'
GROUP_TOPIC['mouza_settlement'] = 'demographics'
GROUP_TOPIC['mouza_markets_credit'] = 'economic'
# A school or a clinic in the village is about reaching a service, which is
# what the access topic is for.
GROUP_TOPIC['mouza_schools'] = 'access'
GROUP_TOPIC['mouza_health'] = 'access'
