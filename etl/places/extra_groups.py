"""Indicator groups that originate in the ETL rather than in app.js.

curated_groups.json is lifted from app.js, which is where the site's indicator
vocabulary has lived. Anything ingested from here on is declared here instead:
the app will come to read its vocabulary from place_indicator_index, and a new
group should not have to be written into app.js first to be visible.

Same shape as a curated_groups.json entry.
"""

GROUPS = [
    {
        'group_key': 'migration',
        'topic': 'migration',
        # The design's ten Places topics were drawn before this data existed and
        # have no home for it. Emigration is labour migration in almost every
        # case, so Employment would do - but 9.9 million registered emigrants
        # reads oddly as a sub-topic of anything, and the series has its own
        # source, frame and period. Worth confirming as an eleventh topic.
        'topic_label': 'Migration',
        'group_label': 'Emigration — Bureau of Emigration',
        'dataset': 'Bureau of Emigration & Overseas Employment, via PBS 2026-09',
        'dp': {},
        'indicators': {
            'emigrants_registered':
                'Registered emigrants in the year',
            'emigrants_cumulative':
                'Registered emigrants, all years on record',
        },
    },
]
