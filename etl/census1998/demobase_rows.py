"""The 1998 census by tehsil, from the US Census Bureau's Pakistan Demobase, as
census-panel rows on PBS's 2023 tehsils.

Demobase holds the 1998 census (PBS's census CD) for 365 tehsils on their
1998 boundaries. tehsil_crosswalk.py links them to the 2017 tehsils in groups
whose 1998 populations balance against PBS's own restatement, and every
figure here is the group's: counts are summed over its 1998 tehsils, and a
rate is worked out from those summed counts - never averaged - so a group of
three tehsils has the literacy rate of the three together.

What it holds, and what it does not claim:
  - literacy is calculated from the literate and 10+ counts (the source's
    printed ratios are whole numbers); nationally it gives PBS's published
    43.92% excluding FATA, and every province's printed rate;
  - average household size is household population over households, rebuilt
    from the printed rural and urban averages and their household counts;
  - Demobase adjusted some age distributions (75+ split by IDB, FATA female
    ages from NWFP), so the age rows carry that note;
  - the source wrote census dashes as zeros, so a zero can mean unavailable.
"""
import collections

LANGS = [('URDU', 'URDU', 'M_URDU', 'F_URDU'), ('PUNJABI', 'PUNJABI', 'M_PUNJABI', 'F_PUNJABI'),
         ('SINDHI', 'SINDHI', 'M_SINDHI', 'F_SINDHI'), ('PUSHTO', 'PUSHTO', 'M_PUSHTO', 'F_PUSHTO'),
         ('BALOCHI', 'BALOCHI', 'M_BALOCHI', 'F_BALOCHI'), ('SARAIKI', 'SARAIKI', 'M_SARAIKI', 'F_SARAIKI'),
         ('OTHERS', 'OTHERS_1', 'M_OTHERS_1', 'F_OTHERS_1'), ('TOTAL', 'TOTAL', 'M_TOTAL', 'F_TOTAL')]
EDU = [('TOTAL', 'T_10_PLUS', 'M_10_PLUS', 'F_10_PLUS'), ('BELOW PRIMARY', 'BELOW__PRI', 'M_BLW__PRI', 'F_BLW__PRI'),
       ('PRIMARY', 'PRIMARY', 'M_PRIMARY', 'F_PRIMARY'), ('MIDDLE', 'MIDDLE', 'M_MIDDLE', 'F_MIDDLE'),
       ('MATRIC', 'MATRIC', 'M_MATRIC', 'F_MATRIC'), ('INTERMEDIATE', 'INTERMED', 'M_INTERMED', 'F_INTERMED'),
       ('BA/BSC OR EQUIVALENT', 'BA_BSC_EQ', 'M_BABSC_EQ', 'F_BABSC_EQ'),
       ('MA/MSC OR EQUIVALENT', 'MA_MSC_EQ', 'M_MAMSC_EQ', 'F_MAMSC_EQ'),
       ('DIPLOMA/CERTIFICATE', 'DIPLOMA_CR', 'M_DIPLO_CT', 'F_DIPLO_CT'), ('OTHERS', 'OTHERS', 'M_OTHERS', 'F_OTHERS')]
LIT = [('POPULATION 10+', 'T_10PLUS', 'M_T10PLUS', 'F_T10PLUS'), ('LITERATE', 'T_LIT', 'M_LIT', 'F_LIT'),
       ('ILLITERATE', 'T_ILL', 'M_ILL', 'F_ILL'), ('LITERATE, FORMAL', 'T_LIT_FOR', 'M_LIT_FOR', 'F_LIT_FOR'),
       ('LITERATE, INFORMAL', 'T_LIT_INF', 'M_LIT_INF', 'F_LIT_INF')]
SIZES = ['1PERSON', '2PERSON', '3PERSON', '4PERSON', '5PERSON', '6PERSON', '7PERSON', '8PERSON', '9PERSON']
AGES = ['00_04', '05_09', '10_14', '15_19', '20_24', '25_29', '30_34', '35_39', '40_44', '45_49',
        '50_54', '55_59', '60_64', '65_69', '70_74', '75_79', '80_']
SEXES = (('all', 1), ('male', 2), ('female', 3))
SOURCE = 'US Census Bureau, Pakistan Demobase (1998 census by tehsil, from PBS’s census CD)'


def num(r, f):
    v = r.get(f)
    try:
        return float(v) if v not in (None, '') else 0.0
    except ValueError:
        return 0.0


def specs():
    """(indicator, col_label, sex, locality, field or callable, is_rate, note)"""
    out = [('POPULATION - 1998 BY SEX', 'MALE', 'all', 'all', 'C_98_MALE', False, None),
           ('POPULATION - 1998 BY SEX', 'FEMALE', 'all', 'all', 'C_98FEMALE', False, None)]
    for col, *fs in LANGS:
        for (sx, j) in SEXES:
            out.append(('POPULATION BY MOTHER TONGUE', col, sx, 'all', fs[j - 1], False, None))
    for col, *fs in LIT:
        for (sx, j) in SEXES:
            out.append(('LITERACY (10+)', col, sx, 'all', fs[j - 1], False, None))
    for col, *fs in EDU:
        for (sx, j) in SEXES:
            out.append(('LITERATE POPULATION (10+) BY EDUCATIONAL ATTAINMENT', col, sx, 'all',
                        fs[j - 1], False, None))
    for loc, p, tot in (('all', 'T_', 'TOTAL_HH'), ('rural', 'R_', 'R_TOTALHH'), ('urban', 'U_', 'U_TOTALHH')):
        out.append(('HOUSEHOLDS', 'TOTAL', 'all', loc, tot, False, None))
        for s in SIZES:
            out.append(('HOUSEHOLDS BY SIZE', s.replace('PERSON', ' PERSON' + ('' if s == '1PERSON' else 'S')),
                        'all', loc, p + s, False, None))
        out.append(('HOUSEHOLDS BY SIZE', '10+ PERSONS', 'all', loc,
                    'T_10_PLU_1' if loc == 'all' else p + '10_PLUS', False, None))
    for loc, cols in (('rural', [('TOTAL', 'R_HHTOTAL'), ('PACCA', 'R_PACCA'), ('SEMI-PACCA', 'R_SEMI_PAC'),
                                 ('KACHA', 'R_KACHA')]),
                      ('urban', [('TOTAL', 'U_HHTOTAL'), ('PACCA', 'U_PACCA'), ('SEMI-PACCA', 'U_SEMIPAC'),
                                 ('KACHA', 'R_KACHA_1')])):   # R_KACHA_1 completes the urban total
        for col, f in cols:
            out.append(('HOUSING UNITS BY CONSTRUCTION', col, 'all', loc, f, False, None))
    agenote = ('Demobase adjusted some 1998 age distributions: it split 75+ into 75-79 and 80+ '
               'from the US IDB, and estimated FATA’s female ages from NWFP.')
    for a in AGES:
        lab = a.replace('_', '-').rstrip('-') + ('+' if a.endswith('_') else '')
        for sx, pre in (('all', 'C_98T'), ('male', 'C_98M'), ('female', 'C_98F')):
            out.append(('POPULATION BY AGE', lab, sx, 'all', pre + a, False, agenote))
    return out


def rates(S):
    """Rates from a group's summed counts S (field -> sum). None where the
    denominator is zero (the source writes unavailable as zero)."""
    div = lambda a, b, k=100: (k * a / b) if b else None
    out = [('SEX RATIO', 'SEX RATIO', 'all', 'all', div(S['C_98_MALE'], S['C_98FEMALE']))]
    for sx, t, l in (('all', 'T_10PLUS', 'T_LIT'), ('male', 'M_T10PLUS', 'M_LIT'),
                     ('female', 'F_T10PLUS', 'F_LIT')):
        out.append(('LITERACY RATIO (10+)', 'LITERACY RATIO', sx, 'all', div(S[l], S[t])))
    # household population over households, rebuilt from the printed rural
    # and urban averages (avg x households = household population)
    hp_r, hp_u = S['_hp_r'], S['_hp_u']
    hh_r, hh_u = S['_hh_r'], S['_hh_u']
    out.append(('AVERAGE HOUSEHOLD SIZE', 'AVERAGE HOUSEHOLD SIZE', 'all', 'all',
                div(hp_r + hp_u, hh_r + hh_u, 1)))
    out.append(('AVERAGE HOUSEHOLD SIZE', 'AVERAGE HOUSEHOLD SIZE', 'all', 'rural', div(hp_r, hh_r, 1)))
    out.append(('AVERAGE HOUSEHOLD SIZE', 'AVERAGE HOUSEHOLD SIZE', 'all', 'urban', div(hp_u, hh_u, 1)))
    return out


def group_sums(recs):
    fields = {s[4] for s in specs()} | {'C_98_TOTAL', 'C_98_MALE', 'C_98FEMALE', 'T_10PLUS', 'T_LIT',
                                         'M_T10PLUS', 'M_LIT', 'F_T10PLUS', 'F_LIT'}
    S = collections.defaultdict(float)
    for r in recs:
        for f in fields:
            S[f] += num(r, f)
        # a printed average with households but no average is unavailable,
        # and so is that part of the household population
        if num(r, 'R_TOTALHH') and num(r, 'R_AVG_HHSZ'):
            S['_hp_r'] += num(r, 'R_AVG_HHSZ') * num(r, 'R_TOTALHH')
            S['_hh_r'] += num(r, 'R_TOTALHH')
        if num(r, 'U_TOTALHH') and num(r, 'U_AVG_HHSZ'):
            S['_hp_u'] += num(r, 'U_AVG_HHSZ') * num(r, 'U_TOTALHH')
            S['_hh_u'] += num(r, 'U_TOTALHH')
    return S
