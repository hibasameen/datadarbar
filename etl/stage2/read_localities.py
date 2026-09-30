"""Read the individual-locality tables (31-34) into tidy observations.

These differ from the unit tables in kind: a row is a *place* — a mauza, deh or
urban locality — not a published administrative unit. Places nest inside
revenue groupings that the tables print as subtotal rows, and those groupings
are named differently by province or territory:

    Punjab, KP, Balochistan   district -> tehsil -> QH -> PC -> mauza
    Sindh                     district -> taluka -> STC -> TC -> deh

Every QH, PC, STC and TC row is a subtotal of its children. Summing a column
without excluding them roughly triples the national population.
"""
import hashlib, re
from read_workbook import (anchor, numbered_columns, header, txt, cell, unit_type,
                           ADMIN, OTHER_UNIT)
from unit_aliases import apply as apply_alias

# Grouping levels, outermost first. The revenue hierarchy is not the same in
# every area: KP's ex-FATA districts nest TRIBE and SECTION between the
# tehsil and the village, and both KP and Balochistan use union councils in
# places. Omitting them counts the same people three times or more — Bajaur's
# mauza level over-counts by exactly 2.99x without TRIBE and SECTION.
# The tier word is not always the last thing on the line. Bajaur prints its
# sections as "MAMUND SECTION-I (PART)", Khyber as "SECTION NO 1", and Karachi
# West numbers its tapedar circles "MANGOPIR TC II", so a tier word may be
# followed by a part number, a roman numeral or a (PART) qualifier. Anchoring at
# the end of the line missed 42 such rows in 2023 — Bajaur's whole 595,702
# over-count and Khyber's whole 26,429. 2017 has none at village level: its
# hadbast numbers let the arithmetic rule catch the same rows instead.
GROUPING = re.compile(r'(?:^|\s)(QH|PC|STC|TC|UC|TRIBE|SECTION|CIRCLE)'
                      r'(?:[-\s]*(?:NO\.?\s*)?[0-9IVXL]+)?'
                      r'(?:\s*\(PART\))?$')
LEVEL_OF = {'TRIBE': 'tribe', 'SECTION': 'section', 'UC': 'union_council',
            'QH': 'qanungo_halqa', 'PC': 'patwar_circle',
            'STC': 'supervisory_tapedar_circle', 'TC': 'tapedar_circle',
            # Gwadar, alone in the country, nests a revenue circle between the
            # union council and the village: NILNAT UC 16,172 contains CHAKLI,
            # KAPUR and NILNAT CIRCLE, which add to exactly that, each above its
            # own villages. Read as villages they double the UC. Named CIRCLE and
            # not PC because that is the word Gwadar's own table uses, and
            # distinct from the urban tables' CIRCLE NO nn, which is a census
            # operational unit rather than a revenue one.
            'CIRCLE': 'revenue_circle'}
# Which levels are "outer" (they reset the inner one) rather than the immediate
# parent of a village.
OUTER = {'tribe', 'union_council', 'qanungo_halqa', 'supervisory_tapedar_circle'}

# The urban tables nest two census operational levels inside each named
# locality: CHARGE NO nn and, under that, CIRCLE NO nn. Their numbers restart in
# every locality, so "CIRCLE NO 01" is meaningless without its parents.
CHARGE = re.compile(r'^CHARGE\s*N[O0]?\b')
CIRCLE = re.compile(r'^CIRCLE\s*N[O0]?\b')


# The innermost grouping tier has a different name in Sindh, so a row relabelled
# as the immediate parent of villages must be named for its own area.
INNER_LEVEL = {'SINDH': 'tapedar_circle'}


def relabel_unsuffixed_groupings(recs, key):
    """Reclassify subtotal rows that PBS published without a suffix.

    Three shapes occur, and each is confirmed by arithmetic rather than inferred
    from the name:

    * a row whose value equals the single row beneath it — Panjgur prints a bare
      "GICHK" between GICHK SUB-TEHSIL and GICHK UC, all three 33,578, a chain
      of single-child levels with no suffix on the middle one;
    * a row whose value equals the sum of the run of villages beneath it —
      Okara prints "CHAK NO 016/1-L" as a subtotal above the four mauzas that
      add to exactly its 20,573. The run may include villages whose hadbast
      number was not printed: Karachi West's MANGOPIR TC II is exactly the sum
      of five dehs, two of which (HUB, MAIGARHI) carry no number;
    * a row whose value equals the sum of the run of *groupings* beneath it,
      which sits a tier higher again. Bahawalpur's ABLANI-QH is ABLANI PC +
      KHAIRO GHAZI KHANANA PC + TALHER PC exactly, and MULTAN CANTONMENT is the
      sum of the patwar circles printed under it. Balochistan's restatements of
      a sub-division at village depth are the same shape: PANJGUR TEHSIL 178,752
      is followed by a bare PANJGUR 178,752 and then the union councils that add
      to it. Matching the *parent's* value instead would be far too loose — a
      union council with one village shares that village's population, and
      treating the village as a subtotal deletes it.

    A row is only reclassified when the equality is exact, and never when it
    carries a hadbast number, which marks it as a real village. The arithmetic
    is what authorises the change, not the name: most rows lacking a hadbast are
    ordinary villages whose number PBS simply left blank, and relabelling on the
    missing number alone would corrupt them.
    """
    n = len(recs)
    for i, r in enumerate(recs):
        if r['level'] != 'mauza' or r['hadbast'] or not r.get(key):
            continue
        # single-child chain: identical to whatever follows
        if i + 1 < n and recs[i + 1].get(key) == r[key]:
            r['level'] = 'unnamed_grouping'
            r['relabelled'] = True
            r['relabel_rule'] = 'single_child_chain'
            continue
        # subtotal of the run of villages that follows. The numbered run is
        # tried first and the unnumbered extension only as a fallback: a run
        # that stops at a village with no hadbast is often the right run, and
        # reading past it overshoots the subtotal and loses the match.
        run, rule = None, None
        for numbered_only in (True, False):
            cand, j = [], i + 1
            while j < n and recs[j]['level'] == 'mauza' and (
                    recs[j]['hadbast'] or not numbered_only):
                cand.append(recs[j]); j += 1
            if len(cand) >= 2 and abs(sum(x.get(key) or 0 for x in cand) - r[key]) < 0.5:
                run = cand
                rule = 'village_run' if numbered_only else 'village_run_unnumbered'
                break
        if run is not None:
            r['level'] = INNER_LEVEL.get(r.get('province_area'), 'patwar_circle')
            r['relabelled'] = True
            r['relabel_rule'] = rule
            r['patwar_circle'] = r['name']
            for x in run:
                x['patwar_circle'] = r['name']
                x['parent_id'] = r['own_id']
            continue
        # subtotal of a run of groupings, one tier higher again. The run is the
        # groupings at a single level: stopping at the first grouping of another
        # level keeps a qanungo halqa's circles from absorbing the next halqa's.
        run, j, lvl = [], i + 1, None
        while j < n and recs[j]['level'] not in ('district', 'sub_district'):
            if recs[j]['level'] != 'mauza':
                if lvl is None:
                    lvl = recs[j]['level']
                elif recs[j]['level'] != lvl:
                    break
                run.append(recs[j])
            j += 1
        if len(run) >= 2 and abs(sum(x.get(key) or 0 for x in run) - r[key]) < 0.5:
            r['level'] = 'unnamed_grouping'
            r['relabelled'] = True
            r['relabel_rule'] = 'grouping_run'
    return recs


def place_id(province_area, path, name, hadbast, seq):
    """A stable identifier for a place, independent of which table it came from.

    Tables 31 and 32 describe the same villages — population and housing — but
    do not have the same row counts, so a row-position identifier cannot join
    them. The id is built from the place's position in the hierarchy instead,
    with a sequence number to separate the handful of same-named siblings.
    """
    parts = [province_area] + [p or '' for p in path] + [name or '', hadbast or '', str(seq)]
    h = hashlib.sha256('\u241f'.join(parts).encode()).hexdigest()[:16]
    return f'DDL-{h}'


def read(rows, province_area, table, merges=(), urban=None):
    """Yield one observation per place.

    `urban` says whether this is an urban-locality table, where the second column
    is a measurement rather than a hadbast number and places nest in charges and
    circles instead of revenue circles. It defaults to 2023's numbering; Census
    2017 publishes the same four tables as 23-26 and passes it explicitly.
    """
    a = anchor(rows)
    if a is None:
        raise ValueError(f'table {table}: no column-number row')
    stub, data_cols = numbered_columns(rows, a)
    cols = header(rows, a, stub, merges, data_cols)
    urban = (table in ('33', '34')) if urban is None else urban
    # In the rural tables column 2 holds the hadbast / deh number, which is an
    # identifier rather than a measurement.
    id_col = data_cols[0] if not urban else None
    measure_cols = [c for c in data_cols if c != id_col]
    def ident(name, hadbast, path):
        k = (tuple(path), name, hadbast)
        seq = seen.get(k, 0)
        seen[k] = seq + 1
        return place_id(province_area, path, name, hadbast, seq)

    district = subdist = qh = pc = None
    locality = charge = None
    own_override = None
    # A grouping is identified by where it appears, not by its name. Dera Ismail
    # Khan's Paharpur tehsil publishes two different patwar circles both called
    # WANDA KHAN MOHD PC; keying children on the name merges them and their
    # populations stop reconciling.
    pc_id = qh_id = charge_id = None
    seen = {}          # how many times this exact path+name has appeared
    for i, r in enumerate(rows):
        if i <= a:
            continue
        label = txt(r[stub]) if stub < len(r) else None
        if not label:
            continue
        # The corpus-wide alias corrections apply here as they do to the unit
        # tables. One matters: these tables print Malakand as MALAKAND PROTECTED
        # AREA, a name carrying none of the words that mark a unit, so without
        # the alias the district's own row reads as a village. In 2017 that
        # orphaned all 650,120 of its rural residents from the reconciliation;
        # in 2023 it attached Malakand's tehsils to Lower Kohistan, which then
        # over-counted by 445%.
        label, _ = apply_alias(table, label)
        U = label.upper()
        if U.isdigit() or U.startswith(('TABLE', 'NAME OF', 'HADBAST', 'URBAN LOCAL')):
            continue
        ut = unit_type(label) if (ADMIN.search(U) or OTHER_UNIT.search(U)) else None
        if ut == 'district':
            district, subdist, qh, pc = label, None, None, None
            locality = charge = None
            level = 'district'
        elif ut:
            subdist, qh, pc = label, None, None
            locality = charge = None
            level = 'sub_district'
        else:
            level = None
        if level is None:
            level = 'mauza'
        if urban and level not in ('district', 'sub_district'):
            if CHARGE.match(U):
                charge_id = own_override = ident(label, None, [district, subdist, locality])
                charge = label
                level = 'charge'
            elif CIRCLE.match(U):
                level = 'circle'
            else:
                locality, charge = label, None
                charge_id = None
                level = 'locality'
        g = GROUPING.search(U)
        if g and not urban:
            # Subtotal rows are emitted too, tagged with their level. They are
            # published figures and useful for validation, but they must never
            # be summed with their children: doing so roughly triples the
            # national population.
            kind = LEVEL_OF[g.group(1)]
            if kind in OUTER:
                # The id is computed before this row becomes the current
                # grouping, so a grouping's own_id is exactly the parent_id its
                # children carry.
                qh_id = own_override = ident(label, None, [district, subdist])
                qh, pc, pc_id = label, None, None
            else:
                pc_id = own_override = ident(label, None, [district, subdist, qh])
                pc = label
            level = kind
        vals, miss = {}, []
        for c in measure_cols:
            if c >= len(r):
                continue
            v, kind = cell(r[c])
            name = cols.get(c, f'col{c+1}')
            if kind == 'number':
                vals[name] = v
            elif kind == 'dash':
                miss.append(name)
        if not vals and not miss:
            own_override = None
            continue
        hadbast = None
        if id_col is not None and id_col < len(r):
            hv, hk = cell(r[id_col])
            if hk in ('number', 'text'):
                hadbast = str(hv).strip()
        yield dict(province_area=province_area, table_id=table, district=district,
                   sub_district=subdist, qanungo_halqa=qh, patwar_circle=pc,
                   locality=locality if urban else label,
                   charge=charge if urban and level != 'charge' else
                          (label if level == 'charge' else None),
                   name=label, level=level, hadbast=hadbast,
                   parent_id=(charge_id if urban else pc_id or qh_id),
                   own_id=own_override or ident(label, hadbast,
                                [district, subdist, locality, charge] if urban
                                else [district, subdist, qh, pc]),
                   locality_type='urban' if urban else 'rural',
                   missing=';'.join(miss), src_row=i + 1, **vals)
        own_override = None
