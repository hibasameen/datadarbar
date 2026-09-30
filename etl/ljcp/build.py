"""Build isolated LJCP CSV/JSON/Parquet datasets without touching the app or DB."""
from __future__ import annotations
import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path
import re
import duckdb
from extract import FIELDS, JUDGES, HERE, WORKSPACE, is_total

def token(s):return re.sub('[^a-z0-9]','',s.lower())

ALIASES={
 'Punjab':{'dgkhan':'dera ghazi khan','mbdin':'mandi bahauddin','mandibahaudin':'mandi bahauddin','mandibhaudin':'mandi bahauddin','nandkanasb':'nankana sahib','nankanasb':'nankana sahib','rykhan':'rahim yar khan','ttsingh':'toba tek singh','pakpattansharif':'pakpattan'},
 'Sindh':{'south':'karachi south','west':'karachi west','east':'karachi east','central':'karachi central','karachimalir':'malir','kambershahdadkot':'kambar shahdadkot','kashmorekandhkot':'kashmore','naushahroferoze':'naushehro feroze','nausheroferoze':'naushehro feroze','sangher':'sanghar','sujawal':'sajawal','tandomkhan':'tando muhammad khan'},
 'Khyber Pakhtunkhwa':{'dikhan':'dera ismail khan','lakki':'lakki marwat','dirlower':'lower dir','dirupper':'upper dir','chitral':'chitral lower','lowerchitral':'chitral lower','upperchitral':'chitral upper','kollaipallas':'kolai pallas','kollaipallaskohistan':'kolai pallas','kolaipalaskohistan':'kolai pallas','battagram':'batagram','bajaurdistrict':'bajaur','khyberdistrict':'khyber','mohmanddistrict':'mohmand','orakzaidistrict':'orakzai','kurramdistrict':'kurram','swatbabuzai':'swat'},
 'Balochistan':{'gawadar':'gwadar','killahsaifullah':'killa saifullah','panjgoor':'panjgur','musakhail':'musakhail','noshki':'nushki','msatung':'mastung'},
}
ALIASES['Punjab']['nankana']='nankana sahib'
ALIASES['Khyber Pakhtunkhwa'].update({'bajour':'bajaur','kohistan':'kohistan upper','lowerkohistan':'kohistan lower'})

def canonical(name,province,pop):
    t=token(name)
    if province=='Islamabad':
        if 'east' in t:return 'islamabad east'
        if 'west' in t:return 'islamabad west'
        return 'islamabad'
    if t in ALIASES.get(province,{}):return ALIASES[province][t]
    matches={token(n):n for n in list(pop)+['chitral lower','chitral upper','kohistan lower','kohistan upper','kolai pallas','bajaur','khyber','mohmand','orakzai','kurram','north waziristan','south waziristan','basima','chaman','dalbandin','dera allah yar','dera murad jamali','dhadar','gandawah','hub','sariab','surab','turbat','usta muhammad','uthal']}
    return matches.get(t,re.sub(r'\s+',' ',name.lower()).strip())

def strict_sum(values):
    values=list(values)
    return sum(values) if values and all(v is not None for v in values) else None

def ratio(a,b,scale=1):return a/b*scale if a is not None and b is not None and b>0 else None

def flow_indicators(r):
    r['clearance_rate_pct']=ratio(r['disposed'],r['instituted'],100)
    r['backlog_change']=r['pending_end']-r['pending_start'] if None not in (r['pending_end'],r['pending_start']) else None
    r['backlog_growth_pct']=ratio(r['backlog_change'],r['pending_start'],100)
    if all(r.get(f) is not None for f in FIELDS):
        r['stock_flow_residual']=r['pending_end']-(r['pending_start']+r['instituted']+r['transfers_in']-r['transfers_out']-r['disposed'])
        r['stock_flow_check']='balanced' if r['stock_flow_residual']==0 else 'source_residual'
    else:
        r['stock_flow_residual']=None;r['stock_flow_check']='transfers_or_counts_not_reported'
    r['unadjusted_flow_residual']=r['pending_end']-r['pending_start']-r['instituted']+r['disposed'] if all(r.get(f) is not None for f in ['pending_start','pending_end','instituted','disposed']) else None
    return r

def base_case(r):
    return dict(year=r['year'],period_start=f"{r['year']}-01-01",period_end=f"{r['year']}-12-31",period_type='annual',province=r['province'],session_division=r['session_division'],category=r['category'],court_tier=r['court_tier'],
                **{f:r.get(f) for f in FIELDS}, source_record_ids=[r['record_id']],source_ids=[r['source_id']],
                pdf_pages=[r['pdf_page']],method='reported',scope_note=r['scope_note'],
                category_scope='all_categories' if r['category']=='all' else 'edition_specific_reported_category')

def combine(rs,method='sum_court_tiers'):
    r=rs[0].copy()
    for f in FIELDS:r[f]=strict_sum(x.get(f) for x in rs)
    for f in ['source_record_ids','source_ids','pdf_pages']:r[f]=sorted(set(v for x in rs for v in x[f]))
    r['court_tier']='all_courts';r['method']=method
    r['scope_note']='; '.join(sorted(set(x['scope_note'] for x in rs if x['scope_note'])))
    return r

def table_quality(obs):
    groups=defaultdict(list);checks=[]
    for r in obs:groups[(r['source_id'],r['province'],r['section'])].append(r)
    for key,rs in groups.items():
        detail=[r for r in rs if not r['is_total']]
        for total in [r for r in rs if r['is_total']]:
            for f in FIELDS+JUDGES+['working']:
                if f not in total:continue
                s=strict_sum(r.get(f) for r in detail)
                checks.append(dict(source_id=key[0],province=key[1],section=key[2],metric=f,
                                   detail_sum=s,reported_total=total[f],difference=s-total[f] if s is not None and total[f] is not None else None,
                                   status='not_comparable_missing_cells' if s is None or total[f] is None else ('pass' if s==total[f] else 'source_mismatch'),
                                   total_source_record_id=total['record_id']))
    return checks

def build_cases(obs):
    tiers=[base_case(r) for r in obs if r['kind']=='cases' and not r['is_total']]
    groups=defaultdict(list)
    for r in tiers:groups[(r['year'],r['province'],r['session_division'],r['category'])].append(r)
    annual=[]
    for key,rs in sorted(groups.items()):
        if key[1]=='Balochistan' and key[0]==2020 and key[3]=='criminal':continue
        if len(rs)>1:
            if {r['court_tier'] for r in rs}!={'civil_courts','sessions_courts'}:raise ValueError(f'Duplicate case records: {key}')
            annual.append(combine(rs))
        else:annual.append(rs[0])
    lookup={(r['year'],r['province'],r['session_division'],r['category']):r for r in annual}
    for r in list(annual):
        if r['province']=='Balochistan' and r['year'] in (2020,2024) and r['category']=='all':
            civil=lookup.get((r['year'],r['province'],r['session_division'],'civil'))
            if civil:
                derived=combine([r,civil],'all_minus_civil');derived['category']='criminal'
                for f in FIELDS:derived[f]=r[f]-civil[f] if r[f] is not None and civil[f] is not None else None
                if any(derived[f] is not None and derived[f]<0 for f in FIELDS):raise ValueError('Negative derived category')
                derived['scope_note']='Derived from the exhaustive civil/criminal province-level partition and corresponding all/civil district tables. 2020 printed criminal table repeats all cases; 2024 does not supply a separate criminal district table.'
                annual.append(derived)
    for r in annual:flow_indicators(r)
    return tiers,sorted(annual,key=lambda r:(r['year'],r['province'],r['session_division'],r['category']))

def build_judges(obs):
    groups=defaultdict(list)
    for r in obs:
        if r['kind']=='judges':groups[(r['source_id'],r['province'],r['section'])].append(r)
    out=[]
    for key,rs in groups.items():
        detail=[r for r in rs if not r['is_total']];totals=[r for r in rs if r['is_total']]
        # Sparse gender cells are kept null unless their column's known values
        # reconcile exactly to the printed total. Even then the assumption is
        # recorded, and original blanks survive in source_observations.json.
        zero_columns=set()
        numeric_fields=set(k for r in rs for k,v in r.items() if isinstance(v,int) or v is None)
        for f in numeric_fields:
            if not any(x in f for x in ('working_male','working_female')):continue
            if len(totals)==1 and totals[0].get(f) is not None and sum(r.get(f) or 0 for r in detail)==totals[0][f]:zero_columns.add(f)
        for raw in detail:
            if raw['province']=='Punjab' and raw['session_division']=='islamabad':continue
            if raw['province']=='Islamabad' and raw['session_division']=='islamabad':continue
            r=raw.copy();inferred=[]
            for f in zero_columns:
                if r.get(f) is None:r[f]=0;inferred.append(f)
            if 'sanctioned' in r:
                sanctioned=r['sanctioned'];working=r.get('working') if 'working' in r else strict_sum([r.get('working_male'),r.get('working_female')]);vacant=r.get('vacant')
            else:
                sanctioned=strict_sum(r.get(f'rank{i}_sanctioned') for i in range(4))
                working=strict_sum(r.get(f'rank{i}_working_{sex}') for i in range(4) for sex in ['male','female'])
                vacant=strict_sum(r.get(f'rank{i}_vacant') for i in range(4))
            ambiguous_date=r['year']==2022 and r['province']=='Punjab'
            if r['province']=='Islamabad':sanctioned=None;vacant=None
            out.append(dict(year=r['year'],as_of_date=f"{2021 if ambiguous_date else r['year']}-12-31",province=r['province'],session_division=r['session_division'],sanctioned_judges=sanctioned,working_judges=working,vacant_judges=vacant,
                            court_tier=r['court_tier'],
                            strength_date_status='edition_date_conflict' if ambiguous_date else 'same_year',
                            rank_coverage='rank_specific' if r['court_tier']!='all_courts' else ('four_reported_ranks_only' if r['year']==2022 and r['province']=='Balochistan' else 'consolidated_reported'),
                            working_definition='reported_working_total' if r['year']==2020 else 'working_in_field_excludes_excadre',
                            inferred_zero_fields=inferred,source_record_ids=[r['record_id']],source_ids=[r['source_id']],pdf_pages=[r['pdf_page']],scope_note=r['scope_note']))
    return out

# Court seats that LJCP reports separately but that are not map/census districts, keyed to the
# map unit that contains them. Where a district has two seats in a year (Quetta+Sariab,
# Uthal+Hub, Dera Allah Yar+Usta Muhammad, Kalat+Surab, Killa Abdullah+Chaman) the seats are
# summed; where only one seat is reported it is the whole district for that year. The map
# geometry predates the 2021-22 Balochistan splits (Chaman, Usta Muhammad, Surab), so those
# seats fold back into their parent map units rather than being withheld.
SEAT_TO_DISTRICT={'basima':'washuk','dalbandin':'chaghi','dera allah yar':'jaffarabad','dera murad jamali':'nasirabad','dhadar':'kachhi','gandawah':'jhal magsi','hub':'lasbela','uthal':'lasbela','sariab':'quetta','turbat':'kech','usta muhammad':'jaffarabad','chaman':'killa abdullah','surab':'kalat'}
# Map/census districts with no court seat of their own in a given year. Their cases are filed
# in the host division, so the host's population denominator includes them for that year and
# the district itself gets no observation. Applied only when the district has no reported unit
# in that year (Sujawal and Washuk acquire their own seats within the series). Counts are never
# split across polygons or counted against two populations.
HOSTED_DISTRICTS={'korangi':('Sindh','karachi east'),'keamari':('Sindh','karachi west'),'sajawal':('Sindh','thatta'),
                  'sherani':('Balochistan','zhob'),'sohbatpur':('Balochistan','jaffarabad'),'washuk':('Balochistan','kharan'),'harnai':('Balochistan','sibi')}

# Balochistan staffing tables name the seat and sometimes the district ("Chagai at Dalbandin",
# "Lasbella at Uthal", "Kuchlak"); keyed to the same map units as the case-flow seats.
BALOCHISTAN_STAFF_UNITS={'chagai at dalbanddin':'chaghi','chagai at dalbandin':'chaghi','d murad jamali/nasirabad':'nasirabad','dera murad jamali/ nasirabad':'nasirabad',
    'dera allah yar':'jaffarabad','usta muhammad':'jaffarabad','jhal magsi at gandawa':'jhal magsi','kachhi at dhadar':'kachhi','killa abdullah at chaman':'killa abdullah',
    'chaman':'killa abdullah','killa abdullah at jungie pir alizai':'killa abdullah','kuchlak':'quetta','sariab quetta':'quetta','lasbella at uthai':'lasbela','lasbella at uthal':'lasbela',
    'hub':'lasbela','mekran at turbat':'kech','surab':'kalat','washuk at basima':'washuk'}
FOUR_RANKS=['district_sessions_judges','additional_district_sessions_judges','senior_civil_judges','civil_judges_magistrates_family_judges']
OTHER_RANKS=['majlis_e_shoora','qazi']

def balochistan_consolidated(rank_rows,pop):
    """Sum Balochistan's rank-specific staffing tables to one row per map district.

    A rank absent for a district means no post of that rank, so ranks are summed as reported
    (not strict-summed). Majlis-e-Shoora members and Qazis are Balochistan-specific judicial
    officers; they are carried separately so the four-rank total stays comparable with the
    other provinces' consolidated tables.
    """
    groups=defaultdict(list)
    for r in rank_rows:
        if r['province']!='Balochistan':continue
        unit=BALOCHISTAN_STAFF_UNITS.get(r['session_division'],r['session_division'])
        if unit not in pop:continue
        groups[(r['year'],unit)].append(r)
    out=[]
    for (year,unit),rs in sorted(groups.items()):
        # Editions with a consolidated table (2020, 2022) are summed per map unit as printed;
        # editions with only rank tables (2023, 2024) are summed over the four ranks.
        four=[r for r in rs if r['court_tier'] in FOUR_RANKS+['all_courts']];other=[r for r in rs if r['court_tier'] in OTHER_RANKS]
        if not four:continue
        def total(rows,f):
            vals=[r[f] for r in rows if r[f] is not None]
            return sum(vals) if vals else None
        printed=[r for r in four if r['court_tier']=='all_courts']
        out.append(dict(year=year,as_of_date=f'{year}-12-31',province='Balochistan',session_division=unit,
                        sanctioned_judges=total(four,'sanctioned_judges'),working_judges=total(four,'working_judges'),vacant_judges=total(four,'vacant_judges'),
                        other_judicial_officers_working=total(other,'working_judges'),other_judicial_officers_sanctioned=total(other,'sanctioned_judges'),
                        court_tier='all_courts',strength_date_status='same_year',rank_coverage=(printed[0]['rank_coverage'] if printed else 'sum_of_four_reported_ranks'),
                        ranks_present=sorted(set(r['court_tier'] for r in four)),staff_units_included=sorted(set(r['session_division'] for r in rs)),
                        working_definition=(printed[0]['working_definition'] if printed else 'working_in_field_excludes_excadre'),inferred_zero_fields=sorted(set(f for r in rs for f in r['inferred_zero_fields'])),
                        source_record_ids=sorted(set(v for r in rs for v in r['source_record_ids'])),source_ids=sorted(set(v for r in rs for v in r['source_ids'])),
                        pdf_pages=sorted(set(v for r in rs for v in r['pdf_pages'])),scope_note='Consolidated by summing the separately printed rank tables; keyed to the map district that contains each staffing seat.'))
    return out

def mapping(province,name,year,pop):
    adm=name if name in pop else None;status='name_match_provisional';note='District-name correspondence; no independent judicial boundary geometry supplied.'
    if province=='Islamabad':adm='islamabad';status='aggregate_required';note='East and West must be summed once for the ICT population.'
    if province=='Khyber Pakhtunkhwa':
        if name in ['kohistan upper','kohistan lower','kolai pallas']:adm='kohistan';status='aggregate_required';note='Sum all three subdivisions to the existing Kohistan map/census unit.'
        elif name in ['chitral lower','chitral upper']:adm='chitral';status='aggregate_required';note='Sum both subdivisions to the existing Chitral map/census unit.'
        elif name in ['bajaur','khyber','kurram','mohmand','orakzai','north waziristan','south waziristan']:adm=name+' agency'
        if name=='south waziristan':note='Upper/Lower South Waziristan are reported as one division; the map unit is the undivided agency.'
    withheld=False
    if province=='Balochistan' and name in SEAT_TO_DISTRICT:
        adm=SEAT_TO_DISTRICT[name];status='seat_to_district';note=f'Court seat inside the {adm} map unit; summed with any other seat of the same district in the same year.'
    if province=='Sindh' and (name.startswith('karachi') or name=='malir'):
        note='Karachi court division matched to the census district of the same name; Korangi (East) and Keamari (West) have no separate division and are counted in the host population.'
    if province=='Sindh' and name=='thatta':
        note='Thatta division includes Sujawal in years before a separate Sujawal unit is reported; the host population then includes Sujawal.'
    if province=='Punjab' and name in ['chakwal','rawalpindi','gujranwala','gujrat','dera ghazi khan','muzaffargarh','rajanpur']:
        note='Sessions division matches the 36-district map geometry; the 2022-23 splits (Talagang, Murree, Wazirabad, Taunsa, Kot Addu) are not separate map units.'
    if adm not in pop:withheld=True;note='No matching population key in the supplied census dataset.'
    if withheld:status='needs_jurisdiction_review'
    return dict(year=year,province=province,session_division=name,adm2_key=adm,crosswalk_status=status,population_rate_eligible=not withheld,note=note)

CENSUS_2017=2017.21  # reference date 15 March 2017
CENSUS_2023=2023.17  # reference date 1 March 2023

def interpolated_population(pop,keys,year):
    """Year-end population from the 2017 and 2023 census counts, geometric growth per unit.

    A stock reported on 31 December of `year` is compared with the population at that date,
    so the exponent runs to year+1.0; 2024 extrapolates ten months beyond the 2023 census.
    Units without a 2017 count fall back to the fixed 2023 value; the basis records which.
    """
    t=(year+1-CENSUS_2017)/(CENSUS_2023-CENSUS_2017)
    p17=strict_sum(pop[k].get('t1_2017_pop_total') for k in keys);p23=strict_sum(pop[k].get('t1_2023_pop_total') for k in keys)
    if not p23:return None,'no_2023_population'
    if not p17:return int(p23),'fixed_2023_no_2017_count'
    growth=(p23/p17)**(1/(CENSUS_2023-CENSUS_2017))-1
    # Boundary changes between the censuses (e.g. Karachi West/Keamari) produce negative or
    # runaway implied growth; those units keep the fixed 2023 count rather than a bad trend.
    if not 0<=growth<=0.06:return int(p23),f'fixed_2023_implausible_growth_{growth:.3f}'
    return int(round(p17*(p23/p17)**t)),'geometric_2017_2023'

def district_data(annual,judges,pop):
    walks={};groups=defaultdict(list)
    for r in annual:
        k=(r['year'],r['province'],r['session_division'])
        walks[k]=mapping(r['province'],r['session_division'],r['year'],pop)
        m=walks[k]
        if m['population_rate_eligible']:groups[(r['year'],r['province'],m['adm2_key'],r['category'],r['court_tier'])].append(r)
    judge_lookup={(r['year'],r['province'],r['session_division']):r for r in judges}
    mapped=defaultdict(set)
    for m in walks.values():
        if m['population_rate_eligible']:mapped[(m['year'],m['province'])].add(m['adm2_key'])
    hosted=[]
    for (year,prov),adms in sorted(mapped.items()):
        for district,(hprov,host) in HOSTED_DISTRICTS.items():
            if hprov==prov and host in adms and district not in adms and district in pop:
                hosted.append(dict(year=year,province=prov,district=district,host_adm2_key=host,population=int(pop[district].get('t1_2023_pop_total') or 0),
                                   note='No separate court unit reported this year; counted in the host division and its population denominator.'))
    host_pop={(h['year'],h['province'],h['host_adm2_key']):[] for h in hosted}
    for h in hosted:host_pop[(h['year'],h['province'],h['host_adm2_key'])].append(h['district'])
    out=[];issues=[]
    for (year,prov,adm,cat,tier),rs in sorted(groups.items()):
        sessions=sorted(r['session_division'] for r in rs)
        required={'islamabad':{'islamabad east','islamabad west'},'kohistan':{'kohistan upper','kohistan lower','kolai pallas'},'chitral':{'chitral lower','chitral upper'}}.get(adm)
        if required and set(sessions)!=required:
            issues.append(dict(year=year,province=prov,adm2_key=adm,category=cat,issue='incomplete_aggregation_group',present=sessions,required=sorted(required)));continue
        if len(sessions)!=len(set(sessions)):raise ValueError('Duplicate sessions would duplicate the population numerator')
        r=combine(rs,'sum_sessions_to_map_unit' if len(rs)>1 else rs[0]['method']);r.pop('session_division')
        r['court_tier']=tier;r['adm2_key']=adm;r['sessions_included']=sessions
        r['crosswalk_status']='aggregated_to_existing_map_unit' if len(rs)>1 else ('seat_to_district' if sessions[0]!=adm else 'name_match_provisional')
        r['population_year']=2023;r['population_basis']='fixed_2023_census_benchmark_not_annual_population'
        extra=host_pop.get((year,prov,adm),[])
        r['population_keys']=[adm]+extra;r['hosted_districts']=extra
        population=strict_sum(pop[k].get('t1_2023_pop_total') for k in r['population_keys']);r['population']=int(population) if population else None
        r['pending_per_100k_population']=ratio(r['pending_end'],r['population'],100000)
        r['population_interpolated'],r['population_interpolation_basis']=interpolated_population(pop,r['population_keys'],year)
        r['pending_per_100k_interpolated']=ratio(r['pending_end'],r['population_interpolated'],100000)
        js=[judge_lookup.get((year,prov,s)) for s in sessions]
        if not all(js) and (year,prov,adm) in judge_lookup:js=[judge_lookup[(year,prov,adm)]]  # staffing keyed to the map district (Balochistan seat sums)
        usable=all(j and j['strength_date_status']=='same_year' and j['rank_coverage'] in ('consolidated_reported','sum_of_four_reported_ranks','four_reported_ranks_only') for j in js)
        r['working_judges']=strict_sum(j['working_judges'] for j in js) if usable else None
        r['sanctioned_judges']=strict_sum(j['sanctioned_judges'] for j in js) if usable else None
        r['judge_source_record_ids']=[v for j in js if j for v in j['source_record_ids']]
        r['judge_coverage_status']='matched' if usable and r['working_judges'] is not None else 'missing_unverified_or_date_mismatch'
        r['working_judges_per_million']=ratio(r['working_judges'],r['population'],1000000)
        r['working_judges_per_million_interpolated']=ratio(r['working_judges'],r['population_interpolated'],1000000)
        r['pending_per_working_judge']=ratio(r['pending_end'],r['working_judges']) if cat=='all' and tier=='all_courts' else None
        r['instituted_per_working_judge']=ratio(r['instituted'],r['working_judges']) if cat=='all' and tier=='all_courts' else None
        flow_indicators(r);out.append(r)
    return out,list(walks.values()),issues,hosted

CURATED_CATEGORIES={'all','civil','criminal'}

def build_categories(cat_obs,annual,pop):
    """Sessions-level case-category series from categories.py output.

    Punjab prints some categories per court tier; a category reported in both tiers is
    summed once, otherwise the single reported tier is kept with its scope. Categories
    that overlap the curated all/civil/criminal series are kept for cross-checking only.
    """
    for r in cat_obs:r['session_division']=canonical(r['name'],r['province'],pop)
    groups=defaultdict(list)
    for r in cat_obs:
        if r['is_total']:continue
        rec=base_case(r);rec['kind']='category_cases';rec['transfers']=r.get('transfers');rec['column_order_evidence']=r['column_order_evidence'];rec['section']=r['section']
        groups[(r['year'],r['province'],rec['session_division'],r['category'])].append(rec)
    rows=[]
    for key,rs in sorted(groups.items()):
        tiers={x['court_tier'] for x in rs}
        if len(rs)>1 and tiers=={'civil_courts','sessions_courts'}:
            r=combine(rs)
        elif len(rs)>1:
            # Several tables under one category label in the same tier (e.g. ICT 2020
            # "miscellaneous" printed per jurisdiction): keep them apart by section.
            for x in rs:
                x=x.copy();x['category']=f"{key[3]}__{x['section']}";x['curated_overlap']=False;flow_indicators(x);rows.append(x)
            continue
        else:r=rs[0].copy()
        r['curated_overlap']=key[3] in CURATED_CATEGORIES
        flow_indicators(r);rows.append(r)
    # Cross-check: the automatic reader against the curated manual specifications.
    curated={(r['year'],r['province'],r['session_division'],r['category']):r for r in annual}
    checks=[]
    for r in rows:
        if not r['curated_overlap']:continue
        c=curated.get((r['year'],r['province'],r['session_division'],r['category']))
        if not c:checks.append(dict(year=r['year'],province=r['province'],session_division=r['session_division'],category=r['category'],status='no_curated_row',court_tier=r['court_tier']));continue
        if c['court_tier']!=r['court_tier'] and not (c['court_tier']=='all_courts' and r['court_tier']=='all_courts'):
            checks.append(dict(year=r['year'],province=r['province'],session_division=r['session_division'],category=r['category'],status='tier_scope_differs',court_tier=f"{r['court_tier']} vs {c['court_tier']}"));continue
        diffs={f:(r[f],c[f]) for f in FIELDS if r.get(f)!=c.get(f)}
        checks.append(dict(year=r['year'],province=r['province'],session_division=r['session_division'],category=r['category'],status='matches' if not diffs else 'differs',court_tier=r['court_tier'],differences=diffs))
    return rows,checks

def write_dataset(out,name,rows):
    (out/f'{name}.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2,allow_nan=False))
    if not rows:return
    keys=list(dict.fromkeys(k for r in rows for k in r))
    with (out/f'{name}.csv').open('w',newline='',encoding='utf-8') as f:
        w=csv.DictWriter(f,fieldnames=keys);w.writeheader()
        w.writerows({k:json.dumps(v,ensure_ascii=False) if isinstance(v,(list,dict)) else v for k,v in r.items()} for r in rows)
    # Native DuckDB export avoids a dependency on a particular pandas/Arrow ABI.
    con=duckdb.connect();path=str(out/f'{name}.json').replace("'","''");target=str(out/f'{name}.parquet').replace("'","''")
    con.execute(f"COPY (SELECT * FROM read_json_auto('{path}', maximum_object_size=16777216)) TO '{target}' (FORMAT PARQUET)")
    con.close()

def partition_checks(rows,geography_field):
    groups=defaultdict(dict)
    for r in rows:groups[(r['period_start'],r['period_end'],r['province'],r.get(geography_field,r['province']))][r['category']]=r
    output=[]
    for key,cats in groups.items():
        if not {'all','civil','criminal'}<=set(cats):continue
        all_cases,civil,criminal=[cats[c] for c in ['all','civil','criminal']]
        comparable=len({r.get('court_tier','all_courts') for r in [all_cases,civil,criminal]})==1
        for f in FIELDS:
            vals=[r.get(f) for r in [all_cases,civil,criminal]]
            diff=vals[0]-vals[1]-vals[2] if comparable and all(v is not None for v in vals) else None
            output.append(dict(period_start=key[0],period_end=key[1],province=key[2],geography=key[3],metric=f,all_minus_civil_minus_criminal=diff,
                               status='not_comparable_scope_or_missing' if diff is None else ('matches' if diff==0 else 'non_exhaustive_categories_or_source_difference')))
    return output

def main():
    ap=argparse.ArgumentParser();ap.add_argument('--out',type=Path,default=WORKSPACE/'data_darbar_warehouse/ljcp');ap.add_argument('--population',type=Path,default=HERE.parents[1]/'app/data/districts.json');ap.add_argument('--raw',type=Path,default=WORKSPACE/'raw_data/ljcp');a=ap.parse_args()
    out=a.out;pop=json.loads(a.population.read_text());obs=json.loads((out/'source_observations.json').read_text())
    if (out/'extraction_failures.json').exists() and json.loads((out/'extraction_failures.json').read_text()):raise ValueError('Resolve extraction failures before building curated data')
    for r in obs:r['session_division']=canonical(r['name'],r['province'],pop)
    # Names may be normalised, but raw spelling and raw numeric cells remain.
    write_dataset(out,'source_observations',obs)
    tiers,annual=build_cases(obs);all_judges=build_judges(obs);judges=[r for r in all_judges if r['court_tier']=='all_courts']
    by_rank=[r for r in all_judges if r['court_tier']!='all_courts']
    write_dataset(out,'judicial_strength_by_rank',by_rank)
    judges=[r for r in judges if r['province']!='Balochistan']+balochistan_consolidated(all_judges,pop)
    _,province_annual=build_cases([{**r,'is_total':False,'session_division':r['province']} for r in obs if r['kind']=='cases' and r['is_total']])
    for r in province_annual:r.pop('session_division');r['geography_level']='province'
    from aggregates import extract_aggregates
    aggregates=extract_aggregates(a.raw)
    for r in aggregates:flow_indicators(r)
    for r in aggregates:
        if r['period_type']=='annual':province_annual.append(r)
    write_dataset(out,'annual_provinces',province_annual)
    write_dataset(out,'halfyear_provinces',[r for r in aggregates if r['period_type']=='half_year'])
    write_dataset(out,'category_partition_checks',partition_checks(annual,'session_division'))
    write_dataset(out,'halfyear_partition_checks',partition_checks([r for r in aggregates if r['period_type']=='half_year'],'province'))
    district,crosswalk,issues,hosted=district_data(annual,judges,pop)
    checks=table_quality(obs)
    for name,rows in [('court_tier_cases',tiers),('annual_sessions',annual),('judicial_strength',judges),('district_indicators',district),('sessions_crosswalk',crosswalk),('table_validation',checks),('geography_issues',issues),('hosted_districts',hosted)]:write_dataset(out,name,rows)
    # One observation per year/province/session: no case-category duplicates in
    # the policy scatterplot or the default map/year-slider feed.
    policy=[r for r in district if r['category']=='all' and r['court_tier']=='all_courts']
    write_dataset(out,'district_policy_indicators',policy)
    # Case-category layers (family, narcotics, murder, bail, rent, appeals, revisions, ...).
    cat_path=out/'category_observations.json'
    category_rows,category_checks,category_district=[],[],[]
    if cat_path.exists():
        category_rows,category_checks=build_categories(json.loads(cat_path.read_text()),annual,pop)
        extra=[r for r in category_rows if not r['curated_overlap']]
        category_district,_,cat_issues,_=district_data(extra,judges,pop)
        all_pending={(r['year'],r['adm2_key']):r['pending_end'] for r in policy}
        for r in category_district:
            for f in ['working_judges','sanctioned_judges','judge_source_record_ids','judge_coverage_status','working_judges_per_million','pending_per_working_judge','instituted_per_working_judge']:r.pop(f,None)
            r['share_of_all_pending_pct']=ratio(r['pending_end'],all_pending.get((r['year'],r['adm2_key'])),100)
        issues+=cat_issues
        write_dataset(out,'geography_issues',issues)
    write_dataset(out,'category_sessions',category_rows)
    write_dataset(out,'category_crosscheck',category_checks)
    write_dataset(out,'district_category_indicators',category_district)
    continuity=[];lookup={(r['year'],r['province'],r['session_division']):r for r in annual if r['category']=='all'}
    for (y,p,s),r in lookup.items():
        prev=lookup.get((y-1,p,s))
        if prev and r['pending_start'] is not None and prev['pending_end'] is not None:
            continuity.append(dict(year=y,province=p,session_division=s,previous_pending_end=prev['pending_end'],current_pending_start=r['pending_start'],difference=r['pending_start']-prev['pending_end']))
    write_dataset(out,'continuity_checks',continuity)
    coverage=[]
    for y in range(2019,2026):
        for p in ['Punjab','Sindh','Khyber Pakhtunkhwa','Balochistan','Islamabad']:
            selected=[r for r in annual if r['year']==y and r['province']==p and r['category']=='all']
            coverage.append(dict(year=y,province=p,annual_sessions=len(selected),status='extracted' if selected else ('not_located' if y==2019 else 'not_available_in_curated_annual_series')))
    write_dataset(out,'coverage',coverage)
    quality=dict(annual_records=len(annual),source_records=len(obs),judicial_strength_rows=len(judges),district_indicator_rows=len(district),policy_rows=sum(r['category']=='all' and r['court_tier']=='all_courts' for r in district),
                 source_stock_flow_residual_rows=sum(r['stock_flow_check']=='source_residual' for r in annual),table_mismatches=sum(r['status']=='source_mismatch' for r in checks),
                 halfyear_records=sum(r['period_type']=='half_year' for r in aggregates),
                 rank_specific_staffing_rows=sum(r['court_tier']!='all_courts' for r in all_judges),
                 policy_rows_with_staffing=sum(r['judge_coverage_status']=='matched' for r in policy),
                 category_session_rows=len(category_rows),category_crosscheck_matches=sum(c['status']=='matches' for c in category_checks),
                 category_crosscheck_other=sum(c['status']!='matches' for c in category_checks),district_category_rows=len(category_district),
                 unresolved_crosswalk_rows=sum(not r['population_rate_eligible'] for r in crosswalk),population_source=str(a.population),population_sha256=hashlib.sha256(a.population.read_bytes()).hexdigest(),population_year=2023,
                 caveats=['Civil/criminal categories are edition-specific and need not sum to all cases.','Sessions divisions are not universally census districts.','Population rates use a fixed 2023 benchmark; they are not annual estimates.','Missing values stay null. Source arithmetic errors are preserved.','A cross-sectional judges/backlog association is descriptive, not a causal staffing effect.'])
    (out/'quality_report.json').write_text(json.dumps(quality,indent=2));print(json.dumps(quality,indent=2))
    lines=['# LJCP judicial data — extracted release','',
           f"{len(annual):,} annual case records across 2020–2024; {quality['halfyear_records']} separately dated half-year provincial records through June 2025.",'',
           '## Annual sessions coverage','',
           '| Year | Punjab | Sindh | Khyber Pakhtunkhwa | Balochistan | ICT |','|---|---:|---:|---:|---:|---:|']
    for year in range(2019,2026):
        counts=[next(r['annual_sessions'] for r in coverage if r['year']==year and r['province']==p) for p in ['Punjab','Sindh','Khyber Pakhtunkhwa','Balochistan','Islamabad']]
        lines.append('| '+str(year)+' | '+' | '.join(str(n) if n else '—' for n in counts)+' |')
    lines += ['', 'A dash means unavailable in this annual extraction, never zero cases. Balochistan 2023 is available at province level. 2025 data are January–June provincial figures, not annual district data. No 2019 annual edition was located.', '',
              '## Files to use','',
              '- `annual_sessions.csv` / `.json` / `.parquet`: source-geography case counts and flow indicators. One year × province × sessions division × category.',
              '- `district_policy_indicators.csv` / `.json` / `.parquet`: one all-cases observation per year and eligible map unit, with population and staffing indicators. Best starting point for the map/year slider or staffing-versus-pendency scatterplot.',
              '- `district_category_indicators.*`: map unit × year × case category (family, narcotics, murder, bail, rent, civil/criminal appeals and revisions, seven-years-plus, hudood) with per-100k rates and share of all pending cases; `category_sessions.*` is the same at source geography.',
              '- `judicial_strength.*` and `judicial_strength_by_rank.*`: consolidated and rank-specific staffing, kept separate. Balochistan consolidated rows are sums of its rank tables per map district.',
              '- `annual_provinces.*` and `halfyear_provinces.*`: provincial aggregates; keep annual and half-year periods separate.',
              '- `sessions_crosswalk.*`: every source unit, candidate ADM2 key, aggregation rule/status and reasons for withholding rates.',
              '- `source_observations.*`: original cells, URLs, PDF checksum, page, table and row. Derived records link back to these IDs.',
              '- `table_validation.*`, `category_partition_checks.*`, `halfyear_partition_checks.*`, `continuity_checks.*`, `category_crosscheck.*`, `coverage.*`, and `quality_report.json`: audit and completeness reports.', '',
              f"Case-category layers: {quality['district_category_rows']} map-unit/year/category observations from {quality['category_session_rows']} source rows (2020, 2022, 2023, 2024; the scanned 2021 edition is not covered). The automatic table reader also reads the all/civil/criminal tables and is checked against the curated series: {quality['category_crosscheck_matches']} rows match exactly, {quality['category_crosscheck_other']} differ, all of them the 2020 Balochistan printed criminal table that repeats the all-cases table. Categories are edition-specific and not an exhaustive partition of all cases.", '',
              f"Staffing is matched to {quality['policy_rows_with_staffing']} of {quality['policy_rows']} policy rows. `population_interpolated` and `pending_per_100k_interpolated` use geometric growth between the 2017 and 2023 census counts to the year-end of each judicial year (2024 extrapolated); units with implausible implied growth (boundary changes) keep the fixed 2023 count, recorded in `population_interpolation_basis`.", '',
              '## Interpretation','',
              f"The policy feed contains {quality['policy_rows']} district-year observations. "+(f"{quality['unresolved_crosswalk_rows']} sessions-year correspondences need jurisdiction review; they remain in the source-geography dataset but do not receive population rates." if quality['unresolved_crosswalk_rows'] else 'Every reported sessions unit is keyed to a map unit; see `sessions_crosswalk.*` for the rule applied to each.'), '',
              'All population rates use the supplied **2023 census benchmark**. They are not estimates of population in each judicial year. Ordinary name matches remain provisional. ICT, Chitral, Kohistan and the two-seat Balochistan districts (Quetta+Sariab, Lasbela: Uthal+Hub, Jaffarabad: Dera Allah Yar+Usta Muhammad, Kalat+Surab, Killa Abdullah+Chaman) are summed before their population is counted; Balochistan town seats are keyed to the map district that contains them (`crosswalk_status = seat_to_district`).', '',
              f"Map districts with no court seat of their own in a year (Korangi and Keamari in Karachi East/West; Sujawal in Thatta before 2023; Sherani in Zhob; Sohbatpur in Jaffarabad; Washuk in Kharan before Basima reports; Harnai in Sibi in 2020) receive no observation. Their population is added to the host's denominator for that year, listed in `population_keys`/`hosted_districts` and in `hosted_districts.*` ({len(hosted)} district-years). Counts are never split across polygons.", '',
              f"There are {quality['source_stock_flow_residual_rows']} annual observations with a nonzero reported stock-flow residual and {quality['table_mismatches']} comparable table-sum discrepancies. Values are preserved and flagged. A flag is not an extraction correction.", '',
              'Civil and criminal categories are not a stable exhaustive partition in every edition. Punjab 2020 category tables cover civil courts only; overall totals also include sessions courts. Use the all-cases series for the default historical comparison.', '',
              'Working judges exclude ex-cadre where the report distinguishes them. Missing gender values remain unknown unless their column reconciles exactly to a printed total; inferred zeros are logged. Punjab staffing in the 2022 edition is withheld from case indicators because its printed reference date is 2021. KP district staffing was not found in the selected 2024 chapter.', '',
              'The staffing-versus-pendency comparison is descriptive. It cannot establish that staffing differences caused the backlog.', '',
              f"Full methodology and refresh instructions: `{HERE/'README.md'}`.",'']
    (out/'README.md').write_text('\n'.join(lines))

if __name__=='__main__':main()
