/* global L, chroma */

// ── Config ──────────────────────────────────────────────────────────────────

const GEOJSON_PATH = 'data/pakistan_districts_province_boundries.geojson';
const DATA_PATH    = 'data/districts.json';

// Topic → ordered list of dataset group keys
const TOPICS = {
  demographics:   { label: 'Demographics',              groups: ['demographics', 'urbanRural'] },
  education:      { label: 'Education',                 groups: ['literacy', 'education', 'pslmEducation'] },
  employment:     { label: 'Employment',                groups: ['employment', 'pslmEmployment', 'lfs', 'lfs25'] },
  economic:       { label: 'Economic Activity',         groups: ['econCensus'] },
  welfare:        { label: 'Household Welfare',         groups: ['hies'] },
  housing:        { label: 'Housing & Infrastructure',  groups: ['hiesHousing', 'pslmWash', 'hiesWaste'] },
  ict:            { label: 'ICT & Digital',             groups: ['hiesIct', 'pslmDigital'] },
  health:         { label: 'Health',                    groups: ['pslmHealth'] },
  women:          { label: "Women's Empowerment",       groups: ['hiesDecisions'] },
};

const INDICATOR_GROUPS = {
  // ── DEMOGRAPHICS ─────────────────────────────────────────────────────
  demographics: {
    label: 'Census 2017/23',
    dataset: 'Census 2017/23',
    indicators: {
      pop_total:          'Total Population',
      pop_male:           'Male Population',
      pop_female:         'Female Population',
      sex_ratio:          'Sex Ratio (M per 100 F)',
      density_per_sq_km:  'Population Density (per km\u00B2)',
      urban_proportion:   'Urban Proportion (%)',
      avg_household_size: 'Avg. Household Size',
      annual_growth_rate: 'Annual Growth Rate (%)',
      area_sq_km:         'Area (km\u00B2)',
    },
    prefix: 't1', hasYears: true,
  },
  urbanRural: {
    label: 'Urban / Rural — Census 2017/23',
    dataset: 'Census 2017/23',
    indicators: {
      total_all: 'Total Population', total_male: 'Total Male', total_female: 'Total Female',
      rural_all: 'Rural Population', rural_male: 'Rural Male', rural_female: 'Rural Female',
      urban_all: 'Urban Population', urban_male: 'Urban Male', urban_female: 'Urban Female',
    },
    prefix: 't5', hasYears: true,
  },
  // ── EDUCATION ────────────────────────────────────────────────────────
  literacy: {
    label: 'Literacy — Census 2017/23',
    dataset: 'Census 2017/23',
    indicators: {
      literacy_ratio_all:    'Literacy Rate (Total %)',
      literacy_ratio_male:   'Literacy Rate (Male %)',
      literacy_ratio_female: 'Literacy Rate (Female %)',
      literate_all:          'Literate Population',
      illiterate_all:        'Illiterate Population',
    },
    prefix: 't12', hasYears: true,
  },
  education: {
    label: 'Education Attainment — Census 2017/23',
    dataset: 'Census 2017/23',
    indicators: {
      pct_never_attended: '% Never Attended School',
      pct_below_primary: '% Below Primary', pct_primary: '% Primary',
      pct_middle: '% Middle', pct_matric: '% Matric', pct_intermediate: '% Intermediate',
      pct_graduate: '% Graduate', pct_masters_above: '% Masters & Above',
      pct_matric_plus: '% Matric or Above',
      total: 'Total Pop. 5+ (count)', never_attended: 'Never Attended (count)',
      below_primary: 'Below Primary (count)', primary: 'Primary (count)',
      middle: 'Middle (count)', matric: 'Matric (count)', intermediate: 'Intermediate (count)',
      graduate: 'Graduate (count)', masters_above: 'Masters & Above (count)',
    },
    prefix: 't_edu', hasYears: true,
  },
  pslmEducation: {
    label: 'Education — PSLM 2019-20',
    dataset: 'PSLM 2019-20',
    indicators: {
      literacy_rate: 'Literacy Rate (%)',
      numeracy_rate: 'Numeracy Rate (%)',
      net_enrolment_rate: 'Net Enrolment Rate 5-16 (%)',
      pct_never_attended: '% Never Attended School',
      pct_govt_school: '% in Govt. School',
      pct_private_school: '% in Private School',
    },
    prefix: 'pslm', hasYears: false, noYear: true,
  },
  // ── EMPLOYMENT ───────────────────────────────────────────────────────
  employment: {
    label: 'Employment — Census 2017/23',
    dataset: 'Census 2017/23',
    indicators: {
      total:                    'Total Pop. (10+)',
      worked:                   'Employed',
      seeking_work:             'Unemployed',
      lfpr:                     'Labour Force Participation Rate (%)',
      unemployment_rate:        'Unemployment Rate (%)',
      employment_ratio:         'Employment-to-Pop Ratio (%)',
      employment_ratio_male:    'Employment Ratio Male (%)',
      employment_ratio_female:  'Employment Ratio Female (%)',
      lfpr_male:                'LFPR Male (%)',
      lfpr_female:              'LFPR Female (%)',
      unemployment_rate_male:   'Unemployment Rate Male (%)',
      unemployment_rate_female: 'Unemployment Rate Female (%)',
      pct_paid_employee:        '% Paid Employees (2023)',
      pct_self_employed:        '% Self-Employed (2023)',
      pct_unpaid_family:        '% Unpaid Family Workers (2023)',
      not_lf_student_youth:     'Not in LF & Students 15-24 (2023)',
      not_in_lf:                'Not in Labour Force (2023)',
      student:                  'Students (2017 only)',
      house_keeping:            'House Keeping (2017 only)',
    },
    prefix: 't_emp', hasYears: true,
  },
  pslmEmployment: {
    label: 'Employment — PSLM 2019-20',
    dataset: 'PSLM 2019-20',
    indicators: {
      work_participation_rate: 'Work Participation Rate (%)',
      work_participation_male: 'Work Participation Male (%)',
      work_participation_female: 'Work Participation Female (%)',
      avg_hh_size: 'Avg. Household Size',
    },
    prefix: 'pslm', hasYears: false, noYear: true,
  },
  lfs: {
    label: 'Employment — LFS 2020-21',
    dataset: 'LFS 2020-21',
    indicators: {
      employment_ratio:         'Employment Ratio (%)',
      employment_ratio_male:    'Employment Ratio Male (%)',
      employment_ratio_female:  'Employment Ratio Female (%)',
      lfpr:                     'Labour Force Participation (%)',
      lfpr_male:                'LFPR Male (%)',
      lfpr_female:              'LFPR Female (%)',
      unemployment_rate:        'Unemployment Rate (%)',
      youth_employment_ratio:   'Youth Employment 15-24 (%)',
      pct_agriculture:          '% Employed in Agriculture',
      pct_manufacturing:        '% Employed in Manufacturing',
      pct_trade:                '% Employed in Trade',
    },
    prefix: 'lfs21', hasYears: false, noYear: true,
  },
  lfs25: {
    label: 'Employment — LFS 2024-25',
    dataset: 'LFS 2024-25',
    indicators: {
      employment_ratio:         'Employment Ratio (%)',
      employment_ratio_male:    'Employment Ratio Male (%)',
      employment_ratio_female:  'Employment Ratio Female (%)',
      lfpr:                     'Labour Force Participation (%)',
      lfpr_male:                'LFPR Male (%)',
      lfpr_female:              'LFPR Female (%)',
      unemployment_rate:        'Unemployment Rate (%)',
      youth_employment_ratio:   'Youth Employment 15-24 (%)',
      pct_agriculture:          '% Employed in Agriculture',
      pct_manufacturing:        '% Employed in Manufacturing',
      pct_trade:                '% Employed in Trade',
    },
    prefix: 'lfs25', hasYears: false, noYear: true,
  },
  // ── ECONOMIC ACTIVITY ────────────────────────────────────────────────
  econCensus: {
    label: 'Economic Activity — Economic Census 2023',
    dataset: 'Economic Census 2023',
    indicators: {
      total_establishments: 'Total Establishments',
      total_workforce:      'Total Workforce',
      avg_workers_per_est:  'Avg. Workers per Establishment',
      pct_manufacturing:    '% Manufacturing',
      pct_trade:            '% Wholesale & Retail Trade',
      pct_services:         '% Services',
      pct_agriculture:      '% Agriculture',
      pct_construction:     '% Construction',
    },
    prefix: 'ec', hasYears: false, noYear: true,
  },
  // ── HOUSEHOLD WELFARE ────────────────────────────────────────────────
  hies: {
    label: 'Consumption & Food Security — HIES 2024-25',
    dataset: 'HIES 2024-25',
    indicators: {
      median_monthly_percapita: 'Median Monthly Per-Capita Exp. (PKR)',
      mean_monthly_percapita:   'Mean Monthly Per-Capita Exp. (PKR)',
      food_share:               'Food Expenditure Share (%)',
      food_insecurity_pct:      'Food Insecure HH (%)',
      avg_fies_score:           'Avg. FIES Score (0-8)',
      avg_hh_size:              'Avg. Household Size',
    },
    prefix: 'hies', hasYears: false, noYear: true,
  },
  // ── HOUSING & INFRASTRUCTURE ─────────────────────────────────────────
  hiesHousing: {
    label: 'Housing & Utilities — HIES 2024-25',
    dataset: 'HIES 2024-25',
    indicators: {
      pct_electricity:    '% HH Electricity',
      pct_owner_occupied: '% HH Owner-Occupied',
      pct_piped_water:    '% HH Piped Water',
      avg_rooms:          'Avg. Rooms per HH',
      pct_pucca_floor:    '% Pucca Floor (Cement/Tiles)',
      pct_rcc_roof:       '% RCC/RBC Roof',
      pct_brick_walls:    '% Burnt Brick Walls',
      pct_pucca_house:    '% Fully Pucca House',
      pct_katcha_house:   '% Katcha House',
    },
    prefix: 'hies', hasYears: false, noYear: true,
    // Note: electricity/owner/water use hies_ prefix, housing quality uses hies_hq_
    // Override dataKey for mixed-prefix group
    mixedKeys: {
      pct_electricity: 'hies_pct_electricity',
      pct_owner_occupied: 'hies_pct_owner_occupied',
      pct_piped_water: 'hies_pct_piped_water',
      avg_rooms: 'hies_hq_avg_rooms',
      pct_pucca_floor: 'hies_hq_pct_pucca_floor',
      pct_rcc_roof: 'hies_hq_pct_rcc_roof',
      pct_brick_walls: 'hies_hq_pct_brick_walls',
      pct_pucca_house: 'hies_hq_pct_pucca_house',
      pct_katcha_house: 'hies_hq_pct_katcha_house',
    },
  },
  pslmWash: {
    label: 'Water & Sanitation — PSLM 2019-20',
    dataset: 'PSLM 2019-20',
    indicators: {
      pct_piped_water:  '% HH Piped Water',
      pct_flush_toilet: '% HH Flush Toilet',
      pct_no_toilet:    '% HH No Toilet',
    },
    prefix: 'pslm', hasYears: false, noYear: true,
  },
  hiesWaste: {
    label: 'Waste Management — HIES 2024-25',
    dataset: 'HIES 2024-25',
    indicators: {
      pct_open_dumping:          '% Open Dumping',
      pct_municipal_collection:  '% Municipal Collection',
      pct_formal_collection:     '% Any Formal Collection',
      pct_no_bin_access:         '% No Bin Access',
    },
    prefix: 'hies_waste', hasYears: false, noYear: true,
  },
  // ── ICT & DIGITAL ───────────────────────────────────────────────────
  hiesIct: {
    label: 'ICT & Digital Access — HIES 2024-25',
    dataset: 'HIES 2024-25',
    indicators: {
      pct_smartphone:       '% Smartphone Ownership',
      pct_any_mobile:       '% Any Mobile Phone',
      pct_computer:         '% Computer Access',
      pct_internet_user:    '% Internet Users',
      pct_daily_internet:   '% Daily Internet Users',
      pct_digital_finance:  '% Digital Finance (Mobile Money/Bank)',
    },
    prefix: 'hies_ict', hasYears: false, noYear: true,
  },
  pslmDigital: {
    label: 'ICT & Digital Access — PSLM 2019-20',
    dataset: 'PSLM 2019-20',
    indicators: {
      pct_computer_access:  '% Computer Access (10+)',
      pct_smartphone:       '% Smartphone (10+)',
      pct_any_mobile:       '% Any Mobile (10+)',
      pct_internet_user:    '% Internet User (10+)',
      pct_internet_male:    '% Internet Male (10+)',
      pct_internet_female:  '% Internet Female (10+)',
      pct_internet:         '% HH Internet Access',
      pct_mobile:           '% HH Mobile Phone',
    },
    prefix: 'pslm', hasYears: false, noYear: true,
    // Note: pct_internet/pct_mobile use pslm_ prefix, rest use pslm_digital_
    mixedKeys: {
      pct_computer_access: 'pslm_digital_pct_computer_access',
      pct_smartphone: 'pslm_digital_pct_smartphone',
      pct_any_mobile: 'pslm_digital_pct_any_mobile',
      pct_internet_user: 'pslm_digital_pct_internet_user',
      pct_internet_male: 'pslm_digital_pct_internet_male',
      pct_internet_female: 'pslm_digital_pct_internet_female',
      pct_internet: 'pslm_pct_internet',
      pct_mobile: 'pslm_pct_mobile',
    },
  },
  // ── HEALTH ──────────────────────────────────────────────────────────
  pslmHealth: {
    label: 'Health Access — PSLM 2019-20',
    dataset: 'PSLM 2019-20',
    indicators: {
      morbidity_rate:        'Morbidity Rate (% ill in 2 weeks)',
      pct_sought_treatment:  '% Sought Treatment (if ill)',
      pct_govt_facility:     '% Used Govt. Facility',
      pct_private_facility:  '% Used Private Facility',
    },
    prefix: 'pslm_health', hasYears: false, noYear: true,
  },
  // ── WOMEN'S EMPOWERMENT ─────────────────────────────────────────────
  hiesDecisions: {
    label: "Women's Empowerment — HIES 2024-25",
    dataset: 'HIES 2024-25',
    indicators: {
      pct_edu_self:              '% Women Decide Own Education',
      pct_edu_consulted:         '% Women Consulted on Education',
      pct_work_self:             '% Women Decide Own Work',
      pct_health_consulted:      '% Women Consulted on Healthcare',
      pct_fp_joint_or_self:      '% FP Decisions Joint/Self',
      pct_fp_husband_alone:      '% FP Husband Decides Alone',
      pct_not_permitted_work:    '% Not Permitted to Work',
      pct_domestic_burden:       '% Too Busy with Domestic Work',
    },
    prefix: 'hies_wdm', hasYears: false, noYear: true,
  },
};

// Colour ramps per group — topic-consistent colours
const COLOR_RAMPS = {
  // Demographics
  demographics:     ['#e6f4ec', '#145228'],
  urbanRural:       ['#e6f4ec', '#1a5632'],
  // Education
  literacy:         ['#fef6dc', '#b8941a'],
  education:        ['#fef6dc', '#d4a017'],
  pslmEducation:    ['#fef6dc', '#8d6e0f'],
  // Employment
  employment:       ['#e6f4ec', '#1e6b3e'],
  pslmEmployment:   ['#e6f4ec', '#22804a'],
  lfs:              ['#e6f4ec', '#0c3a1e'],
  lfs25:            ['#e6f4ec', '#1e6b3e'],
  // Economic Activity
  econCensus:       ['#fef6dc', '#1a5632'],
  // Household Welfare
  hies:             ['#fef6dc', '#b8941a'],
  // Housing & Infrastructure
  hiesHousing:      ['#fbe9e7', '#bf360c'],
  pslmWash:         ['#e0f2f1', '#004d40'],
  hiesWaste:        ['#e0f2f1', '#00695c'],
  // ICT & Digital
  hiesIct:          ['#e8eaf6', '#1a237e'],
  pslmDigital:      ['#e3f2fd', '#0d47a1'],
  // Health
  pslmHealth:       ['#e8f5e9', '#1b5e20'],
  // Women's Empowerment
  hiesDecisions:    ['#fce4ec', '#880e4f'],
};

// Indicators where an INCREASE is bad (red) and a DECREASE is good (green).
// All other indicators default to: increase = good (green), decrease = bad (red).
const HIGHER_IS_WORSE = new Set([
  // Demographics
  'sex_ratio',            // gender imbalance
  'avg_household_size',   // overcrowding
  // Literacy
  'illiterate_all',       // more illiterate = worse
  // Education
  'pct_never_attended',   // never attended school
  'pct_below_primary',    // didn't finish primary
  // Employment
  'seeking_work',         // more job seekers = worse
  // Economic activity
  'unemployment_rate',    // higher unemployment = worse
  // PSLM
  'pct_no_toilet',        // no toilet = worse
  // HIES
  'food_share',           // higher food expenditure share = poorer
  'food_insecurity_pct',  // food insecure households
  'avg_fies_score',       // food insecurity score (0-8)
  // Housing Quality
  'pct_katcha_house',     // katcha = worse quality
  // Waste Management
  'pct_open_dumping',     // open dumping = worse
  'pct_no_bin_access',    // no bin = worse
  // Women's Decision-Making
  'pct_fp_husband_alone',     // husband alone = less agency
  'pct_not_permitted_work',   // not permitted = less agency
  'pct_domestic_burden',      // domestic burden = constraint
  // Health
  'morbidity_rate',           // higher illness = worse
]);

// ── State ───────────────────────────────────────────────────────────────────

let map, districtLayer, rawData = {}, geoData;
let currentTopic = 'demographics';
let currentGroup = 'demographics';
let currentIndicator = 'pop_total';
let currentYear = '2023';
let selectedDistrict = null;
let isZoomedIn = false;
let originalBounds = null;

// Store per-layer fill so hover can restore it
const layerFills = new WeakMap();

// ── DOM refs ────────────────────────────────────────────────────────────────

const topicSelect     = document.getElementById('topicSelect');
const groupSelect     = document.getElementById('groupSelect');
const indicatorSelect = document.getElementById('indicatorSelect');
const provinceSelect  = document.getElementById('provinceSelect');
const searchInput     = document.getElementById('searchInput');
const resetBtn        = document.getElementById('resetBtn');
const downloadBtn     = document.getElementById('downloadData');
const statsDiv        = document.getElementById('stats');
const legendDiv       = document.getElementById('legend');
const districtNameEl  = document.getElementById('districtName');
const districtProvEl  = document.getElementById('districtProvince');
const diagEl          = document.getElementById('diagnostics');
const yearBtns        = document.querySelectorAll('.year-toggle button');
const zoomBar         = document.getElementById('zoomBar');
const zoomOutBtn      = document.getElementById('zoomOutBtn');
const zoomDistName    = document.getElementById('zoomDistrictName');
const aboutBtn        = document.getElementById('aboutBtn');
const methodologyBtn  = document.getElementById('methodologyBtn');
const contactBtn      = document.getElementById('contactBtn');
const aboutModal      = document.getElementById('aboutModal');
const methodologyModal = document.getElementById('methodologyModal');
const contactModal    = document.getElementById('contactModal');
const contactForm     = document.getElementById('contactForm');
const contactStatus   = document.getElementById('contactStatus');

// ── Helpers ─────────────────────────────────────────────────────────────────

function normName(s) {
  return (s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim();
}

function fmt(v, pct) {
  if (v === null || v === undefined || (typeof v === 'number' && isNaN(v))) return '\u2014';
  const n = Number(v);
  if (isNaN(n)) return '\u2014';
  if (pct) return n.toFixed(1) + '%';
  if (Math.abs(n) >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (Math.abs(n) >= 1e3) return n.toLocaleString('en-US', { maximumFractionDigits: 1 });
  return n.toFixed(1);
}

function isPct(indicator) {
  const label = INDICATOR_GROUPS[currentGroup]?.indicators?.[indicator] || '';
  return isPctLabel(label);
}
function isPctLabel(label) {
  // Match "rate", "ratio", "proportion", or "(%)" — but not "literate"/"illiterate"
  return /\(.*%\)|\bratio\b|\bproportion\b/i.test(label) ||
         (/\brate\b/i.test(label) && !/literate|illiterate/i.test(label));
}

// Survey groups that carry sample-size metadata (n_obs + low_n) from ETL.
// Maps group prefix → the prefix used for n_obs/low_n keys in the data.
// Groups sharing a prefix (e.g. pslmEducation and pslmEmployment both use 'pslm')
// but without their own n_obs will fall through gracefully (return null).
const SURVEY_META_PREFIX = {
  lfs21:        'lfs21',
  lfs25:        'lfs25',
  hies:         'hies',
  hies_ict:     'hies_ict',
  hies_hq:      'hies_hq',
  hies_waste:   'hies_waste',
  hies_wdm:     'hies_wdm',
  pslm:         'pslm',
  pslm_health:  'pslm_health',
  pslm_digital: 'pslm_digital',
};

function _surveyMetaPrefixes() {
  // Returns array of prefixes to check for n_obs/low_n metadata.
  // Mixed-prefix groups check all unique prefixes from their mixedKeys.
  const g = INDICATOR_GROUPS[currentGroup];
  if (!g) return [];
  if (g.mixedKeys) {
    const prefixes = new Set();
    for (const fullKey of Object.values(g.mixedKeys)) {
      // Extract prefix: everything before the last _indicator part
      const parts = fullKey.split('_');
      // Find the prefix by matching against known survey prefixes
      for (const sp of Object.keys(SURVEY_META_PREFIX)) {
        if (fullKey.startsWith(sp + '_')) { prefixes.add(sp); break; }
      }
    }
    return [...prefixes];
  }
  if (SURVEY_META_PREFIX[g.prefix]) return [SURVEY_META_PREFIX[g.prefix]];
  return [];
}

function isLowN(props) {
  const mps = _surveyMetaPrefixes();
  if (!mps.length) return false;
  const dist = props.districts || props.district_agency || '';
  const row = rawData[normName(dist)];
  if (!row) return false;
  // Flag if ANY contributing source is low_n
  return mps.some(mp => !!row[`${mp}_low_n`]);
}

function getNObs(props) {
  const mps = _surveyMetaPrefixes();
  if (!mps.length) return null;
  const dist = props.districts || props.district_agency || '';
  const row = rawData[normName(dist)];
  if (!row) return null;
  // Return minimum n_obs across sources (most conservative)
  const obs = mps.map(mp => row[`${mp}_n_obs`]).filter(v => v != null);
  return obs.length ? Math.min(...obs) : null;
}

function dataKey(group, year, indicator) {
  const g = INDICATOR_GROUPS[group];
  // Mixed-prefix groups: explicit key mapping overrides the prefix convention
  if (g.mixedKeys && g.mixedKeys[indicator]) return g.mixedKeys[indicator];
  if (g.noYear) return `${g.prefix}_${indicator}`;
  return `${g.prefix}_${year}_${indicator}`;
}
function diffKey(group, indicator) {
  return `${INDICATOR_GROUPS[group].prefix}_diff_${indicator}`;
}

function fmtDiff(v, pct) {
  if (v === null || v === undefined) return '\u2014';
  const n = Number(v);
  if (isNaN(n)) return '\u2014';
  const sign = n > 0 ? '+' : '';
  if (pct) return `${sign}${n.toFixed(1)}pp`;
  if (Math.abs(n) >= 1e6) return `${sign}${(n / 1e6).toFixed(2)}M`;
  if (Math.abs(n) >= 1e3) return `${sign}${n.toLocaleString('en-US', { maximumFractionDigits: 0 })}`;
  return `${sign}${n.toFixed(1)}`;
}

// ── Map setup ───────────────────────────────────────────────────────────────

function initMap() {
  map = L.map('map', {
    zoomControl: false,
    attributionControl: false,
    zoomSnap: 0.25,
    minZoom: 4,
    maxZoom: 12,
  }).setView([30.2, 69.0], 5.25);

  L.control.zoom({ position: 'bottomright' }).addTo(map);
  map.getContainer().style.background = '#faf6e9';
}

// ── Data loading ────────────────────────────────────────────────────────────

async function loadData() {
  const [geo, data] = await Promise.all([
    fetch(GEOJSON_PATH).then(r => r.json()),
    fetch(DATA_PATH).then(r => r.json()).catch(() => ({}))
  ]);
  geoData = geo;
  rawData = data;
}

// ── Build UI ────────────────────────────────────────────────────────────────

function populateTopicSelect() {
  topicSelect.innerHTML = '';
  for (const [key, t] of Object.entries(TOPICS)) {
    const opt = document.createElement('option');
    opt.value = key;
    opt.textContent = t.label;
    topicSelect.appendChild(opt);
  }
  topicSelect.value = currentTopic;
}

function populateGroupSelect() {
  groupSelect.innerHTML = '';
  const topic = TOPICS[currentTopic];
  if (!topic) return;
  for (const gKey of topic.groups) {
    const g = INDICATOR_GROUPS[gKey];
    if (!g) continue;
    const opt = document.createElement('option');
    opt.value = gKey;
    opt.textContent = g.label;
    groupSelect.appendChild(opt);
  }
  // If current group isn't in this topic, default to first
  if (!topic.groups.includes(currentGroup)) {
    currentGroup = topic.groups[0];
  }
  groupSelect.value = currentGroup;
}

function populateIndicatorSelect() {
  const g = INDICATOR_GROUPS[currentGroup];
  indicatorSelect.innerHTML = '';
  for (const [key, label] of Object.entries(g.indicators)) {
    const opt = document.createElement('option');
    opt.value = key;
    opt.textContent = label;
    indicatorSelect.appendChild(opt);
  }
  if (!g.indicators[currentIndicator]) {
    currentIndicator = Object.keys(g.indicators)[0];
  }
  indicatorSelect.value = currentIndicator;
}

// Display names for province filter dropdown
const PROVINCE_LABELS = {
  'Federally Administered Tribal Areas': 'Former FATA',
};

// When filtering by province, also include these extra province_territory values
const PROVINCE_INCLUDES = {
  'Khyber Pakhtunkhwa': ['Federally Administered Tribal Areas'],
};

function matchesProvince(provFilter, districtProv) {
  if (provFilter === 'ALL') return true;
  if (districtProv === provFilter) return true;
  const extras = PROVINCE_INCLUDES[provFilter];
  return extras && extras.includes(districtProv);
}

function populateProvinceSelect() {
  const provinces = new Set();
  districtLayer.eachLayer(l => {
    provinces.add((l.feature.properties || {}).province_territory || 'Unknown');
  });
  provinceSelect.innerHTML = '<option value="ALL">All Provinces</option>';
  [...provinces].sort().forEach(p => {
    const opt = document.createElement('option');
    opt.value = p; opt.textContent = PROVINCE_LABELS[p] || p;
    provinceSelect.appendChild(opt);
  });
}

function updateYearButtons() {
  const g = INDICATOR_GROUPS[currentGroup];
  if (g.noYear) {
    yearBtns.forEach(btn => { btn.disabled = true; btn.classList.remove('active'); });
    currentYear = '2017';
  } else if (!g.hasYears) {
    yearBtns.forEach(btn => {
      const y = btn.dataset.year;
      btn.disabled = (y !== '2017');
      btn.classList.toggle('active', y === '2017');
    });
    currentYear = '2017';
  } else {
    yearBtns.forEach(btn => {
      btn.disabled = false;
      btn.classList.toggle('active', btn.dataset.year === currentYear);
    });
  }
}

// ── GeoJSON layer ───────────────────────────────────────────────────────────

function getVal(props) {
  const dist = props.districts || props.district_agency || '';
  const row  = rawData[normName(dist)];
  if (!row) return null;
  const k = (currentYear === 'diff')
    ? diffKey(currentGroup, currentIndicator)
    : dataKey(currentGroup, currentYear, currentIndicator);
  const v = row[k];
  return (v === null || v === undefined) ? null : Number(v);
}

// Build rich tooltip content showing district name + current indicator value
function getTooltipContent(props) {
  const dist = props.districts || props.district_agency || '';
  const v = getVal(props);
  const pct = isPct(currentIndicator);
  const g = INDICATOR_GROUPS[currentGroup];
  const indicatorLabel = g.indicators[currentIndicator] || '';

  let valStr;
  if (currentYear === 'diff') {
    valStr = fmtDiff(v, pct);
  } else {
    valStr = fmt(v, pct);
  }

  let lowNNote = '';
  if (isLowN(props)) {
    const nObs = getNObs(props);
    lowNNote = `<span class="tooltip-lown">⚠ Small sample (n=${nObs ?? '?'})</span>`;
    valStr = '—';
  } else {
    const nObs = getNObs(props);
    if (nObs !== null) {
      lowNNote = `<span class="tooltip-nobs">n=${nObs}</span>`;
    }
  }

  return `<strong>${dist}</strong>`
    + `<span class="tooltip-value">${valStr}</span>`
    + `<span class="tooltip-indicator">${indicatorLabel}</span>`
    + lowNNote;
}

function onEachDistrict(feature, layer) {
  const p = feature.properties || {};

  // Rich tooltip with value
  layer.bindTooltip(() => getTooltipContent(p), {
    sticky: true,
    direction: 'top',
    offset: [0, -10],
    className: 'leaflet-tooltip',
  });

  layer.on({
    mouseover: e => {
      const l = e.target;
      const stored = layerFills.get(l);
      // Don't highlight hidden (province-filtered) districts
      if (stored && stored.fillOpacity === 0) return;
      l.setStyle({ weight: 2.5, color: '#0c3a1e' });
      l.bringToFront();
    },
    mouseout: e => {
      const l = e.target;
      const stored = layerFills.get(l);
      if (stored) {
        l.setStyle({
          weight: l === selectedDistrict ? 2.5 : (stored.weight ?? 1),
          color: l === selectedDistrict ? '#0c3a1e' : (stored.color || '#8a9480'),
          fillColor: stored.fillColor,
          fillOpacity: stored.fillOpacity,
          opacity: stored.opacity ?? 1,
        });
      }
    },
    click: e => {
      const stored = layerFills.get(e.target);
      // Don't select hidden districts
      if (stored && stored.fillOpacity === 0) return;

      // If clicking the already-selected district, zoom in
      if (selectedDistrict === e.target) {
        zoomToDistrict(e.target);
        return;
      }

      // Deselect previous
      if (selectedDistrict) {
        const prev = layerFills.get(selectedDistrict);
        if (prev) selectedDistrict.setStyle({ weight: prev.weight || 1, color: prev.color || '#8a9480', fillColor: prev.fillColor, fillOpacity: prev.fillOpacity });
      }
      selectedDistrict = e.target;
      e.target.setStyle({ weight: 2.5, color: '#0c3a1e' });
      e.target.bringToFront();
      showDistrictDetail(e.target.feature.properties);
    }
  });
}

// ── Zoom to district ────────────────────────────────────────────────────────

function zoomToDistrict(layer) {
  if (!originalBounds) {
    originalBounds = map.getBounds();
  }

  const bounds = layer.getBounds();
  map.fitBounds(bounds, { padding: [60, 60], maxZoom: 10 });
  isZoomedIn = true;

  const dist = layer.feature.properties.districts || layer.feature.properties.district_agency || '';
  zoomDistName.textContent = dist;
  zoomBar.classList.remove('hidden');
}

function zoomOut() {
  if (!isZoomedIn) return;
  isZoomedIn = false;
  zoomBar.classList.add('hidden');
  map.fitBounds(districtLayer.getBounds(), { padding: [20, 20] });
}

function buildLayer() {
  if (districtLayer) map.removeLayer(districtLayer);
  districtLayer = L.geoJSON(geoData, {
    style: () => ({ color: '#8a9480', weight: 1, fillOpacity: 0.92, fillColor: '#e2e5ea' }),
    onEachFeature: onEachDistrict,
  }).addTo(map);

  map.fitBounds(districtLayer.getBounds(), { padding: [20, 20] });
  populateProvinceSelect();
}

// ── Colorize ────────────────────────────────────────────────────────────────

function colorize() {
  const provFilter = provinceSelect.value;
  const values = [];

  districtLayer.eachLayer(l => {
    const p = l.feature.properties || {};
    const prov = p.province_territory || 'Unknown';
    if (!matchesProvince(provFilter, prov)) return;
    const v = getVal(p);
    if (v !== null && !isNaN(v)) values.push(v);
  });

  if (!values.length) {
    const provBoundsGroup = L.featureGroup();
    districtLayer.eachLayer(l => {
      const p = l.feature.properties || {};
      const prov = p.province_territory || 'Unknown';
      let s;
      if (!matchesProvince(provFilter, prov)) {
        s = { fillOpacity: 0, fillColor: 'transparent', weight: 0, color: 'transparent', opacity: 0 };
      } else {
        provBoundsGroup.addLayer(l);
        s = { fillOpacity: 0.5, fillColor: '#e2e5ea', color: '#8a9480', weight: 1, opacity: 1 };
      }
      l.setStyle(s);
      layerFills.set(l, s);
    });
    if (provFilter !== 'ALL' && provBoundsGroup.getLayers().length) {
      map.fitBounds(provBoundsGroup.getBounds(), { padding: [30, 30] });
    }
    legendDiv.innerHTML = '<p class="legend-empty">No data for this selection</p>';
    updateSummary([]);
    return;
  }

  const isDiff = currentYear === 'diff';
  let scale, breaks;
  const ramp = COLOR_RAMPS[currentGroup] || ['#e6f4ec', '#145228'];

  if (isDiff) {
    const absMax = Math.max(Math.abs(Math.min(...values)), Math.abs(Math.max(...values))) || 1;
    // For "higher is worse" indicators, flip: positive change = red, negative = green
    // For normal indicators: positive change = green, negative = red
    const inverted = HIGHER_IS_WORSE.has(currentIndicator);
    const worse = '#c0392b';  // red
    const better = '#1a5632'; // green
    const lo = inverted ? better : worse;   // color for negative values
    const hi = inverted ? worse  : better;  // color for positive values
    scale = chroma.scale([lo, '#fafafa', hi]).domain([-absMax, 0, absMax]);
    breaks = [-absMax, -absMax / 2, 0, absMax / 2, absMax];
  } else {
    breaks = chroma.limits(values, 'q', 5);
    scale = chroma.scale(ramp).domain([breaks[0], breaks[breaks.length - 1]]);
  }

  // Build bounds for the visible (filtered) province
  const provBoundsGroup = L.featureGroup();

  districtLayer.eachLayer(l => {
    const p = l.feature.properties || {};
    const prov = p.province_territory || 'Unknown';
    let style;
    if (!matchesProvince(provFilter, prov)) {
      // Hide non-matching provinces entirely
      style = { fillOpacity: 0, fillColor: 'transparent', weight: 0, color: 'transparent', opacity: 0 };
    } else {
      provBoundsGroup.addLayer(l);
      const v = getVal(p);
      if (isLowN(p)) {
        // Low sample size — distinct striped appearance
        style = { fillOpacity: 0.25, fillColor: '#f5e6b8', weight: 1.5, color: '#c49515', opacity: 0.8, dashArray: '4 3' };
      } else if (v === null || isNaN(v)) {
        style = { fillOpacity: 0.35, fillColor: '#e2e5ea', weight: 1, color: '#8a9480', opacity: 1 };
      } else {
        style = { fillColor: scale(v).hex(), fillOpacity: 0.92, weight: 1, color: '#8a9480', opacity: 1 };
      }
    }
    l.setStyle(style);
    layerFills.set(l, style);
  });

  // Zoom to the selected province, or back to all
  if (provFilter !== 'ALL' && provBoundsGroup.getLayers().length) {
    map.fitBounds(provBoundsGroup.getBounds(), { padding: [30, 30] });
  } else if (provFilter === 'ALL' && !isZoomedIn) {
    map.fitBounds(districtLayer.getBounds(), { padding: [20, 20] });
  }

  // Keep selected district highlighted
  if (selectedDistrict) {
    const selProv = (selectedDistrict.feature.properties || {}).province_territory || '';
    if (provFilter === 'ALL' || selProv === provFilter) {
      selectedDistrict.setStyle({ weight: 2.5, color: '#0c3a1e' });
      selectedDistrict.bringToFront();
    }
  }

  renderLegend(breaks, scale, isDiff);
  updateDiagnostics(values.length);
  updateSummaryBar();
  prepareDownload();

  // Refresh sidebar detail if a district is currently selected
  if (selectedDistrict) {
    showDistrictDetail(selectedDistrict.feature.properties);
  }
}

// ── Legend ───────────────────────────────────────────────────────────────────

function renderLegend(breaks, scale, isDiff) {
  const g = INDICATOR_GROUPS[currentGroup];
  const pct = isPct(currentIndicator);
  const yearLabel = currentYear === 'diff' ? 'Change 2017\u21922023' : currentYear;

  let html = `<div class="legend-title">${g.indicators[currentIndicator]}<span>${yearLabel}</span></div>`;
  html += '<div class="legend-scale">';
  const n = isDiff ? 5 : Math.min(breaks.length - 1, 5);
  for (let i = 0; i < n; i++) {
    const mid = isDiff
      ? breaks[0] + (breaks[breaks.length - 1] - breaks[0]) * (i + 0.5) / n
      : (breaks[i] + (breaks[i + 1] || breaks[i])) / 2;
    html += `<span style="background:${scale(mid).hex()}"></span>`;
  }
  html += '</div>';
  html += `<div class="legend-labels"><span>${fmt(breaks[0], pct)}</span><span>${fmt(breaks[breaks.length - 1], pct)}</span></div>`;
  // Add low-n legend entry for survey groups
  if (_surveyMetaPrefixes().length) {
    html += '<div class="legend-lown"><span class="legend-lown-swatch"></span> Small sample (n&lt;30) — estimate suppressed</div>';
  }
  legendDiv.innerHTML = html;
}

// ── District detail panel ───────────────────────────────────────────────────

function showDistrictDetail(props) {
  const dist = props.districts || props.district_agency || '';
  const prov = props.province_territory || '';
  const key = normName(dist);
  const row = rawData[key] || {};

  districtNameEl.textContent = dist;
  districtProvEl.textContent = prov;

  const g = INDICATOR_GROUPS[currentGroup];
  let html = '';

  for (const [ind, label] of Object.entries(g.indicators)) {
    const pct = isPctLabel(label);
    let val;

    if (g.noYear) {
      // Use mixedKeys if available, otherwise default prefix
      const k = (g.mixedKeys && g.mixedKeys[ind]) || `${g.prefix}_${ind}`;
      const v = row[k];
      val = fmt(v, pct);
    } else {
      const v17 = row[`${g.prefix}_2017_${ind}`];
      const v23 = row[`${g.prefix}_2023_${ind}`];
      const vDiff = row[`${g.prefix}_diff_${ind}`];

      if (currentYear === 'diff') {
        val = fmtDiff(vDiff, pct);
      } else {
        val = fmt(currentYear === '2023' ? v23 : v17, pct);
      }
    }

    html += `<div class="stat"><span>${label}</span><strong>${val}</strong></div>`;
  }

  html += '<div class="stat-divider"></div>';
  html += quickStat(row, 't1', 'pop_total', 'Population');
  html += quickStat(row, 't1', 'density_per_sq_km', 'Density /km\u00B2');
  html += quickStat(row, 't12', 'literacy_ratio_all', 'Literacy', true);
  html += quickStat(row, 't1', 'urban_proportion', 'Urban', true);
  html += quickStatRaw(row, 'pslm_net_enrolment_rate', 'PSLM Enrolment', true);

  statsDiv.innerHTML = html;
}

function quickStat(row, prefix, ind, label, pct = false) {
  const v = row[`${prefix}_2023_${ind}`] ?? row[`${prefix}_2017_${ind}`];
  return `<div class="stat stat-quick"><span>${label}</span><strong>${fmt(v, pct)}</strong></div>`;
}
function quickStatRaw(row, key, label, pct = false) {
  const v = row[key];
  return `<div class="stat stat-quick"><span>${label}</span><strong>${fmt(v, pct)}</strong></div>`;
}

// ── Diagnostics ─────────────────────────────────────────────────────────────

function updateDiagnostics(matched) {
  let total = 0;
  districtLayer.eachLayer(() => total++);
  diagEl.textContent = `${matched} / ${total} districts matched`;
}

// ── CSV download ────────────────────────────────────────────────────────────

function prepareDownload() {
  const provFilter = provinceSelect.value;
  const g = INDICATOR_GROUPS[currentGroup];
  const rows = [];
  districtLayer.eachLayer(l => {
    const p = l.feature.properties || {};
    const dist = p.districts || p.district_agency || '';
    const prov = p.province_territory || 'Unknown';
    if (!matchesProvince(provFilter, prov)) return;
    const key = normName(dist);
    const row = rawData[key] || {};
    const entry = { district: dist, province: prov };
    for (const ind of Object.keys(g.indicators)) {
      if (g.noYear) {
        entry[ind] = row[`${g.prefix}_${ind}`] ?? '';
      } else {
        entry[`${ind}_2017`] = row[`${g.prefix}_2017_${ind}`] ?? '';
        entry[`${ind}_2023`] = row[`${g.prefix}_2023_${ind}`] ?? '';
        entry[`${ind}_diff`] = row[`${g.prefix}_diff_${ind}`] ?? '';
      }
    }
    rows.push(entry);
  });
  if (!rows.length) return;
  const header = Object.keys(rows[0]);
  const csv = [header.join(',')].concat(rows.map(r => header.map(h => r[h]).join(','))).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  downloadBtn.href = URL.createObjectURL(blob);
  downloadBtn.download = `data_darbar_${currentGroup}_${currentYear}.csv`;
}

// ── Search ──────────────────────────────────────────────────────────────────

function handleSearch() {
  const q = normName(searchInput.value);
  if (!q) return;
  let found;
  districtLayer.eachLayer(l => {
    const dist = l.feature.properties.districts || l.feature.properties.district_agency || '';
    if (normName(dist).includes(q) && !found) found = l;
  });
  if (found) {
    found.fire('click');
  }
}

// ── Event wiring ────────────────────────────────────────────────────────────

function wireEvents() {
  topicSelect.addEventListener('change', () => {
    currentTopic = topicSelect.value;
    populateGroupSelect();        // re-fill datasets for this topic
    populateIndicatorSelect();    // re-fill indicators for the new dataset
    updateYearButtons();
    colorize();
  });
  groupSelect.addEventListener('change', () => {
    currentGroup = groupSelect.value;
    populateIndicatorSelect();
    updateYearButtons();
    colorize();
  });
  indicatorSelect.addEventListener('change', () => {
    currentIndicator = indicatorSelect.value;
    colorize();
  });
  provinceSelect.addEventListener('change', colorize);
  yearBtns.forEach(btn => {
    btn.addEventListener('click', () => {
      if (btn.disabled) return;
      currentYear = btn.dataset.year;
      yearBtns.forEach(b => b.classList.toggle('active', b === btn));
      colorize();
    });
  });

  // Reset
  if (resetBtn) resetBtn.addEventListener('click', () => {
    currentTopic = 'demographics';
    currentGroup = 'demographics';
    currentIndicator = 'pop_total';
    currentYear = '2023';
    selectedDistrict = null;
    isZoomedIn = false;
    zoomBar.classList.add('hidden');
    topicSelect.value = currentTopic;
    populateGroupSelect();
    populateIndicatorSelect();
    provinceSelect.value = 'ALL';
    searchInput.value = '';
    districtNameEl.textContent = 'Select a district';
    districtProvEl.textContent = '';
    statsDiv.innerHTML = '<p class="stats-placeholder">Click any district on the map to explore its data.</p>';
    updateYearButtons();
    colorize();
    map.fitBounds(districtLayer.getBounds(), { padding: [20, 20] });
  });

  searchInput.addEventListener('input', handleSearch);

  // Zoom out on button click
  zoomOutBtn.addEventListener('click', zoomOut);

  // Escape key: close modals first, then zoom out
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape') {
      const modals = [aboutModal, methodologyModal, contactModal];
      for (const m of modals) {
        if (!m.classList.contains('hidden')) { m.classList.add('hidden'); return; }
      }
      if (isZoomedIn) zoomOut();
    }
  });

  // Modal open buttons
  aboutBtn.addEventListener('click', () => aboutModal.classList.remove('hidden'));
  methodologyBtn.addEventListener('click', () => methodologyModal.classList.remove('hidden'));
  contactBtn.addEventListener('click', () => contactModal.classList.remove('hidden'));

  // Close modal buttons
  document.querySelectorAll('.modal-close').forEach(btn => {
    btn.addEventListener('click', () => {
      const id = btn.dataset.close;
      if (id) document.getElementById(id).classList.add('hidden');
    });
  });

  // Close modal on overlay click
  [aboutModal, methodologyModal, contactModal].forEach(modal => {
    modal.addEventListener('click', e => {
      if (e.target === modal) modal.classList.add('hidden');
    });
  });

  // Contact form — uses FormSubmit.co to forward to Gmail without exposing email
  // The email is obfuscated in JS to avoid scraping
  contactForm.addEventListener('submit', e => {
    e.preventDefault();
    const submitBtn = document.getElementById('contactSubmit');
    submitBtn.disabled = true;
    submitBtn.textContent = 'Sending…';
    contactStatus.textContent = '';
    contactStatus.className = 'contact-status';

    const name    = document.getElementById('contactName').value.trim();
    const email   = document.getElementById('contactEmail').value.trim();
    const subject = document.getElementById('contactSubject').value.trim() || 'Data Darbar Contact';
    const message = document.getElementById('contactMessage').value.trim();

    // Obfuscated recipient — assembled at runtime to prevent scraping
    const r = ['hiba', 'sameen', '@', 'gmail', '.com'].join('');

    // Use FormSubmit.co API (free, no signup, sends to email)
    fetch(`https://formsubmit.co/ajax/${r}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify({
        name: name,
        email: email,
        _subject: subject,
        message: message,
        _template: 'table',
      }),
    })
    .then(res => res.json())
    .then(data => {
      if (data.success === 'true' || data.success === true) {
        contactStatus.textContent = 'Message sent successfully!';
        contactStatus.className = 'contact-status success';
        contactForm.reset();
      } else {
        contactStatus.textContent = 'Something went wrong. Please try again.';
        contactStatus.className = 'contact-status error';
      }
    })
    .catch(() => {
      contactStatus.textContent = 'Network error. Please try again later.';
      contactStatus.className = 'contact-status error';
    })
    .finally(() => {
      submitBtn.disabled = false;
      submitBtn.innerHTML = '<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><line x1="22" y1="2" x2="11" y2="13"/><polygon points="22 2 15 22 11 13 2 9 22 2"/></svg> Send Message';
    });
  });
}

// ── Summary bar ────────────────────────────────────────────────────────────

function updateSummaryBar() {
  const provFilter = provinceSelect.value;
  const g = INDICATOR_GROUPS[currentGroup];
  let count = 0, total = 0;

  districtLayer.eachLayer(l => {
    const p = l.feature.properties || {};
    const prov = p.province_territory || 'Unknown';
    if (!matchesProvince(provFilter, prov)) return;
    total++;
    const v = getVal(p);
    if (v !== null && !isNaN(v)) count++;
  });

  const summDistricts = document.getElementById('summaryDistricts');
  const summIndLabel  = document.getElementById('summaryIndicatorLabel');
  const summIndValue  = document.getElementById('summaryIndicatorValue');
  const summYear      = document.getElementById('summaryYear');

  if (summDistricts) summDistricts.textContent = count;
  if (summIndLabel)  summIndLabel.textContent = g.indicators[currentIndicator] || 'Indicator';
  if (summIndValue) {
    // Show national/filtered aggregate:
    //   - rates/percentages → simple mean
    //   - means, medians, averages, scores → simple mean (not summable)
    //   - counts (population, establishments) → sum
    const values = [];
    districtLayer.eachLayer(l => {
      const p = l.feature.properties || {};
      const prov = p.province_territory || 'Unknown';
      if (!matchesProvince(provFilter, prov)) return;
      const v = getVal(p);
      if (v !== null && !isNaN(v)) values.push(v);
    });
    if (values.length) {
      const pct = isPct(currentIndicator);
      const indLabel = g.indicators[currentIndicator] || '';
      const isAvgType = /\b(avg|average|mean|median|score|per\b)/i.test(indLabel) ||
                        /\b(avg|mean|median)/.test(currentIndicator);
      if (pct || isAvgType) {
        const avg = values.reduce((a, b) => a + b, 0) / values.length;
        summIndValue.textContent = pct
          ? avg.toFixed(1) + '%'
          : avg.toLocaleString('en-US', { maximumFractionDigits: 1 });
      } else {
        const sum = values.reduce((a, b) => a + b, 0);
        summIndValue.textContent = sum >= 1e6
          ? (sum / 1e6).toFixed(1) + 'M'
          : sum.toLocaleString('en-US', { maximumFractionDigits: 0 });
      }
    } else {
      summIndValue.textContent = '\u2014';
    }
  }
  if (summYear) {
    const g2 = INDICATOR_GROUPS[currentGroup];
    if (g2.noYear) {
      summYear.textContent = g2.label.match(/\d{4}/)?.[0] || '\u2014';
    } else if (currentYear === 'diff') {
      summYear.textContent = '\u0394 2017\u21922023';
    } else {
      summYear.textContent = currentYear;
    }
  }
}

// ── Mobile nav & controls ──────────────────────────────────────────────────

function wireMobile() {
  const mobileMenuBtn = document.getElementById('mobileMenuBtn');
  const mobileNav = document.getElementById('mobileNav');
  const mobileControlToggle = document.getElementById('mobileControlToggle');
  const controlPanelInner = document.getElementById('controlPanelInner');

  // Hamburger menu toggle
  if (mobileMenuBtn && mobileNav) {
    mobileMenuBtn.addEventListener('click', () => {
      mobileNav.classList.toggle('hidden');
    });
  }

  // Mobile nav links → same actions as header
  const mobileResetBtn = document.getElementById('mobileResetBtn');
  const mobileAboutBtn = document.getElementById('mobileAboutBtn');
  const mobileMethodologyBtn = document.getElementById('mobileMethodologyBtn');
  const mobileContactBtn = document.getElementById('mobileContactBtn');

  if (mobileResetBtn) mobileResetBtn.addEventListener('click', () => {
    resetBtn.click();
    mobileNav.classList.add('hidden');
  });
  if (mobileAboutBtn) mobileAboutBtn.addEventListener('click', () => {
    aboutModal.classList.remove('hidden');
    mobileNav.classList.add('hidden');
  });
  if (mobileMethodologyBtn) mobileMethodologyBtn.addEventListener('click', () => {
    methodologyModal.classList.remove('hidden');
    mobileNav.classList.add('hidden');
  });
  if (mobileContactBtn) mobileContactBtn.addEventListener('click', () => {
    contactModal.classList.remove('hidden');
    mobileNav.classList.add('hidden');
  });

  // Control panel toggle on mobile
  if (mobileControlToggle && controlPanelInner) {
    mobileControlToggle.addEventListener('click', () => {
      const expanded = controlPanelInner.classList.toggle('expanded');
      mobileControlToggle.classList.toggle('expanded', expanded);
    });
  }
}

// ── Init ────────────────────────────────────────────────────────────────────

async function init() {
  initMap();
  await loadData();
  buildLayer();
  populateTopicSelect();
  populateGroupSelect();
  populateIndicatorSelect();
  updateYearButtons();
  wireEvents();
  wireMobile();
  colorize();
}

init();
