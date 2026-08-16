/* Smoke test for the district panel's provenance notices.
 *
 *   node tests/panel_notices.test.js
 *
 * Why this exists. Several figures on the site describe a coarser or older
 * geography than the district you clicked — Karachi before it was split into
 * seven, Karachi West before Keamari was carved out of it, HIES's rural-only
 * strata. Each carries a flag in districts.json and a notice in the panel. A
 * borrowed number rendering without its notice looks exactly like a district
 * estimate, which is the failure the flags exist to prevent.
 *
 * It shipped once already: the notice code read `props` (the GeoJSON feature's
 * own properties) instead of `row` (the districts.json record), so it could
 * never fire. It passed review because the check simulated the lookup instead
 * of calling showDistrictDetail. This test drives the real function with real
 * GeoJSON properties, so the call path is the one the browser uses.
 */
const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');   // npm i --no-save jsdom

const ROOT = path.join(__dirname, '..');
const src = fs.readFileSync(path.join(ROOT, 'app/assets/js/app.js'), 'utf8');
const DATA = JSON.parse(fs.readFileSync(path.join(ROOT, 'app/data/districts.json'), 'utf8'));
const GEO = JSON.parse(fs.readFileSync(
  path.join(ROOT, 'app/data/pakistan_districts_province_boundries.geojson'), 'utf8'));

// Pull the real declarations out of app.js rather than reimplementing them.
function balanced(from) {
  let depth = 0;
  for (let i = src.indexOf('{', from); i < src.length; i++) {
    if (src[i] === '{') depth++;
    else if (src[i] === '}' && --depth === 0) return src.slice(from, i + 1);
  }
  throw new Error('unbalanced braces from ' + from);
}
const fn = n => balanced(src.indexOf('function ' + n));
const cst = n => balanced(src.indexOf('const ' + n)) + ';';

const body = [
  cst('INDICATOR_GROUPS'), cst('TOPICS'), cst('SURVEY_META_PREFIX'),
  fn('normName'), fn('_surveyMetaPrefixes'), fn('isNotSurveyed'), fn('dataKey'),
  fn('diffKey'), fn('isPctLabel'), fn('fmt'), fn('fmtDiff'), fn('quickStat'),
  fn('quickStatRaw'), fn('showDistrictDetail'),
].join('\n');

const dom = new JSDOM('<!doctype html><body><div id="districtName"></div>' +
  '<div id="districtProv"></div><div id="districtStats"></div>' +
  '<div id="sidebar"></div><div id="topBar"></div></body>');
const doc = dom.window.document;
const stats = doc.getElementById('districtStats');

const make = group => new Function(
  'rawData', 'currentGroup', 'currentYear', 'districtNameEl', 'districtProvEl',
  'districtStatsEl', 'document', 'statsDiv', body + '; return showDistrictDetail;'
)(DATA, group, '2023', doc.getElementById('districtName'),
  doc.getElementById('districtProv'), stats, doc, stats);

const propsFor = key => {
  const norm = s => String(s).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  const f = GEO.features.find(f =>
    norm(f.properties.districts || f.properties.district_agency) === key);
  if (!f) throw new Error('no polygon for ' + key);
  return f.properties;
};

// district, group, substring each expected notice must contain
const CASES = [
  ['keamari',      'hies',              ['Rural households only', "Karachi West's"]],
  ['keamari',      'demographics',      ['Boundary changed since 2017']],
  ['karachi west', 'demographics',      ['Boundary changed since 2017']],
  ['karachi east', 'dhsFamilyPlanning', ['Karachi city-wide']],
  ['lahore',       'hies',              ['No figures for this district']],
  ['multan',       'hies',              ['Rural households only']],
  ['multan',       'demographics',      []],   // control: no notice at all
];

let failed = 0;
for (const [dk, group, expected] of CASES) {
  stats.innerHTML = '';
  make(group)(propsFor(dk));
  const notices = [...stats.querySelectorAll('.stat-notice')].map(n => n.textContent);
  const missing = expected.filter(e => !notices.some(n => n.includes(e)));
  const extra = expected.length === 0 && notices.length > 0;
  const ok = !missing.length && !extra;
  if (!ok) failed++;
  console.log(`${ok ? 'ok  ' : 'FAIL'}  ${dk} / ${group}` +
    (missing.length ? `\n        missing: ${missing.join(' | ')}` : '') +
    (extra ? `\n        unexpected: ${notices.join(' | ')}` : ''));
}

// The inlined copy is what the pages actually read; if it drifts from
// districts.json the site serves stale numbers while the JSON looks correct.
global.window = {};
require(path.join(ROOT, 'app/data/census_data.js'));
const inlineMatches = JSON.stringify(global.window.DD_DATA) === JSON.stringify(DATA);
console.log(`${inlineMatches ? 'ok  ' : 'FAIL'}  census_data.js matches districts.json`);
if (!inlineMatches) failed++;

console.log(failed ? `\n${failed} failing` : '\nall passing');
process.exit(failed ? 1 : 0);
