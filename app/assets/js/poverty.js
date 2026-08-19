/* global L, chroma */
/* Data Darbar — Poverty Metrics
   Five layers on two geometries: the survey-based MPI at district (ADM2) level,
   three satellite-derived layers and the Mouza Census facility inventory at tehsil
   (ADM3) level. Join keys (dd_key / dd_id) are baked into the GeoJSON, so no name
   matching happens in the browser. */

const GEO_DISTRICT = 'data/poverty_districts.geojson';
const GEO_TEHSIL   = 'data/poverty_tehsils.geojson';
const DATA_PATH    = 'data/poverty_data.json';

const N = (v) => (v === null || v === undefined || Number.isNaN(v)) ? null : +v;

// Mouza Census indicators all live under rec.mz and are already percentages.
// grp drives the grouped dropdown and the grouped detail panel.
const MZ = (k, label, grp, extra) => Object.assign(
  { label, grp, pct: true, get: r => (r.mz && r.mz.cov) ? N(r.mz[k]) : null }, extra || {});

const LAYERS = {
  mpi: {
    label: 'Multidimensional Poverty',
    geo: 'district',
    unitLabel: 'districts',
    source: 'PSLM 2019-20 · Alkire–Foster',
    blurb: 'Adjusted headcount M₀ = H × A, computed from PSLM 2019-20 household microdata across two dimensions (education, living standards). Higher = poorer.',
    ramp: ['#fdf3e3', '#a8471c', '#6b1503'],
    indicators: {
      mpi:            { label: 'MPI  (M₀ = H × A)',                  get: r => N(r.mpi),  dp: 3 },
      H:              { label: 'Poverty headcount H (%)',            get: r => N(r.H),    pct: true },
      A:              { label: 'Deprivation intensity A (%)',        get: r => N(r.A),    pct: true },
      c_schooling:    { label: 'Deprived — years of schooling (%)',  get: r => N(r.c_schooling),   pct: true },
      c_attendance:   { label: 'Deprived — child attendance (%)',    get: r => N(r.c_attendance),  pct: true },
      c_electricity:  { label: 'Deprived — electricity (%)',         get: r => N(r.c_electricity), pct: true },
      c_cooking_fuel: { label: 'Deprived — cooking fuel (%)',        get: r => N(r.c_cooking_fuel),pct: true },
      c_sanitation:   { label: 'Deprived — sanitation (%)',          get: r => N(r.c_sanitation),  pct: true },
      c_water:        { label: 'Deprived — drinking water (%)',      get: r => N(r.c_water),       pct: true },
      c_housing:      { label: 'Deprived — housing (%)',             get: r => N(r.c_housing),     pct: true },
    },
    allWorse: true,
  },
  rwi: {
    label: 'Relative Wealth',
    geo: 'tehsil',
    unitLabel: 'tehsils',
    source: 'Meta Relative Wealth Index · ~2.4 km grid',
    blurb: 'Machine-learning wealth estimate built from satellite imagery, connectivity and night-lights, aggregated to tehsils and weighted by population. Higher = wealthier.',
    ramp: ['#a8331a', '#f4efe2', '#1a5632'],
    diverging: true,
    indicators: {
      rwi:     { label: 'Relative Wealth Index',            get: r => N(r.rwi),     dp: 2, diverging: true },
      rwi_pct: { label: 'Relative Wealth (percentile)',     get: r => N(r.rwi_pct), dp: 0 },
    },
  },
  pop: {
    label: 'Population',
    geo: 'tehsil',
    unitLabel: 'tehsils',
    source: 'WorldPop 2020 (UN-adjusted) · 1 km',
    blurb: 'Modelled population, zonal-summed to tehsils. Totals 220.7 million, matching the UN 2020 estimate for Pakistan.',
    ramp: ['#eef2f7', '#3d6f9e', '#10243d'],
    indicators: {
      popdens: { label: 'Population density (per km²)', get: r => N(r.popdens), dp: 0 },
      pop:     { label: 'Population',                   get: r => N(r.pop),     dp: 0 },
    },
  },

  mouza: {
    label: 'Rural Facilities',
    geo: 'tehsil',
    unitLabel: 'tehsils',
    source: 'PBS Mouza Census 2020 · 48,738 mouzas',
    blurb: 'What exists on the ground in each revenue village. Every figure is a share of the '
         + 'mouzas that answered that question, so it measures places, not people — a mouza of '
         + '12,000 and a mouza of 300 count the same. Rural only: cities are not in the frame.',
    ramp: ['#eef4f1', '#3f7d6a', '#12332b'],
    indicators: {
      // Electricity and energy
      elec_none:    MZ('elec_none',    'No electricity (%)',                  'Electricity & energy'),
      elec_partial: MZ('elec_partial', 'Electricity in some houses only (%)', 'Electricity & energy'),
      elec_all:     MZ('elec_all',     'Electricity in all houses (%)',       'Electricity & energy'),
      solar:        MZ('solar',        'Solar in use (%)',                    'Electricity & energy'),
      alt_none:     MZ('alt_none',     'No alternative energy source (%)',    'Electricity & energy'),
      // Drinking water
      w_piped:      MZ('w_piped',   'Government piped supply (%)',        'Drinking water'),
      w_pump:       MZ('w_pump',    'Hand or motor pump (%)',             'Drinking water'),
      w_well:       MZ('w_well',    'Well (%)',                           'Drinking water'),
      w_surface:    MZ('w_surface', 'River, canal or pond (%)',           'Drinking water'),
      w_karez:      MZ('w_karez',   'Spring, ravine or karez (%)',        'Drinking water'),
      w_treated:    MZ('w_treated', 'Filtration or RO plant (%)',         'Drinking water'),
      wdepth:       MZ('wdepth',    'Water table depth (feet)',           'Drinking water', { pct: false, dp: 0 }),
      // Streets and roads
      st_dirt:      MZ('st_dirt',      'Dirt streets (%)',            'Streets & roads'),
      st_paved:     MZ('st_paved',     'Cemented or bricked streets (%)', 'Streets & roads'),
      st_metaled:   MZ('st_metaled',   'Metalled streets (%)',        'Streets & roads'),
      road_metaled: MZ('road_metaled', 'On a metalled road (%)',      'Streets & roads'),
      // Housing
      h_bricked:    MZ('h_bricked', 'Houses mainly brick (%)', 'Housing'),
      h_mud:        MZ('h_mud',     'Houses mainly mud (%)',   'Housing'),
      // Schools
      sch_pri_f:    MZ('sch_pri_f',  "Girls' primary school (%)",  'Schools'),
      sch_pri_m:    MZ('sch_pri_m',  "Boys' primary school (%)",   'Schools'),
      sch_mid_f:    MZ('sch_mid_f',  "Girls' middle school (%)",   'Schools'),
      sch_mid_m:    MZ('sch_mid_m',  "Boys' middle school (%)",    'Schools'),
      sch_high_f:   MZ('sch_high_f', "Girls' high school (%)",     'Schools'),
      sch_high_m:   MZ('sch_high_m', "Boys' high school (%)",      'Schools'),
      col_f:        MZ('col_f',      "Girls' college (%)",         'Schools'),
      col_m:        MZ('col_m',      "Boys' college (%)",          'Schools'),
      madrasa:      MZ('madrasa',    'Deeni madrasa (%)',          'Schools'),
      // Health
      hf_bhu:       MZ('hf_bhu',     'Basic health unit (%)',        'Health'),
      hf_rhc:       MZ('hf_rhc',     'Rural health centre (%)',      'Health'),
      hf_hosp:      MZ('hf_hosp',    'Hospital or dispensary (%)',   'Health'),
      hf_private:   MZ('hf_private', 'Private MBBS doctor (%)',      'Health'),
      hf_midwife:   MZ('hf_midwife', 'Midwife (%)',                  'Health'),
      hf_mch:       MZ('hf_mch',     'Mother and child centre (%)',  'Health'),
      // Connectivity
      mobile:       MZ('mobile',     'Mobile signal (%)',            'Connectivity'),
      net_mobile:   MZ('net_mobile', 'Mobile internet (%)',          'Connectivity'),
      net_dsl:      MZ('net_dsl',    'Fixed-line DSL (%)',           'Connectivity'),
      transport:    MZ('transport',  'Public transport (%)',         'Connectivity'),
      post:         MZ('post',       'Post office (%)',              'Connectivity'),
      police:       MZ('police',     'Police station (%)',           'Connectivity'),
      // Cooking fuel
      fuel_wood:    MZ('fuel_wood', 'Firewood (%)',        'Cooking fuel'),
      fuel_dung:    MZ('fuel_dung', 'Animal dung cake (%)', 'Cooking fuel'),
      fuel_gas:     MZ('fuel_gas',  'Piped sui gas (%)',   'Cooking fuel'),
      fuel_lpg:     MZ('fuel_lpg',  'LPG (%)',             'Cooking fuel'),
      // Markets and credit
      bazar:        MZ('bazar',       'Bazar (%)',                  'Markets & credit'),
      mkt_grain:    MZ('mkt_grain',   'Grain wholesale market (%)', 'Markets & credit'),
      bank_online:  MZ('bank_online', 'Commercial bank branch (%)', 'Markets & credit'),
      credit_mfi:   MZ('credit_mfi',  'Microfinance lender (%)',    'Markets & credit'),
      ind_none:     MZ('ind_none',    'No industry of any scale (%)', 'Markets & credit'),
      // Hazards
      dis_any:      MZ('dis_any',     'Exposed to natural disaster (%)', 'Hazards'),
      dis_flood:    MZ('dis_flood',   'Flood (%)',                      'Hazards'),
      dis_drought:  MZ('dis_drought', 'Drought (%)',                    'Hazards'),
      // Settlement
      rural:        MZ('rural',    'Mouzas classified rural (%)',            'Settlement'),
      urbanish:     MZ('urbanish', 'Mouzas urban or partly urban (%)',       'Settlement'),
      unpop:        MZ('unpop',    'Mouzas unpopulated (%)',                 'Settlement'),
    },
  },
  mouza_about: {
    label: 'What is a mouza?',
    explainer: true,
  },
  nl: {
    label: 'Night-time Lights',
    geo: 'tehsil',
    unitLabel: 'tehsils',
    source: 'VIIRS DNB · June composites 2020–2026',
    blurb: 'Satellite radiance as a proxy for economic activity and electrification. Background haze below 1 nW and persistent gas flares are filtered out.',
    ramp: ['#fffbe6', '#d4a017', '#4a2c00'],
    hasYears: true,
    indicators: {
      density: { label: 'Lights per km² (radiance)', get: (r, y) => (r.nl && r.nl[y] !== undefined) ? N(r.nl[y]) : null, dp: 2 },
      growth:  { label: 'Growth in lights 2020→2026 (%)', get: r => N(r.nl_growth), pct: true, diverging: true, noYear: true },
    },
  },
};

// Higher value = worse outcome (red end of the ramp)
const WORSE = new Set(['growth_none']);

let map, layerGroup, geoCache = {}, DATA = null;
let curLayer = 'mpi', curInd = 'mpi', curYear = '2026', provFilter = 'ALL';
let selected = null, selectedKey = null;
const fillCache = new WeakMap();

const $ = id => document.getElementById(id);
const layerSelect = $('layerSelect'), indicatorSelect = $('indicatorSelect'),
      provinceSelect = $('provinceSelect'), legendDiv = $('legend'),
      yearPanel = $('yearPanel'), yearSlider = $('yearSlider'), yearLabel = $('yearLabel'),
      unitNameEl = $('unitName'), unitProvEl = $('unitProvince'), statsDiv = $('stats'),
      searchInput = $('searchInput'), downloadBtn = $('downloadData'),
      sourceNote = $('sourceNote');

function fmt(v, pct, dp) {
  if (v === null || v === undefined || Number.isNaN(v)) return '—';
  if (pct) return v.toFixed(1) + '%';
  if (dp !== undefined) return dp === 0 ? Math.round(v).toLocaleString() : v.toFixed(dp);
  return Math.abs(v) >= 1000 ? Math.round(v).toLocaleString() : v.toFixed(2);
}

function isExplainer() { return !!LAYERS[curLayer].explainer; }
function spec() { return LAYERS[curLayer].indicators[curInd]; }
function isTehsil() { return LAYERS[curLayer].geo === 'tehsil'; }
function records() { return isTehsil() ? DATA.tehsils : DATA.districts; }
function keyOf(props) { return isTehsil() ? props.dd_id : props.dd_key; }

function valueOf(props) {
  const rec = records()[keyOf(props)];
  if (!rec) return null;
  if (suppressed(rec)) return null;
  return spec().get(rec, curYear);
}

// Suppression: MPI small-sample, and night-lights over effectively uninhabited terrain
// where June snow/sand albedo, not activity, drives the signal.
function suppressed(rec) {
  if (!rec) return true;
  if (curLayer === 'mpi') return rec.low_n === 1;
  if (curLayer === 'nl') return rec.nl_lowc === 1;
  return false;
}
function hasData(rec) {
  if (!rec) return false;
  if (curLayer === 'mpi') return rec.mpi !== undefined;
  // A tehsil can be outside the Mouza Census frame entirely (urban Karachi), or
  // inside it but never enumerated (AJK, GB, three Makran sub-tehsils).
  if (curLayer === 'mouza') return !!(rec.mz && rec.mz.cov);
  return true;
}

function syncExplainer() {
  const on = isExplainer();
  document.body.classList.toggle('explainer-mode', on);
  $('explainer').hidden = !on;
  if (!on) return;
  // Repurpose the summary bar as the scale of the census rather than leaving
  // three em-dashes hanging over an explainer with no map behind it.
  $('summaryUnitsLabel').textContent = 'Mouzas counted';
  $('summaryUnits').textContent = '48,738';
  $('summaryIndicatorLabel').textContent = 'Tehsils';
  $('summaryIndicatorValue').textContent = '595';
  $('summaryScope').textContent = 'PBS Mouza Census 2020';
}

// ── Controls ────────────────────────────────────────────────────────────────
function buildLayerSelect() {
  layerSelect.innerHTML = Object.entries(LAYERS)
    .map(([k, v]) => `<option value="${k}">${v.label}</option>`).join('');
  layerSelect.value = curLayer;
}
function buildIndicatorSelect() {
  const L0 = LAYERS[curLayer];
  if (!L0.indicators) { indicatorSelect.innerHTML = ''; return; }
  const entries = Object.entries(L0.indicators);
  if (entries.some(([, v]) => v.grp)) {
    // 50-odd indicators is too many for a flat list, so group them by theme.
    const groups = [];
    entries.forEach(([k, v]) => {
      const g = v.grp || 'Other';
      let bucket = groups.find(x => x.name === g);
      if (!bucket) groups.push(bucket = { name: g, items: [] });
      bucket.items.push([k, v]);
    });
    indicatorSelect.innerHTML = groups.map(g =>
      `<optgroup label="${g.name}">` +
      g.items.map(([k, v]) => `<option value="${k}">${v.label}</option>`).join('') +
      '</optgroup>').join('');
  } else {
    indicatorSelect.innerHTML = entries
      .map(([k, v]) => `<option value="${k}">${v.label}</option>`).join('');
  }
  if (!L0.indicators[curInd]) curInd = Object.keys(L0.indicators)[0];
  indicatorSelect.value = curInd;
}
function buildProvinceSelect() {
  const provs = new Set();
  Object.values(DATA.districts).forEach(r => r.prov && provs.add(r.prov));
  provinceSelect.innerHTML = '<option value="ALL">All Provinces</option>' +
    [...provs].sort().map(p => `<option value="${p}">${p}</option>`).join('');
  provinceSelect.value = provFilter;
}
function syncYearUI() {
  const L0 = LAYERS[curLayer];
  const show = !!L0.hasYears && !L0.explainer && !spec().noYear;
  yearPanel.style.display = show ? '' : 'none';
  if (show) { yearSlider.value = DATA.meta.years.indexOf(+curYear); yearLabel.textContent = 'June ' + curYear; }
}

// ── Geometry ────────────────────────────────────────────────────────────────
// Prefer data inlined as window.DD_POV_GEO_* (works under file://); fall back to
// fetching the JSON (works when served over http). Mirrors app.js.
async function loadGeo(which) {
  if (geoCache[which]) return geoCache[which];
  const inline = which === 'tehsil' ? window.DD_POV_GEO_T : window.DD_POV_GEO_D;
  const g = inline || await (await fetch(which === 'tehsil' ? GEO_TEHSIL : GEO_DISTRICT)).json();
  geoCache[which] = g;
  return g;
}

async function drawGeometry() {
  if (isExplainer()) return;
  const which = LAYERS[curLayer].geo;
  const geo = await loadGeo(which);
  if (layerGroup) { map.removeLayer(layerGroup); }
  selected = null; selectedKey = null;
  layerGroup = L.geoJSON(geo, {
    style: { weight: which === 'tehsil' ? 0.5 : 1, color: '#8a9480', fillOpacity: 0.9 },
    onEachFeature: (f, lyr) => {
      lyr.on('click', () => selectUnit(lyr));
      lyr.on('mouseover', () => { lyr.setStyle({ weight: 2, color: '#0c3a1e' }); lyr.bringToFront(); });
      lyr.on('mouseout', () => {
        if (selected === lyr) return;
        const s = fillCache.get(lyr) || {};
        lyr.setStyle({ weight: which === 'tehsil' ? 0.5 : 1, color: s.color || '#8a9480' });
      });
      lyr.bindTooltip(() => tooltipHtml(f.properties), { sticky: true, className: 'dd-tip' });
    },
  }).addTo(map);
  map.fitBounds(layerGroup.getBounds(), { padding: [8, 8] });
}

function tooltipHtml(props) {
  const rec = records()[keyOf(props)];
  const nm = rec ? rec.name : '—';
  const v = valueOf(props);
  const s = spec();
  return `<b>${nm}</b><br>${s.label}: ${v === null ? '—' : fmt(v, s.pct, s.dp)}`;
}

// ── Render ──────────────────────────────────────────────────────────────────
function render() {
  if (isExplainer() || !layerGroup) return;
  const L0 = LAYERS[curLayer], s = spec();
  const recs = records();
  const vals = [];
  layerGroup.eachLayer(l => {
    const p = l.feature.properties;
    if (!matchProv(p)) return;
    const v = valueOf(p);
    if (v !== null) vals.push(v);
  });

  let scale, breaks, classColors = null;
  const diverging = s.diverging || false;
  if (!vals.length) {
    scale = () => chroma('#e2e5ea'); breaks = [0, 1];
  } else if (diverging) {
    // Robust symmetric domain: a handful of tehsils grew by >10,000% from a near-dark
    // 2020 baseline, and using the raw max would flatten the entire map to the midpoint.
    // Clip to the 95th percentile of |value| so the ramp spans the bulk of the distribution.
    const abs = vals.map(Math.abs).sort((a, b) => a - b);
    const absMax = abs[Math.floor(abs.length * 0.95)] || abs[abs.length - 1] || 1;
    // wealth/growth: negative = red (worse), positive = green (better)
    scale = chroma.scale(['#a8331a', '#f4efe2', '#1a5632']).domain([-absMax, 0, absMax]);
    breaks = [-absMax, -absMax / 2, 0, absMax / 2, absMax];
  } else {
    // CLASSED quantile choropleth rather than a linear ramp. Lights and population density
    // are heavily right-skewed (Karachi is ~250x the median tehsil); stretching a linear
    // domain from min to max washes almost every unit out to the lightest colour.
    // Equal-count classes keep the map readable and the classes honest.
    breaks = chroma.limits(vals, 'q', 5);
    classColors = chroma.scale(L0.ramp).colors(breaks.length - 1);
    scale = v => chroma(classColors[classOf(v, breaks)]);
  }

  const grp = L.featureGroup();
  layerGroup.eachLayer(l => {
    const p = l.feature.properties;
    let st;
    if (!matchProv(p)) {
      st = { fillOpacity: 0, weight: 0, opacity: 0, color: 'transparent' };
    } else {
      grp.addLayer(l);
      const rec = recs[keyOf(p)];
      const v = valueOf(p);
      if (rec && suppressed(rec)) {
        st = { fillColor: '#f5e6b8', fillOpacity: 0.3, weight: 0.8, color: '#c49515', opacity: 0.9, dashArray: '4 3' };
      } else if (!hasData(rec) || v === null) {
        st = { fillColor: '#e2e5ea', fillOpacity: 0.4, weight: 0.6, color: '#b0b8c1', opacity: 1, dashArray: '2 4' };
      } else {
        st = { fillColor: scale(v).hex(), fillOpacity: 0.92, weight: isTehsil() ? 0.4 : 1, color: '#8a9480', opacity: 1 };
      }
    }
    l.setStyle(st); fillCache.set(l, st);
  });

  if (provFilter !== 'ALL' && grp.getLayers().length) map.fitBounds(grp.getBounds(), { padding: [24, 24] });
  if (selected) selected.setStyle({ weight: 2.5, color: '#0c3a1e' });

  renderLegend(breaks, scale, diverging, classColors);
  updateSummary(vals);
  buildRankings();
  prepareDownload();
  sourceNote.innerHTML = `<b>${L0.label}</b> — ${L0.source}<br>${L0.blurb}`;
  if (selectedKey) showDetail(selectedKey);
}

function matchProv(p) { return provFilter === 'ALL' || (p.prov || '') === provFilter; }

// Which quantile class a value falls in (0 … breaks.length-2)
function classOf(v, breaks) {
  for (let i = 1; i < breaks.length; i++) if (v <= breaks[i]) return i - 1;
  return breaks.length - 2;
}

function renderLegend(breaks, scale, diverging, classColors) {
  const s = spec();
  const yr = (LAYERS[curLayer].hasYears && !s.noYear) ? `June ${curYear}` : '';
  let h = `<div class="legend-title">${s.label}<span>${yr}</span></div><div class="legend-scale">`;
  if (classColors) {
    classColors.forEach((c, i) => {
      const lo = fmt(breaks[i], s.pct, s.dp), hi = fmt(breaks[i + 1], s.pct, s.dp);
      h += `<span style="background:${c}" title="${lo} – ${hi}"></span>`;
    });
  } else {
    const n = 5;
    for (let i = 0; i < n; i++) {
      const mid = breaks[0] + (breaks[breaks.length - 1] - breaks[0]) * (i + 0.5) / n;
      h += `<span style="background:${scale(mid).hex()}"></span>`;
    }
  }
  h += '</div>';
  h += `<div class="legend-labels"><span>${fmt(breaks[0], s.pct, s.dp)}</span><span>${fmt(breaks[breaks.length - 1], s.pct, s.dp)}</span></div>`;
  if (curLayer === 'mpi') h += '<div class="legend-lown"><span class="legend-lown-swatch"></span> Small sample (n&lt;30) — suppressed</div>';
  if (curLayer === 'nl') h += '<div class="legend-lown"><span class="legend-lown-swatch"></span> Uninhabited terrain — snow/sand albedo, not activity</div>';
  if (curLayer === 'mouza') h += '<div class="legend-lown">Urban tehsils sit outside the rural frame; AJK and GB were not enumerated</div>';
  h += '<div class="legend-lown"><span class="legend-nosurv-swatch"></span> No data for this unit</div>';
  legendDiv.innerHTML = h;
}

function updateSummary(vals) {
  const s = spec();
  $('summaryUnits').textContent = vals.length.toLocaleString();
  $('summaryUnitsLabel').textContent = LAYERS[curLayer].unitLabel === 'tehsils' ? 'Tehsils showing' : 'Districts showing';
  $('summaryIndicatorLabel').textContent = s.label;
  const mean = vals.length ? vals.reduce((a, b) => a + b, 0) / vals.length : null;
  $('summaryIndicatorValue').textContent = mean === null ? '—' : fmt(mean, s.pct, s.dp);
  $('summaryScope').textContent = (LAYERS[curLayer].hasYears && !s.noYear) ? 'June ' + curYear
    : (isTehsil() ? 'Tehsil (ADM3)' : 'District (ADM2)');
}

function buildRankings() {
  const s = spec(), recs = records();
  const rows = [];
  layerGroup.eachLayer(l => {
    const p = l.feature.properties;
    if (!matchProv(p)) return;
    const v = valueOf(p); if (v === null) return;
    const rec = recs[keyOf(p)];
    rows.push({ name: rec.name, prov: rec.prov, v });
  });
  rows.sort((a, b) => b.v - a.v);
  const row = r => `<div class="ranking-row"><span class="ranking-name">${r.name}</span><span class="ranking-val">${fmt(r.v, s.pct, s.dp)}</span></div>`;
  $('rankingTop10Body').innerHTML = rows.slice(0, 10).map(row).join('') || '<div class="ranking-row">—</div>';
  $('rankingBottom10Body').innerHTML = rows.slice(-10).reverse().map(row).join('') || '<div class="ranking-row">—</div>';
}

// ── Detail ──────────────────────────────────────────────────────────────────
function selectUnit(lyr) {
  if (selected) { const s0 = fillCache.get(selected) || {}; selected.setStyle({ weight: isTehsil() ? 0.4 : 1, color: s0.color || '#8a9480' }); }
  selected = lyr; selectedKey = keyOf(lyr.feature.properties);
  lyr.setStyle({ weight: 2.5, color: '#0c3a1e' }); lyr.bringToFront();
  showDetail(selectedKey);
}

function showDetail(key) {
  const rec = records()[key];
  if (!rec) { statsDiv.innerHTML = '<p class="stats-placeholder">No data for this unit.</p>'; return; }
  unitNameEl.textContent = rec.name;
  unitProvEl.textContent = isTehsil() ? `${rec.prov} · ${(rec.dk || '').replace(/\b\w/g, c => c.toUpperCase())} district` : rec.prov;
  const L0 = LAYERS[curLayer];
  let h = '', lastGrp = null;
  for (const [k, sp] of Object.entries(L0.indicators)) {
    if (sp.grp && sp.grp !== lastGrp) { h += `<div class="stat-group">${sp.grp}</div>`; lastGrp = sp.grp; }
    const v = suppressed(rec) ? null : sp.get(rec, curYear);
    const on = k === curInd ? ' stat-active' : '';
    h += `<div class="stat${on}"><span>${sp.label}</span><strong>${fmt(v, sp.pct, sp.dp)}</strong></div>`;
  }
  if (curLayer === 'mouza' && rec.mz && rec.mz.cov) {
    const mz = rec.mz;
    h += `<p class="stat-note">${mz.m.toLocaleString()} mouzas`
       + (mz.np > 1 ? `, pooled from ${mz.np} PBS tehsils that share this boundary` : '')
       + (mz.apx ? ' · includes a tehsil placed here by judgement, see the crosswalk' : '')
       + (mz.bsp > 5 ? ` · the reporting base varies by up to ${mz.bsp}% between questions` : '')
       + '.</p>';
  }
  if (curLayer === 'mpi' && rec.rank) h += `<p class="stat-note">Rank ${rec.rank} of 119 districts by MPI (1 = poorest). Sample: ${rec.n_obs.toLocaleString()} households.</p>`;
  if (curLayer === 'nl' && rec.nl && Object.keys(rec.nl).length) {
    h += '<p class="stat-note">Lights per km², June of each year:</p><div class="spark">';
    const ys = DATA.meta.years, vs = ys.map(y => rec.nl[String(y)] || 0), mx = Math.max(...vs, 0.0001);
    ys.forEach((y, i) => {
      h += `<div class="spark-bar" title="${y}: ${vs[i].toFixed(2)}"><div style="height:${Math.max(2, vs[i] / mx * 46)}px"></div><span>${String(y).slice(2)}</span></div>`;
    });
    h += '</div>';
  }
  if (suppressed(rec)) h += '<p class="stat-note">Values suppressed for this unit — see the legend.</p>';
  statsDiv.innerHTML = h;
}

function prepareDownload() {
  const s = spec(), recs = records();
  const lines = [['unit', 'province', isTehsil() ? 'district' : '', s.label].filter(Boolean).join(',')];
  layerGroup.eachLayer(l => {
    const p = l.feature.properties; if (!matchProv(p)) return;
    const rec = recs[keyOf(p)]; if (!rec) return;
    const v = valueOf(p);
    const cells = [`"${rec.name}"`, `"${rec.prov}"`];
    if (isTehsil()) cells.push(`"${rec.dk || ''}"`);
    cells.push(v === null ? '' : v);
    lines.push(cells.join(','));
  });
  downloadBtn.href = URL.createObjectURL(new Blob([lines.join('\n')], { type: 'text/csv' }));
  downloadBtn.download = `data_darbar_${curLayer}_${curInd}.csv`;
}

// ── Search ──────────────────────────────────────────────────────────────────
function doSearch(q) {
  q = q.trim().toLowerCase(); if (!q) return;
  let hit = null;
  layerGroup.eachLayer(l => {
    if (hit) return;
    const rec = records()[keyOf(l.feature.properties)];
    if (rec && rec.name.toLowerCase().includes(q)) hit = l;
  });
  if (hit) { map.fitBounds(hit.getBounds(), { padding: [60, 60] }); selectUnit(hit); }
}

// ── Init ────────────────────────────────────────────────────────────────────
async function init() {
  // zoomSnap defaults to 1, so fitBounds has to round DOWN to a whole zoom level and
  // Pakistan landed at zoom 5 in a container that comfortably fits 5.9 — the country
  // drew at about half size with the slack as empty margin. Fractional snapping lets
  // the fit use the space it actually has. zoomDelta stays coarse so the +/- buttons
  // and keyboard still move in useful steps rather than 0.1 at a time.
  map = L.map('map', { zoomControl: true, attributionControl: false, minZoom: 4, maxZoom: 11,
                       zoomSnap: 0.1, zoomDelta: 0.5 });
  DATA = window.DD_POV || await (await fetch(DATA_PATH)).json();
  // Mouza Census payload ships separately and is keyed on the same dd_id.
  if (window.DD_POV_MOUZA) {
    for (const [id, mz] of Object.entries(window.DD_POV_MOUZA)) {
      if (DATA.tehsils[id]) DATA.tehsils[id].mz = mz;
    }
  }
  curYear = String(DATA.meta.years[DATA.meta.years.length - 1]);

  buildLayerSelect(); buildIndicatorSelect(); buildProvinceSelect(); syncExplainer();
  yearSlider.min = 0; yearSlider.max = DATA.meta.years.length - 1;
  yearSlider.value = DATA.meta.years.length - 1;
  syncYearUI();

  await drawGeometry();
  render();

  layerSelect.addEventListener('change', async e => {
    const prevGeo = LAYERS[curLayer].geo;
    curLayer = e.target.value;
    if (LAYERS[curLayer].indicators) curInd = Object.keys(LAYERS[curLayer].indicators)[0];
    buildIndicatorSelect(); syncExplainer(); syncYearUI();
    if (isExplainer()) return;
    // The map was display:none while the explainer showed, so Leaflet's cached
    // container size is stale; invalidateSize before any fitBounds runs.
    map.invalidateSize();
    if (LAYERS[curLayer].geo !== prevGeo) await drawGeometry();
    else map.fitBounds(layerGroup.getBounds(), { padding: [8, 8] });
    render();
  });
  indicatorSelect.addEventListener('change', e => { curInd = e.target.value; syncYearUI(); render(); });
  provinceSelect.addEventListener('change', e => { provFilter = e.target.value; render(); });
  yearSlider.addEventListener('input', e => {
    curYear = String(DATA.meta.years[+e.target.value]);
    yearLabel.textContent = 'June ' + curYear;
    render();
  });
  searchInput.addEventListener('keydown', e => { if (e.key === 'Enter') doSearch(e.target.value); });
  $('resetBtn').addEventListener('click', () => {
    provFilter = 'ALL'; provinceSelect.value = 'ALL'; searchInput.value = '';
    selected = null; selectedKey = null;
    unitNameEl.textContent = 'Select an area';
    unitProvEl.textContent = '';
    statsDiv.innerHTML = '<p class="stats-placeholder">Click any area on the map to explore its data.</p>';
    map.fitBounds(layerGroup.getBounds(), { padding: [8, 8] });
    render();
  });
  const mb = $('mobileControlToggle');
  if (mb) mb.addEventListener('click', () => $('controlPanelInner').classList.toggle('open'));
}

document.addEventListener('DOMContentLoaded', init);
