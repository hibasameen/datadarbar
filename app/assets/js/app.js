/* global L, chroma, fetch */
const map = L.map('map', {
  zoomControl: true,
  attributionControl: true
}).setView([29.9, 69.4], 5);

L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
  maxZoom: 10,
  attribution: '&copy; OpenStreetMap'
}).addTo(map);

let districtLayer;
let rawData = {};   // district-level indicators
let provinceIndex = {}; // district -> province
let currentIndicator = document.getElementById('indicatorSelect').value;

const provinceSelect = document.getElementById('provinceSelect');
const indicatorSelect = document.getElementById('indicatorSelect');
const resetBtn = document.getElementById('resetBtn');
const searchInput = document.getElementById('searchInput');
const statsDiv = document.getElementById('stats');
const legendDiv = document.getElementById('legend');
const districtNameEl = document.getElementById('districtName');
const downloadData = document.getElementById('downloadData');

const niceLabel = {
  literacy_total: 'Literacy (Total %)',
  literacy_male: 'Literacy (Male %)',
  literacy_female: 'Literacy (Female %)',
  school_attendance: 'School Attendance (Net %, 5–16)',
  disability_rate: 'Disability Rate (%)',
  no_health_facility_5km: 'No Health Facility within 5km (%)'
};

function normName(s) {
  return (s || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').replace(/\s+/g, ' ').trim();
}

async function init() {
  const [geojson, data] = await Promise.all([
    fetch(GEOJSON_PATH).then(r => r.json()),
    fetch(DATA_PATH).then(r => r.json()).catch(() => ({}))
  ]);

  rawData = data;

  districtLayer = L.geoJSON(geojson, {
    style: baseStyle,
    onEachFeature: onEachDistrict
  }).addTo(map);

  // Build province index and dropdown
  const provinces = new Set();
  districtLayer.eachLayer(layer => {
    const props = layer.feature.properties || {};
    const prov = props.province_name || props.province || props.Prov_Name || props.Province || 'Unknown';
    const dist = props.district_name || props.district || props.Dist_Name || props.District || props.DISTRICT || props.NAME_2;
    const key = normName(dist);
    provinceIndex[key] = prov;
    provinces.add(prov);
  });

  [...provinces].sort().forEach(p => {
    const opt = document.createElement('option');
    opt.value = p;
    opt.textContent = p;
    provinceSelect.appendChild(opt);
  });

  // Wire up events
  indicatorSelect.addEventListener('change', () => {
    currentIndicator = indicatorSelect.value;
    colorize();
  });

  provinceSelect.addEventListener('change', () => { colorize(); });

  resetBtn.addEventListener('click', () => {
    provinceSelect.value = 'ALL';
    indicatorSelect.value = 'literacy_total';
    currentIndicator = indicatorSelect.value;
    searchInput.value = '';
    districtNameEl.textContent = 'Click a district';
    statsDiv.innerHTML = '';
    colorize();
  });

  searchInput.addEventListener('input', () => {
    const q = normName(searchInput.value);
    if (!q) return;
    let found;
    districtLayer.eachLayer(layer => {
      const props = layer.feature.properties || {};
      const dist = props.district_name || props.district || props.Dist_Name || props.District || props.DISTRICT || props.NAME_2;
      if (normName(dist).includes(q)) found = layer;
    });
    if (found) {
      map.fitBounds(found.getBounds(), { maxZoom: 8 });
      found.fire('click');
    }
  });

  // initial paint
  colorize();
}

function baseStyle() {
  return { color: '#94a3b8', weight: 0.8, fillOpacity: 0.9, fillColor: '#e2e8f0' };
}

function getValueForFeature(props, ind = currentIndicator) {
  const dist = props.district_name || props.district || props.Dist_Name || props.District || props.DISTRICT || props.NAME_2;
  const key = normName(dist);
  const row = rawData[key];
  if (!row) return null;
  const val = row[ind];
  return (val === null || val === undefined || val === '') ? null : Number(val);
}

function onEachDistrict(feature, layer) {
  layer.on({
    mouseover: e => {
      const l = e.target;
      l.setStyle({ weight: 1.2, color: '#0ea5e9' });
      if (!L.Browser.ie && !L.Browser.opera && !L.Browser.edge) {
        l.bringToFront();
      }
    },
    mouseout: e => {
      districtLayer.resetStyle(e.target);
    },
    click: e => {
      const p = e.target.feature.properties || {};
      const dist = p.district_name || p.district || p.Dist_Name || p.District || p.DISTRICT || p.NAME_2;
      const prov = p.province_name || p.province || p.Prov_Name || p.Province || '—';
      const key = normName(dist);
      const row = rawData[key] || {};
      districtNameEl.textContent = `${dist} — ${prov}`;
      statsDiv.innerHTML = `
        <div class="stat"><span>${niceLabel.literacy_total}</span><strong>${fmtPct(row.literacy_total)}</strong></div>
        <div class="stat"><span>${niceLabel.literacy_male}</span><strong>${fmtPct(row.literacy_male)}</strong></div>
        <div class="stat"><span>${niceLabel.literacy_female}</span><strong>${fmtPct(row.literacy_female)}</strong></div>
        <hr/>
        <div class="stat"><span>${niceLabel.school_attendance}</span><strong>${fmtPct(row.school_attendance)}</strong></div>
        <div class="stat"><span>${niceLabel.disability_rate}</span><strong>${fmtPct(row.disability_rate)}</strong></div>
        <div class="stat"><span>${niceLabel.no_health_facility_5km}</span><strong>${fmtPct(row.no_health_facility_5km)}</strong></div>
      `;

      // Prepare CSV for download (single district OR full filtered? let's do filtered set)
      prepareDownload();
    }
  });
}

function fmtPct(v) {
  if (v === null || v === undefined || (typeof v === 'number' && isNaN(v))) return '—';
  const num = Number(v);
  if (isNaN(num)) return '—';
  return num.toFixed(1) + '%';
}

function colorize() {
  // collect values for current indicator, filtered by province
  const provFilter = provinceSelect.value;
  const values = [];
  districtLayer.eachLayer(l => {
    const p = l.feature.properties || {};
    const prov = p.province_name || p.province || p.Prov_Name || p.Province || 'Unknown';
    if (provFilter !== 'ALL' && prov !== provFilter) return;
    const v = getValueForFeature(p);
    if (v !== null && !isNaN(v)) values.push(v);
  });

  // if no data, style grey
  if (values.length === 0) {
    districtLayer.setStyle(f => baseStyle());
    legendDiv.innerHTML = '<em>No data</em>';
    return;
  }

  const breaks = chroma.limits(values, 'q', 5);
  const scale = chroma.scale(['#f1f5f9', '#0ea5e9']).domain([breaks[0], breaks[breaks.length - 1]]);

  districtLayer.eachLayer(l => {
    const p = l.feature.properties || {};
    const prov = p.province_name || p.province || p.Prov_Name || p.Province || 'Unknown';
    if (provFilter !== 'ALL' && prov !== provFilter) {
      l.setStyle({ fillOpacity: 0.2, fillColor: '#e5e7eb' });
      return;
    }
    const v = getValueForFeature(p);
    if (v === null || isNaN(v)) {
      l.setStyle({ fillOpacity: 0.5, fillColor: '#e5e7eb' });
    } else {
      l.setStyle({ fillColor: scale(v).hex(), fillOpacity: 0.9 });
    }
  });

  // legend
  renderLegend(breaks, scale);

  // update CSV link
  prepareDownload();
}

function renderLegend(breaks, scale) {
  legendDiv.innerHTML = '';
  const bar = document.createElement('div');
  bar.className = 'legend-scale';
  for (let i = 0; i < 5; i++) {
    const span = document.createElement('span');
    const v = (breaks[i] + breaks[i+1]) / 2 || breaks[i];
    span.style.background = scale(v).hex();
    bar.appendChild(span);
  }
  legendDiv.appendChild(bar);

  const labels = document.createElement('div');
  labels.className = 'legend-labels';
  const min = document.createElement('span');
  const max = document.createElement('span');
  min.textContent = breaks[0].toFixed(1) + '%';
  max.textContent = breaks[breaks.length - 1].toFixed(1) + '%';
  labels.appendChild(min);
  labels.appendChild(max);
  legendDiv.appendChild(labels);
}

function prepareDownload() {
  const provFilter = provinceSelect.value;
  const rows = [];
  districtLayer.eachLayer(l => {
    const p = l.feature.properties || {};
    const dist = p.district_name || p.district || p.Dist_Name || p.District || p.DISTRICT || p.NAME_2;
    const prov = p.province_name || p.province || p.Prov_Name || p.Province || 'Unknown';
    if (provFilter !== 'ALL' && prov !== provFilter) return;
    const key = normName(dist);
    const row = rawData[key] || {};
    rows.push({
      district: dist,
      province: prov,
      literacy_total: row.literacy_total ?? '',
      literacy_male: row.literacy_male ?? '',
      literacy_female: row.literacy_female ?? '',
      school_attendance: row.school_attendance ?? '',
      disability_rate: row.disability_rate ?? '',
      no_health_facility_5km: row.no_health_facility_5km ?? ''
    });
  });
  const header = Object.keys(rows[0] || { district: '', province: '' });
  const csv = [header.join(',')].concat(rows.map(r => header.map(h => r[h]).join(','))).join('\n');
  const blob = new Blob([csv], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  downloadData.href = url;
}

init();
