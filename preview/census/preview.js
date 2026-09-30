/* global d3 */
(() => {
  'use strict';
  const METRICS = {
    population: {
      pop_total: 'Total population', pop_male: 'Male population',
      pop_female: 'Female population', pop_transgender: 'Transgender population'
    },
    education: {
      pct_matric_plus: 'Matric or higher (%)', pct_never_attended: 'Never attended school (%)',
      total: 'Population aged 5+', never_attended: 'Never attended school · people',
      below_primary: 'Below primary · people', primary: 'Primary · people', middle: 'Middle · people',
      matric: 'Matric · people', intermediate: 'Intermediate · people', graduate: 'Graduate · people',
      masters_above: 'Masters or above · people', diploma_certificate: 'Diploma / certificate · people',
      others: 'Other attainment · people', matric_plus: 'Matric or higher · people'
    }
  };
  const POPULATION = 'population';
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const number = (value, digits = 0) => value == null ? '—' : Number(value).toLocaleString('en-GB', {minimumFractionDigits: digits, maximumFractionDigits: digits});
  const signed = (value, digits = 0) => value == null ? '—' : (value > 0 ? '+' : value < 0 ? '−' : '') + number(Math.abs(value), digits);
  const isRate = indicator => indicator.startsWith('pct_');
  const reading = (area, year, indicator) => area.values[year]?.[indicator] ?? {value: null, status: 'missing'};
  const delta = (a, b) => a == null || b == null ? null : b - a;
  const percentChange = (a, b) => a == null || b == null || a === 0 ? null : 100 * (b - a) / a;
  function viewValue(area, state) {
    const a = reading(area, '2017', state.indicator).value;
    const b = reading(area, '2023', state.indicator).value;
    if (state.view === 'change') {
      if (state.module !== POPULATION) return null;
      return state.changeUnit === 'percent' ? percentChange(a, b) : delta(a, b);
    }
    return state.view === '2017' ? a : b;
  }
  function aggregate(areas, year, indicator) {
    if (!areas.length) return null;
    const rows = areas.map(a => reading(a, year, indicator));
    if (rows.some(r => r.value == null)) return null;
    if (isRate(indicator)) {
      if (rows.some(r => r.numerator == null || r.denominator == null)) return null;
      const denom = rows.reduce((s,r) => s + r.denominator, 0);
      return denom ? 100 * rows.reduce((s,r) => s + r.numerator, 0) / denom : null;
    }
    return rows.reduce((s,r) => s + r.value, 0);
  }
  function normalizedState(input, data) {
    const module = Object.hasOwn(METRICS, input.module) ? input.module : POPULATION;
    const indicator = Object.hasOwn(METRICS[module], input.indicator) ? input.indicator : Object.keys(METRICS[module])[0];
    let view = ['2017','2023','change'].includes(input.view) ? input.view : '2023';
    if (module !== POPULATION && view === 'change') view = '2023';
    return {module, indicator, view, changeUnit: input.changeUnit === 'absolute' ? 'absolute' : 'percent',
      province: data.areas.some(a => a.province === input.province) ? input.province : 'ALL',
      query: String(input.query || ''), selected: data.areas.some(a => a.id === input.selected) ? input.selected : 'DDG-0130'};
  }
  function filteredAreas(data, state) {
    const q = state.query.trim().toLocaleLowerCase();
    return data.areas.filter(a => (state.province === 'ALL' || a.province === state.province) &&
      (!q || [a.name, a.id, ...a.members['2017'], ...a.members['2023'], ...a.polygon_names].join(' ').toLocaleLowerCase().includes(q)))
      .sort((a,b) => a.name.localeCompare(b.name, 'en'));
  }
  function csvRows(areas, state, release) {
    return areas.map(a => {
      const common = {comparison_id:a.id, comparison_name:a.name, province:a.province,
        module:state.module, indicator:state.indicator, view:state.view, map_status:a.map_status,
        polygon_match_certified:false, release_id:release};
      if (state.module === POPULATION) {
        const v17 = reading(a, '2017', state.indicator), v23 = reading(a, '2023', state.indicator);
        return {...common, unit:'persons', value_2017:v17.value, value_2023:v23.value,
          status_2017:v17.status, status_2023:v23.status, change_people:delta(v17.value, v23.value),
          change_percent:percentChange(v17.value, v23.value), display_value:viewValue(a,state),
          display_unit:state.view === 'change' && state.changeUnit === 'percent' ? 'percent_change' : 'persons'};
      }
      const r = reading(a, state.view, state.indicator);
      return {...common, year:state.view, value:r.value, unit:r.unit, status:r.status,
        numerator:r.numerator, denominator:r.denominator, change_enabled:false,
        definition_status:'questionnaire_equivalence_not_certified'};
    });
  }
  function toCSV(rows) {
    if (!rows.length) return '';
    const quote = v => '"' + String(v ?? '').replace(/"/g,'""') + '"';
    const keys = Object.keys(rows[0]);
    return [keys.map(quote).join(','), ...rows.map(r => keys.map(k => quote(r[k])).join(','))].join('\r\n') + '\r\n';
  }
  const API = {METRICS, delta, percentChange, viewValue, aggregate, normalizedState, filteredAreas, csvRows, toCSV};
  if (typeof module !== 'undefined' && module.exports) module.exports = API;
  if (typeof window === 'undefined' || !window.DD_CENSUS_PREVIEW) return;
  window.CensusPreview = API;
  const data = window.DD_CENSUS_PREVIEW, $ = id => document.getElementById(id);
  const params = new URLSearchParams(location.search);
  let state = normalizedState(Object.fromEntries(params), data), visible = [], colour, bins = [];
  const byId = new Map(data.areas.map(a => [a.id,a]));
  const allFeatures = [...data.geometry.features, ...data.context.features];
  // Project points individually before drawing planar rings. This avoids
  // d3's spherical ring-winding convention in older GeoJSON files.
  const mercator = d3.geoMercator().scale(1).translate([0,0]);
  const projectCoords = coords => typeof coords[0] === 'number' ? mercator(coords) : coords.map(projectCoords);
  const projected = allFeatures.map(f => ({...f, geometry:{...f.geometry, coordinates:projectCoords(f.geometry.coordinates)}}));
  const projection = d3.geoIdentity().fitExtent([[25,18],[735,630]],{type:'FeatureCollection',features:projected});
  const path = d3.geoPath(projection), svg = d3.select('#map'), layer = d3.select('#map-content');
  const zoom = d3.zoom().scaleExtent([1,18]).extent([[0,0],[760,650]])
    .on('zoom', event => { layer.attr('transform',event.transform); $('map-tooltip').hidden = true; });
  svg.call(zoom).on('dblclick.zoom', null);
  layer.selectAll('.context').data(projected.filter(f => !f.properties.comparison_id)).join('path')
    .attr('class','context').attr('d',path).append('title')
    .text(f => `${f.properties.districts} · outside this census release`);
  const paths = layer.selectAll('.area').data(projected.filter(f => f.properties.comparison_id)).join('path')
    .attr('class','area').attr('d',path).attr('role','button').attr('tabindex',0)
    .on('click', (event,f) => choose(f.properties.comparison_id))
    .on('keydown', (event,f) => {if (event.key === 'Enter' || event.key === ' ') {event.preventDefault();choose(f.properties.comparison_id);}})
    .on('pointermove', (event,f) => {
      const a=byId.get(f.properties.comparison_id), tip=$('map-tooltip'), rect=tip.parentElement.getBoundingClientRect();
      tip.innerHTML=`<strong>${esc(a.name)}</strong>${esc(formatValue(viewValue(a,state)))}${a.map_status === 'unverified_frontier_extent' ? '<br>Boundary extent unverified · figure in table' : ''}`;
      tip.hidden=false; tip.style.left=Math.max(8,Math.min(event.clientX-rect.left+12,rect.width-240))+'px';
      tip.style.top=Math.max(65,event.clientY-rect.top-70)+'px';
    }).on('pointerleave', () => {$('map-tooltip').hidden=true;});
  paths.append('title');
  for (const p of [...new Set(data.areas.map(a=>a.province))].sort()) $('province').add(new Option(p,p));
  $('scope-note').textContent=data.meta.scope+' Summary totals follow the current filters.';
  $('population-note').textContent=data.meta.population_note;
  $('education-note').textContent=data.meta.education_note;
  $('release-note').textContent=`Census release: ${data.meta.release_id}. ${data.meta.observation_count.toLocaleString()} observations; 127 statistical areas. Rebuilt from locked source files.`;

  function formatValue(value) {
    if (value == null) return '—';
    if (state.view === 'change') return state.changeUnit === 'percent' ? signed(value,1)+'%' : signed(value);
    return isRate(state.indicator) ? number(value,1)+'%' : number(value);
  }
  function label() {return METRICS[state.module][state.indicator];}
  function syncControls() {
    $('module').value=state.module; $('province').value=state.province; $('search').value=state.query;
    $('indicator').replaceChildren(...Object.entries(METRICS[state.module]).map(([key,name])=>new Option(name,key)));
    $('indicator').value=state.indicator; $('change-unit').value=state.changeUnit;
    document.querySelectorAll('[data-view]').forEach(b=>{
      b.disabled=state.module !== POPULATION && b.dataset.view === 'change';
      b.classList.toggle('active',b.dataset.view === state.view);
      b.setAttribute('aria-pressed',String(b.dataset.view === state.view));
      b.title=b.disabled?'Education definitions across years still require review.':'';
    });
    $('change-unit').hidden=$('change-unit-label').hidden=state.view !== 'change';
    $('measure-note').textContent=state.module === POPULATION
      ? 'Both years use the same comparison areas. Population differences can also reflect enumeration coverage.'
      : 'Ages 5+. Definitions across years still require review, so education is shown one year at a time.';
  }
  function renderSummary() {
    if (state.module === POPULATION) {
      const a=aggregate(visible,'2017',state.indicator), b=aggregate(visible,'2023',state.indicator);
      $('summary-label-2017').textContent=label()+' · 2017';
      $('summary-label-2023').textContent=label()+' · 2023';
      $('summary-label-change').textContent='Change · 2017 → 2023';
      $('summary-2017').textContent=a == null && visible.length?'Incomplete':number(a);
      $('summary-2023').textContent=b == null && visible.length?'Incomplete':number(b);
      const change=percentChange(a,b); $('summary-change').textContent=change == null?'—':signed(change,1)+'%';
    } else {
      const value=aggregate(visible,state.view,state.indicator);
      $('summary-label-2017').textContent=label()+' · '+state.view;
      $('summary-label-2023').textContent='Population aged 5+ · '+state.view;
      $('summary-label-change').textContent='Change comparison';
      $('summary-2017').textContent=value == null?'—':number(value,isRate(state.indicator)?1:0)+(isRate(state.indicator)?'%':'');
      $('summary-2023').textContent=number(aggregate(visible,state.view,'total'));
      $('summary-change').textContent='Under review';
    }
    $('summary-areas').textContent=String(visible.length)+' / 127';
  }
  function renderColours() {
    const values=visible.filter(a=>a.map_status !== 'unverified_frontier_extent').map(a=>viewValue(a,state)).filter(v=>v != null);
    bins=[];
    if (state.view === 'change') {
      const max=d3.max(values,v=>Math.abs(v)) || 1;
      const palette=['#9b563b','#cd987d','#eee7d7','#97b6a3','#205c43'];
      colour=d3.scaleThreshold().domain([-max*.6,-max*.2,max*.2,max*.6]).range(palette);
      const edges=[-max,-max*.6,-max*.2,max*.2,max*.6,max];
      bins=palette.map((c,i)=>({c,text:`${legendNumber(edges[i])} to ${legendNumber(edges[i+1])}`}));
    } else {
      const palette=['#e0ecdc','#b5d2b5','#7fb18e','#4a8868','#18513b'];
      colour=d3.scaleQuantile().domain(values).range(palette);
      bins=values.length?palette.map(c=>{const [lo,hi]=colour.invertExtent(c);return {c,text:`${legendNumber(lo)} – ${legendNumber(hi)}`};}):[];
    }
    $('legend-title').textContent=state.view === 'change' ? 'Change · '+(state.changeUnit === 'percent'?'%':'people') : label()+' · quintiles';
    $('legend-swatches').innerHTML=bins.length?bins.map(b=>`<p><span class="swatch" style="background:${b.c}"></span>${esc(b.text)}</p>`).join(''):'<p>No values in this view.</p>';
    const ids=new Set(visible.map(a=>a.id));
    paths.attr('display',f=>ids.has(f.properties.comparison_id)?null:'none')
      .attr('tabindex',f=>ids.has(f.properties.comparison_id)?0:-1)
      .attr('fill',f=>{const a=byId.get(f.properties.comparison_id), v=viewValue(a,state);return a.map_status === 'unverified_frontier_extent'?'url(#incomplete)':v == null?'#dedee6':colour(v);})
      .classed('selected',f=>f.properties.comparison_id === state.selected)
      .attr('aria-pressed',f=>String(f.properties.comparison_id === state.selected))
      .attr('aria-label',f=>{const a=byId.get(f.properties.comparison_id);return `${a.name}: ${formatValue(viewValue(a,state))}${a.map_status === 'unverified_frontier_extent' ? '. Boundary extent unverified' : ''}`;});
    paths.select('title').text(f=>{const a=byId.get(f.properties.comparison_id);return `${a.name} · ${formatValue(viewValue(a,state))}`;});
    $('map-title').textContent=label()+' · '+(state.view === 'change'?'2017 → 2023':state.view);
    const mapped=visible.filter(a=>a.map_status !== 'unverified_frontier_extent').length;
    $('map-subtitle').textContent=`${mapped} illustrative outlines · ${visible.length-mapped} unverified extents`;
  }
  function legendNumber(v) {
    if (!Number.isFinite(v)) return '—';
    if (isRate(state.indicator) || (state.view === 'change' && state.changeUnit === 'percent')) return number(v,1)+'%';
    return Math.abs(v)>=1000000?number(v/1000000,1)+'m':Math.abs(v)>=1000?number(v/1000,1)+'k':number(v);
  }
  function renderDetail() {
    const a=visible.find(a=>a.id === state.selected);
    if (!a) {$('area-detail').innerHTML='<p class="eyebrow">AREA DETAILS</p><h2>No matching areas</h2><p class="explanation">Try another district name or reset the filters.</p>';return;}
    const combined=a.relationship === 'aggregated';
    let html=`<p class="eyebrow">${esc(a.id)}</p><span class="tag">${combined?'Combined comparison area':'Comparison area'}</span><h2>${esc(a.name)}</h2><p class="province">${esc(a.province)}</p><h3>${esc(label())}</h3>`;
    if (state.module === POPULATION) {
      const v17=reading(a,'2017',state.indicator), v23=reading(a,'2023',state.indicator), pct=percentChange(v17.value,v23.value);
      html+=`<div class="detail-values"><div><span>2017 · people</span><strong>${number(v17.value)}</strong></div><div><span>2023 · people</span><strong>${number(v23.value)}</strong></div><div class="wide"><span>Change · 2017 → 2023</span><strong>${pct == null?'—':signed(pct,1)+'%'}</strong><span>${signed(delta(v17.value,v23.value))} people</span></div></div>`;
      if (v17.value == null || v23.value == null) html+='<p class="notice">A source component is missing. The blank is preserved, and change is not calculated.</p>';
    } else {
      const r=reading(a,state.view,state.indicator);
      html+=`<div class="detail-values"><div class="wide"><span>${state.view} · ${r.unit === 'percent'?'percent of people aged 5+':'people aged 5+'}</span><strong>${formatValue(r.value)}</strong></div></div>`;
      if (isRate(state.indicator)) html+=`<p class="explanation">${number(r.numerator)} people ÷ ${number(r.denominator)} people aged 5+. Rates are calculated from summed counts.</p>`;
      html+='<p class="notice">Education change comparisons are unavailable while questionnaire definitions are under review.</p>';
    }
    if (combined) html+='<p class="notice">Counted once as a combined area in each year. These values cannot be assigned separately to its member districts.</p>';
    if (['DDG-0130','DDG-0131'].includes(a.id)) html+='<p class="explanation">Individual historical allocations remain uncertain. Published district population baselines are retained in a <a href="downloads/district_population_bridge.csv" download>separate download</a>.</p>';
    if (a.map_status === 'unverified_frontier_extent') html+=`<p class="notice">${esc(a.map_note)} The table and totals include the complete statistical area.</p>`;
    html+='<details><summary>Which places make up this area?</summary>';
    for (const year of ['2017','2023']) html+=`<p><span class="member-year">${year}</span><br>${a.members[year].map(esc).join('<br>')}</p>`;
    html+='</details><details open><summary>Source tables</summary>';
    const years=state.module === POPULATION?['2017','2023']:[state.view];
    for (const year of years) {
      html+=`<p class="member-year">${year} · ${state.module === POPULATION?'Population':'Education'}</p><ul class="source-list">`;
      for (const s of a.sources[year][state.module]) {
        const page=s.locator.match(/PDF page (\d+)/)?.[1] || '1';
        html+=`<li><a href="${esc(s.archived)}#page=${page}" target="_blank" rel="noopener">${esc(s.name)} · archived PDF ↗</a><small>${esc(s.locator)} · <a href="${esc(s.url)}" target="_blank" rel="noopener">PBS publication link</a></small></li>`;
      }
      html+='</ul>';
    }
    html+='<p class="explanation">Archived PDFs are checksum-verified copies. Some older PBS publication links may no longer work.</p></details>';
    $('area-detail').innerHTML=html;
  }
  function renderTable() {
    const pop=state.module === POPULATION;
    const headers=pop?['Comparison area','Province / territory','2017 · people','2023 · people','Change · people','Change · %']:['Comparison area','Province / territory',`${state.view} · ${isRate(state.indicator)?'%':'people'}`,'Source status'];
    $('results').tHead.innerHTML='<tr>'+headers.map(h=>`<th scope="col">${esc(h)}</th>`).join('')+'</tr>';
    $('results').tBodies[0].innerHTML=visible.map(a=>{
      const start=`<tr class="${a.id === state.selected?'selected':''}"><td><button type="button" data-area="${a.id}">${esc(a.name)}</button>${a.map_status === 'unverified_frontier_extent'?'<span class="map-status">Boundary extent unverified · complete statistical area</span>':''}</td><td>${esc(a.province)}</td>`;
      if (!pop) {const r=reading(a,state.view,state.indicator);return start+`<td>${formatValue(r.value)}</td><td>${r.value == null?'Missing component':'Published category'}</td></tr>`;}
      const a17=reading(a,'2017',state.indicator).value,a23=reading(a,'2023',state.indicator).value,pct=percentChange(a17,a23);
      return start+`<td>${number(a17)}</td><td>${number(a23)}</td><td>${signed(delta(a17,a23))}</td><td>${pct == null?'—':signed(pct,1)+'%'}</td></tr>`;
    }).join('') || `<tr><td class="empty" colspan="${headers.length}">No matching areas. Try a different search or reset the filters.</td></tr>`;
    $('table-description').textContent=`${visible.length} comparison areas · ${label()}. ${pop?'Blank source values stay blank; change is withheld when either year is missing.':'One census year at a time; education definitions across years remain under review.'}`;
    $('download-view').disabled=!visible.length;
  }
  function writeURL() {
    const url=new URL(location.href);url.search=new URLSearchParams(Object.entries(state).filter(([k,v])=>v !== '' && !(k === 'province' && v === 'ALL'))).toString();
    try {history.replaceState(null,'',url);} catch (_) { /* file:// previews may disallow history replacement. */ }
  }
  function render(fit=false) {
    state=normalizedState(state,data);visible=filteredAreas(data,state);
    if (visible.length && !visible.some(a=>a.id === state.selected)) state.selected=visible[0].id;
    syncControls();renderSummary();renderColours();renderDetail();renderTable();writeURL();
    if (fit) fitView();
  }
  function fitView() {
    const ids=new Set(visible.map(a=>a.id));
    const features=state.province === 'ALL' && !state.query.trim()?projected:projected.filter(f=>ids.has(f.properties.comparison_id));
    if (!features.length) {svg.call(zoom.transform,d3.zoomIdentity);return;}
    const [[x0,y0],[x1,y1]]=path.bounds({type:'FeatureCollection',features});
    const k=Math.min(15,.9/Math.max((x1-x0)/760,(y1-y0)/650));
    svg.call(zoom.transform,d3.zoomIdentity.translate(380,325).scale(Math.max(1,k)).translate(-(x0+x1)/2,-(y0+y1)/2));
  }
  function choose(id) {state.selected=id;render();}
  $('module').addEventListener('change',()=>{state.module=$('module').value;state.indicator=Object.keys(METRICS[state.module])[0];render();});
  $('indicator').addEventListener('change',()=>{state.indicator=$('indicator').value;render();});
  $('province').addEventListener('change',()=>{state.province=$('province').value;render(true);});
  $('search').addEventListener('input',()=>{state.query=$('search').value;render(true);});
  $('change-unit').addEventListener('change',()=>{state.changeUnit=$('change-unit').value;render();});
  document.querySelectorAll('[data-view]').forEach(b=>b.addEventListener('click',()=>{state.view=b.dataset.view;render();}));
  $('reset').addEventListener('click',()=>{state=normalizedState({},data);render(true);});
  $('results').addEventListener('click',event=>{const b=event.target.closest('[data-area]');if(b){choose(b.dataset.area);if(innerWidth<=1100)$('area-detail').scrollIntoView({block:'start'});}});
  $('zoom-in').addEventListener('click',()=>svg.call(zoom.scaleBy,1.5));
  $('zoom-out').addEventListener('click',()=>svg.call(zoom.scaleBy,1/1.5));
  $('map-reset').addEventListener('click',fitView);
  $('download-view').addEventListener('click',()=>{
    const csv=toCSV(csvRows(visible,state,data.meta.release_id));
    const url=URL.createObjectURL(new Blob(['\ufeff',csv],{type:'text/csv;charset=utf-8'}));
    const a=document.createElement('a');a.href=url;a.download=`data-darbar-${state.module}-${state.indicator}-${state.view}.csv`;a.click();
    setTimeout(()=>URL.revokeObjectURL(url),1000);
  });
  window.addEventListener('popstate',()=>{state=normalizedState(Object.fromEntries(new URLSearchParams(location.search)),data);render(true);});
  render(true);
})();
