/* Run the actual preview UI in a DOM, without a browser or network. */
const fs = require('node:fs');
const path = require('node:path');
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { JSDOM } = require('jsdom');
const root = path.resolve(__dirname,'../../../local_preview/census');
const { aggregate, percentChange, normalizedState, csvRows, filteredAreas, viewValue, toCSV } = require('./preview.js');
const data = JSON.parse(fs.readFileSync(path.join(root,'census-preview-data.js'),'utf8').replace(/^window.DD_CENSUS_PREVIEW=/,'').replace(/;\n$/,''));
const state = extras => normalizedState(extras || {}, data);

function ui(query='') {
  const dom = new JSDOM(fs.readFileSync(path.join(root,'index.html'),'utf8'), {
    runScripts:'outside-only',pretendToBeVisual:true,url:'https://local-preview.invalid/'+query
  });
  for (const name of ['d3.v7.min.js','census-preview-data.js','preview.js']) dom.window.eval(fs.readFileSync(path.join(root,name),'utf8'));
  return dom;
}
function change(dom,id,value,type='change') {
  const el=dom.window.document.getElementById(id);el.value=value;
  el.dispatchEvent(new dom.window.Event(type,{bubbles:true}));
}

test('national totals agree across filtered table and export; joint districts counted once',()=>{
  const areas=filteredAreas(data,state());
  const rows=csvRows(areas,state(),data.meta.release_id);
  assert.equal(rows.length,127);
  assert.equal(rows.reduce((s,r)=>s+r.value_2017,0),207684626);
  assert.equal(rows.reduce((s,r)=>s+r.value_2023,0),241499431);
  assert.equal(rows.filter(r=>r.comparison_id==='DDG-0130').length,1);
  assert.equal(rows.filter(r=>r.comparison_id==='DDG-0131').length,1);
});
test('missing baselines and zero denominators never become invented growth',()=>{
  assert.equal(percentChange(null,15),null);assert.equal(percentChange(0,15),null);
  assert.equal(percentChange(100,0),-100);
  assert.equal(aggregate(data.areas,'2017','pop_transgender'),null);
});
test('education aggregate is a ratio of counts, not an average of district percentages',()=>{
  const areas=[1,99].map(n=>({values:{'2023':{pct_matric_plus:{value:n===1?100:0,numerator:n===1?1:0,denominator:n}}}}));
  assert.equal(aggregate(areas,'2023','pct_matric_plus'),1);
});
test('education changes cannot be enabled by URL or function call',()=>{
  const s=state({module:'education',view:'change'});
  assert.equal(s.view,'2023');
  assert.equal(viewValue(data.areas[0],{...s,view:'change'}),null);
  assert.ok(csvRows(data.areas,s,data.meta.release_id).every(r=>r.change_enabled===false && !Object.hasOwn(r,'change_percent')));
});
test('search finds joint areas through either member, not duplicate rows',()=>{
  for(const q of ['Jhang','Toba Tek Singh','Kachhi','Nasirabad','Keamari','Upper Chitral']) {
    assert.equal(filteredAreas(data,state({query:q})).length,1,q);
  }
});
test('CSV retains full precision and blank missing cells',()=>{
  const rows=csvRows(filteredAreas(data,state({query:'Kharan',indicator:'pop_transgender'})),state({indicator:'pop_transgender'}),data.meta.release_id);
  assert.equal(rows[0].value_2017,null);assert.equal(rows[0].change_percent,null);
  assert.ok(toCSV(rows).includes('"missing_source_component"'));
  assert.ok(!toCSV(rows).includes('NaN'));
});
test('real UI renders all areas, correct totals, and six hatched outlines',()=>{
  const dom=ui(),d=dom.window.document;
  assert.equal(d.querySelectorAll('#results tbody tr').length,127);
  assert.equal(d.getElementById('summary-2017').textContent,'207,684,626');
  assert.equal(d.getElementById('summary-2023').textContent,'241,499,431');
  assert.equal(d.querySelectorAll('path.area').length,127);
  assert.equal(d.querySelectorAll('path.area[fill="url(#incomplete)"]').length,6);
  assert.equal(d.querySelectorAll('path.area.selected').length,1);
  assert.ok(d.getElementById('area-detail').textContent.includes('Jhang + Toba Tek Singh'));
  assert.ok([...d.querySelectorAll('path.area')].every(p=>p.getAttribute('d') && !p.getAttribute('d').includes('NaN')));
  dom.window.close();
});
test('actual controls update map, summary, detail, table, and export together',()=>{
  const dom=ui(),d=dom.window.document;
  change(dom,'search','Nasirabad','input');
  assert.equal(d.querySelectorAll('#results tbody tr').length,1);
  assert.equal(d.getElementById('summary-2017').textContent,'797,779');
  assert.equal(d.getElementById('summary-2023').textContent,'1,005,989');
  assert.ok(d.getElementById('area-detail').textContent.includes('Counted once'));
  d.querySelector('[data-view="change"]').click();
  assert.ok(d.getElementById('map-title').textContent.includes('2017 → 2023'));
  change(dom,'change-unit','absolute');
  assert.ok(d.querySelector('path.area.selected').getAttribute('aria-label').includes('+208,210'));
  let blob, filename;
  dom.window.URL.createObjectURL=b=>{blob=b;return 'blob:test'};
  dom.window.URL.revokeObjectURL=()=>{};
  dom.window.HTMLAnchorElement.prototype.click=function(){filename=this.download};
  d.getElementById('download-view').click();
  assert.ok(blob);assert.equal(filename,'data-darbar-population-pop_total-change.csv');
  dom.window.close();
});
test('switching from change to education removes every change value and disables toggle',()=>{
  const dom=ui('?view=change'),d=dom.window.document;
  change(dom,'module','education');
  assert.ok(d.querySelector('[data-view="change"]').disabled);
  assert.ok(d.querySelector('[data-view="2023"]').classList.contains('active'));
  assert.equal(d.querySelectorAll('#results thead th').length,4);
  assert.ok(!d.getElementById('results').tHead.textContent.includes('2017'));
  assert.equal(d.getElementById('summary-change').textContent,'Under review');
  assert.ok(d.getElementById('area-detail').textContent.includes('questionnaire definitions'));
  dom.window.close();
});
test('province filters and empty search do not leave stale values or selected areas',()=>{
  const dom=ui(),d=dom.window.document;
  change(dom,'province','Sindh');
  assert.equal(d.querySelectorAll('#results tbody tr').length,29);
  assert.ok(d.getElementById('area-detail').textContent.includes('Sindh'));
  change(dom,'search','no matching place','input');
  assert.equal(d.getElementById('summary-areas').textContent,'0 / 127');
  assert.ok(d.getElementById('download-view').disabled);
  assert.ok(d.getElementById('area-detail').textContent.includes('No matching areas'));
  d.getElementById('reset').click();
  assert.equal(d.querySelectorAll('#results tbody tr').length,127);
  dom.window.close();
});
test('keyboard activation on a map area selects its details',()=>{
  const dom=ui(),d=dom.window.document;
  const p=[...d.querySelectorAll('path.area')].find(p=>p.getAttribute('aria-label').startsWith('Kachhi + Nasirabad'));
  p.dispatchEvent(new dom.window.KeyboardEvent('keydown',{key:'Enter',bubbles:true}));
  assert.ok(p.classList.contains('selected'));
  assert.ok(d.getElementById('area-detail').textContent.includes('1,005,989'));
  dom.window.close();
});
test('sources and downloads resolve offline without a remote runtime dependency',()=>{
  const dom=ui(),d=dom.window.document;
  for(const el of d.querySelectorAll('script[src],link[rel="stylesheet"][href],a[href^="sources/"],a[href^="downloads/"]')) {
    const ref=el.getAttribute('src')||el.getAttribute('href');
    assert.ok(!ref.startsWith('http'));assert.ok(fs.existsSync(path.join(root,ref.split('#')[0])),ref);
  }
  dom.window.close();
});
