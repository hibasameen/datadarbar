/* Data Darbar — Economy & Budget: interactive structure-of-the-economy dashboard.
   D3 v7 + window.ECON.{structure,industry,budget,indicators,io}. Linked views:
   selYear (year cursor) and selSector (macro focus) drive the top panels. */
const fmtBn=v=>v==null?'—':(Math.abs(v)>=1000?(v/1000).toFixed(v>=10000?0:1)+' tn':Math.round(v).toLocaleString()+' bn');
const fmtRs=v=>v==null?'—':'Rs '+fmtBn(v);
const fmtPct=v=>v==null?'—':(v>=0?'+':'')+v.toFixed(1)+'%';
const tip=d3.select('#tip');
const showTip=(h,e)=>{tip.html(h).style('opacity',1);moveTip(e);};
const moveTip=e=>{const p=12;let x=e.clientX+p,y=e.clientY+p,w=tip.node().offsetWidth,hh=tip.node().offsetHeight;if(x+w>innerWidth)x=e.clientX-w-p;if(y+hh>innerHeight)y=e.clientY-hh-p;tip.style('left',x+'px').style('top',y+'px');};
const hideTip=()=>tip.style('opacity',0);
const fyEnd=y=>+y.slice(0,4)+1;          // '1951-52' -> 1952
const fyLbl=n=>`${n-1}-${String(n).padStart(4,'0').slice(2)}`; // 1952 -> '1951-52'

const MACRO={agri:{label:'Agriculture',c:'#5b8c5a'},ind:{label:'Industry',c:'#e07b39'},serv:{label:'Services',c:'#3d6db5'}};
const ERAS=[[1952,1958,'Early years'],[1958,1969,'Ayub industrialisation'],[1969,1977,'War & nationalisation'],[1977,1988,'Zia decade'],[1988,1999,'Adjustment years'],[1999,2008,'Musharraf boom'],[2008,2013,'Energy crisis'],[2013,2020,'CPEC era'],[2020,2022,'COVID'],[2022,2026,'Squeeze & stabilisation']];

const LSM_SHORT={'QIM':'QIM (overall)','Manufacturing of Food':'Food','Manufacturing of Beverages':'Beverages','Manufacturing of Tobacco':'Tobacco','Manufacturing of Textile':'Textiles','Manufacture of wearing apparel':'Wearing apparel','Manufacturing of Leather Products':'Leather','Manufacturing of Wood Products':'Wood','Manufacturing of Paper & Board':'Paper & board','Manufacturing of Coke & Petroleum Products':'Petroleum products','Manufacturing of Chemicals':'Chemicals','Manufacturing of Pharmaceuticals Products':'Pharmaceuticals','Manufacturing of Rubber Products':'Rubber','Manufacturing of Non Metalic Mineral Products':'Cement & minerals','Manufacturing of Iron & Steel Products':'Iron & steel','Manufacture of Fabricated Metal':'Fabricated metal','Manufacture of Computer, electronics and Optical products':'Electronics & optics','Manufacture of Electrical Equipment':'Electrical equipment','Manufacture of Machinery and  Equipment n.e.c':'Machinery','Manufacturing of Automobiles':'Automobiles','Manufacture of other transport  Equipment':'Other transport','Manufacture of furniture':'Furniture','Other manufacturing':'Other (footballs)'};
const OLD2NEW={'Textile':'Manufacturing of Textile','Pharmaceuticals':'Manufacturing of Pharmaceuticals Products','Chemicals':'Manufacturing of Chemicals','Automobiles':'Manufacturing of Automobiles','Iron & Steel Products':'Manufacturing of Iron & Steel Products','Coke & Petroleum Products':'Manufacturing of Coke & Petroleum Products','Leather Products':'Manufacturing of Leather Products','Rubber Products':'Manufacturing of Rubber Products','Wood Products':'Manufacturing of Wood Products','Non Metalic Mineral Products':'Manufacturing of Non Metalic Mineral Products','Paper & Board':'Manufacturing of Paper & Board','Electronics':'Manufacture of Electrical Equipment','QIM':'QIM'};
const LSM_PALETTE=['#0c3a1e','#c0392b','#3d6db5','#d4a017','#9b59b6','#e07b39','#16a085','#2c3e8f','#a0522d','#5b8c5a','#d98880','#0e8a8a','#6d4c2b','#7f8c8d','#b8941a','#c39bd3','#34495e','#e8b92e','#8e44ad','#1e6b3e','#5d6d7e','#95a5a6','#17a2b8'];

/* ---- topics (sidebar navigation, one topic at a time; #hash deep links) ---- */
const TOPICS=[
 {k:'all',label:'Everything',
  desc:'Every topic on one page, top to bottom — the full picture.',
  meta:'All sources are listed at the foot of the page and under About → Sources.'},
 {k:'structure',label:'Structural change',
  title:"The structure of Pakistan's economy",
  lede:'In 1952 agriculture produced roughly half of everything Pakistan made; today it is under a quarter, and services produce more than half. Trace that shift, then read off the economy’s mix in any single year.',
  desc:'Seven decades of sector shares and the economy’s composition in any year.',
  meta:'PBS national accounts (2015-16 base): Macro Economic Indicators 1951-52→, Sectoral Shares (Table 7b). Pre-1999 shares are backcast — indicative.'},
 {k:'growth',label:'Composition of growth',
  title:'The composition of Pakistan’s growth',
  lede:'Headline growth is the sum of its parts. This splits each year’s real GDP growth into the percentage points each sector supplied — so you can see which sectors carried the economy and which dragged on it.',
  desc:'Each year’s GDP growth decomposed into the percentage points contributed by each sector.',
  meta:'Contribution = previous-year share of GVA × this-year real growth. PBS Tables 6 (real growth) and 7b (shares), 2000-01→2025-26.'},
 {k:'industry',label:'Industry & factories',
  title:'Inside industry: what Pakistan’s factories make',
  lede:'Large-scale manufacturing is a fifth of industry and the part measured monthly. Two decades of sector production indices, and the monthly quantum index that tracks them.',
  desc:'Large-scale manufacturing: annual sector production indices and the monthly quantum index.',
  meta:'PBS Quantum Index of Manufacturing (QIM), base 2015-16, spliced to the 2005-06-base series. Latest month: May 2026.'},
 {k:'censuses',label:'Manufacturing censuses',
  title:'What the manufacturing censuses show',
  lede:'Roughly once a decade PBS counts every registered factory. Comparing the 2005-06 and 2015-16 rounds shows which industries employ, produce and proliferate — and how much textiles receded.',
  desc:'Employment, establishments and value added by industry, 2005-06 vs 2015-16.',
  meta:'PBS CMI 2005-06 and 2015-16 reports. Shares of all manufacturing, for comparability across rounds.'},
 {k:'linkages',label:'Sector linkages',
  title:'How Pakistan’s sectors feed each other',
  lede:'No sector stands alone: agriculture supplies food processing, refineries supply transport. The 2015-16 input-output table records every rupee of output used as an input elsewhere.',
  desc:'Who buys from whom — the 2015-16 input-output flows between 12 broad sectors.',
  meta:'PBS Supply-Use & Input-Output Tables 2015-16 — the only IO table PBS has published.'},
 {k:'budget',label:'State & budget',
  title:'The state’s own ledger',
  lede:'Where the federal government’s money comes from and where current spending goes, budget year by budget year.',
  desc:'The federal budget as a treemap: receipts and current expenditure.',
  meta:'Finance Division: Budget in Brief & Explanatory Memorandum on Federal Receipts, FY2009-10→2026-27.'}];
const TOPIC_DRAWS={structure:()=>{drawArc();drawMix();drawSGrowth();},growth:()=>{drawContrib();drawCYear();drawCEras();},industry:()=>{drawLsm();drawQim();drawWeights();},censuses:()=>drawCmi(),linkages:()=>drawIO(),budget:()=>redraw()};
// meaningful groupings for the sidebar (plus an "Everything" shortcut at the top)
const TOPIC_GROUPS=[
 {label:null,keys:['all']},
 {label:'Growth & structure',keys:['structure','growth']},
 {label:'Manufacturing',keys:['industry','censuses']},
 {label:'Linkages & the state',keys:['linkages','budget']}];
const drawAll=()=>Object.values(TOPIC_DRAWS).forEach(f=>f());
let topic='structure';

let E,ST,IND;
let selYear,selSector='all',arcYears=[],arcData=[],shareYears=[],lsmSeries={},lsmColors={},lsmSel;
function start(){
 if(!window.ECON){return setTimeout(start,30);}
 E=window.ECON;ST=E.structure;IND=E.industry;
 prepData();
 initArc();initMix();drawSGrowth();initContrib();
 initLsm();drawQim();drawWeights();initCmi();
 initIO();initBudget();initCsv();
 initTopics();
 let rt;window.addEventListener('resize',()=>{clearTimeout(rt);rt=setTimeout(()=>{if(topic==='all')drawAll();else if(TOPIC_DRAWS[topic])TOPIC_DRAWS[topic]();},150);});
}
function initTopics(){
 // grouped topic list (replaces the old dropdown + duplicate list)
 const list=d3.select('#topicList');list.selectAll('*').remove();
 TOPIC_GROUPS.forEach((g,gi)=>{
  if(g.label)list.append('div').attr('class','topic-group'+(gi===0?' first':'')).text(g.label);
  g.keys.forEach(k=>{
   const t=TOPICS.find(x=>x.k===k);if(!t)return;
   list.append('button').attr('class','topic-item'+(k==='all'?' all':'')).attr('data-k',k)
    .html(`<span class="t-dot"></span>${t.label}`)
    .on('click',()=>applyTopic(k,true));
  });
 });
 d3.select('#sectorSelect').on('change',function(){setSector(this.value);});
 window.addEventListener('hashchange',()=>{
  const k=location.hash.replace('#','');
  if(k!==topic&&TOPICS.some(t=>t.k===k))applyTopic(k,false);
 });
 const h=location.hash.replace('#','');
 applyTopic(TOPICS.some(t=>t.k===h)?h:'structure',false);
}
function applyTopic(k,push){
 topic=k;
 const t=TOPICS.find(x=>x.k===k);
 d3.selectAll('#topicList .topic-item').classed('on',function(){return this.dataset.k===k;});
 d3.select('#topicDesc').text(t.desc);
 d3.select('#topicMeta').html('<b>Sources.</b> '+t.meta);
 d3.select('#sideStructCtl').style('display',(k==='structure'||k==='all')?null:'none');
 const all=k==='all';
 d3.selectAll('[data-topic]').classed('topic-hidden',function(){return !all&&this.dataset.topic!==k;});
 if(push){if(history.pushState)history.pushState(null,'','#'+k);else location.hash=k;}
 if(all)drawAll();else if(TOPIC_DRAWS[k])TOPIC_DRAWS[k]();
 if(push)window.scrollTo({top:0,behavior:'smooth'});
}

/* ================= data prep ================= */
function prepData(){
 shareYears=ST.shares.agri.map(p=>p.year);
 const sh=k=>{const m={};(ST.shares[k]||[]).forEach(p=>m[p.year]=p.value);return m;};
 const bc=k=>{const m={};(ST.backcast[k]||[]).forEach(p=>m[p.year]=p.value);return m;};
 const A=sh('agri'),I=sh('ind'),S=sh('serv'),bA=bc('agri'),bS=bc('serv');
 const backYears=ST.backcast.agri.map(p=>p.year);
 arcYears=backYears.concat(shareYears);
 arcData=arcYears.map(y=>{
  if(A[y]!=null)return {year:y,n:fyEnd(y),agri:A[y],ind:I[y],serv:S[y],back:false};
  const a=bA[y],s=bS[y];return {year:y,n:fyEnd(y),agri:a,ind:Math.max(0,100-a-s),serv:s,back:true};
 });
 selYear=shareYears[shareYears.length-1];
 // ---- LSM spliced series ----
 const NB=IND.new_base,OB=IND.old_base;
 const old15={};Object.entries(OB['2015-16']||{}).forEach(([k,v])=>{if(v.annual)old15[k]=v.annual;});
 const canon=IND.canonical.filter(c=>LSM_SHORT[c]);
 canon.forEach((c,i)=>lsmColors[c]=LSM_PALETTE[i%LSM_PALETTE.length]);
 canon.forEach(c=>{
  const pts=[];
  Object.keys(OB).sort().forEach(fy=>{
   if(fyEnd(fy)>2016)return; // old base only before the overlap
   const oldName=Object.keys(OLD2NEW).find(o=>OLD2NEW[o]===c);
   if(!oldName||!OB[fy][oldName]||OB[fy][oldName].annual==null||!old15[oldName])return;
   if(fy==='2015-16')return; // new base covers it exactly (=100)
   pts.push({fy,n:fyEnd(fy),v:OB[fy][oldName].annual*100/old15[oldName],linked:true});
  });
  Object.keys(NB).sort().forEach(fy=>{
   const d=NB[fy][c];if(d&&d.annual!=null)pts.push({fy,n:fyEnd(fy),v:d.annual,linked:false});
  });
  if(pts.length)lsmSeries[c]=pts;
 });
 lsmSel=new Set(['QIM','Manufacturing of Textile','Manufacture of wearing apparel','Manufacturing of Automobiles','Manufacturing of Pharmaceuticals Products'].filter(c=>lsmSeries[c]));
}
function lastPt(arr){return arr[arr.length-1];}

/* ================= KPIs (per topic) ================= */

/* ================= 1. arc: stacked shares 1952-2026 ================= */
function initArc(){
 const lg=d3.select('#arcLegend');
 const items=[['all','Whole economy','#17301f']].concat(Object.entries(MACRO).map(([k,v])=>[k,v.label,v.c]));
 lg.selectAll('.li').data(items).join('div').attr('class','li')
  .html(d=>`<span class="sw" style="background:${d[2]}"></span>${d[1]}`)
  .on('click',(e,d)=>setSector(d[0]));
 drawArc();
}
function setSector(k){selSector=k;d3.select('#sectorSelect').property('value',k);drawArc();drawMix();drawSGrowth();}
function setYear(y){selYear=y;d3.select('#selYearLbl').text(y);syncSlider();drawArc();drawMix();drawSGrowth();}
function drawArc(){
 const el=d3.select('#arc');el.selectAll('*').remove();
 const W=el.node().clientWidth||1100,H=Math.max(250,Math.min(320,W*0.28)),m={t:26,r:14,b:26,l:40};
 const x=d3.scaleLinear().domain([arcData[0].n,lastPt(arcData).n]).range([m.l,W-m.r]);
 const y=d3.scaleLinear().domain([0,100]).range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 // era bands
 ERAS.forEach((e,i)=>{
  const x0=x(Math.max(e[0],arcData[0].n)),x1=x(Math.min(e[1],lastPt(arcData).n));
  if(x1<=x0)return;
  if(i%2)svg.append('rect').attr('x',x0).attr('y',m.t).attr('width',x1-x0).attr('height',H-m.t-m.b).attr('fill','#17301f').attr('opacity',.045);
  if(x1-x0>55)svg.append('text').attr('class','era-lbl').attr('x',(x0+x1)/2).attr('y',m.t-8).attr('text-anchor','middle').text(e[2]);
 });
 const keys=['agri','ind','serv'];
 const stack=d3.stack().keys(keys)(arcData);
 const area=d3.area().x(d=>x(d.data.n)).y0(d=>y(d[0])).y1(d=>y(d[1])).curve(d3.curveMonotoneX);
 svg.selectAll('path.band').data(stack).join('path').attr('class','band').attr('d',area)
  .attr('fill',(d,i)=>MACRO[keys[i]].c)
  .attr('opacity',(d,i)=>selSector==='all'||selSector===keys[i]?.92:.25)
  .style('cursor','pointer')
  .on('click',(e,d)=>{const k=keys[stack.indexOf(d)];setSector(selSector===k?'all':k);e.stopPropagation();})
  .on('mousemove',function(e,d){
    const k=keys[stack.indexOf(d)];const n=Math.round(x.invert(e.offsetX));
    const row=arcData.reduce((a,b)=>Math.abs(b.n-n)<Math.abs(a.n-n)?b:a);
    showTip(`<b>${MACRO[k].label}</b> · ${row.year}${row.back?' (backcast)':''}<br>${row[k].toFixed(1)}% of GDP`,e);
  }).on('mouseleave',hideTip);
 // backcast hatch
 const bEnd=arcData.filter(d=>d.back);
 if(bEnd.length){
  const defs=svg.append('defs');
  defs.append('pattern').attr('id','hatch').attr('width',6).attr('height',6).attr('patternUnits','userSpaceOnUse').attr('patternTransform','rotate(45)')
   .append('line').attr('y2',6).attr('stroke','#faf7ef').attr('stroke-width',1.1).attr('opacity',.55);
  svg.append('rect').attr('x',x(bEnd[0].n)).attr('y',m.t).attr('width',x(lastPt(bEnd).n)-x(bEnd[0].n)).attr('height',H-m.t-m.b).attr('fill','url(#hatch)').attr('pointer-events','none');
  svg.append('line').attr('x1',x(1999.5)).attr('x2',x(1999.5)).attr('y1',m.t).attr('y2',H-m.b).attr('stroke','#faf7ef').attr('stroke-width',1.5).attr('pointer-events','none');
 }
 // axes
 svg.append('g').attr('transform',`translate(0,${H-m.b})`).attr('class','axis').call(d3.axisBottom(x).ticks(Math.min(15,Math.floor(W/85))).tickFormat(n=>fyLbl(n)));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(5).tickFormat(d=>d+'%'));
 // in-band labels (latest values)
 const lastRow=lastPt(arcData);let acc=0;
 keys.forEach(k=>{const mid=acc+lastRow[k]/2;acc+=lastRow[k];
  svg.append('text').attr('x',W-m.r-6).attr('y',y(mid)).attr('text-anchor','end').attr('font-size',11.5).attr('font-weight',800).attr('fill','#fff').attr('pointer-events','none')
   .text(`${MACRO[k].label} ${lastRow[k].toFixed(0)}%`);});
 // year cursor + drag
 const cx=x(fyEnd(selYear));
 svg.append('line').attr('class','cursor-line').attr('x1',cx).attr('x2',cx).attr('y1',m.t-4).attr('y2',H-m.b).attr('pointer-events','none');
 svg.append('text').attr('x',cx).attr('y',H-6).attr('text-anchor','middle').attr('font-size',10.5).attr('font-weight',800).attr('fill','var(--green-900)').text(selYear).attr('pointer-events','none');
 const pick=e=>{const n=Math.round(x.invert(d3.pointer(e,svg.node())[0]));
  const row=arcData.reduce((a,b)=>Math.abs(b.n-n)<Math.abs(a.n-n)?b:a);setYear(row.year);};
 svg.call(d3.drag().on('start drag',pick));
 svg.on('click',pick);
}

/* ================= 2a. mix in selected year ================= */
const SUBS=[
 ['crops','agri'],['livestock','agri'],['agri_o','agri'],
 ['lsm','ind'],['ssm','ind'],['mfg_o','ind'],['constr','ind'],['mining','ind'],['utilities','ind'],
 ['trade','serv'],['transport','serv'],['finance','serv'],['realestate','serv'],['ict','serv'],['public','serv'],['education','serv'],['health','serv'],['serv_o','serv']];
const SUB_LBL={crops:'Crops',livestock:'Livestock',agri_o:'Forestry & fishing',lsm:'Large-scale manufacturing',ssm:'Small-scale manufacturing',mfg_o:'Slaughtering & ginning',constr:'Construction',mining:'Mining & quarrying',utilities:'Electricity, gas & water',trade:'Wholesale & retail trade',transport:'Transport & storage',finance:'Finance & insurance',realestate:'Real estate',ict:'Information & communication',public:'Public administration',education:'Education',health:'Health & social work',serv_o:'Other private services'};
function initMix(){
 const s=d3.select('#yrSlider');
 s.attr('min',0).attr('max',arcYears.length-1).property('value',arcYears.indexOf(selYear));
 s.on('input',function(){setYear(arcYears[+this.value]);});
 d3.select('#selYearLbl').text(selYear);syncSlider();drawMix();
}
function syncSlider(){d3.select('#yrSlider').property('value',arcYears.indexOf(selYear));d3.select('#yrSliderLbl').text(selYear);}
function mixRows(year){
 const val=k=>{const p=(ST.shares[k]||[]).find(q=>q.year===year);return p?p.value:null;};
 if(val('agri')==null){ // backcast years: 3-way split only
  const row=arcData.find(d=>d.year===year);
  return [{k:'agri',lbl:'Agriculture',v:row.agri,parent:'agri'},{k:'ind',lbl:'Industry (residual)',v:row.ind,parent:'ind'},{k:'serv',lbl:'Services',v:row.serv,parent:'serv'}];
 }
 const A=val('agri'),M=val('mfg'),S=val('serv');
 const base={crops:val('crops'),livestock:val('livestock'),lsm:val('lsm'),ssm:val('ssm'),constr:val('constr'),mining:val('mining'),utilities:val('utilities'),trade:val('trade'),transport:val('transport'),finance:val('finance'),realestate:val('realestate'),ict:val('ict'),public:val('public'),education:val('education'),health:val('health')};
 base.agri_o=Math.max(0,A-(base.crops||0)-(base.livestock||0));
 base.mfg_o=Math.max(0,(M||0)-(base.lsm||0)-(base.ssm||0));
 base.serv_o=Math.max(0,S-['trade','transport','finance','realestate','ict','public','education','health'].reduce((a,k)=>a+(base[k]||0),0));
 return SUBS.filter(([k])=>base[k]!=null&&base[k]>0.01).map(([k,parent])=>({k,lbl:SUB_LBL[k],v:base[k],parent}));
}
function drawMix(){
 const el=d3.select('#mix');
 const rows=mixRows(selYear);
 // order: within parent desc, parents agri/ind/serv
 const order={agri:0,ind:1,serv:2};
 rows.sort((a,b)=>order[a.parent]-order[b.parent]||b.v-a.v);
 const W=el.node().clientWidth||520,rh=24,H=rows.length*rh+8;
 const lblW=Math.max(120,Math.min(190,W*0.4));
 const xmax=d3.max(rows,d=>d.v);
 const x=d3.scaleLinear().domain([0,xmax]).range([lblW+8,W-46]);
 let svg=el.select('svg');
 if(svg.empty()||+svg.attr('data-n')!==rows.length||+svg.attr('width')!==W){el.selectAll('*').remove();svg=el.append('svg');}
 svg.attr('width',W).attr('height',H).attr('data-n',rows.length).style('display','block');
 const shade=d=>{const c=d3.hcl(MACRO[d.parent].c);const rank=rows.filter(r=>r.parent===d.parent).indexOf(d);c.l=Math.min(88,c.l+rank*4.5);return c.formatHex();};
 const g=svg.selectAll('g.row').data(rows,d=>d.k);
 const gE=g.enter().append('g').attr('class','row');
 gE.append('text').attr('class','rl').attr('text-anchor','end').attr('font-size',11).attr('font-weight',600).attr('fill','var(--slate-700)');
 gE.append('rect').attr('rx',4).attr('height',rh-8).style('cursor','pointer');
 gE.append('text').attr('class','rv').attr('font-size',10.5).attr('font-weight',700).attr('fill','var(--slate-600)');
 const gm=gE.merge(g);
 gm.transition().duration(450).attr('transform',(d,i)=>`translate(0,${i*rh+4})`);
 gm.select('text.rl').attr('x',lblW).attr('y',rh/2-1).attr('dy','.3em')
  .attr('opacity',d=>selSector==='all'||selSector===d.parent?1:.35)
  .text(d=>{const max=Math.floor(lblW/6.4);return d.lbl.length>max?d.lbl.slice(0,max-1)+'…':d.lbl;});
 gm.select('rect').attr('x',x.range()[0]).attr('y',2)
  .attr('fill',shade)
  .attr('opacity',d=>selSector==='all'||selSector===d.parent?1:.3)
  .on('click',(e,d)=>setSector(selSector===d.parent?'all':d.parent))
  .on('mousemove',(e,d)=>showTip(`<b>${d.lbl}</b> · ${selYear}<br>${d.v.toFixed(1)}% of GDP · ${MACRO[d.parent].label}`,e)).on('mouseleave',hideTip)
  .transition().duration(450).attr('width',d=>Math.max(1.5,x(d.v)-x.range()[0]));
 gm.select('text.rv').attr('y',rh/2-1).attr('dy','.3em')
  .attr('opacity',d=>selSector==='all'||selSector===d.parent?1:.35)
  .transition().duration(450).attr('x',d=>x(d.v)+5).tween('text',function(d){const self=d3.select(this);return()=>self.text(d.v.toFixed(1)+'%');});
 g.exit().remove();
 const row=arcData.find(d=>d.year===selYear);
 d3.select('#mixSrc').text(row&&row.back?`${selYear}: three-sector split, backcast from real growth rates (indicative)`:`${selYear}: PBS national accounts, sectoral shares of GVA (2015-16 base)`);
}

/* ================= 2b. sector growth ================= */
function drawSGrowth(){
 const el=d3.select('#sgrowth');el.selectAll('*').remove();
 const map={all:['gdp','whole economy','#17301f'],agri:['agri','agriculture','#5b8c5a'],ind:['mfg','manufacturing (industry proxy)','#e07b39'],serv:['serv','services','#3d6db5']};
 const [key,lbl,color]=map[selSector]||map.all;
 d3.select('#growthSecLbl').text(lbl);
 const pts=ST.growth[key].map(p=>({...p,n:fyEnd(p.year)}));
 const W=el.node().clientWidth||520,H=300,m={t:16,r:12,b:26,l:40};
 const x=d3.scaleLinear().domain([pts[0].n-0.5,lastPt(pts).n+0.5]).range([m.l,W-m.r]);
 const ext=d3.extent(pts,p=>p.value);
 const y=d3.scaleLinear().domain([Math.min(-3,ext[0]),Math.max(8,ext[1])]).nice().range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 ERAS.forEach((e,i)=>{if(i%2)svg.append('rect').attr('x',x(e[0])).attr('y',m.t).attr('width',x(Math.min(e[1],lastPt(pts).n))-x(e[0])).attr('height',H-m.t-m.b).attr('fill','#17301f').attr('opacity',.045);});
 svg.append('g').attr('transform',`translate(0,${H-m.b})`).attr('class','axis').call(d3.axisBottom(x).ticks(Math.floor(W/80)).tickFormat(n=>fyLbl(n)));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(6).tickFormat(d=>d+'%')).call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 svg.append('line').attr('x1',m.l).attr('x2',W-m.r).attr('y1',y(0)).attr('y2',y(0)).attr('stroke','var(--slate-300)');
 const bw=Math.max(2,(W-m.l-m.r)/pts.length-1.5);
 svg.selectAll('rect.b').data(pts).join('rect').attr('class','b')
  .attr('x',d=>x(d.n)-bw/2).attr('width',bw)
  .attr('y',d=>Math.min(y(0),y(d.value))).attr('height',d=>Math.abs(y(0)-y(d.value)))
  .attr('rx',1.5).attr('fill',d=>d.value>=0?color:'#c0392b').attr('opacity',d=>d.value>=0?.75:.85)
  .style('cursor','pointer')
  .on('click',(e,d)=>setYear(d.year))
  .on('mousemove',(e,d)=>showTip(`<b>${d.year}</b><br>${lbl} ${fmtPct(d.value)}`,e)).on('mouseleave',hideTip);
 // decade average step
 const decades=d3.groups(pts,p=>Math.floor((p.n-1)/10)*10).map(([dec,arr])=>({x0:Math.max(arr[0].n-0.5,pts[0].n-0.5),x1:lastPt(arr).n+0.5,v:d3.mean(arr,p=>p.value)}));
 svg.selectAll('line.dec').data(decades).join('line').attr('class','dec')
  .attr('x1',d=>x(d.x0)).attr('x2',d=>x(d.x1)).attr('y1',d=>y(d.v)).attr('y2',d=>y(d.v))
  .attr('stroke','#17301f').attr('stroke-width',2.2).attr('opacity',.65)
  .on('mousemove',(e,d)=>showTip(`Decade average: <b>${d.v.toFixed(1)}%</b>`,e)).on('mouseleave',hideTip);
 // year cursor
 const cx=x(fyEnd(selYear));
 svg.append('line').attr('class','cursor-line').attr('x1',cx).attr('x2',cx).attr('y1',m.t).attr('y2',H-m.b).attr('pointer-events','none');
}

/* ================= growth contributions ================= */
let cGroup='broad',cYear=null;
const CONTRIB_COLORS={agri:'#5b8c5a',ind:'#e07b39',serv:'#3d6db5'};
const DETAIL_PALETTE=['#2f6b3a','#5b8c5a','#8ab27f','#b7cf9f','#c0392b','#e07b39','#e8a05a','#f0c489','#a0522d',
 '#1f4e79','#3d6db5','#6a95d0','#9dbbe4','#2c3e8f','#7b68a6','#9b59b6','#c39bd3','#0e8a8a','#16a085','#5dbfae'];
function contribKeys(){
 const ks=Object.keys(ST.contrib||{});
 const order={agri:0,ind:1,serv:2};
 return ks.sort((a,b)=>order[ST.contrib[a].parent]-order[ST.contrib[b].parent]||a.localeCompare(b));
}
let detailColor={};
function contribRows(year,group){
 // returns [{key,label,parent,v}] for one year at the chosen aggregation
 const out={};
 contribKeys().forEach(k=>{
  const c=ST.contrib[k],p=c.points.find(q=>q.year===year);
  if(!p)return;
  if(group==='broad'){
   out[c.parent]=out[c.parent]||{key:c.parent,label:MACRO[c.parent].label,parent:c.parent,v:0};
   out[c.parent].v+=p.value;
  }else out[k]={key:k,label:c.label,parent:c.parent,v:p.value};
 });
 return Object.values(out);
}
function contribYears(){return (ST.contrib_gdp||[]).map(p=>p.year);}
function initContrib(){
 contribKeys().forEach((k,i)=>detailColor[k]=DETAIL_PALETTE[i%DETAIL_PALETTE.length]);
 cYear=lastPt(ST.contrib_gdp).year;
 d3.selectAll('#cGroup button').on('click',function(){
  d3.selectAll('#cGroup button').classed('on',false);d3.select(this).classed('on',true);
  cGroup=this.dataset.g;drawContrib();drawCYear();drawCEras();});
 d3.select('#cYearSelect').selectAll('option').data(contribYears().slice().reverse()).join('option')
  .attr('value',y=>y).text(y=>y);
 d3.select('#cYearSelect').property('value',cYear).on('change',function(){setCYear(this.value);});
 drawContrib();drawCYear();drawCEras();
}
function contribColor(r){return cGroup==='broad'?CONTRIB_COLORS[r.parent]:detailColor[r.key];}
function setCYear(y){cYear=y;d3.select('#cYearLbl').text(y);d3.select('#cYearSelect').property('value',y);drawContrib();drawCYear();}
function drawContrib(){
 const el=d3.select('#contrib');el.selectAll('*').remove();
 const years=contribYears();
 const rowsBy={};years.forEach(y=>rowsBy[y]=contribRows(y,cGroup));
 const keys=cGroup==='broad'?['agri','ind','serv']:contribKeys();
 const W=el.node().clientWidth||900,H=Math.max(260,Math.min(330,W*0.3)),m={t:14,r:14,b:30,l:44};
 const x=d3.scaleBand().domain(years).range([m.l,W-m.r]).padding(.22);
 let lo=0,hi=0;
 years.forEach(y=>{let p=0,n=0;rowsBy[y].forEach(r=>r.v>=0?p+=r.v:n+=r.v);hi=Math.max(hi,p);lo=Math.min(lo,n);});
 const y=d3.scaleLinear().domain([lo*1.08,hi*1.08]).nice().range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(6).tickFormat(d=>d+'pp'))
  .call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 svg.append('g').attr('transform',`translate(0,${y(0)})`).attr('class','axis')
  .call(d3.axisBottom(x).tickFormat((d,i)=>years.length>14&&i%2?'':d.slice(2)))
  .call(g=>g.selectAll('text').attr('y',12).attr('fill','var(--slate-500)'));
 // stacked bars, positive up / negative down
 years.forEach(yr=>{
  const rows=rowsBy[yr].slice().sort((a,b)=>keys.indexOf(a.key)-keys.indexOf(b.key));
  let up=0,dn=0;
  const g=svg.append('g').attr('class','cy').style('cursor','pointer')
   .on('click',()=>setCYear(yr));
  rows.forEach(r=>{
   const y0=r.v>=0?up:dn, y1=y0+r.v;
   if(r.v>=0)up=y1;else dn=y1;
   g.append('rect').attr('x',x(yr)).attr('width',x.bandwidth())
    .attr('y',Math.min(y(y0),y(y1))).attr('height',Math.abs(y(y1)-y(y0)))
    .attr('fill',contribColor(r)).attr('opacity',yr===cYear?1:.82)
    .on('mousemove',e=>showTip(`<b>${r.label}</b> · ${yr}<br>${(r.v>=0?'+':'')+r.v.toFixed(2)} pp of GDP growth`,e))
    .on('mouseleave',hideTip);
  });
 });
 // headline GDP growth line
 const gdp=ST.contrib_gdp;
 const lx=yr=>x(yr)+x.bandwidth()/2;
 svg.append('path').datum(gdp).attr('fill','none').attr('stroke','#17301f').attr('stroke-width',2)
  .attr('d',d3.line().x(p=>lx(p.year)).y(p=>y(p.value)));
 svg.selectAll('circle.gd').data(gdp).join('circle').attr('class','gd')
  .attr('cx',p=>lx(p.year)).attr('cy',p=>y(p.value)).attr('r',2.8).attr('fill','#17301f')
  .on('mousemove',(e,p)=>showTip(`<b>GDP growth ${p.year}</b><br>${fmtPct(p.value)}`,e)).on('mouseleave',hideTip);
 // selected-year marker
 svg.append('rect').attr('x',x(cYear)-2).attr('y',m.t).attr('width',x.bandwidth()+4).attr('height',H-m.t-m.b)
  .attr('fill','none').attr('stroke','var(--green-900)').attr('stroke-width',1.3).attr('rx',3).attr('pointer-events','none');
 // legend
 const lg=d3.select('#contribLegend');
 const items=(cGroup==='broad'?['agri','ind','serv']:contribKeys())
  .map(k=>cGroup==='broad'?{key:k,label:MACRO[k].label,parent:k}:{key:k,label:ST.contrib[k].label,parent:ST.contrib[k].parent});
 lg.selectAll('span.it').data(items,d=>d.key).join('span').attr('class','it')
  .html(d=>`<i style="width:11px;height:11px;border-radius:3px;background:${contribColor(d)}"></i>${d.label}`);
 lg.selectAll('span.gdpk').data([0]).join('span').attr('class','gdpk')
  .html('<i style="width:14px;height:2.5px;border-radius:2px;background:#17301f"></i>GDP growth');
}
function drawCYear(){
 const el=d3.select('#cYear');el.selectAll('*').remove();
 d3.select('#cYearLbl').text(cYear);
 // ranked diverging bars: positives point right, negatives point left of the zero axis.
 // negative VALUE LABELS are placed to the right of the axis (in red) so they never
 // crowd the sector names on the left.
 const rows=contribRows(cYear,cGroup).sort((a,b)=>b.v-a.v).filter(r=>Math.abs(r.v)>0.001);
 const RED='#c0392b';
 const W=el.node().clientWidth||520,rh=cGroup==='broad'?34:22,H=rows.length*rh+26;
 const lblW=Math.max(110,Math.min(170,W*0.34));
 const minV=Math.min(0,d3.min(rows,r=>r.v)),maxV=Math.max(0,d3.max(rows,r=>r.v));
 const negRoom=minV<0?Math.max(20,(W-lblW)*0.12):0; // space left of zero for negative bars
 const x=d3.scaleLinear().domain([minV,maxV]).range([lblW+12+negRoom,W-56]);
 const zero=x(0);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 svg.append('line').attr('x1',zero).attr('x2',zero).attr('y1',4).attr('y2',H-20).attr('stroke','var(--slate-400)');
 const g=svg.selectAll('g').data(rows).join('g').attr('transform',(d,i)=>`translate(0,${i*rh+4})`)
  .on('mousemove',(e,d)=>showTip(`<b>${d.label}</b> · ${cYear}<br>${(d.v>=0?'+':'−')+Math.abs(d.v).toFixed(2)} pp — ${d.v>=0?'added to':'subtracted from'} growth`,e)).on('mouseleave',hideTip);
 g.append('text').attr('x',lblW).attr('y',rh/2-2).attr('dy','.32em').attr('text-anchor','end')
  .attr('font-size',cGroup==='broad'?12.5:11).attr('font-weight',600).attr('fill',d=>d.v<0?RED:'var(--slate-700)')
  .text(d=>{const max=Math.floor(lblW/6.2);return d.label.length>max?d.label.slice(0,max-1)+'…':d.label;});
 // bar: from zero, right for positive, left for negative
 g.append('rect').attr('y',3).attr('height',rh-9).attr('rx',3)
  .attr('x',d=>Math.min(zero,x(d.v))).attr('width',d=>Math.max(1,Math.abs(x(d.v)-zero)))
  .attr('fill',d=>d.v<0?RED:contribColor(d));
 // value label: positives at the bar tip; negatives just right of the zero axis (empty space)
 g.append('text').attr('y',rh/2-2).attr('dy','.32em').attr('font-size',10.5).attr('font-weight',700)
  .attr('text-anchor','start')
  .attr('x',d=>d.v>=0?x(d.v)+5:zero+5)
  .attr('fill',d=>d.v>=0?'var(--green-600)':RED).text(d=>(d.v>=0?'+':'−')+Math.abs(d.v).toFixed(2));
 const tot=d3.sum(rows,r=>r.v);
 const gdpP=(ST.contrib_gdp.find(p=>p.year===cYear)||{}).value;
 svg.append('text').attr('x',lblW).attr('y',H-6).attr('text-anchor','end').attr('font-size',11).attr('font-weight',800).attr('fill','var(--slate-600)').text('Sum');
 svg.append('text').attr('x',zero+5).attr('y',H-6).attr('font-size',11).attr('font-weight',800).attr('fill','var(--ink)')
  .text(`${tot>=0?'+':'−'}${Math.abs(tot).toFixed(2)} pp` + (gdpP!=null?`  ·  published GDP growth ${fmtPct(gdpP)}`:''));
}
const CERAS=[['2000s','2000-01','2007-08'],['Energy crisis','2008-09','2012-13'],['CPEC era','2013-14','2019-20'],['COVID & after','2020-21','2025-26']];
function drawCEras(){
 const el=d3.select('#cEras');el.selectAll('*').remove();
 const keys=cGroup==='broad'?['agri','ind','serv']:contribKeys();
 const data=CERAS.map(([label,y0,y1])=>{
  const yrs=contribYears().filter(y=>y>=y0&&y<=y1);
  const o={label,n:yrs.length};
  keys.forEach(k=>{
   const vals=yrs.map(y=>{const r=contribRows(y,cGroup).find(r=>r.key===k);return r?r.v:0;});
   o[k]=d3.mean(vals)||0;});
  return o;});
 const W=el.node().clientWidth||520,H=300,m={t:14,r:14,b:44,l:40};
 const x=d3.scaleBand().domain(data.map(d=>d.label)).range([m.l,W-m.r]).padding(.3);
 let lo=0,hi=0;data.forEach(d=>{let p=0,n=0;keys.forEach(k=>d[k]>=0?p+=d[k]:n+=d[k]);hi=Math.max(hi,p);lo=Math.min(lo,n);});
 const y=d3.scaleLinear().domain([Math.min(0,lo*1.1),hi*1.1]).nice().range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(5).tickFormat(d=>d+'pp'))
  .call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 svg.append('g').attr('transform',`translate(0,${y(0)})`).attr('class','axis').call(d3.axisBottom(x).tickSize(0))
  .call(g=>g.selectAll('text').attr('y',14).attr('font-weight',600).attr('fill','var(--slate-600)'));
 data.forEach(d=>{
  let up=0,dn=0;
  keys.forEach(k=>{
   const v=d[k];if(!v)return;
   const y0=v>=0?up:dn,y1=y0+v;if(v>=0)up=y1;else dn=y1;
   const label=cGroup==='broad'?MACRO[k].label:ST.contrib[k].label;
   svg.append('rect').attr('x',x(d.label)).attr('width',x.bandwidth())
    .attr('y',Math.min(y(y0),y(y1))).attr('height',Math.abs(y(y1)-y(y0)))
    .attr('fill',contribColor({key:k,parent:cGroup==='broad'?k:ST.contrib[k].parent}))
    .on('mousemove',e=>showTip(`<b>${label}</b> · ${d.label}<br>${(v>=0?'+':'')+v.toFixed(2)} pp per year`,e)).on('mouseleave',hideTip);
  });
  svg.append('text').attr('x',x(d.label)+x.bandwidth()/2).attr('y',y(up)-6).attr('text-anchor','middle')
   .attr('font-size',11).attr('font-weight',800).attr('fill','var(--ink)').text((up+dn).toFixed(1)+'pp');
 });
}

/* ================= 3. LSM spliced lines ================= */
function initLsm(){
 const chips=d3.select('#lsmChips');
 const canon=Object.keys(lsmSeries);
 // order: QIM first, then by weight desc
 const w=c=>c==='QIM'?999:(IND.new_base['2025-26'][c]?IND.new_base['2025-26'][c].weight:0);
 canon.sort((a,b)=>w(b)-w(a));
 chips.selectAll('button').data(canon).join('button').attr('class','chip')
  .html(c=>LSM_SHORT[c])
  .on('click',(e,c)=>{lsmSel.has(c)?lsmSel.delete(c):lsmSel.add(c);styleChips();drawLsm();drawWeights();});
 styleChips();drawLsm();
}
function styleChips(){
 d3.selectAll('#lsmChips .chip').classed('on',c=>lsmSel.has(c)).style('background',c=>lsmSel.has(c)?lsmColors[c]:null).style('border-color',c=>lsmSel.has(c)?lsmColors[c]:null).style('color',c=>lsmSel.has(c)?'#fff':null);
}
function drawLsm(){
 const el=d3.select('#lsm');el.selectAll('*').remove();
 const sel=Object.keys(lsmSeries).filter(c=>lsmSel.has(c));
 const W=el.node().clientWidth||1100,H=Math.max(260,Math.min(330,W*0.28)),m={t:16,r:150,b:28,l:42};
 const allPts=sel.flatMap(c=>lsmSeries[c]);
 if(!allPts.length){el.append('div').style('padding','30px').style('color','var(--slate-500)').text('Pick at least one sector.');return;}
 const x=d3.scaleLinear().domain(d3.extent(allPts,p=>p.n)).range([m.l,W-m.r]);
 const y=d3.scaleLinear().domain([0,d3.max(allPts,p=>p.v)*1.05]).nice().range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 svg.append('g').attr('transform',`translate(0,${H-m.b})`).attr('class','axis').call(d3.axisBottom(x).ticks(Math.floor((W-m.r)/70)).tickFormat(n=>fyLbl(n)));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(6)).call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 svg.append('line').attr('x1',m.l).attr('x2',W-m.r).attr('y1',y(100)).attr('y2',y(100)).attr('stroke','var(--slate-300)').attr('stroke-dasharray','3 3');
 svg.append('text').attr('x',m.l+4).attr('y',y(100)-5).attr('font-size',10).attr('fill','var(--slate-400)').text('2015-16 = 100');
 // base-change marker
 svg.append('line').attr('x1',x(2016)).attr('x2',x(2016)).attr('y1',m.t).attr('y2',H-m.b).attr('stroke','var(--slate-200)');
 svg.append('text').attr('x',x(2016)).attr('y',m.t-4).attr('text-anchor','middle').attr('font-size',9.5).attr('fill','var(--slate-400)').text('base change');
 const line=d3.line().x(p=>x(p.n)).y(p=>y(p.v)).curve(d3.curveMonotoneX);
 sel.forEach(c=>{
  const pts=lsmSeries[c];
  const oldPts=pts.filter(p=>p.linked||p.n===2016),newPts=pts.filter(p=>!p.linked);
  if(oldPts.length>1||(oldPts.length===1&&newPts.length))
   svg.append('path').datum(oldPts.concat(newPts.filter(p=>p.n===2016)).sort((a,b)=>a.n-b.n))
    .attr('fill','none').attr('stroke',lsmColors[c]).attr('stroke-width',2).attr('stroke-dasharray','5 4').attr('d',line);
  svg.append('path').datum(newPts).attr('fill','none').attr('stroke',lsmColors[c]).attr('stroke-width',2.5).attr('d',line);
  svg.selectAll(null).data(pts).join('circle').attr('cx',p=>x(p.n)).attr('cy',p=>y(p.v)).attr('r',3).attr('fill',lsmColors[c]).attr('stroke','#fff').attr('stroke-width',1)
   .on('mousemove',(e,p)=>showTip(`<b>${LSM_SHORT[c]}</b> ${p.fy}${p.linked?' (old base, linked)':''}${p.fy==='2025-26'?' (Jul–May)':''}<br>index ${p.v.toFixed(1)} (2015-16 = 100)`,e)).on('mouseleave',hideTip);
  const lp=lastPt(pts);
  svg.append('text').attr('x',x(lp.n)+7).attr('y',y(lp.v)+4).attr('font-size',11).attr('font-weight',700).attr('fill',lsmColors[c]).text(LSM_SHORT[c]);
 });
}

/* ================= 3b. QIM monthly ================= */
function drawQim(){
 const el=d3.select('#qimMonthly');el.selectAll('*').remove();
 const pts=IND.trend.map(p=>({...p,d:new Date(+p.month.slice(0,4),+p.month.slice(5,7)-1,15)}));
 const W=el.node().clientWidth||520,H=320,m={t:14,r:12,b:24,l:38},split=H-108;
 const x=d3.scaleTime().domain(d3.extent(pts,p=>p.d)).range([m.l,W-m.r]);
 const y=d3.scaleLinear().domain([d3.min(pts,p=>p.qim)*0.95,d3.max(pts,p=>p.qim)*1.03]).range([split,m.t]);
 const yb=d3.scaleLinear().domain(d3.extent(pts,p=>p.yoy||0)).nice().range([H-m.b,split+12]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 // crisis shading
 [[new Date(2020,1,1),new Date(2020,7,31),'COVID'],[new Date(2022,5,1),new Date(2023,5,30),'import squeeze']].forEach(([a,b,l])=>{
  svg.append('rect').attr('x',x(a)).attr('y',m.t).attr('width',x(b)-x(a)).attr('height',H-m.t-m.b).attr('fill','#c0392b').attr('opacity',.06);
  svg.append('text').attr('class','era-lbl').attr('x',(x(a)+x(b))/2).attr('y',m.t+9).attr('text-anchor','middle').attr('fill','#a04338').text(l);});
 svg.append('g').attr('transform',`translate(0,${split})`).attr('class','axis').call(d3.axisBottom(x).ticks(Math.floor(W/90)));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(4)).call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(yb).ticks(3).tickFormat(d=>d+'%'));
 svg.append('path').datum(pts).attr('fill','none').attr('stroke','var(--green-600)').attr('stroke-width',1.8)
  .attr('d',d3.line().x(p=>x(p.d)).y(p=>y(p.qim)));
 const bw=Math.max(1,(W-m.l-m.r)/pts.length-0.6);
 svg.append('line').attr('x1',m.l).attr('x2',W-m.r).attr('y1',yb(0)).attr('y2',yb(0)).attr('stroke','var(--slate-200)');
 svg.selectAll('rect.yy').data(pts.filter(p=>p.yoy!=null)).join('rect').attr('class','yy')
  .attr('x',p=>x(p.d)-bw/2).attr('width',bw)
  .attr('y',p=>Math.min(yb(0),yb(p.yoy))).attr('height',p=>Math.abs(yb(0)-yb(p.yoy)))
  .attr('fill',p=>p.yoy>=0?'var(--green-500)':'#c0392b').attr('opacity',.8);
 svg.append('rect').attr('x',m.l).attr('y',m.t).attr('width',W-m.l-m.r).attr('height',H-m.t-m.b).attr('fill','transparent')
  .on('mousemove',function(e){
   const d0=x.invert(d3.pointer(e,this)[0]);
   const p=pts.reduce((a,b)=>Math.abs(b.d-d0)<Math.abs(a.d-d0)?b:a);
   showTip(`<b>${p.month}</b><br>QIM ${p.qim.toFixed(1)}<br>YoY ${fmtPct(p.yoy)} · MoM ${fmtPct(p.mom)}`,e);
  }).on('mouseleave',hideTip);
 const lp=lastPt(pts);
 d3.select('#qimSrc').text(`PBS QIM trend sheet, Jul 2016 – ${lp.month}. Latest: ${lp.qim.toFixed(1)} (YoY ${fmtPct(lp.yoy)}); FY cumulative ${lp.cum_qim.toFixed(1)} (${fmtPct(lp.cum_chg)}).`);
}

/* ================= 3c. weights + FY growth ================= */
function fyGrowth(c){
 const a=IND.new_base['2025-26'][c],b=IND.new_base['2024-25'][c];
 if(!a||!b)return null;
 const common=Object.keys(a.monthly).map(k=>k.slice(5)).filter(mm=>Object.keys(b.monthly).some(k2=>k2.slice(5)===mm));
 if(!common.length)return null;
 const avg=(o,keep)=>d3.mean(Object.entries(o).filter(([k])=>keep.includes(k.slice(5))).map(([,v])=>v));
 const va=avg(a.monthly,common),vb=avg(b.monthly,common);
 return vb?100*(va/vb-1):null;
}
function drawWeights(){
 const el=d3.select('#lsmWeights');el.selectAll('*').remove();
 const rows=Object.keys(lsmSeries).filter(c=>c!=='QIM')
  .map(c=>({c,w:IND.new_base['2025-26'][c]?IND.new_base['2025-26'][c].weight:0,g:fyGrowth(c)}))
  .sort((a,b)=>b.w-a.w);
 const W=el.node().clientWidth||520,rh=23,H=rows.length*rh+6;
 const lblW=Math.max(110,Math.min(160,W*0.32));
 const x=d3.scaleLinear().domain([0,d3.max(rows,d=>d.w)]).range([lblW+6,W-92]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 const g=svg.selectAll('g').data(rows).join('g').attr('transform',(d,i)=>`translate(0,${i*rh+3})`).style('cursor','pointer')
  .on('click',(e,d)=>{lsmSel.has(d.c)?lsmSel.delete(d.c):lsmSel.add(d.c);styleChips();drawLsm();drawWeights();})
  .on('mousemove',(e,d)=>showTip(`<b>${LSM_SHORT[d.c]}</b><br>weight ${d.w.toFixed(2)}% of QIM<br>Jul–May 2025-26: ${fmtPct(d.g)}`,e)).on('mouseleave',hideTip);
 g.append('text').attr('x',lblW).attr('y',rh/2).attr('dy','.32em').attr('text-anchor','end').attr('font-size',11).attr('font-weight',d=>lsmSel.has(d.c)?800:600)
  .attr('fill',d=>lsmSel.has(d.c)?lsmColors[d.c]:'var(--slate-600)')
  .text(d=>{const max=Math.floor(lblW/6.2);const t=LSM_SHORT[d.c];return t.length>max?t.slice(0,max-1)+'…':t;});
 g.append('rect').attr('x',x(0)).attr('y',3).attr('height',rh-9).attr('rx',3)
  .attr('width',d=>Math.max(1.5,x(d.w)-x(0)))
  .attr('fill',d=>lsmSel.has(d.c)?lsmColors[d.c]:'var(--slate-300)');
 g.append('text').attr('x',d=>x(d.w)+5).attr('y',rh/2).attr('dy','.32em').attr('font-size',10.5).attr('font-weight',700).attr('fill','var(--slate-500)').text(d=>d.w.toFixed(1)+'%');
 g.append('text').attr('x',W-4).attr('y',rh/2).attr('dy','.32em').attr('text-anchor','end').attr('font-size',10.5).attr('font-weight',800)
  .attr('fill',d=>d.g==null?'var(--slate-400)':d.g>=0?'var(--green-600)':'#c0392b').text(d=>fmtPct(d.g));
}

/* ================= 4. CMI dumbbells ================= */
let cmiM='emp';
function initCmi(){
 d3.selectAll('#cmiMode button').on('click',function(){d3.selectAll('#cmiMode button').classed('on',false);d3.select(this).classed('on',true);cmiM=this.dataset.m;drawCmi();});
 drawCmi();
}
function drawCmi(){
 const el=d3.select('#cmi');el.selectAll('*').remove();
 const C=IND.cmi;
 const tot={emp:[1100814,2340966],est:[8680,42578]};
 let rows;
 if(cmiM==='gva'){
  rows=Object.entries(C.gva_weights).filter(([d])=>d!=='OD').map(([d,v])=>({d,lbl:v.label,p05:v.w2005,p15:v.w2015,a05:null,a15:null}));
 }else{
  const src=cmiM==='emp'?C.employment:C.establishments;
  rows=Object.entries(src).filter(([d])=>d!=='OD').map(([d,v])=>({d,lbl:v.label,p05:100*v.v2005/tot[cmiM][0],p15:100*v.v2015/tot[cmiM][1],a05:v.v2005,a15:v.v2015}));
 }
 rows.sort((a,b)=>b.p15-a.p15);
 const W=el.node().clientWidth||1100,rh=27,m={l:Math.max(120,Math.min(190,W*0.18)),r:56},H=rows.length*rh+40;
 const x=d3.scaleLinear().domain([0,d3.max(rows,d=>Math.max(d.p05,d.p15))*1.08]).range([m.l,W-m.r]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 svg.selectAll('line.gv').data(x.ticks(8)).join('line').attr('class','gl').attr('x1',d=>x(d)).attr('x2',d=>x(d)).attr('y1',4).attr('y2',H-26);
 svg.append('g').attr('transform',`translate(0,${H-24})`).attr('class','axis').call(d3.axisBottom(x).ticks(8).tickFormat(d=>d+'%'));
 const unit=cmiM==='emp'?'persons':cmiM==='est'?'establishments':'% of manufacturing GVA';
 const g=svg.selectAll('g.r').data(rows).join('g').attr('class','r').attr('transform',(d,i)=>`translate(0,${i*rh+12})`)
  .on('mousemove',(e,d)=>showTip(`<b>${d.lbl}</b><br>2005-06: ${d.p05.toFixed(1)}%${d.a05?` (${d.a05.toLocaleString()} ${unit})`:''}<br>2015-16: ${d.p15.toFixed(1)}%${d.a15?` (${d.a15.toLocaleString()} ${unit})`:''}<br><b>${d.p15>=d.p05?'+':''}${(d.p15-d.p05).toFixed(1)}pp</b> change in share`,e)).on('mouseleave',hideTip);
 g.append('text').attr('x',m.l-10).attr('y',2).attr('dy','.32em').attr('text-anchor','end').attr('font-size',11.5).attr('font-weight',600).attr('fill','var(--slate-700)').text(d=>d.lbl);
 g.append('line').attr('x1',d=>x(d.p05)).attr('x2',d=>x(d.p15)).attr('y1',2).attr('y2',2)
  .attr('stroke',d=>d.p15>=d.p05?'var(--green-500)':'#c0392b').attr('stroke-width',2.5).attr('opacity',.75);
 g.append('circle').attr('cx',d=>x(d.p05)).attr('cy',2).attr('r',4.5).attr('fill','#fff').attr('stroke','var(--slate-400)').attr('stroke-width',2);
 g.append('circle').attr('cx',d=>x(d.p15)).attr('cy',2).attr('r',5).attr('fill',d=>d.p15>=d.p05?'var(--green-600)':'#c0392b');
 g.append('text').attr('x',W-6).attr('y',2).attr('dy','.32em').attr('text-anchor','end').attr('font-size',10.5).attr('font-weight',700)
  .attr('fill',d=>d.p15>=d.p05?'var(--green-600)':'#c0392b').text(d=>`${d.p15>=d.p05?'+':''}${(d.p15-d.p05).toFixed(1)}`);
 // legend lives in HTML above the chart so it can never overlap the rows
 d3.select('#cmiLegend').html(
   '<span><i style="width:9px;height:9px;border-radius:50%;background:#fff;border:2px solid var(--slate-400)"></i>2005-06 census</span>'+
   '<span><i style="width:10px;height:10px;border-radius:50%;background:var(--green-600)"></i>2015-16 census</span>'+
   '<span style="color:var(--slate-400)">line = change in share · right-hand number = percentage-point change</span>');
}

/* ================= IO: focus / chord / grid ================= */
let ioView='focus',ioSec=0;
function initIO(){
 if(!E.io)return;
 const {sectors,matrix_nodiag:M}=E.io;
 // default focus = sector with the largest total two-way flow
 const tot=sectors.map((s,i)=>d3.sum(M[i])+d3.sum(M.map(r=>r[i])));
 ioSec=tot.indexOf(d3.max(tot));
 d3.select('#ioSector').selectAll('option').data(sectors.map((s,i)=>({s,i}))).join('option')
  .attr('value',d=>d.i).text(d=>d.s);
 d3.select('#ioSector').property('value',ioSec).on('change',function(){ioSec=+this.value;drawIO();});
 d3.selectAll('#ioView button').on('click',function(){
  d3.selectAll('#ioView button').classed('on',false);d3.select(this).classed('on',true);
  ioView=this.dataset.v;drawIO();});
 drawIO();
}
function setIOSector(i){ioSec=i;d3.select('#ioSector').property('value',i);if(ioView!=='focus'){ioView='focus';d3.selectAll('#ioView button').classed('on',function(){return this.dataset.v==='focus';});}drawIO();}
function drawIO(){
 const el=d3.select('#io');if(!el.node()||!E.io)return;el.selectAll('*').remove();
 d3.select('#ioSecWrap').style('display',ioView==='focus'?null:'none');
 d3.select('#ioFocusStats').style('display',ioView==='focus'?null:'none').html('');
 if(ioView==='chord')return drawIOChord();
 if(ioView==='grid')return drawIOGrid();
 drawIOFocus();
}
/* --- focus: suppliers → sector → buyers --- */
function drawIOFocus(){
 const el=d3.select('#io');
 const {sectors,colors,matrix:MD,matrix_nodiag:M}=E.io;
 const i=ioSec;
 const suppliers=sectors.map((s,j)=>({j,s,v:M[j][i]})).filter(d=>d.v>0&&d.j!==i).sort((a,b)=>b.v-a.v);
 const buyers=sectors.map((s,j)=>({j,s,v:M[i][j]})).filter(d=>d.v>0&&d.j!==i).sort((a,b)=>b.v-a.v);
 const own=MD&&MD[i]?MD[i][i]:0;
 const inTot=d3.sum(suppliers,d=>d.v),outTot=d3.sum(buyers,d=>d.v);
 const W=el.node().clientWidth||900;
 const rows=Math.max(suppliers.length,buyers.length);
 const H=Math.max(300,rows*30+70);
 const cx=W/2,colW=Math.min(215,(W-160)/2.6),gap=Math.min(150,W*0.16);
 const maxV=d3.max([...suppliers,...buyers],d=>d.v)||1;
 const bw=d3.scaleLinear().domain([0,maxV]).range([0,colW]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 const cy=y0=>y0;
 const hdr=(x,txt,anchor)=>svg.append('text').attr('x',x).attr('y',18).attr('text-anchor',anchor)
   .attr('font-size',11).attr('font-weight',800).attr('fill','var(--slate-500)')
   .attr('letter-spacing','.05em').text(txt);
 hdr(cx-gap/2-6,'SUPPLIES INTO IT','end');hdr(cx+gap/2+6,'IT SUPPLIES TO','start');
 // centre block
 const cH=Math.min(H-56,rows*30);
 svg.append('rect').attr('x',cx-gap/2).attr('y',34).attr('width',gap).attr('height',cH)
  .attr('rx',10).attr('fill',colors[i]).attr('opacity',.14).attr('stroke',colors[i]).attr('stroke-width',1.5);
 svg.append('text').attr('x',cx).attr('y',34+cH/2-6).attr('text-anchor','middle')
  .attr('font-size',13).attr('font-weight',800).attr('fill',colors[i])
  .call(t=>wrapText(t,sectors[i],gap-14,13));
 const draw=(arr,side)=>{
  const g=svg.selectAll(null).data(arr).join('g').attr('transform',(d,k)=>`translate(0,${40+k*30})`)
   .style('cursor','pointer').on('click',(e,d)=>setIOSector(d.j))
   .on('mousemove',(e,d)=>showTip(side<0
      ? `<b>${d.s}</b> supplies<br><b>${sectors[i]}</b><br>${fmtRs(d.v)} · ${(100*d.v/inTot).toFixed(1)}% of its purchased inputs`
      : `<b>${sectors[i]}</b> supplies<br><b>${d.s}</b><br>${fmtRs(d.v)} · ${(100*d.v/outTot).toFixed(1)}% of its sales to other sectors`,e))
   .on('mouseleave',hideTip);
  const edge=side<0?cx-gap/2:cx+gap/2;
  g.append('rect').attr('y',6).attr('height',15).attr('rx',3)
   .attr('x',d=>side<0?edge-bw(d.v):edge).attr('width',d=>bw(d.v))
   .attr('fill',d=>colors[d.j]).attr('opacity',.85);
  g.append('text').attr('y',13.5).attr('dy','.32em').attr('font-size',11).attr('font-weight',600).attr('fill','var(--slate-700)')
   .attr('text-anchor',side<0?'end':'start')
   .attr('x',d=>side<0?edge-bw(d.v)-7:edge+bw(d.v)+7)
   .text(d=>{const room=side<0?(edge-bw(d.v)-14):(W-(edge+bw(d.v))-14);const max=Math.floor(room/6.2);
     const t=`${d.s}  ${fmtBn(d.v)}`;return max<6?'':(t.length>max?t.slice(0,max-1)+'…':t);});
 };
 draw(suppliers,-1);draw(buyers,1);
 d3.select('#ioFocusStats').html(
  `<span><b style="color:${colors[i]}">${sectors[i]}</b></span>`+
  `<span>buys <b>${fmtRs(inTot)}</b> of inputs from other sectors</span>`+
  `<span>sells <b>${fmtRs(outTot)}</b> of inputs to them</span>`+
  (own>0?`<span>uses <b>${fmtRs(own)}</b> of its own output</span>`:'')+
  `<span style="color:var(--slate-400)">click any bar to re-centre</span>`);
}
function wrapText(sel,text,width,size){
 const words=text.split(/\s+/);const lines=[];let cur='';
 words.forEach(w=>{const t=cur?cur+' '+w:w;if(t.length*size*0.5>width&&cur){lines.push(cur);cur=w;}else cur=t;});
 if(cur)lines.push(cur);
 const node=sel.node();
 lines.forEach((l,k)=>d3.select(node).append('tspan').attr('x',node.getAttribute('x')).attr('dy',k?'1.15em':0).text(l));
}
/* --- circle (chord) --- */
function drawIOChord(){
 const el=d3.select('#io');
 const {sectors,colors,matrix_nodiag:M}=E.io;
 const W=el.node().clientWidth||900,H=Math.min(620,Math.max(470,W*0.6));
 const outerR=Math.max(90,Math.min(W,H)/2-135),innerR=outerR-13;
 const chordGen=(d3.chordDirected?d3.chordDirected():d3.chord()).padAngle(14/innerR).sortSubgroups(d3.descending).sortChords(d3.descending);
 const chords=chordGen(M);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 const g=svg.append('g').attr('transform',`translate(${W/2},${H/2})`);
 const arc=d3.arc().innerRadius(innerR).outerRadius(outerR);
 const ribbon=(d3.ribbonArrow?d3.ribbonArrow():d3.ribbon()).radius(innerR-1).padAngle(1/innerR);
 const rib=g.append('g').attr('fill-opacity',0.62).selectAll('path').data(chords).join('path')
   .attr('d',ribbon).attr('fill',d=>colors[d.source.index]).attr('stroke','#fff').attr('stroke-width',0.3)
   .style('cursor','pointer').on('click',(e,d)=>setIOSector(d.source.index))
   .on('mousemove',function(e,d){rib.attr('fill-opacity',x=>x===d?.95:.12);
     showTip(`<b>${sectors[d.source.index]}</b> supplies<br><b>${sectors[d.target.index]}</b><br>${fmtRs(d.source.value)}<br><span style="color:#9ca3af">click to focus</span>`,e);})
   .on('mouseleave',()=>{rib.attr('fill-opacity',null);hideTip();});
 const grp=g.append('g').selectAll('g').data(chords.groups).join('g').style('cursor','pointer')
   .on('click',(e,d)=>setIOSector(d.index));
 grp.append('path').attr('d',arc).attr('fill',d=>colors[d.index]).attr('stroke','#fff')
   .on('mousemove',function(e,d){rib.attr('fill-opacity',x=>x.source.index===d.index||x.target.index===d.index?.9:.1);
     showTip(`<b>${sectors[d.index]}</b><br>supplies ${fmtRs(d.value)} to other sectors<br><span style="color:#9ca3af">click to focus</span>`,e);})
   .on('mouseleave',()=>{rib.attr('fill-opacity',null);hideTip();});
 grp.append('text').each(function(d){d.ang=(d.startAngle+d.endAngle)/2;}).attr('dy','.35em')
   .attr('transform',d=>`rotate(${d.ang*180/Math.PI-90}) translate(${outerR+6}) ${d.ang>Math.PI?'rotate(180)':''}`)
   .attr('text-anchor',d=>d.ang>Math.PI?'end':'start').attr('font-size',11).attr('font-weight',600).attr('fill','#3d424d')
   .text(d=>sectors[d.index]);
}
/* --- grid (matrix heatmap) --- */
function drawIOGrid(){
 const el=d3.select('#io');
 const {sectors,colors,matrix_nodiag:M}=E.io;
 const n=sectors.length;
 const W=el.node().clientWidth||900;
 const m={t:112,l:Math.max(120,Math.min(190,W*0.19)),r:70,b:26};
 const cell=Math.max(16,Math.min(40,(W-m.l-m.r)/n));
 const H=m.t+cell*n+m.b;
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 const maxV=d3.max(M.flat());
 const col=d3.scaleSequential(d3.interpolateYlGn).domain([0,Math.sqrt(maxV)]);
 const rowTot=M.map(r=>d3.sum(r));
 // column headers (rotated)
 sectors.forEach((s,j)=>{
  svg.append('text').attr('transform',`translate(${m.l+j*cell+cell/2},${m.t-8}) rotate(-52)`)
   .attr('font-size',10.5).attr('font-weight',600).attr('fill','var(--slate-600)').text(s.length>22?s.slice(0,21)+'…':s);
 });
 svg.append('text').attr('x',m.l).attr('y',m.t-88).attr('font-size',11).attr('font-weight',800).attr('fill','var(--slate-500)').attr('letter-spacing','.05em').text('BUYER →');
 svg.append('text').attr('x',6).attr('y',m.t-8).attr('font-size',11).attr('font-weight',800).attr('fill','var(--slate-500)').attr('letter-spacing','.05em').text('SUPPLIER ↓');
 sectors.forEach((s,i)=>{
  svg.append('text').attr('x',m.l-8).attr('y',m.t+i*cell+cell/2).attr('dy','.32em').attr('text-anchor','end')
   .attr('font-size',10.5).attr('font-weight',600).attr('fill','var(--slate-700)').style('cursor','pointer')
   .on('click',()=>setIOSector(i)).text(s);
  svg.append('text').attr('x',m.l+n*cell+8).attr('y',m.t+i*cell+cell/2).attr('dy','.32em')
   .attr('font-size',10).attr('font-weight',700).attr('fill','var(--slate-500)').text(fmtBn(rowTot[i]));
  sectors.forEach((s2,j)=>{
   const v=M[i][j];
   svg.append('rect').attr('x',m.l+j*cell).attr('y',m.t+i*cell).attr('width',cell-1).attr('height',cell-1).attr('rx',2)
    .attr('fill',i===j?'var(--slate-100)':(v>0?col(Math.sqrt(v)):'#fbfbfa'))
    .style('cursor','pointer').on('click',()=>setIOSector(i))
    .on('mousemove',e=>showTip(i===j?`<b>${s}</b> — own use excluded here`:`<b>${s}</b> supplies<br><b>${s2}</b><br>${fmtRs(v)}`,e))
    .on('mouseleave',hideTip);
  });
 });
 svg.append('text').attr('x',m.l+n*cell+8).attr('y',m.t-8).attr('font-size',10).attr('font-weight',800).attr('fill','var(--slate-500)').text('total');
}

/* ================= budget (treemap + trend, unchanged mechanics) ================= */
let bSide='expenditure',bView='tree',bPrice='nom',bYears=[],bYi=0;
function redraw(){bView==='trend'?drawBudgetTrend():drawBudget();}
function applyView(){
 const trend=bView==='trend';
 d3.select('#budget').style('display',trend?'none':null);
 d3.select('#budgetTrend').style('display',trend?null:'none');
 d3.select('#bYrWrap').style('display',trend?'none':null);
 d3.select('#bPrice').style('display',trend?null:'none');
 redraw();
}
let _defl=null;
function deflator(){
 if(_defl)return _defl;
 const cur=E.indicators.series.find(s=>s.key==='gdp_mp_curr'),con=E.indicators.series.find(s=>s.key==='gdp_mp_const');
 const cm={},km={};(cur?cur.points:[]).forEach(p=>cm[p.year]=p.value);(con?con.points:[]).forEach(p=>km[p.year]=p.value);
 const d={};Object.keys(cm).forEach(y=>{if(km[y])d[y]=cm[y]/km[y]*100;});
 _defl=d;return d;
}
function deflForYears(years){
 const d=deflator();const out={};let last=null,prevRatio=1.06;
 const known=Object.keys(d).sort();
 years.forEach(y=>{
  if(d[y]!=null){out[y]=d[y];last=d[y];}
  else{const k=known.filter(x=>x<y);
   if(k.length>=2){const a=d[k[k.length-2]],b=d[k[k.length-1]];prevRatio=b/a;last=(last||b);}
   out[y]=last=Math.round((last||100)*prevRatio*10)/10;}
 });
 return out;
}
function initBudget(){
 d3.selectAll('#bSide button').on('click',function(){d3.selectAll('#bSide button').classed('on',false);d3.select(this).classed('on',true);bSide=this.dataset.s;setBYears();applyView();});
 d3.selectAll('#bView button').on('click',function(){d3.selectAll('#bView button').classed('on',false);d3.select(this).classed('on',true);bView=this.dataset.v;applyView();});
 d3.selectAll('#bPrice button').on('click',function(){d3.selectAll('#bPrice button').classed('on',false);d3.select(this).classed('on',true);bPrice=this.dataset.p;drawBudgetTrend();});
 setBYears();drawBudget();
}
function setBYears(){
 bYears=Object.keys(E.budget[bSide]).sort();bYi=bYears.length-1;
 const s=d3.select('#bYr');s.attr('min',0).attr('max',bYears.length-1).property('value',bYi);
 d3.select('#bYrLbl').text(bYears[bYi]);
 s.on('input',function(){bYi=+this.value;d3.select('#bYrLbl').text(bYears[bYi]);drawBudget();});
}
function drawBudgetTrend(){
 const el=d3.select('#budgetTrend');el.selectAll('*').remove();
 const years=Object.keys(E.budget[bSide]).sort();
 const colOf={};const totOf={};
 years.forEach(y=>E.budget[bSide][y].forEach(g=>{colOf[g.label]=g.color;totOf[g.label]=(totOf[g.label]||0)+d3.sum(g.children,c=>c.bn);}));
 const keys=Object.keys(totOf).sort((a,b)=>totOf[b]-totOf[a]);
 const real=bPrice==='real';const defl=deflForYears(years);
 const adj=(v,y)=>real?v/(defl[y]/100):v;
 const rows=years.map(y=>{const o={year:y};const m={};E.budget[bSide][y].forEach(g=>m[g.label]=d3.sum(g.children,c=>c.bn));keys.forEach(k=>o[k]=adj(m[k]||0,y));return o;});
 const W=el.node().clientWidth||900,H=Math.max(360,Math.min(520,W*0.5)),M={t:14,r:14,b:34,l:52};
 const x=d3.scalePoint().domain(years).range([M.l,W-M.r]);
 const ymax=d3.max(rows,r=>d3.sum(keys,k=>r[k]));
 const y=d3.scaleLinear().domain([0,ymax*1.02]).range([H-M.b,M.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 const yt=y.ticks(5);
 svg.append('g').selectAll('line').data(yt).join('line').attr('x1',M.l).attr('x2',W-M.r).attr('y1',d=>y(d)).attr('y2',d=>y(d)).attr('stroke','#eceae2');
 svg.append('g').selectAll('text').data(yt).join('text').attr('x',M.l-7).attr('y',d=>y(d)+3).attr('text-anchor','end').attr('font-size',10).attr('fill','#8a8f98').text(d=>fmtBn(d));
 svg.append('g').selectAll('text.xt').data(years).join('text').attr('class','xt').attr('x',d=>x(d)).attr('y',H-M.b+16).attr('text-anchor','middle').attr('font-size',9.5).attr('fill','#8a8f98').text((d,i)=>years.length>10&&i%2?'':d);
 const stack=d3.stack().keys(keys)(rows);
 const area=d3.area().x((d,i)=>x(years[i])).y0(d=>y(d[0])).y1(d=>y(d[1])).curve(d3.curveMonotoneX);
 svg.append('g').selectAll('path').data(stack).join('path').attr('d',area).attr('fill',s=>colOf[s.key]).attr('opacity',.82).attr('stroke','#fff').attr('stroke-width',.4)
  .on('mousemove',function(e,s){const xi=Math.round((e.offsetX-M.l)/((W-M.r-M.l)/(years.length-1)));const yr=years[Math.max(0,Math.min(years.length-1,xi))];const v=rows.find(r=>r.year===yr)[s.key];showTip(`<b>${s.key}</b><br>${yr}: ${fmtRs(v)}`,e);}).on('mouseleave',hideTip);
 const lg=el.append('div').attr('class','trend-legend');
 keys.forEach(k=>lg.append('span').attr('class','tl-item').html(`<i style="background:${colOf[k]}"></i>${k}`));
 d3.select('#budgetMeta').text(`${bSide==='expenditure'?'Current expenditure':'Tax & non-tax receipts'} by category, ${years[0]}–${years[years.length-1]} · ${real?'constant 2015-16 Rs (GDP-deflated)':'nominal Rs'} · ${bSide==='expenditure'?'Federal Budget in Brief':'Explanatory Memorandum on Federal Receipts'}`);
}
function drawBudget(){
 const el=d3.select('#budget');el.selectAll('*').remove();
 const yr=bYears[bYi];const groups=E.budget[bSide][yr]||[];
 const W=el.node().clientWidth||900,H=Math.max(420,Math.min(580,W*0.6));
 const root=d3.hierarchy({children:groups}).sum(d=>d.bn||0).sort((a,b)=>b.value-a.value);
 const total=root.value;
 d3.treemap().size([W,H]).paddingOuter(2).paddingTop(17).paddingInner(1).round(true)(root);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 const grp=svg.selectAll('g.grp').data(root.children||[]).join('g').attr('class','grp');
 grp.append('rect').attr('x',d=>d.x0).attr('y',d=>d.y0).attr('width',d=>d.x1-d.x0).attr('height',d=>d.y1-d.y0).attr('fill',d=>d.data.color).attr('opacity',.28).attr('rx',3)
   .on('mousemove',(e,d)=>showTip(`<b>${d.data.label}</b><br>${fmtRs(d.value)} · ${(100*d.value/total).toFixed(1)}%`,e)).on('mouseleave',hideTip);
 grp.filter(d=>(d.x1-d.x0)>66).append('text').attr('x',d=>d.x0+5).attr('y',d=>d.y0+13).attr('font-size',11.5).attr('font-weight',800).attr('fill','#123').attr('pointer-events','none')
   .text(d=>{const w=d.x1-d.x0,m=Math.floor(w/6.6);return d.data.label.length>m?d.data.label.slice(0,m-1)+'…':d.data.label;});
 const leaf=svg.selectAll('g.lf').data(root.leaves()).join('g').attr('class','lf').attr('transform',d=>`translate(${d.x0},${d.y0})`);
 leaf.append('rect').attr('width',d=>Math.max(0,d.x1-d.x0)).attr('height',d=>Math.max(0,d.y1-d.y0)).attr('rx',2).attr('fill',d=>d.parent.data.color).attr('stroke','#fff').attr('stroke-width',.6)
   .on('mousemove',(e,d)=>showTip(`<b>${d.data.name}</b><br>${fmtRs(d.data.bn)} · ${(100*d.value/total).toFixed(1)}%`,e)).on('mouseleave',hideTip);
 leaf.filter(d=>(d.x1-d.x0)>60&&(d.y1-d.y0)>22).each(function(d){
   const w=d.x1-d.x0,s=d3.select(this),m=Math.floor(w/6),c=d3.hcl(d.parent.data.color);
   s.append('text').attr('class','cl').attr('x',5).attr('y',15).style('fill',c.l>62?'#1a1a1a':'#fff').text(d.data.name.length>m?d.data.name.slice(0,m-1)+'…':d.data.name);
   if((d.y1-d.y0)>34)s.append('text').attr('class','cv').attr('x',5).attr('y',29).style('fill',c.l>62?'#333':'rgba(255,255,255,.85)').text(`${fmtRs(d.data.bn)} · ${(100*d.value/total).toFixed(0)}%`);
 });
 d3.select('#budgetMeta').text(`${bSide==='expenditure'?'Current expenditure (function-wise)':'Tax & non-tax receipts'} ${yr} · total ${fmtRs(total)} · ${bSide==='expenditure'?'Federal Budget in Brief':'Explanatory Memorandum on Federal Receipts'}`);
}

/* ================= CSV export ================= */
function toCSV(rows){
 if(!rows||!rows.length)return '';
 const cols=Object.keys(rows[0]);
 const esc=v=>{if(v==null)v='';v=String(v);return /[",\n]/.test(v)?'"'+v.replace(/"/g,'""')+'"':v;};
 return [cols.join(',')].concat(rows.map(r=>cols.map(c=>esc(r[c])).join(','))).join('\n');
}
function downloadCSV(name,rows){
 if(!rows||!rows.length)return;
 const blob=new Blob([toCSV(rows)],{type:'text/csv;charset=utf-8'});
 const a=document.createElement('a');a.href=URL.createObjectURL(blob);a.download=name.replace(/[^\w.-]+/g,'_');
 document.body.appendChild(a);a.click();setTimeout(()=>{URL.revokeObjectURL(a.href);a.remove();},100);
}
const CSV={
 arc:()=>['Pakistan_sector_shares_1952-2026.csv',
   arcData.map(d=>({fiscal_year:d.year,agriculture_pct:round2(d.agri),industry_pct:round2(d.ind),services_pct:round2(d.serv),source:d.back?'backcast':'published'}))],
 mix:()=>[`Pakistan_sector_mix_${selYear}.csv`,
   mixRows(selYear).map(r=>({fiscal_year:selYear,sector:r.lbl,broad_sector:MACRO[r.parent].label,share_of_gdp_pct:round2(r.v)}))],
 sgrowth:()=>{const map={all:'gdp',agri:'agri',ind:'mfg',serv:'serv'},lbl={all:'whole economy',agri:'agriculture',ind:'manufacturing',serv:'services'};
   return [`Pakistan_real_growth_${map[selSector]}.csv`,ST.growth[map[selSector]].map(p=>({fiscal_year:p.year,series:lbl[selSector],real_growth_pct:p.value}))];},
 contrib:()=>{const out=[];contribYears().forEach(y=>contribRows(y,cGroup).forEach(r=>out.push({fiscal_year:y,sector:r.label,broad_sector:MACRO[r.parent].label,contribution_pp:round2(r.v)})));
   (ST.contrib_gdp||[]).forEach(p=>out.push({fiscal_year:p.year,sector:'TOTAL (published GDP growth)',broad_sector:'',contribution_pp:round2(p.value)}));
   return [`Pakistan_growth_contributions_${cGroup}.csv`,out];},
 cyear:()=>[`Pakistan_growth_breakdown_${cYear}.csv`,
   contribRows(cYear,cGroup).sort((a,b)=>b.v-a.v).map(r=>({fiscal_year:cYear,sector:r.label,broad_sector:MACRO[r.parent].label,contribution_pp:round2(r.v)}))],
 ceras:()=>{const keys=cGroup==='broad'?['agri','ind','serv']:contribKeys();const out=[];
   CERAS.forEach(([label,y0,y1])=>{const yrs=contribYears().filter(y=>y>=y0&&y<=y1);
     keys.forEach(k=>{const vals=yrs.map(y=>{const r=contribRows(y,cGroup).find(r=>r.key===k);return r?r.v:0;});
       const lbl=cGroup==='broad'?MACRO[k].label:ST.contrib[k].label;
       out.push({era:label,years:`${y0}–${y1}`,sector:lbl,avg_contribution_pp:round2(d3.mean(vals)||0)});});});
   return ['Pakistan_growth_by_era.csv',out];},
 lsm:()=>{const out=[];Object.keys(lsmSeries).filter(c=>lsmSel.has(c)).forEach(c=>lsmSeries[c].forEach(p=>
     out.push({sector:LSM_SHORT[c],fiscal_year:p.fy,production_index_2015_16_base:round2(p.v),series:p.linked?'old-base (linked)':'new base'})));
   return ['Pakistan_LSM_sector_indices.csv',out];},
 qim:()=>['Pakistan_QIM_monthly.csv',
   IND.trend.map(p=>({month:p.month,qim_index:p.qim,mom_change_pct:p.mom,yoy_change_pct:p.yoy,fy_cumulative_index:p.cum_qim,fy_cumulative_change_pct:p.cum_chg}))],
 weights:()=>['Pakistan_LSM_weights.csv',
   Object.keys(lsmSeries).filter(c=>c!=='QIM').map(c=>({sector:LSM_SHORT[c],weight_in_qim_pct:IND.new_base['2025-26'][c]?IND.new_base['2025-26'][c].weight:null,jul_may_2025_26_growth_pct:round2(fyGrowth(c))})).sort((a,b)=>(b.weight_in_qim_pct||0)-(a.weight_in_qim_pct||0))],
 cmi:()=>{const C=IND.cmi,tot={emp:[1100814,2340966],est:[8680,42578]};
   let rows;
   if(cmiM==='gva')rows=Object.entries(C.gva_weights).filter(([d])=>d!=='OD').map(([d,v])=>({industry:v.label,share_2005_06_pct:v.w2005,share_2015_16_pct:v.w2015}));
   else{const src=cmiM==='emp'?C.employment:C.establishments;
     rows=Object.entries(src).filter(([d])=>d!=='OD').map(([d,v])=>({industry:v.label,value_2005_06:v.v2005,value_2015_16:v.v2015,share_2005_06_pct:round2(100*v.v2005/tot[cmiM][0]),share_2015_16_pct:round2(100*v.v2015/tot[cmiM][1])}));}
   return [`Pakistan_CMI_${cmiM}_2005_vs_2015.csv`,rows.sort((a,b)=>b.share_2015_16_pct-a.share_2015_16_pct)];},
 io:()=>{const {sectors,matrix_nodiag:M}=E.io;const out=[];
   sectors.forEach((s,i)=>sectors.forEach((s2,j)=>{if(i!==j&&M[i][j]>0)out.push({supplier:s,buyer:s2,flow_rs_bn:round2(M[i][j])});}));
   return ['Pakistan_input_output_2015-16.csv',out.sort((a,b)=>b.flow_rs_bn-a.flow_rs_bn)];},
 budget:()=>{const yr=bYears[bYi],out=[];(E.budget[bSide][yr]||[]).forEach(g=>g.children.forEach(c=>out.push({fiscal_year:yr,side:bSide,category:g.label,line_item:c.name,rs_bn:round2(c.bn)})));
   return [`Pakistan_budget_${bSide}_${yr}.csv`,out];}
};
function round2(v){return v==null?null:Math.round(v*100)/100;}
function initCsv(){
 d3.selectAll('.csvbtn').on('click',function(e){
  e.stopPropagation();
  const k=this.dataset.csv,fn=CSV[k];if(!fn)return;
  try{const [name,rows]=fn();downloadCSV(name,rows);}catch(err){console.error('CSV',k,err);}
 });
}

if(document.readyState!=='loading')start();else document.addEventListener('DOMContentLoaded',start);
