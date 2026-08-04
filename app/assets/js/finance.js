/* Data Darbar — Public Finance (Product 3). D3 v7 + window.ECON.budget */
const fmtBn=v=>v==null?'—':(Math.abs(v)>=1000?(v/1000).toFixed(v>=10000?0:1)+' tn':Math.round(v).toLocaleString()+' bn');
const fmtRs=v=>v==null?'—':'Rs '+fmtBn(v);
const tip=d3.select('#tip');
const showTip=(h,e)=>{tip.html(h).style('opacity',1);moveTip(e);};
const moveTip=e=>{const p=12;let x=e.clientX+p,y=e.clientY+p,w=tip.node().offsetWidth,hh=tip.node().offsetHeight;if(x+w>innerWidth)x=e.clientX-w-p;if(y+hh>innerHeight)y=e.clientY-hh-p;tip.style('left',x+'px').style('top',y+'px');};
const hideTip=()=>tip.style('opacity',0);
let E;
function start(){if(!window.ECON){return setTimeout(start,30);}E=window.ECON;initBudget();buildKpis();drawGrowth();drawSectors();drawIO();window.addEventListener('resize',()=>{redraw();drawFbr();drawGrowth();drawSectors();drawIO();});}

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
// GDP deflator (2015-16 = 100) from national accounts; extrapolated forward for any budget year
// beyond the published GDP series (e.g. 2026-27) using the latest year-on-year change.
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
  else{ // extrapolate forward from last known using recent avg growth
   const k=known.filter(x=>x<y);
   if(k.length>=2){const a=d[k[k.length-2]],b=d[k[k.length-1]];prevRatio=b/a;last=(last||b);}
   out[y]=last=Math.round((last||100)*prevRatio*10)/10;
  }
 });
 return out;
}
function initBudget(){
 d3.selectAll('#bSide button').on('click',function(){d3.selectAll('#bSide button').classed('on',false);d3.select(this).classed('on',true);bSide=this.dataset.s;setBYears();applyView();buildKpis();});
 d3.selectAll('#bView button').on('click',function(){d3.selectAll('#bView button').classed('on',false);d3.select(this).classed('on',true);bView=this.dataset.v;applyView();});
 d3.selectAll('#bPrice button').on('click',function(){d3.selectAll('#bPrice button').classed('on',false);d3.select(this).classed('on',true);bPrice=this.dataset.p;drawBudgetTrend();});
 setBYears();drawBudget();drawFbr();
}
function setBYears(){
 bYears=Object.keys(E.budget[bSide]).sort();bYi=bYears.length-1;
 const s=d3.select('#bYr');s.attr('min',0).attr('max',bYears.length-1).property('value',bYi);
 d3.select('#bYrLbl').text(bYears[bYi]);
 s.on('input',function(){bYi=+this.value;d3.select('#bYrLbl').text(bYears[bYi]);drawBudget();buildKpis();});
}
// Stacked-area trend: top-level categories over every available year
function drawBudgetTrend(){
 const el=d3.select('#budgetTrend');el.selectAll('*').remove();
 const years=Object.keys(E.budget[bSide]).sort();
 // series = union of group labels; value per year = sum of that group's children
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
 // gridlines + y axis
 const yt=y.ticks(5);
 svg.append('g').selectAll('line').data(yt).join('line').attr('x1',M.l).attr('x2',W-M.r).attr('y1',d=>y(d)).attr('y2',d=>y(d)).attr('stroke','#eceae2');
 svg.append('g').selectAll('text').data(yt).join('text').attr('x',M.l-7).attr('y',d=>y(d)+3).attr('text-anchor','end').attr('font-size',10).attr('fill','#8a8f98').text(d=>fmtBn(d));
 // x labels (every other to avoid crowding)
 svg.append('g').selectAll('text').data(years).join('text').attr('x',d=>x(d)).attr('y',H-M.b+16).attr('text-anchor','middle').attr('font-size',9.5).attr('fill','#8a8f98').text((d,i)=>years.length>10&&i%2?'':d);
 const stack=d3.stack().keys(keys)(rows);
 const area=d3.area().x((d,i)=>x(years[i])).y0(d=>y(d[0])).y1(d=>y(d[1])).curve(d3.curveMonotoneX);
 svg.append('g').selectAll('path').data(stack).join('path').attr('d',area).attr('fill',s=>colOf[s.key]).attr('opacity',.82).attr('stroke','#fff').attr('stroke-width',.4)
  .on('mousemove',function(e,s){const xi=Math.round((e.offsetX-M.l)/((W-M.r-M.l)/(years.length-1)));const yr=years[Math.max(0,Math.min(years.length-1,xi))];const v=rows.find(r=>r.year===yr)[s.key];showTip(`<b>${s.key}</b><br>${yr}: ${fmtRs(v)}`,e);}).on('mouseleave',hideTip);
 // legend
 const lg=el.append('div').attr('class','trend-legend');
 keys.forEach(k=>lg.append('span').attr('class','tl-item').html(`<i style="background:${colOf[k]}"></i>${k}`));
 d3.select('#budgetMeta').text(`${bSide==='expenditure'?'Current expenditure':'Tax & non-tax receipts'} by category, ${years[0]}–${years[years.length-1]} · ${real?'constant 2015-16 Rs (GDP-deflated)':'nominal Rs'} · ${bSide==='expenditure'?'Federal Budget in Brief':'Explanatory Memorandum on Federal Receipts'}`);
}
function total(side,year){return d3.sum(E.budget[side][year],g=>d3.sum(g.children,c=>c.bn));}
function buildKpis(){
 const yr=bYears[bYi];
 const ry=Object.keys(E.budget.receipts).sort().slice(-1)[0];
 const rec=E.budget.receipts[ry]?total('receipts',ry):null;
 const exp=E.budget.expenditure[bYears[bYi]]?total('expenditure',bYears[bYi]):null;
 // debt servicing (now a sub-item under General Public Services)
 let debt=0;(E.budget.expenditure[yr]||[]).forEach(g=>g.children.forEach(c=>{if(/debt serv|mark-up/i.test(c.name))debt+=c.bn;}));
 const fbrS=E.indicators.series.find(s=>s.key==='fbr_tax');const fbr=fbrS&&fbrS.points.length?fbrS.points[fbrS.points.length-1]:null;
 const gs=E.indicators.series.find(x=>x.key==='gdp_growth');const g=gs&&gs.points.length?gs.points[gs.points.length-1]:null;
 const cards=[
  {l:'Real GDP growth',v:g?g.value.toFixed(1)+'%':'—',s:g&&g.year,c:g&&g.value>=0?'':'neg'},
  {l:'FBR tax target',v:fmtRs(fbr&&fbr.value),s:fbr&&fbr.year},
  {l:'Current expenditure',v:fmtRs(exp),s:bYears[bYi]},
  {l:'Debt servicing',v:fmtRs(debt||null),s:exp?Math.round(100*debt/exp)+'% of current spend':'',c:'neg'},
  {l:'Tax + non-tax receipts',v:fmtRs(rec),s:ry},
 ];
 d3.select('#kpis').selectAll('.kpi').data(cards).join('div').attr('class','kpi').html(d=>`<div class="lbl">${d.l}</div><div class="val">${d.v}</div><div class="sub ${d.c||''}">${d.s||''}</div>`);
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
function drawFbr(){
 const el=d3.select('#fbr');if(!el.node())return;el.selectAll('*').remove();
 const s=E.indicators.series.find(x=>x.key==='fbr_tax');if(!s)return;const pts=s.points;
 const W=el.node().clientWidth||900,H=250,m={t:14,r:16,b:28,l:48};
 const x=d3.scaleBand().domain(pts.map(p=>p.year)).range([m.l,W-m.r]).padding(.28);
 const y=d3.scaleLinear().domain([0,d3.max(pts,p=>p.value)]).nice().range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H);
 svg.append('g').attr('transform',`translate(0,${H-m.b})`).attr('class','axis').call(d3.axisBottom(x));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(5).tickFormat(fmtBn)).call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 svg.selectAll('rect').data(pts).join('rect').attr('x',d=>x(d.year)).attr('width',x.bandwidth()).attr('y',d=>y(d.value)).attr('height',d=>y(0)-y(d.value)).attr('rx',4).attr('fill','#d4a017')
   .on('mousemove',(e,d)=>showTip(`<b>FBR target ${d.year}</b><br>${fmtRs(d.value)}`,e)).on('mouseleave',hideTip);
}
function drawGrowth(){
 const el=d3.select('#growth');if(!el.node())return;el.selectAll('*').remove();
 const s=E.indicators.series.find(x=>x.key==='gdp_growth');if(!s)return;const pts=s.points;
 const W=el.node().clientWidth||900,H=280,m={t:14,r:20,b:28,l:44};
 const x=d3.scalePoint().domain(pts.map(p=>p.year)).range([m.l,W-m.r]).padding(.5);
 const ext=d3.extent(pts,p=>p.value);const y=d3.scaleLinear().domain([Math.min(0,ext[0]),ext[1]]).nice().range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H);const tm=Math.ceil(pts.length/8);
 svg.append('g').attr('transform',`translate(0,${H-m.b})`).attr('class','axis').call(d3.axisBottom(x).tickValues(x.domain().filter((d,i)=>i%tm===0)));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(5).tickFormat(d=>d+'%')).call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 svg.append('line').attr('x1',m.l).attr('x2',W-m.r).attr('y1',y(0)).attr('y2',y(0)).attr('stroke','#c5cad3');
 svg.append('path').datum(pts).attr('fill','none').attr('stroke','#1e6b3e').attr('stroke-width',2.5).attr('d',d3.line().x(d=>x(d.year)).y(d=>y(d.value)));
 svg.selectAll('circle').data(pts).join('circle').attr('cx',d=>x(d.year)).attr('cy',d=>y(d.value)).attr('r',3.3).attr('fill','#1e6b3e').attr('stroke','#fff').attr('stroke-width',1.2)
   .on('mousemove',(e,d)=>showTip(`<b>${d.year}</b><br>${d.value.toFixed(2)}%`,e)).on('mouseleave',hideTip);
}
if(document.readyState!=='loading')start();else document.addEventListener('DOMContentLoaded',start);

/* ---------- GDP by sector (stacked area) ---------- */
function drawSectors(){
 const el=d3.select('#sectors');if(!el.node())return;el.selectAll('*').remove();
 const keys=[['va_agri','Agriculture','#5b8c5a'],['va_ind','Industry','#e07b39'],['va_serv','Services','#3d6db5']];
 const ser=keys.map(k=>E.indicators.series.find(s=>s.key===k[0])).filter(Boolean);if(ser.length<3)return;
 const years=ser[0].points.map(p=>p.year);
 const data=years.map(yr=>{const o={year:yr};keys.forEach(k=>{const s=E.indicators.series.find(x=>x.key===k[0]);const p=s&&s.points.find(pp=>pp.year===yr);o[k[0]]=p?p.value:0;});return o;});
 const W=el.node().clientWidth||900,H=300,m={t:14,r:16,b:28,l:54};
 const x=d3.scalePoint().domain(years).range([m.l,W-m.r]);
 const stack=d3.stack().keys(keys.map(k=>k[0]))(data);
 const y=d3.scaleLinear().domain([0,d3.max(stack[stack.length-1],d=>d[1])]).nice().range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H);
 const tm=Math.ceil(years.length/8);
 svg.append('g').attr('transform',`translate(0,${H-m.b})`).attr('class','axis').call(d3.axisBottom(x).tickValues(years.filter((d,i)=>i%tm===0)));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(5).tickFormat(fmtBn)).call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 const area=d3.area().x(d=>x(d.data.year)).y0(d=>y(d[0])).y1(d=>y(d[1]));
 svg.selectAll('path.a').data(stack).join('path').attr('class','a').attr('d',area).attr('fill',(d,i)=>keys[i][2]).attr('opacity',.9)
  .on('mousemove',function(e,d){const i=stack.indexOf(d);showTip(`<b>${keys[i][1]}</b> value added`,e);}).on('mouseleave',hideTip);
 d3.select('#secLegend').selectAll('.li').data(keys).join('div').attr('class','li').html(k=>`<span class="sw" style="background:${k[2]}"></span>${k[1]}`);
}
/* ---------- Input-Output chord (2015-16) ---------- */
function drawIO(){
 const el=d3.select('#io');if(!el.node()||!E.io)return;el.selectAll('*').remove();
 const {sectors,colors,matrix_nodiag:M}=E.io;
 const W=el.node().clientWidth||900,H=Math.min(600,Math.max(460,W*0.62));
 const outerR=Math.min(W,H)/2-130,innerR=outerR-13;
 const chordGen=(d3.chordDirected?d3.chordDirected():d3.chord()).padAngle(14/innerR).sortSubgroups(d3.descending).sortChords(d3.descending);
 const chords=chordGen(M);
 const svg=el.append('svg').attr('width',W).attr('height',H);
 const g=svg.append('g').attr('transform',`translate(${W/2},${H/2})`);
 const arc=d3.arc().innerRadius(innerR).outerRadius(outerR);
 const ribbon=(d3.ribbonArrow?d3.ribbonArrow():d3.ribbon()).radius(innerR-1).padAngle(1/innerR);
 // ribbons
 g.append('g').attr('fill-opacity',0.62).selectAll('path').data(chords).join('path')
   .attr('d',ribbon).attr('fill',d=>colors[d.source.index]).attr('stroke','#fff').attr('stroke-width',0.3)
   .on('mousemove',(e,d)=>showTip(`<b>${sectors[d.source.index]}</b> supplies<br><b>${sectors[d.target.index]}</b><br>${fmtRs(d.source.value)}`,e))
   .on('mouseleave',hideTip);
 // group arcs
 const grp=g.append('g').selectAll('g').data(chords.groups).join('g');
 grp.append('path').attr('d',arc).attr('fill',d=>colors[d.index]).attr('stroke','#fff')
   .on('mousemove',(e,d)=>showTip(`<b>${sectors[d.index]}</b><br>supplies ${fmtRs(d.value)} to other sectors`,e)).on('mouseleave',hideTip);
 // labels
 grp.append('text').each(function(d){d.ang=(d.startAngle+d.endAngle)/2;}).attr('dy','.35em')
   .attr('transform',d=>`rotate(${d.ang*180/Math.PI-90}) translate(${outerR+6}) ${d.ang>Math.PI?'rotate(180)':''}`)
   .attr('text-anchor',d=>d.ang>Math.PI?'end':'start').attr('font-size',11).attr('font-weight',600).attr('fill','#3d424d')
   .text(d=>sectors[d.index]);
}
