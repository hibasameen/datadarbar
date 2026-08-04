/* Data Darbar — Trade Atlas (Product 2). D3 v7 + window.ECON. */
const SC={XI:'#1e6b3e',II:'#d4a017',V:'#3d6db5',VI:'#9b59b6',XVI:'#e07b39',XV:'#5b8c5a',IV:'#c0392b',I:'#16a085',VII:'#8e44ad',XVII:'#2c3e8f',III:'#b8941a',XVIII:'#0e8a8a',VIII:'#a0522d',X:'#7f8c8d',XIII:'#c39bd3',XII:'#d98880',IX:'#6d4c2b',XIV:'#e8b92e',XX:'#5d6d7e',XIX:'#34495e',XXI:'#95a5a6'};
const SN={I:'Animals & products',II:'Vegetables',III:'Fats & oils',IV:'Food, bev, tobacco',V:'Minerals & fuels',VI:'Chemicals',VII:'Plastics & rubber',VIII:'Hides & leather',IX:'Wood',X:'Paper & pulp',XI:'Textiles',XII:'Footwear & headgear',XIII:'Stone, cement, glass',XIV:'Gems & precious metal',XV:'Base metals',XVI:'Machinery & electrical',XVII:'Transport equip.',XVIII:'Instruments',XIX:'Arms',XX:'Misc. manufactures',XXI:'Art & special'};
const scolor=s=>SC[s]||'#95a5a6';
const fmtBn=v=>v==null?'—':(Math.abs(v)>=1000?(v/1000).toFixed(v>=10000?0:1)+' tn':(Math.abs(v)>=1?Math.round(v).toLocaleString():v.toFixed(1))+' bn');
const fmtRs=v=>v==null?'—':'Rs '+fmtBn(v);
const tip=d3.select('#tip');
const showTip=(h,e)=>{tip.html(h).style('opacity',1);moveTip(e);};
const moveTip=e=>{const p=12;let x=e.clientX+p,y=e.clientY+p,w=tip.node().offsetWidth,hh=tip.node().offsetHeight;if(x+w>innerWidth)x=e.clientX-w-p;if(y+hh>innerHeight)y=e.clientY-hh-p;tip.style('left',x+'px').style('top',y+'px');};
const hideTip=()=>tip.style('opacity',0);
let E,D={};

function start(){
 if(!window.ECON){return setTimeout(start,30);}
 E=window.ECON;D.ser={};E.indicators.series.forEach(s=>D.ser[s.key]=s);
 buildKpis();initDrill();initPartners();initProducts();drawTotals();
 window.addEventListener('resize',()=>{drawDrill();drawTotals();drawPartners();drawProducts();});
}
function lastPt(k){const s=D.ser[k];return s&&s.points.length?s.points[s.points.length-1]:null;}
function buildKpis(){
 const ex=lastPt('trade_export'),im=lastPt('trade_import'),bal=lastPt('trade_balance');
 const cov=(bal&&im&&ex)?Math.round(100*ex.value/im.value):null;
 const cards=[{l:'Exports',v:fmtRs(ex&&ex.value),s:ex&&ex.year},{l:'Imports',v:fmtRs(im&&im.value),s:im&&im.year},
  {l:'Trade deficit',v:fmtRs(bal&&Math.abs(bal.value)),s:bal&&bal.year,c:'neg'},{l:'Exports cover imports',v:cov?cov+'%':'—',s:'of import bill',c:cov<60?'neg':'pos'}];
 d3.select('#kpis').selectAll('.kpi').data(cards).join('div').attr('class','kpi').html(d=>`<div class="lbl">${d.l}</div><div class="val">${d.v}</div><div class="sub ${d.c||''}">${d.s||''}</div>`);
}

/* ---------- ATLAS NESTED TREEMAP (all products, grouped by section) ---------- */
let dDir='export',dYears=[],dYi=0,dPath=[];
function initDrill(){
 d3.selectAll('#dDir button').on('click',function(){d3.selectAll('#dDir button').classed('on',false);d3.select(this).classed('on',true);dDir=this.dataset.d;dPath=[];setDYears();drawDrill();});
 d3.select('#dYr').on('input',function(){dYi=+this.value;d3.select('#dYrLbl').text(dYears[dYi]);drawDrill();});
 setDYears();drawDrill();
}
function setDYears(){dYears=Object.keys(E.tree[dDir]).sort();dYi=dYears.length-1;d3.select('#dYr').attr('min',0).attr('max',dYears.length-1).property('value',dYi);d3.select('#dYrLbl').text(dYears[dYi]);}
function drawDrill(){
 const el=d3.select('#drill');el.selectAll('*').remove();
 const secs=E.tree[dDir][dYears[dYi]];
 let groups;
 if(dPath.length===0){
   groups=secs.map(s=>({key:s.section,label:s.name,color:scolor(s.section),kind:'section',
     children:s.chapters.flatMap(c=>c.products.map(p=>({hs8:p.hs8,name:p.name,bn:p.bn,section:s.section})))}));
 } else {
   const s=secs.find(x=>x.section===dPath[0]);
   const base=d3.color(scolor(s.section));
   groups=s.chapters.map((c,i)=>({key:c.code,label:c.name,section:s.section,kind:'chapter',
     color:(i%2?base.brighter(0.5):base.darker(0.2)).formatHex(),
     children:c.products.map(p=>({hs8:p.hs8,name:p.name,bn:p.bn,section:s.section}))}));
 }
 const W=el.node().clientWidth||900,H=Math.max(420,Math.min(600,W*0.62));
 const root=d3.hierarchy({children:groups}).sum(d=>d.bn||0).sort((a,b)=>b.value-a.value);
 const total=root.value;
 d3.treemap().size([W,H]).paddingOuter(2).paddingTop(16).paddingInner(1).round(true)(root);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block').style('font-family','inherit');
 // group rects + labels (depth 1)
 const grp=svg.selectAll('g.grp').data(root.children||[]).join('g').attr('class','grp');
 grp.append('rect').attr('x',d=>d.x0).attr('y',d=>d.y0).attr('width',d=>d.x1-d.x0).attr('height',d=>d.y1-d.y0)
   .attr('fill',d=>d.data.color).attr('rx',3).attr('opacity',0.28)
   .style('cursor',d=>dPath.length===0?'pointer':'default')
   .on('click',(e,d)=>{if(dPath.length===0){dPath=[d.data.key];drawDrill();}})
   .on('mousemove',(e,d)=>showTip(`<b>${d.data.label}</b><br>${fmtRs(d.value)} · ${(100*d.value/total).toFixed(1)}%${dPath.length===0?'<br><span style=opacity:.7>click to zoom in</span>':''}`,e)).on('mouseleave',hideTip);
 grp.filter(d=>(d.x1-d.x0)>60).append('text').attr('x',d=>d.x0+5).attr('y',d=>d.y0+12).attr('font-size',11).attr('font-weight',800).attr('fill','#123')
   .attr('pointer-events','none').text(d=>{const w=d.x1-d.x0,m=Math.floor(w/6.6);return d.data.label.length>m?d.data.label.slice(0,m-1)+'…':d.data.label;});
 // product leaves (depth 2)
 const leaf=svg.selectAll('g.lf').data(root.leaves()).join('g').attr('class','lf').attr('transform',d=>`translate(${d.x0},${d.y0})`);
 leaf.append('rect').attr('width',d=>Math.max(0,d.x1-d.x0)).attr('height',d=>Math.max(0,d.y1-d.y0)).attr('rx',2)
   .attr('fill',d=>d.parent.data.color).attr('stroke','#fff').attr('stroke-width',0.5)
   .style('cursor',dPath.length===0?'pointer':'default')
   .on('click',(e,d)=>{if(dPath.length===0){dPath=[d.data.section];drawDrill();}})
   .on('mousemove',(e,d)=>showTip(`<b>${d.data.name}</b><br>HS ${d.data.hs8} · Sec ${d.data.section}<br>${fmtRs(d.data.bn)} · ${(100*d.value/total).toFixed(2)}%`,e)).on('mouseleave',hideTip);
 leaf.filter(d=>(d.x1-d.x0)>52&&(d.y1-d.y0)>20).append('text').attr('class','cl').attr('x',4).attr('y',13)
   .style('fill',d=>{const c=d3.hcl(d.parent.data.color);return c.l>62?'#1a1a1a':'#fff';})
   .text(d=>{const w=d.x1-d.x0,m=Math.floor(w/5.8);return d.data.name.length>m?d.data.name.slice(0,m-1)+'…':d.data.name;});
 // breadcrumb
 const bc=d3.select('#crumb');bc.selectAll('*').remove();
 const lv=[{t:dDir==='export'?'All exports':'All imports',i:0}];
 if(dPath[0])lv.push({t:SN[dPath[0]],i:1});
 lv.forEach((x,i)=>{if(i>0)bc.append('span').attr('class','sep').text(' › ');
   bc.append('span').attr('class','cr'+(i===lv.length-1?' on':'')).text(x.t).style('cursor',i<lv.length-1?'pointer':'default').on('click',()=>{dPath=dPath.slice(0,x.i);drawDrill();});});
 d3.select('#drillMeta').text(`${dDir==='export'?'Exports':'Imports'} ${dYears[dYi]} · ${dPath.length===0?'all products, grouped by section':'section '+dPath[0]+', by chapter'} · total ${fmtRs(total)}`);
}
/* ---------- trade over time ---------- */
function drawTotalsInto(el,W,H,hi){
 const T=E.totals,years=T.years,m={t:16,r:20,b:28,l:54};
 const x=d3.scalePoint().domain(years).range([m.l,W-m.r]).padding(.5);
 const av=[];years.forEach(y=>['export','import','balance'].forEach(k=>{if(T[k][y]!=null)av.push(T[k][y]);}));
 const y=d3.scaleLinear().domain([Math.min(0,d3.min(av)),d3.max(av)]).nice().range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H);
 svg.append('g').attr('transform',`translate(0,${H-m.b})`).attr('class','axis').call(d3.axisBottom(x).tickValues(years.filter((d,i)=>i%2===0)));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(5).tickFormat(fmtBn)).call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 svg.append('line').attr('x1',m.l).attr('x2',W-m.r).attr('y1',y(0)).attr('y2',y(0)).attr('stroke','#c5cad3');
 const ser=[['export','Exports',scolor('XI')],['import','Imports','#c0392b'],['balance','Balance','#3d6db5']];
 const line=d3.line().defined(d=>d.v!=null).x(d=>x(d.y)).y(d=>y(d.v));
 ser.forEach(s=>{const pts=years.map(yr=>({y:yr,v:T[s[0]][yr]}));
   const dim=(hi==='gap'&&s[0]==='balance')||(hi==='grow'&&s[0]==='balance');
   svg.append('path').datum(pts).attr('fill','none').attr('stroke',s[2]).attr('stroke-width',3).attr('opacity',dim?0.25:1).attr('d',line);
   const lp=pts.filter(p=>p.v!=null).slice(-1)[0];if(lp)svg.append('text').attr('x',x(lp.y)+8).attr('y',y(lp.v)+4).attr('font-size',12).attr('font-weight',700).attr('fill',s[2]).attr('opacity',dim?0.3:1).text(s[1]);});
 if(hi==='gap'){const yr=years[years.length-1];const e=T.export[yr],im=T.import[yr];if(e&&im)svg.append('line').attr('x1',x(yr)).attr('x2',x(yr)).attr('y1',y(e)).attr('y2',y(im)).attr('stroke','#c0392b').attr('stroke-width',2).attr('stroke-dasharray','4 3');}
}
function drawTotals(){const el=d3.select('#ttChart');el.selectAll('*').remove();drawTotalsInto(el,el.node().clientWidth||900,300,null);}

/* ---------- partners & products (year slider) ---------- */
let pDir='export',prDir='export';
function setSlider(id,lblId,years,cb){const s=d3.select('#'+id);s.attr('min',0).attr('max',Math.max(0,years.length-1)).property('value',years.length-1);d3.select('#'+lblId).text(years[years.length-1]||'');s.on('input',function(){d3.select('#'+lblId).text(years[+this.value]);cb();});}
const pYears=()=>Object.keys(E.partners[pDir]).sort();
const prYears=()=>Object.keys(E.products[prDir]).sort();
function initPartners(){d3.selectAll('#pDir button').on('click',function(){d3.selectAll('#pDir button').classed('on',false);d3.select(this).classed('on',true);pDir=this.dataset.d;setSlider('pYr','pYrLbl',pYears(),drawPartners);drawPartners();});setSlider('pYr','pYrLbl',pYears(),drawPartners);drawPartners();}
function initProducts(){d3.selectAll('#prDir button').on('click',function(){d3.selectAll('#prDir button').classed('on',false);d3.select(this).classed('on',true);prDir=this.dataset.d;setSlider('prYr','prYrLbl',prYears(),drawProducts);drawProducts();});setSlider('prYr','prYrLbl',prYears(),drawProducts);drawProducts();}
function drawPartners(){const ys=pYears();const y=ys[+d3.select('#pYr').property('value')||ys.length-1];hbar('#partners',E.partners[pDir][y]||[],d=>d.country,()=>pDir==='export'?scolor('XI'):'#c0392b');}
function drawProducts(){const ys=prYears();const y=ys[+d3.select('#prYr').property('value')||ys.length-1];hbar('#products',E.products[prDir][y]||[],d=>d.name,d=>scolor(d.section));}
function hbar(elSel,rows,label,color){
 const el=d3.select(elSel);el.selectAll('*').remove();rows=rows.slice(0,12);
 const W=el.node().clientWidth||500,rh=26,H=rows.length*rh+14,lblW=Math.max(96,Math.min(170,W*0.42)),maxc=Math.max(9,Math.floor(lblW/7.3));
 const x=d3.scaleLinear().domain([0,d3.max(rows,d=>d.bn)||1]).range([lblW+8,W-52]);
 const svg=el.append('svg').attr('width',W).attr('height',H);
 const g=svg.selectAll('g').data(rows).join('g').attr('transform',(d,i)=>`translate(0,${i*rh+7})`);
 g.append('title').text(d=>label(d));
 g.append('text').attr('x',lblW).attr('y',rh/2).attr('dy','.35em').attr('text-anchor','end').attr('font-size',11).attr('font-weight',600).attr('fill','#3d424d').text(d=>{const t=label(d);return t.length>maxc?t.slice(0,maxc-1)+'…':t;});
 g.append('rect').attr('x',x(0)).attr('y',3).attr('height',rh-9).attr('width',d=>x(d.bn)-x(0)).attr('rx',4).attr('fill',d=>color(d)).on('mousemove',(e,d)=>showTip(`<b>${label(d)}</b><br>${fmtRs(d.bn)}`,e)).on('mouseleave',hideTip);
 g.append('text').attr('x',d=>x(d.bn)+6).attr('y',rh/2).attr('dy','.35em').attr('font-size',11).attr('font-weight',700).attr('fill','#505662').text(d=>fmtBn(d.bn));
}

if(document.readyState!=='loading')start();else document.addEventListener('DOMContentLoaded',start);
