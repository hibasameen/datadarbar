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
let E,D={},TX;

/* ---- topics (sidebar navigation; #hash deep links) ---- */
const TOPICS=[
 {k:'all',label:'Everything',
  desc:'Every trade chart on one page, top to bottom.',
  meta:'PBS External Trade Statistics (8-digit), 2015–2024; gaps flagged in the source note below.'},
 {k:'basket',label:'What we trade',
  desc:'The full export/import basket as a drill-down treemap, plus the largest single products.',
  meta:'PBS External Trade Statistics — 8-digit imports & exports by commodity.'},
 {k:'movers',label:'What’s growing',
  desc:'The biggest risers and fallers in the trade basket over the decade, at your chosen detail level.',
  meta:'PBS 8-digit trade; change between 2015-16 and 2024-25. Detail level set in the sidebar.'},
 {k:'overtime',label:'Trade over time',
  desc:'Exports, imports and the trade balance year by year, 2015–2024.',
  meta:'PBS External Trade Statistics; a few years’ gaps filled from UN Comtrade / Economic Survey (see note).'},
 {k:'partners',label:'Trading partners',
  desc:'The largest countries Pakistan trades with — pick one for its own profile.',
  meta:'PBS External Trade Statistics — trade by partner country.'}];
const TOPIC_GROUPS=[
 {label:null,keys:['all']},
 {label:'What & how much',keys:['basket','movers','overtime']},
 {label:'With whom',keys:['partners']}];
const TOPIC_DRAWS={basket:()=>{drawDrill();drawProducts();},movers:()=>drawMovers(),overtime:()=>drawTotals(),partners:()=>{drawPartners();drawCountry();}};
const drawAll=()=>Object.values(TOPIC_DRAWS).forEach(f=>f());
let topic='basket',tCountry='all',tLevel='section',mDir='export',mMeasure='abs',mMode='span',mWinIdx=0,applyingHash=false;
const LEVEL_LABEL={section:'HS section',chapter:'HS chapter',product:'8-digit product'};

function start(){
 if(!window.ECON){return setTimeout(start,30);}
 E=window.ECON;D.ser={};E.indicators.series.forEach(s=>D.ser[s.key]=s);
 TX=E.trade_extra||{sel:[],country:{},movers:{},meta:{}};
 initDrill();initPartners();initProducts();drawTotals();initMovers();initCountry();initCsv();initTopics();
 let rt;window.addEventListener('resize',()=>{clearTimeout(rt);rt=setTimeout(()=>{if(topic==='all')drawAll();else if(TOPIC_DRAWS[topic])TOPIC_DRAWS[topic]();},150);});
}
function lastPt(k){const s=D.ser[k];return s&&s.points.length?s.points[s.points.length-1]:null;}
function initTopics(){
 const list=d3.select('#topicList');list.selectAll('*').remove();
 TOPIC_GROUPS.forEach((g,gi)=>{
  if(g.label)list.append('div').attr('class','topic-group'+(gi===0?' first':'')).text(g.label);
  g.keys.forEach(k=>{const t=TOPICS.find(x=>x.k===k);if(!t)return;
   list.append('button').attr('class','topic-item'+(k==='all'?' all':'')).attr('data-k',k)
    .html(`<span class="t-dot"></span>${t.label}`).on('click',()=>applyTopic(k,true));});
 });
 window.addEventListener('hashchange',()=>{if(!applyingHash)applyStateFromHash();});
 window.addEventListener('popstate',()=>{if(!applyingHash)applyStateFromHash();});
 initShare();applyStateFromHash();
}
function writeHash(push){
 if(applyingHash)return;
 const p=new URLSearchParams();p.set('t',topic);
 if(tCountry!=='all')p.set('c',tCountry);
 if(topic==='movers'||topic==='partners'||topic==='all'){if(tLevel!=='section')p.set('lvl',tLevel);}
 if(topic==='movers'||topic==='all'){if(mDir!=='export')p.set('md',mDir);if(mMeasure!=='abs')p.set('mm',mMeasure);if(mMode!=='span')p.set('mw',(moverWindows()[mWinIdx]||{}).key||'');}
 const hv='#'+p.toString();
 try{if(push&&history.pushState)history.pushState(null,'',hv);else if(history.replaceState)history.replaceState(null,'',hv);else location.hash=hv;}catch(e){location.hash=hv;}
}
function readHash(){let h='';try{h=decodeURIComponent(location.hash.replace(/^#/,''));}catch(e){h=location.hash.replace(/^#/,'');}
 if(!h)return {t:'basket'};if(!h.includes('='))return {t:h};const o={};new URLSearchParams(h).forEach((v,k)=>o[k]=v);if(!o.t)o.t='basket';return o;}
function applyStateFromHash(){
 const o=readHash();const k=TOPICS.some(t=>t.k===o.t)?o.t:'basket';
 applyingHash=true;
 applyTopic(k,false);
 if(o.lvl){tLevel=o.lvl;d3.select('#tLevel').property('value',o.lvl);}
 if(o.md){mDir=o.md;d3.selectAll('#mDir button').classed('on',function(){return this.dataset.d===o.md;});}
 if(o.mm){mMeasure=o.mm;d3.selectAll('#mMeasure button').classed('on',function(){return this.dataset.m===o.mm;});}
 if(o.mw){mMode='yoy';d3.selectAll('#mMode button').classed('on',function(){return this.dataset.w==='yoy';});d3.select('#mWinWrap').style('display',null);setMWin();const i=moverWindows().findIndex(w=>w.key===o.mw);if(i>=0){mWinIdx=i;d3.select('#mWin').property('value',i);d3.select('#mWinLbl').text((moverWindows()[i]||{}).label||'—');}}
 if(o.c){tCountry=o.c;d3.select('#tCountry').property('value',o.c);}
 applyingHash=false;
 if(TOPIC_DRAWS[k]&&k!=='all')TOPIC_DRAWS[k]();else if(k==='all')drawAll();
 drawCountry();
 writeHash(false);
 if(o.at){const el=document.getElementById(o.at);if(el)setTimeout(()=>el.scrollIntoView({behavior:'smooth',block:'start'}),140);}
}
const SHARE_ICON='<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2"><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.6" y1="10.7" x2="15.4" y2="6.3"/><line x1="8.6" y1="13.3" x2="15.4" y2="17.7"/></svg>';
const CHECK_ICON='<svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.6"><polyline points="20 6 9 17 4 12"/></svg>';
function initShare(){
 document.querySelectorAll('.card[id]').forEach(card=>{
  if(card.querySelector('.sharebtn'))return;
  const hasCsv=!!card.querySelector('.csvbtn');
  const btn=document.createElement('button');btn.className='sharebtn';btn.type='button';
  btn.title='Copy a link to this exact view';btn.innerHTML=SHARE_ICON+'Share';btn.style.right=hasCsv?'76px':'14px';
  btn.addEventListener('click',e=>{e.stopPropagation();shareCard(card.id,btn);});
  card.appendChild(btn);
 });
}
function shareCard(cardId,btn){
 writeHash(false);
 const p=new URLSearchParams(location.hash.replace(/^#/,''));if(cardId)p.set('at',cardId);
 const url=location.origin+location.pathname+location.search+'#'+p.toString();
 const done=()=>{const o=btn.innerHTML;btn.classList.add('ok');btn.innerHTML=CHECK_ICON+'Copied';setTimeout(()=>{btn.classList.remove('ok');btn.innerHTML=o;},1400);};
 const fb=()=>{try{const t=document.createElement('textarea');t.value=url;t.style.position='fixed';t.style.opacity='0';document.body.appendChild(t);t.select();document.execCommand('copy');t.remove();done();}catch(e){window.prompt('Copy this link:',url);}};
 if(navigator.clipboard&&navigator.clipboard.writeText)navigator.clipboard.writeText(url).then(done,fb);else fb();
}
function applyTopic(k,push){
 topic=k;const t=TOPICS.find(x=>x.k===k);
 d3.selectAll('#topicList .topic-item').classed('on',function(){return this.dataset.k===k;});
 d3.select('#topicDesc').text(t.desc);
 d3.select('#topicMeta').html('<b>Sources.</b> '+t.meta);
 const all=k==='all';
 d3.selectAll('[data-topic]').classed('topic-hidden',function(){return !all&&this.dataset.topic!==k;});
 // topic-relevant sidebar controls
 d3.select('#sideCountry').style('display',(k==='partners'||all)?null:'none');
 d3.select('#sideLevel').style('display',(k==='movers'||k==='partners'||all)?null:'none');
 if(!applyingHash)writeHash(push);
 if(all)drawAll();else if(TOPIC_DRAWS[k])TOPIC_DRAWS[k]();
 if(push)window.scrollTo({top:0,behavior:'smooth'});
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
 const W=el.node().clientWidth||900,H=Math.max(320,Math.min(430,W*0.42));
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

/* ================= movers: risers & fallers ================= */
function moverWindows(){return ((TX.meta.windows||{})[mDir]||[]).filter(w=>w.key!=='span');}
function currentWindow(){
 if(mMode==='span')return ((TX.meta.windows||{})[mDir]||[]).find(w=>w.key==='span')||{key:'span'};
 const yoy=moverWindows();return yoy[Math.min(mWinIdx,yoy.length-1)]||yoy[yoy.length-1]||{key:'span'};
}
function moverList(){
 const lvl=(TX.movers[mDir]||{})[tLevel]||{span:[],yoy:{}};
 return mMode==='span'?(lvl.span||[]):((lvl.yoy||{})[currentWindow().key]||[]);
}
function setMWin(){
 const yoy=moverWindows();
 const s=d3.select('#mWin');s.attr('min',0).attr('max',Math.max(0,yoy.length-1));
 mWinIdx=Math.min(mWinIdx,yoy.length-1);s.property('value',mWinIdx);
 d3.select('#mWinLbl').text((yoy[mWinIdx]||{}).label||'—');
}
function initMovers(){
 d3.selectAll('#mDir button').on('click',function(){d3.selectAll('#mDir button').classed('on',false);d3.select(this).classed('on',true);mDir=this.dataset.d;setMWin();drawMovers();writeHash();});
 d3.selectAll('#mMeasure button').on('click',function(){d3.selectAll('#mMeasure button').classed('on',false);d3.select(this).classed('on',true);mMeasure=this.dataset.m;drawMovers();writeHash();});
 d3.selectAll('#mMode button').on('click',function(){d3.selectAll('#mMode button').classed('on',false);d3.select(this).classed('on',true);mMode=this.dataset.w;
   d3.select('#mWinWrap').style('display',mMode==='yoy'?null:'none');setMWin();drawMovers();writeHash();});
 d3.select('#mWin').on('input',function(){mWinIdx=+this.value;d3.select('#mWinLbl').text((moverWindows()[mWinIdx]||{}).label||'—');drawMovers();writeHash();});
 d3.select('#tLevel').on('change',function(){tLevel=this.value;drawMovers();drawCountry();writeHash();});
 // default the year-on-year timeline to the most recent pair
 mWinIdx=Math.max(0,moverWindows().length-1);setMWin();
}
function moverRows(){
 const arr=moverList().slice();
 if(mMeasure==='pct'){
  const el=arr.filter(r=>r.pct!=null);
  const up=el.filter(r=>r.pct>0).sort((a,b)=>b.pct-a.pct).slice(0,10);
  const dn=el.filter(r=>r.pct<0).sort((a,b)=>a.pct-b.pct).slice(0,10);
  return up.concat(dn.reverse()).map(r=>({...r,val:r.pct}));
 }
 const up=arr.filter(r=>r.delta>0).sort((a,b)=>b.delta-a.delta).slice(0,10);
 const dn=arr.filter(r=>r.delta<0).sort((a,b)=>a.delta-b.delta).slice(0,10);
 return up.concat(dn.reverse()).map(r=>({...r,val:r.delta}));
}
function drawMovers(){
 const el=d3.select('#movers');el.selectAll('*').remove();
 const rows=moverRows();const GREEN='#1e6b3e',RED='#c0392b';
 const isPct=mMeasure==='pct';
 const fmtV=v=>isPct?((v>=0?'+':'−')+Math.abs(v).toFixed(0)+'%'):((v>=0?'+':'−')+fmtBn(Math.abs(v)));
 if(!rows.length){el.append('div').style('padding','24px').style('color','var(--slate-500)').text('No data at this level.');return;}
 const W=el.node().clientWidth||900,rh=22,H=rows.length*rh+30;
 const lblW=Math.max(130,Math.min(230,W*0.3));
 const minV=Math.min(0,d3.min(rows,r=>r.val)),maxV=Math.max(0,d3.max(rows,r=>r.val));
 const neg=minV<0?Math.max(24,(W-lblW)*0.16):0;
 const x=d3.scaleLinear().domain([minV,maxV]).range([lblW+12+neg,W-70]);
 const zero=x(0);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 svg.append('line').attr('x1',zero).attr('x2',zero).attr('y1',4).attr('y2',H-24).attr('stroke','var(--slate-400)');
 const g=svg.selectAll('g').data(rows).join('g').attr('transform',(d,i)=>`translate(0,${i*rh+4})`)
  .on('mousemove',(e,d)=>showTip(`<b>${d.name}</b>${tLevel==='product'?' · HS '+d.code:''}<br>${d.y0}: ${fmtRs(d.v0)} → ${d.y1}: ${fmtRs(d.v1)}<br><b>${(d.delta>=0?'+':'−')+fmtRs(Math.abs(d.delta))}</b>${d.pct!=null?`  ·  ${d.pct>=0?'+':''}${d.pct.toFixed(0)}%`:''}`,e)).on('mouseleave',hideTip);
 g.append('text').attr('x',lblW).attr('y',rh/2-2).attr('dy','.32em').attr('text-anchor','end').attr('font-size',11).attr('font-weight',600)
  .attr('fill',d=>d.val<0?RED:'var(--slate-700)')
  .text(d=>{const max=Math.floor(lblW/6.1);return d.name.length>max?d.name.slice(0,max-1)+'…':d.name;});
 g.append('rect').attr('y',3).attr('height',rh-9).attr('rx',3)
  .attr('x',d=>Math.min(zero,x(d.val))).attr('width',d=>Math.max(1,Math.abs(x(d.val)-zero)))
  .attr('fill',d=>d.val<0?RED:GREEN);
 g.append('text').attr('y',rh/2-2).attr('dy','.32em').attr('font-size',10.5).attr('font-weight',700).attr('text-anchor','start')
  .attr('x',d=>d.val>=0?x(d.val)+5:zero+5).attr('fill',d=>d.val>=0?GREEN:RED).text(d=>fmtV(d.val));
 const w=currentWindow();
 d3.select('#moversMeta').text(`${mDir==='export'?'Exports':'Imports'} · ${LEVEL_LABEL[tLevel]} · biggest ${isPct?'% movers':'rupee movers'} · ${mMode==='span'?'full span ':''}${w.label||''}`);
}

/* ================= country detail ================= */
function initCountry(){
 const sel=d3.select('#tCountry');
 sel.append('option').attr('value','all').text('All partners');
 (TX.sel||[]).forEach(c=>sel.append('option').attr('value',c.name).text(c.name));
 sel.on('change',function(){tCountry=this.value;drawCountry();writeHash();});
}
function drawCountry(){
 const card=document.getElementById('sec-country');
 const show=tCountry!=='all'&&TX.country&&TX.country[tCountry];
 if(card)card.style.display=show?'':'none';
 if(!show)return;
 const c=TX.country[tCountry];
 d3.select('#cName').text(tCountry);
 // over-time line: exp & imp
 const years=Object.keys(c.series).sort();
 const el=d3.select('#cSeries');el.selectAll('*').remove();
 const W=el.node().clientWidth||900,H=230,m={t:14,r:52,b:26,l:52};
 const x=d3.scalePoint().domain(years).range([m.l,W-m.r]).padding(.5);
 const vals=[];years.forEach(y=>['exp','imp'].forEach(k=>{if(c.series[y][k]!=null)vals.push(c.series[y][k]);}));
 const y=d3.scaleLinear().domain([0,d3.max(vals)||1]).nice().range([H-m.b,m.t]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 svg.append('g').attr('transform',`translate(0,${H-m.b})`).attr('class','axis').call(d3.axisBottom(x).tickValues(years.filter((d,i)=>i%2===0)));
 svg.append('g').attr('transform',`translate(${m.l},0)`).attr('class','axis').call(d3.axisLeft(y).ticks(5).tickFormat(fmtBn)).call(g=>g.selectAll('.tick line').clone().attr('x2',W-m.r-m.l).attr('class','gl'));
 [['exp','Pakistan’s exports',scolor('XI')],['imp','Pakistan’s imports','#c0392b']].forEach(s=>{
  const pts=years.map(yr=>({y:yr,v:c.series[yr][s[0]]})).filter(p=>p.v!=null);
  const line=d3.line().x(p=>x(p.y)).y(p=>y(p.v));
  svg.append('path').datum(pts).attr('fill','none').attr('stroke',s[2]).attr('stroke-width',2.5).attr('d',line);
  svg.selectAll(null).data(pts).join('circle').attr('cx',p=>x(p.y)).attr('cy',p=>y(p.v)).attr('r',3).attr('fill',s[2]).attr('stroke','#fff').attr('stroke-width',1)
   .on('mousemove',(e,p)=>showTip(`<b>${s[1]}</b> · ${p.y}<br>${fmtRs(p.v)}`,e)).on('mouseleave',hideTip);
  const lp=pts.slice(-1)[0];if(lp)svg.append('text').attr('x',x(lp.y)+7).attr('y',y(lp.v)+4).attr('font-size',11).attr('font-weight',700).attr('fill',s[2]).text(s[0]==='exp'?'exports':'imports');
 });
 // top items at chosen level
 const lvl=(c.level&&c.level[tLevel])||{exp:[],imp:[]};
 chbar('#cExp',lvl.exp||[],scolor('XI'));
 chbar('#cImp',lvl.imp||[],'#c0392b');
 const lat=TX.meta.latest||{};
 d3.select('#cMeta').text(`${LEVEL_LABEL[tLevel]} breakdown · exports ${lat.export||''}, imports ${lat.import||''} · PBS 8-digit trade`);
}
function chbar(elSel,rows,color){
 const el=d3.select(elSel);el.selectAll('*').remove();rows=rows.slice(0,10);
 if(!rows.length){el.append('div').style('padding','12px').style('color','var(--slate-400)').style('font-size','12px').text('No recorded trade at this level.');return;}
 const W=el.node().clientWidth||440,rh=23,H=rows.length*rh+8,lblW=Math.max(96,Math.min(180,W*0.46));
 const x=d3.scaleLinear().domain([0,d3.max(rows,d=>d.bn)||1]).range([lblW+6,W-54]);
 const svg=el.append('svg').attr('width',W).attr('height',H).style('display','block');
 const g=svg.selectAll('g').data(rows).join('g').attr('transform',(d,i)=>`translate(0,${i*rh+3})`)
  .on('mousemove',(e,d)=>showTip(`<b>${d.name}</b>${/^\d/.test(d.code)?' · HS '+d.code:''}<br>${fmtRs(d.bn)}`,e)).on('mouseleave',hideTip);
 g.append('text').attr('x',lblW).attr('y',rh/2).attr('dy','.32em').attr('text-anchor','end').attr('font-size',10.5).attr('font-weight',600).attr('fill','var(--slate-700)')
  .text(d=>{const max=Math.floor(lblW/5.9);return d.name.length>max?d.name.slice(0,max-1)+'…':d.name;});
 g.append('rect').attr('x',x(0)).attr('y',3).attr('height',rh-9).attr('rx',3).attr('width',d=>Math.max(1,x(d.bn)-x(0))).attr('fill',color);
 g.append('text').attr('x',d=>x(d.bn)+5).attr('y',rh/2).attr('dy','.32em').attr('font-size',10).attr('font-weight',700).attr('fill','var(--slate-600)').text(d=>fmtBn(d.bn));
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
const r2=v=>v==null?null:Math.round(v*100)/100;
const CSV={
 drill:()=>{const yr=dYears[dYi],secs=E.tree[dDir][yr]||[],out=[];
   secs.forEach(s=>s.chapters.forEach(c=>c.products.forEach(p=>out.push({
     direction:dDir,year:yr,section:s.section,section_name:s.name,chapter_code:c.code,chapter:c.name,hs8:p.hs8,product:p.name,rs_bn:r2(p.bn)}))));
   return [`Pakistan_trade_${dDir}_products_${yr}.csv`,out.sort((a,b)=>b.rs_bn-a.rs_bn)];},
 products:()=>{const ys=prYears(),yr=ys[+d3.select('#prYr').property('value')||ys.length-1];
   return [`Pakistan_top_products_${prDir}_${yr}.csv`,(E.products[prDir][yr]||[]).map(p=>({direction:prDir,year:yr,product:p.name,section:p.section,rs_bn:r2(p.bn)}))];},
 totals:()=>{const T=E.totals;
   return ['Pakistan_trade_over_time.csv',T.years.map(y=>({year:y,exports_rs_bn:r2(T.export[y]),imports_rs_bn:r2(T.import[y]),balance_rs_bn:r2(T.balance[y])}))];},
 partners:()=>{const ys=pYears(),yr=ys[+d3.select('#pYr').property('value')||ys.length-1];
   return [`Pakistan_top_partners_${pDir}_${yr}.csv`,(E.partners[pDir][yr]||[]).map(p=>({direction:pDir,year:yr,partner:p.country,rs_bn:r2(p.bn)}))];},
 movers:()=>{const arr=moverList(),w=currentWindow();
   return [`Pakistan_trade_movers_${mDir}_${tLevel}_${w.key}.csv`,arr.slice().sort((a,b)=>b.delta-a.delta).map(r=>({
     direction:mDir,level:tLevel,window:w.label||w.key,code:r.code,name:r.name,[`rs_bn_${r.y0}`]:r.v0,[`rs_bn_${r.y1}`]:r.v1,change_rs_bn:r.delta,pct_growth:r.pct}))];},
 country:()=>{const c=TX.country&&TX.country[tCountry];if(!c)return ['',[]];
   const out=[];Object.keys(c.series).sort().forEach(y=>out.push({country:tCountry,year:y,exports_rs_bn:c.series[y].exp??null,imports_rs_bn:c.series[y].imp??null}));
   return [`Pakistan_trade_with_${tCountry}.csv`,out];}
};
function initCsv(){
 d3.selectAll('.csvbtn').on('click',function(e){
  e.stopPropagation();const k=this.dataset.csv,fn=CSV[k];if(!fn)return;
  try{const [name,rows]=fn();downloadCSV(name,rows);}catch(err){console.error('CSV',k,err);}
 });
}

if(document.readyState!=='loading')start();else document.addEventListener('DOMContentLoaded',start);
