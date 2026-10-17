'use strict';
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),{pathToFileURL}=require('node:url'),runPage=require('./headless-overlay-session.cjs');
const parts=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date()).map(p=>[p.type,p.value]));
if(os.platform()!=='linux'||process.env.VISUAL_BENCH_HOST!=='es4'||!Object.values(os.networkInterfaces()).flat().some(a=>a?.address==='192.168.100.4')||!['Sat','Sun'].includes(parts.weekday)&&Number(parts.hour)*60+Number(parts.minute)>=570&&Number(parts.hour)*60+Number(parts.minute)<960)throw Error('PERMITTED_HOST_OUTSIDE_RTH_REQUIRED');
(async()=>{
 const dir=fs.mkdtempSync(path.join(os.tmpdir(),'linked-browser-'));let failed=false;
 try{
  for(const variant of ['baseline','candidate'])for(const feature of [false,true]){
   if(variant==='baseline'&&feature)continue;
   const source=path.join(process.argv[2],variant),inline=name=>fs.readFileSync(path.join(source,name),'utf8').replace(/<\/script/gi,'<\\/script');
   const html=`<!doctype html><meta charset="utf-8"><style>${inline('context-tape-orders.css')}</style><div id="chart" style="position:relative;width:1200px;height:620px"><svg id="candles"><rect width="3" height="10"></rect></svg></div><script>${inline('context-tape-orders-model.js')}</script><script>${inline('context-tape-orders-overlay.js')}</script><script>
window.measure=async()=>{
 const chart=document.querySelector('#chart'),candles=document.querySelector('#candles'),candleContent=candles.innerHTML,candleChild=candles.firstElementChild,overlay=ContextTapeOrdersOverlay.mount(chart),g={left:0,right:1000,top:0,bottom:600,priceTop:7800,priceBottom:7500,y:p=>(7800-p)*2};
 const feature=${feature},variant=${JSON.stringify(variant)},items=Array.from({length:16},(_,i)=>({id:'position:trade-'+i,geometry:{strategy:'bearcall',strike:7550+i*10,wing:10},locked:true,...(feature?{visual:i%3===0?'closing':'filled',status:'Stop loss closing',exits:{stopLevel:String(7570+i*10),profitLevel:String(7540+i*10)},linkedPlan:{trigger:{purpose:'STOP'}}}:{})}));
 const samples=[],renderSamples=[],layoutSamples=[],nativeFrame=window.requestAnimationFrame.bind(window);let complete,iteration=0,nativeFrames=0,lastTimestamp=-1,distinctFrameTimestamps=true,frameError;
 // Measure callback + forced layout cost on actual distinct browser frames. Scheduling wait
 // is excluded exactly as in the earlier callback-cost metric; no virtual clock is used.
 window.requestAnimationFrame=callback=>nativeFrame(timestamp=>{const at=performance.now();try{callback(timestamp);const rendered=performance.now();chart.offsetHeight;const laidOut=performance.now();nativeFrames++;if(timestamp<=lastTimestamp)distinctFrameTimestamps=false;lastTimestamp=timestamp;if(iteration>=50){samples.push(laidOut-at);renderSamples.push(rendered-at);layoutSamples.push(laidOut-rendered);}}catch(error){frameError=error;}finally{complete();}});
 try{
  for(let i=0;i<250;i++){iteration=i;for(let n=0;n<items.length;n++){items[n].geometry.strike=7550+n*10+(i%2?0.1:0);if(feature){items[n].visual=i%2?'closing':'filled';items[n].exits.stopLevel=String(7570+n*10+(i%2?0.1:0));}}await new Promise(resolve=>{complete=resolve;overlay.paint({geometry:g,items,selected:items[0].id});});if(frameError)throw frameError;}
  const tail=values=>values.sort((a,b)=>a-b)[Math.floor(values.length*.99)],p99=tail(samples),mean=samples.reduce((a,b)=>a+b)/samples.length;
  const threads=chart.querySelectorAll('.mm-exit-thread').length,labels=chart.querySelectorAll('.mm-exit-label').length,shades=chart.querySelectorAll('.mm-order-shade').length,sameCandle=candles===document.querySelector('#candles')&&candles.innerHTML===candleContent&&candles.firstElementChild===candleChild;
  const pass=p99<=16&&samples.length===200&&nativeFrames===250&&distinctFrameTimestamps&&sameCandle&&shades===16&&(!feature||threads===32&&labels===32);
  return {probe:feature?'linked-overlay':'existing-overlay',variant,records:samples.length,elapsedMs:mean,sampleP99Ms:p99,renderP99Ms:tail(renderSamples),layoutP99Ms:tail(layoutSamples),budgetMs:16,withinBudget:pass,threads,labels,shades,sameCandle,frameScheduler:'native-requestAnimationFrame',nativeFrames,distinctFrameTimestamps};
 }finally{overlay.dispose();}
};</script>`;
   const file=path.join(dir,variant+'-'+feature+'.html');fs.writeFileSync(file,html);
   const row=await runPage('/usr/bin/google-chrome',pathToFileURL(file).href,path.join(dir,variant+'-'+feature));console.log('VISUAL_PROBE '+JSON.stringify(row));if(!row.withinBudget)failed=true;
  }
  if(failed)throw Error('Overlay latency or semantic fixture failed; every scenario remains reported');
 }finally{fs.rmSync(dir,{recursive:true,force:true});}
})().catch(error=>{console.error(error.stack);process.exitCode=1;});
