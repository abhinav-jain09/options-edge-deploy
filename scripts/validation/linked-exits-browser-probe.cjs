'use strict';
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),{spawnSync}=require('node:child_process'),{pathToFileURL}=require('node:url');
const parts=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date()).map(p=>[p.type,p.value]));
if(os.platform()!=='linux'||process.env.VISUAL_BENCH_HOST!=='es4'||!Object.values(os.networkInterfaces()).flat().some(a=>a?.address==='192.168.100.4')||!['Sat','Sun'].includes(parts.weekday)&&Number(parts.hour)*60+Number(parts.minute)>=570&&Number(parts.hour)*60+Number(parts.minute)<960)throw Error('PERMITTED_HOST_OUTSIDE_RTH_REQUIRED');
const dir=fs.mkdtempSync(path.join(os.tmpdir(),'linked-browser-'));
try{
 for(const variant of ['baseline','candidate'])for(const feature of [false,true]){
  if(variant==='baseline'&&feature)continue;
  const source=path.join(process.argv[2],variant),inline=name=>fs.readFileSync(path.join(source,name),'utf8').replace(/<\/script/gi,'<\\/script');
  const html=`<!doctype html><meta charset="utf-8"><style>${inline('context-tape-orders.css')}</style><div id="chart" style="position:relative;width:1200px;height:620px"></div><pre id="result"></pre><script>${inline('context-tape-orders-model.js')}</script><script>${inline('context-tape-orders-overlay.js')}</script><script>
 let callback;window.requestAnimationFrame=f=>{callback=f;return 1;};window.cancelAnimationFrame=()=>{};
 const chart=document.querySelector('#chart'),overlay=ContextTapeOrdersOverlay.mount(chart),g={left:0,right:1000,top:0,bottom:600,priceTop:7800,priceBottom:7500,y:p=>(7800-p)*2};
 const feature=${feature},items=Array.from({length:16},(_,i)=>({id:'position:trade-'+i,geometry:{strategy:'bearcall',strike:7550+i*10,wing:10},locked:true,...(feature?{visual:i%3===0?'closing':'filled',status:'Stop loss closing',exits:{stopLevel:String(7570+i*10),profitLevel:String(7540+i*10)},linkedPlan:{trigger:{purpose:'STOP'}}}:{})}));
 const samples=[];for(let i=0;i<250;i++){const at=performance.now();overlay.paint({geometry:g,items,selected:items[0].id});callback();chart.offsetHeight;if(i>=50)samples.push(performance.now()-at);}
 samples.sort((a,b)=>a-b);const p99=samples[Math.floor(samples.length*.99)],mean=samples.reduce((a,b)=>a+b)/samples.length;
 const threads=chart.querySelectorAll('.mm-exit-thread').length,labels=chart.querySelectorAll('.mm-exit-label').length,shades=chart.querySelectorAll('.mm-order-shade').length;
 const pass=p99<=16&&shades===16&&(!feature||threads===32&&labels===32)&&!document.querySelector('svg');
 document.querySelector('#result').textContent=JSON.stringify({probe:feature?'linked-overlay':'existing-overlay',variant,records:200,elapsedMs:mean,sampleP99Ms:p99,budgetMs:16,withinBudget:pass,threads,labels,shades});
 overlay.dispose();</script>`;
  const file=path.join(dir,variant+'-'+feature+'.html');fs.writeFileSync(file,html);
  const result=spawnSync('/usr/bin/google-chrome',['--headless=new','--no-sandbox','--disable-gpu','--disable-background-networking','--host-resolver-rules=MAP * ~NOTFOUND','--no-proxy-server','--user-data-dir='+path.join(dir,variant+'-'+feature), '--dump-dom',pathToFileURL(file).href],{encoding:'utf8',timeout:60000,maxBuffer:2*1024*1024});
  if(result.error||result.status!==0)throw Error('Browser probe failed: '+(result.error?.message||result.stderr));
  const raw=/<pre id="result">([^<]+)<\/pre>/.exec(result.stdout);if(!raw)throw Error('No completed browser measurement');const row=JSON.parse(raw[1].replace(/&quot;/g,'"').replace(/&amp;/g,'&'));console.log('VISUAL_PROBE '+JSON.stringify(row));if(!row.withinBudget)throw Error('Overlay latency or semantic fixture failed');
 }
}finally{fs.rmSync(dir,{recursive:true,force:true});}
