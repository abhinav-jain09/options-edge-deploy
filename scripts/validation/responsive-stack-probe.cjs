/* TEST ONLY: actual assembled app, isolated HTTP/SSE fake, native Chromium clocks/frames. */
'use strict';
const fs=require('node:fs'),os=require('node:os'),path=require('node:path'),http=require('node:http'),assert=require('node:assert/strict');
function permitted(){
 const p=Object.fromEntries(new Intl.DateTimeFormat('en-US',{timeZone:'America/New_York',weekday:'short',hour:'2-digit',minute:'2-digit',hourCycle:'h23'}).formatToParts(new Date()).map(x=>[x.type,x.value]));const t=+p.hour*60+(+p.minute);
 assert.ok(os.platform()==='linux'&&process.env.RESPONSIVE_BENCH_HOST==='es4'&&Object.values(os.networkInterfaces()).flat().some(a=>a?.address==='192.168.100.4'),'ES4_ONLY');
 assert.equal(fs.readFileSync('/proc/self/status','utf8').match(/^Cpus_allowed_list:\s*(.*)$/m)?.[1],'2','CPU2_ONLY');
 assert.ok(['Sat','Sun'].includes(p.weekday)||t<570||t>=960,'OUTSIDE_RTH_REQUIRED');
}
permitted();
const {chromium}=require('playwright-core'),[source,fixtureSource,variant,countText,repeatText]=process.argv.slice(2),count=+countText,repeat=+repeatText;
assert.ok(['baseline','candidate'].includes(variant)&&[390,1439].includes(count)&&repeat>=1&&repeat<=5);
const {createFixture}=require(path.join(fixtureSource,'test/browser/helpers/trade-workspace-fixture.cjs')),{capacitySession}=require(path.join(fixtureSource,'test/browser/helpers/capacity-session.cjs'));
const decoded=capacitySession(count),fixture=createFixture({session:decoded.session,assetsDir:path.join(source,'src/app/web/assets'),lifetimeMs:3600000,pageSize:128,transformBundle:text=>{
 const anchor="if(type==='execution-readiness'){";
 assert.equal(text.split(anchor).length,2,'MEASUREMENT_ACK_ANCHOR_UNIQUE');
 return text.replace(anchor,anchor+'global.__probeReadinessSequence=value.probeSequence;');
}});
// Exactly4096 accepted book records,16 active positions/plans. Terminal history is
// retained by the actual snapshot decoder, never silently omitted from the book.
const prototype=fixture.positions.values().next().value;fixture.positions.clear();
for(let i=0;i<4096;i++){
 const id='capacity-'+i,strike=6005+(i%16)*5,active=i<16,entry={...prototype.entryTicket,intentId:id,reviewId:'entry-'+id,lastTradingCutoffMs:fixture.clock()+3600000,legs:[{strike:String(strike),action:'SELL',right:'PUT',conId:strike},{strike:String(strike-5),action:'BUY',right:'PUT',conId:strike-5}]};
 const row={...prototype,id,entryTicket:entry,state:active?'OPEN':'CLOSED',openQuantity:active?100:0,closeableQuantity:active?100:0,allowedActions:active?['UPDATE','CLOSE']:[]};
 if(active){const plan={positionRef:id,version:1,state:'ARMED',stopLevel:String(strike-10),profitLevel:String(strike+10),stopQuantityMode:'FULL',profitQuantityMode:'FULL',stopInstructionState:'ARMED',profitInstructionState:'ARMED',stopCoverageQuantity:100,profitCoverageQuantity:100,stopConsumedQuantity:0,profitConsumedQuantity:0,sequentialCloseAuthority:{STOP:2,PROFIT:2},policyRevision:'linked-exit-touch-beyond-mid-limit-v1',quantityPolicyRevision:'POSITION_QUANTITY_V1'};fixture.plans.set(id,plan);row.linkedExit=plan;}
 fixture.positions.set(id,row);
}
const guard=http.createServer((req,res)=>{res.writeHead(403);res.end('Non-fixture network denied');});guard.on('connect',(req,socket)=>socket.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'));
const results={variant,count,repeat,classification:decoded.classification,assetHashes:fixture.assetHashes,bookRecords:4096,activePositions:16,linkedPlans:16,paths:{},memory:{},errors:[]};
let browserServer,browser,context;
for(const signal of ['SIGTERM','SIGINT'])process.once(signal,()=>{
 const timer=setTimeout(()=>process.exit(143),10000);timer.unref();
 Promise.resolve(browserServer?.kill()).finally(()=>process.exit(143));
});
const samplesStats=s=>{assert.equal(s.length,200);assert.ok(s.every(x=>Number.isFinite(x)&&x>=0));const sorted=[...s].sort((a,b)=>a-b);return {records:200,samples:s,meanMs:s.reduce((a,b)=>a+b,0)/200,p99Ms:sorted[198]};};
async function frames(page,n=2){await page.evaluate(n=>new Promise(resolve=>{let left=n;const next=()=>{if(--left<=0)resolve();else window.__nativeFrame(next);};window.__nativeFrame(next);}),n);}
async function taskTime(cdp){const {metrics}=await cdp.send('Performance.getMetrics'),value=metrics.find(x=>x.name==='TaskDuration')?.value;assert.ok(Number.isFinite(value)&&value>=0,'MANDATORY_TASK_DURATION');return value*1000;}
async function measurePath(page,cdp,name,operation){const samples=[];for(let i=0;i<250;i++){if(i%50===0)permitted();const start=await taskTime(cdp);await operation(i);await frames(page);const delta=(await taskTime(cdp))-start;if(i>=50)samples.push(delta);}results.paths[name]=samplesStats(samples);}
function processPss(root){
 const rows=new Map();for(const name of fs.readdirSync('/proc'))if(/^\d+$/.test(name)){try{const text=fs.readFileSync('/proc/'+name+'/status','utf8'),ppid=+text.match(/^PPid:\s*(\d+)/m)[1],uid=+text.match(/^Uid:\s*(\d+)/m)[1];rows.set(+name,{ppid,uid});}catch(e){if(!['ENOENT','ESRCH'].includes(e.code))throw e;}}
 assert.equal(rows.get(root)?.uid,process.getuid(),'OWN_BROWSER_PID_REQUIRED');const owned=new Set([root]);let changed=true;while(changed){changed=false;for(const [pid,r]of rows)if(owned.has(r.ppid)&&!owned.has(pid)){assert.equal(r.uid,process.getuid());owned.add(pid);changed=true;}}
 let bytes=0;for(const pid of owned){try{const pss=fs.readFileSync('/proc/'+pid+'/smaps_rollup','utf8').match(/^Pss:\s*(\d+) kB$/m);assert.ok(pss,'MANDATORY_PSS');bytes+=+pss[1]*1024;}catch(e){if(!['ENOENT','ESRCH'].includes(e.code))throw e;}}assert.ok(bytes>0);return {bytes,pids:[...owned]};
}
async function memory(){
 const seen=new Set();let bytes=0;for(const p of context.pages()){if(p.isClosed())continue;const c=await context.newCDPSession(p);try{const {id}=await c.send('Runtime.getIsolateId');assert.ok(id,'MANDATORY_ISOLATE_ID');if(seen.has(id))continue;seen.add(id);await c.send('HeapProfiler.collectGarbage');const heap=await c.send('Runtime.getHeapUsage');assert.ok(Number.isFinite(heap.usedSize)&&heap.usedSize>0,'MANDATORY_HEAP');bytes+=heap.usedSize;}finally{await c.detach();}}
 return {heapBytes:bytes,isolates:[...seen],pss:processPss(browserServer.process().pid)};
}
(async()=>{
 try{
  await new Promise(r=>guard.listen(0,'127.0.0.1',r));await new Promise(r=>fixture.server.listen(0,'127.0.0.1',r));
  browserServer=await chromium.launchServer({executablePath:'/usr/bin/google-chrome',headless:true,proxy:{server:'http://127.0.0.1:'+guard.address().port,bypass:'127.0.0.1,localhost'},args:['--disable-renderer-backgrounding','--disable-background-timer-throttling']});browser=await chromium.connect(browserServer.wsEndpoint());context=await browser.newContext({viewport:{width:5120,height:1440}});const page=await context.newPage();
  page.on('pageerror',e=>results.errors.push(e.message));context.on('request',r=>{if(/^https?:/.test(r.url()))assert.equal(new URL(r.url()).hostname,'127.0.0.1','NON_FIXTURE_APP_REQUEST');});
  await page.evaluate(()=>{window.__nativeFrame=requestAnimationFrame.bind(window);});results.memory.empty=await memory();
  await context.addInitScript(({issuer,owner,time})=>{
   window.__nativeFrame=requestAnimationFrame.bind(window);if(location.protocol!=='http:')return;const started=performance.now();Date.now=()=>time+performance.now()-started;window.OptionChainAuth={whenReady:()=>Promise.resolve(),token:()=> 'h.'+btoa(JSON.stringify({sub:owner,iss:issuer,exp:time/1000+7200}))+'.s'};
   // Keep the native scheduler/clock. Measure only the actual stack DOM callback
   // and forced layout; no synthetic timer or virtual frame can satisfy this gate.
   const original=requestAnimationFrame.bind(window);window.__stackSamples=[];window.__stackStamps=[];window.requestAnimationFrame=callback=>original(ts=>{const measuring=window.__measureStack&&String(callback).includes('nextRect');const start=performance.now();callback(ts);if(measuring){document.body.offsetHeight;window.__stackSamples.push(performance.now()-start);window.__stackStamps.push(ts);}});
  },{issuer:fixture.issuer,owner:fixture.owner,time:fixture.clock()});
  await page.goto('http://127.0.0.1:'+fixture.server.address().port);await page.evaluate(session=>{window.__session=session;window.map=ContextTapeMarketMap.mount(document.querySelector('#contextTapeRoot'),{fetch:(url,options={})=>fetch(url,{...options,headers:{...options.headers,Authorization:'Bearer '+OptionChainAuth.token()}})});map.update({session,hidden:{}});map.show();},decoded.session);await page.waitForFunction(()=>document.querySelector('[data-order-dot]')?.classList.contains('is-ready'));
  await page.locator('[data-order-record]').selectOption('position:capacity-0');await page.locator('[data-controls]').click();await page.locator('[data-tf="1"]').click();await page.locator('[data-controls]').click();await page.locator('[data-session]').click();await frames(page);
  assert.equal(await page.locator('[data-order-record] option').count(),4097,'ACTUAL_BOOK_ACCEPTED');assert.equal(await page.locator('[data-chart] [data-candle-time]').count(),count,'ACTUAL_RENDERED_CAPACITY');assert.equal(fixture.streams.size,1,'ONE_PRIVATE_STREAM');
  const counts=await page.evaluate(()=>({shades:document.querySelectorAll('.mm-order-shade').length,threads:document.querySelectorAll('.mm-exit-thread').length,labels:document.querySelectorAll('.mm-exit-label').length}));assert.deepEqual(counts,{shades:16,threads:32,labels:32},'FULL_GEOMETRY_RETAINED');results.geometry=counts;
  if(variant==='candidate'){for(const key of ['pending','stop','profit']){const card=page.locator(`[data-stack-card="${key}"]`);if(!await card.getAttribute('open'))await card.evaluate(n=>n.open=true);}await frames(page);assert.equal(await page.locator('[data-stack-card]').count(),4);}
  results.memory.loaded=await memory();results.memory.retainedBytes=results.memory.loaded.heapBytes-results.memory.empty.heapBytes;
  const cdp=await context.newCDPSession(page);await cdp.send('Performance.enable');
  await measurePath(page,cdp,'full-chart-resize',i=>page.setViewportSize({width:5120+(i%2)*16,height:1440}));
  await measurePath(page,cdp,'map-snapshot',i=>page.evaluate(i=>map.update({session:{...__session,generatedAtMs:__session.generatedAtMs+i},hidden:{}}),i));
  await measurePath(page,cdp,'orders-render-health',async i=>{for(const stream of fixture.streams)stream.write('event: execution-readiness\ndata: '+JSON.stringify({probeSequence:i,account:'DUQ287805',mode:'PAPER',state:'HEALTHY',reasonCode:'CONNECTED',serviceInstanceId:'fixture',connectionGeneration:'generation',healthRevision:1,validForMs:15000,components:Object.fromEntries(['webToService','commandStore','dispatcher','reconciliation','brokerSession','managedAccount','brokerResponse','brokerUpstream'].map(k=>[k,true]))})+'\n\n');await page.waitForFunction(i=>window.__probeReadinessSequence===i,i,{polling:10});});
  if(variant==='candidate'){
   await page.setViewportSize({width:1920,height:1440});await frames(page);await page.locator('[data-stack-action="detach"]').click();await frames(page);
   // Narrow through the real keyboard grip to create room for native drag frames.
   const edge=page.locator('[data-stack-grip="resize"][data-direction="e"]');await edge.focus();for(let i=0;i<35;i++)await page.keyboard.press('Shift+ArrowLeft');await frames(page);
   for(const [kind,direction]of [['move','n'],['resize','e']]){
    const target=page.locator(`[data-stack-grip="${kind}"][data-direction="${direction}"]`),r=await target.boundingBox(),x=r.x+r.width/2,y=r.y+r.height/2;await page.mouse.move(x,y);await page.mouse.down();await page.evaluate(()=>{window.__stackSamples=[];window.__stackStamps=[];window.__measureStack=true;window.__beforeSvg=document.querySelector('[data-chart] svg');});
    for(let i=0;i<250;i++){if(i%50===0)permitted();await page.mouse.move(x+(i%2?4:8),y+(kind==='move'?(i%2?4:8):0));await page.waitForFunction(n=>window.__stackSamples.length>=n,i+1,{polling:10});}
    await page.evaluate(()=>window.__measureStack=false);await page.mouse.up();const observed=await page.evaluate(()=>({samples:__stackSamples,stamps:__stackStamps,sameSvg:__beforeSvg===document.querySelector('[data-chart] svg')}));assert.equal(observed.samples.length,250);assert.equal(new Set(observed.stamps).size,250);assert.ok(observed.sameSvg,'NO_CANDLE_REBUILD_FOR_FLOATING_GESTURE');results.paths['floating-'+kind]={...samplesStats(observed.samples.slice(50)),nativeFrames:250,distinctFrameTimestamps:true,sameSvg:true};
   }
   await page.locator('[data-stack-action="dock"]').click();await frames(page);
   const panelSamples=await page.evaluate(async()=>{const samples=[];for(let i=0;i<250;i++){const t=performance.now();document.querySelector('[data-stack-action="'+(i%2?'dock':'detach')+'"]').click();document.body.offsetHeight;const cost=performance.now()-t;if(i>=50)samples.push(cost);await new Promise(__nativeFrame);}return samples;});results.paths['panel-dock-detach']=samplesStats(panelSamples);
   assert.equal(await page.locator('.mm-order-stack').getAttribute('data-stack-mode'),'DOCKED','MEASUREMENT_LOOP_ENDS_DOCKED');await frames(page);results.memory.beforeCompanion=await memory();
   for(let i=0;i<3;i++){const event=context.waitForEvent('page');await page.locator('[data-stack-action="popout"]').click();const child=await event;await child.waitForSelector('.mm-order-stack[data-stack-mode="POPOUT"]');assert.equal(await page.locator('.mm-orders').count(),0);assert.equal(await child.locator('.mm-orders').count(),1);assert.equal(fixture.streams.size,1);if(i===2)results.memory.companion=await memory();await child.locator('[data-stack-action="dock"]').click();await page.bringToFront();await frames(page);}
   results.memory.returned=await memory();results.memory.companionHeapDelta=results.memory.companion.heapBytes-results.memory.beforeCompanion.heapBytes;results.memory.companionPssDelta=results.memory.companion.pss.bytes-results.memory.beforeCompanion.pss.bytes;assert.equal(context.pages().length,1);assert.equal(fixture.streams.size,1);
  }
  assert.deepEqual(results.errors,[]);assert.deepEqual(fixture.unexpected,[]);assert.equal(fixture.calls.filter(c=>!c.path.endsWith('/selections')).length,0,'NO_ORDER_MUTATIONS');results.instrumentation='One receipt acknowledgement added to execution-readiness handler; native RAF callback-cost observer; production source hashes retained';results.status='MEASURED';console.log('RESPONSIVE_PROBE '+JSON.stringify(results));
 }catch(e){results.status='FAIL';results.failure=e.stack;console.log('RESPONSIVE_PROBE '+JSON.stringify(results));throw e;}
 finally{await context?.close();await browser?.close();await browserServer?.close();for(const s of fixture.streams)s.end();fixture.server.closeAllConnections();guard.closeAllConnections();await Promise.all([new Promise(r=>fixture.server.close(r)),new Promise(r=>guard.close(r))]);}
})().catch(e=>{console.error(e.stack);process.exitCode=1;});
