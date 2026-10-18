/* Two-position/190-candle functional isolation/control check; no load or performance measurement. */
'use strict';
const assert=require('node:assert/strict'),http=require('node:http'),path=require('node:path');
const {chromium}=require('playwright-core'),prepare=require('../scripts/validation/prepare-responsive-chart.cjs'),readiness=require('../scripts/validation/readiness-stimulus.cjs');
const source=process.argv[2],{createFixture}=require(path.join(source,'test/browser/helpers/trade-workspace-fixture.cjs'));
(async()=>{
 const guard=http.createServer((req,res)=>{res.writeHead(403);res.end();});guard.on('connect',(req,s)=>s.end('HTTP/1.1 403 Forbidden\r\nConnection: close\r\n\r\n'));await new Promise(r=>guard.listen(0,'127.0.0.1',r));
 const f=createFixture({lifetimeMs:60000,transformBundle(text){
  if(process.env.OE_FLOATING_REBUILDS_CHART==='1'){
   const anchor="if(gesture&&mode==='FLOATING'&&value)apply(value);";assert.equal(text.split(anchor).length,2,'CONTROL_MUTATES_ACTUAL_STACK_CALLBACK');
   text=text.replace(anchor,"if(gesture&&mode==='FLOATING'&&value){apply(value);options.onLayout?.();}");
  }
  return readiness.instrument(text);
 }}),publisher=readiness.attach(f);await new Promise(r=>f.server.listen(0,'127.0.0.1',r));let browser;
 try{
  browser=await chromium.launch({headless:true,executablePath:process.env.PLAYWRIGHT_EXECUTABLE_PATH,proxy:{server:'http://127.0.0.1:'+guard.address().port,bypass:'127.0.0.1,localhost'}});const context=await browser.newContext({viewport:{width:1200,height:900}}),errors=[];
  await context.addInitScript(({issuer,owner,time})=>{const t=performance.now();Date.now=()=>time+performance.now()-t;window.OptionChainAuth={whenReady:()=>Promise.resolve(),token:()=> 'h.'+btoa(JSON.stringify({sub:owner,iss:issuer,exp:time/1000+3600}))+'.s'};},{issuer:f.issuer,owner:f.owner,time:f.clock()});
  const page=await context.newPage();page.on('pageerror',e=>errors.push(e.message));context.on('request',r=>{if(/^https?:/.test(r.url()))assert.equal(new URL(r.url()).hostname,'127.0.0.1');});await page.goto('http://127.0.0.1:'+f.server.address().port);
  await page.evaluate(session=>{const map=ContextTapeMarketMap.mount(document.querySelector('#contextTapeRoot'),{fetch:(u,o={})=>fetch(u,{...o,headers:{Authorization:'Bearer '+OptionChainAuth.token()}})});map.update({session,hidden:{}});map.show();},f.session);
  await prepare(page);const prepared=await prepare.isolateFloating(page,f);await page.locator('[data-stack-action="detach"]').click();const edge=page.locator('[data-stack-grip="resize"][data-direction="e"]');await edge.focus();for(let i=0;i<20;i++)await page.keyboard.press('Shift+ArrowLeft');
  await page.evaluate(()=>new Promise(r=>requestAnimationFrame(()=>requestAnimationFrame(r))));
  const backgroundReads=()=>f.requests.filter(x=>/\/vwap\?|\/level-signals\?/.test(x.path)).length,reads=backgroundReads(),cases=[];
  for(const [kind,direction]of [['move','n'],['resize','e']]){
   const target=page.locator(`[data-stack-grip="${kind}"][data-direction="${direction}"]`),r=await target.boundingBox(),x=r.x+r.width/2,y=r.y+r.height/2,before=await page.locator('.mm-order-stack').boundingBox();await page.mouse.move(x,y);await page.mouse.down();await page.evaluate(()=>window.__oldSvg=document.querySelector('[data-chart] svg'));
   // Cross a real five-second polling interval with native pointer/frame delivery.
   for(let i=0;i<8;i++){await page.mouse.move(x+(i%2?4:8),y+(kind==='move'?(i%2?4:8):0));await page.waitForTimeout(800);}
   await page.mouse.up();const after=await page.locator('.mm-order-stack').boundingBox();assert.equal(await page.evaluate(()=>__oldSvg===document.querySelector('[data-chart] svg')),true,'NO_CANDLE_REBUILD_FOR_FLOATING_GESTURE');assert.ok(kind==='move'?after.x!==before.x||after.y!==before.y:after.width!==before.width,'ACTUAL_FLOATING_GEOMETRY_CHANGED');assert.equal(await page.locator('[data-chart] [data-candle-time]').count(),f.session.candles.length,'ALL_CANDLES_RETAINED');assert.equal(backgroundReads(),reads,'ONLY_UNRELATED_INDICATOR_POLLING_DISABLED');cases.push({kind,status:'PASS',before,after});
  }
  assert.deepEqual(errors,[]);assert.deepEqual(f.unexpected,[]);assert.equal(f.calls.filter(x=>!x.path.endsWith('/selections')).length,0,'NO_ORDER_MUTATIONS');await prepare.waitForOrders(page,f);assert.equal(f.streams.size,1,'ORDER_TRANSPORT_REMAINS_ACTIVE');
  console.log(JSON.stringify({status:'PASS',classification:'Two-position/190-candle functional test only; no performance evidence',sourceCandles:f.session.candles.length,prepared,cases}));await context.close();
 }finally{publisher.dispose();await browser?.close();for(const s of f.streams)s.end();f.server.closeAllConnections();guard.closeAllConnections();await Promise.all([new Promise(r=>f.server.close(r)),new Promise(r=>guard.close(r))]);}
})().catch(e=>{console.error(e);process.exitCode=1;});
