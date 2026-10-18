/* Test-only post-measurement diagnostic. Never supplies or replaces qualifying samples. */
'use strict';
const assert=require('node:assert/strict'),fs=require('node:fs');
const MAX_EVENTS=10000,MAX_BYTES=8*1024*1024;
function within(promise,ms,label){let timer;return Promise.race([promise,new Promise((resolve,reject)=>{timer=setTimeout(()=>reject(new Error(label)),ms);})]).finally(()=>clearTimeout(timer));}
module.exports=async function profileDock(cdp,page,file){
 const events=[];let bytes=0,failure=null,tracing=false,completionListener,completionTimer,result;
 const collect=batch=>{
  if(failure)return;
  if(!Array.isArray(batch.value)||events.length+batch.value.length>MAX_EVENTS){failure=new Error('DOCK_TRACE_EVENT_CAPACITY');return;}
  const size=Buffer.byteLength(JSON.stringify(batch.value));if(bytes+size>MAX_BYTES){failure=new Error('DOCK_TRACE_BYTE_CAPACITY');return;}
  bytes+=size;for(const event of batch.value)events.push(event);
 };
 cdp.on('Tracing.dataCollected',collect);
 try{
  tracing=true;await within(cdp.send('Tracing.start',{traceConfig:{recordMode:'recordUntilFull',traceBufferSizeInKb:8192,includedCategories:['devtools.timeline','blink.user_timing','disabled-by-default-devtools.timeline.stack']},transferMode:'ReportEvents'}),5000,'DOCK_TRACE_START_DEADLINE');
  result=await within(page.evaluate(async()=>{
   const samples=[];
   for(const frames of [1,3])for(let i=0;i<6;i++){
    await new Promise(resolve=>{let left=frames;const next=()=>{if(--left===0)resolve();else __nativeFrame(next);};__nativeFrame(next);});
    const action=i%2?'dock':'detach',label='OE_PANEL_'+frames+'_'+i;performance.mark(label+'_START');const start=performance.now();document.querySelector('[data-stack-action="'+action+'"]').click();document.body.offsetHeight;const costMs=performance.now()-start;performance.mark(label+'_END');samples.push({frames,action,costMs});
   }
   await new Promise(resolve=>__nativeFrame(()=>__nativeFrame(resolve)));
   performance.mark('OE_PANEL_CAPTURE_COMPLETE');
   return {classification:'Diagnostic only: native one/three-frame settling; not qualifying samples',samples,mode:document.querySelector('.mm-order-stack').dataset.stackMode};
  }),30000,'DOCK_TRACE_GESTURE_DEADLINE');
  const complete=new Promise((resolve,reject)=>{completionListener=resolve;cdp.once('Tracing.tracingComplete',completionListener);completionTimer=setTimeout(()=>reject(new Error('DOCK_TRACE_FLUSH_DEADLINE')),10000);});
  const [done]=await Promise.all([complete,within(cdp.send('Tracing.end'),10000,'DOCK_TRACE_END_DEADLINE')]);tracing=false;assert.equal(done.dataLossOccurred,false,'DOCK_TRACE_DATA_LOSS');if(failure)throw failure;
  assert.ok(events.length>0,'DOCK_TRACE_EVENTS_REQUIRED');assert.equal(result.mode,'DOCKED','DOCK_TRACE_RETURNS_DOCKED');
  assert.equal(result.samples.length,12,'DOCK_TRACE_SAMPLE_COUNT');const marks=new Set(events.map(event=>event.name));
  for(let i=0;i<12;i++){const sample=result.samples[i],frames=i<6?1:3,action=i%2?'dock':'detach',label='OE_PANEL_'+frames+'_'+(i%6);assert.ok(sample.frames===frames&&sample.action===action&&Number.isFinite(sample.costMs)&&sample.costMs>=0,'DOCK_TRACE_SAMPLE_IDENTITY');assert.ok(marks.has(label+'_START')&&marks.has(label+'_END'),'DOCK_TRACE_MARKERS_REQUIRED');}
  assert.ok(marks.has('OE_PANEL_CAPTURE_COMPLETE'),'DOCK_TRACE_MARKERS_REQUIRED');
 }catch(error){failure=error;}
 finally{
  clearTimeout(completionTimer);if(completionListener)cdp.off('Tracing.tracingComplete',completionListener);cdp.off('Tracing.dataCollected',collect);
  if(tracing){try{await within(cdp.send('Tracing.end'),2000,'DOCK_TRACE_CLEANUP_DEADLINE');}catch(error){failure=failure?new AggregateError([failure,error],'Dock diagnostic and tracing cleanup failed\nPrimary: '+(failure.stack||String(failure))+'\nCleanup: '+(error.stack||String(error))):error;}}
 }
 if(failure)throw failure;
 const report={classification:'Post-measurement diagnostic only; no gate authority',result,traceEvents:events,collectedBytes:bytes,maxEvents:MAX_EVENTS,maxBytes:MAX_BYTES};fs.writeFileSync(file,JSON.stringify(report));return {classification:report.classification,file,eventCount:events.length,collectedBytes:bytes,samples:result.samples};
};
module.exports.within=within;
