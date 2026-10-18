/* Test-only panel boundary. Prior layout is outside the clock; current work stays inside. */
'use strict';
module.exports=async function measurePanel(page){
 return page.evaluate(async()=>{
  const samples=[],frameTimestamps=[];
  for(let i=0;i<250;i++){
   document.body.offsetHeight;
   const start=performance.now();
   document.querySelector('[data-stack-action="'+(i%2?'dock':'detach')+'"]').click();
   document.body.offsetHeight;
   const cost=performance.now()-start;
   if(i>=50)samples.push(cost);
   frameTimestamps.push(await new Promise(__nativeFrame));
  }
  return {samples,frameTimestamps,warmup:50,method:'prior document layout forced synchronously before panel clock'};
 });
};
