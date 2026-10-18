/* Functional construction checks with synthetic cost units, never host performance evidence. */
'use strict';
const {test}=require('node:test'),assert=require('node:assert/strict'),vm=require('node:vm');
const measure=require('../scripts/validation/measure-responsive-panel.cjs');
async function scenario(transform=source=>source){
 let clock=0,debt=50,clicks=0,frames=0,reads=0;const actions=[];
 const body={get offsetHeight(){reads++;clock+=debt;debt=0;return 1;}};
 const sandbox={document:{body,querySelector(selector){return {click(){actions.push(selector.includes('"dock"')?'dock':'detach');clicks++;clock+=clicks<=50?1000:2;debt+=3;}};}},performance:{now:()=>clock},__nativeFrame(resolve){debt+=50;resolve(++frames);}};
 const result=await measure({evaluate:fn=>vm.runInNewContext('('+transform(fn.toString())+')()',sandbox)});
 return {result,clicks,frames,reads,actions};
}
function assess(run){
 assert.deepEqual(Array.from(run.result.samples),Array(200).fill(5),'PANEL_ONLY_CURRENT_TRANSITION_COST');
 assert.equal(run.clicks,250);assert.equal(run.frames,250);assert.equal(run.reads,500);assert.equal(run.result.warmup,50);
 assert.deepEqual(Array.from(run.result.frameTimestamps),Array.from({length:250},(_,i)=>i+1));
 assert.deepEqual(run.actions,Array.from({length:250},(_,i)=>i%2?'dock':'detach'));
 assert.equal(run.result.method,'prior document layout forced synchronously before panel clock');
}
test('actual evaluated helper excludes prior debt, charges current click/layout and retains all warm/native actions',async()=>assess(await scenario()));
test('removing only the preflush fails the same assessor with prior debt still charged',async()=>{
 const run=await scenario(source=>{assert.equal(source.split('document.body.offsetHeight;').length-1,2);return source.replace('document.body.offsetHeight;','');});
 assert.equal(run.result.samples[0],55);assert.throws(()=>assess(run),/PANEL_ONLY_CURRENT_TRANSITION_COST/);
});
test('removing only the post-click layout fails the same assessor with current layout omitted',async()=>{
 const run=await scenario(source=>{const needle='document.body.offsetHeight;';assert.equal(source.split(needle).length-1,2);const at=source.lastIndexOf(needle);return source.slice(0,at)+source.slice(at+needle.length);});
 assert.equal(run.result.samples[0],2);assert.throws(()=>assess(run),/PANEL_ONLY_CURRENT_TRANSITION_COST/);
});
