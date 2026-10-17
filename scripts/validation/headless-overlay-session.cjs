'use strict';
const {spawn}=require('node:child_process');
// Test-only local-file page, real animation frames; no virtual clock or external requests.
module.exports=async function runPage(chrome,fileUrl,profile){
 const child=spawn(chrome,['--headless=new','--no-sandbox','--disable-gpu','--disable-background-networking','--disable-renderer-backgrounding','--disable-background-timer-throttling','--host-resolver-rules=MAP * ~NOTFOUND','--no-proxy-server','--user-data-dir='+profile,'--remote-debugging-pipe'],{detached:true,stdio:['ignore','ignore','pipe','pipe','pipe']});
 const exited=new Promise(resolve=>child.once('exit',resolve));
 const terminate=()=>{if(Number.isInteger(child.pid))try{process.kill(-child.pid,'SIGKILL');}catch(error){if(error.code!=='ESRCH')child.kill('SIGKILL');}};
 const pending=new Map();let sequence=0,buffer=Buffer.alloc(0),closed=false,stderr='';
 function fail(error){for(const waiter of pending.values())waiter.reject(error);pending.clear();}
 const timeout=setTimeout(()=>{fail(Error('Browser measurement deadline'));terminate();},60000);
 child.on('error',fail);child.on('exit',code=>{closed=true;fail(Error('Browser exited '+code+' '+stderr.slice(-1000)));});
 child.stderr.on('data',chunk=>{stderr=(stderr+chunk.toString()).slice(-4096);});
 child.stdio[4].on('data',chunk=>{
  buffer=Buffer.concat([buffer,chunk]);if(buffer.length>2*1024*1024){fail(Error('Browser protocol capacity'));terminate();return;}
  let end;while((end=buffer.indexOf(0))>=0){const raw=buffer.subarray(0,end);buffer=buffer.subarray(end+1);if(!raw.length)continue;
   try{const value=JSON.parse(raw.toString()),waiter=pending.get(value.id);if(waiter){pending.delete(value.id);if(value.error)waiter.reject(Error(value.error.message));else waiter.resolve(value.result);}}
   catch(error){fail(error);terminate();return;}
  }
 });
 const send=(method,params={},sessionId)=>new Promise((resolve,reject)=>{
  if(closed||pending.size>=16){reject(Error('Browser protocol unavailable'));return;}const id=++sequence;pending.set(id,{resolve,reject});
  child.stdio[3].write(JSON.stringify({id,method,params,...(sessionId?{sessionId}:{})})+'\0',error=>{if(error){pending.delete(id);reject(error);}});
 });
 try{
  const {targetId}=await send('Target.createTarget',{url:'about:blank'}),{sessionId}=await send('Target.attachToTarget',{targetId,flatten:true});
  await send('Page.enable',{},sessionId);await send('Page.navigate',{url:fileUrl},sessionId);
  const result=await send('Runtime.evaluate',{expression:`new Promise((resolve,reject)=>{const deadline=performance.now()+5000;function poll(){if(typeof window.measure==='function'){window.measure().then(resolve,reject);return;}if(performance.now()>deadline){reject(Error('Measurement page did not initialize'));return;}setTimeout(poll,10);}poll();})`,awaitPromise:true,returnByValue:true,timeout:45000},sessionId);
  if(result.exceptionDetails)throw Error(result.exceptionDetails.exception?.description||result.exceptionDetails.text);
  if(result.result?.type!=='object'||result.result.value===null)throw Error('Missing browser measurement object');return result.result.value;
 }finally{
  try{if(!closed)await send('Browser.close');}catch(_){}finally{if(!closed)await new Promise(resolve=>{const timer=setTimeout(resolve,2000);exited.then(()=>{clearTimeout(timer);resolve();});});terminate();clearTimeout(timeout);fail(Error('Browser session closed'));}
 }
};
