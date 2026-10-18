/* Test-only delivery to the actual private stream, including legitimate reconnects. */
'use strict';
const assert=require('node:assert/strict');
const healthy=sequence=>({...(sequence===undefined?{}:{probeSequence:sequence}),account:'DUQ287805',mode:'PAPER',state:'HEALTHY',reasonCode:'CONNECTED',serviceInstanceId:'fixture',connectionGeneration:'generation',healthRevision:1,validForMs:15000,components:Object.fromEntries(['webToService','commandStore','dispatcher','reconciliation','brokerSession','managedAccount','brokerResponse','brokerUpstream'].map(k=>[k,true]))});
const instrument=text=>{const anchor="if(type==='execution-readiness'){";assert.equal(text.split(anchor).length,2,'MEASUREMENT_ACK_ANCHOR_UNIQUE');return text.replace(anchor,anchor+'if(Number.isSafeInteger(value.probeSequence))global.__probeReadinessSequence=value.probeSequence;');};
function attach(fixture){
 let pending=null;
 const write=(response,value)=>{if(!response.destroyed&&!response.writableEnded)response.write('event: execution-readiness\ndata: '+JSON.stringify(value)+'\n\n');};
 const opened=(request,response)=>{if(new URL(request.url,'http://fixture').pathname==='/api/context-tape/orders/events')write(response,pending||healthy());};
 fixture.server.on('request',opened);
 return {
  get pendingSequence(){return pending?.probeSequence??null;},
  publish(sequence){assert.ok(Number.isSafeInteger(sequence)&&sequence>=0,'VALID_READINESS_SEQUENCE');assert.equal(pending,null,'ONE_OUTSTANDING_READINESS_SAMPLE');pending=healthy(sequence);for(const stream of fixture.streams)write(stream,pending);},
  acknowledge(sequence){assert.equal(pending?.probeSequence,sequence,'ACKNOWLEDGE_CURRENT_READINESS_SAMPLE');pending=null;},
  dispose(){fixture.server.removeListener('request',opened);pending=null;}
 };
}
module.exports={healthy,instrument,attach};
