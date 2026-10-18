/* Shared functional preparation: exercise real controls without assuming their initial state. */
'use strict';
const assert=require('node:assert/strict');
module.exports=async function prepareChart(page){
 const toggle=page.locator('[data-controls]');
 if(await toggle.getAttribute('aria-expanded')!=='true')await toggle.click();
 const minute=page.locator('[data-tf="1"]');await minute.click();
 await page.waitForFunction(()=>document.querySelector('[data-tf="1"]')?.getAttribute('aria-pressed')==='true');
 assert.equal(await minute.getAttribute('aria-pressed'),'true','ONE_MINUTE_SELECTED');
 if(await toggle.getAttribute('aria-expanded')==='true')await toggle.click();
 assert.equal(await toggle.getAttribute('aria-expanded'),'false','CONTROLS_EXPLICITLY_CLOSED');
 await page.locator('[data-session]').click();
};

// UI preparation can invalidate a health lease and restart synchronization.
// Recheck the current admitted state, rather than an earlier green indicator.
module.exports.waitForOrders=async function(page,fixture,limitMs=15000){
 const deadline=Date.now()+limitMs,initialStreams=fixture.streams.size;let ready=false,checks=0;
 do{
  ready=await page.evaluate(()=>document.querySelector('[data-order-dot]')?.classList.contains('is-ready')===true);checks++;
  if(ready&&fixture.streams.size===1)return {initialStreams,finalStreams:1,ready:true,checks};
  await new Promise(resolve=>setTimeout(resolve,20));
 }while(Date.now()<deadline);
 assert.equal(fixture.streams.size,1,'PREPARED_ONE_PRIVATE_STREAM');
 assert.equal(ready,true,'PREPARED_EXECUTION_READY');
};

module.exports.admitCompanion=async function(main,child,fixture){
 assert.equal(await main.locator('.mm-orders').count(),0,'COMPANION_ONLY_ROOT');
 assert.equal(await child.locator('.mm-orders').count(),1,'COMPANION_SINGLE_ROOT');
 const prepared=await module.exports.waitForOrders(child,fixture);
 assert.equal(fixture.streams.size,1,'COMPANION_ONE_PRIVATE_STREAM');
 return prepared;
};
