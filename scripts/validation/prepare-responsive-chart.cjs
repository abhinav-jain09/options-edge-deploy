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
