// Run with Playwright installed: node checks/browser-smoke.cjs
// All support-center HTTP traffic is fulfilled locally; no account or production data is used.
const {chromium} = require('playwright');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const extension = path.resolve(__dirname, '..');
const profile = fs.mkdtempSync(path.join(os.tmpdir(), 'support-helper-smoke-'));
const A = {ticket_id:'A',subject:'Alpha',status:'NEW',product:'disk'};
const B = {ticket_id:'B',subject:'Beta',status:'ARCHIVED',product:'mail'};
const C = {ticket_id:'C',subject:'Без темы',status:'NEW',product:'mail'};
const D = {ticket_id:'D',subject:'Delta',status:'NEW',product:'mail'};
const E = {ticket_id:'E',subject:'Echo',status:'NEW',product:'disk'};
let passed = 0;
const faults = [];
async function waitFor(fn, label, timeout=10000) {
  const deadline=Date.now()+timeout;
  while(Date.now()<deadline) { if(await fn()) return; await new Promise(r=>setTimeout(r,100)); }
  throw new Error('Timed out: '+label);
}
function pass(label){passed++;console.log('PASS:',label);}
function fixture(url) {
  const uid=url.searchParams.get('uid')||'one';
  const ticket=url.pathname.split('/')[2]||'';
  const requestTicket=ticket==='alias-B'?'B':ticket;
  const detailOrg=url.searchParams.get('detailOrg')||`org-${uid}`;
  const delay=Number(url.searchParams.get('headerDelay')||30);
  return `<!doctype html><html><head><meta charset="utf-8"><script>
    window.loadTickets = async (params={}, mode='fetch') => {
      const body=JSON.stringify({models:[{name:'unrelated/model',params:{}},
        {name:'support-b2b/get-tickets',params:{org_id:'org-${uid}',...params}}]});
      if(mode==='xhr') return new Promise((resolve,reject)=>{
        const xhr=new XMLHttpRequest();xhr.open('POST','/api/models');xhr.responseType='json';xhr.setRequestHeader('x-sarah-uid','${uid}');
        xhr.onload=()=>resolve(xhr.response);xhr.onerror=reject;xhr.send(body);
      });
      const input=mode==='request'?new Request(location.origin+'/api/models',{method:'POST',body,headers:{'x-sarah-uid':'${uid}'}}):'/api/models';
      return (await fetch(input,mode==='request'?undefined:{method:'POST',body,headers:{'x-sarah-uid':'${uid}'}})).json();
    };
    ${ticket ? `fetch('/api/models',{method:'POST',body:JSON.stringify({models:[{name:'support-b2b/get-ticket',params:{org_id:'${detailOrg}',ticket_id:'${requestTicket}'}}]})});` : "loadTickets({}, 'request');"}
  </script></head><body><main id="app"></main><script>
    window.renderHeader=()=>{
      document.querySelector('#app').innerHTML=${JSON.stringify(ticket ? '<header><div><button data-testid="back-button">Назад</button></div></header>' : `<table><tbody><tr data-testid="resource-table-row"><td data-testid="orb-text">Без темы</td><td data-testid="ticket-status-tag"><span class="Orb-Tag-label">Архивировано</span></td></tr></tbody></table><a id="open-ticket" href="/support-center/A?uid=${uid}">Открыть тикет</a><a id="open-spa" href="/support-center/A?uid=${uid}">Открыть без перезагрузки</a>`)};
      document.querySelector('#open-spa')?.addEventListener('click',event=>{
        event.preventDefault();window.lateListRequest=loadTickets({subject:'empty',fixtureDelay:250});
        history.pushState({},'',event.currentTarget.href);
        document.querySelector('#app').innerHTML='<header><div><button data-testid="back-button">Назад</button></div></header>';
      });
    };setTimeout(renderHeader,${delay});
  </script></body></html>`;
}
(async()=>{
  let context;
  try {
    context=await chromium.launchPersistentContext(profile,{
      channel:'chromium',headless:true,
      args:[`--disable-extensions-except=${extension}`,`--load-extension=${extension}`],
      ignoreDefaultArgs:['--disable-extensions'],
      viewport:{width:1100,height:720}
    });
    console.log('Chromium:',context.browser()?.version()||'persistent context');
    await context.route('**/*',async route=>{
      const url=new URL(route.request().url());
      if(url.protocol==='chrome-extension:') return route.continue();
      if(!['admin.yandex.ru','admin.360.yandex.ru'].includes(url.hostname)) return route.abort();
      if(url.pathname==='/api/models') {
        const req=JSON.parse(route.request().postData()||'{}');
        const model=req.models.find(m=>m.name.startsWith('support-b2b/'));
        const params=model.params;
        if(params.fixtureDelay) await new Promise(resolve=>setTimeout(resolve,params.fixtureDelay));
        let data;
        if(model.name.endsWith('get-ticket')) data={ticket:[A,B,C,D,E].find(t=>t.ticket_id===params.ticket_id)};
        else {
          let items=params.org_id==='org-two'?[{...A,ticket_id:'X'}]:params.cursor==='next'?[D,E]:[A,B,C];
          if(params.subject==='empty'||params.cursor==='end') items=[];
          if(params.subject==='one') items=[A];
          if(params.subject==='archived') items=[B];
          if(params.subject==='filter-regression') items=[{...A,subject:'Без темы'},B,C,D];
          if(params.subject==='all-subjectless') items=[A,B,C,D].map(ticket=>({...ticket,subject:'Без темы'}));
          if(params.subject==='pagination-regression') items=params.pageMarker ?
            Array.from({length:4},(_,i)=>({...C,ticket_id:`tail-${i}`})) :
            [A,C,...Array.from({length:13},(_,i)=>({...D,ticket_id:`page-${i}`}))];
          if(params.missingSubjects) items=items.map(({subject,...ticket})=>ticket);
          data={items};
        }
        return route.fulfill({status:200,contentType:'application/json',body:JSON.stringify({models:[{name:'unrelated/model',data:{}},{name:model.name,status:'OK',errors:[],data}]})});
      }
      return route.fulfill({status:200,contentType:'text/html',body:fixture(url)});
    });
    const watch=page=>{
      page.on('pageerror',e=>faults.push(e.message));
      page.on('console',msg=>{if(msg.type()==='error'&&msg.text().includes('Support Center Helper'))faults.push(msg.text());});
    };
    let worker=context.serviceWorkers()[0]||await context.waitForEvent('serviceworker');
    const id=new URL(worker.url()).hostname;
    await worker.evaluate(()=>chrome.storage.local.set({enableNavigation:true}));
    const page=await context.newPage();watch(page);
    await page.goto('https://admin.yandex.ru/support-center?uid=one');
    const tabId=await worker.evaluate(async()=> (await chrome.tabs.query({url:'https://admin.yandex.ru/support-center?uid=one'}))[0].id);
    // Real popup page supplies an extension context for message/storage checks.
    const popup=await context.newPage();watch(popup);
    await popup.goto(`chrome-extension://${id}/popup.html`);
    await page.bringToFront();
    const message=(action,extra={},tab=tabId)=>popup.evaluate(async({action,extra,tab})=>
      chrome.runtime.sendMessage({action,...extra,tabId:tab}),{action,extra,tab});
    const count=async tab=>(await message('getTicketsCount',{},tab)).count;
    await waitFor(async()=>await count()===3,'capture early Request fetch');
    pass('early Request fetch captured at document_start with correct batched model');
    await page.evaluate(()=>loadTickets({cursor:'next'},'xhr'));
    await waitFor(async()=>await count()===5,'XHR continuation');
    pass('XHR JSON pagination accumulates five tickets');
    await page.reload();
    await waitFor(async()=>await count()===5,'list page reload');
    await new Promise(r=>setTimeout(r,300));
    assert.equal(await count(),5);pass('list-page reload preserves all loaded pages');
    await page.locator('#open-ticket').click();await page.waitForURL('**/support-center/A?uid=one');
    const next=page.locator('[data-nav-direction="next"]');
    const prev=page.locator('[data-nav-direction="prev"]');
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'initial buttons');
    assert(await prev.isDisabled());assert.equal(await count(),5);
    await next.click();await page.waitForURL('**/support-center/B?uid=one');
    await waitFor(async()=>await prev.count()===1&&await prev.isEnabled(),'buttons after next');
    assert.equal(await count(),5);pass('Next loads B, restores buttons and keeps the list');
    await prev.click();await page.waitForURL('**/support-center/A?uid=one');
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'buttons after previous');
    assert.equal(await count(),5);pass('Previous loads A, restores buttons and keeps the list');
    await page.reload();
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'buttons after reload');
    assert.equal(await count(),5);pass('ticket reload restores enabled buttons and cached navigation');
    await page.evaluate(()=>renderHeader());
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'replacement header');
    pass('buttons are restored after app replaces its header');
    await page.evaluate(()=>fetch('/api/models',{method:'POST',body:JSON.stringify({models:[{
      name:'support-b2b/get-ticket',params:{org_id:'org-one',ticket_id:'C'}
    }]})}));
    await waitFor(async()=> (await message('getDiagnostics')).history.at(-1)?.reason==='detail-updated','prefetched detail saved');
    await new Promise(resolve=>setTimeout(resolve,200));
    assert(await prev.isDisabled());assert(await next.isEnabled());
    await next.click();await page.waitForURL('**/support-center/B?uid=one');
    pass('prefetched details for another ticket cannot replace the current route position');
    await popup.evaluate(()=>chrome.storage.local.set({hideArchived:true,hideNoSubject:true}));
    await page.goto('https://admin.yandex.ru/support-center/B?uid=one');
    await waitFor(async()=>await next.count()===1&&await next.isEnabled()&&await prev.isEnabled(),'hidden current ticket');
    await page.reload();
    await waitFor(async()=>await next.count()===1&&await next.isEnabled()&&await prev.isEnabled(),'hidden anchor after reload');
    await next.click();await page.waitForURL('**/support-center/D?uid=one');
    pass('hidden current ticket keeps both neighbours after reload; Next skips other hidden tickets');
    await popup.evaluate(()=>chrome.storage.local.set({hideArchived:false,hideNoSubject:false}));
    await page.goto('https://admin.yandex.ru/support-center/alias-B?uid=one');
    await waitFor(async()=>await next.count()===1&&await next.isEnabled()&&await prev.isEnabled(),'detail ID differs from URL');
    await page.reload();
    await waitFor(async()=>await next.count()===1&&await next.isEnabled()&&await prev.isEnabled(),'detail ID after reload');
    await next.click();await page.waitForURL('**/support-center/C?uid=one');
    pass('ticket ID from detail request restores buttons and navigation when URL uses another identifier');
    await page.goto('https://admin.yandex.ru/support-center?uid=one');
    await page.waitForSelector('#open-ticket');
    await page.locator('#open-ticket').evaluate(link=>link.href+='&detailOrg=card-org');
    await page.locator('#open-ticket').click();
    await page.waitForURL('**/support-center/A?uid=one&detailOrg=card-org');
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'card with different API context');
    assert.equal(await count(),5);
    await page.evaluate(()=>loadTickets({subject:'empty'}));
    await waitFor(async()=> (await message('getDiagnostics')).history.some(entry=>entry.reason==='list-outside-list-view'),'card list ignored');
    assert.equal(await count(),5);
    await page.bringToFront();await popup.evaluate(()=>loadState());
    await waitFor(async()=>await popup.locator('#totalTicketsLabel').textContent()==='Открыта карточка тикета','card counter label');
    assert.equal(await popup.locator('#totalTickets').textContent(),'—');
    assert.equal(await popup.locator('#collectedCount').textContent(),'5');
    await popup.evaluate(()=>document.querySelector('#navigationDiagnostics').open=true);
    await waitFor(async()=> (await popup.locator('#diagnosticsText').inputValue()).includes('list-outside-list-view'),'popup diagnostics');
    const diagnostic=JSON.parse(await popup.locator('#diagnosticsText').inputValue());
    assert.equal(diagnostic.stored,5);assert.equal(diagnostic.navigation.currentFound,true);
    assert(diagnostic.history.some(entry=>entry.reason==='detail-context-mismatch'));
    pass('actual list click retains selection despite detail context mismatch and auxiliary empty list; popup distinguishes counters');
    await page.goto('https://admin.yandex.ru/support-center?uid=one');
    await page.locator('#open-spa').click();await page.waitForURL('**/support-center/A?uid=one');
    await page.evaluate(()=>lateListRequest);
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'SPA click with late list response');
    assert.equal(await count(),5);
    pass('SPA click keeps collected selection when an earlier empty list response arrives in the card');
    await page.goto('https://admin.yandex.ru/support-center/A?uid=one&headerDelay=6100');
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'late header',12000);
    pass('buttons appear when header arrives later than the old five-second deadline');
    await popup.evaluate(()=>chrome.storage.local.set({hideArchived:true,hideNoSubject:true}));
    await waitFor(async()=>JSON.stringify((await message('getNavigationList')).tickets)==='["A","D","E"]','settings filters');
    await next.click();await page.waitForURL('**/support-center/D?uid=one&headerDelay=6100');
    assert.equal(await count(),5);pass('live settings skip archived and subjectless tickets without discarding cache');
    await popup.evaluate(()=>chrome.storage.local.set({enableNavigation:false}));
    await waitFor(async()=>await page.locator('.ticket-nav-buttons-wrapper').count()===0,'disable navigation');
    await popup.evaluate(()=>chrome.storage.local.set({enableNavigation:true,hideArchived:false,hideNoSubject:false}));
    await waitFor(async()=>await prev.count()===1&&await prev.isEnabled(),'re-enable navigation',12000);
    pass('navigation can be enabled again from existing cache');
    // Stop the real extension service worker via CDP; next message must revive it.
    const cdp=await context.newCDPSession(page);
    const versions=new Map();
    cdp.on('ServiceWorker.workerVersionUpdated',event=>{for(const version of event.versions)versions.set(version.versionId,version);});
    await cdp.send('ServiceWorker.enable');
    await waitFor(()=>[...versions.values()].some(v=>v.scriptURL===worker.url()&&v.runningStatus==='running'),'worker registration');
    const version=[...versions.values()].find(v=>v.scriptURL===worker.url()&&v.runningStatus==='running');
    await cdp.send('ServiceWorker.stopWorker',{versionId:version.versionId});
    await waitFor(()=>versions.get(version.versionId)?.runningStatus==='stopped','worker stopped');
    assert.equal(await count(),5);
    assert.deepEqual((await message('getNavigationList')).tickets,['A','B','C','D','E']);
    pass('actual service-worker stop/restart preserves navigation and export state');
    await cdp.detach();
    const second=await context.newPage();watch(second);
    await second.goto('https://admin.yandex.ru/support-center?uid=two');
    const tab2=await popup.evaluate(async()=> (await chrome.tabs.query({url:'https://admin.yandex.ru/support-center?uid=two'}))[0].id);
    await waitFor(async()=>await count(tab2)===1,'second tab');
    assert.equal(await count(),5);
    await second.bringToFront();await popup.evaluate(()=>loadState());
    await waitFor(async()=>await popup.locator('#collectedCount').textContent()==='1','popup active tab count');
    const downloading=popup.waitForEvent('download');
    await popup.evaluate(()=>exportToCSV());
    const download=await downloading;
    const csv=fs.readFileSync(await download.path(),'utf8');
    assert.match(csv,/,X,,Alpha,Новый,Диск/);
    assert(!csv.includes(',B,'));
    pass('popup CSV download contains only the active tab tickets');
    await popup.evaluate(()=>document.querySelector('#clearBtn').click());
    await waitFor(async()=>await count(tab2)===0,'popup scoped clear');
    assert.equal(await count(),5);pass('popup reads and clears only active tab; other tab survives');
    await page.goto('https://admin.yandex.ru/support-center?uid=one');
    await page.waitForSelector('tr[data-testid="resource-table-row"]');
    await page.evaluate(()=>{
      history.pushState({},'', '/support-center/A?uid=one');
      document.querySelector('#app').innerHTML='<header><div><button data-testid="back-button">Назад</button></div></header>';
    });
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'SPA route buttons');
    assert.equal(await count(),5);
    await page.evaluate(()=>{history.pushState({},'', '/support-center?uid=one');document.querySelector('#app').innerHTML='';});
    await waitFor(async()=>await page.locator('.ticket-nav-buttons-wrapper').count()===0,'SPA return');
    assert.equal(await count(),5);pass('SPA ticket/list transitions keep cache and mount/unmount buttons');
    await page.evaluate(()=>loadTickets({cursor:'end'}));
    await new Promise(r=>setTimeout(r,300));assert.equal(await count(),5);
    await page.evaluate(()=>loadTickets({subject:'empty'}));
    await waitFor(async()=>await count()===0,'empty filtered list');
    pass('empty continuation preserves list; empty new selection clears stale data');
    await second.goto('https://admin.360.yandex.ru/support-center?uid=two');
    await waitFor(async()=>await count(tab2)===1,'second admin hostname');
    await popup.evaluate(()=>chrome.storage.local.set({hideNoSubject:true}));
    await waitFor(async()=>await second.locator('tr.ext-hidden-no-subject').count()===1,'row hiding after setting');
    await popup.evaluate(()=>chrome.storage.local.set({hideNoSubject:false}));
    await waitFor(async()=>await second.locator('tr.ext-hidden-no-subject').count()===0,'row showing after setting');
    pass('admin.360 hostname and live DOM row filtering work');
    await page.goto('https://admin.yandex.ru/support-center');
    await waitFor(async()=>await count()===3,'header UID list');
    await page.goto('https://admin.yandex.ru/support-center/A');
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'header UID buttons');
    await next.click();await page.waitForURL('**/support-center/B?uid=one');
    pass('navigation restores UID from captured request when source URL has none');
    await page.goto('https://admin.yandex.ru/support-center?uid=one');
    await page.evaluate(()=>loadTickets({filters:{fixture:'partial-records'},missingSubjects:true}));
    await popup.evaluate(()=>chrome.storage.local.set({hideNoSubject:true,hideArchived:false}));
    await waitFor(async()=> (await message('getDiagnostics')).missingSubject===3,'partial records captured');
    assert.equal((await message('getTicketsCount')).visibleCount,3);
    assert.equal((await message('exportTickets')).count,3);
    await page.locator('#open-ticket').click();
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'navigation with partial subjects');
    pass('omitted subjects remain unknown and do not remove every ticket from navigation or CSV');
    // All known tickets can legitimately be filtered; the popup must show why,
    // then immediately re-enable export when the last filter is disabled.
    await popup.evaluate(()=>chrome.storage.local.set({hideNoSubject:false,hideArchived:true}));
    await page.goto('https://admin.yandex.ru/support-center?uid=one');
    await page.evaluate(()=>loadTickets({subject:'archived'}));
    await waitFor(async()=> (await message('getTicketsCount')).visibleCount===0,'all records filtered');
    await page.bringToFront();await popup.evaluate(()=>loadState());
    await waitFor(async()=>await popup.locator('#exportBtn').isDisabled(),'export disabled with filter explanation');
    assert.equal(await popup.locator('#visibleCount').textContent(),'0');
    await popup.evaluate(()=>chrome.storage.local.set({hideArchived:false}));
    await waitFor(async()=>await popup.locator('#exportBtn').isEnabled(),'export re-enabled without polling');
    assert.equal(await popup.locator('#visibleCount').textContent(),'1');
    pass('popup explains filtered-out exports and refreshes immediately when the last filter is disabled');
    await page.goto('https://admin.yandex.ru/support-center?uid=one');
    await page.evaluate(()=>loadTickets({subject:'filter-regression'}));
    await waitFor(async()=>await count()===4,'four-ticket filter regression');
    await page.locator('#open-ticket').evaluate(link=>link.href='/support-center/C?uid=one');
    await page.locator('#open-ticket').click();
    await waitFor(async()=>await prev.count()===1&&await prev.isEnabled()&&await next.isEnabled(),'subjectless current before filtering');
    await page.bringToFront();await popup.evaluate(()=>loadState());
    await waitFor(async()=>!(await popup.locator('#toggleNoSubject').isChecked()),'popup filter initially disabled');
    await popup.evaluate(()=>document.querySelector('#toggleNoSubject').click());
    await waitFor(async()=> (await message('getTicketsCount')).visibleCount===2,'filter applied via popup click');
    await waitFor(async()=>await prev.isEnabled()&&await next.isEnabled(),'subjectless current keeps neighbours');
    assert.equal(await count(),4);
    assert.equal((await message('getNavigationList',{currentTicketId:'C',routeTicketId:'C'})).metadata.currentFound,true);
    await page.reload();
    await waitFor(async()=>await prev.count()===1&&await prev.isEnabled()&&await next.isEnabled(),'subjectless current after reload');
    await next.click();await page.waitForURL('**/support-center/D?uid=one');
    await waitFor(async()=>await prev.count()===1&&await prev.isEnabled(),'filtered destination loaded');
    await prev.click();await page.waitForURL('**/support-center/B?uid=one');
    pass('four collected tickets: actual popup subject filter preserves hidden current position, reload and both directions');
    await popup.evaluate(()=>chrome.storage.local.set({hideNoSubject:false}));
    await page.goto('https://admin.yandex.ru/support-center?uid=one');
    await page.evaluate(()=>loadTickets({subject:'all-subjectless'}));
    await waitFor(async()=>await count()===4,'four subjectless tickets');
    await page.locator('#open-ticket').evaluate(link=>link.href='/support-center/C?uid=one');
    await page.locator('#open-ticket').click();
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'all subjectless before filter');
    await popup.evaluate(()=>chrome.storage.local.set({hideNoSubject:true}));
    await waitFor(async()=>await next.isDisabled()&&await prev.isDisabled(),'no eligible neighbours');
    assert.equal(await next.getAttribute('title'),'Нет других тикетов с учётом фильтров расширения');
    assert.equal((await message('getNavigationList',{currentTicketId:'C',routeTicketId:'C'})).metadata.currentFound,true);
    assert.equal(await count(),4);
    pass('all four tickets filtered: current ticket is still found; disabled buttons explain missing eligible neighbours');
    await popup.evaluate(()=>chrome.storage.local.set({hideNoSubject:false,hideArchived:false}));
    await page.goto('https://admin.yandex.ru/support-center?uid=one');
    await page.evaluate(()=>loadTickets({subject:'pagination-regression',statuses:[],limit:15}));
    await waitFor(async()=>await count()===15,'pagination regression first 15');
    await page.waitForSelector('tr[data-testid="resource-table-row"]');
    await page.evaluate(()=>{
      window.autoContinuation=new Promise(resolve=>{
        const observer=new MutationObserver(()=>{
          if(!document.querySelector('tr.ext-hidden-no-subject'))return;
          observer.disconnect();
          loadTickets({subject:'pagination-regression',pageMarker:{opaque:'next'},limit:15}).then(resolve);
        });
        observer.observe(document.documentElement,{subtree:true,attributes:true,attributeFilter:['class']});
      });
    });
    await page.bringToFront();await popup.evaluate(()=>loadState());
    await popup.evaluate(()=>document.querySelector('#toggleNoSubject').click());
    await page.evaluate(()=>autoContinuation);
    await waitFor(async()=>await count()===19,'15 plus 4, not replacement');
    const pageTrace=(await message('getDiagnostics')).history;
    assert(pageTrace.some(entry=>entry.before===15&&entry.after===19&&entry.received===4&&!entry.selectionChanged));
    assert.equal((await message('getTicketsCount')).visibleCount,14);
    await page.locator('#open-ticket').click();
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'current ticket survives filter-triggered pagination');
    await next.click();await page.waitForURL('**/support-center/page-0?uid=one');
    await waitFor(async()=>await prev.count()===1&&await prev.isEnabled(),'next page restores previous');
    await prev.click();await page.waitForURL('**/support-center/A?uid=one');
    await page.reload();
    await waitFor(async()=>await next.count()===1&&await next.isEnabled(),'reload after additional page');
    assert.equal(await count(),19);
    pass('subject filter triggers automatic continuation: 15 + 4 remain collected; current ticket, both directions and reload work');
    assert.deepEqual(faults,[]);
    console.log(`Browser scenarios passed: ${passed}; page/extension errors: ${faults.length}`);
  } catch (error) {
    console.error('Captured errors:', faults);
    if(context) {
      for(const p of context.pages()) {
        if(p.url().startsWith('https://admin.')) console.error('Fixture diagnostics:', await p.evaluate(()=>({url:location.href,fetchSource:window.fetch.toString().slice(0,140),fixture:typeof loadTickets,styles:!!document.getElementById('ext-hide-styles')})));
      }
      const w=context.serviceWorkers()[0];
      if(w) console.error('Session snapshot:', await w.evaluate(()=>chrome.storage.session.get(null)));
    }
    throw error;
  } finally {
    if(context)await context.close();
    fs.rmSync(profile,{recursive:true,force:true});
  }
})().catch(error=>{console.error(error);process.exitCode=1;});
