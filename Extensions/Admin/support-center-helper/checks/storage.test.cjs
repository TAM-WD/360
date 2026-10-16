const {test} = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const source = fs.readFileSync(path.join(__dirname, '../background.js'), 'utf8');
const copy = value => JSON.parse(JSON.stringify(value));
const A = {ticket_id:'A', subject:'Original', status:'NEW', product:'disk'};
const B = {ticket_id:'B', subject:'Без темы', status:'ARCHIVED', product:'mail'};
const C = {ticket_id:'C', subject:'Third', status:'NEW', product:'mail'};
function boot(persisted = {}) {
  const settings = {};
  let listener, onRemoved, failWrite = false;
  const sandbox = {
    console: {log(){},error(){}}, URL,
    chrome: {
      storage: {
        session: {
          async get(key) { await new Promise(r=>setImmediate(r)); return copy({[key]:persisted[key] || null}); },
          async set(values) { if(failWrite) throw new Error('QUOTA_BYTES'); Object.assign(persisted,copy(values)); },
          async remove(key) { delete persisted[key]; }
        },
        local: {async get(){return copy(settings);}}
      },
      runtime: {getManifest:()=>({version:'test'}),getURL:p=>'chrome-extension://test/'+p,onMessage:{addListener(fn){listener=fn;}},sendMessage:async()=>{}},
      tabs: {sendMessage:async()=>{},onRemoved:{addListener(fn){onRemoved=fn;}}}
    }
  };
  vm.runInNewContext(source,sandbox);
  let time = Date.now();
  const send = (action, data={}, tabId=1, popup=false) => new Promise(resolve => {
    listener({action,...data,tabId}, popup?{}:{tab:{id:tabId},url:'https://admin.yandex.ru/support-center?uid=test'}, value=>resolve(copy(value)));
  });
  return {persisted,settings,send,onRemoved,setFailWrite(value){failWrite=value;},
    capture(tickets,filters={},tabId=1,requestedAt=++time,context={uid:'test'}) {
      return send('captureTickets',{tickets,filters:{org_id:'org-1',...filters},requestedAt,context},tabId);
    },
    async ids(tabId=1){return (await send('getNavigationList',{},tabId)).tickets;}
  };
}

test('worker restart preserves collected pages and export',async()=>{
  let w=boot(); await w.capture([A,B]); await w.capture([C],{cursor:'next'});
  w=boot(w.persisted); assert.deepEqual(await w.ids(),['A','B','C']);
  assert.equal((await w.send('exportTickets')).count,3);
});
test('refreshing first page preserves previously loaded continuation',async()=>{
  const w=boot(); await w.capture([A,B]); await w.capture([C],{cursor:'next'}); await w.capture([A,B]);
  assert.deepEqual(await w.ids(),['A','B','C']);
});
test('duplicates update fields and server order',async()=>{
  const w=boot(); await w.capture([A,B]); await w.capture([B,{...A,subject:'Changed',status:'CLOSED'}]);
  assert.deepEqual(await w.ids(),['B','A']);
  const result=await w.send('exportTickets'); assert.equal(result.count,2); assert.match(result.csv,/Changed,Решён/);
});
test('empty first page clears an existing selection',async()=>{
  const w=boot(); await w.capture([A]); await w.capture([B],{cursor:'next'}); await w.capture([]);
  assert.deepEqual(await w.ids(),[]);
});
test('empty continuation does not clear loaded tickets',async()=>{
  const w=boot(); await w.capture([A]); await w.capture([],{cursor:'end'}); assert.deepEqual(await w.ids(),['A']);
});
test('filter and sort changes replace the selection, including empty results',async()=>{
  const w=boot(); await w.capture([A]); await w.capture([B],{sort:'subject'}); assert.deepEqual(await w.ids(),['B']);
  await w.capture([],{subject:'nothing'}); assert.deepEqual(await w.ids(),[]);
});
test('pagination parameters do not reset query; array filter order is immaterial',async()=>{
  const w=boot(); await w.capture([A],{products:['mail','disk'],limit:1});
  await w.capture([B],{products:['disk','mail'],limit:1,offset:1});
  await w.capture([C],{products:['mail','disk'],pagination:{cursor:'last'}});
  assert.deepEqual(await w.ids(),['A','B','C']);
});
test('tabs cannot overwrite or clear one another',async()=>{
  const w=boot(); await w.capture([A],{},1); await w.capture([B],{},2);
  assert.deepEqual(await w.ids(1),['A']); assert.deepEqual(await w.ids(2),['B']);
  await w.send('clearTickets',{},2,true); assert.deepEqual(await w.ids(1),['A']); assert.deepEqual(await w.ids(2),[]);
});
test('UID and organisation switches isolate navigation',async()=>{
  const w=boot(); await w.capture([A]);
  assert.deepEqual((await w.send('getNavigationList',{context:{uid:'another'}})).tickets,[]);
  await w.capture([B],{org_id:'org-2'}); assert.deepEqual(await w.ids(),['B']);
  await w.send('captureTicket',{context:{uid:'another',orgId:'org-3'},requestedAt:Date.now()+100,ticket:{ticket_id:'C'}});
  assert.deepEqual(await w.ids(),['B']);
  assert.deepEqual((await w.send('getNavigationList',{context:{uid:'another',orgId:'org-3'}})).tickets,[]);
});
test('filter toggles immediately rebuild both navigation and export from retained tickets',async()=>{
  const w=boot(); await w.capture([A,B,C]); w.settings.hideArchived=true;
  assert.deepEqual(await w.ids(),['A','C']); assert.equal((await w.send('exportTickets')).count,2);
  w.settings.hideArchived=false; w.settings.hideNoSubject=true; assert.deepEqual(await w.ids(),['A','C']);
  w.settings.hideNoSubject=false; assert.deepEqual(await w.ids(),['A','B','C']);
});
test('ticket details update an existing record without replacing the list',async()=>{
  const w=boot(); await w.capture([A,B]);
  await w.send('captureTicket',{context:{uid:'test',orgId:'org-1'},requestedAt:Date.now()+100,
    ticket:{ticket_id:'A',status:'ARCHIVED'}});
  assert.deepEqual(await w.ids(),['A','B']); w.settings.hideArchived=true; assert.deepEqual(await w.ids(),[]);
  assert.match((await w.send('exportTickets')).csv,/Автор/);
});
test('concurrent pages survive asynchronous storage writes',async()=>{
  const w=boot(); await Promise.all([w.capture([A]),w.capture([B],{offset:1}),w.capture([C],{offset:2})]);
  assert.deepEqual(await w.ids(),['A','B','C']);
});
test('late responses from old filters cannot replace newer selection',async()=>{
  const w=boot(); await w.capture([B],{subject:'new'},1,200);
  await w.capture([A],{subject:'old'},1,100); assert.deepEqual(await w.ids(),['B']);
});
test('late first page is inserted before an already received continuation',async()=>{
  const w=boot(); await w.capture([B],{offset:1},1,200); await w.capture([A],{},1,100);
  assert.deepEqual(await w.ids(),['A','B']);
});
test('old duplicate response cannot roll back a newer status',async()=>{
  const w=boot(); await w.capture([{...A,subject:'Newest'}],{},1,200);
  await w.capture([A],{},1,100); assert.match((await w.send('exportTickets')).csv,/Newest/);
});
test('manual clear rejects outstanding earlier responses',async()=>{
  const w=boot(); await w.capture([A],{},1,100); await w.send('clearTickets'); await w.capture([B],{},1,200);
  assert.deepEqual(await w.ids(),[]);
});
test('storage write failures are returned, never acknowledged as saved',async()=>{
  const w=boot(); await w.capture([A]); w.setFailWrite(true);
  const result=await w.capture([B]); assert.equal(result.success,false); assert.match(result.error,/QUOTA_BYTES/);
  w.setFailWrite(false); assert.deepEqual(await w.ids(),['A']);
});
test('closed tab removes only its session record',async()=>{
  const w=boot(); await w.capture([A],{},1); await w.capture([B],{},2); w.onRemoved(1);
  await w.send('getTicketsCount',{},1); assert.equal(w.persisted['supportCenter.tab.1'],undefined);
  assert.deepEqual(await w.ids(2),['B']);
});
test('CSV retains escaping, BOM, translations and export columns',async()=>{
  const w=boot(); await w.capture([{...A,subject:'Hello, "world"\nsecond',created_by:{email:'synthetic@example.invalid'}}]);
  const csv=(await w.send('exportTickets')).csv;
  assert(csv.startsWith('\uFEFFАвтор,Дата создания,ID тикета,Внешний ID,Тема,Статус,Сервис'));
  assert.match(csv,/"Hello, ""world""\nsecond",Новый,Диск/);
});
test('empty first page prevents an older outstanding continuation from resurrecting tickets',async()=>{
  const w=boot(); await w.capture([A],{},1,100); await w.capture([],{},1,300);
  await w.capture([B],{offset:1},1,200); assert.deepEqual(await w.ids(),[]);
});
test('manual clear barrier survives a later successful response',async()=>{
  const w=boot(); await w.send('clearTickets'); await w.capture([A],{},1,Date.now()+100);
  await w.capture([B],{offset:1},1,100); assert.deepEqual(await w.ids(),['A']);
});
test('a shorter first page after reload preserves tickets collected with a larger limit',async()=>{
  const w=boot(); await w.capture([A,B,C],{limit:100}); await w.capture([{...A,subject:'Fresh'}],{limit:1});
  assert.deepEqual(await w.ids(),['A','B','C']); assert.match((await w.send('exportTickets')).csv,/Fresh/);
});
test('a hidden current ticket remains an anchor between visible neighbours',async()=>{
  const w=boot();await w.capture([A,B,C]);w.settings.hideArchived=true;w.settings.hideNoSubject=true;
  const result=await w.send('getNavigationList',{currentTicketId:'B'});
  assert.deepEqual(result.tickets,['A','B','C']);
  assert.deepEqual(await w.ids(),['A','C']);
  assert.equal((await w.send('exportTickets')).count,2);
});
test('detail response with a different context never deletes the collected selection',async()=>{
  const w=boot();await w.capture([A,B,C]);
  await w.send('captureTicket',{context:{uid:'test',orgId:'card-org'},requestedAt:Date.now()+100,
    ticket:{ticket_id:'A',subject:'Different context'}});
  assert.deepEqual(await w.ids(),['A','B','C']);
  assert.equal((await w.send('getTicketsCount')).count,3);
  assert.match((await w.send('exportTickets')).csv,/Original/);
});
test('list requests made in a ticket card cannot replace the collected selection',async()=>{
  const w=boot();await w.capture([A,B,C]);
  for(const tickets of [[],[{...A,ticket_id:'X'}]]) {
    await w.send('captureTickets',{tickets,filters:{org_id:'card-org'},context:{uid:'test'},
      pageUrl:'https://admin.yandex.ru/support-center/A?uid=test',
      receivedPageUrl:'https://admin.yandex.ru/support-center/A?uid=test',requestedAt:Date.now()+100});
    assert.deepEqual(await w.ids(),['A','B','C']);
  }
});
test('list response arriving after leaving the list cannot clear the selection',async()=>{
  const w=boot();await w.capture([A,B,C]);
  await w.send('captureTickets',{tickets:[],filters:{org_id:'org-1'},context:{uid:'test'},
    pageUrl:'https://admin.yandex.ru/support-center?uid=test',
    receivedPageUrl:'https://admin.yandex.ru/support-center/A?uid=test',requestedAt:Date.now()+100});
  assert.deepEqual(await w.ids(),['A','B','C']);
});
test('malformed nonempty list data is not treated as an empty selection',async()=>{
  const w=boot();await w.capture([A,B,C]);
  const result=await w.capture([{id:'unexpected'}]);
  assert.equal(result.ignored,true);assert.deepEqual(await w.ids(),['A','B','C']);
});
test('diagnostics distinguish a context mismatch from deleted data without exposing contents',async()=>{
  const w=boot();await w.capture([A,B,C]);
  const diagnostic=await w.send('getDiagnostics',{context:{uid:'another'}});
  assert.equal(diagnostic.stored,3);assert.equal(diagnostic.available,0);assert.equal(diagnostic.contextMismatch,true);
  const serialized=JSON.stringify(diagnostic);
  for(const privateValue of ['Original','org-1','ticket_id','subject','test@example.com']) assert(!serialized.includes(privateValue));
  for(let i=0;i<20;i++) await w.capture([A,B,C]);
  assert.equal((await w.send('getDiagnostics')).history.length,12);
});
test('known route ID takes precedence over a prefetched detail ID, including hidden current tickets',async()=>{
  const w=boot();await w.capture([A,B,C]);w.settings.hideArchived=true;
  const nav=await w.send('getNavigationList',{routeTicketId:'B',currentTicketId:'unrelated'});
  assert.equal(nav.currentTicketId,'B');assert.deepEqual(nav.tickets,['A','B','C']);
});
test('missing subject is unknown, not proof that a ticket is subjectless',async()=>{
  const w=boot();await w.capture([{ticket_id:'unknown',status:'NEW'},
    {...A,subject:''},{...B,subject:null},{...C,subject:'Без темы'}]);
  w.settings.hideNoSubject=true;
  assert.deepEqual(await w.ids(),['unknown']);
  const count=await w.send('getTicketsCount');assert.equal(count.count,4);assert.equal(count.visibleCount,1);
  assert.equal((await w.send('exportTickets')).count,1);
});
test('unrecognised continuation parameter cannot replace 15 tickets with 4 subjectless tickets',async()=>{
  const w=boot();
  const first=Array.from({length:15},(_,i)=>({...A,ticket_id:`first-${i}`}));
  const tail=Array.from({length:4},(_,i)=>({...B,ticket_id:`tail-${i}`}));
  await w.capture(first,{limit:15});
  await w.capture(tail,{limit:15,continuation:{next:'opaque-token'}});
  w.settings.hideNoSubject=true;
  assert.equal((await w.send('getTicketsCount')).count,19);
  assert.deepEqual(await w.ids(),first.map(ticket=>ticket.ticket_id));
  const nav=await w.send('getNavigationList',{currentTicketId:'first-5',routeTicketId:'first-5'});
  assert.equal(nav.metadata.currentFound,true);
  await w.capture(first,{limit:15});
  w.settings.hideNoSubject=false;
  assert.deepEqual(await w.ids(),[...first,...tail].map(ticket=>ticket.ticket_id));
});
test('empty optional filters and omitted optional filters describe the same selection',async()=>{
  const w=boot();await w.capture([A,B],{subject:'',products:[],statuses:null,filters:{subject:'',products:[]},filter:{}});
  await w.capture([C],{cursor:'next'});
  assert.deepEqual(await w.ids(),['A','B','C']);
});
test('an empty unknown continuation is not an empty first page',async()=>{
  const w=boot();await w.capture([A,B]);await w.capture([],{continuation:{next:'end'}});
  assert.deepEqual(await w.ids(),['A','B']);
});
test('late first response is ordered before an unknown continuation',async()=>{
  const w=boot();await w.capture([C],{continuation:{next:'tail'}},1,200);
  await w.capture([A,B],{},1,100);
  assert.deepEqual(await w.ids(),['A','B','C']);
});
test('meaningful search, nested filters and sorting changes still replace the selection',async()=>{
  const w=boot();await w.capture([A],{filters:{products:['disk']},sort_by:'created_at'});
  await w.capture([B],{filters:{products:['mail']},sort_by:'created_at'});assert.deepEqual(await w.ids(),['B']);
  await w.capture([C],{filters:{products:['mail']},sort_by:'updated_at'});assert.deepEqual(await w.ids(),['C']);
  await w.capture([],{search:'nothing'});assert.deepEqual(await w.ids(),[]);
});
test('opaque page parameters outside the cursor aliases accumulate and keep order even when responses arrive late',async()=>{
  const w=boot();await w.capture([C],{pageMarker:{opaque:'tail'}},1,200);
  await w.capture([A,B],{},1,100);assert.deepEqual(await w.ids(),['A','B','C']);
  await w.capture([],{pageMarker:{opaque:'end'}},1,300);assert.deepEqual(await w.ids(),['A','B','C']);
  await w.capture([A,B],{limit:30},1,400);assert.deepEqual(await w.ids(),['A','B','C']);
});
