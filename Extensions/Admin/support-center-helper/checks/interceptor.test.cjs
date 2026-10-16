const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../interceptor.js'),'utf8');
const LIST='support-b2b/get-tickets';
const tick=()=>new Promise(resolve=>setImmediate(resolve));
function setup() {
  let json={models:[{name:LIST,data:{items:[{ticket_id:'A'}]}}]}, status=200, originalPromise;
  const window=new EventTarget();
  const received=[];
  window.addEventListener('supportHelperNetwork',event=>received.push(JSON.parse(event.detail)));
  window.fetch=()=>{originalPromise=Promise.resolve(new Response(JSON.stringify(json),{status}));return originalPromise;};
  class XHR extends EventTarget {
    open(){} send(){} setRequestHeader(){} finish(data,responseType='json'){
      this.status=200;this.responseType=responseType;this.response=data;this.responseText=JSON.stringify(data);
      this.dispatchEvent(new Event('load'));this.dispatchEvent(new Event('loadend'));
    }
  }
  const ctx={window,XMLHttpRequest:XHR,CustomEvent,Request,Response,Headers,URL,URLSearchParams,
    location:{href:'https://admin.yandex.ru/support-center?uid=synthetic',search:'?uid=synthetic'},performance};
  vm.runInNewContext(source,ctx);
  return {window,received,XHR,location:ctx.location,original:()=>originalPromise,
    setResponse(value,code=200){json=value;status=code;},ready(){window.dispatchEvent(new CustomEvent('supportHelperReady'));},
    async fetch(params={},extraModels=[]){await window.fetch('/api/models',{method:'POST',body:JSON.stringify({models:[...extraModels,{name:LIST,params}]})});await tick();}
  };
}
test('events captured before bridge readiness are flushed exactly once',async()=>{
  const s=setup();await s.fetch({org_id:'org'});assert.equal(s.received.length,0);
  s.ready();s.ready();assert.equal(s.received.length,1);assert.equal(s.received[0].context.uid,'synthetic');
});
test('empty successful response is forwarded with filters',async()=>{
  const s=setup();s.ready();s.setResponse({models:[{name:LIST,data:{items:[]}}]});await s.fetch({subject:'empty'});
  assert.equal(s.received.length,1);assert.deepEqual(s.received[0].tickets,[]);assert.equal(s.received[0].filters.subject,'empty');
});
test('filters are taken from matching model rather than first batch entry',async()=>{
  const s=setup();s.ready();await s.fetch({org_id:'correct'},[{name:'other',params:{org_id:'wrong'}}]);
  assert.equal(s.received[0].filters.org_id,'correct');
});
test('fetch Request body is cloned and original response remains readable',async()=>{
  const s=setup();s.ready();
  const request=new Request('https://admin.yandex.ru/api/models',{method:'POST',body:JSON.stringify({models:[{name:LIST,params:{org_id:'org'}}]})});
  const result=s.window.fetch(request);assert.equal(result,s.original());
  assert.equal((await (await result).json()).models[0].data.items[0].ticket_id,'A');
  await tick();await tick();assert.equal(s.received.length,1);assert.equal(request.bodyUsed,false);
});
test('HTTP and model errors cannot clear a cached list',async()=>{
  const s=setup();s.ready();s.setResponse({models:[{name:LIST,data:{items:[]}}]},500);await s.fetch();
  s.setResponse({models:[{name:LIST,error:'failed',data:{items:[]}}]});await s.fetch();
  assert.equal(s.received.length,0);
});
test('XHR supports JSON and text responses, including reuse',()=>{
  const s=setup();s.ready();const xhr=new s.XHR();
  for(const responseType of ['json','text']){
    xhr.open('POST','/api/models');xhr.send(JSON.stringify({models:[{name:LIST,params:{org_id:'org'}}]}));
    xhr.finish({models:[{name:LIST,data:{items:[]}}]},responseType);
  }
  assert.equal(s.received.length,2);assert.deepEqual(s.received[0].tickets,[]);
});
test('direct GET endpoint URL parameters are preserved',async()=>{
  const s=setup();s.ready();s.setResponse({data:{items:[{ticket_id:'A'}]}});
  await s.window.fetch('https://admin.yandex.ru/support-b2b/get-tickets?org_id=org&cursor=next');await tick();
  assert.equal(s.received[0].filters.org_id,'org');assert.equal(s.received[0].filters.cursor,'next');
});
test('detail model forwards ticket fields without capturing authentication headers',async()=>{
  const s=setup();s.ready();s.setResponse({models:[{name:'support-b2b/get-ticket',data:{ticket:{ticket_id:'A',status:'CLOSED'}}}]});
  await s.window.fetch('/api/models',{method:'POST',body:JSON.stringify({models:[{name:'support-b2b/get-ticket',params:{org_id:'org',ticket_id:'A'}}]})});await tick();
  assert.equal(s.received[0].action,'captureTicket');assert.equal(s.received[0].ticket.status,'CLOSED');assert.equal(s.received[0].sarahCkey,undefined);
});
test('UID can be recovered from Request/XHR headers when URL has no UID',async()=>{
  const s=setup();s.ready();s.location.search='';
  const body=JSON.stringify({models:[{name:LIST,params:{org_id:'org'}}]});
  await s.window.fetch(new Request('https://admin.yandex.ru/api/models',{method:'POST',body,headers:{'x-sarah-uid':'header-user'}}));
  await tick();await tick();assert.equal(s.received[0].context.uid,'header-user');
  const xhr=new s.XHR();xhr.open('POST','/api/models');xhr.setRequestHeader('X-Sarah-Uid','xhr-user');xhr.send(body);
  xhr.finish({models:[{name:LIST,data:{items:[]}}]});assert.equal(s.received[1].context.uid,'xhr-user');
});
test('successful model envelopes accept uppercase status and empty error containers',async()=>{
  const s=setup();s.ready();
  for(const status of ['OK','SUCCESS','200',200,undefined]) {
    s.setResponse({models:[{name:LIST,status,error:{},errors:[],data:{items:[{ticket_id:'A'}]}}]});
    await s.fetch();
  }
  assert.equal(s.received.length,5);
});
test('model ID can be omitted in a unique response or represented as a string',async()=>{
  const s=setup();s.ready();
  for(const id of [undefined,'7']) {
    s.setResponse({models:[{name:LIST,id,data:{items:[{ticket_id:'A'}]}}]});
    await s.window.fetch('/api/models',{method:'POST',body:JSON.stringify({models:[{name:LIST,id:7,params:{org_id:'org'}}]})});
    await tick();
  }
  assert.equal(s.received.length,2);
});
test('explicit error statuses and populated error containers do not clear a list',async()=>{
  const s=setup();s.ready();
  for(const envelope of [{status:'ERROR'},{status:500},{errors:['failed']},{error:{code:'failed'}}]) {
    s.setResponse({models:[{name:LIST,...envelope,data:{items:[]}}]});await s.fetch();
  }
  assert.equal(s.received.length,0);
});
test('detail identity is bound to the page where the request started',async()=>{
  const s=setup();s.ready();const pageUrl=s.location.href;
  s.setResponse({models:[{name:'support-b2b/get-ticket',data:{ticket:{ticket_id:'A'}}}]});
  const response=s.window.fetch('/api/models',{method:'POST',body:JSON.stringify({models:[{name:'support-b2b/get-ticket',params:{ticket_id:'A'}}]})});
  s.location.href='https://admin.yandex.ru/support-center/B';await response;await tick();
  assert.equal(s.received[0].pageUrl,pageUrl);assert.equal(s.received[0].currentTicketId,'A');
});
test('ambiguous model responses and explicit mismatching IDs are never paired by name',async()=>{
  const s=setup();s.ready();
  for(const models of [[{name:LIST,id:8,data:{items:[]}}],[{name:LIST,data:{items:[]}},{name:LIST,data:{items:[]}}]]) {
    s.setResponse({models});
    await s.window.fetch('/api/models',{method:'POST',body:JSON.stringify({models:[{name:LIST,id:7,params:{}}]})});await tick();
  }
  assert.equal(s.received.length,0);
});
