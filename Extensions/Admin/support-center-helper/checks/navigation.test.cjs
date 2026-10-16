const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
const source=fs.readFileSync(path.join(__dirname,'../navigation.js'),'utf8');
function setup() {
  const location={href:'https://admin.yandex.ru/support-center/alias?uid=one',pathname:'/support-center/alias',search:'?uid=one'};
  let message;
  const navigation=vm.runInNewContext(source+'; new TicketNavigation()',{
    location,URLSearchParams,chrome:{runtime:{async sendMessage(value){message=value;return {success:true,tickets:['A','B']};}}}
  });
  return {navigation,location,message:()=>message};
}
test('navigation uses the ticket request ID when the route uses a different identifier',async()=>{
  const s=setup();assert.equal(s.navigation.currentId(),'alias');
  s.navigation.setCurrentTicket('A',s.location.href,100);
  await s.navigation.getList();assert.equal(s.message().currentTicketId,'A');
});
test('late details from a previous route cannot replace the current ticket',()=>{
  const s=setup();const old=s.location.href;
  s.location.href='https://admin.yandex.ru/support-center/B?uid=one';s.location.pathname='/support-center/B';
  s.navigation.setCurrentTicket('A',old,100);assert.equal(s.navigation.currentId(),'B');
});
test('SPA route change discards an earlier request ID even without a new API response',()=>{
  const s=setup();s.navigation.setCurrentTicket('A',s.location.href,100);
  s.location.href='https://admin.yandex.ru/support-center/B?uid=one';s.location.pathname='/support-center/B';
  assert.equal(s.navigation.currentId(),'B');
});
test('out-of-order details cannot override newer identity and numeric IDs are normalized',()=>{
  const s=setup();s.navigation.setCurrentTicket(2,s.location.href,200);
  s.navigation.setCurrentTicket(1,s.location.href,100);assert.equal(s.navigation.currentId(),'2');
});
