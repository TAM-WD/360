const {test}=require('node:test');
const assert=require('node:assert/strict');
const fs=require('node:fs');
const path=require('node:path');
const vm=require('node:vm');
test('a settings change during initialization preserves unchanged stored navigation setting',async()=>{
  let changed, resolveSettings, enabled;
  const window=new EventTarget();
  const sandbox={window,CustomEvent,console,URLSearchParams,
    location:{href:'https://admin.yandex.ru/support-center',pathname:'/support-center',search:''},
    document:{},MutationObserver:class{observe(){}},setTimeout(){return 1;},clearTimeout(){},setInterval(){},
    TicketNavigation:class{start(){}schedule(){}setEnabled(value){enabled=value;}},
    chrome:{runtime:{onMessage:{addListener(){}},sendMessage:async()=>({success:true})},
      storage:{onChanged:{addListener(fn){changed=fn;}},local:{get:()=>new Promise(resolve=>resolveSettings=resolve)}}}
  };
  vm.runInNewContext(fs.readFileSync(path.join(__dirname,'../content.js'),'utf8'),sandbox);
  changed({hideArchived:{newValue:true}},'local');
  resolveSettings({hideArchived:false,hideNoSubject:true,enableNavigation:true});
  await new Promise(resolve=>setImmediate(resolve));
  assert.equal(enabled,true);
});
