const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const {base,settings,rules}=require('./fixtures.cjs');
const clone = value => JSON.parse(JSON.stringify(value));
async function setup(managed = {}, user = {}, initialFile, failWrite = false) {
  const io=require('./file-store-fixture.cjs')();
  io.failWrite=failWrite;
  if(initialFile !== undefined) io.files.set('user-rules.json', initialFile);
  const listeners=[];
  const local={userSettings:clone(user)};
  const policy=clone(managed);
  const chrome={
    runtime:{getURL:name=>name},
    storage:{
      onChanged:{addListener:fn=>listeners.push(fn)},
      managed:{get:async()=>clone(policy)},
      local:{
        get:async key=>key ? {[key]:local[key]} : clone(local),
        set:async values=>{
          const changes={};
          for(const [key,value] of Object.entries(values)) {
            if(JSON.stringify(local[key])!==JSON.stringify(value)) changes[key]={oldValue:local[key],newValue:clone(value)};
            local[key]=clone(value);
          }
          if(Object.keys(changes).length) queueMicrotask(()=>listeners.forEach(fn=>fn(changes,'local')));
        }
      }
    }
  };
  const ctx=vm.createContext({chrome,URL,TextEncoder,navigator:io.navigator,fetch:async name=>({ok:true,json:async()=>clone(name==='config.json'?settings:rules)})});
  for(const file of ['url-router.js','config-core.js','user-rules-file.js','config-service.js']) vm.runInContext(fs.readFileSync(path.join(root,file),'utf8'),ctx);
  const service=ctx.ConfigService;
  await service.ready;
  return {service,local,policy,io,changePolicy(values){Object.assign(policy,values);listeners.forEach(fn=>fn({},'managed'));}};
}
test('managed JSON replaces packaged defaults and ignores old user overrides when locked',async()=>{
  const config=clone(base);config.allowUserChanges=false;config.rules[2].enabled=false;
  const {service}=await setup({ConfigJSON:JSON.stringify(config)},{enabled:{'telemost-staff':true,'telemost-j':false}});
  assert.equal(service.snapshot.managed,true);
  assert.equal(service.snapshot.config.rules[2].enabled,false);
  assert.equal(service.snapshot.config.rules[0].enabled,true);
  assert.equal(service.toAppUrl('https://telemost.yandex.ru/staff/person@example.net'),null);
  await assert.rejects(()=>service.execute({type:'set-enabled',id:'telemost-staff',enabled:true}),/запрещены/);
});
test('separate enterprise flags override flags inside ConfigJSON',async()=>{
  const {service}=await setup({ConfigJSON:JSON.stringify(base),AllowUserChanges:false,AllowCustomRules:false});
  assert.equal(service.snapshot.controls.canEdit,false);
  assert.equal(service.snapshot.controls.canAdd,false);
});
test('policy change is rechecked at mutation time even before a change event',async()=>{
  const {service,policy}=await setup();
  policy.AllowUserChanges=false;
  await assert.rejects(()=>service.execute({type:'set-enabled',id:'telemost-j',enabled:false}),/запрещены/);
  assert.equal(service.snapshot.config.rules[0].enabled,true);
});
test('malformed managed JSON disables interception until corrected, never defaults to enabled',async()=>{
  const {service,changePolicy}=await setup({ConfigJSON:'{bad'});
  assert.equal(service.snapshot.config.rules.length,0);
  assert.match(service.snapshot.error,/не загружены/);
  assert.equal(service.toAppUrl('https://telemost.yandex.ru/j/123'),null);
  changePolicy({ConfigJSON:JSON.stringify(base)});await service.ready;
  assert.equal(service.snapshot.error,null);
  assert.equal(service.toAppUrl('https://telemost.yandex.ru/j/123'),'telemost://https://telemost.yandex.ru/j/123');
});
test('UI operation updates both persisted JSON user settings and the content-script cache',async()=>{
  const {service,local}=await setup();
  await service.execute({type:'set-enabled',id:'telemost-join',enabled:false});await service.ready;
  assert.equal(local.userSettings.enabled['telemost-join'],false);
  assert.equal(local.effectiveConfig.rules[1].enabled,false);
  assert.equal(service.toAppUrl('https://telemost.360.yandex.ru/join#room'),null);
});

test('separate Settings and Rules policies work with the existing enterprise locks',async()=>{
  const policyRules=clone(rules); policyRules.rules[0].enabled=false;
  const {service}=await setup({ConfigJSON:JSON.stringify({...settings,allowUserChanges:false}),RulesJSON:JSON.stringify(policyRules)});
  assert.equal(service.snapshot.error,null);
  assert.equal(service.toAppUrl('https://telemost.yandex.ru/j/123'),null);
  assert.equal(service.snapshot.controls.canEdit,false);
});
const custom = {
  id:'custom-migrate',name:'Migrated',enabled:false,userToggleAllowed:true,
  match:{origins:['https://example.org'],pathname:'/room/{tail}'},target:'sampleapp://room/{tail}',
  examples:[{url:'https://example.org/room/123456',appUrl:'sampleapp://room/123456'}]
};
test('old user rules migrate to a physical JSON file before removal from settings',async()=>{
  const {service,local,io}=await setup({}, {enabled:{'telemost-j':false},rules:[custom]});
  assert.equal(service.snapshot.error,null);
  assert.equal(local.userSettings.rules,undefined);
  assert.equal(local.userSettings.enabled['telemost-j'],false);
  assert.equal(local.userSettings.rulesFileInitialized,true);
  assert.deepEqual(JSON.parse(io.files.get('user-rules.json')).rules,[custom]);
});
test('existing user file is the source of truth and survives an empty settings record',async()=>{
  const {service,io}=await setup({}, {}, JSON.stringify({schemaVersion:1,rules:[custom]}));
  assert.equal(service.snapshot.error,null);
  assert.equal(service.snapshot.config.rules.at(-1).id,custom.id);
  assert.equal(JSON.parse(io.files.get('user-rules.json')).rules[0].enabled,false);
});
test('save, toggle and remove update the separate file without storing rules in userSettings',async()=>{
  const {service,local,io}=await setup();
  await service.execute({type:'save-custom',rule:{...custom,enabled:true}});
  assert.equal(local.userSettings.rules,undefined);
  assert.equal(JSON.parse(io.files.get('user-rules.json')).rules[0].enabled,true);
  await service.execute({type:'set-enabled',id:custom.id,enabled:false});
  assert.equal(JSON.parse(io.files.get('user-rules.json')).rules[0].enabled,false);
  await service.execute({type:'remove-custom',id:custom.id});
  assert.deepEqual(JSON.parse(io.files.get('user-rules.json')).rules,[]);
});
test('failed file write leaves the old file, stored settings and routing unchanged',async()=>{
  const {service,local,io}=await setup();
  const oldFile=io.files.get('user-rules.json');const oldSettings=clone(local.userSettings);
  io.failWrite=true;
  await assert.rejects(()=>service.execute({type:'save-custom',rule:custom}),/Disk write failed/);
  assert.equal(io.files.get('user-rules.json'),oldFile);
  assert.deepEqual(local.userSettings,oldSettings);
  assert.equal(service.snapshot.config.rules.length,base.rules.length);
});
test('malformed user file is not overwritten and can be restored by an explicit import',async()=>{
  const {service,io}=await setup({}, {rulesFileInitialized:true}, '{bad');
  assert.match(service.snapshot.error,/не загружены/);
  assert.equal(io.files.get('user-rules.json'),'{bad');
  assert.equal(service.toAppUrl('https://telemost.yandex.ru/j/123'),null);
  assert.equal(service.snapshot.controls.canEdit,true);
  await service.execute({type:'import-file',document:{schemaVersion:1,rules:[custom]}});
  assert.equal(service.snapshot.error,null);
  assert.equal(JSON.parse(io.files.get('user-rules.json')).rules[0].id,custom.id);
});
test('missing initialized user file fails closed until an explicit reset or import',async()=>{
  const {service,io}=await setup({}, {rulesFileInitialized:true});
  assert.match(service.snapshot.error,/отсутствует/);
  assert.equal(io.files.has('user-rules.json'),false);
  await service.execute({type:'reset-settings'});
  assert.equal(service.snapshot.error,null);
  assert.deepEqual(JSON.parse(io.files.get('user-rules.json')).rules,[]);
});
test('three exports have disjoint settings, bundled rules and personal rule contents',async()=>{
  const {service}=await setup({}, {rules:[custom],enabled:{'telemost-j':false}});
  const docs=await service.exportFiles();
  assert.equal(docs.settings.rules,undefined);
  assert.equal(docs.settings.enabled['telemost-j'],false);
  assert.equal(docs.rules.allowUserChanges,undefined);
  assert.equal(docs.rules.rules.length,base.rules.length);
  assert.deepEqual(clone(docs.userRules),{schemaVersion:1,rules:[custom]});
});
test('importing settings preserves personal rules and cannot override organization flags',async()=>{
  const {service,io}=await setup({}, {rules:[custom]});
  await service.execute({type:'import-file',document:{schemaVersion:1,allowUserChanges:false,allowCustomRules:false,enabled:{'telemost-j':false}}});
  assert.equal(service.snapshot.config.allowUserChanges,true);
  assert.equal(service.snapshot.config.allowCustomRules,true);
  assert.equal(service.snapshot.config.rules[0].enabled,false);
  assert.deepEqual(JSON.parse(io.files.get('user-rules.json')).rules,[custom]);
});
test('importing personal rules preserves built-in switches and rejects bundled IDs',async()=>{
  const {service,local}=await setup({}, {enabled:{'telemost-j':false}});
  await service.execute({type:'import-file',document:{schemaVersion:1,rules:[custom]}});
  assert.equal(local.userSettings.enabled['telemost-j'],false);
  await assert.rejects(()=>service.execute({type:'import-file',document:rules}),/custom-/);
  assert.equal(service.snapshot.config.rules.at(-1).id,custom.id);
});
test('a failed first migration preserves the legacy copy and retries without loss',async()=>{
  const user={enabled:{'telemost-j':false},rules:[custom]};
  const {service,local,io}=await setup({},user,undefined,true);
  assert.match(service.snapshot.error,/Disk write failed/);
  assert.deepEqual(local.userSettings,user);
  assert.equal(io.files.has('user-rules.json'),false);
  io.failWrite=false;
  await service.execute({type:'set-enabled',id:'telemost-join',enabled:false});
  assert.equal(service.snapshot.error,null);
  assert.equal(local.userSettings.rules,undefined);
  assert.deepEqual(JSON.parse(io.files.get('user-rules.json')).rules,[custom]);
});
test('bad RulesJSON does not fall back to the packaged rules',async()=>{
  const {service}=await setup({RulesJSON:'{bad'});
  assert.match(service.snapshot.error,/не загружены/);
  assert.equal(service.snapshot.config.rules.length,0);
});
test('the old combined JSON imports through the new file entry point',async()=>{
  const incoming=clone(base);incoming.rules[0].enabled=false;incoming.rules.push(custom);
  const {service,io}=await setup();
  await service.execute({type:'import-file',document:incoming});
  assert.equal(service.snapshot.config.rules[0].enabled,false);
  assert.deepEqual(JSON.parse(io.files.get('user-rules.json')).rules,[custom]);
});
