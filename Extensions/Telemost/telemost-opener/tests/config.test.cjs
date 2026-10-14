const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const assert = require('node:assert/strict');
const { webcrypto } = require('node:crypto');
const root = join(__dirname, '..');
const ctx = vm.createContext({ URL, crypto: webcrypto });
for (const file of ['url-router.js', 'rule-inference.js', 'config-core.js']) vm.runInContext(readFileSync(join(root,file),'utf8'),ctx);
const { TelemostRouter: R, RuleInference: I, ConfigCore: C } = ctx;
const base = require('./fixtures.cjs').base;
const copy = value => JSON.parse(JSON.stringify(value));
const map = (url, config = base) => R.toAppUrl(url, undefined, config);
test('JSON contains four enabled built-in rules, two examples each', () => {
  const validated = R.validateConfig(base);
  assert.equal(validated.rules.length, 4);
  for (const rule of validated.rules) {
    assert.equal(rule.enabled, true); assert.equal(rule.examples.length, 2);
    for (const pair of rule.examples) assert.equal(map(pair.url), pair.appUrl);
  }
});
for (const rule of base.rules) test(`disabling ${rule.id} disables its examples without affecting other routes`, () => {
  const user = C.change(base, {}, {type:'set-enabled', id:rule.id, enabled:false});
  const config = C.resolve(base, user).config;
  for (const item of base.rules) for (const pair of item.examples) assert.equal(map(pair.url, config), item.id === rule.id ? null : pair.appUrl);
});
test('global lock ignores previously saved user values and rejects import unlock', () => {
  const locked = {...copy(base), allowUserChanges:false};
  const snapshot = C.resolve(locked, {enabled:{'telemost-j':false}});
  assert.equal(snapshot.config.rules[0].enabled,true);
  assert.equal(snapshot.controls.canEdit,false);
  assert.throws(()=>C.change(locked,{}, {type:'import-config',config:base}), /запрещены/);
});
test('per-rule lock also rejects direct writes and ignores imported changes', () => {
  const locked = copy(base); locked.rules[0].userToggleAllowed=false;
  assert.throws(()=>C.change(locked,{}, {type:'set-enabled',id:'telemost-j',enabled:false}), /заблокирован/);
  const imported=copy(base);imported.rules[0].enabled=false;
  const user=C.change(locked,{}, {type:'import-config',config:imported});
  assert.equal(C.resolve(locked,user).config.rules[0].enabled,true);
});
const cases = [
  ['whole URL', 'https://meet.example/room/123?pwd=a%2Bb#test', 'mymeet://https://meet.example/room/123?pwd=a%2Bb#test', 'https://meet.example/room/999?pwd=b%2Bc#other', 'mymeet://https://meet.example/room/999?pwd=b%2Bc#other'],
  ['without scheme', 'https://telemost.example/staff/person@example.net?uid=123', 'mychat://ychat/telemost.example/staff/person@example.net?uid=123', 'https://telemost.example/staff/another@example.net?uid=456', 'mychat://ychat/telemost.example/staff/another@example.net?uid=456'],
  ['encoded URL', 'https://meet.example/join?id=123', 'mymeet://open?url=https%3A%2F%2Fmeet.example%2Fjoin%3Fid%3D123', 'https://meet.example/join?id=456&pwd=A%2BB', 'mymeet://open?url=https%3A%2F%2Fmeet.example%2Fjoin%3Fid%3D456%26pwd%3DA%252BB'],
  ['path and query', 'https://meet.example/room/123?pwd=abc', 'mymeet://host/room/123?pwd=abc', 'https://meet.example/room/456?pwd=def', 'mymeet://host/room/456?pwd=def'],
  ['ID and password', 'https://meet.example/room/123?pwd=a%2Bb', 'mymeet://join?id=123&password=a%2Bb', 'https://meet.example/room/999?pwd=c%2Fd', 'mymeet://join?id=999&password=c%2Fd'],
  ['fragment', 'https://meet.example/join#abc123', 'mymeet://join?id=abc123', 'https://meet.example/join#def456', 'mymeet://join?id=def456'],
  ['hash route', 'https://messenger.360.yandex.ru/#/staff/lyxm', 'telemost://ychat/telemost.yandex.ru/staff/lyxm', 'https://messenger.360.yandex.ru/#/staff/lyxm2', 'telemost://ychat/telemost.yandex.ru/staff/lyxm2'],
  ['hash route segment', 'https://messenger.example/#/staff/lyxm', 'mychat://person/lyxm', 'https://messenger.example/#/staff/lyxm2', 'mychat://person/lyxm2'],
  ['hash route encoded path', 'https://messenger.example/#/staff/lyxm', 'mychat://open?path=%2Fstaff%2Flyxm', 'https://messenger.example/#/staff/lyxm2?uid=456', 'mychat://open?path=%2Fstaff%2Flyxm2%3Fuid%3D456'],
  ['hash route encoded segment', 'https://messenger.example/#/staff/user%40example.net', 'mychat://person/user%2540example.net', 'https://messenger.example/#/staff/other%40example.net', 'mychat://person/other%2540example.net'],
  ['hash route plus outer query', 'https://messenger.example/?tenant=company1#/staff/lyxm', 'mychat://person/lyxm?tenant=company1', 'https://messenger.example/?tenant=company2#/staff/lyxm2', 'mychat://person/lyxm2?tenant=company2']
];
for (const [name, url, appUrl, other, expected] of cases) test(`infers ${name} and substitutes a different link`, () => {
  const {rule}=I.infer(url,appUrl,'custom-test');
  assert.equal(map(url,{rules:[rule]}),appUrl);
  assert.equal(map(other,{rules:[rule]}),expected);
  assert.equal(map(other.replace(new URL(other).origin,'https://evil.example'),{rules:[rule]}),null);
  const serialized=JSON.parse(JSON.stringify(rule));
  assert.equal(R.validateRule(serialized).examples[0].url,new URL(url).href);
});
test('ambiguous equal identifiers fall back to exact URL, never guessed',()=>{
  const {rule}=I.infer('https://meet.example/room/123?id=123','mymeet://join/123','custom-ambiguous');
  assert.equal(rule.match.exactUrl,'https://meet.example/room/123?id=123');
  assert.equal(map('https://meet.example/room/456?id=456',{rules:[rule]}),null);
});
test('unrelated constant mapping remains exact',()=>{
  const {rule}=I.infer('https://meet.example/room/123','mymeet://open/home','custom-exact');
  assert.equal(rule.match.exactUrl,'https://meet.example/room/123');
});
test('missing or ambiguous required query values do not launch',()=>{
  const {rule}=I.infer('https://meet.example/room/123?pwd=abc','mymeet://join?id=123&pwd=abc','custom-q');
  for(const url of ['https://meet.example/room/999','https://meet.example/room/999?pwd=','https://meet.example/room/999?pwd=a&pwd=b']) assert.equal(map(url,{rules:[rule]}),null);
});
test('partial identifier coincidence is not generalized',()=>{
  const {rule}=I.infer('https://meet.example/room/123','mymeet://open/91234','custom-partial');
  assert.ok(rule.match.exactUrl);
});
test('custom rules survive JSON export/import; policy fields and installed templates cannot be overridden',()=>{
  const {rule}=I.infer('https://meet.example/room/123','mymeet://join?id=123','custom-test');
  const added=C.change(base,{}, {type:'save-custom',rule});
  const exported=copy(C.resolve(base,added).config);
  const imported=C.change(base,{}, {type:'import-config',config:exported});
  assert.equal(map('https://meet.example/room/456',C.resolve(base,imported).config),'mymeet://join?id=456');
  exported.allowUserChanges=false;
  const result=C.resolve(base,C.change(base,{}, {type:'import-config',config:exported}));
  assert.equal(result.config.allowUserChanges,true);
});
test('disallowing personal rules hides saved ones and prevents new additions',()=>{
  const {rule}=I.infer('https://meet.example/r/123','mymeet://join?id=123','custom-test');
  const locked={...base,allowCustomRules:false};
  assert.equal(C.resolve(locked,{rules:[rule]}).config.rules.length,base.rules.length);
  assert.throws(()=>C.change(locked,{}, {type:'save-custom',rule}), /запрещены/);
});
test('disabled installed scope cannot be re-enabled through a custom fallback',()=>{
  const cfg=copy(base);cfg.rules[0].enabled=false;
  const shadow=copy(base.rules[0]);shadow.id='custom-shadow';cfg.rules.push(shadow);
  assert.equal(map(base.rules[0].examples[0].url,cfg),null);
});
for(const scheme of ['https://evil.example/', 'javascript:alert(1)', 'file:///tmp/x', 'data:text/html,x', 'shell:run', 'chrome://settings']) test(`rejects non-app or executable target ${scheme}`,()=>{
  assert.throws(()=>I.infer('https://meet.example/room/123',scheme,'custom-bad'));
});
test('rejects invalid examples, templates, duplicate IDs and wildcard origins',()=>{
  const bad=copy(base.rules[0]);bad.target='telemost://{unknown}';assert.throws(()=>R.validateRule(bad));
  bad.target='telemost://wrong';assert.throws(()=>R.validateRule(bad));
  const wildcard=copy(base.rules[0]);wildcard.match.origins=['https://*.example.com'];assert.throws(()=>R.validateRule(wildcard), /Origin/);
  const cfg=copy(base);cfg.rules.push(copy(base.rules[0]));assert.throws(()=>R.validateConfig(cfg));
});

test('whole-path coincidence inside a longer identifier is not generalized',()=>{
  const {rule}=I.infer('https://meet.example/room/123','mymeet://host/room/1234','custom-whole-partial');
  assert.ok(rule.match.exactUrl);
});

test('hash route inference keeps the staff section, origin and outer path fixed',()=>{
  const {rule}=I.infer('https://messenger.360.yandex.ru/#/staff/lyxm','telemost://ychat/telemost.yandex.ru/staff/lyxm');
  assert.equal(rule.match.exactUrl,undefined);
  assert.equal(rule.match.hashPathname,'/staff/{tail}');
  assert.equal(rule.target,'telemost://ychat/telemost.yandex.ru{hashPathAndQuery}');
  for(const url of ['https://messenger.360.yandex.ru/#/chats/lyxm2', 'https://messenger.360.yandex.ru/#/staff/', 'https://messenger.360.yandex.ru/#staff/lyxm2', 'https://messenger.360.yandex.ru/staff/lyxm2', 'https://messenger.360.yandex.ru/other#/staff/lyxm2']) assert.equal(map(url,{rules:[rule]}),null);
  assert.equal(map('https://messenger.360.yandex.ru/#/staff/user%40example.net?selectedUid=456&from=test',{rules:[rule]}),'telemost://ychat/telemost.yandex.ru/staff/user%40example.net?selectedUid=456&from=test');
});
test('whole-URL inference also confines hash routes to the original section',()=>{
  const {rule}=I.infer('https://web.example/#/staff/lyxm','mychat://https://web.example/#/staff/lyxm');
  assert.equal(map('https://web.example/#/staff/lyxm2',{rules:[rule]}),'mychat://https://web.example/#/staff/lyxm2');
  assert.equal(map('https://web.example/#/admin/lyxm2',{rules:[rule]}),null);
});
test('hash path segments do not include hash query parameters',()=>{
  const {rule}=I.infer('https://web.example/#/staff/lyxm?uid=123','mychat://person/lyxm');
  assert.equal(rule.target,'mychat://person/{hashPath:1}');
  assert.equal(map('https://web.example/#/staff/lyxm2?uid=456',{rules:[rule]}),'mychat://person/lyxm2');
  assert.equal(map('https://web.example/#/staff/lyxm2/extra',{rules:[rule]}),null);
});
test('ambiguous path and hash identifiers keep the exact-URL fallback',()=>{
  const {rule}=I.infer('https://web.example/person/lyxm#/staff/lyxm','mychat://person/lyxm');
  assert.ok(rule.match.exactUrl);
});
test('manual hashTail template preserves encoded tail without decoding it',()=>{
  const {rule}=I.infer('https://web.example/#/staff/lyxm','mychat://host/staff/lyxm');
  rule.target='mychat://host/staff/{hashTail}';
  assert.equal(R.validateRule(rule).target,rule.target);
  assert.equal(map('https://web.example/#/staff/a%2Fb',{rules:[rule]}),'mychat://host/staff/a%2Fb');
});
test('invalid hash path patterns and unresolved hash tokens are rejected',()=>{
  const {rule}=I.infer('https://web.example/#/staff/lyxm','mychat://host/staff/lyxm');
  for(const pattern of ['staff/{p1}','/staff/{bad-name}','/staff/{tail}/suffix','/staff/{p1}?uid=x',false]) {
    assert.throws(()=>R.validateRule({...rule,match:{...rule.match,hashPathname:pattern}}));
  }
  assert.throws(()=>R.validateRule({...rule,target:'mychat://person/{hashPath:9}'}));
  assert.equal(R.render('mychat://person/{hashPath:1}',new URL('https://web.example/#person'),{tail:''}),null);
});
test('exact rules remain exact until explicitly re-analyzed; new templates survive JSON import',()=>{
  const original={id:'custom-mess-staff',name:'mess staff',enabled:false,userToggleAllowed:true,match:{origins:['https://messenger.360.yandex.ru'],pathname:'/',exactUrl:'https://messenger.360.yandex.ru/#/staff/lyxm'},target:'telemost://ychat/telemost.yandex.ru/staff/lyxm',examples:[{url:'https://messenger.360.yandex.ru/#/staff/lyxm',appUrl:'telemost://ychat/telemost.yandex.ru/staff/lyxm'}]};
  const user={rules:[original]}; // Existing profiles may still contain legacy exact rules.
  assert.equal(C.resolve(base,user).config.rules.at(-1).match.exactUrl,original.match.exactUrl);
  const {rule}=I.infer(original.examples[0].url,original.examples[0].appUrl,original.id);
  const updated=C.change(base,user,{type:'save-custom',rule:{...rule,name:original.name}});
  const imported=C.change(base,{}, {type:'import-config',config:copy(C.resolve(base,updated).config)});
  assert.equal(imported.rules.length,1);
  assert.equal(imported.rules[0].id,original.id);
  assert.equal(map('https://messenger.360.yandex.ru/#/staff/lyxm2',C.resolve(base,imported).config),'telemost://ychat/telemost.yandex.ru/staff/lyxm2');
});
