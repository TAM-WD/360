const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const assert = require('node:assert/strict');
const { webcrypto } = require('node:crypto');
const root = join(__dirname, '..');
const ctx = vm.createContext({ URL, crypto: webcrypto });
for (const file of ['url-router.js', 'rule-inference.js', 'config-core.js']) vm.runInContext(readFileSync(join(root, file), 'utf8'), ctx);
const { TelemostRouter: R, RuleInference: I, ConfigCore: C } = ctx;
const base = require('./fixtures.cjs').base;
const copy = value => JSON.parse(JSON.stringify(value));
const infer = (url, appUrl) => I.infer(url, appUrl, 'custom-private').rule;
const save = rule => C.change(base, {}, { type: 'save-custom', rule }).rules[0];
const cases = [
  ['numeric and text query', 'https://meet.example/room/987654321?pwd=PrivatePassword', 'myapp://join?id=987654321&password=PrivatePassword', ['987654321', 'PrivatePassword']],
  ['Telegram invite', 'https://t.me/+Private_Invite987', 'tg://join?invite=Private_Invite987', ['Private_Invite987']],
  ['Messenger hash route', 'https://messenger.example/#/staff/PrivateLogin?selectedUid=987654321', 'telemost://ychat/telemost.yandex.ru/staff/PrivateLogin?selectedUid=987654321', ['PrivateLogin', '987654321']],
  ['whole encoded URL', 'https://meet.example/room/987654321?pwd=PrivatePassword#PrivateHash', 'myapp://open?url=https%3A%2F%2Fmeet.example%2Froom%2F987654321%3Fpwd%3DPrivatePassword%23PrivateHash', ['987654321', 'PrivatePassword', 'PrivateHash']],
  ['fragment', 'https://meet.example/join#PrivateHash', 'myapp://join?id=PrivateHash', ['PrivateHash']],
  ['encoded path segment', 'https://meet.example/room/private%40example.net', 'myapp://person/private%40example.net', ['private%40example.net']],
  ['query-only', 'https://meet.example/join?token=PrivateToken', 'myapp://join?token=PrivateToken', ['PrivateToken']]
];
for (const [label, url, appUrl, secrets] of cases) test(`save anonymizes ${label} without changing routing`, () => {
  const original = infer(url, appUrl);
  const saved = save(original);
  assert.deepEqual(copy(saved.match), copy(original.match));
  assert.equal(saved.target, original.target);
  assert.equal(R.toAppUrl(url, undefined, { rules: [saved] }), appUrl);
  assert.equal(R.validateRule(saved).id, original.id);
  for (const value of secrets) assert.equal(JSON.stringify(saved).includes(value), false, value);
  assert.equal(original.examples[0].url, url, 'input is not mutated');
});
test('numeric values and text become consistent, distinguishable sample values', () => {
  const rule = infer('https://meet.example/c/987654321/876543219?pwd=Secret', 'myapp://chat?room=987654321&id=876543219&pwd=Secret');
  const pair = save(rule).examples[0];
  assert.equal(pair.url, 'https://meet.example/c/123456/123457?pwd=a1b2c3');
  assert.equal(pair.appUrl, 'myapp://chat?room=123456&id=123457&pwd=a1b2c3');
});
test('named captures keep literal prefixes/suffixes and per-value equality', () => {
  const rule = { id: 'custom-named', name: 'Named', enabled: true, userToggleAllowed: true, match: { origins: ['https://meet.example'], pathname: '/file-{id}.json', hashPathname: '/copy-{id}.json' }, target: 'myapp://open/{capture:id}/{hashCapture:id}', examples: [{ url: 'https://meet.example/file-999999.json#/copy-999999.json', appUrl: 'myapp://open/999999/999999' }] };
  const saved = save(rule);
  assert.equal(saved.examples[0].url, 'https://meet.example/file-123456.json#/copy-123456.json');
  assert.equal(saved.examples[0].appUrl, 'myapp://open/123456/123456');
});
test('import and re-save are idempotent and export contains synthetic pairs', () => {
  const rule = infer('https://meet.example/room/987654321?pwd=Secret', 'myapp://join?id=987654321&pwd=Secret');
  const user = C.change(base, {}, { type: 'save-custom', rule });
  const exported = copy(C.resolve(base, user).config);
  const imported = C.change(base, {}, { type: 'import-config', config: exported });
  assert.deepEqual(copy(imported.rules), copy(user.rules));
  assert.deepEqual(copy(save(user.rules[0])), copy(user.rules[0]));
  assert.equal(JSON.stringify(exported).includes('Secret'), false);
});
test('direct import also sanitizes real incoming examples', () => {
  const rule = infer('https://meet.example/room/987654321', 'myapp://join/987654321');
  const incoming = { ...copy(base), rules: [...copy(base.rules), rule] };
  const saved = C.change(base, {}, { type: 'import-config', config: incoming });
  assert.equal(saved.rules[0].examples[0].url, 'https://meet.example/room/123456');
});
test('all source examples are replaced and validate against the unchanged template', () => {
  const rule = infer('https://meet.example/room/987654321', 'myapp://join/987654321');
  rule.examples.push({ url: 'https://meet.example/room/876543219', appUrl: 'myapp://join/876543219' });
  const saved = save(rule);
  assert.equal(saved.examples.length, 2);
  assert.equal(saved.examples[1].appUrl, 'myapp://join/123457');
  assert.equal(R.validateRule(saved).examples.length, 2);
});
test('new exact rules cannot retain the real URL; old stored rules still work', () => {
  const rule = infer('https://meet.example/room/Secret', 'myapp://home');
  assert.throws(() => save(rule), /точного URL/);
  const existing = { rules: [rule] };
  assert.equal(R.toAppUrl(rule.examples[0].url, undefined, C.resolve(base, existing).config), 'myapp://home');
  assert.throws(() => C.change(base, {}, { type: 'import-config', config: { ...base, rules: [rule] } }), /точного URL/);
});
test('an invalid source pair cannot be hidden by generating a new example', () => {
  const rule = infer('https://meet.example/room/987654321', 'myapp://join/987654321');
  rule.examples[0].appUrl = 'myapp://wrong';
  assert.throws(() => save(rule), /не воспроизводит/);
});
