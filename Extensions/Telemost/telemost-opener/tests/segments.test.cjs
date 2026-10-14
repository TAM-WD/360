const { readFileSync } = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');
const { webcrypto } = require('node:crypto');
const test = require('node:test');
const assert = require('node:assert/strict');
const root = path.resolve(__dirname, '..');
const ctx = vm.createContext({ URL, crypto: webcrypto });
for (const file of ['url-router.js', 'rule-inference.js', 'config-core.js']) vm.runInContext(readFileSync(path.join(root, file), 'utf8'), ctx);
const { TelemostRouter: R, RuleInference: I, ConfigCore: C } = ctx;
const example = JSON.parse(readFileSync(path.join(root, 'examples/telegram-invite-rule.json')));
const base = require('./fixtures.cjs').base;
const copy = value => JSON.parse(JSON.stringify(value));
const map = (url, rule = example) => R.toAppUrl(url, undefined, { rules: [rule] });
test('the supplied invitation pair validates with named prefix capture', () => {
  assert.equal(R.validateRule(example).target, 'tg://join?invite={capture:invite}');
  assert.equal(map(example.examples[0].url), example.examples[0].appUrl);
});
for (const code of ['OtherCode_123-456', 'a', 'With%2FEncoding', 'a1b2c3-1234567890-2']) test(`substitutes another invite without its plus prefix: ${code}`, () => {
  assert.equal(map(`https://t.me/+${code}`), `tg://join?invite=${code}`);
});
test('missing plus, empty capture, different origin and extra segments are outside the rule', () => {
  for (const url of ['https://t.me/', 'https://t.me/+', 'https://t.me/SomeUser', 'https://t.me/code', 'https://t.me/+code/extra', 'https://t.me/+code/', 'http://t.me/+code', 'https://evil.example/+code']) assert.equal(map(url), null, url);
});
test('an outer query and fragment do not become part of the invite code', () => {
  assert.equal(map('https://t.me/+NewCode?foo=123#section'), 'tg://join?invite=NewCode');
});
test('pathname and path:0 stay raw; capture:invite extracts only the variable', () => {
  const url = new URL('https://t.me/+NewCode');
  assert.equal(R.render('testapp://?raw={pathname}&segment={path:0}&code={capture:invite}', url, R.matches(example, url)), 'testapp://?raw=/+NewCode&segment=+NewCode&code=NewCode');
});
test('prefix and suffix both constrain a single nonempty variable', () => {
  const rule = copy(example);
  rule.match.pathname = '/files/report-{id}.json';
  rule.target = 'testapp://open?id={capture:id}';
  rule.examples = [{ url: 'https://t.me/files/report-123.json', appUrl: 'testapp://open?id=123' }];
  R.validateRule(rule);
  assert.equal(map('https://t.me/files/report-456.json', rule), 'testapp://open?id=456');
  for (const name of ['report-.json', 'report-456.xml', 'other-456.json', 'report-456.json/extra']) assert.equal(map(`https://t.me/files/${name}`, rule), null);
});
test('suffix-only variables strip the literal suffix', () => {
  const rule = copy(example);
  rule.match.pathname = '/{invite}.html';
  rule.examples = [{ url: 'https://t.me/123.html', appUrl: 'tg://join?invite=123' }];
  R.validateRule(rule);
  assert.equal(map('https://t.me/456.html', rule), 'tg://join?invite=456');
});
test('regex metacharacters in the prefix/suffix stay literal', () => {
  const rule = copy(example);
  rule.match.pathname = '/file.[{invite}].json';
  rule.examples = [{ url: 'https://t.me/file.[123].json', appUrl: 'tg://join?invite=123' }];
  R.validateRule(rule);
  assert.equal(map('https://t.me/file.[456].json', rule), 'tg://join?invite=456');
  assert.equal(map('https://t.me/fileX[456]Xjson', rule), null);
});
test('overlapping prefix and suffix cannot produce an empty or reversed capture', () => {
  const rule = copy(example);
  rule.match.pathname = '/aa{invite}aa';
  rule.examples = [{ url: 'https://t.me/aa123aa', appUrl: 'tg://join?invite=123' }];
  R.validateRule(rule);
  for (const value of ['aa', 'aaa', 'aaaa']) assert.equal(map(`https://t.me/${value}`, rule), null);
  assert.equal(map('https://t.me/aaaaa', rule), 'tg://join?invite=a');
});
test('indices still refer to physical positions, independently of placeholder labels', () => {
  const rule = copy(example);
  rule.match.pathname = '/+{p9}';
  rule.target = 'tg://join?invite={capture:p9}';
  R.validateRule(rule);
  assert.equal(map('https://t.me/+Other', rule), 'tg://join?invite=Other');
  assert.throws(() => R.validateRule({ ...rule, target: 'tg://join?invite={path:9}' }), /не воспроизводит/);
});
test('whole-segment variables preserve the previous raw value, including a leading plus', () => {
  const rule = copy(example);
  rule.match.pathname = '/{p0}';
  rule.target = 'tg://join?invite={path:0}';
  rule.examples[0].appUrl = 'tg://join?invite=' + new URL(example.examples[0].url).pathname.slice(1);
  R.validateRule(rule);
  assert.equal(map('https://t.me/+Other', rule), 'tg://join?invite=+Other');
});
test('mixed captures also work inside hash paths', () => {
  const rule = copy(example);
  rule.match.pathname = '/'; rule.match.hashPathname = '/staff/user-{id}.html';
  rule.target = 'testapp://open?id={hashCapture:id}';
  rule.examples = [{ url: 'https://t.me/#/staff/user-123.html', appUrl: 'testapp://open?id=123' }];
  R.validateRule(rule);
  assert.equal(map('https://t.me/#/staff/user-456.html?view=card', rule), 'testapp://open?id=456');
  assert.equal(map('https://t.me/#/chats/user-456.html', rule), null);
});
test('outer and hash captures with the same label do not interfere', () => {
  const rule = copy(example);
  rule.match.pathname = '/+{id}'; rule.match.hashPathname = '/item-{id}.json';
  rule.target = 'testapp://open?a={capture:id}&b={hashCapture:id}';
  rule.examples = [{ url: 'https://t.me/+A#/item-B.json', appUrl: 'testapp://open?a=A&b=B' }];
  R.validateRule(rule);
  assert.equal(map('https://t.me/+C#/item-D.json', rule), 'testapp://open?a=C&b=D');
});
test('URL encoding applies to the captured portion, without decoding it first', () => {
  const rule = copy(example);
  rule.target = 'tg://join?invite={capture:invite|encode}';
  R.validateRule(rule);
  assert.equal(map('https://t.me/+a%2Fb', rule), 'tg://join?invite=a%252Fb');
});
test('encoded fixed prefixes can be written explicitly', () => {
  const rule = copy(example);
  rule.match.pathname = '/%2B{invite}';
  rule.examples[0].url = example.examples[0].url.replace('/+', '/%2B');
  R.validateRule(rule);
  assert.equal(map('https://t.me/%2BNew', rule), 'tg://join?invite=New');
  assert.equal(map('https://t.me/+New', rule), null);
});
test('multiple placeholders, malformed variables and partial tails are rejected', () => {
  for (const pathname of ['/{p0}{p1}', '/{p0}-{p1}', '/+{bad-name}', '/+{p0', '/+{tail}', '/{tail}.json', '/{tail}/more']) {
    assert.throws(() => R.validateRule({ ...example, match: { ...example.match, pathname } }), undefined, pathname);
  }
});
test('automatic analysis recognizes the Telegram invitation marker', () => {
  const { rule } = I.infer(example.examples[0].url, example.examples[0].appUrl, example.id);
  assert.equal(rule.match.pathname, '/+{code0}');
  assert.equal(rule.target, 'tg://join?invite={capture:code0}');
  assert.equal(rule.match.exactUrl, undefined);
  assert.equal(map('https://t.me/+Other_123', rule), 'tg://join?invite=Other_123');
});
test('automatic analysis keeps the plus if it is present in the application value', () => {
  const { rule } = I.infer('https://t.me/+ABC', 'testapp://open?id=+ABC');
  assert.equal(rule.match.pathname, '/{p0}');
  assert.equal(map('https://t.me/+XYZ', rule), 'testapp://open?id=+XYZ');
});
test('automatic analysis recognizes the plus marker in a hash path', () => {
  const { rule } = I.infer('https://web.example/#/invite/+ABC', 'testapp://open?id=ABC');
  assert.equal(rule.match.hashPathname, '/invite/+{code1}');
  assert.equal(map('https://web.example/#/invite/+XYZ', rule), 'testapp://open?id=XYZ');
});
test('ambiguous matching values still require an exact URL', () => {
  const { rule } = I.infer('https://t.me/+ABC?id=ABC', 'tg://join?invite=ABC');
  assert.equal(rule.match.exactUrl, 'https://t.me/+ABC?id=ABC');
});
test('prefix captures survive config export/import and remain subject to the switch', () => {
  const added = C.change(base, {}, { type: 'save-custom', rule: example });
  const imported = C.change(base, {}, { type: 'import-config', config: copy(C.resolve(base, added).config) });
  const cfg = C.resolve(base, imported).config;
  assert.equal(R.toAppUrl('https://t.me/+Other', undefined, cfg), 'tg://join?invite=Other');
  const disabled = C.change(base, imported, { type: 'set-enabled', id: example.id, enabled: false });
  assert.equal(R.toAppUrl('https://t.me/+Other', undefined, C.resolve(base, disabled).config), null);
});
test('capture names must exist and be unambiguous; old repeated p-labels still work by position', () => {
  assert.throws(() => R.validateRule({ ...example, target: 'tg://join?invite={capture:missing}' }), /не воспроизводит/);
  assert.throws(() => R.validateRule({ ...example, target: 'tg://join?invite={hashCapture:invite}' }), /не воспроизводит/);
  const repeated = copy(example);
  repeated.match.pathname = '/{p0}/{p0}';
  repeated.target = 'testapp://?a={path:0}&b={path:1}';
  repeated.examples = [{ url: 'https://t.me/A/B', appUrl: 'testapp://?a=A&b=B' }];
  R.validateRule(repeated);
  assert.equal(map('https://t.me/C/D', repeated), 'testapp://?a=C&b=D');
  repeated.target = 'testapp://?a={capture:p0}';
  repeated.examples[0].appUrl = 'testapp://?a=A';
  assert.throws(() => R.validateRule(repeated), /не воспроизводит/);
});
test('named captures use literal dictionary keys without inherited properties', () => {
  const rule = copy(example);
  rule.match.pathname = '/+{constructor}';
  rule.target = 'tg://join?invite={capture:constructor}';
  R.validateRule(rule);
  assert.equal(map('https://t.me/+Other', rule), 'tg://join?invite=Other');
  assert.throws(() => R.validateRule({ ...example, target: 'tg://join?invite={capture:constructor}' }), /не воспроизводит/);
});
test('capture names have a bounded and documented syntax', () => {
  for (const name of ['1invite', 'bad name', 'bad-name', 'a'.repeat(41)]) {
    assert.throws(() => R.validateRule({ ...example, match: { ...example.match, pathname: `/+{${name}}` } }));
  }
  const rule = copy(example);
  rule.match.pathname = '/+{Invite_Code1}';
  rule.target = 'tg://join?invite={capture:Invite_Code1}';
  R.validateRule(rule);
  assert.equal(map('https://t.me/+Other', rule), 'tg://join?invite=Other');
});
const referenceUrl = new URL('https://example.org:8443/room/123/details?id=456&mode=guest#chat');
const referenceRule = { match: { origins: [referenceUrl.origin], pathname: '/room/{tail}' } };
for (const [token, expected] of Object.entries({
  host: 'example.org:8443', pathname: '/room/123/details', pathAndQuery: '/room/123/details?id=456&mode=guest#chat',
  search: '?id=456&mode=guest', hash: '#chat', fragment: 'chat', tail: '123/details', 'path:1': '123', 'query:id': '456'
})) test(`the documented value of {${token}} is correct`, () => {
  assert.equal(R.render(`testapp://value={${token}}`, referenceUrl, R.matches(referenceRule, referenceUrl)), `testapp://value=${expected}`);
});
