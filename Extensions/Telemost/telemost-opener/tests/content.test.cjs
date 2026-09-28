const { readFileSync } = require('node:fs');
const { join } = require('node:path');
const vm = require('node:vm');
const test = require('node:test');
const assert = require('node:assert/strict');

const source = readFileSync(join(__dirname, '..', 'url-router.js'), 'utf8') + '\n' +
  readFileSync(join(__dirname, '..', 'content.js'), 'utf8');

function setup() {
  const listeners = new Map();
  const launches = [];
  class Element {
    constructor(href, tag = 'a', base = 'https://example.com/page') {
      this.href = href;
      this.localName = tag;
      this.baseURI = base;
    }
    getAttribute(name) { return name === 'href' ? this.href : null; }
    getAttributeNS() { return null; }
  }
  const window = {
    location: { assign: url => launches.push(url) },
    addEventListener(type, handler, options) {
      assert.equal(options.capture, true);
      assert.equal(options.passive, false);
      listeners.set(type, handler);
    }
  };
  vm.runInNewContext(source, { window, Element, URL });
  function activate(href, props = {}) {
    const node = new Element(href, props.tag, props.base);
    const event = {
      type: 'click', button: 0, isTrusted: true,
      prevented: false, stopped: false,
      composedPath: () => [new Element(null, 'span'), node, window],
      preventDefault() { this.prevented = true; },
      stopImmediatePropagation() { this.stopped = true; },
      ...props
    };
    listeners.get(event.type)(event);
    return event;
  }
  return { activate, launches, Element, listeners };
}

for (const [route, url, expected] of [
  ['j legacy domain', 'https://telemost.yandex.ru/j/ABC%2Fdef?token=A%2BB&source=calendar#room',
    'telemost://https://telemost.yandex.ru/j/ABC%2Fdef?token=A%2BB&source=calendar#room'],
  ['j', 'https://telemost.360.yandex.ru/j/ABC%2Fdef?token=A%2BB&source=calendar#room',
    'telemost://https://telemost.360.yandex.ru/j/ABC%2Fdef?token=A%2BB&source=calendar#room'],
  ['join legacy domain', 'https://telemost.yandex.ru/join?source=calendar#ABC%2Fdef?token=A%2BB&room=1',
    'telemost://ychat/telemost.yandex.ru/join?source=calendar#ABC%2Fdef?token=A%2BB&room=1'],
  ['join', 'https://telemost.360.yandex.ru/join?source=calendar#ABC%2Fdef?token=A%2BB&room=1',
    'telemost://ychat/telemost.360.yandex.ru/join?source=calendar#ABC%2Fdef?token=A%2BB&room=1']
]) {
  for (const [name, props] of [
    ['left', {}], ['Ctrl', { ctrlKey: true }], ['Cmd', { metaKey: true }],
    ['Shift', { shiftKey: true }], ['middle', { type: 'auxclick', button: 1 }],
    ['keyboard', { detail: 0 }], ['image map', { tag: 'area' }]
  ]) {
    test(`${route}: ${name} cancels the web navigation and preserves the query/fragment`, () => {
      const { activate, launches } = setup();
      const event = activate(url, props);
      assert.equal(event.prevented, true);
      assert.equal(event.stopped, true);
      assert.deepEqual(launches, [expected]);
    });
  }
}

for (const href of [
  'https://example.com/', 'https://telemost.yandex.ru.evil.example/j/1',
  'https://telemost.360.yandex.ru.evil.example/j/1',
  'https://eviltelemost.yandex.ru/j/1', 'https://telemost.yandex.ru@evil.example/j/1',
  'https://user:password@telemost.yandex.ru/j/1',
  'http://telemost.yandex.ru/j/1', 'javascript:alert(1)',
  'telemost://https://telemost.yandex.ru/j/1', 'mailto:test@example.com',
  'https://[invalid', '', '#section',
  'https://telemost.yandex.ru/join', 'https://telemost.yandex.ru/join#',
  'https://telemost.yandex.ru/join/#room',
  'https://telemost.yandex.ru/', 'https://telemost.yandex.ru/j/',
  'https://telemost.360.yandex.ru/', 'https://telemost.360.yandex.ru/j',
  'https://telemost.360.yandex.ru/j/', 'https://telemost.360.yandex.ru/j/?id=123#room',
  'https://telemost.360.yandex.ru/j//room', 'https://telemost.360.yandex.ru/join',
  'https://telemost.360.yandex.ru/join#', 'https://telemost.360.yandex.ru/join/#room',
  'https://telemost.360.yandex.ru/join/room#id', 'https://telemost.360.yandex.ru/joined#room',
  'https://telemost.360.yandex.ru/other#https://telemost.360.yandex.ru/j/1',
  'https://telemost.360.yandex.ru/J/room', 'https://telemost.360.yandex.ru/%6A/room',
  'https://telemost.360.yandex.ru:8443/j/1', 'http://telemost.360.yandex.ru/j/1',
  'https://user:password@telemost.360.yandex.ru/j/1',
  'https://telemost.360.yandex.ru.evil.example/join#room',
  'ychat://https://telemost.360.yandex.ru/join#room'
]) {
  test(`does not alter unrelated or invalid link: ${href}`, () => {
    const { activate, launches } = setup();
    const event = activate(href);
    assert.equal(event.prevented, false);
    assert.equal(event.stopped, false);
    assert.equal(launches.length, 0);
    for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup']) {
      const early = activate(href, { type });
      assert.equal(early.prevented, false);
      assert.equal(early.stopped, false);
    }
  });
}

test('resolves protocol-relative and relative URLs with the element baseURI', () => {
  const { activate, launches } = setup();
  activate('//telemost.360.yandex.ru/j/1');
  activate('/j/2?x=1#room', { base: 'https://telemost.360.yandex.ru/page' });
  activate('https://TELEMOST.360.YANDEX.RU/j/3');
  activate('/join#room%2F123', { base: 'https://telemost.360.yandex.ru/page' });
  activate('/join#room%2F123', { base: 'https://telemost.yandex.ru/page' });
  activate('https://TELEMOST.YANDEX.RU/join#room%2F456');
  assert.deepEqual(launches, [
    'telemost://https://telemost.360.yandex.ru/j/1',
    'telemost://https://telemost.360.yandex.ru/j/2?x=1#room',
    'telemost://https://telemost.360.yandex.ru/j/3',
    'telemost://ychat/telemost.360.yandex.ru/join#room%2F123',
    'telemost://ychat/telemost.yandex.ru/join#room%2F123',
    'telemost://ychat/telemost.yandex.ru/join#room%2F456'
  ]);
});

test('ignores script-generated events and right clicks', () => {
  const { activate, launches } = setup();
  for (const props of [{ isTrusted: false }, { type: 'auxclick', button: 2 }]) {
    const event = activate('https://telemost.360.yandex.ru/j/1', props);
    assert.equal(event.prevented, false);
    assert.equal(event.stopped, false);
  }
  assert.equal(launches.length, 0);
});

test('blocks early site handlers without launching until the completed click', () => {
  const { activate, launches } = setup();
  for (const type of ['pointerdown', 'mousedown', 'pointerup', 'mouseup']) {
    const event = activate('https://telemost.360.yandex.ru/j/1', { type });
    assert.equal(event.stopped, true);
    assert.equal(event.prevented, false, 'left-button focus/selection remains native');
    assert.equal(launches.length, 0);
  }
  activate('https://telemost.360.yandex.ru/j/1');
  assert.equal(launches.length, 1);
});

test('middle mouse down suppresses native autoscroll without an early launch', () => {
  const { activate, launches } = setup();
  const event = activate('https://telemost.360.yandex.ru/j/1', { type: 'mousedown', button: 1 });
  assert.equal(event.prevented, true);
  assert.equal(launches.length, 0);
  activate('https://telemost.360.yandex.ru/j/1', { type: 'auxclick', button: 1 });
  assert.equal(launches.length, 1);
});

test('finds a link within an open shadow event path and accepts an SVG xlink', () => {
  const { activate, launches, Element } = setup();
  const svg = new Element(null);
  svg.getAttributeNS = () => 'https://telemost.360.yandex.ru/j/svg';
  activate(null, { composedPath: () => [new Element(null, 'path'), svg, {}, {}] });
  assert.deepEqual(launches, ['telemost://https://telemost.360.yandex.ru/j/svg']);
});

test('does not interpret a URL attribute on a button as a link', () => {
  const { activate, launches } = setup();
  const event = activate('https://telemost.360.yandex.ru/j/1', { tag: 'button' });
  assert.equal(event.prevented, false);
  assert.equal(launches.length, 0);
});
