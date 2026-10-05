'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const XLSX = require('../libs/xlsx.full.min.js');
const ChatExport = require('../export-model.js');

const popupPath = require.resolve('../popup.js');
const htmlPath = require.resolve('../popup.html');
const manifest = JSON.parse(fs.readFileSync(require.resolve('../manifest.json'), 'utf8'));
const supportedURL = 'https://telemost.yandex.ru/chats/0%2F0%2Fcf7238c7-c559-4c67-93c0-29c1f869fad4';
const coverage = { startReached: true, endReached: true, complete: true, reason: 'stable-boundaries' };
const collected = {
  detected: true,
  chatTitle: 'Тестовый чат',
  messages: [
    { id: '100', order: 1, sender: 'Анна', message: 'Привет', attachments: [], type: 'Сообщение' },
    { id: '101', order: 2, sender: 'Борис', message: 'Ответ', replyToId: '100', attachments: [], type: 'Ответ' }
  ],
  coverage,
  metadata: { chatTitle: 'Тестовый чат', coverage, warnings: [], exportedAt: '2026-10-05T10:00:00+03:00' }
};

function fakeElement(id) {
  const listeners = new Map();
  return {
    id, style: {}, disabled: false, hidden: false, textContent: '', className: '',
    addEventListener(name, listener) { listeners.set(name, listener); },
    async dispatch(name) {
      const listener = listeners.get(name);
      assert.ok(listener, `Missing ${name} handler for #${id}`);
      return listener({ target: this, preventDefault() {} });
    }
  };
}

// Chrome executes serialized functions in the page's isolated world. A separate
// VM catches accidental dependencies on the popup's lexical scope.
function createPopup(options = {}) {
  const nodes = new Map();
  for (const [, id] of fs.readFileSync(htmlPath, 'utf8').matchAll(/\bid="([^"]+)"/g)) nodes.set(id, fakeElement(id));
  const readyListeners = [];
  const injections = [];
  const writes = [];
  const windows = [];
  const permissionRequests = [];
  const runCalls = [];
  let closed = false;
  let queryCount = 0;
  let getCount = 0;
  const tab = { id: 17, url: options.url === undefined ? supportedURL : options.url };
  const sourceMode = options.sourceMode !== false;
  const document = { getElementById: id => nodes.get(id) || null };
  const window = {
    addEventListener(name, listener) { if (name === 'DOMContentLoaded') readyListeners.push(listener); },
    close() { closed = true; }
  };
  const frames = options.frames || [{ frameId: 0, url: tab.url, inspect: options.inspectResult, collect: options.collectResult }];
  const chrome = {
    runtime: { getManifest: () => manifest, getURL: path => `chrome-extension://fixture/${path}` },
    tabs: {
      async query() {
        queryCount++;
        if (options.queryError) throw new Error(options.queryError);
        return options.noTab ? [] : [tab];
      },
      async get(id) {
        getCount++;
        assert.equal(id, 17);
        if (options.queryError || options.noTab) throw new Error(options.queryError || 'No tab with id: 17');
        return tab;
      }
    },
    windows: { async create(value) { windows.push(value); return { id: 5 }; } },
    webNavigation: {
      async getAllFrames(value) {
        assert.equal(value.tabId, 17);
        return frames.map(frame => ({ frameId: frame.frameId, url: frame.url || supportedURL }));
      }
    },
    permissions: {
      async contains() { return Boolean(options.permissionContains); },
      async request(value) {
        permissionRequests.push(value);
        if (options.onPermissionRequest) options.onPermissionRequest(frames);
        return options.grantPermission !== false;
      }
    },
    scripting: {
      async executeScript(injection) {
        injections.push(injection);
        const selected = injection.target.frameIds
          ? frames.filter(frame => injection.target.frameIds.includes(frame.frameId))
          : injection.target.allFrames ? frames : frames.filter(frame => frame.frameId === 0);
        if (options.injectionError || selected.some(frame => frame.inaccessible)) {
          throw new Error(options.injectionError || 'Cannot access embedded frame');
        }
        if (injection.files) return [{ result: undefined }];
        assert.equal(typeof injection.func, 'function', 'Extractor invocation must use executeScript.func');
        const results = [];
        for (const frame of selected) {
          const pageURL = new URL(frame.url || supportedURL);
          if (options.changeURLBeforeCollect && injection.args?.length) pageURL.searchParams.set('chat', 'changed-chat');
          const page = vm.createContext({
            URL, location: { href: pageURL.href, origin: pageURL.origin },
            document: { querySelectorAll: () => (frame.iframes || []).map(src => ({ getAttribute: () => src })) },
            ChatExtractor: {
              async run(runOptions) {
                runCalls.push({ ...runOptions, frameId: frame.frameId });
                return structuredClone(runOptions.mode === 'inspect'
                  ? frame.inspect || { detected: true, chatTitle: 'Тестовый чат' }
                  : frame.collect || collected);
              }
            }
          });
          const fn = vm.runInContext(`(${injection.func.toString()})`, page);
          results.push({ frameId: frame.frameId, result: await fn(...(injection.args || [])) });
        }
        return options.noScriptResult ? [] : results;
      }
    }
  };
  const context = vm.createContext({
    document, window, chrome, console: { error() {}, log() {} }, URL, URLSearchParams, Date,
    location: { search: sourceMode ? '?tabId=17' : '' }, ChatExport,
    XLSX: {
      ...XLSX,
      writeFile(workbook, filename) {
        if (options.writeError) throw new Error(options.writeError);
        writes.push({ workbook, filename });
      }
    }
  });
  vm.runInContext(fs.readFileSync(popupPath, 'utf8'), context, { filename: popupPath });
  return {
    nodes, injections, writes, windows, permissionRequests, runCalls,
    get closed() { return closed; },
    get queryCount() { return queryCount; },
    get getCount() { return getCount; },
    async ready() { for (const listener of readyListeners) await listener(); },
    async export() { return nodes.get('exportBtn').dispatch('click'); }
  };
}

test('extension advances to 1.3 and keeps broad host access optional', () => {
  assert.equal(manifest.manifest_version, 3);
  assert.equal(manifest.version, '1.3');
  assert.deepEqual(manifest.permissions, ['activeTab', 'scripting', 'webNavigation']);
  assert.equal(manifest.host_permissions, undefined);
  assert.ok(manifest.optional_host_permissions.includes('https://messenger.yandex.ru/*'));
  const html = fs.readFileSync(htmlPath, 'utf8');
  assert.ok(html.indexOf('export-model.js') >= 0);
  assert.ok(html.indexOf('export-model.js') < html.indexOf('popup.js'));
});

test('browser-action popup opens a persistent export window bound to the source tab', async () => {
  const popup = createPopup({ sourceMode: false });
  await popup.ready();
  assert.equal(popup.injections.length, 0);
  await popup.export();
  assert.equal(popup.windows.length, 1);
  assert.equal(popup.windows[0].url, 'chrome-extension://fixture/popup.html?tabId=17');
  assert.equal(popup.windows[0].type, 'popup');
  assert.equal(popup.closed, true);
  assert.equal(popup.writes.length, 0);
});

test('export window uses its source tab and injects before inspecting and collecting', async () => {
  const popup = createPopup();
  await popup.ready();
  assert.equal(popup.queryCount, 0, 'Export window must not accidentally select itself as the active tab');
  assert.ok(popup.getCount > 0);
  assert.deepEqual(popup.runCalls.map(call => call.mode), ['inspect', 'collect']);
  assert.deepEqual(Array.from(popup.injections[0].files), ['chat-extractor.js']);
  assert.equal(popup.injections[0].target.tabId, 17);
  assert.deepEqual(Array.from(popup.injections[0].target.frameIds), [0]);
  assert.equal(popup.injections[0].target.allFrames, undefined);
  assert.deepEqual(Array.from(popup.injections.at(-1).target.frameIds), [0]);
  assert.deepEqual(Array.from(popup.injections.at(-1).args), [supportedURL]);
  assert.equal(popup.nodes.get('exportBtn').disabled, false);
  assert.equal(popup.nodes.get('version').textContent, '1.3');
});

test('unified domains, personal/group/broadcast routes and legacy Messenger URLs are accepted', async () => {
  for (const url of [
    supportedURL,
    'https://telemost.360.yandex.ru/chats/0%2F22%2F4a0414db-91e1-4862-8cbe-efddd1758886',
    'https://telemost.360.yandex.ru/chats/0%2F0%2F9213621f-dccc-4c4c-92e2-888ab0c060fe',
    'https://messenger.yandex.ru/#/chat/123',
    'https://yandex.ru/chat/#/chats/123',
    'https://telemost.yandex.ru/'
  ]) {
    const popup = createPopup({ url });
    await popup.ready();
    assert.equal(popup.writes.length, 1, url);
    assert.equal(popup.nodes.get('status').className, 'success', url);
  }
});

test('unrelated origins, non-HTTPS URLs and absent tabs are rejected before injecting', async () => {
  for (const options of [
    { url: 'https://example.test/chats/123' },
    { url: 'https://telemost.yandex.ru.evil.test/chats/123' },
    { url: 'http://telemost.yandex.ru/chats/123' },
    { url: 'https://yandex.ru/search/' },
    { url: 'chrome://extensions/' },
    { noTab: true }
  ]) {
    const popup = createPopup(options);
    await popup.ready();
    assert.equal(popup.nodes.get('exportBtn').disabled, true, JSON.stringify(options));
    assert.equal(popup.injections.length, 0, JSON.stringify(options));
    assert.equal(popup.nodes.get('status').className, 'error');
  }
});

test('a supported URL without a rendered conversation explains the problem and allows retry', async () => {
  const popup = createPopup({ inspectResult: { detected: false, error: 'Дождитесь загрузки сообщений.' } });
  await popup.ready();
  assert.match(popup.nodes.get('status').textContent, /Дождитесь загрузки сообщений/);
  assert.equal(popup.writes.length, 0);
  assert.equal(popup.nodes.get('exportBtn').disabled, false);
});

test('export generates an XLSX with every message, correct counts and release metadata', async () => {
  const popup = createPopup();
  await popup.ready();
  assert.equal(popup.writes.length, 1);
  const { workbook, filename } = popup.writes[0];
  assert.deepEqual(workbook.SheetNames, ['Сообщения', 'Вопросы и ответы', 'Об экспорте']);
  assert.match(filename, /^chat_export_\d{4}-\d{2}-\d{2}_\d{2}-\d{2}-\d{2}\.xlsx$/);
  const rows = XLSX.utils.sheet_to_json(workbook.Sheets['Сообщения'], { header: 1 });
  assert.equal(rows.length, 3);
  const metadata = XLSX.utils.sheet_to_json(workbook.Sheets['Об экспорте'], { header: 1 });
  assert.ok(metadata.some(row => row[0] === 'Версия расширения' && row[1] === manifest.version));
  assert.equal(popup.nodes.get('messagesCount').textContent, 2);
  assert.equal(popup.nodes.get('answersCount').textContent, 1);
  assert.equal(popup.nodes.get('attachmentsCount').textContent, 0);
  assert.equal(popup.nodes.get('status').className, 'success');
});

test('an empty chat does not download a misleading workbook', async () => {
  const popup = createPopup({ collectResult: { ...collected, messages: [] } });
  await popup.ready();
  assert.equal(popup.writes.length, 0);
  assert.match(popup.nodes.get('status').textContent, /не найдено сообщений/i);
  assert.equal(popup.nodes.get('exportBtn').disabled, false);
});

test('partial history downloads collected messages and exposes the limitation in UI and workbook', async () => {
  const partialCoverage = { startReached: false, endReached: false, complete: false, reason: 'limit' };
  const popup = createPopup({ collectResult: {
    ...collected, coverage: partialCoverage,
    metadata: { ...collected.metadata, coverage: partialCoverage, warnings: ['Достигнут предел времени.'] }
  } });
  await popup.ready();
  assert.equal(popup.writes.length, 1);
  assert.match(popup.nodes.get('status').textContent, /доступная часть истории/i);
  assert.equal(popup.nodes.get('status').className, 'info');
  const rows = XLSX.utils.sheet_to_json(popup.writes[0].workbook.Sheets['Об экспорте'], { header: 1 });
  assert.ok(rows.some(row => row[0] === 'Предупреждения' && String(row[1]).includes('Достигнут предел времени.')));
});

test('unrelated third-party frames are skipped while the top-level chat exports', async () => {
  const popup = createPopup({ frames: [
    { frameId: 0, inspect: { detected: true } },
    { frameId: 4, url: 'https://thirdparty.test/embedded', inaccessible: true }
  ] });
  await popup.ready();
  assert.equal(popup.writes.length, 1);
  assert.ok(popup.injections.every(injection => Array.from(injection.target.frameIds).join(',') === '0'));
  assert.equal(popup.permissionRequests.length, 0);
});

test('child-frame chat is collected in that frame, while ambiguous frames are rejected', async () => {
  const frames = [
    { frameId: 0, inspect: { detected: false, error: 'Нет чата' } },
    { frameId: 7, url: 'https://messenger.yandex.ru/', inspect: { detected: true } }
  ];
  const popup = createPopup({ frames });
  await popup.ready();
  assert.equal(popup.writes.length, 1);
  assert.deepEqual(Array.from(popup.injections.at(-1).target.frameIds), [7]);
  const ambiguous = createPopup({ frames: [{ frameId: 0, inspect: { detected: true } }, frames[1]] });
  await ambiguous.ready();
  assert.equal(ambiguous.writes.length, 0);
  assert.match(ambiguous.nodes.get('status').textContent, /несколько чатов/i);
});

test('cross-origin embedded chat access is requested only after a user click', async () => {
  const frames = [
    { frameId: 0, inspect: { detected: false } },
    { frameId: 8, url: 'https://messenger.yandex.ru/chat/123', inaccessible: true, inspect: { detected: true } }
  ];
  const popup = createPopup({
    frames,
    onPermissionRequest(values) { values[1].inaccessible = false; }
  });
  await popup.ready();
  assert.equal(popup.permissionRequests.length, 0);
  assert.equal(popup.writes.length, 0);
  assert.match(popup.nodes.get('exportBtn').textContent, /Разрешить доступ/);
  await popup.export();
  assert.equal(popup.permissionRequests.length, 1);
  assert.deepEqual(Array.from(popup.permissionRequests[0].origins), ['https://messenger.yandex.ru/*']);
  assert.equal(popup.writes.length, 1);
});

test('denied optional access stays visible and does not download', async () => {
  const popup = createPopup({
    frames: [
      { frameId: 0, inspect: { detected: false } },
      { frameId: 8, url: 'https://messenger.yandex.ru/chat/123', inaccessible: true }
    ],
    grantPermission: false
  });
  await popup.ready();
  await popup.export();
  assert.equal(popup.writes.length, 0);
  assert.match(popup.nodes.get('status').textContent, /Доступ.*не предоставлен/);
});

test('an inaccessible chat with existing host permission does not request permission again', async () => {
  const popup = createPopup({
    frames: [
      { frameId: 0, inspect: { detected: false } },
      { frameId: 8, url: 'https://messenger.yandex.ru/chat/123', inaccessible: true }
    ],
    permissionContains: true
  });
  await popup.ready();
  assert.equal(popup.permissionRequests.length, 0);
  assert.equal(popup.writes.length, 0);
  assert.match(popup.nodes.get('status').textContent, /отдельной вкладке/);
});

test('changing the selected chat between inspect and collect aborts before reading messages', async () => {
  const popup = createPopup({ changeURLBeforeCollect: true });
  await popup.ready();
  assert.deepEqual(popup.runCalls.map(call => call.mode), ['inspect']);
  assert.equal(popup.writes.length, 0);
  assert.match(popup.nodes.get('status').textContent, /Чат изменился перед началом выгрузки/);
});

test('collection errors, injection failures and missing results do not download and allow retry', async () => {
  for (const options of [
    { collectResult: { error: 'Во время выгрузки чат изменился.' } },
    { injectionError: 'Cannot access contents of this tab' },
    { noScriptResult: true }
  ]) {
    const popup = createPopup(options);
    await popup.ready();
    assert.equal(popup.writes.length, 0);
    assert.equal(popup.nodes.get('status').className, 'error');
    assert.equal(popup.nodes.get('exportBtn').disabled, false);
  }
});

test('file generation errors remain visible and allow retry', async () => {
  const popup = createPopup({ writeError: 'Не удалось сформировать файл' });
  await popup.ready();
  assert.equal(popup.writes.length, 0);
  assert.equal(popup.nodes.get('status').className, 'error');
  assert.match(popup.nodes.get('status').textContent, /Не удалось сформировать файл/);
  assert.equal(popup.nodes.get('exportBtn').disabled, false);
});
