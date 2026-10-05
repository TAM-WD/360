"use strict";

const SUPPORTED_HOSTS = new Set([
  'telemost.yandex.ru', 'telemost.360.yandex.ru', 'messenger.yandex.ru', 'messenger.360.yandex.ru',
  'telemost.yandex.com', 'telemost.360.yandex.com', 'messenger.yandex.com', 'messenger.360.yandex.com',
  'messenger.yandex-team.ru', 'telemost.yandex-team.ru'
]);
const sourceTabId = Number(new URLSearchParams(location.search).get('tabId'));
const button = document.getElementById('exportBtn');
const status = document.getElementById('status');
let pendingOrigins = [];

function supportedURL(value) {
  try {
    const url = new URL(value);
    return url.protocol === 'https:' && (SUPPORTED_HOSTS.has(url.hostname) ||
      ['yandex.ru', 'yandex.com'].includes(url.hostname) && /^\/chat(?:\/|$)/.test(url.pathname));
  } catch { return false; }
}

function setStatus(message, type = 'info') {
  status.style.display = 'block';
  status.className = type;
  status.textContent = message;
}

async function getSourceTab() {
  const tab = sourceTabId ? await chrome.tabs.get(sourceTabId)
    : (await chrome.tabs.query({ active: true, currentWindow: true }))[0];
  if (!tab || !supportedURL(tab.url)) throw new Error('Откройте переписку в Яндекс Мессенджере или Телемосте.');
  return tab;
}

async function detectChat(tabId) {
  // Isolate inaccessible third-party frames instead of failing an allFrames injection.
  const frames = await chrome.webNavigation.getAllFrames({ tabId }) || [{ frameId: 0 }];
  const inspected = [];
  const inaccessibleOrigins = new Set();
  let injectionError = '';
  for (const frame of frames) {
    if (frame.frameId !== 0 && !supportedURL(frame.url)) continue;
    const target = { tabId, frameIds: [frame.frameId] };
    try {
      await chrome.scripting.executeScript({ target, files: ['chat-extractor.js'] });
      const result = await chrome.scripting.executeScript({
        target,
        func: async () => ({ ...await globalThis.ChatExtractor.run({ mode: 'inspect' }), url: location.href })
      });
      inspected.push(...result);
    } catch (error) {
      injectionError = error.message;
      if (frame.frameId !== 0 && supportedURL(frame.url)) inaccessibleOrigins.add(`${new URL(frame.url).origin}/*`);
    }
  }
  const found = inspected.filter(item => item.result && item.result.detected);
  if (found.length > 1) throw new Error('На странице открыто несколько чатов. Оставьте одну переписку и повторите выгрузку.');
  if (found.length) return found[0];
  pendingOrigins = [];
  for (const origin of inaccessibleOrigins) {
    if (!await chrome.permissions.contains({ origins: [origin] })) pendingOrigins.push(origin);
  }
  if (pendingOrigins.length) {
    button.textContent = 'Разрешить доступ к встроенному чату';
    throw new Error('Чат встроен с другого адреса. Нажмите кнопку, чтобы разрешить выгрузку из этого чата.');
  }
  throw new Error(inaccessibleOrigins.size ? 'Встроенный чат недоступен для расширения. Откройте его в отдельной вкладке.'
    : inspected.find(item => item.result?.error)?.result.error || injectionError || 'Чат не найден. Откройте переписку и дождитесь загрузки сообщений.');
}

function filename() {
  const now = new Date();
  const pad = value => String(value).padStart(2, '0');
  return `chat_export_${now.getFullYear()}-${pad(now.getMonth() + 1)}-${pad(now.getDate())}_${pad(now.getHours())}-${pad(now.getMinutes())}-${pad(now.getSeconds())}.xlsx`;
}

async function exportChat() {
  button.disabled = true;
  document.getElementById('statsContainer').style.display = 'none';
  try {
    const tab = await getSourceTab();
    setStatus('Поиск открытой переписки…');
    const frame = await detectChat(tab.id);
    setStatus('Собираю историю. Не закрывайте вкладку с чатом и это окно; можно переключиться в другое приложение.');
    const results = await chrome.scripting.executeScript({
      target: { tabId: tab.id, frameIds: [frame.frameId] },
      func: expectedURL => !expectedURL || location.href === expectedURL
        ? globalThis.ChatExtractor.run({ mode: 'collect' })
        : { error: 'Чат изменился перед началом выгрузки. Повторите выгрузку в нужной переписке.' },
      args: [frame.result.url]
    });
    const result = results[0]?.result;
    if (!result) throw new Error('Страница не вернула результат. Проверьте, что вкладка с чатом открыта.');
    if (result.error) throw new Error(result.error);
    if (!result.messages.length) throw new Error('В открытой переписке не найдено сообщений.');
    const data = ChatExport.buildExportData(result.messages, {
      ...result.metadata, version: chrome.runtime.getManifest().version
    });
    XLSX.writeFile(ChatExport.buildWorkbook(data, XLSX), filename());
    document.getElementById('statsContainer').style.display = 'block';
    document.getElementById('messagesCount').textContent = data.stats.messages;
    document.getElementById('questionsCount').textContent = data.stats.questions;
    document.getElementById('answersCount').textContent = data.stats.answers;
    document.getElementById('attachmentsCount').textContent = data.stats.attachments;
    setStatus(result.coverage.complete ? `Сохранено сообщений: ${data.stats.messages}. История чата собрана.`
      : `Сохранено сообщений: ${data.stats.messages}. Выгружена доступная часть истории; подробности — на листе «Об экспорте».`,
    result.coverage.complete ? 'success' : 'info');
  } catch (error) {
    setStatus(error.message || 'Не удалось выгрузить переписку.', 'error');
  } finally {
    button.disabled = false;
    if (!pendingOrigins.length) button.textContent = 'Начать новую выгрузку';
  }
}

button.addEventListener('click', async () => {
  if (!sourceTabId) {
    button.disabled = true;
    try {
      const tab = await getSourceTab();
      // A browser-action popup closes on blur. This window keeps the result alive.
      await chrome.windows.create({ url: `${chrome.runtime.getURL('popup.html')}?tabId=${tab.id}`,
        type: 'popup', width: 430, height: 650 });
      window.close();
    } catch (error) {
      setStatus(error.message, 'error');
      button.disabled = false;
    }
    return;
  }
  if (pendingOrigins.length) {
    try {
      const granted = await chrome.permissions.request({ origins: pendingOrigins });
      if (!granted) { setStatus('Доступ к встроенному чату не предоставлен.', 'error'); return; }
      pendingOrigins = [];
    } catch (error) { setStatus(error.message, 'error'); return; }
  }
  await exportChat();
});

window.addEventListener('DOMContentLoaded', async () => {
  document.getElementById('version').textContent = chrome.runtime.getManifest().version;
  document.getElementById('warningBox').style.display = 'block';
  try {
    await getSourceTab();
    if (sourceTabId) await exportChat();
    else setStatus('Откройте нужную переписку и начните выгрузку. Для сбора истории откроется отдельное окно.');
  } catch (error) {
    button.disabled = true;
    setStatus(error.message, 'error');
  }
});
