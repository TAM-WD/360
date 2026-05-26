/**
 * Popup — представление + команды:
 *   - GET_STATE на открытии;
 *   - START_EXPORT по кнопке;
 *   - сборка CSV и инициирование скачивания.
 */

import { MSG } from '../core/protocol.js';
import { buildMembersCsv } from '../core/csv.js';

const els = {
  status: document.getElementById('status'),
  chatId: document.getElementById('chat-id'),
  count: document.getElementById('count'),
  errorRow: document.getElementById('error-row'),
  error: document.getElementById('error'),
  start: document.getElementById('start'),
  download: document.getElementById('download'),
  reset: document.getElementById('reset'),
  hint: document.getElementById('hint')
};

const STATUS_LABEL = {
  idle: 'ожидание',
  captured: 'готов к выгрузке',
  loading: 'загрузка…',
  done: 'готово',
  error: 'ошибка'
};

const SUPPORTED_HOSTS = ['admin.yandex.ru', 'admin.360.yandex.ru'];

let currentTabId = null;
let currentSession = null;

const setStatus = (status) => {
  els.status.className = `status status--${status}`;
  els.status.textContent = STATUS_LABEL[status] || status;
};

const render = (session) => {
  currentSession = session;
  const hasMembers = session?.members?.length > 0;
  const hasCapture = Boolean(session?.chatId);
  const hasMore = Boolean(session?.nextCursor);
  const isLoading = session?.status === 'loading';

  els.chatId.textContent = session?.chatId || '—';
  els.chatId.title = session?.chatId || '';
  els.count.textContent = String(session?.members?.length || 0);

  setStatus(session?.status || 'idle');

  if (session?.error) {
    els.errorRow.hidden = false;
    els.error.textContent = session.error;
  } else {
    els.errorRow.hidden = true;
    els.error.textContent = '';
  }

  els.start.disabled = !hasCapture || isLoading || !hasMore;
  els.start.textContent = isLoading ? 'идёт выгрузка…' : 'Начать выгрузку';
  els.download.disabled = !hasMembers;
};

const getActiveTab = async () => {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab || null;
};

const isSupportedTab = (tab) => {
  if (!tab || !tab.url) return false;
  try {
    const u = new URL(tab.url);
    return SUPPORTED_HOSTS.includes(u.hostname) && /^\/messenger-chats(\/|$)/.test(u.pathname);
  } catch { return false; }
};

const requestState = async (tabId) =>
  (await chrome.runtime.sendMessage({ type: MSG.GET_STATE, tabId }))?.session || null;

const startExport = async () => {
  if (currentTabId == null) return;
  els.start.disabled = true;
  const resp = await chrome.runtime.sendMessage({ type: MSG.START_EXPORT, tabId: currentTabId });
  if (!resp?.ok) {
    console.warn('[yc-exporter:popup] start failed', resp);
  }
};

const triggerDownload = () => {
  if (!currentSession?.members?.length) return;
  const csv = buildMembersCsv(currentSession.members);
  const blob = new Blob([csv], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const safeChatId = (currentSession.chatId || 'chat').replace(/[^a-zA-Z0-9_.-]/g, '_');
  const stamp = new Date().toISOString().slice(0, 19).replace(/[:T]/g, '-');
  const a = document.createElement('a');
  a.href = url;
  a.download = `chat-members_${safeChatId}_${stamp}.csv`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
};

const resetSession = async () => {
  if (currentTabId == null) return;
  await chrome.runtime.sendMessage({ type: MSG.RESET_SESSION, tabId: currentTabId });
  render(await requestState(currentTabId));
};

const init = async () => {
  const tab = await getActiveTab();
  currentTabId = tab?.id ?? null;

  if (!isSupportedTab(tab)) {
    els.hint.innerHTML = 'Эта вкладка не поддерживается. Откройте <code>admin.yandex.ru/messenger-chats/</code> или <code>admin.360.yandex.ru/messenger-chats/</code>.';
  }

  render(currentTabId != null ? await requestState(currentTabId) : null);

  els.start.addEventListener('click', startExport);
  els.download.addEventListener('click', triggerDownload);
  els.reset.addEventListener('click', resetSession);

  chrome.runtime.onMessage.addListener((msg) => {
    if (msg?.type === MSG.STATE_UPDATED && msg.payload?.tabId === currentTabId) {
      render(msg.payload.session);
    }
  });
};

init();
