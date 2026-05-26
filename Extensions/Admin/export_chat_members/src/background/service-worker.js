/**
 * Service worker — оркестратор.
 *
 * Принимает события со страницы (батчи, готовность интерсептора, логи),
 * хранит состояние per-tab, перенаправляет команды popup-а на страницу.
 * Не знает о DOM/CSV.
 */

import { SessionStore } from './session-store.js';
import { MSG } from '../core/protocol.js';

const LOG = '[yc-exporter:bg]';
const log = (...args) => console.log(LOG, ...args);

const notifyPopup = (tabId, session) => {
  chrome.runtime
    .sendMessage({ type: MSG.STATE_UPDATED, payload: { tabId, session } })
    .catch(() => {});
};

const onMembersBatch = async (tabId, payload) => {
  log('batch tab=', tabId, 'items=', payload?.items?.length, 'next=', payload?.nextCursor ? '…' : 'null', 'source=', payload?.source);
  const session = await SessionStore.update(tabId, (s) => {
    const isNewChat = s.chatId && s.chatId !== payload.chatId;
    const base = isNewChat ? SessionStore.emptySession() : s;

    const seen = new Set(base.members.map((m) => m.id || `${m.uid}:${m.login}`));
    const merged = base.members.slice();
    for (const item of payload.items || []) {
      const k = item.id || `${item.uid}:${item.login}`;
      if (seen.has(k)) continue;
      seen.add(k);
      merged.push({ id: item.id, uid: item.uid, name: item.name, login: item.login });
    }

    const nextStatus = payload.nextCursor
      ? (payload.source === 'pagination' ? 'loading' : 'captured')
      : 'done';

    return {
      ...base,
      chatId: payload.chatId,
      orgId: payload.orgId,
      ckey: payload.ckey ?? base.ckey,
      nextCursor: payload.nextCursor ?? null,
      status: nextStatus,
      error: null,
      members: merged
    };
  });
  notifyPopup(tabId, session);
};

const onDone = async (tabId) => {
  log('done tab=', tabId);
  const session = await SessionStore.update(tabId, (s) => ({ ...s, status: 'done', nextCursor: null }));
  notifyPopup(tabId, session);
};

const onError = async (tabId, payload) => {
  log('error tab=', tabId, payload);
  const session = await SessionStore.update(tabId, (s) => ({
    ...s,
    status: 'error',
    error: payload?.message || 'unknown'
  }));
  notifyPopup(tabId, session);
};

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  // от страницы / content-script
  if (sender.tab && msg && msg.type) {
    const tabId = sender.tab.id;
    switch (msg.type) {
      case MSG.MEMBERS_BATCH:    onMembersBatch(tabId, msg.payload); return;
      case MSG.PAGINATION_DONE:  onDone(tabId); return;
      case MSG.PAGINATION_ERROR: onError(tabId, msg.payload); return;
      case MSG.INTERCEPTOR_READY: log('interceptor ready tab=', tabId); return;
      case MSG.LOG:              console.log('[yc-exporter:page-relay]', ...(msg.payload?.args || [])); return;
    }
  }

  // от popup
  if (!sender.tab && msg && msg.type) {
    if (msg.type === MSG.GET_STATE) {
      (async () => {
        if (typeof msg.tabId !== 'number') return sendResponse({ session: SessionStore.emptySession(), tabId: null });
        const session = await SessionStore.get(msg.tabId);
        sendResponse({ session, tabId: msg.tabId });
      })();
      return true;
    }
    if (msg.type === MSG.RESET_SESSION) {
      (async () => {
        const tabId = msg.tabId;
        if (typeof tabId !== 'number') return sendResponse({ ok: false });
        const fresh = await SessionStore.reset(tabId);
        notifyPopup(tabId, fresh);
        // Просим страницу сбросить локальный прогресс и переподтянуть первую
        // страницу — иначе SPA новых XHR не сделает, и расширение замолчит.
        try {
          await chrome.tabs.sendMessage(tabId, { type: MSG.RESET_PAGE_STATE, target: 'page' });
        } catch (e) {
          log('reset: не удалось достучаться до content-script', e && e.message || e);
        }
        sendResponse({ ok: true });
      })();
      return true;
    }
    if (msg.type === MSG.START_EXPORT) {
      (async () => {
        const tabId = msg.tabId;
        if (typeof tabId !== 'number') return sendResponse({ ok: false, error: 'no tabId' });
        log('START_EXPORT → tab', tabId);
        // переводим в loading сразу, чтобы UI среагировал
        const session = await SessionStore.update(tabId, (s) => ({ ...s, status: 'loading', error: null }));
        notifyPopup(tabId, session);
        try {
          await chrome.tabs.sendMessage(tabId, { type: MSG.START_PAGINATION, target: 'page' });
          sendResponse({ ok: true });
        } catch (e) {
          const errSession = await SessionStore.update(tabId, (s) => ({
            ...s, status: 'error', error: 'content-script недоступен; обновите вкладку'
          }));
          notifyPopup(tabId, errSession);
          sendResponse({ ok: false, error: String(e && e.message || e) });
        }
      })();
      return true;
    }
  }
});

chrome.tabs.onRemoved.addListener((tabId) => {
  chrome.storage.session.remove(`sessions:${tabId}`).catch(() => {});
});
