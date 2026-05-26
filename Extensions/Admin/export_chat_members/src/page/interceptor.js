/**
 * MAIN-world перехватчик HTTP-вызовов messenger/get-chat-members.
 *
 * Что делает:
 *   1. Патчит XMLHttpRequest и fetch (сайт использует XHR, но fetch
 *      оставлен как страховка).
 *   2. На первый перехваченный ответ публикует MEMBERS_BATCH с участниками,
 *      cursor и ckey — НИЧЕГО не пагинируя автоматически.
 *   3. Ждёт сообщение START_PAGINATION из расширения и тогда сам обходит
 *      все страницы через fetch (та же origin → cookies подставятся).
 *
 * Общение с внешним миром — только через window.postMessage с тегом.
 */
(() => {
  const BRIDGE_TAG = 'yandex-chat-members-exporter';
  const TARGET_ENDPOINT = '/api/models?_models=messenger/get-chat-members';
  const MODEL_NAME = 'messenger/get-chat-members';
  const LOG_PREFIX = '[yc-exporter:page]';

  const MSG = {
    MEMBERS_BATCH: 'members-batch',
    PAGINATION_DONE: 'pagination-done',
    PAGINATION_ERROR: 'pagination-error',
    INTERCEPTOR_READY: 'interceptor-ready',
    LOG: 'log',
    // входящие
    START_PAGINATION: 'start-pagination',
    RESET_PAGE_STATE: 'reset-page-state'
  };

  const log = (...args) => {
    console.log(LOG_PREFIX, ...args);
    try {
      window.postMessage(
        { source: BRIDGE_TAG, type: MSG.LOG, payload: { args: args.map(safeStr) } },
        window.location.origin
      );
    } catch {}
  };
  const safeStr = (v) => {
    try { return typeof v === 'string' ? v : JSON.stringify(v); }
    catch { return String(v); }
  };

  const state = {
    orgId: null,
    chatId: null,
    ckey: null,
    nextCursor: null,
    paginating: false,
    capturedAt: 0
  };

  const post = (type, payload) => {
    window.postMessage({ source: BRIDGE_TAG, type, payload }, window.location.origin);
  };

  const matchesEndpoint = (url) =>
    typeof url === 'string' && url.indexOf(TARGET_ENDPOINT) !== -1;

  const extractModel = (json) => {
    if (!json || !Array.isArray(json.models)) return null;
    return json.models.find((m) => m && m.name === MODEL_NAME) || null;
  };

  const handleCapturedResponse = (requestBodyText, responseText, sourceLabel) => {
    try {
      const reqParsed = requestBodyText ? JSON.parse(requestBodyText) : null;
      const reqParams = reqParsed?.models?.[0]?.params;
      if (!reqParams) {
        log(`[${sourceLabel}] нет params в теле запроса, пропускаем`);
        return;
      }
      let json;
      try { json = JSON.parse(responseText); }
      catch (e) { log(`[${sourceLabel}] не JSON-ответ`); return; }

      const model = extractModel(json);
      if (!model) { log(`[${sourceLabel}] в ответе нет модели get-chat-members`); return; }
      if (model.status !== 'ok') { log(`[${sourceLabel}] статус модели не ok:`, model.status); return; }

      state.orgId = reqParams.org_id;
      state.chatId = reqParams.chat_id;
      state.ckey = json.ckey || state.ckey;
      state.nextCursor = (model.data && model.data.next_cursor) || null;
      state.capturedAt = Date.now();

      const items = (model.data && model.data.items) || [];
      log(`[${sourceLabel}] поймали ответ:`, {
        chat: state.chatId, org: state.orgId,
        items: items.length, nextCursor: state.nextCursor ? '…' : null,
        hasCkey: Boolean(state.ckey)
      });

      post(MSG.MEMBERS_BATCH, {
        chatId: state.chatId,
        orgId: state.orgId,
        ckey: state.ckey,
        nextCursor: state.nextCursor,
        source: 'capture',
        items: items.map((it) => ({ id: it.id, uid: it.uid, name: it.name, login: it.login }))
      });
    } catch (e) {
      log('ошибка обработки ответа:', e && e.message || e);
      post(MSG.PAGINATION_ERROR, { message: String(e && e.message || e) });
    }
  };

  // --- XHR patch -------------------------------------------------------------
  const OrigXHR = window.XMLHttpRequest;
  function PatchedXHR() {
    const xhr = new OrigXHR();
    let _url = null;
    let _method = null;
    let _body = null;

    const origOpen = xhr.open;
    xhr.open = function (method, url, ...rest) {
      _method = method;
      _url = url;
      return origOpen.call(this, method, url, ...rest);
    };

    const origSend = xhr.send;
    xhr.send = function (body) {
      _body = body;
      if (matchesEndpoint(_url)) {
        log('XHR обнаружен:', _method, _url);
        xhr.addEventListener('load', () => {
          try {
            const bodyText = typeof _body === 'string' ? _body : null;
            handleCapturedResponse(bodyText, xhr.responseText, 'xhr');
          } catch (e) {
            log('XHR onload error', e && e.message || e);
          }
        });
      }
      return origSend.call(this, body);
    };

    return xhr;
  }
  PatchedXHR.prototype = OrigXHR.prototype;
  window.XMLHttpRequest = PatchedXHR;

  // --- fetch patch (страховка) ----------------------------------------------
  const origFetch = window.fetch ? window.fetch.bind(window) : null;
  if (origFetch) {
    window.fetch = async function patchedFetch(input, init) {
      const url = typeof input === 'string' ? input : input?.url;
      const response = await origFetch(input, init);
      if (matchesEndpoint(url)) {
        log('fetch обнаружен:', url);
        try {
          let bodyText = null;
          if (init && typeof init.body === 'string') bodyText = init.body;
          else if (init && init.body instanceof Blob) bodyText = await init.body.text();
          else if (input && typeof input !== 'string' && typeof input.clone === 'function') {
            bodyText = await input.clone().text();
          }
          const responseText = await response.clone().text();
          handleCapturedResponse(bodyText, responseText, 'fetch');
        } catch (e) {
          log('fetch capture error', e && e.message || e);
        }
      }
      return response;
    };
  }

  // --- пагинация (по команде из popup) --------------------------------------
  const paginateAll = async () => {
    if (state.paginating) { log('пагинация уже идёт, игнорируем'); return; }
    if (!state.chatId || !state.orgId) {
      log('нет chat_id/org_id — сначала откройте чат и дождитесь первого ответа');
      post(MSG.PAGINATION_ERROR, { message: 'Нет данных захвата: chat_id/org_id отсутствуют' });
      return;
    }
    if (!state.nextCursor) {
      log('next_cursor=null уже на первом ответе, всё выгружено');
      post(MSG.PAGINATION_DONE, { chatId: state.chatId });
      return;
    }
    state.paginating = true;
    log('старт пагинации, начальный cursor есть');

    let page = 0;
    try {
      while (state.nextCursor) {
        page += 1;
        const body = JSON.stringify({
          models: [{
            name: MODEL_NAME,
            params: { org_id: state.orgId, chat_id: state.chatId, cursor: state.nextCursor }
          }]
        });

        const fetcher = origFetch || ((u, init) => window.fetch(u, init));
        const res = await fetcher(TARGET_ENDPOINT, {
          method: 'POST',
          credentials: 'include',
          headers: {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'X-Requested-With': 'XMLHttpRequest',
            ...(state.ckey ? { 'x-sarah-ckey': state.ckey } : {})
          },
          body
        });
        if (!res.ok) {
          log(`страница ${page}: HTTP ${res.status}`);
          post(MSG.PAGINATION_ERROR, { message: `HTTP ${res.status} на странице ${page}` });
          return;
        }
        const json = await res.json();
        const model = extractModel(json);
        if (!model || model.status !== 'ok') {
          log('плохой статус модели:', model && model.status);
          post(MSG.PAGINATION_ERROR, { message: 'Bad model status' });
          return;
        }
        state.ckey = json.ckey || state.ckey;
        const items = (model.data && model.data.items) || [];
        state.nextCursor = (model.data && model.data.next_cursor) || null;
        log(`страница ${page}: items=${items.length}, next=${state.nextCursor ? '…' : 'null'}`);
        post(MSG.MEMBERS_BATCH, {
          chatId: state.chatId,
          orgId: state.orgId,
          ckey: state.ckey,
          nextCursor: state.nextCursor,
          source: 'pagination',
          items: items.map((it) => ({ id: it.id, uid: it.uid, name: it.name, login: it.login }))
        });
      }
      log('пагинация завершена');
      post(MSG.PAGINATION_DONE, { chatId: state.chatId });
    } catch (e) {
      log('исключение в пагинации:', e && e.message || e);
      post(MSG.PAGINATION_ERROR, { message: String(e && e.message || e) });
    } finally {
      state.paginating = false;
    }
  };

  const refetchFirstPage = async () => {
    if (!state.chatId || !state.orgId) {
      log('refetch: нет chat_id/org_id, нечего перезапрашивать');
      return;
    }
    log('refetch первой страницы для', state.chatId);
    try {
      const body = JSON.stringify({
        models: [{ name: MODEL_NAME, params: { org_id: state.orgId, chat_id: state.chatId } }]
      });
      const fetcher = origFetch || ((u, init) => window.fetch(u, init));
      const res = await fetcher(TARGET_ENDPOINT, {
        method: 'POST',
        credentials: 'include',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
          'X-Requested-With': 'XMLHttpRequest',
          ...(state.ckey ? { 'x-sarah-ckey': state.ckey } : {})
        },
        body
      });
      if (!res.ok) { log('refetch: HTTP', res.status); return; }
      const json = await res.json();
      state.ckey = json.ckey || state.ckey;
      const model = extractModel(json);
      if (!model || model.status !== 'ok') { log('refetch: bad model'); return; }
      const items = (model.data && model.data.items) || [];
      state.nextCursor = (model.data && model.data.next_cursor) || null;
      log('refetch ок: items=', items.length, 'next=', state.nextCursor ? '…' : 'null');
      post(MSG.MEMBERS_BATCH, {
        chatId: state.chatId,
        orgId: state.orgId,
        ckey: state.ckey,
        nextCursor: state.nextCursor,
        source: 'capture',
        items: items.map((it) => ({ id: it.id, uid: it.uid, name: it.name, login: it.login }))
      });
      if (!state.nextCursor) post(MSG.PAGINATION_DONE, { chatId: state.chatId });
    } catch (e) {
      log('refetch error:', e && e.message || e);
      post(MSG.PAGINATION_ERROR, { message: String(e && e.message || e) });
    }
  };

  // Команды из ISOLATED-мира.
  window.addEventListener('message', (event) => {
    if (event.source !== window) return;
    const data = event.data;
    if (!data || data.source !== BRIDGE_TAG) return;
    if (data.direction !== 'to-page') return;
    if (data.type === MSG.START_PAGINATION) {
      log('получена команда START_PAGINATION');
      paginateAll();
    } else if (data.type === MSG.RESET_PAGE_STATE) {
      log('получена команда RESET_PAGE_STATE');
      const hadChat = Boolean(state.chatId);
      // Очищаем только пагинационный прогресс; chat_id/org_id/ckey оставляем,
      // чтобы можно было сразу перезапросить первую страницу.
      state.nextCursor = null;
      state.paginating = false;
      if (hadChat) refetchFirstPage();
    }
  });

  post(MSG.INTERCEPTOR_READY, {});
  log('interceptor установлен (XHR + fetch)');
})();
