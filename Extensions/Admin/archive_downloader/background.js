// background.js

// Глобальное хранилище Sarah-токенов
let sarahCkey = null;
let sarahCid = null;

// Механизм очереди для последовательного выполнения запросов (защита от race condition)
let requestQueue = Promise.resolve();

function generateSarahCid() {
  const randomPart = Math.floor(Math.random() * 100000000);
  const timestamp = Date.now();
  return `SARAH-${randomPart}-${timestamp}`;
}

function enqueue(taskFn) {
  const resultPromise = requestQueue.then(() => taskFn());
  requestQueue = resultPromise.catch(() => {}); 
  return resultPromise;
}

// Внутренняя функция запроса с автоматическим "рукопожатием" (ckey handshake)
async function apiRequestInternal(model, params, additionalHeaders = {}, allowRetry = true) {
  const url = `https://admin.yandex.ru/api/models?_models=${encodeURIComponent(model)}`;
  
  if (!sarahCid) {
    sarahCid = generateSarahCid();
    console.log('🆔 Сгенерирован новый sarah-cid:', sarahCid);
    await chrome.storage.local.set({ sessionCid: sarahCid });
  }
  
  const headers = {
    'Content-Type': 'application/json',
    'X-Requested-With': 'XMLHttpRequest',
    'Accept': 'application/json',
    ...additionalHeaders
  };
  
  headers['x-sarah-cid'] = sarahCid;
  if (sarahCkey) {
    headers['x-sarah-ckey'] = sarahCkey;
  }
  if (params.org_id) {
    headers['x-sarah-uid'] = String(params.org_id);
  }
  headers['x-sarah-locale'] = 'ru';
  
  console.log('📤 Запрос на сервер:', { url, model, params, headers: { 'x-sarah-cid': sarahCid, hasCkey: !!sarahCkey } });
  
  const response = await fetch(url, {
    method: 'POST',
    headers: headers,
    credentials: 'include',
    body: JSON.stringify({ models: [{ name: model, params }] })
  });
  
  const responseText = await response.text();
  
  if (!response.ok) {
    console.error('❌ Ошибка HTTP:', response.status, responseText.substring(0, 200));
    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
  }
  
  let data;
  try {
    data = JSON.parse(responseText);
  } catch (e) {
    console.error('❌ Ошибка парсинга JSON:', e);
    throw new Error('Неверный формат ответа от сервера');
  }
    
  // Сохраняем ckey из общего тела ответа, если он новый
  if (data.ckey && data.ckey !== sarahCkey) {
    sarahCkey = data.ckey;
    console.log('🔑 Сохранён новый ckey:', sarahCkey.substring(0, 20) + '...');
    await chrome.storage.local.set({ sessionCkey: sarahCkey });
  }
  
  // ✅ КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: 
  // Если это "рукопожатие" — сервер сообщает об ошибке ckey, но при этом дал нам новый токен.
  // Автоматически повторяем этот же запрос один раз с уже свежим токеном.
  if (allowRetry && data.models && data.models[0] && data.models[0].status === 'error' && data.models[0].error === 'ckey') {
    console.warn('⚠️ Сервер запросил "рукопожатие" (ckey handshake). Автоматически повторяем запрос с обновлённым токеном...');
    return apiRequestInternal(model, params, additionalHeaders, false); // Повторяем без права на еще одну попытку
  }
  
  return data;
}

function apiRequest(model, params, additionalHeaders = {}) {
  return enqueue(() => apiRequestInternal(model, params, additionalHeaders));
}

// Инициализация сессии (получение начального токена)
async function initSession(orgId, uid) {
  return enqueue(async () => {
    sarahCid = generateSarahCid();
    sarahCkey = null;
    console.log('🔐 Инициализация сессии...', { orgId, uid, newCid: sarahCid });
    
    const params = {
      org_id: orgId,
      count: 1,
      before_date: Date.now(),
      after_date: Date.now() - 86400000,
      type: ['message_trash'],
      include_departments: [],
      include_groups: [],
      include_uids: [String(uid)]
    };
    
    const referer = `https://admin.yandex.ru/auditlog-users?uid=${uid}`;
    
    const data = await apiRequestInternal('auditlog/mail-event-list', params, {
      'X-Retpath-Y': referer,
      'Referer': referer
    });
    
    if (data.models && data.models[0]) {
      if (data.models[0].status === 'error') {
        throw new Error(`Ошибка инициализации: ${data.models[0].error || 'Неизвестная ошибка'}`);
      }
      console.log('✅ Сессия инициализирована, токен получен');
      return { success: true, ckey: sarahCkey };
    }
    throw new Error('Не удалось инициализировать сессию');
  });
}

// Поиск аудит-лога с пагинацией
async function searchAuditLog(orgId, uid, dateFrom, dateTo, eventTypes) {
  const startDate = new Date(dateFrom);
  startDate.setHours(0, 0, 0, 0);
  const endDate = new Date(dateTo);
  endDate.setHours(23, 59, 59, 999);

  const types = eventTypes || ['message_trash', 'message_purge'];

  let allEvents = [];
  let next = null;
  let pageCount = 0;

  const referer = `https://admin.yandex.ru/auditlog-users?uid=${uid}`;
  
  console.log('🔍 Начало поиска аудит-лога:', { orgId, uid, dateFrom, dateTo, eventTypes: types });
  
  do {
    pageCount++;
    console.log(`📄 Страница аудита ${pageCount}, next:`, next);
    
    const params = {
      org_id: orgId,
      count: 100,
      before_date: endDate.getTime(),
      after_date: startDate.getTime(),
      type: types,
      include_departments: [],
      include_groups: [],
      include_uids: [String(uid)]
    };
    if (next) params.next = next;

    const data = await apiRequest('auditlog/mail-event-list', params, {
      'X-Retpath-Y': referer,
      'Referer': referer
    });
    
    if (data.models && data.models[0]) {
      if (data.models[0].status === 'error') {
        throw new Error(`Ошибка API: ${data.models[0].error || 'Неизвестная ошибка'}`);
      }
      if (data.models[0].data && data.models[0].data.events) {
        const events = data.models[0].data.events;
        if (events && events.length > 0) {
          allEvents = allEvents.concat(events);
          console.log(`✅ Добавлено ${events.length} событий, всего: ${allEvents.length}`);
        }
        next = data.models[0].data.next || null;
      } else {
        break;
      }
    } else {
      break;
    }

    if (next) {
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  } while (next);

  console.log('🏁 Поиск аудита завершен, всего событий:', allEvents.length);
  return allEvents;
}

// Создание поиска в архиве
async function createStashSearch(orgId, uid, auditEvents, dateFrom, dateTo) {
  if (!auditEvents || auditEvents.length === 0) {
    throw new Error('Нет событий для создания поиска');
  }
  
  const startDate = new Date(dateFrom);
  startDate.setHours(0, 0, 0, 0);
  const startTimestamp = startDate.getTime();
  
  const endDate = new Date(dateTo);
  endDate.setHours(23, 59, 59, 999);
  const endTimestamp = endDate.getTime();

  console.log('📦 Создание поиска в архиве (по датам календаря):', { dateFrom: startTimestamp, dateTo: endTimestamp, uid });

  const userInfo = {
    id: parseInt(uid),
    email: auditEvents[0].user_login,
    type: 'user',
    fullTitle: auditEvents[0].user_name,
    shortTitle: auditEvents[0].user_name.split(' ').map(w => w[0]).join('.') + '.',
    subTitle: auditEvents[0].user_login.split('@')[0],
    pic: { src: 'https://avatars.mds.yandex.net/get-yapic/0/0-0/islands-200' },
    typeId: `user-${uid}`,
    displayName: auditEvents[0].user_name.split(' ')[0],
    avatarUrl: 'https://avatars.mds.yandex.net/get-yapic/0/0-0/islands-200'
  };

  const payload = {
    org_id: orgId,
    text: '',
    includeUnits: [userInfo],
    excludeUnits: [],
    dates: [startTimestamp, endTimestamp],
    from: [], to: [], subject: false, cc: [], bcc: [],
    attachments: false,
    clientTimezoneOffset: -new Date().getTimezoneOffset()
  };

  const referer = `https://admin.yandex.ru/stash?uid=${uid}`;
  
  const data = await apiRequest('spaniel/search-create', payload, {
    'X-Retpath-Y': referer,
    'Referer': referer
  });
  
  if (data.models && data.models[0]) {
    if (data.models[0].status === 'error') {
      throw new Error(`Ошибка API: ${data.models[0].error || 'Неизвестная ошибка'}`);
    }
    if (data.models[0].data && data.models[0].data.fraudStatus === 'NEED_CHALLENGE') {
      const challengeUrl = data.models[0].data.challengeUrl;
      throw new Error(`Требуется проверка безопасности. Откройте: ${challengeUrl}`);
    }
    if (data.models[0].data && data.models[0].data.search_id) {
      console.log('✅ Поиск создан, search_id:', data.models[0].data.search_id);
      return { searchId: data.models[0].data.search_id, orgId };
    }
  }
  throw new Error('Не удалось создать поиск в архиве');
}

// Получение ОДНОЙ страницы с несколькими попытками (защита от "тихого" rate-limit)
async function fetchStashPage(searchId, orgId, referer, nextParam, maxAttempts = 5) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const payload = { count: 20, org_id: orgId, search_id: searchId, next: nextParam };
      const data = await apiRequest('spaniel/messages', payload, {
        'X-Retpath-Y': referer,
        'Referer': referer
      });
      
      if (data.models && data.models[0]) {
        if (data.models[0].status === 'error') {
          console.warn(`⚠️ Попытка ${attempt}/${maxAttempts}: API вернул ошибку:`, data.models[0].error);
        } else if (data.models[0].data) {
          const list = data.models[0].data.list || [];
          const next = data.models[0].data.next;
          if (list.length > 0 || next) {
            return { list, next };
          } else {
            console.warn(`⚠️ Попытка ${attempt}/${maxAttempts}: пустая страница без next. Повторяем...`);
          }
        }
      }
    } catch (e) {
      console.warn(`⚠️ Попытка ${attempt}/${maxAttempts}: сетевая ошибка:`, e.message);
    }
    
    await new Promise(resolve => setTimeout(resolve, 1500 * attempt));
  }
  
  console.error(`❌ После ${maxAttempts} попыток страница так и осталась пустой. Считаем это концом архива.`);
  return { list: [], next: null };
}

async function getAllStashMessages(searchId, orgId, uid) {
  let nextParam = '0';
  let allMessagesRaw = [];
  let pageCount = 0;
  
  const referer = `https://admin.yandex.ru/stash/${searchId}?uid=${uid}`;
  
  console.log('📬 Получение писем из архива, search_id:', searchId);
  
  while (true) {
    pageCount++;
    console.log(`📄 Страница писем ${pageCount}, next:`, nextParam);
    
    const { list, next } = await fetchStashPage(searchId, orgId, referer, nextParam);
    
    if (list.length > 0) {
      allMessagesRaw = allMessagesRaw.concat(list);
      console.log(`✅ Добавлено ${list.length} писем, всего: ${allMessagesRaw.length}`);
    }
    
    if (next) {
      nextParam = next;
      await new Promise(resolve => setTimeout(resolve, 1000));
    } else {
      console.log('🏁 Архив закончился');
      break;
    }
  }
  
  console.log(`🏁 Получение писем завершено. Всего писем: ${allMessagesRaw.length}`);
  return allMessagesRaw;
}

// Пересылка письма с ретраями (до 3 попыток)
async function forwardMessage(orgId, messageId, searchId, to, uid) {
  const payload = { org_id: orgId, message_id: messageId, search_id: searchId, to: to };
  const referer = `https://admin.yandex.ru/stash/${searchId}/${uid}/${messageId.split('_')[0]}?uid=${uid}`;
  
  let attempts = 0;
  const maxAttempts = 3;
  let lastError = null;

  while (attempts < maxAttempts) {
    attempts++;
    try {
      const data = await apiRequest('spaniel/forward', payload, {
        'X-Retpath-Y': referer,
        'Referer': referer
      });
      
      if (data.models && data.models[0] && data.models[0].status === 'ok') {
        return { success: true };
      } else {
        lastError = data.models?.[0]?.error || 'API вернул ошибку';
      }
    } catch (e) {
      lastError = e.message;
    }
    
    if (attempts < maxAttempts) {
      console.warn(`⚠️ Ошибка пересылки (попытка ${attempts}/${maxAttempts}). Повтор через 1 сек...`);
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
  
  return { success: false, error: `Failed after ${maxAttempts} attempts: ${lastError}` };
}

// Восстановление токенов при старте service worker
chrome.runtime.onStartup.addListener(async () => {
  console.log('🔄 Service Worker запущен');
  const result = await chrome.storage.local.get(['sessionCkey', 'sessionCid']);
  if (result.sessionCkey) {
    sarahCkey = result.sessionCkey;
  }
  if (result.sessionCid) {
    sarahCid = result.sessionCid;
  }
});

// Обработка сообщений
chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  (async () => {
    try {
      console.log('📨 Получено сообщение:', message.type);
      
      switch (message.type) {
        case 'resetSession':
          sarahCkey = null;
          sarahCid = null;
          await chrome.storage.local.remove(['sessionCkey', 'sessionCid']);
          sendResponse({ success: true });
          break;
          
        case 'initSession':
          const initResult = await initSession(message.orgId, message.uid);
          sendResponse(initResult);
          break;
          
        case 'searchAuditLog':
          const events = await searchAuditLog(message.orgId, message.uid, message.dateFrom, message.dateTo, message.eventTypes);
          sendResponse({ success: true, events });
          break;

        case 'createStashSearch':
          const stash = await createStashSearch(message.orgId, message.uid, message.auditEvents, message.dateFrom, message.dateTo);
          sendResponse({ success: true, searchId: stash.searchId, orgId: stash.orgId });
          break;
          
        case 'checkForwardCount': {
          const { auditEvents, searchId, orgId } = message;
          const allMessagesRaw = await getAllStashMessages(searchId, orgId, auditEvents[0].user_uid);
          
          const allMessagesSimplified = allMessagesRaw.map(msg => {
            const getField = (type) => {
              if (!msg.meta.field) return '';
              return msg.meta.field.filter(f => f.type === type).map(f => f.email).join(', ');
            };
            return {
              uid: msg.uid, mid: msg.meta.mid, message_id: msg.meta.message_id,
              subject: msg.meta.subject, email: msg.email,
              date_timestamp: msg.meta.date ? msg.meta.date.timestamp : 0,
              from: getField('from'), to: getField('to'), cc: getField('cc'), bcc: getField('bcc'),
              tabId: msg.meta.tabId, fid: msg.meta.fid, size: msg.meta.size, lid: msg.meta.lid
            };
          });
          
          const mids = [...new Set(auditEvents.map(e => e.mid).filter(m => m && m !== "0"))];
          const matchedMessages = allMessagesRaw.filter(msg => mids.includes(msg.meta.mid));
          
          sendResponse({ 
            success: true, 
            matchCount: matchedMessages.length, 
            totalMids: mids.length,
            allMessages: allMessagesSimplified 
          });
          break;
        }

        case 'forwardEmails':
          const { auditEvents, searchId, orgId, to } = message;
          const allMessagesRaw = await getAllStashMessages(searchId, orgId, auditEvents[0].user_uid);
          
          const allMessages = allMessagesRaw.map(msg => {
            const getField = (type) => {
              if (!msg.meta.field) return '';
              return msg.meta.field.filter(f => f.type === type).map(f => f.email).join(', ');
            };
            return {
              uid: msg.uid, mid: msg.meta.mid, message_id: msg.meta.message_id,
              subject: msg.meta.subject, email: msg.email,
              date_timestamp: msg.meta.date ? msg.meta.date.timestamp : 0,
              from: getField('from'), to: getField('to'), cc: getField('cc'), bcc: getField('bcc'),
              tabId: msg.meta.tabId, fid: msg.meta.fid, size: msg.meta.size, lid: msg.meta.lid
            };
          });

          const mids = [...new Set(auditEvents.map(e => e.mid).filter(m => m && m !== "0"))];
          const messagesToForward = allMessagesRaw.filter(msg => mids.includes(msg.meta.mid));

          const report = [];
          let forwardedCount = 0;

          for (const msg of messagesToForward) {
            const messageId = `${msg.meta.mid}_${msg.uid}`;
            const subject = msg.meta.subject || '';
            const res = await forwardMessage(orgId, messageId, searchId, to, auditEvents[0].user_uid);
            
            report.push({
              mid: msg.meta.mid,
              subject: subject,
              to_email: to,
              status: res.success ? 'OK' : 'Failed',
              error_message: res.success ? '' : (res.error || 'Unknown error')
            });
            
            if (res.success) forwardedCount++;
            await new Promise(resolve => setTimeout(resolve, 500));
          }

          sendResponse({ success: true, forwarded: forwardedCount, report, allMessages });
          break;

        default:
          sendResponse({ success: false, error: 'Неизвестный тип запроса' });
      }
    } catch (e) {
      console.error('❌ Ошибка:', e);
      sendResponse({ success: false, error: e.message });
    }
  })();
  return true;
});
