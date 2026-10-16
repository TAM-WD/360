const MAPPINGS = {
  status: {
    'WAITING': 'Ждёт уточнения',
    'ARCHIVED': 'Архивирован',
    'CLOSED': 'Решён',
    'IN_PROGRESS': 'В работе',
    'NEW': 'Новый'
  },
  product: {
    'forms': 'Формы',
    'disk': 'Диск',
    'docs': 'Документы',
    'premium': 'Яндекс 360 Премиум',
    'webs': 'Вебинары',
    'wiki': 'Вики',
    'messenger': 'Мессенджер',
    'telemost': 'Телемост',
    'mail': 'Почта',
    'calendar': 'Календарь',
    'yandex_id': 'Яндекс ID',
    'boards': 'Доски',
    'admin': 'Управление организацией',
    'tracker': 'Трекер',
    'sender': 'Рассылки',
    'alicepro': 'Алиса ПРО',
    'notes': 'Заметки',
    'other': 'Другой'
  }
};

function getStatusLabel(status) {
  if (!status) return '';
  const upperStatus = String(status).toUpperCase();
  return MAPPINGS.status[upperStatus] || status;
}

function getProductLabel(product) {
  if (!product) return '';
  const lowerProduct = String(product).toLowerCase();
  return MAPPINGS.product[lowerProduct] || product;
}

// Session storage survives service-worker shutdowns and page navigation.
// Each tab owns one current organisation/query. Never keep ticket data only in globals.
const STORAGE_PREFIX = 'supportCenter.tab.';
const queues = new Map();
// Selection criteria are explicit. Treating every unknown request parameter as
// a filter made an unrecognised continuation token erase the previous pages.
const FILTER_KEYS = new Set(['org_id', 'uid', 'subject', 'author_uids', 'author_uid',
  'products', 'product', 'statuses', 'status', 'created_at_from', 'created_at_to',
  'updated_at_from', 'updated_at_to', 'search', 'query', 'q', 'text', 'filter', 'filters',
  'sort', 'sort_by', 'sort_order', 'order', 'order_by', 'direction']);
const PAGE_SIZE_KEYS = new Set(['limit', 'page_size', 'per_page']);
const ACTIONS = new Set(['captureTickets', 'captureTicket', 'getNavigationList',
  'getTicketsCount', 'exportTickets', 'clearTickets', 'getDiagnostics']);

function pageKind(url) {
  try {
    const path = new URL(url).pathname;
    if (/^\/support-center\/?$/.test(path)) return 'list';
    if (/^\/support-center\/[^/]+\/?$/.test(path)) return 'ticket';
  } catch { /* Missing/invalid route is not a list page. */ }
  return 'other';
}

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, stable(value[key])]));
  }
  return value;
}

function normalizedFilter(value, key) {
  if (value == null || value === '') return undefined;
  if (Array.isArray(value)) {
    if (value.length === 0) return undefined;
    // These filters represent sets; their order does not change the selection.
    return ['author_uids', 'products', 'statuses'].includes(key) ? [...value].sort() : value;
  }
  if (typeof value === 'object') {
    const result = Object.fromEntries(Object.entries(value).map(([name, item]) =>
      [name, normalizedFilter(item, name)]).filter(([, item]) => item !== undefined));
    return Object.keys(result).length ? result : undefined;
  }
  return ['org_id', 'uid'].includes(key) ? String(value) : value;
}

function queryKey(filters) {
  const query = Object.fromEntries(Object.entries(filters).filter(([key]) => FILTER_KEYS.has(key))
    .map(([key, value]) => [key, normalizedFilter(value, key)]).filter(([, value]) => value !== undefined));
  return JSON.stringify(stable(query));
}

function pageInfo(filters, state, requestedAt) {
  const params = {...filters, ...(filters.pagination || {})};
  const cursorKeys = ['cursor', 'page_token', 'next_page_token', 'continuation_token', 'last_ticket_id',
    'next_cursor', 'page_cursor', 'continuation', 'after', 'after_id', 'last_id', 'from_ticket_id', 'lastTicketId'];
  const cursor = cursorKeys.find(key => params[key] !== undefined && params[key] !== null && params[key] !== '');
  const offset = Number(params.offset || 0);
  const page = Number(params.page ?? params.page_number ?? 1);
  // Preserve the whole continuation signature, including parameters not known
  // to this version. Page-size changes alone must not create a new page.
  const pagination = Object.fromEntries(Object.entries(params).filter(([key, value]) =>
    !FILTER_KEYS.has(key) && !PAGE_SIZE_KEYS.has(key) && key !== 'pagination' &&
    value != null && value !== '' && !(key === 'offset' && Number(value) === 0) &&
    !(['page', 'page_number'].includes(key) && Number(value) <= 1)));
  const signature = JSON.stringify(stable(pagination));
  const key = signature === '{}' ? 'first' : signature;
  const previousFirst = state.pages.find(item => item.first);
  const first = !cursor && offset <= 0 && page <= 1 &&
    (!previousFirst || previousFirst.key === key || requestedAt < previousFirst.order);
  return {
    key,
    first,
    position: first ? 0 : offset > 0 ? offset : page > 1 ? page : null
  };
}

function contextOf(message) {
  const context = message.context || {};
  return {uid: String(context.uid || ''), orgId: String(message.filters?.org_id || context.orgId || '')};
}

function differentContext(a, b) {
  return ['uid', 'orgId'].some(key => a[key] && b[key] && a[key] !== b[key]);
}

function emptyState(context = {}) {
  return {context, queryKey: null, filters: {}, pages: [], records: {}, recordTimes: {},
    lastUpdate: null, queryStartedAt: 0, clearedAt: 0};
}

function ticketFields(ticket) {
  if (!ticket || !['string', 'number'].includes(typeof ticket.ticket_id)) return null;
  const id = String(ticket.ticket_id);
  if (!id || ['__proto__', 'constructor', 'prototype'].includes(id)) return null;
  const result = {ticket_id: id};
  for (const key of ['created_at', 'external_ticket_id', 'subject', 'status', 'product']) {
    if (Object.hasOwn(ticket, key)) result[key] = ticket[key];
  }
  if (ticket.created_by && Object.hasOwn(ticket.created_by, 'email')) {
    result.created_by = {email: ticket.created_by.email};
  }
  return result;
}

function orderedTickets(state) {
  const ids = [...new Set(state.pages.flatMap(page => page.ids))];
  return ids.map(id => state.records[id]).filter(Boolean);
}

function visibleTickets(tickets, settings) {
  return tickets.filter(ticket =>
    // Partial API records may omit subject; only an explicit empty value proves
    // the ticket has no subject. Do not filter out unknown fields.
    !(settings.hideNoSubject && Object.hasOwn(ticket, 'subject') &&
      (ticket.subject == null || ['', 'Без темы'].includes(String(ticket.subject).trim()))) &&
    !(settings.hideArchived && String(ticket.status).toUpperCase() === 'ARCHIVED')
  );
}

function inTab(tabId, operation) {
  const previous = queues.get(tabId) || Promise.resolve();
  const next = previous.catch(() => {}).then(operation);
  queues.set(tabId, next);
  next.finally(() => { if (queues.get(tabId) === next) queues.delete(tabId); }).catch(() => {});
  return next;
}

function notifyTab(tabId) {
  chrome.tabs.sendMessage(tabId, {action: 'ticketsUpdated'}).catch(() => {});
  chrome.runtime.sendMessage({action: 'ticketsUpdated', tabId}).catch(() => {});
}

async function handleMessage(message, sender, tabId) {
  const key = STORAGE_PREFIX + tabId;
  let state = (await chrome.storage.session.get(key))[key] || emptyState();
  const context = contextOf(message);
  const requestedAt = Number(message.requestedAt) || Date.now();
  const before = orderedTickets(state).length;
  const previousQuery = state.queryKey;
  const history = state.history || [];
  const save = async reason => {
    // Bounded diagnostics contain counts/reasons only, never ticket IDs, text or account IDs.
    const oldCriteria = JSON.parse(previousQuery || '{}');
    const newCriteria = JSON.parse(state.queryKey || '{}');
    state.history = [...history, {action: message.action, reason, before,
      after: orderedTickets(state).length, at: Date.now(),
      ...(message.action === 'captureTickets' ? {received: message.tickets?.length || 0,
        pages: state.pages.length, selectionChanged: previousQuery !== state.queryKey,
        changedFilterKeys: [...FILTER_KEYS].filter(name => JSON.stringify(oldCriteria[name]) !== JSON.stringify(newCriteria[name])),
        pageParameterKeys: Object.keys(message.filters || {}).filter(name => !FILTER_KEYS.has(name) && /^[a-zA-Z_][a-zA-Z0-9_]{0,63}$/.test(name))} : {})}].slice(-12);
    await chrome.storage.session.set({[key]: state});
  };
  const ignore = async reason => {
    await save(reason);
    return {success: true, ignored: true, reason, count: before};
  };

  if (message.action === 'captureTickets' || message.action === 'captureTicket') {
    // Data must originate in the tab; popup messages can only read or clear its cache.
    if (!sender.tab || !/^https:\/\/admin(?:\.360)?\.yandex\.ru\//.test(sender.url || '')) {
      throw new Error('Неизвестный источник списка тикетов');
    }
    if (requestedAt < state.clearedAt) return {success: true, ignored: true};
    if (message.action === 'captureTicket') {
      // A detail response can refresh an existing record, never own/reset the selection.
      if (differentContext(state.context, context)) return ignore('detail-context-mismatch');
      const ticket = ticketFields(message.ticket);
      if (!ticket || !state.records[ticket.ticket_id]) return ignore('detail-not-in-list');
      if (requestedAt < (state.recordTimes[ticket.ticket_id] || 0)) return ignore('older-detail');
      state.records[ticket.ticket_id] = {...state.records[ticket.ticket_id], ...ticket};
      state.recordTimes[ticket.ticket_id] = requestedAt;
      state.lastUpdate = Date.now();
      await save('detail-updated');
      notifyTab(tabId);
      return {success: true, count: before};
    }
    // Card/sidebar/prefetch requests must not replace the user's list selection.
    if (pageKind(message.pageUrl || sender.url) !== 'list' ||
        pageKind(message.receivedPageUrl || sender.url) !== 'list') {
      return ignore('list-outside-list-view');
    }
    if (!Array.isArray(message.tickets)) throw new Error('Некорректный список тикетов');
    const tickets = message.tickets.map(ticketFields).filter(Boolean);
    if (tickets.length !== message.tickets.length) return ignore('invalid-list-items');
    if (differentContext(state.context, context)) {
      if (requestedAt < state.queryStartedAt) return {success: true, ignored: true};
      state = {...emptyState(context), clearedAt: state.clearedAt};
    }
    state.context = {
      uid: context.uid || state.context.uid || '',
      orgId: context.orgId || state.context.orgId || ''
    };

    if (message.action === 'captureTickets') {
      if (!Array.isArray(message.tickets)) throw new Error('Некорректный список тикетов');
      const filters = message.filters || {};
      const selection = queryKey(filters);
      if (state.queryKey !== selection) {
        if (requestedAt < state.queryStartedAt) return {success: true, ignored: true};
        state = {...emptyState(state.context), queryKey: selection, queryStartedAt: requestedAt, clearedAt: state.clearedAt};
      }
      const page = pageInfo(filters, state, requestedAt);
      const oldPage = state.pages.find(item => item.key === page.key);
      if (oldPage && requestedAt < oldPage.requestedAt) return {success: true, ignored: true};
      // Empty first page is an empty selection. Empty continuation is only the end of pagination.
      if (page.first && tickets.length === 0) {
        state.pages = [];
        state.records = {};
        state.recordTimes = {};
        state.queryStartedAt = requestedAt;
        state.clearedAt = Math.max(state.clearedAt, requestedAt);
      } else {
        const receivedIds = tickets.map(ticket => ticket.ticket_id);
        // Some list views grow `limit` instead of sending a cursor. A reload may then
        // return a shorter first page; retain collected IDs beyond that fresh prefix.
        const ids = page.first ? [...new Set([...receivedIds, ...(oldPage?.ids || [])])] : receivedIds;
        const batch = {...page, ids, requestedAt,
          order: oldPage?.order ?? requestedAt};
        state.pages = state.pages.filter(item => item.key !== page.key);
        if (page.first) {
          // A response that started earlier can arrive after an unknown continuation.
          for (const item of state.pages) {
            if (item.first) { item.first = false; item.position = null; }
          }
        }
        state.pages.push(batch);
        state.pages.sort((a, b) => {
          if (a.first !== b.first) return a.first ? -1 : 1;
          if (a.position !== null && b.position !== null) return a.position - b.position;
          return a.order - b.order;
        });
        for (const ticket of tickets) {
          if (requestedAt >= (state.recordTimes[ticket.ticket_id] || 0)) {
            state.records[ticket.ticket_id] = {...state.records[ticket.ticket_id], ...ticket};
            state.recordTimes[ticket.ticket_id] = requestedAt;
          }
        }
        const retained = new Set(state.pages.flatMap(item => item.ids));
        for (const id of Object.keys(state.records)) {
          if (!retained.has(id)) { delete state.records[id]; delete state.recordTimes[id]; }
        }
      }
      state.filters = filters;
    }
    state.lastUpdate = Date.now();
    // Acknowledge only after the write succeeds (including quota/error handling).
    await save(tickets.length ? 'list-updated' : state.pages.length === 0 ? 'empty-list' : 'end-of-list');
    notifyTab(tabId);
    return {success: true, count: orderedTickets(state).length};
  }

  if (message.action === 'clearTickets') {
    state = {...emptyState(context), clearedAt: Date.now()};
    await save('manual-clear');
    notifyTab(tabId);
    return {success: true};
  }

  const tickets = differentContext(state.context, context) ? [] : orderedTickets(state);
  const settings = await chrome.storage.local.get(['hideNoSubject', 'hideArchived']);
  const filtered = visibleTickets(tickets, settings);
  if (message.action === 'getTicketsCount') return {success: true, count: tickets.length, visibleCount: filtered.length};
  if (message.action === 'getDiagnostics') {
    return {success: true, version: chrome.runtime.getManifest().version,
      stored: orderedTickets(state).length, available: tickets.length, visible: filtered.length,
      missingSubject: tickets.filter(ticket => !Object.hasOwn(ticket, 'subject')).length,
      excludedNoSubject: tickets.length - visibleTickets(tickets, {hideNoSubject: settings.hideNoSubject}).length,
      excludedArchived: tickets.length - visibleTickets(tickets, {hideArchived: settings.hideArchived}).length,
      contextMismatch: differentContext(state.context, context), history: state.history || []};
  }
  if (message.action === 'getNavigationList') {
    // The app can prefetch other cards in this document. A known route ID is
    // stronger evidence than the most recent get-ticket request.
    const currentTicketId = tickets.some(ticket => ticket.ticket_id === message.routeTicketId) ?
      message.routeTicketId : message.currentTicketId;
    // A ticket may become hidden while it is open (for example, after archiving).
    // Keep its position as the anchor, while skipping hidden destination tickets.
    const visibleIds = new Set(filtered.map(ticket => ticket.ticket_id));
    const navigation = tickets.filter(ticket => visibleIds.has(ticket.ticket_id) || ticket.ticket_id === currentTicketId);
    return {success: true, tickets: navigation.map(ticket => ticket.ticket_id), currentTicketId,
      context: differentContext(state.context, context) ? {} : state.context,
      metadata: {lastUpdate: state.lastUpdate, filters: state.filters, total: tickets.length,
        visible: filtered.length, currentFound: tickets.some(ticket => ticket.ticket_id === currentTicketId),
        routeFound: tickets.some(ticket => ticket.ticket_id === message.routeTicketId),
        contextMismatch: differentContext(state.context, context)}};
  }
  return {success: true, csv: convertToCSV(filtered), count: filtered.length};
}

chrome.runtime.onMessage.addListener((message, sender, sendResponse) => {
  if (!message || !ACTIONS.has(message.action)) return false;
  const fromPopup = sender.url === chrome.runtime.getURL('popup.html');
  const tabId = fromPopup ? message.tabId : sender.tab?.id ?? message.tabId;
  if (!Number.isInteger(tabId) || tabId < 0) {
    sendResponse({success: false, error: 'Не выбрана вкладка Центра поддержки'});
    return false;
  }
  inTab(tabId, () => handleMessage(message, sender, tabId)).then(sendResponse, error => {
    console.error('Support Center Helper: ошибка хранилища', error.message);
    sendResponse({success: false, error: error.message});
  });
  return true;
});

chrome.tabs.onRemoved.addListener(tabId => {
  inTab(tabId, () => chrome.storage.session.remove(STORAGE_PREFIX + tabId)).catch(error => {
    console.error('Support Center Helper: ошибка очистки вкладки', error.message);
  });
});

// Конвертация данных в CSV
function convertToCSV(tickets) {
  if (tickets.length === 0) {
    return 'Автор,Дата создания,ID тикета,Внешний ID,Тема,Статус,Сервис\n';
  }
  
  const headers = ['Автор', 'Дата создания', 'ID тикета', 'Внешний ID', 'Тема', 'Статус', 'Сервис'];
  
  function escapeCSV(value) {
    if (value === null || value === undefined) {
      return '';
    }
    
    const stringValue = String(value);
    
    if (stringValue.includes(',') || stringValue.includes('"') || stringValue.includes('\n')) {
      return `"${stringValue.replace(/"/g, '""')}"`;
    }
    
    return stringValue;
  }
  
  function formatDate(isoString) {
    if (!isoString) return '';
    
    try {
      const date = new Date(isoString);
      
      if (isNaN(date.getTime())) return '';
      
      const day = String(date.getDate()).padStart(2, '0');
      const month = String(date.getMonth() + 1).padStart(2, '0');
      const year = date.getFullYear();
      const hours = String(date.getHours()).padStart(2, '0');
      const minutes = String(date.getMinutes()).padStart(2, '0');
      
      return `${day}.${month}.${year} ${hours}:${minutes}`;
    } catch (error) {
      return '';
    }
  }
  
  const rows = tickets.map(ticket => {
    const email = ticket.created_by?.email || '';
    
    return [
      escapeCSV(email),
      escapeCSV(formatDate(ticket.created_at)),
      escapeCSV(ticket.ticket_id || ''),
      escapeCSV(ticket.external_ticket_id || ''),
      escapeCSV(ticket.subject || ''),
      escapeCSV(getStatusLabel(ticket.status)),
      escapeCSV(getProductLabel(ticket.product))
    ].join(',');
  });
  
  const csv = [headers.join(','), ...rows].join('\n');
  
  return '\uFEFF' + csv;
}
