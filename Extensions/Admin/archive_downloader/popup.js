// popup.js
const statusEl = document.getElementById('status');
const errorEl = document.getElementById('error');
const logArea = document.getElementById('log-area');
const tokenStatusEl = document.getElementById('token-status');

let currentAuditEvents = [];
let currentStashSearchId = null;
let currentOrgId = null;
let currentUid = null;
let forwardReportData = [];
let allStashMessages = [];

const EVENT_TYPES = [
  { value: 'message_trash', label: 'Удаление в корзину', desc: 'Письмо перемещено в Trash' },
  { value: 'message_purge', label: 'Удаление навсегда', desc: 'Письмо удалено окончательно' },
  { value: 'mailbox_send', label: 'Отправка письма', desc: 'Письмо отправлено из ящика' },
  { value: 'message_receive', label: 'Получение письма', desc: 'Письмо получено в ящик' },
  { value: 'message_forward', label: 'Пересылка', desc: 'Письмо переслано другому адресату' },
  { value: 'message_copy', label: 'Копирование', desc: 'Письмо скопировано в другую папку' },
  { value: 'message_move', label: 'Перемещение', desc: 'Письмо перемещено в другую папку' },
  { value: 'message_seen', label: 'Просмотр', desc: 'Письмо отмечено как прочитанное' },
  { value: 'message_unseen', label: 'Непрочитанное', desc: 'Письмо отмечено как непрочитанное' },
  { value: 'message_spam', label: 'Спам', desc: 'Письмо перемещено в спам' },
  { value: 'message_answer', label: 'Ответ', desc: 'На письмо отправлен ответ' }
];

const formFields = [
  'org-id', 'audit-uid', 'audit-date-from', 'audit-date-to', 'forward-to'
];

// === БЕЗОПАСНОЕ СОХРАНЕНИЕ В STORAGE ===

// Безопасное сохранение в storage с обработкой ошибок (например, превышение квоты)
async function safeStorageSet(data) {
  try {
    await chrome.storage.local.set(data);
    return true;
  } catch (e) {
    console.error('❌ Ошибка сохранения в storage:', e);
    logMessage(`⚠️ Не удалось сохранить данные в постоянное хранилище (${e.message}). Данные доступны только в текущей сессии — экспортируйте их сейчас, не закрывая попап!`);
    return false;
  }
}

async function safeStorageRemove(keys) {
  try {
    await chrome.storage.local.remove(keys);
    return true;
  } catch (e) {
    console.error('❌ Ошибка удаления из storage:', e);
    return false;
  }
}

// === ИНИЦИАЛИЗАЦИЯ ЧЕКБОКСОВ СОБЫТИЙ ===

function initEventCheckboxes() {
  const container = document.getElementById('event-types-container');
  container.innerHTML = '';
  EVENT_TYPES.forEach(eventType => {
    const div = document.createElement('div');
    div.className = 'event-type-item';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.id = `event-${eventType.value}`;
    checkbox.value = eventType.value;
    checkbox.addEventListener('change', () => {
      updateSelectAllCheckbox();
      saveFormData();
    });
    const label = document.createElement('label');
    label.htmlFor = `event-${eventType.value}`;
    label.innerHTML = `<strong>${eventType.label}</strong><span class="event-desc">${eventType.desc}</span>`;
    div.appendChild(checkbox);
    div.appendChild(label);
    container.appendChild(div);
  });
  document.getElementById('event-message_trash').checked = true;
  document.getElementById('event-message_purge').checked = true;
}

function updateSelectAllCheckbox() {
  const selectAll = document.getElementById('select-all-events');
  const checkboxes = document.querySelectorAll('.event-types input[type="checkbox"]');
  selectAll.checked = Array.from(checkboxes).every(cb => cb.checked);
}

function getSelectedEventTypes() {
  return Array.from(document.querySelectorAll('.event-types input[type="checkbox"]:checked')).map(cb => cb.value);
}

function setAllEventTypes(checked) {
  document.querySelectorAll('.event-types input[type="checkbox"]').forEach(cb => cb.checked = checked);
  updateSelectAllCheckbox();
  saveFormData();
}

// === СОХРАНЕНИЕ И ВОССТАНОВЛЕНИЕ ДАННЫХ ФОРМЫ ===

async function saveFormData() {
  const data = {};
  formFields.forEach(id => {
    const el = document.getElementById(id);
    if (el) data[id] = el.value;
  });
  data.eventTypes = getSelectedEventTypes();
  await safeStorageSet({ formData: data });
}

async function restoreFormData() {
  const result = await chrome.storage.local.get('formData');
  if (result.formData) {
    formFields.forEach(id => {
      const el = document.getElementById(id);
      if (el && result.formData[id]) el.value = result.formData[id];
    });
    if (result.formData.eventTypes && Array.isArray(result.formData.eventTypes)) {
      result.formData.eventTypes.forEach(type => {
        const checkbox = document.getElementById(`event-${type}`);
        if (checkbox) checkbox.checked = true;
      });
    }
    updateSelectAllCheckbox();
  }
  
  const sessionData = await chrome.storage.local.get([
    'auditEvents', 'stashSearchId', 'orgId', 'uid', 'forwardReportData', 'sessionCkey', 'allStashMessages'
  ]);
  if (sessionData.auditEvents) currentAuditEvents = sessionData.auditEvents;
  if (sessionData.stashSearchId) currentStashSearchId = sessionData.stashSearchId;
  if (sessionData.orgId) currentOrgId = sessionData.orgId;
  if (sessionData.uid) currentUid = sessionData.uid;
  if (sessionData.forwardReportData) forwardReportData = sessionData.forwardReportData;
  if (sessionData.allStashMessages) allStashMessages = sessionData.allStashMessages;
  
  updateTokenStatus(!!sessionData.sessionCkey);
  updateExportButtons();
  
  if (currentStashSearchId) {
    document.getElementById('check-forward-count').style.display = 'block';
  }
  
  if (currentAuditEvents.length > 0) {
    setStatus(`Восстановлено ${currentAuditEvents.length} событий из предыдущей сессии`);
  }
}

function updateTokenStatus(hasToken) {
  if (hasToken) {
    tokenStatusEl.className = 'token-status valid';
    tokenStatusEl.textContent = '🔑 Токен сессии: Активен (можно выполнять поиск)';
  } else {
    tokenStatusEl.className = 'token-status invalid';
    tokenStatusEl.textContent = '🔑 Токен сессии: Не получен (нажмите "Инициализировать сессию")';
  }
}

function updateExportButtons() {
  document.getElementById('export-forward-report').style.display = (forwardReportData && forwardReportData.length > 0) ? 'block' : 'none';
  document.getElementById('export-stash-messages').style.display = (allStashMessages && allStashMessages.length > 0) ? 'block' : 'none';
}

function setStatus(msg) { statusEl.textContent = msg; errorEl.textContent = ''; logArea.style.display = 'none'; }
function setError(msg) { errorEl.textContent = msg; statusEl.textContent = ''; logArea.style.display = 'none'; }
function logMessage(msg) {
  logArea.style.display = 'block';
  const div = document.createElement('div');
  div.textContent = msg;
  logArea.appendChild(div);
  logArea.scrollTop = logArea.scrollHeight;
}

function sendMessage(message) {
  return new Promise((resolve, reject) => {
    chrome.runtime.sendMessage(message, (response) => {
      if (chrome.runtime.lastError) reject(new Error(chrome.runtime.lastError.message));
      else resolve(response);
    });
  });
}

// === СБРОС СЕССИИ ===

document.getElementById('reset-session').addEventListener('click', async () => {
  if (!confirm('Вы уверены? Все результаты поиска аудит-лога, найденные письма в архиве и отчеты будут удалены. Придется заново инициализировать сессию.')) {
    return;
  }
  
  try {
    await sendMessage({ type: 'resetSession' });
  } catch (e) {
    console.error('Ошибка при сбросе токенов background:', e);
  }
  
  await safeStorageRemove([
    'auditEvents', 'stashSearchId', 'orgId', 'uid', 
    'forwardReportData', 'sessionCkey', 'sessionCid', 'allStashMessages'
  ]);
  
  currentAuditEvents = [];
  currentStashSearchId = null;
  currentOrgId = null;
  currentUid = null;
  forwardReportData = [];
  allStashMessages = [];
  
  updateTokenStatus(false);
  updateExportButtons();
  document.getElementById('check-forward-count').style.display = 'none';
  document.getElementById('stash-forward').style.display = 'none';
  
  logArea.innerHTML = '';
  logArea.style.display = 'none';
  setStatus('✅ Сессия и все результаты поиска сброшены. Можно начинать заново.');
});

// === ИНИЦИАЛИЗАЦИЯ СЕССИИ ===

document.getElementById('init-session').addEventListener('click', async (event) => {
  const button = event.target;
  const orgId = document.getElementById('org-id').value.trim();
  const uid = document.getElementById('audit-uid').value.trim();
  if (!orgId) { setError('Укажите Org ID'); return; }
  if (!uid) { setError('Укажите UID сотрудника'); return; }
  
  button.disabled = true;
  button.textContent = '⏳ Инициализация...';
  
  setStatus('Инициализация сессии...');
  try {
    const result = await sendMessage({ type: 'initSession', orgId: parseInt(orgId), uid: uid });
    if (result.success) {
      updateTokenStatus(true);
      setStatus('✅ Сессия инициализирована! Теперь можно выполнять поиск.');
      logMessage('Токен сессии получен');
      await saveFormData();
    } else { setError(result.error || 'Ошибка инициализации'); }
  } catch (e) { 
    setError(e.message); 
  } finally {
    button.disabled = false;
    button.textContent = '🔑 Инициализировать сессию';
  }
});

document.getElementById('select-all-events').addEventListener('change', (e) => setAllEventTypes(e.target.checked));

// === ЧАСТЬ 1: АУДИТ-ЛОГ ===

document.getElementById('audit-search').addEventListener('click', async () => {
  const orgId = document.getElementById('org-id').value.trim();
  const uid = document.getElementById('audit-uid').value.trim();
  const dateFrom = document.getElementById('audit-date-from').value;
  const dateTo = document.getElementById('audit-date-to').value;
  const eventTypes = getSelectedEventTypes();

  if (!orgId) { setError('Укажите Org ID'); return; }
  if (!uid) { setError('Укажите UID сотрудника'); return; }
  if (!dateFrom || !dateTo) { setError('Укажите период дат'); return; }
  if (new Date(dateFrom) > new Date(dateTo)) { setError('Дата начала не может быть позже даты окончания'); return; }
  if (eventTypes.length === 0) { setError('Выберите хотя бы один тип события'); return; }

  // При новом поиске сбрасываем предыдущие результаты по архиву
  currentStashSearchId = null;
  forwardReportData = [];
  allStashMessages = [];
  await safeStorageRemove(['stashSearchId', 'forwardReportData', 'allStashMessages']);
  updateExportButtons();
  document.getElementById('check-forward-count').style.display = 'none';
  document.getElementById('stash-forward').style.display = 'none';

  setStatus(`Поиск аудит-лога... (типов событий: ${eventTypes.length})`);
  try {
    const result = await sendMessage({ type: 'searchAuditLog', orgId: parseInt(orgId), uid, dateFrom, dateTo, eventTypes });
    if (result.success) {
      currentAuditEvents = result.events;
      currentOrgId = parseInt(orgId);
      currentUid = uid;
      
      const saved = await safeStorageSet({ auditEvents: currentAuditEvents, orgId: currentOrgId, uid: currentUid });
      
      const eventTypeCount = {};
      currentAuditEvents.forEach(e => eventTypeCount[e.type] = (eventTypeCount[e.type] || 0) + 1);
      const uniqueMids = [...new Set(currentAuditEvents.map(e => e.mid).filter(m => m))];
      
      if (saved) {
        setStatus(`Найдено событий: ${currentAuditEvents.length} (уникальных MID: ${uniqueMids.length}). Готово к экспорту.`);
      } else {
        setStatus(`⚠️ Найдено ${currentAuditEvents.length} событий, НО не сохранено на диск! Экспортируйте XLSX прямо сейчас, не закрывая попап.`);
      }
      logMessage(`Найдено ${currentAuditEvents.length} событий за период с ${dateFrom} по ${dateTo}`);
      logMessage(`Уникальных MID: ${uniqueMids.length}`);
      logMessage(`Распределение: ${Object.entries(eventTypeCount).map(([k,v]) => `${k}: ${v}`).join(', ')}`);
      await saveFormData();
    } else { setError(result.error || 'Ошибка поиска'); }
  } catch (e) { setError(e.message); }
});

document.getElementById('audit-export').addEventListener('click', () => {
  if (!currentAuditEvents || currentAuditEvents.length === 0) { setError('Сначала выполните поиск аудит-лога'); return; }
  try {
    if (typeof XLSX === 'undefined') { setError('Библиотека XLSX не загружена'); return; }
    const events = currentAuditEvents.map(e => ({
      user_uid: e.user_uid, org_id: e.org_id, type: e.type, date: new Date(e.date).toISOString(),
      date_timestamp: e.date, request_id: e.request_id, user_login: e.user_login, user_name: e.user_name,
      uniq_id: e.uniq_id, subject: e.subject, actor_uid: e.actor_uid, msg_id: e.msg_id,
      email_from: e.email_from, email_to: e.email_to, email_cc: e.email_cc, email_bcc: e.email_bcc,
      source: e.source, mid: e.mid, dest_mid: e.dest_mid, folder_name: e.folder_name,
      folder_type: e.folder_type, labels: JSON.stringify(e.labels), client_ip: e.client_ip
    }));
    const ws = XLSX.utils.json_to_sheet(events);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'AuditLog');
    const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
    const blob = new Blob([wbout], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `audit_log_${new Date().toISOString().slice(0,10)}_${currentUid}.xlsx`;
    document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
    setStatus('Экспорт выполнен успешно!');
  } catch (e) { setError('Ошибка экспорта: ' + e.message); }
});

// === ЧАСТЬ 2: АРХИВ ПИСЕМ И ПЕРЕСЫЛКА ===

document.getElementById('stash-search').addEventListener('click', async () => {
  if (!currentAuditEvents || currentAuditEvents.length === 0) { setError('Сначала выполните поиск аудит-лога'); return; }
  if (!currentOrgId || !currentUid) { setError('Org ID и UID не определены'); return; }
  
  const dateFrom = document.getElementById('audit-date-from').value;
  const dateTo = document.getElementById('audit-date-to').value;
  
  setStatus('Создание поиска в архиве...');
  try {
    const result = await sendMessage({ 
      type: 'createStashSearch', 
      orgId: currentOrgId, 
      uid: currentUid, 
      auditEvents: currentAuditEvents,
      dateFrom: dateFrom,
      dateTo: dateTo
    });
    if (result.success) {
      currentStashSearchId = result.searchId;
      await safeStorageSet({ stashSearchId: currentStashSearchId });
      setStatus(`Поиск создан. ID: ${currentStashSearchId}. Теперь проверьте количество писем для пересылки.`);
      logMessage(`Создан поиск в архиве: ID=${currentStashSearchId}`);
      
      document.getElementById('check-forward-count').style.display = 'block';
      document.getElementById('stash-forward').style.display = 'none';
      
      await saveFormData();
    } else {
      if (result.error && result.error.includes('challengeUrl')) setError('⚠️ Требуется проверка безопасности! Откройте Архив писем и пройдите проверку телефона.');
      else setError(result.error || 'Ошибка создания поиска');
    }
  } catch (e) { setError(e.message); }
});

document.getElementById('check-forward-count').addEventListener('click', async () => {
  if (!currentStashSearchId || !currentOrgId) { setError('Сначала создайте поиск в архиве'); return; }
  
  setStatus('🔎 Сканирование архива для подсчета писем... Это может занять время.');
  try {
    const result = await sendMessage({ 
      type: 'checkForwardCount', 
      auditEvents: currentAuditEvents, 
      searchId: currentStashSearchId, 
      orgId: currentOrgId 
    });
    
    if (result.success) {
      allStashMessages = result.allMessages || [];
      const saved = await safeStorageSet({ allStashMessages });
      
      const uniqueMidsFromAudit = [...new Set(currentAuditEvents.map(e => e.mid).filter(m => m && m !== "0"))];
      
      if (saved) {
        setStatus(`✅ Найдено ${result.matchCount} писем из ${uniqueMidsFromAudit.length} для пересылки. Всего в архиве: ${allStashMessages.length} писем.`);
      } else {
        setStatus(`⚠️ Найдено ${result.matchCount} писем, НО архив не сохранён на диск! Экспортируйте XLSX прямо сейчас.`);
      }
      logMessage(`Проверка завершена: совпадений по MID — ${result.matchCount} из ${uniqueMidsFromAudit.length}`);
      logMessage(`Всего писем найдено в архиве за период: ${allStashMessages.length}`);
      
      if (result.matchCount > 0) {
        document.getElementById('stash-forward').style.display = 'block';
        logMessage('👉 Если результат вас устраивает, нажмите "Переслать найденные письма"');
      } else {
        document.getElementById('stash-forward').style.display = 'none';
        logMessage('⚠️ Совпадений не найдено. Пересылать нечего.');
      }
      
      updateExportButtons();
    } else {
      setError(result.error || 'Ошибка проверки количества писем');
    }
  } catch (e) { setError(e.message); }
});

document.getElementById('stash-forward').addEventListener('click', async () => {
  const to = document.getElementById('forward-to').value.trim();
  if (!to) { setError('Укажите ящик для пересылки'); return; }
  if (!currentStashSearchId || !currentOrgId) { setError('Сначала создайте поиск в архиве'); return; }

  if (!allStashMessages || allStashMessages.length === 0) {
    if (!confirm('Вы еще не проверяли количество писем для пересылки. Рекомендуется сначала нажать "Проверить количество писем". Продолжить пересылку без проверки?')) {
      return;
    }
  }

  setStatus('Пересылка писем... Это может занять время.');
  try {
    const result = await sendMessage({ type: 'forwardEmails', auditEvents: currentAuditEvents, searchId: currentStashSearchId, orgId: currentOrgId, to });
    if (result.success) {
      forwardReportData = result.report;
      allStashMessages = result.allMessages || allStashMessages;
      const saved = await safeStorageSet({ forwardReportData, allStashMessages });
      
      const uniqueMidsFromAudit = [...new Set(currentAuditEvents.map(e => e.mid).filter(m => m && m !== "0"))];
      const uniqueMidsFromStash = [...new Set(forwardReportData.map(r => r.mid))];
      const missingMids = uniqueMidsFromAudit.filter(mid => !uniqueMidsFromStash.includes(mid));
      
      if (saved) {
        setStatus(`Пересылка завершена. Переслано: ${result.forwarded} из ${uniqueMidsFromAudit.length}. Не найдено: ${missingMids.length}`);
      } else {
        setStatus(`⚠️ Переслано ${result.forwarded} писем, НО отчёт не сохранён на диск! Экспортируйте XLSX прямо сейчас.`);
      }
      logMessage(`Переслано: ${result.forwarded} писем`);
      logMessage(`Уникальных MID из аудит-лога: ${uniqueMidsFromAudit.length}`);
      logMessage(`Найдено в архиве: ${allStashMessages.length}`);
      logMessage(`Не найдено в архиве: ${missingMids.length} MID`);
      
      updateExportButtons();
      await saveFormData();
    } else { setError(result.error || 'Ошибка пересылки'); }
  } catch (e) { setError(e.message); }
});

document.getElementById('export-stash-messages').addEventListener('click', () => {
  if (!allStashMessages || allStashMessages.length === 0) { setError('Нет данных писем из архива'); return; }
  try {
    const stashData = allStashMessages.map(msg => ({
      user_uid: msg.uid, org_id: currentOrgId || '', type: 'archive_message',
      date: msg.date_timestamp ? new Date(msg.date_timestamp).toISOString() : '',
      date_timestamp: msg.date_timestamp || 0, request_id: '', user_login: msg.email || '',
      user_name: '', uniq_id: '', subject: msg.subject || '', actor_uid: '', msg_id: msg.message_id || '',
      email_from: msg.from, email_to: msg.to, email_cc: msg.cc, email_bcc: msg.bcc,
      source: 'stash', mid: msg.mid, dest_mid: '', folder_name: msg.tabId || '',
      folder_type: msg.fid ? String(msg.fid) : '', labels: JSON.stringify(msg.lid || []), client_ip: '', size: msg.size || 0
    }));
    const ws = XLSX.utils.json_to_sheet(stashData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'StashMessages');
    const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
    const blob = new Blob([wbout], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `stash_messages_${new Date().toISOString().slice(0,10)}_${currentUid}.xlsx`;
    document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
    setStatus('Экспорт писем архива выполнен успешно!');
  } catch (e) { setError('Ошибка экспорта: ' + e.message); }
});

document.getElementById('export-forward-report').addEventListener('click', () => {
  if (!forwardReportData || forwardReportData.length === 0) { setError('Нет данных отчета о пересылке'); return; }
  try {
    const ws = XLSX.utils.json_to_sheet(forwardReportData);
    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, ws, 'ForwardReport');
    const wbout = XLSX.write(wb, { bookType: 'xlsx', type: 'array' });
    const blob = new Blob([wbout], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `forward_report_${new Date().toISOString().slice(0,10)}.xlsx`;
    document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
    setStatus('Отчет экспортирован успешно!');
  } catch (e) { setError('Ошибка экспорта: ' + e.message); }
});

formFields.forEach(id => {
  const el = document.getElementById(id);
  if (el) {
    el.addEventListener('change', saveFormData);
    el.addEventListener('input', saveFormData);
  }
});

document.addEventListener('DOMContentLoaded', async () => {
  initEventCheckboxes();
  await restoreFormData();
});
