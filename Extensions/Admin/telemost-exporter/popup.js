const analyticsUrl = 'https://admin.yandex.ru/telemost-monitoring';
const form = document.querySelector('#exportForm');
const orgInput = document.querySelector('#orgId');
const startInput = document.querySelector('#startDate');
const endInput = document.querySelector('#endDate');
const timeZoneInput = document.querySelector('#timeZone');
const exportButton = document.querySelector('#startExport');
const refreshButton = document.querySelector('#refreshContext');
const openButton = document.querySelector('#openAnalytics');
const connectionState = document.querySelector('#connectionState');
const connectionMessage = document.querySelector('#connectionMessage');
const formMessage = document.querySelector('#formMessage');

let contexts = [];
let contextLoading = true;
let starting = false;

function dateInZone(date, timeZone) {
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone, year: 'numeric', month: '2-digit', day: '2-digit',
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map(({ type, value }) => [type, value]));
  return `${values.year}-${values.month}-${values.day}`;
}

function setDefaultDates() {
  const end = dateInZone(new Date(), timeZoneInput.value);
  const start = new Date(`${end}T12:00:00Z`);
  start.setUTCDate(start.getUTCDate() - 6);
  startInput.value = start.toISOString().slice(0, 10);
  endInput.value = end;
}

function selectedContext() {
  const orgId = orgInput.value.trim();
  return contexts.find((context) => context.orgId === orgId) || null;
}

function validationMessage() {
  if (!orgInput.value.trim()) return 'Укажите ID организации.';
  if (!startInput.value || !endInput.value) return 'Укажите обе даты.';
  if (!startInput.validity.valid || !endInput.validity.valid) return 'Проверьте формат дат.';
  if (startInput.value > endInput.value) return 'Дата начала должна быть не позже даты окончания.';
  if (!selectedContext()) return 'Откройте аналитику этой организации и обновите страницу кабинета.';
  return '';
}

function showFormMessage(message, isError = true) {
  formMessage.textContent = message;
  formMessage.hidden = !message;
  formMessage.classList.toggle('is-error', isError);
}

function updateConnection() {
  const context = selectedContext();
  connectionState.classList.toggle('is-ready', Boolean(context) && !contextLoading);
  if (contextLoading) {
    connectionState.textContent = 'Проверяю кабинет…';
    connectionMessage.textContent = 'Ищу открытую аналитику встреч.';
  } else if (context) {
    connectionState.textContent = 'Кабинет подключён';
    connectionMessage.textContent = `Найден контекст организации ${String(context.orgId)}.`;
  } else {
    connectionState.textContent = 'Нужно открыть аналитику';
    connectionMessage.textContent = contexts.length
      ? 'Для указанной организации нет контекста. Откройте её аналитику и обновите страницу кабинета.'
      : 'Откройте admin.yandex.ru/telemost-monitoring и обновите страницу кабинета. Затем нажмите «Обновить» здесь.';
  }
  openButton.hidden = contextLoading || Boolean(context);
  refreshButton.disabled = contextLoading || starting;
  exportButton.disabled = contextLoading || starting || Boolean(validationMessage());
}

async function refreshContexts() {
  contextLoading = true;
  updateConnection();
  try {
    const response = await chrome.runtime.sendMessage({ type: 'CONTEXTS' });
    contexts = Array.isArray(response?.contexts)
      ? response.contexts
        .filter((context) => context && context.orgId != null && Number.isInteger(context.tabId))
        .map((context) => ({ ...context, orgId: String(context.orgId) }))
      : [];
    if (!orgInput.value.trim() && contexts.length) orgInput.value = String(contexts[0].orgId);
    showFormMessage('');
  } catch (error) {
    contexts = [];
    showFormMessage(`Не удалось проверить кабинет: ${error.message || String(error)}`);
  } finally {
    contextLoading = false;
    updateConnection();
  }
}

refreshButton.addEventListener('click', refreshContexts);
openButton.addEventListener('click', async () => {
  try {
    await chrome.tabs.create({ url: analyticsUrl });
  } catch (error) {
    showFormMessage(`Не удалось открыть аналитику: ${error.message || String(error)}`);
  }
});

form.addEventListener('input', () => {
  showFormMessage('');
  updateConnection();
});
form.addEventListener('change', updateConnection);

form.addEventListener('submit', async (event) => {
  event.preventDefault();
  if (starting || contextLoading) return;
  const problem = validationMessage();
  if (problem) {
    showFormMessage(problem);
    return;
  }
  const orgId = orgInput.value.trim();
  const context = selectedContext();
  // A manually entered organization ID may only select an observed matching context.
  if (!context || context.orgId !== orgId) {
    showFormMessage('Организация не совпадает с открытой аналитикой.');
    return;
  }
  const options = {
    orgId,
    startDate: startInput.value,
    endDate: endInput.value,
    timeZone: timeZoneInput.value,
    includeIPs: document.querySelector('#includeIPs').checked,
    includeEvents: document.querySelector('#includeEvents').checked,
  };
  starting = true;
  exportButton.textContent = 'Открываю сбор…';
  updateConnection();
  const jobId = crypto.randomUUID();
  const key = `job:${jobId}`;
  try {
    await chrome.storage.session.set({ [key]: { options, context } });
    await chrome.tabs.create({ url: `${chrome.runtime.getURL('runner.html')}?job=${encodeURIComponent(jobId)}` });
    window.close();
  } catch (error) {
    await chrome.storage.session.remove(key).catch(() => {});
    showFormMessage(`Не удалось запустить сбор: ${error.message || String(error)}`);
    starting = false;
    exportButton.textContent = 'Скачать XLSX';
    updateConnection();
  }
});

setDefaultDates();
refreshContexts();
