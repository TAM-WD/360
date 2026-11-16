import {normalizeRooms, toCsv} from './lib/data.mjs';
import {readAdminCatalog} from './lib/admin.mjs';
import {toXlsx} from './lib/xlsx.mjs';

const $ = (id) => document.getElementById(id);
const initialTab = new URL(location.href).searchParams.get('tab');
const tabId = initialTab && /^\d+$/.test(initialTab) ? Number(initialTab) : null;
let catalog = null;
let normalized = null;
let busy = false;
const blobs = new Map();

function status(message, error = false) {
  $('status').textContent = message;
  $('status').classList.toggle('error', error);
}

function setBusy(value, label = 'Загружаем…') {
  busy = value;
  for (const id of ['load', 'organization', 'org-id', 'xlsx', 'csv', 'json']) $(id).disabled = value;
  $('load').textContent = value ? label : 'Загрузить из админки ↗';
}

function clearResult() {
  catalog = null;
  normalized = null;
  $('result').hidden = true;
  $('head').replaceChildren();
  $('body').replaceChildren();
}

function requestedOrg() {
  const text = $('org-id').value.trim() || $('organization').value;
  if (!text) return undefined;
  const id = Number(text);
  if (!/^\d+$/.test(text) || !Number.isSafeInteger(id) || id <= 0) throw new Error('ID организации должен быть положительным целым числом.');
  return id;
}

async function runInAdmin(options) {
  if (tabId === null || !Number.isSafeInteger(tabId)) throw new Error('Откройте админку и нажмите значок расширения на её вкладке.');
  let result;
  try {
    [result] = await chrome.scripting.executeScript({target: {tabId}, world: 'ISOLATED', func: readAdminCatalog, args: [options]});
  } catch {
    throw new Error('Исходная вкладка закрыта или недоступна. Нажмите значок расширения на открытой вкладке admin.yandex.ru.');
  }
  if (!result?.result?.ok) throw new Error(result?.result?.message || 'Не удалось прочитать данные админки. Обновите исходную вкладку.');
  return result.result;
}

function showCatalog(data) {
  const table = normalizeRooms(data.rooms, data.offices);
  catalog = {schemaVersion: 1, exportedAt: new Date().toISOString(), source: data.source, orgId: data.orgId ?? null,
    roomCount: data.rooms.length, warnings: data.warnings || [], rooms: data.rooms, offices: data.offices};
  normalized = table;
  const connected = data.rooms.filter((room) => room.is_bound === true).length;
  const bookable = data.rooms.filter((room) => room.can_book === true).length;
  const unknownConnected = data.rooms.filter((room) => typeof room.is_bound !== 'boolean').length;
  const unknownBookable = data.rooms.filter((room) => typeof room.can_book !== 'boolean').length;
  $('summary').textContent = `${data.rooms.length} комнат · ${connected} подключены к Телемосту · ${bookable} доступны для бронирования${data.orgId ? ` · Организация ${data.orgId}` : ''}`;
  $('column-count').textContent = `${table.columns.length} столбцов в XLSX и CSV`;
  const previewColumns = ['Название', 'Город', 'Расположение', 'Вместимость', 'Почта', 'Описание', 'Активирована в Телемосте', 'Доступна для бронирования'];
  const header = document.createElement('tr');
  for (const name of previewColumns) {
    const cell = document.createElement('th'); cell.scope = 'col'; cell.textContent = name; header.append(cell);
  }
  $('head').replaceChildren(header);
  const fragment = document.createDocumentFragment();
  for (const row of table.rows.slice(0, 20)) {
    const tr = document.createElement('tr');
    for (const name of previewColumns) {
      const cell = document.createElement('td'); cell.textContent = row[name] === '' || row[name] === null || row[name] === undefined ? '—' : String(row[name]); tr.append(cell);
    }
    fragment.append(tr);
  }
  $('body').replaceChildren(fragment);
  const warnings = [...(data.warnings || [])];
  if (unknownConnected || unknownBookable) warnings.push(`Неизвестный статус: Телемост — ${unknownConnected}, бронирование — ${unknownBookable}. В таблице указано «Не указано».`);
  const missing = data.rooms.filter((room) => !room.description).length;
  if (missing) warnings.push(`У ${missing} комнат описание не заполнено в источнике.`);
  $('warnings').replaceChildren();
  for (const warning of warnings) { const p = document.createElement('p'); p.textContent = warning; $('warnings').append(p); }
  $('warnings').hidden = warnings.length === 0;
  $('result').hidden = false;
  status(`Загружено ${data.rooms.length} комнат из текущей сессии админки.`);
}

$('load').addEventListener('click', async () => {
  if (busy) return;
  clearResult(); setBusy(true); status('Читаем каталог переговорных и справочник офисов…');
  try { showCatalog(await runInAdmin({orgId: requestedOrg()})); }
  catch (error) { status(error.message, true); }
  finally { setBusy(false); }
});

$('organization').addEventListener('change', () => { $('org-id').value = ''; clearResult(); });
$('org-id').addEventListener('input', () => { clearResult(); });

async function releaseBlob(id) {
  const url = blobs.get(id);
  if (url) { URL.revokeObjectURL(url); blobs.delete(id); }
}
chrome.downloads.onChanged.addListener((delta) => {
  if (delta.state?.current === 'complete' || delta.state?.current === 'interrupted') releaseBlob(delta.id);
});
async function download(kind) {
  if (busy || !catalog) return;
  const mimeTypes = {xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', csv: 'text/csv;charset=utf-8', json: 'application/json;charset=utf-8'};
  if (!mimeTypes[kind]) return;
  let url;
  setBusy(true, 'Готовим файл…');
  try {
    status(`Подготавливаем ${kind.toUpperCase()}…`);
    const content = kind === 'xlsx' ? toXlsx(normalized.rows, normalized.columns)
      : kind === 'csv' ? toCsv(normalized.rows, normalized.columns) : JSON.stringify(catalog, null, 2);
    const blob = new Blob([content], {type: mimeTypes[kind]});
    url = URL.createObjectURL(blob);
    const date = new Date().toISOString().replace(/[:.]/g, '-');
    const id = await chrome.downloads.download({url, filename: `meeting-rooms-${catalog.orgId || 'catalog'}-${date}.${kind}`, saveAs: true});
    blobs.set(id, url);
    const [item] = await chrome.downloads.search({id}).catch(() => []);
    if (item && item.state !== 'in_progress') await releaseBlob(id);
    status(`Файл ${kind.toUpperCase()} передан браузеру для сохранения.`);
  } catch (error) {
    if (url) URL.revokeObjectURL(url);
    status(url ? 'Сохранение отменено или браузер не смог создать файл. Повторите скачивание.' : `Не удалось создать ${kind.toUpperCase()}: ${error.message}`, true);
  } finally { setBusy(false); }
}
$('xlsx').addEventListener('click', () => download('xlsx'));
$('csv').addEventListener('click', () => download('csv'));
$('json').addEventListener('click', () => download('json'));

async function initialize() {
  try {
    const {organizations, activeOrgId} = await runInAdmin({mode: 'probe'});
    $('organization').replaceChildren();
    const placeholder = document.createElement('option'); placeholder.value = ''; placeholder.textContent = 'Выберите организацию или укажите ID'; $('organization').append(placeholder);
    for (const org of organizations) {
      const option = document.createElement('option'); option.value = String(org.id); option.textContent = `${org.name} · ${org.id}`; $('organization').append(option);
    }
    if (activeOrgId && !organizations.some((org) => org.id === activeOrgId)) {
      const option = document.createElement('option'); option.value = String(activeOrgId); option.textContent = `Текущая организация · ${activeOrgId}`; $('organization').append(option);
    }
    if (activeOrgId) $('organization').value = String(activeOrgId);
    $('connection').textContent = 'Админка подключена';
    if (!busy && !catalog) status(activeOrgId ? 'Выбрана текущая организация. Нажмите «Загрузить из админки».' : 'Выберите организацию перед загрузкой каталога.');
  } catch (error) {
    $('connection').textContent = 'Админка недоступна';
    if (!busy && !catalog) status(error.message);
  }
}
initialize();
