import { collect, buildSheets, dateRange } from './core.js';
import { writeXlsx } from './xlsx.js';
import { createTransport } from './transport.js';

const ui = Object.fromEntries([
  'statusBadge', 'statusTitle', 'activityIndicator', 'jobSummary', 'progressMessage',
  'progress', 'progressDetail', 'meetingCount', 'participantCount', 'eventCount',
  'errorCount', 'resultMessage', 'errorMessage', 'downloadButton', 'cancelButton',
  'keepOpenNote',
].map((id) => [id, document.getElementById(id)]));

const controller = new AbortController();
const countFormatter = new Intl.NumberFormat('ru-RU');
let result = null;
let options = null;
let workbook = null;
let downloadUrl = null;
let running = true;
let preparingDownload = false;

function showError(message) {
  ui.errorMessage.textContent = message;
  ui.errorMessage.hidden = false;
}

function setCount(element, value) {
  if (Number.isFinite(value) && value >= 0) element.textContent = countFormatter.format(value);
}

function setActivity(active) {
  ui.activityIndicator.hidden = !active;
  if (!active) {
    ui.progress.max = 1;
    ui.progress.value = 1;
  }
}

function reportProgress(progress = {}) {
  const stageNames = {
    meetings: 'Собираю встречи…', participants: 'Собираю участников…',
    events: 'Собираю замеры качества…', details: 'Собираю подробности…',
    listing: 'Ищу встречи за выбранный период…',
  };
  ui.progressMessage.textContent = String(progress.message || stageNames[progress.stage] || 'Собираю данные…');
  setCount(ui.meetingCount, progress.meetings);
  setCount(ui.participantCount, progress.participants);
  setCount(ui.eventCount, progress.events);
  setCount(ui.errorCount, progress.errors);
  if (Number.isFinite(progress.total) && progress.total > 0 && Number.isFinite(progress.current)) {
    ui.progress.max = progress.total;
    ui.progress.value = Math.max(0, Math.min(progress.current, progress.total));
    ui.progressDetail.textContent = `${countFormatter.format(progress.current)} из ${countFormatter.format(progress.total)}`;
  } else {
    ui.progress.removeAttribute('value');
    ui.progressDetail.textContent = '';
  }
}

function dateLabel(value) {
  const [year, month, day] = value.split('-');
  return `${day}.${month}.${year}`;
}

function showResult() {
  const status = result.status;
  const labels = {
    complete: ['Готово', 'Выгрузка готова', 'Все доступные данные за выбранный период собраны.'],
    partial: ['С замечаниями', 'Сбор завершён с ошибками', 'Часть запросов завершилась ошибкой. Скачайте собранные данные; подробности есть на листе ошибок.'],
    cancelled: ['Остановлено', 'Сбор остановлен', 'Можно скачать данные, которые удалось собрать до остановки.'],
  };
  const [badge, title, message] = labels[status] || ['Завершено', 'Сбор завершён', 'Собранные данные доступны для скачивания.'];
  ui.statusBadge.textContent = badge;
  ui.statusBadge.dataset.state = status;
  ui.statusTitle.textContent = title;
  ui.resultMessage.textContent = message;
  ui.resultMessage.hidden = false;
  ui.progressMessage.textContent = 'Подготавливаю файл Excel…';
  ui.progressDetail.textContent = '';
  setCount(ui.meetingCount, result.meetings?.length || 0);
  setCount(ui.participantCount, result.participants?.length || 0);
  setCount(ui.eventCount, result.events?.length || 0);
  setCount(ui.errorCount, result.errors?.length || 0);
  ui.cancelButton.hidden = true;
  ui.downloadButton.textContent = status === 'cancelled' ? 'Скачать собранное' : 'Скачать XLSX';
  ui.keepOpenNote.textContent = 'Данные хранятся в памяти этой вкладки. После её закрытия повторное скачивание будет недоступно.';
}

async function prepareWorkbook() {
  if (workbook) return workbook;
  const sheets = await buildSheets(result, options);
  const bytes = await writeXlsx(sheets);
  if (!(bytes instanceof Uint8Array) && !(bytes instanceof ArrayBuffer) && !(bytes instanceof Blob)) {
    throw new Error('Не удалось сформировать файл Excel: неизвестный формат результата.');
  }
  workbook = bytes instanceof Blob ? bytes : new Blob([bytes], {
    type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  });
  return workbook;
}

async function downloadWorkbook() {
  if (!result || preparingDownload) return;
  preparingDownload = true;
  ui.downloadButton.disabled = true;
  try {
    const blob = await prepareWorkbook();
    if (!downloadUrl) downloadUrl = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = downloadUrl;
    anchor.download = `telemost_${options.startDate}_${options.endDate}.xlsx`;
    document.body.append(anchor);
    anchor.click();
    anchor.remove();
    ui.progressMessage.textContent = 'Файл готов. Если скачивание не началось, нажмите кнопку ниже.';
  } catch (error) {
    showError(`Не удалось скачать XLSX: ${error.message || String(error)}`);
    ui.progressMessage.textContent = 'Данные собраны. Попробуйте скачать файл ещё раз.';
  } finally {
    preparingDownload = false;
    ui.downloadButton.disabled = false;
  }
}

ui.cancelButton.addEventListener('click', () => {
  if (!running) return;
  controller.abort();
  ui.cancelButton.disabled = true;
  ui.cancelButton.textContent = 'Останавливаю…';
  ui.statusBadge.textContent = 'Остановка';
  ui.progressMessage.textContent = 'Остановлю после текущего запроса (до 25 секунд) и сохраню полученные данные…';
});
ui.downloadButton.addEventListener('click', downloadWorkbook);
window.addEventListener('pagehide', () => {
  controller.abort();
  if (downloadUrl) URL.revokeObjectURL(downloadUrl);
});

async function run() {
  try {
    const jobId = new URL(window.location.href).searchParams.get('job');
    if (!jobId || !/^[a-zA-Z0-9-]{1,80}$/.test(jobId)) {
      throw new Error('Не найдено задание на выгрузку. Запустите сбор из окна расширения.');
    }
    const key = `job:${jobId}`;
    const stored = await chrome.storage.session.get(key);
    const job = stored[key];
    if (!job?.options || !job.context) {
      throw new Error('Задание недоступно или уже было запущено. Откройте расширение и начните новую выгрузку.');
    }
    options = job.options;
    if (typeof options.orgId !== 'string' || !options.orgId.trim() || job.context.orgId !== options.orgId) {
      throw new Error('Организация не совпадает с контекстом открытой аналитики. Начните выгрузку заново.');
    }
    dateRange(options.startDate, options.endDate, options.timeZone);
    await chrome.storage.session.remove(key);
    ui.jobSummary.textContent = `${dateLabel(options.startDate)} — ${dateLabel(options.endDate)} · ${options.timeZone} · Организация ${options.orgId}`;
    ui.statusBadge.textContent = 'Сбор данных';
    ui.statusTitle.textContent = 'Собираю встречи и участников';
    result = await collect(options, createTransport(job.context, options.orgId), {
      signal: controller.signal,
      onProgress: reportProgress,
    });
    running = false;
    showResult();
    await prepareWorkbook();
    ui.downloadButton.hidden = false;
    if (result.status === 'complete' || result.status === 'partial') {
      await downloadWorkbook();
    } else {
      ui.progressMessage.textContent = 'Собранные данные готовы к скачиванию.';
    }
  } catch (error) {
    running = false;
    ui.statusBadge.textContent = 'Ошибка';
    ui.statusBadge.dataset.state = 'error';
    ui.statusTitle.textContent = result ? 'Не удалось подготовить Excel' : 'Сбор не завершён';
    ui.progressMessage.textContent = result
      ? 'Данные остаются в памяти этой вкладки. Можно повторить скачивание.'
      : 'Запустите новую выгрузку из окна расширения.';
    ui.progressDetail.textContent = '';
    ui.cancelButton.hidden = true;
    if (result) ui.downloadButton.hidden = false;
    showError(error.message || String(error));
  } finally {
    setActivity(false);
  }
}

run();
