import {extraColumnLabel} from './labels.js';

export const MEDIA = ['audio', 'video', 'sharing-video'];
export const ROLES = ['publisher', 'subscriber'];
export const AGGREGATES = ['audio_incoming', 'audio_outgoing', 'video_incoming', 'video_outgoing', 'sharing_video_incoming', 'sharing_video_outgoing', 'sharing_audio_incoming', 'sharing_audio_outgoing'];

export function dateRange(startDate, endDate, timeZone = 'Europe/Moscow') {
  if (!['Europe/Moscow', 'UTC'].includes(timeZone)) throw new Error('Выберите Москву или UTC.');
  const valid = value => {
    if (!/^\d{4}-\d{2}-\d{2}$/.test(value || '')) return false;
    const d = new Date(value + 'T00:00:00Z');
    return Number.isFinite(d.getTime()) && d.toISOString().slice(0, 10) === value;
  };
  if (!valid(startDate) || !valid(endDate)) throw new Error('Укажите корректные даты начала и окончания.');
  if (startDate > endDate) throw new Error('Дата окончания должна быть не раньше даты начала.');
  if (startDate < '2000-01-01' || endDate > '2099-12-31') throw new Error('Поддерживаются даты 2000–2099.');
  // Moscow has a fixed UTC+3 offset throughout the supported Telemost history.
  if (timeZone === 'Europe/Moscow' && startDate < '2014-10-27') throw new Error('Для Москвы поддерживаются даты с 27.10.2014.');
  const offset = timeZone === 'Europe/Moscow' ? '+03:00' : 'Z';
  const from = new Date(startDate + 'T00:00:00' + offset).toISOString();
  const until = new Date(Date.parse(endDate + 'T00:00:00' + offset) + 86400000).toISOString();
  return {from, to: new Date(Date.parse(until) - 1).toISOString(), until};
}

export function durationSeconds(value) {
  if (typeof value !== 'string') return null;
  const m = /^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)D)?(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?$/.exec(value);
  if (!m || Number(m[1]) || Number(m[2])) return null;
  return Number(m[3] || 0) * 86400 + Number(m[4] || 0) * 3600 + Number(m[5] || 0) * 60 + Number(m[6] || 0);
}

const sleep = (ms, signal) => new Promise((resolve, reject) => {
  signal?.throwIfAborted();
  const abort = () => {clearTimeout(timer); reject(signal.reason || new DOMException('Отмена', 'AbortError'));};
  const timer = setTimeout(() => {signal?.removeEventListener('abort', abort); resolve();}, ms);
  signal?.addEventListener('abort', abort, {once: true});
});
const isObject = value => value && typeof value === 'object' && !Array.isArray(value);
const numeric = value => typeof value === 'number' && Number.isFinite(value) ? value : null;
const idPath = value => encodeURIComponent(String(value));
export const meetingKey = m => `${m.conference_id}/${m.session_id}`;

export async function collect(options, transport, {signal, onProgress = () => {}, wait = sleep} = {}) {
  const range = dateRange(options.startDate, options.endDate, options.timeZone);
  if (!/^\d+$/.test(String(options.orgId || ''))) throw new Error('Укажите числовой ID организации.');
  const result = {meetings: [], participants: [], events: [], errors: [], status: 'complete', metadata: {range, startedAt: new Date().toISOString(), listComplete: false}};
  let loadedDetails = 0;
  const progress = (stage, message, extra = {}) => onProgress({stage, message, meetings: result.meetings.length, participants: result.participants.length, events: result.events.length, errors: result.errors.length, ...extra});
  const log = (stage, m, p, error) => {
    result.errors.push({stage, conference_id: m?.conference_id || '', session_id: m?.session_id || '', participant_id: p?.participant_id || '', status: error.status ?? '', message: error.message, at: new Date().toISOString()});
    if (error && typeof error === 'object') error.recorded = true;
  };
  const request = async (path, params = {}) => {
    for (let attempt = 0; ; attempt++) {
      signal?.throwIfAborted();
      await wait(120, signal);
      try {
        const data = await transport(path, params, signal);
        signal?.throwIfAborted();
        if (!isObject(data)) throw new Error('Неожиданный формат ответа API.');
        return data;
      } catch (error) {
        if (signal?.aborted || error.fatal || attempt >= 3 || (error.status && error.status !== 429 && error.status < 500)) throw error;
        progress('retry', `Повтор запроса ${attempt + 1}/3`);
        await wait(error.retryMs || Math.min(1000 * 2 ** attempt, 10000), signal);
      }
    }
  };
  const safeRequest = async (stage, m, p, path, validate = () => true) => {
    try {
      const data = await request(path);
      if (!validate(data)) throw new Error('Карточка API пуста, имеет неожиданный формат или ID не совпадают с запросом.');
      return data;
    }
    catch (error) {
      if (signal?.aborted) throw error;
      log(stage, m, p, error);
      if (error.fatal) throw error;
      return null;
    }
  };
  async function* pages(path, field) {
    const cursors = new Set(); let cursor; let page = 0;
    do {
      if (++page > 100000) throw new Error('Превышен защитный лимит страниц. Выгрузка неполная.');
      const params = {limit: '15'};
      if (field === 'conferences_sessions') Object.assign(params, {date_from: range.from, date_to: range.to});
      if (cursor) params.iteration_key = cursor;
      const data = await request(path, params);
      if (!Array.isArray(data[field])) throw new Error(`В ответе API отсутствует массив ${field}.`);
      yield data[field];
      cursor = data.iteration_key;
      if (cursor != null && typeof cursor !== 'string') throw new Error('Неожиданный формат курсора API.');
      if (cursor && cursors.has(cursor)) throw new Error('API повторяет курсор. Выгрузка неполная.');
      if (cursor) cursors.add(cursor);
    } while (cursor);
  }
  try {
    progress('meetings', 'Загружаю список встреч');
    const seen = new Set();
    for await (const page of pages('/conferences', 'conferences_sessions')) {
      for (const m of page) {
        if (!m?.conference_id || !m?.session_id) throw new Error('У встречи отсутствует conference_id или session_id.');
        const key = meetingKey(m);
        if (seen.has(key)) continue;
        seen.add(key);
        const start = Date.parse(m.start_date);
        if (Number.isFinite(start) && (start < Date.parse(range.from) || start >= Date.parse(range.until))) continue;
        const record = {list: m, info: null, participantsComplete: false};
        result.meetings.push(record);
        if (!Number.isFinite(start)) log('Период', m, null, new Error('Отсутствует дата начала: принадлежность периоду не проверена.'));
      }
      progress('meetings', `Найдено встреч: ${result.meetings.length}`);
    }
    result.metadata.listComplete = true;
  } catch (error) {
    if (signal?.aborted) result.status = 'cancelled';
    else {if (!error.recorded) log('Список встреч', null, null, error); result.status = 'partial'; if (error.fatal) result.metadata.fatal = true;}
  }
  // Even a later failed list page must not discard meetings already found.
  try {
    if (result.metadata.fatal) throw Object.assign(new Error('Остановка после ошибки доступа.'), {recorded: true});
    for (let i = 0; i < result.meetings.length; i++) {
      signal?.throwIfAborted();
      const record = result.meetings[i], m = record.list;
      const base = `/conferences/${idPath(m.conference_id)}/sessions/${idPath(m.session_id)}`;
      progress('details', `Встреча ${i + 1} из ${result.meetings.length}`, {current: i + 1, total: result.meetings.length});
      record.info = await safeRequest('Карточка встречи', m, null, base + '/info', data => String(data.conference_id) === String(m.conference_id) && String(data.session_id) === String(m.session_id));
      const participants = []; const seen = new Set();
      try {
        for await (const page of pages(base + '/participants', 'participants')) {
          for (const p of page) {
            if (!p?.participant_id) throw new Error('У участника отсутствует participant_id.');
            if (seen.has(p.participant_id)) continue;
            seen.add(p.participant_id);
            const item = {meeting: m, list: p, detail: null};
            participants.push(item); result.participants.push(item);
          }
        }
        record.participantsComplete = true;
      } catch (error) {
        if (signal?.aborted) throw error;
        log('Список участников', m, null, error);
        if (error.fatal) throw error;
      }
      const total = numeric(record.info?.total_participants_count);
      record.exportedParticipants = participants.length;
      if (record.participantsComplete && total != null && total !== participants.length) log('Сверка участников', m, null, new Error(`API total_participants_count=${total}, строк участников=${participants.length}. Определение счётчика API требует проверки.`));
      for (const p of participants) {
        signal?.throwIfAborted();
        const participantBase = base + '/participants/' + idPath(p.list.participant_id);
        p.detail = await safeRequest('Карточка участника', m, p.list, participantBase, data => data.participant_id === p.list.participant_id);
        if (p.detail) loadedDetails++;
        progress('participants', `Загружено карточек: ${loadedDetails}`);
        if (options.includeEvents) for (const role of ROLES) for (const media of MEDIA) {
          const data = await safeRequest(`Замеры ${role}/${media}`, m, p.list, `${participantBase}/stats/${role}/${media}`, data => Object.entries(data).some(([k, v]) => k.endsWith('_events') && Array.isArray(v)));
          if (data) result.events.push({meeting: m, participant_id: p.list.participant_id, role, media, data});
          progress('events', `Наборов замеров: ${result.events.length}`);
        }
      }
    }
  } catch (error) {
    if (signal?.aborted) result.status = 'cancelled';
    else {if (!error.recorded) log('Обход встреч', null, null, error); result.status = 'partial';}
  }
  if (result.status !== 'cancelled' && result.errors.length) result.status = 'partial';
  result.metadata.endedAt = new Date().toISOString();
  progress('finished', result.status === 'complete' ? 'Сбор завершён' : result.status === 'cancelled' ? 'Сбор остановлен' : 'Сбор завершён с пропусками');
  return result;
}

const col = (key, label, type = 'text', width = 24) => ({key, label, type, width});
function wallDate(value, timeZone) {
  if (!value || !Number.isFinite(Date.parse(value))) return null;
  const parts = new Intl.DateTimeFormat('en-GB', {timeZone, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23'}).formatToParts(new Date(value));
  const p = Object.fromEntries(parts.map(x => [x.type, x.value]));
  return new Date(Date.UTC(Number(p.year), Number(p.month) - 1, Number(p.day), Number(p.hour), Number(p.minute), Number(p.second)));
}
// Remove credentials and optional IP addresses before preparing extra fields.
export function sanitize(value, includeIPs = false, key = '') {
  if (/(?:token|cookie|password|authorization|credential|secret|ckey|signature)/i.test(key)) return '[исключено]';
  if (!includeIPs && /(?:^|_)(?:ip|ip_address|user_ip)$/i.test(key)) return '[IP исключён]';
  if (Array.isArray(value)) return value.map(v => sanitize(v, includeIPs));
  if (isObject(value)) return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, sanitize(v, includeIPs, k)]));
  if (typeof value === 'string' && /^https?:\/\//i.test(value)) {
    try { const u = new URL(value); if (u.search || u.hash) return u.origin + u.pathname + '?[параметры исключены]'; } catch { /* Plain text. */ }
  }
  return value;
}
function flatten(value, prefix = '', result = {}) {
  if (isObject(value)) {
    for (const [k, v] of Object.entries(value)) flatten(v, prefix ? `${prefix}.${k}` : k, result);
  } else if (Array.isArray(value)) {
    if (value.length) result[prefix] = JSON.stringify(value);
  } else if (value != null && value !== '' && !['[исключено]', '[IP исключён]'].includes(value)) result[prefix] = value;
  return result;
}
function extraFields(value, includeIPs) {
  const fields = {};
  for (const [key, v] of Object.entries(flatten(sanitize(value, includeIPs)))) {
    if (typeof v !== 'string' || v.length <= 30000) fields['api.' + key] = v;
    else {
      let part = 0;
      for (let start = 0; start < v.length;) {
        let end = Math.min(start + 30000, v.length);
        if (end < v.length && /[\uD800-\uDBFF]/.test(v[end - 1])) end--;
        fields[`api.${key}.__part_${++part}`] = v.slice(start, end);
        start = end;
      }
    }
  }
  return fields;
}
function addExtraColumns(sheet, field = 'api.') {
  const keys = new Set(); for (const row of sheet.rows) for (const k of Object.keys(row)) if (k.startsWith(field)) keys.add(k);
  const labels = new Set(sheet.columns.map(c => c.label));
  const unknownFields = new Map();
  for (const key of [...keys].sort((a, b) => a.localeCompare(b, 'en', {numeric: true}))) {
    const sample = sheet.rows.find(r => r[key] != null && r[key] !== '')?.[key];
    if (sample == null || sample === '') continue;
    const part = /\.__part_(\d+)$/.exec(key);
    const path = part ? key.slice(0, part.index) : key;
    let label = extraColumnLabel(path);
    if (!label) {
      if (!unknownFields.has(path)) unknownFields.set(path, unknownFields.size + 1);
      label = `Дополнительное поле ${unknownFields.get(path)}`;
    }
    if (part) label += `, часть ${part[1]}`;
    const base = label;
    let duplicate = 1;
    while (labels.has(label)) label = `${base} (дополнительно ${duplicate++})`;
    labels.add(label);
    sheet.columns.push(col(key, label, typeof sample === 'number' ? 'number' : 'text', 32));
  }
}
export function buildSheets(result, options) {
  const tz = options.timeZone || 'Europe/Moscow';
  const timeLabel = tz === 'Europe/Moscow' ? 'МСК' : 'UTC';
  const contextColumns = [col('conference_id', 'Идентификатор конференции'), col('session_id', 'Идентификатор сессии', 'text', 48)];
  const participantsColumns = [...contextColumns, col('participant_id', 'Идентификатор участника', 'text', 40)];
  const sheets = [];
  const notes = [
    ['Статус', result.status], ['Организация', String(options.orgId)], ['С', options.startDate], ['По (включительно)', options.endDate], ['Часовой пояс дат Excel', tz],
    ['Определение периода', 'Встречи с началом в выбранных календарных датах. Выбранные даты передаются при запросе списка; дополнительно проверяется дата начала встречи. Встречи без даты начала сохранены с предупреждением.'],
    ['Список встреч пройден до конца', result.metadata.listComplete ? 'Да' : 'Нет'], ['Встреч выгружено', result.meetings.length], ['Строк участников', result.participants.length], ['Ошибок и предупреждений', result.errors.length],
    ['Начало сбора UTC', result.metadata.startedAt], ['Окончание сбора UTC', result.metadata.endedAt],
    ['Количество участников', 'Количество участников и пик одновременно получены от сервера. Строк участников — количество разных идентификаторов участников в сессии. Один человек на разных устройствах может иметь несколько идентификаторов.'],
    ['Время присутствия', 'Время подключения, если сервер его вернул, — отдельный показатель. Интервал от первого появления до последнего выхода может включать отсутствие и не считается временем присутствия.'],
    ['Сетевые единицы', 'Потери пакетов — доля (0–1), битрейт — бит/с, задержка и джиттер — мс. Оценка качества связи (MOS) сохраняется в исходном виде. Значения выгружены без пересчёта.'],
    ['IP-адреса', options.includeIPs ? 'Включены' : 'Исключены из выгрузки'], ['Подробные замеры', options.includeEvents ? 'Включены (6 типов потоков)' : 'Выключены']
  ];
  sheets.push({name: 'Описание', columns: [col('key', 'Параметр', 'text', 36), col('value', 'Значение', 'text', 110)], rows: notes.map(([key, value]) => ({key, value: String(value ?? '')}))});
  const meetings = {name: 'Встречи', columns: [...contextColumns, col('name', 'Название (если есть)'), col('owner', 'Организатор (имя)'), col('start', `Начало (${timeLabel})`, 'datetime'), col('end', `Окончание (${timeLabel})`, 'datetime'), col('duration_seconds', 'Длительность, сек', 'number'), col('duration_iso', 'Длительность в исходном формате'), col('state', 'Состояние'), col('total', 'Участников', 'number'), col('max', 'Пик одновременно', 'number'), col('exported', 'Строк участников', 'number'), col('mos', 'Оценка качества встречи (MOS)', 'number'), col('detail_status', 'Карточка'), col('participants_status', 'Список участников')], rows: []};
  for (const r of result.meetings) {
    const m = r.list, info = r.info || {};
    meetings.rows.push({conference_id: m.conference_id, session_id: m.session_id, name: info.conference_name ?? m.conference_name, owner: m.owner?.name, start: wallDate(m.start_date, tz), end: wallDate(m.end_date, tz), duration_seconds: durationSeconds(info.duration ?? m.duration), duration_iso: info.duration ?? m.duration, state: m.state, total: numeric(info.total_participants_count), max: numeric(info.max_active_participants_count ?? m.max_active_participants_count), exported: r.exportedParticipants ?? 0, mos: numeric(m.stats?.mos), detail_status: r.info ? 'Получена' : 'Не получена', participants_status: r.participantsComplete ? 'Пройден до конца' : 'Не завершён'});
  }
  sheets.push(meetings);
  const participants = {name: 'Участники', columns: [...participantsColumns, col('name', 'Имя в Телемосте'), col('platform', 'Платформа'), col('version', 'Версия приложения'), ...(options.includeIPs ? [col('ip', 'IP-адрес')] : []), col('camera', 'Камера'), col('microphone', 'Аудиоустройство'), col('display_video', 'Устройство демонстрации'), col('display_audio', 'Аудио демонстрации'), col('cpu', 'Использование процессора', 'number'), col('first', `Первое появление (${timeLabel})`, 'datetime'), col('last', `Последний выход (${timeLabel})`, 'datetime'), col('breaks', 'Разрывы связи', 'number'), col('connection_ms', 'Время подключения, мс', 'number'), col('waiting_first', `Зал ожидания: начало (${timeLabel})`, 'datetime'), col('waiting_last', `Зал ожидания: конец (${timeLabel})`, 'datetime'), col('publisher_mos', 'Оценка качества отправки (MOS)', 'number'), col('subscriber_mos', 'Оценка качества приёма (MOS)', 'number'), col('status', 'Карточка участника')], rows: []};
  const quality = {name: 'Качество', columns: [...participantsColumns, col('stream', 'Поток')], rows: []};
  for (const p of result.participants) {
    const d = {...p.list, ...(p.detail || {}), client: {...p.list.client, ...p.detail?.client}};
    const context = {conference_id: p.meeting.conference_id, session_id: p.meeting.session_id, participant_id: p.list.participant_id};
    const dates = d.participant_sessions_dates_agg || {};
    const waiting = d.participant_waiting_room_sessions_dates_agg || {};
    const extras = extraFields(d, options.includeIPs);
    participants.rows.push({...context, name: d.name, platform: d.client?.user_platform, version: d.client?.user_app_version, ip: options.includeIPs ? d.client?.user_ip : null, camera: d.device_names?.video, microphone: d.device_names?.audio, display_video: d.device_names?.display_video, display_audio: d.device_names?.display_audio, cpu: numeric(d.stats?.cpu_usage), first: wallDate(dates.min_participant_session_created_at ?? d.min_participant_session_created_at, tz), last: wallDate(dates.max_participant_session_ended_at, tz), breaks: numeric(d.reconnect_count), connection_ms: numeric(d.connection_time_ms), waiting_first: wallDate(waiting.min_participant_waiting_room_session_created_at, tz), waiting_last: wallDate(waiting.max_participant_waiting_room_session_ended_at, tz), publisher_mos: numeric(d.stats?.publisher?.mos), subscriber_mos: numeric(d.stats?.subscriber?.mos), status: p.detail ? 'Получена' : 'Только список', ...extras});
    for (const stream of AGGREGATES) if (isObject(d[stream]) && Object.keys(d[stream]).length) quality.rows.push({...context, stream, ...extraFields(d[stream], options.includeIPs)});
  }
  addExtraColumns(participants); addExtraColumns(quality); sheets.push(participants, quality);
  if (options.includeEvents) {
    const events = {name: 'Замеры', columns: [...participantsColumns, col('role', 'Направление потока'), col('media', 'Тип медиа'), col('metric', 'Метрика'), col('unit', 'Единица'), col('ts', `Время (${timeLabel})`, 'datetime'), col('ts_iso', 'Время в исходном формате'), col('value', 'Значение', 'number')], rows: []};
    for (const e of result.events) {
      for (const [key, list] of Object.entries(e.data)) if (key.endsWith('_events') && Array.isArray(list)) {
        for (const point of list) for (const [metric, value] of Object.entries(point)) if (metric !== 'ts') events.rows.push({conference_id: e.meeting.conference_id, session_id: e.meeting.session_id, participant_id: e.participant_id, role: e.role, media: e.media, metric, unit: ({bitrate: 'бит/с', packet_loss: 'доля', rtt: 'мс', jitter: 'мс'})[metric] || 'единица API', ts: wallDate(point.ts, tz), ts_iso: point.ts, value: numeric(value)});
      }
    }
    sheets.push(events);
  }
  sheets.push({name: 'Ошибки', columns: [col('stage', 'Этап'), ...participantsColumns, col('status', 'Код ответа сервера'), col('message', 'Описание', 'text', 90), col('at', 'Время UTC')], rows: result.errors});
  // Excel's row cap applies independently. Split large datasets into numbered tabs.
  return sheets.flatMap(sheet => {
    const size = 1000000;
    if (sheet.rows.length <= size) return [sheet];
    const parts = []; for (let i = 0; i < sheet.rows.length; i += size) parts.push({...sheet, name: sheet.name + ' ' + (parts.length + 1), rows: sheet.rows.slice(i, i + size)});
    return parts;
  });
}
