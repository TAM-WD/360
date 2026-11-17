// Only translate fields whose meaning is known from the recorded analytics schema.
// Unknown fields stay unknown; their original paths can be kept in a data dictionary.
const FIELDS = Object.freeze({
  conference_id: 'ID конференции',
  session_id: 'ID сессии',
  conference_name: 'Название встречи',
  participant_id: 'ID участника',
  name: 'Имя в Телемосте',
  'owner.name': 'Имя организатора',
  start_date: 'Начало встречи',
  end_date: 'Окончание встречи',
  duration: 'Длительность в исходном формате',
  state: 'Состояние встречи',
  total_participants_count: 'Количество участников по данным сервера',
  max_active_participants_count: 'Пик одновременных участников',
  'client.user_platform': 'Платформа участника',
  'client.user_app_version': 'Версия приложения участника',
  'client.user_ip': 'IP-адрес участника',
  'device_names.video': 'Камера',
  'device_names.audio': 'Аудиоустройство',
  'device_names.display_video': 'Устройство демонстрации видео',
  'device_names.display_audio': 'Аудиоустройство демонстрации',
  min_participant_session_created_at: 'Первое подключение к встрече',
  max_participant_session_ended_at: 'Последний выход из встречи',
  'participant_sessions_dates_agg.min_participant_session_created_at': 'Первое подключение к встрече',
  'participant_sessions_dates_agg.max_participant_session_ended_at': 'Последний выход из встречи',
  'participant_waiting_room_sessions_dates_agg.min_participant_waiting_room_session_created_at': 'Первое подключение к залу ожидания',
  'participant_waiting_room_sessions_dates_agg.max_participant_waiting_room_session_ended_at': 'Последний выход из зала ожидания',
  reconnect_count: 'Количество разрывов связи',
  connection_time_ms: 'Время подключения, мс',
  'stats.publisher.mos': 'Оценка качества отправки (MOS)',
  'stats.subscriber.mos': 'Оценка качества приёма (MOS)',
  'stats.mos': 'Оценка качества встречи (MOS)',
  'stats.cpu_usage': 'Использование процессора',
  media_role: 'Направление потока',
  media_stream: 'Тип медиапотока',
  bitrate_events: 'Замеры битрейта',
  rtt_events: 'Замеры задержки полного обмена',
  jitter_events: 'Замеры колебаний задержки',
  packet_loss_events: 'Замеры потери пакетов',
  ts: 'Время замера',
});

const STREAMS = Object.freeze({
  audio_incoming: 'входящего звука',
  audio_outgoing: 'исходящего звука',
  video_incoming: 'входящего видео',
  video_outgoing: 'исходящего видео',
  sharing_video_incoming: 'входящего видео демонстрации',
  sharing_video_outgoing: 'исходящего видео демонстрации',
  sharing_audio_incoming: 'входящего звука демонстрации',
  sharing_audio_outgoing: 'исходящего звука демонстрации',
});

const METRICS = Object.freeze({
  bitrate: ['Битрейт', 'бит/с'],
  bitrate_avg: ['Средний битрейт', 'бит/с'],
  bitrate_max: ['Максимальный битрейт', 'бит/с'],
  packet_loss: ['Потеря пакетов', 'доля'],
  packet_loss_avg: ['Средняя потеря пакетов', 'доля'],
  packet_loss_max: ['Максимальная потеря пакетов', 'доля'],
  rtt: ['Задержка полного обмена', 'мс'],
  rtt_avg: ['Средняя задержка полного обмена', 'мс'],
  rtt_max: ['Максимальная задержка полного обмена', 'мс'],
  jitter: ['Колебания задержки', 'мс'],
  jitter_avg: ['Средние колебания задержки', 'мс'],
  jitter_max: ['Максимальные колебания задержки', 'мс'],
});
const own = (object, key) => Object.prototype.hasOwnProperty.call(object, key);

/** Return a Russian header for a known flattened path, or null without guessing. */
export function extraColumnLabel(path) {
  if (typeof path !== 'string' || !path) return null;
  const field = path.startsWith('api.') ? path.slice(4) : path;
  if (own(FIELDS, field)) return FIELDS[field];
  // The quality sheet has a separate stream column, so its metric paths are flat.
  if (own(METRICS, field)) {
    const [label, unit] = METRICS[field];
    return `${label}, ${unit}`;
  }
  const parts = field.split('.');
  if (parts.length !== 2 || !own(STREAMS, parts[0]) || !own(METRICS, parts[1])) return null;
  const [label, unit] = METRICS[parts[1]];
  return `${label} ${STREAMS[parts[0]]}, ${unit}`;
}
