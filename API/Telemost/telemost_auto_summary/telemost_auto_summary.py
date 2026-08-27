#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Скрипт включает автоматическое конспектирование для встреч Яндекс Телемоста,
созданных в организации. ID встреч и UID организаторов берутся из аудит-лога
по событию telemost_conference.created.

Изменение параметров встречи выполняется от лица её организатора.

ТРЕБОВАНИЯ
    Python 3.7+
    pip install requests

ДОСТУПЫ
    1. OAuth-приложение с правом ya360_security:read_auditlog — чтение аудит-лога.
    2. Сервисное приложение с правом telemost-api:conferences.update — выдача
       временных токенов сотрудникам (нужны client_id и client_secret).

НАСТРОЙКА ПЕРЕД ЗАПУСКОМ
    Заполните в секции КОНФИГУРАЦИЯ:
        ORGID          — ID организации, например '7889122'
        AUDIT_TOKEN    — токен OAuth-приложения (аудит-лог)
        CLIENT_ID      — id сервисного приложения
        CLIENT_SECRET  — secret сервисного приложения

ПЕРИОДИЧНОСТЬ
    LOOKBACK_MINUTES = 15    — раз в 15 минут
    LOOKBACK_MINUTES = 60    — раз в час
    LOOKBACK_MINUTES = 1440  — раз в сутки
    Такую же периодичность поставьте в cron для регулярного запуска. OVERLAP_MINUTES
    добавляет запас по времени; повторную обработку одной
    и той же встречи исключает файл состояния.

РЕЖИМЫ
    Обычный:  MANUAL_TASKS пустой — встречи берутся из аудит-лога.
    Ручной:   MANUAL_TASKS = [('1234567890', '11300000123456789')] — пары
              (ID встречи, UID организатора). Аудит-лог не читается,
              удобно для проверки прав.
    Тестовый: DRY_RUN = True — только показать, что было бы изменено.

О ДАТАХ
    Границы периода уходят в запрос с большим запасом (REQUEST_MARGIN_HOURS),
    а точное окно отбирается на стороне скрипта по полю occurred_at. Так результат
    не зависит от часового пояса

РЕЗУЛЬТАТЫ
    logs/<имя_скрипта>_<дата_время>.log        — лог с ротацией
    reports/<имя_скрипта>_<дата_время>.csv     — обработанные встречи
    telemost_autosummary_state.json            — уже обработанные встречи
'''

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import requests  # type: ignore
from requests.adapters import HTTPAdapter  # type: ignore
from urllib3.util.retry import Retry  # type: ignore

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

ORGID = ''            # ID организации, например '7889122'
AUDIT_TOKEN = ''      # OAuth-токен с правом ya360_security:read_auditlog

CLIENT_ID = ''        # id сервисного приложения с правом telemost-api:conferences.update
CLIENT_SECRET = ''    # secret этого же сервисного приложения

# Переменные окружения имеют приоритет над значениями выше
ORGID = os.getenv('YA360_ORG_ID', ORGID)
AUDIT_TOKEN = os.getenv('YA360_AUDIT_TOKEN', AUDIT_TOKEN)
CLIENT_ID = os.getenv('YA360_CLIENT_ID', CLIENT_ID)
CLIENT_SECRET = os.getenv('YA360_CLIENT_SECRET', CLIENT_SECRET)

# ===== ПЕРИОД ВЫБОРКИ =====
LOOKBACK_MINUTES = 15       # Глубина выборки назад: 15 / 60 / 1440 и т.д.
OVERLAP_MINUTES = 120       # Запас назад на задержку появления событий в аудит-логе
REQUEST_MARGIN_HOURS = 12   # Запас границ в самом запросе (страховка по таймзоне)

DATE_FROM = ''              # Точный период вместо LOOKBACK, формат ДД.ММ.ГГГГ
DATE_TO = ''                # Оба пустые — используется LOOKBACK_MINUTES

# ===== ЧТО ДЕЛАТЬ =====
ENABLE_AUTO_SUMMARIZATION = True   # True — включать, False — выключать
DRY_RUN = True                    # True — ничего не менять, только показать

ACCESS_LEVELS_FILTER = [           # Пустой список — все уровни доступа
    # 'PUBLIC',
    # 'ORGANIZATION',
    # 'ADMINS',
]

MANUAL_TASKS = [                   # Пары (ID встречи, UID организатора)
    # ('1234567890', '11300000123456789'),
]

# ===== ФАЙЛЫ =====
SCRIPT_NAME = Path(__file__).stem
RUN_TS = datetime.now().strftime('%Y%m%d_%H%M%S')
OUT_DIR = Path(__file__).parent / 'telemost_autosummary_out'
LOGS_DIR = OUT_DIR / 'logs'
REPORTS_DIR = OUT_DIR / 'reports'
LOG_FILE = LOGS_DIR / f'{SCRIPT_NAME}_{RUN_TS}.log'
CSV_FILE = REPORTS_DIR / f'{SCRIPT_NAME}_{RUN_TS}.csv'
STATE_FILE = Path(__file__).parent / 'telemost_autosummary_state.json'

ENABLE_LOGGING = True
ENABLE_CSV = True
CSV_DELIMITER = ';'        
STATE_TTL_DAYS = 14
RETENTION_DAYS = 30          # Удалять свои логи и отчёты старше N дней (0 — не удалять)
LOG_MAX_SIZE = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5

# ===== СЕТЬ =====
PAGE_SIZE = 100            
MAX_PAGES = 200
MAX_RETRIES = 5
RETRY_BASE_DELAY = 2         # секунды, удваивается на каждой попытке
REQUEST_TIMEOUT = (15, 90)
TOKEN_LIFETIME = 50 * 60     # сколько держим выданный токен в кеше
PATCH_DELAY = 0.3            # пауза между изменениями встреч
TOKEN_DELAY = 0.1            # пауза между обменами токенов

AUDIT_URL = f'https://cloud-api.yandex.net/v1/auditlog/organizations/{ORGID}/events'
TELEMOST_URL = 'https://cloud-api.yandex.net/v1/telemost-api/conferences/{conf_id}'
OAUTH_TOKEN_URL = 'https://oauth.yandex.ru/token'
EVENT_TYPE = 'telemost_conference.created'
DOMAIN_UID_THRESHOLD = 1130000000000000

RETRYABLE_STATUSES = (408, 429, 500, 502, 503, 504)
PERMANENT_STATUSES = (400, 402, 403, 404, 409, 410, 422)

logger = None
stats = {
    'found': 0,
    'applied': 0,
    'dry_run': 0,
    'skipped_state': 0,
    'skipped_filter': 0,
    'skipped_external': 0,
    'failed': 0,
    'tokens_issued': 0,
    'tokens_from_cache': 0,
    'tokens_failed': 0,
    'http_requests': 0,
}


# ============================================================================
# ЛОГИРОВАНИЕ
# ============================================================================
def setup_logging():
    global logger
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger('TelemostAutoSummary')
    logger.setLevel(logging.INFO)
    logger.propagate = False
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)

    fmt = logging.Formatter('%(asctime)s | %(levelname)-7s | %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    if ENABLE_LOGGING:
        try:
            file_handler = RotatingFileHandler(str(LOG_FILE), mode='a',
                                               maxBytes=LOG_MAX_SIZE,
                                               backupCount=LOG_BACKUP_COUNT,
                                               encoding='utf-8')
            file_handler.setFormatter(fmt)
            logger.addHandler(file_handler)
        except OSError as e:
            print(f'Не удалось создать лог-файл: {e}', file=sys.stderr)


def log_info(message):
    if logger:
        logger.info(message)
    else:
        print(f'[INFO] {message}')


def log_warning(message):
    if logger:
        logger.warning(message)
    else:
        print(f'[WARN] {message}')


def log_error(message):
    if logger:
        logger.error(message)
    else:
        print(f'[ERROR] {message}', file=sys.stderr)


def cleanup_old_files(days=RETENTION_DAYS):
    if days <= 0:
        return 0
    threshold = time.time() - days * 86400
    removed = 0
    for directory in (LOGS_DIR, REPORTS_DIR):
        if not directory.is_dir():
            continue
        for path in directory.iterdir():
            try:
                if (path.is_file() and path.name.startswith(SCRIPT_NAME)
                        and path.stat().st_mtime < threshold):
                    path.unlink()
                    removed += 1
            except OSError:
                pass
    return removed


# ============================================================================
# HTTP
# ============================================================================
def make_session():
    retries = Retry(total=3, backoff_factor=1,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=frozenset(['GET', 'POST', 'PATCH']))
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session


SESSION = make_session()


def http_request(method, url, token=None, params=None, json_body=None, form_data=None):
    '''Возвращает (данные, http_status, текст_ошибки). Ретраи при 5xx/429 и сетевых сбоях.'''
    headers = {'Accept': 'application/json'}
    if token:
        headers['Authorization'] = f'OAuth {token}'
    if json_body is not None:
        headers['Content-Type'] = 'application/json'

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            stats['http_requests'] += 1
            response = SESSION.request(method, url, headers=headers, params=params,
                                       json=json_body, data=form_data,
                                       timeout=REQUEST_TIMEOUT)
        except requests.exceptions.RequestException as e:
            if attempt >= MAX_RETRIES:
                return None, None, f'сетевая ошибка: {e}'
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
            log_warning(f'   Сетевая ошибка ({e}). Повтор через {delay} c '
                        f'[{attempt}/{MAX_RETRIES}]')
            time.sleep(delay)
            continue

        if 200 <= response.status_code < 300:
            if not response.content:
                return {}, response.status_code, None
            try:
                return response.json(), response.status_code, None
            except ValueError:
                return {}, response.status_code, None

        body = ' '.join(response.text.split())[:300]
        if response.status_code in RETRYABLE_STATUSES and attempt < MAX_RETRIES:
            delay = RETRY_BASE_DELAY * (2 ** (attempt - 1))
            log_warning(f'   HTTP {response.status_code}. Повтор через {delay} c '
                        f'[{attempt}/{MAX_RETRIES}]: {body}')
            time.sleep(delay)
            continue
        return None, response.status_code, body

    return None, None, 'исчерпаны попытки'


# ============================================================================
# ВРЕМЕННЫЕ ТОКЕНЫ СОТРУДНИКОВ (СЕРВИСНОЕ ПРИЛОЖЕНИЕ)
# ============================================================================
class TokenCache:
    '''
    Выдаёт и кеширует временные токены сотрудников через сервисное приложение.
    Обмен: grant_type=urn:ietf:params:oauth:grant-type:token-exchange,
           subject_token=<uid>, subject_token_type=urn:yandex:params:oauth:token-type:uid
    '''

    def __init__(self):
        self.tokens = {}    # uid -> (token, expires_at)
        self.failed = {}    # uid -> текст ошибки (чтобы не долбить обмен повторно)

    @staticmethod
    def is_domain_user(uid):
        try:
            return int(uid) > DOMAIN_UID_THRESHOLD
        except (TypeError, ValueError):
            return False

    def get(self, uid, user_login=''):
        '''Возвращает (token, источник, ошибка). Источник: 'кеш' | 'обмен'.'''
        uid = str(uid or '').strip()
        who = user_login or f'UID {uid}'

        if not uid or uid == '0':
            return None, None, 'в событии нет UID организатора'
        if not self.is_domain_user(uid):
            return None, None, (f'UID {uid} не доменный — временный токен '
                                f'через сервисное приложение недоступен')
        if uid in self.failed:
            return None, None, self.failed[uid]

        cached = self.tokens.get(uid)
        if cached and cached[1] > time.time():
            stats['tokens_from_cache'] += 1
            return cached[0], 'кеш', None

        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'subject_token': uid,
            'subject_token_type': 'urn:yandex:params:oauth:token-type:uid',
        }
        payload, status, error = http_request('POST', OAUTH_TOKEN_URL, form_data=data)
        time.sleep(TOKEN_DELAY)

        if payload is None:
            message = f'обмен токена не удался (HTTP {status}): {error}'
            self.failed[uid] = message
            stats['tokens_failed'] += 1
            log_error(f'   Не удалось получить токен для {who}: {message}')
            if status == 400:
                log_error('      Проверьте CLIENT_ID / CLIENT_SECRET и что сервисному '
                          'приложению выдано право telemost-api:conferences.update')
            return None, None, message

        token = payload.get('access_token')
        if not token:
            message = f'в ответе обмена нет access_token: {payload}'
            self.failed[uid] = message
            stats['tokens_failed'] += 1
            return None, None, message

        try:
            lifetime = min(int(payload.get('expires_in', TOKEN_LIFETIME)), TOKEN_LIFETIME)
        except (TypeError, ValueError):
            lifetime = TOKEN_LIFETIME
        self.tokens[uid] = (token, time.time() + max(60, lifetime - 60))
        stats['tokens_issued'] += 1
        log_info(f'   Получен временный токен для {who} (действует ~{lifetime // 60} мин)')
        return token, 'обмен', None


# ============================================================================
# ДАТЫ
# ============================================================================
def parse_date(date_str):
    if not date_str or not str(date_str).strip():
        return None
    for fmt in ('%d.%m.%Y', '%Y-%m-%d'):
        try:
            return datetime.strptime(str(date_str).strip(), fmt)
        except ValueError:
            continue
    log_error(f'Неверный формат даты "{date_str}". Используйте ДД.ММ.ГГГГ')
    return None


def get_period():
    '''
    Возвращает (request_from, request_to, cutoff_utc, описание).
    request_* уходят в запрос с запасом, cutoff_utc — точная левая граница для отбора.
    '''
    now_local = datetime.now()
    now_utc = datetime.now(timezone.utc)

    if DATE_FROM or DATE_TO:
        from_date = parse_date(DATE_FROM) or (now_local - timedelta(days=1))
        to_date = parse_date(DATE_TO) or now_local
        if DATE_FROM and parse_date(DATE_FROM) is None:
            return None, None, None, None
        if DATE_TO and parse_date(DATE_TO) is None:
            return None, None, None, None
        from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
        to_date = to_date.replace(hour=23, minute=59, second=59, microsecond=0)
        if from_date > to_date:
            log_error(f'Дата начала ({DATE_FROM}) позже даты окончания ({DATE_TO})')
            return None, None, None, None
        cutoff_utc = from_date.astimezone().astimezone(timezone.utc)
        description = f'период {DATE_FROM or "—"} .. {DATE_TO or "—"}'
    else:
        minutes = LOOKBACK_MINUTES + OVERLAP_MINUTES
        from_date = now_local - timedelta(minutes=minutes)
        to_date = now_local
        cutoff_utc = now_utc - timedelta(minutes=minutes)
        description = (f'последние {LOOKBACK_MINUTES} мин + запас {OVERLAP_MINUTES} мин '
                       f'на задержку аудит-лога')

    margin = timedelta(hours=REQUEST_MARGIN_HOURS)
    request_from = (from_date - margin).strftime('%Y-%m-%dT%H:%M:%SZ')
    request_to = (to_date + margin).strftime('%Y-%m-%dT%H:%M:%SZ')
    return request_from, request_to, cutoff_utc, description


def parse_occurred_at(value):
    if not value or not isinstance(value, str):
        return None
    text = value.strip().replace('Z', '+00:00')
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        try:
            dt = datetime.strptime(text[:19], '%Y-%m-%dT%H:%M:%S')
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def to_local_str(dt):
    return dt.astimezone().strftime('%Y-%m-%d %H:%M:%S') if dt else '—'


# ============================================================================
# АУДИТ-ЛОГ
# ============================================================================
def fetch_created_conferences(request_from, request_to, cutoff_utc):
    found = {}
    iteration_key = None
    seen_keys = set()
    page = 0
    total_items = 0
    newest = None

    log_info('Загрузка событий из аудит-лога...')
    while True:
        page += 1
        if page > MAX_PAGES:
            log_warning(f'   Достигнут лимит страниц ({MAX_PAGES}), останавливаюсь')
            break

        params = {
            'count': PAGE_SIZE,
            'types': EVENT_TYPE,
            'started_at': request_from,
            'ended_at': request_to,
        }
        if iteration_key:
            params['iteration_key'] = iteration_key

        data, status, error = http_request('GET', AUDIT_URL, token=AUDIT_TOKEN, params=params)
        if data is None:
            log_error(f'Ошибка чтения аудит-лога (HTTP {status}): {error}')
            if status in (401, 403):
                log_error('   Проверьте AUDIT_TOKEN, право ya360_security:read_auditlog '
                          'и тариф с поддержкой аудит-логов')
            elif status == 404:
                log_error('   Аудит-лог не найден — проверьте ORGID')
            break

        items = data.get('items') or []
        total_items += len(items)
        in_window = older = 0

        for item in items:
            event = item.get('event') or {}
            if event.get('type') and event['type'] != EVENT_TYPE:
                continue
            if str(event.get('status', '')).lower() not in ('', 'success'):
                continue
            meta = event.get('meta') or {}
            conf_id = meta.get('conference_id')
            if not conf_id:
                continue

            occurred = parse_occurred_at(event.get('occurred_at'))
            if occurred and (newest is None or occurred > newest):
                newest = occurred
            if occurred and occurred < cutoff_utc:
                older += 1
                continue

            in_window += 1
            conf_id = str(conf_id).strip()      # строка: бывают ведущие нули
            previous = found.get(conf_id)
            if previous is None or (occurred and previous['occurred']
                                    and occurred > previous['occurred']):
                found[conf_id] = {
                    'conference_id': conf_id,
                    'uid': str(event.get('uid') or ''),
                    'occurred': occurred,
                    'user_login': item.get('user_login', ''),
                    'user_name': item.get('user_name', ''),
                    'access_level': meta.get('conference_access_level', ''),
                    'service': event.get('service', ''),
                }

        log_info(f'   Страница {page}: получено {len(items)}, в периоде {in_window}, '
                 f'старше периода {older}')

        # События идут по убыванию времени: пошли старые — дальше смысла нет
        if older and in_window == 0:
            break
        iteration_key = data.get('iteration_key')
        if not iteration_key or not items:
            break
        iteration_key = str(iteration_key)
        if iteration_key in seen_keys:
            log_warning('   iteration_key повторился, останавливаю постраничный обход')
            break
        seen_keys.add(iteration_key)
        time.sleep(0.2)

    log_info(f'Всего событий от API: {total_items}')
    log_info(f'Встреч в выбранном периоде: {len(found)}')
    if newest:
        age_min = (datetime.now(timezone.utc) - newest).total_seconds() / 60
        log_info(f'Самое свежее событие в логе: {to_local_str(newest)} '
                 f'(возраст {age_min:.1f} мин)')
        if not found:
            log_info('   Похоже, свежие события ещё не попали в аудит-лог. '
                     'Следующий запуск их подхватит — за это отвечает OVERLAP_MINUTES')

    return sorted(found.values(),
                  key=lambda x: x['occurred'] or datetime.min.replace(tzinfo=timezone.utc))


# ============================================================================
# ИЗМЕНЕНИЕ ВСТРЕЧИ
# ============================================================================
def set_auto_summarization(conference_id, token, enabled):
    '''Возвращает (успех, join_url, http_status, ошибка).'''
    url = TELEMOST_URL.format(conf_id=conference_id)
    body = {'is_auto_summarization_enabled': bool(enabled)}
    data, status, error = http_request('PATCH', url, token=token, json_body=body)
    if data is None:
        return False, '', status, error
    return True, (data.get('join_url') or ''), status, None


# ============================================================================
# СОСТОЯНИЕ
# ============================================================================
def load_state():
    if not STATE_FILE.exists():
        return {}
    try:
        raw = json.loads(STATE_FILE.read_text(encoding='utf-8'))
        records = raw.get('conferences', {}) if isinstance(raw, dict) else {}
        threshold = datetime.now(timezone.utc) - timedelta(days=STATE_TTL_DAYS)
        cleaned = {}
        for key, value in records.items():
            if not isinstance(value, dict):
                continue
            stamp = parse_occurred_at(value.get('ts'))
            if stamp is None or stamp >= threshold:
                cleaned[str(key)] = value
        return cleaned
    except Exception as e:
        log_warning(f'Файл состояния не прочитан ({e}), начинаю с чистого списка')
        return {}


def save_state(state):
    try:
        payload = {'updated_at': datetime.now(timezone.utc).isoformat(),
                   'conferences': state}
        tmp = STATE_FILE.with_suffix('.tmp')
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=1), encoding='utf-8')
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        log_warning(f'Не удалось сохранить файл состояния: {e}')


# ============================================================================
# ОТЧЁТ
# ============================================================================
CSV_FIELDS = ['Время обработки', 'Результат', 'ID встречи', 'Автоконспектирование',
              'Создана', 'Организатор', 'Email организатора', 'UID организатора',
              'Уровень доступа', 'Источник токена', 'Ссылка для участников',
              'HTTP', 'Ошибка']


def save_csv(rows):
    if not ENABLE_CSV or not rows:
        return None
    try:
        with open(CSV_FILE, 'w', newline='', encoding='utf-8-sig') as fh:
            writer = csv.DictWriter(fh, fieldnames=CSV_FIELDS,
                                    delimiter=CSV_DELIMITER, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(rows)
        log_info(f'Отчёт сохранён: {CSV_FILE} (строк: {len(rows)})')
        return CSV_FILE
    except Exception as e:
        log_error(f'Ошибка при сохранении CSV: {e}')
        return None


# ============================================================================
# ОБРАБОТКА
# ============================================================================
def process(conferences, state, token_cache):
    rows = []
    levels = {str(x).upper() for x in ACCESS_LEVELS_FILTER if str(x).strip()}
    target = 'включено' if ENABLE_AUTO_SUMMARIZATION else 'выключено'
    total = len(conferences)

    for index, conf in enumerate(conferences, 1):
        conf_id = conf['conference_id']
        uid = conf.get('uid', '')
        who = conf.get('user_login') or (f'UID {uid}' if uid else '—')
        level = (conf.get('access_level') or '').upper()

        log_info(f'[{index}/{total}] Встреча {conf_id} | организатор {who} | '
                 f'создана {to_local_str(conf.get("occurred"))} | доступ {level or "—"}')

        if levels and level not in levels:
            stats['skipped_filter'] += 1
            log_info('   Пропуск: уровень доступа не входит в фильтр')
            continue

        record = state.get(conf_id)
        if record and record.get('status') in ('ok', 'permanent_error'):
            stats['skipped_state'] += 1
            log_info(f'   Пропуск: уже обработана ранее ({record.get("status")})')
            continue

        row = {
            'Время обработки': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'ID встречи': conf_id,
            'Автоконспектирование': target,
            'Создана': to_local_str(conf.get('occurred')),
            'Организатор': conf.get('user_name', ''),
            'Email организатора': conf.get('user_login', ''),
            'UID организатора': uid,
            'Уровень доступа': conf.get('access_level', ''),
            'Источник токена': '',
            'Ссылка для участников': '',
            'HTTP': '',
            'Ошибка': '',
        }

        if DRY_RUN:
            stats['dry_run'] += 1
            row['Результат'] = 'тест (без изменений)'
            rows.append(row)
            log_info(f'   [ТЕСТ] автоконспектирование было бы {target}')
            continue

        token, source, token_error = token_cache.get(uid, conf.get('user_login', ''))
        if not token:
            if 'не доменный' in (token_error or '') or 'нет UID' in (token_error or ''):
                stats['skipped_external'] += 1
                row['Результат'] = 'пропущено (внешний организатор)'
            else:
                stats['failed'] += 1
                row['Результат'] = 'ошибка получения токена'
            row['Ошибка'] = token_error or ''
            rows.append(row)
            log_warning(f'   {row["Результат"]}: {token_error}')
            continue

        row['Источник токена'] = source or ''
        ok, join_url, status, error = set_auto_summarization(
            conf_id, token, ENABLE_AUTO_SUMMARIZATION)
        row['HTTP'] = status or ''

        if ok:
            stats['applied'] += 1
            row['Результат'] = 'применено'
            row['Ссылка для участников'] = join_url
            state[conf_id] = {'status': 'ok',
                              'ts': datetime.now(timezone.utc).isoformat(),
                              'uid': uid}
            log_info(f'   Готово: автоконспектирование {target}'
                     f'{f" | {join_url}" if join_url else ""}')
        else:
            stats['failed'] += 1
            row['Результат'] = 'ошибка'
            row['Ошибка'] = error or ''
            if status in PERMANENT_STATUSES:
                state[conf_id] = {'status': 'permanent_error',
                                  'ts': datetime.now(timezone.utc).isoformat(),
                                  'error': f'HTTP {status}'}
                log_error(f'   HTTP {status} — повторов не будет: {error}')
                if status == 403:
                    log_error('      Токен организатора получен, но доступа к встрече нет. '
                              'Проверьте, что сервисному приложению выдано право '
                              'telemost-api:conferences.update')
                elif status == 404:
                    log_error('      Встреча не найдена (возможно, уже удалена)')
            else:
                log_error(f'   HTTP {status} — повторю при следующем запуске: {error}')

        rows.append(row)
        if index < total and PATCH_DELAY:
            time.sleep(PATCH_DELAY)

    return rows


# ============================================================================
# MAIN
# ============================================================================
def main():
    started = datetime.now()
    setup_logging()

    log_info('=' * 90)
    log_info('АВТОКОНСПЕКТИРОВАНИЕ ВСТРЕЧ ЯНДЕКС ТЕЛЕМОСТ')
    log_info('=' * 90)
    log_info(f'Время запуска: {started.strftime("%Y-%m-%d %H:%M:%S")}')
    log_info(f'Папка результатов: {OUT_DIR}')
    if ENABLE_LOGGING:
        log_info(f'Лог-файл: {LOG_FILE.name}')

    missing = [name for name, value in (('ORGID', ORGID), ('AUDIT_TOKEN', AUDIT_TOKEN),
                                        ('CLIENT_ID', CLIENT_ID),
                                        ('CLIENT_SECRET', CLIENT_SECRET)) if not value]
    if missing and not (MANUAL_TASKS and 'ORGID' not in missing):
        log_error(f'Не заполнены обязательные параметры: {", ".join(missing)}')
        return 2

    removed = cleanup_old_files()
    if removed:
        log_info(f'Удалено старых файлов: {removed}')

    state = load_state()
    token_cache = TokenCache()
    log_info(f'В файле состояния записей: {len(state)}')
    log_info(f'Целевое значение: автоконспектирование '
             f'{"ВКЛЮЧИТЬ" if ENABLE_AUTO_SUMMARIZATION else "ВЫКЛЮЧИТЬ"}'
             f'{" | РЕЖИМ ТЕСТА (ничего не меняем)" if DRY_RUN else ""}')
    log_info('Изменение встреч выполняется от лица организаторов '
             '(временные токены сервисного приложения)')

    if MANUAL_TASKS:
        log_info(f'Ручной режим: {len(MANUAL_TASKS)} встреч(и), аудит-лог не читаем')
        conferences = []
        for entry in MANUAL_TASKS:
            if isinstance(entry, (list, tuple)) and len(entry) >= 2:
                conf_id, uid = str(entry[0]).strip(), str(entry[1]).strip()
            else:
                log_warning(f'   Пропуск некорректной записи MANUAL_TASKS: {entry!r} '
                            f'(нужна пара (ID встречи, UID организатора))')
                continue
            conferences.append({'conference_id': conf_id, 'uid': uid, 'occurred': None,
                                'user_login': '', 'user_name': '', 'access_level': '',
                                'service': ''})
    else:
        request_from, request_to, cutoff_utc, description = get_period()
        if request_from is None:
            log_error('Не удалось определить период выборки')
            return 2
        log_info(f'Период: {description}')
        log_info(f'   Отбираю события начиная с: {to_local_str(cutoff_utc)} (локальное время)')
        log_info(f'   В запрос уходит с запасом ±{REQUEST_MARGIN_HOURS} ч: '
                 f'{request_from} .. {request_to}')
        log_info(f'Тип события: {EVENT_TYPE}')
        if ACCESS_LEVELS_FILTER:
            log_info(f'Фильтр по уровню доступа: {", ".join(ACCESS_LEVELS_FILTER)}')
        conferences = fetch_created_conferences(request_from, request_to, cutoff_utc)

    stats['found'] = len(conferences)
    if not conferences:
        log_info('Встреч для обработки не найдено')
        log_info('   Возможные причины: в этот период встречи не создавались; '
                 'событие ещё не попало в аудит-лог (увеличьте OVERLAP_MINUTES); '
                 'все встречи уже обработаны ранее')
        save_state(state)
        return 0

    log_info('-' * 90)
    log_info(f'ОБРАБОТКА ВСТРЕЧ: {len(conferences)}')
    log_info('-' * 90)
    rows = process(conferences, state, token_cache)
    save_state(state)
    saved_csv = save_csv(rows)

    finished = datetime.now()
    log_info('=' * 90)
    log_info('ИТОГИ')
    log_info('=' * 90)
    log_info(f'Найдено встреч:                 {stats["found"]}')
    log_info(f'Настройка применена:            {stats["applied"]}')
    if DRY_RUN:
        log_info(f'Показано в режиме теста:        {stats["dry_run"]}')
    log_info(f'Пропущено (уже обработано):     {stats["skipped_state"]}')
    log_info(f'Пропущено (фильтр доступа):     {stats["skipped_filter"]}')
    log_info(f'Пропущено (внешний организатор):{stats["skipped_external"]}')
    log_info(f'Ошибок:                         {stats["failed"]}')
    log_info('-' * 90)
    log_info(f'Токенов выдано сервисным приложением: {stats["tokens_issued"]}')
    log_info(f'Токенов взято из кеша:               {stats["tokens_from_cache"]}')
    log_info(f'Не удалось получить токен:           {stats["tokens_failed"]}')
    log_info(f'HTTP-запросов всего:                 {stats["http_requests"]}')
    log_info('-' * 90)
    log_info(f'Завершено: {finished.strftime("%Y-%m-%d %H:%M:%S")} '
             f'(за {(finished - started).total_seconds():.1f} c)')
    if saved_csv:
        log_info(f'Отчёт: {saved_csv}')
    if ENABLE_LOGGING:
        log_info(f'Лог: {LOG_FILE}')
    log_info('=' * 90)

    return 1 if stats['failed'] else 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log_warning('Прервано пользователем (Ctrl+C)')
        sys.exit(130)
    except Exception as exc:  # noqa: BLE001
        log_error(f'Критическая ошибка: {exc}')
        import traceback
        log_error(traceback.format_exc())
        sys.exit(1)
