#!/usr/bin/env python3
"""
Скрипт ищет файлы и папки на Яндекс Дисках сотрудников организации,
которые расшарены вовне.

Для запуска необходимы библиотеки requests и urllib3.

pip install requests urllib3

ПРЕДВАРИТЕЛЬНАЯ НАСТРОЙКА

Перед запуском заполните переменные в начале скрипта, блок "Настройки".

Переменная                  Описание
ORGID                       ID организации Яндекс 360
ADMIN_TOKEN                 OAuth-токен администратора организации

CLIENT_ID                   ID сервисного OAuth-приложения для token exchange
CLIENT_SECRET               Секрет сервисного OAuth-приложения для token exchange

USE_UID_LIST                True — обрабатывать только UID из UID_LIST_FILE
                            False — обрабатывать всю организацию

UID_LIST_FILE               CSV/TXT-файл со списком UID для обработки.
                            Используется, если USE_UID_LIST = True

EXCLUDE_LIST_FILE           CSV/TXT-файл со списком UID для исключения.
                            Используется, если USE_UID_LIST = False.
                            Если пустой — исключения не применяются

DO_FALLBACK                 True — при отказе admin public-resources запускать
                            рекурсивный обход Диска пользователя
                            False — не запускать fallback, а писать пользователя
                            в failed_users CSV

ALL_SHARE                   True — искать доступы "для всех" через macros all
PERSONAL_SHARE              True — искать персональные доступы на внешних пользователей

SCAN_ONLY_DOMAIN_UIDS       True — обрабатывать только UID >= UID_THRESHOLD
UID_THRESHOLD               Граница доменных UID. По умолчанию 1130000000000000

PERPAGE                     Размер страницы Directory API
RESOURCE_LIMIT              Размер страницы admin public-resources.
                            Рекомендуется 10 для снижения вероятности 500

FALLBACK_DIR_LIMIT          Размер страницы при рекурсивном обходе папок
WORKERS                     Количество параллельных потоков обработки сотрудников

RPS_LIMIT                   Общий глобальный RPS для обычных API-запросов
ADMIN_RPS_LIMIT             Отдельный RPS для admin public-resources.
                            По умолчанию 5 запросов/сек

ADMIN_REQUEST_TIMEOUT       Таймаут ожидания ответа admin public-resources.
                            По умолчанию 300 секунд = 5 минут

PUBLIC_RESOURCES_MAX_ATTEMPTS
                            Количество попыток admin public-resources при 500

PUBLIC_SETTINGS_MAX_ATTEMPTS
                            Количество попыток получения public-settings.
                            По умолчанию 3

БЫСТРЫЙ СТАРТ

1. Заполните обязательные параметры:

    ORGID = '...'
    ADMIN_TOKEN = '...'

2. Для работы fallback дополнительно заполните:

    CLIENT_ID = '...'
    CLIENT_SECRET = '...'

3. Запустите:

    python external_share_scanner.py

или:

    python3 external_share_scanner.py

РЕЖИМ 1. ОБРАБОТКА ВСЕЙ ОРГАНИЗАЦИИ

Настройка:

    USE_UID_LIST = False
    EXCLUDE_LIST_FILE = ''

РЕЖИМ 2. ОБРАБОТКА ВСЕЙ ОРГАНИЗАЦИИ С ИСКЛЮЧЕНИЯМИ

Настройка:

    USE_UID_LIST = False
    EXCLUDE_LIST_FILE = 'exclude_uids.csv'

Формат exclude_uids.csv:

    1130000069104397
    1130000064999397
    1130000065108028

Также поддерживаются:

- UTF-8 с BOM
- пустые строки
- строки-комментарии, начинающиеся с #

Пример:

    # тестовые пользователи
    1130000069104397
    1130000064999397

РЕЖИМ 3. ОБРАБОТКА ТОЛЬКО UID ИЗ CSV

Настройка:

    USE_UID_LIST = True
    UID_LIST_FILE = 'uid_list.csv'

ВЫХОДНЫЕ ФАЙЛЫ

При запуске создаётся папка:

    <имя_скрипта>_YYYY-MM-DD_HH-MM-SS/

Внутри сохраняются:

1. external_shared_resources_YYYY-MM-DD_HH-MM-SS.csv

Основной отчёт по внешним шарам.

Поля:

    share_reason
    email
    uid
    source_api
    type
    name
    path
    created_at
    modified_at
    public_key
    public_url
    available_until
    access_type
    access_id
    access_rights
    access_org_id
    access_macros

2. unreachable_resources_YYYY-MM-DD_HH-MM-SS.csv

Ресурсы, для которых не удалось получить public-settings.

Поля:

    email
    uid
    source_api
    type
    name
    path
    created_at
    modified_at
    public_key
    public_url
    last_status_code
    last_error

3. failed_users_YYYY-MM-DD_HH-MM-SS.csv

Сотрудники, для которых admin public-resources отказал,
и fallback был отключён, недоступен или был использован.

Поля:

    email
    uid
    failed_at_offset
    last_status_code
    last_error
    resources_before_fail
    fallback_attempted
    fallback_result

4. Лог-файл:

    <имя_скрипта>_YYYY-MM-DD_HH-MM-SS.log



ПРИМЕРЫ ТИПОВЫХ СЦЕНАРИЕВ

1. Вся организация, fallback включён

    USE_UID_LIST = False
    EXCLUDE_LIST_FILE = ''
    DO_FALLBACK = True

2. Вся организация, но исключить тестовых пользователей

    USE_UID_LIST = False
    EXCLUDE_LIST_FILE = 'exclude_uids.csv'
    DO_FALLBACK = True

3. Только конкретные UID из файла

    USE_UID_LIST = True
    UID_LIST_FILE = 'uid_list.csv'
    DO_FALLBACK = True

4. Только конкретные UID без fallback

    USE_UID_LIST = True
    UID_LIST_FILE = 'uid_list.csv'
    DO_FALLBACK = False

5. Поиск только публичных доступов "для всех"

    ALL_SHARE = True
    PERSONAL_SHARE = False

6. Поиск только персональных доступов внешним пользователям

    ALL_SHARE = False
    PERSONAL_SHARE = True

7. Максимально осторожный режим для тяжёлых Дисков

    RESOURCE_LIMIT = 10
    ADMIN_RPS_LIMIT = 5
    ADMIN_REQUEST_TIMEOUT = 300
    WORKERS = 3

СВОДНАЯ ТАБЛИЦА ЗНАЧЕНИЙ ПО УМОЛЧАНИЮ

Параметр                         Значение по умолчанию
USE_UID_LIST                     False
UID_LIST_FILE                    uid_list.csv
EXCLUDE_LIST_FILE                ''
DO_FALLBACK                      True
ALL_SHARE                        True
PERSONAL_SHARE                   True
SCAN_ONLY_DOMAIN_UIDS            True
UID_THRESHOLD                    1130000000000000
PERPAGE                          1000
RESOURCE_LIMIT                   10
FALLBACK_DIR_LIMIT               1000
WORKERS                          3
RPS_LIMIT                        39
ADMIN_RPS_LIMIT                  5
ADMIN_REQUEST_TIMEOUT            300
PUBLIC_RESOURCES_MAX_ATTEMPTS    6
PUBLIC_SETTINGS_MAX_ATTEMPTS     3
TOKEN_LIFETIME                   50 минут
CSV encoding                     utf-8-sig

"""

import os
import sys
import csv
import time
import logging
import threading
import collections
import urllib.parse
import gc
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from requests.adapters import HTTPAdapter, Retry

# ━━━━━━━━━━━━━━━━━━ Настройки ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ORGID = ''
ADMIN_TOKEN = ''

# Сервисное приложение для token exchange (cloud_api:disk.info)
CLIENT_ID = ''  # ID сервисного приложения
CLIENT_SECRET = ''  # Secret сервисного приложения

# ── Режимы работы ──────────────────────────────────────────
USE_UID_LIST = False                # True – только UID из файла
UID_LIST_FILE = 'uid_list.csv'      # список UID (одна колонка, без заголовка)
EXCLUDE_LIST_FILE = ''              # исключить эти UID (только при USE_UID_LIST=False)

DO_FALLBACK = False                  # True – включать рекурсивный обход диска при 500
                                    # False – просто писать в failed_users

# ── Параметры API ──────────────────────────────────────────
PERPAGE = 1000
RESOURCE_LIMIT = 10                 # admin public-resources (маленький лимит)
FALLBACK_DIR_LIMIT = 1000           # при обходе папки в fallback
WORKERS = 5                         # параллельных потоков обработки пользователей
RPS_LIMIT = 30                      # общий RPS (directory, settings, disk resources)
ADMIN_RPS_LIMIT = 5                 # отдельный RPS для admin public-resources
ADMIN_REQUEST_TIMEOUT = 300         # таймаут для admin public-resources (5 мин)
LOG_MAX_BYTES = 20 * 1024 * 1024

UID_THRESHOLD = 1130000000000000

ALL_SHARE = True                    # искать доступ "всем"
PERSONAL_SHARE = True               # искать доступ не-сотрудникам
SCAN_ONLY_DOMAIN_UIDS = True        # только uid >= UID_THRESHOLD

# Retry для admin public-resources при 500
PUBLIC_RESOURCES_MAX_ATTEMPTS = 6
PUBLIC_RESOURCES_INITIAL_SLEEP = 3.0
PUBLIC_RESOURCES_MAX_SLEEP = 60.0

# Retry для public-settings
PUBLIC_SETTINGS_MAX_ATTEMPTS = 3
PUBLIC_SETTINGS_RETRY_SLEEP = 1.0

# Token exchange
TOKEN_LIFETIME = 50 * 60            # 50 минут

# Внешний retry (сетевые ошибки)
OUTER_RETRY_INITIAL_WAIT = 15
OUTER_RETRY_MAX_WAIT = 300

# GC
GC_INTERVAL_FILES = 500
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

SCRIPT_NAME = os.path.splitext(os.path.basename(__file__))[0]
RUN_TS = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
OUTPUT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    f'{SCRIPT_NAME}_{RUN_TS}',
)
os.makedirs(OUTPUT_DIR, exist_ok=True)

FALLBACK_AVAILABLE = bool(CLIENT_ID and CLIENT_SECRET)   # техническая возможность

# ═══════════════════════════════════════════════════════════════
#  Логирование
# ═══════════════════════════════════════════════════════════════
class IncrementalRotatingHandler(logging.Handler):
    def __init__(self, base_path, max_bytes=20*1024*1024, encoding='utf-8'):
        super().__init__()
        self.base_path = base_path
        self.max_bytes = max_bytes
        self.encoding = encoding
        self._current_size = (
            os.path.getsize(base_path) if os.path.exists(base_path) else 0
        )
        self._stream = open(base_path, 'a', encoding=encoding)

    def _backup_path(self, n):
        name, ext = os.path.splitext(self.base_path)
        return f'{name}({n}){ext}'

    def _max_backup_index(self):
        d = os.path.dirname(self.base_path) or '.'
        name, ext = os.path.splitext(os.path.basename(self.base_path))
        prefix, suffix = f'{name}(', f'){ext}'
        mx = 0
        for f in os.listdir(d):
            if f.startswith(prefix) and f.endswith(suffix):
                try:
                    mx = max(mx, int(f[len(prefix):-len(suffix)]))
                except ValueError:
                    pass
        return mx

    def _do_rollover(self):
        self._stream.flush()
        self._stream.close()
        mx = self._max_backup_index()
        for i in range(mx, 0, -1):
            os.rename(self._backup_path(i), self._backup_path(i + 1))
        os.rename(self.base_path, self._backup_path(1))
        self._stream = open(self.base_path, 'w', encoding=self.encoding)
        self._current_size = 0

    def emit(self, record):
        try:
            msg = self.format(record) + '\n'
            byte_len = len(msg.encode(self.encoding, errors='replace'))
            if self._current_size + byte_len > self.max_bytes:
                self._do_rollover()
            self._stream.write(msg)
            self._stream.flush()
            self._current_size += byte_len
        except Exception:
            self.handleError(record)

    def close(self):
        self.acquire()
        try:
            self._stream.flush()
            self._stream.close()
        finally:
            self.release()
        super().close()

_log_path = os.path.join(OUTPUT_DIR, f'{SCRIPT_NAME}_{RUN_TS}.log')
_file_h = IncrementalRotatingHandler(_log_path, max_bytes=LOG_MAX_BYTES)
_file_h.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)-7s | %(threadName)-12s | %(message)s'))
_console_h = logging.StreamHandler(sys.stdout)
_console_h.setFormatter(logging.Formatter(
    '%(asctime)s | %(levelname)-7s | %(message)s'))

logger = logging.getLogger('disk_scanner')
logger.setLevel(logging.DEBUG)
logger.addHandler(_file_h)
logger.addHandler(_console_h)

# ═══════════════════════════════════════════════════════════════
#  Rate limiters
# ═══════════════════════════════════════════════════════════════
class SlidingWindowRateLimiter:
    def __init__(self, max_calls, period=1.0):
        self.max_calls = max_calls
        self.period = period
        self._lock = threading.Lock()
        self._ts = collections.deque()

    def acquire(self):
        while True:
            with self._lock:
                now = time.monotonic()
                while self._ts and self._ts[0] <= now - self.period:
                    self._ts.popleft()
                if len(self._ts) < self.max_calls:
                    self._ts.append(now)
                    return
                wait = self._ts[0] + self.period - now
            time.sleep(max(0.0, wait))

RATE = SlidingWindowRateLimiter(RPS_LIMIT)
RATE_ADMIN = SlidingWindowRateLimiter(ADMIN_RPS_LIMIT)

# ═══════════════════════════════════════════════════════════════
#  HTTP
# ═══════════════════════════════════════════════════════════════
_tls = threading.local()

def _new_session():
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 502, 503, 504],
        allowed_methods=frozenset(['GET', 'POST']),
        raise_on_status=False,
    )
    s = requests.Session()
    s.mount('https://', HTTPAdapter(max_retries=retries))
    return s

def _session():
    if not hasattr(_tls, 's'):
        _tls.s = _new_session()
    return _tls.s

def _reset_session():
    if hasattr(_tls, 's'):
        try:
            _tls.s.close()
        except Exception:
            pass
        del _tls.s

def api_request(method, url, timeout=60, **kwargs):
    """Выполнить HTTP-запрос с retry и выбранным rate-limiter."""
    outer_attempt = 0
    while True:
        # Выбираем лимитер
        if 'public-resources' in url:
            RATE_ADMIN.acquire()
        else:
            RATE.acquire()
        try:
            return _session().request(method, url, timeout=timeout, **kwargs)
        except (requests.exceptions.RetryError,
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout) as exc:
            outer_attempt += 1
            wait = min(
                OUTER_RETRY_INITIAL_WAIT * (2 ** (outer_attempt - 1)),
                OUTER_RETRY_MAX_WAIT,
            )
            logger.warning(
                f'api_request outer-retry #{outer_attempt}: '
                f'{type(exc).__name__}. Жду {wait}s … {url[:120]}')
            _reset_session()
            time.sleep(wait)

def api_get(url, timeout=60, **kwargs):
    return api_request('GET', url, timeout=timeout, **kwargs)

def api_post(url, timeout=60, **kwargs):
    return api_request('POST', url, timeout=timeout, **kwargs)

# ═══════════════════════════════════════════════════════════════
#  CSV writers
# ═══════════════════════════════════════════════════════════════
class SafeCSVWriter:
    def __init__(self, path, fields):
        self.fields = fields
        self._file = open(path, 'w', newline='',
                          encoding='utf-8-sig', buffering=1)
        self._writer = csv.DictWriter(
            self._file, self.fields,
            extrasaction='ignore', delimiter=';')
        self._lock = threading.Lock()
        self._writer.writeheader()
        self._file.flush()

    def writerow(self, row):
        with self._lock:
            self._writer.writerow(row)

    def close(self):
        self._file.flush()
        self._file.close()

SHARED_FIELDS = [
    'share_reason', 'email', 'uid', 'source_api',
    'type', 'name', 'path',
    'created_at', 'modified_at', 'public_key', 'public_url',
    'available_until',
    'access_type', 'access_id', 'access_rights',
    'access_org_id', 'access_macros',
]

UNREACHABLE_FIELDS = [
    'email', 'uid', 'source_api',
    'type', 'name', 'path',
    'created_at', 'modified_at', 'public_key', 'public_url',
    'last_status_code', 'last_error',
]

FAILED_USERS_FIELDS = [
    'email', 'uid', 'failed_at_offset',
    'last_status_code', 'last_error',
    'resources_before_fail', 'fallback_attempted', 'fallback_result',
]

# ═══════════════════════════════════════════════════════════════
#  Вспомогательные функции
# ═══════════════════════════════════════════════════════════════
def load_uids_from_file(filepath):
    """Читает UID из CSV или txt (одна колонка, без заголовка)."""
    uids = set()
    if not os.path.exists(filepath):
        logger.error(f'Файл не найден: {filepath}')
        return uids
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            # убираем возможные кавычки и пробелы
            uid = line.strip('"\' ')
            if uid:
                uids.add(uid)
    logger.info(f'Загружено {len(uids)} UID из {filepath}')
    return uids

def parse_uid(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None

def build_org_uid_sets(users):
    org_uids_low = set()
    org_uids_high = set()
    for user in users:
        uid_int = parse_uid(user.get('id'))
        if uid_int is None:
            continue
        if uid_int < UID_THRESHOLD:
            org_uids_low.add(uid_int)
        else:
            org_uids_high.add(uid_int)
    return org_uids_low, org_uids_high

def is_org_user_uid(uid_int, org_uids_low, org_uids_high):
    if uid_int < UID_THRESHOLD:
        return uid_int in org_uids_low
    return uid_int in org_uids_high

def resolve_public_url(public_key):
    if not public_key:
        return ''
    encoded_key = urllib.parse.quote(public_key, safe='')
    return f'https://disk.yandex.ru/public/?hash={encoded_key}'

# ═══════════════════════════════════════════════════════════════
#  Token exchange
# ═══════════════════════════════════════════════════════════════
class UserTokenManager:
    def __init__(self):
        self._cache = {}
        self._lock = threading.Lock()

    def get_token(self, uid):
        uid_str = str(uid)
        now = time.time()
        with self._lock:
            cached = self._cache.get(uid_str)
            if cached and (now - cached[1]) < TOKEN_LIFETIME:
                return cached[0]

        token = self._exchange(uid_str)
        if token:
            with self._lock:
                self._cache[uid_str] = (token, time.time())
        return token

    @staticmethod
    def _exchange(uid_str):
        resp = api_post(
            'https://oauth.yandex.ru/token',
            data={
                'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
                'client_id': CLIENT_ID,
                'client_secret': CLIENT_SECRET,
                'subject_token': uid_str,
                'subject_token_type': 'urn:yandex:params:oauth:token-type:uid',
            },
            headers={'Content-Type': 'application/x-www-form-urlencoded'},
        )
        if resp.status_code == 200:
            try:
                data = resp.json()
                token = data.get('access_token')
                if token:
                    logger.debug(f'token-exchange OK uid={uid_str}')
                    return token
            except Exception as exc:
                logger.warning(f'token-exchange parse error uid={uid_str}: {exc}')
        else:
            logger.warning(f'token-exchange FAIL uid={uid_str}: {resp.status_code} {resp.text[:200]}')
        return None

USER_TOKEN_MGR = UserTokenManager()

# ═══════════════════════════════════════════════════════════════
#  API-функции
# ═══════════════════════════════════════════════════════════════
AUTH_HEADER = {'Authorization': f'OAuth {ADMIN_TOKEN}'}

def get_users(page):
    resp = api_get(
        f'https://api360.yandex.net/directory/v1/org/{ORGID}/users',
        params={'page': page, 'perPage': PERPAGE},
        headers=AUTH_HEADER,
    )
    logger.info(f'get_users page={page} → {resp.status_code}')
    if resp.status_code != 200:
        logger.error(f'get_users page={page}: {resp.status_code} {resp.text}')
        return 0, []
    data = resp.json()
    return data.get('pages', 0), data.get('users', [])

# ── Admin public-resources с увеличенным таймаутом ─────────
def get_public_resources_admin(user_id, offset=0):
    last_status = None
    for attempt in range(1, PUBLIC_RESOURCES_MAX_ATTEMPTS + 1):
        resp = api_get(
            'https://cloud-api.yandex.net/v1/disk/public/resources/admin/public-resources',
            params={
                'user_id': user_id,
                'org_id': ORGID,
                'limit': RESOURCE_LIMIT,
                'offset': offset,
            },
            headers=AUTH_HEADER,
            timeout=ADMIN_REQUEST_TIMEOUT,
        )
        last_status = resp.status_code
        logger.debug(
            f'admin public-resources uid={user_id} off={offset} '
            f'attempt={attempt}/{PUBLIC_RESOURCES_MAX_ATTEMPTS} → {resp.status_code}')
        if resp.status_code == 200:
            return resp.json(), False
        if resp.status_code == 500:
            sleep_time = min(
                PUBLIC_RESOURCES_INITIAL_SLEEP * (2 ** (attempt - 1)),
                PUBLIC_RESOURCES_MAX_SLEEP,
            )
            logger.warning(
                f'admin public-resources uid={user_id} off={offset} '
                f'attempt={attempt}: 500, жду {sleep_time:.0f}s')
            if attempt < PUBLIC_RESOURCES_MAX_ATTEMPTS:
                time.sleep(sleep_time)
                continue
            return None, True
        logger.warning(
            f'admin public-resources uid={user_id} off={offset}: '
            f'{resp.status_code} {resp.text[:200]}')
        return None, False
    return None, True

# ── Fallback: рекурсивный обход диска ──────────────────────
def disk_get_resources(user_token, path, limit=1000, offset=0):
    resp = api_get(
        'https://cloud-api.yandex.net/v1/disk/resources',
        params={
            'path': path,
            'limit': limit,
            'offset': offset,
            'fields': (
                'path,type,name,created,modified,'
                'public_key,public_url,'
                '_embedded.total,'
                '_embedded.items.path,'
                '_embedded.items.type,'
                '_embedded.items.name,'
                '_embedded.items.created,'
                '_embedded.items.modified,'
                '_embedded.items.public_key,'
                '_embedded.items.public_url'
            ),
        },
        headers={'Authorization': f'OAuth {user_token}'},
    )
    if resp.status_code == 200:
        return resp.json()
    logger.warning(f'disk_get_resources {path}: {resp.status_code} {resp.text[:200]}')
    return None

def iter_shared_resources_fallback(user_id):
    user_token = USER_TOKEN_MGR.get_token(user_id)
    if not user_token:
        logger.warning(f'fallback: нет user token для uid={user_id}')
        return
    logger.info(f'fallback: рекурсивный обход диска uid={user_id}')
    dir_stack = [('disk:/', 0)]
    visited = set()
    yielded_count = 0
    dirs_visited = 0
    while dir_stack:
        current_path, depth = dir_stack.pop()
        if current_path in visited:
            continue
        visited.add(current_path)
        dirs_visited += 1
        fresh_token = USER_TOKEN_MGR.get_token(user_id)
        if not fresh_token:
            logger.warning(f'fallback: потеря токена uid={user_id} на {current_path}')
            return
        offset = 0
        while True:
            data = disk_get_resources(fresh_token, current_path,
                                      limit=FALLBACK_DIR_LIMIT, offset=offset)
            if data is None:
                break
            # сама папка может быть расшарена
            if offset == 0:
                folder_pk = data.get('public_key', '')
                if folder_pk and current_path != 'disk:/':
                    yield {
                        'public_key': folder_pk,
                        'public_url': data.get('public_url', '') or '',
                        'name': data.get('name', ''),
                        'path': data.get('path', current_path),
                        'type': 'dir',
                        'created_at': data.get('created', ''),
                        'modified_at': data.get('modified', ''),
                        '_source': 'fallback',
                    }
                    yielded_count += 1
            emb = data.get('_embedded', {})
            items = emb.get('items', [])
            total = emb.get('total', 0)
            for item in items:
                item_pk = item.get('public_key', '')
                if item_pk:
                    yield {
                        'public_key': item_pk,
                        'public_url': item.get('public_url', '') or '',
                        'name': item.get('name', ''),
                        'path': item.get('path', ''),
                        'type': item.get('type', ''),
                        'created_at': item.get('created', ''),
                        'modified_at': item.get('modified', ''),
                        '_source': 'fallback',
                    }
                    yielded_count += 1
                if item.get('type') == 'dir' and item.get('path') not in visited:
                    dir_stack.append((item.get('path'), depth + 1))
            if len(items) < FALLBACK_DIR_LIMIT or offset + FALLBACK_DIR_LIMIT >= total:
                break
            offset += FALLBACK_DIR_LIMIT
        if dirs_visited % 100 == 0:
            gc.collect()
            logger.debug(
                f'fallback uid={user_id}: dirs={dirs_visited}, '
                f'shared_found={yielded_count}, stack={len(dir_stack)}')
    logger.info(
        f'fallback uid={user_id}: завершён, dirs={dirs_visited}, shared={yielded_count}')

# ── Нормализация ────────────────────────────────────────────
def normalize_admin_item(item):
    public_key = item.get('public_hash', '') or item.get('public_key', '') or ''
    return {
        'public_key': public_key,
        'public_url': item.get('public_url', '') or '',
        'name': item.get('name', ''),
        'path': item.get('path', ''),
        'type': item.get('type', ''),
        'created_at': item.get('created_at', '') or item.get('created', '') or '',
        'modified_at': item.get('modified_at', '') or item.get('modified', '') or '',
        '_source': 'admin',
    }

# ── Итератор ресурсов с fallback (опционально) ─────────────
def iter_resources(user_id, failed_users_csv_w=None, email=''):
    seen_paths = set()
    offset = 0
    admin_failed = False
    total_before_fail = 0

    # Фаза 1: Admin API
    while not admin_failed:
        data, failed = get_public_resources_admin(user_id, offset)
        if data is None:
            if failed:
                admin_failed = True
                logger.warning(f'Admin API отказал uid={user_id} off={offset}, ресурсов до сбоя: {total_before_fail}')
                break
            else:
                return
        items = data.get('items', [])
        for raw_item in items:
            norm = normalize_admin_item(raw_item)
            path = norm.get('path', '')
            if path:
                seen_paths.add(path)
            total_before_fail += 1
            yield norm
        if len(items) < RESOURCE_LIMIT:
            return
        offset += RESOURCE_LIMIT

    # Фаза 2: Fallback (если разрешён и технически возможен)
    fallback_attempted = False
    fallback_count = 0

    if DO_FALLBACK and FALLBACK_AVAILABLE:
        fallback_attempted = True
        logger.info(f'Fallback: uid={user_id}, рекурсивный обход диска')
        for norm_item in iter_shared_resources_fallback(user_id):
            path = norm_item.get('path', '')
            if path and path in seen_paths:
                continue
            if path:
                seen_paths.add(path)
            fallback_count += 1
            yield norm_item
        logger.info(f'Fallback uid={user_id}: новых расшаренных ресурсов: {fallback_count}')
    else:
        logger.warning(f'Fallback отключён или недоступен для uid={user_id}')

    # Пишем в failed_users
    if failed_users_csv_w:
        failed_users_csv_w.writerow({
            'email': email,
            'uid': user_id,
            'failed_at_offset': str(offset),
            'last_status_code': '500',
            'last_error': 'Admin API 500',
            'resources_before_fail': str(total_before_fail),
            'fallback_attempted': str(fallback_attempted).lower(),
            'fallback_result': f'found {fallback_count} new' if fallback_attempted else 'not attempted',
        })

# ── public-settings ─────────────────────────────────────────
def get_public_settings_limited(public_key):
    last_status_code = None
    last_error = ''
    for attempt in range(1, PUBLIC_SETTINGS_MAX_ATTEMPTS + 1):
        resp = api_get(
            'https://cloud-api.yandex.net/v1/disk/public/resources/admin/public-settings',
            params={'public_key': public_key},
            headers=AUTH_HEADER,
        )
        last_status_code = resp.status_code
        if resp.status_code == 200:
            return resp.json(), resp.status_code, ''
        last_error = resp.text
        if attempt < PUBLIC_SETTINGS_MAX_ATTEMPTS:
            time.sleep(PUBLIC_SETTINGS_RETRY_SLEEP)
    logger.warning(f'public-settings UNREACHABLE key={public_key[:32]}…')
    return None, last_status_code, last_error

# ═══════════════════════════════════════════════════════════════
#  Логика внешних шар
# ═══════════════════════════════════════════════════════════════
def get_external_share_reasons(accesses, org_uids_low, org_uids_high):
    result = []
    for acc in accesses:
        acc_type = acc.get('type', '')
        macros = acc.get('macros') or []
        if ALL_SHARE and 'all' in macros:
            result.append(('all_share', acc))
            continue
        if acc_type == 'owner':
            continue
        if acc_type in ('department', 'group'):
            continue
        if PERSONAL_SHARE and acc_type == 'user':
            access_uid = parse_uid(acc.get('id'))
            if access_uid is None:
                result.append(('personal_share_invalid_uid', acc))
                continue
            if not is_org_user_uid(access_uid, org_uids_low, org_uids_high):
                result.append(('personal_share', acc))
    return result

def make_base_row(email, uid, norm_item):
    public_key = norm_item.get('public_key', '')
    public_url = norm_item.get('public_url', '')
    if not public_url and public_key:
        public_url = resolve_public_url(public_key)
    return {
        'email': email,
        'uid': uid,
        'source_api': norm_item.get('_source', ''),
        'type': norm_item.get('type', ''),
        'name': norm_item.get('name', ''),
        'path': norm_item.get('path', ''),
        'created_at': norm_item.get('created_at', ''),
        'modified_at': norm_item.get('modified_at', ''),
        'public_key': public_key,
        'public_url': public_url,
    }

# ═══════════════════════════════════════════════════════════════
#  Воркер
# ═══════════════════════════════════════════════════════════════
def process_user(email, uid, shared_csv_w, unreachable_csv_w,
                 failed_users_csv_w, org_uids_low, org_uids_high):
    total_count = 0
    external_count = 0
    unreachable_count = 0
    for norm_item in iter_resources(uid, failed_users_csv_w, email):
        total_count += 1
        base = make_base_row(email, uid, norm_item)
        public_key = base['public_key']
        if not public_key:
            continue
        settings, status_code, error_text = get_public_settings_limited(public_key)
        if settings is None:
            unreachable_count += 1
            unreachable_csv_w.writerow({
                **base,
                'last_status_code': str(status_code or ''),
                'last_error': error_text or '',
            })
            continue
        accesses = settings.get('accesses') or []
        if not accesses:
            continue
        external_accesses = get_external_share_reasons(accesses, org_uids_low, org_uids_high)
        if not external_accesses:
            continue
        base['available_until'] = settings.get('available_until', '')
        for reason, acc in external_accesses:
            shared_csv_w.writerow({
                **base,
                'share_reason': reason,
                'access_type': acc.get('type', ''),
                'access_id': str(acc.get('id', '')),
                'access_rights': ', '.join(acc.get('rights', []) or []),
                'access_org_id': str(acc.get('org_id', '')),
                'access_macros': ', '.join(acc.get('macros', []) or []),
            })
        external_count += 1
        if total_count % GC_INTERVAL_FILES == 0:
            gc.collect()
    return total_count, external_count, unreachable_count

# ═══════════════════════════════════════════════════════════════
#  Точка входа
# ═══════════════════════════════════════════════════════════════
if __name__ == '__main__':
    shared_csv_path = os.path.join(OUTPUT_DIR, f'external_shared_resources_{RUN_TS}.csv')
    unreachable_csv_path = os.path.join(OUTPUT_DIR, f'unreachable_resources_{RUN_TS}.csv')
    failed_users_csv_path = os.path.join(OUTPUT_DIR, f'failed_users_{RUN_TS}.csv')

    shared_csv_w = SafeCSVWriter(shared_csv_path, SHARED_FIELDS)
    unreachable_csv_w = SafeCSVWriter(unreachable_csv_path, UNREACHABLE_FIELDS)
    failed_users_csv_w = SafeCSVWriter(failed_users_csv_path, FAILED_USERS_FIELDS)

    logger.info(f'Папка результатов       : {OUTPUT_DIR}')
    logger.info(f'CSV внешних шар         : {shared_csv_path}')
    logger.info(f'CSV недоступных ресурсов: {unreachable_csv_path}')
    logger.info(f'CSV failed users        : {failed_users_csv_path}')
    logger.info(f'USE_UID_LIST            : {USE_UID_LIST}')
    if USE_UID_LIST:
        logger.info(f'UID list file           : {UID_LIST_FILE}')
    else:
        logger.info(f'Exclude list file       : {EXCLUDE_LIST_FILE}')
    logger.info(f'DO_FALLBACK             : {DO_FALLBACK}')
    logger.info(f'Fallback технически     : {FALLBACK_AVAILABLE}')
    logger.info(f'ADMIN_RPS_LIMIT         : {ADMIN_RPS_LIMIT}')
    logger.info(f'ADMIN_REQUEST_TIMEOUT   : {ADMIN_REQUEST_TIMEOUT}')

    # ── Получаем список пользователей ──────────────────────
    if USE_UID_LIST:
        # Только указанные UID
        uid_set = load_uids_from_file(UID_LIST_FILE)
        if not uid_set:
            logger.error('Список UID пуст — выход')
            sys.exit(1)
        # Получаем информацию о каждом UID через directory API
        all_users_list = []
        for uid_str in uid_set:
            resp = api_get(
                f'https://api360.yandex.net/directory/v1/org/{ORGID}/users/{uid_str}',
                headers=AUTH_HEADER,
            )
            if resp.status_code == 200:
                all_users_list.append(resp.json())
            else:
                logger.warning(f'Пользователь {uid_str} не найден: {resp.status_code}')
        logger.info(f'Загружено {len(all_users_list)} пользователей по списку')
    else:
        # Все пользователи организации
        total_pages, first_users = get_users(1)
        if total_pages == 0:
            logger.error('Не удалось получить список пользователей — выход')
            sys.exit(1)
        all_users_list = list(first_users)
        for p in range(2, total_pages + 1):
            _, page_users = get_users(p)
            all_users_list.extend(page_users)
        logger.info(f'Всего пользователей в организации: {len(all_users_list)}')

        # Исключения
        if EXCLUDE_LIST_FILE:
            exclude_set = load_uids_from_file(EXCLUDE_LIST_FILE)
            if exclude_set:
                before = len(all_users_list)
                all_users_list = [u for u in all_users_list if str(u.get('id', '')) not in exclude_set]
                after = len(all_users_list)
                logger.info(f'Исключено {before - after} пользователей, осталось {after}')

    # ── Строим set UID организации ────────────────────────
    org_uids_low, org_uids_high = build_org_uid_sets(all_users_list)
    logger.info(
        f'UID сотрудников: low(<{UID_THRESHOLD})={len(org_uids_low)}, '
        f'high(>={UID_THRESHOLD})={len(org_uids_high)}')

    # ── Запуск воркеров ────────────────────────────────────
    user_count = 0
    skipped_count = 0
    resource_total = 0
    external_total = 0
    unreachable_total = 0
    futures = {}

    with ThreadPoolExecutor(max_workers=WORKERS, thread_name_prefix='worker') as pool:
        for user in all_users_list:
            email = user.get('email', '')
            uid = str(user.get('id', ''))
            uid_int = parse_uid(uid)
            if uid_int is None:
                logger.debug(f'Пропуск {email}: невалидный uid={uid}')
                skipped_count += 1
                continue
            if SCAN_ONLY_DOMAIN_UIDS and uid_int < UID_THRESHOLD:
                logger.debug(f'Пропуск {email} uid={uid}: uid < {UID_THRESHOLD}')
                skipped_count += 1
                continue

            fut = pool.submit(
                process_user, email, uid,
                shared_csv_w, unreachable_csv_w, failed_users_csv_w,
                org_uids_low, org_uids_high)
            futures[fut] = (email, uid)
            user_count += 1

        logger.info(f'К обработке: {user_count} | Пропущено: {skipped_count}')

        done_count = 0
        for fut in as_completed(futures):
            email, uid = futures.pop(fut)
            done_count += 1
            try:
                total_cnt, ext_cnt, unreach_cnt = fut.result()
                resource_total += total_cnt
                external_total += ext_cnt
                unreachable_total += unreach_cnt
                logger.info(
                    f'[{done_count}/{user_count}] ✔ {email} uid={uid}: '
                    f'ресурсов={total_cnt}, внешних={ext_cnt}, недоступных={unreach_cnt}')
            except Exception:
                logger.exception(f'[{done_count}/{user_count}] ✘ {email} uid={uid}')

    shared_csv_w.close()
    unreachable_csv_w.close()
    failed_users_csv_w.close()

    logger.info('═' * 60)
    logger.info(
        f'Готово. Обработано: {user_count}, пропущено: {skipped_count}, '
        f'ресурсов: {resource_total}, внешних шар: {external_total}, '
        f'недоступных: {unreachable_total}')
    logger.info(f'Результаты: {OUTPUT_DIR}')
