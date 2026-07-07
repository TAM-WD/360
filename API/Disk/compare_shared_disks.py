#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Скрипт для анализа и сравнения двух Общих Дисков организации Яндекс 360

ТРЕБОВАНИЯ:
Python 3.7+
Библиотека requests: pip install requests

НАСТРОЙКА:
Заполните секцию КОНФИГУРАЦИЯ ниже

ЗАПУСК:
python сompare__shared_disks.py
'''

import os
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime
import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
import threading
import time
import queue
import gc
import signal
import atexit
import re
import socket
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Any, Callable
import shutil
import urllib3


class RateLimiter:
    def __init__(self, max_calls_per_second=40):
        self.max_calls = max_calls_per_second
        self.period = 1.0
        self.calls = []
        self.lock = Lock()

    def acquire(self):
        with self.lock:
            now = time.time()
            self.calls = [t for t in self.calls if now - t < self.period]
            if len(self.calls) >= self.max_calls:
                oldest_call = self.calls[0]
                sleep_time = self.period - (now - oldest_call) + 0.01
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.calls = [t for t in self.calls if now - t < self.period]
            self.calls.append(now)

    def get_current_rate(self):
        with self.lock:
            now = time.time()
            self.calls = [t for t in self.calls if now - t < self.period]
            return len(self.calls)


# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# ОБЯЗАТЕЛЬНЫЕ ПАРАМЕТРЫ
ORGID = ''
TOKEN_ORG = ''  # Токен c правами cloud_api:disk.info и cloud_api:disk.read

# ДВА ОБЩИХ ДИСКА: source и destination
VD_HASH_SOURCE = ''       # vd_hash диска-источника
VD_HASH_DESTINATION = ''  # vd_hash диска-назначения

# Необязательные имена дисков (для отображения в логах и CSV)
# Если пусто — скрипт попытается получить имя через API
DISK_NAME_SOURCE = ''
DISK_NAME_DESTINATION = ''

# Стартовая папка для рекурсивного обхода (отправляется в API как есть)
# '/' — обход с корня диска
# '/some/folder' — обход только внутри указанной папки
START_PATH = '/'

# Сравнение результатов после сканирования
COMPARE_RESULTS = True  # True — сравнить два CSV после сканирования

# ──────────────────────────────────────────────────────────────────────────────
# SSL / TLS НАСТРОЙКИ
# ──────────────────────────────────────────────────────────────────────────────
# True  — отключить проверку SSL-сертификатов (для корп. прокси, самоподписанных
#          сертификатов, закрытых контуров без доступа к CA)
# False — стандартная проверка SSL (рекомендуется для продакшена)
DISABLE_SSL_VERIFICATION = True

# Путь к собственному CA-bundle (PEM). Если пусто — используется системный.
# Игнорируется, если DISABLE_SSL_VERIFICATION = True.
# Пример: CUSTOM_CA_BUNDLE = '/path/to/corporate-ca-bundle.crt'
CUSTOM_CA_BUNDLE = ''
# ──────────────────────────────────────────────────────────────────────────────

# Параметры работы
LIMIT_VD = 100
MAX_WORKERS = 2  # По одному воркеру на каждый диск
NUM_HELPERS_PER_DISK = 50

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOGS_DIR = Path(__file__).parent / f'logs_{timestamp}'
LOGS_DIR.mkdir(exist_ok=True)
RESULTS_DIR = Path(__file__).parent / 'results'

OUTPUT_FILE_SOURCE = f'disk_source_{timestamp}.csv'
OUTPUT_FILE_DESTINATION = f'disk_destination_{timestamp}.csv'
LOG_FILE = LOGS_DIR / f'shared_disks_parser_{timestamp}.log'

BATCH_WRITE_SIZE = 50
MAX_RECURSION_DEPTH = 100
GC_INTERVAL = 30

# Параметры защиты от потери интернета
INTERNET_CHECK_TIMEOUT = 10 * 60
INTERNET_RETRY_INTERVAL = 10
INTERNET_CHECK_HOSTS = [
    ('8.8.8.8', 53),
    ('1.1.1.1', 53),
    ('oauth.yandex.ru', 443),
]

LOG_LEVEL = logging.INFO
LOG_MAX_SIZE = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 5
LOG_MAX_AGE_DAYS = 365

csv_lock = Lock()
stats_lock = Lock()
internet_check_lock = Lock()

resources_rate_limiter = RateLimiter(max_calls_per_second=40)

stats = {
    'processed_disks': 0,
    'found_files_total': 0,
    'files_written': 0,
    'skipped_disks': 0,
    'gc_collections': 0,
    'total_api_calls': 0,
    'helper_workers_created': 0,
    'helper_tasks_processed': 0,
    'rate_limited_calls': 0
}

internet_stats = {
    'last_check': 0,
    'is_available': True,
    'reconnect_attempts': 0,
    'total_downtime': 0
}

shutdown_flag = Event()
session_pool = threading.local()

logger: Optional[logging.Logger] = None


# ============================================================================
# SSL HELPER — вычисляем параметр verify один раз
# ============================================================================

def get_ssl_verify():
    """
    Возвращает значение для параметра verify= в requests:
      - False              → SSL отключён
      - '/path/to/ca.crt'  → кастомный CA-bundle
      - True               → системный CA-bundle (по умолчанию)
    """
    if DISABLE_SSL_VERIFICATION:
        return False
    if CUSTOM_CA_BUNDLE:
        ca = Path(CUSTOM_CA_BUNDLE)
        if ca.exists():
            return str(ca)
        else:
            print(f'⚠️ CA-bundle не найден: {CUSTOM_CA_BUNDLE}, '
                  f'используется системный', file=sys.stderr)
    return True


# Вычисляем один раз при импорте
SSL_VERIFY = get_ssl_verify()


def apply_ssl_settings():
    """Подавляет предупреждения urllib3, если SSL отключён."""
    if DISABLE_SSL_VERIFICATION:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        os.environ['PYTHONWARNINGS'] = (
            'ignore:Unverified HTTPS request'
        )


# ============================================================================
# ЛОГИРОВАНИЕ
# ============================================================================

class CustomRotatingFileHandler(RotatingFileHandler):
    def __init__(self, filename, mode='a', maxBytes=0, backupCount=0,
                 encoding=None, delay=False, errors=None):
        self.base_filename_without_ext = None
        self.file_extension = None
        if isinstance(filename, Path):
            filename = str(filename)
        name_parts = filename.rsplit('.', 1)
        if len(name_parts) == 2:
            self.base_filename_without_ext = name_parts[0]
            self.file_extension = '.' + name_parts[1]
        else:
            self.base_filename_without_ext = filename
            self.file_extension = ''
        super().__init__(filename, mode, maxBytes, backupCount,
                         encoding, delay, errors)

    def rotation_filename(self, default_name):
        parts = default_name.rsplit('.', 1)
        if len(parts) == 2 and parts[1].isdigit():
            rotation_num = parts[1]
            return (f"{self.base_filename_without_ext}"
                    f"_{rotation_num}{self.file_extension}")
        return default_name

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None
        if self.backupCount > 0:
            for i in range(self.backupCount - 1, 0, -1):
                sfn = self.rotation_filename(f"{self.baseFilename}.{i}")
                dfn = self.rotation_filename(f"{self.baseFilename}.{i + 1}")
                if os.path.exists(sfn):
                    if os.path.exists(dfn):
                        os.remove(dfn)
                    os.rename(sfn, dfn)
            dfn = self.rotation_filename(f"{self.baseFilename}.1")
            if os.path.exists(dfn):
                os.remove(dfn)
            self.rotate(self.baseFilename, dfn)
        else:
            max_num = 0
            log_dir = os.path.dirname(self.baseFilename) or '.'
            base_name = os.path.basename(self.base_filename_without_ext)
            try:
                for filename in os.listdir(log_dir):
                    if (filename.startswith(base_name)
                            and filename.endswith(self.file_extension)):
                        name_without_ext = filename[:-len(self.file_extension)]
                        parts = name_without_ext.split('_')
                        if parts and parts[-1].isdigit():
                            num = int(parts[-1])
                            max_num = max(max_num, num)
            except OSError:
                pass
            next_num = max_num + 1
            dfn = (f"{self.base_filename_without_ext}"
                   f"_{next_num}{self.file_extension}")
            if os.path.exists(self.baseFilename):
                os.rename(self.baseFilename, dfn)
        if not self.delay:
            self.stream = self._open()


def cleanup_old_logs(max_age_days: int = LOG_MAX_AGE_DAYS):
    try:
        current_dir = Path(__file__).parent
        current_time = time.time()
        deleted_folders = 0
        deleted_files = 0

        for item in current_dir.iterdir():
            if item.is_dir() and item.name.startswith('logs_'):
                try:
                    folder_age = current_time - item.stat().st_mtime
                    if folder_age > max_age_days * 86400:
                        shutil.rmtree(item)
                        deleted_folders += 1
                        print(f'🧹 Удалена папка: {item.name}')
                except OSError as e:
                    print(f'⚠️ Не удалось удалить папку {item.name}: {e}',
                          file=sys.stderr)

        for item in current_dir.iterdir():
            if (item.is_file()
                    and item.name.startswith('disk_')
                    and item.name.endswith('.csv')):
                try:
                    file_age = current_time - item.stat().st_mtime
                    if file_age > max_age_days * 86400:
                        item.unlink()
                        deleted_files += 1
                        print(f'🧹 Удалён отчёт: {item.name}')
                except OSError as e:
                    print(f'⚠️ Не удалось удалить {item.name}: {e}',
                          file=sys.stderr)

        if deleted_folders > 0 or deleted_files > 0:
            print(f'✅ Удалено: {deleted_folders} папок, {deleted_files} отчётов')
        else:
            print(f'ℹ️ Старые логи и отчёты не найдены')
    except Exception as e:
        print(f'⚠️ Ошибка очистки (не критично): {e}', file=sys.stderr)


def setup_logging():
    global logger
    logger = logging.getLogger('SharedDisksParser')
    logger.setLevel(LOG_LEVEL)
    logger.propagate = False
    logger.handlers.clear()

    log_format = logging.Formatter(
        '[%(levelname)s] [%(threadName)s] %(asctime)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    file_handler = CustomRotatingFileHandler(
        str(LOG_FILE), mode='a', maxBytes=LOG_MAX_SIZE,
        backupCount=LOG_BACKUP_COUNT, encoding='utf-8', delay=True
    )
    file_handler.setLevel(LOG_LEVEL)
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(LOG_LEVEL)
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(log_format)
    logger.addHandler(error_handler)

    logger.info('=' * 80)
    logger.info('Система логирования инициализирована')
    logger.info(f'📁 Папка логов: {LOGS_DIR}')
    logger.info(f'📄 Лог-файл: {LOG_FILE.name}')
    logger.info(f'🔄 Ротация: каждые {LOG_MAX_SIZE // (1024*1024)} МБ')
    logger.info(f'📦 Количество бэкапов: {LOG_BACKUP_COUNT}')
    logger.info(f'🗑️ Автоудаление: {LOG_MAX_AGE_DAYS} дней')
    logger.info('=' * 80)


def log_info(message):
    if shutdown_flag.is_set():
        return
    if logger:
        logger.info(message)
    else:
        print(f'[INFO] [{threading.current_thread().name}] '
              f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {message}')


def log_error(message):
    if shutdown_flag.is_set():
        return
    if logger:
        logger.error(message)
    else:
        print(f'[ERROR] [{threading.current_thread().name}] '
              f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {message}',
              file=sys.stderr)


def log_warning(message):
    if shutdown_flag.is_set():
        return
    if logger:
        logger.warning(message)
    else:
        print(f'[WARNING] [{threading.current_thread().name}] '
              f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {message}')


# ============================================================================
# СЕТЬ И ИНТЕРНЕТ
# ============================================================================

def check_internet_connection(timeout: int = 5) -> bool:
    for host, port in INTERNET_CHECK_HOSTS:
        try:
            socket.setdefaulttimeout(timeout)
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((host, port))
            s.close()
            return True
        except (socket.error, socket.timeout, OSError):
            continue
    return False


def wait_for_internet_connection(
    max_wait_time: int = INTERNET_CHECK_TIMEOUT
) -> bool:
    start_time = time.time()
    attempt = 0
    log_error('⚠️ Обнаружена потеря интернет-соединения!')
    log_info(f'Ожидание восстановления '
             f'(макс. {max_wait_time // 60} минут)...')

    with internet_check_lock:
        internet_stats['is_available'] = False
        internet_stats['reconnect_attempts'] = 0

    while True:
        elapsed = time.time() - start_time
        if elapsed >= max_wait_time:
            log_error(f'❌ Время ожидания истекло '
                      f'({max_wait_time // 60} минут).')
            with internet_check_lock:
                internet_stats['total_downtime'] += elapsed
            return False

        attempt += 1
        remaining = max_wait_time - elapsed
        log_info(f'Попытка {attempt}: Проверка... '
                 f'(осталось {int(remaining)}с)')

        if check_internet_connection():
            downtime = time.time() - start_time
            log_info(f'✅ Интернет восстановлен! '
                     f'(простой: {int(downtime)}с)')
            with internet_check_lock:
                internet_stats['is_available'] = True
                internet_stats['reconnect_attempts'] = attempt
                internet_stats['total_downtime'] += downtime
                internet_stats['last_check'] = time.time()
            return True

        time.sleep(INTERNET_RETRY_INTERVAL)


def network_request_with_retry(
    func: Callable, *args,
    max_retries: int = 3, retry_delay: int = 5, **kwargs
) -> Any:
    last_exception = None
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except requests.exceptions.RequestException as e:
            last_exception = e
            log_error(f'Ошибка HTTP '
                      f'(попытка {attempt + 1}/{max_retries}): {str(e)}')
            if isinstance(e, (requests.exceptions.ConnectionError,
                              requests.exceptions.Timeout)):
                if not check_internet_connection():
                    if not wait_for_internet_connection():
                        return None
                    continue
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        except (socket.error, OSError) as e:
            last_exception = e
            log_error(f'Ошибка сокета '
                      f'(попытка {attempt + 1}/{max_retries}): {str(e)}')
            if not check_internet_connection():
                if not wait_for_internet_connection():
                    return None
                continue
            if attempt < max_retries - 1:
                time.sleep(retry_delay)

    log_error(f'Все попытки исчерпаны. '
              f'Последняя ошибка: {str(last_exception)}')
    return None


# ============================================================================
# HTTP-СЕССИИ
# ============================================================================

def get_session():
    if not hasattr(session_pool, 'session') or session_pool.session is None:
        session = requests.Session()

        # ── SSL настройки сессии ──
        session.verify = SSL_VERIFY

        retries = Retry(
            total=5, backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(
            max_retries=retries,
            pool_connections=10,
            pool_maxsize=20
        )
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        session_pool.session = session
    return session_pool.session


def close_session():
    if hasattr(session_pool, 'session') and session_pool.session is not None:
        try:
            session_pool.session.close()
        except Exception:
            pass
        session_pool.session = None


# ============================================================================
# РАБОТА С ПУТЯМИ
# ============================================================================

def extract_relative_path(full_path, vd_hash):
    if not full_path:
        return '/'
    if full_path.startswith('/') and not full_path.startswith('/disk'):
        return full_path

    pattern = r'vd:/[^/]+/disk(.*)$'
    match = re.search(pattern, full_path)
    if match:
        relative = match.group(1)
        return relative if relative else '/'

    if full_path.startswith('disk:'):
        return full_path.replace('disk:', '')

    return full_path


# ============================================================================
# API ЯНДЕКС ДИСКА
# ============================================================================

def get_virtual_disk_resources(vd_hash, relative_path='/',
                               limit=1000, offset=0):
    """Получить содержимое папки на Общем Диске."""
    if shutdown_flag.is_set():
        return None
    try:
        resources_rate_limiter.acquire()
        with stats_lock:
            stats['rate_limited_calls'] += 1

        session = get_session()
        full_path = f'vd:{vd_hash}:disk:{relative_path}'

        url = ('https://cloud-api.yandex.net'
               '/v1/disk/virtual-disks/resources')
        params = {
            'path': full_path,
            'limit': limit,
            'offset': offset,
            'fields': (
                '_embedded.items.name,_embedded.items.path,'
                '_embedded.items.type,_embedded.items.size,'
                '_embedded.items.created,_embedded.items.modified,'
                '_embedded.items.media_type,_embedded.total,'
                'name,path,type'
            )
        }
        headers = {
            'Authorization': f'OAuth {TOKEN_ORG}',
            'Accept': 'application/json'
        }

        # verify уже задан на уровне сессии (session.verify = SSL_VERIFY)
        response = session.get(url, params=params, headers=headers, timeout=30)
        with stats_lock:
            stats['total_api_calls'] += 1

        if response.status_code == 200:
            return response.json()
        elif response.status_code == 404:
            return None
        elif response.status_code == 403:
            log_error(f'Доступ запрещён: {full_path}')
            return None
        else:
            log_error(f'Ошибка {response.status_code} для {full_path}')
            return None
    except Exception as e:
        if not shutdown_flag.is_set():
            log_error(f'Исключение при запросе: {str(e)}')
        return None


def get_disk_name_from_api(vd_hash):
    """Попытка получить имя диска через API."""
    try:
        response = network_request_with_retry(
            get_virtual_disk_resources, vd_hash, '/', limit=1, offset=0
        )
        if response:
            return response.get('name', vd_hash)
    except Exception:
        pass
    return vd_hash


# ============================================================================
# ЗАПИСЬ CSV
# ============================================================================

def write_to_csv_batch(writer, batch):
    if not batch:
        return
    with csv_lock:
        for row in batch:
            writer.writerow(row)
        with stats_lock:
            stats['files_written'] += len(batch)
    batch.clear()


# ============================================================================
# ОБРАБОТКА ПАПОК (РЕКУРСИВНАЯ ЧЕРЕЗ ОЧЕРЕДЬ)
# ============================================================================

def process_folder_task(vd_hash, path, disk_name, csv_writer,
                        files_count_ref, local_queue,
                        active_tasks_counter):
    worker_id = threading.current_thread().name
    batch = []

    try:
        log_info(f'[{worker_id}] Обработка папки: {path}')
        offset = 0
        limit = 1000
        subdirs_found = []

        while not shutdown_flag.is_set():
            response = network_request_with_retry(
                get_virtual_disk_resources, vd_hash, path, limit, offset
            )
            if not response:
                break

            embedded = response.get('_embedded', {})
            items = embedded.get('items', [])
            total = embedded.get('total', 0)

            for item in items:
                if shutdown_flag.is_set():
                    break

                item_type = item.get('type')
                item_name = item.get('name', 'N/A')
                item_path = item.get('path', 'N/A')

                if item_type == 'file':
                    relative_item_path = extract_relative_path(
                        item_path, vd_hash
                    )
                    file_row = {
                        'disk_name': disk_name,
                        'vd_hash': vd_hash,
                        'file_name': item_name,
                        'file_path': relative_item_path,
                        'file_size': item.get('size', 'N/A'),
                        'file_created': item.get('created', 'N/A'),
                        'file_modified': item.get('modified', 'N/A'),
                        'media_type': item.get('media_type', 'N/A')
                    }
                    batch.append(file_row)
                    files_count_ref['count'] += 1

                    if len(batch) >= BATCH_WRITE_SIZE:
                        if csv_writer:
                            write_to_csv_batch(csv_writer, batch)
                        batch = []
                        if files_count_ref['count'] % GC_INTERVAL == 0:
                            gc.collect()
                            with stats_lock:
                                stats['gc_collections'] += 1

                elif item_type == 'dir':
                    relative_subdir_path = extract_relative_path(
                        item_path, vd_hash
                    )
                    subdirs_found.append(relative_subdir_path)

            if len(items) < limit or offset + limit >= total:
                break
            offset += limit

        if batch and csv_writer:
            write_to_csv_batch(csv_writer, batch)

        if subdirs_found:
            log_info(f'[{worker_id}] Найдено '
                     f'{len(subdirs_found)} подпапок в {path}')
            with active_tasks_counter['lock']:
                active_tasks_counter['count'] += len(subdirs_found)
            for subdir_path in subdirs_found:
                local_queue.put(subdir_path)

        log_info(f'[{worker_id}] Завершена папка: {path} '
                 f'(файлов: {files_count_ref["count"]}, '
                 f'подпапок: {len(subdirs_found)})')

    except Exception as e:
        log_error(f'[{worker_id}] Ошибка папки {path}: {str(e)}')
    finally:
        with active_tasks_counter['lock']:
            active_tasks_counter['count'] -= 1
        gc.collect()


def get_files_with_helpers(vd_hash, disk_name, csv_writer,
                           start_path='/'):
    """Обход диска начиная с start_path с helper-потоками."""
    worker_id = threading.current_thread().name
    ts = int(time.time())

    log_info('=' * 80)
    log_info(f'[{worker_id}] 🔍 Сканирование диска "{disk_name}"')
    log_info(f'[{worker_id}]    VD Hash: {vd_hash}')
    log_info(f'[{worker_id}]    Стартовый путь: {start_path}')
    log_info('=' * 80)

    files_count_ref = {'count': 0}
    active_tasks_counter = {'count': 0, 'lock': Lock()}

    try:
        response = network_request_with_retry(
            get_virtual_disk_resources, vd_hash, start_path,
            limit=1000, offset=0
        )
        if not response:
            log_error(f'Не удалось получить ресурсы для {start_path}')
            return 0

        emb = response.get('_embedded', {})
        items = emb.get('items', [])
        folders = []
        batch = []

        for item in items:
            item_type = item.get('type')
            item_name = item.get('name', 'N/A')
            item_path = item.get('path', 'N/A')

            if item_type == 'file':
                relative_item_path = extract_relative_path(
                    item_path, vd_hash
                )
                file_row = {
                    'disk_name': disk_name,
                    'vd_hash': vd_hash,
                    'file_name': item_name,
                    'file_path': relative_item_path,
                    'file_size': item.get('size', 'N/A'),
                    'file_created': item.get('created', 'N/A'),
                    'file_modified': item.get('modified', 'N/A'),
                    'media_type': item.get('media_type', 'N/A')
                }
                batch.append(file_row)
                files_count_ref['count'] += 1

            elif item_type == 'dir':
                relative_path = extract_relative_path(item_path, vd_hash)
                folders.append(relative_path)

        # Пагинация стартовой папки
        total_root = emb.get('total', 0)
        root_offset = 1000
        while root_offset < total_root and not shutdown_flag.is_set():
            response = network_request_with_retry(
                get_virtual_disk_resources, vd_hash, start_path,
                1000, root_offset
            )
            if not response:
                break
            emb2 = response.get('_embedded', {})
            items2 = emb2.get('items', [])
            if not items2:
                break
            for item in items2:
                item_type = item.get('type')
                item_name = item.get('name', 'N/A')
                item_path = item.get('path', 'N/A')
                if item_type == 'file':
                    relative_item_path = extract_relative_path(
                        item_path, vd_hash
                    )
                    file_row = {
                        'disk_name': disk_name,
                        'vd_hash': vd_hash,
                        'file_name': item_name,
                        'file_path': relative_item_path,
                        'file_size': item.get('size', 'N/A'),
                        'file_created': item.get('created', 'N/A'),
                        'file_modified': item.get('modified', 'N/A'),
                        'media_type': item.get('media_type', 'N/A')
                    }
                    batch.append(file_row)
                    files_count_ref['count'] += 1
                elif item_type == 'dir':
                    relative_path = extract_relative_path(
                        item_path, vd_hash
                    )
                    folders.append(relative_path)
            root_offset += 1000

        if batch and csv_writer:
            write_to_csv_batch(csv_writer, batch)

        log_info(f'[{worker_id}] Стартовый уровень ({start_path}): '
                 f'{len(folders)} папок, '
                 f'{files_count_ref["count"]} файлов')

        if not folders:
            return files_count_ref['count']

        local_queue = queue.Queue()
        with active_tasks_counter['lock']:
            active_tasks_counter['count'] = len(folders)
        for folder_path in folders:
            local_queue.put(folder_path)

        log_info(f'[{worker_id}] Создаём {NUM_HELPERS_PER_DISK} '
                 f'helper-воркеров для "{disk_name}"')
        with stats_lock:
            stats['helper_workers_created'] += NUM_HELPERS_PER_DISK

        stop_helpers = Event()

        def helper_worker(helper_id):
            helper_name = f'Helper_{disk_name}_{ts}_{helper_id}'
            threading.current_thread().name = helper_name
            log_info(f'[{helper_name}] Запущен')
            tasks_processed = 0
            consecutive_empty = 0
            max_consecutive_empty = 30

            while not shutdown_flag.is_set() and not stop_helpers.is_set():
                try:
                    folder_path = local_queue.get(timeout=10.0)
                    consecutive_empty = 0

                    process_folder_task(
                        vd_hash, folder_path, disk_name,
                        csv_writer, files_count_ref,
                        local_queue, active_tasks_counter
                    )
                    tasks_processed += 1
                    with stats_lock:
                        stats['helper_tasks_processed'] += 1
                    local_queue.task_done()

                except queue.Empty:
                    consecutive_empty += 1
                    with active_tasks_counter['lock']:
                        active_count = active_tasks_counter['count']
                        queue_size = local_queue.qsize()
                    if active_count > 0 or queue_size > 0:
                        consecutive_empty = 0
                        time.sleep(1)
                        continue
                    if consecutive_empty >= max_consecutive_empty:
                        log_info(f'[{helper_name}] Нет задач '
                                 f'{consecutive_empty} раз, завершаю')
                        break
                    continue
                except Exception as e:
                    log_error(f'[{helper_name}] Ошибка: {str(e)}')
                    continue

            log_info(f'[{helper_name}] Завершён. '
                     f'Обработано: {tasks_processed}')
            return tasks_processed

        with ThreadPoolExecutor(
            max_workers=NUM_HELPERS_PER_DISK,
            thread_name_prefix=f'HelperPool_{disk_name}_{ts}'
        ) as helper_executor:

            helper_futures = [
                helper_executor.submit(helper_worker, i + 1)
                for i in range(NUM_HELPERS_PER_DISK)
            ]

            main_tasks_processed = 0
            consecutive_empty = 0
            max_consecutive_empty = 30

            while not shutdown_flag.is_set():
                try:
                    folder_path = local_queue.get(timeout=10.0)
                    consecutive_empty = 0

                    process_folder_task(
                        vd_hash, folder_path, disk_name,
                        csv_writer, files_count_ref,
                        local_queue, active_tasks_counter
                    )
                    main_tasks_processed += 1
                    local_queue.task_done()

                except queue.Empty:
                    consecutive_empty += 1
                    with active_tasks_counter['lock']:
                        active_count = active_tasks_counter['count']
                        queue_size = local_queue.qsize()
                    if active_count > 0 or queue_size > 0:
                        consecutive_empty = 0
                        time.sleep(1)
                        continue
                    if consecutive_empty >= max_consecutive_empty:
                        break
                    continue
                except Exception as e:
                    log_error(f'[{worker_id}] Ошибка: {str(e)}')
                    continue

            stop_helpers.set()

            total_helper_tasks = 0
            for future in as_completed(helper_futures, timeout=120):
                try:
                    tasks_done = future.result(timeout=30)
                    total_helper_tasks += tasks_done
                except Exception as e:
                    log_error(f'[{worker_id}] Ошибка helper: {str(e)}')

        log_info('=' * 80)
        log_info(f'[{worker_id}] ✅ Диск "{disk_name}" завершён')
        log_info(f'[{worker_id}]    Файлов: {files_count_ref["count"]}')
        log_info(f'[{worker_id}]    Задач: '
                 f'{main_tasks_processed + total_helper_tasks}')
        log_info('=' * 80)

        return files_count_ref['count']

    except Exception as e:
        log_error(f'Ошибка обхода: {str(e)}')
        import traceback
        log_error(traceback.format_exc())
        return files_count_ref.get('count', 0)
    finally:
        gc.collect()


# ============================================================================
# ОБРАБОТКА ОДНОГО ДИСКА
# ============================================================================

def process_single_disk(vd_hash, disk_name, output_file, start_path='/'):
    """Сканирует один диск → отдельный CSV."""
    if shutdown_flag.is_set():
        return False, 0, output_file

    start_time = time.time()
    worker_id = threading.current_thread().name

    try:
        if not vd_hash:
            log_error(f'❌ Отсутствует vd_hash для "{disk_name}"')
            return False, 0, output_file

        with open(output_file, 'w', newline='', encoding='utf-8-sig',
                  buffering=65536) as csvfile:
            field_names = [
                'disk_name', 'vd_hash',
                'file_name', 'file_path', 'file_size',
                'file_created', 'file_modified', 'media_type'
            ]
            writer = csv.DictWriter(
                csvfile, field_names,
                extrasaction='ignore', delimiter=';'
            )
            writer.writeheader()

            files_count = get_files_with_helpers(
                vd_hash=vd_hash,
                disk_name=disk_name,
                csv_writer=writer,
                start_path=start_path
            )

            if files_count == 0:
                log_info(f'[{worker_id}] ℹ Диск "{disk_name}" пуст')
                info = {
                    'disk_name': disk_name,
                    'vd_hash': vd_hash,
                    'file_name': 'N/A',
                    'file_path': 'N/A',
                    'file_size': 'N/A',
                    'file_created': 'N/A',
                    'file_modified': 'N/A',
                    'media_type': 'N/A'
                }
                writer.writerow(info)

            csvfile.flush()
            os.fsync(csvfile.fileno())

        with stats_lock:
            stats['processed_disks'] += 1
            stats['found_files_total'] += files_count

        elapsed = time.time() - start_time
        log_info(f'[{worker_id}] ✅ "{disk_name}" → '
                 f'{files_count} файлов за {int(elapsed)}с → '
                 f'{output_file}')
        return True, files_count, output_file

    except Exception as e:
        if not shutdown_flag.is_set():
            log_error(f'❌ Ошибка "{disk_name}": {str(e)}')
            import traceback
            log_error(traceback.format_exc())
        with stats_lock:
            stats['skipped_disks'] += 1
        return False, 0, output_file
    finally:
        close_session()
        gc.collect()


# ============================================================================
# СРАВНЕНИЕ CSV
# ============================================================================

FILE_PATH_COL = 'file_path'
FILE_SIZE_COL = 'file_size'


def normalize_path(path: str) -> str:
    return re.sub(r'^disk:?', '', path)


def read_csv_for_compare(filepath: str) -> dict:
    data = {}
    p = Path(filepath)
    if not p.exists():
        log_error(f'Файл не найден: {filepath}')
        return data

    with open(p, newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f, delimiter=';')
        if FILE_PATH_COL not in (reader.fieldnames or []):
            log_error(f'Колонка "{FILE_PATH_COL}" не найдена в {filepath}')
            return data
        for row in reader:
            file_path = normalize_path(row[FILE_PATH_COL].strip())
            if file_path == 'N/A' or not file_path:
                continue
            try:
                file_size = int(row.get(FILE_SIZE_COL, 0) or 0)
            except ValueError:
                file_size = 0
            data[file_path] = file_size
    return data


def format_size(size_bytes) -> str:
    try:
        size_bytes = float(size_bytes)
    except (ValueError, TypeError):
        return '0 B'
    for unit in ('B', 'KB', 'MB', 'GB', 'TB'):
        if size_bytes < 1024:
            return f'{size_bytes:.2f} {unit}'
        size_bytes /= 1024
    return f'{size_bytes:.2f} PB'


def compare_csv_files(csv1: str, csv2: str):
    log_info('')
    log_info('=' * 80)
    log_info('📊 СРАВНЕНИЕ РЕЗУЛЬТАТОВ')
    log_info('=' * 80)
    log_info(f'   Файл 1 (source):      {csv1}')
    log_info(f'   Файл 2 (destination):  {csv2}')
    log_info('')

    table1 = read_csv_for_compare(csv1)
    table2 = read_csv_for_compare(csv2)

    if not table1 and not table2:
        log_warning('Оба файла пусты — сравнение невозможно')
        return

    paths1 = set(table1.keys())
    paths2 = set(table2.keys())

    only_in_1 = paths1 - paths2
    only_in_2 = paths2 - paths1

    RESULTS_DIR.mkdir(exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = RESULTS_DIR / f'diff_{ts}.csv'

    with open(out_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow(['file_path', 'missing_in', 'file_size'])
        for path in sorted(only_in_1):
            writer.writerow([path, Path(csv2).name, table1[path]])
        for path in sorted(only_in_2):
            writer.writerow([path, Path(csv1).name, table2[path]])

    total_size1 = sum(table1.values())
    total_size2 = sum(table2.values())

    log_info('=' * 60)
    log_info(f'{"Таблица":<30} {"Файлов":>8} {"Общий размер":>15}')
    log_info('-' * 60)
    log_info(f'{Path(csv1).name:<30} {len(table1):>8} '
             f'{format_size(total_size1):>15}')
    log_info(f'{Path(csv2).name:<30} {len(table2):>8} '
             f'{format_size(total_size2):>15}')
    log_info('=' * 60)
    log_info('')
    log_info(f'Совпадают (в обоих):          {len(paths1 & paths2)}')
    log_info(f'Только в source:              {len(only_in_1)}')
    log_info(f'Только в destination:         {len(only_in_2)}')
    log_info('')
    log_info(f'📄 Diff записан в: {out_path}')
    log_info('=' * 80)


# ============================================================================
# ЗАВЕРШЕНИЕ И СИГНАЛЫ
# ============================================================================

def cleanup():
    if shutdown_flag.is_set():
        return
    log_info('Завершение работы...')
    shutdown_flag.set()
    try:
        close_session()
    except Exception:
        pass
    time.sleep(0.5)
    if logger:
        log_info('Закрытие лог-файла...')
        for handler in logger.handlers:
            handler.close()


def signal_handler(signum, frame):
    log_info(f'Получен сигнал {signum}, завершение...')
    cleanup()
    os._exit(0)


# ============================================================================
# MAIN
# ============================================================================

def main():
    # ── Применяем SSL настройки до любых запросов ──
    apply_ssl_settings()

    try:
        cleanup_old_logs()
    except Exception as e:
        print(f'⚠️ Ошибка очистки логов: {e}', file=sys.stderr)

    setup_logging()

    atexit.register(cleanup)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # ── Лог SSL-статуса ──
    ssl_status_str = '🔓 ОТКЛЮЧЕНА' if DISABLE_SSL_VERIFICATION else '🔒 ВКЛЮЧЕНА'
    ssl_verify_detail = ''
    if not DISABLE_SSL_VERIFICATION and CUSTOM_CA_BUNDLE:
        ca_exists = Path(CUSTOM_CA_BUNDLE).exists()
        ssl_verify_detail = (
            f' (CA-bundle: {CUSTOM_CA_BUNDLE}, '
            f'{"найден" if ca_exists else "НЕ НАЙДЕН — системный"})'
        )

    log_info('=' * 80)
    log_info('🚀 АНАЛИЗ ДВУХ ОБЩИХ ДИСКОВ ЯНДЕКС 360 v11.1 + SSL OPTION')
    log_info('=' * 80)
    log_info(f'Формат пути: vd:{{vd_hash}}:disk:{{path}}')
    log_info(f'Организация: {ORGID}')
    log_info(f'Стартовый путь: {START_PATH}')
    log_info(f'Основных потоков: {MAX_WORKERS}')
    log_info(f'Helper\'ов на диск: {NUM_HELPERS_PER_DISK}')
    log_info(f'Батч-запись: {BATCH_WRITE_SIZE}')
    log_info(f'🚦 Rate Limit: 40 RPS')
    log_info(f'🔐 SSL проверка: {ssl_status_str}{ssl_verify_detail}')
    log_info(f'Защита от потери интернета: '
             f'{INTERNET_CHECK_TIMEOUT // 60} минут')
    log_info(f'Сравнение CSV: '
             f'{"ДА" if COMPARE_RESULTS else "НЕТ"}')
    log_info(f'VD_HASH Source:      {VD_HASH_SOURCE}')
    log_info(f'VD_HASH Destination: {VD_HASH_DESTINATION}')
    log_info('=' * 80)
    log_info('')

    if not all([ORGID, TOKEN_ORG]):
        log_error('❌ Не заполнены: ORGID, TOKEN_ORG!')
        return
    if not VD_HASH_SOURCE:
        log_error('❌ Не заполнен VD_HASH_SOURCE!')
        return
    if not VD_HASH_DESTINATION:
        log_error('❌ Не заполнен VD_HASH_DESTINATION!')
        return

    log_info('Проверка интернет-соединения...')
    if not check_internet_connection():
        log_error('❌ Нет подключения к интернету!')
        if not wait_for_internet_connection():
            log_error('Не удалось установить соединение. Завершение.')
            return
    log_info('✅ Интернет-соединение активно')
    log_info('')

    name_source = DISK_NAME_SOURCE
    name_destination = DISK_NAME_DESTINATION

    if not name_source:
        log_info('Получение имени Source-диска через API...')
        name_source = get_disk_name_from_api(VD_HASH_SOURCE)
        log_info(f'   Source: "{name_source}"')

    if not name_destination:
        log_info('Получение имени Destination-диска через API...')
        name_destination = get_disk_name_from_api(VD_HASH_DESTINATION)
        log_info(f'   Destination: "{name_destination}"')

    log_info('')
    log_info(f'📁 Source:      "{name_source}" ({VD_HASH_SOURCE})')
    log_info(f'📁 Destination: "{name_destination}" '
             f'({VD_HASH_DESTINATION})')
    log_info(f'📂 Стартовый путь: {START_PATH}')
    log_info('')

    executor = None
    results = {}

    try:
        executor = ThreadPoolExecutor(
            max_workers=MAX_WORKERS,
            thread_name_prefix='DiskWorker'
        )

        disks_to_process = [
            {
                'vd_hash': VD_HASH_SOURCE,
                'disk_name': name_source,
                'output_file': OUTPUT_FILE_SOURCE,
                'role': 'source'
            },
            {
                'vd_hash': VD_HASH_DESTINATION,
                'disk_name': name_destination,
                'output_file': OUTPUT_FILE_DESTINATION,
                'role': 'destination'
            }
        ]

        futures = {}
        for disk_info in disks_to_process:
            future = executor.submit(
                process_single_disk,
                disk_info['vd_hash'],
                disk_info['disk_name'],
                disk_info['output_file'],
                START_PATH
            )
            futures[future] = disk_info

        for future in as_completed(futures):
            if shutdown_flag.is_set():
                break
            disk_info = futures[future]
            try:
                success, files_count, out_file = future.result(timeout=7200)
                results[disk_info['role']] = {
                    'success': success,
                    'files_count': files_count,
                    'output_file': out_file,
                    'disk_name': disk_info['disk_name']
                }
            except Exception as e:
                log_error(f'❌ Исключение '
                          f'"{disk_info["disk_name"]}": {str(e)}')
                results[disk_info['role']] = {
                    'success': False,
                    'files_count': 0,
                    'output_file': disk_info['output_file'],
                    'disk_name': disk_info['disk_name']
                }

        log_info('')
        log_info('=' * 80)
        log_info('🎉 СКАНИРОВАНИЕ ЗАВЕРШЕНО!')
        log_info('=' * 80)

        for role in ('source', 'destination'):
            if role in results:
                r = results[role]
                status = '✅' if r['success'] else '❌'
                log_info(f'   {status} {role.upper()}: '
                         f'"{r["disk_name"]}" → '
                         f'{r["files_count"]} файлов → '
                         f'{r["output_file"]}')

        log_info(f'   🔧 API вызовов: {stats["total_api_calls"]}')
        log_info(f'   🚦 Rate limited: {stats["rate_limited_calls"]}')
        log_info(f'   👥 Helper\'ов: '
                 f'{stats["helper_workers_created"]}')
        log_info(f'   📦 Helper задач: '
                 f'{stats["helper_tasks_processed"]}')
        log_info(f'   💾 Записано строк: {stats["files_written"]}')
        log_info(f'   🗑 Сборок мусора: {stats["gc_collections"]}')
        log_info(f'   🔐 SSL: {ssl_status_str}')

        with internet_check_lock:
            if internet_stats['reconnect_attempts'] > 0:
                log_info(f'   🌐 Переподключений: '
                         f'{internet_stats["reconnect_attempts"]}')
                log_info(f'   ⏱ Простой: '
                         f'{int(internet_stats["total_downtime"])}с')

        log_info(f'   📁 Папка логов: {LOGS_DIR}')
        log_info('=' * 80)

        if COMPARE_RESULTS:
            src = results.get('source', {})
            dst = results.get('destination', {})
            if src.get('success') and dst.get('success'):
                compare_csv_files(
                    src['output_file'],
                    dst['output_file']
                )
            else:
                log_warning('⚠️ Сравнение пропущено: одно или оба '
                            'сканирования завершились с ошибкой')

    except KeyboardInterrupt:
        log_info('⚠ Прервано (Ctrl+C)')
    except Exception as e:
        if not shutdown_flag.is_set():
            log_error(f'❌ Критическая ошибка: {str(e)}')
            import traceback
            log_error(traceback.format_exc())
    finally:
        if executor:
            try:
                log_info('Завершение потоков...')
                executor.shutdown(wait=True, cancel_futures=True)
            except Exception:
                pass
        cleanup()
        time.sleep(0.3)


if __name__ == '__main__':
    try:
        main()
    except Exception:
        pass
    finally:
        os._exit(0)