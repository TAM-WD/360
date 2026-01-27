#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Скрипт для анализа Общих Дисков организации Яндекс 360  

ТРЕБОВАНИЯ:
Python 3.7+
Библиотека requests: pip install requests

НАСТРОЙКА:
Заполните секцию КОНФИГУРАЦИЯ ниже

ЗАПУСК:
python Parser_for_shared_disks.py
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

class RateLimiter:
    def __init__(self, max_calls_per_second=40):
        self.max_calls = max_calls_per_second
        self.period = 1.0
        self.calls = []
        self.lock = Lock()
    
    def acquire(self):
        with self.lock:
            now = time.time()
            
            self.calls = [call_time for call_time in self.calls if now - call_time < self.period]
            
            if len(self.calls) >= self.max_calls:
                oldest_call = self.calls[0]
                sleep_time = self.period - (now - oldest_call) + 0.01
                
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.calls = [call_time for call_time in self.calls if now - call_time < self.period]
            
            self.calls.append(now)
    
    def get_current_rate(self):
        with self.lock:
            now = time.time()
            self.calls = [call_time for call_time in self.calls if now - call_time < self.period]
            return len(self.calls)

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# ОБЯЗАТЕЛЬНЫЕ ПАРАМЕТРЫ
ORGID = ''
TOKEN_ORG = ''  # Токен c правами cloud_api:disk.info и cloud_api:disk.read

# Параметры работы
LIMIT_VD = 100
MAX_WORKERS = 10 
NUM_HELPERS_PER_DISK = 50  # Количество helper'ов на диск

# Поиск
SEARCH_FILE_NAME = ''  # Имя файла для поиска (пустое = все файлы)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOGS_DIR = Path(__file__).parent / f'logs_{timestamp}'
LOGS_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = f'shared_disks_report_{timestamp}.csv'
LOG_FILE = LOGS_DIR / f'shared_disks_parser_{timestamp}.log'

BATCH_WRITE_SIZE = 50
MAX_RECURSION_DEPTH = 100
GC_INTERVAL = 30

# Параметры защиты от потери интернета
INTERNET_CHECK_TIMEOUT = 10 * 60  # 10 минут
INTERNET_RETRY_INTERVAL = 10  # 10 секунд
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
            new_name = f"{self.base_filename_without_ext}_{rotation_num}{self.file_extension}"
            return new_name
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
                    if filename.startswith(base_name) and filename.endswith(self.file_extension):
                        name_without_ext = filename[:-len(self.file_extension)]
                        parts = name_without_ext.split('_')
                        if parts and parts[-1].isdigit():
                            num = int(parts[-1])
                            max_num = max(max_num, num)
            except OSError:
                pass
            
            next_num = max_num + 1
            dfn = f"{self.base_filename_without_ext}_{next_num}{self.file_extension}"
            
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
                    print(f'⚠️ Не удалось удалить папку {item.name}: {e}', file=sys.stderr)
        
        for item in current_dir.iterdir():
            if item.is_file() and item.name.startswith('shared_disks_report_') and item.name.endswith('.csv'):
                try:
                    file_age = current_time - item.stat().st_mtime
                    if file_age > max_age_days * 86400:
                        item.unlink()
                        deleted_files += 1
                        print(f'🧹 Удалён отчёт: {item.name}')
                except OSError as e:
                    print(f'⚠️ Не удалось удалить {item.name}: {e}', file=sys.stderr)
        
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
        str(LOG_FILE),
        mode='a',
        maxBytes=LOG_MAX_SIZE,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8',
        delay=True
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
        print(f'[INFO] [{threading.current_thread().name}] {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {message}')


def log_error(message):
    if shutdown_flag.is_set():
        return
    if logger:
        logger.error(message)
    else:
        print(f'[ERROR] [{threading.current_thread().name}] {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {message}', 
              file=sys.stderr)


def log_warning(message):
    if shutdown_flag.is_set():
        return
    if logger:
        logger.warning(message)
    else:
        print(f'[WARNING] [{threading.current_thread().name}] {datetime.now().strftime("%Y-%m-%d %H:%M:%S")} | {message}')


def check_internet_connection(timeout: int = 5) -> bool:
    for host, port in INTERNET_CHECK_HOSTS:
        try:
            socket.setdefaulttimeout(timeout)
            socket.socket(socket.AF_INET, socket.SOCK_STREAM).connect((host, port))
            return True
        except (socket.error, socket.timeout, OSError):
            continue
    return False


def wait_for_internet_connection(max_wait_time: int = INTERNET_CHECK_TIMEOUT) -> bool:
    start_time = time.time()
    attempt = 0
    
    log_error('⚠️ Обнаружена потеря интернет-соединения!')
    log_info(f'Начинаю ожидание восстановления соединения (макс. {max_wait_time // 60} минут)...')
    
    with internet_check_lock:
        internet_stats['is_available'] = False
        internet_stats['reconnect_attempts'] = 0
    
    while True:
        elapsed = time.time() - start_time
        
        if elapsed >= max_wait_time:
            log_error(f'❌ Время ожидания истекло ({max_wait_time // 60} минут).')
            with internet_check_lock:
                internet_stats['total_downtime'] += elapsed
            return False
        
        attempt += 1
        remaining = max_wait_time - elapsed
        
        log_info(f'Попытка {attempt}: Проверка соединения... (осталось {int(remaining)}с)')
        
        if check_internet_connection():
            downtime = time.time() - start_time
            log_info(f'✅ Интернет-соединение восстановлено! (простой: {int(downtime)}с)')
            
            with internet_check_lock:
                internet_stats['is_available'] = True
                internet_stats['reconnect_attempts'] = attempt
                internet_stats['total_downtime'] += downtime
                internet_stats['last_check'] = time.time()
            
            return True
        
        time.sleep(INTERNET_RETRY_INTERVAL)


def network_request_with_retry(
    func: Callable,
    *args,
    max_retries: int = 3,
    retry_delay: int = 5,
    **kwargs
) -> Any:
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            return result
        
        except requests.exceptions.RequestException as e:
            last_exception = e
            log_error(f'Ошибка HTTP запроса (попытка {attempt + 1}/{max_retries}): {str(e)}')
            
            if isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
                if not check_internet_connection():
                    log_error('Проблема с интернет-соединением обнаружена')
                    
                    if not wait_for_internet_connection():
                        log_error('Не удалось восстановить соединение, прерываем запрос')
                        return None
                    
                    log_info('Повторяю запрос после восстановления соединения...')
                    continue
            
            if attempt < max_retries - 1:
                log_info(f'Ожидание {retry_delay}с перед следующей попыткой...')
                time.sleep(retry_delay)
        
        except (socket.error, OSError) as e:
            last_exception = e
            log_error(f'Ошибка сокета/системы (попытка {attempt + 1}/{max_retries}): {str(e)}')
            
            if not check_internet_connection():
                log_error('Проблема с интернет-соединением обнаружена')
                
                if not wait_for_internet_connection():
                    log_error('Не удалось восстановить соединение, прерываем запрос')
                    return None
                
                log_info('Повторяю запрос после восстановления соединения...')
                continue
            
            if attempt < max_retries - 1:
                log_info(f'Ожидание {retry_delay}с перед следующей попыткой...')
                time.sleep(retry_delay)
    
    log_error(f'Все попытки исчерпаны. Последняя ошибка: {str(last_exception)}')
    return None


def get_session():
    if not hasattr(session_pool, 'session') or session_pool.session is None:
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retries, pool_connections=10, pool_maxsize=20)
        session.mount('https://', adapter)
        session.mount('http://', adapter)
        session_pool.session = session
    return session_pool.session


def close_session():
    if hasattr(session_pool, 'session') and session_pool.session is not None:
        try:
            session_pool.session.close()
        except:
            pass
        session_pool.session = None


def extract_relative_path(full_path, vd_hash):
    if not full_path:
        return '/'
    
    if full_path.startswith('/') and not full_path.startswith('/disk'):
        return full_path
    
    pattern = rf'vd:/[^/]+/disk(.*)$'
    match = re.search(pattern, full_path)
    if match:
        relative = match.group(1)
        return relative if relative else '/'
    
    if full_path.startswith('disk:'):
        return full_path.replace('disk:', '')
    
    return full_path


def disk_get_shared_disks(offset):
    try:
        session = get_session()
        url = 'https://cloud-api.yandex.net/v1/disk/virtual-disks/manage/org-resources/list'
        params = {
            'org_id': ORGID,
            'limit': LIMIT_VD,
            'offset': offset
        }
        headers = {'Authorization': f'OAuth {TOKEN_ORG}'}
        
        response = session.get(url, params=params, headers=headers, timeout=30)
        
        if response.status_code != 200:
            log_error(f'Ошибка получения Общих Дисков: {response.status_code}, {response.text}')
            return None
        
        log_info(f'✓ Получена порция Общих Дисков (offset {offset})')
        return response.json()
        
    except Exception as e:
        log_error(f'Исключение при получении Общих Дисков (offset {offset}): {str(e)}')
        return None


def get_all_shared_disks():
    all_disks = []
    offset = 0
    
    log_info('Загрузка списка Общих Дисков организации...')
    
    while not shutdown_flag.is_set():
        response = network_request_with_retry(disk_get_shared_disks, offset)
        
        if not response:
            break
        
        disks = response.get('items', [])
        
        if not disks:
            break
        
        for disk in disks:
            resource_id = disk.get('resource_id')
            disk_name = disk.get('name', 'N/A')
            vd_hash = disk.get('vd_hash')
            
            all_disks.append({
                'resource_id': resource_id,
                'name': disk_name,
                'vd_hash': vd_hash,
                'state': disk.get('state', 'unknown'),
                'description': disk.get('description', '')
            })
            
            log_info(f'  📁 {disk_name} (vd_hash: {vd_hash})')
        
        total = response.get('total', 0)
        if offset + LIMIT_VD >= total:
            break
        
        offset += LIMIT_VD
    
    log_info(f'✓ Всего загружено Общих Дисков: {len(all_disks)}')
    return all_disks


def get_virtual_disk_resources(vd_hash, relative_path='/', limit=1000, offset=0):
    if shutdown_flag.is_set():
        return None
    
    try:
        resources_rate_limiter.acquire()
        
        with stats_lock:
            stats['rate_limited_calls'] += 1
        
        session = get_session()
        
        full_path = f'vd:{vd_hash}:disk:{relative_path}'
        
        url = 'https://cloud-api.yandex.net/v1/disk/virtual-disks/resources'
        params = {
            'path': full_path,
            'limit': limit,
            'offset': offset,
            'fields': '_embedded.items.name,_embedded.items.path,_embedded.items.type,_embedded.items.size,_embedded.items.created,_embedded.items.modified,_embedded.items.media_type,_embedded.total,name,path,type'
        }
        headers = {
            'Authorization': f'OAuth {TOKEN_ORG}',
            'Accept': 'application/json'
        }
        
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


def write_to_csv_batch(writer, batch):
    if not batch:
        return
    
    with csv_lock:
        for row in batch:
            writer.writerow(row)
        
        with stats_lock:
            stats['files_written'] += len(batch)
    
    batch.clear()


def process_folder_task(vd_hash, path, disk_name, search_name, csv_writer, files_count_ref, local_queue, active_tasks_counter):
    worker_id = threading.current_thread().name
    batch = []
    
    try:
        log_info(f'[{worker_id}] Обработка папки: {path}')
        
        offset = 0
        limit = 1000
        subdirs_found = []
        
        while not shutdown_flag.is_set():
            response = network_request_with_retry(
                get_virtual_disk_resources, 
                vd_hash, 
                path, 
                limit, 
                offset
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
                    if not search_name or search_name.lower() in item_name.lower():
                        relative_item_path = extract_relative_path(item_path, vd_hash)
                        
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
                    relative_subdir_path = extract_relative_path(item_path, vd_hash)
                    subdirs_found.append(relative_subdir_path)
            
            if len(items) < limit or offset + limit >= total:
                break
            
            offset += limit
        
        if batch and csv_writer:
            write_to_csv_batch(csv_writer, batch)
        
        if subdirs_found:
            log_info(f'[{worker_id}] Найдено {len(subdirs_found)} подпапок в {path}, добавляю в очередь')
            
            with active_tasks_counter['lock']:
                active_tasks_counter['count'] += len(subdirs_found)
            
            for subdir_path in subdirs_found:
                local_queue.put(subdir_path)
        
        log_info(f'[{worker_id}] Завершена обработка папки: {path} (файлов: {files_count_ref["count"]}, подпапок: {len(subdirs_found)})')
        
    except Exception as e:
        log_error(f'[{worker_id}] Ошибка при обработке папки {path}: {str(e)}')
    finally:
        with active_tasks_counter['lock']:
            active_tasks_counter['count'] -= 1
        gc.collect()


def get_files_with_helpers(vd_hash, disk_name, search_name, csv_writer):
    worker_id = threading.current_thread().name
    timestamp = int(time.time())
    
    log_info('=' * 80)
    log_info(f'[{worker_id}] 🔍 Начало сканирования диска "{disk_name}" с HELPER\'ами')
    log_info(f'[{worker_id}]    VD Hash: {vd_hash}')
    log_info('=' * 80)
    
    files_count_ref = {'count': 0}
    
    active_tasks_counter = {
        'count': 0,
        'lock': Lock()
    }
    
    try:
        response = network_request_with_retry(
            get_virtual_disk_resources, 
            vd_hash, 
            '/', 
            limit=1000, 
            offset=0
        )
        
        if not response:
            log_error(f'Не удалось получить ресурсы для корня')
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
                if not search_name or search_name.lower() in item_name.lower():
                    relative_item_path = extract_relative_path(item_path, vd_hash)
                    
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
        
        if batch and csv_writer:
            write_to_csv_batch(csv_writer, batch)
        
        log_info(f'[{worker_id}] Корневой уровень: {len(folders)} папок, {files_count_ref["count"]} файлов')
        
        if not folders:
            return files_count_ref['count']
        
        local_queue = queue.Queue()
        
        with active_tasks_counter['lock']:
            active_tasks_counter['count'] = len(folders)
        
        for folder_path in folders:
            local_queue.put(folder_path)
        
        log_info(f'[{worker_id}] Создаем {NUM_HELPERS_PER_DISK} helper воркеров для диска "{disk_name}"')
        
        with stats_lock:
            stats['helper_workers_created'] += NUM_HELPERS_PER_DISK
        
        stop_helpers = Event()
        
        def helper_worker(helper_id):
            helper_name = f'Helper_{disk_name}_{timestamp}_{helper_id}'
            threading.current_thread().name = helper_name
            
            log_info(f'[{helper_name}] Запущен')
            
            tasks_processed = 0
            consecutive_empty = 0
            max_consecutive_empty = 30
            
            while not shutdown_flag.is_set() and not stop_helpers.is_set():
                try:
                    folder_path = local_queue.get(timeout=10.0)
                    consecutive_empty = 0
                    
                    log_info(f'[{helper_name}] Взял задачу: {folder_path}')
                    
                    process_folder_task(
                        vd_hash, 
                        folder_path, 
                        disk_name, 
                        search_name, 
                        csv_writer, 
                        files_count_ref,
                        local_queue,
                        active_tasks_counter
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
                        log_info(f'[{helper_name}] Нет задач {consecutive_empty} раз подряд, завершаю работу')
                        break
                    
                    continue
                
                except Exception as e:
                    log_error(f'[{helper_name}] Ошибка: {str(e)}')
                    continue
            
            log_info(f'[{helper_name}] Завершён. Обработано задач: {tasks_processed}')
            return tasks_processed
        
        with ThreadPoolExecutor(
            max_workers=NUM_HELPERS_PER_DISK, 
            thread_name_prefix=f'HelperPool_{disk_name}_{timestamp}'
        ) as helper_executor:
            
            helper_futures = [
                helper_executor.submit(helper_worker, i+1)
                for i in range(NUM_HELPERS_PER_DISK)
            ]
            
            log_info(f'[{worker_id}] Основной воркер также обрабатывает очередь')
            
            main_tasks_processed = 0
            consecutive_empty = 0
            max_consecutive_empty = 30
            
            while not shutdown_flag.is_set():
                try:
                    folder_path = local_queue.get(timeout=10.0)
                    consecutive_empty = 0
                    
                    log_info(f'[{worker_id}] Взял задачу: {folder_path}')
                    
                    process_folder_task(
                        vd_hash, 
                        folder_path, 
                        disk_name, 
                        search_name, 
                        csv_writer, 
                        files_count_ref,
                        local_queue,
                        active_tasks_counter
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
                        log_info(f'[{worker_id}] Очередь пуста {consecutive_empty} раз, завершаю обработку')
                        break
                    
                    continue
                
                except Exception as e:
                    log_error(f'[{worker_id}] Ошибка: {str(e)}')
                    continue
            
            log_info(f'[{worker_id}] Основной воркер обработал {main_tasks_processed} задач')
            log_info(f'[{worker_id}] Ожидание завершения helper\'ов...')
            
            stop_helpers.set()
            
            total_helper_tasks = 0
            for future in as_completed(helper_futures, timeout=120):
                try:
                    tasks_done = future.result(timeout=30)
                    total_helper_tasks += tasks_done
                    log_info(f'[{worker_id}] Helper завершил {tasks_done} задач')
                except Exception as e:
                    log_error(f'[{worker_id}] Ошибка в helper: {str(e)}')
            
            log_info(f'[{worker_id}] Все helper\'ы завершены. Helper\'ы обработали: {total_helper_tasks} задач')
        
        log_info('=' * 80)
        log_info(f'[{worker_id}] ✅ Диск "{disk_name}" завершён')
        log_info(f'[{worker_id}]    Найдено файлов: {files_count_ref["count"]}')
        log_info(f'[{worker_id}]    Всего задач обработано: {main_tasks_processed + total_helper_tasks}')
        log_info('=' * 80)
        log_info('')
        
        return files_count_ref['count']
        
    except Exception as e:
        log_error(f'Ошибка при обходе с helper\'ами: {str(e)}')
        import traceback
        log_error(traceback.format_exc())
        return files_count_ref.get('count', 0)
    finally:
        gc.collect()


def update_stats(processed=0, files_found=0, skipped=0):
    with stats_lock:
        stats['processed_disks'] += processed
        stats['found_files_total'] += files_found
        stats['skipped_disks'] += skipped


def process_shared_disk(disk, writer):
    if shutdown_flag.is_set():
        return False, 0
    
    start_time = time.time()
    worker_id = threading.current_thread().name
    
    try:
        disk_name = disk.get('name', 'N/A')
        vd_hash = disk.get('vd_hash')
        
        if not vd_hash:
            log_error(f'❌ Отсутствует vd_hash для диска "{disk_name}"')
            update_stats(skipped=1)
            return False, 0
        
        files_count = get_files_with_helpers(
            vd_hash=vd_hash,
            disk_name=disk_name,
            search_name=SEARCH_FILE_NAME,
            csv_writer=writer
        )
        
        if files_count == 0:
            log_info(f'[{worker_id}] ℹ Диск "{disk_name}" пуст или файлы не найдены')
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
            with csv_lock:
                writer.writerow(info)
                with stats_lock:
                    stats['files_written'] += 1
        
        update_stats(processed=1, files_found=files_count)
        
        elapsed_time = time.time() - start_time
        log_info(f'[{worker_id}] ✅ "{disk_name}" → {files_count} файлов за {int(elapsed_time)}с')
        log_info('')
        
        return True, files_count
        
    except Exception as e:
        if not shutdown_flag.is_set():
            log_error(f'❌ Ошибка "{disk.get("name", "N/A")}": {str(e)}')
            import traceback
            log_error(traceback.format_exc())
        update_stats(skipped=1)
        return False, 0
    finally:
        close_session()
        gc.collect()


def cleanup():
    if shutdown_flag.is_set():
        return
    
    log_info('Завершение работы...')
    shutdown_flag.set()
    
    try:
        close_session()
    except:
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


def main():
    try:
        cleanup_old_logs()
    except Exception as e:
        print(f'⚠️ Ошибка очистки логов: {e}', file=sys.stderr)
    
    setup_logging()
    
    atexit.register(cleanup)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    log_info('=' * 80)
    log_info('🚀 АНАЛИЗ ОБЩИХ ДИСКОВ ЯНДЕКС 360 v10.8 LOGS IN FOLDERS + RATE LIMIT 40 RPS')
    log_info('=' * 80)
    log_info(f'Формат пути: vd:{{vd_hash}}:disk:{{path}}')
    log_info(f'Организация: {ORGID}')
    log_info(f'Основных потоков: {MAX_WORKERS}')
    log_info(f'Helper\'ов на диск: {NUM_HELPERS_PER_DISK}')
    log_info(f'Батч-запись: {BATCH_WRITE_SIZE}')
    log_info(f'🚦 Rate Limit: 40 RPS (фиксировано для /virtual-disks/resources)')
    log_info(f'Защита от потери интернета: {INTERNET_CHECK_TIMEOUT // 60} минут')
    if SEARCH_FILE_NAME:
        log_info(f'🔍 Поиск: "{SEARCH_FILE_NAME}"')
    else:
        log_info('🔍 Режим: ВСЕ файлы')
    log_info('=' * 80)
    log_info('')
    
    if not all([ORGID, TOKEN_ORG]):
        log_error('❌ Не заполнены параметры: ORGID, TOKEN_ORG!')
        return
    
    log_info('Проверка интернет-соединения...')
    if not check_internet_connection():
        log_error('❌ Нет подключения к интернету!')
        if not wait_for_internet_connection():
            log_error('Не удалось установить соединение. Завершение работы.')
            return
    log_info('✅ Интернет-соединение активно')
    log_info('')
    
    executor = None
    
    try:
        with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig', buffering=65536) as csvfile:
            field_names = [
                'disk_name', 'vd_hash',
                'file_name', 'file_path', 'file_size',
                'file_created', 'file_modified', 'media_type'
            ]
            writer = csv.DictWriter(csvfile, field_names, extrasaction='ignore', delimiter=';')
            writer.writeheader()
            
            all_disks = get_all_shared_disks()
            
            if not all_disks:
                log_error('❌ Не удалось получить список дисков')
                return
            
            total_disks = len(all_disks)
            log_info('')
            log_info(f'📊 Дисков к обработке: {total_disks}')
            log_info('')
            
            executor = ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='Worker')
            
            futures = {
                executor.submit(process_shared_disk, disk, writer): disk
                for disk in all_disks
            }
            
            completed = 0
            for future in as_completed(futures):
                if shutdown_flag.is_set():
                    break
                
                completed += 1
                disk = futures[future]
                
                try:
                    success, files_count = future.result(timeout=3600)
                    
                    if completed % 3 == 0 or completed == total_disks:
                        log_info('─' * 80)
                        log_info(f'📊 ПРОГРЕСС: {completed}/{total_disks} ({int(completed/total_disks*100)}%)')
                        log_info(f'   ✅ Обработано: {stats["processed_disks"]}')
                        log_info(f'   ⏭ Пропущено: {stats["skipped_disks"]}')
                        log_info(f'   📄 Найдено файлов: {stats["found_files_total"]}')
                        log_info(f'   💾 Записано строк: {stats["files_written"]}')
                        log_info(f'   🔧 API вызовов: {stats["total_api_calls"]}')
                        log_info(f'   🚦 Rate limited: {stats["rate_limited_calls"]} (текущая частота: {resources_rate_limiter.get_current_rate()}/s)')
                        log_info(f'   👥 Helper\'ов создано: {stats["helper_workers_created"]}')
                        log_info(f'   📦 Helper задач: {stats["helper_tasks_processed"]}')
                        log_info(f'   🗑 Сборок мусора: {stats["gc_collections"]}')
                        
                        with internet_check_lock:
                            if internet_stats['reconnect_attempts'] > 0:
                                log_info(f'   🌐 Переподключений: {internet_stats["reconnect_attempts"]}')
                                log_info(f'   ⏱ Простой: {int(internet_stats["total_downtime"])}с')
                        
                        log_info('─' * 80)
                        log_info('')
                        
                except Exception as e:
                    if not shutdown_flag.is_set():
                        log_error(f'❌ Исключение "{disk.get("name", "N/A")}": {str(e)}')
                        update_stats(skipped=1)
            
            csvfile.flush()
            os.fsync(csvfile.fileno())
            
            log_info('')
            log_info('=' * 80)
            log_info('🎉 ОБРАБОТКА ЗАВЕРШЕНА!')
            log_info('=' * 80)
            log_info(f'📊 Всего дисков: {total_disks}')
            log_info(f'   ✅ Обработано: {stats["processed_disks"]}')
            log_info(f'   ⏭ Пропущено: {stats["skipped_disks"]}')
            log_info(f'   📄 Найдено файлов: {stats["found_files_total"]}')
            log_info(f'   💾 Записано строк: {stats["files_written"]}')
            log_info(f'   🔧 API вызовов: {stats["total_api_calls"]}')
            log_info(f'   🚦 Rate limited запросов: {stats["rate_limited_calls"]}')
            log_info(f'   👥 Helper воркеров создано: {stats["helper_workers_created"]}')
            log_info(f'   📦 Helper задач обработано: {stats["helper_tasks_processed"]}')
            log_info(f'   🗑 Сборок мусора: {stats["gc_collections"]}')
            
            with internet_check_lock:
                if internet_stats['reconnect_attempts'] > 0:
                    log_info(f'   🌐 Переподключений к интернету: {internet_stats["reconnect_attempts"]}')
                    log_info(f'   ⏱ Общее время простоя: {int(internet_stats["total_downtime"])}с')
            
            log_info('─' * 80)
            log_info(f'📄 CSV-отчет: {OUTPUT_FILE}')
            log_info(f'📁 Папка логов: {LOGS_DIR}')
            log_info(f'📋 Лог-файл: {LOG_FILE.name}')
            log_info('=' * 80)
            
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
            except:
                pass
        
        cleanup()
        time.sleep(0.3)


if __name__ == '__main__':
    try:
        main()
    except:
        pass
    finally:
        os._exit(0)
