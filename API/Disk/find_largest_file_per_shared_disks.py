#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Скрипт для поиска самого крупного файла на каждом Общем Диске Яндекс 360

РЕЖИМЫ РАБОТЫ:
1. Все Общие Диски организации - оставить VD_HASH_LIST пустым.
Не рекомендуется использовать функцию поиска информации по всем Общим Дискам в нескольких ситуациях:
- >1 Общего Диска с большим количеством файлов
- > Общих Дисков более 100

Выполнение поиска по всем Общим Дискам можно занять большое количество времени

2. Конкретные диски - заполнить VD_HASH_LIST хэшами дисков

vd_hash Общего Диска можно найти запросом ниже:
https://yandex.ru/dev/disk-api/doc/ru/reference/shared-disks/shd-list-for-org

ТРЕБОВАНИЯ:
Python 3.7+
Библиотека requests: pip install requests

ЗАПУСК:
python find_largest_file_per_shared_disks.py
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
from typing import Optional, Any, Callable, Dict, List
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

# РЕЖИМ ПОИСКА:
# Пустой список = сканировать ВСЕ диски организации
# Заполненный список = сканировать только указанные диски
VD_HASH_LIST = [
    # 'hash1',
    # 'hash2',
    # 'hash3',
]

# Параметры работы
LIMIT_VD = 100
MAX_WORKERS = 10 
NUM_HELPERS_PER_DISK = 50

# Создаём единую папку для всех результатов
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
RESULTS_DIR = Path(__file__).parent / f'results_largest_files_{timestamp}'
RESULTS_DIR.mkdir(exist_ok=True)

# Все файлы результатов в одной папке
OUTPUT_FILE = RESULTS_DIR / f'largest_files_per_disk.csv'
SUMMARY_REPORT = RESULTS_DIR / f'summary_report.txt'
LOG_FILE = RESULTS_DIR / f'execution.log'

BATCH_WRITE_SIZE = 50
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
results_lock = Lock()

resources_rate_limiter = RateLimiter(max_calls_per_second=39)

stats = {
    'processed_disks': 0,
    'found_files_total': 0,
    'files_written': 0,
    'skipped_disks': 0,
    'gc_collections': 0,
    'total_api_calls': 0,
    'helper_workers_created': 0,
    'helper_tasks_processed': 0,
    'rate_limited_calls': 0,
    'empty_disks': 0
}

internet_stats = {
    'last_check': 0,
    'is_available': True,
    'reconnect_attempts': 0,
    'total_downtime': 0
}

# Словарь для хранения самого крупного файла на КАЖДОМ диске
largest_files_per_disk: Dict[str, dict] = {}

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


def format_file_size(size_bytes):
    """Форматирование размера файла в человекочитаемый вид"""
    if size_bytes is None or size_bytes == 'N/A' or size_bytes == 0:
        return 'N/A'
    
    try:
        size_bytes = int(size_bytes)
    except (ValueError, TypeError):
        return 'N/A'
    
    for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} ПБ"


class DiskLargestFileTracker:
    """Класс для отслеживания самого крупного файла на конкретном диске"""
    
    def __init__(self, vd_hash: str, disk_name: str):
        self.vd_hash = vd_hash
        self.disk_name = disk_name
        self.lock = Lock()
        self.largest_file = {
            'disk_name': disk_name,
            'vd_hash': vd_hash,
            'file_name': None,
            'file_path': None,
            'file_size': 0,
            'file_created': None,
            'file_modified': None,
            'media_type': None
        }
        self.files_count = 0
    
    def update(self, file_info: dict):
        """Обновление информации о самом крупном файле на этом диске"""
        try:
            current_size = file_info.get('file_size', 0)
            if current_size == 'N/A':
                return
            
            current_size = int(current_size)
            
            with self.lock:
                self.files_count += 1
                
                if current_size > self.largest_file['file_size']:
                    self.largest_file = {
                        'disk_name': self.disk_name,
                        'vd_hash': self.vd_hash,
                        'file_name': file_info.get('file_name'),
                        'file_path': file_info.get('file_path'),
                        'file_size': current_size,
                        'file_created': file_info.get('file_created'),
                        'file_modified': file_info.get('file_modified'),
                        'media_type': file_info.get('media_type')
                    }
        except (ValueError, TypeError):
            pass
    
    def get_result(self) -> dict:
        """Получение результата"""
        with self.lock:
            return self.largest_file.copy()
    
    def get_files_count(self) -> int:
        """Получение количества файлов"""
        with self.lock:
            return self.files_count


def cleanup_old_results(max_age_days: int = LOG_MAX_AGE_DAYS):
    """Очистка старых папок с результатами"""
    try:
        current_dir = Path(__file__).parent
        current_time = time.time()
        deleted_folders = 0
        
        for item in current_dir.iterdir():
            if item.is_dir() and item.name.startswith('results_largest_files_'):
                try:
                    folder_age = current_time - item.stat().st_mtime
                    if folder_age > max_age_days * 86400:
                        shutil.rmtree(item)
                        deleted_folders += 1
                        print(f'🧹 Удалена папка: {item.name}')
                except OSError as e:
                    print(f'⚠️ Не удалось удалить папку {item.name}: {e}', file=sys.stderr)
        
        if deleted_folders > 0:
            print(f'✅ Удалено старых папок: {deleted_folders}')
        else:
            print(f'ℹ️ Старые результаты не найдены')
            
    except Exception as e:
        print(f'⚠️ Ошибка очистки (не критично): {e}', file=sys.stderr)


def setup_logging():
    global logger
    
    logger = logging.getLogger('LargestFilePerDisk')
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
    logger.info(f'📁 Папка результатов: {RESULTS_DIR}')
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


def get_disk_info_by_hash(vd_hash: str) -> Optional[dict]:
    """Получение информации о диске по vd_hash"""
    try:
        session = get_session()
        url = 'https://cloud-api.yandex.net/v1/disk/virtual-disks/resources'
        params = {
            'path': f'vd:{vd_hash}:disk:/',
            'limit': 1,
            'fields': 'name,path'
        }
        headers = {
            'Authorization': f'OAuth {TOKEN_ORG}',
            'Accept': 'application/json'
        }
        
        response = session.get(url, params=params, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            name = data.get('name', vd_hash)
            return {
                'vd_hash': vd_hash,
                'name': name if name else vd_hash,
                'resource_id': None,
                'state': 'unknown',
                'description': ''
            }
        else:
            log_warning(f'Не удалось получить информацию о диске {vd_hash}: {response.status_code}')
            return {
                'vd_hash': vd_hash,
                'name': f'Диск {vd_hash}',
                'resource_id': None,
                'state': 'unknown',
                'description': ''
            }
            
    except Exception as e:
        log_error(f'Ошибка получения информации о диске {vd_hash}: {str(e)}')
        return {
            'vd_hash': vd_hash,
            'name': f'Диск {vd_hash}',
            'resource_id': None,
            'state': 'unknown',
            'description': ''
        }


def get_all_shared_disks() -> List[dict]:
    """Получение списка всех Общих Дисков организации"""
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


def get_disks_by_hash_list(hash_list: List[str]) -> List[dict]:
    """Получение списка дисков по списку vd_hash"""
    disks = []
    
    log_info(f'Загрузка информации о {len(hash_list)} указанных дисках...')
    
    for vd_hash in hash_list:
        if shutdown_flag.is_set():
            break
        
        vd_hash = vd_hash.strip()
        if not vd_hash:
            continue
        
        disk_info = get_disk_info_by_hash(vd_hash)
        if disk_info:
            disks.append(disk_info)
            log_info(f'  📁 {disk_info["name"]} (vd_hash: {vd_hash})')
    
    log_info(f'✓ Загружено дисков: {len(disks)}')
    return disks


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


def process_folder_task(vd_hash, path, disk_name, tracker: DiskLargestFileTracker, 
                       local_queue, active_tasks_counter):
    """Обработка папки для поиска самого крупного файла"""
    worker_id = threading.current_thread().name
    
    try:
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
                    relative_item_path = extract_relative_path(item_path, vd_hash)
                    
                    file_info = {
                        'disk_name': disk_name,
                        'vd_hash': vd_hash,
                        'file_name': item_name,
                        'file_path': relative_item_path,
                        'file_size': item.get('size', 'N/A'),
                        'file_created': item.get('created', 'N/A'),
                        'file_modified': item.get('modified', 'N/A'),
                        'media_type': item.get('media_type', 'N/A')
                    }
                    
                    tracker.update(file_info)
                
                elif item_type == 'dir':
                    relative_subdir_path = extract_relative_path(item_path, vd_hash)
                    subdirs_found.append(relative_subdir_path)
            
            if len(items) < limit or offset + limit >= total:
                break
            
            offset += limit
        
        if subdirs_found:
            with active_tasks_counter['lock']:
                active_tasks_counter['count'] += len(subdirs_found)
            
            for subdir_path in subdirs_found:
                local_queue.put(subdir_path)
        
    except Exception as e:
        log_error(f'[{worker_id}] Ошибка при обработке папки {path}: {str(e)}')
    finally:
        with active_tasks_counter['lock']:
            active_tasks_counter['count'] -= 1


def scan_disk_for_largest_file(vd_hash: str, disk_name: str) -> Optional[dict]:
    """Сканирование диска для поиска самого крупного файла"""
    worker_id = threading.current_thread().name
    timestamp_local = int(time.time())
    
    log_info('=' * 80)
    log_info(f'[{worker_id}] 🔍 Сканирование диска "{disk_name}"')
    log_info(f'[{worker_id}]    VD Hash: {vd_hash}')
    log_info('=' * 80)
    
    tracker = DiskLargestFileTracker(vd_hash, disk_name)
    
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
            log_error(f'Не удалось получить ресурсы для корня диска {disk_name}')
            return None
        
        emb = response.get('_embedded', {})
        items = emb.get('items', [])
        
        folders = []
        
        for item in items:
            item_type = item.get('type')
            item_name = item.get('name', 'N/A')
            item_path = item.get('path', 'N/A')
            
            if item_type == 'file':
                relative_item_path = extract_relative_path(item_path, vd_hash)
                
                file_info = {
                    'disk_name': disk_name,
                    'vd_hash': vd_hash,
                    'file_name': item_name,
                    'file_path': relative_item_path,
                    'file_size': item.get('size', 'N/A'),
                    'file_created': item.get('created', 'N/A'),
                    'file_modified': item.get('modified', 'N/A'),
                    'media_type': item.get('media_type', 'N/A')
                }
                
                tracker.update(file_info)
            
            elif item_type == 'dir':
                relative_path = extract_relative_path(item_path, vd_hash)
                folders.append(relative_path)
        
        log_info(f'[{worker_id}] Корневой уровень: {len(folders)} папок')
        
        if not folders:
            result = tracker.get_result()
            files_count = tracker.get_files_count()
            
            if result['file_name']:
                log_info(f'[{worker_id}] ✅ Диск "{disk_name}" обработан')
                log_info(f'[{worker_id}]    Файлов: {files_count}')
                log_info(f'[{worker_id}]    🏆 Крупнейший: {result["file_name"]} ({format_file_size(result["file_size"])})')
            else:
                log_info(f'[{worker_id}] ℹ️ Диск "{disk_name}" пуст')
                with stats_lock:
                    stats['empty_disks'] += 1
            
            return result
        
        local_queue = queue.Queue()
        
        with active_tasks_counter['lock']:
            active_tasks_counter['count'] = len(folders)
        
        for folder_path in folders:
            local_queue.put(folder_path)
        
        log_info(f'[{worker_id}] Создаём {NUM_HELPERS_PER_DISK} helper воркеров')
        
        with stats_lock:
            stats['helper_workers_created'] += NUM_HELPERS_PER_DISK
        
        stop_helpers = Event()
        
        def helper_worker(helper_id):
            helper_name = f'Helper_{disk_name[:10]}_{timestamp_local}_{helper_id}'
            threading.current_thread().name = helper_name
            
            tasks_processed = 0
            consecutive_empty = 0
            max_consecutive_empty = 30
            
            while not shutdown_flag.is_set() and not stop_helpers.is_set():
                try:
                    folder_path = local_queue.get(timeout=10.0)
                    consecutive_empty = 0
                    
                    process_folder_task(
                        vd_hash, 
                        folder_path, 
                        disk_name, 
                        tracker,
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
                        break
                    
                    continue
                
                except Exception as e:
                    log_error(f'[{helper_name}] Ошибка: {str(e)}')
                    continue
            
            return tasks_processed
        
        with ThreadPoolExecutor(
            max_workers=NUM_HELPERS_PER_DISK, 
            thread_name_prefix=f'Helper_{disk_name[:10]}_{timestamp_local}'
        ) as helper_executor:
            
            helper_futures = [
                helper_executor.submit(helper_worker, i+1)
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
                        vd_hash, 
                        folder_path, 
                        disk_name, 
                        tracker,
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
                    log_error(f'[{worker_id}] Ошибка в helper: {str(e)}')
        
        result = tracker.get_result()
        files_count = tracker.get_files_count()
        
        log_info('=' * 80)
        log_info(f'[{worker_id}] ✅ Диск "{disk_name}" завершён')
        log_info(f'[{worker_id}]    Просканировано файлов: {files_count}')
        
        if result['file_name']:
            log_info(f'[{worker_id}]    🏆 Самый крупный файл:')
            log_info(f'[{worker_id}]       Имя: {result["file_name"]}')
            log_info(f'[{worker_id}]       Путь: {result["file_path"]}')
            log_info(f'[{worker_id}]       Размер: {format_file_size(result["file_size"])}')
        else:
            log_info(f'[{worker_id}]    ℹ️ Файлы не найдены')
            with stats_lock:
                stats['empty_disks'] += 1
        
        log_info('=' * 80)
        log_info('')
        
        return result
        
    except Exception as e:
        log_error(f'Ошибка при сканировании диска: {str(e)}')
        import traceback
        log_error(traceback.format_exc())
        return None
    finally:
        gc.collect()


def update_stats(processed=0, files_found=0, skipped=0):
    with stats_lock:
        stats['processed_disks'] += processed
        stats['found_files_total'] += files_found
        stats['skipped_disks'] += skipped


def process_shared_disk(disk: dict, writer) -> tuple:
    """Обработка одного Общего Диска"""
    if shutdown_flag.is_set():
        return False, None
    
    start_time = time.time()
    worker_id = threading.current_thread().name
    
    try:
        disk_name = disk.get('name', 'N/A')
        vd_hash = disk.get('vd_hash')
        
        if not vd_hash:
            log_error(f'❌ Отсутствует vd_hash для диска "{disk_name}"')
            update_stats(skipped=1)
            return False, None
        
        result = scan_disk_for_largest_file(vd_hash, disk_name)
        
        if result:
            with results_lock:
                largest_files_per_disk[vd_hash] = result
            
            with csv_lock:
                if result['file_name']:
                    writer.writerow(result)
                else:
                    empty_row = {
                        'disk_name': disk_name,
                        'vd_hash': vd_hash,
                        'file_name': 'ПУСТО',
                        'file_path': 'N/A',
                        'file_size': 0,
                        'file_created': 'N/A',
                        'file_modified': 'N/A',
                        'media_type': 'N/A'
                    }
                    writer.writerow(empty_row)
                
                with stats_lock:
                    stats['files_written'] += 1
        
        update_stats(processed=1)
        
        elapsed_time = time.time() - start_time
        log_info(f'[{worker_id}] Время обработки "{disk_name}": {int(elapsed_time)}с')
        
        return True, result
        
    except Exception as e:
        if not shutdown_flag.is_set():
            log_error(f'❌ Ошибка "{disk.get("name", "N/A")}": {str(e)}')
            import traceback
            log_error(traceback.format_exc())
        update_stats(skipped=1)
        return False, None
    finally:
        close_session()
        gc.collect()


def print_and_save_results():
    """Вывод и сохранение итоговых результатов"""
    with results_lock:
        results = list(largest_files_per_disk.values())
    
    results_with_files = [r for r in results if r.get('file_name') and r.get('file_size', 0) > 0]
    results_with_files.sort(key=lambda x: x.get('file_size', 0), reverse=True)
    
    empty_disks = [r for r in results if not r.get('file_name') or r.get('file_size', 0) == 0]
    
    print('\n')
    print('=' * 100)
    print('🏆 САМЫЕ КРУПНЫЕ ФАЙЛЫ НА КАЖДОМ ОБЩЕМ ДИСКЕ')
    print('=' * 100)
    print(f'{"№":<4} {"Диск":<30} {"Размер":<15} {"Файл":<50}')
    print('-' * 100)
    
    for i, result in enumerate(results_with_files, 1):
        disk_name = result.get('disk_name', 'N/A')[:28]
        file_name = result.get('file_name', 'N/A')[:48]
        size_str = format_file_size(result.get('file_size', 0))
        print(f'{i:<4} {disk_name:<30} {size_str:<15} {file_name:<50}')
    
    if empty_disks:
        print('-' * 100)
        print(f'Пустых дисков: {len(empty_disks)}')
        for disk in empty_disks:
            print(f'  - {disk.get("disk_name", "N/A")}')
    
    print('=' * 100)
    
    if results_with_files:
        biggest = results_with_files[0]
        print('\n')
        print('🥇 АБСОЛЮТНЫЙ ЛИДЕР (самый крупный файл среди всех дисков):')
        print('-' * 100)
        print(f'   Диск:      {biggest.get("disk_name")}')
        print(f'   Файл:      {biggest.get("file_name")}')
        print(f'   Путь:      {biggest.get("file_path")}')
        print(f'   Размер:    {format_file_size(biggest.get("file_size"))} ({biggest.get("file_size"):,} байт)')
        print(f'   Создан:    {biggest.get("file_created")}')
        print(f'   Изменён:   {biggest.get("file_modified")}')
        print(f'   Тип:       {biggest.get("media_type")}')
        print('=' * 100)
    
    try:
        with open(SUMMARY_REPORT, 'w', encoding='utf-8') as f:
            f.write('=' * 100 + '\n')
            f.write('САМЫЕ КРУПНЫЕ ФАЙЛЫ НА КАЖДОМ ОБЩЕМ ДИСКЕ ЯНДЕКС 360\n')
            f.write('=' * 100 + '\n')
            f.write(f'Дата отчета: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
            f.write(f'Организация: {ORGID}\n')
            f.write(f'Всего дисков обработано: {len(results)}\n')
            f.write(f'Дисков с файлами: {len(results_with_files)}\n')
            f.write(f'Пустых дисков: {len(empty_disks)}\n')
            f.write('=' * 100 + '\n\n')
            
            f.write(f'{"№":<4} {"Диск":<35} {"Размер":<20} {"Файл"}\n')
            f.write('-' * 100 + '\n')
            
            for i, result in enumerate(results_with_files, 1):
                disk_name = result.get('disk_name', 'N/A')
                file_name = result.get('file_name', 'N/A')
                size_str = format_file_size(result.get('file_size', 0))
                f.write(f'{i:<4} {disk_name:<35} {size_str:<20} {file_name}\n')
            
            if empty_disks:
                f.write('\n' + '-' * 100 + '\n')
                f.write(f'ПУСТЫЕ ДИСКИ ({len(empty_disks)}):\n')
                for disk in empty_disks:
                    f.write(f'  - {disk.get("disk_name", "N/A")} (vd_hash: {disk.get("vd_hash")})\n')
            
            if results_with_files:
                biggest = results_with_files[0]
                f.write('\n' + '=' * 100 + '\n')
                f.write('АБСОЛЮТНЫЙ ЛИДЕР:\n')
                f.write('-' * 100 + '\n')
                f.write(f'Диск:      {biggest.get("disk_name")}\n')
                f.write(f'VD Hash:   {biggest.get("vd_hash")}\n')
                f.write(f'Файл:      {biggest.get("file_name")}\n')
                f.write(f'Путь:      {biggest.get("file_path")}\n')
                f.write(f'Размер:    {format_file_size(biggest.get("file_size"))} ({biggest.get("file_size"):,} байт)\n')
                f.write(f'Создан:    {biggest.get("file_created")}\n')
                f.write(f'Изменён:   {biggest.get("file_modified")}\n')
                f.write(f'Тип:       {biggest.get("media_type")}\n')
                f.write('=' * 100 + '\n')
        
        log_info(f'📝 Текстовый отчет сохранен: {SUMMARY_REPORT}')
        
    except Exception as e:
        log_error(f'Ошибка сохранения текстового отчета: {str(e)}')
    
    log_info('=' * 80)
    log_info('🏆 ИТОГОВЫЕ РЕЗУЛЬТАТЫ:')
    log_info(f'   Дисков обработано: {len(results)}')
    log_info(f'   Дисков с файлами: {len(results_with_files)}')
    log_info(f'   Пустых дисков: {len(empty_disks)}')
    
    if results_with_files:
        biggest = results_with_files[0]
        log_info(f'   🥇 Абсолютный лидер: {biggest.get("file_name")} ({format_file_size(biggest.get("file_size"))}) на диске "{biggest.get("disk_name")}"')
    
    log_info('=' * 80)


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
        cleanup_old_results()
    except Exception as e:
        print(f'⚠️ Ошибка очистки: {e}', file=sys.stderr)
    
    setup_logging()
    
    atexit.register(cleanup)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    log_info('=' * 80)
    log_info('🚀 ПОИСК САМОГО КРУПНОГО ФАЙЛА НА КАЖДОМ ОБЩЕМ ДИСКЕ')
    log_info('=' * 80)
    log_info(f'📁 Папка результатов: {RESULTS_DIR}')
    log_info(f'   📄 CSV-отчет: {OUTPUT_FILE.name}')
    log_info(f'   📝 Текстовый отчет: {SUMMARY_REPORT.name}')
    log_info(f'   📋 Лог выполнения: {LOG_FILE.name}')
    log_info('-' * 80)
    log_info(f'Организация: {ORGID}')
    log_info(f'Основных потоков: {MAX_WORKERS}')
    log_info(f'Helper\'ов на диск: {NUM_HELPERS_PER_DISK}')
    log_info(f'🚦 Rate Limit: 40 RPS')
    
    if VD_HASH_LIST:
        log_info(f'📋 РЕЖИМ: Конкретные диски ({len(VD_HASH_LIST)} шт.)')
        for h in VD_HASH_LIST:
            log_info(f'   - {h}')
    else:
        log_info('📋 РЕЖИМ: Все диски организации')
    
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
            
            if VD_HASH_LIST:
                all_disks = get_disks_by_hash_list(VD_HASH_LIST)
            else:
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
                    success, result = future.result(timeout=3600)
                    
                    if completed % 3 == 0 or completed == total_disks:
                        log_info('─' * 80)
                        log_info(f'📊 ПРОГРЕСС: {completed}/{total_disks} ({int(completed/total_disks*100)}%)')
                        log_info(f'   ✅ Обработано: {stats["processed_disks"]}')
                        log_info(f'   ⏭ Пропущено: {stats["skipped_disks"]}')
                        log_info(f'   📭 Пустых дисков: {stats["empty_disks"]}')
                        log_info(f'   🔧 API вызовов: {stats["total_api_calls"]}')
                        log_info('─' * 80)
                        log_info('')
                        
                except Exception as e:
                    if not shutdown_flag.is_set():
                        log_error(f'❌ Исключение "{disk.get("name", "N/A")}": {str(e)}')
                        update_stats(skipped=1)
            
            csvfile.flush()
            os.fsync(csvfile.fileno())
            
            print_and_save_results()
            
            log_info('')
            log_info('=' * 80)
            log_info('🎉 ОБРАБОТКА ЗАВЕРШЕНА!')
            log_info('=' * 80)
            log_info(f'📊 Всего дисков: {total_disks}')
            log_info(f'   ✅ Обработано: {stats["processed_disks"]}')
            log_info(f'   ⏭ Пропущено: {stats["skipped_disks"]}')
            log_info(f'   📭 Пустых дисков: {stats["empty_disks"]}')
            log_info('─' * 80)
            log_info(f'📁 Все результаты в папке: {RESULTS_DIR}')
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