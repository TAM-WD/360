'''
Скрипт для анализа Яндекс Дисков пользователей организации Яндекс 360. 
Позволяет собрать информацию о файлах всех пользователей или выборочно по списку UID.

ТРЕБОВАНИЯ:
Python 3.7+
Библиотека requests. Установить её можно так: pip install requests

ДОСТУПЫ И ТОКЕНЫ:
Токен OAuth приложения с правами directory:read_users
Сервисное приложение с правами cloud_api:disk.info

НАСТРОЙКА СКРИПТА ПЕРЕД ЗАПУСКОМ:
Откройте файл personal_disks_file_searcher.py и заполните секцию КОНФИГУРАЦИЯ

НАСТРОЙКА РАБОТЫ СКРИПТА:
USE_UID_LIST = False или True, где False = вся организация, True = список UID из файла
FILTER_DOMAIN_USERS_ONLY = False или True, где True = только доменные пользователи (uid > 1130000000000000)
SEARCH_FILE_NAME = '', где можно указать имя файла с расширением или без, а пустая строка = все файлы
MAX_WORKERS = 5, где указанное число = количество параллельных потоков. Настраивается индивидуально в зависимости от необходимой скорости и объёмов записей для поиска
URL для основного механизма функции disk_get_files_paginated_streaming можно конкретизировать.
Если требуется искать документ, то указать ссылку можно так:
url = f'https://cloud-api.yandex.net/v1/disk/resources/files?limit={DISK_LIMIT}&offset={offset}&media_type=document'

РЕЖИМЫ РАБОТЫ
Режим 1: Вся организация
Настройка:
USE_UID_LIST = False
FILTER_DOMAIN_USERS_ONLY = True  # только доменные пользователи
Результат: Обработает всех пользователей организации

Режим 2: Список UID
Настройка:
USE_UID_LIST = True
UID_LIST_FILE = 'uid_list.txt'

Создайте файл uid_list.txt:

1130000000000001
1130000000000002
1130000000000003

Формат:

Один UID на строку
Комментарии: строки с #
Пустые строки игнорируются
Результат: Обработает только пользователей из списка

ЗАПУСК

Windows
Откройте командную строку (cmd)
Перейдите в папку со скриптом:
cd C:\путь\к\скрипту
Запустите:
python "personal_disks_file_searcher.py"

Linux/macOS

cd /путь/к/скрипту
python3 personal_disks_file_searcher.py


РЕЗУЛЬТАТЫ РАБОТЫ СКРИПТА
CSV-отчет (disk_report_ГГГГММДД_ЧЧММСС.csv) и лог-файл (logs/disk_parser_ГГГГММДД_ЧЧММСС.log)

CSV-отчет содержит результаты поиска в определённом формате.

Название поля	Описание поля
email	        Email пользователя
uid	            UID пользователя
isEnabled	    Статус (True/False)
file_name	    Имя файла
file_path	    Полный путь
file_size	    Размер в байтах
file_created	Дата создания
file_modified	Дата изменения

Лог-файл содержит:
- Подробную информация о работе
- Ошибки и предупреждения
- Статистику обработки


ОСОБЕННОСТИ РАБОТЫ С ЗАБЛОКИРОВАННЫМИ ЧЕРЕЗ SCIM:
Если у пользователя установлен статус isEnabled = False:

1. Скрипт временно разблокирует пользователя (нужен SCIM_TOKEN)
2. Получит список файлов
3. Заблокирует обратно

Без SCIM токена: заблокированные пользователи будут пропущены

ПРИ ПОТЕРЕ ИНТЕРНЕТ-СОЕДИНЕНИЯ:
- Скрипт ждет восстановления до 10 минут
- Проверяет доступность каждые 10 секунд
- Продолжает работу после восстановления

ВОЗМОЖНЫЕ ПРОБЛЕМЫ:

Ошибка: "Не заполнены обязательные параметры"
Решение: Заполните ORGID, ORG_TOKEN, CLIENT_ID, CLIENT_SECRET

Ошибка: "Не удалось получить токен для uid ХХХ"
Причины:
Неверный CLIENT_ID или CLIENT_SECRET
У пользователя нет доступа к Диску
Пользователь не является доменным
Решение: Проверьте данные по сотруднику

Ошибка: "Файл со списком UID не найден"
Решение: Создайте файл uid_list.txt в папке со скриптом

Проблема: Медленная работа
Решение:
Увеличьте MAX_WORKERS
Используйте режим списка UID для выборочной обработки


ПОСЛЕ ЗАВЕРШЕНИЯ ВЫВОДИТСЯ ФИНАЛЬНАЯ СТАТИСТИКА:

========================================================================================
ОБРАБОТКА ЗАВЕРШЕНА!
========================================================================================
Обработано пользователей: 195/200
Пропущено пользователей: 5
Записано файлов: 15234
Сборок мусора: 45
Результаты CSV: disk_report_20240115_103045.csv
Лог-файл: logs/disk_parser_20240115_103045.log
========================================================================================

ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ:
Пример 1: Поиск конкретного файла у всех пользователей
USE_UID_LIST = False
FILTER_DOMAIN_USERS_ONLY = True
SEARCH_FILE_NAME = 'secret_document.pdf'

Пример 2: Полный аудит дисков конкретных пользователей
USE_UID_LIST = True
UID_LIST_FILE = 'directors.txt'
SEARCH_FILE_NAME = ''  # все файлы

Пример 3: Поиск по маске у всех пользователей
USE_UID_LIST = False
SEARCH_FILE_NAME = 'report_2024'  # найдет report_2024.xlsx, report_2024_final.pdf и т.д.

При возникновении проблем проверьте:
Лог-файл (logs/disk_parser_*.log)
Права доступа токенов
Наличие интернет-соединения
'''

import os
import requests  # type: ignore
from requests.adapters import HTTPAdapter, Retry  # type: ignore
from datetime import datetime
import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock, Event
import threading
import time
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Set, Callable, Any
import queue
import gc
import socket
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from collections import deque
from functools import wraps

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

ORGID = ''  # ID организации
ORG_TOKEN = ''  # токен OAuth приложения с правами directory:read_users

CLIENT_ID = ''  # id сервисного приложения с правами на чтение диска cloud_api:disk.info
CLIENT_SECRET = ''  # secret сервисного приложения с правами на чтение диска cloud_api:disk.info

DOMAINID = ''  # ID домена из конфига утилиты YandexADSCIM
SCIM_TOKEN = ''  # Токен из конфига утилиты YandexADSCIM

# Режимы работы
USE_UID_LIST = False  # True - по списку UID, False - вся организация
UID_LIST_FILE = 'uid_list.txt'
FILTER_DOMAIN_USERS_ONLY = True  # True - только uid > 1130000000000000
SEARCH_FILE_NAME = ''  # Имя файла для поиска

# Папка и файлы
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
LOGS_DIR = Path(__file__).parent / f'logs_{timestamp}'
LOGS_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = f'disk_report_{timestamp}.csv'
LOG_FILE = LOGS_DIR / f'disk_parser_{timestamp}.log'
SCIM_LOG_FILE = LOGS_DIR / f'scim_operations_{timestamp}.log'

# Параметры производительности
PERPAGE = 1000
DISK_LIMIT = 1000
MAX_WORKERS = 100
MAX_HELPERS = 200
MAX_RPS = 39
TOKEN_LIFETIME = 50 * 60
WORKER_IDLE_THRESHOLD = 30

RPS_MONITOR_INTERVAL = 5
RPS_LOG_INTERVAL = 30

MAX_PATHS_CACHE = 20000
PATHS_CLEANUP_THRESHOLD = 0.7
BATCH_WRITE_SIZE = 50
MAX_RECURSION_DEPTH = 100
GC_INTERVAL = 30

INTERNET_CHECK_TIMEOUT = 10 * 60
INTERNET_RETRY_INTERVAL = 10
INTERNET_CHECK_HOSTS = [
    ('8.8.8.8', 53),
    ('1.1.1.1', 53),
    ('oauth.yandex.ru', 443),
]

LOG_LEVEL = logging.INFO
LOG_MAX_SIZE = 10 * 1024 * 1024
LOG_BACKUP_COUNT = 0
LOG_MAX_AGE_DAYS = 30

csv_lock = Lock()
stats_lock = Lock()
internet_check_lock = Lock()
scim_lock = Lock()

stats = {
    'processed_users': 0,
    'files_written': 0,
    'gc_collections': 0,
    'skipped_users': 0,
    'http_requests': 0,
    'start_time': 0,
}

internet_stats = {
    'last_check': 0,
    'is_available': True,
    'reconnect_attempts': 0,
    'total_downtime': 0
}

scim_operations = {
    'unlocked_users': [],
    'locked_users': [],
    'failed_unlock': [],
    'failed_lock': []
}

logger: Optional[logging.Logger] = None
scim_logger: Optional[logging.Logger] = None
rate_limiter: Optional['RateLimiter'] = None
rps_monitor: Optional['RPSMonitor'] = None
task_manager: Optional['TaskManager'] = None
global_helpers_executor: Optional[ThreadPoolExecutor] = None


class RPSMonitor:
    
    def __init__(self, window_seconds: int = 10):
        self.window_seconds = window_seconds
        self.lock = Lock()
        
        self.oauth_requests = deque()
        self.directory_requests = deque()
        self.disk_requests = deque()
        self.scim_requests = deque()
        self.other_requests = deque()
        
        self.total_requests = 0
        self.start_time = time.time()
        
        self.interval_stats = []
        self.last_log_time = time.time()
        
        self.stop_event = Event()
        
        self.monitor_thread = threading.Thread(
            target=self._monitor_loop,
            daemon=False,
            name='RPSMonitor'
        )
        self.monitor_thread.start()
    
    def stop(self):
        self.stop_event.set()
        if self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5)
    
    def _monitor_loop(self):
        log_info('📊 RPS мониторинг запущен')
        
        try:
            while not self.stop_event.is_set():
                if self.stop_event.wait(timeout=RPS_MONITOR_INTERVAL):
                    break
                
                rps_data = self.get_current_rps()
                self.interval_stats.append(rps_data)
                
                if time.time() - self.last_log_time >= RPS_LOG_INTERVAL:
                    self._log_rps_stats(rps_data)
                    self.last_log_time = time.time()
        except Exception as e:
            log_error(f'❌ Ошибка в RPS Monitor: {e}')
        finally:
            log_info('📊 RPS мониторинг остановлен')
    
    def record_request(self, request_type: str = 'other'):
        current_time = time.time()
        
        with self.lock:
            self.total_requests += 1
            
            if request_type == 'oauth':
                self.oauth_requests.append(current_time)
            elif request_type == 'directory':
                self.directory_requests.append(current_time)
            elif request_type == 'disk':
                self.disk_requests.append(current_time)
            elif request_type == 'scim':
                self.scim_requests.append(current_time)
            else:
                self.other_requests.append(current_time)
    
    def _cleanup_old_requests(self, requests_queue: deque, current_time: float):
        cutoff_time = current_time - self.window_seconds
        while requests_queue and requests_queue[0] < cutoff_time:
            requests_queue.popleft()
    
    def get_current_rps(self) -> Dict[str, float]:
        current_time = time.time()
        
        with self.lock:
            self._cleanup_old_requests(self.oauth_requests, current_time)
            self._cleanup_old_requests(self.directory_requests, current_time)
            self._cleanup_old_requests(self.disk_requests, current_time)
            self._cleanup_old_requests(self.scim_requests, current_time)
            self._cleanup_old_requests(self.other_requests, current_time)
            
            oauth_rps = len(self.oauth_requests) / self.window_seconds
            directory_rps = len(self.directory_requests) / self.window_seconds
            disk_rps = len(self.disk_requests) / self.window_seconds
            scim_rps = len(self.scim_requests) / self.window_seconds
            other_rps = len(self.other_requests) / self.window_seconds
            
            total_rps = oauth_rps + directory_rps + disk_rps + scim_rps + other_rps
            
            elapsed = current_time - self.start_time
            avg_rps = self.total_requests / elapsed if elapsed > 0 else 0
            
            return {
                'oauth': round(oauth_rps, 2),
                'directory': round(directory_rps, 2),
                'disk': round(disk_rps, 2),
                'scim': round(scim_rps, 2),
                'other': round(other_rps, 2),
                'total': round(total_rps, 2),
                'average': round(avg_rps, 2),
                'total_count': self.total_requests
            }
    
    def _monitor_loop(self):
        log_info('📊 RPS мониторинг запущен')
        
        while True:
            time.sleep(RPS_MONITOR_INTERVAL)
            
            rps_data = self.get_current_rps()
            self.interval_stats.append(rps_data)
            
            if time.time() - self.last_log_time >= RPS_LOG_INTERVAL:
                self._log_rps_stats(rps_data)
                self.last_log_time = time.time()
    
    def _log_rps_stats(self, rps_data: Dict[str, float]):
        log_info('═' * 70)
        log_info('📊 МЕТРИКИ RPS (запросов в секунду)')
        log_info('─' * 70)
        log_info(f'🔑 OAuth (токены):     {rps_data["oauth"]:>6.2f} RPS')
        log_info(f'👥 Directory API:      {rps_data["directory"]:>6.2f} RPS')
        log_info(f'💾 Disk API:           {rps_data["disk"]:>6.2f} RPS')
        log_info(f'🔐 SCIM API:           {rps_data["scim"]:>6.2f} RPS')
        log_info(f'📡 Прочие:             {rps_data["other"]:>6.2f} RPS')
        log_info('─' * 70)
        log_info(f'📈 Текущий TOTAL:      {rps_data["total"]:>6.2f} RPS')
        log_info(f'📊 Средний (за всё время): {rps_data["average"]:>6.2f} RPS')
        log_info(f'🎯 Лимит:              {MAX_RPS:>6} RPS')
        log_info(f'📦 Всего запросов:     {rps_data["total_count"]}')
        
        usage_percent = (rps_data["total"] / MAX_RPS * 100) if MAX_RPS > 0 else 0
        log_info(f'⚡ Использование:      {usage_percent:>6.1f}%')
        log_info('═' * 70)


def make_http_request(request_func: Callable, request_type: str, *args, **kwargs):

    if rate_limiter:
        rate_limiter.acquire(1)
    
    try:
        response = request_func(*args, **kwargs)
        
        if rps_monitor:
            rps_monitor.record_request(request_type)
        
        with stats_lock:
            stats['http_requests'] += 1
        
        return response
    except Exception as e:
        if rps_monitor:
            rps_monitor.record_request(request_type)
        with stats_lock:
            stats['http_requests'] += 1
        raise e


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
        current_time = time.time()
        deleted_count = 0
        
        for log_file in LOGS_DIR.glob('*.log*'):
            try:
                file_age = current_time - log_file.stat().st_mtime
                if file_age > max_age_days * 86400:
                    log_file.unlink()
                    deleted_count += 1
            except OSError as e:
                print(f'❌ Ошибка удаления {log_file.name}: {e}', file=sys.stderr)
        
        if deleted_count > 0:
            print(f'🧹 Удалено старых логов: {deleted_count}')
            
    except Exception as e:
        print(f'❌ Ошибка очистки логов: {e}', file=sys.stderr)


def setup_logging():
    global logger, scim_logger
    
    logger = logging.getLogger('DiskParser')
    logger.setLevel(LOG_LEVEL)
    logger.propagate = False
    
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
    
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
    
    scim_logger = logging.getLogger('SCIMOperations')
    scim_logger.setLevel(logging.INFO)
    scim_logger.propagate = False
    
    scim_format = logging.Formatter(
        '%(asctime)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    scim_file_handler = CustomRotatingFileHandler(
        str(SCIM_LOG_FILE),
        mode='a',
        maxBytes=LOG_MAX_SIZE,
        backupCount=LOG_BACKUP_COUNT,
        encoding='utf-8',
        delay=True
    )
    scim_file_handler.setLevel(logging.INFO)
    scim_file_handler.setFormatter(scim_format)
    scim_logger.addHandler(scim_file_handler)
    
    logger.info('═' * 80)
    logger.info('🚀 Система логирования инициализирована')
    logger.info('─' * 80)
    logger.info(f'📁 Папка логов: {LOGS_DIR}')
    logger.info(f'📄 Основной лог: {LOG_FILE.name}')
    logger.info(f'🔐 SCIM лог: {SCIM_LOG_FILE.name}')
    logger.info(f'🔄 Ротация: каждые {LOG_MAX_SIZE // (1024*1024)} МБ')
    logger.info(f'📦 Количество файлов: {"безлимитное" if LOG_BACKUP_COUNT == 0 else LOG_BACKUP_COUNT}')
    logger.info(f'🗑️ Автоудаление: {LOG_MAX_AGE_DAYS} дней')
    logger.info('═' * 80)


def log_info(message):
    if logger:
        logger.info(message)
    else:
        print(f'[INFO] {message}')

def log_error(message):
    if logger:
        logger.error(message)
    else:
        print(f'[ERROR] {message}', file=sys.stderr)

def log_warning(message):
    if logger:
        logger.warning(message)
    else:
        print(f'[WARNING] {message}')


def log_scim(operation: str, uid: str, email: str, success: bool, details: str = ''):

    if scim_logger:
        status = '✅ SUCCESS' if success else '❌ FAILED'
        icon = '🔓' if operation == 'UNLOCK' else '🔒'
        message = f'{icon} {operation} | {status} | UID: {uid} | Email: {email}'
        if details:
            message += f' | {details}'
        scim_logger.info(message)


def load_uid_list(filename: str) -> Set[str]:
    uid_set = set()
    
    if not os.path.exists(filename):
        log_error(f'❌ Файл со списком UID не найден: {filename}')
        return uid_set
    
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                
                if not line or line.startswith('#'):
                    continue
                
                try:
                    uid_int = int(line)
                    uid = str(uid_int)
                    
                    if FILTER_DOMAIN_USERS_ONLY:
                        if uid_int > 1130000000000000:
                            uid_set.add(uid)
                            log_info(f'✅ Строка {line_num}: UID {uid} добавлен (доменный)')
                        else:
                            log_warning(f'⚠️ Строка {line_num}: UID {uid} пропущен (недоменный)')
                    else:
                        uid_set.add(uid)
                        log_info(f'✅ Строка {line_num}: UID {uid} добавлен')
                        
                except ValueError:
                    log_warning(f'⚠️ Строка {line_num}: Некорректный UID "{line}"')
                    continue
        
        log_info(f'📋 Загружено {len(uid_set)} валидных UID из {filename}')
        return uid_set
        
    except IOError as e:
        log_error(f'❌ Ошибка чтения файла {filename}: {e}')
        return uid_set


def is_domain_user(uid: str) -> bool:
    try:
        uid_int = int(uid)
        return uid_int > 1130000000000000
    except (ValueError, TypeError):
        log_warning(f'⚠️ Некорректный UID: {uid}')
        return False


def should_process_user(uid: str, email: str = 'N/A') -> bool:
    if not uid:
        log_warning(f'⚠️ Пустой UID для {email}')
        return False
    
    try:
        uid_int = int(uid)
    except (ValueError, TypeError):
        log_warning(f'⚠️ Некорректный UID "{uid}" для {email}')
        return False
    
    if FILTER_DOMAIN_USERS_ONLY:
        if uid_int <= 1130000000000000:
            log_info(f'👤 Пропуск недоменного: {email} (UID: {uid})')
            return False
    
    return True


def get_user_by_uid(uid: str) -> Optional[Dict]:
    if FILTER_DOMAIN_USERS_ONLY and not is_domain_user(uid):
        log_warning(f'⚠️ UID {uid} не является доменным, пропуск')
        return None
    
    try:
        url = f'https://api360.yandex.net/directory/v1/org/{ORGID}/users/{uid}'
        headers = {'Authorization': f'OAuth {ORG_TOKEN}'}
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        response = make_http_request(session.get, 'directory', url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            user_data = response.json()
            log_info(f'👤 Получена информация UID {uid}: {user_data.get("email", "N/A")}')
            return user_data
        elif response.status_code == 404:
            log_warning(f'⚠️ Пользователь UID {uid} не найден')
            return None
        else:
            log_error(f'❌ Ошибка получения UID {uid}: {response.status_code}')
            return None
            
    except Exception as e:
        log_error(f'❌ Исключение при получении UID {uid}: {e}')
        return None


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
    
    log_error('🌐 ⚠️ Потеря интернет-соединения!')
    log_info(f'🌐 Ожидание восстановления (макс. {max_wait_time // 60} мин)...')
    
    with internet_check_lock:
        internet_stats['is_available'] = False
        internet_stats['reconnect_attempts'] = 0
    
    while True:
        elapsed = time.time() - start_time
        
        if elapsed >= max_wait_time:
            log_error(f'🌐 ❌ Время ожидания истекло ({max_wait_time // 60} мин)')
            with internet_check_lock:
                internet_stats['total_downtime'] += elapsed
            return False
        
        attempt += 1
        remaining = max_wait_time - elapsed
        
        log_info(f'🌐 Попытка {attempt}: Проверка соединения... (осталось {int(remaining)}с)')
        
        if check_internet_connection():
            downtime = time.time() - start_time
            log_info(f'🌐 ✅ Соединение восстановлено! (простой: {int(downtime)}с)')
            
            with internet_check_lock:
                internet_stats['is_available'] = True
                internet_stats['reconnect_attempts'] = attempt
                internet_stats['total_downtime'] += downtime
                internet_stats['last_check'] = time.time()
            
            return True
        
        time.sleep(INTERNET_RETRY_INTERVAL)


def network_request_with_retry(func: Callable, *args, max_retries: int = 3, 
                              retry_delay: int = 5, **kwargs) -> Any:
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            result = func(*args, **kwargs)
            return result
        
        except requests.exceptions.RequestException as e:
            last_exception = e
            log_error(f'🌐 ❌ HTTP ошибка (попытка {attempt + 1}/{max_retries}): {e}')
            
            if isinstance(e, (requests.exceptions.ConnectionError, requests.exceptions.Timeout)):
                if not check_internet_connection():
                    log_error('🌐 Проблема с интернетом обнаружена')
                    
                    if not wait_for_internet_connection():
                        log_error('🌐 Не удалось восстановить соединение')
                        return None
                    
                    log_info('🌐 Повтор запроса после восстановления...')
                    continue
            
            if attempt < max_retries - 1:
                log_info(f'🌐 Ожидание {retry_delay}с перед повтором...')
                time.sleep(retry_delay)
        
        except (socket.error, OSError) as e:
            last_exception = e
            log_error(f'🌐 ❌ Ошибка сокета (попытка {attempt + 1}/{max_retries}): {e}')
            
            if not check_internet_connection():
                if not wait_for_internet_connection():
                    return None
                continue
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
    
    log_error(f'🌐 ❌ Все попытки исчерпаны: {last_exception}')
    return None

def scim_enable_user(user_id: str, email: str = 'N/A'):
    try:
        url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
        headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
        body = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "replace", "path": "active", "value": True}]
        }
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        response = make_http_request(session.patch, 'scim', url, json=body, headers=headers, timeout=30)
        
        if response.status_code in [200, 204]:
            log_info(f'🔓 Пользователь {email} (UID: {user_id}) временно разблокирован')
            log_scim('UNLOCK', user_id, email, True, 'Temporary unlock for disk scan')
            
            with scim_lock:
                scim_operations['unlocked_users'].append((user_id, email, time.time()))
            
            return True
        
        log_error(f'🔓 ❌ Ошибка разблокировки {email} (UID: {user_id}): {response.status_code}')
        log_scim('UNLOCK', user_id, email, False, f'HTTP {response.status_code}')
        
        with scim_lock:
            scim_operations['failed_unlock'].append((user_id, email, time.time()))
        
        return False
            
    except Exception as e:
        log_error(f'🔓 ❌ Исключение при разблокировке {email} (UID: {user_id}): {e}')
        log_scim('UNLOCK', user_id, email, False, f'Exception: {str(e)}')
        
        with scim_lock:
            scim_operations['failed_unlock'].append((user_id, email, time.time()))
        
        return False


def scim_disable_user(user_id: str, email: str = 'N/A'):
    try:
        url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
        headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
        body = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "replace", "path": "active", "value": False}]
        }
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        response = make_http_request(session.patch, 'scim', url, json=body, headers=headers, timeout=30)
        
        if response.status_code in [200, 204]:
            log_info(f'🔒 Пользователь {email} (UID: {user_id}) заблокирован обратно')
            log_scim('LOCK', user_id, email, True, 'Restored block after disk scan')
            
            with scim_lock:
                scim_operations['locked_users'].append((user_id, email, time.time()))
            
            return True
        
        log_error(f'🔒 ❌ Ошибка блокировки {email} (UID: {user_id}): {response.status_code}')
        log_scim('LOCK', user_id, email, False, f'HTTP {response.status_code}')
        
        with scim_lock:
            scim_operations['failed_lock'].append((user_id, email, time.time()))
        
        return False
            
    except Exception as e:
        log_error(f'🔒 ❌ Исключение при блокировке {email} (UID: {user_id}): {e}')
        log_scim('LOCK', user_id, email, False, f'Exception: {str(e)}')
        
        with scim_lock:
            scim_operations['failed_lock'].append((user_id, email, time.time()))
        
        return False

class RateLimiter:
    def __init__(self, max_rps: int):
        self.max_rps = max_rps
        self.tokens = max_rps
        self.max_tokens = max_rps
        self.last_update = time.time()
        self.lock = Lock()
        self.total_requests = 0
    
    def acquire(self, tokens: int = 1):
        sleep_time = 0
        
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.max_tokens, self.tokens + elapsed * self.max_rps)
            self.last_update = now
            
            if self.tokens < tokens:
                sleep_time = (tokens - self.tokens) / self.max_rps
            else:
                self.tokens -= tokens
                self.total_requests += tokens
        
        if sleep_time > 0:
            time.sleep(sleep_time)
            
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                self.tokens = min(self.max_tokens, self.tokens + elapsed * self.max_rps)
                self.last_update = now
                self.tokens -= tokens
                self.total_requests += tokens


class TokenManager:
    def __init__(self, uid):
        self.uid = uid
        self.token = None
        self.token_time = None
        self.token_lifetime = TOKEN_LIFETIME
        self.lock = Lock()
    
    def get_valid_token(self):
        with self.lock:
            current_time = time.time()
            if self.token is None or (current_time - self.token_time) >= self.token_lifetime:
                new_token = network_request_with_retry(get_token, self.uid)
                if new_token:
                    self.token = new_token
                    self.token_time = current_time
            return self.token


@dataclass
class WorkerActivity:
    worker_id: str
    last_activity: float
    current_task: Optional[str] = None
    is_busy: bool = False
    
    def update_activity(self, task: Optional[str] = None):
        self.last_activity = time.time()
        self.current_task = task
        self.is_busy = task is not None
    
    def idle_time(self) -> float:
        return time.time() - self.last_activity


@dataclass
class FolderTask:
    path: str
    token_manager: TokenManager
    search_name: str
    parent_task_id: str
    depth: int = 0
    user_email: str = 'N/A'
    user_uid: str = 'N/A'
    user_enabled: bool = True
    csv_writer: Optional[object] = None
    last_check_time: float = field(default_factory=time.time)


class TaskManager:
    
    def __init__(self, max_workers: int, max_helpers: int):
        self.max_workers = max_workers
        self.max_helpers = max_helpers
        self.workers: Dict[str, WorkerActivity] = {}
        self.workers_lock = threading.RLock()
        
        self.folder_queue = queue.Queue()
        self.completed_paths: Set[str] = set()
        self.in_progress_paths: Set[str] = set()
        self.queued_paths: Set[str] = set()
        self.paths_lock = Lock()
        
        self.active_recursive_tasks: Dict[str, FolderTask] = {}
        self.active_tasks_lock = Lock()
        
        self.user_tasks_count: Dict[str, int] = {}
        self.user_tasks_lock = Lock()
        
        self.shutdown_event = Event()
        self.helpers_active = 0
        self.helpers_lock = Lock()
        
        self.worker_stats: Dict[str, Dict] = {}
        self.worker_stats_lock = Lock()
        
        self.empty_monitor_iterations = 0
        self.paths_added = 0
    
    def increment_user_tasks(self, user_uid: str):
        with self.user_tasks_lock:
            self.user_tasks_count[user_uid] = self.user_tasks_count.get(user_uid, 0) + 1
            log_info(f'📊 [TASK_TRACK] UID {user_uid}: задач +1 = {self.user_tasks_count[user_uid]}')
    
    def decrement_user_tasks(self, user_uid: str):
        with self.user_tasks_lock:
            if user_uid in self.user_tasks_count:
                self.user_tasks_count[user_uid] = max(0, self.user_tasks_count[user_uid] - 1)
                log_info(f'📊 [TASK_TRACK] UID {user_uid}: задач -1 = {self.user_tasks_count[user_uid]}')
                
                if self.user_tasks_count[user_uid] == 0:
                    del self.user_tasks_count[user_uid]
                    log_info(f'✅ [TASK_TRACK] UID {user_uid}: все задачи завершены')
    
    def get_user_tasks_count(self, user_uid: str) -> int:
        with self.user_tasks_lock:
            return self.user_tasks_count.get(user_uid, 0)
    
    def wait_for_user_tasks(self, user_uid: str, timeout: int = 600, check_interval: int = 2):
        start_time = time.time()
        wait_iterations = 0
        last_tasks_count = -1
        stuck_iterations = 0
    
        while time.time() - start_time < timeout:
            tasks_count = self.get_user_tasks_count(user_uid)
        
            if tasks_count == 0:
                log_info(f'✅ [WAIT] UID {user_uid}: все задачи завершены за {int(time.time() - start_time)}с')
                return True
        
            if tasks_count == last_tasks_count:
                stuck_iterations += 1
            else:
                stuck_iterations = 0
            last_tasks_count = tasks_count
        
            if stuck_iterations >= 30:
                queue_size = self.folder_queue.qsize()
            
                with self.paths_lock:
                    in_progress = len(self.in_progress_paths)
                    queued = len(self.queued_paths)
                    completed = len(self.completed_paths)
            
                with self.helpers_lock:
                    helpers = self.helpers_active
            
                log_warning(f'⚠️ [WAIT] UID {user_uid}: DEADLOCK обнаружен!')
                log_warning(f'    Счетчик задач: {tasks_count}')
                log_warning(f'    Очередь: {queue_size}')
                log_warning(f'    В обработке: {in_progress}')
                log_warning(f'    В queued_paths: {queued}')
                log_warning(f'    Завершено: {completed}')
                log_warning(f'    Активных хелперов: {helpers}')
            
                if queue_size == 0 and in_progress == 0:
                    log_warning(f'⚠️ [WAIT] UID {user_uid}: Очередь и in_progress пусты!')
                    log_warning(f'⚠️ [WAIT] UID {user_uid}: Принудительный сброс {tasks_count} потерянных задач')
                
                    with self.user_tasks_lock:
                        if user_uid in self.user_tasks_count:
                            del self.user_tasks_count[user_uid]
                
                    return True
            
                if queue_size > 0 and helpers > 0:
                    log_warning(f'⚠️ [WAIT] UID {user_uid}: Очередь={queue_size}, но задачи не берутся!')
                    log_warning(f'⚠️ [WAIT] UID {user_uid}: Возможно, все пути в in_progress или completed')
                
                    with self.paths_lock:
                        temp_tasks = []
                        cleaned = 0
                    
                        try:
                            while True:
                                task = self.folder_queue.get_nowait()
                            
                                if task.path in self.completed_paths or task.path in self.in_progress_paths:
                                    self.queued_paths.discard(task.path)
                                    if task.user_uid:
                                        self.decrement_user_tasks(task.user_uid)
                                    cleaned += 1
                                else:
                                    temp_tasks.append(task)
                        except queue.Empty:
                            pass
                    
                        for task in temp_tasks:
                            self.folder_queue.put(task)
                    
                        log_warning(f'🧹 [WAIT] UID {user_uid}: Очищено {cleaned} дубликатов из очереди')
        
            wait_iterations += 1
        
            if wait_iterations % 5 == 0:
                elapsed = int(time.time() - start_time)
                queue_size = self.folder_queue.qsize()
                log_info(f'⏳ [WAIT] UID {user_uid}: осталось {tasks_count} задач, '
                         f'прошло {elapsed}с, очередь={queue_size}, stuck={stuck_iterations}')
        
            time.sleep(check_interval)
    
        remaining = self.get_user_tasks_count(user_uid)
        log_warning(f'⚠️ [WAIT] UID {user_uid}: timeout после {timeout}с, осталось {remaining} задач')
    
        with self.user_tasks_lock:
            if user_uid in self.user_tasks_count:
                del self.user_tasks_count[user_uid]
    
        return False

    
    def _cleanup_paths_cache(self):
        with self.paths_lock:
            total_paths = len(self.completed_paths)
            if total_paths > MAX_PATHS_CACHE * PATHS_CLEANUP_THRESHOLD:
                paths_to_keep = int(MAX_PATHS_CACHE * 0.4)
                paths_list = list(self.completed_paths)
                self.completed_paths = set(paths_list[-paths_to_keep:])
                
                removed = total_paths - len(self.completed_paths)
                log_info(f'🧹 [CLEANUP] Очищено {removed} путей, осталось {len(self.completed_paths)}')
                
                gc.collect()
                with stats_lock:
                    stats['gc_collections'] += 1
    
    def register_worker(self, worker_id: str):
        with self.workers_lock:
            self.workers[worker_id] = WorkerActivity(worker_id=worker_id, last_activity=time.time())
        with self.worker_stats_lock:
            self.worker_stats[worker_id] = {
                'tasks_completed': 0, 'files_found': 0, 'folders_processed': 0
            }
        log_info(f'⚙️ Воркер {worker_id} зарегистрирован')
    
    def update_worker_activity(self, worker_id: str, task: Optional[str] = None):
        with self.workers_lock:
            if worker_id in self.workers:
                self.workers[worker_id].update_activity(task)
    
    def update_worker_stats(self, worker_id: str, **kwargs):
        with self.worker_stats_lock:
            if worker_id not in self.worker_stats:
                self.worker_stats[worker_id] = {
                    'tasks_completed': 0, 'files_found': 0, 'folders_processed': 0
                }
            for key, value in kwargs.items():
                if key in self.worker_stats[worker_id]:
                    self.worker_stats[worker_id][key] += value
    
    def get_idle_workers(self, threshold: float = WORKER_IDLE_THRESHOLD) -> List[str]:
        idle = []
        with self.workers_lock:
            for worker_id, activity in self.workers.items():
                if not activity.is_busy and activity.idle_time() >= threshold:
                    idle.append(worker_id)
        return idle
    
    def has_active_tasks(self) -> bool:
        with self.active_tasks_lock:
            return len(self.active_recursive_tasks) > 0
    
    def register_recursive_task(self, task_id: str, root_path: str, 
                               token_manager: TokenManager, search_name: str,
                               user_email: str = 'N/A', user_uid: str = 'N/A',
                               user_enabled: bool = True, csv_writer: object = None):
        with self.active_tasks_lock:
            task = FolderTask(
                path=root_path, token_manager=token_manager,
                search_name=search_name, parent_task_id=task_id,
                depth=0, user_email=user_email, user_uid=user_uid,
                user_enabled=user_enabled, csv_writer=csv_writer
            )
            self.active_recursive_tasks[task_id] = task
            log_info(f'🎯 Зарегистрирована задача: {task_id} для {root_path}')
    
    def unregister_recursive_task(self, task_id: str):
        with self.active_tasks_lock:
            if task_id in self.active_recursive_tasks:
                del self.active_recursive_tasks[task_id]
                log_info(f'✅ Задача {task_id} завершена')
    
    def add_folder_task(self, task: FolderTask):
        added = False
    
        with self.paths_lock:
            if (task.path not in self.completed_paths and 
                task.path not in self.in_progress_paths and
                task.path not in self.queued_paths):
            
                self.queued_paths.add(task.path)
                added = True
            else:
                status = "completed" if task.path in self.completed_paths else \
                         "in_progress" if task.path in self.in_progress_paths else "queued"
                log_info(f'⚠️ [DUPLICATE_PREVENTED] {task.path}: {status}')
    
        if added:
            if task.user_uid:
                self.increment_user_tasks(task.user_uid)
        
            self.folder_queue.put(task)
        
            self.paths_added += 1
            if self.paths_added % 1000 == 0:
                self._cleanup_paths_cache()

    
    def mark_path_completed(self, path: str):
        with self.paths_lock:
            self.completed_paths.add(path)
            self.in_progress_paths.discard(path)
            self.queued_paths.discard(path)
    
    def get_folder_task(self, timeout: float = 2.0) -> Optional[FolderTask]:
        try:
            task = self.folder_queue.get(timeout=timeout)
        
            with self.paths_lock:
                self.queued_paths.discard(task.path)
            
                skip_reason = None
            
                if task.path in self.completed_paths:
                    skip_reason = "already_completed"
                elif task.path in self.in_progress_paths:
                    skip_reason = "in_progress"
            
                if skip_reason:
                    if task.user_uid:
                        self.decrement_user_tasks(task.user_uid)
                        log_info(f'⚠️ [TASK_SKIP] {task.path}: {skip_reason} для UID {task.user_uid}')
                    return None
            
                self.in_progress_paths.add(task.path)
                return task
            
        except queue.Empty:
            return None

    
    def shutdown(self):
        log_info('⚙️ Остановка TaskManager...')
        self.shutdown_event.set()


def get_token(uid):
    try:
        url = 'https://oauth.yandex.ru/token'
        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'subject_token': uid,
            'subject_token_type': 'urn:yandex:params:oauth:token-type:uid'
        }
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        response = make_http_request(session.post, 'oauth', url, data=data, timeout=30)
        
        if response.status_code == 200:
            return response.json().get('access_token')
        
        log_error(f'❌ Ошибка получения токена для uid {uid}: {response.status_code}')
        return None
        
    except Exception as e:
        log_error(f'❌ Исключение при получении токена uid {uid}: {e}')
        return None


def get_users(page):
    try:
        url = f'https://api360.yandex.net/directory/v1/org/{ORGID}/users?page={page}&perPage={PERPAGE}'
        headers = {'Authorization': f'OAuth {ORG_TOKEN}'}
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        response = make_http_request(session.get, 'directory', url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('pages', 0), data.get('users', [])
        
        log_error(f'❌ Ошибка получения пользователей (страница {page}): {response.status_code}')
        return None, None
        
    except Exception as e:
        log_error(f'❌ Исключение при получении пользователей (страница {page}): {e}')
        return None, None


def disk_get_resources(token_manager, path, limit=1000, offset=0):
    token = token_manager.get_valid_token()
    if not token:
        return None
    
    url = 'https://cloud-api.yandex.net/v1/disk/resources'
    params = {
        'path': path,
        'fields': 'path,type,name,size,created,modified,_embedded.total,_embedded.items.path,_embedded.items.type,_embedded.items.name,_embedded.items.size,_embedded.items.created,_embedded.items.modified',
        'limit': limit,
        'offset': offset,
    }
    headers = {'Authorization': f'OAuth {token}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        response = make_http_request(session.get, 'disk', url, params=params, headers=headers, timeout=30)
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        log_error(f'❌ Ошибка disk_get_resources для {path}: {e}')
        return None


def write_to_csv_batch(writer, batch):
    if not batch or not writer:
        return
    try:
        with csv_lock:
            if hasattr(writer, 'writerow'):
                for row in batch:
                    writer.writerow(row)
                with stats_lock:
                    stats['files_written'] += len(batch)
        batch.clear()
    except Exception as e:
        log_warning(f"⚠️ CSV write error: {e}")
        batch.clear()


def update_stats(processed=0, skipped=0):
    with stats_lock:
        stats['processed_users'] += processed
        stats['skipped_users'] += skipped


def get_files_recursive_single_folder_streaming(token_manager, path, search_name='', 
                                               user_email='N/A', user_uid='N/A', 
                                               user_enabled=True, csv_writer=None, 
                                               depth=0, files_count_ref=None):
    worker_id = threading.current_thread().name
    
    batch = []
    
    if task_manager:
        task_manager.update_worker_activity(worker_id, f'Обход: {path}')
    
    try:
        log_info(f'📁 [ITER] {worker_id}: Обход {path} (глубина: {depth})')
        
        offset = 0
        limit = 1000
        subdirs_found = []
        
        while True:
            response = network_request_with_retry(
                disk_get_resources, token_manager, path, limit, offset
            )
            
            if not response:
                log_warning(f'⚠️ [ITER] {worker_id}: Нет ответа для {path}')
                break
            
            emb = response.get('_embedded', {})
            items = emb.get('items', [])
            total = emb.get('total', 0)
            
            for item in items:
                item_type = item.get('type')
                
                if item_type == 'file':
                    if not search_name or search_name.lower() in item.get('name', '').lower():
                        file_row = {
                            'email': user_email,
                            'uid': user_uid,
                            'isEnabled': user_enabled,
                            'file_name': item.get('name', 'N/A'),
                            'file_path': item.get('path', 'N/A'),
                            'file_size': item.get('size', 'N/A'),
                            'file_created': item.get('created', 'N/A'),
                            'file_modified': item.get('modified', 'N/A')
                        }
                        
                        batch.append(file_row)
                        
                        if files_count_ref is not None:
                            files_count_ref['count'] += 1
                        
                        if len(batch) >= BATCH_WRITE_SIZE:
                            if csv_writer:
                                write_to_csv_batch(csv_writer, batch)
                            batch = []
                            
                            if files_count_ref and files_count_ref['count'] % GC_INTERVAL == 0:
                                gc.collect()
                                with stats_lock:
                                    stats['gc_collections'] += 1
                
                elif item_type == 'dir':
                    subdirs_found.append(item.get('path'))
            
            if len(items) < limit or offset + limit >= total:
                break
            
            offset += limit
            
            if task_manager:
                task_manager.update_worker_activity(worker_id, f'Обход: {path} ({offset}/{total})')
        
        if subdirs_found and task_manager and depth < MAX_RECURSION_DEPTH:
            log_info(f'📁 [ITER] {worker_id}: Найдено {len(subdirs_found)} подпапок в {path}, добавляем в очередь')
            
            for subdir_path in subdirs_found:
                folder_task = FolderTask(
                    path=subdir_path,
                    token_manager=token_manager,
                    search_name=search_name,
                    parent_task_id=f'recursive_{user_uid}_{int(time.time())}',
                    depth=depth + 1,
                    user_email=user_email,
                    user_uid=user_uid,
                    user_enabled=user_enabled,
                    csv_writer=csv_writer
                )
                task_manager.add_folder_task(folder_task)
        
        if batch and csv_writer:
            write_to_csv_batch(csv_writer, batch)
        
        if task_manager:
            task_manager.mark_path_completed(path)
        
        log_info(f'📁 [ITER] {worker_id}: Завершен {path}, файлов: {files_count_ref["count"] if files_count_ref else 0}, подпапок: {len(subdirs_found)}')
        
    except Exception as e:
        log_error(f'❌ [ITER] {worker_id}: Ошибка {path}: {e}')
    finally:
        if task_manager:
            task_manager.update_worker_activity(worker_id, None)
        gc.collect()


def get_files_with_dynamic_distribution(token_manager, path='disk:/', search_name='',
                                       user_email='N/A', user_uid='N/A', 
                                       user_enabled=True, csv_writer=None):
    try:
        response = network_request_with_retry(
            disk_get_resources, token_manager, path, limit=1000, offset=0
        )
        if not response:
            return 0
        
        emb = response.get('_embedded', {})
        items = emb.get('items', [])
        
        folders = []
        files_count = 0
        batch = []
        
        for item in items:
            if item.get('type') == 'file':
                if not search_name or search_name.lower() in item.get('name', '').lower():
                    file_row = {
                        'email': user_email, 'uid': user_uid, 'isEnabled': user_enabled,
                        'file_name': item.get('name', 'N/A'),
                        'file_path': item.get('path', 'N/A'),
                        'file_size': item.get('size', 'N/A'),
                        'file_created': item.get('created', 'N/A'),
                        'file_modified': item.get('modified', 'N/A')
                    }
                    batch.append(file_row)
                    files_count += 1
            
            elif item.get('type') == 'dir':
                folders.append(item.get('path'))
        
        if batch and csv_writer:
            write_to_csv_batch(csv_writer, batch)
        
        log_info(f'📁 [FALLBACK] Найдено {len(folders)} папок, {files_count} файлов для {user_email}')
        
        if folders and task_manager:
            log_info(f'📁 [FALLBACK] Добавление {len(folders)} папок для UID {user_uid}')
            
            for folder_path in folders:
                folder_task = FolderTask(
                    path=folder_path, token_manager=token_manager,
                    search_name=search_name, parent_task_id=f'fallback_{user_uid}_{int(time.time())}',
                    depth=1, user_email=user_email, user_uid=user_uid,
                    user_enabled=user_enabled, csv_writer=csv_writer
                )
                task_manager.add_folder_task(folder_task)
            
            log_info(f'📁 [FALLBACK] ✅ Добавлено {len(folders)} папок в очередь для UID {user_uid}')
        
        return files_count
    
    except Exception as e:
        log_error(f'❌ Ошибка fallback для {user_email}: {e}')
        return 0


def disk_get_files_paginated_streaming(token_manager, search_name='', 
                                       user_email='N/A', user_uid='N/A',
                                       user_enabled=True, csv_writer=None):
    offset = 0
    use_fallback = False
    error_count = 0
    files_count = 0
    batch = []
    max_iterations = 1000
    iterations = 0
    
    try:
        while True:
            iterations += 1
            if iterations > max_iterations:
                log_warning(f'⚠️ [{user_email}] Достигнут лимит итераций ({max_iterations}), переход на fallback')
                use_fallback = True
                break
            
            token = token_manager.get_valid_token()
            if not token:
                log_warning(f'⚠️ [{user_email}] Нет токена, переход на fallback')
                use_fallback = True
                break
            
            url = f'https://cloud-api.yandex.net/v1/disk/resources/files?limit={DISK_LIMIT}&offset={offset}'
            headers = {'Authorization': f'OAuth {token}'}
            
            try:
                retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
                session = requests.Session()
                session.mount('https://', HTTPAdapter(max_retries=retries))
                
                response = make_http_request(session.get, 'disk', url, headers=headers, timeout=30)
                
                if not response:
                    log_warning(f'⚠️ [{user_email}] Нет ответа от API, переход на fallback')
                    use_fallback = True
                    break
                
                error_count = 0
                
            except Exception as e:
                error_count += 1
                log_error(f'❌ [{user_email}] Ошибка (попытка {error_count}): {e}')
                
                if error_count >= 2:
                    log_warning(f'⚠️ [{user_email}] Слишком много ошибок, переход на fallback')
                    use_fallback = True
                    break
                time.sleep(3)
                continue
            
            if response.status_code == 500:
                error_count += 1
                if error_count >= 2:
                    log_warning(f'⚠️ [{user_email}] HTTP 500, переход на fallback')
                    use_fallback = True
                    break
                time.sleep(2)
                continue
            
            if response.status_code != 200:
                log_warning(f'⚠️ [{user_email}] HTTP {response.status_code}, переход на fallback')
                use_fallback = True
                break
            
            data = response.json()
            items = data.get('items', [])
            
            if not items:
                log_info(f'📄 [{user_email}] Нет больше файлов, завершение')
                break
            
            for item in items:
                if not search_name or search_name.lower() in item.get('name', '').lower():
                    file_row = {
                        'email': user_email, 'uid': user_uid, 'isEnabled': user_enabled,
                        'file_name': item.get('name', 'N/A'),
                        'file_path': item.get('path', 'N/A'),
                        'file_size': item.get('size', 'N/A'),
                        'file_created': item.get('created', 'N/A'),
                        'file_modified': item.get('modified', 'N/A')
                    }
                    batch.append(file_row)
                    files_count += 1
                    
                    if len(batch) >= BATCH_WRITE_SIZE:
                        if csv_writer:
                            write_to_csv_batch(csv_writer, batch)
                        batch = []
            
            if len(items) < DISK_LIMIT:
                log_info(f'📄 [{user_email}] Получено {len(items)} < {DISK_LIMIT}, завершение')
                break
            
            offset += DISK_LIMIT
            log_info(f'📄 [{user_email}] Обработано {offset} файлов, продолжение...')
            
            if offset % (DISK_LIMIT * 10) == 0:
                gc.collect()
        
        if batch and csv_writer:
            write_to_csv_batch(csv_writer, batch)
        
        if use_fallback:
            log_info('═' * 60)
            log_info(f'📁 FALLBACK для {user_email}: Рекурсивный обход')
            log_info('═' * 60)
            
            fallback_count = get_files_with_dynamic_distribution(
                token_manager, 'disk:/', search_name,
                user_email, user_uid, user_enabled, csv_writer
            )
            
            files_count += fallback_count
        else:
            log_info(f'✅ [{user_email}] Получение файлов завершено без fallback, файлов: {files_count}')
        
        return files_count
        
    except Exception as e:
        log_error(f'❌ [{user_email}] Исключение при получении файлов: {e}')
        
        if batch and csv_writer:
            write_to_csv_batch(csv_writer, batch)
        
        return files_count
    finally:
        gc.collect()


def global_helper_worker(helper_id: int):
    worker_name = f'GlobalHelper_{helper_id}'
    
    if task_manager:
        task_manager.register_worker(worker_name)
        with task_manager.helpers_lock:
            task_manager.helpers_active += 1
    
    log_info(f'🚀 [{worker_name}] Запущен')
    
    try:
        tasks_processed = 0
        consecutive_empty = 0
        last_log_time = time.time()
        
        while True:
            if task_manager and task_manager.shutdown_event.is_set():
                queue_size = task_manager.folder_queue.qsize()
                if queue_size == 0:
                    log_info(f'✅ [{worker_name}] Shutdown + пустая очередь, выход')
                    break
            
            folder_task = task_manager.get_folder_task(timeout=2.0) if task_manager else None
            
            if folder_task is None:
                consecutive_empty += 1
                
                now = time.time()
                if now - last_log_time >= 60 or consecutive_empty == 30:
                    queue_size = task_manager.folder_queue.qsize() if task_manager else 0
                    log_info(f'⏳ [{worker_name}] Пусто {consecutive_empty}, очередь={queue_size}')
                    last_log_time = now
                
                continue
            
            consecutive_empty = 0
            tasks_processed += 1
            
            log_info(f'📁 [{worker_name}] Взял задачу #{tasks_processed}: {folder_task.path}')
            
            try:
                files_count_ref = {'count': 0}
                get_files_recursive_single_folder_streaming(
                    folder_task.token_manager, folder_task.path,
                    folder_task.search_name, folder_task.user_email,
                    folder_task.user_uid, folder_task.user_enabled,
                    folder_task.csv_writer, depth=folder_task.depth,
                    files_count_ref=files_count_ref
                )
                
                log_info(f'✅ [{worker_name}] Завершил: {folder_task.path}, файлов: {files_count_ref["count"]}')
                
                if task_manager:
                    task_manager.update_worker_stats(worker_name, 
                        tasks_completed=1, files_found=files_count_ref['count'])
                    
            except Exception as e:
                log_error(f'❌ [{worker_name}] Ошибка: {e}')
            finally:
                if task_manager and folder_task.user_uid:
                    task_manager.decrement_user_tasks(folder_task.user_uid)
        
        log_info(f'🏁 [{worker_name}] Завершение, задач: {tasks_processed}')
    
    except Exception as e:
        log_error(f'❌ [{worker_name}] Ошибка: {e}')
    finally:
        if task_manager:
            with task_manager.helpers_lock:
                task_manager.helpers_active -= 1


def process_user(user, writer):
    start_time = time.time()
    worker_id = threading.current_thread().name
    
    log_info(f'🔵 [{worker_id}] ЗАПУСК process_user')
    
    if task_manager:
        with task_manager.workers_lock:
            if worker_id not in task_manager.workers:
                task_manager.register_worker(worker_id)
                log_info(f'🆕 [{worker_id}] Воркер зарегистрирован впервые')
        task_manager.update_worker_activity(worker_id, 'Обработка пользователя')
    
    try:
        email = user.get('email', 'N/A')
        uid = str(user.get('id', ''))
        is_enabled = user.get('isEnabled', False)
        
        log_info(f'👤 [{worker_id}] Проверка пользователя: {email} (UID: {uid})')
        
        
        if not should_process_user(uid, email):
            update_stats(skipped=1)
            return False, 0
        
        log_info(f'👤 Обработка пользователя: {email} (UID: {uid})')
        
        ban_needed = False
        
        if not is_enabled:
            log_info(f'🔓 Пользователь {email} заблокирован, временно разблокируем...')
            if SCIM_TOKEN and DOMAINID:
                ban_needed = network_request_with_retry(scim_enable_user, uid, email)
                if not ban_needed:
                    log_warning(f'⚠️ Не удалось разблокировать {email}')
                    return False, 0
            else:
                log_warning(f'⚠️ SCIM не настроен, пропуск {email}')
                return False, 0
        
        token_manager_user = TokenManager(uid)
        
        initial_token = token_manager_user.get_valid_token()
        if not initial_token:
            log_error(f'❌ Не удалось получить токен для {email}')
            if ban_needed:
                network_request_with_retry(scim_disable_user, uid, email)
            return False, 0
        
        log_info(f'📄 Получение списка файлов для {email}...')
        
        files_count = disk_get_files_paginated_streaming(
            token_manager_user, SEARCH_FILE_NAME,
            email, uid, is_enabled, writer
        )
        
        if task_manager:
            pending_tasks = task_manager.get_user_tasks_count(uid)
            if pending_tasks > 0:
                log_info(f'⏳ [{email}] Ожидание завершения {pending_tasks} задач fallback...')
                task_manager.wait_for_user_tasks(uid, timeout=900, check_interval=2)
        
        if files_count == 0:
            info = {
                'email': email, 'uid': uid, 'isEnabled': is_enabled,
                'file_name': 'N/A', 'file_path': 'N/A', 'file_size': 'N/A',
                'file_created': 'N/A', 'file_modified': 'N/A'
            }
            with csv_lock:
                writer.writerow(info)
        
        update_stats(processed=1)
        
        if ban_needed:
            network_request_with_retry(scim_disable_user, uid, email)
        
        elapsed_time = time.time() - start_time
        log_info(f'✅ Пользователь {email} обработан за {int(elapsed_time)}с, файлов: {files_count}')
        
        gc.collect()
        
        return True, files_count
        
    except Exception as e:
        log_error(f'❌ Ошибка при обработке {user.get("email", "N/A")}: {e}')
        return False, 0
    finally:
        if task_manager:
            task_manager.update_worker_activity(worker_id, None)


def main():
    global task_manager, rate_limiter, rps_monitor, global_helpers_executor
    
    cleanup_old_logs()
    setup_logging()
    
    log_info('═' * 80)
    log_info('🚀 ЗАПУСК СКРИПТА АНАЛИЗА ЯНДЕКС ДИСКА')
    log_info('═' * 80)
    log_info(f'🔧 Потоков: {MAX_WORKERS}, Хелперов: {MAX_HELPERS}, RPS: {MAX_RPS}')
    log_info(f'🔧 Батч-запись: {BATCH_WRITE_SIZE}, Макс. глубина: {MAX_RECURSION_DEPTH}')
    log_info(f'🔧 Макс. кеш путей: {MAX_PATHS_CACHE}')
    
    if USE_UID_LIST:
        log_info(f'🎯 Режим: СПИСОК UID из {UID_LIST_FILE}')
    else:
        log_info('🎯 Режим: ВСЯ ОРГАНИЗАЦИЯ')
    
    if FILTER_DOMAIN_USERS_ONLY:
        log_info('🎯 Фильтр: ТОЛЬКО доменные (uid > 1130000000000000)')
    else:
        log_info('🎯 Фильтр: ВСЕ пользователи')
    
    if SEARCH_FILE_NAME:
        log_info(f'🎯 Поиск: "{SEARCH_FILE_NAME}"')
    else:
        log_info('🎯 Поиск: ВСЕ файлы')
    
    log_info('═' * 80)
    
    log_info('🌐 Проверка интернет-соединения...')
    if not check_internet_connection():
        log_error('❌ Нет подключения к интернету!')
        if not wait_for_internet_connection():
            log_error('❌ Не удалось установить соединение. Завершение.')
            return
    log_info('✅ Интернет-соединение активно')
    
    if not all([ORGID, ORG_TOKEN, CLIENT_ID, CLIENT_SECRET]):
        log_error('❌ Не заполнены обязательные параметры!')
        return
    
    rate_limiter = RateLimiter(MAX_RPS)
    rps_monitor = RPSMonitor(window_seconds=10)
    task_manager = TaskManager(MAX_WORKERS, MAX_HELPERS)
    
    log_info('🚀 Запуск глобальных хелперов...')

    class NonDaemonThreadPoolExecutor(ThreadPoolExecutor):
        def _adjust_thread_count(self):
            def worker():
                try:
                    while True:
                        work_item = self._work_queue.get(block=True)
                        if work_item is not None:
                            work_item.run()
                            del work_item
                        else:
                            break
                except BaseException:
                    pass

            num_threads = len(self._threads)
            if num_threads < self._max_workers:
                thread_name = f'{self._thread_name_prefix or self}_{num_threads}'
                t = threading.Thread(name=thread_name, target=worker, daemon=False)
                t.start()
                self._threads.add(t)

    global_helpers_executor = ThreadPoolExecutor(
        max_workers=MAX_HELPERS, 
        thread_name_prefix='GlobalHelper'
    )

    for i in range(MAX_HELPERS):
        global_helpers_executor.submit(global_helper_worker, i)
        if i < MAX_HELPERS - 1:
            time.sleep(0.01)

    log_info(f'✅ Запущено {MAX_HELPERS} хелперов')


    csvfile = None
    writer = None
    
    try:
        csvfile = open(OUTPUT_FILE, 'w', newline='', encoding='utf-8-sig')
        writer = csv.DictWriter(csvfile, 
            ['email', 'uid', 'isEnabled', 'file_name', 'file_path', 'file_size', 'file_created', 'file_modified'], 
            delimiter=';')
        writer.writeheader()
        
        all_users = []
        
        if USE_UID_LIST:
            log_info('📋 Загрузка списка UID из файла...')
            uid_list = load_uid_list(UID_LIST_FILE)
            
            if not uid_list:
                log_error('❌ Список UID пуст!')
                return
            
            log_info(f'📋 Загружено {len(uid_list)} UID для обработки')
            
            for uid in uid_list:
                user_info = network_request_with_retry(get_user_by_uid, uid)
                if user_info:
                    all_users.append(user_info)
                else:
                    log_warning(f'⚠️ Пользователь UID {uid} не найден')
                    update_stats(skipped=1)
            
            log_info(f'👥 Найдено {len(all_users)} пользователей из {len(uid_list)} UID')
            
        else:
            log_info('👥 Получение списка пользователей из организации...')
            
            result = network_request_with_retry(get_users, 1)
            if result is None or result[0] is None:
                log_error('❌ Не удалось получить список пользователей')
                return
            
            response_pages = result[0]
            log_info(f'📄 Всего страниц: {response_pages}')
            
            for page in range(1, response_pages + 1):
                log_info(f'📄 Загрузка страницы {page} из {response_pages}')
                
                result = network_request_with_retry(get_users, page)
                if result is None:
                    continue
                
                _, users = result
                if users is None:
                    continue
                
                all_users.extend(users)
        
        total_users = len(all_users)
        log_info(f'👥 Всего пользователей для обработки: {total_users}')
        
        if total_users == 0:
            log_warning('⚠️ Нет пользователей для обработки!')
            return
        
        with ThreadPoolExecutor(max_workers=MAX_WORKERS, thread_name_prefix='Worker') as executor:
            log_info(f'🚀 Создание {len(all_users)} задач для {MAX_WORKERS} воркеров...')
    
            futures = {
                executor.submit(process_user, user, writer): user
                for user in all_users
            }
    
            log_info(f'✅ Создано {len(futures)} futures, ожидание выполнения...')
    
            completed_count = 0
            for future in as_completed(futures):
                completed_count += 1
                user = futures[future]
                try:
                    result = future.result()
                    if completed_count % 10 == 0:
                        log_info(f'📊 Прогресс: {completed_count}/{total_users}, '
                               f'обработано: {stats["processed_users"]}, '
                               f'пропущено: {stats["skipped_users"]}, '
                               f'файлов записано: {stats["files_written"]}, '
                               f'GC: {stats["gc_collections"]}')
                        gc.collect()
                except Exception as e:
                    log_error(f'❌ Исключение при обработке {user.get("email", "N/A")}: {e}')
        
        log_info('⏳ Пользователи обработаны, ожидание хелперов...')
        
        max_wait = 1800
        wait_start = time.time()
        stable_empty = 0
        
        while time.time() - wait_start < max_wait:
            queue_size = task_manager.folder_queue.qsize()
            with task_manager.paths_lock:
                in_progress = len(task_manager.in_progress_paths)
            with task_manager.helpers_lock:
                helpers = task_manager.helpers_active
            
            log_info(f'⏳ Ожидание: очередь={queue_size}, в работе={in_progress}, хелперов={helpers}')
            
            if queue_size == 0 and in_progress == 0:
                stable_empty += 1
                if stable_empty >= 6:
                    log_info('✅ Очередь стабильно пуста')
                    break
            else:
                stable_empty = 0
            
            time.sleep(5)
        
        log_info('⚙️ Начало graceful shutdown...')
        
        if task_manager:
            task_manager.shutdown()
            log_info('✅ TaskManager остановлен')
        
        log_info('⏳ Ожидание завершения хелперов (10 сек)...')
        time.sleep(10)
        
        if global_helpers_executor:
            log_info('⚙️ Остановка GlobalHelpers executor...')
            global_helpers_executor.shutdown(wait=True, cancel_futures=False)
            log_info('✅ GlobalHelpers executor остановлен')
        
        if rps_monitor:
            log_info('⚙️ Остановка RPS Monitor...')
            rps_monitor.stop()
            log_info('✅ RPS Monitor остановлен')
        
        if csvfile:
            csvfile.flush()
            os.fsync(csvfile.fileno())
            csvfile.close()
            log_info('✅ CSV файл закрыт')
        
        log_info('═' * 80)
        log_info('🏁 ОБРАБОТКА ЗАВЕРШЕНА!')
        log_info('═' * 80)
        log_info(f'👥 Обработано пользователей: {stats["processed_users"]}/{total_users}')
        log_info(f'⏭️ Пропущено пользователей: {stats["skipped_users"]}')
        log_info(f'💾 Записано файлов в CSV: {stats["files_written"]}')
        log_info(f'🧹 Сборок мусора: {stats["gc_collections"]}')
        
        if rps_monitor:
            final_rps = rps_monitor.get_current_rps()
            log_info('─' * 80)
            log_info('📊 ИТОГОВАЯ СТАТИСТИКА RPS:')
            log_info(f'   Всего HTTP запросов: {stats["http_requests"]}')
            log_info(f'   Средний RPS: {final_rps["average"]:.2f}')
            log_info(f'   Макс. лимит RPS: {MAX_RPS}')
        
        with scim_lock:
            if scim_operations['unlocked_users'] or scim_operations['locked_users']:
                log_info('─' * 80)
                log_info('🔐 SCIM ОПЕРАЦИИ:')
                log_info(f'   🔓 Разблокировано: {len(scim_operations["unlocked_users"])}')
                log_info(f'   🔒 Заблокировано обратно: {len(scim_operations["locked_users"])}')
                log_info(f'   ❌ Ошибок разблокировки: {len(scim_operations["failed_unlock"])}')
                log_info(f'   ❌ Ошибок блокировки: {len(scim_operations["failed_lock"])}')
                log_info(f'   📋 Подробности в: {SCIM_LOG_FILE.name}')
        
        with internet_check_lock:
            if internet_stats['reconnect_attempts'] > 0:
                log_info('─' * 80)
                log_info('🌐 ИНТЕРНЕТ-СОЕДИНЕНИЕ:')
                log_info(f'   Переподключений: {internet_stats["reconnect_attempts"]}')
                log_info(f'   Общее время простоя: {int(internet_stats["total_downtime"])}с')
        
        log_info('─' * 80)
        log_info(f'💾 Результаты CSV: {OUTPUT_FILE}')
        log_info(f'📄 Лог-файл: {LOG_FILE.name}')
        log_info('═' * 80)
        
    except KeyboardInterrupt:
        log_warning('⚠️ Прервано пользователем (Ctrl+C)')
    except Exception as e:
        log_error(f'❌ Критическая ошибка: {e}')
        import traceback
        log_error(traceback.format_exc())
    finally:
        log_info('🧹 Финальная очистка ресурсов...')
        
        if csvfile and not csvfile.closed:
            try:
                csvfile.close()
                log_info('✅ CSV файл закрыт в finally')
            except Exception as e:
                log_error(f'❌ Ошибка закрытия CSV: {e}')
        
        if task_manager:
            try:
                task_manager.shutdown()
            except Exception:
                pass
        
        if global_helpers_executor:
            try:
                global_helpers_executor.shutdown(wait=False, cancel_futures=True)
                log_info('✅ Executor остановлен принудительно')
            except Exception as e:
                log_error(f'❌ Ошибка остановки executor: {e}')
        
        gc.collect()
        
        if logger:
            log_info('🏁 Завершение работы скрипта')
            time.sleep(0.5)
            for handler in logger.handlers[:]:
                try:
                    handler.flush()
                    handler.close()
                    logger.removeHandler(handler)
                except Exception:
                    pass
        
        if scim_logger:
            for handler in scim_logger.handlers[:]:
                try:
                    handler.flush()
                    handler.close()
                    scim_logger.removeHandler(handler)
                except Exception:
                    pass
        
        time.sleep(1)


if __name__ == '__main__':
    main()
