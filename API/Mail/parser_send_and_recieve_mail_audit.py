#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Сбор статистики почтовой активности из аудит-логов Яндекс 360:
- Количество отправленных/полученных писем
- Статистика по дням
- Статистика после 19:00 (вечерние письма)
- Многопоточная обработка
'''

import requests
import time
import csv
import logging
import os
import sys
import json
import threading
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, Any, Optional, List, Tuple
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
import gc
import traceback

_original_excepthook = sys.excepthook
def _custom_excepthook(exc_type, exc_value, exc_tb):
    if exc_type is TypeError and "NoneType" in str(exc_value) and "context manager" in str(exc_value):
        return
    _original_excepthook(exc_type, exc_value, exc_tb)
sys.excepthook = _custom_excepthook

def _custom_unraisablehook(unraisable):
    if unraisable.exc_type is TypeError:
        exc_str = str(unraisable.exc_value)
        if "NoneType" in exc_str and "context manager" in exc_str:
            return
    sys.__unraisablehook__(unraisable)
sys.unraisablehook = _custom_unraisablehook

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("⚠️ psutil не установлен. Мониторинг памяти отключён.")
    print("   Установите: pip install psutil")

# ============================================================================
# НАСТРОЙКИ
# ============================================================================

# OAuth токен для доступа к API с правами ya360_security:audit_log_mail
OAUTH_TOKEN = ""

# ID организации
ORG_ID = ""

# ----------------------------------------------------------------------------
# ЧАСОВОЙ ПОЯС
# ----------------------------------------------------------------------------
# UTC offset в часах
# Примеры: 
#   3  - Москва (UTC+3)
#   5  - Екатеринбург (UTC+5)
#   -5 - Нью-Йорк (UTC-5)
#   0  - Лондон (UTC+0)
TIMEZONE_OFFSET_HOURS = 3  # Москва по умолчанию

# ----------------------------------------------------------------------------
# ПЕРИОД СБОРА ДАННЫХ
# ----------------------------------------------------------------------------
# Формат дат: "YYYY-MM-DD" - ОБЯЗАТЕЛЬНО В КАВЫЧКАХ!
# 
# Если DATE_FROM и DATE_TO заданы - используется указанный период
# Если None - используется автоматический расчёт на основе PERIOD_DAYS
#
# Примеры:
#   DATE_FROM = "2024-01-01"
#   DATE_TO = "2024-01-31"
#
#   DATE_FROM = None  # автоматический расчёт
#   DATE_TO = None

DATE_FROM = None  # Начало периода (или None для автоматического расчёта)
DATE_TO = None    # Конец периода (или None для автоматического расчёта)

# Период по умолчанию в днях (используется если DATE_FROM/DATE_TO = None)
# Минимум: 1, Максимум: 31
PERIOD_DAYS = 31  # Неделя по умолчанию

# ----------------------------------------------------------------------------
# ВЕЧЕРНЕЕ ВРЕМЯ
# ----------------------------------------------------------------------------
# Час, после которого письма считаются "вечерними" (в локальном времени)
# Используется для статистики активности после рабочего дня
EVENING_HOUR = 19  # После 19:00

# ----------------------------------------------------------------------------
# ПАРАМЕТРЫ API
# ----------------------------------------------------------------------------
# Количество записей на страницу (максимум 100)
PAGE_SIZE = 100

# Количество воркеров для параллельной обработки
# Каждый воркер обрабатывает свой диапазон дат
NUM_WORKERS = 10

# ----------------------------------------------------------------------------
# НАСТРОЙКИ RETRY И ЗАЩИТЫ
# ----------------------------------------------------------------------------

# Максимальное количество повторных попыток при ошибках
MAX_RETRIES = 5

# Базовая задержка для retry (секунды), увеличивается экспоненциально
BASE_RETRY_DELAY = 2

# Максимальное время ожидания восстановления сети (минуты)
NETWORK_RECOVERY_TIMEOUT_MINUTES = 30

# Задержка между запросами (секунды)
REQUEST_DELAY = 0.1

# Порог использования памяти (%) для принудительного сброса буфера
MEMORY_THRESHOLD_PERCENT = 80

# Размер батча для записи в файл (количество событий)
BATCH_SIZE_FOR_FLUSH = 5000

# Интервал автосохранения прогресса (секунды)
CHECKPOINT_INTERVAL_SECONDS = 60

# ============================================================================
# КОНЕЦ НАСТРОЕК
# ============================================================================

API_URL = "https://api360.yandex.net/security/v1/org/{org_id}/audit_log/mail"

EVENT_TYPE_RECEIVE = "message_receive"
EVENT_TYPE_SEND = "mailbox_send"


@dataclass
class WorkerTask:
    worker_id: int
    date_from: datetime
    date_to: datetime

@dataclass  
class WorkerResult:
    worker_id: int
    events_count: int
    send_count: int
    receive_count: int
    errors_count: int
    completed: bool = True
    error_message: Optional[str] = None


class MailStatsCollector:
    
    def __init__(
        self,
        oauth_token: str,
        org_id: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        period_days: int = 7,
        timezone_offset_hours: int = 3,
        evening_hour: int = 19,
        num_workers: int = 10
    ):
        self.oauth_token = oauth_token
        self.org_id = org_id
        self.api_url = API_URL.format(org_id=org_id)
        self.evening_hour = evening_hour
        self.num_workers = num_workers
        self.timezone_offset_hours = timezone_offset_hours
        
        # Часовой пояс
        self.tz_offset = timedelta(hours=timezone_offset_hours)
        self.tz = timezone(self.tz_offset)
        self.tz_name = f"UTC{'+' if timezone_offset_hours >= 0 else ''}{timezone_offset_hours}"
        
        # Период (с валидацией)
        period_days = max(1, min(31, period_days))
        self.date_from, self.date_to = self._setup_dates(date_from, date_to, period_days)
        
        # Рабочая директория
        self.output_dir = self._create_output_directory()
        
        # Логирование
        self._setup_logging()
        
        # Thread-safe структуры
        self._stats_lock = threading.Lock()
        self._file_lock = threading.Lock()
        self._buffer_lock = threading.Lock()
        
        # Счётчики
        self._events_processed = 0
        self._api_calls = 0
        self._retries_count = 0
        self._errors_count = 0
        
        # Статистика
        self.stats = {
            'total_sent': 0,
            'total_received': 0,
            'total_sent_evening': 0,
            'total_received_evening': 0,
            'daily_sent': defaultdict(int),
            'daily_received': defaultdict(int),
            'daily_sent_evening': defaultdict(int),
            'daily_received_evening': defaultdict(int),
            'unique_users': set()
        }
        
        # Статистика по пользователям
        self.user_stats: Dict[str, Dict] = defaultdict(lambda: {
            'email': '', 'name': '', 'uid': '',
            'sent_total': 0, 'received_total': 0,
            'sent_evening': 0, 'received_evening': 0,
            'sent_by_day': defaultdict(int),
            'received_by_day': defaultdict(int),
            'sent_evening_by_day': defaultdict(int),
            'received_evening_by_day': defaultdict(int)
        })
        
        # Буфер и файлы
        self._events_buffer: List[Dict] = []
        self._checkpoint_file = os.path.join(self.output_dir, "checkpoint.json")
        self._last_checkpoint_time = time.time()
        
        self._csv_file = None
        self._csv_writer = None
        self._csv_initialized = False
        
        # Флаг остановки
        self._stop_flag = threading.Event()
    
    def _get_current_time_local(self) -> datetime:
        """Возвращает текущее время в локальном часовом поясе."""
        return datetime.now(self.tz)
    
    def _utc_to_local(self, dt: datetime) -> datetime:
        """Конвертирует UTC время в локальный часовой пояс."""
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(self.tz)
    
    def _format_local_datetime(self, dt: datetime) -> str:
        if dt is None:
            return "N/A"
        return self._utc_to_local(dt).strftime("%Y-%m-%d %H:%M:%S")
    
    def _format_local_date(self, dt: datetime) -> str:
        if dt is None:
            return "N/A"
        return self._utc_to_local(dt).strftime("%Y-%m-%d")
    
    def _format_local_time(self, dt: datetime) -> str:
        if dt is None:
            return "N/A"
        return self._utc_to_local(dt).strftime("%H:%M:%S")
    
    def _is_evening(self, dt: datetime) -> bool:
        """Проверяет, является ли время вечерним."""
        if dt is None:
            return False
        return self._utc_to_local(dt).hour >= self.evening_hour
    
    def _setup_dates(
        self, 
        date_from: Optional[str], 
        date_to: Optional[str], 
        period_days: int
    ) -> Tuple[datetime, datetime]:
        """
        Настраивает даты периода.
        
        Логика:
        1. Если обе даты указаны - используем их (с ограничением 31 день)
        2. Если даты не указаны - рассчитываем от текущей даты
        """
        if date_from and date_to:
            # Парсим указанные даты
            from_dt = self._parse_date(date_from)
            to_dt = self._parse_date(date_to)
            
            # Проверяем порядок дат
            if from_dt > to_dt:
                from_dt, to_dt = to_dt, from_dt
                print(f"⚠️ Даты переставлены: {from_dt.strftime('%Y-%m-%d')} - {to_dt.strftime('%Y-%m-%d')}")
            
            if (to_dt - from_dt).days > 31:
                to_dt = from_dt + timedelta(days=31)
                print(f"⚠️ Период ограничен до 31 дня: {from_dt.strftime('%Y-%m-%d')} - {to_dt.strftime('%Y-%m-%d')}")
        else:
            # Автоматический расчёт от текущей даты
            local_now = self._get_current_time_local()
            to_dt = local_now.replace(tzinfo=None)
            from_dt = to_dt - timedelta(days=period_days)
            print(f"📅 Автоматический период: последние {period_days} дней")
        
        # Устанавливаем время начала и конца дня
        from_dt = from_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = to_dt.replace(hour=23, minute=59, second=59, microsecond=0)
        
        return from_dt, to_dt
    
    def _parse_date(self, date_str: str) -> datetime:
        """
        Парсит строку даты в datetime.
        
        Поддерживаемые форматы:
        - YYYY-MM-DD (2024-01-15)
        - DD.MM.YYYY (15.01.2024)
        - DD/MM/YYYY (15/01/2024)
        """
        if isinstance(date_str, datetime):
            return date_str
        
        if not date_str:
            raise ValueError("Дата не может быть пустой")
        
        date_str = str(date_str).strip()
        
        formats = [
            "%Y-%m-%d",   # 2024-01-15
            "%d.%m.%Y",   # 15.01.2024
            "%d/%m/%Y",   # 15/01/2024
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        raise ValueError(
            f"Не удалось распарсить дату: '{date_str}'. "
            f"Используйте формат YYYY-MM-DD (например, 2024-01-15)"
        )
    
    def _create_output_directory(self) -> str:
        """Создаёт директорию для результатов."""
        timestamp = self._get_current_time_local().strftime("%Y%m%d_%H%M%S")
        dir_name = f"{timestamp}_mail_stats"
        os.makedirs(dir_name, exist_ok=True)
        return dir_name
    
    def _setup_logging(self):
        """Настройка логирования."""
        log_file = os.path.join(self.output_dir, "execution.log")
        
        class LocalTimeFormatter(logging.Formatter):
            def __init__(self, fmt, datefmt, tz):
                super().__init__(fmt, datefmt)
                self.tz = tz
            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=self.tz)
                return dt.strftime(datefmt) if datefmt else dt.isoformat()
        
        formatter = LocalTimeFormatter(
            '%(asctime)s - %(levelname)s - [%(threadName)s] %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            tz=self.tz
        )
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        self.logger = logging.getLogger(f"MailStats_{id(self)}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.handlers.clear()
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    
    def _format_datetime_for_api(self, dt: datetime) -> str:
        """Форматирует datetime для API в ISO 8601."""
        if self.timezone_offset_hours >= 0:
            offset_str = f"+{self.timezone_offset_hours:02d}:00"
        else:
            offset_str = f"{self.timezone_offset_hours:03d}:00"
        
        return dt.strftime("%Y-%m-%dT%H:%M:%S") + offset_str
    
    def _parse_event_datetime(self, date_str: str) -> Optional[datetime]:
        """Парсит дату события из API."""
        if not date_str:
            return None
        try:
            if '+' in date_str or date_str.endswith('Z'):
                clean_ts = date_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(clean_ts)
                return dt.astimezone(timezone.utc)
            else:
                return datetime.fromisoformat(date_str).replace(tzinfo=timezone.utc)
        except:
            return None
    
    def _check_memory_usage(self) -> bool:
        """Проверяет использование памяти."""
        if not PSUTIL_AVAILABLE:
            return False
        try:
            mem_percent = psutil.Process().memory_percent()
            sys_mem = psutil.virtual_memory().percent
            if mem_percent > MEMORY_THRESHOLD_PERCENT or sys_mem > MEMORY_THRESHOLD_PERCENT:
                self.logger.warning(f"⚠️ Память: процесс {mem_percent:.1f}%, система {sys_mem:.1f}%")
                return True
        except:
            pass
        return False
    
    def _create_session(self) -> requests.Session:
        """Создаёт HTTP сессию с настройками retry."""
        session = requests.Session()
        session.headers.update({"Authorization": f"OAuth {self.oauth_token}"})
        
        retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
        session.mount('https://', adapter)
        
        return session
    
    def _build_params(
        self, 
        after_date: datetime, 
        before_date: datetime, 
        page_token: Optional[str] = None
    ) -> List[Tuple[str, str]]:
        """
        Строит параметры запроса.
        
        Формат API:
        ?pageSize=100&afterDate=2025-01-01T00:00:00+03:00&beforeDate=...&types=mailbox_send&types=message_receive
        """
        after_str = self._format_datetime_for_api(after_date)
        before_str = self._format_datetime_for_api(before_date)
        
        params = [
            ('pageSize', str(PAGE_SIZE)),
            ('afterDate', after_str),
            ('beforeDate', before_str),
            ('types', EVENT_TYPE_SEND),
            ('types', EVENT_TYPE_RECEIVE),
        ]
        
        if page_token:
            params.append(('pageToken', page_token))
        
        return params
    
    def _make_request_with_retry(
        self, 
        session: requests.Session,
        params: List[Tuple[str, str]], 
        worker_id: int = 0
    ) -> Tuple[Optional[Dict], bool, str]:
        """Выполняет запрос с retry логикой."""
        last_error = ""
        network_error_start = None
        
        for attempt in range(MAX_RETRIES):
            if self._stop_flag.is_set():
                return None, False, "Остановлено"
            
            try:
                with self._stats_lock:
                    self._api_calls += 1
                
                response = session.get(self.api_url, params=params, timeout=60)
                
                if attempt == 0:
                    self.logger.debug(f"[W{worker_id}] URL: {response.url[:150]}...")
                
                network_error_start = None
                
                if response.status_code == 200:
                    try:
                        return response.json(), True, ""
                    except json.JSONDecodeError as e:
                        self.logger.error(f"[W{worker_id}] JSON ошибка: {e}")
                        last_error = "JSON decode error"
                        continue
                
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"[W{worker_id}] Rate limit, ждём {retry_after}с")
                    time.sleep(retry_after)
                    with self._stats_lock:
                        self._retries_count += 1
                    continue
                
                elif response.status_code == 400:
                    self.logger.warning(f"[W{worker_id}] HTTP 400: {response.text[:300]}")
                    if "feature is not active" in response.text.lower():
                        return None, False, "feature_not_active"
                    last_error = "HTTP 400"
                    with self._stats_lock:
                        self._retries_count += 1
                    time.sleep(BASE_RETRY_DELAY * (2 ** attempt))
                    continue
                
                elif response.status_code >= 500:
                    delay = BASE_RETRY_DELAY * (2 ** attempt)
                    self.logger.warning(f"[W{worker_id}] HTTP {response.status_code}, повтор через {delay}с")
                    with self._stats_lock:
                        self._retries_count += 1
                    time.sleep(delay)
                    last_error = f"HTTP {response.status_code}"
                    continue
                
                else:
                    last_error = f"HTTP {response.status_code}"
                    self.logger.warning(f"[W{worker_id}] {last_error}")
                    time.sleep(BASE_RETRY_DELAY)
                    continue
                    
            except requests.exceptions.Timeout:
                last_error = "Timeout"
                self.logger.warning(f"[W{worker_id}] Таймаут, попытка {attempt + 1}")
                with self._stats_lock:
                    self._retries_count += 1
                time.sleep(BASE_RETRY_DELAY * (2 ** attempt))
                
            except requests.exceptions.ConnectionError:
                last_error = "ConnectionError"
                if network_error_start is None:
                    network_error_start = time.time()
                
                elapsed = (time.time() - network_error_start) / 60
                if elapsed >= NETWORK_RECOVERY_TIMEOUT_MINUTES:
                    self.logger.error(f"[W{worker_id}] Сеть недоступна {NETWORK_RECOVERY_TIMEOUT_MINUTES} мин")
                    self._save_checkpoint()
                    return None, False, "Сеть недоступна"
                
                wait = min(60, BASE_RETRY_DELAY * (2 ** attempt))
                self.logger.warning(f"[W{worker_id}] Ошибка сети ({elapsed:.1f} мин), ждём {wait}с")
                with self._stats_lock:
                    self._retries_count += 1
                time.sleep(wait)
                
            except Exception as e:
                last_error = str(e)
                self.logger.error(f"[W{worker_id}] Ошибка: {e}")
                with self._stats_lock:
                    self._retries_count += 1
                time.sleep(BASE_RETRY_DELAY * (2 ** attempt))
        
        self.logger.error(f"[W{worker_id}] Не удалось после {MAX_RETRIES} попыток: {last_error}")
        with self._stats_lock:
            self._errors_count += 1
        
        return None, False, last_error
    
    def _create_worker_tasks(self) -> List[WorkerTask]:
        """Создаёт задачи, разбивая период по датам."""
        total_days = (self.date_to - self.date_from).days + 1
        active_workers = min(self.num_workers, total_days)
        
        self.logger.info(f"📋 Разбиение: {total_days} дней на {active_workers} воркеров")
        
        tasks = []
        days_per_worker = total_days / active_workers
        
        for i in range(active_workers):
            start_day = int(i * days_per_worker)
            end_day = int((i + 1) * days_per_worker) - 1
            
            if i == active_workers - 1:
                end_day = total_days - 1
            
            task_from = self.date_from + timedelta(days=start_day)
            task_to = self.date_from + timedelta(days=end_day)
            task_to = task_to.replace(hour=23, minute=59, second=59)
            
            tasks.append(WorkerTask(
                worker_id=i,
                date_from=task_from,
                date_to=task_to
            ))
            
            self.logger.debug(f"  Worker-{i}: {task_from.strftime('%Y-%m-%d')} - {task_to.strftime('%Y-%m-%d')}")
        
        return tasks
    
    def _worker_fetch_events(self, task: WorkerTask) -> WorkerResult:
        """Воркер загружает события за свой диапазон дат."""
        session = self._create_session()
        events_count = 0
        send_count = 0
        receive_count = 0
        errors_count = 0
        page = 0
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        self.logger.info(
            f"[W{task.worker_id}] Старт: "
            f"{task.date_from.strftime('%Y-%m-%d')} - {task.date_to.strftime('%Y-%m-%d')}"
        )
        
        try:
            page_token = None
            
            while not self._stop_flag.is_set():
                page += 1
                
                params = self._build_params(
                    after_date=task.date_from,
                    before_date=task.date_to,
                    page_token=page_token
                )
                
                data, success, error_msg = self._make_request_with_retry(session, params, task.worker_id)
                
                if not success:
                    errors_count += 1
                    consecutive_errors += 1
                    
                    if error_msg == "feature_not_active":
                        self.logger.info(f"[W{task.worker_id}] feature_not_active, завершаем")
                        break
                    
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.error(f"[W{task.worker_id}] Много ошибок подряд, останавливаемся")
                        break
                    
                    time.sleep(BASE_RETRY_DELAY * consecutive_errors)
                    continue
                
                consecutive_errors = 0
                
                if not data:
                    break
                
                events = data.get('events', [])
                
                if not events:
                    break
                
                # Обрабатываем события
                batch_events = []
                for event in events:
                    event_type = event.get('eventType', '')
                    
                    if event_type == EVENT_TYPE_SEND:
                        send_count += 1
                    elif event_type == EVENT_TYPE_RECEIVE:
                        receive_count += 1
                    else:
                        continue
                    
                    processed = self._process_single_event(event)
                    if processed:
                        batch_events.append(processed)
                
                if batch_events:
                    with self._buffer_lock:
                        self._events_buffer.extend(batch_events)
                        buffer_size = len(self._events_buffer)
                    
                    if buffer_size >= BATCH_SIZE_FOR_FLUSH:
                        self._flush_events_buffer()
                
                events_count += len(batch_events)
                
                with self._stats_lock:
                    self._events_processed += len(batch_events)
                
                if page % 50 == 0:
                    self.logger.info(
                        f"[W{task.worker_id}] Страница {page}: "
                        f"{events_count:,} событий (send: {send_count}, receive: {receive_count})"
                    )
                
                if page % 100 == 0 and self._check_memory_usage():
                    self._flush_events_buffer()
                    gc.collect()
                
                if time.time() - self._last_checkpoint_time > CHECKPOINT_INTERVAL_SECONDS:
                    self._save_checkpoint()
                
                next_token = data.get('nextPageToken')
                if not next_token:
                    break
                
                page_token = next_token
                time.sleep(REQUEST_DELAY)
            
            self.logger.info(
                f"[W{task.worker_id}] Завершён: {events_count:,} событий "
                f"(send: {send_count}, receive: {receive_count}), {errors_count} ошибок"
            )
            
            return WorkerResult(
                worker_id=task.worker_id,
                events_count=events_count,
                send_count=send_count,
                receive_count=receive_count,
                errors_count=errors_count,
                completed=True
            )
            
        except Exception as e:
            self.logger.exception(f"[W{task.worker_id}] Критическая ошибка: {e}")
            return WorkerResult(
                worker_id=task.worker_id,
                events_count=events_count,
                send_count=send_count,
                receive_count=receive_count,
                errors_count=errors_count + 1,
                completed=False,
                error_message=str(e)
            )
        finally:
            session.close()
    
    def _process_single_event(self, event: Dict) -> Optional[Dict]:
        """Обрабатывает одно событие."""
        try:
            event_type = event.get('eventType', '')
            event_datetime = self._parse_event_datetime(event.get('date', ''))
            
            if not event_datetime:
                return None
            
            user_login = event.get('userLogin', '')
            if not user_login:
                return None
            
            user_uid = event.get('userUid', '')
            user_name = event.get('userName', '')
            
            local_date = self._format_local_date(event_datetime)
            is_evening = self._is_evening(event_datetime)
            
            is_send = (event_type == EVENT_TYPE_SEND)
            is_receive = (event_type == EVENT_TYPE_RECEIVE)
            
            # Обновляем статистику (thread-safe)
            with self._stats_lock:
                user_stat = self.user_stats[user_login]
                user_stat['email'] = user_login
                user_stat['name'] = user_name or user_stat['name']
                user_stat['uid'] = user_uid or user_stat['uid']
                
                if is_send:
                    user_stat['sent_total'] += 1
                    user_stat['sent_by_day'][local_date] += 1
                    self.stats['total_sent'] += 1
                    self.stats['daily_sent'][local_date] += 1
                    if is_evening:
                        user_stat['sent_evening'] += 1
                        user_stat['sent_evening_by_day'][local_date] += 1
                        self.stats['total_sent_evening'] += 1
                        self.stats['daily_sent_evening'][local_date] += 1
                
                elif is_receive:
                    user_stat['received_total'] += 1
                    user_stat['received_by_day'][local_date] += 1
                    self.stats['total_received'] += 1
                    self.stats['daily_received'][local_date] += 1
                    if is_evening:
                        user_stat['received_evening'] += 1
                        user_stat['received_evening_by_day'][local_date] += 1
                        self.stats['total_received_evening'] += 1
                        self.stats['daily_received_evening'][local_date] += 1
                
                self.stats['unique_users'].add(user_login)
            
            return {
                'event_type': 'send' if is_send else 'receive',
                'datetime': event_datetime,
                'local_date': local_date,
                'local_time': self._format_local_time(event_datetime),
                'local_datetime': self._format_local_datetime(event_datetime),
                'is_evening': is_evening,
                'user_uid': user_uid,
                'user_login': user_login,
                'user_name': user_name,
                'subject': event.get('subject', ''),
                'from': event.get('from', ''),
                'to': event.get('to', ''),
                'msg_id': event.get('msgId', ''),
                'client_ip': event.get('clientIp', '')
            }
        except Exception as e:
            self.logger.debug(f"Ошибка обработки: {e}")
            return None
    
    def _init_csv_file(self):
        """Инициализирует CSV файл."""
        with self._file_lock:
            if self._csv_initialized:
                return
            
            csv_path = os.path.join(self.output_dir, "mail_events.csv")
            self._csv_file = open(csv_path, 'w', newline='', encoding='utf-8-sig')
            
            self._csv_writer = csv.DictWriter(
                self._csv_file, 
                fieldnames=['date', 'time', 'datetime', 'event_type', 'user_email',
                           'user_name', 'is_evening', 'subject', 'from', 'to', 'msg_id', 'client_ip'],
                delimiter=';', quoting=csv.QUOTE_ALL
            )
            self._csv_writer.writeheader()
            self._csv_initialized = True
    
    def _flush_events_buffer(self):
        """Сбрасывает буфер событий в CSV."""
        with self._buffer_lock:
            if not self._events_buffer:
                return
            events_to_write = self._events_buffer.copy()
            self._events_buffer.clear()
        
        if not events_to_write:
            return
        
        self._init_csv_file()
        
        with self._file_lock:
            for event in events_to_write:
                try:
                    self._csv_writer.writerow({
                        'date': event['local_date'],
                        'time': event['local_time'],
                        'datetime': event['local_datetime'],
                        'event_type': event['event_type'],
                        'user_email': event['user_login'],
                        'user_name': event['user_name'],
                        'is_evening': 'да' if event['is_evening'] else 'нет',
                        'subject': event.get('subject', ''),
                        'from': event.get('from', ''),
                        'to': event.get('to', ''),
                        'msg_id': event.get('msg_id', ''),
                        'client_ip': event.get('client_ip', '')
                    })
                except:
                    pass
            self._csv_file.flush()
        
        self.logger.debug(f"Сброшено {len(events_to_write)} событий в CSV")
        del events_to_write
        gc.collect()
    
    def _close_csv_file(self):
        """Закрывает CSV файл."""
        with self._file_lock:
            if self._csv_file:
                try:
                    self._csv_file.close()
                except:
                    pass
                self._csv_file = None
    
    def _save_checkpoint(self):
        """Сохраняет прогресс."""
        checkpoint = {
            'timestamp': datetime.now().isoformat(),
            'events_processed': self._events_processed,
            'api_calls': self._api_calls,
            'stats': {
                'total_sent': self.stats['total_sent'],
                'total_received': self.stats['total_received'],
                'total_sent_evening': self.stats['total_sent_evening'],
                'total_received_evening': self.stats['total_received_evening'],
                'unique_users': len(self.stats['unique_users'])
            }
        }
        try:
            with open(self._checkpoint_file, 'w', encoding='utf-8') as f:
                json.dump(checkpoint, f, ensure_ascii=False, indent=2)
            self._last_checkpoint_time = time.time()
        except:
            pass
    
    def _fetch_all_events(self):
        """Загружает все события."""
        self.logger.info("=" * 60)
        self.logger.info("ЗАГРУЗКА СОБЫТИЙ")
        self.logger.info("=" * 60)
        self.logger.info(f"Период: {self.date_from.strftime('%Y-%m-%d %H:%M:%S')} - {self.date_to.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Дней в периоде: {(self.date_to - self.date_from).days + 1}")
        self.logger.info(f"Часовой пояс: {self.tz_name}")
        self.logger.info(f"Вечернее время: после {self.evening_hour}:00")
        self.logger.info(f"Воркеров: {self.num_workers}")
        self.logger.info("=" * 60)
        
        tasks = self._create_worker_tasks()
        
        if not tasks:
            self.logger.warning("Нет задач для выполнения")
            return
        
        self.logger.info(f"🚀 Запуск {len(tasks)} воркеров...")
        
        all_results = []
        executor = ThreadPoolExecutor(max_workers=len(tasks), thread_name_prefix='Worker')
        
        try:
            futures = {executor.submit(self._worker_fetch_events, task): task for task in tasks}
            
            for future in as_completed(futures, timeout=7200):
                if self._stop_flag.is_set():
                    break
                
                task = futures[future]
                try:
                    result = future.result(timeout=3600)
                    all_results.append(result)
                    
                    status = "✓" if result.completed else "⚠️"
                    self.logger.info(
                        f"{status} Worker-{result.worker_id}: {result.events_count:,} событий "
                        f"(send: {result.send_count}, receive: {result.receive_count})"
                    )
                except Exception as e:
                    self.logger.error(f"Worker-{task.worker_id} ошибка: {e}")
        
        finally:
            executor.shutdown(wait=True, cancel_futures=False)
            del executor
            gc.collect()
        
        self._flush_events_buffer()
        
        total_events = sum(r.events_count for r in all_results)
        total_send = sum(r.send_count for r in all_results)
        total_receive = sum(r.receive_count for r in all_results)
        total_errors = sum(r.errors_count for r in all_results)
        
        self.logger.info(f"\n{'='*60}")
        self.logger.info("ИТОГИ ЗАГРУЗКИ")
        self.logger.info(f"{'='*60}")
        self.logger.info(f"Всего событий: {total_events:,}")
        self.logger.info(f"  - отправлено: {total_send:,}")
        self.logger.info(f"  - получено: {total_receive:,}")
        self.logger.info(f"Ошибок: {total_errors}")
        self.logger.info(f"API вызовов: {self._api_calls:,}")
    
    def _write_user_stats_csv(self):
        """Записывает статистику по пользователям."""
        csv_path = os.path.join(self.output_dir, "user_statistics.csv")
        
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'user_email', 'user_name', 'sent_total', 'received_total',
                'total_messages', 'sent_evening', 'received_evening', 'total_evening'
            ], delimiter=';', quoting=csv.QUOTE_ALL)
            writer.writeheader()
            
            sorted_users = sorted(
                self.user_stats.items(),
                key=lambda x: x[1]['sent_total'] + x[1]['received_total'],
                reverse=True
            )
            
            for login, stats in sorted_users:
                total = stats['sent_total'] + stats['received_total']
                total_eve = stats['sent_evening'] + stats['received_evening']
                writer.writerow({
                    'user_email': login,
                    'user_name': stats['name'],
                    'sent_total': stats['sent_total'],
                    'received_total': stats['received_total'],
                    'total_messages': total,
                    'sent_evening': stats['sent_evening'],
                    'received_evening': stats['received_evening'],
                    'total_evening': total_eve
                })
        
        self.logger.info(f"Статистика пользователей: {csv_path}")
    
    def _write_daily_stats_csv(self):
        """Записывает статистику по дням."""
        csv_path = os.path.join(self.output_dir, "daily_statistics.csv")
        
        all_dates = set()
        all_dates.update(self.stats['daily_sent'].keys())
        all_dates.update(self.stats['daily_received'].keys())
        
        with open(csv_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=[
                'date', 'sent', 'received', 'total',
                'sent_evening', 'received_evening', 'total_evening'
            ], delimiter=';', quoting=csv.QUOTE_ALL)
            writer.writeheader()
            
            for date in sorted(all_dates):
                sent = self.stats['daily_sent'].get(date, 0)
                recv = self.stats['daily_received'].get(date, 0)
                sent_eve = self.stats['daily_sent_evening'].get(date, 0)
                recv_eve = self.stats['daily_received_evening'].get(date, 0)
                writer.writerow({
                    'date': date,
                    'sent': sent,
                    'received': recv,
                    'total': sent + recv,
                    'sent_evening': sent_eve,
                    'received_evening': recv_eve,
                    'total_evening': sent_eve + recv_eve
                })
        
        self.logger.info(f"Статистика по дням: {csv_path}")
    
    def _generate_report(self):
        """Генерирует итоговый отчёт."""
        local_now = self._get_current_time_local()
        total_msg = self.stats['total_sent'] + self.stats['total_received']
        total_eve = self.stats['total_sent_evening'] + self.stats['total_received_evening']
        
        lines = [
            "=" * 80,
            "СТАТИСТИКА ПОЧТОВОЙ АКТИВНОСТИ",
            "=" * 80,
            "",
            f"Период: {self.date_from.strftime('%Y-%m-%d')} - {self.date_to.strftime('%Y-%m-%d')}",
            f"Дней в периоде: {(self.date_to - self.date_from).days + 1}",
            f"Часовой пояс: {self.tz_name}",
            f"Вечернее время: после {self.evening_hour}:00",
            "",
            "-" * 80,
            "ОБЩАЯ СТАТИСТИКА",
            "-" * 80,
            f"  Всего писем:              {total_msg:,}",
            f"    отправлено:             {self.stats['total_sent']:,}",
            f"    получено:               {self.stats['total_received']:,}",
            "",
            f"  После {self.evening_hour}:00:              {total_eve:,}",
            f"    отправлено:             {self.stats['total_sent_evening']:,}",
            f"    получено:               {self.stats['total_received_evening']:,}",
            "",
            f"  Уникальных пользователей: {len(self.stats['unique_users']):,}",
            "",
            "-" * 80,
            "СТАТИСТИКА ПО ДНЯМ",
            "-" * 80,
            f"{'Дата':<12} | {'Отпр':>8} | {'Получ':>8} | {'Всего':>8} | {'Веч.отпр':>9} | {'Веч.пол':>8} | {'Веч.всего':>10}",
            "-" * 80,
        ]
        
        all_dates = set(self.stats['daily_sent'].keys()) | set(self.stats['daily_received'].keys())
        for date in sorted(all_dates):
            s = self.stats['daily_sent'].get(date, 0)
            r = self.stats['daily_received'].get(date, 0)
            se = self.stats['daily_sent_evening'].get(date, 0)
            re = self.stats['daily_received_evening'].get(date, 0)
            lines.append(f"{date:<12} | {s:>8,} | {r:>8,} | {s+r:>8,} | {se:>9,} | {re:>8,} | {se+re:>10,}")
        
        if not all_dates:
            lines.append("  (нет данных)")
        
        lines.extend([
            "-" * 80,
            "",
            "-" * 80,
            "ТОП-10 ПО ОБЩЕЙ АКТИВНОСТИ",
            "-" * 80,
            f"{'Email':<40} | {'Отпр':>8} | {'Получ':>8} | {'Всего':>8}",
            "-" * 80,
        ])
        
        top_users = sorted(
            self.user_stats.items(), 
            key=lambda x: x[1]['sent_total'] + x[1]['received_total'], 
            reverse=True
        )[:10]
        
        for login, st in top_users:
            total = st['sent_total'] + st['received_total']
            disp = login[:38] + '..' if len(login) > 40 else login
            lines.append(f"{disp:<40} | {st['sent_total']:>8,} | {st['received_total']:>8,} | {total:>8,}")
        
        if not top_users:
            lines.append("  (нет данных)")
        
        lines.extend([
            "-" * 80,
            "",
            "-" * 80,
            f"ТОП-10 ПО ВЕЧЕРНЕЙ АКТИВНОСТИ (после {self.evening_hour}:00)",
            "-" * 80,
            f"{'Email':<40} | {'Отпр':>8} | {'Получ':>8} | {'Всего':>8}",
            "-" * 80,
        ])
        
        top_evening = sorted(
            self.user_stats.items(), 
            key=lambda x: x[1]['sent_evening'] + x[1]['received_evening'], 
            reverse=True
        )[:10]
        
        has_evening = False
        for login, st in top_evening:
            total_eve = st['sent_evening'] + st['received_evening']
            if total_eve == 0:
                continue
            has_evening = True
            disp = login[:38] + '..' if len(login) > 40 else login
            lines.append(f"{disp:<40} | {st['sent_evening']:>8,} | {st['received_evening']:>8,} | {total_eve:>8,}")
        
        if not has_evening:
            lines.append("  (нет данных)")
        
        lines.extend([
            "-" * 80,
            "",
            "-" * 80,
            f"ВЕЧЕРНЯЯ АКТИВНОСТЬ ПО ДНЯМ (после {self.evening_hour}:00)",
            "-" * 80,
            f"{'Дата':<12} | {'Отправлено':>12} | {'Получено':>12} | {'Всего':>12}",
            "-" * 80,
        ])
        
        has_evening_daily = False
        for date in sorted(all_dates):
            se = self.stats['daily_sent_evening'].get(date, 0)
            re = self.stats['daily_received_evening'].get(date, 0)
            if se > 0 or re > 0:
                has_evening_daily = True
                lines.append(f"{date:<12} | {se:>12,} | {re:>12,} | {se+re:>12,}")
        
        if not has_evening_daily:
            lines.append("  (нет данных)")
        
        lines.extend([
            "-" * 80,
            "",
            "-" * 80,
            "ТЕХНИЧЕСКАЯ ИНФОРМАЦИЯ",
            "-" * 80,
            f"  API вызовов:        {self._api_calls:,}",
            f"  Событий обработано: {self._events_processed:,}",
            f"  Повторных попыток:  {self._retries_count}",
            f"  Ошибок:             {self._errors_count}",
            "",
            "-" * 80,
            "ВЫХОДНЫЕ ФАЙЛЫ",
            "-" * 80,
            "  mail_events.csv       - все письма (1 письмо = 1 строка)",
            "  user_statistics.csv   - статистика по пользователям",
            "  daily_statistics.csv  - статистика по дням",
            "  summary.txt           - этот отчёт",
            "  execution.log         - лог выполнения",
            "  checkpoint.json       - файл прогресса",
            "",
            f"Сгенерировано: {local_now.strftime('%Y-%m-%d %H:%M:%S')} ({self.tz_name})",
            f"Папка: {os.path.abspath(self.output_dir)}/",
            ""
        ])
        
        report = "\n".join(lines)
        print("\n" + report)
        
        with open(os.path.join(self.output_dir, "summary.txt"), 'w', encoding='utf-8') as f:
            f.write(report)
    
    def collect(self):
        """Основной метод сбора статистики."""
        self.logger.info("=" * 60)
        self.logger.info("СБОР СТАТИСТИКИ ПОЧТЫ")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            self._fetch_all_events()
            self._close_csv_file()
            self._write_user_stats_csv()
            self._write_daily_stats_csv()
            self._generate_report()
            self._save_checkpoint()
            
            self.user_stats.clear()
            gc.collect()
            
        except KeyboardInterrupt:
            self.logger.warning("Прервано пользователем. Сохраняем прогресс...")
            self._stop_flag.set()
            self._flush_events_buffer()
            self._close_csv_file()
            self._save_checkpoint()
            raise
            
        except Exception as e:
            self.logger.exception(f"Ошибка: {e}")
            self._flush_events_buffer()
            self._close_csv_file()
            self._save_checkpoint()
            raise
        
        self.logger.info(f"✓ Завершено за {time.time() - start_time:.2f} сек")


def main():
    """Точка входа."""
    print("=" * 60)
    print("ПАРСЕР ПОЧТОВЫХ ЛОГОВ ЯНДЕКС 360")
    print("=" * 60)
    
    if not OAUTH_TOKEN:
        print("\n❌ ОШИБКА: Укажите OAUTH_TOKEN в настройках скрипта")
        print("   Получить токен: https://oauth.yandex.ru/")
        print("   Требуемые права: ya360_security:audit_log_mail")
        return
    
    if not ORG_ID:
        print("\n❌ ОШИБКА: Укажите ORG_ID в настройках скрипта")
        print("   Найти ID организации: Яндекс 360 → Админка → О компании")
        return
    
    print(f"\n✓ Токен: {'*' * 10}...{OAUTH_TOKEN[-4:]}")
    print(f"✓ Организация: {ORG_ID}")
    print(f"✓ Часовой пояс: UTC{'+' if TIMEZONE_OFFSET_HOURS >= 0 else ''}{TIMEZONE_OFFSET_HOURS}")
    
    if DATE_FROM and DATE_TO:
        print(f"✓ Период: {DATE_FROM} - {DATE_TO}")
    else:
        print(f"✓ Период: последние {PERIOD_DAYS} дней")
    
    print(f"✓ Вечернее время: после {EVENING_HOUR}:00")
    print("=" * 60)
    
    collector = MailStatsCollector(
        oauth_token=OAUTH_TOKEN,
        org_id=ORG_ID,
        date_from=DATE_FROM,
        date_to=DATE_TO,
        period_days=PERIOD_DAYS,
        timezone_offset_hours=TIMEZONE_OFFSET_HOURS,
        evening_hour=EVENING_HOUR,
        num_workers=NUM_WORKERS
    )
    
    try:
        collector.collect()
    except KeyboardInterrupt:
        print("\n\n⚠️ Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
