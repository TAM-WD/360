#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
Сбор статистики встреч из аудит-логов Телемоста:
- Количество встреч
- Длительность встреч
- Средняя длительность
- Количество уникальных пользователей
'''

import requests
import time
import csv
import logging
import os
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from typing import Dict, Any, Optional, List
import gc

# ============================================================================
# НАСТРОЙКИ
# ============================================================================

# OAuth токен для доступа к API с правами ya360_security:read_auditlog
OAUTH_TOKEN = ""

# ID организации
ORG_ID = ""

# Часовой пояс для отображения времени (UTC offset в часах)
# Примеры: 3 для Москвы (UTC+3), 5 для Екатеринбурга (UTC+5), -5 для Нью-Йорка
TIMEZONE_OFFSET_HOURS = 3  # Москва по умолчанию

# Период сбора данных (формат: "YYYY-MM-DD" - ОБЯЗАТЕЛЬНО В КАВЫЧКАХ!)
# Если None - используется автоматический расчёт на основе PERIOD_DAYS
DATE_FROM = "2025-12-12"  # Например: "2024-01-01"
DATE_TO = "2026-01-12"    # Например: "2024-01-07"

# Период по умолчанию в днях (используется если DATE_FROM/DATE_TO = None)
# Минимум: 1, Максимум: 31 (месяц)
PERIOD_DAYS = 7  # Неделя по умолчанию

# Количество записей на страницу
PAGE_SIZE = 100

# Задержка между запросами (секунды)
REQUEST_DELAY = 0.2

# Максимальное количество повторных попыток при ошибках
MAX_RETRIES = 5

# Базовая задержка для retry (секунды)
BASE_RETRY_DELAY = 2

# Включать ли незавершённые встречи (без времени окончания) в статистику и CSV
# True - включать, False - только завершённые встречи
INCLUDE_INCOMPLETE = False  # По умолчанию только завершённые

# ============================================================================
# КОНЕЦ НАСТРОЕК
# ============================================================================

# URL API аудит-логов
API_URL = "https://cloud-api.yandex.net/v1/auditlog/organizations/{org_id}/events"

# Типы событий Телемоста
EVENT_TYPES = [
    "telemost_conference.created",
    "telemost_conference.started",
    "telemost_conference.ended",
    "telemost_conference.peer.joined"
]


class TelemostStatsCollector:
    """Сборщик статистики встреч Телемоста."""
    
    def __init__(
        self,
        oauth_token: str,
        org_id: str,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        period_days: int = 7,
        timezone_offset_hours: int = 3,
        include_incomplete: bool = False
    ):
        self.oauth_token = oauth_token
        self.org_id = org_id
        self.api_url = API_URL.format(org_id=org_id)
        
        # Настройка часового пояса
        self.tz_offset = timedelta(hours=timezone_offset_hours)
        self.tz = timezone(self.tz_offset)
        self.tz_name = f"UTC{'+' if timezone_offset_hours >= 0 else ''}{timezone_offset_hours}"
        
        # Настройка сессии
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"OAuth {oauth_token}",
            "Content-Type": "application/json"
        })

        self.include_incomplete = include_incomplete
        
        # Валидация и установка периода
        period_days = max(1, min(31, period_days))
        
        # Устанавливаем даты
        self.date_from, self.date_to = self._setup_dates(
            date_from, date_to, period_days
        )
        
        # Создание рабочей директории
        self.output_dir = self._create_output_directory()
        
        # Настройка логирования
        self._setup_logging()
        
        # Хранилище событий по конференциям
        self.conferences: Dict[str, Dict[str, Any]] = {}
        
        # Статистика
        self.stats = {
            'total_meetings': 0,
            'total_meetings_incomplete': 0,
            'total_duration_seconds': 0,
            'unique_users': set(),
            'daily_stats': defaultdict(lambda: {
                'meetings': 0,
                'meetings_incomplete': 0,
                'duration_seconds': 0,
                'users': set()
            })
        }
        
        # Счётчики
        self.events_processed = 0
        self.api_calls = 0
        self.retries_count = 0
        
        # CSV writers
        self.csv_file = None
        self.csv_writer = None
        self.csv_participants_file = None
        self.csv_participants_writer = None
    
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
        """Форматирует datetime в локальном часовом поясе."""
        if dt is None:
            return "N/A"
        local_dt = self._utc_to_local(dt)
        return local_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    def _format_local_date(self, dt: datetime) -> str:
        """Форматирует дату в локальном часовом поясе."""
        if dt is None:
            return "N/A"
        local_dt = self._utc_to_local(dt)
        return local_dt.strftime("%Y-%m-%d")
    
    def _format_local_time(self, dt: datetime) -> str:
        """Форматирует время в локальном часовом поясе."""
        if dt is None:
            return "N/A"
        local_dt = self._utc_to_local(dt)
        return local_dt.strftime("%H:%M:%S")
    
    def _setup_dates(
        self,
        date_from: Optional[str],
        date_to: Optional[str],
        period_days: int
    ) -> tuple:
        """Настраивает даты периода."""
        if date_from and date_to:
            from_dt = self._parse_date(date_from)
            to_dt = self._parse_date(date_to)
            
            if (to_dt - from_dt).days > 31:
                to_dt = from_dt + timedelta(days=31)
        else:
            local_now = self._get_current_time_local()
            to_dt = local_now.replace(tzinfo=None)
            from_dt = to_dt - timedelta(days=period_days)
        
        from_dt = from_dt.replace(hour=0, minute=0, second=0, microsecond=0)
        to_dt = to_dt.replace(hour=23, minute=59, second=59, microsecond=0)
        
        return from_dt, to_dt
    
    def _parse_date(self, date_str: str) -> datetime:
        """Парсит строку даты в datetime."""
        if not date_str:
            raise ValueError("Дата не может быть пустой")
        
        if isinstance(date_str, datetime):
            return date_str
        
        date_str = str(date_str)
        
        formats = ["%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"Не удалось распарсить дату: {date_str}")
    
    def _create_output_directory(self) -> str:
        """Создаёт директорию для результатов."""
        local_now = self._get_current_time_local()
        timestamp = local_now.strftime("%Y%m%d_%H%M%S")
        dir_name = f"{timestamp}_telemost_stats"
        os.makedirs(dir_name, exist_ok=True)
        return dir_name
    
    def _setup_logging(self):
        """Настройка логирования в файл и консоль."""
        log_file = os.path.join(self.output_dir, "execution.log")
        
        class LocalTimeFormatter(logging.Formatter):
            def __init__(self, fmt, datefmt, tz):
                super().__init__(fmt, datefmt)
                self.tz = tz
            
            def formatTime(self, record, datefmt=None):
                dt = datetime.fromtimestamp(record.created, tz=self.tz)
                if datefmt:
                    return dt.strftime(datefmt)
                return dt.isoformat()
        
        formatter = LocalTimeFormatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S',
            tz=self.tz
        )
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        
        self.logger = logging.getLogger(f"TelemostStats_{id(self)}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False
    
    def _format_datetime_for_api(self, dt: datetime) -> str:
        """Форматирует datetime для API (в UTC)."""
        local_dt = dt.replace(tzinfo=self.tz)
        utc_dt = local_dt.astimezone(timezone.utc)
        return utc_dt.strftime("%Y-%m-%dT%H:%M:%S") + "+00:00"
    
    def _make_request_with_retry(
        self, 
        params: Dict[str, Any]
    ) -> Optional[Dict]:
        """Выполняет запрос к API с повторными попытками."""
        self.api_calls += 1
        
        for attempt in range(MAX_RETRIES):
            try:
                self.logger.debug(
                    f"API запрос #{self.api_calls}, попытка {attempt + 1}"
                )
                
                response = self.session.get(
                    self.api_url, 
                    params=params, 
                    timeout=60
                )
                
                if response.status_code == 200:
                    return response.json()
                
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(
                        f"Rate limit (429), ожидание {retry_after} сек..."
                    )
                    time.sleep(retry_after)
                    self.retries_count += 1
                    continue
                
                elif response.status_code >= 500:
                    delay = BASE_RETRY_DELAY * (2 ** attempt)
                    self.logger.warning(
                        f"Серверная ошибка {response.status_code}, "
                        f"попытка {attempt + 1}/{MAX_RETRIES}, "
                        f"повтор через {delay} сек..."
                    )
                    self.retries_count += 1
                    time.sleep(delay)
                    continue
                
                else:
                    self.logger.error(
                        f"Ошибка API: {response.status_code}\n"
                        f"Response: {response.text[:1000]}"
                    )
                    if 400 <= response.status_code < 500:
                        return None
                    
                    self.retries_count += 1
                    time.sleep(BASE_RETRY_DELAY * (2 ** attempt))
                    continue
                    
            except requests.exceptions.Timeout:
                self.logger.warning(
                    f"Таймаут запроса, попытка {attempt + 1}/{MAX_RETRIES}"
                )
                self.retries_count += 1
                time.sleep(BASE_RETRY_DELAY * (2 ** attempt))
                continue
                
            except requests.exceptions.ConnectionError as e:
                self.logger.warning(
                    f"Ошибка соединения: {e}, "
                    f"попытка {attempt + 1}/{MAX_RETRIES}"
                )
                self.retries_count += 1
                time.sleep(BASE_RETRY_DELAY * (2 ** attempt))
                continue
                
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Ошибка запроса: {e}")
                self.retries_count += 1
                time.sleep(BASE_RETRY_DELAY * (2 ** attempt))
                continue
        
        self.logger.error(
            f"Не удалось выполнить запрос после {MAX_RETRIES} попыток"
        )
        return None
    
    def _fetch_all_events(self) -> List[Dict]:
        """Загружает все события за период."""
        all_events = []
        iteration_key = None
        
        self.logger.info("Загрузка событий из API...")
        self.logger.info(f"URL: {self.api_url}")
        self.logger.info(f"Часовой пояс: {self.tz_name}")
        self.logger.info(
            f"Период (локальное время): "
            f"{self.date_from.strftime('%Y-%m-%d %H:%M:%S')} - "
            f"{self.date_to.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        self.logger.info(f"Типы событий: {', '.join(EVENT_TYPES)}")
        
        page = 0
        while True:
            page += 1
            
            params = {
                "started_at": self._format_datetime_for_api(self.date_from),
                "ended_at": self._format_datetime_for_api(self.date_to),
                "count": PAGE_SIZE,
                "types": ",".join(EVENT_TYPES)
            }
            
            if iteration_key:
                params["iteration_key"] = iteration_key
            
            data = self._make_request_with_retry(params)
            
            if not data:
                self.logger.warning("Не удалось получить данные")
                break
            
            items = data.get("items", [])
            
            if not items:
                self.logger.debug("Нет событий в ответе")
                break
            
            event_counts = {}
            for item in items:
                event = item.get("event", {})
                event_type = event.get("type", "unknown")
                event_counts[event_type] = event_counts.get(event_type, 0) + 1
            
            counts_str = ", ".join(
                [f"{t.split('.')[-1]}: {c}" for t, c in event_counts.items()]
            )
            self.logger.info(
                f"  Страница {page}: {len(items)} событий ({counts_str})"
            )
            
            all_events.extend(items)
            self.events_processed += len(items)
            
            if self.events_processed % 1000 == 0:
                gc.collect()
            
            iteration_key = data.get("iteration_key")
            
            if not iteration_key:
                break
            
            time.sleep(REQUEST_DELAY)
        
        self.logger.info(f"Всего загружено событий: {self.events_processed}")
        return all_events
    
    def _parse_timestamp_ms(self, timestamp_ms: int) -> Optional[datetime]:
        """Парсит timestamp в миллисекундах (возвращает UTC)."""
        if not timestamp_ms:
            return None
        try:
            return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
        except:
            return None
    
    def _parse_timestamp_iso(self, timestamp_str: str) -> Optional[datetime]:
        """Парсит ISO timestamp (возвращает UTC)."""
        if not timestamp_str:
            return None
        try:
            if '+' in timestamp_str or timestamp_str.endswith('Z'):
                clean_ts = timestamp_str.replace('Z', '+00:00')
                dt = datetime.fromisoformat(clean_ts)
                return dt.astimezone(timezone.utc)
            else:
                dt = datetime.fromisoformat(timestamp_str)
                return dt.replace(tzinfo=timezone.utc)
        except:
            return None
    
    def _process_events(self, events: List[Dict]):
        """Обрабатывает все события и группирует по конференциям."""
        self.logger.info("Обработка событий...")
        
        for item in events:
            try:
                event = item.get("event", {})
                meta = event.get("meta", {})
                
                event_type = event.get("type", "")
                conference_id = meta.get("conference_id")
                
                if not conference_id:
                    continue
                
                occurred_at = event.get("occurred_at")
                event_time = self._parse_timestamp_iso(occurred_at)
                
                conference_start_ms = meta.get("conference_start")
                conference_start = self._parse_timestamp_ms(conference_start_ms)
                
                user_uid = event.get("uid")
                user_name = item.get("user_name", "")
                user_email = item.get("user_login", "")
                
                if conference_id not in self.conferences:
                    self.conferences[conference_id] = {
                        'id': conference_id,
                        'name': "",
                        'organizer_uid': None,
                        'organizer_email': None,
                        'organizer_name': None,
                        'started_at': None,
                        'ended_at': None,
                        'participants': {},
                        'participant_count': 0,
                        'has_started': False,
                        'has_ended': False,
                        'has_peer_joined': False
                    }
                
                conf = self.conferences[conference_id]
                
                if conference_start and not conf['started_at']:
                    conf['started_at'] = conference_start
                
                if user_uid and user_uid not in conf['participants']:
                    conf['participants'][user_uid] = {
                        'uid': user_uid,
                        'email': user_email,
                        'name': user_name,
                        'role': meta.get("role", "") or meta.get("peer_role", "")
                    }
                elif user_uid and user_uid in conf['participants']:
                    if user_email and not conf['participants'][user_uid]['email']:
                        conf['participants'][user_uid]['email'] = user_email
                    if user_name and not conf['participants'][user_uid]['name']:
                        conf['participants'][user_uid]['name'] = user_name
                
                if event_type == "telemost_conference.created":
                    if user_uid:
                        conf['organizer_uid'] = user_uid
                        conf['organizer_email'] = user_email
                        conf['organizer_name'] = user_name
                        
                elif event_type == "telemost_conference.started":
                    conf['has_started'] = True
                    if event_time:
                        conf['started_at'] = event_time
                    
                elif event_type == "telemost_conference.ended":
                    conf['has_ended'] = True
                    if event_time:
                        conf['ended_at'] = event_time
                    
                elif event_type == "telemost_conference.peer.joined":
                    conf['has_peer_joined'] = True
                            
            except Exception as e:
                self.logger.debug(f"Ошибка обработки события: {e}")
        
        for conf in self.conferences.values():
            conf['participant_count'] = len(conf['participants'])
        
        self.logger.info(f"Найдено конференций: {len(self.conferences)}")
        
        complete = sum(
            1 for c in self.conferences.values() 
            if c['started_at'] and c['ended_at']
        )
        only_created = sum(
            1 for c in self.conferences.values() 
            if not c['has_started'] and not c['has_ended'] and not c['has_peer_joined']
        )
        has_activity = sum(
            1 for c in self.conferences.values() 
            if c['has_started'] or c['has_ended'] or c['has_peer_joined']
        )
        
        self.logger.info(
            f"  С активностью: {has_activity}, "
            f"полные (start+end): {complete}, "
            f"только созданы (пустые): {only_created}"
        )
    
    def _is_empty_conference(self, conf: Dict) -> bool:
        """Проверяет, является ли конференция "пустой" (только создана, никакой активности)."""
        return (
            not conf['has_started'] and 
            not conf['has_ended'] and 
            not conf['has_peer_joined']
        )
    
    def _init_csv_writers(self):
        """Инициализирует CSV writers с UTF-8 BOM."""
        csv_path = os.path.join(self.output_dir, "meetings.csv")
        self.csv_file = open(csv_path, 'w', newline='', encoding='utf-8-sig')
        
        fieldnames = [
            'conference_id',
            'date',
            'start_time',
            'end_time',
            'duration_seconds',
            'duration_formatted',
            'status',
            'organizer_uid',
            'organizer_email',
            'organizer_name',
            'participants_count',
            'participants_emails',
            'participants_names'
        ]
        
        self.csv_writer = csv.DictWriter(
            self.csv_file, 
            fieldnames=fieldnames,
            delimiter=';',
            quoting=csv.QUOTE_ALL
        )
        self.csv_writer.writeheader()
        
        csv_participants_path = os.path.join(
            self.output_dir, "participants.csv"
        )
        self.csv_participants_file = open(
            csv_participants_path, 'w', newline='', encoding='utf-8-sig'
        )
        
        participants_fieldnames = [
            'conference_id',
            'conference_date',
            'participant_uid',
            'participant_email',
            'participant_name',
            'participant_role',
            'is_organizer'
        ]
        
        self.csv_participants_writer = csv.DictWriter(
            self.csv_participants_file, 
            fieldnames=participants_fieldnames,
            delimiter=';',
            quoting=csv.QUOTE_ALL
        )
        self.csv_participants_writer.writeheader()
    
    def _close_csv_writers(self):
        """Закрывает CSV файлы."""
        if self.csv_file:
            self.csv_file.close()
        if self.csv_participants_file:
            self.csv_participants_file.close()
    
    def _format_duration(self, seconds: int) -> str:
        """Форматирует длительность в читаемый вид."""
        if seconds < 0:
            return "N/A"
            
        hours, remainder = divmod(seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        
        if hours > 0:
            return f"{hours} ч {minutes} мин {secs} сек"
        elif minutes > 0:
            return f"{minutes} мин {secs} сек"
        else:
            return f"{secs} сек"
    
    def _format_duration_short(self, seconds: int) -> str:
        """Форматирует длительность HH:MM:SS."""
        if seconds < 0:
            return "N/A"
        hours, remainder = divmod(seconds, 3600)
        minutes, secs = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    def _calculate_statistics(self):
        """Вычисляет статистику и записывает данные в CSV."""
        self.logger.info("Вычисление статистики...")
        self.logger.info(
            f"Режим: {'включая незавершённые' if self.include_incomplete else 'только завершённые'}"
        )
    
        self._init_csv_writers()
    
        skipped_empty = 0
        skipped_incomplete = 0
    
        try:
            for conf_id, conf in self.conferences.items():
                if self._is_empty_conference(conf):
                    skipped_empty += 1
                    continue
            
                has_start = conf['started_at'] is not None
                has_end = conf['ended_at'] is not None
            
                if has_start and has_end:
                    status = "complete"
                    duration_seconds = int(
                        (conf['ended_at'] - conf['started_at']).total_seconds()
                    )
                    if duration_seconds < 0:
                        duration_seconds = 0
                        status = "invalid_duration"
                elif has_start:
                    status = "no_end"
                    duration_seconds = 0
                elif has_end:
                    status = "no_start"
                    duration_seconds = 0
                else:
                    status = "no_times"
                    duration_seconds = 0
            
                is_complete = (status == "complete" and duration_seconds > 0)
            
                if not is_complete and not self.include_incomplete:
                    skipped_incomplete += 1
                    self.stats['total_meetings_incomplete'] += 1
                    continue
            
                if conf['started_at']:
                    meeting_date = self._format_local_date(conf['started_at'])
                    start_time = self._format_local_time(conf['started_at'])
                elif conf['ended_at']:
                    meeting_date = self._format_local_date(conf['ended_at'])
                    start_time = "N/A"
                else:
                    meeting_date = "N/A"
                    start_time = "N/A"
            
                end_time = self._format_local_time(conf['ended_at'])
            
                participants_emails = []
                participants_names = []
                for p in conf['participants'].values():
                    if p['email']:
                        participants_emails.append(p['email'])
                    if p['name']:
                        participants_names.append(p['name'])
            
                self.csv_writer.writerow({
                    'conference_id': conf_id,
                    'date': meeting_date,
                    'start_time': start_time,
                    'end_time': end_time,
                    'duration_seconds': duration_seconds,
                    'duration_formatted': (
                        self._format_duration_short(duration_seconds) 
                        if duration_seconds > 0 else "N/A"
                    ),
                    'status': status,
                    'organizer_uid': conf['organizer_uid'] or "N/A",
                    'organizer_email': conf['organizer_email'] or "N/A",
                    'organizer_name': conf['organizer_name'] or "N/A",
                    'participants_count': conf['participant_count'],
                    'participants_emails': "; ".join(participants_emails),
                    'participants_names': "; ".join(participants_names)
                })
            
                for p_uid, p_data in conf['participants'].items():
                    self.csv_participants_writer.writerow({
                        'conference_id': conf_id,
                        'conference_date': meeting_date,
                        'participant_uid': p_uid,
                        'participant_email': p_data['email'] or "N/A",
                        'participant_name': p_data['name'] or "N/A",
                        'participant_role': p_data['role'] or "N/A",
                        'is_organizer': (
                            "yes" if p_uid == conf['organizer_uid'] else "no"
                        )
                    })
            
                if is_complete:
                    self.stats['total_meetings'] += 1
                    self.stats['total_duration_seconds'] += duration_seconds
                
                    if meeting_date != "N/A":
                        daily = self.stats['daily_stats'][meeting_date]
                        daily['meetings'] += 1
                        daily['duration_seconds'] += duration_seconds
                else:
                    self.stats['total_meetings_incomplete'] += 1
                
                    if meeting_date != "N/A":
                        daily = self.stats['daily_stats'][meeting_date]
                        daily['meetings_incomplete'] += 1
            
                for p_uid in conf['participants'].keys():
                    self.stats['unique_users'].add(p_uid)
                    if meeting_date != "N/A":
                        self.stats['daily_stats'][meeting_date]['users'].add(p_uid)
            
        finally:
            self._close_csv_writers()
    
        self.logger.info(
            f"Статистика рассчитана. "
            f"Завершённых: {self.stats['total_meetings']}"
        )
        if self.include_incomplete:
            self.logger.info(
                f"Незавершённых (включены): {self.stats['total_meetings_incomplete']}"
            )
        else:
            self.logger.info(
                f"Незавершённых (пропущено): {skipped_incomplete}"
            )
        self.logger.debug(f"Пустых конференций пропущено: {skipped_empty}")
    
    def _generate_report(self):
        """Генерирует и сохраняет отчёт."""
        avg_duration = 0
        if self.stats['total_meetings'] > 0:
            avg_duration = (
                self.stats['total_duration_seconds'] / 
                self.stats['total_meetings']
            )
    
        local_now = self._get_current_time_local()
    
        report_lines = [
            "=" * 80,
            "ОТЧЁТ ПО СТАТИСТИКЕ ВСТРЕЧ ТЕЛЕМОСТА",
            "=" * 80,
            "",
            f"Часовой пояс: {self.tz_name}",
            f"Период: {self.date_from.strftime('%Y-%m-%d')} - "
            f"{self.date_to.strftime('%Y-%m-%d')}",
            f"Дней в периоде: {(self.date_to - self.date_from).days + 1}",
            f"Режим: {'включая незавершённые' if self.include_incomplete else 'только завершённые встречи'}",
            "",
            "-" * 80,
            "ОБЩАЯ СТАТИСТИКА",
            "-" * 80,
            "",
            f"  Завершённых встреч:          {self.stats['total_meetings']:,}",
        ]
    
        if self.include_incomplete:
            report_lines.append(
                f"  Незавершённых встреч:        "
                f"{self.stats['total_meetings_incomplete']:,}"
            )
    
        report_lines.extend([
            f"  Общая длительность:          "
            f"{self._format_duration(self.stats['total_duration_seconds'])}",
            f"  Средняя длительность:        "
            f"{self._format_duration(int(avg_duration))}",
            f"  Уникальных пользователей:    "
            f"{len(self.stats['unique_users']):,}",
            "",
            "-" * 80,
            f"СТАТИСТИКА ПО ДНЯМ ({self.tz_name})",
            "-" * 80,
            "",
        ])
    
        if self.include_incomplete:
            report_lines.append(
                f"{'Дата':<12} | {'Встречи':>8} | {'Незаверш.':>9} | "
                f"{'Общая длит.':>16} | {'Средняя':>12} | {'Польз.':>7}"
            )
        else:
            report_lines.append(
                f"{'Дата':<12} | {'Встречи':>8} | "
                f"{'Общая длит.':>16} | {'Средняя':>12} | {'Польз.':>7}"
            )
    
        report_lines.append("-" * 80)
    
        sorted_days = sorted(self.stats['daily_stats'].keys())
    
        for date in sorted_days:
            if date == "N/A":
                continue
            daily = self.stats['daily_stats'][date]
            meetings = daily['meetings']
            incomplete = daily['meetings_incomplete']
            duration = daily['duration_seconds']
            users = len(daily['users'])
            avg = duration / meetings if meetings > 0 else 0
        
            if self.include_incomplete:
                report_lines.append(
                    f"{date:<12} | {meetings:>8,} | {incomplete:>9,} | "
                    f"{self._format_duration(duration):>16} | "
                    f"{self._format_duration(int(avg)):>12} | {users:>7,}"
                )
            else:
                report_lines.append(
                    f"{date:<12} | {meetings:>8,} | "
                    f"{self._format_duration(duration):>16} | "
                    f"{self._format_duration(int(avg)):>12} | {users:>7,}"
                )
    
        report_lines.extend([
            "-" * 80,
            "",
            "ТЕХНИЧЕСКАЯ ИНФОРМАЦИЯ",
            "-" * 80,
            f"  Обработано событий:          {self.events_processed:,}",
            f"  API вызовов:                 {self.api_calls:,}",
            f"  Повторных попыток:           {self.retries_count:,}",
            "",
            "ВЫХОДНЫЕ ФАЙЛЫ",
            "-" * 80,
            f"  meetings.csv      - все конференции с участниками",
            f"  participants.csv  - детальный список участников",
            f"  report.txt        - этот отчёт",
            f"  execution.log     - лог выполнения",
            "",
            f"Отчёт сгенерирован: {local_now.strftime('%Y-%m-%d %H:%M:%S')} "
            f"({self.tz_name})",
            f"Результаты сохранены в: {os.path.abspath(self.output_dir)}/",
            ""
        ])
    
        report_text = "\n".join(report_lines)
    
        print("\n" + report_text)
    
        report_path = os.path.join(self.output_dir, "report.txt")
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(report_text)
    
        self.logger.info(f"Отчёт сохранён в {report_path}")
    
    def collect(self):
        """Основной метод сбора статистики."""
        self.logger.info("=" * 60)
        self.logger.info("Начало сбора статистики Телемоста")
        self.logger.info(f"Часовой пояс: {self.tz_name}")
        self.logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            events = self._fetch_all_events()
            self._process_events(events)
            
            del events
            gc.collect()
            
            self._calculate_statistics()
            self._generate_report()
            
            self.conferences.clear()
            gc.collect()
            
        except Exception as e:
            self.logger.exception(f"Ошибка при сборе статистики: {e}")
            raise
        
        elapsed_time = time.time() - start_time
        self.logger.info(f"Сбор завершён за {elapsed_time:.2f} секунд")


def main():
    """Точка входа."""
    if OAUTH_TOKEN == "YOUR_OAUTH_TOKEN_HERE":
        print("=" * 60)
        print("ОШИБКА: Укажите OAUTH_TOKEN в настройках скрипта")
        print("=" * 60)
        return
    
    if ORG_ID == "YOUR_ORG_ID_HERE":
        print("=" * 60)
        print("ОШИБКА: Укажите ORG_ID в настройках скрипта")
        print("=" * 60)
        return
    
    collector = TelemostStatsCollector(
        oauth_token=OAUTH_TOKEN,
        org_id=ORG_ID,
        date_from=DATE_FROM,
        date_to=DATE_TO,
        period_days=PERIOD_DAYS,
        timezone_offset_hours=TIMEZONE_OFFSET_HOURS,
        include_incomplete=INCLUDE_INCOMPLETE
    )
    
    try:
        collector.collect()
    except KeyboardInterrupt:
        print("\n\nПрервано пользователем")
    except Exception as e:
        print(f"\nКритическая ошибка: {e}")
        raise


if __name__ == "__main__":
    main()
