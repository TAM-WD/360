'''
Скрипт предназначен для выгрузки списка участников встреч, а также зрителей трансляций.

Для запуска скрипта необходим Python версии 3.7 или выше, а также библиотеки requests и tabulate.

Установить их можно с помощью pip. Команда выглядит так (может отличаться в зависимости от ОС):

pip install requests tabulate

Также предварительно нужно получить токен с правами ya360_security:read_auditlog

Для запуска в самом скрипте обязательно указать:
OAUTH_TOKEN = "ваш_токен_здесь"  # Токен из предыдущего шага
ORG_ID = "12345678"               # ID организации

Запустить скрипт можно через командную строку или программу с возможностью запуска кода (например, VSCode)

Для кастомизации процесса поиска есть:
1. Настройки периода выгрузки.

По умолчанию установлены последние 70 дней. Если нужно найти конкретную встречу или трансляцию за определённую дату, то указывать надо так:

DATE_FROM = "13.11.2025"
DATE_TO = "13.11.2025"

2. Фильтр по встречам и трансляциям. 

По умолчанию не заполнено, поиск будет выполняться по всем встречам в периоде.

Если нужно искать конкретную встречу, трансляцию, или их список, то необходимо указать их ID в параметрах:

CONFERENCE_IDS_FILTER = [
    "123456789",
    "987654321"
]

LIVE_STREAM_IDS_FILTER = [
    "aaa1111a1a1a1eb5a76645f6c764523f",
]

Не рекомендуется заполнять оба этих поля одновременно.

3. Настройка вывода таблиц.

Можно настроить показ информации о встрече, участникам и зрителях.

Поиск трансляции по ID встречи:

SHOW_CONFERENCE_INFO = True
SHOW_PARTICIPANTS_TABLE = True
SHOW_VIEWERS_TABLE = True

Поиск трансляции по ID трансляции:
SHOW_CONFERENCE_INFO = False 
SHOW_PARTICIPANTS_TABLE = False
SHOW_VIEWERS_TABLE = True 

Поиск встречи по ID встречи:
SHOW_CONFERENCE_INFO = True
SHOW_PARTICIPANTS_TABLE = True
SHOW_VIEWERS_TABLE = False

4. Изменение стиля таблиц.

TABLE_STYLE = "fancy_grid"  # По умолчанию

Доступные стили:

"fancy_grid" — красивая сетка с двойными линиями
"grid" — простая сетка
"simple" — минималистичный стиль
"github" — стиль GitHub Markdown
"pipe" — стиль Markdown с |

5. Настройка имени файла

FILE_PREFIX = "my_report"  # Вместо telemost_events_report
ADD_FILTERS_TO_FILENAME = True  # Добавлять информацию о фильтрах

6. Настройка длины полей в таблицах

MAX_NAME_LENGTH = 35   # Максимальная длина имени
MAX_LOGIN_LENGTH = 40  # Максимальная длина логина
'''

import requests
import csv
from datetime import datetime, timedelta
import time
from tabulate import tabulate
from collections import defaultdict
import sys

# ===== НАСТРОЙКИ =====
OAUTH_TOKEN = ""  # Токен с правами ya360_security:read_auditlog
ORG_ID = ""  # ID организации

API_URL = f"https://cloud-api.yandex.net/v1/auditlog/organizations/{ORG_ID}/events"

EVENT_TYPES = [
    "telemost_conference.live_stream.viewer.joined",
    "telemost_conference.live_stream.started",
    "telemost_conference.live_stream.access_level_changed",
    "telemost_conference.live_stream.ended",
    "telemost_conference.created",
    "telemost_conference.started",
    "telemost_conference.ended",
    "telemost_conference.peer.joined"
]

# ===== ФИЛЬТРЫ =====

# 1. Период выгрузки (в формате ДД.ММ.ГГГГ или пусто для автоматического периода)
# Если не указаны - берется период последних 70 дней
DATE_FROM = ""  # Например: "01.10.2024" или ""
DATE_TO = ""    # Например: "09.11.2024" или ""

# 2. Фильтрация по ID конференций (локальная фильтрация)
# Если список пустой - выгружаются все конференции
CONFERENCE_IDS_FILTER = [
    # "conference_id_1",
    # "conference_id_2",
]

# 3. Фильтрация по ID трансляций (локальная фильтрация)
# Если список пустой - выгружаются все трансляции
LIVE_STREAM_IDS_FILTER = [
    # "live_stream_id_1",
    # "live_stream_id_2",
]

# ===== НАСТРОЙКИ ФАЙЛА =====
# Префикс для имени файла (можно изменить на свой)
FILE_PREFIX = "telemost_events_report"

# Добавлять ли информацию о фильтрах в имя файла
ADD_FILTERS_TO_FILENAME = True

ENABLE_LOGGING = True

# Префикс для имени лог-файла
LOG_FILE_PREFIX = "telemost_log"

# ===== НАСТРОЙКИ ВЫВОДА =====
# Показывать ли детальные таблицы в консоли
SHOW_CONFERENCE_INFO = True         # Информация о встречах. Если поиск по LIVE_STREAM_IDS_FILTER, то указать False
SHOW_PARTICIPANTS_TABLE = True      # Участники встреч. Если поиск по LIVE_STREAM_IDS_FILTER, то указать False
SHOW_VIEWERS_TABLE = True           # Зрители трансляций. Если поиск по обычной встрече, то указать False

# Стиль таблицы tabulate
# Доступные: "grid", "fancy_grid", "pipe", "orgtbl", "presto", "pretty", 
#            "psql", "rst", "simple", "github", "rounded_grid"
TABLE_STYLE = "fancy_grid"

MAX_NAME_LENGTH = 35
MAX_LOGIN_LENGTH = 40

headers = {
    "Authorization": f"OAuth {OAUTH_TOKEN}",
    "Content-Type": "application/json"
}

log_file = None
log_filename = None
original_stdout = None

class TeeLogger:
    def __init__(self, file_object, terminal):
        self.file = file_object
        self.terminal = terminal
        
    def write(self, message):
        self.terminal.write(message)
        if self.file:
            try:
                self.file.write(message)
                self.file.flush()
            except:
                pass
    
    def flush(self):
        self.terminal.flush()
        if self.file:
            try:
                self.file.flush()
            except:
                pass

def setup_logger():
    global log_file, log_filename, original_stdout
    
    if not ENABLE_LOGGING:
        return None
    
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    log_filename = f"{LOG_FILE_PREFIX}_{timestamp}.log"
    
    try:
        log_file = open(log_filename, 'w', encoding='utf-8')
        
        original_stdout = sys.stdout
        
        sys.stdout = TeeLogger(log_file, original_stdout)
        
        return log_filename
    except Exception as e:
        print(f"⚠️  Не удалось создать лог-файл: {e}")
        return None

def close_logger():
    global log_file, original_stdout
    
    if original_stdout:
        sys.stdout = original_stdout
    
    if log_file:
        try:
            log_file.close()
        except:
            pass

def parse_date(date_str):
    if not date_str or not date_str.strip():
        return None
    
    try:
        date_obj = datetime.strptime(date_str.strip(), "%d.%m.%Y")
        return date_obj
    except ValueError:
        try:
            date_obj = datetime.strptime(date_str.strip(), "%Y-%m-%d")
            return date_obj
        except ValueError:
            print(f"❌ Ошибка: Неверный формат даты '{date_str}'. Используйте ДД.ММ.ГГГГ (например: 19.09.2025)")
            return None

def get_date_range(date_from_str, date_to_str): # Если даты не указаны - используем последние 70 дней
    if not date_from_str and not date_to_str:
        to_date = datetime.now()
        from_date = to_date - timedelta(days=70)
        return from_date, to_date, False
    
    from_date = None
    to_date = None
    
    if date_from_str:
        from_date = parse_date(date_from_str)
        if from_date is None:
            return None, None, None
    else:
        from_date = datetime.now() - timedelta(days=70)
    
    if date_to_str:
        to_date = parse_date(date_to_str)
        if to_date is None:
            return None, None, None
    else:
        to_date = datetime.now()
    
    from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
    to_date = to_date.replace(hour=23, minute=59, second=59, microsecond=0)
    
    if from_date > to_date:
        print(f"❌ Ошибка: Дата начала ({date_from_str}) позже даты окончания ({date_to_str})")
        return None, None, None
    
    return from_date, to_date, True

def get_audit_logs(from_date, to_date, event_types, iteration_key=None):
    params = {
        "started_at": from_date,
        "ended_at": to_date,
        "count": 100,
        "types": ",".join(event_types)
    }
    
    if iteration_key:
        params["iteration_key"] = iteration_key
    
    try:
        response = requests.get(API_URL, headers=headers, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Ошибка: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"   Ответ сервера: {e.response.text}")
        return None

def filter_events(items, conference_ids=None, live_stream_ids=None):
    filtered_items = items
    
    if conference_ids:
        filtered_items = [
            item for item in filtered_items
            if item.get("event", {}).get("meta", {}).get("conference_id", "") in conference_ids
        ]
    
    if live_stream_ids:
        filtered_items = [
            item for item in filtered_items
            if item.get("event", {}).get("meta", {}).get("live_stream_id", "") in live_stream_ids
        ]
    
    return filtered_items

def fetch_events(from_date, to_date, event_types, conference_ids_filter=None, live_stream_ids_filter=None):
    all_events = []
    filtered_events = []
    iteration_key = None
    page = 0
    total_items = 0
    
    has_filters = bool(conference_ids_filter or live_stream_ids_filter)
    
    print("Загрузка событий...")
    
    while True:
        page += 1
        
        data = get_audit_logs(from_date, to_date, event_types, iteration_key)
        
        if not data:
            break
        
        items = data.get("items", [])
        total_items += len(items)
        all_events.extend(items)
        
        if has_filters:
            filtered_page_items = filter_events(items, conference_ids_filter, live_stream_ids_filter)
            filtered_events.extend(filtered_page_items)
            items_to_show = filtered_page_items
        else:
            filtered_events.extend(items)
            items_to_show = items
        
        event_counts = {}
        for item in items_to_show:
            event_type = item.get("event", {}).get("type", "unknown")
            event_counts[event_type] = event_counts.get(event_type, 0) + 1
        
        print(f"  Страница {page}: получено {len(items)} событий", end="")
        
        if has_filters:
            print(f", после фильтрации: {len(items_to_show)}", end="")
        
        if event_counts:
            counts_str = ", ".join([f"{t.split('.')[-1]}: {c}" for t, c in event_counts.items()])
            print(f" ({counts_str})", end="")
        
        print(f" | Всего: {len(filtered_events)}")
        
        iteration_key = data.get("iteration_key")
        
        if not iteration_key or len(items) == 0:
            break
        
        time.sleep(0.2)
    
    print(f"\n📊 Обработано всего событий от API: {total_items}")
    
    if has_filters:
        print(f"🔍 После применения фильтров: {len(filtered_events)}")
        print(f"   Отфильтровано: {total_items - len(filtered_events)} событий")
    
    final_counts = {}
    for item in filtered_events:
        event_type = item.get("event", {}).get("type", "unknown")
        final_counts[event_type] = final_counts.get(event_type, 0) + 1
    
    if final_counts:
        print(f"\n🎯 Распределение по типам событий:")
        for event_type, count in sorted(final_counts.items()):
            print(f"   • {event_type}: {count}")
    
    if conference_ids_filter and filtered_events:
        found_conf_ids = set()
        for item in filtered_events:
            conf_id = item.get("event", {}).get("meta", {}).get("conference_id", "")
            if conf_id:
                found_conf_ids.add(conf_id)
        
        print(f"\n📋 Найдены следующие конференции из фильтра:")
        for conf_id in sorted(found_conf_ids):
            count = sum(1 for item in filtered_events 
                       if item.get("event", {}).get("meta", {}).get("conference_id") == conf_id)
            print(f"   • {conf_id}: {count} событий")
        
        not_found = set(conference_ids_filter) - found_conf_ids
        if not_found:
            print(f"\n⚠️  ID конференций из фильтра, по которым не найдено событий:")
            for conf_id in sorted(not_found):
                print(f"   • {conf_id}")
    
    if live_stream_ids_filter and filtered_events:
        found_stream_ids = set()
        for item in filtered_events:
            stream_id = item.get("event", {}).get("meta", {}).get("live_stream_id", "")
            if stream_id:
                found_stream_ids.add(stream_id)
        
        print(f"\n📡 Найдены следующие трансляции из фильтра:")
        for stream_id in sorted(found_stream_ids):
            count = sum(1 for item in filtered_events 
                       if item.get("event", {}).get("meta", {}).get("live_stream_id") == stream_id)
            print(f"   • {stream_id}: {count} событий")
        
        not_found = set(live_stream_ids_filter) - found_stream_ids
        if not_found:
            print(f"\n⚠️  ID трансляций из фильтра, по которым не найдено событий:")
            for stream_id in sorted(not_found):
                print(f"   • {stream_id}")
    
    return filtered_events

def extract_event_info(item):
    event = item.get("event", {})
    meta = event.get("meta", {})
    event_type = event.get("type", "")
    
    conference_start = meta.get("conference_start")
    if conference_start:
        try:
            conference_start_dt = datetime.fromtimestamp(conference_start / 1000).strftime("%Y-%m-%d %H:%M:%S")
        except:
            conference_start_dt = str(conference_start)
    else:
        conference_start_dt = ""
    
    occurred_at = event.get("occurred_at", "")
    if occurred_at:
        try:
            occurred_dt = datetime.fromisoformat(occurred_at.replace("+00:00", "")).strftime("%Y-%m-%d %H:%M:%S")
        except:
            occurred_dt = occurred_at
    else:
        occurred_dt = ""
    
    info = {
        "Тип события": event_type.split(".")[-1].replace("_", " ").title(),
        "Полный тип события": event_type,
        "Дата и время": occurred_at,
        "Дата и время (форматированная)": occurred_dt,
        "Имя участника": item.get("user_name", ""),
        "Email участника": item.get("user_login", ""),
        "UID участника": event.get("uid", ""),
        "IP адрес": event.get("ip", ""),
        "ID конференции": meta.get("conference_id", ""),
        "ID трансляции": meta.get("live_stream_id", ""),
        "Начало конференции": conference_start_dt,
        "Сервис": event.get("service", ""),
        "Версия приложения": meta.get("user_app_version", "") or "Не указана",
        "Статус": event.get("status", ""),
        "Роль": meta.get("role", "") or meta.get("peer_role", "")
    }
    
    if "access_level_changed" in event_type:
        info["Уровень доступа"] = meta.get("access_level", "")
    
    return info

def truncate_string(s, max_length):
    if not s:
        return ""
    s = str(s)
    if len(s) <= max_length:
        return s
    return s[:max_length-3] + "..."

def format_role(role):
    role_mapping = {
        "host": "Организатор",
        "moderator": "Модератор",
        "participant": "Участник",
        "viewer": "Зритель",
        "": "Участник"
    }
    return role_mapping.get(role.lower(), role or "Участник")

def format_duration(start_time, end_time):
    if not start_time or not end_time:
        return "—"
    
    try:
        start_dt = datetime.fromisoformat(start_time.replace("+00:00", ""))
        end_dt = datetime.fromisoformat(end_time.replace("+00:00", ""))
        duration = end_dt - start_dt
        
        hours = duration.seconds // 3600
        minutes = (duration.seconds % 3600) // 60
        seconds = duration.seconds % 60
        
        if hours > 0:
            return f"{hours}ч {minutes}м {seconds}с"
        elif minutes > 0:
            return f"{minutes}м {seconds}с"
        else:
            return f"{seconds}с"
    except:
        return "—"

def group_events_by_conference(events_data):
    conferences = defaultdict(lambda: {
        'created': None,
        'started': None,
        'ended': None,
        'stream_started': None,
        'stream_ended': None,
        'participants': [],
        'viewers': [],
        'organizer': None,
        'conference_start_time': None
    })
    
    for event in events_data:
        conf_id = event["ID конференции"]
        if not conf_id:
            continue
            
        event_type = event["Полный тип события"]
        
        if event_type == "telemost_conference.created":
            conferences[conf_id]['created'] = event
            conferences[conf_id]['organizer'] = event.get("Имя участника") or event.get("Email участника")
        elif event_type == "telemost_conference.started":
            conferences[conf_id]['started'] = event
        elif event_type == "telemost_conference.ended":
            conferences[conf_id]['ended'] = event
        elif event_type == "telemost_conference.peer.joined":
            conferences[conf_id]['participants'].append(event)
        elif event_type == "telemost_conference.live_stream.started":
            conferences[conf_id]['stream_started'] = event
        elif event_type == "telemost_conference.live_stream.ended":
            conferences[conf_id]['stream_ended'] = event
        elif event_type == "telemost_conference.live_stream.viewer.joined":
            conferences[conf_id]['viewers'].append(event)
        
        if event["Начало конференции"]:
            conferences[conf_id]['conference_start_time'] = event["Начало конференции"]
    
    return conferences

def print_conference_info(conferences):
    if not conferences:
        return
    
    print("\n" + "="*100)
    print("📊 ИНФОРМАЦИЯ О ВСТРЕЧАХ")
    print("="*100)
    
    sorted_conferences = sorted(
        conferences.items(),
        key=lambda x: x[1].get('conference_start_time') or '',
        reverse=True
    )
    
    for idx, (conf_id, data) in enumerate(sorted_conferences, 1):
        print(f"\n{'─' * 100}")
        print(f"🎥 ВСТРЕЧА #{idx}")
        print(f"{'─' * 100}")
        
        info_table = []
        
        info_table.append(["ID встречи", conf_id])
        
        start_time = "—"
        if data['started']:
            start_time = data['started']['Дата и время (форматированная)']
        elif data['conference_start_time']:
            start_time = data['conference_start_time']
        info_table.append(["Начало", start_time])
        
        end_time = "—"
        if data['ended']:
            end_time = data['ended']['Дата и время (форматированная)']
        info_table.append(["Окончание", end_time])
        
        duration = "—"
        if data['started'] and data['ended']:
            duration = format_duration(
                data['started']['Дата и время'],
                data['ended']['Дата и время']
            )
        info_table.append(["Длительность", duration])
        
        participants_count = len(data['participants'])
        info_table.append(["Количество участников", participants_count])
        
        viewers_count = len(data['viewers'])
        info_table.append(["Количество зрителей", viewers_count])
        
        stream_id = "—"
        if data['stream_started']:
            stream_id = data['stream_started'].get('ID трансляции', '—')
        info_table.append(["ID трансляции", stream_id or "—"])
        
        organizer = data.get('organizer', '—')
        info_table.append(["Организатор", organizer])
        
        status = "Не завершена"
        if data['ended']:
            status = "Завершена"
        elif data['started']:
            status = "В процессе"
        elif data['created']:
            status = "Создана"
        info_table.append(["Статус", status])
        
        print(tabulate(info_table, tablefmt=TABLE_STYLE))
    
    print(f"\n{'═' * 100}\n")

def print_participants_table(conferences):
    if not conferences:
        return
        
    has_participants = any(conf['participants'] for conf in conferences.values())
    
    if not has_participants:
        return
    
    print("\n" + "="*120)
    print("👥 УЧАСТНИКИ ВСТРЕЧ")
    print("="*120)
    
    sorted_conferences = sorted(
        conferences.items(),
        key=lambda x: x[1].get('conference_start_time') or '',
        reverse=True
    )
    
    for conf_id, data in sorted_conferences:
        if not data['participants']:
            continue
        
        print(f"\n🎥 Конференция: {conf_id}")
        if data['conference_start_time']:
            print(f"📅 Начало: {data['conference_start_time']}")
        print(f"👥 Всего участников: {len(data['participants'])}\n")
        
        sorted_participants = sorted(data['participants'], key=lambda x: x['Дата и время'])
        
        table_data = []
        for idx, participant in enumerate(sorted_participants, 1):
            name = participant['Имя участника'] if participant['Имя участника'] else "Неизвестный"
            name = truncate_string(name, MAX_NAME_LENGTH)
            
            login = participant['Email участника'] if participant['Email участника'] else f"UID: {participant['UID участника']}"
            login = truncate_string(login, MAX_LOGIN_LENGTH)
            
            role = format_role(participant.get('Роль', ''))
            
            time_str = participant['Дата и время (форматированная)']
            
            table_data.append([idx, name, login, role, time_str])
        
        headers = ["№", "Участник", "Логин", "Роль", "Время входа"]
        print(tabulate(table_data, headers=headers, tablefmt=TABLE_STYLE))
        print()

def print_viewers_table(conferences):
    if not conferences:
        return
        
    has_viewers = any(conf['viewers'] for conf in conferences.values())
    
    if not has_viewers:
        return
    
    print("\n" + "="*120)
    print("📺 ЗРИТЕЛИ ТРАНСЛЯЦИЙ")
    print("="*120)
    
    sorted_conferences = sorted(
        conferences.items(),
        key=lambda x: x[1].get('conference_start_time') or '',
        reverse=True
    )
    
    for conf_id, data in sorted_conferences:
        if not data['viewers']:
            continue
        
        print(f"\n🎥 Конференция: {conf_id}")
        if data['conference_start_time']:
            print(f"📅 Начало: {data['conference_start_time']}")
        if data.get('stream_started'):
            stream_id = data['stream_started'].get('ID трансляции', '')
            if stream_id:
                print(f"📡 ID трансляции: {stream_id}")
        print(f"👁️  Всего зрителей: {len(data['viewers'])}\n")
        
        sorted_viewers = sorted(data['viewers'], key=lambda x: x['Дата и время'])
        
        table_data = []
        for idx, viewer in enumerate(sorted_viewers, 1):
            name = viewer['Имя участника'] if viewer['Имя участника'] else "Неизвестный"
            name = truncate_string(name, MAX_NAME_LENGTH)
            
            login = viewer['Email участника'] if viewer['Email участника'] else f"UID: {viewer['UID участника']}"
            login = truncate_string(login, MAX_LOGIN_LENGTH)
            
            time_str = viewer['Дата и время (форматированная)']
            
            table_data.append([idx, name, login, time_str])
        
        headers = ["№", "Зритель", "Логин", "Время входа"]
        print(tabulate(table_data, headers=headers, tablefmt=TABLE_STYLE))
        print()

def generate_filename(prefix, date_from_str, date_to_str, conference_ids, stream_ids, add_filters):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    
    filename_parts = [prefix, timestamp]
    
    if add_filters:
        filter_parts = []
        
        if date_from_str or date_to_str:
            date_part = ""
            if date_from_str:
                date_part += date_from_str.replace(".", "")
            if date_to_str:
                if date_part:
                    date_part += "-" + date_to_str.replace(".", "")
                else:
                    date_part += date_to_str.replace(".", "")
            if date_part:
                filter_parts.append(f"period_{date_part}")
        
        if conference_ids:
            filter_parts.append(f"conf_{len(conference_ids)}")
        
        if stream_ids:
            filter_parts.append(f"stream_{len(stream_ids)}")
        
        if filter_parts:
            filename_parts.append("_".join(filter_parts))
    
    filename = "_".join(filename_parts) + ".csv"
    return filename

def save_to_csv(events_data, date_from_str="", date_to_str="", conference_ids=None, stream_ids=None):
    if not events_data:
        print("\n⚠️ Нет данных для сохранения")
        return None
    
    filename = generate_filename(
        FILE_PREFIX,
        date_from_str,
        date_to_str,
        conference_ids or [],
        stream_ids or [],
        ADD_FILTERS_TO_FILENAME
    )
    
    fieldnames = [
        "Тип события",
        "Полный тип события",
        "Дата и время (форматированная)",
        "Имя участника",
        "Email участника",
        "UID участника",
        "Роль",
        "IP адрес",
        "ID конференции",
        "ID трансляции",
        "Начало конференции",
        "Сервис",
        "Версия приложения",
        "Статус"
    ]
    
    all_keys = set()
    for event in events_data:
        all_keys.update(event.keys())
    
    extra_fields = sorted(all_keys - set(fieldnames) - {'Дата и время'})
    fieldnames.extend(extra_fields)
    
    csv_data = []
    for event in events_data:
        csv_row = {k: v for k, v in event.items() if k != 'Дата и время'}
        csv_data.append(csv_row)
    
    try:
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(csv_data)
        
        print(f"\n✅ Данные успешно сохранены в файл:")
        print(f"   📄 {filename}")
        print(f"   📊 Всего записей: {len(events_data)}")
        
        return filename
    except Exception as e:
        print(f"❌ Ошибка при сохранении CSV: {e}")
        return None

def print_summary(events_data):
    if not events_data:
        return
    
    conferences = {}
    for event in events_data:
        conf_id = event["ID конференции"]
        if not conf_id:
            continue
        if conf_id not in conferences:
            conferences[conf_id] = {
                'start': event["Начало конференции"],
                'stream_id': event["ID трансляции"],
                'events': []
            }
        conferences[conf_id]['events'].append(event)
    
    print("\n" + "="*120)
    print("📈 СТАТИСТИКА")
    print("="*120)
    print(f"Всего конференций: {len(conferences)}")
    print(f"Всего событий: {len(events_data)}")
    
    unique_users = set()
    for event in events_data:
        if event['Email участника']:
            unique_users.add(event['Email участника'])
        elif event['UID участника']:
            unique_users.add(str(event['UID участника']))
    
    if unique_users:
        print(f"Уникальных пользователей: {len(unique_users)}")
    
    event_type_counts = {}
    for event in events_data:
        event_type = event['Тип события']
        event_type_counts[event_type] = event_type_counts.get(event_type, 0) + 1
    
    print(f"\n📋 По типам событий:")
    for event_type, count in sorted(event_type_counts.items()):
        print(f"   • {event_type}: {count}")

def main():
    script_start_time = datetime.now()
    
    log_file_created = setup_logger()
    
    print("="*120)
    print("📺 ВЫГРУЗКА СОБЫТИЙ ЯНДЕКС ТЕЛЕМОСТ")
    print("="*120)
    print(f"🕐 Время запуска: {script_start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    if ENABLE_LOGGING and log_file_created:
        print(f"📝 Лог-файл: {log_file_created}")
    
    if not OAUTH_TOKEN or not ORG_ID:
        print("\n❌ Ошибка: Не указаны OAUTH_TOKEN или ORG_ID в настройках скрипта")
        close_logger()
        return
    
    if not EVENT_TYPES:
        print("\n❌ Ошибка: Не указаны типы событий (EVENT_TYPES)")
        close_logger()
        return
    
    from_date, to_date, is_custom = get_date_range(DATE_FROM, DATE_TO)
    
    if from_date is None or to_date is None:
        print("\n❌ Не удалось определить период выгрузки")
        close_logger()
        return
    
    from_date_str = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    to_date_str = to_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    print(f"\n📅 Период выгрузки:")
    if is_custom:
        print(f"   Режим: Пользовательский")
        if DATE_FROM:
            print(f"   С: {DATE_FROM} → {from_date_str}")
        else:
            print(f"   С: {from_date_str} (по умолчанию)")
        if DATE_TO:
            print(f"   По: {DATE_TO} → {to_date_str}")
        else:
            print(f"   По: {to_date_str} (по умолчанию)")
    else:
        print(f"   Режим: Автоматический (последние 70 дней)")
        print(f"   С: {from_date_str}")
        print(f"   По: {to_date_str}")
    
    days_diff = (to_date - from_date).days + 1
    print(f"   Дней в периоде: {days_diff}")
    
    print(f"\n🔍 Фильтры API:")
    print(f"   Типы событий: {len(EVENT_TYPES)}")
    for event_type in EVENT_TYPES:
        print(f"      • {event_type}")
    
    has_local_filters = bool(CONFERENCE_IDS_FILTER or LIVE_STREAM_IDS_FILTER)
    
    if has_local_filters:
        print(f"\n🎯 Фильтры (применяются после получения данных):")
        
        if CONFERENCE_IDS_FILTER:
            print(f"   ID конференций ({len(CONFERENCE_IDS_FILTER)}):")
            for conf_id in CONFERENCE_IDS_FILTER[:5]:
                print(f"      • {conf_id}")
            if len(CONFERENCE_IDS_FILTER) > 5:
                print(f"      ... и ещё {len(CONFERENCE_IDS_FILTER) - 5}")
        else:
            print(f"   ID конференций: НЕТ (все конференции)")
        
        if LIVE_STREAM_IDS_FILTER:
            print(f"   ID трансляций ({len(LIVE_STREAM_IDS_FILTER)}):")
            for stream_id in LIVE_STREAM_IDS_FILTER[:5]:
                print(f"      • {stream_id}")
            if len(LIVE_STREAM_IDS_FILTER) > 5:
                print(f"      ... и ещё {len(LIVE_STREAM_IDS_FILTER) - 5}")
        else:
            print(f"   ID трансляций: НЕТ (все трансляции)")
    else:
        print(f"\n📋 Фильтры: НЕТ (будут загружены все события)")
    
    print()
    
    events = fetch_events(
        from_date_str, 
        to_date_str, 
        EVENT_TYPES, 
        CONFERENCE_IDS_FILTER if CONFERENCE_IDS_FILTER else None,
        LIVE_STREAM_IDS_FILTER if LIVE_STREAM_IDS_FILTER else None
    )
    
    if not events:
        print("\n⚠️ События не найдены за указанный период")
        if has_local_filters:
            print("\n💡 Возможно, указанные фильтры слишком строгие")
            print("   Попробуйте:")
            print("   - Проверить правильность ID конференций и трансляций")
            print("   - Изменить период выгрузки")
            print("   - Запустить без фильтров, чтобы увидеть все доступные данные")
        else:
            print("\nВозможные причины:")
            print("  - В этот период не было событий указанных типов")
            print("  - Неверно указаны типы событий")
            print("  - Проблемы с доступом или токеном")
        close_logger()
        return
    
    print(f"\n✅ Найдено событий: {len(events)}")
    
    events_data = [extract_event_info(item) for item in events]
    
    print_summary(events_data)
    
    conferences = group_events_by_conference(events_data)
    
    if not conferences:
        print("\n⚠️ Не удалось сгруппировать события по конференциям")
        print("   Возможно, в событиях отсутствуют ID конференций")
    else:
        if SHOW_CONFERENCE_INFO:
            print_conference_info(conferences)
        
        if SHOW_PARTICIPANTS_TABLE:
            print_participants_table(conferences)
        
        if SHOW_VIEWERS_TABLE:
            print_viewers_table(conferences)
    
    saved_filename = save_to_csv(
        events_data,
        DATE_FROM,
        DATE_TO,
        CONFERENCE_IDS_FILTER if CONFERENCE_IDS_FILTER else None,
        LIVE_STREAM_IDS_FILTER if LIVE_STREAM_IDS_FILTER else None
    )
    
    script_end_time = datetime.now()
    execution_time = (script_end_time - script_start_time).total_seconds()
    
    print("\n" + "="*120)
    print("✅ ВЫГРУЗКА ЗАВЕРШЕНА")
    print("="*120)
    print(f"🕐 Время завершения: {script_end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Время выполнения: {execution_time:.2f} секунд")
    if saved_filename:
        print(f"📄 Файл CSV: {saved_filename}")
    if ENABLE_LOGGING and log_filename:
        print(f"📝 Файл логов: {log_filename}")
    print("="*120)
    
    close_logger()

if __name__ == "__main__":
    main()
