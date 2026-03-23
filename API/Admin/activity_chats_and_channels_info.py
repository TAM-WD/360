#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import csv
import os
import time
import re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

# OAuth токен и ID организации
OAUTH_TOKEN = ""  # Замените на ваш OAuth токен с правами ya360_security:read_auditlog
ORG_ID = ""  # ID организации

# Настройки периода выгрузки
START_DATE = "2026-01-01"  # Формат: "2025-01-01" или None
END_DATE = "2026-03-23"    # Формат: "2025-12-31" или None (None = сегодня)
DAYS = 30                   # Количество дней от END_DATE, если START_DATE = None

# Настройки retry
MAX_RETRIES = 5
RETRY_DELAY = 2
RETRY_BACKOFF = 2

# ============================================================

AUDIT_LOGS_URL = f"https://cloud-api.yandex.net/v1/auditlog/organizations/{ORG_ID}/events"
headers = {
    'Authorization': f'OAuth {OAUTH_TOKEN}'
}

# Типы событий мессенджера
MESSENGER_EVENT_TYPES = [
    "messenger_chat.created",
    "messenger_chat.info_changed",
    "messenger_chat.invite_link_renewed",
    "messenger_chat.member_rights_changed",
    "messenger_chat.member.added",
    "messenger_chat.member.role_changed",
    "messenger_chat.member.removed",
    "messenger_chat.group_added",
    "messenger_chat.group_removed",
    "messenger_chat.department_added",
    "messenger_chat.department_removed",
    "messenger_chat.file_sent",
    "messenger_chat.message_deleted",
    "messenger_chat.call_started",
    "messenger_user.takeout_requested"
]


class Logger:
    """Класс для логирования в файл и консоль"""
    
    def __init__(self, log_file):
        self.log_file = log_file
        self.start_time = datetime.now()
        
    def log(self, message, to_console=True):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry + '\n')
        
        if to_console:
            print(message)
    
    def log_separator(self, char='=', length=60):
        self.log(char * length)


class ChatInfo:
    """Класс для хранения информации о чате"""
    
    def __init__(self, chat_id, chat_type):
        self.chat_id = chat_id
        self.chat_type = chat_type
        self.name = None
        self.last_activity = None
        self.event_count = 0
        self.events = []
    
    def update_from_event(self, event):
        """Обновляет информацию о чате из события"""
        self.event_count += 1
        self.events.append(event.get('type', 'unknown'))
        
        # Обновляем дату последней активности
        occurred_at = event.get('occurred_at')
        if occurred_at:
            if self.last_activity is None or occurred_at > self.last_activity:
                self.last_activity = occurred_at
        
        # Извлекаем информацию из meta
        meta = event.get('meta', {})
        chat_info = meta.get('chat_info', {})
        
        # Название чата
        if chat_info.get('name') and not self.name:
            self.name = chat_info['name']
    
    def to_dict(self):
        return {
            'chat_id': self.chat_id,
            'name': self.name or '',
            'type': self.chat_type,
            'last_activity': self.last_activity or '',
            'event_count': self.event_count
        }


def clean_value(value):
    """Очищает значение от специальных символов"""
    if value is None:
        return ''
    
    if isinstance(value, str):
        value = value.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        value = re.sub(r'\s+', ' ', value)
        value = value.strip()
    
    return value


def flatten_dict(d, parent_key='', sep='_'):
    """Преобразует вложенный словарь в плоский"""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            if v and all(isinstance(item, str) for item in v):
                cleaned_items = [clean_value(item) for item in v]
                items.append((new_key, ', '.join(cleaned_items)))
            else:
                cleaned_json = json.dumps(v, ensure_ascii=False)
                items.append((new_key, clean_value(cleaned_json)))
        else:
            items.append((new_key, clean_value(v)))
    return dict(items)


def parse_date_config(date_str):
    """Парсит дату из конфигурации"""
    if date_str is None:
        return None
    
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"❌ Ошибка: неверный формат даты '{date_str}'. Используйте YYYY-MM-DD")
        exit(1)


def make_request_with_retry(url, headers, params, logger, max_retries=MAX_RETRIES):
    """Выполняет запрос с повторными попытками при ошибках"""
    retry_delay = RETRY_DELAY
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            response.raise_for_status()
            return response
            
        except requests.exceptions.HTTPError as e:
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
                
                if 500 <= status_code < 600:
                    if attempt < max_retries - 1:
                        msg = f"⚠️  Ошибка {status_code}, попытка {attempt + 1}/{max_retries}. Ожидание {retry_delay}с..."
                        logger.log(msg)
                        time.sleep(retry_delay)
                        retry_delay *= RETRY_BACKOFF
                        continue
                    else:
                        logger.log(f"❌ Ошибка {status_code} после {max_retries} попыток")
                        raise
                else:
                    raise
            else:
                raise
                
        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout,
                ConnectionResetError) as e:
            if attempt < max_retries - 1:
                msg = f"⚠️  Ошибка соединения, попытка {attempt + 1}/{max_retries}. Ожидание {retry_delay}с..."
                logger.log(msg)
                time.sleep(retry_delay)
                retry_delay *= RETRY_BACKOFF
                continue
            else:
                logger.log(f"❌ Ошибка соединения после {max_retries} попыток")
                raise
        
        except Exception as e:
            logger.log(f"❌ Неожиданная ошибка: {e}")
            raise
    
    raise Exception(f"Не удалось выполнить запрос после {max_retries} попыток")


def get_audit_logs_by_type(event_type, start_date, end_date, logger, count=100):
    """Получает события определенного типа из аудит-логов"""
    ended_at = end_date.isoformat().replace("+00:00", "Z")
    started_at = start_date.isoformat().replace("+00:00", "Z")
    
    params = {
        "count": count,
        "started_at": started_at,
        "ended_at": ended_at,
        "types": event_type
    }
    
    all_events = []
    page = 1
    
    logger.log(f"  → {event_type}...", to_console=True)
    
    while True:
        try:
            response = make_request_with_retry(AUDIT_LOGS_URL, headers, params, logger)
            
            data = response.json()
            items = data.get("items", [])
            
            if not items:
                break
            
            for item in items:
                event = item.get("event", {})
                if event:
                    event['user_login'] = item.get('user_login', '')
                    event['user_name'] = item.get('user_name', '')
                    all_events.append(event)
            
            if page % 10 == 0:
                print(f"    {len(all_events)} событий...", flush=True)
            
            iteration_key = data.get("iteration_key")
            if not iteration_key:
                break
            
            params['iteration_key'] = iteration_key
            page += 1
            
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            logger.log(f"❌ ОШИБКА: {e}")
            logger.log(f"Получено событий до ошибки: {len(all_events)}")
            break
            
        except Exception as e:
            logger.log(f"❌ НЕОЖИДАННАЯ ОШИБКА: {e}")
            break
    
    logger.log(f"    ✓ {len(all_events)} событий")
    return all_events


def extract_chat_info_from_event(event):
    """Извлекает информацию о чате из события"""
    meta = event.get('meta', {})
    
    chat_id = meta.get('chat_id')
    if not chat_id:
        return None, None
    
    chat_info = meta.get('chat_info', {})
    chat_type = chat_info.get('type', 'unknown')
    
    return chat_id, chat_type


def process_events_to_chats(events, logger):
    """Обрабатывает события и группирует их по чатам"""
    chats = {}  # chat_id -> ChatInfo
    
    logger.log(f"\nОбработка {len(events)} событий...")
    
    for event in events:
        chat_id, chat_type = extract_chat_info_from_event(event)
        
        if not chat_id:
            continue
        
        if chat_id not in chats:
            chats[chat_id] = ChatInfo(chat_id, chat_type)
        
        chats[chat_id].update_from_event(event)
    
    logger.log(f"Найдено уникальных чатов: {len(chats)}")
    
    return chats


def export_all_events_to_csv(events, filename, logger):
    """Экспортирует все события в CSV файл"""
    if not events:
        logger.log(f"⚠️  Нет событий для экспорта в {filename}")
        return
    
    logger.log(f"\nЭкспорт всех событий в: {filename}")
    
    flat_events = []
    for event in events:
        flat_event = flatten_dict(event)
        flat_events.append(flat_event)
    
    all_keys = set()
    for event in flat_events:
        all_keys.update(event.keys())
    
    priority_fields = [
        'occurred_at', 'type', 'user_login', 'user_name', 'uid', 
        'org_id', 'service', 'ip', 'status', 'is_system',
        'meta_chat_id', 'meta_chat_info_type', 'meta_chat_info_name'
    ]
    
    fieldnames = []
    for field in priority_fields:
        if field in all_keys:
            fieldnames.append(field)
            all_keys.remove(field)
    
    fieldnames.extend(sorted(all_keys))
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(
            csvfile, 
            fieldnames=fieldnames, 
            extrasaction='ignore',
            quoting=csv.QUOTE_MINIMAL,
            quotechar='"',
            escapechar='\\'
        )
        writer.writeheader()
        
        for event in flat_events:
            writer.writerow(event)
    
    logger.log(f"✓ Экспортировано: {len(events)} событий, {len(fieldnames)} колонок")


def export_chats_to_csv(chats, chat_type, filename, logger):
    """Экспортирует список чатов определенного типа в CSV"""
    filtered_chats = [c for c in chats.values() if c.chat_type == chat_type]
    
    if not filtered_chats:
        logger.log(f"⚠️  Нет чатов типа '{chat_type}' для экспорта в {filename}")
        # Создаем пустой файл с заголовками
        with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
            writer = csv.DictWriter(
                csvfile,
                fieldnames=['chat_id', 'name', 'type', 'last_activity', 'event_count'],
                quoting=csv.QUOTE_MINIMAL
            )
            writer.writeheader()
        return 0
    
    logger.log(f"\nЭкспорт чатов типа '{chat_type}' в: {filename}")
    
    # Сортируем по количеству событий (убывание)
    filtered_chats.sort(key=lambda x: x.event_count, reverse=True)
    
    fieldnames = ['chat_id', 'name', 'type', 'last_activity', 'event_count']
    
    with open(filename, 'w', newline='', encoding='utf-8-sig') as csvfile:
        writer = csv.DictWriter(
            csvfile,
            fieldnames=fieldnames,
            quoting=csv.QUOTE_MINIMAL,
            quotechar='"',
            escapechar='\\'
        )
        writer.writeheader()
        
        for chat in filtered_chats:
            writer.writerow(chat.to_dict())
    
    logger.log(f"✓ Экспортировано: {len(filtered_chats)} чатов")
    return len(filtered_chats)


def main():
    # Определяем даты
    end_date = parse_date_config(END_DATE)
    if end_date is None:
        end_date = datetime.now(timezone.utc)
    else:
        end_date = end_date.replace(hour=23, minute=59, second=59)
    
    start_date = parse_date_config(START_DATE)
    if start_date is None:
        start_date = (end_date - timedelta(days=DAYS))
    
    start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
    
    if start_date > end_date:
        print("❌ Ошибка: дата начала не может быть больше даты окончания")
        return
    
    # Создаем папку для результатов
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"messenger_audit_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    # Инициализируем логгер
    log_file = os.path.join(output_dir, "script_log.txt")
    logger = Logger(log_file)
    
    # Начало работы
    logger.log_separator()
    logger.log("ВЫГРУЗКА СОБЫТИЙ МЕССЕНДЖЕРА ИЗ АУДИТ-ЛОГОВ")
    logger.log_separator()
    logger.log(f"\nПериод выгрузки:")
    logger.log(f"  С: {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.log(f"  По: {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logger.log(f"  Дней: {(end_date - start_date).days}")
    logger.log(f"\nДиректория результатов: {output_dir}")
    logger.log(f"Настройки retry: {MAX_RETRIES} попыток, задержка {RETRY_DELAY}с")
    
    # Получаем все события мессенджера
    logger.log_separator()
    logger.log("ПОЛУЧЕНИЕ СОБЫТИЙ МЕССЕНДЖЕРА")
    logger.log_separator()
    logger.log(f"\nТипов событий: {len(MESSENGER_EVENT_TYPES)}")
    
    all_events = []
    success_count = 0
    failed_types = []
    
    for event_type in MESSENGER_EVENT_TYPES:
        events = get_audit_logs_by_type(event_type, start_date, end_date, logger)
        
        if events:
            all_events.extend(events)
            success_count += 1
        else:
            failed_types.append(event_type)
    
    logger.log(f"\nПолучено всего: {len(all_events)} событий")
    logger.log(f"Успешно: {success_count}/{len(MESSENGER_EVENT_TYPES)} типов")
    
    if failed_types:
        logger.log(f"Типы без данных:")
        for ft in failed_types:
            logger.log(f"  - {ft}")
    
    # Обрабатываем события и группируем по чатам
    logger.log_separator()
    logger.log("ОБРАБОТКА И ГРУППИРОВКА ПО ЧАТАМ")
    logger.log_separator()
    
    chats = process_events_to_chats(all_events, logger)
    
    # Статистика по типам чатов
    type_counts = defaultdict(int)
    for chat in chats.values():
        type_counts[chat.chat_type] += 1
    
    logger.log(f"\nРаспределение по типам:")
    for chat_type, count in sorted(type_counts.items()):
        logger.log(f"  {chat_type}: {count}")
    
    # Экспорт результатов
    logger.log_separator()
    logger.log("ЭКСПОРТ РЕЗУЛЬТАТОВ")
    logger.log_separator()
    
    # 1. Общий файл со всеми событиями
    all_events_file = os.path.join(output_dir, "all_events.csv")
    export_all_events_to_csv(all_events, all_events_file, logger)
    
    # 2. Файл с каналами (channel)
    channels_file = os.path.join(output_dir, "channels.csv")
    channel_count = export_chats_to_csv(chats, 'channel', channels_file, logger)
    
    # 3. Файл с открытыми чатами (group)
    groups_file = os.path.join(output_dir, "groups.csv")
    group_count = export_chats_to_csv(chats, 'group', groups_file, logger)
    
    # 4. Файл с закрытыми чатами (private)
    private_file = os.path.join(output_dir, "private_chats.csv")
    private_count = export_chats_to_csv(chats, 'private', private_file, logger)
    
    # Итоговая статистика
    logger.log_separator()
    logger.log("✓ ГОТОВО!")
    logger.log_separator()
    logger.log(f"\nИТОГОВАЯ СТАТИСТИКА:")
    logger.log(f"  Всего событий: {len(all_events)}")
    logger.log(f"  Уникальных чатов: {len(chats)}")
    logger.log(f"    - Каналов (channel): {channel_count}")
    logger.log(f"    - Открытых чатов (group): {group_count}")
    logger.log(f"    - Закрытых чатов (private): {private_count}")
    
    # Статистика по типам событий
    event_type_counts = defaultdict(int)
    for event in all_events:
        et = event.get("type", "unknown")
        event_type_counts[et] += 1
    
    logger.log(f"\nСтатистика по типам событий:")
    for et, count in sorted(event_type_counts.items(), key=lambda x: -x[1]):
        logger.log(f"  {et}: {count}")
    
    logger.log(f"\nФАЙЛЫ:")
    logger.log(f"  📁 {output_dir}/")
    logger.log(f"    📄 all_events.csv - все события ({len(all_events)})")
    logger.log(f"    📄 channels.csv - каналы ({channel_count})")
    logger.log(f"    📄 groups.csv - открытые чаты ({group_count})")
    logger.log(f"    📄 private_chats.csv - закрытые чаты ({private_count})")
    logger.log(f"    📄 script_log.txt - лог выполнения")
    
    execution_time = datetime.now() - logger.start_time
    logger.log(f"\nВремя выполнения: {execution_time}")


if __name__ == "__main__":
    main()
