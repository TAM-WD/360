#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import json
import csv
import os
import time
import re
from datetime import datetime, timezone, timedelta

# ============================================================
# КОНФИГУРАЦИЯ
# ============================================================

# OAuth токен и ID организации
OAUTH_TOKEN = ""  # Замените на ваш OAuth токен с правами ya360_security:read_auditlog
ORG_ID = ""  # ID организации

# Настройки периода выгрузки
# Если START_DATE = None, то используется END_DATE - DAYS дней
START_DATE = "2025-11-01"  # Формат: "2025-01-01" или None
END_DATE = "2025-11-30"    # Формат: "2025-12-31" или None (None = сегодня)
DAYS = 30          # Количество дней от END_DATE, если START_DATE = None

# Настройки retry
MAX_RETRIES = 5
RETRY_DELAY = 2
RETRY_BACKOFF = 2

# ============================================================


AUDIT_LOGS_URL = f"https://cloud-api.yandex.net/v1/auditlog/organizations/{ORG_ID}/events"
headers = {
    'Authorization': f'OAuth {OAUTH_TOKEN}'
}
EVENT_TYPES = {
    "authorization": [
        "id_cookie.set",
        "id_nondevice_token.issued",
        "id_device_token.issued",
        "id_app_password.login",
        "id_account.glogout",
        "id_account.changed_password"
    ],
    "messenger": [
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
}


def clean_value(value):
    if value is None:
        return ''
    
    if isinstance(value, str):
        value = value.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
        value = re.sub(r'\s+', ' ', value)
        value = value.strip()
    
    return value


def flatten_dict(d, parent_key='', sep='_'):
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
    if date_str is None:
        return None
    
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"❌ Ошибка: неверный формат даты '{date_str}'. Используйте YYYY-MM-DD")
        exit(1)


def make_request_with_retry(url, headers, params, max_retries=MAX_RETRIES):
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
                        print(f"\n    ⚠️  Ошибка {status_code}, попытка {attempt + 1}/{max_retries}. Ожидание {retry_delay}с...")
                        time.sleep(retry_delay)
                        retry_delay *= RETRY_BACKOFF
                        continue
                    else:
                        print(f"\n    ❌ Ошибка {status_code} после {max_retries} попыток")
                        raise
                else:
                    raise
            else:
                raise
                
        except (requests.exceptions.ConnectionError, 
                requests.exceptions.Timeout,
                ConnectionResetError) as e:
            if attempt < max_retries - 1:
                print(f"\n    ⚠️  Ошибка соединения, попытка {attempt + 1}/{max_retries}. Ожидание {retry_delay}с...")
                time.sleep(retry_delay)
                retry_delay *= RETRY_BACKOFF
                continue
            else:
                print(f"\n    ❌ Ошибка соединения после {max_retries} попыток")
                raise
        
        except Exception as e:
            print(f"\n    ❌ Неожиданная ошибка: {e}")
            raise
    
    raise Exception(f"Не удалось выполнить запрос после {max_retries} попыток")


def get_audit_logs_by_type(event_type, start_date, end_date, count=100):
    ended_at = end_date.isoformat().replace("+00:00", "Z")
    started_at = start_date.isoformat().replace("+00:00", "Z")
    
    params = {
        "count": count,
        "started_at": started_at,  # Добавляем started_at
        "ended_at": ended_at,
        "types": event_type
    }
    
    all_events = []
    page = 1
    
    print(f"  → {event_type}...", end=" ", flush=True)
    
    while True:
        try:
            response = make_request_with_retry(AUDIT_LOGS_URL, headers, params)
            
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
                print(f" {len(all_events)}...", end="", flush=True)
            
            iteration_key = data.get("iteration_key")
            if not iteration_key:
                break
            
            params['iteration_key'] = iteration_key
            page += 1
            
            time.sleep(0.1)
            
        except requests.exceptions.RequestException as e:
            print(f"\n    ❌ ОШИБКА: {e}")
            print(f"    Получено событий до ошибки: {len(all_events)}")
            break
            
        except Exception as e:
            print(f"\n    ❌ НЕОЖИДАННАЯ ОШИБКА: {e}")
            break
    
    print(f" ✓ {len(all_events)} событий")
    return all_events


def get_audit_logs_by_category(category, start_date, end_date):
    event_types = EVENT_TYPES.get(category)
    if not event_types:
        print(f"Неизвестная категория: {category}")
        return []
    
    print(f"\n{'='*60}")
    print(f"Категория: {category.upper()}")
    print(f"Типов событий: {len(event_types)}")
    print(f"Период: {start_date.strftime('%Y-%m-%d %H:%M:%S')} - {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")
    
    all_events = []
    success_count = 0
    failed_types = []
    
    for event_type in event_types:
        events = get_audit_logs_by_type(event_type, start_date, end_date)
        
        if events:
            all_events.extend(events)
            success_count += 1
        else:
            failed_types.append(event_type)
    
    print(f"\n{'='*60}")
    print(f"ИТОГО: {len(all_events)} событий")
    print(f"Успешно: {success_count}/{len(event_types)} типов")
    if failed_types:
        print(f"Не получены данные:")
        for ft in failed_types:
            print(f"  - {ft}")
    print(f"{'='*60}")
    
    return all_events


def export_to_csv(events, filename, category):
    if not events:
        print(f"⚠️  Нет событий для экспорта в {filename}")
        return
    
    print(f"\nЭкспорт в CSV: {filename}")
    
    flat_events = []
    for event in events:
        flat_event = flatten_dict(event)
        flat_events.append(flat_event)
    
    all_keys = set()
    for event in flat_events:
        all_keys.update(event.keys())
    
    priority_fields = [
        'occurred_at', 'type', 'user_login', 'user_name', 'uid', 
        'org_id', 'service', 'ip', 'status', 'is_system'
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
    
    print(f"✓ Экспортировано: {len(events)} событий")
    print(f"  Колонок: {len(fieldnames)}")


def main():
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
    
    print("="*60)
    print("Экспорт аудит-логов Yandex 360 в CSV")
    print("="*60)
    print(f"\nПериод выгрузки:")
    print(f"  С: {start_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  По: {end_date.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Дней: {(end_date - start_date).days}")
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = f"audit_logs_csv_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"\nДиректория: {output_dir}")
    print(f"Настройки retry: {MAX_RETRIES} попыток, задержка {RETRY_DELAY}с")
    
    print("\n" + "="*60)
    print("1. АВТОРИЗАЦИЯ")
    print("="*60)
    
    auth_logs = get_audit_logs_by_category("authorization", start_date, end_date)
    
    if auth_logs:
        auth_csv = os.path.join(output_dir, "authorization_all.csv")
        export_to_csv(auth_logs, auth_csv, "authorization")
        
        print(f"\nСтатистика по типам:")
        event_type_counts = {}
        for event in auth_logs:
            et = event.get("type", "unknown")
            event_type_counts[et] = event_type_counts.get(et, 0) + 1
        
        for et, count in sorted(event_type_counts.items()):
            print(f"  {et}: {count}")
    
    print("\n" + "="*60)
    print("2. МЕССЕНДЖЕР")
    print("="*60)
    
    messenger_logs = get_audit_logs_by_category("messenger", start_date, end_date)
    
    if messenger_logs:
        messenger_csv = os.path.join(output_dir, "messenger_all.csv")
        export_to_csv(messenger_logs, messenger_csv, "messenger")
        
        print(f"\nСтатистика по типам:")
        event_type_counts = {}
        for event in messenger_logs:
            et = event.get("type", "unknown")
            event_type_counts[et] = event_type_counts.get(et, 0) + 1
        
        for et, count in sorted(event_type_counts.items()):
            print(f"  {et}: {count}")
    
    total_events = len(auth_logs) + len(messenger_logs)
    
    print(f"\n{'='*60}")
    print("✓ ГОТОВО!")
    print(f"{'='*60}")
    print(f"\nИТОГОВАЯ СТАТИСТИКА:")
    print(f"  Авторизация: {len(auth_logs)} событий")
    print(f"  Мессенджер: {len(messenger_logs)} событий")
    print(f"  ВСЕГО: {total_events} событий")
    print(f"\nФАЙЛЫ:")
    print(f"  📁 {output_dir}/")
    print(f"    📄 authorization_all.csv")
    print(f"    📄 messenger_all.csv")


if __name__ == "__main__":
    main()
