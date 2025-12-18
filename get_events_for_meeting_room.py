"""
Скрипт предназначен для получения событий у всех пользователей организации, в которых забронированы переговорки. Используются:
- получение списка юзеров организации https://yandex.ru/dev/api360/doc/ru/ref/UserService/UserService_List
- сервисные приложения https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications
- Используются методы CalDAV https://caldav.yandex.ru/calendars/{user_email}/events-default и https://caldav.yandex.ru/calendars/{user_email}/events-default/{event_id}

Для работы скрипта понадобится установить библиотеку, не входящую в стандартную поставку Python, сделать это можно с помощью pip, выполнив следующую команду в терминале:
pip install requests chardet

Скрипт реализует ограничения на количество запросов:
Получение временного токена пользователя: не более 1000 запросов в час;
События Календаря: не более 200 запросов в секунду.

Скрипт использует пул потоков для параллельной обработки пользователей. Максимальное количество одновременно обрабатываемых пользователей = 10

Перед запуском скрипта необходимо заполнить константы:
ORGID = 'ID вашей организации'
ORG_TOKEN = 'токен OAuth приложения с правами directory:read_users для просмотра списка сотрудников https://yandex.ru/dev/api360/doc/ru/ref/UserService/UserService_List'
CLIENT_ID = 'Client ID приложения, которое добавлено в список сервисных по организации и для которого выданы соответствующие права на внесение изменений в Календари сотрудников организации: calendar:all по инструкции https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications'
CLIENT_SECRET = 'Client secret для того же приложения, что и Client ID'
"""

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from datetime import datetime, timedelta
import csv
import re
import concurrent.futures
import logging
from functools import lru_cache
import chardet
import time
import threading
import queue
from collections import deque

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Константы
ORGID = ''
ORG_TOKEN = ''
CLIENT_ID = ''
CLIENT_SECRET = ''
PERPAGE = 1000
TOKEN_CACHE = {}  # Кеш для токенов, чтобы не запрашивать их повторно

# Создаем сессию для повторного использования
SESSION = requests.Session()
retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
SESSION.mount('https://', HTTPAdapter(max_retries=retries))

# Ограничение для токенов: 1000 запросов в час
TOKEN_LIMIT_PER_HOUR = 1000
token_lock = threading.RLock()
token_request_times = deque(maxlen=TOKEN_LIMIT_PER_HOUR)  # Хранит временные метки последних 1000 запросов токенов

# Ограничение для событий календаря: 200 RPS
event_lock = threading.RLock()
event_last_request = 0
event_min_interval = 1.0 / 200  # 200 RPS

def token_rate_limiter():
    """Ограничение скорости запросов для токенов до 1000 в час"""
    with token_lock:
        current_time = time.time()
        
        # Удаляем устаревшие записи (старше 1 часа)
        hour_ago = current_time - 3600  # 3600 секунд = 1 час
        
        # Если очередь не пуста и самая старая запись старше 1 часа, удаляем её
        while token_request_times and token_request_times[0] < hour_ago:
            token_request_times.popleft()
        
        # Проверяем, не превышен ли лимит запросов
        if len(token_request_times) >= TOKEN_LIMIT_PER_HOUR:
            # Вычисляем, сколько нужно подождать
            oldest_request = token_request_times[0]
            wait_time = hour_ago - oldest_request + 0.1  # + небольшой запас
            logger.warning(f"Token rate limit reached. Waiting {wait_time:.2f} seconds.")
            time.sleep(max(0, wait_time))
            
            # После ожидания, рекурсивно вызываем функцию для новой проверки
            return token_rate_limiter()
        
        # Добавляем текущее время в очередь
        token_request_times.append(current_time)

def event_rate_limiter():
    """Ограничение скорости запросов для событий до 200 RPS"""
    global event_last_request
    with event_lock:
        now = time.time()
        sleep_time = event_min_interval - (now - event_last_request)
        if sleep_time > 0:
            time.sleep(sleep_time)
        event_last_request = time.time()

@lru_cache(maxsize=128)
def get_token(uid):
    """Получение токена для доступа к Календарям пользователей с кешированием."""
    # Проверяем кеш
    if uid in TOKEN_CACHE and TOKEN_CACHE[uid]['expiry'] > datetime.now():
        return TOKEN_CACHE[uid]['token']
    
    # Применяем ограничение скорости для токенов (1000 в час)
    token_rate_limiter()
    
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'subject_token': uid,
        'subject_token_type': 'urn:yandex:params:oauth:token-type:uid'
    }
    
    response = SESSION.post(url, data=data, headers=headers)
    response.raise_for_status()
    
    response_json = response.json()
    user_token = response_json['access_token']
    ttl = int(response_json['expires_in']) - 100
    expiry_time = datetime.now() + timedelta(seconds=ttl)
    
    # Сохраняем в кеш
    TOKEN_CACHE[uid] = {'token': user_token, 'expiry': expiry_time}
    
    logger.info(f'Token obtained for UID {uid}, valid until {expiry_time}')
    return user_token

def get_users(page):
    """Получение списка сотрудников."""
    url = f'https://api360.yandex.net/directory/v1/org/{ORGID}/users'
    params = {'page': page, 'perPage': PERPAGE}
    headers = {'Authorization': f'OAuth {ORG_TOKEN}'}
    
    response = SESSION.get(url, params=params, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    
    return response_json.get('pages', 1), response_json.get('users', [])

def get_calendar_events(user_email, token):
    """Получение списка событий календаря."""
    try:
        # Применяем ограничение скорости для событий календаря (200 RPS)
        event_rate_limiter()
        
        url = f'https://caldav.yandex.ru/calendars/{user_email}/events-default'
        headers = {'Authorization': f'OAuth {token}'}
        
        response = SESSION.get(url, headers=headers)
        response.raise_for_status()
        
        return [line for line in response.text.split('\n') if line.strip()]
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logger.warning(f"Authorization failed for user {user_email}: 401 Unauthorized")
            return []
        raise

def extract_event_id(event_url):
    """Извлечение ID события из URL."""
    match = re.search(r'[^/]+\.ics$', event_url)
    return match.group(0) if match else None

def get_event_details(user_email, event_id, token):
    """Получение детальной информации о событии."""
    try:
        # Применяем ограничение скорости для событий календаря (200 RPS)
        event_rate_limiter()
        
        url = f'https://caldav.yandex.ru/calendars/{user_email}/events-default/{event_id}'
        headers = {'Authorization': f'OAuth {token}'}
        response = SESSION.get(url, headers=headers)
        response.raise_for_status()
        
        # Определяем кодировку с помощью chardet
        content = response.content  # Получаем контент в виде байтов
        detected = chardet.detect(content)
        detected_encoding = detected['encoding']
        confidence = detected['confidence']
        logger.debug(f"Detected encoding: {detected_encoding} with confidence: {confidence}")
        
        # Декодируем контент с использованием обнаруженной кодировки
        if detected_encoding and confidence > 0.7:
            event_details = content.decode(detected_encoding)
        else:
            event_details = response.text  # Используем декодирование по умолчанию
            
        return event_details
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 401:
            logger.warning(f"Authorization failed for user {user_email}, event {event_id}: 401 Unauthorized")
            return ""
        else:
            raise

def parse_event_details(event_data):
    """Парсинг данных события."""
    event = {}
    for line in event_data.split('\n'):
        if ':' in line:
            key, value = line.split(':', 1)
            event[key.strip()] = value.strip()
    return event

def find_meeting_rooms(event):
    """Поиск переговорных комнат в событии."""
    meeting_rooms = []
    for key, value in event.items():
        if 'ATTENDEE;CUTYPE=ROOM' in key and 'mailto:' in value:
            room_email = value.replace('mailto:', '').strip()
            meeting_rooms.append(room_email)
    return meeting_rooms

def extract_organizer_email(event_dict):
    """Извлечение email организатора."""
    for key, value in event_dict.items():
        if key.startswith('ORGANIZER'):
            return value.replace('mailto:', '').strip() if 'mailto:' in value else value.strip()
    return ""

def process_user(user):
    """Обработка одного пользователя."""
    email = user.get('email', '')
    uid = user.get('id', '0')
    is_enabled = user.get('isEnabled', False)
    
    if not email or not uid or int(uid) <= 1130000000000000 or not is_enabled:
        return []
    
    results = []
    try:
        token = get_token(uid)
        event_urls = get_calendar_events(email, token)
        
        for event_url in event_urls:
            event_id = extract_event_id(event_url)
            if not event_id:
                continue
            
            event_details = get_event_details(email, event_id, token)
            if not event_details:
                continue
            
            event = parse_event_details(event_details)
            meeting_rooms = find_meeting_rooms(event)
            
            if meeting_rooms:
                organizer_email = extract_organizer_email(event)
                results.append({
                    'event_id': event_id,
                    'url': event.get('URL', ''),
                    'organizer': organizer_email,
                    'event_name': event.get('SUMMARY', ''),
                    'meeting_rooms': ', '.join(meeting_rooms)
                })
    except Exception as e:
        logger.error(f"Error processing user {email}: {str(e)}")
    
    return results

def main():
    """Основная функция скрипта."""
    start_time = time.time()
    
    # Получение общего числа страниц
    total_pages, _ = get_users(1)
    pages = range(1, total_pages + 1)
    
    all_results = []
    
    # Обработка пользователей
    for page in pages:
        _, users = get_users(page)
        
        # Параллельная обработка пользователей
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            for user in users:
                futures.append(executor.submit(process_user, user))
            
            # Собираем результаты по мере завершения
            for future in concurrent.futures.as_completed(futures):
                try:
                    result = future.result()
                    if result:
                        all_results.extend(result)
                except Exception as exc:
                    logger.error(f"Future raised an exception: {exc}")
    
    # Запись результатов в CSV
    with open('meeting_rooms.csv', 'w', encoding='utf-8-sig', newline='') as csvfile:
        fieldnames = ['event_id', 'url', 'organizer', 'event_name', 'meeting_rooms']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        
        for result in all_results:
            writer.writerow(result)
    
    end_time = time.time()
    logger.info(f"Processing completed in {end_time - start_time:.2f} seconds. Found {len(all_results)} events with meeting rooms.")

if __name__ == '__main__':
    # Закрываем все ресурсы при завершении программы
    try:
        main()
    finally:
        # Явно очищаем все ресурсы
        SESSION.close()
