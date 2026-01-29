### Скрипт предназначен для получения публичных ссылок на Дисках организации, получения их настроек, и изменения видимости на "Только внутри организации" для всех ресурсов пользователей кроме персональных доступов. Используются:
# - получение списка юзеров организации https://yandex.ru/dev/api360/doc/ru/ref/UserService/UserService_List
# - сервисные приложения https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications
# - получение списка опубликованных ресурсов на Диске сотрудника https://yandex.ru/dev/disk-api/doc/ru/reference/recent-public
# - получение списка настроек для публичной ссылки https://yandex.ru/dev/disk-api/doc/ru/reference/public-settings-get-admin-pkey
# - изменение настроек публичного доступа к ресурсам https://yandex.ru/dev/disk-api/doc/ru/reference/public-settings-change

#Скрипт получает список всех сотрудников организации с постраничной пагинацией, для каждого сотрудника получает 
#временный токен с помощью сервисного приложения, с помощью него получает список всех публичных ссылок конкретного сотрудника, 
#заходит в каждую публичную ссылку и проверяет, выданы ли общие доступы (редактирование или просмотр) на всех, если да, смотрит, 
#включен ли флаг "Только внутри организации", если нет, смотрит, совпадает ли ID просматриваемого аккаунта с ID создателем ссылки, 
#после чего меняет доступ на "Только внутри организации", в других случаях скрипт выходит и пропускает итерацию.

#После завершения работы в папке, из который был запущен скрипт, создастся CSV файл, который содержит ID пользователя, логин, и публичную ссылку на файл, для которой была изменена видимость на "Только внутри организации".

import os
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timedelta
import csv
import time
import threading


ORGID = '' # ID организации
ORG_TOKEN = '' # токен OAuth приложения с правами directory:read_users, cloud_api:disk.read

CLIENT_ID = '' # id сервисного приложения с правами cloud_api:disk.read, cloud_api:disk.write, cloud_api:disk.app_folder, directory:read_users
CLIENT_SECRET = '' # secret сервисного приложения с правами cloud_api:disk.read, cloud_api:disk.write, cloud_api:disk.app_folder, directory:read_users


PERPAGE = 1000
PAGE_LIMIT = 1000
RPS_LIMIT = 20  # Ограничение запросов в секунду

# Класс для контроля RPS
class RateLimiter:
    def __init__(self, max_calls, period=1.0):
        self.max_calls = max_calls
        self.period = period
        self.calls = []
        self.lock = threading.Lock()
    
    def __call__(self):
        with self.lock:
            now = time.time()
            # Удаляем старые вызовы за пределами периода
            self.calls = [call for call in self.calls if call > now - self.period]
            
            if len(self.calls) >= self.max_calls:
                # Вычисляем время ожидания
                sleep_time = self.period - (now - self.calls[0])
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    now = time.time()
                    self.calls = [call for call in self.calls if call > now - self.period]
            
            self.calls.append(time.time())

# Создаем экземпляры rate limiter для каждого API endpoint
rate_limiter_public_resources = RateLimiter(RPS_LIMIT)
rate_limiter_public_settings = RateLimiter(RPS_LIMIT)
rate_limiter_update_settings = RateLimiter(RPS_LIMIT)


def get_token(disk_id): # получение токена для доступа к содержимому Диска
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{CLIENT_ID}',
        'client_secret': f'{CLIENT_SECRET}',
        'subject_token': f'{disk_id}',
        'subject_token_type': f'urn:yandex:params:oauth:token-type:uid'
    }
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.post(url, data = data, headers=headers)
    print(f'{datetime.now()} | get_token | status: {response.status_code}  без SCIM меняю видимость.py:80 - Смена уровня доступа ссылок на организацию.py:80')
    user_token = response.json()['access_token']
    ttl = int(response.json()['expires_in']) - 100
    expiry_time = datetime.now() + timedelta(seconds=ttl)
    return user_token, expiry_time

def get_users(page): # получение списка сотрудников
    url = f'https://api360.yandex.net/directory/v1/org/{ORGID}/users?page={page}&perPage={PERPAGE}'
    headers = {'Authorization' : f'OAuth {ORG_TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    request = session.get(url, headers=headers)
    response = request.json()
    return response['pages'], response['users']

def get_public_resources(token, offset): # получение публичных ресурсов
    rate_limiter_public_resources()  # Применяем rate limiting
    
    url = "https://cloud-api.yandex.net/v1/disk/resources/public"
    params = {
        'limit': PAGE_LIMIT,
        'offset': offset,
        'fields': 'limit,offset,items.name,items.path,items.type,items.created,items.modified,items.public_url,items.public_key'
    }
    headers = {'Authorization': f"OAuth {token}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        response = session.get(url, params=params, headers=headers)
        print(f"{datetime.now()} | get_public_resources | status: {response.status_code}  без SCIM меняю видимость.py:111 - Смена уровня доступа ссылок на организацию.py:111")
        return response.json()
    except requests.exceptions.RetryError as e:
        print(f"{datetime.now()} | get_public_resources | RetryError: {e}  без SCIM меняю видимость.py:114 - Смена уровня доступа ссылок на организацию.py:114")
        return {'items': []}  # Возвращаем пустой список
    except requests.exceptions.RequestException as e:
        print(f"{datetime.now()} | get_public_resources | RequestException: {e}  без SCIM меняю видимость.py:117 - Смена уровня доступа ссылок на организацию.py:117")
        return {'items': []}

def get_public_settings(public_url): # получение настроек публичного ресурса
    rate_limiter_public_settings()  # Применяем rate limiting
    
    url = "https://cloud-api.yandex.net/v1/disk/public/resources/admin/public-settings"
    params = {'public_key': public_url}
    headers = {'Authorization': f"OAuth {ORG_TOKEN}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        response = session.get(url, params=params, headers=headers)
        print(f"{datetime.now()} | get_public_settings | status: {response.status_code} | public_url: {public_url}  без SCIM меняю видимость.py:131 - Смена уровня доступа ссылок на организацию.py:131")
        if response.status_code == 200:
            return response.json()
        return None
    except requests.exceptions.RetryError as e:
        print(f"{datetime.now()} | get_public_settings | RetryError: {e} | public_url: {public_url}  без SCIM меняю видимость.py:136 - Смена уровня доступа ссылок на организацию.py:136")
        return None
    except requests.exceptions.RequestException as e:
        print(f"{datetime.now()} | get_public_settings | RequestException: {e} | public_url: {public_url}  без SCIM меняю видимость.py:139 - Смена уровня доступа ссылок на организацию.py:139")
        return None

def update_public_settings(token, path, rights): # обновление настроек публичного ресурса
    rate_limiter_update_settings()  # Применяем rate limiting
    
    url = "https://cloud-api.yandex.net/v1/disk/public/resources/public-settings"
    params = {
        'path': path,
        'allow_address_access': 'true'
    }
    headers = {'Authorization': f"OAuth {token}", 'Content-Type': 'application/json'}
    body = {
        "accesses": [
            {
                "type": "macro",
                "macros": ["employees"],
                "org_id": int(ORGID),
                "rights": rights
            }
        ]
    }
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        response = session.patch(url, params=params, json=body, headers=headers)
        print(f"{datetime.now()} | update_public_settings | status: {response.status_code} | path: {path}  без SCIM меняю видимость.py:166 - Смена уровня доступа ссылок на организацию.py:166")
        return response.status_code
    except requests.exceptions.RetryError as e:
        print(f"{datetime.now()} | update_public_settings | RetryError: {e} | path: {path}  без SCIM меняю видимость.py:169 - Смена уровня доступа ссылок на организацию.py:169")
        return 500  # Возвращаем код ошибки
    except requests.exceptions.RequestException as e:
        print(f"{datetime.now()} | update_public_settings | RequestException: {e} | path: {path}  без SCIM меняю видимость.py:172 - Смена уровня доступа ссылок на организацию.py:172")
        return 500

def process_public_resource(token, item, uid, email, writer): # обработка публичного ресурса
    public_url = item.get('public_url')
    path = item.get('path')
    
    if not public_url or not path:
        print(f"{datetime.now()} | process_public_resource | Skipping: no public_url or path  без SCIM меняю видимость.py:180 - Смена уровня доступа ссылок на организацию.py:180")
        return
    
    # Получаем настройки публичного ресурса
    settings = get_public_settings(public_url)
    if not settings or 'accesses' not in settings:
        print(f"{datetime.now()} | process_public_resource | Skipping: no settings or accesses  без SCIM меняю видимость.py:186 - Смена уровня доступа ссылок на организацию.py:186")
        return
    
    accesses = settings.get('accesses', [])
    
    # Ищем owner и проверяем его id
    owner_id = None
    for access in accesses:
        if access.get('type') == 'owner':
            owner_id = access.get('id')
            break
    
    if owner_id is None:
        print(f"{datetime.now()} | process_public_resource | Skipping: no owner found  без SCIM меняю видимость.py:199 - Смена уровня доступа ссылок на организацию.py:199")
        return
    
    if str(owner_id) != str(uid):
        print(f"{datetime.now()} | process_public_resource | Skipping: owner_id {owner_id} != uid {uid}  без SCIM меняю видимость.py:203 - Смена уровня доступа ссылок на организацию.py:203")
        return
    
    # Ищем macro с "all"
    macro_access = None
    macro_rights = None
    
    for access in accesses:
        if access.get('type') == 'macro':
            macros = access.get('macros', [])
            if 'employees' in macros:
                print(f"{datetime.now()} | process_public_resource | Skipping: already has 'employees' macro  без SCIM меняю видимость.py:214 - Смена уровня доступа ссылок на организацию.py:214")
                return
            elif 'all' in macros:
                macro_access = access
                macro_rights = access.get('rights', [])
                break
    
    if macro_access is None:
        print(f"{datetime.now()} | process_public_resource | Skipping: no macro with 'all' found  без SCIM меняю видимость.py:222 - Смена уровня доступа ссылок на организацию.py:222")
        return
    
    # Проверяем, что нет org_id
    if 'org_id' in macro_access:
        print(f"{datetime.now()} | process_public_resource | Skipping: macro already has org_id  без SCIM меняю видимость.py:227 - Смена уровня доступа ссылок на организацию.py:227")
        return
    
    # Обновляем настройки
    print(f"{datetime.now()} | process_public_resource | Updating settings for path: {path}  без SCIM меняю видимость.py:231 - Смена уровня доступа ссылок на организацию.py:231")
    status = update_public_settings(token, path, macro_rights)
    
    # Записываем в CSV только если обновление успешно
    if status == 200:
        info = {
            'uid': uid,
            'email': email,
            'public_url': public_url
        }
        writer.writerow(info)

def get_links(token, offset, uid, email, writer): # получаем публичные ресурсы на диске
    response = get_public_resources(token, offset)
    items = response.get('items')
    total = len(items)
    if total > 0:
        for item in items:
            # Обрабатываем публичный ресурс
            process_public_resource(token, item, uid, email, writer)
    else:
        print('total items = 0  без SCIM меняю видимость.py:252 - Смена уровня доступа ссылок на организацию.py:252')
    if total == PAGE_LIMIT:
        offset += PAGE_LIMIT
        get_links(token, offset, uid, email, writer)
    else:
        print('no more pages  без SCIM меняю видимость.py:257 - Смена уровня доступа ссылок на организацию.py:257')
    return print('done  без SCIM меняю видимость.py:258 - Смена уровня доступа ссылок на организацию.py:258')


if __name__ == '__main__':
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/changed_resources_{now}.csv')

    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        field_names = [
            'uid',
            'email',
            'public_url'
        ]
        writer = csv.DictWriter(csvfile, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        response_pages = get_users(1)[0]
        pages = set(range(1, response_pages + 1))
        for page in pages:
            users = get_users(page)[1]
            for user in users:
                uid = user['id']
                email = user['email']  # ← Достаем email
                if int(uid) > 1130000000000000 and user['isEnabled'] == True:
                    token = get_token(uid)[0]
                    get_links(token, 0, uid, email, writer)  # ← Передаем email
        
    print('Completed  без SCIM меняю видимость.py:285 - Смена уровня доступа ссылок на организацию.py:285')

