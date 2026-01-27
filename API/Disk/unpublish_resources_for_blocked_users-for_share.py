### Скрипт предназначен для удаления публичных ссылок заблокированных пользователей

# ATTENTION удалённые ссылки восстановлению не подлежат, использование скрипта в полной ответственности администратора

import os
import requests
from requests.adapters import HTTPAdapter, Retry
import csv
from datetime import datetime, timedelta

TOKEN = '' # Токен с правами на чтение пользователей организации
ORGID = '' # ID организации
DOMAINID = '' # DomainID из конфига утилиты YandexADSCIM
SCIM_TOKEN = '' # Токен из конфига утилиты YandexADSCIM

CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.read
CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.read


LIMIT = 1000

def get_token(disk): # получение токена для доступа к содержимому Диска
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{CLIENT_ID}',
        'client_secret': f'{CLIENT_SECRET}',
        'subject_token': f'{disk}',
        'subject_token_type': f'urn:yandex:params:oauth:token-type:uid'
    }
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.post(url, data = data, headers=headers)
    print(f'{datetime.now()} | get_token | status: {response.status_code}')
    user_token = response.json()['access_token']
    ttl = int(response.json()['expires_in']) - 100
    expiry_time = datetime.now() + timedelta(seconds=ttl)
    return user_token, expiry_time

def scim_enable_user(user_id): # scim-api разблокировка юзера
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization' : f'Bearer {SCIM_TOKEN}'}
    body = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "replace","path": "active","value": True}]}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.patch(url, json=body, headers=headers)
    return print(user_id, 'was enabled', response.status_code)

def scim_disable_user(user_id): # scim-api блокировка юзера
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization' : f'Bearer {SCIM_TOKEN}'}
    body = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [{"op": "replace","path": "active","value": False}]}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.patch(url, json=body, headers=headers)
    return print(user_id, 'was disabled', response.status_code)

def api360_get_users(page): # получение списка сотрудников
    url = f'https://api360.yandex.net/directory/v1/org/{ORGID}/users'
    params = {
        'page': page,
        'perPage': LIMIT
    }
    headers = {'Authorization' : f'OAuth {TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    request = session.get(url, params=params, headers=headers)
    response = request.json()
    print(f'{datetime.now()} | api360_get_users | status: {request.status_code}')
    return response['users'], response['pages']

def disk_get_public_resources(token, offset): # получение публичных ресурсов
    url = "https://cloud-api.yandex.net/v1/disk/resources/public"
    params = {
        'limit': LIMIT,
        'offset': offset,
        'fields': 'limit,offset,items.path,items.public_key,items.public_url'
    }
    headers = {'Authorization': f"OAuth {token}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f"{datetime.now()} | get_public_resources | status: {response.status_code}")
    return response.json()

def disk_unpublish_resource(token, path): # распубликовать ресурс на диске
    #path = urllib.parse.quote(path)
    url = 'https://cloud-api.yandex.net/v1/disk/resources/unpublish'
    params = {'path': path}
    headers = {'Authorization': f"OAuth {token}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.put(url, params=params, headers=headers)
    print(f"{datetime.now()} | disk_unpublish_resource | status: {response.status_code} | path: {path}")
    return response.status_code

def get_links(disk_id, offset): # получаем публичные ресурсы на диске, записываем в файл
    global token
    global expiry
    now = datetime.now()
    if now < expiry:
        response = disk_get_public_resources(token, offset)
        items = response.get('items')
        total = len(items)
        if total > 0:
            for item in items:
                path = item.get('path')
                disk_unpublish_resource(token,path)
                info = {
                    'email': user_email,
                    'uid': disk_id,
                    'path': path,
                    'link': item.get('public_url')
                }
                writer.writerow(info)
        else:
            print('total items = 0')
        if total == LIMIT:
            offset += LIMIT
            get_links(disk_id, offset)
    else:
        get_new_token = get_token(disk_id)
        token = get_new_token[0]
        expiry = get_new_token[1]
        get_links(disk_id, offset)
    return print('done')


if __name__ == '__main__':
    start = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/unpublished_resources_{start}.csv')
    with open(file_path,'w', encoding='utf-8', newline='') as csv_file:
        field_names = [
            'email',
            'uid',
            'path',
            'link'
        ]
        writer = csv.DictWriter(csv_file, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        page = 1
        total_pages = 2
        while page <= total_pages:
            response = api360_get_users(page)
            users = response[0]
            for user in users:
                user_email = user.get('email')
                user_id = user.get('id')
                if int(user_id) > 1130000000000000:
                    if user['isEnabled'] == False:
                        scim_enable_user(user_id)
                        auth = get_token(user_id)
                        token = auth[0]
                        expiry = auth[1]
                        get_links(user_id, 0)
                        scim_disable_user(user_id)
            total_pages = response[1]
            print(f'Got users for page {page}')
            page += 1
    print('Complete')