# Скрипт предназначен для удаления файлов с Диска. Используются
# - сервисные приложения https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications
# - API Диска https://yandex.ru/dev/disk-api/doc/ru/reference/meta
# - SCIM-API для блокировки / разблокировки пользователя

import os
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timedelta


CLIENT_ID = '' # id сервисного приложения с правами cloud_api:disk.read, cloud_api:disk.write
CLIENT_SECRET = '' # secret сервисного приложения с правами cloud_api:disk.read, cloud_api:disk.write

DOMAINID = '' # ID домена из конфига утилиты YandexADSCIM
SCIM_TOKEN = '' # Токен из конфига утилиты YandexADSCIM

DISK_ID = '' # user_id (uid) Диска, который нужно очистить

PERMANENTLY = False # <True> для удаления мимо корзины. !!!Восстановлению не подлежит!!! Если False, нужно будет очистить корзину отдельной операцией


PATH = '/'
PAGE_LIMIT = 1000


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
    print(f'{datetime.now()} | get_token | status: {response.status_code}')
    logs.write(f"\n{datetime.now()} | get_token | status: {response.status_code} | for {disk_id}, url: {url}, headers: {headers}, data: {data} | response text: {response.text}, headers: {response.headers['Date']}")
    user_token = response.json()['access_token']
    ttl = int(response.json()['expires_in']) - 100
    expiry_time = datetime.now() + timedelta(seconds=ttl)
    logs.write(f"\n{datetime.now()} | get_token | user_token: {user_token}, expiry_time: {expiry_time}")
    return user_token, expiry_time


def scim_get_ban_status(user_id): # scim-api проверка блокировки. Возвращает статус True = разблокирован, False = заблокирован
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization' : f'Bearer {SCIM_TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    active = response.json()['active']
    logs.write(f"\n{datetime.now()} | scim_get_ban_status | status: {response.status_code} | url: {url}, headers: {headers}, text: {response.text}, headers: {response.headers['Date']} {response.headers['X-Request-Id']}")
    return active

def scim_enable_user(user_id): # scim-api разблокировка юзера
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization' : f'Bearer {SCIM_TOKEN}'}
    body = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "replace","path": "active","value": True}]}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.patch(url, json=body, headers=headers)
    logs.write(f"\n{datetime.now()} | scim_enable_user | status: {response.status_code} | url: {url}, headers: {headers}, body: {body}, text: {response.text}, headers: {response.headers['Date']} {response.headers['X-Request-Id']}")
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
    logs.write(f"\n{datetime.now()} | scim_disable_user | status: {response.status_code} | url: {url}, headers: {headers}, body: {body}, text: {response.text}, headers: {response.headers['Date']} {response.headers['X-Request-Id']}")
    return print(user_id, 'was disabled', response.status_code)


def disk_get_meta(token, limit, offset, path): # запрос получения метаинформации о ресурсе
    url = 'https://cloud-api.yandex.net/v1/disk/resources'
    headers = {'Authorization': f'OAuth {token}'}
    params = {
        'path': path,
        'fields': 'path,type,_embedded.total,_embedded.items.path,_embedded.items.type,_embedded.items.public_key,_embedded.items.name',
        'limit': limit,
        'offset': offset,
        'sort': 'path'
    }
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f'{datetime.now()} | disk_get_meta | status: {response.status_code} | path: {path}')
    logs.write(f"\n{datetime.now()} | disk_get_meta | status: {response.status_code} | params | path: {path}, limit: {limit}, offset: {offset} | headers: {response.headers['Date']}, {response.headers['Yandex-Cloud-Request-ID']}, text: {response.text}")
    return response.json()

def disk_delete_resource(token,path,permanently): # запрос удаления ресурса
    url = f'https://cloud-api.yandex.net/v1/disk/resources?path={path}&permanently={permanently}'
    headers = {'Authorization': f'OAuth {token}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.delete(url, headers=headers)
    logs.write(f"\n{datetime.now()} | delete_resource | status: {response.status_code} | params | path: {path} | headers: {response.headers['Date']}, {response.headers['Yandex-Cloud-Request-ID']}, text: {response.text}")
    return print(f'Deleting: {path}, Status: {response.status_code}')

def deletion(offset, path, disk_id): # обход папки для удаления ресурсов
    now = datetime.now()
    global token
    global expiry
    logs.write(f'\n{datetime.now()} | deletion | disk_id: {disk_id}, expiry: {token}, path {path}, offset {offset}')
    if now < expiry:
        logs.write(f"\n{datetime.now()} | now < expiry")
        response = disk_get_meta(token, PAGE_LIMIT, offset, path)
        emb = response.get('_embedded')
        if type(emb) == dict:
            items = emb.get('items')
            logs.write(f"\n{datetime.now()} | deletion | items total: {total}, items: {items}")
            for item in items:
                disk_delete_resource(token, item['path'], PERMANENTLY)
            total = emb.get('total')
            if total == PAGE_LIMIT:
                offset += PAGE_LIMIT
                logs.write(f"\n{datetime.now()} | deletion | {offset}, {path}")
                deletion(offset, path, disk_id)
            else:
                logs.write(f'\n{datetime.now()} | deletion | no more pages, offset: {offset}, total: {total}')
        else:
            print(f'No such resource: {DISK_ID} | {path}')
    else:
        logs.write(f"\n{datetime.now()} | token expired")
        get_new_token = get_token(disk_id)
        token = get_new_token[0]
        expiry = get_new_token[1]
        deletion(offset, path, disk_id)
    return print('Resources were cleared')


if __name__ == '__main__':

    dir = os.path.dirname(__file__)
    start = datetime.now()
    logs_path = os.path.abspath(f'{dir}/logs_{start}.log')
    logs = open(logs_path, 'a', encoding = 'utf-8')
    logs.write(f"start: {start}, Clearing Disk {DISK_ID}\n")

    ban_need = False
    is_active = scim_get_ban_status(DISK_ID)
    if is_active == False:
        scim_enable_user(DISK_ID)
        ban_need = True
    else:
        print(f"User {DISK_ID} is active")

    auth = get_token(DISK_ID)
    token = auth[0]
    expiry = auth[1]
    deletion(0,PATH,DISK_ID)

    if ban_need == True:
        scim_disable_user(DISK_ID)

    logs.write(f"\n{datetime.now()} | Completed\n")
    logs.close()
    print('Completed')