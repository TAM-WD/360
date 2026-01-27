# Скрипт предназначен для сохранения файлов с многих Дисков на другие в другой организации. Скрипт сработает, если логин без доменной части совпадает в 2 организациях. Используются
# - сервисные приложения https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications
# - API Диска https://yandex.ru/dev/disk-api/doc/ru/reference/meta
# - SCIM-API для блокировки / разблокировки пользователя

import os
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timedelta


SOURCE_CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.read, cloud_api:disk.write, cloud_api:disk.info
SOURCE_CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.read, cloud_api:disk.write, cloud_api:disk.info

DESTINATION_CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.read, cloud_api:disk.write, cloud_api:disk.info
DESTINATION_CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.read, cloud_api:disk.write, cloud_api:disk.info

SOURCE_DOMAIN = ''
DESTINATION_DOMAIN = ''

# список логинов без домена, через запятую
logins = [
    
    ]

#email будет формироваться как login = f'{login}@{domain}'

PATH = '/'
PAGE_LIMIT = 1000


def get_token(client_id, client_secret, disk_id): # получение токена для доступа к содержимому Диска
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{client_id}',
        'client_secret': f'{client_secret}',
        'subject_token': f'{disk_id}',
        'subject_token_type': f'urn:yandex:params:oauth:token-type:email' #email
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


def disk_get_space_info(token): # получение информации о месте на диске
    url = 'https://cloud-api.yandex.net/v1/disk/'
    headers = {'Authorization': f'OAuth {token}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    logs.write(f"\n{datetime.now()} | disk_get_space_info | status: {response.status_code} | url: {url}, headers: {headers}, | headers: {response.headers['Date']}, {response.headers['Yandex-Cloud-Request-ID']}, text: {response.text}")
    used_space = int(response.json()['used_space'])
    total_space = int(response.json()['total_space'])
    free_space = (total_space - used_space)
    return used_space, free_space

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

def disk_publish_resource(token, path): # публикация ресурса на диске
    url = f'https://cloud-api.yandex.net/v1/disk/resources/publish'
    params = {
        'path': path
    }
    headers = {'Authorization': f'OAuth {token}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.put(url,params=params,headers=headers)
    logs.write(f"\n{datetime.now()} | disk_publish_resource | status: {response.status_code} | url: {url}, headers: {headers}, | headers: {response.headers['Date']}, {response.headers['Yandex-Cloud-Request-ID']}, text: {response.text}")
    return print(f'{datetime.now()} | disk_publish_resource | status: {response.status_code} | path: {path}')

def disk_save_public_resource(public_key, name): # сохранение опубликованного ресурса на диск
    now = datetime.now()
    global destination_token
    global destination_expiry
    if now < destination_expiry:
        url = f'https://cloud-api.yandex.net/v1/disk/public/resources/save-to-disk'
        params = {
            'public_key': public_key
        }
        headers = {'Authorization': f'OAuth {destination_token}'}
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=retries))
        response = session.post(url,params=params,headers=headers)
        logs.write(f"\n{datetime.now()} | disk_save_public_resource | status: {response.status_code}, url: {url}, headers: {headers} | headers: {response.headers['Date']}, {response.headers['Yandex-Cloud-Request-ID']}, text: {response.text}")
    else:
        logs.write(f"\n{datetime.now()} | token expired")
        get_new_token = get_token(destination_login)
        destination_token = get_new_token[0]
        destination_expiry = get_new_token[1]
        disk_save_public_resource(public_key, name)
    
    return print(f'{datetime.now()} | disk_save_public_resource | status: {response.status_code} | path: {name}')


def make_links(offset, path, disk_id): # обход папки для создания ссылок
    now = datetime.now()
    global source_token
    global source_expiry
    logs.write(f'\n{datetime.now()} | make_links | disk_id: {disk_id}, expiry: {source_expiry}, path {path}, offset {offset}')
    if now < source_expiry:
        logs.write(f"\n{datetime.now()} | now < expiry")
        response = disk_get_meta(source_token, PAGE_LIMIT, offset, path)
        emb = response.get('_embedded')
        if type(emb) == dict:
            total = emb.get('total')
            pages = total // PAGE_LIMIT + 1
            page = offset // PAGE_LIMIT + 1
            items = emb.get('items')
            logs.write(f"\n{datetime.now()} | make_links | items total: {total}, pages: {pages}, items: {items}")
            for item in items:
                disk_publish_resource(source_token, item['path'])
            if page < pages:
                offset = page * PAGE_LIMIT
                print('OFFSET',offset,total,page)
                logs.write(f"\n{datetime.now()} | make_links | {offset}, {page}, {path}")
                make_links(offset, path, disk_id)
            else:
                logs.write(f'\n{datetime.now()} | make_links | no more pages, page: {page}, pages: {pages}, offset: {offset}, total: {total}')
        else:
            print(f'No such resource: {source_login} | {path}')
    else:
        logs.write(f"\n{datetime.now()} | token expired")
        get_new_token = get_token(disk_id)
        source_token = get_new_token[0]
        source_expiry = get_new_token[1]
        make_links(offset, path, disk_id)
    return print('Links were made')

def get_links(offset, path, disk_id): # обход папки для получения ссылок
    now = datetime.now()
    global source_token
    global source_expiry
    global links
    global fails
    logs.write(f'\n{datetime.now()} | get_links | disk_id: {disk_id}, expiry: {source_expiry}, path {path}, offset {offset}')
    if now < source_expiry:
        logs.write(f"\n{datetime.now()} | now < expiry")
        response = disk_get_meta(source_token, PAGE_LIMIT, offset, path)
        emb = response.get('_embedded')
        if type(emb) == dict:
            total = emb.get('total')
            pages = total // PAGE_LIMIT + 1
            page = offset // PAGE_LIMIT + 1
            items = emb.get('items')
            logs.write(f"\n{datetime.now()} | get_links | items total: {total}, pages: {pages}, page: {page}, items: {items}")
            for item in items:
                item_path = item.get('path')
                item_name = item.get('name')
                item_public_key = item.get('public_key')
                if type(item_public_key) == str:
                    links.append({'path': item_path,'name': item_name,'public_key':item_public_key})
                else:
                    fails.append({'path': item_path,'name': item_name})
            logs.write(f"\n\ {links}, FAILS {fails}")
            if page < pages:
                offset = page * PAGE_LIMIT
                print('OFFSET',offset,total,page)
                logs.write(f"\n{datetime.now()} | get_links | {offset}, {page}, {path}")
                get_links(offset, path, disk_id)
            else:
                print(f'no more pages, {page}, {pages}, {offset}, {total}')
                logs.write(f'\n\n{datetime.now()} | make_links | no more pages | page: {page}, pages: {pages}, offset: {offset}, total: {total}')
        else:
            print(f'No such resource: {source_login} | {path}')
    else:
        logs.write(f"\n{datetime.now()} | token expired")
        get_new_token = get_token(disk_id)
        source_token = get_new_token[0]
        source_expiry = get_new_token[1]
        logs.write(f"\n{datetime.now()} | token:{source_token}, expiry: {source_expiry}")
        get_links(offset, path, disk_id)
    return print('Get links done')

def save_links(): # сохранение ссылок на другой Диск
    global links
    logs.write(f'\n{datetime.now()} | save_links | path {PATH}, links: {links}')
    for link in links:
        disk_save_public_resource(link['public_key'], link['name'])
    return print('Links were saved')


if __name__ == '__main__':

    dir = os.path.dirname(__file__)
    start = datetime.now()
    logs_path = os.path.abspath(f'{dir}/logs_{start}.log')
    logs = open(logs_path, 'a', encoding = 'utf-8')
    logs.write(f"start: {start}\n")

    links = []
    fails = []

    for login in logins:
        source_login = f'{login}@{SOURCE_DOMAIN}'
        destination_login = f'{login}@{DESTINATION_DOMAIN}'
        source_auth = get_token(SOURCE_CLIENT_ID, SOURCE_CLIENT_SECRET, source_login)
        source_token = source_auth[0]
        source_expiry = source_auth[1]
        destination_auth = get_token(DESTINATION_CLIENT_ID, DESTINATION_CLIENT_SECRET, destination_login)
        destination_token = destination_auth[0]
        destination_expiry = destination_auth[1]
        needed_space = disk_get_space_info(source_token)[0]
        free_space = disk_get_space_info(destination_token)[1]
        print(f'Need space: {needed_space}, Free space: {free_space}')
        logs.write(f"\n{datetime.now()} | Need space: {needed_space}, Free space: {free_space}")
        if free_space > needed_space:
            make_links(0, PATH, source_login)
            get_links(0, PATH, source_login)
            save_links()
        else:
            print(f'Not enough space on destination disk {destination_login}')

    logs.write(f"\n{datetime.now()} | Successfully saved: {links}.")
    logs.write(f"\n{datetime.now()} | Could not save links: {fails}.")
    logs.write(f"\n{datetime.now()} | Completed\n")
    logs.close()
    print('Completed')