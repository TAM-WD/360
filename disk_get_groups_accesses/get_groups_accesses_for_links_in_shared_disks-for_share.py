### Скрипт предназначен для получения доступов к ссылкам в общих дисках

import os
import requests
from requests.adapters import HTTPAdapter, Retry
import csv
from datetime import datetime, timedelta

TOKEN_DISK = '' # токен OAuth приложения администратора организации, с правами cloud_api:disk.read
ORG_TOKEN = '' # токен OAuth приложения с доступом к чтению групп и сотрудников
ORGID = '' # ID организации
DOMAIN = '' # Домен организации

CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.read
CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.read

LIMIT_VD = 100
LIMIT = 1000

def api360_get_group_info(orgid, groupId): # получение инфы по группе
    url = f'https://api360.yandex.net/directory/v1/org/{orgid}/groups/{groupId}'
    headers = {'Authorization': f"OAuth {ORG_TOKEN}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    print(f'{datetime.now()} | api360_get_group_info | status: {response.status_code}')
    return response.json()

def disk_get_ods(offset): # получение общих дисков организации
    url = 'https://cloud-api.yandex.net/v1/disk/virtual-disks/manage/org-resources/list' 
    params = {
        'org_id': ORGID,
        'limit': LIMIT_VD,
        'offset': offset
    }
    headers = {'Authorization': f'OAuth {TOKEN_DISK}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f'{datetime.now()} | disk_get_ods | status: {response.status_code}')
    return response.json()

def get_token(disk): # получение токена для доступа к содержимому Диска
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{CLIENT_ID}',
        'client_secret': f'{CLIENT_SECRET}',
        'subject_token': f'{disk}',
        'subject_token_type': f'urn:yandex:params:oauth:token-type:email'
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

def disk_get_public_resources(token, offset): # получение публичных ресурсов
    url = "https://cloud-api.yandex.net/v1/disk/resources/public"
    params = {
        'limit': LIMIT,
        'offset': offset,
        'fields': 'limit,offset,items.path,items.public_key'
    }
    headers = {'Authorization': f"OAuth {token}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f"{datetime.now()} | get_public_resources | status: {response.status_code}")
    return response.json()

def disk_get_accesses_of_public_link(public_key):
    url = 'https://cloud-api.yandex.net/v1/disk/public/resources/admin/public-settings'
    params = {'public_key': public_key}
    headers = {'Authorization': f"OAuth {TOKEN_DISK}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        response = session.get(url, params=params, headers=headers)
        response.raise_for_status()
        print(f'{datetime.now()} | disk_get_accesses_of_public_link | status: {response.status_code}')
        return response.json()
    except requests.exceptions.HTTPError as http_err:
        if http_err.response.status_code == 500:
            return None

def get_vd_info(offset):
    response = disk_get_ods(offset)
    disks = response.get('items')
    for disk in disks:
        resource_id = disk.get('resource_id')
        disk_nickname = f'robot-resource-{resource_id}@{DOMAIN}'
        disk_ids.append(disk_nickname)

    total = response.get('total')
    if total == LIMIT_VD:
        offset += LIMIT_VD
        get_vd_info(offset)
    return print('Got VDs')

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
                public_key = item.get('public_key')
                public_settings = disk_get_accesses_of_public_link(public_key)
                if public_settings != None:
                    accesses = public_settings.get('accesses')
                    for access in accesses:
                        if access['type'] == 'group':
                            print(access)
                            group_id = access.get('id')
                            group_info = api360_get_group_info(ORGID, group_id)
                            group_name = group_info.get('name')
                            info = {
                                'user_id': token.split('.')[1],
                                'path': item.get('path'),
                                'type': item.get('type'),
                                'public_key': public_key,
                                'group_id': group_id,
                                'group_name': group_name
                            }
                            writer.writerow(info)
        else:
            print('total items = 0')
        if total == LIMIT:
            offset += LIMIT
            get_links(disk_id, offset)
        else:
            print('no more pages')
    else:
        get_new_token = get_token(disk_id)
        token = get_new_token[0]
        expiry = get_new_token[1]
        get_links(disk_id, offset)
    return print('done')


if __name__ == '__main__':
    start = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/links_in_shared_disks_{start}.csv')
    with open(file_path,'w', encoding='utf-8', newline='') as csv_file:
        field_names = [
            'user_id',
            'path',
            'type',
            'public_key',
            'group_id',
            'group_name'
        ]
        writer = csv.DictWriter(csv_file, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        disk_ids = []
        virtual_disks = get_vd_info(0)
        for disk in disk_ids:
            auth = get_token(disk)
            token = auth[0]
            expiry = auth[1]
            get_links(disk, 0)
    print('Complete')
