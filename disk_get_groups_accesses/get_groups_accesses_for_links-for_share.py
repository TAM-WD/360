### Скрипт предназначен для получения доступов групп организации к ссылкам пользователей

import os
import requests
from requests.adapters import HTTPAdapter, Retry
import csv
from datetime import datetime


TOKEN_DISK = '' # токен OAuth приложения администратора организации, с правами cloud_api:disk.read
ORG_TOKEN = '' # токен OAuth приложения с доступом к чтению групп и пользователей
ORGID = '' # ID организации

LIMIT = 1000

def api360_get_users(page): # получение списка сотрудников
    url = f'https://api360.yandex.net/directory/v1/org/{ORGID}/users'
    params = {
        'page': page,
        'perPage': LIMIT
    }
    headers = {'Authorization' : f'OAuth {ORG_TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    request = session.get(url, params=params, headers=headers)
    response = request.json()
    print(f'{datetime.now()} | api360_get_users | status: {request.status_code}')
    return response['users'], response['pages']

def api360_get_group_info(orgid, groupId): # получение инфы по группе
    url = f'https://api360.yandex.net/directory/v1/org/{orgid}/groups/{groupId}'
    headers = {'Authorization': f"OAuth {ORG_TOKEN}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    print(f'{datetime.now()} | api360_get_group_info | status: {response.status_code}')
    return response.json()

def disk_get_user_public_resources(org_id,user_id,offset):
    url = 'https://cloud-api.yandex.net/v1/disk/public/resources/admin/public-resources'
    params = {
        'user_id': user_id,
        'org_id': org_id,
        'limit': LIMIT,
        'offset': offset
    }
    headers = {'Authorization': f"OAuth {TOKEN_DISK}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f'{datetime.now()} | disk_get_user_public_resources | status: {response.status_code}')
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

def get_public_resources(org_id,user_id,offset):
    resources = disk_get_user_public_resources(org_id,user_id,offset)
    items = resources.get('items')
    for item in items:
        public_hash = item.get('public_hash')
        public_settings = disk_get_accesses_of_public_link(public_hash)
        if public_settings != None:
            accesses = public_settings.get('accesses')
            for access in accesses:
                if access['type'] == 'group':
                    print(access)
                    group_id = access.get('id')
                    group_info = api360_get_group_info(ORGID, group_id)
                    group_name = group_info.get('name')
                    info = {
                        'user_id': user_id,
                        'path': item.get('path'),
                        'type': item.get('type'),
                        'public_hash': public_hash,
                        'group_id': group_id,
                        'group_name': group_name
                    }
                    writer.writerow(info)
    total = len(items)
    if total == LIMIT:
        offset += LIMIT
        get_public_resources(org_id,user_id,offset)
    return print(f'Got public resources for {user_id}')

if __name__ == '__main__':
    start = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/groups_accesses_for_links_{start}.csv')
    with open(file_path,'w', encoding='utf-8', newline='') as csv_file:
        field_names = [
            'user_id',
            'path',
            'type',
            'public_hash',
            'group_id',
            'group_name'
        ]
        writer = csv.DictWriter(csv_file, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        page = 1
        total_pages = 2
        while page <= total_pages:
            response = api360_get_users(page)
            users = response[0]
            for user in users:
                user_id = user.get('id')
                if int(user_id) > 1130000000000000:
                    get_public_resources(ORGID, user_id, 0)
            total_pages = response[1]
            print(f'Got users for page {page}')
            page += 1
    print('Completed')
