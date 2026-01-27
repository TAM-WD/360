### Скрипт предназначен для поиска дубликатов resource_id в Диске. Используются сервисные приложения и API Диска

import os
import csv
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timedelta
import pandas

CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.read
CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.read

DISK_ID = '' # user_id (uid) Диска

SEARCH_ONLY = False # True - если есть готовый csv-файл нужного формата
FILE_PATH = '' # заполнять, если SEARCH_ONLY = True

PAGE_LIMIT = 1000

def get_token(disk_id): # получаем токен для доступа к содержимому Диска
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{CLIENT_ID}',
        'client_secret': f'{CLIENT_SECRET}',
        'subject_token': f'{disk_id}',
        'subject_token_type': 'urn:yandex:params:oauth:token-type:uid'
    }
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.post(url, data = data, headers=headers)
    print(f'{datetime.now()} | get_token | status: {response.status_code}\n')
    user_token = response.json()['access_token']
    ttl = int(response.json()['expires_in']) - 100
    expiry_time = datetime.now() + timedelta(seconds=ttl)
    return user_token, expiry_time

def disk_get_resources(space, token, path, limit, offset):
    if space == 'disk':
        url = 'https://cloud-api.yandex.net/v1/disk/resources'
    elif space == 'trash':
        url = 'https://cloud-api.yandex.net/v1/disk/trash/resources'
    else:
        return print('no such space')
    params = {
        'path': path,
        'fields': 'path,resource_id,_embedded.total,_embedded.items.resource_id,_embedded.items.path,_embedded.items.public_key,_embedded.items.type', 
        'limit': limit,
        'offset': offset,
    }
    headers = {'Authorization': f'OAuth {token}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    return response.json()

def get_tree(space, disk_id, path, offset, writer):
    now = datetime.now()
    global token
    global expiry
    if now < expiry:
        response = disk_get_resources(space, token, path, PAGE_LIMIT, offset)
        emb = response.get('_embedded')
        if type(emb) == dict:
            total = emb.get('total')
            items = emb.get('items')
            for item in items:
                data = {
                    'resource_id': item.get('resource_id'),
                    'path': item.get('path'),
                    'type': item.get('type'),
                    'public_key': item.get('public_key')
                }
                writer.writerow(data)
                if item['type'] == 'dir':
                    new_path = item['path']
                    get_tree(space, disk_id, new_path, 0, writer)
            if total == PAGE_LIMIT:
                offset += PAGE_LIMIT
                get_tree(space, disk_id, path, offset, writer)
        else:
            print(f'Something went wrong: {disk_id} | {path}')
    else:
        get_new_token = get_token(disk_id)
        token = get_new_token[0]
        expiry = get_new_token[1]
        get_tree(space, disk_id, path, offset, writer)
    return print(f'Got resources for {path}')

def create_file(file_path):
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        get_tree('disk', DISK_ID, '/', 0, writer)
        get_tree('trash', DISK_ID, 'trash:/', 0, writer)
    return print('File was created')

def search_for_duplicates(file_path, column_name):
    df = pandas.read_csv(file_path, delimiter=';')
    duplicates = df[df.duplicated(subset=column_name, keep=False)][column_name]
    if not duplicates.empty:
        du = duplicates.unique()
        print(f'Duplicates: {du}')
        return write_duplicates(file_path, du)
    else:
        return print('No duplicates on the disk')

def write_duplicates(file_path, duplicates):
    duplicates_file_path = os.path.abspath(f'{dir}/duplicates_{start_time}.csv')
    with open(file_path, 'r', newline='', encoding='utf-8') as csvfile:
        with open(duplicates_file_path, 'w', newline='', encoding='utf-8') as duplicates_file:
            reader = csv.DictReader(csvfile, delimiter=';')
            writer = csv.DictWriter(duplicates_file, field_names, extrasaction='ignore', delimiter=';')
            writer.writeheader()
            for row in reader:
                for d in duplicates:
                    id = row['resource_id']
                    if id == d:
                        writer.writerow(row)
    return print('Duplicates done')


if __name__ == '__main__':
    start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/files_with_ids_{start_time}.csv')
    field_names = ['resource_id','path','type','public_key']
    if SEARCH_ONLY == False:
        auth = get_token(DISK_ID)
        token = auth[0]
        expiry = auth[1]
        create_file(file_path)
    elif SEARCH_ONLY == True:
        file_path = FILE_PATH
    duplicates = search_for_duplicates(file_path, 'resource_id')

    print('Completed')