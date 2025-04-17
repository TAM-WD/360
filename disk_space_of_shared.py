# Скрипт предназначен для получения места на общих дисках. Используется API Общих Дисков: https://yandex.ru/dev/disk-api/doc/ru/reference/shared-disks/shd-info

import os
import requests
from requests.adapters import HTTPAdapter, Retry
import csv
from datetime import datetime

TOKEN = '' # токен OAuth приложения администратора организации, с правами cloud_api:disk.read или cloud_api:disk.info
ORGID = '' # ID организации

LIMIT = 100

def disk_get_ods(offset): # получение общих дисков организации
    url = 'https://cloud-api.yandex.net/v1/disk/virtual-disks/manage/org-resources/list'
    params = {
        'org_id': ORGID,
        'limit': LIMIT,
        'offset': offset
    }
    headers = {'Authorization': f'OAuth {TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f'{datetime.now()} | disk_get_ods | status: {response.status_code}')
    return response.json()

def disk_get_vd_space_info(vd_hash): # получение информации о месте на диске
    url = f'https://cloud-api.yandex.net/v1/disk/virtual-disks?vd_hash={vd_hash}'
    headers = {'Authorization': f'OAuth {TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    print(f'{datetime.now()} | disk_get_vd_space_info | {response.status_code}')
    used_space = round(int(response.json()['used_space'])/1024**3,4)
    total_space = round(int(response.json()['total_space'])/1024**3,4)
    free_space = (total_space - used_space)
    return used_space, free_space, total_space

def get_disks_info(offset):
    response = disk_get_ods(offset)
    items = response.get('items')
    for item in items:
        vd_hash = item.get('vd_hash')
        name = item.get('name')
        description = item.get('description')
        disk_space = disk_get_vd_space_info(vd_hash)
        info = {
            'vd_hash': vd_hash,
            'name': name,
            'description': description,
            'used_space': disk_space[0],
            'free_space': disk_space[1],
            'total_space': disk_space[2]
        }
        writer.writerow(info)
        print(f'Got info for disk {vd_hash}')
    total = response.get('total')
    print(f'Got disks info | total: {total}, offset: {offset}')
    if total == LIMIT:
        offset += LIMIT
        get_disks_info(offset)
    return print(f'Got disks info | offset: {offset}')

if __name__ == '__main__':
    now = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/shared_disk_space_{now}.csv')
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        field_names = [
            'vd_hash',
            'name',
            'description',
            'used_space',
            'free_space',
            'total_space'
        ]
        writer = csv.DictWriter(csvfile, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        get_disks_info(0)
    print('Completed')