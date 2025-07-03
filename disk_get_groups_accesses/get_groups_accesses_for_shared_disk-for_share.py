### Скрипт предназначен для получения информации о группах, на которые назначен доступ к Общим Дискам

import os
import requests
from requests.adapters import HTTPAdapter, Retry
import csv
from datetime import datetime

TOKEN_DISK = '' # токен OAuth приложения администратора организации, с правами cloud_api:disk.read или cloud_api:disk.info
TOKEN_GROUPS = '' # токен OAuth приложения с доступом к чтению групп
ORGID = '' # ID организации

PERPAGE = 100

def disk_get_ods(offset): # получение общих дисков организации
    url = 'https://cloud-api.yandex.net/v1/disk/virtual-disks/manage/org-resources/list' 
    params = {
        'org_id': ORGID,
        'limit': PERPAGE,
        'offset': offset
    }
    headers = {'Authorization': f'OAuth {TOKEN_DISK}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f'{datetime.now()} | disk_get_ods | status: {response.status_code}')
    return response.json()

def disk_get_od_accesses(vd_hash): # получение доступов к ОД
    url = 'https://cloud-api.yandex.net/v1/disk/virtual-disks/permissions'
    params = {'vd_hash': vd_hash}
    headers = {'Authorization': f'OAuth {TOKEN_DISK}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f'{datetime.now()} | disk_get_od_accesses | status: {response.status_code}')
    return response.json()

def api360_get_group_info(orgid, groupId): # получение инфы по группе
    url = f'https://api360.yandex.net/directory/v1/org/{orgid}/groups/{groupId}'
    headers = {'Authorization': f"OAuth {TOKEN_GROUPS}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    print(f'{datetime.now()} | api360_get_group_info | status: {response.status_code}')
    return response.json()

def get_vd_info(offset):
    response = disk_get_ods(offset)
    disks = response.get('items')
    for disk in disks:
        vd_hash = disk.get('vd_hash')
        vd_name = disk.get('name')
        accesses = disk_get_od_accesses(vd_hash).get('items')
        for item in accesses:
            if item['type'] == 'group':
                group_id = item.get('id')
                group_info = api360_get_group_info(ORGID, group_id)
                group_name = group_info.get('name')
                info = {
                    'vd_hash': vd_hash,
                    'vd_name': vd_name,
                    'groupId': group_id,
                    'group_name': group_name
                }
                writer.writerow(info)
    total = response.get('total')
    if total == PERPAGE:
        offset += PERPAGE
        get_vd_info(offset)
    return


if __name__ == '__main__':
    start = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/shared_disk_accesses_{start}.csv')
    with open(file_path,'w', encoding='utf-8', newline='') as csv_file:
        field_names = [
            'vd_hash',
            'vd_name',
            'groupId',
            'group_name'
        ]
        writer = csv.DictWriter(csv_file, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        get_vd_info(0)
    print('Complete')