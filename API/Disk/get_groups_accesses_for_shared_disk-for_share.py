### Скрипт предназначен для получения информации о группах, на которые назначен доступ к общим дискам

import os
import requests
from requests.adapters import HTTPAdapter, Retry
import csv
from datetime import datetime

TOKEN_DISK = '' # токен OAuth приложения администратора организации, с правами cloud_api:disk.read
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
    print(f'{datetime.now()} | disk_get_od_accesses for {vd_hash} | status: {response.status_code}')
    return response.json()

def api360_get_group_info(org_id, group_id): # получение инфы по группе
    url = f'https://cloud-api.yandex.net/v1/directory/organizations/{org_id}/groups/{group_id}'
    headers = {'Authorization': f"OAuth {TOKEN_GROUPS}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    print(f'{datetime.now()} | api360_get_group_info for {group_id} | status: {response.status_code}')
    return response.json()

def get_vd_info(offset):
    while True:
        response = disk_get_ods(offset)
        disks = response.get('items')
        for disk in disks:
            vd_hash = disk.get('vd_hash')
            vd_name = disk.get('name')
            try:
                accesses_response = disk_get_od_accesses(vd_hash)
            except requests.exceptions.RetryError as e:
                print(f'{datetime.now()} | SKIP {vd_hash} ({vd_name}): retries exhausted — {e}')
                writer.writerow({'vd_hash': vd_hash, 'vd_name': vd_name, 'groupId': 'ERROR', 'group_name': 'retries exhausted'})
                continue
            accesses = accesses_response.get('items')
            if accesses is None:
                error = accesses_response.get('error', 'unknown error')
                print(f'{datetime.now()} | SKIP {vd_hash} ({vd_name}): failed to get accesses — {error}')
                writer.writerow({'vd_hash': vd_hash, 'vd_name': vd_name, 'groupId': 'ERROR', 'group_name': 'ERROR'})
                continue
            for item in accesses:
                if item['type'] == 'group':
                    group_id = item.get('id')
                    try:
                        group_info = api360_get_group_info(ORGID, group_id)
                    except requests.exceptions.RetryError as e:
                        print(f'{datetime.now()} | SKIP group {group_id} in {vd_hash} ({vd_name}): retries exhausted — {e}')
                        writer.writerow({'vd_hash': vd_hash, 'vd_name': vd_name, 'groupId': group_id, 'group_name': 'ERROR'})
                        continue
                    group_name = group_info.get('name', 'unknown')
                    info = {
                        'vd_hash': vd_hash,
                        'vd_name': vd_name,
                        'groupId': group_id,
                        'group_name': group_name
                    }
                    writer.writerow(info)
        total = response.get('total')
        if total != PERPAGE:
            break
        offset += PERPAGE


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
