# Скрипт предназначен для получения Общих Дисков, доступных списку пользователей. Используются
# - сервисные приложения https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications
# - API Диска https://yandex.ru/dev/disk-api/doc/ru/reference/meta

import os
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timedelta
import csv

CLIENT_ID = '' # id сервисного приложения с правами cloud_api:disk.info или cloud_api:disk.read
CLIENT_SECRET = '' # secret сервисного приложения с правами cloud_api:disk.info или cloud_api:disk.read

ORGID = '' # ID организации

LIMIT = 100

# список email пользователей, для которых нужно получить информацию
logins = []

def get_token(email): # запрос для получения токена для доступа к содержимому Диска
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{CLIENT_ID}',
        'client_secret': f'{CLIENT_SECRET}',
        'subject_token': f'{email}',
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

def disk_get_ods(token, offset): # запрос для получения общих дисков, к которым у пользователя есть доступ
    url = 'https://cloud-api.yandex.net/v1/disk/virtual-disks/discovery'
    params = {
        'org_id': ORGID,
        'limit': LIMIT,
        'offset': offset
    }
    headers = {'Authorization': f'OAuth {token}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f'{datetime.now()} | disk_get_ods | status: {response.status_code}')
    return response.json()

def get_ods(login, token, offset): # получение и запись общих дисков пользователя
    response = disk_get_ods(token, offset)
    items = response.get('items')
    for item in items:
        name = item.get('name')
        vd_hash = item.get('vd_hash')
        info = {
            'login': login,
            'name': name,
            'vd_hash': vd_hash
        }
        writer.writerow(info)
    total = response.get('total')
    if total == LIMIT:
        offset += LIMIT
        get_ods(login, token, offset)
    return print(f'Got disks for {login}, offset: {offset}')

if __name__ == '__main__':
    dir = os.path.dirname(__file__)
    start = datetime.now()

    file_path = os.path.abspath(f'{dir}/shared_disks_{start}.csv')
    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        field_names = [
            'login',
            'name',
            'vd_hash'
        ]
        writer = csv.DictWriter(csvfile, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        for login in logins:
            token = get_token(login)[0]
            get_ods(login, token, 0)
            print(f'Got info for {login}')
    
    print('Completed')