### Скрипт предназначен для получения публичных ссылок одного пользователя. Используются:
# - сервисные приложения https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications
# - API Диска https://yandex.ru/dev/disk-api/doc/ru/reference/meta

import os
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timedelta
import csv

CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.read
CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.read

USER_ID = ''

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
#    logs.write(f"\n{datetime.now()} | get_token | status: {response.status_code} | for {disk_id}, url: {url}, headers: {headers}, data: {data} | response text: {response.text}, headers: {response.headers['Date']}")
    user_token = response.json()['access_token']
    ttl = int(response.json()['expires_in']) - 100
    expiry_time = datetime.now() + timedelta(seconds=ttl)
#    logs.write(f"\n{datetime.now()} | get_token | user_token: {user_token}, expiry_time: {expiry_time}")
    return user_token, expiry_time

def get_public_resources(token, offset): # получение публичных ресурсов
    url = "https://cloud-api.yandex.net/v1/disk/resources/public"
    params = {
        'limit': PAGE_LIMIT,
        'offset': offset,
        'fields': 'limit,offset,items.name,items.path,items.type,items.created,items.modified,items.public_url,items.public_key'
    }
    headers = {'Authorization': f"OAuth {token}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f"{datetime.now()} | get_public_resources | status: {response.status_code}")
    return response.json()

def get_links(token, offset, uid): # получаем публичные ресурсы на диске, записываем в файл
    response = get_public_resources(token, offset)
    items = response.get('items')
    total = len(items)
    if total > 0:
        for item in items:
            info = {
                'uid': uid,
                'type': item.get('type'),
                'name': item.get('name'),
                'path': item.get('path'),
                'created': item.get('created'),
                'modified': item.get('modified'),
                'public_url': item.get('public_url'),
                'public_key': item.get('public_key'),
            }
            writer.writerow(info)
    else:
        print('total items = 0')
    if total == PAGE_LIMIT:
        offset += PAGE_LIMIT
        get_links(token, offset, uid)
    else:
        print('no more pages')
    return print('done')


if __name__ == '__main__':
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/shared_resources_{now}.csv')

    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        field_names = [
            'uid',
            'type',
            'name',
            'path',
            'created',
            'modified',
            'public_url',
            'public_key'
        ]
        writer = csv.DictWriter(csvfile, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        token = get_token(USER_ID)[0]
        get_links(token,0,USER_ID)

    print('Completed')