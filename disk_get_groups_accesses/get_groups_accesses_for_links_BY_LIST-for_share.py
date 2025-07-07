import os
import requests
from requests.adapters import HTTPAdapter, Retry
import csv
from datetime import datetime, timedelta

CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.read
CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.read

DOMAINID = '' # DomainID из конфига утилиты YandexADSCIM
SCIM_TOKEN = '' # Токен из конфига утилиты YandexADSCIM

FILE_NAME = ''

LIMIT = 1000

def get_token(disk): # получение токена для доступа к содержимому Диска
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{CLIENT_ID}',
        'client_secret': f'{CLIENT_SECRET}',
        'subject_token': f'{disk}',
        'subject_token_type': f'urn:yandex:params:oauth:token-type:uid'
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

def scim_get_ban_status(user_id): # scim-api проверка блокировки. Возвращает статус True = разблокирован, False = заблокирован
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization' : f'Bearer {SCIM_TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    active = response.json()['active']
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
    return print(user_id, 'was disabled', response.status_code)

def disk_get_public_resources(token, offset): # получение публичных ресурсов
    url = "https://cloud-api.yandex.net/v1/disk/resources/public"
    params = {
        'limit': LIMIT,
        'offset': offset,
        'fields': 'limit,offset,items.path,items.public_key,items.public_url'
    }
    headers = {'Authorization': f"OAuth {token}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f"{datetime.now()} | get_public_resources | status: {response.status_code}")
    return response.json()

def disk_get_accesses_of_public_link(token,public_key):
    url = 'https://cloud-api.yandex.net/v1/disk/public/resources/admin/public-settings'
    params = {'public_key': public_key}
    headers = {'Authorization': f"OAuth {token}"}
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
                public_settings = disk_get_accesses_of_public_link(token,public_key)
                if public_settings != None:
                    accesses = public_settings.get('accesses')
                    for access in accesses:
                        if access['type'] == 'group':
                            print(access)
                            group_id = access.get('id')
                            info = {
                                'user_id': token.split('.')[1],
                                'path': item.get('path'),
                                'type': item.get('type'),
                                'public_key': public_key,
                                'group_id': group_id,
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

def read_users_from_csv(file_name):
    users_file_path = os.path.abspath(f'{dir}/{file_name}.csv')
    with open(users_file_path, 'r') as users_file:
        reader = csv.DictReader(users_file,delimiter=';')
        for row in reader:
            user_id = row['id']
            users.append(user_id)
    return print('Got users from file')

if __name__ == '__main__':
    start = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/groups_accesses_by_list_{start}.csv')
    with open(file_path,'w', encoding='utf-8', newline='') as csv_file:
        field_names = [
            'user_id',
            'path',
            'type',
            'public_key',
            'group_id'
        ]
        writer = csv.DictWriter(csv_file, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        users = []
        read_users_from_csv(FILE_NAME)
        for user in users:
            if int(user) > 1130000000000000:
                ban_needed = False
                is_active = scim_get_ban_status(user)
                if is_active == False:
                    scim_enable_user(user)
                    ban_needed = True
                auth = get_token(user)
                token = auth[0]
                expiry = auth[1]
                get_links(user, 0)
                if ban_needed == True:
                    scim_disable_user(user)
    print('Complete')