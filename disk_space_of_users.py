# Скрипт предназначен для выгрузки места на Дисках пользователей для организации, использующей SSO + SCIM
# В результате работы скрипта будет создан файл .csv, содержащий [email, uid, статус блокировки, занятое место, всего места]; место указано в ГБ

# Используются
# - сервисные приложения https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications
# - API Диска https://yandex.ru/dev/disk-api/doc/ru/reference/meta
# - SCIM-API для блокировки и разблокировки пользователя


import os
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime
import csv


ORGID = '' # ID организации
ORG_TOKEN = '' # токен OAuth приложения с правами directory:read_users

CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.info
CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.info

DOMAINID = '' # ID домена из конфига утилиты YandexADSCIM
SCIM_TOKEN = '' # Токен из конфига утилиты YandexADSCIM


PERPAGE = 1000

def get_token(uid): # получение токена для доступа к содержимому Диска
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{CLIENT_ID}',
        'client_secret': f'{CLIENT_SECRET}',
        'subject_token': f'{uid}',
        'subject_token_type': f'urn:yandex:params:oauth:token-type:uid'
    }
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.post(url, data = data, headers=headers)
    print(f'{datetime.now()} | get_token | uid: {uid}, status: {response.status_code}')
    user_token = response.json()['access_token']
    return user_token

def get_users(page): # получаем список сотрудников
    url = f'https://api360.yandex.net/directory/v1/org/{ORGID}/users?page={page}&perPage={PERPAGE}'
    headers = {'Authorization' : f'OAuth {ORG_TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    request = session.get(url, headers=headers)
    response = request.json()
    return response['pages'], response['users']

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


def disk_get_space_info(token): # получение информации о месте на диске
    url = 'https://cloud-api.yandex.net/v1/disk/'
    headers = {'Authorization': f'OAuth {token}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    used_space = round(int(response.json()['used_space'])/1024**3, 4)
    total_space = round(int(response.json()['total_space'])/1024**3, 4)
    return used_space, total_space


if __name__ == '__main__':
    now = datetime.now().strftime("%Y-%m-%d_%H:%M:%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/disk_space_{now}.csv')

    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        field_names = [
            'email',
            'uid',
            'isEnabled',
            'used_space(gb)',
            'total_space(gb)'
        ]
        writer = csv.DictWriter(csvfile, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        response_pages = get_users(1)[0]
        pages = set(range(1, response_pages + 1))
        for page in pages:
            users = get_users(page)[1]
            for user in users:
                ban_needed = False
                email = user['email']
                uid = user['id']
                if int(uid) > 1130000000000000:
                    if user['isEnabled'] == False:
                        ban_needed = True
                        scim_enable_user(uid)

                    token = get_token(uid)
                    space_info = disk_get_space_info(token)
                    used_space = space_info[0]
                    total_space = space_info[1]
                    info = {
                        'email': email,
                        'uid': uid,
                        'isEnabled': user['isEnabled'],
                        'used_space(gb)': used_space,
                        'total_space(gb)': total_space
                    }
                    writer.writerow(info)
                    print(f"Got info for {uid}")
                    if ban_needed == True:
                        scim_disable_user(uid)
                else:
                    print(f'Not domain user: {uid}')

    print('Completed')