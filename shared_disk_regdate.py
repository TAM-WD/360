import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime

CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.info
CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.info

DISK_ID = '' # uid Общего Диска.
# Можно посмотреть в https://admin.yandex.ru/virtual-disk, в панели разработчика браузера, ответе ручки https://admin.yandex.ru/api/models?_models=directory/get-virtual-disks
# Пример на скриншоте: https://disk.yandex.ru/i/ByE9d9czmVottg

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
    print(f'{datetime.now()} | get_token | user: {disk_id}, status: {response.status_code}')
    user_token = response.json()['access_token']
    return user_token

def disk_get_space_info(token): # получение информации о диске
    url = 'https://cloud-api.yandex.net/v1/disk/'
    headers = {'Authorization': f'OAuth {token}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    creation_time = response.json()['reg_time']
    return creation_time

if __name__ == '__main__':
    
    print(f"Get creation time for {DISK_ID}")
    token = get_token(DISK_ID)
    time = disk_get_space_info(token)
    print(f"\nDisk {DISK_ID} were created {time}\n")
    print('Complete')