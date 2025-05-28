### Скрипт предназначен для сравнения структуры файлов и папок (деревьев) между двумя Дисками. Используются сервисные приложения и API Диска.
import requests
from requests.adapters import HTTPAdapter, Retry
import os
from datetime import datetime, timedelta

CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.read
CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.read


DISK_ID_1 = '' # user_id (uid) первого Диска
DISK_ID_2 = '' # user_id (uid) второго Диска
PATH_1 = '/' # Путь к папке на первом Диске. Путь нужно указывать, начиная со слеша "/". Например, путь к ресурсам в корне Диска -- "/", путь к папке 'test' -- "/test"
PATH_2 = '/' # Путь к папке на втором Диске.

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
    logs.write(f"\n{datetime.now()} | Get token for {disk_id}")
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.post(url, data = data, headers=headers)
    print(f'{datetime.now()} | get_token | status: {response.status_code}\n')
    logs.write(f"\n{datetime.now()} | get_token | status: {response.status_code}, text: {response.text}, headers: {response.headers['Date']}")
    user_token = response.json()['access_token']
    ttl = int(response.json()['expires_in']) - 100
    expiry_time = datetime.now() + timedelta(seconds=ttl)
    logs.write(f"\n{datetime.now()} | user_token: {user_token}, expiry_time: {expiry_time}")
    return user_token, expiry_time

def disk_get_resources(token, path, limit, offset):
    url = 'https://cloud-api.yandex.net/v1/disk/resources'
    params = {
        'path': path,
        'fields': 'path,type,_embedded.total,_embedded.items.path,_embedded.items.type',
        'limit': limit,
        'offset': offset,
    }
    headers = {'Authorization': f'OAuth {token}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, params=params, headers=headers)
    print(f'{datetime.now()} | get_items | status: {response.status_code} | path: {path}')
    logs.write(f"\n{datetime.now()} | get_items | status: {response.status_code} | headers: {response.headers['Date']}, {response.headers['Yandex-Cloud-Request-ID']}, text: {response.text}")
    return response.json()

def get_tree(disk_id, file, token, expiry, path, offset): # рекурсивный обход дерева файлов
    now = datetime.now()
    if now < expiry:
        response = disk_get_resources(token, path, PAGE_LIMIT, offset)
        emb = response.get('_embedded')
        if type(emb) == dict:
            total = emb.get('total')
            items = emb.get('items')
            for item in items:
                file.write(f"{item.get('path')}\n")
                if item['type'] == 'dir':
                    new_path = item['path']
                    get_tree(disk_id, file, token, expiry, new_path, 0)
            if total == PAGE_LIMIT:
                offset += PAGE_LIMIT
                get_tree(disk_id, file, token, expiry, path, offset)
        else:
            print(f'Something went wrong: {disk_id} | {path}')
    else:
        get_new_token = get_token(disk_id)
        token = get_new_token[0]
        expiry = get_new_token[1]
        get_tree(disk_id, file, token, expiry, path, offset)
    return print(f'Got resources for {disk_id}, {path}')


def make_tree(file_path, disk_id, path):
    with open(file_path, 'w', encoding = 'utf-8', newline='') as file:
        auth = get_token(disk_id)
        token = auth[0]
        expiry = auth[1]
        get_tree(disk_id, file, token, expiry, path, 0)
    return print(f"Got tree for {disk_id}")

def read_tree(file_path,path):
    disk_path = 'disk:'
    folders = path.split('/')
    for i in set(range(0,len(folders))):
        folder = str(folders[i])
        if folder != '':
            folder_path = f'/{folder}'
            disk_path += folder_path
    lentgh = len(disk_path)
    print(f'path {disk_path}, len: {lentgh}')

    new_set = set()
    file = open(file_path,'r',encoding='utf-8')
    lines = set(file.read().split(f'\n'))
    for line in lines:
        line = line[lentgh:]
        new_set.add(f'{line}')
    
    file.close()
    return new_set, disk_path

def write_dif(set,path):
    for i in set:
        dif_file.write(f'{path}{i}\n')
    return

if __name__ == '__main__':

    dir = os.path.dirname(__file__)
    start = datetime.now()
    logs_path = os.path.abspath(f'{dir}/logs_{start}.log')
    logs = open(logs_path, 'a', encoding = 'utf-8')
    logs.write(f"start: {start}\nDISK_ID_1={DISK_ID_1}, DISK_ID_2={DISK_ID_2}, PATH_1={PATH_1}, PATH_2={PATH_2}")

    file_path_1 = os.path.abspath(f'{dir}/files_{DISK_ID_1}_1_{start}.txt')
    make_tree(file_path_1, DISK_ID_1, PATH_1)
    file_path_2 = os.path.abspath(f'{dir}/files_{DISK_ID_2}_2_{start}.txt')
    make_tree(file_path_2, DISK_ID_2, PATH_2)

    # сравнение деревьев
    read_tree_1 = read_tree(file_path_1,PATH_1)
    tree_1 = read_tree_1[0]
    tree_path_1 = read_tree_1[1]
    read_tree_2 = read_tree(file_path_2,PATH_2)
    tree_2 = read_tree_2[0]
    tree_path_2 = read_tree_2[1]
    dif_file_path = os.path.abspath(f'{dir}/difference_{DISK_ID_1}_{DISK_ID_2}_{start}.txt')
    dif_file = open(dif_file_path, 'w', encoding='utf-8')

    difference_1 = tree_1.difference(tree_2)
    if len(difference_1) == 0:
        one = True
    else:
        one = False
    difference_2 = tree_2.difference(tree_1)
    if len(difference_2) == 0:
        two = True
    else:
        two = False

    if (one == True) and (two == True):
        print('Identical trees')
        dif_file.write('Identical trees')
    elif (one == False) and (two == True):
        print('First tree has more resources')
        dif_file.write(f'First tree has more resources:\n\n')
        write_dif(difference_1,tree_path_1)
    elif (one == True) and (two == False):
        print('Second tree has more resources')
        dif_file.write(f'Second tree has more resources:\n\n')
        write_dif(difference_2,tree_path_2)
    else:
        print('Both trees have unique resources')
        dif_file.write(f'First tree\n\n')
        write_dif(difference_1,tree_path_1)
        dif_file.write(f'\n\nSecond tree\n\n')
        write_dif(difference_2,tree_path_2)

    logs.write(f"\n{datetime.now()} | Completed\n")
    logs.close()
    dif_file.close()
    print('Completed')