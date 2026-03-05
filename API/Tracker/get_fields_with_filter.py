### Скрипт предназначен для выгрузки глобальных полей Трекера в .csv файл

import requests
from typing import List, Dict, Any
import csv
import os

ORGID = '' # ID организации
TOKEN = '' # OAuth токен с правами на чтение Трекера
ORG_TYPE = 'X-Org-Id' # или X-Cloud-Org-ID - тип организации (Яндекс 360 / Cloud Organization)

GET_GLOBAL_FIELDS = True # True для выгрузки глобальных полей, False для пропуска
GET_LOCAL_FIELDS = True # True для выгрузки локальных полей из всех очередей (должны быть доступы до очереди), False для пропуска

FILTER_KEY : str = '' # название поля, по которому нужен фильтр (например, 'name'). Поддерживаются только поля первого уровня вложенности
FILTER_VALUE : str = '' # значение поля, по которому нужен фильтр (например, 'test')


headers = {ORG_TYPE: ORGID, 'Authorization': f"OAuth {TOKEN}"}

def get_global_fields():
    url = 'https://api.tracker.yandex.net/v3/fields'
    try:
        response = requests.get(url,headers=headers)
        return response.json()
    except Exception as e:
        print(f'Got exception while getting global fields: {e}')

def filter_fields(
        fields: List[Dict[str, Any]],
        filter_key : str = FILTER_KEY,
        filter_value : Any = FILTER_VALUE
        ) -> List[Dict[str, Any]]:
    filtered_fields : List[Dict[str, Any]] = []
    for field in fields:
        try:
            field_value = field.get(filter_key)
            if filter_value in field_value:
                filtered_fields.append(field)
        except Exception as e:
            print(f'Got exception while getting field_key: {e}')
    return filtered_fields

def put_to_csv(filename : str, fields: List[Dict[str, Any]]):
    dir = os.path.dirname(__file__)
    output_file = os.path.abspath(f'{dir}/{filename}.csv')
    try:
        keys = [key for field in fields for key in field.keys()]
        with open(output_file, 'w', encoding='utf-8', newline='') as file:
            fieldnames = keys
            writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';')
            writer.writeheader()
            writer.writerows(fields)
            print(f'Created file: {output_file}')
    except Exception as e:
        print(f'Got exception while writing csv: {e}')

def get_queues(page: int, perPage : int):
    url = 'https://api.tracker.yandex.net/v3/queues/'
    params = {'perPage': perPage, 'page': page}
    try:
        response = requests.get(url, params=params, headers=headers)
        return response.json()
    except Exception as e:
        print(f'Got exception while getting queues: {e}')

def get_local_fields(queue : str):
    url = f'https://api.tracker.yandex.net/v3/queues/{queue}/localFields'
    try:
        response = requests.get(url, headers=headers)
        return response.status_code, response.json()

    except Exception as e:
        print(f'Got exception while getting global fields: {e}')

def get_all_local_fields():
    queues_list : List[str] = []
    queues_count, page, perPage = 100, 1, 100
    
    while queues_count == perPage:
        queues = get_queues(page, perPage)
        for queue in queues:
            queues_list.append(queue.get('key'))
        queues_count = len(queues)
        page += 1

    local_fields_list : List[Dict[str, Any]] = []
    for queue in queues_list:
        status, local_fields = get_local_fields(queue)
        if local_fields and status in [200, 201, 202, 204]:
            local_fields_list.extend(local_fields)
            print('local', queue, local_fields)
    
    return local_fields_list


if __name__ == '__main__':

    if GET_GLOBAL_FIELDS:
        global_fields = get_global_fields()
        global_fields = filter_fields(global_fields) if FILTER_KEY and FILTER_VALUE != '' else global_fields
        put_to_csv('global_fields', global_fields)
    else:
        print('Skipped global fields')
    
    if GET_LOCAL_FIELDS:
        local_fields = get_all_local_fields()
        local_fields = filter_fields(local_fields) if FILTER_KEY and FILTER_VALUE != '' else local_fields
        put_to_csv('local_fields', local_fields)
    else:
        print('Skipped local fields')