### Скрипт предназначен для выгрузки глобальных полей Трекера в .csv файл

import requests
from typing import List, Dict, Any
import csv
import os

ORGID = '' # ID организации
TOKEN = '' # OAuth токен с правами на чтение Трекера
ORG_TYPE = 'X-Org-Id' # или X-Cloud-Org-ID - тип организации (Яндекс 360 / Cloud Organization)

FILTER_KEY : str = '' # название поля, по которому нужен фильтр (например, 'name')
FILTER_VALUE : str = '' # значение поля, по которому нужен фильтр (например, 'скрытое_')


headers = {
        ORG_TYPE: ORGID,
        'Authorization': f"OAuth {TOKEN}"
    }

def get_global_fields():
    url = 'https://api.tracker.yandex.net/v3/fields'
    try:
        response = requests.get(url,headers=headers)
        return response.json()
    except Exception as e:
        print(f'Got error while getting global fields: {e}')

def filter_fields(fields: List[Dict[str, Any]], filter_key : str, filter_value : Any):
    filtered_fields : List[Dict[str, Any]] = []
    for field in fields:
        try:
            field_value = field.get(filter_key)
            if filter_value in str(field_value):
                filtered_fields.append(field)
        except Exception as e:
            print(f'Got exception while getting field_key: {e}')
    return filtered_fields

def put_to_csv(fields: List[Dict[str, Any]]):
    dir = os.path.dirname(__file__)
    output_file = os.path.abspath(f'{dir}/global_fields.csv')
    keys = [key for field in fields for key in field.keys()]
    with open(output_file, 'w', encoding='utf-8', newline='') as file:
        fieldnames = keys
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        writer.writerows(fields)
    return print(f'Created file: {output_file}')

if __name__ == '__main__':
    global_fields = get_global_fields()
    filtered_global_fields = filter_fields(global_fields, FILTER_KEY, FILTER_VALUE)
    put_to_csv(filtered_global_fields)