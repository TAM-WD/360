### Скрипт предназначен для выгрузки всех групп в организации
# Используется API Яндекс 360 для бизнеса https://yandex.ru/dev/api360/doc/ru/ref/GroupService/GroupService_List

import os
import csv
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime

TIMEOUT = 1
PERPAGE = 100
TOKEN = '' # токен OAuth приложения администратора организации, с правами directory:read_groups
ORGID = '' # ID организации

def api360_get_groups(orgid, page):
    url = f'https://api360.yandex.net/directory/v1/org/{orgid}/groups'
    params = {
        'page': page,
        'perPage': PERPAGE
    }
    headers = {'Authorization': f"OAuth {TOKEN}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    request = session.get(url, params=params, headers=headers)
    response = request.json()
    return response['groups'], response['pages']

if __name__ == '__main__':
    start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/groups_orgid_{ORGID}_{start_time}.csv')
    with open(file_path, 'w', encoding='utf-8', newline='') as csv_file:
        field_names = [
            'id',
            'name',
            'type',
            'description',
            'membersCount',
            'label',
            'email',
            'emailId',
            'aliases',
            'externalId',
            'removed',
            'members',
            'memberOf',
            'createdAt'
        ]
        writer = csv.DictWriter(csv_file, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        page = 1
        total_pages = 2
        while page <= total_pages:
            response = api360_get_groups(ORGID, page)
            groups = response[0]
            for group in groups:
                writer.writerow(group)
            total_pages = response[1]
            print(f'Got users for pаge {page}')
            page += 1
    print('Complete')