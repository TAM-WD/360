### Скрипт предназначен для выгрузки всех пользователей в организации
# используется API Яндекс 360 для бизнеса

import os
import csv
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime

PERPAGE = 1000
TOKEN = '' # токен OAuth приложения администратора организации, с правами directory:read_users
ORGID = '' # ID организации

def api360_get_users(orgid, page):
    url = f'https://api360.yandex.net/directory/v1/org/{orgid}/users'
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
    return response['users'], response['pages']

if __name__ == '__main__':
    start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/users_orgid_{ORGID}_{start_time}.csv')
    with open(file_path, 'w', encoding='utf-8', newline='') as csv_file:
        field_names = [
            'id',
            'nickname',
            'email',
            'firstName',
            'lastName',
            'middleName',
            'position',
            'isEnabled',
            'isAdmin',
            'departmentId',
            'gender',
            'about',
            'birthday',
            'aliases',
            'groups',
            'externalId',
            'isRobot',
            'isDismissed',
            'timezone',
            'language',
            'createdAt',
            'updatedAt',
            'contacts',
        ]
        writer = csv.DictWriter(csv_file, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        page = 1
        total_pages = 2
        while page <= total_pages:
            response = api360_get_users(ORGID, page)
            users = response[0]
            for user in users:
                user.update({
                    'firstName': user.get('name').get('first'),
                    'lastName': user.get('name').get('last'),
                    'middleName': user.get('name').get('middle')
                })
                writer.writerow(user)
            total_pages = response[1]
            print(f'Got users for apge {page}')
            page += 1
    print('Complete')