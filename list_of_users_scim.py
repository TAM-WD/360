### Скрипт предназначен для получения пользователей организации с помощью scim-api.

import csv
import os
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime

DOMAINID = '' # ID домена из конфига YandexADSCIM
SCIM_TOKEN = '' # токен из конфига YandexADSCIM

PAGE_SIZE = 100

def scim_get_users(page_size, startIndex):
    url = f"https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users"
    params = {
        'count': page_size,
        'startIndex': startIndex
    }
    headers = {'Authorization' : f'Bearer {SCIM_TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    request = session.get(url, params=params, headers=headers)
    response = request.json()
    users = response.get('Resources')
    total = response.get('totalResults')
    return users, total


if __name__ == '__main__':
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/users_from_scim_{now}.csv')

    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        field_names = ['userName','uid','email']
        writer = csv.DictWriter(csvfile, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        total = scim_get_users(1,1)[1]
        startIndex = 1
        while startIndex <= total:
            users = scim_get_users(PAGE_SIZE, startIndex)[0]
            for user in users:
                userName = user.get('userName')
                uid = user.get('id')
                emails = user.get('emails')
                for email in emails:
                    if email.get('primary') == True:
                        primary_email = email.get('value')
                
                data = {
                    'userName': userName,
                    'uid': uid,
                    'email': primary_email
                }
                writer.writerow(data)
            startIndex += PAGE_SIZE
    print('Complete')