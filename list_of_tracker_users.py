### Скрипт позволяет получить информацию о всех пользователях организации из Трекера
# Используется API Трекера 

import requests
import csv
import os
from datetime import datetime

ORGID = '' # ID организации
TOKEN = '' # OAuth токен с правами на чтение Трекера
ORG_TYPE = 'X-Org-Id' # или X-Cloud-Org-ID - тип организации (Яндекс 360 / Cloud Organization)

PERPAGE = 1000

def get_tracker_users(id):
    url = 'https://api.tracker.yandex.net/v3/users/_relative'
    params = {
        'perPage': PERPAGE,
        'id': id
    }
    headers = {
        ORG_TYPE: ORGID,
        'Authorization': f"OAuth {TOKEN}"
    }
    response = requests.get(url, params=params, headers=headers)
    return response.json()
    
if __name__ == '__main__':
    start = datetime.now()
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/tracker_users_{ORGID}_{start}.csv')
    field_names = ['uid','login','passportUid','cloudUid','firstName','lastName','display','email','external','hasLicense','dismissed','firstLoginDate','lastLoginDate']
    with open(file_path,mode='w',newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file,field_names, delimiter=';', extrasaction='ignore')
        writer.writeheader()
        hasNext = True
        id = 0
        while hasNext == True:
            response = get_tracker_users(id)
            users = response.get('users')
            for user in users:
                writer.writerow(user)
            hasNext = response.get('hasNext')
            if hasNext == True:
                last_user = users[-1]
                id = last_user.get('uid')
                print(f'Last uid on page: {id}')
            else:
                print('Last page')
    print('complete')