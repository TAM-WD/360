import requests
import os
import csv
from datetime import datetime

TOKEN = ''
ORG_TYPE = '' # X-Org-ID OR X-Cloud-Org-ID
ORGID = ''

USER_LOGIN = '' # логин пользователя
DATE_START = '' # YYYY-MM-DDThh:mm:ss.sss±hhmm
DATE_FINISH = '' # YYYY-MM-DDThh:mm:ss.sss±hhmm


def tracker_get_info_about_user(user):
    url = f'https://api.tracker.yandex.net/v3/users/login:{user}'
    headers = {
        'Host': 'api.tracker.yandex.net',
        'Authorization': f'OAuth {TOKEN}',
        ORG_TYPE: ORGID
    }
    response = requests.get(url,headers=headers)
    return response.json()

def tracket_get_worklogs_post(user,date_start,date_end):
    url = 'https://api.tracker.yandex.net/v3/worklog/_search'
    data = {
        'createdBy': user,
        'createdAt': {
            'from': date_start,
            'to': date_end
        }
    }
    headers = {
        'Host': 'api.tracker.yandex.net',
        'Authorization': f'OAuth {TOKEN}',
        ORG_TYPE: ORGID
    }
    response = requests.post(url,json=data,headers=headers)
    return response.json()

if __name__ == '__main__':
    start = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    field_names = [
        'id',
        'issue',
        'comment',
        'createdBy_id',
        'createdBy_displayName',
        'createdAt',
        'updatedAt',
        'start',
        'duration'
    ]
    user_id = tracker_get_info_about_user(USER_LOGIN)
    user = user_id.get('trackerUid')
    print(f'Getting info for {user}')
    file_path = os.path.abspath(f'{dir}/get_worklogs_for_{user}_{start}.csv')
    with open(file_path, 'w',encoding='utf-8',newline='') as csv_file:
        writer = csv.DictWriter(csv_file, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        worklogs = tracket_get_worklogs_post(user,DATE_START,DATE_FINISH)
        print(worklogs)
        for log in worklogs:
            issue = log.get('issue')
            createdBy = log.get('createdBy')
            log.update({
                'issue': issue.get('key'),
                'createdBy_id': createdBy.get('id'),
                'createdBy_displayName': createdBy.get('display')
            })
            writer.writerow(log)
    print('Complete')
