import requests
import os
import csv
from datetime import datetime


TOKEN = ''
ORG_TYPE = 'X-Org-ID' # OR X-Cloud-Org-ID
ORGID = ''

USER_ID = '' #1130000064987988 koltsovma
DATE_START = '' #YYYY-MM-DDThh:mm:ss.sss±hhmm
DATE_FINISH = '' #YYYY-MM-DDThh:mm:ss.sss±hhmm

headers = {
        'Host': 'api.tracker.yandex.net',
        'Authorization': f'OAuth {TOKEN}',
        ORG_TYPE: ORGID
    }

def tracker_search_tickets(login, date):
    url = 'https://api.tracker.yandex.net/v3/issues/_search?expand=transitions'
    data = {'query': f'"Time Spent": changed(by: {login} date: {date})'}
    response = requests.post(url,json=data,headers=headers)
    return response.json()

def tracker_get_worklogs_by_ticket(ticket):
    url = f'https://api.tracker.yandex.net/v3/issues/{ticket}/worklog'
    response = requests.get(url,headers=headers)
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
    user = USER_ID
    file_path = os.path.abspath(f'{dir}/get_worklogs_for_{user}_{start}.csv')
    with open(file_path, 'w',encoding='utf-8',newline='') as csv_file:
        writer = csv.DictWriter(csv_file, field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()
        date = f'>{DATE_START}'
        tickets = tracker_search_tickets(user,date)
        print(tickets)
        for ticket in tickets:
            key = ticket.get('key')
            worklogs = tracker_get_worklogs_by_ticket(key)
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
