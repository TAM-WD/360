# Скрипт предназначен для замены userName (NameID) по списку UID пользователей в Яндексе на тот же userName БЕЗ ДОМЕНА.
# Скрипт повлияет на аутентификацию пользователей в Яндексе.

import requests
from requests.adapters import HTTPAdapter, Retry

DOMAINID = '' # ID домена из конфига YandexADSCIM
SCIM_TOKEN = '' # токен из конфига YandexADSCIM

# Список UID
users = [
    1,
    2,
]


def patch_user(userId):
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{userId}'
    headers = {'Authorization': f'Bearer {SCIM_TOKEN}', 'ContentType': 'application/json'}
    get = requests.get(url, headers=headers)
    request = get.json()['userName']
    good_userName = request.split('@')[0]
    body = {
        'Operations': [
            {
                'value': good_userName,
                'op': 'replace',
                'path': 'userName'
            }
        ],
        'schemas': ['urn:ietf:params:scim:api:messages:2.0:PatchOp']
    }
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    patch = session.patch(url, headers=headers, json=body)
    return print(f'patching: {userId}, userName: {good_userName}, status: {patch.status_code}')

if __name__ == '__main__':
    for user in users:
        patch_user(user)