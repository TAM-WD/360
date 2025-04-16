# В файле представлен пример запросов scim-api для блокировки, разблокировки и проверки текущего статуса (заблокирован/разблокирован).
# Приложен пример алгоритма, который можно использовать для получения доступа к ресурсам.

import requests
from requests.adapters import HTTPAdapter, Retry

DOMAINID = '' # ID домена из конфига YandexADSCIM
SCIM_TOKEN = '' # токен из конфига YandexADSCIM

uid = '' # UID пользователя, для которого нужно выполнить запрос 

def scim_get_ban_status(user_id): # запрос для получения статуса блокировки юзера. Возвращает is_active = True для разблокированного, is_active = False для заблокированного 
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization' : f'Bearer {SCIM_TOKEN}'}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.get(url, headers=headers)
    is_active = response.json()['active']
    return is_active

def scim_enable_user(user_id): # запрос для разблокировки юзера
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization' : f'Bearer {SCIM_TOKEN}'}
    body = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "replace","path": "active","value": True}]}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.patch(url, json=body, headers=headers)
    return print(user_id, 'was enabled', response.status_code)

def scim_disable_user(user_id): # запрос для блокировки юзера
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization' : f'Bearer {SCIM_TOKEN}'}
    body = {"schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "replace","path": "active","value": False}]}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    response = session.patch(url, json=body, headers=headers)
    return print(user_id, 'was disabled', response.status_code)


if __name__ == '__main__':
    is_active = scim_get_ban_status(uid)
    if is_active == True:
        scim_enable_user(uid)
        ban_need = True
    else:
        print(f'User {uid} already active')
    
    if ban_need == True:
        scim_disable_user(uid)