### Скрипт предназначен для замены никнейма в случае, если новый адрес уже занесён в этот же аккаунт.

import requests
import os
from datetime import datetime

SCIM_TOKEN = '' # Токен из утилиты YandexADSCIM
DOMAINID = '' # ID домена из утилиты YandexADSCIM

TOKEN = '' # Токен OAuth приложения с правами на редактирование пользователей
ORGID = '' # ID организации

USER_ID = '' # ID пользователя
NEW_NICKNAME = '' # никнейм, который нужно установить пользователю

def scim_get_user_info(user_id):
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
    request = requests.get(url,headers=headers)
    response = request.json()
    print(f'scim_get_user_info | {request.status_code} | {user_id}')
    file.write(f"scim_get_user_info | {request.status_code} | {user_id}\n")
    return response

def scim_patch_user(user_id, type, value):
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization': f'Bearer {SCIM_TOKEN}', 'Content-Type': 'application/json'}
    if type == 'emails':
        body = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{
                    "op":"replace",
                    "path":"urn:ietf:params:scim:schemas:extension:yandex360:2.0:User.emails",
                    "value": value
                }]}
    elif type == 'aliases':
        body = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{
                    "op":"replace",
                    "path":"urn:ietf:params:scim:schemas:extension:yandex360:2.0:User.aliases",
                    "value": value
                }]}
    else:
        return print('No such type')
    
    request = requests.patch(url,headers=headers,json=body)
    request.raise_for_status()
    file.write(f"scim_patch_user | {request.status_code} | {user_id}, {type}, {value}\n")
    return

def api360_patch_nickname(user_id, new_nickname):
    url = f'https://api360.yandex.net/directory/v1/org/{ORGID}/users/{user_id}'
    headers = {'Authorization': f"OAuth {TOKEN}", 'Content-Type': 'application/json'}
    body = {
        'nickname': new_nickname
    }
    request = requests.patch(url,headers=headers, json=body)
    file.write(f"api360_patch_nickname | {request.status_code} | {user_id}, {new_nickname}\n")
    return

if __name__ == '__main__':
    start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir = os.path.dirname(__file__)
    file_path = os.path.abspath(f'{dir}/user_{USER_ID}_{start_time}.log')
    file = open(file_path, mode='w', newline='', encoding='utf-8')
    response = scim_get_user_info(USER_ID)
    file.write(f"response start: {response}\n\n")
    emails = response.get('emails')
    new_emails = []
    for email in emails:
        value = email.get('value')
        if value.split('@')[0] != NEW_NICKNAME:
            new_emails.append(email)
        else:
            print(f'Found email: {value}')
    file.write(f"new emails start: {new_emails}\n\n")

    aliases = response.get('urn:ietf:params:scim:schemas:extension:yandex360:2.0:User').get('aliases')
    new_aliases = []
    for alias in aliases:
        value = alias.get('login')
        if value != NEW_NICKNAME:
            new_aliases.append(value)
        else:
            print(f'Found alias: {value}')
    file.write(f"new aliases start: {new_aliases}\n\n")

    scim_patch_user(USER_ID, 'aliases', new_aliases)
    scim_patch_user(USER_ID, 'emails', new_emails)

    api360_patch_nickname(USER_ID, NEW_NICKNAME)

    new_user = scim_get_user_info(USER_ID)
    file.write(f"response finish: {new_user}\n\n")

    file.close()

    print('Complete')
