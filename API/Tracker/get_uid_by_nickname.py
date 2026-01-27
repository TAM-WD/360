import requests


def get_user_uid_from_service_app(client_id, client_secret, nickname, domain): 
    
    """
        Используются сервисные приложения: https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications
        Переменные:
        client_id, client_secret — id и секрет сервисного приложения с любыми правами;
        nickname — логин пользователя без домена (можно использовать алиас);
        domain - домен организации (можно использовать алиас)
    """

    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{client_id}',
        'client_secret': f'{client_secret}',
        'subject_token': f'{nickname}@{domain}',
        'subject_token_type': f'urn:yandex:params:oauth:token-type:email'
    }
    response = requests.post(url, data = data, headers=headers)
    user_id = response.json().get('access_token').split('.')[1]
    return user_id

def get_user_uid_from_tracker(orgid, org_type, nickname, token):

    """
        Используется API Яндекс Трекера: https://yandex.ru/support/tracker/ru/get-user
        Переменные:
        orgid — ID организации;
        org_type — тип организации, может принимать только значения 'X-Org-Id' или 'X-Cloud-Org-Id';
        nickname — логин пользователя без домена;
        token — токен OAuth приложения с доступом tracker:read
    """

    url = f'https://api.tracker.yandex.net/v3/users/login:{nickname}'
    headers = {
        org_type: orgid,
        'Authorization': f"OAuth {token}"
    }
    response = requests.get(url, headers=headers)
    passportUid = response.json().get('passportUid')
    return passportUid

get_user_uid_from_service_app(client_id='', client_secret='', nickname='', domain='')
get_user_uid_from_tracker(orgid='', org_type='', nickname='', token='')