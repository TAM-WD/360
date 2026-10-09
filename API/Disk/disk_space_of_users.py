"""Выгрузка места на Дисках пользователей организации с SSO + SCIM.

Обязательные переменные окружения: ORGID, ADMIN_TOKEN, CLIENT_ID,
CLIENT_SECRET, DOMAIN_ID (из конфига YandexADSCIM), SCIM_TOKEN.
Необязательные: MAX_WORKERS (по умолчанию 5), LOG_LEVEL (по умолчанию INFO).
Настройки берутся из окружения процесса и .env рядом со скриптом.
Уже заданные переменные окружения имеют приоритет над .env.
Рабочая папка проекта на поиск .env не влияет. Поддерживается UTF-8 с BOM и без.
Зависимости для macOS/Windows: python -m pip install requests python-dotenv

Рядом со скриптом создаются CSV с разделителем «;» и папка logs.
Журнал logs/disk_space_of_users.log дополняется при каждом запуске.
Поля CSV: email, uid, isEnabled, used_space(gb), total_space(gb).
Объёмы вычисляются в GiB (1024**3 байт); имена полей сохранены для совместимости.
Изначально заблокированные пользователи временно разблокируются через SCIM.
Обратная блокировка запрашивается в finally, в том числе при ошибках API.
Ошибки выводятся в консоль и журнал; CSV при ошибках может быть неполным.

Сервисные приложения: https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications
API Диска: https://yandex.ru/dev/disk-api/doc/ru/reference/meta
"""

from __future__ import annotations
import csv
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

import requests
from dotenv import load_dotenv


logger = logging.getLogger(__name__)
PERPAGE = 1000
RETRY_STATUSES = {429, 500, 502}
BACKOFF_SECONDS = (1, 2, 4, 8, 16)
REQUEST_TIMEOUT = (10, 60)  # connect, read; секунды
FIELD_NAMES = ['email', 'uid', 'isEnabled', 'used_space(gb)', 'total_space(gb)']


@dataclass(frozen=True)
class Config:
    org_id: str
    org_token: str = field(repr=False)
    client_id: str
    client_secret: str = field(repr=False)
    domain_id: str
    scim_token: str = field(repr=False)
    max_workers: int = 5

    @classmethod
    def from_env(cls):
        names = ('ORGID', 'ADMIN_TOKEN', 'CLIENT_ID', 'CLIENT_SECRET', 'DOMAIN_ID', 'SCIM_TOKEN')
        values = {name: os.getenv(name, '').strip() for name in names}
        missing = [name for name, value in values.items() if not value]
        if missing:
            raise ValueError('Не заданы переменные окружения: ' + ', '.join(missing))
        try:
            max_workers = int(os.getenv('MAX_WORKERS', '5'))
        except ValueError:
            raise ValueError('MAX_WORKERS должен быть положительным целым числом') from None
        if max_workers < 1:
            raise ValueError('MAX_WORKERS должен быть положительным целым числом')
        return cls(
            org_id=values['ORGID'], org_token=values['ADMIN_TOKEN'],
            client_id=values['CLIENT_ID'], client_secret=values['CLIENT_SECRET'],
            domain_id=values['DOMAIN_ID'], scim_token=values['SCIM_TOKEN'],
            max_workers=max_workers,
        )


def api_request(session, method, url, **kwargs):
    """REST-запрос: таймаут и пять повторов для 429/500/502, включая POST/PATCH.

    Session принадлежит одному потоку. Заголовки, тело и параметры URL
    не логируются. Прочие HTTP-ошибки и сетевые ошибки передаются вызывающему коду.
    """
    method = method.upper()
    endpoint = urlsplit(url).hostname
    kwargs.setdefault('timeout', REQUEST_TIMEOUT)
    for attempt in range(len(BACKOFF_SECONDS) + 1):
        try:
            response = session.request(method, url, **kwargs)
        except requests.RequestException as error:
            logger.error('%s %s: сетевая ошибка %s', method, endpoint, type(error).__name__)
            raise
        status = response.status_code
        logger.debug('%s %s: HTTP %s, попытка %s', method, endpoint, status, attempt + 1)
        if status in RETRY_STATUSES and attempt < len(BACKOFF_SECONDS):
            delay = BACKOFF_SECONDS[attempt]
            response.close()
            logger.warning(
                '%s %s: HTTP %s, повтор %s/%s через %s с',
                method, endpoint, status, attempt + 1, len(BACKOFF_SECONDS), delay,
            )
            time.sleep(delay)
            continue
        try:
            response.raise_for_status()
        except requests.HTTPError:
            logger.error('%s %s: HTTP %s, запрос завершился ошибкой', method, endpoint, status)
            response.close()
            raise
        return response


def get_token(uid, config, session):
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': config.client_id,
        'client_secret': config.client_secret,
        'subject_token': str(uid),
        'subject_token_type': 'urn:yandex:params:oauth:token-type:uid',
    }
    response = api_request(session, 'POST', 'https://oauth.yandex.ru/token', data=data)
    return response.json()['access_token']


def get_users(page, config, session):
    response = api_request(
        session, 'GET', f'https://api360.yandex.net/directory/v1/org/{config.org_id}/users',
        headers={'Authorization': f'OAuth {config.org_token}'},
        params={'page': page, 'perPage': PERPAGE},
    ).json()
    return response['pages'], response['users']


def scim_set_active(user_id, active, config, session):
    body = {
        'schemas': ['urn:ietf:params:scim:api:messages:2.0:PatchOp'],
        'Operations': [{'op': 'replace', 'path': 'active', 'value': active}],
    }
    api_request(
        session, 'PATCH', f'https://{config.domain_id}.scim-api.passport.yandex.net/v2/Users/{user_id}',
        headers={'Authorization': f'Bearer {config.scim_token}'}, json=body,
    )
    logger.info('SCIM: uid=%s, active=%s, запрос выполнен', user_id, active)


def scim_enable_user(user_id, config, session):
    scim_set_active(user_id, True, config, session)


def scim_disable_user(user_id, config, session):
    scim_set_active(user_id, False, config, session)


def disk_get_space_info(token, session):
    response = api_request(
        session, 'GET', 'https://cloud-api.yandex.net/v1/disk/',
        headers={'Authorization': f'OAuth {token}'},
    ).json()
    return (
        round(int(response['used_space']) / 1024**3, 4),
        round(int(response['total_space']) / 1024**3, 4),
    )


def process_user(user, config):
    uid = user['id']
    if int(uid) <= 1130000000000000:
        logger.info('Пропущен недоменный пользователь uid=%s', uid)
        return None
    restore_block = user['isEnabled'] is False
    # Отдельная сессия на пользователя: между потоками нет общего HTTP-состояния.
    with requests.Session() as session:
        try:
            if restore_block:
                scim_enable_user(uid, config, session)
            token = get_token(uid, config, session)
            used_space, total_space = disk_get_space_info(token, session)
            return {
                'email': user['email'], 'uid': uid, 'isEnabled': user['isEnabled'],
                'used_space(gb)': used_space, 'total_space(gb)': total_space,
            }
        finally:
            # Даже ошибка enable может означать, что сервер успел применить PATCH.
            if restore_block:
                try:
                    scim_disable_user(uid, config, session)
                except Exception:
                    logger.critical(
                        'Не удалось восстановить блокировку uid=%s. Проверьте состояние пользователя!', uid,
                    )
                    raise


def export_users(config, file_path):
    completed = failed = skipped = 0
    seen_uids = set()
    with requests.Session() as session, ThreadPoolExecutor(
        max_workers=config.max_workers, thread_name_prefix='disk',
    ) as executor, file_path.open('w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, FIELD_NAMES, delimiter=';')
        writer.writeheader()
        total_pages, first_users = get_users(1, config, session)
        logger.info('Страниц пользователей: %s; потоков: %s', total_pages, config.max_workers)
        for page in range(1, total_pages + 1):
            users = first_users if page == 1 else get_users(page, config, session)[1]
            logger.info('Обработка страницы %s/%s, пользователей: %s', page, total_pages, len(users))
            futures = {}
            for user in users:
                uid = str(user['id'])
                # Повторный UID не должен приводить к одновременным SCIM-операциям.
                if uid in seen_uids:
                    logger.warning('Пропущен повторный uid=%s', uid)
                    continue
                seen_uids.add(uid)
                futures[executor.submit(process_user, user, config)] = uid
            for future in as_completed(futures):
                uid = futures[future]
                try:
                    info = future.result()
                except Exception as error:
                    failed += 1
                    # Текст исключения может содержать ответ API или секреты.
                    logger.error('Ошибка обработки uid=%s: %s', uid, type(error).__name__)
                    continue
                if info is None:
                    skipped += 1
                else:
                    # CSV пишет только основной поток после завершения finally.
                    writer.writerow(info)
                    completed += 1
                    logger.info('Получены данные uid=%s', uid)
            csvfile.flush()
    logger.info(
        'Выгрузка завершена: записано=%s, пропущено=%s, ошибок=%s; CSV: %s',
        completed, skipped, failed, file_path,
    )
    return 1 if failed else 0


def main():
    script_dir = Path(__file__).resolve().parent
    env_path = script_dir / '.env'
    log_dir = script_dir / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / 'disk_space_of_users.log'
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f')
    file_path = script_dir / f'disk_space_{now}.csv'
    # Настраиваем вывод до чтения .env. force заменяет обработчики IDE;
    # StreamHandler и FileHandler сбрасывают буфер после каждой записи.
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, mode='a', encoding='utf-8')],
        force=True,
    )
    # Не включаем HTTP debug даже при LOG_LEVEL=DEBUG.
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    try:
        if env_path.is_file():
            load_dotenv(dotenv_path=env_path, override=False, encoding='utf-8-sig')
            logger.info('Прочитан файл настроек: %s', env_path)
        else:
            logger.warning('Файл %s не найден; используются переменные окружения процесса', env_path)
    except (OSError, UnicodeError) as error:
        logger.error('Не удалось прочитать %s: %s', env_path, type(error).__name__)
        return 1
    level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO').strip().upper(), None)
    if not isinstance(level, int):
        logger.error('LOG_LEVEL должен быть DEBUG, INFO, WARNING, ERROR или CRITICAL')
        return 1
    logging.getLogger().setLevel(level)
    try:
        config = Config.from_env()
    except ValueError as error:
        logger.error('%s', error)
        return 1
    logger.info('Начало выгрузки; журнал: %s', log_path)
    try:
        return export_users(config, file_path)
    except Exception as error:
        logger.error('Выгрузка прервана: %s; CSV может быть неполным: %s', type(error).__name__, file_path)
        return 1


if __name__ == '__main__':
    main()
