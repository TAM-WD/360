"""Выгрузка занятого, свободного и общего места на общих Дисках организации.

Обязательные переменные окружения: ORGID, ADMIN_TOKEN.
Необязательные: MAX_WORKERS (по умолчанию 5), LOG_LEVEL (по умолчанию INFO).
Настройки берутся из окружения процесса и .env рядом со скриптом.
Уже заданные переменные окружения имеют приоритет над .env.
Рабочая папка проекта на поиск .env не влияет. Поддерживается UTF-8 с BOM и без.
Зависимости для macOS/Windows: python -m pip install requests python-dotenv

Рядом со скриптом создаются CSV с разделителем «;» и папка logs.
Журнал logs/disk_space_of_shared.log дополняется при каждом запуске.
Поля CSV: vd_hash, name, description, used_space, free_space, total_space.
Объёмы вычисляются в GiB (1024**3 байт) и округляются до четырёх знаков.
Ошибки выводятся в консоль и журнал; CSV при ошибках может быть неполным.

API Общих Дисков: https://yandex.ru/dev/disk-api/doc/ru/reference/shared-disks/shd-info
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
LIMIT = 100
RETRY_STATUSES = {429, 500, 502}
BACKOFF_SECONDS = (1, 2, 4, 8, 16)
REQUEST_TIMEOUT = (10, 60)  # connect, read; секунды
FIELD_NAMES = ['vd_hash', 'name', 'description', 'used_space', 'free_space', 'total_space']


@dataclass(frozen=True)
class Config:
    org_id: str
    admin_token: str = field(repr=False)
    max_workers: int = 5

    @classmethod
    def from_env(cls):
        names = ('ORGID', 'ADMIN_TOKEN')
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
        return cls(org_id=values['ORGID'], admin_token=values['ADMIN_TOKEN'], max_workers=max_workers)


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


def disk_get_ods(offset, config, session):
    return api_request(
        session, 'GET', 'https://cloud-api.yandex.net/v1/disk/virtual-disks/manage/org-resources/list',
        params={'org_id': config.org_id, 'limit': LIMIT, 'offset': offset},
        headers={'Authorization': f'OAuth {config.admin_token}'},
    ).json()


def disk_get_vd_space_info(vd_hash, config, session):
    response = api_request(
        session, 'GET', 'https://cloud-api.yandex.net/v1/disk/virtual-disks',
        params={'vd_hash': vd_hash}, headers={'Authorization': f'OAuth {config.admin_token}'},
    ).json()
    used_bytes = int(response['used_space'])
    total_bytes = int(response['total_space'])
    return (
        round(used_bytes / 1024**3, 4),
        round((total_bytes - used_bytes) / 1024**3, 4),
        round(total_bytes / 1024**3, 4),
    )


def process_disk(item, config):
    vd_hash = item['vd_hash']
    # Сессия принадлежит одному заданию и не разделяется между потоками.
    with requests.Session() as session:
        used_space, free_space, total_space = disk_get_vd_space_info(vd_hash, config, session)
    return {
        'vd_hash': vd_hash,
        'name': item.get('name'),
        'description': item.get('description'),
        'used_space': used_space,
        'free_space': free_space,
        'total_space': total_space,
    }


def export_disks(config, file_path):
    completed = failed = 0
    seen_hashes = set()
    offset = 0
    with requests.Session() as session, ThreadPoolExecutor(
        max_workers=config.max_workers, thread_name_prefix='shared-disk',
    ) as executor, file_path.open('w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, FIELD_NAMES, delimiter=';')
        writer.writeheader()
        logger.info('Начало получения списка общих Дисков; потоков: %s', config.max_workers)
        while True:
            response = disk_get_ods(offset, config, session)
            items = response['items']
            if not isinstance(items, list):
                raise ValueError('API вернул некорректное поле items')
            logger.info('Получена страница: offset=%s, дисков=%s', offset, len(items))
            # Читаем до пустой страницы, не полагаясь на трактовку поля total.
            if not items:
                break
            futures = {}
            for item in items:
                vd_hash = item['vd_hash']
                if vd_hash in seen_hashes:
                    logger.warning('Пропущен повторный Диск vd_hash=%s', vd_hash)
                    continue
                seen_hashes.add(vd_hash)
                futures[executor.submit(process_disk, item, config)] = vd_hash
            if not futures:
                logger.error('Страница offset=%s содержит только ранее полученные Диски; выгрузка прервана', offset)
                return 1
            for future in as_completed(futures):
                vd_hash = futures[future]
                try:
                    info = future.result()
                except Exception as error:
                    failed += 1
                    # Текст исключения может содержать ответ API или секреты.
                    logger.error('Ошибка обработки Диска vd_hash=%s: %s', vd_hash, type(error).__name__)
                    continue
                # CSV пишет только основной поток.
                writer.writerow(info)
                completed += 1
                logger.info('Получены данные Диска vd_hash=%s', vd_hash)
            csvfile.flush()
            offset += len(items)
    logger.info('Выгрузка завершена: записано=%s, ошибок=%s; CSV: %s', completed, failed, file_path)
    return 1 if failed else 0


def main():
    script_dir = Path(__file__).resolve().parent
    env_path = script_dir / '.env'
    log_dir = script_dir / 'logs'
    log_dir.mkdir(exist_ok=True)
    log_path = log_dir / 'disk_space_of_shared.log'
    now = datetime.now().strftime('%Y-%m-%d_%H-%M-%S_%f')
    file_path = script_dir / f'shared_disk_space_{now}.csv'
    # Настраиваем вывод до чтения .env. force заменяет обработчики IDE;
    # StreamHandler и FileHandler сбрасывают буфер после каждой записи.
    logging.basicConfig(
        level=logging.INFO, format='%(asctime)s | %(levelname)s | %(threadName)s | %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler(log_path, mode='a', encoding='utf-8')],
        force=True,
    )
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
        return export_disks(config, file_path)
    except Exception as error:
        logger.error('Выгрузка прервана: %s; CSV может быть неполным: %s', type(error).__name__, file_path)
        return 1


if __name__ == '__main__':
    main()
