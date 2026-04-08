### Скрипт предназначен для выгрузки аудит-логов организации
# Документация по методу получения логов https://yandex.ru/dev/api360/doc/ru/audit-logs/get-logs

import concurrent.futures
import csv
import json
import logging
import requests
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict, Any, Union


TOKEN = '' # токен администратора организации с правами ya360_security:read_auditlog
ORGID = '' # ID организации


STARTED_AT = ''   # начало диапазона выгрузки, ISO 8601 (например '2026-01-01T00:00:00+00:00'). Если задано — константа DAYS игнорируется
ENDED_AT = ''     # конец диапазона выгрузки, ISO 8601. Если пусто — используется текущее время
INCLUDE_UIDS = '' # UID пользователей через запятую: выгружать только их события (нельзя совмещать с EXCLUDE_UIDS)
EXCLUDE_UIDS = '' # UID пользователей через запятую: исключить их события (нельзя совмещать с INCLUDE_UIDS)
IP = ''           # фильтр по IP-адресу (принимает только одно значение)
SERVICE = ''      # фильтр по источнику события

ACTIONS = '' # позволяет задать конкретный фильтр по типам событий. Без заданного фильтра можно выбрать действия из пресетов по сервисам (в соответствии с таблицами документации)

# Пресеты событий аудит-лога. Используются если ACTIONS осталась пустой строкой. Выбираются через input после запуска скрипта
PRESETS = {
    # События авторизации пользователей-сотрудников
    "AUTH": 'id_cookie.set,id_nondevice_token.issued,id_device_token.issued,id_app_password.login,id_account.glogout,id_account.changed_password',
    
    # События управления аккаунтами пользователей-сотрудников
    "USER_ADM": 'ya360_b2bplatform_user_created,ya360_b2bplatform_user_dismissed,ya360_b2bplatform_user_blocked,ya360_b2bplatform_user_unblocked,ya360_b2bplatform_user_updated,ya360_b2bplatform_user_alias_created,ya360_b2bplatform_user_alias_deleted,ya360_b2bplatform_user_password_updated,ya360_b2bplatform_invite_created,ya360_b2bplatform_invite_used',
    
    # События управления организацией
    "ORG_ADM": 'ya360_b2bplatform_organization_owner_updated,ya360_b2bplatform_organization_updated,ya360_b2bplatform_organization_sso_settings_updated,ya360_b2bplatform_domain_created,ya360_b2bplatform_domain_deleted,ya360_b2bplatform_domain_updated',
    
    # События управления группами и подразделениями
    "GROUPS_ADM": 'ya360_b2bplatform_group_created,ya360_b2bplatform_group_deleted,ya360_b2bplatform_group_updated,ya360_b2bplatform_group_member_created,ya360_b2bplatform_group_member_deleted,ya360_b2bplatform_department_created,ya360_b2bplatform_department_deleted,ya360_b2bplatform_department_updated,ya360_b2bplatform_department_member_created,ya360_b2bplatform_department_member_deleted',
    
    # События ресурсов организации
    "RESOURCES": 'ya360_b2bplatform_resource_shared_mailbox_created,ya360_b2bplatform_resource_shared_mailbox_deleted,ya360_b2bplatform_resource_shared_mailbox_updated,ya360_b2bplatform_resource_delegated_mailbox_created,ya360_b2bplatform_resource_delegated_mailbox_deleted,ya360_b2bplatform_resource_shared_disk_created,ya360_b2bplatform_resource_shared_disk_deleted,ya360_b2bplatform_resource_office_created,ya360_b2bplatform_resource_office_deleted,ya360_b2bplatform_resource_office_updated,ya360_b2bplatform_resource_room_created,ya360_b2bplatform_resource_room_deleted,ya360_b2bplatform_resource_room_updated,ya360_b2bplatform_resource_shared_contact_created,ya360_b2bplatform_resource_shared_contact_deleted,ya360_b2bplatform_resource_shared_contact_updated',
    
    # События Телемоста
    "TELEMOST": 'telemost_conference.created,telemost_conference.access_level_changed,telemost_conference.started,telemost_conference.ended,telemost_conference.peer.joined,telemost_conference.peer.kicked,telemost_conference.peer.role_changed,telemost_conference.waiting_room.peer.joined,telemost_conference.waiting_room.peer.admitted,telemost_conference.waiting_room.peer.left,telemost_conference.live_stream.created,telemost_conference.live_stream.started,telemost_conference.live_stream.access_level_changed,telemost_conference.live_stream.ended,telemost_conference.live_stream.viewer.joined',
    
    # События Мессенджера
    "MESSENGER": 'messenger_chat.created,messenger_chat.info_changed,messenger_chat.invite_link_renewed,messenger_chat.member_rights_changed,messenger_chat.member.added,messenger_chat.member.role_changed,messenger_chat.member.removed,messenger_chat.group_added,messenger_chat.group_removed,messenger_chat.department_added,messenger_chat.department_removed,messenger_chat.file_sent,messenger_chat.message_deleted,messenger_chat.call_started,messenger_user.takeout_requested',
    
    # События Диска
    "DISK": 'disk_fs-mkdir,disk_fs-store,disk_fs-hardlink-copy,disk_fs-copy,disk_fs-move,disk_fs-view,disk_fs-get-download-url,disk_fs-public-view,disk_fs-set-public,disk_fs-set-private,disk_fs-set-public-settings,disk_fs-remove-public-settings,disk_fs-trash-append,disk_fs-trash-restore,disk_fs-trash-drop,disk_fs-trash-drop-all,disk_fs-rm,disk_share-create-group,disk_share-invite-user,disk_share-activate-invite,disk_share-change-rights,disk_share-remove-invite',
}


RESULTS_FILE = 'auditlog_result' # имя CSV-файла, куда будут записаны итоги анализа
THREADS = 10  # количество параллельных потоков
DAYS = 180    # глубина выгрузки в днях

ts = datetime.now().strftime('%Y%m%d_%H%M%S')
_log_path = Path(__file__).parent / f'auditlog_{ts}.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(_log_path, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def make_request(
    url: str,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
) -> Optional[Union[Dict, str]]:
    """JSON REST-запрос с ретраями на 429/500"""

    method = method.upper()
    if headers is None:
        headers = {}
    if body is not None:
        headers.setdefault('Content-Type', 'application/json')

    def _do_request() -> requests.Response:
        return requests.request(
            method=method,
            url=url,
            params=params,
            json=body,
            headers=headers,
            timeout=timeout,
        )

    def _parse(response: requests.Response) -> Union[Dict, str]:
        response.encoding = 'utf-8'
        raw = response.text
        if not raw:
            logger.warning('%s %s | status=%s | тело ответа пустое', method, url, response.status_code)
            return {}
        try:
            return response.json()
        except json.JSONDecodeError:
            logger.warning('%s %s | status=%s | ответ не JSON: %s', method, url, response.status_code, raw[:300])
            return raw

    try:
        response = _do_request()
        response.raise_for_status()
        logger.info('%s %s | status=%s | params=%s', method, url, response.status_code, params)
        return _parse(response)

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        logger.error('HTTP %s | %s %s | params=%s', status, method, url, params)

        if status in (429, 500):
            for attempt in range(1, 4):
                logger.warning('Retry %s/3 (status=%s) | %s %s', attempt, status, method, url)
                time.sleep(attempt)
                try:
                    response = _do_request()
                    response.raise_for_status()
                    logger.info('Retry %s OK | %s %s | status=%s', attempt, method, url, response.status_code)
                    return _parse(response)
                except requests.exceptions.HTTPError as retry_e:
                    logger.error('Retry %s failed: HTTP %s', attempt, retry_e.response.status_code if retry_e.response else '?')
                    e = retry_e

        return e

    except Exception as e:
        logger.error('Unexpected error: %s | %s %s', e, method, url)
        return e

def adm_get_auditlog(
        started_at: str = None,
        ended_at: str = datetime.isoformat(datetime.now()),
        types: str = None,
        count: int = 100,
        iteration_key: str = None,
        include_uids: str = None,
        exclude_uids: str = None,
        ip: str = None,
        service: str = None
        ) -> Optional[Union[Dict, str, bytes]]:
    """Функция для получения аудит-логов"""

    url: str = f"https://cloud-api.yandex.net/v1/auditlog/organizations/{ORGID}/events"
    headers: Dict[str, Any] = {'Authorization': f'OAuth {TOKEN}'}
    params: Dict[str, Any] = {
        key: value
        for key, value in {
            'started_at': started_at,
            'ended_at': ended_at,
            'types': types,
            'count': count,
            'iteration_key': iteration_key,
            'include_uids': include_uids,
            'exclude_uids': exclude_uids,
            'ip': ip,
            'service': service
        }.items()
        if value
    }
    return make_request(url=url, method='GET', params=params, headers=headers)

def flatten_record(record: Dict[str, Any], prefix: str = '', sep: str = '.') -> Dict[str, Any]:
    """Рекурсивно разворачивает вложенный словарь в плоский с dot-нотацией ключей"""
    result = {}
    for key, value in record.items():
        full_key = f"{prefix}{sep}{key}" if prefix else key
        if isinstance(value, dict):
            result.update(flatten_record(value, prefix=full_key, sep=sep))
        else:
            result[full_key] = value
    return result

def _fetch_records_for_period(
    started_at: str,
    ended_at: str,
    types: str = None,
    count: int = 100,
    include_uids: str = None,
    exclude_uids: str = None,
    ip: str = None,
    service: str = None,
) -> list[Dict[str, Any]]:
    """Выгружает все записи аудит-лога за один период"""
    records: list[Dict[str, Any]] = []
    iteration_key: Optional[str] = None
    page = 0

    while True:
        page += 1
        response = adm_get_auditlog(
            started_at=started_at,
            ended_at=ended_at,
            types=types,
            count=count,
            iteration_key=iteration_key,
            include_uids=include_uids,
            exclude_uids=exclude_uids,
            ip=ip,
            service=service,
        )

        if isinstance(response, Exception):
            logger.error("[%s] стр.%s: запрос завершился исключением: %s", started_at[:10], page, response)
            break

        # API может вернуть список напрямую или обёрнутый в dict с ключом 'events'
        if isinstance(response, list):
            events = response
        elif isinstance(response, dict):
            events = response.get('items', [])
            if not events:
                keys = list(response.keys())
                logger.debug("[%s] стр.%s: items пустой, ключи ответа: %s", started_at[:10], page, keys)
        else:
            preview = repr(response)[:500] if response else '(пусто)'
            logger.error("[%s] стр.%s: неожиданный тип ответа %s: %s",
                         started_at[:10], page, type(response).__name__, preview)
            break

        if not events:
            break

        for record in events:
            records.append(flatten_record(record))

        logger.info("[%s] стр.%s: +%s событий", started_at[:10], page, len(events))

        iteration_key = response.get('iteration_key') if isinstance(response, dict) else None
        if not iteration_key:
            break

    return records

def export_auditlog_to_csv(
    output_path: str,
    started_at: str = None,
    ended_at: str = None,
    types: str = None,
    count: int = 100,
    include_uids: str = None,
    exclude_uids: str = None,
    ip: str = None,
    service: str = None,
) -> int:
    """Параллельно выгружает аудит-лог за последние DAYS дней в CSV"""

    now = datetime.now(tz=timezone.utc)
    dt_end = datetime.fromisoformat(ended_at) if ended_at else now
    dt_start = datetime.fromisoformat(started_at) if started_at else (now - timedelta(days=DAYS))

    day_intervals: list[tuple[str, str]] = []
    cursor = dt_start.replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor < dt_end:
        day_end = min(cursor + timedelta(days=1), dt_end)
        day_intervals.append((cursor.isoformat(), day_end.isoformat()))
        cursor += timedelta(days=1)

    logger.info("Запуск выгрузки: %s дней, %s потоков", len(day_intervals), THREADS)

    all_records: list[Dict[str, Any]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {
            executor.submit(
                _fetch_records_for_period,
                day_start, day_end, types, count, include_uids, exclude_uids, ip, service,
            ): day_start
            for day_start, day_end in day_intervals
        }
        for future in concurrent.futures.as_completed(futures):
            day_label = futures[future]
            try:
                records = future.result()
                all_records.extend(records)
                logger.info("День %s завершён: %s событий", day_label[:10], len(records))
            except Exception as exc:
                logger.error("День %s упал с ошибкой: %s", day_label[:10], exc)

    if not all_records:
        logger.warning("Нет данных для записи в CSV")
        return 0

    all_records.sort(key=lambda r: r.get('event.occurred_at', ''))

    all_columns: list[str] = list(dict.fromkeys(
        col for record in all_records for col in record
    ))

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=all_columns, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(all_records)

    logger.info("CSV записан: %s (%s строк, %s колонок)", output_path, len(all_records), len(all_columns))
    return len(all_records)

def select_preset() -> str:
    """Интерактивный выбор пресета из PRESETS"""
    keys = list(PRESETS.keys())
    print("\nДоступные пресеты:")
    for i, key in enumerate(keys, 1):
        first_event = PRESETS[key].split(',')[0]
        print(f"  {i}. {key}  ({first_event}, ...)")
    print()
    while True:
        raw = input(f"Выберите пресет [1-{len(keys)}]: ").strip()
        if raw.isdigit() and 1 <= int(raw) <= len(keys):
            chosen = keys[int(raw) - 1]
            print(f"Выбран пресет: {chosen}")
            return PRESETS[chosen]
        print(f"Введите число от 1 до {len(keys)}")


def main():
    if INCLUDE_UIDS and EXCLUDE_UIDS:
        raise ValueError("Нельзя одновременно задавать INCLUDE_UIDS и EXCLUDE_UIDS")

    actions = ACTIONS
    if not actions:
        actions = select_preset()

    total = export_auditlog_to_csv(
        output_path=str(Path(__file__).parent / f'{RESULTS_FILE}_{ts}.csv'),
        types=actions,
        started_at=STARTED_AT or None,
        ended_at=ENDED_AT or None,
        include_uids=INCLUDE_UIDS or None,
        exclude_uids=EXCLUDE_UIDS or None,
        ip=IP or None,
        service=SERVICE or None,
    )
    print(f"Готово: {total} записей")

if __name__ == '__main__':

    main()