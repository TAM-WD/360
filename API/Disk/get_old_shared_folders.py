### Скрипт для получения списка старых общих папок по списку пользователей, и поиска по аудит-логам пользователей, которые взаимодействовали с ресурсами в этой папке.
# Используются: API Диска https://yandex.ru/dev/disk-api/doc/ru/ ; API360 https://yandex.ru/dev/api360/doc/ref/index.html ; сервисные приложения https://yandex.ru/support/yandex-360/business/admin/ru/security-service-applications

import requests
import csv
import json
import logging
import queue
from typing import Optional, Dict, Any, Union
import time
from dataclasses import dataclass
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

"""
Инструкция по использованию:

1. Подготовьте CSV-файл в кодировке utf-8 с пользователями, у которых нужно проверить наличие старых общих папок. Колонку с id пользователем можно назвать uid или id. Альтернативно можно указать email. Разделитель должен быть <,> или <;>

2. Заполните переменные:

    Обязательные:
    - TOKEN == токен администратора с правами directory:read_users и ya360_security:read_auditlog
    - ORGID == id организации Яндекс 360 для бизнеса
    - DOMAINID == id домена, можно взять из конфига YandexADSCIM
    - SCIM_TOKEN == токен с правами к scim, можно взять из конфига YandexADSCIM
    - CLIENT_ID и CLIENT_SECRET == client_id и client_secret сервисного приложения с правами чтения Диска cloud_api:disk.read
    - USERS_FILENAME == имя CSV-файла* с user_id пользователей
    - RESULTS_FILE == имя файла*, в который будет записан результат
    
    Опциональные:
    - AUDITLOG_FILE == имя файла* с аудит-логом. При первом запуске оставить пустой строкой, лог будет получен с помощью API. Гарантируется работа только с файлом, полученным ранее этим скриптом

    *Для имён файлов — Можно указать имя файла, если он лежит в одной папке со скриптом, или полный путь к файлу.

    3. Запустите скрипт
"""

### === Заполняемые переменные === ###

# Авторизация #

TOKEN = '' # dir:read_users, read audit
ORGID = '' # orgid

DOMAINID = '' # ID домена из утилиты YandexADSCIM
SCIM_TOKEN = '' # Токен из утилиты YandexADSCIM

CLIENT_ID = '' # client_id сервисного приложения
CLIENT_SECRET = '' # client_secret сервисного приложения

# Названия файлов #
USERS_FILENAME = '' # имя файла с пользователями. 
RESULTS_FILE = 'shared_folders_result.csv' # имя CSV-файла, куда будут записаны итоги анализа
AUDITLOG_FILE = '' # имя готового CSV аудит-лога

# Сервисные константы, не рекомендуется менять #
MAX_DEPTH = 5 # максимальная глубина рекурсивного обхода папок (None = без ограничений)
MAX_WORKERS = 5 # количество потоков для параллельной обработки пользователей
FIELDS = 'path,type,_embedded.path,_embedded.total,_embedded.items.path,_embedded.items.type,_embedded.items.share,_embedded.items.public_url,_embedded.items.resource_id' # поля, выгружаемые из API Диска
ACTIONS = 'disk_fs-mkdir,disk_fs-store,disk_fs-hardlink-copy,disk_fs-copy,disk_fs-move,disk_fs-view,disk_fs-get-download-url,disk_fs-trash-append,disk_fs-trash-restore,disk_fs-rm,disk_share-invite-user,disk_share-activate-invite,disk_share-change-rights,disk_share-remove-invite' # события, выгружаемые из аудит-логов


_log_path = Path(__file__).parent / 'old_shared_folders.log'
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
    body: Optional[Union[Dict, str, bytes]] = None,
    headers: Optional[Dict[str, str]] = None,
    files: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    verify_ssl: bool = True
) -> Optional[Union[Dict, str, bytes]]:
    """Универсальная функция для rest-api запроса"""

    try:
        method = method.upper()
        if headers is None:
            headers = {}

        logger.debug('TIMEOUT IS %s', timeout)
        content_type = headers.get('Content-Type', '')

        # Multipart запрос с файлами
        if files is not None:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                data=body if isinstance(body, dict) else body,
                files=files,
                headers=headers,
                timeout=timeout,
                verify=verify_ssl
            )

        # Form-urlencoded запрос
        elif isinstance(body, dict) and 'application/x-www-form-urlencoded' in content_type:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                data=body,
                headers=headers,
                timeout=timeout,
                verify=verify_ssl
            )

        # Обычный JSON запрос
        elif isinstance(body, dict):
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json'
            response = requests.request(
                method=method,
                url=url,
                params=params,
                data=json.dumps(body),
                headers=headers,
                timeout=timeout,
                verify=verify_ssl
            )

        # Строка или bytes или None
        else:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                data=body,
                headers=headers,
                timeout=timeout,
                verify=verify_ssl
            )

        response.raise_for_status()
        logger.info('Request | %s %s | status=%s | params=%s | body=%s | timeout=%s',
                    method, url, response.status_code, params, body, timeout)
        response.encoding = 'utf-8'

        try:
            return response.json()
        except json.JSONDecodeError:
            return response.text if response.text else response.content

    except requests.exceptions.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        logger.error("Requests exception accured: %s | %s %s | params=%s | body=%s", e, method, url, params, body)
        if status in (429, 500):
            for attempt in range(1, 4):
                wait = attempt
                logger.warning("Retry %s/3 after %ss (status=%s) | %s %s", attempt, wait, status, method, url)
                time.sleep(wait)
                try:
                    retry_response = requests.request(
                        method=method, url=url, params=params,
                        data=body, headers=headers, timeout=timeout, verify=verify_ssl
                    )
                    retry_response.raise_for_status()
                    logger.info('Request (retry %s) | %s | %s | %s', attempt, url, method, retry_response.status_code)
                    retry_response.encoding = 'utf-8'
                    try:
                        return retry_response.json()
                    except json.JSONDecodeError:
                        return retry_response.text if retry_response.text else retry_response.content
                except requests.exceptions.HTTPError as retry_e:
                    logger.error("Retry %s failed: %s | %s %s", attempt, retry_e, method, url)
                    e = retry_e
                except Exception as retry_e:
                    logger.error("Retry %s unexpected error: %s | %s %s", attempt, retry_e, method, url)
                    return retry_e
        return e
    except Exception as e:
        logger.error("Unexpected error: %s | %s %s | params=%s | body=%s", e, method, url, params, body)
        return e

@dataclass
class UserData:
    uid: int
    email: Optional[str] = None
    token: Optional[str] = None
    token_expires_at: Optional[float] = None

    @property
    def token_expires_in(self) -> Optional[int]:
        if self.token_expires_at is None:
            return None
        return max(0, int(self.token_expires_at - time.time()))

    @property
    def is_token_valid(self) -> bool:
        if self.token is None or self.token_expires_at is None:
            return False
        return time.time() < self.token_expires_at

    def set_token(self, token: str, expires_in: int) -> None:
        self.token = token
        self.token_expires_at = time.time() + expires_in

    def to_dict(self) -> dict:
        return {
            'uid': self.uid,
            'email': self.email,
            'token': self.token,
            'token_expires_at': self.token_expires_at,
            'token_expires_in': self.token_expires_in,
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'UserData':
        return cls(
            uid=int(data['uid']),
            email=data.get('email'),
            token=data.get('token'),
            token_expires_at=data.get('token_expires_at'),
        )

class UserManager:
    def __init__(self, expiry_buffer: int = 60):
        self._users: Dict[int, UserData] = {}
        self._expiry_buffer = expiry_buffer
        self._lock = threading.Lock()

    @staticmethod
    def _parse_uid(user_id: str | int) -> int:
        try:
            return int(user_id)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"user_id должен быть int или str с числом, получено: {user_id!r}"
            ) from e

    def _get_user(self, uid: int) -> UserData:
        if uid not in self._users:
            self._users[uid] = UserData(uid=uid)
        return self._users[uid]

    def _is_token_valid(self, user_data: UserData) -> bool:
        if user_data.token is None or user_data.token_expires_at is None:
            return False
        return time.time() < (user_data.token_expires_at - self._expiry_buffer)

    def _fetch_token(self, uid: int) -> tuple[str, float]:
        url = 'https://oauth.yandex.ru/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        body = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'subject_token': uid,
            'subject_token_type': 'urn:yandex:params:oauth:token-type:uid'
        }

        response = make_request(url=url, method='POST', body=body, headers=headers)

        if isinstance(response, Exception):
            raise RuntimeError(f"Не удалось получить токен для uid={uid}: {response}")
        if not isinstance(response, dict):
            raise RuntimeError(f"Неожиданный ответ при получении токена: {response}")

        token = response.get('access_token')
        expires_in = response.get('expires_in')

        if not token or expires_in is None:
            raise RuntimeError(f"Ответ не содержит токена или времени жизни: {response}")

        return token, time.time() + expires_in

    def _refresh_token(self, uid: int) -> str:
        """Получает, кэширует и возвращает новый токен. Вызывать под локом."""
        logger.info("Запрашиваем токен для uid=%s...", uid)
        token, expires_at = self._fetch_token(uid)

        user = self._get_user(uid)
        user.token = token
        user.token_expires_at = expires_at

        logger.info("Токен для uid=%s получен, истекает через %s сек.", uid, int(expires_at - time.time()))
        return token

    def _ensure_user_active(self, uid: int) -> None:
        is_active = self._get_ban_status(uid)
        if not is_active:
            logger.warning("uid=%s заблокирован, выполняем разблокировку...", uid)
            self._enable_user(uid)

    def _get_ban_status(self, uid: int) -> bool:
        url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{uid}'
        headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
        response = make_request(url, method='GET', headers=headers)

        if isinstance(response, Exception) or not isinstance(response, dict):
            raise RuntimeError(f"Не удалось получить статус uid={uid}: {response}")

        return response.get('active', False)

    def _enable_user(self, uid: int) -> None:
        url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{uid}'
        headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
        body = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "replace", "path": "active", "value": True}]
        }
        response = make_request(url, method='PATCH', body=body, headers=headers)

        if isinstance(response, Exception):
            raise RuntimeError(f"Не удалось разблокировать uid={uid}: {response}")

        logger.info("uid=%s успешно разблокирован", uid)

    def _disable_user(self, uid: int) -> None:
        url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{uid}'
        headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
        body = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [{"op": "replace", "path": "active", "value": False}]
        }
        response = make_request(url, method='PATCH', body=body, headers=headers)

        if isinstance(response, Exception):
            raise RuntimeError(f"Не удалось заблокировать uid={uid}: {response}")

        logger.info("uid=%s успешно заблокирован", uid)

    @staticmethod
    def _is_401(response) -> bool:
        return (
            isinstance(response, requests.exceptions.HTTPError)
            and response.response is not None
            and response.response.status_code == 401
        )

    @staticmethod
    def _is_timeout(response) -> bool:
        return isinstance(response, requests.exceptions.Timeout)


    def get_token(self, user_id: str | int, force_refresh: bool = False) -> str:
        uid = self._parse_uid(user_id)

        with self._lock:
            user = self._get_user(uid)
            token_is_valid = self._is_token_valid(user)

            if token_is_valid and not force_refresh:
                return user.token

            if token_is_valid and force_refresh:
                self._ensure_user_active(uid)

            return self._refresh_token(uid)

    def make_user_request(
            self,
            user_id: str | int,
            url: str,
            method: str,
            params: Optional[Dict] = None,
            body: Optional[Union[Dict, str, bytes]] = None,
            headers: Optional[Dict] = None,
            **kwargs
    ) -> Optional[Union[Dict, str, bytes]]:
        """
        Выполняет запрос от имени пользователя с автоматическим управлением токеном.
        При 401 проверяет бан, перезапрашивает токен и повторяет запрос один раз.

        Returns:
            Ответ API или Exception (включая Timeout — для обработки на стороне вызывающего кода).
        """
        uid = self._parse_uid(user_id)

        if headers is None:
            headers = {}

        headers['Authorization'] = f'OAuth {self.get_token(uid)}'
        response = make_request(url=url, method=method, params=params,
                                body=body, headers=headers, **kwargs)

        if self._is_timeout(response):
            return response

        if not self._is_401(response):
            return response

        logger.warning("Получен 401 для uid=%s, пробуем восстановить доступ...", uid)

        with self._lock:
            user = self._get_user(uid)
            if self._is_token_valid(user):
                self._ensure_user_active(uid)
            new_token = self._refresh_token(uid)

        headers['Authorization'] = f'OAuth {new_token}'
        return make_request(url=url, method=method, params=params,
                            body=body, headers=headers, **kwargs)

    def get_user(self, user_id: str | int) -> Optional[UserData]:
        uid = self._parse_uid(user_id)
        return self._users.get(uid)

    def set_email(self, user_id: str | int, email: str) -> None:
        uid = self._parse_uid(user_id)
        with self._lock:
            self._get_user(uid).email = email

    def block_user(self, user_id: str | int) -> None:
        uid = self._parse_uid(user_id)
        with self._lock:
            self._disable_user(uid)
            user = self._users.get(uid)
            if user:
                user.token = None
                user.token_expires_at = None
            logger.info("Токен uid=%s инвалидирован после блокировки", uid)

    def invalidate_token(self, user_id: str | int) -> None:
        uid = self._parse_uid(user_id)
        with self._lock:
            user = self._users.get(uid)
            if user:
                user.token = None
                user.token_expires_at = None
            logger.info("Токен uid=%s инвалидирован", uid)

    def clear(self) -> None:
        with self._lock:
            self._users.clear()
            logger.info("Все данные пользователей очищены")

user_manager = UserManager()

def disk_get_meta(user_id: str | int, path: str = '/', perPage: int = 1000, offset: int = 0, fields: str = '', sort: str = 'path') -> Optional[Union[Dict, str, bytes]]:
    """Функция получения метаинформации о ресурсе"""

    url: str = 'https://cloud-api.yandex.net/v1/disk/resources'
    params: Dict[str, Any] = {
        'path': path,
        'limit': perPage,
        'offset': offset,
        'sort': sort
    }
    if fields:
        params['fields'] = fields
    return user_manager.make_user_request(user_id=user_id, url=url, method='GET', params=params)

def adm_get_auditlog(
        started_at: str = None,
        ended_at: str = datetime.isoformat(datetime.now()),
        types: str = None,
        count:int = 100,
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
        if value is not None
    }
    return make_request(url=url, method='GET', params=params, headers=headers)

def adm_get_users(limit: int = 1000, offset: int = 0, email: str = None) -> Optional[Union[Dict, str, bytes]]: 
    """Функция для получения списка сотрудников"""
    url = f"https://cloud-api.yandex.net/v1/directory/organizations/{ORGID}/users"
    headers = {'Authorization': f'OAuth {TOKEN}'}
    params: Dict[str, Any] = {
        'limit': limit,
        'offset': offset
    }
    if email:
        params['email'] = email
    return make_request(url=url, method='GET', params=params, headers=headers)

def read_csv(filename: str, column: str = None) -> list[int]:
    """
    Читает колонку из CSV файла и возвращает список uid (int).
    Если column не задан — ищет колонки в порядке приоритета: uid → id → email.
    Если найдены email — обменивает их на uid через adm_get_users.
    """
    PRIORITY_COLUMNS = ('uid', 'id', 'email')
    EMAIL_COLUMNS = ('email',)

    path = Path(filename)
    if not path.is_absolute():
        path = Path(__file__).parent / path
    path = path.resolve()

    if not path.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")

    # Определяем разделитель
    with path.open('r', encoding='utf-8-sig') as file:
        header_line = file.readline()

    delimiter = None
    for candidate in (';', ',', '\t'):
        if candidate in header_line:
            delimiter = candidate
            break

    if delimiter is None:
        raise ValueError(f"Не удалось определить разделитель в файле: {path}")

    with path.open('r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file, delimiter=delimiter)

        if reader.fieldnames is None:
            raise ValueError(f"Файл пустой или не содержит заголовков: {path}")

        fieldnames = [name.strip() for name in reader.fieldnames]

        # Определяем целевую колонку
        if column is not None:
            if column not in fieldnames:
                raise ValueError(
                    f"Колонка '{column}' не найдена в файле. "
                    f"Доступные колонки: {fieldnames}"
                )
            target_column = column
        else:
            target_column = next(
                (col for col in PRIORITY_COLUMNS if col in fieldnames),
                None
            )
            if target_column is None:
                raise ValueError(
                    f"Ни одна из колонок {PRIORITY_COLUMNS} не найдена в файле. "
                    f"Доступные колонки: {fieldnames}"
                )

        logger.info("Читаем колонку '%s' из %s", target_column, path.name)

        values = [
            row[target_column].strip()
            for row in reader
            if row.get(target_column) and row[target_column].strip()
        ]

    # Если прочитали email — обмениваем на uid
    if target_column in EMAIL_COLUMNS:
        return _exchange_emails_to_uids(values)

    return _parse_uids(values)

def _parse_uids(values: list[str]) -> list[int]:
    """Приводит список строк к list[int], пропускает невалидные."""
    result = []
    for value in values:
        try:
            result.append(int(value))
        except ValueError:
            logger.warning("Пропускаем невалидный uid: %r", value)
    return result

def _exchange_emails_to_uids(emails: list[str]) -> list[int]:
    """Обменивает список email на список uid через adm_get_users."""
    uids: list[int] = []
    total = len(emails)

    for i, email in enumerate(emails, 1):
        logger.info("[%s/%s] Получаем uid для %s...", i, total, email)
        uid = _get_uid_by_email(email)

        if uid is not None:
            uids.append(uid)
        else:
            logger.warning("uid для %s не найден, пропускаем", email)

    logger.info("Обмен завершён: %s/%s email успешно обменяны на uid", len(uids), total)
    return uids

def _get_uid_by_email(email: str) -> Optional[int]:
    """Запрашивает uid пользователя по email через adm_get_users."""

    response = adm_get_users(email=email)

    if isinstance(response, Exception):
        logger.error("Ошибка запроса для %s: %s", email, response)
        return None

    if not isinstance(response, dict):
        logger.error("Неожиданный ответ для %s: %s", email, response)
        return None

    users = response.get('users', [])

    if not users:
        logger.warning("Пользователь с email %s не найден", email)
        return None

    uid = users[0].get('id')

    if uid is None:
        logger.warning("В ответе отсутствует поле 'id' для %s", email)
        return None

    return int(uid)

def collect_all_disk_files(
        user_id: str | int,
        perPage: int = 1000,
        fields: str = ''
) -> list[Dict]:
    """Собирает полный список файлов пользователя с Яндекс Диска через рекурсивный обход папок."""
    shared_folders: list[Dict] = []
    logger.info("[uid=%s] Начинаем сбор папок...", user_id)
    _collect_recursive(user_id, path='/', perPage=perPage, fields=fields, shared_folders=shared_folders)
    logger.info("[uid=%s] Рекурсивный обход: найдено %s расшаренных папок", user_id, len(shared_folders))
    return shared_folders

def _collect_recursive(user_id: str | int, path: str, perPage: int, fields: str, shared_folders: list, _depth: int = 0) -> None:
    """Рекурсивно обходит папки через disk_get_meta и собирает расшаренные папки"""

    if MAX_DEPTH is not None and _depth >= MAX_DEPTH:
        return

    offset = 0

    while True:
        response = disk_get_meta(
            user_id=user_id,
            path=path,
            perPage=perPage,
            offset=offset,
            fields=fields
        )

        if isinstance(response, Exception) or not isinstance(response, dict):
            break

        embedded = response.get('_embedded', {})
        items: list[Dict] = embedded.get('items', [])

        for item in items:
            if item.get('type') != 'dir':
                continue

            share = item.get('share')
            if share is not None:
                if share.get('is_owned') is True:
                    shared_folders.append(item)
                continue

            _collect_recursive(
                user_id=user_id,
                path=item.get('path', ''),
                perPage=perPage,
                fields=fields,
                shared_folders=shared_folders,
                _depth=_depth + 1
            )

        if len(items) < perPage:
            break

        offset += perPage


def _auditlog_worker(worker_id: int, weeks_queue: queue.Queue, items_lock: threading.Lock, all_items: list) -> None:
    """
    Поток берёт неделю из очереди, скачивает все её страницы.
    Когда iteration_key пропадает — берёт следующую неделю из очереди.
    Завершается когда очередь пуста.
    """
    while True:
        try:
            week_id, started_at, ended_at = weeks_queue.get_nowait()
        except queue.Empty:
            break

        logger.info("Аудит-лог [поток %s]: начинаем неделю %s (%s — %s)", worker_id, week_id, started_at, ended_at)
        iteration_key: Optional[str] = None
        page = 1

        while True:
            response = adm_get_auditlog(
                started_at=started_at,
                ended_at=ended_at,
                types=ACTIONS,
                iteration_key=iteration_key
            )
            if isinstance(response, Exception) or not isinstance(response, dict):
                logger.error("Аудит-лог [поток %s, неделя %s, страница %s]: ошибка %s",
                             worker_id, week_id, page, response)
                break
            page_items = response.get('items', [])
            logger.info("Аудит-лог [поток %s, неделя %s, страница %s]: %s событий",
                        worker_id, week_id, page, len(page_items))
            with items_lock:
                all_items.extend(page_items)
            iteration_key = response.get('iteration_key')
            if not iteration_key:
                logger.info("Аудит-лог [поток %s]: неделя %s завершена", worker_id, week_id)
                break
            page += 1

        weeks_queue.task_done()


def save_raw_auditlog_to_csv(items: list[Dict]) -> None:
    """Сохраняет сырые события аудит-лога в отдельный CSV файл."""
    path = Path(__file__).parent / 'auditlog_raw.csv'
    fieldnames = ['occurred_at', 'type', 'user_uid', 'owner_uid', 'tgt_rawaddress',
                  'resource_name', 'resource_type', 'status', 'uid', 'user_login', 'user_name']

    with path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';', extrasaction='ignore')
        writer.writeheader()
        for item in items:
            event = item.get('event', {})
            meta = event.get('meta', {})
            tgt = meta.get('tgt_rawaddress') or meta.get('resource_name', '')
            writer.writerow({
                'occurred_at': event.get('occurred_at', ''),
                'type': event.get('type', ''),
                'user_uid': event.get('uid', ''),
                'owner_uid': meta.get('owner_uid', ''),
                'tgt_rawaddress': tgt,
                'resource_name': meta.get('resource_name', ''),
                'resource_type': meta.get('resource_type', ''),
                'status': event.get('status', ''),
                'uid': event.get('uid', ''),
                'user_login': item.get('user_login', ''),
                'user_name': item.get('user_name', ''),
            })

    logger.info("Сырой аудит-лог сохранён в %s (%s событий)", path, len(items))


def load_auditlog_from_csv(filename: str) -> list[Dict]:
    """
    Читает аудит-лог из CSV-файла (в формате save_raw_auditlog_to_csv) и возвращает
    список словарей с теми же ключами, что используются в process_chunk:
    item['event']['uid'], item['event']['meta']['owner_uid'], item['event']['meta']['tgt_rawaddress'],
    item['event']['meta']['resource_name'].
    """
    path = Path(filename)
    if not path.is_absolute():
        path = Path(__file__).parent / path
    path = path.resolve()

    if not path.exists():
        raise FileNotFoundError(f"Файл аудит-лога не найден: {path}")

    items: list[Dict] = []
    with path.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f, delimiter=';')
        for row in reader:
            items.append({
                'event': {
                    'uid': row.get('uid', '') or row.get('user_uid', ''),
                    'occurred_at': row.get('occurred_at', ''),
                    'type': row.get('type', ''),
                    'status': row.get('status', ''),
                    'meta': {
                        'owner_uid': row.get('owner_uid', ''),
                        'tgt_rawaddress': row.get('tgt_rawaddress', ''),
                        'resource_name': row.get('resource_name', ''),
                        'resource_type': row.get('resource_type', ''),
                    }
                },
                'user_login': row.get('user_login', ''),
                'user_name': row.get('user_name', ''),
            })

    logger.info("Аудит-лог загружен из %s (%s событий)", path, len(items))
    return items


def _normalize_path(path: str) -> str:
    """
    Приводит путь к единому виду для сравнения.
    disk:/Папка  ->  /Папка
    1130000071009626:/disk/Папка  ->  /Папка
    """
    if path.startswith('disk:'):
        return path[len('disk:'):]
    idx = path.find(':/disk')
    if idx != -1:
        return path[idx + 1 + len('disk'):]
    return path


def collect_full_auditlog(all_folders: list[Dict], auditlog_file: str = '') -> None:
    """
    Получение ауди-лога. Если auditlog_file указан — читает аудит-лог из CSV. Иначе скачивает аудит-лог организации постранично.
    """
    owner_uids: set[int] = {row['uid'] for row in all_folders}

    all_items: list[Dict] = []

    if auditlog_file:
        all_items = load_auditlog_from_csv(auditlog_file)
    else:
        AUDITLOG_WORKERS = 5
        WEEK_DAYS = 7
        TOTAL_WEEKS = 27

        now = datetime.now()
        weeks_queue: queue.Queue = queue.Queue()
        for i in range(TOTAL_WEEKS):
            end_dt = now - timedelta(days=i * WEEK_DAYS)
            start_dt = now - timedelta(days=(i + 1) * WEEK_DAYS)
            weeks_queue.put((i + 1, start_dt.isoformat(), end_dt.isoformat()))

        logger.info("Запрашиваем аудит-лог организации (%s папок, %s потоков, %s недель)...",
                    len(all_folders), AUDITLOG_WORKERS, TOTAL_WEEKS)

        items_lock = threading.Lock()

        with ThreadPoolExecutor(max_workers=AUDITLOG_WORKERS) as executor:
            futures = [
                executor.submit(_auditlog_worker, i, weeks_queue, items_lock, all_items)
                for i in range(AUDITLOG_WORKERS)
            ]
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error("Ошибка в потоке аудит-лога: %s", e)

        logger.info("Аудит-лог скачан полностью: %s событий", len(all_items))

        save_raw_auditlog_to_csv(all_items)

    folder_index: Dict[tuple, set] = {
        (row['uid'], _normalize_path(row['folder'])): set() for row in all_folders
    }

    owner_paths: Dict[int, list[str]] = {}
    for row in all_folders:
        owner_paths.setdefault(row['uid'], []).append(_normalize_path(row['folder']))

    def process_chunk(chunk: list[Dict]) -> Dict[tuple, set]:
        local_index: Dict[tuple, set] = {}
        for item in chunk:
            event = item.get('event', {})
            meta = event.get('meta', {})

            raw_owner = meta.get('owner_uid')
            raw_user = meta.get('uid')

            if raw_owner is None or raw_user is None:
                continue

            owner = int(raw_owner)
            if owner not in owner_uids:
                continue

            user = int(raw_user)
            if user == owner:
                continue

            tgt_raw = meta.get('tgt_rawaddress')
            if tgt_raw:
                key = (owner, _normalize_path(tgt_raw))
                if key in folder_index:
                    local_index.setdefault(key, set()).add(user)
            else:
                resource_name = meta.get('resource_name', '')
                if resource_name:
                    for norm_path in owner_paths.get(owner, []):
                        if resource_name in norm_path:
                            local_index.setdefault((owner, norm_path), set()).add(user)
        return local_index

    chunk_size = max(1, len(all_items) // MAX_WORKERS)
    chunks = [all_items[i:i + chunk_size] for i in range(0, len(all_items), chunk_size)]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_chunk, chunk) for chunk in chunks]
        for future in as_completed(futures):
            local_index = future.result()
            for key, users in local_index.items():
                folder_index[key].update(users)

    for row in all_folders:
        row['users'] = list(folder_index[(row['uid'], _normalize_path(row['folder']))])

    total_users = sum(len(v) for v in folder_index.values())
    logger.info("Аудит-лог обработан, найдено %s совпадений по папкам", total_users)

def save_to_csv(result: list[Dict], filename: str = RESULTS_FILE) -> None:
    """Сохраняет result в CSV. Каждая строка — отдельная папка, users — через запятую."""
    path = Path(filename)
    if not path.is_absolute():
        path = Path(__file__).parent / path

    with path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['uid', 'folder', 'users'], delimiter=';')
        writer.writeheader()
        for row in result:
            writer.writerow({
                'uid': row['uid'],
                'folder': row['folder'],
                'users': ','.join(str(u) for u in row['users'])
            })

    logger.info("Результат сохранён в %s (%s строк)", path, len(result))


def _collect_shared_folders(user: int) -> list[Dict]:
    """Собирает расшаренные папки одного пользователя."""
    shared_folders = collect_all_disk_files(user, fields=FIELDS)
    return [{'uid': user, 'folder': f.get('path', ''), 'users': []} for f in shared_folders]


def main():
    users = read_csv(filename=USERS_FILENAME, column=None)
    all_folders: list[Dict] = []
    lock = threading.Lock()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(_collect_shared_folders, user): user for user in users}
        for future in as_completed(futures):
            user = futures[future]
            try:
                rows = future.result()
                with lock:
                    all_folders.extend(rows)
            except Exception as e:
                logger.error("[uid=%s] Ошибка сбора папок: %s", user, e)

    logger.info("Обход диска завершён. Всего расшаренных папок: %s", len(all_folders))

    collect_full_auditlog(all_folders, auditlog_file=AUDITLOG_FILE)

    save_to_csv(all_folders)


if __name__ == '__main__':
    main()