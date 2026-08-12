import argparse
from dotenv import load_dotenv
import httpx
import logging
import logging.handlers as handlers
import os.path
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import re
import csv
from dataclasses import dataclass
from textwrap import dedent
from http import HTTPStatus
from collections import namedtuple
from email.parser import BytesHeaderParser
from email.header import decode_header
from email.utils import parsedate_to_datetime

import asyncio
import concurrent.futures
import aioimaplib
from typing import Optional
import time
import json
import base64
import traceback

DEFAULT_IMAP_SERVER = "imap.yandex.ru"
DEFAULT_IMAP_PORT = 993
DEFAULT_360_API_URL = "https://api360.yandex.net"
DEFAULT_OAUTH_API_URL = "https://oauth.yandex.ru/token"
LOG_FILE = "del_message_v3.log"
DEFAULT_DAYS_DIF = 1
FILTERED_EVENTS = ["message_receive"]
FILTERED_MAILBOXES = []
ALL_USERS_REFRESH_IN_MINUTES = 10000 # обновление раз в 3 часа для минимизации времени работы скрипта на больших организациях
ALL_DELEGATE_MAILBOXES_REFRESH_IN_MINUTES = 5
USERS_PER_PAGE_FROM_API = 100
DELEGATE_MAILBOXES_PER_PAGE_FROM_API = 100
MAX_RETRIES = 3
RETRIES_DELAY_SEC = 2
MAX_PARALLEL_THREADS = 10
MAX_PARALLEL_THREADS_SHARED = 10


# Количество страниц для запроса логов последовательно в одном цикле последовательного обращения к API, после чего формируется новый набор стартовой и конечных дат
OLD_LOG_MAX_PAGES = 10

# На сколько секунд сдвигается назад стартовыя дата запроса логов между последовательными обращениями к API (чтобы не потерять записи)
OVERLAPPED_SECONDS = 2
# !!! Don't modify MAIL_LOGS_MAX_RECORDS value !!!
MAIL_LOGS_MAX_RECORDS = 100

ID_HEADER_SET = {'Content-Type', 'From', 'To', 'Cc', 'Bcc', 'Date', 'Subject',
                'Message-ID', 'In-Reply-To', 'References', 'X-Yandex-Fwd', 'Return-Path', 'X-Yandex-Spam', "X-Mailer"}
FETCH_MESSAGE_DATA_UID = re.compile(rb'.*UID (?P<uid>\d+).*')
FETCH_MESSAGE_DATA_SEQNUM = re.compile(rb'(?P<seqnum>\d+) FETCH.*')
FETCH_MESSAGE_DATA_FLAGS  = re.compile(rb'.*FLAGS \((?P<flags>.*?)\).*')
MessageAttributes = namedtuple('MessageAttributes', 'uid flags sequence_number')

EXIT_CODE = 1

# Необходимые права доступа для работы скрипта
NEEDED_PERMISSIONS = [
    "directory:read_users",
    "ya360_admin:mail_write_shared_mailbox_inventory",
    "ya360_admin:mail_read_shared_mailbox_inventory",
    "ya360_security:audit_log_mail"
]

SERVICE_APP_PERMISSIONS = [
    "mail:imap_full",
]
# Логирование
logger = logging.getLogger("delete_messages")
logger.setLevel(logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s:\t%(message)s', datefmt='%H:%M:%S'))
#file_handler = handlers.TimedRotatingFileHandler(LOG_FILE, when='D', interval=1, backupCount=30, encoding='utf-8')
file_handler = handlers.RotatingFileHandler(LOG_FILE, maxBytes=10*1024 * 1024,  backupCount=5, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter('%(asctime)s.%(msecs)03d %(levelname)s:\t%(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Ограничение IMAP команд
RPS_LIMIT = 20
_last_call_imap = 0.0
IMAP_FETCH_RETRY_ATTEMPTS = 5


def rate_limit_imap_commands():
    """
    Ограничивает частоту IMAP-команд для соблюдения лимита RPS.
    
    Функция проверяет время с последнего вызова и при необходимости
    добавляет задержку для соблюдения ограничения RPS_LIMIT запросов в секунду.
    
    Returns:
        None
    """
    global _last_call_imap
    now = time.time()
    delta = now - _last_call_imap
    if delta < 1.0 / RPS_LIMIT:
        time.sleep((1.0 / RPS_LIMIT) - delta)
    _last_call_imap = time.time()

def arg_parser():
    """
    Создает парсер аргументов командной строки.
    
    Returns:
        argparse.ArgumentParser: Настроенный парсер с параметрами --id и --date
    """
    parser = argparse.ArgumentParser(
        description=dedent(
            """
            Script for downloading audit log records from Yandex 360.

            Define Environment variables or use .env file to set values of those variables:
            OAUTH_TOKEN_ARG - OAuth Token,
            ORGANIZATION_ID_ARG - Organization ID,
            APPLICATION_CLIENT_ID_ARG - WEB Application ClientID,
            APPLICATION_CLIENT_SECRET_ARG - WEB Application secret,
            DELEGATE_ALIAS - Delegate alias (login without domain, e.g., "i.petrov"),
            DELEGATE_DOMAIN - Organization domain (e.g., "example.ru"),
            DELEGATE_PASSWORD - Application password for delegate account

            For example:
            OAUTH_TOKEN_ARG = "AgAAgfAAAAD4beAkEsWrefhNeyN1TVYjGT1k",
            ORGANIZATION_ID_ARG = 123,
            DELEGATE_ALIAS = "i.petrov",
            DELEGATE_DOMAIN = "example.ru",
            DELEGATE_PASSWORD = "app_password_here"
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    def argument_range(value: str) -> int:
        try:
            if int(value) < 0 or int(value) > 90:
                raise argparse.ArgumentTypeError(
                    f"{value} is invalid. Valid values in range: [0, 90]"
                )
        except ValueError:
            raise argparse.ArgumentTypeError(f"'{value}' is not int value")
        return int(value)

    parser.add_argument(
        "--id", help="Message ID", type=str, required=False
    )

    parser.add_argument(
        "--date", help="Message date (DD-MM-YYYY)", type=str, required=False
    )
        
    # parser.add_argument(
    #     "--date",
    #     help="Message date",
    #     type=argument_range,
    #     required=False,
    # )
    return parser

def get_initials_config():
    """
    Инициализирует конфигурацию скрипта из аргументов командной строки и переменных окружения.
    
    Загружает настройки из .env файла и параметров командной строки,
    проверяет обязательные параметры и формирует объект настроек.
    
    Returns:
        SettingParams: Объект с настройками скрипта или завершает программу при ошибке
    """
    parsr = arg_parser()
    try:
        args = parsr.parse_args()
    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")

    try:
        settings = get_settings()
        if settings is None:
            logger.error("Required environment vars not provided.")
            sys.exit(EXIT_CODE)
    except ValueError:
        logger.error("The value of ORGANIZATION_ID_ARG must be an integer.")
        sys.exit(EXIT_CODE)
    except KeyError as key:
        logger.error(f"Required environment vars not provided: {key}")
        #parsr.print_usage()
        sys.exit(EXIT_CODE)

    input_params = {}

    input_params["days_diff"] = DEFAULT_DAYS_DIF
    input_params["message_id"] = ""
    input_params["message_date"] = ""
    input_params["events"] = FILTERED_EVENTS
    input_params["mailboxes"] = FILTERED_MAILBOXES
    input_params["is_all_mailboxes"] = False
    input_params["from_file"] = False
    input_params["messages_to_delete"] = []

    if args.id is not None: 
        input_params["message_id"] = args.id
    
    if args.date is not None:
        status, date = is_valid_date(args.date.strip(), min_years_diff=0, max_years_diff=20)
        if status:
            input_params["message_date"] = date.strftime("%d-%m-%Y")

    settings.search_param = input_params

    return settings

def FilterEvents(events: list) -> list:
    """
    Фильтрует события аудит-лога по типу события и почтовому ящику.
    
    Args:
        events: Список событий для фильтрации
        
    Returns:
        list: Отфильтрованный список событий, соответствующих FILTERED_EVENTS и FILTERED_MAILBOXES
    """
    filtered_events = []
    for event in events:
        if event["eventType"] in FILTERED_EVENTS and event["userLogin"] in FILTERED_MAILBOXES:
            filtered_events.append(event)
    return filtered_events

def get_all_api360_users(settings: "SettingParams", force = False, suppress_messages = True):
    """
    Получает список всех пользователей организации с кэшированием.
    
    Использует кэш для минимизации запросов к API. Кэш обновляется при:
    - Первом запросе
    - Параметре force=True
    - Истечении времени ALL_USERS_REFRESH_IN_MINUTES
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        force: Принудительное обновление кэша (по умолчанию False)
        suppress_messages: Подавление информационных сообщений (по умолчанию True)
        
    Returns:
        list: Список пользователей организации
    """
    if not force and not suppress_messages:
        logger.info("Получение всех пользователей организации из кэша...")

    if not settings.all_users or force or (datetime.now() - settings.all_users_get_timestamp).total_seconds() > ALL_USERS_REFRESH_IN_MINUTES * 60:
        #logger.info("Получение всех пользователей организации из API...")
        settings.all_users = get_all_api360_users_from_api(settings, suppress_messages=suppress_messages)
        settings.all_users_get_timestamp = datetime.now()
    return settings.all_users

def get_all_shared_mailboxes_cached(settings: "SettingParams", force = False, suppress_messages = True):
    """
    Получает список всех общих почтовых ящиков с использованием кэша.
    
    Кэш обновляется автоматически при следующих условиях:
    - Кэш пуст (первый запрос)
    - Параметр force=True
    - Прошло больше ALL_USERS_REFRESH_IN_MINUTES минут с последнего обновления
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        force: Принудительное обновление кэша (по умолчанию False)
        
    Returns:
        list: Список объектов с полями resourceId и count
    """
    if not force and not suppress_messages:
        logger.info("Получение всех общих почтовых ящиков из кэша...")

    if not settings.all_shared_mailboxes or force or (datetime.now() - settings.all_shared_mailboxes_get_timestamp).total_seconds() > ALL_USERS_REFRESH_IN_MINUTES * 60:
        settings.all_shared_mailboxes = get_all_shared_mailboxes_with_details(settings, suppress_messages=suppress_messages)
        settings.all_shared_mailboxes_get_timestamp = datetime.now()
    return settings.all_shared_mailboxes

def get_all_api360_users_from_api(settings: "SettingParams", suppress_messages = True):
    """
    Получает список всех пользователей организации напрямую из API Яндекс 360.
    
    Выполняет постраничный запрос к API для получения полного списка пользователей.
    Исключает роботов и служебные аккаунты из результата.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        suppress_messages: Подавление информационных сообщений (по умолчанию True)
        
    Returns:
        list: Список пользователей организации или пустой список при ошибке
    """
    if not suppress_messages:
        logger.info("Получение всех пользователей организации из API...")
    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{settings.organization_id}/users"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    has_errors = False
    users = []
    current_page = 1
    last_page = 1
    with httpx.Client(headers=headers) as client:
        while current_page <= last_page:
            params = {'page': current_page, 'perPage': USERS_PER_PAGE_FROM_API}
            try:
                retries = 1
                while True:
                    logger.debug(f"GET URL - {url}")
                    response = client.get(url, params=params)
                    logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
                    if response.status_code != HTTPStatus.OK.value:
                        logger.error(f"!!! ОШИБКА !!! при GET запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                        if retries < MAX_RETRIES:
                            logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                            time.sleep(RETRIES_DELAY_SEC * retries)
                            retries += 1
                        else:
                            has_errors = True
                            break
                    else:
                        for user in response.json()['users']:
                            if not user.get('isRobot') and int(user["id"]) >= 1130000000000000:
                                users.append(user)
                        logger.debug(f"Загружено {len(response.json()['users'])} пользователей. Текущая страница - {current_page} (всего {last_page} страниц).")
                        current_page += 1
                        last_page = response.json()['pages']
                        break

            except httpx.HTTPError as e:
                logger.error(f"!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
                has_errors = True
                break

            if has_errors:
                break

    if has_errors:
        logger.warning("Есть ошибки при GET запросах. Возвращается пустой список пользователей.")
        return []
    
    return users

def get_delegated_mailboxes(settings: "SettingParams", page: int = 1, per_page: int = 10, thread_id: int = 0):
    """
    Получает список делегированных почтовых ящиков в организации.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        page: Номер страницы ответа (по умолчанию 1)
        per_page: Количество записей на одной странице ответа (по умолчанию 10)
        thread_id: Идентификатор потока для логирования
        
    Returns:
        dict: Словарь с полями:
            - resources: список объектов с resourceId и count
            - page: номер страницы
            - perPage: количество записей на странице
            - total: общее количество записей
        None: в случае ошибки
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.info(f"{thread_prefix}Получение списка делегированных ящиков (страница {page}, записей на странице: {per_page})...")
    url = f"{DEFAULT_360_API_URL}/admin/v1/org/{settings.organization_id}/mailboxes/delegated"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    params = {'page': page, 'perPage': per_page}
    
    try:
        retries = 1
        with httpx.Client(headers=headers) as client:
            while True:
                logger.debug(f"{thread_prefix}GET URL - {url}")
                response = client.get(url, params=params)
                logger.debug(f"{thread_prefix}x-request-id: {response.headers.get('x-request-id','')}")
                
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"{thread_prefix}!!! ОШИБКА !!! при GET запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"{thread_prefix}Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error(f"{thread_prefix}Превышено максимальное количество попыток. Возвращается None.")
                        return None
                else:
                    result = response.json()
                    logger.info(f"{thread_prefix}Успешно получено {len(result.get('resources', []))} делегированных ящиков. " 
                               f"Страница {result.get('page', page)} из {result.get('total', 0) // result.get('perPage', per_page) + 1}")
                    return result
                
    except httpx.HTTPError as e:
        logger.error(f"{thread_prefix}!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return None

def get_all_delegated_mailboxes(settings: "SettingParams", force = False, thread_id: int = 0):
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    if not force:
        logger.info(f"{thread_prefix}Получение всех делегированных почтовых ящиков из кэша...")
    if not settings.all_delegate_mailboxes or force or (datetime.now() - settings.all_delegate_mailboxes_get_timestamp).total_seconds() > ALL_DELEGATE_MAILBOXES_REFRESH_IN_MINUTES * 60:
        settings.all_delegate_mailboxes = get_all_delegated_mailboxes_from_api(settings, per_page=DELEGATE_MAILBOXES_PER_PAGE_FROM_API, thread_id=thread_id)
        settings.all_delegate_mailboxes_get_timestamp = datetime.now()
    return settings.all_delegate_mailboxes

def get_all_delegated_mailboxes_from_api(settings: "SettingParams", per_page: int = DELEGATE_MAILBOXES_PER_PAGE_FROM_API, thread_id: int = 0):
    """
    Получает полный список всех делегированных почтовых ящиков в организации (все страницы).
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        per_page: Количество записей на одной странице ответа (по умолчанию 100)
        thread_id: Идентификатор потока для логирования
        
    Returns:
        list: Список объектов с полями resourceId и count
        None: в случае ошибки
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.info(f"{thread_prefix}Получение полного списка всех делегированных ящиков...")
    all_resources = []
    current_page = 1
    
    while True:
        result = get_delegated_mailboxes(settings, page=current_page, per_page=per_page, thread_id=thread_id)
        
        if result is None:
            logger.error(f"{thread_prefix}Ошибка при получении делегированных ящиков. Возвращается пустой список.")
            return []
        
        resources = result.get('resources', [])
        all_resources.extend(resources)
        
        total = result.get('total', 0)
        
        logger.debug(f"{thread_prefix}Загружено {len(resources)} делегированных ящиков. Всего получено: {len(all_resources)} из {total}")
        
        # Проверяем, есть ли еще страницы
        if len(all_resources) >= total or len(resources) == 0:
            break
            
        current_page += 1
    
    logger.info(f"{thread_prefix}Всего получено {len(all_resources)} делегированных ящиков")
    return all_resources

def enable_mailbox_delegation(settings: "SettingParams", resource_id: str, thread_id: int = 0):
    """
    Включает возможность делегирования для почтового ящика.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        resource_id: Идентификатор почтового ящика (совпадает с идентификатором сотрудника-владельца)
        thread_id: Идентификатор потока для логирования
        
    Returns:
        dict: Словарь с полем resourceId в случае успеха
        None: в случае ошибки
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.info(f"{thread_prefix}Включение делегирования для ящика с resourceId={resource_id}...")
    url = f"{DEFAULT_360_API_URL}/admin/v1/org/{settings.organization_id}/mailboxes/delegated"
    headers = {
        "Authorization": f"OAuth {settings.oauth_token}",
        "Content-Type": "application/json"
    }
    data = {
        "resourceId": str(resource_id)
    }
    
    try:
        retries = 1
        with httpx.Client(headers=headers) as client:
            while True:
                logger.debug(f"{thread_prefix}PUT URL - {url}")
                logger.debug(f"{thread_prefix}Request body: {data}")
                response = client.put(url, json=data)
                logger.debug(f"{thread_prefix}x-request-id: {response.headers.get('x-request-id','')}")
                
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"{thread_prefix}!!! ОШИБКА !!! при PUT запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"{thread_prefix}Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error(f"{thread_prefix}Превышено максимальное количество попыток. Возвращается None.")
                        return None
                else:
                    result = response.json()
                    logger.info(f"{thread_prefix}Успешно включено делегирование для ящика с resourceId={result.get('resourceId', resource_id)}")
                    return result
                
    except httpx.HTTPError as e:
        logger.error(f"{thread_prefix}!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return None

def disable_mailbox_delegation(settings: "SettingParams", resource_id: str, thread_id: int = 0):
    """
    Выключает возможность делегирования для почтового ящика.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        resource_id: Идентификатор почтового ящика (совпадает с идентификатором сотрудника-владельца)
        thread_id: Идентификатор потока для логирования
        
    Returns:
        dict: Пустой словарь в случае успеха (API возвращает пустое тело)
        None: в случае ошибки
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.debug(f"{thread_prefix}Выключение делегирования для ящика с resourceId={resource_id}...")
    url = f"{DEFAULT_360_API_URL}/admin/v1/org/{settings.organization_id}/mailboxes/delegated/{resource_id}"
    headers = {
        "Authorization": f"OAuth {settings.oauth_token}"
    }
    
    try:
        retries = 1
        with httpx.Client(headers=headers) as client:
            while True:
                logger.debug(f"{thread_prefix}DELETE URL - {url}")
                response = client.delete(url)
                logger.debug(f"{thread_prefix}x-request-id: {response.headers.get('x-request-id','')}")
                
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"{thread_prefix}!!! ОШИБКА !!! при DELETE запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"{thread_prefix}Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error(f"{thread_prefix}Превышено максимальное количество попыток. Ошибка выключения делегирования для ящика с resourceId={resource_id}")
                        return False
                else:
                    logger.debug(f"{thread_prefix}Успешно выключено делегирование для ящика с resourceId={resource_id}")
                    # API возвращает пустое тело при успешном выполнении
                    return True
                
    except httpx.HTTPError as e:
        logger.error(f"{thread_prefix}!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return False

def get_shared_mailboxes(settings: "SettingParams", page: int = 1, per_page: int = 10, thread_id: int = 0):
    """
    Получает список общих почтовых ящиков в организации.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        page: Номер страницы ответа (по умолчанию 1)
        per_page: Количество записей на одной странице ответа (по умолчанию 10)
        thread_id: Идентификатор потока для логирования
        
    Returns:
        dict: Словарь с полями:
            - resources: список объектов с resourceId и count
            - page: номер страницы
            - perPage: количество записей на странице
            - total: общее количество записей
        None: в случае ошибки
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.info(f"{thread_prefix}Получение списка общих ящиков (страница {page}, записей на странице: {per_page})...")
    url = f"{DEFAULT_360_API_URL}/admin/v1/org/{settings.organization_id}/mailboxes/shared"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    params = {'page': page, 'perPage': per_page}
    
    try:
        retries = 1
        with httpx.Client(headers=headers) as client:
            while True:
                logger.debug(f"{thread_prefix}GET URL - {url}")
                response = client.get(url, params=params)
                logger.debug(f"{thread_prefix}x-request-id: {response.headers.get('x-request-id','')}")
                
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"{thread_prefix}!!! ОШИБКА !!! при GET запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"{thread_prefix}Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error(f"{thread_prefix}Превышено максимальное количество попыток. Возвращается None.")
                        return None
                else:
                    result = response.json()
                    logger.info(f"{thread_prefix}Успешно получено {len(result.get('resources', []))} общих ящиков. " 
                               f"Страница {result.get('page', page)} из {result.get('total', 0) // result.get('perPage', per_page) + 1}")
                    return result
                
    except httpx.HTTPError as e:
        logger.error(f"{thread_prefix}!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return None

def get_all_shared_mailboxes(settings: "SettingParams", per_page: int = 100, thread_id: int = 0, suppress_messages = True):
    """
    Получает полный список всех общих почтовых ящиков в организации (все страницы).
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        per_page: Количество записей на одной странице ответа (по умолчанию 100)
        thread_id: Идентификатор потока для логирования
        
    Returns:
        list: Список объектов с полями resourceId и count
        None: в случае ошибки
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    if not suppress_messages:
        logger.info(f"{thread_prefix}Получение полного списка всех общих ящиков...")
    all_resources = []
    current_page = 1
    
    while True:
        result = get_shared_mailboxes(settings, page=current_page, per_page=per_page, thread_id=thread_id)
        
        if result is None:
            logger.error(f"{thread_prefix}Ошибка при получении общих ящиков. Возвращается пустой список.")
            return []
        
        resources = result.get('resources', [])
        all_resources.extend(resources)
        
        total = result.get('total', 0)
        
        logger.debug(f"{thread_prefix}Загружено {len(resources)} общих ящиков. Всего получено: {len(all_resources)} из {total}")
        
        # Проверяем, есть ли еще страницы
        if len(all_resources) >= total or len(resources) == 0:
            break
            
        current_page += 1
    
    logger.info(f"{thread_prefix}Всего получено {len(all_resources)} общих ящиков")
    return all_resources

def get_shared_mailbox_info(settings: "SettingParams", resource_id: str, thread_id: int = 0, suppress_messages = True):
    """
    Получает детальную информацию об общем почтовом ящике.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        resource_id: Идентификатор общего почтового ящика
        thread_id: Идентификатор потока для логирования
        
    Returns:
        dict: Словарь с информацией об общем ящике:
            - resourceId: идентификатор ящика
            - email: адрес электронной почты
            - name: название ящика
            - description: описание ящика (опционально)
            - count: количество сотрудников с доступом к ящику
        None: в случае ошибки
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.debug(f"{thread_prefix}Получение информации об общем ящике с resourceId={resource_id}...")
    url = f"{DEFAULT_360_API_URL}/admin/v1/org/{settings.organization_id}/mailboxes/shared/{resource_id}"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    
    try:
        retries = 1
        with httpx.Client(headers=headers) as client:
            while True:
                logger.debug(f"{thread_prefix}GET URL - {url}")
                response = client.get(url)
                logger.debug(f"{thread_prefix}x-request-id: {response.headers.get('x-request-id','')}")
                
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"{thread_prefix}!!! ОШИБКА !!! при GET запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"{thread_prefix}Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error(f"{thread_prefix}Превышено максимальное количество попыток. Возвращается None.")
                        return None
                else:
                    result = response.json()
                    logger.debug(f"{thread_prefix}Успешно получена информация об общем ящике: email={result.get('email', 'N/A')}, name={result.get('name', 'N/A')}")
                    return result
                
    except httpx.HTTPError as e:
        logger.error(f"{thread_prefix}!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return None

def get_all_shared_mailboxes_with_details(settings: "SettingParams", per_page: int = 100, thread_id: int = 0, suppress_messages = True):
    """
    Получает полный список всех общих почтовых ящиков в организации с детальной информацией.
    Сначала получает список resourceId через ListShared, затем для каждого получает детали через GetShared.
    Использует параллельные запросы для ускорения получения детальной информации.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        per_page: Количество записей на одной странице ответа (по умолчанию 100)
        thread_id: Идентификатор потока для логирования
        
    Returns:
        list: Список словарей с полной информацией об общих ящиках:
            - resourceId: идентификатор ящика
            - email: адрес электронной почты
            - name: название ящика
            - description: описание ящика (опционально)
            - count: количество сотрудников с доступом к ящику
        []: пустой список в случае ошибки
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    if not suppress_messages:
        logger.info(f"{thread_prefix}Получение полного списка общих ящиков с детальной информацией...")
    
    # Шаг 1: Получаем список resourceId
    shared_mailboxes_list = get_all_shared_mailboxes(settings, per_page=per_page, thread_id=thread_id, suppress_messages=suppress_messages)
    
    if not shared_mailboxes_list:
        logger.warning(f"{thread_prefix}Не найдено общих ящиков или произошла ошибка при получении списка.")
        return []
    
    if not suppress_messages:
        logger.info(f"{thread_prefix}Получено {len(shared_mailboxes_list)} общих ящиков. Запрашиваем детальную информацию параллельно (max {MAX_PARALLEL_THREADS_SHARED} потоков)...")
    
    # Шаг 2: Для каждого resourceId получаем детальную информацию параллельно
    detailed_mailboxes = []
    
    # Функция-обертка для получения информации об одном ящике
    def fetch_mailbox_info(mailbox_data):
        mailbox, index = mailbox_data
        resource_id = mailbox.get('resourceId')
        if not resource_id:
            logger.warning(f"{thread_prefix}Пропущен ящик без resourceId: {mailbox}")
            return None
        
        logger.debug(f"{thread_prefix}Получение информации для ящика {index}/{len(shared_mailboxes_list)}, resourceId={resource_id}")
        
        mailbox_info = get_shared_mailbox_info(settings, resource_id=resource_id, thread_id=thread_id, suppress_messages=suppress_messages)
        
        if mailbox_info:
            # Добавляем count из первого запроса, если его нет в детальной информации
            if 'count' not in mailbox_info and 'count' in mailbox:
                mailbox_info['count'] = mailbox['count']
            return mailbox_info
        else:
            logger.warning(f"{thread_prefix}Не удалось получить информацию для ящика с resourceId={resource_id}")
            return None
    
    # Используем ThreadPoolExecutor для параллельных запросов
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_PARALLEL_THREADS_SHARED) as executor:
        # Подготавливаем данные: список кортежей (mailbox, index)
        mailbox_data_list = [(mailbox, i) for i, mailbox in enumerate(shared_mailboxes_list, 1)]
        
        # Выполняем параллельные запросы
        results = executor.map(fetch_mailbox_info, mailbox_data_list)
        
        # Собираем результаты, исключая None
        detailed_mailboxes = [result for result in results if result is not None]
    
    logger.info(f"{thread_prefix}Успешно получена детальная информация для {len(detailed_mailboxes)} из {len(shared_mailboxes_list)} общих ящиков")
    return detailed_mailboxes

def get_mailbox_actors(settings: "SettingParams", resource_id: str, thread_id: int = 0):
    """
    Получает список сотрудников, имеющих доступ к делегированному почтовому ящику.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        resource_id: Идентификатор почтового ящика (совпадает с идентификатором сотрудника-владельца)
        thread_id: Идентификатор потока (для логирования)
        
    Returns:
        list: Список объектов с полями:
            - actorId: идентификатор сотрудника (string)
            - roles: список ролей сотрудника (list of strings)
        None: в случае ошибки
    """
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    logger.info(f"{thread_prefix}Получение списка сотрудников с доступом к ящику resourceId={resource_id}...")
    url = f"{DEFAULT_360_API_URL}/admin/v1/org/{settings.organization_id}/mailboxes/actors/{resource_id}"
    headers = {
        "Authorization": f"OAuth {settings.oauth_token}"
    }
    
    try:
        retries = 1
        with httpx.Client(headers=headers) as client:
            while True:
                logger.debug(f"{thread_prefix}GET URL - {url}")
                response = client.get(url)
                logger.debug(f"{thread_prefix}x-request-id: {response.headers.get('x-request-id','')}")
                
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"{thread_prefix}!!! ОШИБКА !!! при GET запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"{thread_prefix}Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error(f"{thread_prefix}Превышено максимальное количество попыток. Возвращается None.")
                        return None
                else:
                    result = response.json()
                    actors = result.get('actors', [])
                    logger.info(f"{thread_prefix}Успешно получено {len(actors)} сотрудников с доступом к ящику resourceId={resource_id}")
                    for actor in actors:
                        logger.debug(f"{thread_prefix}  - actorId: {actor.get('actorId', 'N/A')}, roles: {', '.join(actor.get('roles', []))}")
                    return actors
                
    except httpx.HTTPError as e:
        logger.error(f"{thread_prefix}!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return None

def set_mailbox_permissions(settings: "SettingParams", resource_id: str, actor_id: str, roles: list, notify: str = "all", thread_id: int = 0):
    """
    Устанавливает права доступа сотрудника к делегированному или общему почтовому ящику.
    Операция асинхронная - возвращает taskId для проверки статуса выполнения задачи.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        resource_id: Идентификатор почтового ящика, права доступа к которому необходимо предоставить или изменить
        actor_id: Идентификатор сотрудника, для которого настраивается доступ
        roles: Список ролей для назначения сотруднику. Возможные значения:
            - shared_mailbox_owner: полные права на ящик
            - shared_mailbox_reader: просмотр ящика в веб-интерфейсе
            - shared_mailbox_editor: редактирование ящика в веб-интерфейсе
            - shared_mailbox_admin: управление ящиком в веб-интерфейсе
            - shared_mailbox_imap_admin: управление ящиком в IMAP-клиенте
            - shared_mailbox_sender: отправка писем
            - shared_mailbox_half_sender: ограниченная отправка писем (только в почтовых клиентах в режиме "От имени")
        notify: Кому отправить уведомление об изменении прав:
            - "all": владельцу ящика и сотруднику (по умолчанию)
            - "delegates": только сотруднику
            - "none": никому
        thread_id: Идентификатор потока для логирования
        
    Returns:
        str: taskId для проверки статуса выполнения задачи в случае успеха
        None: в случае ошибки
        
    Example:
        # Предоставление полных прав на ящик
        task_id = set_mailbox_permissions(
            settings, 
            resource_id="1234567890", 
            actor_id="9876543210",
            roles=["shared_mailbox_owner"],
            notify="all"
        )
        
        # Предоставление прав на чтение и отправку писем
        task_id = set_mailbox_permissions(
            settings,
            resource_id="1234567890",
            actor_id="9876543210", 
            roles=["shared_mailbox_reader", "shared_mailbox_sender"],
            notify="delegates"
        )
        
        # Проверка статуса задачи
        if task_id:
            status = check_mailbox_task_status(settings, task_id)
            print(f"Статус задачи: {status.get('status')}")
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.info(f"{thread_prefix}Установка прав для сотрудника actorId={actor_id} на ящик resourceId={resource_id}...")
    logger.debug(f"{thread_prefix}Роли: {', '.join(roles)}, уведомление: {notify}")
    
    url = f"{DEFAULT_360_API_URL}/admin/v1/org/{settings.organization_id}/mailboxes/set/{resource_id}"
    headers = {
        "Authorization": f"OAuth {settings.oauth_token}",
        "Content-Type": "application/json"
    }
    params = {
        "actorId": str(actor_id),
        "notify": notify
    }
    data = {
        "roles": roles
    }
    
    try:
        retries = 1
        with httpx.Client(headers=headers) as client:
            while True:
                logger.debug(f"{thread_prefix}POST URL - {url}")
                logger.debug(f"{thread_prefix}Query params: {params}")
                logger.debug(f"{thread_prefix}Request body: {data}")
                response = client.post(url, params=params, json=data)
                logger.debug(f"{thread_prefix}x-request-id: {response.headers.get('x-request-id','')}")
                
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"{thread_prefix}!!! ОШИБКА !!! при POST запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"{thread_prefix}Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error(f"{thread_prefix}Превышено максимальное количество попыток. Возвращается None.")
                        return None
                else:
                    result = response.json()
                    task_id = result.get('taskId')
                    logger.info(f"{thread_prefix}Успешно инициирована задача на установку прав. taskId={task_id}")
                    logger.debug(f"{thread_prefix}Используйте taskId {task_id} для проверки статуса выполнения задачи")
                    return task_id
                
    except httpx.HTTPError as e:
        logger.error(f"{thread_prefix}!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return None

async def check_mailbox_task_status(settings: "SettingParams", task_id: str, thread_id: int = 0):
    """
    Проверяет статус выполнения задачи на изменение прав доступа к почтовому ящику.
    Спецификация API: https://yandex.ru/dev/api360/doc/ru/ref/MailboxService/MailboxService_TaskStatus
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        task_id: Идентификатор задачи, полученный при вызове set_mailbox_permissions
        thread_id: Идентификатор потока для логирования
        
    Returns:
        dict: Словарь с информацией о задаче:
            - status: статус выполнения задачи (string), возможные значения:
                * "running": задача выполняется
                * "complete": задача успешно завершилась, права изменены
                * "error": задача завершилась с ошибкой
        None: в случае ошибки запроса
        
    Example:
        # Установка прав и проверка статуса задачи
        task_id = set_mailbox_permissions(
            settings,
            resource_id="1234567890",
            actor_id="9876543210",
            roles=["shared_mailbox_owner"]
        )
        
        if task_id:
            # Проверяем статус сразу
            status_info = await check_mailbox_task_status(settings, task_id)
            
            # Ожидаем выполнения задачи (с повторными проверками)
            max_attempts = 10
            for attempt in range(max_attempts):
                status_info = await check_mailbox_task_status(settings, task_id)
                if status_info and status_info.get('status') in ['complete', 'error']:
                    break
                await asyncio.sleep(2)  # Ждем 2 секунды перед следующей проверкой
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.info(f"{thread_prefix}Проверка статуса задачи taskId={task_id}...")
    url = f"{DEFAULT_360_API_URL}/admin/v1/org/{settings.organization_id}/mailboxes/tasks/{task_id}"
    headers = {
        "Authorization": f"OAuth {settings.oauth_token}"
    }
    
    try:
        retries = 1
        async with httpx.AsyncClient(headers=headers) as client:
            while True:
                logger.debug(f"{thread_prefix}GET URL - {url}")
                response = await client.get(url)
                logger.debug(f"{thread_prefix}x-request-id: {response.headers.get('x-request-id','')}")
                
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"{thread_prefix}!!! ОШИБКА !!! при GET запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                    if retries < MAX_RETRIES:
                        logger.error(f"{thread_prefix}Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        await asyncio.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error(f"{thread_prefix}Превышено максимальное количество попыток. Возвращается None.")
                        return None
                else:
                    result = response.json()
                    status = result.get('status', 'unknown')
                    logger.info(f"{thread_prefix}Статус задачи taskId={task_id}: {status}")
                    
                    if status == "complete":
                        logger.info(f"{thread_prefix}Задача успешно выполнена, права изменены")
                    elif status == "error":
                        logger.error(f"{thread_prefix}Задача завершилась с ошибкой")
                    elif status == "running":
                        logger.info(f"{thread_prefix}Задача выполняется...")
                        
                    return result
                
    except httpx.HTTPError as e:
        logger.error(f"{thread_prefix}!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        return None

async def wait_for_task_completion(settings: "SettingParams", task_id: str, max_attempts: int = 5, delay_seconds: int = 2, thread_id: int = 0):
    """
    Асинхронно ожидает завершения задачи на изменение прав доступа к почтовому ящику.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        task_id: Идентификатор задачи, полученный при вызове set_mailbox_permissions
        max_attempts: Максимальное количество попыток проверки статуса (по умолчанию 30)
        delay_seconds: Задержка между проверками в секундах (по умолчанию 2)
        thread_id: Идентификатор потока для логирования
        
    Returns:
        dict: Словарь с информацией о задаче в случае успешного завершения
        None: в случае ошибки или превышения количества попыток
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.info(f"{thread_prefix}Ожидание завершения задачи taskId={task_id}...")
    
    for attempt in range(1, max_attempts + 1):
        status_info = await check_mailbox_task_status(settings, task_id, thread_id)
        
        if status_info is None:
            logger.error(f"{thread_prefix}Ошибка при проверке статуса задачи (попытка {attempt}/{max_attempts})")
            await asyncio.sleep(delay_seconds)
            continue
            
        status = status_info.get('status', 'unknown')
        
        if status == 'complete':
            logger.info(f"{thread_prefix}Задача taskId={task_id} успешно завершена")
            return status_info
        elif status == 'error':
            logger.error(f"{thread_prefix}Задача taskId={task_id} завершилась с ошибкой")
            return None
        elif status == 'running':
            logger.debug(f"{thread_prefix}Задача выполняется... (попытка {attempt}/{max_attempts})")
            await asyncio.sleep(delay_seconds)
        else:
            logger.warning(f"{thread_prefix}Неизвестный статус задачи: {status} (попытка {attempt}/{max_attempts})")
            await asyncio.sleep(delay_seconds)
    
    logger.error(f"{thread_prefix}Превышено максимальное количество попыток ({max_attempts}) ожидания завершения задачи taskId={task_id}")
    return None

def restore_mailbox_permissions(settings: "SettingParams", resource_id: str, original_actors: list):
    """
    Восстанавливает оригинальные права доступа к делегированному почтовому ящику.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        resource_id: Идентификатор почтового ящика
        original_actors: Список оригинальных сотрудников с правами доступа
        
    Returns:
        list: Список taskId для каждого восстановленного сотрудника
        None: в случае ошибки
    """
    logger.info(f"Восстановление оригинальных прав доступа для ящика resourceId={resource_id}...")
    
    if not original_actors:
        logger.info("Оригинальный список доступа пуст, ничего не восстанавливается")
        return []
    
    task_ids = []
    
    for actor in original_actors:
        actor_id = actor.get('actorId')
        roles = actor.get('roles', [])
        
        if not actor_id or not roles:
            logger.warning(f"Пропуск восстановления для актора: некорректные данные {actor}")
            continue
        
        logger.info(f"Восстановление прав для actorId={actor_id}, роли: {', '.join(roles)}")
        task_id = set_mailbox_permissions(settings, resource_id, actor_id, roles, notify="none")
        
        if task_id:
            task_ids.append(task_id)
        else:
            logger.error(f"Не удалось восстановить права для actorId={actor_id}")
    
    return task_ids

def get_resource_id_by_email(settings: "SettingParams", all_users: list, all_shared_mailboxes: list, email: str, thread_prefix: str = ""):
    """
    Получает информацию о пользователе по email адресу или алиасу.
    
    Функция извлекает алиас (часть до @) из переданного email адреса и ищет 
    пользователя по этому алиасу в полях nickname и aliases.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        all_users: Список всех пользователей организации
        all_shared_mailboxes: Список всех общих почтовых ящиков
        email: Email адрес пользователя (например, "user@domain.com" или просто "user")
        
    Returns:
        dict: Информация о пользователе (включая id)
        resource_type: Тип ресурса (user, shared_mailbox)
        isEnabled: Флаг, указывающий, является ли пользователь включенным
        None: в случае ошибки или если пользователь не найден
    """
    logger.debug(f"{thread_prefix}Поиск ресурса с email={email}...")
    resource_type = None
    resource_id = None
    
    # Извлекаем алиас (часть до @) из email адреса
    if '@' in email:
        alias = email.split('@')[0].lower()
    else:
        alias = email.lower()
    
    logger.debug(f"{thread_prefix}Извлечённый алиас для поиска: {alias}")
    
    if not all_users and not all_shared_mailboxes:
        logger.error(f"{thread_prefix}Список всех ресурсов пуст. Выход из функции.")
        return None, None, False

    resource_type = None
    resource_id = None
    isEnabled = True
    for user in all_users:
        if user.get('nickname', '').lower() == alias or any(a.lower() == alias for a in user.get('aliases', [])):
            resource_id = user["id"]
            resource_type = "user"
            isEnabled = user["isEnabled"]
            break
    for shared in all_shared_mailboxes:
        if shared.get('email', '').split('@')[0].lower() == alias:
            resource_id = shared["id"]
            resource_type = "shared_mailbox"
            break

    return resource_id, resource_type, isEnabled
    
async def reconnect_imap_session(
    settings: "SettingParams", folder: str = None, thread_prefix: str = "", username: str = None, mode: str = "delegate"
) -> aioimaplib.IMAP4_SSL:
    """
    Создает новое IMAP-соединение с авторизацией и выбором папки.
    
    Args:
        settings: Объект настроек с параметрами подключения
        folder: Имя папки для SELECT после подключения (опционально)
        thread_prefix: Префикс для логирования
        username: Имя пользователя для авторизации
        mode: Режим авторизации ("delegate" - пароль, "service_application" - OAuth)
        
    Returns:
        aioimaplib.IMAP4_SSL: Новое IMAP-соединение
        
    Raises:
        Exception: При ошибке подключения или авторизации
    """
    try:
        if mode == "delegate":
            imap_password = settings.delegate_password
        elif mode == "service_application":
            try:
                imap_password = get_service_app_token(settings, username)
            except Exception as e:
                logger.error(f"{thread_prefix}Ошибка при получении service app token: {type(e).__name__}: {e}")
                raise
        else:
            logger.error(f"{thread_prefix}Неизвестный режим работы: {mode}")
            raise ValueError("Неизвестный режим работы")
        
        logger.warning(f"{thread_prefix}↻ Переподключение к IMAP...")
        last_exc = None
        imap_connector = None
        for attempt in range(1, IMAP_FETCH_RETRY_ATTEMPTS + 1):
            no_retry = False
            try:
                imap_connector = aioimaplib.IMAP4_SSL(
                    host=DEFAULT_IMAP_SERVER, port=DEFAULT_IMAP_PORT
                )
                await imap_connector.wait_hello_from_server()
                if mode == "service_application":
                    login_response = await imap_connector.xoauth2(user=username, token=imap_password)
                else:
                    login_response = await imap_connector.login(username, imap_password)
                if login_response.result != 'OK':
                    response_text = str(login_response.lines)
                    is_server_error = 'UNAVAILABLE' in response_text or 'internal server error' in response_text.lower()
                    no_retry = login_response.result == 'NO' and not is_server_error
                    raise Exception(
                        f"LOGIN вернул {login_response.result}: {login_response.lines}"
                    )
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                imap_connector = None
                if no_retry:
                    logger.error(
                        f"{thread_prefix}LOGIN отклонён сервером (NO), повторные попытки не выполняются: {e}"
                    )
                    raise
                logger.warning(
                    f"{thread_prefix}LOGIN попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} завершилась ошибкой: {type(e).__name__}: {e}"
                )
                await asyncio.sleep(0.5 * attempt)
        else:
            # Цикл завершился без break (не было no_retry raise) — проверяем результат
            if last_exc or imap_connector is None:
                logger.error(f"{thread_prefix}Ошибка при подключении или логине IMAP: {type(last_exc).__name__}: {last_exc}")
                raise last_exc

        if folder:
            try:
                rate_limit_imap_commands()
                logger.warning(f"{thread_prefix}↻ SELECT папки {folder}...")
                await imap_connector.select(folder)
            except Exception as e:
                logger.error(f"{thread_prefix}Ошибка при SELECT папки {folder}: {type(e).__name__}: {e}")
                await imap_connector.logout()
                raise

        logger.info(f"{thread_prefix}✓ Переподключение выполнено")
        return imap_connector

    except Exception as e:
        err_str = str(e)
        if 'FORBIDDEN' in err_str or 'AUTHENTICATIONFAILED' in err_str:
            logger.error(f"{thread_prefix}Ошибка переподключения к IMAP (доступ запрещён): {e}")
        else:
            logger.exception(f"{thread_prefix}Ошибка переподключения к IMAP: {type(e).__name__}: {e}")
        raise


def quote_imap_string(value: str) -> str:
    """
    Оборачивает строку в двойные кавычки для использования в IMAP-командах.
    
    Args:
        value: Строка для обработки
        
    Returns:
        str: Строка в двойных кавычках или пустые кавычки для пустой строки
    """
    if not value:
        return '""'
    if value.startswith('"') and value.endswith('"'):
        return value
    return f'"{value}"'


def imap_utf7_decode(value: str) -> str:
    """
    Декодирует строку из модифицированного UTF-7 формата IMAP в Unicode.
    
    Args:
        value: Строка в IMAP UTF-7 формате
        
    Returns:
        str: Декодированная Unicode строка
    """
    if not value or "&" not in value:
        return value
    result = []
    i = 0
    while i < len(value):
        if value[i] != "&":
            result.append(value[i])
            i += 1
            continue
        j = value.find("-", i)
        if j == -1:
            result.append("&")
            break
        if j == i + 1:
            result.append("&")
            i = j + 1
            continue
        chunk = value[i + 1:j].replace(",", "/")
        pad = (-len(chunk)) % 4
        if pad:
            chunk += "=" * pad
        try:
            decoded = base64.b64decode(chunk).decode("utf-16-be")
        except Exception:
            decoded = ""
        result.append(decoded)
        i = j + 1
    return "".join(result)


def imap_utf7_encode(value: str) -> str:
    """
    Кодирует Unicode строку в модифицированный UTF-7 формат IMAP.
    
    Args:
        value: Unicode строка для кодирования
        
    Returns:
        str: Строка в IMAP UTF-7 формате
    """
    if value is None:
        return ""
    result = []
    buf = []

    def flush_buf():
        if not buf:
            return
        utf16 = "".join(buf).encode("utf-16-be")
        enc = base64.b64encode(utf16).decode("ascii").rstrip("=")
        result.append("&" + enc.replace("/", ",") + "-")
        buf.clear()

    for ch in value:
        code = ord(ch)
        if 0x20 <= code <= 0x7E and ch != "&":
            flush_buf()
            result.append(ch)
        elif ch == "&":
            flush_buf()
            result.append("&-")
        else:
            buf.append(ch)
    flush_buf()
    return "".join(result)


def parse_imap_list_line(folder_line: Optional[bytes]) -> Optional[dict]:
    """
    Парсит строку ответа IMAP LIST команды.
    
    Args:
        folder_line: Байтовая строка ответа IMAP LIST
        
    Returns:
        dict: Словарь с полями flags, delimiter, mailbox, mailbox_display
        None: При невалидной строке или ошибке парсинга
    """
    if not folder_line or folder_line == b"LIST Completed.":
        return None
    try:
        decoded = folder_line.decode("utf-8", errors="replace").strip()
    except Exception:
        return None
    match = re.match(r'^\((?P<flags>[^)]*)\)\s+(?P<delimiter>NIL|"[^"]*")\s+(?P<mailbox>.+)$', decoded)
    if not match:
        return None
    flags_raw = match.group("flags").strip()
    delimiter_raw = match.group("delimiter").strip()
    mailbox_raw = match.group("mailbox").strip()
    delimiter = None
    if delimiter_raw and delimiter_raw != "NIL":
        delimiter = delimiter_raw.strip('"')
    if mailbox_raw.startswith('"') and mailbox_raw.endswith('"'):
        mailbox_raw = mailbox_raw.strip('"')
    if not mailbox_raw:
        return None
    return {
        "flags": [flag for flag in flags_raw.split() if flag],
        "delimiter": delimiter,
        "mailbox": mailbox_raw,
        "mailbox_display": imap_utf7_decode(mailbox_raw),
    }


async def list_folders_with_retry(
    imap_connector,
    username: str,
    reference: str = '""',
    pattern: str = "*",
    thread_prefix: str = "",
    settings: "SettingParams" = None,
    mode: str = "delegate",
):
    """
    Выполняет IMAP LIST команду с повторными попытками и автоматическим переподключением.
    
    Args:
        imap_connector: IMAP-соединение
        username: Имя пользователя для переподключения
        reference: Базовая директория для LIST (по умолчанию '""')
        pattern: Шаблон поиска папок (по умолчанию "*")
        thread_prefix: Префикс для логирования
        settings: Объект настроек
        mode: Режим работы ("delegate" или "service_application")
        
    Returns:
        tuple: (ответ LIST, обновленный imap_connector)
        
    Raises:
        RuntimeError: При невозможности выполнить LIST после всех попыток
    """
    last_exc = None

    for attempt in range(1, IMAP_FETCH_RETRY_ATTEMPTS + 1):
        try:
            rate_limit_imap_commands()
            list_response = await imap_connector.list(reference, pattern)
            if isinstance(list_response, (list, tuple)) and len(list_response) >= 2:
                result = list_response[0]
                if result == "OK":
                    return list_response, imap_connector
            logger.warning(
                f"{thread_prefix}LIST попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} вернула {getattr(list_response, 'result', list_response)}"
            )
        except Exception as e:
            last_exc = e
            logger.warning(
                f"{thread_prefix}LIST попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} завершилась ошибкой: {type(e).__name__}: {e}"
            )

        try:
            imap_connector = await reconnect_imap_session(
                settings, "INBOX", thread_prefix, username, mode = mode   
            )
        except Exception as reconnect_error:
            last_exc = reconnect_error
            logger.error(
                f"{thread_prefix}Не удалось переподключиться к IMAP при LIST: {type(reconnect_error).__name__}: {reconnect_error}"
            )
            await asyncio.sleep(0.5 * attempt)
            continue

        await asyncio.sleep(0.5 * attempt)

    if last_exc:
        raise last_exc
    raise RuntimeError("Не удалось выполнить LIST после переподключений")


async def list_all_folders_recursive(
    imap_connector,
    username: str,
    thread_prefix: str = "",
    settings: "SettingParams" = None,
    mode: str = "delegate",
):
    """
    Рекурсивно получает список всех папок почтового ящика.
    
    Обходит иерархию папок и возвращает список всех доступных папок,
    исключая папки с флагом \\Noselect.
    
    Args:
        imap_connector: IMAP-соединение
        username: Имя пользователя
        thread_prefix: Префикс для логирования
        settings: Объект настроек
        mode: Режим работы ("delegate" или "service_application")
        
    Returns:
        tuple: (список папок в кавычках, обновленный imap_connector)
    """
    folders = []
    folders_set = set()
    queue = [("", None)]
    seen = set()

    while queue:
        parent_mailbox, parent_delimiter = queue.pop(0)
        if parent_mailbox:
            delimiter = parent_delimiter or "/"
            pattern = f"{parent_mailbox}{delimiter}%"
        else:
            pattern = "%"

        (status, list_lines), imap_connector = await list_folders_with_retry(
            imap_connector=imap_connector,
            username=username,
            reference='""',
            pattern=quote_imap_string(pattern),
            thread_prefix=thread_prefix,
            settings=settings,
            mode=mode,
        )
        if status != "OK":
            continue

        for folder_line in list_lines:
            parsed = parse_imap_list_line(folder_line)
            if not parsed:
                continue
            mailbox = parsed["mailbox"]
            mailbox_quoted = quote_imap_string(mailbox)
            if mailbox_quoted not in folders_set:
                if not any(flag.lower() == "\\noselect" for flag in parsed["flags"]):
                    folders.append(mailbox_quoted)
                folders_set.add(mailbox_quoted)

            flags = parsed["flags"]
            has_no_children = any(flag.lower() == "\\hasnochildren" for flag in flags)
            if not has_no_children:
                delimiter = parsed["delimiter"] or parent_delimiter or "/"
                queue_key = (mailbox, delimiter)
                if queue_key not in seen:
                    seen.add(queue_key)
                    queue.append(queue_key)

    return folders, imap_connector


async def fetch_message_headers_with_retry(
    imap_connector,
    username: str,
    folder: str,
    msg_num: int,
    thread_prefix: str = "",
    settings: "SettingParams" = None,
    mode: str = "delegate",
):
    """
    Получает заголовки сообщения через IMAP FETCH с повторными попытками.
    
    Запрашивает UID, FLAGS и основные заголовки сообщения (Message-ID, From, To и др.).
    При ошибке выполняет переподключение и повторные попытки.
    
    Args:
        imap_connector: IMAP-соединение
        username: Имя пользователя для переподключения
        folder: Имя папки для SELECT при переподключении
        msg_num: Порядковый номер сообщения
        thread_prefix: Префикс для логирования
        settings: Объект настроек
        mode: Режим работы ("delegate" или "service_application")
        
    Returns:
        tuple: (ответ FETCH, обновленный imap_connector)
        
    Raises:
        RuntimeError: При невозможности выполнить FETCH после всех попыток
    """
    last_exc = None
    fetch_cmd = "(UID FLAGS BODY.PEEK[HEADER.FIELDS (%s)])" % " ".join(ID_HEADER_SET)

    for attempt in range(1, IMAP_FETCH_RETRY_ATTEMPTS + 1):
        try:
            rate_limit_imap_commands()
            fetch_response = await imap_connector.fetch(int(msg_num), fetch_cmd)
            if fetch_response.result == "OK":
                return fetch_response, imap_connector
            logger.warning(
                f"{thread_prefix}FETCH попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} вернула {fetch_response.result}"
            )
        except Exception as e:
            last_exc = e
            logger.warning(
                f"{thread_prefix}FETCH попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} завершилась ошибкой: {type(e).__name__}: {e}"
            )

        try:
            imap_connector = await reconnect_imap_session(
                settings, folder, thread_prefix, username, mode
            )
        except Exception as reconnect_error:
            last_exc = reconnect_error
            logger.error(
                f"{thread_prefix}Не удалось переподключиться к IMAP: {type(reconnect_error).__name__}: {reconnect_error}"
            )
            await asyncio.sleep(0.5 * attempt)
            continue

        await asyncio.sleep(0.5 * attempt)

    if last_exc:
        raise last_exc
    raise RuntimeError("Не удалось получить заголовки сообщения после переподключений")


async def store_message_with_retry(
    imap_connector,
    username: str,
    folder: str,
    msg_num: int,
    thread_prefix: str = "",
    settings: "SettingParams" = None,
    mode: str = "delegate",
):
    """
    Помечает сообщение флагом \\Deleted через IMAP STORE с повторными попытками.
    
    Args:
        imap_connector: IMAP-соединение
        username: Имя пользователя для переподключения
        folder: Имя папки для SELECT при переподключении
        msg_num: Порядковый номер сообщения
        thread_prefix: Префикс для логирования
        settings: Объект настроек
        mode: Режим работы ("delegate" или "service_application")
        
    Returns:
        tuple: (ответ STORE, обновленный imap_connector)
        
    Raises:
        RuntimeError: При невозможности выполнить STORE после всех попыток
    """
    last_exc = None

    for attempt in range(1, IMAP_FETCH_RETRY_ATTEMPTS + 1):
        try:
            rate_limit_imap_commands()
            store_response = await imap_connector.store(int(msg_num), "+FLAGS", "\\Deleted")
            result = getattr(store_response, "result", None)
            if result is None and isinstance(store_response, (list, tuple)) and store_response:
                result = store_response[0]
            if result == "OK":
                return store_response, imap_connector
            logger.warning(
                f"{thread_prefix}STORE попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} вернула {result}"
            )
        except Exception as e:
            last_exc = e
            logger.warning(
                f"{thread_prefix}STORE попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} завершилась ошибкой: {type(e).__name__}: {e}"
            )

        try:
            imap_connector = await reconnect_imap_session(
                settings, folder, thread_prefix, username, mode
            )
        except Exception as reconnect_error:
            last_exc = reconnect_error
            logger.error(
                f"{thread_prefix}Не удалось переподключиться к IMAP при STORE: {type(reconnect_error).__name__}: {reconnect_error}"
            )
            await asyncio.sleep(0.5 * attempt)
            continue

        await asyncio.sleep(0.5 * attempt)

    if last_exc:
        raise last_exc
    raise RuntimeError("Не удалось выполнить STORE после переподключений")


async def select_folder_with_retry(
    imap_connector,
    username: str,
    folder: str,
    thread_prefix: str = "",
    settings: "SettingParams" = None,
    mode: str = "delegate",
):
    """
    Выполняет IMAP SELECT команду для выбора папки с повторными попытками.
    
    Args:
        imap_connector: IMAP-соединение
        username: Имя пользователя для переподключения
        folder: Имя папки для выбора
        thread_prefix: Префикс для логирования
        settings: Объект настроек
        mode: Режим работы ("delegate" или "service_application")
        
    Returns:
        tuple: (ответ SELECT, обновленный imap_connector)
        
    Raises:
        RuntimeError: При невозможности выполнить SELECT после всех попыток
    """
    last_exc = None

    for attempt in range(1, IMAP_FETCH_RETRY_ATTEMPTS + 1):
        try:
            rate_limit_imap_commands()
            select_response = await imap_connector.select(folder)
            result = getattr(select_response, "result", None)
            if result is None and isinstance(select_response, (list, tuple)) and select_response:
                result = select_response[0]
            if result == "OK":
                return select_response, imap_connector
            logger.warning(
                f"{thread_prefix}SELECT попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} вернула {result}"
            )
        except Exception as e:
            last_exc = e
            logger.warning(
                f"{thread_prefix}SELECT попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} завершилась ошибкой: {type(e).__name__}: {e}"
            )

        try:
            imap_connector = await reconnect_imap_session(
                settings, folder, thread_prefix, username, mode
            )
        except Exception as reconnect_error:
            last_exc = reconnect_error
            logger.error(
                f"{thread_prefix}Не удалось переподключиться к IMAP при SELECT: {type(reconnect_error).__name__}: {reconnect_error}"
            )
            await asyncio.sleep(0.5 * attempt)
            continue

        await asyncio.sleep(0.5 * attempt)

    if last_exc:
        raise last_exc
    raise RuntimeError("Не удалось выполнить SELECT после переподключений")


async def search_with_retry(
    imap_connector,
    username: str,
    folder: str,
    search_criteria: str,
    thread_prefix: str = "",
    settings: "SettingParams" = None,
    mode: str = "delegate",
    ):
    """
    Выполняет IMAP SEARCH команду с повторными попытками и переподключением.
    
    Args:
        imap_connector: IMAP-соединение
        username: Имя пользователя для переподключения
        folder: Имя папки для SELECT при переподключении
        search_criteria: Критерии поиска IMAP (например, "SINCE 01-Jan-2024")
        thread_prefix: Префикс для логирования
        settings: Объект настроек
        mode: Режим работы ("delegate" или "service_application")
        
    Returns:
        tuple: (ответ SEARCH, обновленный imap_connector)
        
    Raises:
        RuntimeError: При невозможности выполнить SEARCH после всех попыток
    """
    last_exc = None

    for attempt in range(1, IMAP_FETCH_RETRY_ATTEMPTS + 1):
        try:
            rate_limit_imap_commands()
            response = await imap_connector.search(search_criteria)
            if response.result == "OK":
                return response, imap_connector
            logger.warning(
                f"{thread_prefix}SEARCH попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} вернула {response.result}"
            )
        except Exception as e:
            last_exc = e
            logger.warning(
                f"{thread_prefix}SEARCH попытка {attempt}/{IMAP_FETCH_RETRY_ATTEMPTS} завершилась ошибкой: {type(e).__name__}: {e}"
            )

        try:
            imap_connector = await reconnect_imap_session(
                settings, folder, thread_prefix, username, mode
            )
        except Exception as reconnect_error:
            last_exc = reconnect_error
            logger.error(
                f"{thread_prefix}Не удалось переподключиться к IMAP при SEARCH: {type(reconnect_error).__name__}: {reconnect_error}"
            )
            await asyncio.sleep(0.5 * attempt)
            continue

        await asyncio.sleep(0.5 * attempt)

    if last_exc:
        raise last_exc
    raise RuntimeError("Не удалось выполнить SEARCH после переподключений")


async def delete_messages_via_imap_basic_auth(
    delegate_alias: str,
    delegated_mailbox_alias: str,
    org_domain: str,
    mode: str,
    messages_to_delete: list,
    settings: "SettingParams",
    thread_id: int = 0
):
    """
    Удаляет сообщения из делегированного почтового ящика через IMAP с basic authentication.
    
    Args:
        delegate_alias: Логин делегата на Яндексе (например, "i.petrov")
        delegated_mailbox_alias: Имя делегированного ящика (например, "office")
        org_domain: Домен организации (например, "example.ru")
        mode: Режим работы для ящика (delegate, service_application, skip)
        messages_to_delete: Список словарей с информацией о сообщениях для удаления.
            Каждый словарь должен содержать:
            - message_id: идентификатор сообщения
            - message_date: дата сообщения в формате "DD-MM-YYYY"
            - days_diff: количество дней для диапазона поиска (по умолчанию ±1 день)
        settings: Объект настроек
        thread_id: Идентификатор потока для логирования
        
    Returns:
        dict: Словарь с результатами удаления {message_id: {"status": status, "folders": [folder1, folder2, ...]}}
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    if mode == "delegate":
        # Формируем имя пользователя в формате: домен/логин_делегата/имя_ящика
        username = f"{org_domain}/{delegate_alias}/{delegated_mailbox_alias}"
    elif mode == "service_application":
        username = f"{delegated_mailbox_alias}@{org_domain}"
    else:
        raise ValueError("Неизвестный режим работы")
    
    logger.info(f"{thread_prefix}Подключение к IMAP для удаления сообщений. Пользователь: {username}")
    
    results = {}
    
    # Подключаемся к IMAP серверу (вне try/except — если логин не удался,
    # исключение должно всплыть к вызывающему коду)
    logger.debug(f"{thread_prefix}Авторизация для пользователя {username}...")
    imap_connector = await reconnect_imap_session(
        username=username, settings=settings, thread_prefix=thread_prefix, mode=mode
    )
    logger.info(f"{thread_prefix}Успешная авторизация для {username}")
    
    try:
        # Рекурсивно получаем список папок
        folders, imap_connector = await list_all_folders_recursive(
            imap_connector=imap_connector,
            username=username,
            thread_prefix=thread_prefix,
            settings=settings,
            mode=mode,
        )
        
        logger.debug(f"{thread_prefix}Найдено папок: {len(folders)}")
        
        # Проходим по всем папкам и ищем сообщения
        for folder in folders:
            folder_display = imap_utf7_decode(folder.strip('"'))
            logger.debug(f"{thread_prefix}Поиск сообщений в папке {folder_display}...")
            _, imap_connector = await select_folder_with_retry(
                imap_connector=imap_connector,
                username=username,
                folder=folder,
                thread_prefix=thread_prefix,
                settings=settings,
                mode=mode,
            )
            
            for msg in messages_to_delete:
                # Нормализуем message_id
                message_id = msg["message_id"]
                search_id = f'<{message_id.replace("<", "").replace(">", "").strip()}>'
                
                # Вычисляем диапазон дат для поиска на основе даты сообщения
                msg_date = datetime.strptime(msg["message_date"], "%d-%m-%Y")
                first_date = msg_date + relativedelta(days=-msg["days_diff"], hour=0, minute=0, second=0)
                last_date = msg_date + relativedelta(days=msg["days_diff"] + 1, hour=0, minute=0, second=0)
                
                # Поиск по диапазону дат вместо заголовка (IMAP сервер не поддерживает HEADER Message-ID)
                search_criteria = f'(SINCE {first_date.strftime("%d-%b-%Y")}) BEFORE {last_date.strftime("%d-%b-%Y")}'
                logger.debug(f"{thread_prefix}Критерий поиска для {search_id}: {search_criteria}")
                response, imap_connector = await search_with_retry(
                    imap_connector=imap_connector,
                    username=username,
                    folder=folder,
                    search_criteria=search_criteria,
                    thread_prefix=thread_prefix,
                    settings=settings,
                    mode=mode,
                )
                
                if response.result == 'OK':
                    if len(response.lines[0]) > 0:
                        message_numbers = response.lines[0].split()
                        
                        # Для каждого найденного сообщения получаем заголовки и проверяем Message-ID
                        for num in message_numbers:
                            try:
                                # Получаем заголовки сообщения
                                fetch_response, imap_connector = (
                                    await fetch_message_headers_with_retry(
                                        imap_connector=imap_connector,
                                        username=username,
                                        folder=folder,
                                        msg_num=int(num),
                                        thread_prefix=thread_prefix,
                                        settings=settings,
                                        mode=mode,
                                    )
                                )
                                
                                if fetch_response.result == 'OK':
                                    # Парсим заголовки
                                    for i in range(0, len(fetch_response.lines) - 1, 3):
                                        # Парсим заголовки сообщения
                                        message_headers = BytesHeaderParser().parsebytes(fetch_response.lines[i + 1])
                                        
                                        # Получаем Message-ID из заголовков
                                        if 'Message-ID' in message_headers or 'message-id' in message_headers:
                                            header_msg_id = message_headers.get('Message-ID') or message_headers.get('message-id')
                                            
                                            # Декодируем заголовок
                                            decoded_to_intermediate = decode_header(header_msg_id)
                                            header_value = []
                                            for s in decoded_to_intermediate:
                                                if s[1] is not None:
                                                    header_value.append(s[0].decode(s[1]))
                                                else:
                                                    if isinstance(s[0], (bytes, bytearray)):
                                                        header_value.append(s[0].decode("ascii").strip())
                                                    else:
                                                        header_value.append(s[0])
                                            
                                            found_msg_id = ' '.join(header_value) if len(header_value) > 1 else header_value[0]
                                            
                                            # Сравниваем с искомым message-id
                                            if found_msg_id.strip() == search_id.strip():
                                                if settings.dry_run:
                                                    logger.info(f"{thread_prefix}DRY_RUN: Виртуальное удаление сообщения {search_id} в папке {folder_display}")
                                                    # Инициализируем результат, если его еще нет
                                                    if message_id not in results:
                                                        results[message_id] = {
                                                            "status": "DRY_RUN: Виртуальное удаление",
                                                            "folders": []
                                                        }
                                                    # Добавляем папку в список, если её там еще нет
                                                    if folder_display not in results[message_id]["folders"]:
                                                        results[message_id]["folders"].append(folder_display)
                                                else:
                                                    logger.info(f"{thread_prefix}Удаление сообщения {search_id} в папке {folder_display}")
                                                    _, imap_connector = await store_message_with_retry(
                                                        imap_connector=imap_connector,
                                                        username=username,
                                                        folder=folder,
                                                        msg_num=int(num),
                                                        thread_prefix=thread_prefix,
                                                        settings=settings,
                                                        mode=mode,
                                                    )
                                                    # Инициализируем результат, если его еще нет
                                                    if message_id not in results:
                                                        results[message_id] = {
                                                            "status": "Успешно удалено",
                                                            "folders": []
                                                        }
                                                    # Добавляем папку в список, если её там еще нет
                                                    if folder_display not in results[message_id]["folders"]:
                                                        results[message_id]["folders"].append(folder_display)
                            except Exception as e:
                                logger.debug(f"{thread_prefix}Ошибка при обработке сообщения {num} в папке {folder}: {e}")
                                continue
        
        # Закрываем соединение
        rate_limit_imap_commands()
        await imap_connector.logout()
        logger.info(f"{thread_prefix}Отключение от IMAP сервера для {username}")
        
    except Exception as e:
        error_msg = f"{thread_prefix}Ошибка при работе с IMAP: {type(e).__name__}: {e}"
        logger.error(error_msg)
        logger.error(f"{thread_prefix}Детали: at line {e.__traceback__.tb_lineno} of {__file__}")
        
        # Помечаем все необработанные сообщения как ошибочные
        for msg in messages_to_delete:
            message_id = msg["message_id"]
            if message_id not in results:
                results[message_id] = {
                    "status": f"Ошибка: {str(e)}",
                    "folders": []
                }
    
    return results

def create_checkpoint_file(check_dir: str) -> Optional[tuple]:
    """
    Создает новые checkpoint файлы (checkin и checkout).
    
    Args:
        check_dir: Путь к каталогу для хранения состояний
        
    Returns:
        tuple: Кортеж (checkin_filepath, checkout_filepath) или None в случае ошибки
    """
    try:
        # Создаем каталог, если не существует
        if not os.path.exists(check_dir):
            os.makedirs(check_dir)
            logger.info(f"Создан каталог для сохранения состояний: {check_dir}")
        
        # Формируем имя файла с датой и временем
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        checkin_filename = f"checkin_{timestamp}.txt"
        checkout_filename = f"checkout_{timestamp}.txt"
        checkin_filepath = os.path.join(check_dir, checkin_filename)
        checkout_filepath = os.path.join(check_dir, checkout_filename)
        
        # Если файл уже существует (вызов в ту же секунду), добавляем микросекунды
        if os.path.exists(checkin_filepath):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            checkin_filename = f"checkin_{timestamp}.txt"
            checkout_filename = f"checkout_{timestamp}.txt"
            checkin_filepath = os.path.join(check_dir, checkin_filename)
            checkout_filepath = os.path.join(check_dir, checkout_filename)
        
        # Создаем пустые файлы
        with open(checkin_filepath, 'w', encoding='utf-8'):
            pass
        with open(checkout_filepath, 'w', encoding='utf-8'):
            pass
        
        logger.info(f"Созданы checkpoint файлы: {checkin_filepath}, {checkout_filepath}")
        return (checkin_filepath, checkout_filepath)
        
    except Exception as e:
        logger.error(f"Ошибка при создании checkpoint файлов: {str(e)}")
        return None


def create_report_file(check_dir: str, timestamp: Optional[str] = None) -> Optional[str]:
    """
    Создает файл отчета для запуска удаления сообщений.
    
    Args:
        check_dir: Путь к каталогу для хранения файлов отчета
        timestamp: Метка времени для имени файла (если не указана, используется текущее время)
        
    Returns:
        str: Путь к файлу отчета или None в случае ошибки
    """
    try:
        # Создаем каталог, если не существует
        if not os.path.exists(check_dir):
            os.makedirs(check_dir)
            logger.info(f"Создан каталог для сохранения отчетов: {check_dir}")
        
        # Формируем имя файла с датой и временем
        if not timestamp:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"report_{timestamp}.csv"
        report_filepath = os.path.join(check_dir, report_filename)
        
        # Если файл уже существует (вызов в ту же секунду), добавляем микросекунды
        if os.path.exists(report_filepath):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            report_filename = f"report_{timestamp}.csv"
            report_filepath = os.path.join(check_dir, report_filename)
        
        # Создаем файл отчета с заголовками
        with open(report_filepath, 'w', encoding='utf-8') as f:
            f.write("thread_id;date;email;mailbox_type;status;folder;message_id;message_date;time_shift;dry_run;error\n")
        
        logger.info(f"Создан файл отчета: {report_filepath}")
        return report_filepath
        
    except Exception as e:
        logger.error(f"Ошибка при создании файла отчета: {str(e)}")
        return None


def append_report_record(
    report_file: str,
    thread_id: int,
    email: str,
    mailbox_type: str,
    status: str,
    folder: str,
    message_id: str,
    message_date: str,
    time_shift: str,
    dry_run: str,
    error: str
) -> bool:
    """
    Добавляет запись об операции удаления в файл отчета.
    
    Returns:
        bool: True если запись успешна, False в случае ошибки
    """
    try:
        if not report_file:
            return False
        record_date = datetime.now().strftime("%d.%m.%y %H:%M:%S")
        safe_error = (error or "").replace("\n", " ").replace("\r", " ").strip()
        with open(report_file, 'a', encoding='utf-8') as f:
            f.write(
                f"{thread_id};{record_date};{email};{mailbox_type};"
                f"{status};{folder};{message_id};{message_date};{time_shift};"
                f"{dry_run};{safe_error}\n"
            )
        return True
    except Exception as e:
        logger.error(f"Ошибка при записи в файл отчета {report_file}: {str(e)}")
        return False


def compare_checkpoint_files(checkin_file: str, checkout_file: str, check_dir: str) -> tuple[Optional[str], list]:
    """
    Сравнивает содержимое файла checkin с файлом checkout.
    Для каждой строки в checkin ищет идентичную строку в checkout.
    Если строка не найдена, добавляет её в файл diff.
    
    Args:
        checkin_file: Путь к файлу checkin
        checkout_file: Путь к файлу checkout
        check_dir: Путь к каталогу для сохранения diff файла
        
    Returns:
        tuple: (путь к diff файлу или None, список отсутствующих строк)
    """
    try:
        # Проверяем существование файлов
        if not checkin_file or not checkout_file:
            logger.warning("Checkpoint файлы не были созданы. Пропускаем сравнение.")
            return None, []
            
        if not os.path.exists(checkin_file):
            logger.error(f"Файл checkin не найден: {checkin_file}")
            return None, []
            
        if not os.path.exists(checkout_file):
            logger.error(f"Файл checkout не найден: {checkout_file}")
            return None, []
        
        # Читаем содержимое файлов
        with open(checkin_file, 'r', encoding='utf-8') as f:
            checkin_lines = [line.strip() for line in f.readlines() if line.strip()]
        
        with open(checkout_file, 'r', encoding='utf-8') as f:
            checkout_lines = [line.strip() for line in f.readlines() if line.strip()]
        
        # Преобразуем checkout_lines в set для быстрого поиска
        checkout_set = set(checkout_lines)
        
        # Находим строки из checkin, которых нет в checkout
        missing_lines = []
        for line in checkin_lines:
            if line not in checkout_set:
                missing_lines.append(line)
        
        # Если различий нет, возвращаем None и пустой список
        if not missing_lines:
            logger.info("✓ Сравнение checkpoint файлов: различий не обнаружено. Все строки из checkin присутствуют в checkout.")
            return None, []
        
        # Извлекаем timestamp из имени checkin файла
        checkin_basename = os.path.basename(checkin_file)
        # Формат имени: checkin_YYYYMMDD_HHMMSS.txt или checkin_YYYYMMDD_HHMMSS_microseconds.txt
        if checkin_basename.startswith("checkin_"):
            timestamp_part = checkin_basename.replace("checkin_", "").replace(".txt", "")
            timestamp = timestamp_part
        else:
            # Если не удалось извлечь, используем текущее время
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        diff_filename = f"diff_{timestamp}.txt"
        diff_filepath = os.path.join(check_dir, diff_filename)
        
        # Если файл уже существует, добавляем микросекунды
        if os.path.exists(diff_filepath):
            timestamp_with_micro = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            diff_filename = f"diff_{timestamp_with_micro}.txt"
            diff_filepath = os.path.join(check_dir, diff_filename)
        
        # Записываем различия в файл
        with open(diff_filepath, 'w', encoding='utf-8') as f:
            f.write("# Строки из checkin, отсутствующие в checkout\n")
            f.write(f"# Checkin file: {os.path.basename(checkin_file)}\n")
            f.write(f"# Checkout file: {os.path.basename(checkout_file)}\n")
            f.write(f"# Дата сравнения: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"# Всего строк в checkin: {len(checkin_lines)}\n")
            f.write(f"# Всего строк в checkout: {len(checkout_lines)}\n")
            f.write(f"# Строк отсутствует в checkout: {len(missing_lines)}\n")
            f.write("#\n")
            f.write("# === РАЗЛИЧИЯ ===\n")
            f.write("#\n")
            for line in missing_lines:
                f.write(f"{line}\n")
        
        logger.warning("⚠ Обнаружены различия между checkin и checkout!")
        logger.warning(f"   Всего строк в checkin: {len(checkin_lines)}")
        logger.warning(f"   Всего строк в checkout: {len(checkout_lines)}")
        logger.warning(f"   Строк отсутствует в checkout: {len(missing_lines)}")
        logger.warning(f"   Файл с различиями сохранен: {diff_filepath}")
        
        return diff_filepath, missing_lines
        
    except Exception as e:
        logger.error(f"Ошибка при сравнении checkpoint файлов: {str(e)}")
        return None, []


def check_incomplete_sessions(settings: "SettingParams") -> bool:
    """
    Проверяет наличие незавершенных сессий при запуске скрипта.
    
    Функция ищет последний checkin файл и проверяет наличие соответствующего checkout файла:
    1. Если checkout есть, но содержимое отличается - предлагает восстановить разрешения
    2. Если checkout отсутствует - предполагает аварийное завершение и предлагает восстановление
    
    Args:
        settings: Объект настроек с oauth_token, organization_id и check_dir
        
    Returns:
        bool: True если проверка прошла успешно (восстановление не требуется или выполнено), 
              False если пользователь отказался от восстановления
    """
    try:
        check_dir = settings.check_dir
        
        # Проверяем существование каталога
        if not os.path.exists(check_dir):
            logger.debug(f"Каталог для checkpoint файлов не найден: {check_dir}")
            return True
        
        # Находим все checkin файлы
        checkin_files = []
        for filename in os.listdir(check_dir):
            if filename.startswith("checkin_") and filename.endswith(".txt"):
                filepath = os.path.join(check_dir, filename)
                # Получаем время модификации файла
                mtime = os.path.getmtime(filepath)
                checkin_files.append((filepath, mtime, filename))
        
        if not checkin_files:
            logger.debug("Checkin файлы не найдены. Пропускаем проверку незавершенных сессий.")
            return True
        
        # Сортируем по времени и берем последний
        checkin_files.sort(key=lambda x: x[1], reverse=True)
        last_checkin_path, _, last_checkin_filename = checkin_files[0]
        
        # Извлекаем timestamp из имени файла
        # Формат: checkin_YYYYMMDD_HHMMSS.txt
        if last_checkin_filename.startswith("checkin_"):
            timestamp = last_checkin_filename.replace("checkin_", "").replace(".txt", "")
        else:
            logger.warning(f"Не удалось извлечь timestamp из имени файла: {last_checkin_filename}")
            return True
        
        # Пытаемся найти соответствующий checkout файл
        checkout_filename = f"checkout_{timestamp}.txt"
        checkout_path = os.path.join(check_dir, checkout_filename)
        
        missing_lines = []
        
        if os.path.exists(checkout_path):
            # Checkout файл существует - сравниваем содержимое
            logger.info("="*80)
            logger.info("ПРОВЕРКА НЕЗАВЕРШЕННЫХ СЕССИЙ")
            logger.info("="*80)
            logger.info(f"Найден последний checkin файл: {last_checkin_filename}")
            logger.info(f"Найден соответствующий checkout файл: {checkout_filename}")
            logger.info("Выполняется сравнение содержимого...")
            
            diff_file, missing_lines = compare_checkpoint_files(
                last_checkin_path, 
                checkout_path, 
                check_dir
            )
            
            if not missing_lines:
                # Различий нет - всё в порядке
                logger.info("✓ Предыдущий запуск завершился корректно. Различий не обнаружено.")
                logger.info("="*80)
                return True
            
            # Обнаружены различия
            logger.warning("="*80)
            logger.warning("⚠ ВНИМАНИЕ: Обнаружены различия между checkin и checkout файлами!")
            logger.warning("⚠ Это может означать, что предыдущий запуск завершился аварийно.")
            logger.warning(f"⚠ Обнаружено строк с несовпадениями: {len(missing_lines)}")
            if diff_file:
                logger.warning(f"⚠ Детали сохранены в файл: {os.path.basename(diff_file)}")
            logger.warning("="*80)
            
        else:
            # Checkout файла нет - предполагаем аварийное завершение
            logger.warning("="*80)
            logger.warning("⚠ ВНИМАНИЕ: Обнаружен checkin файл без соответствующего checkout файла!")
            logger.warning(f"⚠ Checkin файл: {last_checkin_filename}")
            logger.warning(f"⚠ Checkout файл не найден: {checkout_filename}")
            logger.warning("⚠ Это означает, что предыдущий запуск был прерван до завершения.")
            logger.warning("="*80)
            
            # Читаем все строки из checkin файла
            with open(last_checkin_path, 'r', encoding='utf-8') as f:
                missing_lines = [line.strip() for line in f.readlines() if line.strip()]
            
            logger.info(f"Найдено строк для возможного восстановления: {len(missing_lines)}")
        
        # Предлагаем пользователю восстановить разрешения
        print("\n")
        print("="*80)
        print("ОБНАРУЖЕНА НЕЗАВЕРШЕННАЯ СЕССИЯ")
        print("="*80)
        print(f"Checkin файл: {last_checkin_filename}")
        if os.path.exists(checkout_path):
            print(f"Checkout файл: {checkout_filename} (содержимое отличается)")
        else:
            print(f"Checkout файл: {checkout_filename} (НЕ НАЙДЕН)")
        unique_mailboxes = len(set(line.split('|')[0] for line in missing_lines))
        print(f"Количество почтовых ящиков для восстановления: {unique_mailboxes}")
        print("="*80)
        print("\nВосстановление разрешений почтовых ящиков вернет их в исходное состояние")
        print("(как до запуска операции удаления сообщений).")
        print("")
        
        response = input("Хотите восстановить исходные разрешения почтовых ящиков? (y/n): ").lower().strip()
        
        if response in ['y', 'yes', 'д', 'да']:
            logger.info("Пользователь подтвердил восстановление разрешений.")
            logger.info("Запуск функции восстановления...")
            
            # Запускаем восстановление
            result = restore_permissions_from_diff(missing_lines, settings)
            
            logger.info("="*80)
            logger.info("РЕЗУЛЬТАТЫ ВОССТАНОВЛЕНИЯ:")
            logger.info(f"  Всего ящиков: {result['total']}")
            logger.info(f"  Обработано: {result['processed']}")
            logger.info(f"  Успешно: {result['success']}")
            logger.info(f"  Ошибок: {result['errors']}")
            logger.info("="*80)
            
            if result['success'] > 0:
                print(f"\n✓ Восстановление завершено. Успешно восстановлено: {result['success']} из {result['total']}")
            else:
                print("\n✗ Восстановление не удалось. Проверьте логи для деталей.")
            
            return True
        else:
            logger.info("Пользователь отказался от восстановления разрешений.")
            print("\nВосстановление отменено. Вы можете выполнить его позже вручную.")
            print(f"Checkin файл: {last_checkin_path}")
            if os.path.exists(checkout_path):
                print(f"Checkout файл: {checkout_path}")
            return True
            
    except Exception as e:
        logger.error(f"Ошибка при проверке незавершенных сессий: {str(e)}")
        logger.exception(e)
        return True  # Продолжаем работу даже при ошибке


def is_mailbox_delegation_enabled(resource_id: str, delegated_mailboxes: list, thread_id: int = 0):
    """
    Проверяет, включено ли делегирование для почтового ящика.
    
    Args:
        resource_id: Идентификатор почтового ящика
        delegated_mailboxes: Список всех делегированных ящиков организации
        thread_id: Идентификатор потока для логирования
        
    Returns:
        bool: True если делегирование включено, False если выключено
    """
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    logger.debug(f"{thread_prefix}Проверка статуса делегирования для resourceId={resource_id}...")
    
    # Проверяем, есть ли наш ящик в списке делегированных
    for mailbox in delegated_mailboxes:
        if str(mailbox.get('resourceId')) == str(resource_id):
            logger.debug(f"{thread_prefix}Ящик найден в списке делегированных (делегирование включено)")
            return True
    
    logger.debug(f"{thread_prefix}Ящик не найден в списке делегированных (делегирование выключено)")
    return False


def remove_all_mailbox_actors(settings: "SettingParams", resource_id: str, current_actors: list, thread_id: int = 0):
    """
    Удаляет всех делегатов (делегатов) из почтового ящика.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        resource_id: Идентификатор почтового ящика
        current_actors: Список текущих делегатов
        thread_id: Идентификатор потока для логирования
        
    Returns:
        list: Список taskId для каждого удалённого делегата
        None: в случае ошибки
    """
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    logger.info(f"{thread_prefix}Удаление всех делегатов из ящика resourceId={resource_id}...")
    
    if not current_actors:
        logger.info(f"{thread_prefix}Список делегатов уже пуст, ничего не удаляется")
        return []
    
    task_ids = []
    
    for actor in current_actors:
        actor_id = actor.get('actorId')
        
        if not actor_id:
            logger.warning(f"{thread_prefix}Пропуск удаления для делегата: некорректные данные {actor}")
            continue
        
        logger.info(f"{thread_prefix}Удаление прав для actorId={actor_id}")
        # Передаём пустой список ролей для удаления доступа
        task_id = set_mailbox_permissions(settings, resource_id, actor_id, [], notify="none", thread_id=thread_id)
        
        if task_id:
            task_ids.append(task_id)
            logger.info(f"{thread_prefix}Инициировано удаление прав для actorId={actor_id}, taskId={task_id}")
        else:
            logger.error(f"{thread_prefix}Не удалось удалить права для actorId={actor_id}")
    
    return task_ids


def compare_actors_lists(current_actors: list, target_actors: list):
    """
    Сравнивает два списка делегатов и определяет, есть ли различия.
    
    Args:
        current_actors: Текущий список делегатов
        target_actors: Целевой список делегатов
        
    Returns:
        bool: True если списки различаются, False если идентичны
    """
    # Если длины различаются, списки точно разные
    if len(current_actors) != len(target_actors):
        return True
    
    # Создаём множества для сравнения (actorId + sorted roles)
    def normalize_actor(actor):
        return (actor.get('actorId'), tuple(sorted(actor.get('roles', []))))
    
    current_set = set(normalize_actor(a) for a in current_actors)
    target_set = set(normalize_actor(a) for a in target_actors)
    
    return current_set != target_set


def restore_permissions_from_diff(missing_lines: list, settings: "SettingParams", all_users: list = None):
    """
    Восстанавливает исходные разрешения для почтовых ящиков на основе различий между checkin и checkout.
    
    Функция парсит строки из diff файла и выполняет соответствующие API запросы для восстановления:
    - delegation_enabled=true/false -> включение/выключение делегирования
    - actors=[...] -> восстановление списка делегатов с правами доступа
    
    Порядок операций:
    1. Если delegation_enabled=true и есть actors:
       - Проверить текущий статус делегирования
       - Если нужно, включить делегирование
       - Проверить текущий список actors
       - Если нужно, установить новые разрешения
       
    2. Если delegation_enabled=false и actors=[]:
       - Проверить текущий список actors
       - Если нужно, удалить всех делегатов
       - Проверить текущий статус делегирования
       - Если нужно, выключить делегирование
    
    Args:
        missing_lines: Список строк из checkin, отсутствующих в checkout
        settings: Объект настроек с oauth_token и organization_id
        all_users: Список всех пользователей (если не передан, будет получен автоматически)
        
    Returns:
        dict: Статистика восстановления с ключами:
            - total: общее количество строк для восстановления
            - processed: количество обработанных строк
            - success: количество успешно восстановленных
            - errors: количество ошибок
            - details: список деталей по каждому ящику
    """
    logger.info("="*80)
    logger.info("НАЧАЛО ВОССТАНОВЛЕНИЯ РАЗРЕШЕНИЙ ИЗ DIFF")
    logger.info("="*80)
    
    if not missing_lines:
        logger.info("Список различий пуст. Восстановление не требуется.")
        return {
            "total": 0,
            "processed": 0,
            "success": 0,
            "errors": 0,
            "details": []
        }
    
    # Получаем список всех пользователей, если не передан
    if not all_users:
        all_users = get_all_api360_users(settings, force=False)
    all_shared_mailboxes = get_all_shared_mailboxes_cached(settings, force=False)
    if not all_users and not all_shared_mailboxes:
        logger.error("Не удалось получить список пользователей и общих ящиков. Восстановление невозможно.")
        return {
            "total": len(missing_lines),
            "processed": 0,
            "success": 0,
            "errors": len(missing_lines),
            "details": []
        }
    
    # Получаем список всех делегированных ящиков
    logger.info("Получение списка всех делегированных ящиков...")
    delegated_mailboxes = get_all_delegated_mailboxes(settings)
    if not delegated_mailboxes:
        logger.error("Не удалось получить список делегированных ящиков. Восстановление невозможно.")
        return {
            "total": len(missing_lines),
            "processed": 0,
            "success": 0,
            "errors": len(missing_lines),
            "details": []
        }
    logger.info(f"Получено {len(delegated_mailboxes)} делегированных ящиков")
    
    # Группируем строки по почтовому ящику
    mailbox_data = {}
    for line in missing_lines:
        if '|' not in line:
            logger.warning(f"Пропуск некорректной строки (нет разделителя |): {line}")
            continue
        
        parts = line.split('|', 1)
        if len(parts) != 2:
            logger.warning(f"Пропуск некорректной строки (неверный формат): {line}")
            continue
        
        mailbox_alias = parts[0].strip()
        property_value = parts[1].strip()
        
        if mailbox_alias not in mailbox_data:
            mailbox_data[mailbox_alias] = {
                "delegation_enabled": None,
                "actors": None
            }
        
        # Парсим значение свойства
        if property_value.startswith("delegation_enabled="):
            delegation_value = property_value.split("=", 1)[1].strip()
            mailbox_data[mailbox_alias]["delegation_enabled"] = delegation_value.lower() == "true"
        elif property_value.startswith("actors="):
            actors_json = property_value.split("=", 1)[1].strip()
            try:
                import json
                actors = json.loads(actors_json)
                mailbox_data[mailbox_alias]["actors"] = actors
            except json.JSONDecodeError as e:
                logger.error(f"Ошибка парсинга JSON для actors в строке: {line}, ошибка: {e}")
    
    logger.info(f"Обнаружено {len(mailbox_data)} почтовых ящиков для восстановления")
    
    # Статистика (считаем по почтовым ящикам, не по строкам)
    stats = {
        "total": len(mailbox_data),
        "processed": 0,
        "success": 0,
        "errors": 0,
        "details": []
    }
    
    # Восстанавливаем разрешения для каждого ящика
    for mailbox_alias, data in mailbox_data.items():
        logger.info("-"*80)
        logger.info(f"Обработка ящика: {mailbox_alias}")
        
        mailbox_detail = {
            "mailbox": mailbox_alias,
            "delegation_restored": False,
            "actors_restored": False,
            "error": None
        }
        
        # Получаем информацию о пользователе (владельце ящика)
        resource_id, resource_type, isEnabled = get_resource_id_by_email(settings, all_users, all_shared_mailboxes, mailbox_alias)
        if not resource_id:
            error_msg = f"Пользователь или общий ящик {mailbox_alias} не найден в организации"
            logger.error(error_msg)
            mailbox_detail["error"] = error_msg
            stats["details"].append(mailbox_detail)
            continue
        
        logger.info(f"  Найден пользователь/общий ящик: {mailbox_alias}, resource_id={resource_id}")
        
        # Определяем порядок операций на основе требуемых значений
        target_delegation_enabled = data["delegation_enabled"]
        target_actors = data["actors"]
        
        # Проверяем текущее состояние
        logger.info("  Проверка текущего состояния ящика...")
        current_delegation_enabled = is_mailbox_delegation_enabled(resource_id, delegated_mailboxes)
        
        # Получаем список делегатов только если ящик в списке делегированных
        if current_delegation_enabled:
            logger.info("  Ящик делегирован, получение списка делегатов...")
            current_actors = get_mailbox_actors(settings, resource_id)
            if current_actors is None:
                error_msg = f"Не удалось получить список делегатов для ящика {mailbox_alias}"
                logger.error(error_msg)
                mailbox_detail["error"] = error_msg
                stats["details"].append(mailbox_detail)
                continue
        else:
            logger.info("  Ящик не делегирован, список делегатов пуст")
            current_actors = []
        
        logger.info(f"  Текущее состояние: delegation_enabled={current_delegation_enabled}, actors={len(current_actors)}")
        logger.info(f"  Целевое состояние: delegation_enabled={target_delegation_enabled}, actors={len(target_actors) if target_actors is not None else 'N/A'}")
        
        # Сценарий 1: Включение делегирования и установка разрешений
        if target_delegation_enabled is True and target_actors is not None and len(target_actors) > 0:
            logger.info("  Сценарий: Включение делегирования и установка разрешений")
            
            # Шаг 1: Включаем делегирование (если выключено)
            if not current_delegation_enabled:
                logger.info(f"  Включение делегирования для {mailbox_alias}...")
                result = enable_mailbox_delegation(settings, resource_id)
                if result:
                    logger.info(f"  ✓ Делегирование успешно включено для {mailbox_alias}")
                    mailbox_detail["delegation_restored"] = True
                else:
                    logger.error(f"  ✗ Не удалось включить делегирование для {mailbox_alias}")
                    mailbox_detail["error"] = "Ошибка включения делегирования"
                    stats["details"].append(mailbox_detail)
                    continue
            else:
                logger.info("  Делегирование уже включено, пропуск операции")
                mailbox_detail["delegation_restored"] = True
            
            # Шаг 2: Устанавливаем разрешения (если есть различия)
            if compare_actors_lists(current_actors, target_actors):
                logger.info("  Обнаружены различия в списке делегатов, восстановление...")
                task_ids = restore_mailbox_permissions(settings, resource_id, target_actors)
                
                if task_ids and len(task_ids) == len(target_actors):
                    logger.info(f"  ✓ Все делегаты успешно восстановлены для {mailbox_alias}")
                    mailbox_detail["actors_restored"] = True
                elif task_ids:
                    logger.warning(f"  ⚠ Частично восстановлены делегаты для {mailbox_alias}: {len(task_ids)}/{len(target_actors)}")
                    mailbox_detail["actors_restored"] = "partial"
                    mailbox_detail["error"] = f"Восстановлено только {len(task_ids)} из {len(target_actors)} делегатов"
                else:
                    logger.error(f"  ✗ Не удалось восстановить делегатов для {mailbox_alias}")
                    mailbox_detail["error"] = "Ошибка восстановления делегатов"
            else:
                logger.info("  Списки делегатов идентичны, пропуск операции")
                mailbox_detail["actors_restored"] = True
        
        # Сценарий 2: Удаление делегатов и отключение делегирования
        elif target_delegation_enabled is False and target_actors is not None and len(target_actors) == 0:
            logger.info("  Сценарий: Удаление делегатов и отключение делегирования")
            
            # Шаг 1: Удаляем всех делегатов (если есть)
            if len(current_actors) > 0:
                logger.info(f"  Удаление всех делегатов для {mailbox_alias}...")
                task_ids = remove_all_mailbox_actors(settings, resource_id, current_actors)
                
                if task_ids and len(task_ids) == len(current_actors):
                    logger.info(f"  ✓ Все делегаты успешно удалены для {mailbox_alias}")
                    mailbox_detail["actors_restored"] = True
                elif task_ids:
                    logger.warning(f"  ⚠ Частично удалены делегаты для {mailbox_alias}: {len(task_ids)}/{len(current_actors)}")
                    mailbox_detail["actors_restored"] = "partial"
                    mailbox_detail["error"] = f"Удалено только {len(task_ids)} из {len(current_actors)} делегатов"
                else:
                    logger.error(f"  ✗ Не удалось удалить делегатов для {mailbox_alias}")
                    mailbox_detail["error"] = "Ошибка удаления делегатов"
            else:
                logger.info("  Список делегатов уже пуст, пропуск операции")
                mailbox_detail["actors_restored"] = True
            
            # Шаг 2: Выключаем делегирование (если включено)
            if current_delegation_enabled:
                logger.info(f"  Выключение делегирования для {mailbox_alias}...")
                result = disable_mailbox_delegation(settings, resource_id)
                if result: 
                    logger.info(f"  ✓ Делегирование успешно выключено для {mailbox_alias}")
                    mailbox_detail["delegation_restored"] = True
                else:
                    logger.error(f"  ✗ Не удалось выключить делегирование для {mailbox_alias}")
                    mailbox_detail["error"] = "Ошибка выключения делегирования для ящика с resourceId={resource_id}"
            else:
                logger.info("  Делегирование уже выключено, пропуск операции")
                mailbox_detail["delegation_restored"] = True
        
        # Обработка только изменения delegation_enabled без изменения actors
        elif target_delegation_enabled is not None and target_actors is None:
            logger.info("  Сценарий: Изменение только статуса делегирования")
            
            if target_delegation_enabled != current_delegation_enabled:
                if target_delegation_enabled:
                    result = enable_mailbox_delegation(settings, resource_id)
                    if result:
                        logger.info(f"  ✓ Делегирование успешно включено для {mailbox_alias}")
                        mailbox_detail["delegation_restored"] = True
                    else:
                        logger.error(f"  ✗ Не удалось включить делегирование для {mailbox_alias}")
                        mailbox_detail["error"] = "Ошибка включения делегирования для ящика с resourceId={resource_id}"
                else:
                    result = disable_mailbox_delegation(settings, resource_id)
                    if result:
                        logger.info(f"  ✓ Делегирование успешно выключено для {mailbox_alias}")
                        mailbox_detail["delegation_restored"] = True
                    else:
                        logger.error(f"  ✗ Не удалось выключить делегирование для {mailbox_alias}")
                        mailbox_detail["error"] = "Ошибка выключения делегирования для ящика с resourceId={resource_id}"
            else:
                logger.info("  Статус делегирования уже соответствует целевому, пропуск операции")
                mailbox_detail["delegation_restored"] = True
        
        # Обработка только изменения actors без изменения delegation_enabled
        elif target_delegation_enabled is None and target_actors is not None:
            logger.info("  Сценарий: Изменение только списка делегатов")
            
            if compare_actors_lists(current_actors, target_actors):
                if len(target_actors) == 0:
                    # Удаляем всех
                    task_ids = remove_all_mailbox_actors(settings, resource_id, current_actors)
                else:
                    # Восстанавливаем список
                    task_ids = restore_mailbox_permissions(settings, resource_id, target_actors)
                
                expected_count = len(current_actors) if len(target_actors) == 0 else len(target_actors)
                
                if task_ids and len(task_ids) == expected_count:
                    logger.info(f"  ✓ Делегаты успешно обновлены для {mailbox_alias}")
                    mailbox_detail["actors_restored"] = True
                elif task_ids:
                    logger.warning(f"  ⚠ Частично обновлены делегаты для {mailbox_alias}: {len(task_ids)}/{expected_count}")
                    mailbox_detail["actors_restored"] = "partial"
                    mailbox_detail["error"] = f"Обновлено только {len(task_ids)} из {expected_count} делегатов"
                else:
                    logger.error(f"  ✗ Не удалось обновить делегатов для {mailbox_alias}")
                    mailbox_detail["error"] = "Ошибка обновления делегатов"
            else:
                logger.info("  Списки делегатов идентичны, пропуск операции")
                mailbox_detail["actors_restored"] = True
        
        stats["details"].append(mailbox_detail)
    
    # Вычисляем итоговую статистику из деталей по каждому ящику
    stats["processed"] = len(stats["details"])
    stats["errors"] = sum(1 for d in stats["details"] if d.get("error") is not None)
    stats["success"] = stats["processed"] - stats["errors"]
    
    logger.info("="*80)
    logger.info("ЗАВЕРШЕНИЕ ВОССТАНОВЛЕНИЯ РАЗРЕШЕНИЙ")
    logger.info(f"Всего ящиков для восстановления: {stats['total']}")
    logger.info(f"Обработано: {stats['processed']}")
    logger.info(f"Успешно: {stats['success']}")
    logger.info(f"Ошибок: {stats['errors']}")
    logger.info("="*80)
    
    return stats


def append_delegation_status(
    checkpoint_file: str,
    mailbox_alias: str,
    delegation_enabled: bool,
    thread_id: int = 0
) -> bool:
    """
    Добавляет информацию о статусе делегирования почтового ящика в checkpoint файл.
    Открывает файл в режиме append, записывает данные и закрывает файл для сброса буфера.
    
    Args:
        checkpoint_file: Путь к checkpoint файлу
        mailbox_alias: Email почтового ящика
        delegation_enabled: Флаг, включено ли делегирование для ящика
        thread_id: Идентификатор потока для логирования
        
    Returns:
        bool: True если успешно, False в случае ошибки
        
    File format:
        <mailbox_alias>|delegation_enabled=<true/false>
    """
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    try:
        # Открываем файл в режиме append, записываем и сразу закрываем
        with open(checkpoint_file, 'a', encoding='utf-8') as f:
            delegation_status = "true" if delegation_enabled else "false"
            f.write(f"{mailbox_alias}|delegation_enabled={delegation_status}\n")
            # Файл автоматически закроется при выходе из блока with, буфер будет сброшен
        
        logger.info(f"{thread_prefix}Статус делегирования для ящика {mailbox_alias} сохранен (enabled={delegation_status})")
        
        return True
        
    except Exception as e:
        logger.error(f"{thread_prefix}Ошибка при сохранении статуса делегирования ящика {mailbox_alias}: {str(e)}")
        return False


def append_mailbox_actors(
    checkpoint_file: str,
    mailbox_alias: str,
    actors: list,
    thread_id: int = 0
) -> bool:
    """
    Добавляет информацию о делегатах и их разрешениях в checkpoint файл.
    Открывает файл в режиме append, записывает данные и закрывает файл для сброса буфера.
    
    Args:
        checkpoint_file: Путь к checkpoint файлу
        mailbox_alias: Email почтового ящика
        actors: Список делегатов с их разрешениями
        thread_id: Идентификатор потока для логирования
        
    Returns:
        bool: True если успешно, False в случае ошибки
        
    File format:
        <mailbox_alias>|actors=<json_dump_of_actors>
    """
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    try:
        # Открываем файл в режиме append, записываем и сразу закрываем
        with open(checkpoint_file, 'a', encoding='utf-8') as f:
            actors_json = json.dumps(actors, ensure_ascii=False)
            f.write(f"{mailbox_alias}|actors={actors_json}\n")
            # Файл автоматически закроется при выходе из блока with, буфер будет сброшен
        
        logger.info(f"{thread_prefix}Список делегатов для ящика {mailbox_alias} сохранен ({len(actors)} записей)")
        
        return True
        
    except Exception as e:
        logger.error(f"{thread_prefix}Ошибка при сохранении списка делегатов ящика {mailbox_alias}: {str(e)}")
        return False

async def select_method_for_delete_messages(
    delegated_mailbox_alias: str,
    delegate_alias: str,
    messages_to_delete: list,
    org_domain: str,
    settings: "SettingParams",
    thread_id: int = 0,
    checkpoint_file: Optional[str] = None,
    checkout_file: Optional[str] = None,
    report_file: Optional[str] = None
):
    """
    Определяет и выполняет подходящий метод удаления сообщений для ящика.
    
    Выбирает между режимами delegate, service_application или skip
    в зависимости от типа ящика и настроек run_mode.
    
    Args:
        delegated_mailbox_alias: Email целевого почтового ящика
        delegate_alias: Алиас делегата
        messages_to_delete: Список сообщений для удаления
        org_domain: Домен организации
        settings: Объект настроек
        thread_id: ID потока для логирования
        checkpoint_file: Путь к файлу checkin
        checkout_file: Путь к файлу checkout
        report_file: Путь к файлу отчета
        
    Returns:
        dict: Результат операции с полями success, message, deleted_messages
    """
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    mode = "skip"
    status = ""
    error = ""
    all_users = get_all_api360_users(settings, force=False, suppress_messages=True)
    all_shared_mailboxes = get_all_shared_mailboxes_cached(settings, force=False, suppress_messages=True)
    
    resource_id, resource_type, isEnabled = get_resource_id_by_email(settings, all_users, all_shared_mailboxes, delegated_mailbox_alias)
    resource_id2, resource_type2, isEnabled2 = get_resource_id_by_email(settings, all_users, all_shared_mailboxes, settings.delegate_alias)
    
    if not resource_id:
        mode = "skip"
        status = f"Ящик {delegated_mailbox_alias} не найден."
        error = "Not found."

    elif resource_id == resource_id2:
        if settings.run_mode in ["service_application", "hybrid"]:
            mode = "service_application"
        else:
            print(f"Ящик {delegated_mailbox_alias} изза совпадения с алисом делегата обработать в режиме 'delegated' нельзя. Пропускаем ящик.")
            mode = "skip"
            status = f"Ящик {delegated_mailbox_alias} из-за совпадения с алисом делегата обработать в режиме 'delegated' нельзя. Пропускаем ящик."
            error = "Same delegate alias and mailbox alias. Can't use service application."
    else:
        # Определяем режим работы для ящика
        if resource_type == "shared_mailbox":
            if settings.run_mode == "service_application":
                mode = "skip"
                status = f"Ящик {delegated_mailbox_alias} является общим ящиком. Использовать сервисное приложение невозможно."
                error = "Shared mailbox. Can't use service application."
            else:
                mode = "delegate"
        else:
            if settings.run_mode == "delegate":
                mode = "delegate"
            else:
                if isEnabled:
                    if settings.run_mode in ["service_application", "hybrid"]:
                        mode = "service_application"
                    else:
                        mode = "delegate"
                else:
                    if settings.run_mode == "hybrid":
                        # Ящик заблокирован, работаем в режиме делегата
                        mode = "delegate"
                    else:
                        # Если ящик заблокрован и режим работы только через сервисное приложение, пропускаем ящик
                        mode = "skip"
                        status = f"Ящик {delegated_mailbox_alias} заблокирован. Использовать сервисное приложение невозможно."
                        error = "User blocked. Can't use service application."

    if mode == "skip":
        if report_file:
            def normalize_message_id(value: str) -> str:
                return value.replace("<", "").replace(">", "").strip()
            
            message_meta = {
                normalize_message_id(msg.get("message_id", "")): msg
                for msg in messages_to_delete
            }
            
            for msg in messages_to_delete:
                msg_id = msg.get("message_id", "")
                append_report_record(
                    report_file=report_file,
                    thread_id=thread_id,
                    email=delegated_mailbox_alias,
                    mailbox_type=resource_type,
                    status=status,
                    folder="",
                    message_id=msg_id,
                    message_date=msg.get("message_date", ""),
                    time_shift=msg.get("days_diff", ""),
                    dry_run="true" if settings.dry_run else "false",
                    error=error
                )
        else:
            logger.debug(f"{thread_prefix}Файл отчета не указан, пропускаем запись результатов удаления")
        return {
            "success": False,
            "message": status,
            "deleted_messages": {}
        }
    else:
        if mode == "delegate":
            return await temporary_delegate_and_delete_messages(
                delegated_mailbox_alias=delegated_mailbox_alias, 
                delegate_alias=delegate_alias, 
                messages_to_delete=messages_to_delete, 
                org_domain=org_domain, 
                settings=settings, 
                thread_id=thread_id, 
                checkpoint_file=checkpoint_file, 
                checkout_file=checkout_file, 
                report_file=report_file)
        else:
            return await delete_messages_by_service_application(
                delegated_mailbox_alias=delegated_mailbox_alias, 
                delegate_alias=delegate_alias, 
                messages_to_delete=messages_to_delete, 
                org_domain=org_domain, 
                settings=settings, 
                thread_id=thread_id, 
                report_file=report_file)


async def temporary_delegate_and_delete_messages(
    delegated_mailbox_alias: str,
    delegate_alias: str,
    messages_to_delete: list,
    org_domain: str,
    settings: "SettingParams",
    thread_id: int = 0,
    checkpoint_file: Optional[str] = None,
    checkout_file: Optional[str] = None,
    report_file: Optional[str] = None
):
    """
    Временно назначает права делегату в делегированный почтовый ящик и удаляет сообщения через IMAP.
    
    Функция выполняет следующие шаги:
    1. Находит делегированный почтовый ящик в Яндекс 360
    2. Проверяет статус делегирования для ящика
    3. Если делегирование выключено - включает его (и запоминает для последующего выключения)
    4. Получает список делегатов и проверяет права нужного делегата
    5. Если нужных прав нет - добавляет shared_mailbox_owner к существующим правам
    6. Ожидает завершения операции изменения прав (taskId)
    7. Подключается по IMAP с basic auth и удаляет сообщения
    8. Восстанавливает исходные права доступа
    9. Если делегирование было выключено - выключает его обратно
    
    Args:
        delegated_mailbox_alias: str
            Алиас делегированного почтового ящика (например, "office@example.ru").
        delegate_alias: str
            Алиас делегата (логин пользователя без домена, например "i.petrov").
        messages_to_delete: list
            Список словарей с сообщениями для удаления. Каждый словарь должен содержать ключи:
                - message_id: str — идентификатор сообщения (Message-ID)
                - message_date: str — дата сообщения в формате "DD-MM-YYYY"
                - days_diff: int — допустимое отклонение в днях для поиска сообщения (обычно ±1 день)
        org_domain: str
            Домен организации (например, "example.ru").
        settings: SettingParams
            Объект с параметрами конфигурации (например, oauth_token, organization_id, check_dir и др.).
        thread_id: int, optional
            Идентификатор потока для логирования (по умолчанию 0).
        checkpoint_file: Optional[str], optional
            Путь к checkpoint-файлу для сохранения исходного состояния почтового ящика (до изменений).
        checkout_file: Optional[str], optional
            Путь к checkout-файлу для сохранения состояния после восстановления прав (по окончании работы).
        report_file: Optional[str], optional
            Путь к файлу отчёта, в который будут записаны результаты удаления сообщений.
        
    Returns:
        dict: Словарь с результатами операции:
            - success: True/False
            - message: Описание результата
            - deleted_messages: Словарь с результатами удаления сообщений
            
    Example:
        result = await temporary_delegate_and_delete_messages(
            delegated_mailbox_alias="office@example.ru",
            delegate_alias="i.petrov",
            messages_to_delete=[
                {
                    "message_id": "<msg1@example.com>",
                    "message_date": "15-11-2024",
                    "days_diff": 1
                },
                {
                    "message_id": "<msg2@example.com>",
                    "message_date": "16-11-2024",
                    "days_diff": 2
                }
            ],
            org_domain="example.ru",
            settings=settings,
            thread_id=1
        )
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.info(f"{thread_prefix}" + "=" * 80)
    logger.info(f"{thread_prefix}Начало обработки делегированного ящика: {delegated_mailbox_alias}")
    logger.info(f"{thread_prefix}Делегат: {delegate_alias}")
    logger.info(f"{thread_prefix}Количество сообщений для удаления: {len(messages_to_delete)}")
    logger.info(f"{thread_prefix}" + "=" * 80)
    
    result = {
        "success": False,
        "message": "",
        "deleted_messages": {}
    }
    
    original_actors = []
    resource_id = None
    delegate_user_id = None
    delegation_was_enabled = False
    delegation_was_enabled_by_us = False
    delegate_original_roles = None  # Исходные роли делегата (None если его не было)
    has_owner_permission = False  # Инициализируем заранее для блока except

    all_users = get_all_api360_users(settings, force=False, suppress_messages=False)
    all_shared_mailboxes = get_all_shared_mailboxes_cached(settings, force=False, suppress_messages=False)
    
    try:
        # Шаг 1: Находим делегированный почтовый ящик
        logger.info(f"{thread_prefix}Шаг 1: Поиск делегированного почтового ящика...")
        resource_id, resource_type, isEnabled = get_resource_id_by_email(settings, all_users, all_shared_mailboxes, delegated_mailbox_alias)
        
        if not resource_id:
            result["message"] = f"Делегированный ящик {delegated_mailbox_alias} не найден"
            logger.error(f"{thread_prefix}{result['message']}")
            return result
        logger.info(f"{thread_prefix}Делегированный ящик найден: resourceId={resource_id}")
        mailbox_type = "shared_mailbox" if resource_type == "shared_mailbox" else "user_mailbox"
        if resource_type == "shared_mailbox":
            logger.info(f"{thread_prefix}Обнаружен общий ящик (shared_mailbox), включение/выключение делегирования не требуется")
        
        # Шаг 2: Находим делегата
        logger.info(f"{thread_prefix}Шаг 2: Поиск делегата...")
        
        # Формируем полный email делегата для поиска
        delegate_email = f"{delegate_alias}@{org_domain}"
        delegate_user_id, delegate_user_type, isEnabled = get_resource_id_by_email(settings, all_users, all_shared_mailboxes,delegate_email)
        
        if not delegate_user_id:
            result["message"] = f"Делегат {delegate_email} не найден"
            logger.error(f"{thread_prefix}{result['message']}")
            return result
        
        logger.info(f"{thread_prefix}Делегат найден: actorId={delegate_user_id}")
        
        # Шаг 3: Проверяем, включено ли делегирование для ящика (кроме shared_mailbox)
        if resource_type == "shared_mailbox":
            logger.info(f"{thread_prefix}Шаг 3: Для shared_mailbox статус делегирования не проверяется")
            delegation_was_enabled = True
        else:
            logger.info(f"{thread_prefix}Шаг 3: Проверка статуса делегирования для ящика...")
            all_delegated = get_all_delegated_mailboxes(settings, thread_id=thread_id)
            
            if all_delegated is None:
                result["message"] = "Не удалось получить список делегированных ящиков"
                logger.error(f"{thread_prefix}{result['message']}")
                return result
            
            # Проверяем, есть ли наш ящик в списке делегированных
            delegation_was_enabled = any(
                mailbox.get('resourceId') == str(resource_id) 
                for mailbox in all_delegated
            )
            
            if delegation_was_enabled:
                logger.info(f"{thread_prefix}Делегирование для ящика {delegated_mailbox_alias} УЖЕ включено")
            else:
                logger.info(f"{thread_prefix}Делегирование для ящика {delegated_mailbox_alias} НЕ включено")
                logger.info(f"{thread_prefix}Включение делегирования...")
                
                enable_result = enable_mailbox_delegation(settings, resource_id, thread_id)
                if not enable_result:
                    result["message"] = "Не удалось включить делегирование для ящика"
                    logger.error(f"{thread_prefix}{result['message']}")
                    return result
                
                delegation_was_enabled_by_us = True
                logger.info(f"{thread_prefix}Делегирование успешно включено")
            
            # Сохраняем статус делегирования сразу после получения данных о нём
            if checkpoint_file:
                logger.info(f"{thread_prefix}Сохранение статуса делегирования в checkpoint файл...")
                success = append_delegation_status(
                    checkpoint_file=checkpoint_file,
                    mailbox_alias=delegated_mailbox_alias,
                    delegation_enabled=delegation_was_enabled,
                    thread_id=thread_id
                )
                if not success:
                    logger.warning(f"{thread_prefix}Не удалось сохранить статус делегирования в checkpoint файл")
            else:
                logger.debug(f"{thread_prefix}Checkpoint файл не указан, пропускаем сохранение статуса делегирования")
        
        # Шаг 4: Получаем список делегатов (только если делегирование включено)
        logger.info(f"{thread_prefix}Шаг 4: Получение списка делегатов...")
        
        if delegation_was_enabled:
            # Ящик уже был делегированным, получаем список
            original_actors = get_mailbox_actors(settings, resource_id, thread_id=thread_id)
            
            if original_actors is None:
                result["message"] = f"Не удалось получить список доступа для ящика {delegated_mailbox_alias}"
                logger.error(f"{thread_prefix}{result['message']}")
                return result
            
            logger.info(f"{thread_prefix}Оригинальный список доступа получен ({len(original_actors)} записей)")
        else:
            # Делегирование только что включено, список пустой
            original_actors = []
            logger.info(f"{thread_prefix}Оригинальный список доступа пуст (делегирование было выключено)")
        
        # Сохраняем список делегатов сразу после получения данных о них
        if checkpoint_file:
            logger.info(f"{thread_prefix}Сохранение списка делегатов в checkpoint файл...")
            success = append_mailbox_actors(
                checkpoint_file=checkpoint_file,
                mailbox_alias=delegated_mailbox_alias,
                actors=original_actors,
                thread_id=thread_id
            )
            if not success:
                logger.warning(f"{thread_prefix}Не удалось сохранить список делегатов в checkpoint файл")
        else:
            logger.debug(f"{thread_prefix}Checkpoint файл не указан, пропускаем сохранение списка делегатов")
        
        # Шаг 5: Проверяем права делегата и сохраняем его исходные роли
        logger.info(f"{thread_prefix}Шаг 5: Проверка прав делегата...")
        
        delegate_in_list = None
        # has_owner_permission уже объявлен выше
        
        for actor in original_actors:
            if actor.get('actorId') == str(delegate_user_id):
                delegate_in_list = actor
                roles = actor.get('roles', [])
                # Сохраняем исходные роли делегата
                delegate_original_roles = roles.copy() if roles else []
                if 'shared_mailbox_owner' in roles:
                    has_owner_permission = True
                logger.info(f"{thread_prefix}Делегат найден в списке с правами: {', '.join(roles)}")
                logger.debug(f"{thread_prefix}Сохранены исходные роли делегата: {', '.join(delegate_original_roles)}")
                break
        
        # Если делегата не было в списке, delegate_original_roles остается None
        if delegate_in_list is None:
            logger.info(f"{thread_prefix}Делегат отсутствует в исходном списке доступа")
            logger.debug(f"{thread_prefix}Исходные роли делегата: отсутствуют (делегата не было)")
        
        if has_owner_permission:
            logger.info(f"{thread_prefix}Делегат уже имеет право shared_mailbox_owner, дополнительные действия не требуются")
            # Можно сразу переходить к удалению
        else:
            # Нужно добавить или изменить права
            logger.info(f"{thread_prefix}Шаг 6: Добавление/изменение прав делегату...")
            
            if delegate_in_list:
                # Делегат есть в списке, но нет нужного права
                logger.info(f"{thread_prefix}Делегат в списке, но без права shared_mailbox_owner. Добавляем право...")
                current_roles = delegate_in_list.get('roles', [])
                new_roles = list(set(current_roles + ['shared_mailbox_owner']))
            else:
                # Делегата нет в списке
                logger.info(f"{thread_prefix}Делегат отсутствует в списке. Добавляем с правом shared_mailbox_owner...")
                new_roles = ['shared_mailbox_owner']
            
            logger.debug(f"{thread_prefix}Новые роли для делегата: {', '.join(new_roles)}")
            
            task_id = set_mailbox_permissions(
                settings,
                resource_id=resource_id,
                actor_id=delegate_user_id,
                roles=new_roles,
                notify="none",
                thread_id=thread_id
            )
            
            if not task_id:
                result["message"] = "Не удалось инициировать задачу на назначение прав"
                logger.error(f"{thread_prefix}{result['message']}")
                return result
            
            logger.debug(f"{thread_prefix}Задача на назначение прав создана: taskId={task_id}")
            
            # Шаг 7: Ожидаем завершения операции изменения прав
            logger.info(f"{thread_prefix}Шаг 7: Ожидание завершения изменения прав...")
            task_result = await wait_for_task_completion(settings, task_id, thread_id=thread_id)
            
            if not task_result:
                result["message"] = "Задача на изменение прав не завершилась успешно"
                logger.error(f"{thread_prefix}{result['message']}")
                return result
            
            logger.info(f"{thread_prefix}Права успешно назначены")
        
        # Шаг 8: Подключаемся по IMAP и удаляем сообщения
        logger.info(f"{thread_prefix}Шаг 8: Удаление сообщений через IMAP...")
        
        # Извлекаем только имя ящика без домена
        mailbox_name = delegated_mailbox_alias.split('@')[0]
        
        deleted_results = await delete_messages_via_imap_basic_auth(
            delegate_alias=delegate_alias,
            delegated_mailbox_alias=mailbox_name,
            org_domain=org_domain,
            mode="delegate",
            messages_to_delete=messages_to_delete,
            settings=settings,
            thread_id=thread_id
        )
        
        result["deleted_messages"] = deleted_results
        logger.info(f"{thread_prefix}Обработано сообщений: {len(deleted_results)}")
        
        # Записываем результаты удаления в файл отчета
        if report_file:
            def normalize_message_id(value: str) -> str:
                return value.replace("<", "").replace(">", "").strip()
            
            message_meta = {
                normalize_message_id(msg.get("message_id", "")): msg
                for msg in messages_to_delete
            }
            
            if not deleted_results:
                for msg in messages_to_delete:
                    msg_id = msg.get("message_id", "")
                    append_report_record(
                        report_file=report_file,
                        thread_id=thread_id,
                        email=delegated_mailbox_alias,
                        mailbox_type=mailbox_type,
                        status="not found",
                        folder="",
                        message_id=msg_id,
                        message_date=msg.get("message_date", ""),
                        time_shift=msg.get("days_diff", ""),
                        dry_run="true" if settings.dry_run else "false",
                        error=""
                    )
            else:
                for msg_id, msg_data in deleted_results.items():
                    normalized_msg_id = normalize_message_id(msg_id)
                    msg_meta = message_meta.get(normalized_msg_id, {})
                    message_date = msg_meta.get("message_date", "")
                    time_shift = msg_meta.get("days_diff", "")
                    
                    if isinstance(msg_data, dict):
                        status_text = msg_data.get("status", "")
                        folders = msg_data.get("folders", [])
                    else:
                        status_text = str(msg_data)
                        folders = []
                    
                    status_lower = status_text.lower()
                    is_fail = ("ошибка" in status_lower) or ("error" in status_lower)
                    status = "fail" if is_fail else "success"
                    error_text = status_text if is_fail else ""
                    dry_run_value = "true" if settings.dry_run else "false"
                    folder_value = ",".join(folders) if folders else ""
                    
                    append_report_record(
                        report_file=report_file,
                        thread_id=thread_id,
                        email=delegated_mailbox_alias,
                        mailbox_type=mailbox_type,
                        status=status,
                        folder=folder_value,
                        message_id=msg_id,
                        message_date=message_date,
                        time_shift=time_shift,
                        dry_run=dry_run_value,
                        error=error_text
                    )
        else:
            logger.debug(f"{thread_prefix}Файл отчета не указан, пропускаем запись результатов удаления")
        
        # Шаг 9: Восстанавливаем исходные права делегата (только если что-то меняли)
        if not has_owner_permission:
            logger.info(f"{thread_prefix}Шаг 9: Восстановление исходных прав делегата...")
            
            if delegate_original_roles is not None:
                # Делегат был в списке, восстанавливаем его исходные роли
                logger.info(f"{thread_prefix}Восстановление исходных ролей делегата: {', '.join(delegate_original_roles) if delegate_original_roles else 'нет ролей'}")
                restore_task_id = set_mailbox_permissions(
                    settings,
                    resource_id=resource_id,
                    actor_id=delegate_user_id,
                    roles=delegate_original_roles,
                    notify="none",
                    thread_id=thread_id
                )
            else:
                # Делегата не было в списке, удаляем его (передаем пустой список)
                logger.info(f"{thread_prefix}Удаление делегата из списка доступа (его не было изначально)")
                restore_task_id = set_mailbox_permissions(
                    settings,
                    resource_id=resource_id,
                    actor_id=delegate_user_id,
                    roles=[],
                    notify="none",
                    thread_id=thread_id
                )
            
            if restore_task_id:
                logger.debug(f"{thread_prefix}Ожидание завершения задачи восстановления прав...")
                restore_result = await wait_for_task_completion(settings, restore_task_id, thread_id=thread_id)
                
                if restore_result:
                    logger.info(f"{thread_prefix}Права делегата успешно восстановлены")
                    
                    # Сохраняем состояние после восстановления прав в checkout файл
                    if checkout_file:
                        logger.info(f"{thread_prefix}Сохранение состояния делегатов после восстановления в checkout файл...")
                        restored_actors = get_mailbox_actors(settings, resource_id, thread_id=thread_id)
                        
                        if restored_actors is not None:
                            success = append_mailbox_actors(
                                checkpoint_file=checkout_file,
                                mailbox_alias=delegated_mailbox_alias,
                                actors=restored_actors,
                                thread_id=thread_id
                            )
                            if not success:
                                logger.warning(f"{thread_prefix}Не удалось сохранить восстановленный список делегатов в checkout файл")
                        else:
                            logger.warning(f"{thread_prefix}Не удалось получить список делегатов после восстановления")
                    else:
                        logger.debug(f"{thread_prefix}Checkout файл не указан, пропускаем сохранение восстановленного состояния")
                else:
                    logger.warning(f"{thread_prefix}Не удалось подтвердить восстановление прав делегата")
            else:
                logger.error(f"{thread_prefix}Не удалось инициировать задачу восстановления прав")
        else:
            logger.info(f"{thread_prefix}Шаг 9: Восстановление прав не требуется (права не изменялись)")
            
            # Сохраняем текущее состояние в checkout файл, даже если права не изменялись
            if checkout_file:
                logger.info(f"{thread_prefix}Сохранение текущего состояния делегатов в checkout файл...")
                current_actors = get_mailbox_actors(settings, resource_id, thread_id=thread_id)
                
                if current_actors is not None:
                    success = append_mailbox_actors(
                        checkpoint_file=checkout_file,
                        mailbox_alias=delegated_mailbox_alias,
                        actors=current_actors,
                        thread_id=thread_id
                    )
                    if not success:
                        logger.warning(f"{thread_prefix}Не удалось сохранить текущий список делегатов в checkout файл")
                else:
                    logger.warning(f"{thread_prefix}Не удалось получить список делегатов")
            else:
                logger.debug(f"{thread_prefix}Checkout файл не указан, пропускаем сохранение текущего состояния")
        
        # Шаг 10: Выключаем делегирование, если мы его включали
        if resource_type == "shared_mailbox":
            logger.info(f"{thread_prefix}Шаг 10: Для shared_mailbox делегирование не переключается")
        elif delegation_was_enabled_by_us:
            logger.info(f"{thread_prefix}Шаг 10: Выключение делегирования (было включено нами)...")
            disable_result = disable_mailbox_delegation(settings, resource_id, thread_id)
            
            if disable_result:
                logger.info(f"{thread_prefix}Делегирование успешно выключено для ящика с resourceId={resource_id}")
                
                # Сохраняем статус делегирования после выключения в checkout файл
                if checkout_file:
                    logger.info(f"{thread_prefix}Сохранение статуса делегирования после выключения в checkout файл...")
                    # Не проверяем текущий статус делегирования, т.к. он уже выключен (для ускорения работы)
                    success = append_delegation_status(
                            checkpoint_file=checkout_file,
                            mailbox_alias=delegated_mailbox_alias,
                            delegation_enabled=False,
                            thread_id=thread_id
                        )
                    if not success:
                        logger.warning(f"{thread_prefix}Не удалось сохранить статус делегирования после выключения в checkout файл")
                    
                    # После выключения делегирования проверяем текущий статус
                    # all_delegated_after = get_all_delegated_mailboxes(settings, thread_id=thread_id)
                    
                    # if all_delegated_after is not None:
                    #     # Проверяем, есть ли наш ящик в списке делегированных после выключения
                    #     delegation_after_restore = any(
                    #         mailbox.get('resourceId') == str(resource_id) 
                    #         for mailbox in all_delegated_after
                    #     )
                        
                    #     success = append_delegation_status(
                    #         checkpoint_file=checkout_file,
                    #         mailbox_alias=delegated_mailbox_alias,
                    #         delegation_enabled=delegation_after_restore,
                    #         thread_id=thread_id
                    #     )
                    #     if not success:
                    #         logger.warning(f"{thread_prefix}Не удалось сохранить статус делегирования после выключения в checkout файл")
                    # else:
                    #     logger.warning(f"{thread_prefix}Не удалось получить список делегированных ящиков после выключения")
                else:
                    logger.debug(f"{thread_prefix}Checkout файл не указан, пропускаем сохранение статуса делегирования для ящика с resourceId={resource_id}")
            else:
                logger.warning(f"{thread_prefix}Не удалось выключить делегирование для ящика с resourceId={resource_id} (но сообщения удалены)")
        else:
            logger.info(f"{thread_prefix}Шаг 10: Делегирование не выключаем (было включено ранее) для ящика с resourceId={resource_id}")
            
            # Сохраняем текущий статус делегирования в checkout файл
            if checkout_file:
                logger.info(f"{thread_prefix}Сохранение текущего статуса делегирования в checkout файл...")
                all_delegated_current = get_all_delegated_mailboxes(settings, thread_id=thread_id)
                
                if all_delegated_current is not None:
                    # Проверяем текущий статус делегирования
                    delegation_current = any(
                        mailbox.get('resourceId') == str(resource_id) 
                        for mailbox in all_delegated_current
                    )
                    
                    success = append_delegation_status(
                        checkpoint_file=checkout_file,
                        mailbox_alias=delegated_mailbox_alias,
                        delegation_enabled=delegation_current,
                        thread_id=thread_id
                    )
                    if not success:
                        logger.warning(f"{thread_prefix}Не удалось сохранить текущий статус делегирования в checkout файл")
                else:
                    logger.warning(f"{thread_prefix}Не удалось получить список делегированных ящиков")
            else:
                logger.debug(f"{thread_prefix}Checkout файл не указан, пропускаем сохранение статуса делегирования")
        
        result["success"] = True
        result["message"] = f"Успешно обработан ящик {delegated_mailbox_alias}"
        logger.info(f"{thread_prefix}" + "=" * 80)
        logger.info(f"{thread_prefix}Обработка завершена успешно")
        logger.info(f"{thread_prefix}" + "=" * 80)
        
    except Exception as e:
        error_msg = f"Критическая ошибка: {type(e).__name__}: {e}"
        logger.error(f"{thread_prefix}{error_msg}")
        logger.error(f"{thread_prefix}Детали: at line {e.__traceback__.tb_lineno} of {__file__}")
        result["message"] = error_msg
        
        # Пытаемся восстановить состояние в случае ошибки
        if resource_id and delegate_user_id:
            logger.warning(f"{thread_prefix}Попытка восстановления состояния после ошибки...")
            
            # Восстанавливаем права делегата, если мы их меняли
            if not has_owner_permission:
                try:
                    logger.info(f"{thread_prefix}Восстановление прав делегата...")
                    
                    if delegate_original_roles is not None:
                        # Восстанавливаем исходные роли
                        logger.debug(f"{thread_prefix}Восстановление исходных ролей: {', '.join(delegate_original_roles) if delegate_original_roles else 'нет ролей'}")
                        restore_task_id = set_mailbox_permissions(
                            settings,
                            resource_id=resource_id,
                            actor_id=delegate_user_id,
                            roles=delegate_original_roles,
                            notify="none",
                            thread_id=thread_id
                        )
                    else:
                        # Удаляем делегата (его не было)
                        logger.debug(f"{thread_prefix}Удаление делегата (его не было изначально)")
                        restore_task_id = set_mailbox_permissions(
                            settings,
                            resource_id=resource_id,
                            actor_id=delegate_user_id,
                            roles=[],
                            notify="none",
                            thread_id=thread_id
                        )
                    
                    if restore_task_id:
                        await wait_for_task_completion(settings, restore_task_id, thread_id=thread_id)
                        logger.info(f"{thread_prefix}Права делегата восстановлены после ошибки")
                    else:
                        logger.warning(f"{thread_prefix}Не удалось инициировать восстановление прав после ошибки")
                        
                except Exception as restore_error:
                    logger.error(f"{thread_prefix}Не удалось восстановить права после ошибки: {restore_error}")
            
            # Выключаем делегирование, если мы его включали
            if delegation_was_enabled_by_us:
                try:
                    logger.info(f"{thread_prefix}Выключение делегирования...")
                    disable_mailbox_delegation(settings, resource_id, thread_id)
                    logger.info(f"{thread_prefix}Делегирование выключено после ошибки")
                except Exception as disable_error:
                    logger.error(f"{thread_prefix}Не удалось выключить делегирование после ошибки: {disable_error}")
    
    return result

async def delete_messages_by_service_application(
    delegated_mailbox_alias: str,
    delegate_alias: str,
    messages_to_delete: list,
    org_domain: str,
    settings: "SettingParams",
    thread_id: int = 0,
    report_file: Optional[str] = None
):
    """
    Удаляет сообщения через IMAP с использованием токена сервисного приложения.
    
    В отличие от режима делегирования, не требует временного назначения прав.
    Использует OAuth XOAUTH2 авторизацию через сервисное приложение.
    
    Args:
        delegated_mailbox_alias: Email целевого почтового ящика
        delegate_alias: Алиас делегата (не используется в этом режиме)
        messages_to_delete: Список сообщений для удаления
        org_domain: Домен организации
        settings: Объект настроек с application_client_id и secret
        thread_id: ID потока для логирования
        report_file: Путь к файлу отчета
        
    Returns:
        dict: Результат операции с полями success, message, deleted_messages
    """
    # Формируем префикс для логов
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    
    logger.info(f"{thread_prefix}" + "=" * 80)
    logger.info(f"{thread_prefix}Начало обработки ящика с помощью сервисного приложения: {delegated_mailbox_alias}")
    logger.info(f"{thread_prefix}Количество сообщений для удаления: {len(messages_to_delete)}")
    logger.info(f"{thread_prefix}" + "=" * 80)
    
    result = {
        "success": False,
        "message": "",
        "deleted_messages": {}
    }
    
    try:
        # Шаг 1: Подключаемся по IMAP и удаляем сообщения
        logger.info(f"{thread_prefix}Удаление сообщений через IMAP для ящика {delegated_mailbox_alias}...")
        
        # Извлекаем только имя ящика без домена
        if "@" in delegated_mailbox_alias:
            mailbox_name = delegated_mailbox_alias.split('@')[0]
        else:
            mailbox_name = delegated_mailbox_alias
        
        deleted_results = await delete_messages_via_imap_basic_auth(
            delegate_alias=delegate_alias,
            delegated_mailbox_alias=mailbox_name,
            org_domain=org_domain,
            mode="service_application",
            messages_to_delete=messages_to_delete,
            settings=settings,
            thread_id=thread_id
        )
        
        result["deleted_messages"] = deleted_results
        logger.info(f"{thread_prefix}Обработано сообщений: {len(deleted_results)}")
        
        # Записываем результаты удаления в файл отчета
        if report_file:
            def normalize_message_id(value: str) -> str:
                return value.replace("<", "").replace(">", "").strip()
            
            message_meta = {
                normalize_message_id(msg.get("message_id", "")): msg
                for msg in messages_to_delete
            }
            
            if not deleted_results:
                for msg in messages_to_delete:
                    msg_id = msg.get("message_id", "")
                    append_report_record(
                        report_file=report_file,
                        thread_id=thread_id,
                        email=delegated_mailbox_alias,
                        mailbox_type="user_mailbox",
                        status="not found",
                        folder="",
                        message_id=msg_id,
                        message_date=msg.get("message_date", ""),
                        time_shift=msg.get("days_diff", ""),
                        dry_run="true" if settings.dry_run else "false",
                        error=""
                    )
            else:
                for msg_id, msg_data in deleted_results.items():
                    normalized_msg_id = normalize_message_id(msg_id)
                    msg_meta = message_meta.get(normalized_msg_id, {})
                    message_date = msg_meta.get("message_date", "")
                    time_shift = msg_meta.get("days_diff", "")
                    
                    if isinstance(msg_data, dict):
                        status_text = msg_data.get("status", "")
                        folders = msg_data.get("folders", [])
                    else:
                        status_text = str(msg_data)
                        folders = []
                    
                    status_lower = status_text.lower()
                    is_fail = ("ошибка" in status_lower) or ("error" in status_lower)
                    status = "fail" if is_fail else "success"
                    error_text = status_text if is_fail else ""
                    dry_run_value = "true" if settings.dry_run else "false"
                    folder_value = ",".join(folders) if folders else ""
                    
                    append_report_record(
                        report_file=report_file,
                        thread_id=thread_id,
                        email=delegated_mailbox_alias,
                        mailbox_type="user_mailbox",
                        status=status,
                        folder=folder_value,
                        message_id=msg_id,
                        message_date=message_date,
                        time_shift=time_shift,
                        dry_run=dry_run_value,
                        error=error_text
                    )
        else:
            logger.debug(f"{thread_prefix}Файл отчета не указан, пропускаем запись результатов удаления")
        
        result["success"] = True
        result["message"] = f"Успешно обработан ящик {delegated_mailbox_alias}"
        logger.info(f"{thread_prefix}" + "=" * 80)
        logger.info(f"{thread_prefix}Обработка завершена успешно")
        logger.info(f"{thread_prefix}" + "=" * 80)
        
    except Exception as e:
        error_msg = f"Критическая ошибка: {type(e).__name__}: {e}"
        logger.error(f"{thread_prefix}{error_msg}")
        logger.error(f"{thread_prefix}Детали: at line {e.__traceback__.tb_lineno} of {__file__}")
        result["message"] = error_msg
    
    return result

async def process_multiple_mailboxes_parallel(
    mailboxes_data: list,
    settings: "SettingParams",
    checkpoint_file: Optional[str] = None,
    checkout_file: Optional[str] = None,
    report_file: Optional[str] = None
):
    """
    Обрабатывает несколько делегированных почтовых ящиков параллельно.
    Вызывает temporary_delegate_and_delete_messages для каждого ящика асинхронно.
    Количество одновременно выполняемых задач ограничено константой MAX_PARALLEL_THREADS.
    
    Args:
        mailboxes_data: Список словарей с данными для каждого ящика:
            - delegated_mailbox_alias: алиас делегированного ящика (email)
            - delegate_alias: алиас делегата (логин)
            - messages_to_delete: список словарей с информацией о сообщениях (message_id, message_date, days_diff)
            - org_domain: домен организации
            - mode: режим работы для ящика (delegate, service_application, skip)
        settings: Объект настроек
        checkpoint_file: Путь к checkpoint файлу для сохранения состояния почтовых ящиков (до изменений)
        checkout_file: Путь к checkout файлу для сохранения состояния почтовых ящиков (после восстановления)
        report_file: Путь к файлу отчета для записи результатов удаления
        
    Returns:
        list: Список результатов обработки для каждого ящика
        
    Example:
        mailboxes_data = [
            {
                "delegated_mailbox_alias": "office@example.ru",
                "delegate_alias": "i.petrov",
                "messages_to_delete": [
                    {
                        "message_id": "<msg1@example.com>",
                        "message_date": "15-11-2024",
                        "days_diff": 1
                    },
                    {
                        "message_id": "<msg2@example.com>",
                        "message_date": "16-11-2024",
                        "days_diff": 1
                    }
                ],
                "org_domain": "example.ru",
                "mode": "delegate"
            },
            {
                "delegated_mailbox_alias": "sales@example.ru",
                "delegate_alias": "i.ivanov",
                "messages_to_delete": [
                    {
                        "message_id": "<msg3@example.com>",
                        "message_date": "17-11-2024",
                        "days_diff": 2
                    }
                ],
                "org_domain": "example.ru",
                "mode": "delegate"
            }
        ]
        
        results = await process_multiple_mailboxes_parallel(mailboxes_data, settings)
        
        for i, result in enumerate(results):
            print(f"Ящик {mailboxes_data[i]['delegated_mailbox_alias']}: {result['message']}")
    """
    
    if settings.run_mode == "service_application":
        result = check_service_app_status(settings, skip_permissions_check=True)
        if not result:
            logger.error("Сервисное приложение не настрено. Продолжение работы невозможно.")
            return []
    elif settings.run_mode == "hybrid":
        result = check_service_app_status(settings, skip_permissions_check=True)
        if not result:
            logger.error("Сервисное приложение не настрено. Работа в режиме делегирования.")
            settings.run_mode = "delegate"
        else:
            settings.run_mode = "hybrid"
    else:
        env_mode = os.environ.get("RUN_MODE", "delegate")
        if env_mode == "hybrid":
            result = check_service_app_status(settings, skip_permissions_check=True)
            if result:
                settings.run_mode = "hybrid"
    
    logger.info("=" * 100)
    logger.info(f"Начало параллельной обработки {len(mailboxes_data)} делегированных ящиков")
    logger.info(f"Максимальное количество одновременных задач: {MAX_PARALLEL_THREADS}")
    logger.info("=" * 100)
    
    # Фильтруем ящики: пропускаем те, имена которых совпадают с алиасами
    valid_mailboxes = []
    skipped_mailboxes = []

    all_users = get_all_api360_users(settings, force=False)
    if not all_users:
        logger.error("Не удалось получить список всех пользователей. Завершение работы.")
        return []
    
    # Генерируем thread_id для каждого ящика (начиная с 1)
    for idx, mailbox_data in enumerate(mailboxes_data, start=1):
        # Добавляем thread_id к данным ящика
        mailbox_data["thread_id"] = idx
        valid_mailboxes.append(mailbox_data)        
    
    # Логируем результаты фильтрации
    if skipped_mailboxes:
        logger.warning(f"Пропущено ящиков (совпадают с алиасами): {len(skipped_mailboxes)}")
        logger.info(f"Ящиков для обработки: {len(valid_mailboxes)}")

    if not valid_mailboxes:
        logger.warning("Нет ящиков для обработки после фильтрации. Завершение работы.")
        return []
    
    # Создаем семафор для ограничения количества одновременных задач
    semaphore = asyncio.Semaphore(MAX_PARALLEL_THREADS)
    
    async def process_with_semaphore(mailbox_data):
        """Обертка для выполнения задачи с ограничением через семафор"""
        async with semaphore:
            thread_id = mailbox_data.get("thread_id", 0)
            thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
            logger.debug(f"{thread_prefix}Начало обработки ящика {mailbox_data['delegated_mailbox_alias']}")
            result = await select_method_for_delete_messages(
                delegated_mailbox_alias=mailbox_data["delegated_mailbox_alias"],
                delegate_alias=mailbox_data["delegate_alias"],
                messages_to_delete=mailbox_data["messages_to_delete"],
                org_domain=mailbox_data["org_domain"],
                settings=settings,
                thread_id=thread_id,
                checkpoint_file=checkpoint_file,
                checkout_file=checkout_file,
                report_file=report_file
            )
            return result
    
    # Создаем задачи только для валидных ящиков с ограничением через семафор
    tasks = [process_with_semaphore(mailbox_data) for mailbox_data in valid_mailboxes]
    
    # Выполняем все задачи параллельно с ограничением
    if tasks:
        logger.info(f"Запуск {len(tasks)} задач (максимум {MAX_PARALLEL_THREADS} одновременно)...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
    else:
        logger.warning("Нет ящиков для обработки после фильтрации")
        results = []
    
    # Обрабатываем результаты
    logger.info("=" * 100)
    logger.info("Результаты обработки:")
    logger.info("=" * 100)
    
    # Создаем словари для быстрого поиска результатов
    skipped_dict = {skipped["mailbox_data"]["delegated_mailbox_alias"]: skipped for skipped in skipped_mailboxes}
    valid_dict = {valid_mailboxes[i]["delegated_mailbox_alias"]: results[i] for i in range(len(results))}
    
    processed_results = []
    
    # Проходим по исходному списку mailboxes_data, чтобы сохранить порядок
    for mailbox_data in mailboxes_data:
        mailbox_alias = mailbox_data["delegated_mailbox_alias"]
        
        # Проверяем, был ли ящик пропущен
        if mailbox_alias in skipped_dict:
            skipped = skipped_dict[mailbox_alias]
            skipped_result = {
                "success": False,
                "message": skipped["reason"],
                "deleted_messages": {},
                "skipped": True
            }
            logger.warning(f"Ящик {mailbox_alias}: ПРОПУЩЕН - {skipped['reason']}")
            processed_results.append(skipped_result)
        # Иначе ищем в результатах обработанных ящиков
        elif mailbox_alias in valid_dict:
            result = valid_dict[mailbox_alias]
            
            if isinstance(result, Exception):
                error_result = {
                    "success": False,
                    "message": f"Исключение при обработке: {str(result)}",
                    "deleted_messages": {}
                }
                logger.error(f"Ящик {mailbox_alias}: ОШИБКА - {str(result)}")
                processed_results.append(error_result)
            else:
                status = "УСПЕШНО" if result["success"] else "ОШИБКА"
                logger.info(f"Ящик {mailbox_alias}: {status} - {result['message']}")
                processed_results.append(result)
        else:
            # На случай, если что-то пошло не так
            logger.error(f"Ящик {mailbox_alias}: Результат не найден")
            processed_results.append({
                "success": False,
                "message": "Результат обработки не найден",
                "deleted_messages": {}
            })
    
    logger.info("=" * 100)
    logger.info(f"Завершена обработка {len(mailboxes_data)} ящиков")
    logger.info(f"Пропущено (совпадают с алиасами): {len(skipped_mailboxes)}")
    logger.info(f"Обработано: {len(valid_mailboxes)}")
    logger.info(f"Успешно: {sum(1 for r in processed_results if r.get('success') and not r.get('skipped'))}")
    logger.info(f"С ошибками: {sum(1 for r in processed_results if not r.get('success') and not r.get('skipped'))}")
    logger.info("=" * 100)
    
    return processed_results

@dataclass
class SettingParams:
    oauth_token: str
    organization_id: int
    application_client_id: str
    application_client_secret: str
    message_id_file_name: str
    mailboxes_to_search_file_name: str
    dry_run: bool
    search_param : dict
    all_users : list
    all_users_get_timestamp : datetime
    all_shared_mailboxes : list
    all_shared_mailboxes_get_timestamp : datetime
    all_delegate_mailboxes : list
    all_delegate_mailboxes_get_timestamp : datetime
    delegate_alias: str
    delegate_domain: str
    delegate_password: str
    check_dir: str
    mailboxes_list_file: str
    reports_dir: str
    message_ids_file: str
    service_app_api_data_file: str
    run_mode: str
    service_app_status: bool

async def test_delegate_imap_connection(delegate_alias: str, delegate_domain: str, delegate_password: str) -> bool:
    """
    Проверяет подключение к IMAP серверу с учетными данными делегата.
    
    Args:
        delegate_alias: Алиас делегата (например, "i.petrov")
        delegate_domain: Домен организации (например, "example.ru")
        delegate_password: Пароль приложения делегата
        
    Returns:
        bool: True если подключение успешно, False в противном случае
    """
    # Формируем полный email делегата
    delegate_email = f"{delegate_alias}@{delegate_domain}"
    logger.info(f"Проверка подключения к IMAP для делегата {delegate_email}...")
    
    try:
        # Подключаемся к IMAP серверу
        imap_connector = aioimaplib.IMAP4_SSL(host=DEFAULT_IMAP_SERVER, port=DEFAULT_IMAP_PORT, timeout=10)
        await imap_connector.wait_hello_from_server()
        
        # Авторизация через basic auth (login/password)
        logger.debug(f"Авторизация для пользователя {delegate_email}...")
        login_response = await imap_connector.login(delegate_email, delegate_password)
        
        if login_response[0] == 'OK':
            logger.info(f"✓ Успешная авторизация для делегата {delegate_email}")
            # Закрываем соединение
            await imap_connector.logout()
            return True
        else:
            logger.error(f"✗ Неуспешная авторизация для делегата {delegate_email}: {login_response}")
            return False
            
    except asyncio.TimeoutError:
        logger.error(f"✗ Тайм-аут подключения к IMAP серверу для делегата {delegate_email}")
        return False
    except aioimaplib.aioimaplib.IMAP4Error as e:
        logger.error(f"✗ Ошибка IMAP при подключении делегата {delegate_email}: {e}")
        return False
    except Exception as e:
        logger.error(f"✗ Ошибка при проверке подключения делегата {delegate_email}: {type(e).__name__}: {e}")
        logger.error(f"Детали: at line {e.__traceback__.tb_lineno} of {__file__}")
        return False

def check_oauth_token(oauth_token, org_id):
    """
    Проверяет валидность OAuth-токена запросом к API.
    
    Args:
        oauth_token: OAuth-токен для проверки
        org_id: ID организации
        
    Returns:
        bool: True если токен валиден, False в противном случае
    """
    url = f'{DEFAULT_360_API_URL}/directory/v1/org/{org_id}/users?perPage=100'
    headers = {
        'Authorization': f'OAuth {oauth_token}'
    }
    with httpx.Client(headers=headers) as client:
        response = client.get(url)
    if response.status_code == HTTPStatus.OK:
        return True
    return False

def get_settings():
    """
    Создает и валидирует объект настроек из переменных окружения.
    
    Загружает параметры из переменных окружения, проверяет их корректность,
    валидирует OAuth-токен и тестирует IMAP-подключение.
    
    Returns:
        SettingParams: Объект настроек или None при ошибке валидации
    """
    settings = SettingParams (
        oauth_token = os.environ.get("OAUTH_TOKEN_ARG"),
        organization_id = int(os.environ.get("ORGANIZATION_ID_ARG")),
        application_client_id = os.environ.get("APPLICATION_CLIENT_ID_ARG"),
        application_client_secret = os.environ.get("APPLICATION_CLIENT_SECRET_ARG"),
        message_id_file_name = os.environ.get("MESSAGE_ID_FILE_NAME","message_id.txt"),
        mailboxes_to_search_file_name = os.environ.get("MAILBOXES_TO_SEARCH_FILE_NAME","mailboxes_to_search.txt"),
        dry_run = False,
        search_param = {},
        all_users = [],
        all_users_get_timestamp = datetime.now(),
        all_shared_mailboxes = [],
        all_shared_mailboxes_get_timestamp = datetime.now(),
        all_delegate_mailboxes = [],
        all_delegate_mailboxes_get_timestamp = datetime.now(),
        delegate_alias = os.environ.get("DELEGATE_ALIAS", ""),
        delegate_domain = os.environ.get("DELEGATE_DOMAIN", ""),
        delegate_password = os.environ.get("DELEGATE_PASSWORD", ""),
        check_dir = os.environ.get("CHECK_DIR", "mailbox_checkpoints"),
        mailboxes_list_file = os.environ.get("MAILBOXES_LIST_FILE", "mailboxes_list.csv"),
        reports_dir = os.environ.get("REPORTS_DIR", "reports"),
        message_ids_file = os.environ.get("MESSAGE_IDS_FILE", "message-ids.csv"),
        service_app_api_data_file = os.environ.get("SERVICE_APP_API_DATA_FILE", "service_api_data.txt"),
        run_mode = os.environ.get("RUN_MODE", "delegate"),
        service_app_status = False,
    )

    exit_flag = False
    oauth_token_bad = False
    if not settings.oauth_token:
        logger.error("OAUTH_TOKEN_ARG не установлен")
        exit_flag = True

    if settings.organization_id == 0:
        logger.error("ORGANIZATION_ID_ARG не установлен")
        exit_flag = True

    if not (oauth_token_bad or exit_flag):
        hard_error, result_ok = check_token_permissions_simple(settings.oauth_token, settings.organization_id, NEEDED_PERMISSIONS)
        if hard_error:
            logger.error("OAUTH_TOKEN не является действительным или не имеет необходимых прав доступа")
            oauth_token_bad = True
        elif not result_ok:
            print("ВНИМАНИЕ: Функциональность скрипта может быть ограничена. Возможны ошибки при работе с API.")
            print("=" * 100)
            input("Нажмите Enter для продолжения..")

    if settings.run_mode not in ["delegate", "service_application", "hybrid"]:
        logger.error("RUN_MODE должен быть delegate, service_application или hybrid")
        exit_flag = True
    else:
        logger.info(f"RUN_MODE установлен в {settings.run_mode}")

    if settings.run_mode in ["delegate", "hybrid"]:
        # Проверка параметров делегата
        if not settings.delegate_alias:
            logger.error("DELEGATE_ALIAS не установлен")
            exit_flag = True
        if "@" in settings.delegate_alias:
            settings.delegate_alias = settings.delegate_alias.split("@")[0] # remove domain from alias
            logger.info(f"Установлен DELEGATE_ALIAS: {settings.delegate_alias} (домен удален)")
        if not settings.delegate_domain:
            logger.error("DELEGATE_DOMAIN не установлен")
            exit_flag = True
        if not settings.delegate_password:
            logger.error("DELEGATE_PASSWORD не установлен")
            exit_flag = True

    if settings.run_mode in ["service_application","hybrid"]:
        # Проверка параметров сервисного приложения
        if not settings.application_client_id:
            logger.error("APPLICATION_CLIENT_ID не установлен")
            exit_flag = True
        if not settings.application_client_secret:
            logger.error("APPLICATION_CLIENT_SECRET не установлен")
            exit_flag = True

    if os.environ.get("DRY_RUN"):
        if os.environ.get("DRY_RUN").lower() == "true":
            settings.dry_run = True
        elif os.environ.get("DRY_RUN").lower() == "false":
            settings.dry_run = False
        else:
            logger.error("DRY_RUN должен быть true или false")
            exit_flag = True
    else:
        settings.dry_run = False

    if exit_flag or oauth_token_bad:
        return None
    
    if settings.run_mode in ["delegate", "hybrid"]:
        # Проверяем подключение к IMAP с учетными данными делегата
        logger.info("Проверка подключения к IMAP с учетными данными делегата...")
        connection_test = asyncio.run(test_delegate_imap_connection(
            settings.delegate_alias,
            settings.delegate_domain,
            settings.delegate_password
        ))
        
        if not connection_test:
            logger.error("=" * 80)
            logger.error("!!! ОШИБКА: Не удалось подключиться к IMAP с учетными данными делегата !!!")
            logger.error("Проверьте правильность параметров:")
            logger.error(f"  - DELEGATE_ALIAS: {settings.delegate_alias} (алиас делегата)")
            logger.error(f"  - DELEGATE_DOMAIN: {settings.delegate_domain} (домен организации)")
            logger.error(f"  - DELEGATE_PASSWORD: {'*' * len(settings.delegate_password) if settings.delegate_password else '(не задан)'} (пароль делегата)")
            logger.error("=" * 80)
            return None
        else:
            logger.info("=" * 80)
            logger.info("✓ IMAP подключение делегата успешно проверено.")
            logger.info("=" * 80)

    if settings.run_mode in ["service_application", "hybrid"]:
        check_service_app_status(settings, skip_permissions_check=True)
        if not settings.service_app_status:
            logger.error("Сервисное приложение не настроено. Настройте сервисное приложение через меню настроек.")
    
    return settings

def check_token_permissions_simple(token: str, org_id: int, needed_permissions: list) -> tuple[bool, bool]:
    """
    Проверяет наличие необходимых прав у OAuth-токена.
    
    Args:
        token: OAuth-токен для проверки
        org_id: ID организации
        needed_permissions: Список требуемых разрешений
        
    Returns:
        tuple: (hard_error: bool, result_ok: bool)
            - hard_error: True при критической ошибке (невалидный токен)
            - result_ok: True если все права присутствуют
    """
    result, data = check_token_permissions_api(token)
    if not result:
        return True, False
    else:
        try:
            # Извлечение scopes и orgIds из ответа
            token_scopes = data.get('scopes', [])
            token_org_ids = data.get('orgIds', [])
            login = data.get('login', 'unknown')
            
            logger.info(f"Проверка прав доступа для токена пользователя: {login}")
            logger.debug(f"Доступные права: {token_scopes}")
            logger.debug(f"Доступные организации: {token_org_ids}")
            
            # Проверка наличия org_id в списке доступных организаций
            if str(org_id) not in [str(org) for org in token_org_ids]:
                logger.error("=" * 100)
                logger.error(f"ОШИБКА: Токен не имеет доступа к организации с ID {org_id}")
                logger.error(f"Доступные организации для этого токена: {token_org_ids}")
                logger.error("=" * 100)
                return True, False

            # Проверка наличия всех необходимых прав
            missing_permissions = []
            for permission in needed_permissions:
                if permission not in token_scopes:
                    missing_permissions.append(permission)
            
            if missing_permissions:
                logger.error("=" * 100)
                logger.error("ОШИБКА: У токена отсутствуют необходимые права доступа!")
                logger.error("Недостающие права:")
                for perm in missing_permissions:
                    logger.error(f"  - {perm}")
                logger.error("=" * 100)
                return False, False

            logger.info("✓ Все необходимые права доступа присутствуют")
            logger.info(f"✓ Доступ к организации {org_id} подтвержден")
            return False, True
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка при парсинге ответа от API: {e}")
            return False, result
        except Exception as e:
            logger.error(f"Неожиданная ошибка при проверке прав доступа: {type(e).__name__}: {e}")
            return False, result

def check_token_permissions_for_service_application(settings: "SettingParams") -> bool:
    """
    Проверяет наличие прав для управления сервисными приложениями.
    
    Проверяет, что токен выдан личной учетке Яндекс (не организационной)
    и содержит права на чтение и запись сервисных приложений.
    
    Args:
        settings: Объект настроек с oauth_token и organization_id
        
    Returns:
        bool: True если все права присутствуют, False в противном случае
    """
    needed_permissions = ["ya360_security:service_applications_read",
                          "ya360_security:service_applications_write",]

    result, data = check_token_permissions_api(settings.oauth_token)
    if not result:
        return False
    else:
        try:
            token_scopes = data.get('scopes', [])
            token_org_ids = data.get('orgIds', [])
            login = data.get('login', 'unknown')
            if "@" in login:
                logger.error("ОШИБКА: Токен выписан НЕ личной учётке Яндекс. Невозможно настроить сервисное приложение.")
                return False

            logger.info(f"Проверка прав доступа для токена пользователя: {login}")
            logger.debug(f"Доступные права: {token_scopes}")
            logger.debug(f"Доступные организации: {token_org_ids}")

            for permission in needed_permissions:
                if permission not in token_scopes:
                    logger.error(f"ОШИБКА: Токен не имеет права {permission}. Невозможно настроить сервисное приложение.")
                    return False

            logger.info("✓ Все необходимые права доступа для создания сервисного приложения присутствуют.")
            logger.info(f"✓ Доступ к организации {settings.organization_id} подтвержден")
            return True
        except Exception as e:
            logger.error(f"Неожиданная ошибка при проверке прав доступа: {type(e).__name__}: {e}")
            return False

def check_token_permissions_api(token: str) -> tuple[bool, dict]:
    """
    Проверяет права доступа для заданного токена.
    
    Args:
        token: OAuth токен для проверки
        
    Returns:
        bool: Статус выполнения запроса
        dict: Данные ответа от API
    """
    url = 'https://api360.yandex.net/whoami'
    headers = {
        'Authorization': f'OAuth {token}'
    }

    result = None
    try:
        with httpx.Client(headers=headers) as client:
            response = client.get(url)
        
        # Проверка валидности токена
        if response.status_code != HTTPStatus.OK:
            logger.error(f"Невалидный токен. Статус код: {response.status_code}")
            if response.status_code == 401:
                logger.error("Токен недействителен или истек срок его действия.")
            else:
                logger.error(f"Ошибка при проверке токена: {response.text}")
            return False, result
        
        data = response.json()
        return True, data
        
    except httpx.HTTPError as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        return False, result
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка при парсинге ответа от API: {e}")
        return False, result
    except Exception as e:
        logger.error(f"Неожиданная ошибка при проверке прав доступа: {type(e).__name__}: {e}")
        return False, result

class TokenError(RuntimeError):
    pass

def activate_service_applications(settings: "SettingParams") -> bool:
    """
    Активирует работу сервисных приложений.
    Спецификация API: https://yandex.ru/dev/api360/doc/ru/ref/ServiceApplicationsService/ServiceApplicationsService_Activate
    
    Args:
        settings: Объект настроек с oauth_token и org_id
        
    Returns:
        bool: True если функция активирована, False в случае ошибки
    """
    url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/service_applications/activate"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    retries = 1
    try:
        while True:
            logger.debug(f"POST URL - {url}")
            response = httpx.post(url, headers=headers)
            logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Ошибка при активации сервисных приложений: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error("Превышено максимальное количество попыток.")
                    return False
            else:
                logger.info("Сервисные приложения активированы.")
                return True
    except httpx.HTTPError as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при активации сервисных приложений: {type(e).__name__}: {e}")
        return False

def get_service_applications(settings: "SettingParams") -> tuple[list, str]:
    """
    Получает список сервисных приложений организации.
    Спецификация API: https://yandex.ru/dev/api360/doc/ru/ref/ServiceApplicationsService/ServiceApplicationsService_Get
    
    Args:
        settings: Объект настроек с oauth_token и org_id
        
    Returns:
        list: Список сервисных приложений, None в случае ошибки
    """
    url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/service_applications"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    retries = 1
    try:
        while True:
            logger.debug(f"GET URL - {url}")
            response = httpx.get(url, headers=headers)
            logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
            if response.status_code != HTTPStatus.OK.value:
                if response.json()['message'] == 'feature is not active':
                    logger.error('Функционал сервисных приложений не активирован в организации.')
                    return None, response.json()['message']
                if response.json()['message'] == 'Not an owner':
                    logger.error('Токен в параметре OAUTH_TOKEN_ARG выписан НЕ ВЛАДЕЛЬЦЕМ организации (с учеткой в @yandex.ru).')
                    logger.error('Невозможно настроить сервисное приложение. Получите правильный токен и повторите попытку.')
                    return None, response.json()['message']
                logger.error(f"Ошибка при получении списка сервисных приложений: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error("Превышено максимальное количество попыток.")
                    return None, response.json()['message']
            else:
                applications = response.json().get("applications", [])
                logger.info(f"Получен список {len(applications)} сервисных приложений.")
                if not check_service_app_response(settings, response):
                    logger.debug(f"Сервисное приложение {settings.application_client_id} не найдено в списке сервисных приложений организации или не имеет необходимых прав доступа.")
                    return applications, f"Сервисное приложение {settings.application_client_id} не найдено в списке сервисных приложений организации или не имеет необходимых прав доступа."
                return applications, None
    except httpx.HTTPError as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        return None, f'{e.__class__.__name__}: {e}'  
    except json.JSONDecodeError as e:
        logger.error(f"Ошибка при парсинге ответа от API: {e}")
        return None, f'{e.__class__.__name__}: {e}'  
    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении сервисных приложений: {type(e).__name__}: {e}")
        return None, f'{e.__class__.__name__}: {e}'     

def export_service_applications_api_data(settings: "SettingParams") -> bool:
    """
    Выгружает ответ API сервисных приложений в файл.
    Спецификация API: https://yandex.ru/dev/api360/doc/ru/ref/ServiceApplicationsService/ServiceApplicationsService_Get
    """
    if not settings.service_app_api_data_file:
        logger.error("SERVICE_APP_API_DATA_FILE не задан. Невозможно сохранить данные.")
        return False

    applications, error_message = get_service_applications(settings)
    if applications is None:
        logger.error("Не удалось получить данные API сервисных приложений. Проверьте настройки и повторите попытку.")
        return False
    if not applications:
        logger.error("Список сервисных приложений пуст. Невозможно выгрузить данные.")

    data = {"applications": applications}
    target_dir = os.path.dirname(settings.service_app_api_data_file)
    if target_dir and not os.path.exists(target_dir):
        os.makedirs(target_dir)
    base_name = os.path.basename(settings.service_app_api_data_file)
    name_root, ext = os.path.splitext(base_name)
    timestamp = datetime.now().strftime("%y%m%d_%H%M%S")
    output_filename = f"{name_root}_{timestamp}{ext}"
    output_path = os.path.join(target_dir, output_filename) if target_dir else output_filename
    with open(output_path, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)
        logger.info(
            f"Данные API сервисных приложений сохранены в файл: {output_path} "
            f"(кол-во приложений: {len(applications)})"
        )
    return True

def import_service_applications_api_data(settings: "SettingParams") -> bool:
    """
    Загружает параметры сервисных приложений из файла и отправляет в API.
    Спецификация API: https://yandex.ru/dev/api360/doc/ru/ref/ServiceApplicationsService/ServiceApplicationsService_Create
    """
    if not settings.service_app_api_data_file:
        logger.error("SERVICE_APP_API_DATA_FILE не задан. Невозможно загрузить данные.")
        return False

    if not os.path.exists(settings.service_app_api_data_file):
        logger.error(f"Файл не найден: {settings.service_app_api_data_file}")
        return False

    try:
        with open(settings.service_app_api_data_file, "r", encoding="utf-8") as file:
            raw_content = file.read()
    except OSError as e:
        logger.error(f"Ошибка при чтении файла {settings.service_app_api_data_file}: {e}")
        return False

    if not raw_content.strip():
        logger.error(f"Файл пустой: {settings.service_app_api_data_file}")
        return False

    try:
        payload = json.loads(raw_content)
    except json.JSONDecodeError as e:
        logger.error(f"Некорректный JSON в файле {settings.service_app_api_data_file}: {e}")
        return False

    if not isinstance(payload, dict) or "applications" not in payload:
        logger.error("Некорректный формат данных: отсутствует ключ applications.")
        return False

    if not isinstance(payload["applications"], list):
        logger.error("Некорректный формат данных: applications должен быть списком.")
        return False

    CHECK_TOKEN_PERMISSIONS = ["ya360_security:service_applications_read",
                               "ya360_security:service_applications_write",]
    success, data = check_token_permissions_api(settings.oauth_token)
    if not success:
        logger.error("Не удалось проверить токен (параметр OAUTH_TOKEN_ARG). Проверьте настройки.")
        return False
    token_scopes = data.get('scopes', [])
    for permission in CHECK_TOKEN_PERMISSIONS:
        if permission not in token_scopes:
            logger.error(f"В токене OAUTH_TOKEN_ARG отсутствуют необходимые права доступа ({', '.join(CHECK_TOKEN_PERMISSIONS)}) для модификации списка сервисных приложений. Проверьте настройки и повторите попытку.")
            return False

    url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/service_applications"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    retries = 1
    activated = False
    try:
        while True:
            logger.debug(f"POST URL - {url}")
            response = httpx.post(url, headers=headers, json=payload)
            logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
            if response.status_code != HTTPStatus.OK.value:
                if response.json()['message'] == 'feature is not active':
                    if not activated:
                        logger.error('Функционал сервисных приложений не активирован в организации. Выполняем активацию...')
                        result = activate_service_applications(settings)
                        if not result:
                            logger.error("Не удалось активировать функционал сервисных приложений. Проверьте настройки и повторите попытку.")
                            return False
                        activated = True
                        time.sleep(1)
                        continue
                    else:
                        logger.error('Функционал сервисных приложений не активирован в организации. Проверьте настройки и повторите попытку.')
                        return False
                if response.json()['message'] == 'Not an owner':
                    logger.error('Токен в параметре OAUTH_TOKEN_ARG выписан НЕ ВЛАДЕЛЬЦЕМ организации (с учеткой в @yandex.ru).')
                    logger.error('Невозможно настроить сервисное приложение. Получите правильный токен и повторите попытку.')
                    return False
                logger.error(f"Ошибка при загрузке сервисных приложений из файла: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error("Превышено максимальное количество попыток.")
                    return False
            else:
                app_count = len(payload.get("applications", []))
                logger.info(f"Данные сервисных приложений успешно загружены из файла (кол-во приложений: {app_count}).")
                return True
    except httpx.HTTPError as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке сервисных приложений из файла: {type(e).__name__}: {e}")
        return False


def merge_service_app_permissions(existing_permissions: list, required_permissions: list) -> list:
    merged_permissions = list(existing_permissions) if existing_permissions else []
    existing_set = set(merged_permissions)
    for permission in required_permissions:
        if permission not in existing_set:
            merged_permissions.append(permission)
            existing_set.add(permission)
    return merged_permissions

def check_service_app_response(settings: "SettingParams", response: httpx.Response) -> bool:
    """
    Проверяет ответ API сервисных приложений.
    """
    if len(response.json().get("applications", [])) == 0:
        return False
    
    found_app = False
    for app in response.json().get("applications", []):
        if app.get("id") == settings.application_client_id:
            found_app = True
            scopes = app.get("scopes", [])
            found_permissions = True
            for perm in SERVICE_APP_PERMISSIONS:
                if perm not in scopes:
                    found_permissions = False
                    break
            if not found_permissions:
                return False
    if not found_app:
        return False

    return True

def setup_service_application(settings: "SettingParams") -> bool:
    """
    Добавляет/обновляет сервисное приложение и его разрешения.
    Спецификация API: https://yandex.ru/dev/api360/doc/ru/ref/ServiceApplicationsService/ServiceApplicationsService_Create
    
    Args:
        settings: Объект настроек с oauth_token, org_id и application_client_id
        
    Returns:
        bool: True если операция успешна или не требуется, False в случае ошибки
    """
    if not settings.application_client_id:
        logger.error("Параметр APPLICATION_CLIENT_ID не задан. Невозможно настроить сервисное приложение.")
        return False

    if not settings.application_client_secret:
        logger.error("Параметр APPLICATION_CLIENT_SECRET не задан. Невозможно проверить статус сервисного приложения.")
        return False

    CHECK_TOKEN_PERMISSIONS = ["ya360_security:service_applications_read",
                               "ya360_security:service_applications_write",]
    success, data = check_token_permissions_api(settings.oauth_token)
    if not success:
        logger.error("Не удалось проверить токен (параметр OAUTH_TOKEN_ARG). Проверьте настройки.")
        return False
    token_scopes = data.get('scopes', [])
    for permission in CHECK_TOKEN_PERMISSIONS:
        if permission not in token_scopes:
            logger.error(f"В токене OAUTH_TOKEN_ARG отсутствуют необходимые права доступа ({', '.join(CHECK_TOKEN_PERMISSIONS)}) для модификации списка сервисных приложений. Проверьте настройки и повторите попытку.")
            return False
    
    applications, error_message = get_service_applications(settings)
    if applications is None:
        if error_message == 'feature is not active':
            result = activate_service_applications(settings)
            if not result:
                logger.error("Не удалось активировать функционал сервисных приложений. Проверьте настройки и повторите попытку.")
                return False
        else:
            return False

    # if len(applications) == 0:
    #     logger.error("Список сервисных приложений пуст. Невозможно настроить сервисное приложение.")
    #     return False

    client_id = settings.application_client_id
    required_permissions = SERVICE_APP_PERMISSIONS
    changed = False
    found = False

    if applications:
        for app in applications:
            if app.get("id") == client_id:
                found = True
                logger.info(f"Сервисное приложение с ID {client_id} найдено в списке сервисных приложений организации.")
                current_permissions = app.get("scopes", [])
                merged_permissions = merge_service_app_permissions(current_permissions, required_permissions)
                if merged_permissions != current_permissions:
                    app["scopes"] = merged_permissions
                    changed = True
                    logger.info("Добавлены недостающие разрешения для сервисного приложения.")
                else:
                    logger.info("Сервисное приложение уже содержит все необходимые разрешения. Выполняем проверку валидности токена сервисного приложения...")
                    check_service_app_status(settings)
                break
    else:
        applications = []

    if not found:
        applications.append({
            "id": client_id,
            "scopes": list(required_permissions)
        })
        changed = True
        logger.info(f"Сервисное приложение с ID {client_id} не найдено в списке сервисных приложений организации. Создаем новое.")

    if not changed:
        return True

    url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/service_applications"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    payload = {"applications": applications}
    retries = 1
    try:
        while True:
            logger.debug(f"POST URL - {url}")
            response = httpx.post(url, headers=headers, json=payload)
            logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Ошибка при обновлении сервисных приложений: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error("Превышено максимальное количество попыток.")
                    return False
            else:
                if not check_service_app_response(settings, response):
                    logger.error("Не удалось настроить сервисное приложение. Проверьте настройки и повторите попытку.")
                    return False
                logger.info(f"Список сервисных приложений успешно обновлен (Client ID - {client_id}).")
                break
    except httpx.HTTPError as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обновлении сервисных приложений: {type(e).__name__}: {e}")
        return False

    logger.info(f"Сервисное приложение с ID {client_id} успешно настроено. Выполняем проверку валидности токена сервисного приложения...")
    return check_service_app_status(settings)
    
def delete_service_applications_list(settings: "SettingParams") -> bool:
    """
    Очищает список сервисных приложений организации.
    Спецификация API: https://yandex.ru/dev/api360/doc/ru/ref/ServiceApplicationsService/ServiceApplicationsService_Delete
    """
    url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/service_applications"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    retries = 1
    try:
        while True:
            logger.debug(f"DELETE URL - {url}")
            response = httpx.delete(url, headers=headers)
            logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Ошибка при очистке списка сервисных приложений: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error("Превышено максимальное количество попыток.")
                    return False
            else:
                logger.info("Список сервисных приложений успешно очищен.")
                return True
    except httpx.HTTPError as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при очистке списка сервисных приложений: {type(e).__name__}: {e}")
        return False

def deactivate_service_applications(settings: "SettingParams") -> bool:
    """
    Деактивирует функцию сервисных приложений.
    Спецификация API: https://yandex.ru/dev/api360/doc/ru/ref/ServiceApplicationsService/ServiceApplicationsService_Deactivate
    """
    url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/service_applications/deactivate"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    retries = 1
    try:
        while True:
            logger.debug(f"POST URL - {url}")
            response = httpx.post(url, headers=headers)
            logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Ошибка при деактивации сервисных приложений: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error("Превышено максимальное количество попыток.")
                    return False
            else:
                logger.info("Сервисные приложения деактивированы.")
                return True
    except httpx.HTTPError as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при деактивации сервисных приложений: {type(e).__name__}: {e}")
        return False

def delete_service_application_from_list(settings: "SettingParams") -> bool:
    """
    Удаляет сервисное приложение с application_client_id из списка организации.
    Если приложение единственное, очищает список и деактивирует функцию.
    """
    if not settings.application_client_id:
        logger.error("Параметр APPLICATION_CLIENT_ID не задан. Невозможно удалить сервисное приложение.")
        return False

    CHECK_TOKEN_PERMISSIONS = ["ya360_security:service_applications_read",
                               "ya360_security:service_applications_write",]
    success, data = check_token_permissions_api(settings.oauth_token)
    if not success:
        logger.error("Не удалось проверить токен (параметр OAUTH_TOKEN_ARG). Проверьте настройки.")
        return False
    token_scopes = data.get('scopes', [])
    for permission in CHECK_TOKEN_PERMISSIONS:
        if permission not in token_scopes:
            logger.error(f"В токене OAUTH_TOKEN_ARG отсутствуют необходимые права доступа ({', '.join(CHECK_TOKEN_PERMISSIONS)}) для модификации списка сервисных приложений. Проверьте настройки и повторите попытку.")
            return False

    applications, error_message = get_service_applications(settings)
    if applications is None:
        settings.service_app_status = False
        if error_message == 'feature is not active':
            return True
        else:
            return False

    if len(applications) == 0:
        logger.info("Список сервисных приложений пуст. Нечего удалять.")
        settings.service_app_status = False
        return True

    client_id = settings.application_client_id
    found = [app for app in applications if app.get("id") == client_id]
    if not found:
        logger.info(f"Сервисное приложение с ID {client_id} не найдено в списке сервисных приложений организации.")
        settings.service_app_status = False
        return False

    new_applications = [app for app in applications if app.get("id") != client_id]
    if not new_applications:
        logger.info("В списке осталось только удаляемое приложение. Очищаем список и деактивируем функцию.")
        if not delete_service_applications_list(settings):
            return False
        settings.service_app_status = False
        return deactivate_service_applications(settings)

    url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/service_applications"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    payload = {"applications": new_applications}
    retries = 1
    try:
        while True:
            logger.debug(f"POST URL - {url}")
            response = httpx.post(url, headers=headers, json=payload)
            logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"Ошибка при обновлении списка сервисных приложений: {response.status_code}. Сообщение: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    logger.error("Превышено максимальное количество попыток.")
                    return False
            else:
                logger.info(f"Сервисное приложение с ID {client_id} удалено из списка сервисных приложений организации.")
                settings.service_app_status = False
                return True
    except httpx.HTTPError as e:
        logger.error(f"Ошибка при выполнении запроса к API: {e}")
        return False
    except Exception as e:
        logger.error(f"Неожиданная ошибка при обновлении списка сервисных приложений: {type(e).__name__}: {e}")
        return False

def check_service_app_status(settings: "SettingParams", skip_permissions_check: bool = False) -> bool:

    if not settings.application_client_id:
        logger.error("Параметр APPLICATION_CLIENT_ID не задан. Невозможно проверить статус сервисного приложения.")
        return False
    if not settings.application_client_secret:
        logger.error("Параметр APPLICATION_CLIENT_SECRET не задан. Невозможно проверить статус сервисного приложения.")
        return False

    if not skip_permissions_check:
        CHECK_TOKEN_PERMISSIONS = ["ya360_security:service_applications_read",]
        success, data = check_token_permissions_api(settings.oauth_token)
        if not success:
            logger.error("Не удалось проверить токен (параметр OAUTH_TOKEN_ARG). Проверьте настройки.")
            return False
        token_scopes = data.get('scopes', [])
        for permission in CHECK_TOKEN_PERMISSIONS:
            if permission not in token_scopes:
                logger.error(f"В токене OAUTH_TOKEN_ARG отсутствуют необходимые права доступа ({', '.join(CHECK_TOKEN_PERMISSIONS)}) для чтения списка сервисных приложений. Проверьте настройки и повторите попытку.")
                return False

        applications, error_message = get_service_applications(settings)
        if applications is None:
            if error_message == 'feature is not active':
                settings.service_app_status = False
                return False
            else:
                settings.service_app_status = False
                return False
        
        if len(applications) == 0:
            logger.info("Список сервисных приложений пуст. Невозможно проверить статус сервисного приложения.")
            settings.service_app_status = False
            return False

        if error_message:
            logger.error(error_message)
            settings.service_app_status = False
            return False

    # получаем первую страницу списка пользователей
    logger.info("Получение первой страницы списка всех пользователей организации из API...")
    url = f"{DEFAULT_360_API_URL}/directory/v1/org/{settings.organization_id}/users"
    headers = {"Authorization": f"OAuth {settings.oauth_token}"}
    has_errors = False
    users = []
    current_page = 1
    params = {'page': current_page, 'perPage': USERS_PER_PAGE_FROM_API}
    try:
        retries = 1
        while True:
            logger.debug(f"GET URL - {url}")
            response = httpx.get(url, headers=headers, params=params)
            logger.debug(f"x-request-id: {response.headers.get('x-request-id','')}")
            if response.status_code != HTTPStatus.OK.value:
                logger.error(f"!!! ОШИБКА !!! при GET запросе url - {url}: {response.status_code}. Сообщение об ошибке: {response.text}")
                if retries < MAX_RETRIES:
                    logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                    time.sleep(RETRIES_DELAY_SEC * retries)
                    retries += 1
                else:
                    has_errors = True
                    break
            else:
                for user in response.json()['users']:
                    if not user.get('isRobot') and int(user["id"]) >= 1130000000000000:
                        users.append(user)
                logger.debug(f"Загружено {len(response.json()['users'])} пользователей.")
                break

    except httpx.HTTPError as e:
        logger.error(f"!!! ERROR !!! {type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        has_errors = True

    if has_errors:
        return False

    if len(users) == 0:
        logger.error("Не найдено ни одного пользователя в организации. Невозможно проверить статус сервисного приложения.")
        return False

    user = None
    for u in users:
        if u["isEnabled"]:
            user = u
            break
    if not user:
        logger.error("Не найдено ни одного пользователя в организации. Невозможно проверить статус сервисного приложения.")
        return False
    user_email = user.get('email', '')
    try:
        user_token = get_service_app_token(settings, user_email)
    except Exception as e:
        logger.error("Не удалось получить тестовый токен пользователя.")
        settings.service_app_status = False
        return False

    success, data = check_token_permissions_api(user_token)
    if not success:
        logger.error("Не удалось проверить токен пользователя. Проверьте настройки сервисного приложения.")
        return False
    token_scopes = data.get('scopes', [])
    token_org_ids = data.get('orgIds', [])
    login = data.get('login', 'unknown')

    logger.debug(f"Проверка прав доступа для токена пользователя: {login}")
    logger.debug(f"Доступные права: {token_scopes}")
    logger.debug(f"Доступные организации: {token_org_ids}")

    for permission in SERVICE_APP_PERMISSIONS:
        if permission not in token_scopes:
            logger.error(f"В токене пользователя отсутствуют необходимые права доступа {', '.join(SERVICE_APP_PERMISSIONS)}. Проверьте настройки сервисного приложения и повторите попытку.")
            settings.service_app_status = False
            return False

    logger.info("Сервисное приложение для удаления сообщений настроено корректно.")
    settings.service_app_status = True
    return True

def get_user_token(user_mail: str, settings: "SettingParams"):
    """
    Получает OAuth-токен пользователя через Token Exchange.
    
    Использует сервисное приложение для получения токена от имени пользователя.
    
    Args:
        user_mail: Email пользователя для получения токена
        settings: Объект настроек с application_client_id и application_client_secret
        
    Returns:
        str: OAuth-токен пользователя или пустая строка при ошибке
    """
    logger.debug(f"Getting user token for {user_mail}")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "client_id": settings.application_client_id,
        "client_secret": settings.application_client_secret,
        "subject_token": user_mail,
        "subject_token_type": "urn:yandex:params:oauth:token-type:email",
    }
    with httpx.Client(headers=headers) as client:
        response = client.post(url=DEFAULT_OAUTH_API_URL, data=data)

    if response.status_code != HTTPStatus.OK.value:
        logger.error(f"Error during getiing user token. Response: {response.status_code}, reason: {response.reason_phrase}, error: {response.text}")
        return ''
    else:
        logger.debug(f"User token for {user_mail} received successfully - {response.json()['access_token']}")
        return response.json()["access_token"]   

def fetch_audit_logs(settings: "SettingParams"):
    """
    Получает записи почтового журнала аудита из API Яндекс 360.
    
    Загружает события за период, определенный в search_param (message_date и days_diff).
    Использует постраничную загрузку с автоматическим сдвигом дат.
    
    Args:
        settings: Объект настроек с oauth_token, organization_id и search_param
        
    Returns:
        tuple: (error: bool, records: list) - флаг ошибки и список записей аудит-лога
    """
    fmt = '%Y-%m-%dT%H:%M:%S.%fZ'
    log_records = set()
    params = {}
    error = False
    msg_date = datetime.strptime(settings.search_param["message_date"], "%d-%m-%Y")

    initial_after_date =  msg_date + relativedelta(days = -settings.search_param["days_diff"], hour = 0, minute = 0, second = 0) 
    initial_before_date = msg_date + relativedelta(days = settings.search_param["days_diff"]+1, hour = 23, minute = 59, second = 59)
    logger.info(f"Поиск данных с {initial_after_date.strftime('%Y-%m-%d')} по {initial_before_date.strftime('%Y-%m-%d')}.")
    ended_at = initial_before_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    last_date = initial_after_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        params["pageSize"] = MAIL_LOGS_MAX_RECORDS
        if last_date:
            params["afterDate"] = last_date
        if ended_at:
            params["beforeDate"] = ended_at
        url = f"{DEFAULT_360_API_URL}/security/v1/org/{settings.organization_id}/audit_log/mail"
        headers = {"Authorization": f"OAuth {settings.oauth_token}"}
        pages_count = 0
        retries = 1
        with httpx.Client(headers=headers) as client:
            while True:           
                response = client.get(url, params=params)
                if response.status_code != HTTPStatus.OK.value:
                    logger.error(f"Ошибка при GET запросе: {response.status_code}. Сообщение об ошибке: {response.text}")
                    logger.debug(f"Ошибка при GET запросе. url - {url}. Параметры - {params}")
                    logger.debug(f'X-Request-Id: {response.headers.get("X-Request-Id","")}')
                    if retries < MAX_RETRIES:
                        logger.error(f"Повторная попытка ({retries+1}/{MAX_RETRIES})")
                        time.sleep(RETRIES_DELAY_SEC * retries)
                        retries += 1
                    else:
                        logger.error("Принудительный выход без получения данных.")
                        error = True
                        return error, []
                else:
                    retries = 1
                    temp_list = response.json()["events"]
                    if not temp_list:
                        logger.info("Запрос вернул пустой список событий. Выход из цикла сбора записей логов аудита почты.")
                        break
                    sorted_list = sorted(temp_list, key=lambda x: x["date"], reverse=True)
                    if temp_list:
                        logger.debug(f'Получено {len(sorted_list)} записей, с {sorted_list[-1]["date"]} по {sorted_list[0]["date"]}')
                        temp_json = [json.dumps(d, ensure_ascii=False).encode('utf8') for d in sorted_list]
                        log_records.update(temp_json)
                    
                    if response.json()["nextPageToken"] == "":
                        break
                    else:
                        if pages_count < OLD_LOG_MAX_PAGES:
                            pages_count += 1
                            params["pageToken"] = response.json()["nextPageToken"]
                        else:
                            if params.get('pageToken') : del params['pageToken']

                            occurred_at_raw = sorted_list[-1]["date"]
                            match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?)", occurred_at_raw)
                            if match:
                                suggested_date = match.group(1)
                            else:
                                logger.warning(f"Could not parse occurred_at field: {occurred_at_raw}")
                                suggested_date = occurred_at_raw[:19]  # fallback, though не гарантия что корректно

                            suggested_date = f'{suggested_date}Z'
                            msg_date = datetime.strptime(suggested_date, fmt) + relativedelta(microseconds=-1000)
                            params["beforeDate"] = msg_date.strftime(fmt)

                            params["pageSize"] = 100
                            pages_count = 0

    except Exception as e:
        logger.error(f"{type(e).__name__} at line {e.__traceback__.tb_lineno} of {__file__}: {e}")
        error = True
        return error,[]
        
    parsed_records = []
    for item in log_records:
        if isinstance(item, (bytes, bytearray)):
            item = item.decode("utf-8")
        if isinstance(item, str):
            item = json.loads(item)
        parsed_records.append(item)
    return error, parsed_records

def WriteToFile(data, filename):
    """
    Записывает список словарей в CSV-файл.
    
    Args:
        data: Список словарей для записи
        filename: Путь к файлу для сохранения
        
    Returns:
        None
    """
    with open(filename, 'w', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data[0].keys(), delimiter=';')

        writer.writeheader()
        writer.writerows(data)

def is_valid_email(email):
    """
    Проверяет, является ли строка валидным email-адресом.
    
    Args:
        email (str): Строка для проверки
        
    Returns:
        bool: True если строка является email-адресом, иначе False
    """
    regex = re.compile(
    r"(?i)"  # Case-insensitive matching
    r"(?:[A-Z0-9!#$%&'*+/=?^_`{|}~-]+"  # Unquoted local part
    r"(?:\.[A-Z0-9!#$%&'*+/=?^_`{|}~-]+)*"  # Dot-separated atoms in local part
    r"|\"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]"  # Quoted strings
    r"|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*\")"  # Escaped characters in local part
    r"@"  # Separator
    r"[A-Z0-9](?:[A-Z0-9-]*[A-Z0-9])?"  # Domain name
    r"\.(?:[A-Z0-9](?:[A-Z0-9-]*[A-Z0-9])?)+"  # Top-level domain and subdomains
    )
    if re.match(regex, email):
        return True
    return False

def is_valid_date(date_string, min_years_diff=0, max_years_diff=20):
    """
    Проверяет, можно ли преобразовать строку в дату.
    
    Поддерживает несколько распространенных форматов даты:
    - DD.MM.YYYY
    - DD/MM/YYYY
    - DD-MM-YYYY
    - YYYY-MM-DD
    - YYYY/MM/DD
    
    Args:
        date_string (str): Строка для проверки
        
    Returns:
        bool: True если строка может быть преобразована в дату, иначе False
        datetime.date: Объект даты в случае успеха, иначе None
    """
    # Проверяем, что строка не пустая
    if not date_string or not isinstance(date_string, str):
        return False, None
    
    # Набор возможных форматов для проверки
    date_formats = [
        '%d.%m.%Y',  # DD.MM.YYYY
        '%d/%m/%Y',  # DD/MM/YYYY
        '%d-%m-%Y',  # DD-MM-YYYY
        '%Y-%m-%d',  # YYYY-MM-DD (ISO формат)
        '%Y/%m/%d',  # YYYY/MM/DD
        '%m/%d/%Y',  # MM/DD/YYYY (US формат)
        '%d.%m.%y',  # DD.MM.YY
        '%Y.%m.%d',  # YYYY.MM.DD
    ]
    
    # Попытка парсинга каждым из форматов
    current_date = datetime.now().date()
    for date_format in date_formats:
        try:
            date_obj = datetime.strptime(date_string, date_format).date()

            years_diff = abs((current_date.year - date_obj.year) + 
                (current_date.month - date_obj.month) / 12 +
                (current_date.day - date_obj.day) / 365.25)
            
            # if years_diff < min_years_diff:
            #     return False, f"Дата отстоит от текущей менее, чем на {min_years_diff} лет"
            if years_diff > max_years_diff:
                return False, f"Дата отстоит от текущей более, чем на {max_years_diff} лет"
            # Дополнительная проверка на валидность (для високосных лет и т.д.)
            # Эта проверка не требуется, т.к. strptime уже выбросит исключение для невалидной даты
            return True, date_obj
        except ValueError:
            continue
    
    # Если ни один из форматов не подошел, проверяем с помощью регулярных выражений
    # для потенциально более сложных форматов
    date_patterns = [
        # Месяц прописью на английском: 25 December 2021, December 25, 2021
        r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})',
        r'(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})',
    ]
    
    month_map = {
        'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
        'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
    }
    
    for pattern in date_patterns:
        match = re.search(pattern, date_string, re.IGNORECASE)
        if match:
            groups = match.groups()
            try:
                if len(groups) == 3:
                    # 25 December 2021
                    if groups[0].isdigit() and groups[2].isdigit():
                        day = int(groups[0])
                        month = month_map[groups[1].capitalize()]
                        year = int(groups[2])
                    # December 25, 2021
                    else:
                        month = month_map[groups[0].capitalize()]
                        day = int(groups[1])
                        year = int(groups[2])
                    
                    date_obj = datetime.datetime(year, month, day).date()
                    return True, date_obj
            except (ValueError, KeyError):
                continue
    
    return False, None

def parse_to_dict(data: dict):
    """
    Преобразует запись аудит-лога в структурированный словарь.
    
    Извлекает основные поля события и форматирует дату.
    
    Args:
        data: Словарь с данными события аудит-лога
        
    Returns:
        dict: Структурированный словарь с полями eventType, date, userLogin и др.
    """
    #obj = json.dumps(data)
    d = {}
    d["eventType"] = data.get("eventType",'')
    d["raw_date"] = data.get("date")
    d["date"] = data.get("date").replace('T', ' ').replace('Z', '')
    d["userLogin"] = data.get("userLogin",'')
    d["userName"] = data.get("userName",'')
    d["from"] = data.get("from",'')
    d["to"] = data.get("to",'')
    d["subject"] = data.get("subject",'')
    d["folderName"] = data.get("folderName",'')
    d["folderType"] = data.get("folderType",'')
    d["labels"] = data.get("labels",[])
    d["orgId"] = data.get("orgId")
    d["requestId"] = data.get("requestId",'')
    d["clientIp"] = data.get("clientIp",'')
    d["userUid"] = data.get("userUid",'')
    d["msgId"] = data.get("msgId",'')
    d["uniqId"] = data.get("uniqId",'')
    d["source"] = data.get("source",'')
    d["mid"] = data.get("mid",'')
    d["cc"] = data.get("cc",'')
    d["bcc"] = data.get("bcc",'')
    d["destMid"] = data.get("destMid",'')
    d["actorUid"] = data.get("actorUid",'')
    return d
    
def log_error(info="Error"):
    """
    Логирует сообщение с уровнем ERROR.
    
    Args:
        info: Текст сообщения (по умолчанию "Error")
    """
    logger.error(info)

def log_info(info="Info"):
    """
    Логирует сообщение с уровнем INFO.
    
    Args:
        info: Текст сообщения (по умолчанию "Info")
    """
    logger.info(info)

def log_debug(info="Debug"):
    """
    Логирует сообщение с уровнем DEBUG.
    
    Args:
        info: Текст сообщения (по умолчанию "Debug")
    """
    logger.debug(info)

def map_folder(folder: Optional[bytes]) -> Optional[str]:
    """
    Преобразует байтовую строку папки IMAP в формат для использования в командах.
    
    Args:
        folder: Байтовая строка с именем папки из IMAP LIST
        
    Returns:
        str: Имя папки в кавычках или None для невалидных папок
    """
    if not folder or folder == b"LIST Completed.":
        return None
    valid = folder.decode("ascii").split('"|"')[-1].strip().strip('""')
    if valid.startswith('&'):
        return None
    return f'"{valid}"'

def restore_from_checkin_menu(settings: SettingParams):
    """
    Функция для восстановления конфигурации почтовых ящиков из файла checkin.
    
    Порядок действий:
    1. Поиск последнего файла checkin_<datetime>.txt
    2. Запрос имени файла у пользователя (с предложением найденного по умолчанию)
    3. Проверка существования файла
    4. Сканирование файла на наличие записей о делегировании
    5. Получение подтверждения от пользователя
    6. Получение списка всех пользователей
    7. Вызов restore_permissions_from_diff
    
    Args:
        settings: Объект настроек SettingParams
    """
    import glob
    
    logger.info("\n")
    logger.info("="*80)
    logger.info("ВОССТАНОВЛЕНИЕ КОНФИГУРАЦИИ ИЗ CHECKIN ФАЙЛА")
    logger.info("="*80)
    
    # Шаг 1: Поиск последнего файла checkin
    check_dir = settings.check_dir
    if not os.path.exists(check_dir):
        logger.error(f"Каталог {check_dir} не существует!")
        input("Нажмите Enter для продолжения...")
        return
    
    # Ищем все файлы checkin_*.txt
    checkin_pattern = os.path.join(check_dir, "checkin_*.txt")
    checkin_files = glob.glob(checkin_pattern)
    
    if not checkin_files:
        logger.error(f"В каталоге {check_dir} не найдено ни одного файла checkin_*.txt")
        input("Нажмите Enter для продолжения...")
        return
    
    # Сортируем файлы по времени модификации (последний - первый)
    checkin_files.sort(key=os.path.getmtime, reverse=True)
    latest_checkin = os.path.basename(checkin_files[0])
    
    logger.info(f"Найден последний файл checkin: {latest_checkin}")
    
    # Шаг 2: Запрос имени файла у пользователя
    while True:
        user_input = input(f"\nВведите имя файла checkin (Enter для использования {latest_checkin}): ").strip()
        
        if not user_input:
            # Пользователь нажал Enter - используем файл по умолчанию
            selected_file = latest_checkin
            checkin_path = os.path.join(check_dir, selected_file)
            break
        else:
            # Пользователь ввел свое имя файла
            # Проверяем, есть ли расширение .txt
            if not user_input.endswith('.txt'):
                user_input += '.txt'
            
            # Проверяем, есть ли префикс checkin_
            if not user_input.startswith('checkin_'):
                user_input = f'checkin_{user_input}'
            
            checkin_path = os.path.join(check_dir, user_input)
            
            # Шаг 3: Проверка существования файла
            if not os.path.exists(checkin_path):
                logger.warning(f"Файл {user_input} не найден в каталоге {check_dir}")
                retry = input("Попробовать снова? (Y/n): ").strip().upper()
                if retry not in ["Y", "YES", ""]:
                    logger.info("Отмена операции.")
                    return
                continue
            else:
                selected_file = user_input
                break
    
    logger.info(f"Выбран файл: {selected_file}")
    
    # Шаг 4: Сканирование файла на наличие записей о делегировании
    logger.info(f"Чтение файла {checkin_path}...")
    
    try:
        with open(checkin_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
    except Exception as e:
        logger.error(f"Ошибка при чтении файла: {e}")
        input("Нажмите Enter для продолжения...")
        return
    
    # Очищаем строки от символов новой строки и пустых строк
    lines = [line.strip() for line in lines if line.strip()]
    
    if not lines:
        logger.error("Файл пуст или не содержит валидных записей!")
        input("Нажмите Enter для продолжения...")
        return
    
    # Проверяем формат записей
    valid_lines = []
    delegation_records = 0
    actors_records = 0
    
    for line in lines:
        if '|' not in line:
            logger.warning(f"Пропуск некорректной строки (нет разделителя |): {line}")
            continue
        
        parts = line.split('|', 1)
        if len(parts) != 2:
            logger.warning(f"Пропуск некорректной строки (неверный формат): {line}")
            continue
        
        mailbox_alias = parts[0].strip()
        property_value = parts[1].strip()
        
        # Проверяем, что это запись о делегировании или actors
        if property_value.startswith("delegation_enabled="):
            delegation_records += 1
            valid_lines.append(line)
        elif property_value.startswith("actors="):
            actors_records += 1
            valid_lines.append(line)
        else:
            logger.warning(f"Пропуск некорректной строки (неизвестный формат): {line}")
    
    if not valid_lines:
        logger.error("В файле не найдено корректных записей о делегировании!")
        input("Нажмите Enter для продолжения...")
        return
    
    # Выводим статистику
    logger.info("\n" + "="*80)
    logger.info("СТАТИСТИКА ФАЙЛА")
    logger.info("="*80)
    logger.info(f"Всего строк в файле: {len(lines)}")
    logger.info(f"Валидных записей: {len(valid_lines)}")
    logger.info(f"  - delegation_enabled записей: {delegation_records}")
    logger.info(f"  - actors записей: {actors_records}")
    logger.info("="*80)
    
    # Шаг 5: Получение подтверждения от пользователя
    logger.info("ВНИМАНИЕ: Будут внесены изменения в конфигурацию почтовых ящиков!")
    logger.info("Это действие изменит настройки делегирования для почтовых ящиков согласно данным из файла.")
    
    confirmation = input("\nПродолжить восстановление конфигурации? (yes/no): ").strip().lower()
    
    if confirmation not in ["yes", "y", "да", "д"]:
        logger.info("Операция отменена пользователем.")
        input("Нажмите Enter для продолжения...")
        return
    
    # Шаг 6: Получение списка всех пользователей
    logger.info("Получение списка всех пользователей...")
    all_users = get_all_api360_users(settings, force=False)
    
    if not all_users:
        logger.error("Не удалось получить список пользователей. Операция отменена.")
        input("Нажмите Enter для продолжения...")
        return
    
    logger.info(f"Получено {len(all_users)} пользователей")
    
    # Шаг 7: Запуск restore_permissions_from_diff
    logger.info("Запуск восстановления разрешений...")
    restore_stats = restore_permissions_from_diff(valid_lines, settings, all_users)
    
    # Выводим итоговую статистику
    logger.info("\n" + "="*80)
    logger.info("ИТОГОВАЯ СТАТИСТИКА ВОССТАНОВЛЕНИЯ")
    logger.info("="*80)
    logger.info(f"Всего ящиков для восстановления: {restore_stats['total']}")
    logger.info(f"Обработано ящиков: {restore_stats['processed']}")
    logger.info(f"Успешно восстановлено: {restore_stats['success']}")
    logger.info(f"Ошибок: {restore_stats['errors']}")
    logger.info("="*80)
    
    logger.debug(f"Restore stats details: {restore_stats.get('details')}")
    if restore_stats.get('details'):
        logger.info("ДЕТАЛИ ПО ПОЧТОВЫМ ЯЩИКАМ:")
        #logger.info("")
        
        # Заголовок таблицы
        logger.info("┌─────┬─────────────────────────────────────┬──────────────────┬──────────────────┬──────────────────────┐")
        logger.info("│  №  │           Почтовый ящик             │  Делегирование   │     Делегаты     │       Ошибка         │")
        logger.info("├─────┼─────────────────────────────────────┼──────────────────┼──────────────────┼──────────────────────┤")
        
        # Строки таблицы
        for idx, detail in enumerate(restore_stats['details'], 1):
            mailbox = detail.get('mailbox', 'N/A')
            # Обрезаем длинные email адреса
            if len(mailbox) > 35:
                mailbox = mailbox[:32] + "..."
            
            # Статус делегирования
            delegation_status = detail.get('delegation_restored')
            if delegation_status is True:
                delegation_str = "✓ Восстановлено"
            elif delegation_status is False:
                delegation_str = "✗ Не восст."
            else:
                delegation_str = "─ Без изменений"
            
            # Статус делегатов
            actors_status = detail.get('actors_restored')
            if actors_status is True:
                actors_str = "✓ Восстановлены"
            elif actors_status == "partial":
                actors_str = "⚠ Частично"
            elif actors_status is False:
                actors_str = "✗ Не восст."
            else:
                actors_str = "─ Без изменений"
            
            # Ошибка
            error = detail.get('error', '')
            if error:
                if len(error) > 20:
                    error = error[:17] + "..."
            else:
                error = "─"
            
            logger.info(f"│ {idx:>3} │ {mailbox:<35} │ {delegation_str:<16} │ {actors_str:<16} │ {error:<20} │")
        
        # Нижняя граница таблицы
        logger.info("└─────┴─────────────────────────────────────┴──────────────────┴──────────────────┴──────────────────────┘")
    
    input("\nНажмите Enter для продолжения...")

def main_menu(settings: SettingParams):
    """
    Отображает главное меню приложения и обрабатывает выбор пользователя.
    
    Args:
        settings: Объект настроек приложения
        
    Returns:
        SettingParams: Обновленный объект настроек
    """
    while True:
        print("\n")
        print("Настройка параметров поиска")
        print(f'ID сообщения: {settings.search_param["message_id"]}')
        print(f'Дата сообщения: {settings.search_param["message_date"]}')
        print(f'Количество дней назад и вперед от предполагаемой даты сообщения: {settings.search_param["days_diff"]}')
        if settings.search_param.get("from_file", False):
            mailboxes_display = "Список ящиков загружен из файла"
        elif settings.search_param.get("is_all_mailboxes", False):
            mailboxes_display = "Все ящики в организации"
        else:
            mailboxes_count = len(settings.search_param["mailboxes"])
            if mailboxes_count > 10:
                mailboxes_display = f"{mailboxes_count} ящиков"
            else:
                temp_list = ",".join([mbx for mbx in settings.search_param["mailboxes"]])
                mailboxes_display = temp_list
        print(f'Почтовые ящики для поиска: {mailboxes_display}')
        print("")
        print("\n")
        print("Выберите опцию:")
        print("1. Ввести параметры поиска вручную.")        
        print("2. Загрузка параметров из файла.")
        print("3. Восстановить конфигурацию почтовых ящиков из файла checkin.")
        print("4. Проверить/настроить сервисное приложение для удаления сообщений.")
        print("5. Расширенный поиск (дата/время, отправитель, получатель, MessageID) во всех общих ящиках.")
        print("0. Выйти")

        choice = input("Введите ваш выбор (0-5): ")


        if choice == "0":
            print("До свидания!")
            break
        elif choice == "1":
            manually_search_params_menu(settings)
        elif choice == "2":
            file_search_params_menu(settings)
        elif choice == "3":
            restore_from_checkin_menu(settings)
        elif choice == "4":
            service_application_status_menu(settings)
        elif choice == "5":
            advanced_search_menu(settings)
        else:

            print("Неверный выбор. Попробуйте снова.")
    return settings

def manually_search_params_menu(settings: SettingParams):
    """
    Отображает меню ручного ввода параметров поиска и обрабатывает выбор пользователя.
    
    Позволяет задать ID сообщения, дату, временной диапазон и список почтовых ящиков.
    
    Args:
        settings: Объект настроек приложения
        
    Returns:
        SettingParams: Обновленный объект настроек
    """
    while True:
        print("\n")
        print("Параметры поиска")
        print(f'Message ID: {settings.search_param["message_id"]}')
        print(f'Message date: {settings.search_param["message_date"]}')
        print(f'Количество дней назад и вперед от предполагаемой даты сообщения: {settings.search_param["days_diff"]}')
        if settings.search_param.get("from_file", False):
            mailboxes_display = "Список ящиков загружен из файла"
        elif settings.search_param.get("is_all_mailboxes", False):
            mailboxes_display = "Все ящики в организации"
        else:
            mailboxes_count = len(settings.search_param["mailboxes"])
            if mailboxes_count > 10:
                mailboxes_display = f"{mailboxes_count} ящиков"
            else:
                temp_list = ",".join([mbx for mbx in settings.search_param["mailboxes"]])
                mailboxes_display = temp_list
        print(f'Почтовые ящики для поиска: {mailboxes_display}')
        print("")
        print("\n")
        print("Настройка параметров поиска")
        print("1. Введите ID сообщения и дату, разделенные пробелом.")
        print("2. Введите дату сообщения (не более 10 лет назад от текущей даты).")
        print("3. Введите количество дней назад и вперед от предполагаемой даты сообщения (по умолчанию +1).")
        print("4. Введите почтовые ящики для поиска.")
        print("5. Очистить параметры поиска.")
        print("Delete message menu")
        print("8. Удалить сообщения с использованием журнала аудита (список ящиков будет заполнен из журнала).")
        print("9. Удалить сообщения БЕЗ использования журнала аудита. (используется предварительно заполненный список ящиков)")
        print("Выйти в главное меню")

        # print("5. Clear mailboxes to search.")

        print("0. Выйти в главное меню.")

        choice = input("Введите ваш выбор (0-9): ")

        if choice == "0" or choice == "":
            #print("Goodbye!")
            break
        elif choice == "1":
            print('\n')
            set_message_id(settings)
        elif choice == "2":
            print('\n')
            set_message_date(settings)
        elif choice == "3":
            print('\n')
            set_days_diff(settings)
        elif choice == "4":
            print('\n')
            set_mailboxes(settings)
        elif choice == "5":
            clear_search_params(settings)
        elif choice == "8":
            delete_messages(settings)
        elif choice == "9":
            delete_messages(settings, use_log=False)
        else:
            print("Неверный выбор. Попробуйте снова.")
    return settings

def file_search_params_menu(settings: SettingParams):
    """
    Отображает меню загрузки параметров из файлов и обрабатывает выбор пользователя.
    
    Позволяет загружать списки ящиков и сообщений из CSV-файлов.
    
    Args:
        settings: Объект настроек приложения
        
    Returns:
        SettingParams: Обновленный объект настроек
    """
    while True:
        print("\n")
        print("Параметры из файлов")
        mailboxes_count = len(settings.search_param.get("mailboxes", []))
        messages_count = len(settings.search_param.get("messages_to_delete", []))
        print(f"Загружено ящиков: {mailboxes_count}")
        print(f"Загружено сообщений: {messages_count}")
        print("")
        print("\n")
        print("Загрузка параметров из файлов")
        print("1. Загрузить параметры поиска из файлов.")
        print("Delete message menu")
        print("8. Удалить сообщения с использованием журнала аудита (список ящиков будет заполнен из журнала).")
        print("9. Удалить сообщения БЕЗ использования журнала аудита. (используется предварительно заполненный список ящиков)")
        print("Выйти в главное меню")
        print("0. Выйти в главное меню.")

        choice = input("Введите ваш выбор (0-9): ")

        if choice == "0" or choice == "":
            break
        elif choice == "1":
            load_search_params_from_files(settings)
        elif choice == "8":
            delete_messages_from_files(settings, use_log=True)
        elif choice == "9":
            delete_messages_from_files(settings, use_log=False)
        else:
            print("Неверный выбор. Попробуйте снова.")
    return settings

def service_application_status_menu(settings: SettingParams):
    """
    Отображает меню управления сервисным приложением.
    
    Позволяет проверять статус, настраивать, удалять и экспортировать/импортировать
    данные сервисных приложений.
    
    Args:
        settings: Объект настроек приложения
        
    Returns:
        SettingParams: Обновленный объект настроек
    """
    while True:
        print("\n")
        print("Сервисное приложение")
        print("1. Проверить статус сервисного приложения.")
        print("2. Настроить сервисное приложение.")
        print("3. Удаление сервисного приложения из списка организации.")
        print("4. Выгрузить данные сервисных приложений в файл.")
        print("5. Загрузить параметры сервисных приложений из файла.")
        print("Выйти в главное меню")
        print("0. Выйти в главное меню.")
        choice = input("Введите ваш выбор (0-5): ")
        if choice == "0" or choice == "":
            break
        elif choice == "1":
            check_service_app_status(settings)
        elif choice == "2":
            setup_service_application(settings)
        elif choice == "3":
            delete_service_application_from_list(settings)
        elif choice == "4":
            export_service_applications_api_data(settings)
        elif choice == "5":
            import_service_applications_api_data(settings)
        else:
            print("Неверный выбор. Попробуйте снова.")
    return settings

def set_message_id(settings: SettingParams):
    """
    Запрашивает у пользователя ID сообщения и опционально дату.
    
    Args:
        settings: Объект настроек приложения
        
    Returns:
        SettingParams: Обновленный объект настроек с message_id
    """
    print("Введите ID сообщения и дату сообщения, разделенные пробелом (ПРОБЕЛ для очистки)")
    answer = input("ID сообщения и дата: ")
    if answer:
        if answer.strip() == "":
            settings.search_param["message_id"] = ""
            return settings
        if len(answer.strip().split(" ")) == 2:
            settings.search_param["message_id"] = answer.split()[0]
            set_message_date(settings, input_date = answer.split()[1])
        elif len(answer.strip().split(" ")) == 1:
            settings.search_param["message_id"] = answer.replace(" ", "").strip()
        else:
            print("Неверный ввод (строка с пробелами). Попробуйте снова.")
    return settings

def set_message_date(settings: SettingParams, input_date: str = ""):
    """
    Запрашивает у пользователя дату сообщения и валидирует ввод.
    
    Args:
        settings: Объект настроек приложения
        input_date: Предустановленная дата (опционально)
        
    Returns:
        SettingParams: Обновленный объект настроек с message_date
    """
    if not input_date:
        answer = input("Введите дату сообщения DD-MM-YYYY (ПРОБЕЛ для очистки): ")
    else:
        answer = input_date
    if answer.replace(" ", "").strip():
        status, date = is_valid_date(answer.replace(" ", "").strip(), min_years_diff=0, max_years_diff=10)
        if status:
            now = datetime.now().date()
            if date > now:
                print("Дата в будущем. Попробуйте снова.")
            else:
                settings.search_param["message_date"] = date.strftime("%d-%m-%Y")
        else:
            print("Неверный формат даты. Попробуйте снова.")
    else:
        settings.search_param["message_date"] = ""
    return settings

def set_days_diff(settings: SettingParams):
    """
    Запрашивает у пользователя количество дней для расширения диапазона поиска.
    
    Args:
        settings: Объект настроек приложения
        
    Returns:
        SettingParams: Обновленный объект настроек с days_diff
    """
    answer = input("Введите количество дней назад от целевого дня: ")
    if answer:
        if answer.isdigit():
            if int(answer) > 0 and int(answer) < 90:
                settings.search_param["days_diff"] = int(answer.replace(" ", "").strip())
            else:
                print("Неверное количество дней назад (максимум 89 дней). Попробуйте снова.")
        else:
            print("Неверное количество дней назад. Попробуйте снова.")
        
    return settings

def set_mailboxes(settings: SettingParams, use_file: bool = False):
    """
    Задает список почтовых ящиков для поиска сообщений.
    
    Позволяет вводить ящики вручную, загружать из файла или выбрать все ящики организации.
    
    Args:
        settings: Объект настроек приложения
        use_file: Загружать ящики из файла (по умолчанию False)
        
    Returns:
        None (обновляет settings.search_param["mailboxes"])
    """
    break_flag = False
    all_users_flag = False
    from_file_flag = False

    if use_file:
        source_emails = read_mailboxes_csv(settings.mailboxes_list_file)
        if not source_emails:
            logger.info(f"ФАЙЛ ПУСТОЙ - {settings.mailboxes_list_file}. Попробуйте снова.")
            return 
        from_file_flag = True
        users_to_add, break_flag, double_users_flag, all_users_flag, _ = find_users_prompt(settings, answer = ",".join(source_emails))
        logger.info(f"Найдено {len(users_to_add)} почтовых ящиков для поиска.")
        logger.info("\n")
        if not users_to_add:
            logger.info("Нет ящиков в организации для поиска. Попробуйте снова.")
            return 
    else:
        while True:
            users_to_add = []
            double_users_flag = False
            users_to_add, break_flag, double_users_flag, all_users_flag, from_file_flag = find_users_prompt(settings)

            if len(users_to_add) == 0 and break_flag:
                logger.info("Clear mailboxes list.")
                settings.search_param["mailboxes"] = []
                settings.search_param["is_all_mailboxes"] = False
                settings.search_param["from_file"] = False
                return 

            if break_flag:
                break
            
            if double_users_flag:
                continue

            if not users_to_add:
                logger.info("Нет ящиков для поиска. Попробуйте снова.")
                continue

            logger.info(f"Найдено {len(users_to_add)} почтовых ящиков для поиска.")
            logger.info("\n")
            break


        if not users_to_add:
            logger.info("No users to add. Try again.")
            return 

        if all_users_flag:
            settings.search_param["is_all_mailboxes"] = True
            settings.search_param["from_file"] = False
        else:
            settings.search_param["is_all_mailboxes"] = False

        if from_file_flag:
            settings.search_param["from_file"] = True
            settings.search_param["is_all_mailboxes"] = False
        else:
            settings.search_param["from_file"] = False

        if users_to_add == settings.search_param["mailboxes"]:
            return 

        mailboxes_list = set()  # remove duplicates
        for user in users_to_add:
            mailboxes_list.add(user["email"])
        settings.search_param["mailboxes"] = list(mailboxes_list)
    logger.info(f"Всего добавлено {len(settings.search_param['mailboxes'])} почтовых ящиков для поиска.")
    input("Нажмите Enter для продолжения...")
                
    return 

def find_users_prompt(settings: "SettingParams", answer = "") -> tuple[list[dict], bool, bool, bool, bool]:
    """
    Ищет пользователей и общие ящики по введенному запросу.
    
    Поддерживает поиск по email, алиасу, UID, фамилии. Специальные символы:
    - "*" - выбрать все ящики
    - "!" - загрузить из файла
    - " " (пробел) - очистить список
    
    Args:
        settings: Объект настроек приложения
        answer: Предустановленный ответ (опционально)
        
    Returns:
        tuple: (найденные_пользователи, break_flag, double_users_flag, all_users_flag, from_file_flag)
    """
    break_flag = False
    double_users_flag = False
    from_file_flag = False
    users_to_add = settings.search_param["mailboxes"]
    all_users_flag = False
    print("\nВведите email, алиас, UID ящиков для поиска, разделенных запятой или пробелом.")
    print("Очистить список  ПРОБЕЛ, *  ВСЕ ЯЩИКИ, !  ЗАГРУЗИТЬ ИЗ ФАЙЛА.")
    if not answer:
        answer = input(
            "Ящики для поиска: "
        )

    if answer == " ":
        return [], True, double_users_flag, all_users_flag, from_file_flag
    if len(answer) == 0:
        break_flag = True
    else:
        users_to_add = []
        users = get_all_api360_users(settings)
        if not users:
            logger.info("В организации нет пользователей.")
            break_flag = True

        logger.info("Получение списка всех общих почтовых ящиков...")
        logger.info("\n")
        shared_mailboxes = get_all_shared_mailboxes_cached(settings, force=False)

        if answer.strip() == "*":
            for user in users:
                if user.get("email"):
                    users_to_add.append({'email': user['email'], 'shared': False})
            for mailbox in shared_mailboxes:
                if mailbox.get("email"):
                    users_to_add.append({'email': mailbox['email'], 'shared': True})
            all_users_flag = True
            return users_to_add, break_flag, double_users_flag, all_users_flag, from_file_flag

        search_users = []
        if answer.strip() == "!":
            search_users = read_mailboxes_csv(settings.mailboxes_list_file)
            if not search_users:
                logger.info("ФАЙЛ ПУСТОЙ - {settings.mailboxes_list_file}.")
                break_flag = True
                return users_to_add, break_flag, double_users_flag, all_users_flag, from_file_flag
            from_file_flag = True

        if not search_users:
            pattern = r'[;,\s]+'
            search_users = re.split(pattern, answer)
        
        #rus_pattern = re.compile('[-А-Яа-яЁё]+')
        #anti_rus_pattern = r'[^\u0400-\u04FF\s]'
        
        for searched in search_users:
            if "@" in searched.strip():
                searched = searched.split("@")[0]
            searched = searched.lower().strip()  # alias
            found_flag = False
            if all(char.isdigit() for char in searched.strip()):
                if len(searched.strip()) == 16 and searched.strip().startswith("113"):
                    for user in users:
                        if user['id'] == searched.strip():
                            logger.debug(f"User found: {user['nickname']} ({user['id']})")
                            users_to_add.append({'email': user['email'], 'shared': False})
                            found_flag = True
                            break

            else:
                found_last_name_user = []
                for user in users:
                    aliases_lower_case = [r.lower() for r in user['aliases']]
                    if user['nickname'].lower() == searched.lower().strip() or searched.lower().strip() in aliases_lower_case:
                        logger.debug(f"Ящик найден: {user['nickname']} ({user['id']})")
                        users_to_add.append({'email': user['email'], 'shared': False})
                        found_flag = True
                        break
                    if user['name']['last'].lower() == searched.lower().strip():
                        found_last_name_user.append(user)
                if not found_flag and found_last_name_user:
                    if len(found_last_name_user) == 1:
                        logger.debug(f"Ящик найден ({searched}): {found_last_name_user[0]['nickname']} ({found_last_name_user[0]['id']}, {found_last_name_user[0]['position']})")
                        users_to_add.append({'email': found_last_name_user[0]['email'], 'shared': False})
                        found_flag = True
                    else:
                        logger.error(f"Ящик {searched} найден более одного раза в организации")
                        for user in found_last_name_user:
                            logger.error(f" - last name {user['name']['last']}, nickname {user['nickname']} ({user['id']}, {user['position']})")
                        logger.error("Уточните параметры поиска.")
                        double_users_flag = True
                        break
            
            if not found_flag and shared_mailboxes:
                if "@" in searched.strip():
                    searched = searched.split("@")[0].lower().strip()   # alias
                for mailbox in shared_mailboxes:
                    if mailbox.get("email").split("@")[0].lower().strip() == searched:
                        logger.debug(f"Общий ящик найден: {mailbox['email']}")
                        users_to_add.append({'email': mailbox['email'], 'shared': True})
                        found_flag = True
                        break

            if not found_flag:
                logger.error(f"Ящик {searched} не найден в организации.")

    return users_to_add, break_flag, double_users_flag, all_users_flag, from_file_flag

def read_mailboxes_csv(path: str) -> list[str]:
    """
    Читает список почтовых ящиков из CSV-файла.
    
    Ищет колонку 'Email' (регистронезависимо) и возвращает список адресов.
    
    Args:
        path: Путь к CSV-файлу
        
    Returns:
        list[str]: Список email-адресов в нижнем регистре
        
    Raises:
        FileNotFoundError: Если файл не найден
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"ФАЙЛ ПОЧТОВЫХ ЯЩИКОВ НЕ НАЙДЕН: {path}")

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    mailboxes_list = []
    for row in rows:
        # Normalize possible keys like email/Email
        email = row.get("Email") or row.get("email") or row.get("EMAIL")
        if email:
            mailboxes_list.append(email.strip().lower())
    return mailboxes_list

def read_message_ids_csv(path: str) -> list[dict]:
    """
    Читает список сообщений для удаления из CSV-файла.
    
    Ищет колонки message_id/msgId, date/message_date и опционально days_diff.
    Автоматически определяет разделитель (точка с запятой, запятая, табуляция).
    
    Args:
        path: Путь к CSV-файлу
        
    Returns:
        list[dict]: Список словарей с полями message_id, message_date, days_diff
        
    Raises:
        FileNotFoundError: Если файл не найден
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"ФАЙЛ MESSAGE IDS НЕ НАЙДЕН: {path}")

    with open(path, newline="", encoding="utf-8-sig") as f:
        sample = f.read(2048)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
        except csv.Error:
            dialect = csv.excel
            dialect.delimiter = ";"
        reader = csv.DictReader(f, dialect=dialect)
        rows = list(reader)

    if not rows:
        return []

    messages = []
    for idx, row in enumerate(rows, 1):
        normalized = {str(k).strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in row.items()}
        message_id = (
            normalized.get("message-id")
            or normalized.get("message_id")
            or normalized.get("messageid")
            or normalized.get("msgid")
        )
        message_date = normalized.get("date") or normalized.get("message_date") or normalized.get("messagedate")
        days_diff_raw = normalized.get("days_diff") or normalized.get("daysdiff") or normalized.get("days")

        if not message_id or not message_date:
            logger.warning(f"Строка {idx} пропущена: отсутствует message_id или date")
            continue

        status, parsed_date = is_valid_date(str(message_date).strip(), min_years_diff=0, max_years_diff=20)
        if not status:
            logger.warning(f"Строка {idx} пропущена: неверный формат даты ({message_date})")
            continue

        days_diff = DEFAULT_DAYS_DIF
        if days_diff_raw is not None and str(days_diff_raw).strip() != "":
            days_diff_str = str(days_diff_raw).strip()
            if days_diff_str.isdigit():
                days_diff = int(days_diff_str)
                if days_diff < 0 or days_diff > 90:
                    logger.warning(f"Строка {idx}: некорректное days_diff={days_diff}. Используется значение по умолчанию {DEFAULT_DAYS_DIF}")
                    days_diff = DEFAULT_DAYS_DIF
            else:
                logger.warning(f"Строка {idx}: некорректное days_diff={days_diff_raw}. Используется значение по умолчанию {DEFAULT_DAYS_DIF}")

        messages.append({
            "message_id": str(message_id).strip(),
            "message_date": parsed_date.strftime("%d-%m-%Y"),
            "days_diff": days_diff
        })

    return messages

def load_search_params_from_files(settings: SettingParams):
    """
    Загружает параметры поиска из файлов mailboxes_list и message_ids.
    
    Читает список ящиков и сообщений из соответствующих CSV-файлов,
    валидирует данные и сохраняет в search_param.
    
    Args:
        settings: Объект настроек приложения
        
    Returns:
        SettingParams: Обновленный объект настроек
    """
    try:
        source_emails = read_mailboxes_csv(settings.mailboxes_list_file)
    except FileNotFoundError as e:
        logger.error(str(e))
        return settings

    if not source_emails:
        logger.info(f"ФАЙЛ ПУСТОЙ - {settings.mailboxes_list_file}. Попробуйте снова.")
        return settings

    users_to_add, break_flag, double_users_flag, all_users_flag, _ = find_users_prompt(
        settings,
        answer=",".join(source_emails)
    )
    if break_flag or double_users_flag or not users_to_add:
        logger.info("Не удалось загрузить список ящиков из файла.")
        return settings

    mailboxes_list = set()
    for user in users_to_add:
        mailboxes_list.add(user["email"])
    settings.search_param["mailboxes"] = list(mailboxes_list)
    settings.search_param["from_file"] = True
    settings.search_param["is_all_mailboxes"] = False

    try:
        messages_to_delete = read_message_ids_csv(settings.message_ids_file)
    except FileNotFoundError as e:
        logger.error(str(e))
        return settings

    if not messages_to_delete:
        logger.info(f"ФАЙЛ ПУСТОЙ - {settings.message_ids_file}. Попробуйте снова.")
        return settings

    settings.search_param["messages_to_delete"] = messages_to_delete
    logger.info(f"Загружено {len(settings.search_param['mailboxes'])} почтовых ящиков для поиска.")
    logger.info(f"Загружено {len(settings.search_param['messages_to_delete'])} сообщений для удаления.")
    input("Нажмите Enter для продолжения...")
    return settings

def fetch_audit_logs_for_message(settings: "SettingParams", message_date: str, days_diff: int):
    """
    Получает записи аудит-лога для конкретного сообщения.
    
    Временно подменяет параметры поиска в settings и вызывает fetch_audit_logs.
    
    Args:
        settings: Объект настроек приложения
        message_date: Дата сообщения в формате "DD-MM-YYYY"
        days_diff: Количество дней для расширения диапазона поиска
        
    Returns:
        tuple: (error: bool, records: list) - результат fetch_audit_logs
    """
    original_message_date = settings.search_param.get("message_date", "")
    original_days_diff = settings.search_param.get("days_diff", DEFAULT_DAYS_DIF)
    settings.search_param["message_date"] = message_date
    settings.search_param["days_diff"] = days_diff
    try:
        return fetch_audit_logs(settings)
    finally:
        settings.search_param["message_date"] = original_message_date
        settings.search_param["days_diff"] = original_days_diff

def delete_messages_from_files(settings: SettingParams, use_log=True):
    """
    Удаляет сообщения на основе данных, загруженных из файлов.
    
    Использует список сообщений из search_param["messages_to_delete"].
    Может определять целевые ящики из журнала аудита или использовать заранее заданный список.
    
    Args:
        settings: Объект настроек приложения
        use_log: Использовать журнал аудита для определения ящиков (по умолчанию True)
        
    Returns:
        SettingParams: Обновленный объект настроек
    """
    messages_to_delete = settings.search_param.get("messages_to_delete", [])
    if not messages_to_delete:
        logger.error("Список сообщений не загружен. Сначала загрузите параметры из файлов.")
        return settings

    def normalize_message_id(value: str) -> str:
        return value.replace("<", "").replace(">", "").strip()

    mailbox_messages = {}

    if use_log:
        for msg in messages_to_delete:
            msg_id = msg.get("message_id", "")
            msg_date = msg.get("message_date", "")
            days_diff = msg.get("days_diff", DEFAULT_DAYS_DIF)

            if not msg_id or not msg_date:
                logger.warning("Пропущено сообщение с пустым ID или датой.")
                continue

            error, records = fetch_audit_logs_for_message(settings, msg_date, days_diff)
            if error:
                logger.error(f"Ошибка при получении журнала аудита для сообщения {msg_id}: {error}")
                continue

            found_mailboxes = set()
            normalized_search_id = normalize_message_id(msg_id)
            for record in records:
                record_id = normalize_message_id(record.get ("msgId","")) if record.get("msgId","") else ""
                if record_id == normalized_search_id:
                    mailbox = record.get("userLogin","")
                    if mailbox:
                        found_mailboxes.add(mailbox)
                        mailbox_messages.setdefault(mailbox, {})
                        msg_key = (normalized_search_id, msg_date, int(days_diff))
                        mailbox_messages[mailbox][msg_key] = {
                            "message_id": msg_id,
                            "message_date": msg_date,
                            "days_diff": int(days_diff)
                        }

            if not found_mailboxes:
                logger.error(f"Для сообщения ID: {msg_id} не найдено ни одного ящика из журнала аудита.")
    else:
        mailboxes = settings.search_param.get("mailboxes", [])
        if not mailboxes:
            logger.error("Не указаны почтовые ящики. Сначала загрузите список ящиков из файла.")
            input("Нажмите Enter для продолжения...")
            return settings

        for mailbox in mailboxes:
            mailbox_messages.setdefault(mailbox, {})
            for msg in messages_to_delete:
                msg_id = msg.get("message_id", "")
                msg_date = msg.get("message_date", "")
                days_diff = msg.get("days_diff", DEFAULT_DAYS_DIF)
                if not msg_id or not msg_date:
                    logger.warning("Пропущено сообщение с пустым ID или датой.")
                    continue
                msg_key = (normalize_message_id(msg_id), msg_date, int(days_diff))
                mailbox_messages[mailbox][msg_key] = {
                    "message_id": msg_id,
                    "message_date": msg_date,
                    "days_diff": int(days_diff)
                }

    if not mailbox_messages:
        logger.error("Не найдено ни одного почтового ящика для удаления сообщений.")
        input("Нажмите Enter для продолжения...")
        return settings

    mailboxes_data = []
    for mailbox, messages_map in mailbox_messages.items():
        mailboxes_data.append({
            "delegated_mailbox_alias": mailbox,
            "delegate_alias": settings.delegate_alias,
            "messages_to_delete": list(messages_map.values()),
            "app_password": settings.delegate_password,
            "org_domain": settings.delegate_domain
        })

    checkpoint_files = create_checkpoint_file(settings.check_dir)
    if checkpoint_files:
        checkin_file, checkout_file = checkpoint_files
        logger.info(f"Checkpoint файлы созданы: checkin={checkin_file}, checkout={checkout_file}")
    else:
        checkin_file = None
        checkout_file = None
        logger.warning("Не удалось создать checkpoint файлы. Продолжаем без сохранения состояния.")

    report_timestamp = None
    if checkin_file:
        report_timestamp = os.path.basename(checkin_file).replace("checkin_", "").replace(".txt", "")
    report_file = create_report_file(settings.reports_dir, report_timestamp)
    if report_file:
        logger.info(f"Файл отчета создан: {report_file}")
    else:
        logger.warning("Не удалось создать файл отчета. Продолжаем без записи результатов.")

    results = asyncio.run(process_multiple_mailboxes_parallel(
        mailboxes_data,
        settings,
        checkin_file,
        checkout_file,
        report_file
    ))

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Ошибка при обработке ящика {mailboxes_data[i]['delegated_mailbox_alias']}: {result}")
        else:
            logger.info(f"Результат для ящика {mailboxes_data[i]['delegated_mailbox_alias']}: {result.get('message', 'No message')}")

    if checkin_file and checkout_file:
        logger.info("Выполняется сравнение checkpoint файлов...")
        diff_file, missing_lines = compare_checkpoint_files(checkin_file, checkout_file, settings.check_dir)
        if diff_file:
            logger.info(f"Файл с различиями создан: {diff_file}")
            logger.info(f"Количество строк с различиями: {len(missing_lines)}")

            if missing_lines:
                logger.warning("Обнаружены несоответствия между исходным и финальным состоянием разрешений!")
                logger.warning("Начинается восстановление разрешений к исходному состоянию...")

                all_users = get_all_api360_users(settings, force=False)
                restore_stats = restore_permissions_from_diff(missing_lines, settings, all_users)

                if restore_stats["errors"] == 0:
                    logger.info("✓ Все разрешения успешно восстановлены к исходному состоянию")
                else:
                    logger.warning(f"⚠ Восстановление завершено с ошибками: {restore_stats['errors']} из {restore_stats['total']}")
                    logger.warning("Проверьте логи для получения подробной информации об ошибках")

    for msg in messages_to_delete:
        msg_id = msg.get("message_id", "")
        if msg_id:
            print_final_report(msg_id, mailboxes_data, results)

    return settings

def clear_mailboxes(settings: SettingParams):
    """
    Очищает список почтовых ящиков для поиска после подтверждения пользователя.
    
    Args:
        settings: Объект настроек приложения
        
    Returns:
        SettingParams: Обновленный объект настроек
    """
    answer = input("Очистить список ящиков? (Y/n): ")
    if answer.upper() not in ["Y", "YES"]:
        return settings
    settings.search_param["mailboxes"] = []
    settings.search_param["is_all_mailboxes"] = False
    return settings

def clear_search_params(settings: SettingParams):
    """
    Очищает все параметры поиска после подтверждения пользователя.
    
    Сбрасывает mailboxes, message_id, message_date, days_diff и messages_to_delete.
    
    Args:
        settings: Объект настроек приложения
        
    Returns:
        SettingParams: Обновленный объект настроек с очищенными параметрами
    """
    answer = input("Очистить параметры поиска? (Y/n): ")
    if answer.upper() not in ["Y", "YES"]:
        return settings
    settings.search_param["mailboxes"] = []
    settings.search_param["is_all_mailboxes"] = False
    settings.search_param["from_file"] = False
    settings.search_param["message_id"] = ""
    settings.search_param["message_date"] = ""
    settings.search_param["days_diff"] = 1
    settings.search_param["messages_to_delete"] = []
    return settings

def print_final_report(message_id: str, mailboxes_data: list, results: list):
    """
    Выводит финальный отчет о поиске и удалении сообщения.
    
    Args:
        message_id: ID искомого сообщения
        mailboxes_data: Список данных о почтовых ящиках, в которых производился поиск
        results: Список результатов обработки для каждого ящика
    """
    logger.info("")
    logger.info("=" * 100)
    logger.info("ФИНАЛЬНЫЙ ОТЧЕТ О ПОИСКЕ И УДАЛЕНИИ СООБЩЕНИЯ")
    logger.info("=" * 100)
    logger.info(f"Искомое сообщение: {message_id}")
    logger.info("")
    
    # Нормализуем message_id для сравнения
    normalized_search_id = message_id.replace("<", "").replace(">", "").strip()
    
    # Собираем информацию о найденных сообщениях
    found_in_mailboxes = []
    not_found_in_mailboxes = []
    skipped_mailboxes = []
    error_mailboxes = []
    
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            error_mailboxes.append({
                "mailbox": mailboxes_data[i]['delegated_mailbox_alias'],
                "error": str(result)
            })
            continue
        
        mailbox_alias = mailboxes_data[i]['delegated_mailbox_alias']
        
        # Проверяем, был ли ящик пропущен
        if result.get('skipped', False):
            skipped_mailboxes.append({
                "mailbox": mailbox_alias,
                "reason": result.get('message', 'Неизвестная причина')
            })
            continue
        
        # Проверяем deleted_messages
        deleted_messages = result.get('deleted_messages', {})
        
        # Ищем наше сообщение в результатах (с учетом разных вариантов записи message_id)
        message_found = False
        folders = []
        status = ""
        
        for msg_id, msg_data in deleted_messages.items():
            # Нормализуем msg_id для сравнения
            normalized_msg_id = msg_id.replace("<", "").replace(">", "").strip()
            
            if normalized_msg_id == normalized_search_id:
                message_found = True
                
                # Обрабатываем два формата данных: словарь или строка (для совместимости)
                if isinstance(msg_data, dict):
                    status = msg_data.get("status", "Неизвестный статус")
                    folders = msg_data.get("folders", [])
                else:
                    # Старый формат - строка
                    status = msg_data
                    # Пытаемся извлечь папки из строки статуса
                    if "удалено из" in msg_data.lower() or "удаление в" in msg_data.lower():
                        folder_parts = msg_data.split(" из ")
                        if len(folder_parts) > 1:
                            folder_list = folder_parts[1].split(", ")
                            folders.extend(folder_list)
                break
        
        if message_found:
            found_in_mailboxes.append({
                "mailbox": mailbox_alias,
                "folders": folders if folders else ["информация о папке недоступна"],
                "status": status
            })
        else:
            # Сообщение не найдено в этом ящике
            if result.get('success', False):
                not_found_in_mailboxes.append(mailbox_alias)
            else:
                error_mailboxes.append({
                    "mailbox": mailbox_alias,
                    "error": result.get('message', 'Неизвестная ошибка')
                })
    
    # Выводим результаты
    if found_in_mailboxes:
        logger.info("✓ СООБЩЕНИЕ НАЙДЕНО И ОБРАБОТАНО:")
        logger.info("")
        for item in found_in_mailboxes:
            logger.info(f"  Почтовый ящик: {item['mailbox']}")
            logger.info(f"  Папки: {', '.join(item['folders'])}")
            logger.info(f"  Статус: {item['status']}")
            logger.info("")
    else:
        logger.warning("✗ СООБЩЕНИЕ НЕ НАЙДЕНО НИ В ОДНОМ ИЗ ПОЧТОВЫХ ЯЩИКОВ")
        logger.info("")
    
    if not_found_in_mailboxes:
        logger.info("Сообщение НЕ найдено в следующих ящиках:")
        for mailbox in not_found_in_mailboxes:
            logger.info(f"  - {mailbox}")
        logger.info("")
    
    if skipped_mailboxes:
        logger.info("Пропущенные ящики:")
        for item in skipped_mailboxes:
            logger.info(f"  - {item['mailbox']}: {item['reason']}")
        logger.info("")
    
    if error_mailboxes:
        logger.info("Ошибки при обработке:")
        for item in error_mailboxes:
            logger.info(f"  - {item['mailbox']}: {item['error']}")
        logger.info("")
    
    # Итоговая статистика
    logger.info("-" * 100)
    logger.info("ИТОГОВАЯ СТАТИСТИКА:")
    logger.info(f"  Всего почтовых ящиков для проверки: {len(mailboxes_data)}")
    logger.info(f"  Сообщение найдено в ящиках: {len(found_in_mailboxes)}")
    logger.info(f"  Сообщение не найдено в ящиках: {len(not_found_in_mailboxes)}")
    logger.info(f"  Пропущено ящиков: {len(skipped_mailboxes)}")
    logger.info(f"  Ошибок при обработке: {len(error_mailboxes)}")
    logger.info("=" * 100)
    logger.info("")

def delete_messages(settings: SettingParams, use_log=True):
    """
    Выполняет удаление сообщения по ID из почтовых ящиков.
    
    Валидирует параметры поиска, при необходимости получает список ящиков
    из журнала аудита и запускает параллельную обработку удаления.
    
    Args:
        settings: Объект настроек приложения
        use_log: Использовать журнал аудита для определения ящиков (по умолчанию True)
        
    Returns:
        SettingParams: Обновленный объект настроек
    """
    stop_running = False
    if not settings.search_param["message_id"]:
        logger.error("ID сообщения пустой.")
        stop_running = True
    if not settings.search_param["message_date"]:
        logger.error("Дата сообщения пустая.")
        stop_running = True
    else:
        status, date = is_valid_date(settings.search_param["message_date"], min_years_diff=0, max_years_diff=20)
        if not status:
            logger.error(f"Неверная дата сообщения: {date}")
            stop_running = True
        else:
            days_diff = settings.search_param.get("days_diff", DEFAULT_DAYS_DIF)
            try:
                days_diff = int(days_diff)
            except (TypeError, ValueError):
                logger.error(f"Неверное значение days_diff: {days_diff}")
                stop_running = True
                days_diff = 0
            date = date + relativedelta(days = days_diff)
            now = datetime.now().date()
            diff = now - date
            if diff.days > 90 and not settings.search_param["mailboxes"]:
                logger.error("Дата сообщения слишком старая. Не могу получить список ящиков для поиска из журнала аудита. Заполните список ящиков вручную.")
                stop_running = True
            elif diff.days > 90:
                use_log = False
    
    if stop_running:
        input("Нажмите Enter для продолжения...")
        return settings
    
    search_ids = [f"<{settings.search_param['message_id'].replace('<', '').replace('>', '').strip()}>"]
    if use_log:
        settings.search_param["mailboxes"] = []
        logger.info("Начинаем поиск ящиков из журнала аудита.")
        error, records = fetch_audit_logs(settings)
        if error:
            logger.error(f"Ошибка при получении журнала аудита: {error}")
            return settings
        logger.info("Завершаем поиск ящиков из журнала аудита.")
        for r in records:
            try:
                msg_id = r.get("msgId","")
                if msg_id in search_ids:
                    logger.info(f'Письмо ID: {msg_id} найдено в ящике {r["userLogin"]}')
                    user_login = r.get("userLogin","")
                    if user_login not in settings.search_param["mailboxes"]:
                        settings.search_param["mailboxes"].append(user_login)
            except KeyError as e:
                logger.error(f"Ошибка при обработке записи из журнала аудита: {e}")
                continue
            except Exception as e:
                logger.error(f"Ошибка при обработке записи из журнала аудита: {e}")
                continue
    else:
        if not settings.search_param["mailboxes"]:
            logger.error("Не указаны почтовые ящики. Укажите почтовые ящики вручную.")
            input("Нажмите Enter для продолжения...")
            return settings

    if not settings.search_param["mailboxes"]:
        logger.error(f"Для сообщения ID: {settings.search_param['message_id']} не найдено ни одного ящика из журнала аудита.")
        input("Нажмите Enter для продолжения...")
        return settings

    # Создаем checkpoint файлы перед началом обработки
    checkpoint_files = create_checkpoint_file(settings.check_dir)
    if checkpoint_files:
        checkin_file, checkout_file = checkpoint_files
        logger.info(f"Checkpoint файлы созданы: checkin={checkin_file}, checkout={checkout_file}")
    else:
        checkin_file = None
        checkout_file = None
        logger.warning("Не удалось создать checkpoint файлы. Продолжаем без сохранения состояния.")
    
    # Создаем файл отчета перед началом обработки
    report_timestamp = None
    if checkin_file:
        report_timestamp = os.path.basename(checkin_file).replace("checkin_", "").replace(".txt", "")
    report_file = create_report_file(settings.reports_dir, report_timestamp)
    if report_file:
        logger.info(f"Файл отчета создан: {report_file}")
    else:
        logger.warning("Не удалось создать файл отчета. Продолжаем без записи результатов.")
    
    # Подготовка данных для параллельной обработки
    mailboxes_data = []
    for user in settings.search_param["mailboxes"]:
        mailbox_data = {
            "delegated_mailbox_alias": user,
            "delegate_alias": settings.delegate_alias,
            "messages_to_delete": [
                {
                    "message_id": settings.search_param["message_id"],
                    "message_date": settings.search_param["message_date"],
                    "days_diff": settings.search_param["days_diff"]
                }
            ],
            "org_domain": settings.delegate_domain
        }
     
        mailboxes_data.append(mailbox_data)
    
    # Вызов параллельной обработки
    results = asyncio.run(process_multiple_mailboxes_parallel(
        mailboxes_data,
        settings,
        checkin_file,
        checkout_file,
        report_file,
    ))
    
    # Логирование результатов
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            logger.error(f"Ошибка при обработке ящика {mailboxes_data[i]['delegated_mailbox_alias']}: {result}")
        else:
            logger.info(f"Результат для ящика {mailboxes_data[i]['delegated_mailbox_alias']}: {result.get('message', 'No message')}")
    
    if settings.run_mode in ["delegate", "hybrid"]:
        # Сравниваем checkpoint файлы перед выводом финального отчета
        if checkin_file and checkout_file:
            logger.info("Выполняется сравнение checkpoint файлов...")
            diff_file, missing_lines = compare_checkpoint_files(checkin_file, checkout_file, settings.check_dir)
            
            if diff_file:
                logger.info(f"Файл с различиями создан: {diff_file}")
                logger.info(f"Количество строк с различиями: {len(missing_lines)}")
                
                # Если есть различия, пытаемся восстановить разрешения
                if missing_lines:
                    logger.warning("Обнаружены несоответствия между исходным и финальным состоянием разрешений!")
                    logger.warning("Начинается восстановление разрешений к исходному состоянию...")
                    
                    # Получаем список пользователей один раз для всех операций восстановления
                    all_users = get_all_api360_users(settings, force=False)
                    
                    # Выполняем восстановление разрешений
                    restore_stats = restore_permissions_from_diff(missing_lines, settings, all_users)
                    
                    # Логируем результаты восстановления
                    if restore_stats["errors"] == 0:
                        logger.info("✓ Все разрешения успешно восстановлены к исходному состоянию")
                    else:
                        logger.warning(f"⚠ Восстановление завершено с ошибками: {restore_stats['errors']} из {restore_stats['total']}")
                        logger.warning("Проверьте логи для получения подробной информации об ошибках")
    
    # Выводим финальный отчет
    print_final_report(settings.search_param["message_id"], mailboxes_data, results)

    return settings

def get_service_app_token(settings: "SettingParams", user_email: str) -> str:
    """
    Получает IMAP-токен для пользователя через сервисное приложение.
    
    Использует механизм Token Exchange для получения OAuth-токена,
    который можно использовать для IMAP-авторизации через XOAUTH2.
    
    Args:
        settings: Объект настроек с application_client_id и application_client_secret
        user_email: Email пользователя для получения токена
        
    Returns:
        str: OAuth-токен для IMAP-авторизации
        
    Raises:
        TokenError: При ошибке получения токена
    """

    logger.debug(f"Getting user token for {user_email}")
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
    }
    data = {
        "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
        "client_id": settings.application_client_id,
        "client_secret": settings.application_client_secret,
        "subject_token": user_email,
        "subject_token_type": "urn:yandex:params:oauth:token-type:email",
    }

    try:
        with httpx.Client(headers=headers) as client:
            response = client.post(url=DEFAULT_OAUTH_API_URL, data=data)
    except httpx.HTTPError as exc:
        raise TokenError(f"Failed to request token: {exc}") from exc

    if response.status_code != HTTPStatus.OK.value:
        raise TokenError(
            f"Token request failed for {user_email}: {response.status_code} {response.text}"
        )

    payload = response.json()
    access_token = payload.get("access_token")
    if not access_token:
        raise TokenError(f"No access_token in response for {user_email}: {payload}")
    return access_token

# ============================================================================
# НОВЫЙ ФУНКЦИОНАЛ: Универсальный поиск и удаление писем
# Критерии (можно комбинировать любые): отправитель, получатель, Message-ID,
# тема, дата/точное время. Работает во всех общих ящиках организации.
# ============================================================================

def combine_date_and_time(date_str: str, time_str: Optional[str], end_of_range: bool = False) -> datetime:
    """
    Комбинирует дату (DD-MM-YYYY) и время (HH:MM или HH:MM:SS) в единый datetime.

    Если time_str не указан:
      - end_of_range=False -> начало дня (00:00:00)
      - end_of_range=True  -> конец дня (23:59:59)
    """
    base_date = datetime.strptime(date_str, "%d-%m-%Y")
    if time_str:
        parts = time_str.strip().split(":")
        hour = int(parts[0])
        minute = int(parts[1]) if len(parts) > 1 else 0
        second = int(parts[2]) if len(parts) > 2 else 0
        return base_date.replace(hour=hour, minute=minute, second=second)
    else:
        if end_of_range:
            return base_date.replace(hour=23, minute=59, second=59)
        return base_date.replace(hour=0, minute=0, second=0)


def extract_message_id_from_headers(message_headers) -> str:
    """
    Извлекает и нормализует Message-ID из заголовков письма (BytesHeaderParser результат).
    Возвращает нормализованную строку вида <id@domain> или "" если заголовок отсутствует.
    """
    header_msg_id = message_headers.get('Message-ID') or message_headers.get('message-id')
    if not header_msg_id:
        return ""
    try:
        decoded_to_intermediate = decode_header(header_msg_id)
        header_value = []
        for s in decoded_to_intermediate:
            if s[1] is not None:
                header_value.append(s[0].decode(s[1]))
            else:
                if isinstance(s[0], (bytes, bytearray)):
                    header_value.append(s[0].decode("ascii").strip())
                else:
                    header_value.append(s[0])
        found_msg_id = ' '.join(header_value) if len(header_value) > 1 else header_value[0]
        return found_msg_id.strip()
    except Exception:
        return str(header_msg_id).strip()


async def search_messages_universal(
    imap_connector,
    username: str,
    folder: str,
    sender_email: Optional[str] = None,
    recipient_email: Optional[str] = None,
    message_id: Optional[str] = None,
    subject_text: Optional[str] = None,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
    exact_time_filter: bool = False,
    thread_prefix: str = "",
    settings: "SettingParams" = None,
    mode: str = "delegate",
    delete_found: bool = False,
    dry_run: bool = False,
):
    """
    Универсальный поиск писем в одной папке по комбинации критериев.

    IMAP SEARCH (SINCE/BEFORE) поддерживает только дату без времени, поэтому:
    - Сначала выполняется грубый отбор по дате/отправителю/получателю через IMAP SEARCH
    - Затем (если exact_time_filter=True) точное время сверяется по заголовку Date
      каждого найденного письма
    - Если message_id указан - проверяется точное совпадение (Yandex IMAP не
      поддерживает HEADER-поиск по Message-ID напрямую)
    """
    found_messages = []
    search_parts = []
    if start_datetime:
        search_parts.append(f'SINCE {start_datetime.strftime("%d-%b-%Y")}')
    if end_datetime:
        before_date = end_datetime if exact_time_filter else end_datetime + relativedelta(days=1)
        search_parts.append(f'BEFORE {before_date.strftime("%d-%b-%Y")}')



    if search_parts:
        search_criteria = f'({" ".join(search_parts)})'
    else:
        search_criteria = 'ALL'
        logger.warning(
            f"{thread_prefix}Критерии даты/отправителя/получателя не заданы, "
            f"поиск по ВСЕМ письмам в папке {folder} (может быть медленным)"
        )

    logger.debug(f"{thread_prefix}SEARCH критерий в папке {folder}: {search_criteria}")

    try:
        response, imap_connector = await search_with_retry(
            imap_connector=imap_connector,
            username=username,
            folder=folder,
            search_criteria=search_criteria,
            thread_prefix=thread_prefix,
            settings=settings,
            mode=mode,
        )
    except Exception as e:
        logger.error(f"{thread_prefix}Ошибка при SEARCH в папке {folder}: {e}")
        return found_messages, imap_connector

    if response.result != 'OK' or not response.lines[0]:
        return found_messages, imap_connector

    message_numbers = response.lines[0].split()
    folder_display = imap_utf7_decode(folder.strip('"'))

    logger.info(f"{thread_prefix}Найдено {len(message_numbers)} писем по базовым критериям в папке {folder_display}")

    normalized_search_msg_id = None
    if message_id:
        normalized_search_msg_id = f'<{message_id.replace("<", "").replace(">", "").strip()}>'

    for num in message_numbers:
        try:
            fetch_response, imap_connector = await fetch_message_headers_with_retry(
                imap_connector=imap_connector,
                username=username,
                folder=folder,
                msg_num=int(num),
                thread_prefix=thread_prefix,
                settings=settings,
                mode=mode,
            )

            if fetch_response.result != 'OK':
                continue

            for i in range(0, len(fetch_response.lines) - 1, 3):
                message_headers = BytesHeaderParser().parsebytes(fetch_response.lines[i + 1])

                # --- Точное совпадение Message-ID ---
                if normalized_search_msg_id:
                    found_msg_id = extract_message_id_from_headers(message_headers)
                    if found_msg_id.strip() != normalized_search_msg_id.strip():
                        continue

                # --- Тема ---
                subject_raw = message_headers.get('Subject', '')
                decoded_subject = ''
                if subject_raw:
                    try:
                        for part, charset in decode_header(subject_raw):
                            if isinstance(part, bytes):
                                decoded_subject += part.decode(charset or 'utf-8', errors='replace')
                            else:
                                decoded_subject += part
                    except Exception:
                        decoded_subject = subject_raw

                if subject_text and subject_text.lower() not in decoded_subject.lower():
                    continue

                from_header = message_headers.get('From', '')
                to_header = message_headers.get('To', '')
                date_header = message_headers.get('Date', '')
                msg_id_header = extract_message_id_from_headers(message_headers)

                # --- Дополнительная проверка отправителя/получателя (IMAP FROM/TO
                #     иногда матчит по частичным совпадениям вложенных заголовков,
                #     поэтому подтверждаем точнее по строке заголовка) ---
                if sender_email and sender_email.lower() not in from_header.lower():
                    continue
                if recipient_email and recipient_email.lower() not in to_header.lower():
                    continue

                # --- Точная фильтрация по времени ---
                if exact_time_filter and date_header:
                    try:
                        parsed_dt = parsedate_to_datetime(date_header)
                        parsed_dt_naive = parsed_dt.replace(tzinfo=None)
                        if start_datetime and parsed_dt_naive < start_datetime:
                            continue
                        if end_datetime and parsed_dt_naive > end_datetime:
                            continue
                    except Exception as e:
                        logger.debug(f"{thread_prefix}Не удалось распарсить дату письма '{date_header}': {e}")

                msg_info = {
                    'num': int(num),
                    'folder': folder_display,
                    'subject': decoded_subject,
                    'from': from_header,
                    'to': to_header,
                    'date': date_header,
                    'message_id': msg_id_header,
                    'action': 'НАЙДЕНО',
                }
                found_messages.append(msg_info)

                logger.info(f"{thread_prefix}  📧 Найдено письмо:")
                logger.info(f"{thread_prefix}     Тема: {decoded_subject}")
                logger.info(f"{thread_prefix}     От: {from_header}")
                logger.info(f"{thread_prefix}     Кому: {to_header}")
                logger.info(f"{thread_prefix}     Дата: {date_header}")
                logger.info(f"{thread_prefix}     Message-ID: {msg_id_header}")
                logger.info(f"{thread_prefix}     Папка: {folder_display}")

                if delete_found:
                    if dry_run:
                        logger.info(f"{thread_prefix}     🔍 DRY_RUN: письмо было бы удалено")
                        msg_info['action'] = 'DRY_RUN'
                    else:
                        try:
                            _, imap_connector = await store_message_with_retry(
                                imap_connector=imap_connector,
                                username=username,
                                folder=folder,
                                msg_num=int(num),
                                thread_prefix=thread_prefix,
                                settings=settings,
                                mode=mode,
                            )
                            logger.info(f"{thread_prefix}     ✓ Письмо удалено")
                            msg_info['action'] = 'DELETED'
                        except Exception as e:
                            logger.error(f"{thread_prefix}     ✗ Ошибка удаления: {e}")
                            msg_info['action'] = f'ERROR: {e}'

        except Exception as e:
            logger.debug(f"{thread_prefix}Ошибка при обработке письма {num} в папке {folder}: {e}")
            continue

    return found_messages, imap_connector


async def search_messages_in_mailbox(
    settings: "SettingParams",
    mailbox_email: str,
    sender_email: Optional[str] = None,
    recipient_email: Optional[str] = None,
    message_id: Optional[str] = None,
    subject_text: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    delegate_alias: Optional[str] = None,
    org_domain: Optional[str] = None,
    mode: str = "delegate",
    delete_found: bool = False,
    thread_id: int = 0,
):
    """
    Ищет письма в одном почтовом ящике (обычном или общем) по комбинации критериев.
    """
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""

    if delegate_alias is None:
        delegate_alias = settings.delegate_alias
    if org_domain is None:
        org_domain = settings.delegate_domain

    logger.info(f"{thread_prefix}" + "=" * 80)
    logger.info(f"{thread_prefix}Поиск писем в ящике {mailbox_email}")
    if sender_email:
        logger.info(f"{thread_prefix}Отправитель: {sender_email}")
    if recipient_email:
        logger.info(f"{thread_prefix}Получатель: {recipient_email}")
    if message_id:
        logger.info(f"{thread_prefix}Message-ID: {message_id}")
    if subject_text:
        logger.info(f"{thread_prefix}Тема содержит: {subject_text}")
    if start_date:
        period = f"{start_date}{' ' + start_time if start_time else ''} - {end_date or start_date}{' ' + end_time if end_time else ''}"
        logger.info(f"{thread_prefix}Период: {period}")
    logger.info(f"{thread_prefix}Режим: {'DRY_RUN' if settings.dry_run else ('УДАЛЕНИЕ' if delete_found else 'ТОЛЬКО ПОИСК')}")
    logger.info(f"{thread_prefix}" + "=" * 80)

    result = {"success": False, "message": "", "found_messages": [], "deleted_count": 0}

    try:
        mailbox_name = mailbox_email.split('@')[0]

        if mode == "delegate":
            username = f"{org_domain}/{delegate_alias}/{mailbox_name}"
        elif mode == "service_application":
            username = mailbox_email
        else:
            raise ValueError(f"Неизвестный режим работы: {mode}")

        imap_connector = await reconnect_imap_session(
            username=username, settings=settings, thread_prefix=thread_prefix, mode=mode
        )
        logger.info(f"{thread_prefix}Успешная авторизация: {username}")

        folders, imap_connector = await list_all_folders_recursive(
            imap_connector=imap_connector, username=username,
            thread_prefix=thread_prefix, settings=settings, mode=mode,
        )
        logger.info(f"{thread_prefix}Найдено папок: {len(folders)}")

        start_datetime = None
        end_datetime = None
        exact_time_filter = bool(start_time or end_time)

        if start_date:
            start_datetime = combine_date_and_time(start_date, start_time, end_of_range=False)
        if end_date or start_date:
            end_datetime = combine_date_and_time(end_date or start_date, end_time, end_of_range=True)

        for folder in folders:
            _, imap_connector = await select_folder_with_retry(
                imap_connector=imap_connector, username=username, folder=folder,
                thread_prefix=thread_prefix, settings=settings, mode=mode,
            )

            found_in_folder, imap_connector = await search_messages_universal(
                imap_connector=imap_connector,
                username=username,
                folder=folder,
                sender_email=sender_email,
                recipient_email=recipient_email,
                message_id=message_id,
                subject_text=subject_text,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                exact_time_filter=exact_time_filter,
                thread_prefix=thread_prefix,
                settings=settings,
                mode=mode,
                delete_found=delete_found,
                dry_run=settings.dry_run,
            )

            result["found_messages"].extend(found_in_folder)

            if delete_found and not settings.dry_run:
                result["deleted_count"] += sum(1 for m in found_in_folder if m.get('action') == 'DELETED')

        rate_limit_imap_commands()
        await imap_connector.logout()
        logger.info(f"{thread_prefix}Отключение от IMAP сервера для {username}")

        result["success"] = True
        result["message"] = f"Найдено {len(result['found_messages'])} писем, удалено {result['deleted_count']}"

    except Exception as e:
        error_msg = f"Ошибка при поиске в ящике {mailbox_email}: {type(e).__name__}: {e}"
        logger.error(f"{thread_prefix}{error_msg}")
        result["message"] = error_msg

    return result


async def temporary_grant_access_for_search(
    settings: "SettingParams",
    mailbox_email: str,
    thread_id: int = 0
):
    """
    Временно назначает права delegate_alias на ящик (общий или обычный) для поиска.
    Если у делегата нет прав - выдаёт роль shared_mailbox_owner.
    Если делегирование выключено - включает его.
    """
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""

    result = {
        "success": False, "message": "", "resource_id": None, "resource_type": None,
        "delegate_user_id": None, "had_owner_permission": False,
        "original_state": {
            "delegation_was_enabled": False, "delegation_was_enabled_by_us": False,
            "delegate_was_present": False, "delegate_original_roles": None,
        }
    }

    all_users = get_all_api360_users(settings, force=False, suppress_messages=True)
    all_shared_mailboxes = get_all_shared_mailboxes_cached(settings, force=False, suppress_messages=True)

    resource_id, resource_type, isEnabled = get_resource_id_by_email(
        settings, all_users, all_shared_mailboxes, mailbox_email
    )
    if not resource_id:
        result["message"] = f"Ящик {mailbox_email} не найден"
        return result

    result["resource_id"] = resource_id
    result["resource_type"] = resource_type

    delegate_email = f"{settings.delegate_alias}@{settings.delegate_domain}"
    delegate_user_id, _, _ = get_resource_id_by_email(
        settings, all_users, all_shared_mailboxes, delegate_email
    )
    if not delegate_user_id:
        result["message"] = f"Делегат {delegate_email} не найден"
        return result

    if resource_type == "shared_mailbox":
        result["original_state"]["delegation_was_enabled"] = True
    else:
        all_delegated = get_all_delegated_mailboxes(settings, thread_id=thread_id)
        if all_delegated is None:
            result["message"] = "Не удалось получить список делегированных ящиков"
            return result

        delegation_was_enabled = any(
            str(mailbox.get('resourceId')) == str(resource_id) for mailbox in all_delegated
        )
        result["original_state"]["delegation_was_enabled"] = delegation_was_enabled

        if not delegation_was_enabled:
            if not enable_mailbox_delegation(settings, resource_id, thread_id):
                result["message"] = "Не удалось включить делегирование"
                return result
            result["original_state"]["delegation_was_enabled_by_us"] = True

    actors = get_mailbox_actors(settings, resource_id, thread_id=thread_id) or []

    delegate_roles = None
    for actor in actors:
        if str(actor.get('actorId')) == str(delegate_user_id):
            delegate_roles = actor.get('roles', [])
            result["original_state"]["delegate_was_present"] = True
            result["original_state"]["delegate_original_roles"] = delegate_roles.copy()
            break

    has_owner = delegate_roles is not None and 'shared_mailbox_owner' in delegate_roles

    if not has_owner:
        logger.info(f"{thread_prefix}У делегата нет прав на ящик {mailbox_email}, выдаём shared_mailbox_owner...")
        new_roles = list(set((delegate_roles or []) + ['shared_mailbox_owner']))

        task_id = set_mailbox_permissions(
            settings, resource_id=resource_id, actor_id=delegate_user_id,
            roles=new_roles, notify="none", thread_id=thread_id
        )
        if not task_id:
            result["message"] = "Не удалось назначить права делегату"
            return result

        if not await wait_for_task_completion(settings, task_id, thread_id=thread_id):
            result["message"] = "Задача назначения прав не завершилась успешно"
            return result

        logger.info(f"{thread_prefix}Права успешно выданы делегату на ящик {mailbox_email}")
    else:
        logger.debug(f"{thread_prefix}У делегата уже есть права на ящик {mailbox_email}")

    result["success"] = True
    result["message"] = "Доступ настроен успешно"
    result["delegate_user_id"] = delegate_user_id
    result["had_owner_permission"] = has_owner
    return result


async def restore_access_after_search(
    settings: "SettingParams",
    mailbox_email: str,
    resource_id: str,
    resource_type: str,
    delegate_user_id: str,
    had_owner_permission: bool,
    original_state: dict,
    thread_id: int = 0
):
    """Восстанавливает исходное состояние прав доступа после поиска."""
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""
    try:
        if not had_owner_permission:
            if original_state.get("delegate_was_present"):
                roles = original_state.get("delegate_original_roles", [])
                task_id = set_mailbox_permissions(
                    settings, resource_id=resource_id, actor_id=delegate_user_id,
                    roles=roles, notify="none", thread_id=thread_id
                )
            else:
                task_id = set_mailbox_permissions(
                    settings, resource_id=resource_id, actor_id=delegate_user_id,
                    roles=[], notify="none", thread_id=thread_id
                )
            if task_id:
                await wait_for_task_completion(settings, task_id, thread_id=thread_id)

        if resource_type != "shared_mailbox" and original_state.get("delegation_was_enabled_by_us"):
            disable_mailbox_delegation(settings, resource_id, thread_id)

        logger.info(f"{thread_prefix}Права доступа восстановлены для {mailbox_email}")
    except Exception as e:
        logger.error(f"{thread_prefix}Ошибка восстановления прав для {mailbox_email}: {e}")


async def search_and_process_mailbox(
    settings: "SettingParams",
    mailbox_email: str,
    sender_email: Optional[str] = None,
    recipient_email: Optional[str] = None,
    message_id: Optional[str] = None,
    subject_text: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    delete_found: bool = False,
    thread_id: int = 0
):
    """
    Полный цикл обработки одного почтового ящика:
    временный доступ -> поиск -> (удаление) -> восстановление доступа.
    """
    thread_prefix = f"[THREAD #{thread_id}] " if thread_id > 0 else ""

    result = {
        "mailbox": mailbox_email, "success": False, "message": "",
        "found_messages": [], "deleted_count": 0, "skipped": False
    }

    all_users = get_all_api360_users(settings, force=False, suppress_messages=True)
    all_shared_mailboxes = get_all_shared_mailboxes_cached(settings, force=False, suppress_messages=True)

    resource_id, resource_type, isEnabled = get_resource_id_by_email(
        settings, all_users, all_shared_mailboxes, mailbox_email
    )
    if not resource_id:
        result["message"] = f"Ящик {mailbox_email} не найден"
        result["skipped"] = True
        return result

    delegate_email = f"{settings.delegate_alias}@{settings.delegate_domain}"
    resource_id2, _, _ = get_resource_id_by_email(settings, all_users, all_shared_mailboxes, delegate_email)

    if resource_id == resource_id2:
        result["message"] = f"Ящик {mailbox_email} совпадает с делегатом, пропускаем"
        result["skipped"] = True
        return result

    mode = "delegate"
    if resource_type != "shared_mailbox" and settings.run_mode == "service_application":
        mode = "service_application"
    elif resource_type == "shared_mailbox" and settings.run_mode == "service_application":
        result["message"] = f"Общий ящик {mailbox_email} требует режима delegate"
        result["skipped"] = True
        return result

    grant_result = None
    try:
        if mode == "delegate":
            grant_result = await temporary_grant_access_for_search(settings, mailbox_email, thread_id)
            if not grant_result["success"]:
                result["message"] = grant_result["message"]
                return result

        search_result = await search_messages_in_mailbox(
            settings=settings,
            mailbox_email=mailbox_email,
            sender_email=sender_email,
            recipient_email=recipient_email,
            message_id=message_id,
            subject_text=subject_text,
            start_date=start_date,
            end_date=end_date,
            start_time=start_time,
            end_time=end_time,
            mode=mode,
            delete_found=delete_found,
            thread_id=thread_id
        )

        result["success"] = search_result["success"]
        result["message"] = search_result["message"]
        result["found_messages"] = search_result["found_messages"]
        result["deleted_count"] = search_result["deleted_count"]

    finally:
        if mode == "delegate" and grant_result and grant_result.get("success"):
            await restore_access_after_search(
                settings=settings,
                mailbox_email=mailbox_email,
                resource_id=grant_result["resource_id"],
                resource_type=grant_result["resource_type"],
                delegate_user_id=grant_result["delegate_user_id"],
                had_owner_permission=grant_result["had_owner_permission"],
                original_state=grant_result["original_state"],
                thread_id=thread_id
            )

    return result


async def search_in_all_shared_mailboxes(
    settings: "SettingParams",
    sender_email: Optional[str] = None,
    recipient_email: Optional[str] = None,
    message_id: Optional[str] = None,
    subject_text: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    delete_found: bool = False,
        additional_mailboxes: Optional[list] = None,
):
    """
    Ищет письма по любой комбинации критериев во ВСЕХ общих ящиках организации
    (список ящиков получается автоматически через API).
    """
    logger.info("=" * 100)
    logger.info("УНИВЕРСАЛЬНЫЙ ПОИСК ПИСЕМ ВО ВСЕХ ОБЩИХ ЯЩИКАХ ОРГАНИЗАЦИИ")
    if sender_email:
        logger.info(f"Отправитель: {sender_email}")
    if recipient_email:
        logger.info(f"Получатель: {recipient_email}")
    if message_id:
        logger.info(f"Message-ID: {message_id}")
    if subject_text:
        logger.info(f"Тема содержит: {subject_text}")
    if start_date:
        logger.info(f"Период: {start_date}{' ' + start_time if start_time else ''} - {end_date or start_date}{' ' + end_time if end_time else ''}")
    logger.info(f"Режим: {'DRY RUN' if settings.dry_run else ('УДАЛЕНИЕ' if delete_found else 'ТОЛЬКО ПОИСК')}")
    logger.info(f"Дополнительных ящиков указано явно: {len(additional_mailboxes) if additional_mailboxes else 0}")
    logger.info("=" * 100)

    logger.info("Получение списка общих ящиков...")
    shared_mailboxes = get_all_shared_mailboxes_cached(settings, force=True)

    target_mailboxes = []
    if shared_mailboxes:
        for mailbox in shared_mailboxes:
            email = mailbox.get('email', '')
            if email:
                target_mailboxes.append(email)
        logger.info(f"Найдено общих ящиков: {len(target_mailboxes)}")
    else:
        logger.warning("В организации нет общих ящиков!")

    if additional_mailboxes:
        logger.info(f"Добавление {len(additional_mailboxes)} явно указанных ящиков...")
        for email in additional_mailboxes:
            if email and email not in target_mailboxes:
                target_mailboxes.append(email)
        logger.info(f"Добавлено дополнительных ящиков: {len(additional_mailboxes)}")

    if not target_mailboxes:
        logger.error("Нет ящиков для поиска!")
        return {"total": 0, "processed": 0, "found": 0, "deleted": 0, "errors": 0, "details": []}

    logger.info(f"Всего ящиков для обработки: {len(target_mailboxes)}")

    semaphore = asyncio.Semaphore(MAX_PARALLEL_THREADS)

    async def process_with_semaphore(mailbox_email, thread_id):
        async with semaphore:
            return await search_and_process_mailbox(
                settings=settings,
                mailbox_email=mailbox_email,
                sender_email=sender_email,
                recipient_email=recipient_email,
                message_id=message_id,
                subject_text=subject_text,
                start_date=start_date,
                end_date=end_date,
                start_time=start_time,
                end_time=end_time,
                delete_found=delete_found,
                thread_id=thread_id
            )

    tasks = [process_with_semaphore(mailbox_email, idx) for idx, mailbox_email in enumerate(target_mailboxes, 1)]

    logger.info(f"Запуск {len(tasks)} задач (максимум {MAX_PARALLEL_THREADS} одновременно)...")
    results = await asyncio.gather(*tasks, return_exceptions=True)

    stats = {"total": len(target_mailboxes), "processed": 0, "found": 0, "deleted": 0, "errors": 0, "skipped": 0, "details": []}

    for i, result in enumerate(results):
        if isinstance(result, Exception):
            stats["errors"] += 1
            logger.error(f"Ошибка при обработке ящика {target_mailboxes[i]}: {result}")
            continue
        if result.get("skipped"):
            stats["skipped"] += 1
            continue
        stats["processed"] += 1
        stats["found"] += len(result.get("found_messages", []))
        stats["deleted"] += result.get("deleted_count", 0)
        if not result.get("success"):
            stats["errors"] += 1
        if result.get("found_messages"):
            stats["details"].append(result)

    logger.info("\n" + "=" * 100)
    logger.info("СВОДНЫЙ ОТЧЕТ О ПОИСКЕ")
    logger.info("=" * 100)
    logger.info(f"Всего ящиков: {stats['total']}")
    logger.info(f"Обработано: {stats['processed']}")
    logger.info(f"Пропущено: {stats['skipped']}")
    logger.info(f"Найдено писем: {stats['found']}")
    logger.info(f"Удалено писем: {stats['deleted']}")
    logger.info(f"Ошибок: {stats['errors']}")

    if stats["details"]:
        logger.info("\n" + "-" * 100)
        logger.info("ДЕТАЛИ ПО НАЙДЕННЫМ ПИСЬМАМ:")
        logger.info("-" * 100)
        for detail in stats["details"]:
            logger.info(f"\n📬 Ящик: {detail['mailbox']}")
            logger.info(f"   Найдено: {len(detail['found_messages'])}, Удалено: {detail['deleted_count']}")
            for msg in detail["found_messages"]:
                logger.info(f"   📧 [{msg['folder']}] {msg['subject']}")
                logger.info(f"      От: {msg['from']}")
                logger.info(f"      Кому: {msg.get('to', 'N/A')}")
                logger.info(f"      Дата: {msg['date']}")
                logger.info(f"      Message-ID: {msg.get('message_id', 'N/A')}")
                logger.info(f"      Действие: {msg.get('action', 'НАЙДЕНО')}")

    logger.info("=" * 100)
    return stats


def write_search_report(stats: dict, reports_dir: str) -> Optional[str]:
    """Записывает результаты универсального поиска в CSV-файл отчета."""
    try:
        if not os.path.exists(reports_dir):
            os.makedirs(reports_dir)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filepath = os.path.join(reports_dir, f"universal_search_{timestamp}.csv")

        with open(report_filepath, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, delimiter=';')
            writer.writerow(["mailbox", "folder", "subject", "from", "to", "date", "message_id", "action"])
            for detail in stats.get("details", []):
                mailbox = detail["mailbox"]
                for msg in detail.get("found_messages", []):
                    writer.writerow([
                        mailbox, msg.get("folder", ""), msg.get("subject", ""),
                        msg.get("from", ""), msg.get("to", ""), msg.get("date", ""),
                        msg.get("message_id", ""), msg.get("action", "НАЙДЕНО")
                    ])

        logger.info(f"Отчет о поиске сохранен: {report_filepath}")
        return report_filepath
    except Exception as e:
        logger.error(f"Ошибка при сохранении отчета поиска: {e}")
        return None

def select_additional_mailboxes_menu() -> list:
    """
    Позволяет администратору явно указать список конкретных почтовых ящиков
    (личных или общих) для дополнительного поиска, вместо автоматического
    перебора ВСЕХ пользователей организации.

    Returns:
        list: Список email-адресов для дополнительного поиска (может быть пустым)
    """
    print("\n" + "=" * 80)
    print("ДОПОЛНИТЕЛЬНЫЙ ПОИСК В ЛИЧНЫХ ПОЧТОВЫХ ЯЩИКАХ")
    print("=" * 80)
    print("")
    print("Выберите способ добавления конкретных ящиков для поиска:")
    print("1. Не искать в дополнительных ящиках (только общие)")
    print("2. Указать email-адреса вручную (через запятую)")
    print("3. Загрузить список email из текстового файла (один адрес на строку)")
    print("=" * 80)

    choice = input("Ваш выбор (1-3): ").strip()

    additional_mailboxes = []

    if choice == "2":
        emails_input = input(
            "Введите email-адреса через запятую (например: "
            "ivanov@company.ru, petrov@company.ru): "
        ).strip()
        if emails_input:
            additional_mailboxes = [
                e.strip() for e in emails_input.split(",") if e.strip()
            ]
    elif choice == "3":
        filepath = input("Путь к файлу со списком email-адресов: ").strip()
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                additional_mailboxes = [
                    line.strip() for line in f if line.strip()
                ]
        except FileNotFoundError:
            logger.error(f"Файл не найден: {filepath}")
            print(f"Файл не найден: {filepath}. Дополнительные ящики не добавлены.")
            additional_mailboxes = []
        except Exception as e:
            logger.error(f"Ошибка при чтении файла {filepath}: {e}")
            print(f"Ошибка при чтении файла: {e}. Дополнительные ящики не добавлены.")
            additional_mailboxes = []

    if additional_mailboxes:
        print(f"\nУказано дополнительных ящиков для поиска: {len(additional_mailboxes)}")
        for email in additional_mailboxes[:10]:
            print(f"  - {email}")
        if len(additional_mailboxes) > 10:
            print(f"  ... и ещё {len(additional_mailboxes) - 10}")

        if len(additional_mailboxes) > 50:
            print("\n" + "!" * 80)
            print(f"ВНИМАНИЕ: указано {len(additional_mailboxes)} ящиков.")
            print("Обработка такого количества может занять значительное время")
            print("и создать временные изменения прав доступа для каждого ящика.")
            print("!" * 80)
            confirm = input("Продолжить с таким количеством ящиков? (yes/no): ").strip().lower()
            if confirm not in ["yes", "y", "да", "д"]:
                print("Отменено. Дополнительные ящики не будут использованы.")
                return []

    return additional_mailboxes




def advanced_search_menu(settings: "SettingParams"):
    """Меню универсального поиска/удаления писем во всех общих ящиках."""
    print("\n" + "=" * 80)
    print("РАСШИРЕННЫЙ ПОИСК ПИСЕМ (дата/время, отправитель, получатель, MessageID)")
    print("=" * 80)
    print("Заполните только те поля, которые известны. Остальные оставьте пустыми (Enter).\n")

    message_id = input("Message-ID (например 77721785851574@mail.yandex.ru, Enter = не важно): ").strip()
    message_id = message_id if message_id else None

    sender_email = input("Email отправителя (From, Enter = не важно): ").strip()
    sender_email = sender_email if sender_email else None

    recipient_email = input("Email получателя (To, Enter = не важно): ").strip()
    recipient_email = recipient_email if recipient_email else None

    subject_text = input("Фрагмент темы письма (Enter = не важно): ").strip()
    subject_text = subject_text if subject_text else None

    start_date = None
    start_time = None
    end_date = None
    end_time = None

    while True:
        date_input = input("Дата письма DD-MM-YYYY (Enter = не важно/искать по всем датам): ").strip()
        if not date_input:
            break
        status, date_obj = is_valid_date(date_input)
        if not status:
            print("Неверный формат даты! Попробуйте снова (например: 04082026).")
            continue
        start_date = date_obj.strftime("%d-%m-%Y")
        break

    if start_date:
        while True:
            end_date_input = input("Конечная дата DD-MM-YYYY (Enter = совпадает с начальной): ").strip()
            if not end_date_input:
                break
            status, end_date_obj = is_valid_date(end_date_input)
            if not status:
                print("Неверный формат конечной даты! Попробуйте снова (например: 04082026).")
                continue
            end_date = end_date_obj.strftime("%d-%m-%Y")
            break

        time_input = input("Точное время начала HH:MM (Enter = с начала дня): ").strip()
        start_time = time_input if time_input else None

        time_end_input = input("Точное время конца HH:MM (Enter = до конца дня): ").strip()
        end_time = time_end_input if time_end_input else None


    # Проверка, что хоть один критерий указан
    if not any([message_id, sender_email, recipient_email, start_date]):
        print("\n⚠ Не указано ни одного критерия (MessageID/отправитель/получатель/дата).")
        confirm_all = input("Искать по ВСЕМ письмам без ограничений? Это может быть очень медленно (y/N): ").strip().lower()
        if confirm_all not in ["y", "yes", "д", "да"]:
            print("Поиск отменен.")
            return

    additional_mailboxes = select_additional_mailboxes_menu()


    print("\nПараметры поиска:")
    if message_id:
        print(f"MessageID: {message_id}")
    if sender_email:
        print(f"Отправитель: {sender_email}")
    if recipient_email:
        print(f"Получатель: {recipient_email}")
    if subject_text:
        print(f"Тема содержит: {subject_text}")
    if start_date:
        print(f"Период: {start_date}{' ' + start_time if start_time else ''}  {end_date or start_date}{' ' + end_time if end_time else ''}")
    print(f"Дополнительных ящиков указано: {len(additional_mailboxes) if additional_mailboxes else 0}")
    print(f"Режим: {'DRY RUN (без удаления)' if settings.dry_run else 'РЕАЛЬНОЕ УДАЛЕНИЕ возможно'}")

    delete_confirm = input("\nУдалять найденные письма (если не DRY_RUN)? (y/N): ").strip().lower()
    delete_found = delete_confirm in ["y", "yes", "д", "да"]

    confirmation = input("\nНачать поиск? (yes/no): ").strip().lower()
    if confirmation not in ["yes", "y", "да", "д"]:
        print("Поиск отменен.")
        return

    print("\nЗапуск поиска...")
    stats = asyncio.run(search_in_all_shared_mailboxes(
        settings=settings,
        sender_email=sender_email,
        recipient_email=recipient_email,
        message_id=message_id,
        subject_text=subject_text,
        start_date=start_date,
        end_date=end_date,
        start_time=start_time,
        end_time=end_time,
        delete_found=delete_found,
        additional_mailboxes=additional_mailboxes

    ))

    if stats.get("details"):
        report_file = write_search_report(stats, settings.reports_dir)
        if report_file:
            print(f"\nОтчет сохранён: {report_file}")

    print(f"\nПоиск завершен!")
    print(f"Обработано ящиков: {stats['processed']}")
    print(f"Найдено писем: {stats['found']}")
    print(f"Удалено писем: {stats['deleted']}")
    print(f"Ошибок: {stats['errors']}")

    input("\nНажмите Enter для продолжения...")

# ============================================================================
# Конец нового функционала универсального поиска
# ============================================================================



if __name__ == "__main__":

    denv_path = os.path.join(os.path.dirname(__file__), '.env')

    if os.path.exists(denv_path):
        load_dotenv(dotenv_path=denv_path,verbose=True, override=True)

    settings = get_initials_config()
    
    # Проверка незавершенных сессий при запуске
    try:
        check_incomplete_sessions(settings)
    except KeyboardInterrupt:
        logger.info("\nCtrl+C нажата. До свидания!")
        sys.exit(EXIT_CODE)
    except Exception as e:
        logger.error(f"Ошибка при проверке незавершенных сессий: {str(e)}")
        logger.exception(e)
    
    try:
        main_menu(settings)
    except KeyboardInterrupt:
        logger.info("\nCtrl+C нажата. До свидания!")
        sys.exit(EXIT_CODE)
    except Exception as exc:
        tb = traceback.extract_tb(exc.__traceback__)
        last_frame = tb[-1] if tb else None
        if last_frame:
            logger.error(f"{type(exc).__name__} at {last_frame.filename}:{last_frame.lineno} in {last_frame.name}: {exc}")
        else:
            logger.error(f"{type(exc).__name__}: {exc}")
        sys.exit(EXIT_CODE)

