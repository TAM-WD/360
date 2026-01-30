'''
Скрипт позволяет найти и удалить письма от определенного отправителя за указанный период из почтовых ящиков сотрудников организации Яндекс 360.

Для запуска скрипта необходим Python версии 3.10 или выше, а также библиотеки aioimaplib, requests, urllib3 и pydantic.

Установить их можно с помощью pip:
pip install aioimaplib requests urllib3 pydantic

Для запуска скрипта можно использовать переменные окружения или изменить значения в функции get_settings():

Переменные окружения:
    OAUTH_TOKEN - OAuth токен для доступа к API Яндекс 360
    ORGANIZATION_ID - ID организации
    APPLICATION_CLIENT_ID - Client ID сервисного приложения
    APPLICATION_CLIENT_SECRET - Client Secret сервисного приложения

Либо в функции get_settings() укажите:
    HARDCODED_TOKEN = "" # OAuth токен
    HARDCODED_ORG_ID = "" # ID организации
    HARDCODED_CLIENT_ID = "" # Client ID сервисного приложения
    HARDCODED_CLIENT_SECRET = "" # Client Secret сервисного приложения

Также предварительно нужно:
1. Получить OAuth токен с правами на чтение аудит-логов (ya360_security:audit_log_mail)
2. Настроить сервисное приложение с правами для получения токенов пользователей с правами mail:imap_full

Параметры запуска (обязательные):
    --from - Email отправителя
    --date - Дата для поиска в формате DD-MM-YYYY
    --time-from - Начальное время поиска в формате HH:MM:SS
    --time-to - Конечное время поиска в формате HH:MM:SS
    
ВАЖНО: 
- Время указывается в московском часовом поясе (UTC+3). Скрипт автоматически конвертирует его в UTC.
- Если нужно удалить за несколько дней - запустите скрипт несколько раз для каждого дня отдельно.

Примеры:
Поиск за конкретный временной промежуток в один день
python deleting_emails_by_sender.py --from sender@example.com --date 29-01-2026 --time-from 10:35:00 --time-to 10:40:00

Поиск за весь рабочий день (с 9:00 до 18:00)
python deleting_emails_by_sender.py --from sender@example.com --date 29-01-2026 --time-from 09:00:00 --time-to 18:00:00

Поиск за весь день (с 00:00 до 23:59)
python deleting_emails_by_sender.py --from sender@example.com --date 29-01-2026 --time-from 00:00:00 --time-to 23:59:59


ВНИМАНИЕ: Скрипт безвозвратно удаляет письма! Перед запуском убедитесь, что указаны правильные критерии поиска.
'''

import argparse
import asyncio
import concurrent.futures
import enum
import logging
import os
import re
import ssl
import sys
import io
import binascii
import traceback
import time
import socket
import gc
from dataclasses import dataclass
from datetime import datetime, timedelta
from http import HTTPStatus
from os import environ
from textwrap import dedent
from typing import Optional, Union, Dict
from threading import Lock
from collections import defaultdict

if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    print("Windows detected: using ProactorEventLoop (no file descriptor limit)")

import aioimaplib
import requests
import urllib3
from pydantic import BaseModel, Field, ConfigDict
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)

SCRIPT_NAME = "deleting_messages"
RUN_TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_DIR = f"{SCRIPT_NAME}_{RUN_TIMESTAMP}"
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOG_FILE = os.path.join(OUTPUT_DIR, f"logs.log")
DELETED_MESSAGES_FILE = os.path.join(OUTPUT_DIR, "deleted_messages.txt")
FAILED_RECIPIENTS_FILE = os.path.join(OUTPUT_DIR, "failed_recipients.txt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("deleting messages")

logging.getLogger('aioimaplib.aioimaplib').setLevel(logging.WARNING)

AUDIT_LOG_PAGE_SIZE = 100
DEFAULT_IMAP_SERVER = "imap.yandex.ru"
DEFAULT_IMAP_PORT = 993
DEFAULT_360_API_URL = "https://api360.yandex.net"
DEFAULT_OAUTH_API_URL = "https://oauth.yandex.ru/token"
OAUTH_TOKEN_ARG = "OAUTH_TOKEN"
ORGANIZATION_ID_ARG = "ORGANIZATION_ID"
APPLICATION_CLIENT_ID_ARG = "APPLICATION_CLIENT_ID"
APPLICATION_CLIENT_SECRET_ARG = "APPLICATION_CLIENT_SECRET"
EXIT_CODE = 1

SEARCH_DATE_RANGE = None

MAX_CONCURRENT_IMAP = 2
BATCH_SIZE = 50
BATCH_PAUSE = 3
IMAP_CONNECT_TIMEOUT = 60 
IMAP_SELECT_TIMEOUT = 90  
IMAP_SEARCH_TIMEOUT = 60 
IMAP_LIST_TIMEOUT = 30
CONNECTION_DELAY = 0.3  
RECIPIENT_DELAY = 0.3
GC_INTERVAL = 10

if sys.platform == 'win32':
    MAX_CONCURRENT_IMAP = 2
    IMAP_CONNECT_TIMEOUT = 30
    BATCH_SIZE = 20
    BATCH_PAUSE = 5
    CONNECTION_DELAY = 0.8
    GC_INTERVAL = 5
    logger.info("Windows detected: adjusted parameters for stability")
    logger.info(f"  - MAX_CONCURRENT_IMAP: {MAX_CONCURRENT_IMAP}")
    logger.info(f"  - BATCH_SIZE: {BATCH_SIZE}")
    logger.info(f"  - IMAP_CONNECT_TIMEOUT: {IMAP_CONNECT_TIMEOUT}")
    logger.info(f"  - CONNECTION_DELAY: {CONNECTION_DELAY}")

_token_cache: Dict[str, tuple[str, float]] = {}
_token_cache_lock = Lock()
_http_session: Optional[requests.Session] = None
_imap_connection_lock = asyncio.Lock()

# Глобальная статистика удаления
_deletion_stats = {
    'total_deleted': 0,
    'recipients_with_deletions': 0,
    'recipients_without_deletions': 0,
    'recipients_with_errors': 0,
    'lock': Lock()
}


def add_deletion_stats(deleted_count: int, has_error: bool = False):
    """Добавить статистику удаления"""
    with _deletion_stats['lock']:
        if has_error:
            _deletion_stats['recipients_with_errors'] += 1
        else:
            _deletion_stats['total_deleted'] += deleted_count
            if deleted_count > 0:
                _deletion_stats['recipients_with_deletions'] += 1
            else:
                _deletion_stats['recipients_without_deletions'] += 1


def get_deletion_stats() -> dict:
    """Получить текущую статистику удаления"""
    with _deletion_stats['lock']:
        return {
            'total_deleted': _deletion_stats['total_deleted'],
            'recipients_with_deletions': _deletion_stats['recipients_with_deletions'],
            'recipients_without_deletions': _deletion_stats['recipients_without_deletions'],
            'recipients_with_errors': _deletion_stats['recipients_with_errors']
        }


def reset_deletion_stats():
    """Сбросить статистику удаления"""
    with _deletion_stats['lock']:
        _deletion_stats['total_deleted'] = 0
        _deletion_stats['recipients_with_deletions'] = 0
        _deletion_stats['recipients_without_deletions'] = 0
        _deletion_stats['recipients_with_errors'] = 0


def get_http_session() -> requests.Session:
    global _http_session
    if _http_session is None:
        _http_session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=50,
            max_retries=3,
            pool_block=False
        )
        _http_session.mount('https://', adapter)
        _http_session.mount('http://', adapter)
    return _http_session


def get_cached_token(email: str) -> Optional[str]:
    with _token_cache_lock:
        if email in _token_cache:
            token, expiry = _token_cache[email]
            if time.time() < (expiry - 300):
                logger.debug(f"🔑 Using cached token for {email}")
                return token
            else:
                logger.debug(f"🔑 Cached token expired for {email}")
                del _token_cache[email]
    return None


def cache_token(email: str, token: str, expires_in: int):
    with _token_cache_lock:
        expiry = time.time() + expires_in
        _token_cache[email] = (token, expiry)
        logger.debug(f"🔑 Cached token for {email} (expires in {expires_in}s)")

def force_cleanup():
    try:
        gc.collect()
        logger.debug("🧹 Garbage collection completed")
    except Exception as e:
        logger.debug(f"GC error: {e}")


def decode_modified_utf7(s: str) -> str:
    if not s or '&' not in s:
        return s
    
    result = []
    i = 0
    
    while i < len(s):
        if s[i] == '&':
            end = s.find('-', i + 1)
            if end == -1:
                result.append(s[i:])
                break
            
            if end == i + 1:
                result.append('&')
                i = end + 1
            else:
                encoded = s[i + 1:end]
                try:
                    encoded_fixed = encoded.replace(',', '/')
                    padding = (4 - len(encoded_fixed) % 4) % 4
                    encoded_fixed += '=' * padding
                    decoded_bytes = binascii.a2b_base64(encoded_fixed.encode('ascii'))
                    decoded_str = decoded_bytes.decode('utf-16-be', errors='replace')
                    result.append(decoded_str)
                except Exception:
                    result.append(s[i:end + 1])
                i = end + 1
        else:
            result.append(s[i])
            i += 1
    
    return ''.join(result)


def escape_imap_folder_name(folder_name: str) -> str:
    if not folder_name:
        return folder_name
    
    result = folder_name.replace('\\', '\\\\')
    result = result.replace('"', '\\"')
    
    return result


def map_folder(folder: Optional[bytes]) -> Optional[tuple[str, str]]:
    if not folder:
        return None
        
    if folder == b"LIST Completed.":
        return None
    
    try:
        if isinstance(folder, bytearray):
            return None
            
        decoded = folder.decode("utf-8", errors='replace')
    except:
        try:
            decoded = folder.decode("ascii", errors='replace')
        except:
            logger.debug(f"map_folder: failed to decode")
            return None
    
    if '\\Noselect' in decoded:
        return None
    
    if re.search(r'\{\d+\}$', decoded.strip()):
        return None
    
    parts = decoded.split('"|"')
    
    if len(parts) >= 2:
        folder_name_raw = parts[-1].strip().strip('"')
    elif '" "|" ' in decoded:
        folder_name_raw = decoded.split('" "|" ')[-1].strip()
    else:
        tokens = decoded.split()
        if len(tokens) >= 3:
            delimiter_index = None
            for i, token in enumerate(tokens):
                if '"|"' in token:
                    delimiter_index = i
                    break
            
            if delimiter_index is not None:
                folder_name_raw = ' '.join(tokens[delimiter_index + 1:]).strip('"')
            else:
                folder_name_raw = tokens[-1].strip('"')
        else:
            logger.debug(f"map_folder: not enough tokens in '{decoded[:50]}...'")
            return None
    
    folder_name_raw = folder_name_raw.strip().replace('\r', '').replace('\n', '')
    
    if not folder_name_raw or folder_name_raw in ['NIL']:
        logger.debug(f"map_folder: empty or NIL folder name")
        return None
    
    folder_name_decoded = decode_modified_utf7(folder_name_raw)
    folder_name_escaped = escape_imap_folder_name(folder_name_raw)
    
    return (f'"{folder_name_escaped}"', folder_name_decoded if folder_name_decoded else folder_name_raw)


def extract_emails(field_value: str) -> list[str]:
    if not field_value:
        return []
    
    email_pattern = r'<([^>]+)>|([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})'
    
    emails = []
    for match in re.finditer(email_pattern, field_value):
        email = match.group(1) or match.group(2)
        if email:
            emails.append(email.strip())
    
    return emails


def arg_parser():
    parser = argparse.ArgumentParser(
        description=dedent(
            f"""
            Скрипт для удаления писем по отправителю и временному диапазону в организациях Яндекс 360.

            Переменные окружения:
            {OAUTH_TOKEN_ARG} - OAuth токен,
            {ORGANIZATION_ID_ARG} - ID организации,
            {APPLICATION_CLIENT_ID_ARG} - Client ID приложения,
            {APPLICATION_CLIENT_SECRET_ARG} - Client Secret приложения
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--from", 
        dest="sender",
        help="Email адрес отправителя для поиска", 
        type=str, 
        required=True
    )
    parser.add_argument(
        "--date",
        help="Дата для поиска (формат: DD-MM-YYYY).",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--time-from",
        help="Начальное время поиска в московском часовом поясе (формат: HH:MM:SS)",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--time-to",
        help="Конечное время поиска в московском часовом поясе (формат: HH:MM:SS)",
        type=str,
        required=True,
    )

    return parser


def main():
    global SEARCH_DATE_RANGE
    
    parsr = arg_parser()
    args = parsr.parse_args()
    
    try:
        settings = get_settings()
    except ValueError:
        logging.error(f"ОШИБКА: Значение {ORGANIZATION_ID_ARG} должно быть целым числом.")
        sys.exit(EXIT_CODE)
    except KeyError as key:
        logger.error(f"ОШИБКА: Не указаны обязательные переменные окружения: {key}")
        parsr.print_usage()
        sys.exit(EXIT_CODE)
    
    sender_email = args.sender
    date_str = args.date
    time_from_str = args.time_from
    time_to_str = args.time_to
    
    # Смещение для московского времени (UTC+3)
    MOSCOW_UTC_OFFSET = timedelta(hours=3)
    
    try:
        # Парсим дату (одну)
        search_date = datetime.strptime(date_str, "%d-%m-%Y")
        
        # Парсим время
        time_from = datetime.strptime(time_from_str, "%H:%M:%S").time()
        time_to = datetime.strptime(time_to_str, "%H:%M:%S").time()
        
        # Комбинируем дату и время (это московское время)
        datetime_from_moscow = datetime.combine(search_date.date(), time_from)
        datetime_to_moscow = datetime.combine(search_date.date(), time_to)
        
        # Конвертируем из московского времени в UTC (вычитаем 3 часа)
        datetime_from = datetime_from_moscow - MOSCOW_UTC_OFFSET
        datetime_to = datetime_to_moscow - MOSCOW_UTC_OFFSET
        
    except ValueError as e:
        logger.error(f"ОШИБКА: Неверный формат даты/времени. Используйте DD-MM-YYYY для даты и HH:MM:SS для времени")
        logger.error(f"Детали: {e}")
        sys.exit(EXIT_CODE)
    
    if datetime_from > datetime_to:
        logger.error(f"ОШИБКА: Начальное время должно быть раньше или равно конечному времени")
        logger.error(f"ПОДСКАЗКА: Если нужно удалить письма за период с вечера одного дня до утра следующего, запустите скрипт дважды:")
        logger.error(f"  1) --date {date_str} --time-from {time_from_str} --time-to 23:59:59")
        logger.error(f"  2) --date <следующий день> --time-from 00:00:00 --time-to {time_to_str}")
        sys.exit(EXIT_CODE)
    
    # Устанавливаем диапазон для IMAP поиска (используем только даты для IMAP)
    after_date = datetime_from.replace(hour=0, minute=0, second=0)
    before_date = (datetime_to + timedelta(days=1)).replace(hour=0, minute=0, second=0)
    SEARCH_DATE_RANGE = (after_date, before_date)
    
    # Диапазон для API с учетом времени
    api_after_datetime = datetime_from
    api_before_datetime = datetime_to
    
    if os.path.exists(DELETED_MESSAGES_FILE):
        os.remove(DELETED_MESSAGES_FILE)
    
    if os.path.exists(FAILED_RECIPIENTS_FILE):
        os.remove(FAILED_RECIPIENTS_FILE)
    
    # Сбрасываем статистику удаления
    reset_deletion_stats()
    
    logger.info(f"Директория вывода: {os.path.abspath(OUTPUT_DIR)}")
    logger.info(f"Лог-файл: {LOG_FILE}")
    logger.info(f"Файл удаленных сообщений: {DELETED_MESSAGES_FILE}")
    logger.info(f"Фильтр отправителя: {sender_email}")
    logger.info(f"Дата поиска: {search_date.strftime('%d-%m-%Y')}")
    logger.info(f"Диапазон времени (Москва UTC+3): {datetime_from_moscow.strftime('%H:%M:%S')} до {datetime_to_moscow.strftime('%H:%M:%S')}")
    logger.info(f"Полный диапазон datetime (UTC для API): {datetime_from.strftime('%d-%m-%Y %H:%M:%S')} до {datetime_to.strftime('%d-%m-%Y %H:%M:%S')}")
    logger.info(f"Диапазон дат для IMAP: {after_date.strftime('%d-%b-%Y')} до {before_date.strftime('%d-%b-%Y')} (исключая)")
    logger.info(f"Максимум одновременных IMAP подключений: {MAX_CONCURRENT_IMAP}")
    logger.info(f"Размер пакета: {BATCH_SIZE} получателей")
    logger.info(f"Пауза между пакетами: {BATCH_PAUSE} секунд")
    logger.info("Скрипт удаления писем запущен.")
    
    client = Client360(
        token=settings.oauth_token,
        org_id=settings.organization_id,
        client_id=settings.app_client_id,
        secret=settings.app_client_secret,
    )
    
    api_after_datetime_str = datetime_from.strftime("%Y-%m-%dT%H:%M:%SZ")
    api_before_datetime_str = datetime_to.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    fetched_messages = fetch_audit_logs(
        client=client,
        sender_email=sender_email,
        datetime_from=datetime_from,
        datetime_to=datetime_to,
        datetime_from_moscow=datetime_from_moscow,
        datetime_to_moscow=datetime_to_moscow,
        after_date=api_after_datetime_str,
        before_date=api_before_datetime_str,
    )
    
    total_events = sum(len(msg_ids) for msg_ids in fetched_messages.recipient_messages.values())
    total_unique_msgids = len(fetched_messages.all_message_ids)
    
    logger.info(f"Найдено {total_events} событий в аудит-логе")
    logger.info(f"Уникальных message ID: {total_unique_msgids}")
    logger.info(f"Уникальных получателей: {len(fetched_messages.recipient_messages)}")
    
    if fetched_messages.subjects:
        logger.info(f"Примеры тем ({min(5, len(fetched_messages.subjects))} из {len(fetched_messages.subjects)}):")
        for i, subject in enumerate(list(fetched_messages.subjects)[:5]):
            logger.info(f"  {i+1}. {subject}")
    
    if total_events == 0:
        logger.info("Не найдено событий, соответствующих критериям.")
        logger.info("Скрипт удаления писем завершен.")
        logger.info(f"Результаты сохранены в: {os.path.abspath(OUTPUT_DIR)}")
        return
    
    if len(fetched_messages.recipient_messages) == 0:
        logger.info("Не найдено получателей для удаления.")
        logger.info("Скрипт удаления писем завершен.")
        logger.info(f"Результаты сохранены в: {os.path.abspath(OUTPUT_DIR)}")
        return
        
    if is_deletion_approve(
        sender=sender_email,
        event_count=total_events,
        unique_msgids=total_unique_msgids,
        subjects=fetched_messages.subjects,
        recipient_messages=fetched_messages.recipient_messages
    ):
        try:
            asyncio.run(
                process_recipients_in_batches(
                    client=client, 
                    recipient_messages=fetched_messages.recipient_messages
                )
            )
        except KeyboardInterrupt:
            logger.info("Процесс прерван пользователем")
        except Exception as err:
            logger.error(f"Ошибка при обработке: {err}")
            logger.error(traceback.format_exc())
    
    logger.info("Скрипт удаления писем завершен.")
    logger.info(f"Результаты сохранены в: {os.path.abspath(OUTPUT_DIR)}")
    
    if sys.platform == 'win32':
        logger.info("Очистка ресурсов...")
        force_cleanup()
        time.sleep(2)


async def keepalive(connector):
    try:
        if connector and hasattr(connector, 'protocol'):
            state = connector.protocol.state
            if state in ["AUTH", "SELECTED"]:
                await asyncio.wait_for(connector.noop(), timeout=5)
                return True
    except Exception as e:
        logger.debug(f"Keepalive failed: {e}")
    return False


async def safe_close(connector):
    try:
        if not connector:
            return
        
        try:
            state = connector.protocol.state if hasattr(connector, 'protocol') else None
            
            if state == "SELECTED":
                try:
                    await asyncio.wait_for(connector.close(), timeout=2)
                except:
                    pass
            
            try:
                await asyncio.wait_for(connector.logout(), timeout=2)
            except:
                pass
        except:
            pass
        
        try:
            if hasattr(connector, '_transport') and connector._transport:
                connector._transport.abort()
        except:
            pass
        
        try:
            if hasattr(connector, 'protocol'):
                connector.protocol = None
            if hasattr(connector, '_transport'):
                connector._transport = None
        except:
            pass
        
        if sys.platform == 'win32':
            await asyncio.sleep(0.5)
        else:
            await asyncio.sleep(0.3)
        
    except Exception as e:
        logger.debug(f"Error during safe_close: {type(e).__name__}")
    finally:
        connector = None


async def connect_to_mail(username: str, access_token: str, max_retries: int = 3):
    
    async with _imap_connection_lock:
        await asyncio.sleep(CONNECTION_DELAY)
        
        for attempt in range(1, max_retries + 1):
            imap_connector = None
            try:
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                if sys.platform == 'win32':
                    timeout = min(IMAP_CONNECT_TIMEOUT, 30)
                else:
                    timeout = IMAP_CONNECT_TIMEOUT
                
                imap_connector = aioimaplib.IMAP4_SSL(
                    host=DEFAULT_IMAP_SERVER,
                    port=DEFAULT_IMAP_PORT,
                    ssl_context=ssl_context,
                    timeout=timeout
                )
                
                await asyncio.wait_for(imap_connector.wait_hello_from_server(), timeout=timeout)
                
                status, data = await asyncio.wait_for(
                    imap_connector.xoauth2(user=username, token=access_token),
                    timeout=timeout
                )
                
                if status != "OK":
                    await safe_close(imap_connector)
                    raise ConnectionError(f"Auth failed: {status}")
                
                logger.debug(f"✓ IMAP connected for {username}")
                return imap_connector
                
            except socket.gaierror as e:
                if imap_connector:
                    await safe_close(imap_connector)
                logger.warning(f"DNS error connecting to IMAP for {username} (attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(5 * attempt)
                else:
                    raise ConnectionError(f"DNS resolution failed after {max_retries} attempts")
                    
            except (TimeoutError, asyncio.TimeoutError) as e:
                if imap_connector:
                    await safe_close(imap_connector)
                logger.warning(f"Timeout connecting to IMAP for {username} (attempt {attempt}/{max_retries})")
                if attempt < max_retries:
                    await asyncio.sleep(3 * attempt)
                else:
                    raise ConnectionError(f"Connection timeout after {max_retries} attempts")
            
            except aioimaplib.aioimaplib.Abort as e:
                if imap_connector:
                    await safe_close(imap_connector)
                logger.warning(f"IMAP protocol error for {username} (attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(4 * attempt)
                else:
                    raise ConnectionError(f"IMAP protocol error after {max_retries} attempts")
                    
            except ConnectionError as e:
                if imap_connector:
                    await safe_close(imap_connector)
                logger.warning(f"Connection error for {username} (attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    await asyncio.sleep(3 * attempt)
                else:
                    raise
                    
            except Exception as e:
                if imap_connector:
                    await safe_close(imap_connector)
                logger.warning(f"Error connecting to IMAP for {username} (attempt {attempt}/{max_retries}): {type(e).__name__}")
                logger.debug(f"Connection error details: {str(e)[:200]}")
                if attempt < max_retries:
                    await asyncio.sleep(2 * attempt)
                else:
                    raise ConnectionError(f"Connection failed: {type(e).__name__}")


async def delete(connector, message_ids: set[str], loop, username, access_token, folder_encoded, folder_decoded, client):
    """
    Ищет и удаляет все письма из списка message_ids в указанной папке
    """
    global SEARCH_DATE_RANGE
    
    try:
        if SEARCH_DATE_RANGE:
            after_date, before_date = SEARCH_DATE_RANGE
            after_str = after_date.strftime("%d-%b-%Y")
            before_str = before_date.strftime("%d-%b-%Y")
            search_criteria = f'SINCE {after_str} BEFORE {before_str}'
        else:
            search_criteria = "ALL"
        
        try:
            status, messages = await asyncio.wait_for(
                connector.uid_search(search_criteria),
                timeout=IMAP_SEARCH_TIMEOUT
            )
        except Exception as e:
            logger.debug(f"Search error in '{folder_decoded}': {type(e).__name__}")
            return 0
        
        if status != "OK" or not messages or not messages[0]:
            return 0
        
        if isinstance(messages[0], bytes) and b'[UNAVAILABLE]' in messages[0]:
            return 0
        
        message_uids = messages[0].split()
        
        if not message_uids:
            logger.debug(f"No messages in date range in '{folder_decoded}'")
            return 0
        
        total = len(message_uids)
        logger.info(f"Найдено {total} сообщений для проверки в '{folder_decoded}'")
        
        MAX_TO_CHECK = 500000
        if total > MAX_TO_CHECK:
            logger.warning(f"Ограничение до {MAX_TO_CHECK} сообщений")
            message_uids = message_uids[:MAX_TO_CHECK]
        
        # Создаем варианты поиска для каждого message_id
        search_variants = {}
        for msg_id in message_ids:
            rfc_clean = msg_id.strip('<>')
            search_variants[msg_id] = [
                rfc_clean.encode(),
                f"<{rfc_clean}>".encode(),
                f"Message-ID: {rfc_clean}".encode(),
                f"Message-ID: <{rfc_clean}>".encode(),
                f"Message-Id: {rfc_clean}".encode(),
                f"Message-Id: <{rfc_clean}>".encode(),
            ]
        
        CHUNK_SIZE = 500 
        PAUSE_BETWEEN_MESSAGES = 0.05 
        PAUSE_BETWEEN_CHUNKS = 1.0
        KEEPALIVE_INTERVAL = 50
        
        checked = 0
        failed_uids = []
        last_keepalive = 0
        deleted_count = 0
        
        # Если писем мало (< 1000), не делаем переподключение между chunks
        use_reconnect = total > 1000
        
        for chunk_start in range(0, len(message_uids), CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, len(message_uids))
            chunk = message_uids[chunk_start:chunk_end]
            
            chunk_num = chunk_start // CHUNK_SIZE + 1
            total_chunks = (len(message_uids) + CHUNK_SIZE - 1) // CHUNK_SIZE
            
            # Переподключаемся только если много писем и это не первый chunk
            if use_reconnect and chunk_num > 1:
                logger.info(f"📦 Chunk {chunk_num}/{total_chunks}: UIDs {chunk_start+1}-{chunk_end} in '{folder_decoded}'")
                logger.info(f"🔄 Reconnecting for chunk {chunk_num}...")
                
                try:
                    await safe_close(connector)
                except:
                    pass
                
                await asyncio.sleep(2.0)
                
                cached_token = get_cached_token(username)
                if cached_token:
                    access_token = cached_token
                else:
                    try:
                        fresh_token = await loop.run_in_executor(None, client.user_token.get, username)
                        access_token = fresh_token.access_token
                        cache_token(username, access_token, fresh_token.expires_in)
                        logger.debug(f"✓ Token refreshed for chunk {chunk_num}")
                    except Exception as e:
                        logger.debug(f"Token refresh failed: {e}")
                
                reconnect_attempts = 0
                max_reconnect_attempts = 3
                
                while reconnect_attempts < max_reconnect_attempts:
                    try:
                        connector = await connect_to_mail(username=username, access_token=access_token)
                        
                        status, _ = await asyncio.wait_for(
                            connector.select(folder_encoded),
                            timeout=IMAP_SELECT_TIMEOUT
                        )
                        
                        if status == "OK":
                            logger.info(f"✅ Connected for chunk {chunk_num}")
                            break
                        else:
                            raise Exception(f"SELECT failed: {status}")
                        
                    except Exception as e:
                        reconnect_attempts += 1
                        logger.warning(f"Reconnect attempt {reconnect_attempts}/{max_reconnect_attempts} failed: {type(e).__name__}")
                        
                        if reconnect_attempts < max_reconnect_attempts:
                            await asyncio.sleep(3.0 * reconnect_attempts)
                        else:
                            logger.error(f"Failed to reconnect after {max_reconnect_attempts} attempts")
                            return deleted_count
            else:
                if total_chunks > 1:
                    logger.info(f"📦 Chunk {chunk_num}/{total_chunks}: UIDs {chunk_start+1}-{chunk_end}")
            
            for uid in chunk:
                try:
                    msg_uid = int(uid)
                except ValueError:
                    continue
                
                checked += 1
                
                if checked % 100 == 0:
                    logger.info(f"⏳ Прогресс: {checked}/{len(message_uids)} сообщений...")
                
                if checked - last_keepalive >= KEEPALIVE_INTERVAL:
                    await keepalive(connector)
                    last_keepalive = checked
                
                try:
                    status, data = await asyncio.wait_for(
                        connector.uid('fetch', msg_uid, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"),
                        timeout=15
                    )
                    
                    if status == "OK" and len(data) > 1:
                        headers = data[1]
                        
                        # Проверяем все варианты всех message_id
                        found_msg_id = None
                        for msg_id, variants in search_variants.items():
                            for variant in variants:
                                if variant in headers:
                                    found_msg_id = msg_id
                                    break
                            if found_msg_id:
                                break
                        
                        if found_msg_id:
                            logger.info(f"✅ Найдено сообщение с UID {msg_uid} (msgId: {found_msg_id}) в '{folder_decoded}'")
                            try:
                                await asyncio.wait_for(
                                    connector.uid('store', msg_uid, "+FLAGS", "\\Deleted"),
                                    timeout=15
                                )
                                await asyncio.wait_for(connector.expunge(), timeout=30)
                                
                                deleted_count += 1
                                
                                await loop.run_in_executor(
                                    None,
                                    write_deleted,
                                    username,
                                    folder_decoded,
                                    found_msg_id
                                )
                                
                            except Exception as e:
                                logger.error(f"Ошибка удаления UID {msg_uid}: {e}")
                    
                    await asyncio.sleep(PAUSE_BETWEEN_MESSAGES)
                    
                except (TimeoutError, asyncio.TimeoutError, aioimaplib.aioimaplib.CommandTimeout):
                    logger.warning(f"⏱ Timeout on UID {msg_uid}")
                    failed_uids.append(msg_uid)
                    continue
                
                except aioimaplib.aioimaplib.Abort as e:
                    logger.warning(f"⚠️  IMAP Abort on UID {msg_uid}: {str(e)[:100]}")
                    failed_uids.append(msg_uid)
                    continue
                    
                except Exception as e:
                    logger.warning(f"Error on UID {msg_uid}: {type(e).__name__}")
                    failed_uids.append(msg_uid)
                    continue
            
            if chunk_end < len(message_uids) and use_reconnect:
                logger.info(f"💤 Resting {PAUSE_BETWEEN_CHUNKS}s...")
                await asyncio.sleep(PAUSE_BETWEEN_CHUNKS)
        
        if failed_uids:
            logger.warning(f"⚠️  Failed UIDs: {len(failed_uids)}")
        
        if deleted_count == 0:
            logger.debug(f"Соответствующих Message-ID не найдено в '{folder_decoded}' (проверено {checked} сообщений)")
        
        return deleted_count
        
    except Exception as e:
        logger.error(f"Ошибка в delete для '{folder_decoded}': {type(e).__name__} - {e}")
        logger.debug(traceback.format_exc())
        return 0


async def get_user_token_async(client, recipient, loop, max_retries: int = 3):
    
    cached_token = get_cached_token(recipient)
    if cached_token:
        return cached_token
    
    for attempt in range(1, max_retries + 1):
        try:
            if attempt > 1:
                await asyncio.sleep(2 * attempt)
            
            user_token = await loop.run_in_executor(
                None, 
                client.user_token.get, 
                recipient
            )
            
            cache_token(recipient, user_token.access_token, user_token.expires_in)
            
            logger.debug(f"🔑 Token obtained and cached for {recipient}")
            return user_token.access_token
            
        except ClientOAuthError as e:
            error_msg = f"OAuth error: {e}"
            logger.error(f"❌ Token request failed for {recipient} (attempt {attempt}/{max_retries}): {error_msg}")
            if attempt == max_retries:
                raise Exception(f"OAuth failed after {max_retries} attempts: {e}")
                
        except requests.exceptions.ConnectionError as e:
            error_msg = "Network/DNS error"
            logger.error(f"❌ Token request failed for {recipient} (attempt {attempt}/{max_retries}): {error_msg}")
            
            if attempt == max_retries:
                raise Exception(f"Network error after {max_retries} attempts")
            
            await asyncio.sleep(5 * attempt)
            
        except Exception as e:
            error_msg = f"{type(e).__name__}"
            logger.error(f"❌ Token request failed for {recipient} (attempt {attempt}/{max_retries}): {error_msg}")
            logger.debug(f"Token error details: {str(e)[:200]}")
            
            if attempt == max_retries:
                raise Exception(f"Token request failed: {type(e).__name__}")


async def process_recipient(recipient, message_ids, client, loop, semaphore, recipient_index, total_recipients):
    """
    Обрабатывает одного получателя - ищет и удаляет письма с указанными message_ids
    """
    
    await asyncio.sleep(RECIPIENT_DELAY * (recipient_index % MAX_CONCURRENT_IMAP))
    
    async with semaphore:
        logger.info(f"🚀 [{recipient_index + 1}/{total_recipients}] Starting processing for {recipient}")
        logger.info(f"   📧 Писем к удалению для этого получателя: {len(message_ids)}")
        
        if recipient_index % GC_INTERVAL == 0:
            force_cleanup()
        
        connector = None
        try:
            try:
                access_token = await get_user_token_async(client, recipient, loop)
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)[:200]
                logger.error(f"❌ Не удалось получить токен для {recipient}: {error_type} - {error_msg}")
                await loop.run_in_executor(
                    None,
                    write_failed_recipient,
                    recipient,
                    f"Token error: {error_type}",
                    len(message_ids)
                )
                add_deletion_stats(0, has_error=True)
                return
            
            try:
                connector = await connect_to_mail(username=recipient, access_token=access_token)
                
                if connector.protocol.state != "AUTH":
                    raise ConnectionError("Not authenticated")
                
                logger.info(f"🔌 Connected to {recipient}")
                
                status, folders_response = await asyncio.wait_for(
                    connector.list('""', "*"),
                    timeout=IMAP_LIST_TIMEOUT
                )
                
                logger.info(f"📋 LIST command status: {status}, received {len(folders_response)} lines")
                
            except (TimeoutError, asyncio.TimeoutError) as e:
                error_msg = "Таймаут подключения/LIST"
                logger.error(f"❌ Не удалось подключиться к {recipient}: {error_msg}")
                await loop.run_in_executor(
                    None,
                    write_failed_recipient,
                    recipient,
                    "Connection timeout",
                    len(message_ids)
                )
                if connector:
                    await safe_close(connector)
                add_deletion_stats(0, has_error=True)
                return
                
            except ConnectionError as e:
                error_msg = f"Ошибка подключения: {str(e)[:100]}"
                logger.error(f"❌ Не удалось подключиться к {recipient}: {error_msg}")
                await loop.run_in_executor(
                    None,
                    write_failed_recipient,
                    recipient,
                    f"Connection error: {str(e)[:100]}",
                    len(message_ids)
                )
                if connector:
                    await safe_close(connector)
                add_deletion_stats(0, has_error=True)
                return
                
            except socket.gaierror as e:
                error_msg = "Ошибка DNS"
                logger.error(f"❌ Не удалось подключиться к {recipient}: {error_msg}")
                await loop.run_in_executor(
                    None,
                    write_failed_recipient,
                    recipient,
                    "DNS error",
                    len(message_ids)
                )
                if connector:
                    await safe_close(connector)
                add_deletion_stats(0, has_error=True)
                return
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = f"{error_type}: {str(e)[:100]}"
                logger.error(f"❌ Не удалось подключиться к {recipient}: {error_msg}")
                logger.debug(traceback.format_exc())
                await loop.run_in_executor(
                    None,
                    write_failed_recipient,
                    recipient,
                    f"Unexpected: {error_type}",
                    len(message_ids)
                )
                if connector:
                    await safe_close(connector)
                add_deletion_stats(0, has_error=True)
                return
            
            folders = []
            i = 0
            
            while i < len(folders_response):
                folder_line = folders_response[i]
                
                if folder_line == b"LIST Completed.":
                    i += 1
                    continue
                
                if isinstance(folder_line, bytearray):
                    if i > 0:
                        prev_line = folders_response[i - 1]
                        if isinstance(prev_line, bytes):
                            prev_decoded = prev_line.decode('utf-8', errors='replace')
                            if not re.search(r'\{(\d+)\}\s*$', prev_decoded):
                                i += 1
                                continue
                        else:
                            i += 1
                            continue
                    else:
                        i += 1
                        continue
                
                if not isinstance(folder_line, bytes):
                    i += 1
                    continue
                
                try:
                    decoded = folder_line.decode('utf-8', errors='replace')
                except:
                    i += 1
                    continue
                
                if '\\Noselect' in decoded:
                    i += 1
                    continue
                
                literal_match = re.search(r'\{(\d+)\}\s*$', decoded)
                
                if literal_match:
                    if i + 1 < len(folders_response):
                        next_line = folders_response[i + 1]
                        
                        if isinstance(next_line, bytearray):
                            try:
                                folder_name_raw = next_line.decode('utf-8', errors='replace')
                                folder_name_decoded = decode_modified_utf7(folder_name_raw)
                                folder_name_escaped = escape_imap_folder_name(folder_name_raw)
                                
                                folders.append((f'"{folder_name_escaped}"', folder_name_decoded))
                                logger.debug(f"[{i}+{i+1}] ✓ PARSED literal: '{folder_name_decoded}'")
                                
                                i += 2
                                continue
                                
                            except Exception as e:
                                logger.debug(f"[{i+1}] Failed to process literal continuation: {e}")
                                i += 2
                                continue
                        else:
                            logger.debug(f"[{i+1}] Expected bytearray after literal header")
                            i += 1
                            continue
                    else:
                        logger.debug(f"[{i}] Literal header at end of list")
                        i += 1
                        continue
                
                parsed = map_folder(folder_line)
                if parsed:
                    folder_encoded, folder_decoded = parsed
                    folders.append(parsed)
                    logger.debug(f"[{i}] ✓ PARSED: '{folder_decoded}'")
                
                i += 1
            
            logger.info(f"📁 Total parsed folders: {len(folders)} for {recipient}")
            
            if not folders:
                logger.warning(f"⚠️  No folders found for {recipient}")
                if connector:
                    await safe_close(connector)
                return
            
            priority_folders = []
            other_folders = []
            
            priority_keywords = ['inbox', 'sent', 'drafts', 'отправленные', 'черновики', 'входящие', 'spam', 'junk']
            
            for folder_encoded, folder_decoded in folders:
                folder_lower = folder_decoded.lower()
                is_priority = any(keyword in folder_lower for keyword in priority_keywords)
                
                if is_priority:
                    priority_folders.append((folder_encoded, folder_decoded))
                else:
                    other_folders.append((folder_encoded, folder_decoded))
            
            total_deleted = 0
            total_checked_folders = 0
            
            # Обрабатываем приоритетные папки с ОДНИМ соединением
            for folder_encoded, folder_decoded in priority_folders:
                logger.info(f"📂 [{recipient}] Обработка приоритетной папки: {folder_decoded}")
                total_checked_folders += 1
                
                try:
                    # Используем SELECT вместо переподключения
                    status, _ = await asyncio.wait_for(
                        connector.select(folder_encoded),
                        timeout=IMAP_SELECT_TIMEOUT
                    )
                    
                    if status != "OK":
                        logger.debug(f"Cannot select folder '{folder_decoded}': {status}")
                        continue
                    
                    logger.info(f"📂 Обработка '{folder_decoded}'")
                    
                    count = await delete(
                        connector=connector,
                        message_ids=message_ids,
                        loop=loop,
                        username=recipient,
                        access_token=access_token,
                        folder_encoded=folder_encoded,
                        folder_decoded=folder_decoded,
                        client=client
                    )
                    
                    total_deleted += count
                    
                    if count > 0:
                        logger.info(f"✅ Удалено {count} сообщение(й) из '{folder_decoded}'")
                    
                except Exception as e:
                    error_type = type(e).__name__
                    logger.warning(f"⚠️  Error processing '{folder_decoded}': {error_type}")
                    logger.debug(f"Full error: {str(e)[:200]}")
                    continue
            
            # Обрабатываем остальные папки с ТЕМ ЖЕ соединением
            for folder_encoded, folder_decoded in other_folders:
                logger.info(f"📂 [{recipient}] Обработка: {folder_decoded}")
                total_checked_folders += 1
                
                try:
                    status, _ = await asyncio.wait_for(
                        connector.select(folder_encoded),
                        timeout=IMAP_SELECT_TIMEOUT
                    )
                    
                    if status != "OK":
                        logger.debug(f"Cannot select folder '{folder_decoded}': {status}")
                        continue
                    
                    logger.info(f"📂 Обработка '{folder_decoded}'")
                    
                    count = await delete(
                        connector=connector,
                        message_ids=message_ids,
                        loop=loop,
                        username=recipient,
                        access_token=access_token,
                        folder_encoded=folder_encoded,
                        folder_decoded=folder_decoded,
                        client=client
                    )
                    
                    total_deleted += count
                    
                    if count > 0:
                        logger.info(f"✅ Удалено {count} сообщение(й) из '{folder_decoded}'")
                    
                except Exception as e:
                    error_type = type(e).__name__
                    logger.warning(f"⚠️  Error processing '{folder_decoded}': {error_type}")
                    logger.debug(f"Full error: {str(e)[:200]}")
                    continue
            
            if total_deleted > 0:
                logger.info(f"✅ [{recipient}] Total deleted: {total_deleted} message(s) from {total_checked_folders} folder(s)")
            else:
                logger.info(f"ℹ️  [{recipient}] No messages found (checked {total_checked_folders} folder(s)). Messages may have been already deleted or were not delivered to this recipient.")
            
            # Обновляем глобальную статистику
            add_deletion_stats(total_deleted)
        
        except Exception as e:
            error_type = type(e).__name__
            error_msg = f"{error_type}: {str(e)[:200]}"
            logger.error(f"❌ Неожиданная ошибка при обработке {recipient}: {error_msg}")
            logger.debug(traceback.format_exc())
            
            await loop.run_in_executor(
                None,
                write_failed_recipient,
                recipient,
                f"Unexpected error: {error_type}",
                len(message_ids)
            )
            add_deletion_stats(0, has_error=True)
        finally:
            if connector:
                await safe_close(connector)
            force_cleanup()


async def process_recipients_batch(client, recipient_messages, loop, semaphore, batch_offset):
    """
    Обрабатывает пакет получателей
    recipient_messages: dict {recipient: set(message_ids)}
    """
    tasks = []
    recipients = list(recipient_messages.keys())
    total_recipients = len(recipients)
    
    for idx, recipient in enumerate(recipients):
        message_ids = recipient_messages[recipient]
        task = asyncio.create_task(
            process_recipient(
                recipient, 
                message_ids, 
                client, 
                loop, 
                semaphore,
                batch_offset + idx,
                batch_offset + total_recipients
            )
        )
        tasks.append(task)
    
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        successful = sum(1 for r in results if not isinstance(r, Exception))
        failed = sum(1 for r in results if isinstance(r, Exception))
        
        for idx, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"❌ Task exception for {recipients[idx]}: {type(result).__name__} - {result}")
        
        return successful, failed
    return 0, 0


async def process_recipients_in_batches(client, recipient_messages):
    """
    Обрабатывает получателей пакетами
    recipient_messages: dict {recipient: set(message_ids)}
    """
    loop = asyncio.get_running_loop()
    
    def exception_handler(loop, context):
        exception = context.get('exception')
        
        if isinstance(exception, aioimaplib.aioimaplib.CommandTimeout):
            logger.debug("Suppressed CommandTimeout exception in task")
            return
        
        if isinstance(exception, aioimaplib.aioimaplib.Abort):
            logger.debug(f"Suppressed Abort exception: {exception}")
            return
        
        if isinstance(exception, socket.gaierror):
            logger.error(f"⚠️  DNS Error (gaierror): {exception}")
            logger.error(f"   This usually means too many simultaneous connections")
            logger.error(f"   The script will continue with other recipients")
            return
        
        if sys.platform == 'win32' and 'Task was destroyed but it is pending' in str(context.get('message', '')):
            logger.debug("Suppressed 'Task destroyed' warning on Windows")
            return
        
        message = context.get('message', 'No message')
        logger.error(f"⚠️  Exception in async task: {message}")
        if exception:
            logger.error(f"   Exception type: {type(exception).__name__}")
            logger.error(f"   Exception details: {exception}")
    
    loop.set_exception_handler(exception_handler)
    
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_IMAP)
    
    recipients = list(recipient_messages.keys())
    total_recipients = len(recipients)
    total_batches = (total_recipients + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"🚀 Обработка {total_recipients} получателей в {total_batches} пакетах")
    logger.info(f"   Размер пакета: {BATCH_SIZE}")
    logger.info(f"   Пауза между пакетами: {BATCH_PAUSE}с")
    logger.info(f"   Максимум одновременных IMAP: {MAX_CONCURRENT_IMAP}")
    
    total_successful = 0
    total_failed = 0
    
    for batch_num in range(total_batches):
        batch_start = batch_num * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, total_recipients)
        batch_recipients = recipients[batch_start:batch_end]
        
        # Создаем подсловарь для этого пакета
        batch_recipient_messages = {r: recipient_messages[r] for r in batch_recipients}
        
        logger.info(f"")
        logger.info(f"{'='*80}")
        logger.info(f"📦 ПАКЕТ {batch_num + 1}/{total_batches}: Обработка получателей {batch_start + 1}-{batch_end}")
        logger.info(f"{'='*80}")
        
        batch_start_time = time.time()
        
        successful, failed = await process_recipients_batch(
            client, batch_recipient_messages, loop, semaphore, batch_start
        )
        
        total_successful += successful
        total_failed += failed
        
        batch_duration = time.time() - batch_start_time
        
        logger.info(f"")
        logger.info(f"📊 Пакет {batch_num + 1} завершен:")
        logger.info(f"   ✅ Успешно: {successful}")
        logger.info(f"   ❌ Ошибок: {failed}")
        logger.info(f"   ⏱️  Длительность: {batch_duration:.1f}с")
        logger.info(f"   📈 Общий прогресс: {batch_end}/{total_recipients} ({100*batch_end//total_recipients}%)")
        
        force_cleanup()
        
        if batch_end < total_recipients:
            logger.info(f"💤 Пауза {BATCH_PAUSE}с перед следующим пакетом...")
            await asyncio.sleep(BATCH_PAUSE)
    
    logger.info(f"")
    logger.info(f"{'='*80}")
    logger.info(f"📊 ИТОГОВАЯ СТАТИСТИКА")
    logger.info(f"{'='*80}")
    logger.info(f"   Всего обработано: {total_recipients}")
    logger.info(f"   ✅ Успешно: {total_successful}")
    logger.info(f"   ❌ Ошибок: {total_failed}")
    
    # Вывод статистики удаления
    stats = get_deletion_stats()
    logger.info(f"")
    logger.info(f"{'='*80}")
    logger.info(f"📧 СТАТИСТИКА УДАЛЕНИЯ")
    logger.info(f"{'='*80}")
    logger.info(f"   🗑️  Всего удалено сообщений: {stats['total_deleted']}")
    logger.info(f"   ✅ Получателей с удалениями: {stats['recipients_with_deletions']}")
    logger.info(f"   ℹ️  Получателей без удалений: {stats['recipients_without_deletions']}")
    logger.info(f"   ❌ Получателей с ошибками: {stats['recipients_with_errors']}")
    
    if stats['recipients_with_errors'] > 0:
        logger.info(f"")
        logger.info(f"⚠️  ВНИМАНИЕ: {stats['recipients_with_errors']} получателя(ей) не удалось обработать!")
        logger.info(f"   Список необработанных получателей сохранен в: {FAILED_RECIPIENTS_FILE}")
        logger.info(f"   Возможные причины:")
        logger.info(f"   - У пользователя отключен доступ по IMAP")
        logger.info(f"   - Сервисное приложение не имеет прав для получения токена пользователя")
        logger.info(f"   - У пользователя заблокирован аккаунт")
        logger.info(f"   - Проблемы с сетевым подключением")
    
    if stats['recipients_without_deletions'] > 0:
        logger.info(f"")
        logger.info(f"💡 Примечание: У {stats['recipients_without_deletions']} получателя(ей) не найдено сообщений, соответствующих критериям.")
        logger.info(f"   Это может означать:")
        logger.info(f"   - Сообщения уже были удалены")
        logger.info(f"   - Сообщения не были доставлены этим получателям")
        logger.info(f"   - Сообщения не попадают в указанный временной диапазон")


def write_deleted(user: str, folder: str, message_id: str = ""):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(DELETED_MESSAGES_FILE, "a", encoding='utf-8') as file:
        if message_id:
            file.write(f"{timestamp} | {user} | Folder: {folder} | MsgID: {message_id}\n")
        else:
            file.write(f"{timestamp} | {user} | Folder: {folder}\n")
    logger.info(f"✅ Logged deletion: {user} - {folder}")


def write_failed_recipient(user: str, reason: str, message_ids_count: int):
    """Логирование получателей, которых не удалось обработать"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(FAILED_RECIPIENTS_FILE, "a", encoding='utf-8') as file:
        file.write(f"{timestamp} | {user} | Reason: {reason} | Messages to delete: {message_ids_count}\n")
    logger.debug(f"📝 Logged failed recipient: {user} - {reason}")


def get_settings():
    # Вставьте свои данные
    HARDCODED_TOKEN = ""
    HARDCODED_ORG_ID = ""
    HARDCODED_CLIENT_ID = ""
    HARDCODED_CLIENT_SECRET = ""
    
    settings = SettingParams(
        oauth_token=environ.get(OAUTH_TOKEN_ARG, HARDCODED_TOKEN),
        organization_id=int(environ.get(ORGANIZATION_ID_ARG, HARDCODED_ORG_ID)),
        app_client_id=environ.get(APPLICATION_CLIENT_ID_ARG, HARDCODED_CLIENT_ID),
        app_client_secret=environ.get(APPLICATION_CLIENT_SECRET_ARG, HARDCODED_CLIENT_SECRET),
    )
    return settings


def fetch_audit_logs(
    client: "Client360", 
    sender_email: str,
    datetime_from: datetime,
    datetime_to: datetime,
    datetime_from_moscow: datetime,
    datetime_to_moscow: datetime,
    after_date: str, 
    before_date: str
) -> "FetchedMessages":
    """
    Получает из аудит-логов все письма от указанного отправителя за указанный период
    Теперь сохраняет соответствие получатель -> список message_ids
    """
    
    logger.info("Поиск в аудит-логах...")
    logger.info(f"Фильтр отправителя: {sender_email}")
    logger.info(f"Дата: {datetime_from_moscow.strftime('%d-%m-%Y')}")
    logger.info(f"Диапазон времени (Москва UTC+3): {datetime_from_moscow.strftime('%H:%M:%S')} до {datetime_to_moscow.strftime('%H:%M:%S')}")
    logger.info(f"Диапазон DateTime (UTC): {datetime_from.strftime('%d-%m-%Y %H:%M:%S')} до {datetime_to.strftime('%d-%m-%Y %H:%M:%S')}")
    logger.info(f"Диапазон дат API: {after_date} до {before_date}")
    
    fetched_messages = FetchedMessages(
        recipient_messages=defaultdict(set),  # recipient -> set of message_ids
        all_message_ids=set(),
        subjects=set()
    )
    
    page_number = 1
    total_events_processed = 0
    matching_events = 0
    filtered_by_sender = 0
    filtered_by_time = 0
    sum_matching_prev = 0
    
    # Для диагностики - собираем уникальных отправителей
    found_senders = set()
    
    # Запрашиваем первую страницу с максимальным размером
    audit_log = client.audit_log.get(
        after_date=after_date, 
        before_date=before_date,
    )
    
    # Нормализуем email отправителя для сравнения
    sender_normalized = sender_email.lower().strip()
    
    while True:
        logger.info(f"")
        logger.info(f"📄 Страница {page_number}: получено {len(audit_log.events)} событий")
        
        for event in audit_log.events:
            total_events_processed += 1
            
            # Извлекаем email отправителя из поля from_
            event_from = event.from_.lower().strip() if event.from_ else ""
            
            # Если в поле from_ содержится имя и email в формате "Name <email>", извлекаем только email
            from_emails = extract_emails(event.from_) if event.from_ else []
            
            # Проверяем совпадение отправителя
            sender_match = False
            if sender_normalized in event_from:
                sender_match = True
            else:
                for email in from_emails:
                    if email.lower().strip() == sender_normalized:
                        sender_match = True
                        break
            
            if not sender_match:
                filtered_by_sender += 1
                continue
            
            # Проверяем попадание в диапазон дат и времени
            event_datetime = event.date
            
            # Убираем timezone info из event_datetime для корректного сравнения
            if event_datetime.tzinfo is not None:
                event_datetime = event_datetime.replace(tzinfo=None)
            
            # Сравниваем UTC с UTC
            if event_datetime < datetime_from or event_datetime > datetime_to:
                filtered_by_time += 1
                continue
            
            # Событие подходит под критерии
            matching_events += 1
            
            # ===== КЛЮЧЕВОЕ ИЗМЕНЕНИЕ: сохраняем соответствие получатель -> message_id =====
            recipient = event.userLogin
            msg_id = event.msgId
            
            # Добавляем message_id для этого получателя
            fetched_messages.recipient_messages[recipient].add(msg_id)
            
            # Добавляем в общий набор message_id
            fetched_messages.all_message_ids.add(msg_id)
            
            # Добавляем тему
            if event.subject:
                fetched_messages.subjects.add(event.subject)
        
        # Промежуточная статистика после обработки страницы
        page_matching = matching_events - sum_matching_prev
        logger.info(f"   Всего событий на странице: {len(audit_log.events)}")
        logger.info(f"   Подходящих событий на странице: {page_matching}")
        logger.info(f"   Всего подходящих событий: {matching_events}")
        sum_matching_prev = matching_events
        
        # Проверяем есть ли следующая страница
        logger.debug(f"   nextPageToken type: {type(audit_log.nextPageToken)}, value: '{audit_log.nextPageToken}'")
        has_next = audit_log.nextPageToken and audit_log.nextPageToken != ""
        
        # Если получили полную страницу (100 событий) но нет nextPageToken - это подозрительно
        if len(audit_log.events) == 100 and not has_next:
            logger.warning(f"⚠️  ВНИМАНИЕ: Получено максимальное количество событий (100), но nextPageToken пустой!")
            logger.warning(f"   Возможно API не возвращает все события. Попробуйте уменьшить временной диапазон.")
        
        if not has_next:
            logger.info(f"")
            logger.info(f"✓ Достигнута последняя страница аудит-лога (nextPageToken: '{audit_log.nextPageToken}')")
            break
        
        logger.info(f"")
        logger.info(f"→ Переход к следующей странице (nextPageToken: {audit_log.nextPageToken[:50]}...)")
        page_number += 1
        
        audit_log = client.audit_log.get(
            after_date=after_date,
            before_date=before_date,
            page_token=audit_log.nextPageToken,
        )
    
    logger.info(f"Всего обработано страниц: {page_number}")
    logger.info(f"Всего просканировано событий: {total_events_processed}")
    logger.info(f"Найдено подходящих событий: {matching_events}")
    logger.info(f"Отфильтровано по отправителю: {filtered_by_sender}")
    logger.info(f"Отфильтровано по времени: {filtered_by_time}")
    logger.info(f"Уникальных message ID: {len(fetched_messages.all_message_ids)}")
    logger.info(f"Уникальных получателей: {len(fetched_messages.recipient_messages)}")
    logger.info(f"Уникальных тем: {len(fetched_messages.subjects)}")
    
    # Статистика по получателям
    if fetched_messages.recipient_messages:
        logger.info(f"")
        logger.info(f"Распределение событий по получателям (топ-10):")
        sorted_recipients = sorted(
            fetched_messages.recipient_messages.items(), 
            key=lambda x: len(x[1]), 
            reverse=True
        )[:10]
        for recipient, msg_ids in sorted_recipients:
            logger.info(f"  {recipient}: {len(msg_ids)} событие(й)")
    
    if matching_events == 0:
        logger.warning("⚠️  Не найдено ни одного события, подходящего под критерии!")
        if filtered_by_sender > 0:
            logger.warning(f"   {filtered_by_sender} событий отфильтровано по отправителю")
            logger.warning(f"   Проверьте правильность email отправителя: {sender_email}")
        if filtered_by_time > 0:
            logger.warning(f"   {filtered_by_time} событий отфильтровано по времени")
            logger.warning(f"   Проверьте правильность временного диапазона:")
            logger.warning(f"   Москва: {datetime_from_moscow.strftime('%H:%M:%S')} - {datetime_to_moscow.strftime('%H:%M:%S')}")
            logger.warning(f"   UTC: {datetime_from.strftime('%H:%M:%S')} - {datetime_to.strftime('%H:%M:%S')}")
    
    return fetched_messages


def is_deletion_approve(sender: str, event_count: int, unique_msgids: int, subjects: set, recipient_messages: dict) -> bool:
    """
    Запрашивает подтверждение удаления
    """
    logger.info("")
    logger.info(f"=== ПОДТВЕРЖДЕНИЕ УДАЛЕНИЯ ===")
    logger.info(f"Отправитель: {sender}")
    logger.info(f"Всего событий к обработке: {event_count}")
    logger.info(f"Уникальных message ID: {unique_msgids}")
    logger.info(f"Уникальных тем: {len(subjects)}")
    
    if subjects:
        logger.info(f"Примеры тем (первые 10):")
        for i, subject in enumerate(list(subjects)[:10]):
            logger.info(f"  {i+1}. {subject}")
    
    logger.info(f"")
    logger.info(f"Затронутых получателей: {len(recipient_messages)}")
    logger.info(f"Распределение событий по получателям (первые 20):")
    
    sorted_recipients = sorted(
        recipient_messages.items(), 
        key=lambda x: len(x[1]), 
        reverse=True
    )[:20]
    
    for i, (recipient, msg_ids) in enumerate(sorted_recipients):
        logger.info(f"  {i+1}. {recipient}: {len(msg_ids)} событие(й)")
    
    if len(recipient_messages) > 20:
        logger.info(f"  ... и еще {len(recipient_messages) - 20}")
    
    logger.info("")
    a = input("⚠️  Введите 'yes' для подтверждения удаления: ")
    logger.info(f"Ввод пользователя: {a}")
    
    if a.strip().lower() == "yes":
        return True
    return False


class Client360:
    def __init__(self, token: str, org_id: int, client_id: str, secret: str):
        self._token = token
        self._org_id = org_id
        self._id = client_id
        self._secret = secret

    @property
    def audit_log(self):
        return AuditLogAPI(token=self._token, org_id=self._org_id)

    @property
    def user_token(self):
        return UserTokenAPI(client_id=self._id, secret=self._secret)
    
    @property
    def shared_mailboxes(self):
        return SharedMailboxAPI(token=self._token, org_id=self._org_id)


class AuditLogAPI:
    def __init__(self, token: str, org_id: int):
        self._token = token
        self._org_id = org_id

    def get(self, after_date: str, before_date: str,
            page_token: str = "0_0", verify: bool = False):
        url = f"{DEFAULT_360_API_URL}/security/v1/org/{self._org_id}/audit_log/mail"
        headers = {"Authorization": f"OAuth {self._token}"}
        params = {
            "pageSize": AUDIT_LOG_PAGE_SIZE,
            "types": "message_receive",
            "afterDate": after_date,
            "beforeDate": before_date,
            "pageToken": page_token,
        }
        
        session = get_http_session()
        response = session.get(url, headers=headers, params=params, verify=verify)
        
        if response.status_code != HTTPStatus.OK.value:
            logger.error(f"Audit log error: {response.text}")
            raise Client360Error(response.status_code)
        
        audit_log = AuditLog.model_validate(response.json())
        return audit_log


class UserTokenAPI:
    def __init__(self, client_id: str, secret: str):
        self._id = client_id
        self._secret = secret

    def get(self, user_mail: str):
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "client_id": self._id,
            "client_secret": self._secret,
            "subject_token": user_mail,
            "subject_token_type": "urn:yandex:params:oauth:token-type:email",
        }
        
        session = get_http_session()
        response = session.post(url=DEFAULT_OAUTH_API_URL, headers=headers, data=data)
        
        if response.status_code != HTTPStatus.OK.value:
            logger.error(
                f"OAuth error for {user_mail}: {response.status_code} - {response.text}"
            )
            raise ClientOAuthError(response.status_code)
        return UserToken.model_validate(response.json())


class SharedMailboxAPI:
    def __init__(self, token: str, org_id: int):
        self._token = token
        self._org_id = org_id

    def list_all(self) -> list[str]:
        url = f"{DEFAULT_360_API_URL}/admin/v1/org/{self._org_id}/mailboxes/shared"
        headers = {"Authorization": f"OAuth {self._token}"}
        params = {"perPage": 100, "page": 1}
        
        all_shared_boxes = []
        
        try:
            session = get_http_session()
            response = session.get(url, headers=headers, params=params, verify=False)
            
            if response.status_code == 403:
                logger.debug("Access denied to shared mailboxes API")
                return []
            
            if response.status_code != HTTPStatus.OK.value:
                logger.debug(f"Error fetching shared mailboxes: {response.status_code}")
                return []
            
            data = response.json()
            mailboxes = data.get("mailboxes", [])
            
            if not mailboxes:
                return []
            
            while mailboxes:
                for mailbox in mailboxes:
                    if mailbox.get("email"):
                        all_shared_boxes.append(mailbox["email"])
                
                pages = data.get("pages", 1)
                current_page = data.get("page", 1)
                
                if current_page >= pages:
                    break
                
                params["page"] += 1
                response = session.get(url, headers=headers, params=params, verify=False)
                
                if response.status_code != HTTPStatus.OK.value:
                    break
                    
                data = response.json()
                mailboxes = data.get("mailboxes", [])
        
        except Exception as e:
            logger.debug(f"Exception fetching shared mailboxes: {e}")
        
        return all_shared_boxes


class DeletionStatus(enum.Enum):
    NotFound = "Not Found"
    Empty = "Empty"
    Deleted = "Deleted"


@dataclass
class SettingParams:
    oauth_token: str
    organization_id: int
    app_client_id: str
    app_client_secret: str


@dataclass
class FetchedMessages:
    """Хранит информацию о найденных письмах с привязкой к получателям"""
    recipient_messages: Dict[str, set]  # recipient -> set of message_ids для этого получателя
    all_message_ids: set  # все уникальные message_ids (для статистики)
    subjects: set  # все уникальные темы


class AuditLog(BaseModel):
    events: list[Union["AuditLogEvents"]]
    nextPageToken: str


def convert_datetime(date: str) -> datetime:
    return datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%fZ")


class AuditLogEvents(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    
    eventType: str
    orgId: int
    userUid: str
    userLogin: str
    userName: str
    requestId: str
    uniqId: str
    source: str
    clientIp: str
    date: datetime = Field(default_factory=lambda: datetime.now())
    mid: str
    folderName: str
    folderType: str
    labels: list
    msgId: str
    subject: str
    from_: str = Field(alias="from")
    to: str
    cc: str
    bcc: str
    
    def __init__(self, **data):
        super().__init__(**data)
        if isinstance(self.date, str):
            self.date = convert_datetime(self.date)

        if hasattr(self.date, 'tzinfo') and self.date.tzinfo is not None:
            self.date = self.date.replace(tzinfo=None)


class UserToken(BaseModel):
    access_token: str
    expires_in: int
    issued_token_type: str
    scope: Optional[str] = None
    token_type: str


class ToolError(Exception):
    def __init__(self, *args):
        if args:
            self.msg = args[0]
        else:
            self.msg = None

    def __str__(self):
        return self.msg


class Client360Error(ToolError):
    def __str__(self):
        match self.msg:
            case 403:
                return "No access rights to the resource."
            case 401:
                return "Invalid user token."
            case _:
                return f"Unexpected status code: {self.msg}"


class ClientOAuthError(ToolError):
    def __str__(self):
        match self.msg:
            case 400:
                return "Invalid application client id or secret"
            case _:
                return f"Unexpected status code: {self.msg}"


if __name__ == "__main__":
    try:
        main()
    except ToolError as err:
        logging.error(err)
        sys.exit(EXIT_CODE)
    except Exception as exp:
        logging.exception(exp)
        sys.exit(EXIT_CODE)
