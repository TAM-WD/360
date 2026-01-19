'''
Скрипт позволяет найти и удалить письма с определенным Message-ID из почтовых ящиков сотрудников организации Яндекс 360.

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

Параметры запуска:
    --rfc-message-id - Message-ID письма (обязательный)
    --date - Дата для поиска в формате DD-MM-YYYY (обязательный)

Пример:
python deleting_messages_with_shared_mailboxes.py --rfc-message-id "<message-id@example.com>" --date 11-12-2025

ВНИМАНИЕ: Скрипт безвозвратно удаляет письма! Перед запуском убедитесь, что указан правильный Message-ID.
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

LOG_FILE = os.path.join(OUTPUT_DIR, f"{SCRIPT_NAME}.log")
NOT_CONNECTED_FILE = os.path.join(OUTPUT_DIR, "Not_connected_users.txt")
DELETED_MESSAGES_FILE = os.path.join(OUTPUT_DIR, "Deleted_messages.txt")

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

MAX_CONCURRENT_IMAP = 5
BATCH_SIZE = 50
BATCH_PAUSE = 3
IMAP_CONNECT_TIMEOUT = 45
IMAP_SELECT_TIMEOUT = 120
IMAP_SEARCH_TIMEOUT = 90
IMAP_LIST_TIMEOUT = 30
CONNECTION_DELAY = 0.3
RECIPIENT_DELAY = 0.2
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
            Script for deleting emails by Message-ID in Yandex 360 organizations.

            Environment options:
            {OAUTH_TOKEN_ARG} - OAuth Token,
            {ORGANIZATION_ID_ARG} - Organization ID,
            {APPLICATION_CLIENT_ID_ARG} - Application ClientID,
            {APPLICATION_CLIENT_SECRET_ARG} - Application secret
            """
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--rfc-message-id", 
        help="Message-ID to search and delete", 
        type=str, 
        required=True
    )
    parser.add_argument(
        "--date",
        help="Date to search messages (format: DD-MM-YYYY)",
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
        logging.error(f"ERROR: The value of {ORGANIZATION_ID_ARG} must be an integer.")
        sys.exit(EXIT_CODE)
    except KeyError as key:
        logger.error(f"ERROR: Required environment vars not provided: {key}")
        parsr.print_usage()
        sys.exit(EXIT_CODE)
    
    rfc_id = args.rfc_message_id
    search_date_str = args.date
    
    try:
        search_date = datetime.strptime(search_date_str, "%d-%m-%Y")
    except ValueError:
        logger.error(f"ERROR: Invalid date format. Use DD-MM-YYYY (e.g., 11-12-2025)")
        sys.exit(EXIT_CODE)
    
    after_date = search_date.replace(hour=0, minute=0, second=0)
    before_date = (search_date + timedelta(days=1)).replace(hour=0, minute=0, second=0)
    SEARCH_DATE_RANGE = (after_date, before_date)
    
    if os.path.exists(NOT_CONNECTED_FILE):
        os.remove(NOT_CONNECTED_FILE)
    
    if os.path.exists(DELETED_MESSAGES_FILE):
        os.remove(DELETED_MESSAGES_FILE)
    
    logger.info(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info(f"Not connected file: {NOT_CONNECTED_FILE}")
    logger.info(f"Deleted messages file: {DELETED_MESSAGES_FILE}")
    logger.info(f"Search date: {search_date_str}")
    logger.info(f"Date range for IMAP: {after_date.strftime('%d-%b-%Y')} to {before_date.strftime('%d-%b-%Y')} (exclusive)")
    logger.info(f"Max concurrent IMAP connections: {MAX_CONCURRENT_IMAP}")
    logger.info(f"Batch size: {BATCH_SIZE} recipients")
    logger.info(f"Batch pause: {BATCH_PAUSE} seconds")
    logger.info("deleting_messages_tool started.")
    
    client = Client360(
        token=settings.oauth_token,
        org_id=settings.organization_id,
        client_id=settings.app_client_id,
        secret=settings.app_client_secret,
    )
    
    after_date_str = after_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    before_date_str = before_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    fetched_message = fetch_audit_logs(
        client=client,
        rfc_message_id=rfc_id,
        after_date=after_date_str,
        before_date=before_date_str,
    )
    
    logger.info(f"Found {len(fetched_message.recipients)} recipients from audit log")
    logger.info(f"Subject: '{fetched_message.subject}'")
    
    try:
        shared_boxes = client.shared_mailboxes.list_all()
        if shared_boxes:
            logger.info(f"Found {len(shared_boxes)} shared mailboxes")
            for box in shared_boxes:
                fetched_message.recipients.add(box)
    except Exception as e:
        logger.debug(f"Could not fetch shared mailboxes: {e}")
    
    logger.info(f"Total recipients to process: {len(fetched_message.recipients)}")
    
    if fetched_message.subject is None:
        logger.info("No message found for deletion.")
        logger.info("deleting_messages_tool finished.")
        logger.info(f"Results saved to: {os.path.abspath(OUTPUT_DIR)}")
        return
    
    if len(fetched_message.recipients) == 0:
        logger.info("No recipients found for deletion.")
        logger.info("deleting_messages_tool finished.")
        logger.info(f"Results saved to: {os.path.abspath(OUTPUT_DIR)}")
        return
        
    if is_deletion_approve(
        subject=fetched_message.subject, users=fetched_message.recipients
    ):
        recipient_batch = list(fetched_message.recipients)
        try:
            asyncio.run(
                process_recipients_in_batches(
                    client=client, recipients=recipient_batch, rfc_id=rfc_id
                )
            )
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
        except Exception as err:
            logger.error(f"Error during processing: {err}")
            logger.error(traceback.format_exc())
    
    logger.info("deleting_messages_tool finished.")
    logger.info(f"Results saved to: {os.path.abspath(OUTPUT_DIR)}")
    
    if sys.platform == 'win32':
        logger.info("Cleaning up resources...")
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


async def delete(connector, rfc_id, loop, username, access_token, folder_encoded, folder_decoded, client):
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
            return DeletionStatus.Empty
        
        if status != "OK" or not messages or not messages[0]:
            return DeletionStatus.Empty
        
        if isinstance(messages[0], bytes) and b'[UNAVAILABLE]' in messages[0]:
            return DeletionStatus.Empty
        
        message_uids = messages[0].split()
        
        if not message_uids:
            return DeletionStatus.Empty
        
        total = len(message_uids)
        logger.info(f"Found {total} messages to check in '{folder_decoded}'")
        
        MAX_TO_CHECK = 500000
        if total > MAX_TO_CHECK:
            logger.warning(f"Limiting to {MAX_TO_CHECK} messages")
            message_uids = message_uids[:MAX_TO_CHECK]
        
        rfc_clean = rfc_id.strip('<>')
        search_variants = [
            rfc_clean.encode(),
            f"<{rfc_clean}>".encode(),
            f"Message-ID: {rfc_clean}".encode(),
            f"Message-ID: <{rfc_clean}>".encode(),
            f"Message-Id: {rfc_clean}".encode(),
            f"Message-Id: <{rfc_clean}>".encode(),
        ]
        
        CHUNK_SIZE = 100
        PAUSE_BETWEEN_MESSAGES = 0.2
        PAUSE_BETWEEN_CHUNKS = 3.0
        KEEPALIVE_INTERVAL = 20
        
        checked = 0
        failed_uids = []
        last_keepalive = 0
        
        for chunk_start in range(0, len(message_uids), CHUNK_SIZE):
            chunk_end = min(chunk_start + CHUNK_SIZE, len(message_uids))
            chunk = message_uids[chunk_start:chunk_end]
            
            chunk_num = chunk_start // CHUNK_SIZE + 1
            total_chunks = (len(message_uids) + CHUNK_SIZE - 1) // CHUNK_SIZE
            logger.info(f"📦 Chunk {chunk_num}/{total_chunks}: UIDs {chunk_start+1}-{chunk_end} in '{folder_decoded}'")
            
            logger.info(f"🔄 Reconnecting for chunk {chunk_num}...")
            
            try:
                await safe_close(connector)
            except:
                pass
            
            await asyncio.sleep(3.0)
            
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
                        await asyncio.sleep(5.0 * reconnect_attempts)
                    else:
                        logger.error(f"Failed to reconnect after {max_reconnect_attempts} attempts")
                        return DeletionStatus.NotFound
            
            for uid in chunk:
                try:
                    msg_uid = int(uid)
                except ValueError:
                    continue
                
                checked += 1
                
                if checked % 100 == 0:
                    logger.info(f"⏳ Progress: {checked}/{len(message_uids)} messages...")
                
                if checked - last_keepalive >= KEEPALIVE_INTERVAL:
                    await keepalive(connector)
                    last_keepalive = checked
                
                try:
                    status, data = await asyncio.wait_for(
                        connector.uid('fetch', msg_uid, "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"),
                        timeout=20
                    )
                    
                    if status == "OK" and len(data) > 1:
                        headers = data[1]
                        
                        found = False
                        for variant in search_variants:
                            if variant in headers:
                                found = True
                                break
                        
                        if found:
                            logger.info(f"✅ Found message with UID {msg_uid} in '{folder_decoded}'")
                            try:
                                await asyncio.wait_for(
                                    connector.uid('store', msg_uid, "+FLAGS", "\\Deleted"),
                                    timeout=15
                                )
                                await asyncio.wait_for(connector.expunge(), timeout=30)
                                
                                await loop.run_in_executor(
                                    None,
                                    write_deleted,
                                    username,
                                    folder_decoded
                                )
                                
                                return DeletionStatus.Deleted
                            except Exception as e:
                                logger.error(f"Error deleting UID {msg_uid}: {e}")
                                return DeletionStatus.NotFound
                    
                    await asyncio.sleep(PAUSE_BETWEEN_MESSAGES)
                    
                except (TimeoutError, asyncio.TimeoutError, aioimaplib.aioimaplib.CommandTimeout):
                    logger.warning(f"⏱ Timeout on UID {msg_uid}")
                    failed_uids.append(msg_uid)
                    break
                
                except aioimaplib.aioimaplib.Abort as e:
                    logger.warning(f"⚠️  IMAP Abort on UID {msg_uid}: {str(e)[:100]}")
                    failed_uids.append(msg_uid)
                    break
                    
                except Exception as e:
                    logger.warning(f"Error on UID {msg_uid}: {type(e).__name__}")
                    failed_uids.append(msg_uid)
                    break
            
            if chunk_end < len(message_uids):
                logger.info(f"💤 Resting {PAUSE_BETWEEN_CHUNKS}s...")
                await asyncio.sleep(PAUSE_BETWEEN_CHUNKS)
        
        if failed_uids:
            logger.warning(f"⚠️  Failed UIDs: {len(failed_uids)}")
        
        return DeletionStatus.NotFound
        
    except Exception as e:
        logger.error(f"Error in delete for '{folder_decoded}': {type(e).__name__} - {e}")
        logger.debug(traceback.format_exc())
        return DeletionStatus.Empty


async def process_folder_with_reconnect(username, access_token, folder_encoded, folder_decoded, rfc_id, loop, client):
    connector = None
    max_retries = 3
    
    for attempt in range(1, max_retries + 1):
        try:
            connector = await connect_to_mail(username=username, access_token=access_token)
            
            status, body = await asyncio.wait_for(
                connector.select(folder_encoded),
                timeout=IMAP_SELECT_TIMEOUT
            )
            
            if status != "OK":
                logger.debug(f"Cannot select folder '{folder_decoded}': {status}")
                return False
            
            logger.info(f"📂 Processing '{folder_decoded}'")
            
            result = await delete(
                connector=connector,
                rfc_id=rfc_id,
                loop=loop,
                username=username,
                access_token=access_token,
                folder_encoded=folder_encoded,
                folder_decoded=folder_decoded,
                client=client
            )
            
            if result == DeletionStatus.Deleted:
                logger.info(f"✅ Message deleted from '{folder_decoded}'")
                return True
            elif result == DeletionStatus.Empty:
                logger.debug(f"Empty folder '{folder_decoded}'")
                return False
            else:
                logger.debug(f"Not found in '{folder_decoded}'")
                return False
        
        except (TimeoutError, asyncio.TimeoutError) as e:
            logger.warning(f"⚠️  Cannot process '{folder_decoded}': Timeout (attempt {attempt}/{max_retries})")
            
            if attempt < max_retries:
                await asyncio.sleep(5 * attempt)
            else:
                logger.error(f"❌ Failed to process '{folder_decoded}' after {max_retries} timeout attempts")
                return False
                
        except ConnectionError as e:
            logger.warning(f"⚠️  Cannot process '{folder_decoded}': Connection error (attempt {attempt}/{max_retries})")
            
            if attempt < max_retries:
                await asyncio.sleep(5 * attempt)
            else:
                logger.error(f"❌ Failed to process '{folder_decoded}' after {max_retries} connection attempts")
                return False
                
        except Exception as e:
            error_type = type(e).__name__
            error_msg = str(e)[:100]
            logger.warning(f"⚠️  Cannot process '{folder_decoded}': {error_type} (attempt {attempt}/{max_retries})")
            logger.debug(f"Full error: {error_msg}")
            
            if attempt < max_retries:
                await asyncio.sleep(3 * attempt)
            else:
                logger.error(f"❌ Failed to process '{folder_decoded}' after {max_retries} attempts: {error_type}")
                return False
        
        finally:
            if connector:
                await safe_close(connector)
            connector = None


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


async def process_recipient(recipient, rfc_id, client, loop, semaphore, recipient_index, total_recipients):
    
    await asyncio.sleep(RECIPIENT_DELAY * (recipient_index % MAX_CONCURRENT_IMAP))
    
    async with semaphore:
        logger.info(f"🚀 [{recipient_index + 1}/{total_recipients}] Starting processing for {recipient}")
        
        if recipient_index % GC_INTERVAL == 0:
            force_cleanup()
        
        try:
            try:
                access_token = await get_user_token_async(client, recipient, loop)
            except Exception as e:
                error_type = type(e).__name__
                error_msg = str(e)[:200]
                logger.error(f"❌ Cannot get token for {recipient}: {error_type}")
                await loop.run_in_executor(None, write_error, recipient, f"Token error: {error_msg}")
                return
            
            connector = None
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
                
                await safe_close(connector)
                connector = None
                
            except (TimeoutError, asyncio.TimeoutError) as e:
                error_msg = "Connection/LIST timeout"
                logger.error(f"❌ Cannot connect to {recipient}: {error_msg}")
                await loop.run_in_executor(None, write_error, recipient, error_msg)
                if connector:
                    await safe_close(connector)
                return
                
            except ConnectionError as e:
                error_msg = f"Connection failed: {str(e)[:100]}"
                logger.error(f"❌ Cannot connect to {recipient}: {error_msg}")
                await loop.run_in_executor(None, write_error, recipient, error_msg)
                if connector:
                    await safe_close(connector)
                return
                
            except socket.gaierror as e:
                error_msg = "DNS resolution failed"
                logger.error(f"❌ Cannot connect to {recipient}: {error_msg}")
                await loop.run_in_executor(None, write_error, recipient, error_msg)
                if connector:
                    await safe_close(connector)
                return
                
            except Exception as e:
                error_type = type(e).__name__
                error_msg = f"{error_type}: {str(e)[:100]}"
                logger.error(f"❌ Cannot connect to {recipient}: {error_msg}")
                logger.debug(traceback.format_exc())
                await loop.run_in_executor(None, write_error, recipient, error_msg)
                if connector:
                    await safe_close(connector)
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
            
            for folder_encoded, folder_decoded in priority_folders:
                logger.info(f"📂 [{recipient}] Processing priority folder: {folder_decoded}")
                
                found = await process_folder_with_reconnect(
                    username=recipient,
                    access_token=access_token,
                    folder_encoded=folder_encoded,
                    folder_decoded=folder_decoded,
                    rfc_id=rfc_id,
                    loop=loop,
                    client=client
                )
                
                if found:
                    logger.info(f"✅ [{recipient}] Message found and deleted in '{folder_decoded}'")
                    return
            
            for folder_encoded, folder_decoded in other_folders:
                logger.info(f"📂 [{recipient}] Processing: {folder_decoded}")
                
                found = await process_folder_with_reconnect(
                    username=recipient,
                    access_token=access_token,
                    folder_encoded=folder_encoded,
                    folder_decoded=folder_decoded,
                    rfc_id=rfc_id,
                    loop=loop,
                    client=client
                )
                
                if found:
                    logger.info(f"✅ [{recipient}] Message found and deleted in '{folder_decoded}'")
                    return
            
            logger.info(f"🔍 [{recipient}] Message not found in any folder")
        
        except Exception as e:
            error_type = type(e).__name__
            error_msg = f"{error_type}: {str(e)[:200]}"
            logger.error(f"❌ Unexpected error processing {recipient}: {error_msg}")
            logger.debug(traceback.format_exc())
            await loop.run_in_executor(None, write_error, recipient, error_msg)
        finally:
            force_cleanup()


async def process_recipients_batch(client, recipients, rfc_id, loop, semaphore, batch_offset):
    tasks = []
    total_recipients = len(recipients)
    
    for idx, recipient in enumerate(recipients):
        task = asyncio.create_task(
            process_recipient(
                recipient, 
                rfc_id, 
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


async def process_recipients_in_batches(client, recipients, rfc_id):
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
    
    total_recipients = len(recipients)
    total_batches = (total_recipients + BATCH_SIZE - 1) // BATCH_SIZE
    
    logger.info(f"🚀 Processing {total_recipients} recipients in {total_batches} batches")
    logger.info(f"   Batch size: {BATCH_SIZE}")
    logger.info(f"   Pause between batches: {BATCH_PAUSE}s")
    logger.info(f"   Max concurrent IMAP: {MAX_CONCURRENT_IMAP}")
    
    total_successful = 0
    total_failed = 0
    
    for batch_num in range(total_batches):
        batch_start = batch_num * BATCH_SIZE
        batch_end = min(batch_start + BATCH_SIZE, total_recipients)
        batch_recipients = recipients[batch_start:batch_end]
        
        logger.info(f"")
        logger.info(f"{'='*80}")
        logger.info(f"📦 BATCH {batch_num + 1}/{total_batches}: Processing recipients {batch_start + 1}-{batch_end}")
        logger.info(f"{'='*80}")
        
        batch_start_time = time.time()
        
        successful, failed = await process_recipients_batch(
            client, batch_recipients, rfc_id, loop, semaphore, batch_start
        )
        
        total_successful += successful
        total_failed += failed
        
        batch_duration = time.time() - batch_start_time
        
        logger.info(f"")
        logger.info(f"📊 Batch {batch_num + 1} complete:")
        logger.info(f"   ✅ Successful: {successful}")
        logger.info(f"   ❌ Failed: {failed}")
        logger.info(f"   ⏱️  Duration: {batch_duration:.1f}s")
        logger.info(f"   📈 Overall progress: {batch_end}/{total_recipients} ({100*batch_end//total_recipients}%)")
        
        force_cleanup()
        
        if batch_end < total_recipients:
            logger.info(f"💤 Pausing {BATCH_PAUSE}s before next batch...")
            await asyncio.sleep(BATCH_PAUSE)
    
    logger.info(f"")
    logger.info(f"{'='*80}")
    logger.info(f"📊 FINAL STATISTICS")
    logger.info(f"{'='*80}")
    logger.info(f"   Total processed: {total_recipients}")
    logger.info(f"   ✅ Successful: {total_successful}")
    logger.info(f"   ❌ Failed: {total_failed}")
    logger.info(f"   📈 Success rate: {100*total_successful//total_recipients if total_recipients > 0 else 0}%")


def write_error(user: str, reason: str = "Unknown error"):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(NOT_CONNECTED_FILE, "a", encoding='utf-8') as file:
        file.write(f"{timestamp} | {user} | {reason}\n")


def write_deleted(user: str, folder: str):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(DELETED_MESSAGES_FILE, "a", encoding='utf-8') as file:
        file.write(f"{timestamp} | {user} | Folder: {folder}\n")
    logger.info(f"✅ Logged deletion: {user} - {folder}")


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
    client: "Client360", rfc_message_id: str, after_date: str, before_date: str
) -> "FetchedMessage":
    
    logger.info("Searching audit logs...")
    logger.info(f"Message-ID: {rfc_message_id}")
    logger.info(f"Date range: {after_date} to {before_date}")
    
    fetched_message = FetchedMessage(subject=None, recipients=set())
    
    page_number = 1
    total_events_processed = 0
    
    audit_log = client.audit_log.get(after_date=after_date, before_date=before_date)
    
    while True:
        logger.info(f"Processing audit log page {page_number} ({len(audit_log.events)} events)")
        
        for event in audit_log.events:
            total_events_processed += 1
            
            event_msg_id = event.msgId[1:-1] if event.msgId.startswith('<') and event.msgId.endswith('>') else event.msgId
            
            if event_msg_id == rfc_message_id:
                if fetched_message.subject is None:
                    fetched_message.subject = event.subject
                
                fetched_message.recipients.add(event.userLogin)
                
                from_emails = extract_emails(event.from_) if event.from_ else []
                to_emails = extract_emails(event.to) if event.to else []
                cc_emails = extract_emails(event.cc) if event.cc else []
                bcc_emails = extract_emails(event.bcc) if event.bcc else []
                
                all_emails = to_emails + cc_emails + bcc_emails + from_emails
                
                for email in all_emails:
                    fetched_message.recipients.add(email)
        
        if audit_log.nextPageToken == "":
            break
        
        page_number += 1
        audit_log = client.audit_log.get(
            after_date=after_date,
            before_date=before_date,
            page_token=audit_log.nextPageToken,
        )
    
    logger.info(f"Total pages: {page_number}")
    logger.info(f"Total events: {total_events_processed}")
    
    return fetched_message


def is_deletion_approve(subject: str, users: set) -> bool:
    logger.info("")
    logger.info(f"Subject: {subject}")
    logger.info(f"Recipients ({len(users)}):")
    for user in sorted(users):
        logger.info(f"  - {user}")
    
    a = input("\nInput 'yes' to delete: ")
    logger.info(f"User input: {a}")
    
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
class FetchedMessage:
    subject: Optional[str]
    recipients: set


class AuditLog(BaseModel):
    events: list[Union["AuditLogEvents"]]
    nextPageToken: str


def convert_datetime(date: str) -> datetime:
    return datetime.strptime(date, "%Y-%m-%d")


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
    date: datetime = convert_datetime
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
