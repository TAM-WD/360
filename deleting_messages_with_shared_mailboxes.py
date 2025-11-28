'''
Скрипт позволяет найти и удалить письма с определенным Message-ID из почтовых ящиков сотрудников организации Яндекс 360.

Для запуска скрипта необходим Python версии 3.10 или выше, а также библиотеки aioimaplib, requests, urllib3 и pydantic.

Установить их можно с помощью pip. Команда выглядит так (может отличаться в зависимости от ОС):

pip install aioimaplib requests urllib3 pydantic

Также предварительно нужно:
1. Получить OAuth токен с правами на чтение аудит-логов (ya360_security:audit_log_mail)
2. Настроить сервисное приложение с правами для получения токенов пользователей с правами mail:imap_full

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

Параметры запуска скрипта:
    --rfc-message-id - Message-ID письма, которое необходимо удалить (обязательный параметр)
    --max-days-ago - количество дней назад для поиска письма, значение от 0 до 90 (обязательный параметр)
    --min-days-ago - начало периода поиска в днях назад, значение от 0 до 90 (необязательный, по умолчанию 0)

Пример запуска:

python deleting_messages_with_shared_mailboxes.py --rfc-message-id "<message-id@example.com>" --max-days-ago 7

Как работает скрипт:
1. Анализирует аудит-логи организации за указанный период
2. Находит всех получателей письма с указанным Message-ID
3. Добавляет в список общие почтовые ящики организации
4. Запрашивает подтверждение на удаление (необходимо ввести 'yes')
5. Подключается к почтовым ящикам и удаляет найденные письма

По завершении работы скрипта в директории deleting_messages_YYYYMMDD_HHMMSS будут созданы файлы:
    deleting_messages.log - подробные логи выполнения скрипта
    Not_connected_users.txt - список пользователей, к почтовым ящикам которых не удалось подключиться

ВНИМАНИЕ: Скрипт безвозвратно удаляет письма! Перед запуском убедитесь, что указан правильный Message-ID.
'''

import argparse
import asyncio
import concurrent.futures
import enum
import logging
import os
import os.path
import re
import ssl
import sys
import io
from dataclasses import dataclass
from datetime import datetime, timedelta
from http import HTTPStatus
from os import environ
from textwrap import dedent
from typing import Optional, Union

# Принудительно устанавливаем UTF-8 для вывода
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

import aioimaplib
import requests
import urllib3
from pydantic import BaseModel, Field
from urllib3.exceptions import InsecureRequestWarning

urllib3.disable_warnings(InsecureRequestWarning)

SCRIPT_NAME = "deleting_messages"
RUN_TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_DIR = f"{SCRIPT_NAME}_{RUN_TIMESTAMP}"
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOG_FILE = os.path.join(OUTPUT_DIR, f"{SCRIPT_NAME}.log")
NOT_CONNECTED_FILE = os.path.join(OUTPUT_DIR, "Not_connected_users.txt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("deleting messages")

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


def extract_emails(field_value: str) -> list[str]:
    """Извлечь email адреса из строки вида 'Name <email@domain.com>, other@domain.com'"""
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
            Script for searching emails with a subject for the
            last N days in yandex 360 organizations.

            Environment options:
            {OAUTH_TOKEN_ARG} - OAuth Token,
            {ORGANIZATION_ID_ARG} - Organization ID,
            {APPLICATION_CLIENT_ID_ARG} - WEB Application ClientID,
            {APPLICATION_CLIENT_SECRET_ARG} - WEB Application secret

            For example:
            {OAUTH_TOKEN_ARG}="AgAAgfAAAAD4beAkEsWrefhNeyN1TVYjGT1k",
            {ORGANIZATION_ID_ARG}=123,
            {APPLICATION_CLIENT_ID_ARG} = "123BbCc4Dd5Ee6FffFassadsaddas",
            {APPLICATION_CLIENT_SECRET_ARG} = "00b31ffFasse8a481eaaf75955c175a20f",
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
        "--rfc-message-id", help="Message subject", type=str, required=True
    )
    parser.add_argument(
        "--max-days-ago",
        help="Number of days to finish review [0, 90]",
        type=argument_range,
        required=True,
    )
    parser.add_argument(
        "--min-days-ago",
        help="Number of days to start review [0, 90]",
        type=argument_range,
        required=False,
        default=0,
    )

    return parser


def main():
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
    min_days_ago = args.min_days_ago
    max_days_ago = args.max_days_ago
    if min_days_ago > max_days_ago:
        logger.error("ERROR: min_days_ago must be less than max_days_ago.")
        sys.exit(EXIT_CODE)
    
    if os.path.exists(NOT_CONNECTED_FILE):
        os.remove(NOT_CONNECTED_FILE)
    
    logger.info(f"Output directory: {os.path.abspath(OUTPUT_DIR)}")
    logger.info(f"Log file: {LOG_FILE}")
    logger.info(f"Not connected users file: {NOT_CONNECTED_FILE}")
    logger.info("deleting_messages_tool started.")
    
    client = Client360(
        token=settings.oauth_token,
        org_id=settings.organization_id,
        client_id=settings.app_client_id,
        secret=settings.app_client_secret,
    )
    
    fetched_message = fetch_audit_logs(
        client=client,
        rfc_message_id=rfc_id,
        min_days_ago=min_days_ago,
        max_days_ago=max_days_ago,
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
                process_recipients(
                    client=client, recipients=recipient_batch, rfc_id=rfc_id
                )
            )
        except KeyboardInterrupt:
            logger.info("Process interrupted by user")
        except Exception as err:
            logger.error(f"Error during processing: {err}")
    
    logger.info("deleting_messages_tool finished.")
    logger.info(f"Results saved to: {os.path.abspath(OUTPUT_DIR)}")


async def process_recipients(client, recipients, rfc_id):
    loop = asyncio.get_running_loop()
    tasks = []
    
    for recipient in recipients:
        try:
            user_token = client.user_token.get(user_mail=recipient)
        except ClientOAuthError:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                await loop.run_in_executor(
                    pool, log_info, f"Not connected to {recipient}"
                )
                await loop.run_in_executor(pool, write_error, recipient)
            continue
        
        task = asyncio.create_task(
            process_recipient(recipient, rfc_id, user_token.access_token, loop=loop)
        )
        tasks.append(task)
    
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} failed with exception: {result}")


def log_info(info="Error"):
    logger.info(info)


def log_error(info="Error"):
    logger.error(info)


def write_error(user: str):
    with open(NOT_CONNECTED_FILE, "a", encoding='utf-8') as file:
        file.write(f"{user}\n")


async def process_recipient(recipient, rfc_id, user_token, loop):
    connector = None
    try:
        try:
            connector = await asyncio.wait_for(
                connect_to_mail(username=recipient, access_token=user_token), timeout=10
            )
        except (TimeoutError, asyncio.TimeoutError):
            with concurrent.futures.ThreadPoolExecutor() as pool:
                await loop.run_in_executor(pool, log_info, f"Connection timeout for {recipient}")
                await loop.run_in_executor(pool, write_error, recipient)
            return
        except Exception as e:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                await loop.run_in_executor(pool, log_info, f"Connection error for {recipient}: {e}")
                await loop.run_in_executor(pool, write_error, recipient)
            return
        
        count = 0
        while connector.protocol.state != "AUTH" and count < 3:
            try:
                await connector.close()
            except:
                pass
            
            try:
                connector = await asyncio.wait_for(
                    connect_to_mail(username=recipient, access_token=user_token), timeout=10
                )
            except (TimeoutError, asyncio.TimeoutError):
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    await loop.run_in_executor(pool, log_info, f"Retry timeout for {recipient}")
                    await loop.run_in_executor(pool, write_error, recipient)
                return
            except Exception as e:
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    await loop.run_in_executor(pool, log_info, f"Retry error for {recipient}: {e}")
                    await loop.run_in_executor(pool, write_error, recipient)
                return
            count += 1
        
        if count == 3:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                await loop.run_in_executor(
                    pool, log_info, f"Not connected to {recipient}"
                )
                await loop.run_in_executor(pool, write_error, recipient)
            return
        
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, log_info, f"Connect to {recipient}")
        
        status, folders = await connector.list('""', "*")
        folders = [map_folder(folder) for folder in folders if map_folder(folder)]
        
        for folder in folders:
            status = await process_folder(connector, folder, rfc_id, recipient, loop=loop)
            if status:
                break
    
    except Exception as e:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await loop.run_in_executor(pool, log_error, f"Error processing {recipient}: {e}")
    
    finally:
        if connector:
            try:
                if connector.protocol.state == "AUTH":
                    await asyncio.wait_for(connector.close(), timeout=5)
                    logger.info(f"{recipient} - Disconnect")
            except (TimeoutError, asyncio.TimeoutError):
                logger.warning(f"{recipient} - Disconnect timeout")
            except Exception as e:
                logger.warning(f"{recipient} - Error during disconnect: {e}")


async def process_folder(connector, folder, rfc_id, user, loop) -> bool:
    status, body = await connector.select(folder)
    with concurrent.futures.ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, log_info, f"Select {folder} from {user}")
    if status != "OK":
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await loop.run_in_executor(
                pool, log_error, f"Folder selection failed. - {folder}"
            )
        return False
    try:
        del_status: DeletionStatus = await asyncio.wait_for(
            delete(connector=connector, rfc_id=rfc_id, loop=loop), timeout=90
        )
    except TimeoutError:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await loop.run_in_executor(
                pool, log_error, f"Too long time waiting. - {folder} - {user}"
            )
        return False
    except aioimaplib.aioimaplib.CommandTimeout:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            await loop.run_in_executor(
                pool, log_error, f"Too long time waiting. - {folder} - {user}"
            )
        return False
    match del_status:
        case DeletionStatus.Deleted:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                await loop.run_in_executor(
                    pool,
                    log_info,
                    f"Message rfc id: {rfc_id} deleted from user {user }.",
                )
            return True
        case DeletionStatus.Empty:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                await loop.run_in_executor(
                    pool, log_info, f"Empty folder {folder} from {user}"
                )
            return False
        case DeletionStatus.NotFound:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                await loop.run_in_executor(
                    pool,
                    log_info,
                    f"Message rfc id: {rfc_id} not found in folder: {folder}",
                )
            return False
    return False


async def delete(connector, rfc_id, loop):
    status, messages = await connector.search("ALL")
    if not messages[0]:
        return DeletionStatus.Empty
    for num in messages[0].split():
        try:
            status, data = await connector.fetch(
                int(num), "(BODY.PEEK[HEADER.FIELDS (MESSAGE-ID)])"
            )
        except aioimaplib.aioimaplib.Abort:
            with concurrent.futures.ThreadPoolExecutor() as pool:
                await loop.run_in_executor(pool, log_error)
            return
        if status == "OK":
            headers = data[1]
            if rfc_id.encode() in headers:
                await connector.store(int(num), "+FLAGS", "\\Deleted")
                return DeletionStatus.Deleted
    return DeletionStatus.NotFound


def map_folder(folder: Optional[bytes]) -> Optional[str]:
    if not folder or folder == b"LIST Completed.":
        return None
    valid = folder.decode("ascii").split('"|"')[-1].strip().strip('""')
    return f'"{valid}"'


def get_settings():
    # Вставьте сюда свои данные
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
    client: "Client360", rfc_message_id: str, min_days_ago: int, max_days_ago: int
) -> "FetchedMessage":
    current_date = datetime.now().replace(hour=0, minute=0, second=0)
    after_date = (
        current_date - timedelta(days=max_days_ago)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    before_date = (
        current_date - timedelta(days=min_days_ago) + timedelta(days=1)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    logger.info("="*80)
    logger.info("AUDIT LOG ANALYSIS")
    logger.info("="*80)
    logger.info(f"Searching for Message-ID: {rfc_message_id}")
    logger.info(f"Date range: {after_date} to {before_date}")
    logger.info("="*80)
    
    fetched_message = FetchedMessage(subject=None, recipients=set())
    
    page_number = 1
    total_events_processed = 0
    matching_events = []
    
    audit_log = client.audit_log.get(after_date=after_date, before_date=before_date)
    
    while True:
        logger.info(f"Processing audit log page {page_number} ({len(audit_log.events)} events)")
        
        for idx, event in enumerate(audit_log.events):
            total_events_processed += 1
            
            if total_events_processed == 1:
                logger.info(f"Sample event structure:")
                logger.info(f"  Event Type: {event.eventType}")
                logger.info(f"  User Login: {event.userLogin}")
                logger.info(f"  Subject: {event.subject}")
                logger.info(f"  Message ID: {event.msgId}")
                logger.info(f"  From: {event.from_}")
                logger.info(f"  To: {event.to}")
                logger.info(f"  CC: {event.cc}")
                logger.info(f"  BCC: {event.bcc}")
            
            event_msg_id = event.msgId[1:-1] if event.msgId.startswith('<') and event.msgId.endswith('>') else event.msgId
            
            if event_msg_id == rfc_message_id:
                logger.info(f"[OK] Match found for user: {event.userLogin}")
                
                if fetched_message.subject is None:
                    fetched_message.subject = event.subject
                    logger.info(f"  Set subject: '{event.subject}'")
                
                fetched_message.recipients.add(event.userLogin)
                
                to_emails = extract_emails(event.to)
                cc_emails = extract_emails(event.cc)
                bcc_emails = extract_emails(event.bcc)
                
                all_recipient_emails = to_emails + cc_emails + bcc_emails
                
                if to_emails:
                    logger.info(f"  Extracted from To: {to_emails}")
                if cc_emails:
                    logger.info(f"  Extracted from CC: {cc_emails}")
                if bcc_emails:
                    logger.info(f"  Extracted from BCC: {bcc_emails}")
                
                for email in all_recipient_emails:
                    fetched_message.recipients.add(email)
                    logger.info(f"  + Added: {email}")
                
                matching_events.append({
                    'userLogin': event.userLogin,
                    'subject': event.subject,
                    'from': event.from_,
                    'to': event.to,
                    'to_emails': to_emails,
                    'cc': event.cc,
                    'cc_emails': cc_emails,
                    'bcc': event.bcc,
                    'bcc_emails': bcc_emails,
                    'date': str(event.date),
                })
        
        if audit_log.nextPageToken == "":
            break
        
        page_number += 1
        audit_log = client.audit_log.get(
            after_date=after_date,
            before_date=before_date,
            page_token=audit_log.nextPageToken,
        )
    
    logger.info("="*80)
    logger.info("AUDIT LOG SUMMARY")
    logger.info("="*80)
    logger.info(f"Total pages: {page_number}")
    logger.info(f"Total events: {total_events_processed}")
    logger.info(f"Matching events: {len(matching_events)}")
    logger.info(f"Unique recipients: {len(fetched_message.recipients)}")
    logger.info(f"Subject: {fetched_message.subject or 'NOT FOUND'}")
    
    if matching_events:
        logger.info("All matching events:")
        for i, event in enumerate(matching_events, 1):
            logger.info(f"  #{i}:")
            logger.info(f"    User: {event['userLogin']}")
            logger.info(f"    From: {event['from']}")
            logger.info(f"    To: {event['to']}")
            if event['to_emails']:
                logger.info(f"      -> Extracted emails: {', '.join(event['to_emails'])}")
            if event['cc']:
                logger.info(f"    CC: {event['cc']}")
            if event['cc_emails']:
                logger.info(f"      -> Extracted emails: {', '.join(event['cc_emails'])}")
            if event['bcc']:
                logger.info(f"    BCC: {event['bcc']}")
            if event['bcc_emails']:
                logger.info(f"      -> Extracted emails: {', '.join(event['bcc_emails'])}")
        
        logger.info("Recipients list:")
        for recipient in sorted(fetched_message.recipients):
            logger.info(f"  - {recipient}")
    else:
        logger.info("[ERROR] NO MATCHING EVENTS FOUND")
        logger.info(f"Searched for: {rfc_message_id}")
    
    logger.info("="*80)
    
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


def generate_oauth(username, access_token):
    return ("user=%s\1auth=Bearer %s\1\1" % (username, access_token)).encode()


async def connect_to_mail(username: str, access_token: str):
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    try:
        imap_connector = aioimaplib.IMAP4_SSL(
            host=DEFAULT_IMAP_SERVER, 
            port=DEFAULT_IMAP_PORT,
            ssl_context=ssl_context,
            timeout=30
        )
        await asyncio.wait_for(imap_connector.wait_hello_from_server(), timeout=10)
        status, data = await asyncio.wait_for(
            imap_connector.xoauth2(user=username, token=access_token), 
            timeout=10
        )
        if status != "OK":
            logger.error(f"Error connecting to mail for {username}: {status} {data}")
            await imap_connector.close()
            raise ConnectionError(f"Auth failed: {status}")
        return imap_connector
    except (TimeoutError, asyncio.TimeoutError) as e:
        logger.error(f"Timeout connecting to mail for {username}")
        raise
    except Exception as e:
        logger.error(f"Exception connecting to mail for {username}: {e}")
        raise


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
        
        response = requests.get(url, headers=headers, params=params, verify=verify)
        
        logger.info(
            f"Audit log response: {response.status_code}, "
            f"Request-ID: {response.headers.get('X-Request-Id')}"
        )
        
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
        response = requests.post(url=DEFAULT_OAUTH_API_URL, headers=headers, data=data)
        if response.status_code != HTTPStatus.OK.value:
            logger.error(
                f"OAuth error for {user_mail}: {response.status_code}, {response.text}"
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
            response = requests.get(url, headers=headers, params=params, verify=False)
            
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
                response = requests.get(url, headers=headers, params=params, verify=False)
                
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
    from_: str = Field("from")
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
