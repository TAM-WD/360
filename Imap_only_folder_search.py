'''
Скрипт позволяет получить список папок по IMAP у сотрудников.

Для запуска скрипта необходим Python версии 3.7 или выше, а также библиотеки requests и urrlib3.

Установить их можно с помощью pip. Команда выглядит так (может отличаться в зависимости от ОС):

pip install requests urrlib3

Также предварительно нужно получить токен с правами directory: read_users и настроить сервисное приложение с правами mail:imap_full или mail:imap_ro

Для запуска в функции main необходимо указать:
    ORG_ID = "" # ID организации
    OAUTH_TOKEN = "" # OAuth токен с правами на чтение информации о сотрудниках
    CLIENT_ID = "" # Client ID сервисного приложения с правами для действий по IMAP
    CLIENT_SECRET = "" # Client Secret сервисного приложения с правами для действий по IMAP

Также в функции main есть параметры производительности, позволяющие ускорить или обезопасить процесс.
    MAX_WORKERS = 5 - максимальное число воркеров
    IMAP_TIMEOUT = 60 - таймаут операций в секундах
    IMAP_RETRIES = 5 - число ретраев

По завершении работы скрипта будет создано 4 файла:
    imap_report_YYYYMMDD_HHMMSS.csv - основной файл со списком папок сотрудников
    imap_failed_users_YYYYMMDD_HHMMSS.csv - таблица с пользователями, к которым не удалось подключиться по IMAP
    imap_errors_YYYYMMDD_HHMMSS.txt - файл с пользователями, к которым не удалось подключиться по IMAP
    imap_report_YYYYMMDD_HHMMSS.log - логи выполнения скрипта

'''

import imaplib
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import csv
import logging
from datetime import datetime
from typing import List, Dict, Optional, Callable, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
import socket
import sys
import functools
import base64
import re

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

DEBUG_MODE = False #True для дебаг режима с подробным логированием

logging.basicConfig(
    level=logging.DEBUG if DEBUG_MODE else logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'imap_report_{timestamp}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


def decode_imap_folder_name(folder_name: str) -> str:
    try:
        if '&' not in folder_name:
            return folder_name
        
        result = []
        i = 0
        
        while i < len(folder_name):
            if folder_name[i] == '&':
                end = folder_name.find('-', i)
                if end == -1:
                    end = len(folder_name)
                
                if end == i + 1:
                    result.append('&')
                    i = end + 1
                    continue
                
                encoded = folder_name[i+1:end]
                encoded = encoded.replace(',', '/')
                padding = (4 - len(encoded) % 4) % 4
                encoded += '=' * padding
                
                try:
                    decoded_bytes = base64.b64decode(encoded)
                    decoded_str = decoded_bytes.decode('utf-16-be')
                    result.append(decoded_str)
                except Exception as e:
                    logging.debug(f"Не удалось декодировать часть '{encoded}': {e}")
                    result.append(folder_name[i:end+1])
                
                i = end + 1
            else:
                result.append(folder_name[i])
                i += 1
        
        return ''.join(result)
        
    except Exception as e:
        logging.debug(f"Ошибка декодирования названия папки '{folder_name}': {e}")
        return folder_name


def parse_imap_list_response(response_line) -> Optional[str]:
    try:
        if isinstance(response_line, tuple):
            if len(response_line) >= 2:
                folder_name = response_line[1].decode('utf-8').strip()
                if DEBUG_MODE:
                    logging.debug(f"📥 IMAP LITERAL: {repr(response_line)}")
                    logging.debug(f"✅ IMAP PARSED (literal): {repr(folder_name)}")
                return folder_name if folder_name else None
            else:
                logging.warning(f"⚠️ Некорректный tuple: {response_line}")
                return None
        
        if isinstance(response_line, bytes):
            line = response_line.decode('utf-8').strip()
        else:
            line = str(response_line).strip()
        
        if DEBUG_MODE:
            logging.debug(f"📥 IMAP RAW: {repr(line)}")
        
        paren_match = re.match(r'\([^)]*\)\s+', line)
        if not paren_match:
            logging.debug(f"⚠️ Не найдены флаги в скобках")
            return None
        
        remainder = line[paren_match.end():]
        if DEBUG_MODE:
            logging.debug(f"🔹 После удаления флагов: {repr(remainder)}")
        
        delimiter_end = 0
        
        if remainder.startswith('"'):
            i = 1
            escaped = False
            while i < len(remainder):
                if escaped:
                    escaped = False
                elif remainder[i] == '\\':
                    escaped = True
                elif remainder[i] == '"':
                    delimiter_end = i + 1
                    break
                i += 1
        elif remainder.startswith('NIL'):
            delimiter_end = 3
        else:
            space_pos = remainder.find(' ')
            delimiter_end = space_pos if space_pos != -1 else len(remainder)
        
        folder_part = remainder[delimiter_end:].lstrip()
        if DEBUG_MODE:
            logging.debug(f"🔹 Часть с именем папки: {repr(folder_part)}")
        
        folder_name = ""
        
        if folder_part.startswith('"'):
            i = 1
            escaped = False
            chars = []
            
            while i < len(folder_part):
                char = folder_part[i]
                
                if escaped:
                    chars.append(char)
                    escaped = False
                    if DEBUG_MODE:
                        logging.debug(f"  └─ Escaped char: {repr(char)}")
                elif char == '\\':
                    escaped = True
                    if DEBUG_MODE:
                        logging.debug(f"  └─ Escape начат на позиции {i}")
                elif char == '"':
                    if DEBUG_MODE:
                        logging.debug(f"  └─ Конец quoted string на позиции {i}")
                    break
                else:
                    chars.append(char)
                
                i += 1
            
            folder_name = ''.join(chars)
        else:
            folder_name = folder_part.strip()
        
        if DEBUG_MODE:
            logging.debug(f"✅ IMAP PARSED: {repr(folder_name)}")
        
        return folder_name if folder_name else None
        
    except Exception as e:
        logging.error(f"❌ Ошибка парсинга IMAP: {e}")
        logging.debug(f"   RAW data: {repr(response_line)}")
        return None

def retry_on_error(max_retries: int = 5, delay: float = 1.0, backoff: float = 2.0):
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            current_delay = delay
            last_exception = None
            
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logging.warning(
                            f"Попытка {attempt + 1}/{max_retries} для {func.__name__} не удалась: {e}. "
                            f"Повтор через {current_delay:.1f} сек..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logging.error(
                            f"Все {max_retries} попыток для {func.__name__} исчерпаны. "
                            f"Последняя ошибка: {e}"
                        )
            
            raise last_exception
        
        return wrapper
    return decorator


class IMAPErrorLogger:
    
    def __init__(self, filename: str):
        self.filename = filename
        self.lock = Lock()
        self._initialize_file()
    
    def _initialize_file(self):
        try:
            with open(self.filename, 'w', encoding='utf-8') as f:
                f.write("="*80 + "\n")
                f.write("ОТЧЕТ ОБ ОШИБКАХ IMAP ПОДКЛЮЧЕНИЙ\n")
                f.write(f"Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write("="*80 + "\n\n")
        except Exception as e:
            logging.error(f"Не удалось создать файл ошибок IMAP: {e}")
    
    def log_error(self, email: str, nickname: str, error_type: str, error_message: str, 
                  user_id: str = "", attempts: int = 0):
        with self.lock:
            try:
                with open(self.filename, 'a', encoding='utf-8') as f:
                    f.write("-"*80 + "\n")
                    f.write(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"Пользователь: {nickname}\n")
                    f.write(f"Email: {email}\n")
                    if user_id:
                        f.write(f"User ID: {user_id}\n")
                    if attempts > 0:
                        f.write(f"Попыток подключения: {attempts}\n")
                    f.write(f"Тип ошибки: {error_type}\n")
                    f.write(f"Описание: {error_message}\n")
                    f.write("-"*80 + "\n\n")
            except Exception as e:
                logging.error(f"Ошибка записи в файл ошибок IMAP: {e}")
    
    def log_summary(self, total_errors: int, error_types: Dict[str, int]):
        with self.lock:
            try:
                with open(self.filename, 'a', encoding='utf-8') as f:
                    f.write("\n" + "="*80 + "\n")
                    f.write("ИТОГОВАЯ СТАТИСТИКА ОШИБОК\n")
                    f.write("="*80 + "\n")
                    f.write(f"Всего ошибок: {total_errors}\n\n")
                    f.write("Распределение по типам:\n")
                    for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
                        f.write(f"  - {error_type}: {count}\n")
                    f.write("="*80 + "\n")
            except Exception as e:
                logging.error(f"Ошибка записи итоговой статистики: {e}")


class Yandex360IMAPAnalyzer:
    def __init__(self, org_id: str, oauth_token: str, client_id: str, client_secret: str, 
                 max_workers: int = 5, imap_timeout: int = 60, imap_retries: int = 5):
        self.org_id = org_id
        self.oauth_token = oauth_token
        self.client_id = client_id
        self.client_secret = client_secret
        self.max_workers = max_workers
        self.imap_timeout = imap_timeout
        self.imap_retries = imap_retries
        self.base_url = "https://api360.yandex.net"
        self.headers = {
            "Authorization": f"OAuth {oauth_token}",
            "Content-Type": "application/json"
        }
        self.results = []
        self.failed_users = []
        self.results_lock = Lock()
        self.failed_lock = Lock()
        
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.csv_filename = f'imap_report_{self.timestamp}.csv'
        self.failed_csv_filename = f'imap_failed_users_{self.timestamp}.csv'
        self.imap_errors_filename = f'imap_errors_{self.timestamp}.txt'
        
        self.imap_error_logger = IMAPErrorLogger(self.imap_errors_filename)
        
        self.executor = None

    def get_users(self) -> List[Dict]:
        users = []
        page = 1
        per_page = 100
        
        logging.info("Получение списка сотрудников...")
        
        while True:
            url = f"{self.base_url}/directory/v1/org/{self.org_id}/users"
            params = {"page": page, "perPage": per_page}
            
            try:
                response = requests.get(url, headers=self.headers, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                
                page_users = data.get("users", [])
                if not page_users:
                    break
                    
                users.extend(page_users)
                logging.info(f"Получено {len(users)} сотрудников...")
                
                if len(page_users) < per_page:
                    break
                    
                page += 1
                
            except requests.exceptions.RequestException as e:
                logging.error(f"Ошибка при получении списка сотрудников: {e}")
                break
        
        logging.info(f"Всего найдено сотрудников: {len(users)}")
        return users

    def get_user_token(self, uid: str, email: str) -> str:
        url = 'https://oauth.yandex.ru/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'subject_token': uid,
            'subject_token_type': 'urn:yandex:params:oauth:token-type:uid'
        }
        
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        try:
            response = session.post(url, data=data, headers=headers, timeout=30)
            logging.debug(f'get_token | email: {email}, uid: {uid}, status: {response.status_code}')
            response.raise_for_status()
            
            user_token = response.json().get('access_token', '')
            if not user_token:
                logging.error(f"Токен не найден в ответе для {email}")
            return user_token
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка получения токена для {email} (uid: {uid}): {e}")
            return ""
        finally:
            try:
                session.close()
            except:
                pass

    def generate_oauth2_string(self, username: str, access_token: str) -> str:
        return f'user={username}\1auth=Bearer {access_token}\1\1'

    def _create_imap_connection(self, username: str, token: str) -> imaplib.IMAP4_SSL:
        imap_connector = None
        try:
            auth_string = self.generate_oauth2_string(username, token)
            
            imap_connector = imaplib.IMAP4_SSL(
                host="imap.yandex.com", 
                port=993,
                timeout=self.imap_timeout
            )
            
            if hasattr(imap_connector, 'sock') and imap_connector.sock:
                imap_connector.sock.settimeout(self.imap_timeout)
            
            imap_connector.authenticate('XOAUTH2', lambda x: auth_string)
            
            logging.debug(f"Успешное подключение к IMAP для {username}")
            return imap_connector
            
        except socket.timeout:
            if imap_connector:
                try:
                    imap_connector.logout()
                except:
                    pass
            raise Exception(f"Таймаут подключения к IMAP серверу ({self.imap_timeout} сек)")
            
        except socket.error as e:
            if imap_connector:
                try:
                    imap_connector.logout()
                except:
                    pass
            errno_str = f" (errno: {e.errno})" if hasattr(e, 'errno') else ""
            raise Exception(f"Ошибка сети: {e}{errno_str}")
            
        except imaplib.IMAP4.error as e:
            if imap_connector:
                try:
                    imap_connector.logout()
                except:
                    pass
            raise Exception(f"Ошибка аутентификации IMAP: {e}")
            
        except Exception as e:
            if imap_connector:
                try:
                    imap_connector.logout()
                except:
                    pass
            raise Exception(f"{type(e).__name__}: {e}")

    @retry_on_error(max_retries=5, delay=1.0, backoff=2.0)
    def get_imap_connector(self, username: str, token: str) -> imaplib.IMAP4_SSL:
        return self._create_imap_connection(username, token)

    def _get_mailboxes_list(self, imap_connector: imaplib.IMAP4_SSL) -> List:
        status, mailboxes = imap_connector.list()
        if status != 'OK' or not mailboxes:
            raise Exception("Не удалось получить список папок")
        return mailboxes

    def get_user_mailboxes_info(self, imap_connector: imaplib.IMAP4_SSL, email: str) -> List[Dict]:
        mailboxes_info = []
    
        try:
            mailboxes = self._get_mailboxes_list_with_retry(imap_connector)
        
            for mailbox in mailboxes:
                try:
                    mailbox_name = parse_imap_list_response(mailbox)
                
                    if not mailbox_name:
                        logging.debug(f"{email} - пропуск пустого названия папки")
                        continue
                
                    decoded_folder_name = decode_imap_folder_name(mailbox_name)
                
                    if ";" in decoded_folder_name or "\\" in decoded_folder_name:
                        logging.info(f"🔍 ПРОВЕРКА: {email}")
                        logging.info(f"   RAW mailbox: {repr(mailbox)}")
                        logging.info(f"   Parsed name: {repr(mailbox_name)}")
                        logging.info(f"   Decoded: {repr(decoded_folder_name)}")
                        logging.info(f"   Длина: {len(decoded_folder_name)} символов")
                        logging.info(f"   Байты: {decoded_folder_name.encode('utf-8')}")
                
                    mailbox_info = {
                        'email': email,
                        'folder': decoded_folder_name
                    }
                    mailboxes_info.append(mailbox_info)
                
                    if DEBUG_MODE:
                        logging.debug(f"{email} - найдена папка: '{decoded_folder_name}' (оригинал: '{mailbox_name}')")
                    
                except Exception as e:
                    logging.error(f"{email} - ОШИБКА обработки папки: {e}")
                    if DEBUG_MODE:
                        logging.debug(f"{email} - RAW mailbox bytes: {repr(mailbox)}")
                    continue
        
        except Exception as e:
            logging.error(f"Ошибка при получении списка папок для {email}: {e}")
        finally:
            self._close_imap_connection(imap_connector, email)
    
        return mailboxes_info


    @retry_on_error(max_retries=5, delay=0.5, backoff=1.5)
    def _get_mailboxes_list_with_retry(self, imap_connector: imaplib.IMAP4_SSL) -> List:
        return self._get_mailboxes_list(imap_connector)

    def _close_imap_connection(self, imap_connector: Optional[imaplib.IMAP4_SSL], email: str):
        if imap_connector:
            try:
                imap_connector.logout()
                logging.debug(f"IMAP соединение закрыто для {email}")
            except Exception as e:
                logging.debug(f"Ошибка при закрытии IMAP соединения для {email}: {e}")

    def process_user(self, user: Dict, index: int, total: int) -> bool:
        email = user.get('email', '')
        user_id = user.get('id', '')
        nickname = user.get('nickname', '')
        
        if not email or not user_id:
            logging.warning(f"Пропуск пользователя без email или id: {user}")
            return False
        
        logging.info(f"[{index}/{total}] Обработка: {nickname} ({email})")
        
        error_info = {
            'nickname': nickname,
            'email': email,
            'user_id': user_id,
            'error_type': '',
            'error_message': '',
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        token = self.get_user_token(user_id, email)
        if not token:
            error_info['error_type'] = 'TOKEN_ERROR'
            error_info['error_message'] = 'Не удалось получить OAuth токен'
            
            with self.failed_lock:
                self.failed_users.append(error_info)
            
            self.save_failed_user_immediately(error_info)
            self.imap_error_logger.log_error(
                email=email,
                nickname=nickname,
                error_type='TOKEN_ERROR',
                error_message='Не удалось получить OAuth токен',
                user_id=user_id,
                attempts=0
            )
            
            logging.error(f"❌ [{index}/{total}] Не удалось получить токен для {email}")
            return False
        
        time.sleep(0.3)
        
        imap_connector = None
        try:
            imap_connector = self.get_imap_connector(email, token)
                
        except Exception as e:
            error_msg = str(e)
            error_info['error_type'] = 'IMAP_CONNECTION_ERROR'
            error_info['error_message'] = error_msg
            
            with self.failed_lock:
                self.failed_users.append(error_info)
            
            self.save_failed_user_immediately(error_info)
            self.imap_error_logger.log_error(
                email=email,
                nickname=nickname,
                error_type='IMAP_CONNECTION_ERROR',
                error_message=error_msg,
                user_id=user_id,
                attempts=self.imap_retries
            )
            
            logging.error(f"❌ [{index}/{total}] Не удалось подключиться к IMAP для {email}")
            return False
        
        mailboxes_info = self.get_user_mailboxes_info(imap_connector, email)
        
        if not mailboxes_info:
            error_info['error_type'] = 'NO_FOLDERS'
            error_info['error_message'] = 'Папки не найдены'
            
            with self.failed_lock:
                self.failed_users.append(error_info)
            
            self.save_failed_user_immediately(error_info)
            self.imap_error_logger.log_error(
                email=email,
                nickname=nickname,
                error_type='NO_FOLDERS',
                error_message='Папки не найдены',
                user_id=user_id,
                attempts=0
            )
            
            logging.warning(f"⚠️  [{index}/{total}] Не найдено папок для {email}")
            return False
        
        with self.results_lock:
            for info in mailboxes_info:
                info['nickname'] = nickname
                self.results.append(info)
                self.append_to_csv(info)
        
        logging.info(f"✅ [{index}/{total}] {email}: найдено {len(mailboxes_info)} папок")
        
        return True


    def append_to_csv(self, row: Dict) -> None:
        try:
            file_exists = False
            try:
                with open(self.csv_filename, 'r', encoding='utf-8-sig'):  # 👈 Вернули -sig
                    file_exists = True
            except FileNotFoundError:
                pass
        
            with open(self.csv_filename, 'a', newline='', encoding='utf-8-sig') as csvfile:  # 👈 Вернули -sig
                fieldnames = ['nickname', 'email', 'folder']
            
                writer = csv.DictWriter(
                    csvfile, 
                    fieldnames=fieldnames,
                    delimiter=',',
                    quotechar='"',
                    quoting=csv.QUOTE_ALL,
                    lineterminator='\n'
                )
            
                if not file_exists:
                    writer.writeheader()
            
                row_data = {k: row.get(k, '') for k in fieldnames}
            
                if DEBUG_MODE:
                    logging.debug(f"💾 CSV запись: {repr(row_data)}")
            
                writer.writerow(row_data)
                csvfile.flush()
            
        except Exception as e:
            logging.error(f"❌ Ошибка при добавлении в CSV: {e}")
            logging.debug(f"   Данные: {repr(row)}")


    def save_failed_user_immediately(self, error_info: Dict) -> None:
        try:
            file_exists = False
            try:
                with open(self.failed_csv_filename, 'r', encoding='utf-8-sig'):  # 👈 Вернули -sig
                    file_exists = True
            except FileNotFoundError:
                pass
        
            with open(self.failed_csv_filename, 'a', newline='', encoding='utf-8-sig') as csvfile:  # 👈 Вернули -sig
                fieldnames = ['nickname', 'email', 'user_id', 'error_type', 'error_message', 'timestamp']
                writer = csv.DictWriter(
                    csvfile, 
                    fieldnames=fieldnames,
                    delimiter=',',
                    quotechar='"',
                    quoting=csv.QUOTE_ALL,
                    lineterminator='\n'
                )
            
                if not file_exists:
                    writer.writeheader()
            
                writer.writerow(error_info)
                csvfile.flush()
            
        except Exception as e:
            logging.error(f"Ошибка при сохранении ошибки в CSV: {e}")


    def process_all_users(self) -> None:
        users = self.get_users()
        
        if not users:
            logging.error("Не найдено ни одного пользователя!")
            return
        
        total_users = len(users)
        start_time = time.time()
        successful = 0
        
        logging.info(f"\n{'='*60}")
        logging.info(f"Начало обработки {total_users} пользователей")
        logging.info(f"{'='*60}\n")
        
        with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="Worker") as executor:
            futures = {
                executor.submit(self.process_user, user, idx, total_users): (user, idx) 
                for idx, user in enumerate(users, 1)
            }
            
            for future in as_completed(futures):
                user, idx = futures[future]
                try:
                    result = future.result(timeout=self.imap_timeout * (self.imap_retries + 2))
                    if result:
                        successful += 1
                        
                except Exception as e:
                    email = user.get('email', 'Unknown')
                    nickname = user.get('nickname', '')
                    user_id = user.get('id', '')
                    error_msg = str(e)[:200]
                    
                    logging.error(f"💥 Ошибка обработки {email}: {error_msg}")
                    
                    error_info = {
                        'nickname': nickname,
                        'email': email,
                        'user_id': user_id,
                        'error_type': 'TIMEOUT' if 'timeout' in error_msg.lower() else 'EXCEPTION',
                        'error_message': error_msg,
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    
                    with self.failed_lock:
                        self.failed_users.append(error_info)
                    
                    self.save_failed_user_immediately(error_info)
                    self.imap_error_logger.log_error(
                        email=email,
                        nickname=nickname,
                        error_type=error_info['error_type'],
                        error_message=error_msg,
                        user_id=user_id,
                        attempts=0
                    )
        
        elapsed_time = time.time() - start_time
        
        error_types = {}
        for failed in self.failed_users:
            error_type = failed.get('error_type', 'UNKNOWN')
            error_types[error_type] = error_types.get(error_type, 0) + 1
        
        self.imap_error_logger.log_summary(len(self.failed_users), error_types)
        
        logging.info(f"\n{'='*60}")
        logging.info("✅ Обработка завершена!")
        logging.info(f"⏱️  Время: {elapsed_time:.2f} сек")
        logging.info(f"✅ Успешно: {successful}/{total_users}")
        logging.info(f"❌ Ошибок: {len(self.failed_users)}")
        logging.info(f"{'='*60}")

    def print_summary(self) -> None:
        logging.info(f"\n{'='*60}")
        logging.info("📊 ИТОГОВАЯ СТАТИСТИКА")
        logging.info(f"{'='*60}")
    
        users_stats = {}
    
        for row in self.results:
            email = row.get('email', 'Unknown')
        
            if email not in users_stats:
                users_stats[email] = {
                    'nickname': row.get('nickname', ''),
                    'folders': 0
                }
            users_stats[email]['folders'] += 1
    
        logging.info(f"\n✅ Обработано: {len(users_stats)}")
        logging.info(f"❌ Ошибок: {len(self.failed_users)}")
        logging.info(f"📊 Записей о папках: {len(self.results)}")
    
        if self.failed_users:
            error_types = {}
            for failed in self.failed_users:
                error_type = failed.get('error_type', 'UNKNOWN')
                error_types[error_type] = error_types.get(error_type, 0) + 1
        
            logging.info(f"\n📋 Распределение ошибок:")
            for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
                logging.info(f"  - {error_type}: {count}")
    
        if users_stats:
            total_folders = sum(s['folders'] for s in users_stats.values())
        
            logging.info(f"\n📈 ИТОГО:")
            logging.info(f"  Всего папок: {total_folders}")
    
        logging.info(f"\n📁 Созданные файлы:")
        logging.info(f"  ✅ {self.csv_filename}")
        logging.info(f"  ❌ {self.failed_csv_filename}")
        logging.info(f"  📝 {self.imap_errors_filename}")
        logging.info(f"  📋 imap_report_{self.timestamp}.log")


def main():
    # Настройки (замените на свои значения)
    ORG_ID = ""              # ID организации
    OAUTH_TOKEN = ""    # OAuth токен администратора
    CLIENT_ID = ""        # Client ID OAuth приложения
    CLIENT_SECRET = "" # Client Secret OAuth приложения
    
    # Параметры производительности
    MAX_WORKERS = 50          # Количество параллельных потоков
    IMAP_TIMEOUT = 60        # Таймаут для IMAP операций в секундах
    IMAP_RETRIES = 5         # Количество повторных попыток для IMAP операций
    
    try:
        analyzer = Yandex360IMAPAnalyzer(
            org_id=ORG_ID,
            oauth_token=OAUTH_TOKEN,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
            max_workers=MAX_WORKERS,
            imap_timeout=IMAP_TIMEOUT,
            imap_retries=IMAP_RETRIES
        )
        
        analyzer.process_all_users()
        analyzer.print_summary()
        
        logging.info("\n✅ Программа завершена")
        
    except KeyboardInterrupt:
        logging.info("\n⚠️  Прервано пользователем")
        sys.exit(0)
    except Exception as e:
        logging.error(f"💥 Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
