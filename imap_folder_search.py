'''
Скрипт позволяет посчитать число папок и писем в них у сотрудников по IMAP.

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

Запустить скрипт можно через командную строку или программу с возможностью запуска кода.

На Windows запустить скрипт можно так:
    # Перейдите в папку со скриптом
    cd C:\path\to\script
    # Запустите
    # python imap_folder_search.py

На Linux/MacOS запустить скрипт так:
    # Перейдите в папку со скриптом
    cd /path/to/script
    # Запустите
    python3 imap_folder_search.py

По завершении работы скрипта будет создано 4 файла:
    imap_report_YYYYMMDD_HHMMSS.csv - основной файл, куда будет записана информация о папках сотрудников
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

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(threadName)s] - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'imap_report_{timestamp}.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)


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

    def _select_mailbox(self, imap_connector: imaplib.IMAP4_SSL, mailbox_name: str) -> int:
        resp_code, mail_count = imap_connector.select(mailbox=mailbox_name, readonly=True)
        
        if resp_code != 'OK' or not mail_count or not mail_count[0]:
            raise Exception(f"Не удалось открыть папку {mailbox_name}")
        
        return int(mail_count[0].decode("utf-8"))

    def get_user_mailboxes_info(self, imap_connector: imaplib.IMAP4_SSL, email: str) -> List[Dict]:
        mailboxes_info = []
        
        try:
            mailboxes = self._get_mailboxes_list_with_retry(imap_connector)
            
            for mailbox in mailboxes:
                try:
                    mailbox_name = mailbox.decode("utf-8").split()[-1].replace('"', '')
                    
                    count = self._select_mailbox_with_retry(imap_connector, mailbox_name)
                    
                    mailbox_info = {
                        'email': email,
                        'folder': mailbox_name,
                        'emails_count': count
                    }
                    mailboxes_info.append(mailbox_info)
                    logging.debug(f"{email} - {mailbox_name}: {count} писем")
                        
                except Exception as e:
                    logging.warning(f"{email} - Ошибка обработки папки {mailbox_name}: {e}")
                    continue
            
        except Exception as e:
            logging.error(f"Ошибка при обработке почтового ящика {email}: {e}")
        finally:
            self._close_imap_connection(imap_connector, email)
        
        return mailboxes_info

    @retry_on_error(max_retries=5, delay=0.5, backoff=1.5)
    def _get_mailboxes_list_with_retry(self, imap_connector: imaplib.IMAP4_SSL) -> List:
        return self._get_mailboxes_list(imap_connector)

    @retry_on_error(max_retries=5, delay=0.5, backoff=1.5)
    def _select_mailbox_with_retry(self, imap_connector: imaplib.IMAP4_SSL, mailbox_name: str) -> int:
        return self._select_mailbox(imap_connector, mailbox_name)

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
            error_info['error_message'] = 'Не удалось получить OAuth токен для доступа к IMAP'
            
            with self.failed_lock:
                self.failed_users.append(error_info)
            
            self.save_failed_user_immediately(error_info)
            self.imap_error_logger.log_error(
                email=email,
                nickname=nickname,
                error_type='TOKEN_ERROR',
                error_message='Не удалось получить OAuth токен для доступа к IMAP. Проверьте права приложения.',
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
            
            logging.error(f"❌ [{index}/{total}] Не удалось подключиться к IMAP для {email} после {self.imap_retries} попыток: {error_msg}")
            return False
        
        mailboxes_info = self.get_user_mailboxes_info(imap_connector, email)
        
        if not mailboxes_info:
            error_info['error_type'] = 'NO_FOLDERS'
            error_info['error_message'] = 'Папки не найдены или недоступны. Возможно, почтовый ящик пуст или нет прав доступа.'
            
            with self.failed_lock:
                self.failed_users.append(error_info)
            
            self.save_failed_user_immediately(error_info)
            self.imap_error_logger.log_error(
                email=email,
                nickname=nickname,
                error_type='NO_FOLDERS',
                error_message='Папки не найдены или недоступны. Возможно, почтовый ящик пуст или нет прав доступа.',
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
        
        total_emails = sum(info['emails_count'] for info in mailboxes_info)
        logging.info(f"✅ [{index}/{total}] {email}: {len(mailboxes_info)} папок, {total_emails} писем")
        
        return True

    def append_to_csv(self, row: Dict) -> None:
        try:
            file_exists = False
            try:
                with open(self.csv_filename, 'r'):
                    file_exists = True
            except FileNotFoundError:
                pass
            
            with open(self.csv_filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
                fieldnames = ['nickname', 'email', 'folder', 'emails_count']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow({k: row.get(k, '') for k in fieldnames})
                
        except Exception as e:
            logging.error(f"Ошибка при добавлении в CSV: {e}")

    def save_failed_user_immediately(self, error_info: Dict) -> None:
        try:
            file_exists = False
            try:
                with open(self.failed_csv_filename, 'r'):
                    file_exists = True
            except FileNotFoundError:
                pass
            
            with open(self.failed_csv_filename, 'a', newline='', encoding='utf-8-sig') as csvfile:
                fieldnames = ['nickname', 'email', 'user_id', 'error_type', 'error_message', 'timestamp']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(error_info)
                
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
        logging.info(f"Количество потоков: {self.max_workers}")
        logging.info(f"Таймаут IMAP: {self.imap_timeout} сек")
        logging.info(f"Количество ретраев IMAP: {self.imap_retries}")
        logging.info(f"Промежуточное сохранение: включено")
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
                    
                    if idx % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = idx / elapsed if elapsed > 0 else 0
                        remaining = (total_users - idx) / rate if rate > 0 else 0
                        logging.info(f"📊 Прогресс: {idx}/{total_users} ({idx*100//total_users}%) | "
                                   f"Успешно: {successful} | Ошибок: {idx-successful} | "
                                   f"Скорость: {rate:.1f} польз/сек | "
                                   f"Осталось: ~{remaining/60:.1f} мин")
                        
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
        logging.info(f"⏱️  Время выполнения: {elapsed_time:.2f} секунд ({elapsed_time/60:.1f} минут)")
        logging.info(f"✅ Успешно обработано: {successful}/{total_users}")
        logging.info(f"❌ Ошибок: {len(self.failed_users)}")
        logging.info(f"📊 Средняя скорость: {total_users/elapsed_time:.2f} пользователей/сек")
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
                    'folders': 0,
                    'total_emails': 0
                }
            users_stats[email]['folders'] += 1
            users_stats[email]['total_emails'] += row.get('emails_count', 0)
    
        logging.info(f"\n✅ Успешно обработано пользователей: {len(users_stats)}")
        logging.info(f"❌ Ошибок обработки: {len(self.failed_users)}")
        logging.info(f"📊 Всего записей о папках: {len(self.results)}")
    
        if self.failed_users:
            error_types = {}
            for failed in self.failed_users:
                error_type = failed.get('error_type', 'UNKNOWN')
                error_types[error_type] = error_types.get(error_type, 0) + 1
        
            logging.info(f"\n📋 Распределение ошибок:")
            for error_type, count in sorted(error_types.items(), key=lambda x: x[1], reverse=True):
                logging.info(f"  - {error_type}: {count}")
    
        if users_stats:
            total_emails = sum(s['total_emails'] for s in users_stats.values())
            total_folders = sum(s['folders'] for s in users_stats.values())
        
            logging.info(f"\n📈 ИТОГО:")
            logging.info(f"  Всего папок: {total_folders}")
            logging.info(f"  Всего писем: {total_emails}")
    
        logging.info(f"\n📁 Созданные файлы:")
        logging.info(f"  ✅ Основной отчет: {self.csv_filename}")
        logging.info(f"  ❌ Ошибки CSV: {self.failed_csv_filename}")
        logging.info(f"  📝 Ошибки IMAP (подробно): {self.imap_errors_filename}")
        logging.info(f"  📋 Основной лог: imap_report_{self.timestamp}.log")


def main():
    # Настройки (замените на свои значения)
    ORG_ID = ""              # ID организации
    OAUTH_TOKEN = ""    # OAuth токен с правами на чтение информации о сотрудниках
    CLIENT_ID = ""        # Client ID сервисного приложения с правами для действий по IMAP
    CLIENT_SECRET = "" # Client Secret сервисного приложения с правами для действий по IMAP
    
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
        
        logging.info("\n✅ Программа успешно завершена")
        
    except KeyboardInterrupt:
        logging.info("\n⚠️  Прервано пользователем. Завершение работы...")
        sys.exit(0)
    except Exception as e:
        logging.error(f"💥 Критическая ошибка: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
