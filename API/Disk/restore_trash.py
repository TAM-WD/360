### Скрипт для  восстановления файлов
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, unquote
import time
import os
import csv
import sys
import threading

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

CLIENT_ID = '' # id сервисного приложения с правами на чтение диска cloud_api:disk.info и cloud_api:disk.write
CLIENT_SECRET = '' # secret сервисного приложения с правами на чтение диска cloud_api:disk.info и cloud_api:disk.write

DISK_ID = '' # uid =Диска.

# Пути к файлам/папкам для восстановления
PATHS_TO_RESTORE = [
    'trash:/'
]

# ============================================================================
# ФИЛЬТР ПО ДАТЕ УДАЛЕНИЯ
# ============================================================================
DATE_FILTER = {
    'enabled': True,
    'start_date': '2026-01-29 13:47',
    'end_date': '2026-01-29 15:50',
    'timezone': 'UTC',
}

# Параметры восстановления
RESTORE_CONFIG = {
    'overwrite': False,
}

TRACKING_CONFIG = {
    'enabled': True,
    'timeout': 300,
    'batch_check_interval': 2
}

API_CONFIG = {
    'delay_between_requests': 0.1,
    'max_retries': 5,
    'retry_delay': 2,
    'page_size': 1000,
    'max_rps': 10,
    'token_lifetime_minutes': 55,
}

DEBUG = True

# ============================================================================
# ЛОГИРОВАНИЕ
# ============================================================================

class Logger:
    """Логгер с записью в файл и консоль"""
    
    def __init__(self, log_dir=None):
        self.log_dir = log_dir
        self.log_file = None
        self.start_time = datetime.now()
        
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            log_filename = os.path.join(log_dir, 'restore.log')
            self.log_file = open(log_filename, 'w', encoding='utf-8')
            self.log(f"Log started: {self.start_time}")
            self.log(f"Log directory: {log_dir}")
    
    def log(self, message, level='INFO'):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        formatted = f"[{timestamp}] [{level}] {message}"
        
        print(formatted)
        
        if self.log_file:
            self.log_file.write(formatted + '\n')
            self.log_file.flush()
    
    def debug(self, message):
        if DEBUG:
            self.log(message, 'DEBUG')
    
    def info(self, message):
        self.log(message, 'INFO')
    
    def warning(self, message):
        self.log(message, 'WARN')
    
    def error(self, message):
        self.log(message, 'ERROR')
    
    def close(self):
        if self.log_file:
            self.log(f"Log ended. Duration: {datetime.now() - self.start_time}")
            self.log_file.close()


class RateLimiter:
    """Rate limiter для ограничения RPS"""
    
    def __init__(self, max_rps=10):
        self.min_interval = 1.0 / max_rps
        self.last_request_time = 0
        self.lock = threading.Lock()
    
    def wait(self):
        with self._lock:
            current_time = time.time()
            elapsed = current_time - self.last_request_time
            
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                time.sleep(sleep_time)
            
            self.last_request_time = time.time()
    
    @property
    def _lock(self):
        return self.lock


class TokenManager:
    """Менеджер токенов с автоматическим обновлением"""
    
    def __init__(self, disk_id, client_id, client_secret, token_lifetime_minutes=55):
        self.disk_id = disk_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_lifetime = token_lifetime_minutes * 60  # в секундах
        
        self._token = None
        self._token_obtained_at = None
        self._lock = threading.Lock()
        self._refresh_count = 0
    
    def _fetch_new_token(self):
        """Получение нового токена через token exchange"""
        url = 'https://oauth.yandex.ru/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'subject_token': self.disk_id,
            'subject_token_type': 'urn:yandex:params:oauth:token-type:uid'
        }
        
        if logger:
            logger.info(f"Requesting new token for disk: {self.disk_id}")
        
        retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 502, 503, 504])
        session = requests.Session()
        session.mount('https://', HTTPAdapter(max_retries=retries))
        
        try:
            response = session.post(url, data=data, headers=headers, timeout=30)
            
            if response.status_code == 200:
                token_data = response.json()
                self._token = token_data['access_token']
                self._token_obtained_at = time.time()
                self._refresh_count += 1
                
                expires_in = token_data.get('expires_in', 3600)
                if logger:
                    logger.info(f"Token received (expires in {expires_in}s, refresh #{self._refresh_count})")
                    logger.debug(f"Token will be refreshed after {self.token_lifetime}s")
                
                return self._token
            else:
                if logger:
                    logger.error(f"Error getting token: {response.status_code} - {response.text}")
                return None
                
        except requests.exceptions.RequestException as e:
            if logger:
                logger.error(f"Request error while getting token: {e}")
            return None
    
    def _is_token_expired(self):
        """Проверка истечения токена"""
        if self._token is None or self._token_obtained_at is None:
            return True
        
        elapsed = time.time() - self._token_obtained_at
        return elapsed >= self.token_lifetime
    
    def get_token(self):
        """Получение актуального токена. Автоматически обновляет токен если он истёк или скоро истечёт."""
        with self._lock:
            if self._is_token_expired():
                remaining = self.get_token_remaining()
                
                if self._token:
                    if logger:
                        logger.info(f"Token expiring (remaining: {remaining:.0f}s). Refreshing...")
                
                new_token = self._fetch_new_token()
                
                if new_token is None:
                    if self._token:
                        if logger:
                            logger.warning("Failed to refresh token, using existing token")
                        return self._token
                    else:
                        raise Exception("Failed to obtain token")
                
                return new_token
            
            return self._token
    
    def get_token_age(self):
        """Возвращает возраст токена в секундах"""
        if self._token_obtained_at is None:
            return None
        return time.time() - self._token_obtained_at
    
    def get_token_remaining(self):
        """Возвращает оставшееся время жизни токена в секундах"""
        if self._token_obtained_at is None:
            return 0
        elapsed = time.time() - self._token_obtained_at
        return max(0, self.token_lifetime - elapsed)
    
    def force_refresh(self):
        """Принудительное обновление токена"""
        with self._lock:
            if logger:
                logger.info("Force refreshing token...")
            self._token = None
            self._token_obtained_at = None
            return self._fetch_new_token()
    
    def get_status(self):
        """Возвращает статус токена для логирования"""
        if self._token is None:
            return "No token"
        
        age = self.get_token_age()
        remaining = self.get_token_remaining()
        
        return f"age: {age:.0f}s, remaining: {remaining:.0f}s, refreshes: {self._refresh_count}"


class ResultsWriter:
    """Запись результатов в CSV"""
    
    def __init__(self, log_dir):
        self.log_dir = log_dir
        self.results = []
        self.csv_file = os.path.join(log_dir, 'restore_results.csv')
        self.fieldnames = [
            'index',
            'name',
            'type',
            'size_bytes',
            'deleted_at',
            'original_path',
            'trash_path',
            'status',
            'restored_to',
            'status_code',
            'error_message',
            'is_async',
            'restore_time'
        ]
        
        with open(self.csv_file, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writeheader()
    
    def add_result(self, result, index=None):
        """Добавляет результат и записывает в CSV"""
        row = {
            'index': index,
            'name': result.get('name', ''),
            'type': result.get('type', ''),
            'size_bytes': result.get('size', ''),
            'deleted_at': result.get('deleted', ''),
            'original_path': result.get('original_path', ''),
            'trash_path': result.get('trash_path', ''),
            'status': result.get('final_status', ''),
            'restored_to': result.get('restored_to', ''),
            'status_code': result.get('status_code', ''),
            'error_message': result.get('error_message', ''),
            'is_async': result.get('is_async', False),
            'restore_time': datetime.now().isoformat()
        }
        
        self.results.append(row)
        
        with open(self.csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.fieldnames)
            writer.writerow(row)
    
    def write_summary(self):
        """Записывает итоговую статистику"""
        summary_file = os.path.join(self.log_dir, 'summary.txt')
        
        total = len(self.results)
        success = sum(1 for r in self.results if r['status'] == 'success')
        skipped = sum(1 for r in self.results if r['status'] == 'skipped')
        failed = sum(1 for r in self.results if r['status'] == 'failed')
        not_found = sum(1 for r in self.results if r['status'] == 'not_found')
        timeout = sum(1 for r in self.results if r['status'] == 'timeout')
        
        with open(summary_file, 'w', encoding='utf-8-sig') as f:
            f.write(f"Restore Summary\n")
            f.write(f"{'='*50}\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n")
            f.write(f"Disk ID: {DISK_ID}\n\n")
            f.write(f"Total: {total}\n")
            f.write(f"Success: {success}\n")
            f.write(f"Skipped (exists): {skipped}\n")
            f.write(f"Failed: {failed}\n")
            f.write(f"Not found: {not_found}\n")
            f.write(f"Timeout: {timeout}\n")
            f.write(f"{'='*50}\n")


logger = None
rate_limiter = None
results_writer = None
token_manager = None


def create_log_directory():
    """Создаёт директорию для логов"""
    script_name = os.path.splitext(os.path.basename(__file__))[0]
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_dir = f"{script_name}_{timestamp}"
    os.makedirs(log_dir, exist_ok=True)
    return log_dir


def init_logging(log_dir):
    """Инициализация системы логирования"""
    global logger, rate_limiter, results_writer, token_manager
    
    logger = Logger(log_dir)
    rate_limiter = RateLimiter(max_rps=API_CONFIG.get('max_rps', 10))
    results_writer = ResultsWriter(log_dir)
    token_manager = TokenManager(
        disk_id=DISK_ID,
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        token_lifetime_minutes=API_CONFIG.get('token_lifetime_minutes', 55)
    )
    
    return logger


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def parse_date(date_string, use_utc=True):
    """Парсинг строки даты в datetime объект"""
    if date_string is None:
        return None
    
    date_string = date_string.strip()
    
    formats = [
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y-%m-%d',
    ]
    
    parsed_date = None
    for fmt in formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            break
        except ValueError:
            continue
    
    if parsed_date is None:
        raise ValueError(f"Cannot parse date: '{date_string}'")
    
    if use_utc:
        parsed_date = parsed_date.replace(tzinfo=timezone.utc)
    
    return parsed_date


def parse_api_date(date_string):
    """Парсинг даты из API ответа (ISO 8601 формат)"""
    if not date_string:
        return None
    
    try:
        return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    except:
        try:
            return datetime.strptime(date_string[:19], '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc)
        except:
            return None


def filter_items_by_date(items, start_date=None, end_date=None, use_utc=True):
    """Фильтрация элементов по дате удаления"""
    if isinstance(start_date, str):
        start_date = parse_date(start_date, use_utc)
    if isinstance(end_date, str):
        end_date = parse_date(end_date, use_utc)
    
    if start_date is None and end_date is None:
        return items
    
    filtered = []
    
    for item in items:
        deleted_str = item.get('deleted', '')
        deleted_date = parse_api_date(deleted_str)
        
        if deleted_date is None:
            continue
        
        if start_date and deleted_date < start_date:
            continue
        if end_date and deleted_date > end_date:
            continue
        
        filtered.append(item)
    
    return filtered


# ============================================================================
# API ФУНКЦИИ
# ============================================================================

def make_request(method, url, headers=None, params=None, data=None, timeout=30, use_auth=True):
    """
    Универсальная функция для API запросов с ретраями и rate limiting.
    Автоматически добавляет актуальный токен авторизации.
    """
    max_retries = API_CONFIG.get('max_retries', 5)
    retry_delay = API_CONFIG.get('retry_delay', 2)
    
    if headers is None:
        headers = {}
    
    last_error = None
    
    for attempt in range(max_retries):
        try:
            # Rate limiting
            rate_limiter.wait()
            
            # Добавляем актуальный токен авторизации
            if use_auth and token_manager:
                token = token_manager.get_token()
                if token:
                    headers['Authorization'] = f'OAuth {token}'
                else:
                    logger.error("No valid token available")
                    return None
            
            # Выполняем запрос
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, params=params, timeout=timeout)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, params=params, data=data, timeout=timeout)
            elif method.upper() == 'PUT':
                response = requests.put(url, headers=headers, params=params, timeout=timeout)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            # Проверяем на ошибку авторизации
            if response.status_code == 401:
                logger.warning(f"Auth error (401), refreshing token... (attempt {attempt + 1}/{max_retries})")
                if token_manager:
                    token_manager.force_refresh()
                continue
            
            # Успешные коды или ожидаемые ошибки
            if response.status_code < 500 or response.status_code in [404, 409]:
                return response
            
            # 5xx ошибки - ретраим
            logger.warning(f"Server error {response.status_code}, attempt {attempt + 1}/{max_retries}")
            last_error = f"HTTP {response.status_code}"
            
        except requests.exceptions.Timeout as e:
            logger.warning(f"Timeout, attempt {attempt + 1}/{max_retries}: {e}")
            last_error = str(e)
            
        except requests.exceptions.ConnectionError as e:
            logger.warning(f"Connection error, attempt {attempt + 1}/{max_retries}: {e}")
            last_error = str(e)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            last_error = str(e)
            break
        
        # Экспоненциальная задержка
        if attempt < max_retries - 1:
            wait_time = retry_delay * (2 ** attempt)
            logger.debug(f"Waiting {wait_time}s before retry...")
            time.sleep(wait_time)
    
    logger.error(f"All {max_retries} attempts failed. Last error: {last_error}")
    return None


def get_trash_page(path='/', limit=1000, offset=0):
    """Получение одной страницы элементов корзины"""
    url = 'https://cloud-api.yandex.net/v1/disk/trash/resources'
    headers = {'Accept': 'application/json'}
    params = {'path': path, 'limit': limit, 'offset': offset}
    
    logger.debug(f"Fetching trash page: offset={offset}, limit={limit}")
    
    response = make_request('GET', url, headers=headers, params=params)
    
    if response and response.status_code == 200:
        return response.json()
    else:
        error_text = response.text if response else "No response"
        logger.error(f"Error fetching trash: {error_text}")
        return None


def get_all_trash_items(page_size=1000):
    """Получение ВСЕХ элементов корзины с пагинацией"""
    all_items = []
    offset = 0
    total = None
    
    logger.info(f"Fetching all trash items (page size: {page_size})...")
    
    while True:
        page_data = get_trash_page(path='/', limit=page_size, offset=offset)
        
        if page_data is None:
            logger.error(f"Error fetching page at offset {offset}")
            break
        
        embedded = page_data.get('_embedded', {})
        items = embedded.get('items', [])
        
        if total is None:
            total = embedded.get('total', 0)
            logger.info(f"Total items in trash: {total}")
        
        if not items:
            break
        
        all_items.extend(items)
        logger.info(f"Fetched: {len(all_items)} / {total} items")
        
        # Логируем статус токена каждые 10 страниц
        if offset > 0 and (offset // page_size) % 10 == 0:
            logger.debug(f"Token status: {token_manager.get_status()}")
        
        if len(all_items) >= total:
            break
        
        if len(items) < page_size:
            break
        
        offset += page_size
    
    logger.info(f"Total fetched: {len(all_items)} items")
    return all_items


def convert_trash_path_for_api(trash_path):
    """Конвертация пути для restore запроса"""
    path = trash_path.strip()
    
    if path.startswith('trash:'):
        return path
    
    if path.startswith('/'):
        return 'trash:' + path
    else:
        return 'trash:/' + path


def restore_from_trash(trash_path, overwrite=False):
    """Восстановление файла/папки из корзины"""
    api_path = convert_trash_path_for_api(trash_path)
    
    url = 'https://cloud-api.yandex.net/v1/disk/trash/resources/restore'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
    }
    params = {'path': api_path}
    if overwrite:
        params['overwrite'] = 'true'
    
    logger.debug(f"Restoring: {trash_path}")
    logger.debug(f"API path: {api_path}")
    
    response = make_request('PUT', url, headers=headers, params=params)
    
    if response:
        logger.debug(f"Response status: {response.status_code}")
        if response.status_code >= 400:
            logger.debug(f"Response body: {response.text}")
    
    return response


def check_operation_status(operation_href):
    """Проверка статуса асинхронной операции"""
    headers = {'Accept': 'application/json'}
    
    response = make_request('GET', operation_href, headers=headers)
    
    if response and response.status_code == 200:
        return response.json()
    return {'status': 'error', 'error': f'HTTP {response.status_code if response else "No response"}'}


# ============================================================================
# ФУНКЦИИ ВОССТАНОВЛЕНИЯ
# ============================================================================

def normalize_path(path):
    """Нормализация пути для сравнения"""
    path = path.strip()
    for prefix in ['disk:/', 'trash:/']:
        if path.startswith(prefix):
            path = path[len(prefix):]
    return path.lstrip('/')


def find_items_by_folder(items, folder_path):
    """Поиск всех элементов из указанной папки"""
    folder_normalized = normalize_path(folder_path).rstrip('/') + '/'
    
    matches = []
    for item in items:
        original_path = normalize_path(item.get('origin_path', ''))
        if original_path.startswith(folder_normalized):
            matches.append(item)
    
    return matches


def find_item_by_path(items, search_path):
    """Поиск элемента по пути или имени"""
    search_normalized = search_path.strip()
    
    if search_normalized.startswith('trash:/'):
        for item in items:
            if item.get('path', '') == search_normalized:
                return {'item': item, 'match_type': 'direct_trash_path'}
    
    search_clean = normalize_path(search_normalized)
    
    matches = []
    for item in items:
        name = item.get('name', '')
        origin = normalize_path(item.get('origin_path', ''))
        
        if name == search_clean or name == search_normalized:
            matches.append({'item': item, 'match_type': 'name'})
        elif origin == search_clean:
            matches.append({'item': item, 'match_type': 'original_path'})
    
    if not matches:
        return None
    
    matches.sort(key=lambda x: x['item'].get('deleted', ''), reverse=True)
    return matches[0]


def restore_single_item(item, config, index=None):
    """Восстановление одного элемента"""
    trash_path = item.get('path', '')
    original_path = item.get('origin_path', '')
    
    response = restore_from_trash(
        trash_path,
        overwrite=config.get('overwrite', False)
    )
    
    result = {
        'name': item.get('name', 'N/A'),
        'type': item.get('type', 'N/A'),
        'size': item.get('size', 0),
        'trash_path': trash_path,
        'original_path': original_path,
        'deleted': item.get('deleted', 'N/A'),
        'status_code': response.status_code if response else None,
        'success': False,
        'is_async': False,
        'operation_href': None,
        'final_status': None,
        'restored_to': None,
        'error_message': None
    }
    
    if response is None:
        result['final_status'] = 'failed'
        result['error_message'] = 'Request failed after all retries'
        logger.error(f"    ✗ Request failed for {item.get('name')}")
        
        if results_writer:
            results_writer.add_result(result, index)
        
        return result
    
    if response.status_code == 201:
        result['success'] = True
        result['final_status'] = 'success'
        
        restored_path = original_path
        try:
            data = response.json()
            logger.debug(f"Response body: {data}")
            
            if data.get('path'):
                restored_path = data.get('path')
            elif data.get('href'):
                parsed = urlparse(data['href'])
                query_params = parse_qs(parsed.query)
                if 'path' in query_params:
                    restored_path = unquote(query_params['path'][0])
        except:
            pass
        
        result['restored_to'] = restored_path
        logger.info(f"    ✓ Restored: {item.get('name')} → {restored_path}")
    
    elif response.status_code == 202:
        result['success'] = True
        result['is_async'] = True
        result['restored_to'] = original_path
        
        try:
            data = response.json()
            logger.debug(f"Async response body: {data}")
            result['operation_href'] = data.get('href', '')
        except:
            pass
        
        logger.info(f"    ⏳ Queued: {item.get('name')}")
    
    elif response.status_code == 409:
        result['final_status'] = 'skipped'
        result['restored_to'] = original_path
        result['error_message'] = 'File already exists'
        logger.warning(f"    ⚠ Already exists: {item.get('name')} at {original_path}")
    
    elif response.status_code == 404:
        result['final_status'] = 'not_found'
        result['error_message'] = 'Not found in trash'
        logger.error(f"    ✗ Not found: {item.get('name')}")
    
    else:
        result['final_status'] = 'failed'
        try:
            error_data = response.json()
            result['error_message'] = error_data.get('message', f'HTTP {response.status_code}')
        except:
            result['error_message'] = f'HTTP {response.status_code}'
        logger.error(f"    ✗ Error {response.status_code}: {item.get('name')}")
    
    if results_writer:
        results_writer.add_result(result, index)
    
    return result


def wait_for_operations(operations, timeout=300, interval=2):
    """Ожидание завершения асинхронных операций"""
    if not operations:
        return
    
    pending = {op['href']: op for op in operations if op.get('href')}
    start_time = time.time()
    
    logger.info(f"Tracking {len(pending)} async operation(s)...")
    
    while pending and (time.time() - start_time < timeout):
        completed = []
        
        for href, op in pending.items():
            status_info = check_operation_status(href)
            status = status_info.get('status', 'unknown')
            
            if status == 'success':
                op['result']['final_status'] = 'success'
                logger.info(f"    ✓ Completed: {op['name']}")
                completed.append(href)
            elif status in ['failed', 'error']:
                op['result']['final_status'] = 'failed'
                op['result']['success'] = False
                error = status_info.get('error', 'Unknown')
                op['result']['error_message'] = error
                logger.error(f"    ✗ Failed: {op['name']}: {error}")
                completed.append(href)
        
        for href in completed:
            del pending[href]
        
        if pending:
            elapsed = int(time.time() - start_time)
            logger.debug(f"Waiting... ({len(pending)} pending, {elapsed}s)")
            time.sleep(interval)
    
    for op in pending.values():
        op['result']['final_status'] = 'timeout'
        op['result']['success'] = False
        op['result']['error_message'] = 'Operation timed out'
        logger.warning(f"    ⏰ Timeout: {op['name']}")


def restore_files(paths_to_restore, trash_items, config, tracking_config, date_filter=None):
    """Восстановление файлов из корзины"""
    if not paths_to_restore:
        logger.warning("No paths specified")
        return []
    
    logger.info("=" * 80)
    logger.info("Starting restoration")
    logger.info("=" * 80)
    
    use_utc = date_filter.get('timezone', 'UTC').upper() == 'UTC' if date_filter else True
    
    if date_filter and date_filter.get('enabled'):
        start_date = date_filter.get('start_date')
        end_date = date_filter.get('end_date')
        
        if start_date or end_date:
            logger.info(f"Date filter: {start_date} to {end_date}")
    else:
        start_date = None
        end_date = None
    
    results = []
    async_ops = []
    global_index = 0
    
    for search_path in paths_to_restore:
        logger.info(f"Processing: {search_path}")
        
        # Логируем статус токена
        logger.debug(f"Token status: {token_manager.get_status()}")
        
        # Проверка на восстановление всей корзины
        if search_path.strip() in ['trash:/', 'trash:', '/', '*', 'all']:
            items_to_restore = filter_items_by_date(
                trash_items, 
                start_date=start_date, 
                end_date=end_date,
                use_utc=use_utc
            )
            
            if not items_to_restore:
                if start_date or end_date:
                    logger.warning("No items match the date filter")
                else:
                    logger.warning("Trash is empty")
                continue
            
            if start_date or end_date:
                logger.info(f"Found {len(items_to_restore)} items matching date filter (out of {len(trash_items)} total)")
            
            logger.info(f"🗑️ Restoring {len(items_to_restore)} items from trash...")
            
            for idx, item in enumerate(items_to_restore):
                global_index += 1
                logger.info(f"[{idx+1}/{len(items_to_restore)}] {item.get('name')} ({item.get('type')})")
                logger.debug(f"  Deleted: {item.get('deleted')}")
                logger.debug(f"  Original: {item.get('origin_path')}")
                
                # Периодически логируем статус токена
                if idx > 0 and idx % 100 == 0:
                    logger.info(f"Progress: {idx}/{len(items_to_restore)}, Token: {token_manager.get_status()}")
                
                result = restore_single_item(item, config, global_index)
                result['search_path'] = search_path
                results.append(result)
                
                if result.get('is_async') and result.get('operation_href'):
                    async_ops.append({
                        'name': result['name'],
                        'href': result['operation_href'],
                        'result': result
                    })
            
            continue
        
        # Поиск по папке
        folder_items = find_items_by_folder(trash_items, search_path)
        
        if folder_items and (start_date or end_date):
            folder_items = filter_items_by_date(folder_items, start_date, end_date, use_utc)
        
        if folder_items:
            logger.info(f"Found {len(folder_items)} item(s) in folder")
            
            for idx, item in enumerate(folder_items):
                global_index += 1
                logger.info(f"  [{idx+1}/{len(folder_items)}] {item.get('name')} ({item.get('type')})")
                
                result = restore_single_item(item, config, global_index)
                result['search_path'] = search_path
                results.append(result)
                
                if result.get('is_async') and result.get('operation_href'):
                    async_ops.append({
                        'name': result['name'],
                        'href': result['operation_href'],
                        'result': result
                    })
        
        else:
            match = find_item_by_path(trash_items, search_path)
            
            if not match:
                logger.error(f"  ✗ Not found in trash: {search_path}")
                global_index += 1
                result = {
                    'search_path': search_path,
                    'name': search_path,
                    'success': False,
                    'final_status': 'not_found',
                    'error_message': 'Not found in trash'
                }
                results.append(result)
                if results_writer:
                    results_writer.add_result(result, global_index)
                continue
            
            item = match['item']
            
            if start_date or end_date:
                filtered = filter_items_by_date([item], start_date, end_date, use_utc)
                if not filtered:
                    logger.warning(f"  ⚠ Found but doesn't match date filter: {item.get('deleted')}")
                    global_index += 1
                    result = {
                        'search_path': search_path,
                        'name': item.get('name'),
                        'success': False,
                        'final_status': 'filtered_by_date',
                        'error_message': 'Filtered by date'
                    }
                    results.append(result)
                    if results_writer:
                        results_writer.add_result(result, global_index)
                    continue
            
            global_index += 1
            logger.info(f"  Found: {item.get('path')}")
            logger.debug(f"  Match type: {match['match_type']}")
            
            result = restore_single_item(item, config, global_index)
            result['search_path'] = search_path
            results.append(result)
            
            if result.get('is_async') and result.get('operation_href'):
                async_ops.append({
                    'name': result['name'],
                    'href': result['operation_href'],
                    'result': result
                })
    
    # Ожидание асинхронных операций
    if async_ops and tracking_config.get('enabled', True):
        logger.info("=" * 80)
        logger.info("Tracking async operations")
        logger.info("=" * 80)
        wait_for_operations(async_ops, 
                          tracking_config.get('timeout', 300),
                          tracking_config.get('batch_check_interval', 2))
    
    return results


def display_trash_info(items, show_items=True, max_display=50, date_filter=None):
    """Отображение информации о содержимом корзины"""
    logger.info("=" * 80)
    logger.info("Trash information")
    logger.info("=" * 80)
    logger.info(f"Total items in trash: {len(items)}")
    
    if date_filter and date_filter.get('enabled'):
        logger.info(f"Date filter: {date_filter.get('start_date')} to {date_filter.get('end_date')}")
    
    if show_items and items:
        display_count = min(len(items), max_display)
        logger.info(f"Showing first {display_count} items:")
        
        for idx, item in enumerate(items[:display_count], 1):
            logger.info(f"{idx}. {item.get('name', 'N/A')} ({item.get('type', 'N/A')})")
            logger.debug(f"   Size: {item.get('size', 0)} bytes")
            logger.debug(f"   Deleted: {item.get('deleted', 'N/A')}")
            logger.debug(f"   Original: {item.get('origin_path', 'N/A')}")
            logger.debug(f"   Trash: {item.get('path', 'N/A')}")
        
        if len(items) > max_display:
            logger.info(f"... and {len(items) - max_display} more items")


def display_summary(results):
    """Итоговый отчёт"""
    logger.info("=" * 80)
    logger.info("Summary")
    logger.info("=" * 80)
    
    success = [r for r in results if r.get('final_status') == 'success']
    skipped = [r for r in results if r.get('final_status') == 'skipped']
    failed = [r for r in results if r.get('final_status') == 'failed']
    not_found = [r for r in results if r.get('final_status') == 'not_found']
    timeout = [r for r in results if r.get('final_status') == 'timeout']
    filtered_by_date = [r for r in results if r.get('final_status') == 'filtered_by_date']
    
    logger.info(f"Total: {len(results)}")
    logger.info(f"✓ Success: {len(success)}")
    logger.info(f"⚠ Skipped (exists): {len(skipped)}")
    logger.info(f"✗ Failed: {len(failed)}")
    logger.info(f"❓ Not found: {len(not_found)}")
    logger.info(f"📅 Filtered by date: {len(filtered_by_date)}")
    logger.info(f"⏰ Timeout: {len(timeout)}")
    
    # Статистика токена
    logger.info(f"Token refreshes: {token_manager._refresh_count}")
    
    if success:
        logger.info(f"\n✓ Restored ({len(success)}):")
        for r in success[:20]:
            restored_to = r.get('restored_to') or r.get('original_path', 'N/A')
            logger.info(f"  - {r.get('name')} → {restored_to}")
        if len(success) > 20:
            logger.info(f"  ... and {len(success) - 20} more")
    
    if failed:
        logger.info(f"\n✗ Failed ({len(failed)}):")
        for r in failed[:20]:
            logger.info(f"  - {r.get('name')}: {r.get('error_message', 'Unknown error')}")
        if len(failed) > 20:
            logger.info(f"  ... and {len(failed) - 20} more")
    
    if results_writer:
        results_writer.write_summary()
    
    logger.info("=" * 80)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == '__main__':
    
    # Создаём директорию для логов
    log_dir = create_log_directory()
    
    # Инициализируем логирование
    logger = init_logging(log_dir)
    
    try:
        logger.info("=" * 80)
        logger.info("Yandex Disk Trash Manager")
        logger.info(f"Log directory: {log_dir}")
        logger.info(f"Disk ID: {DISK_ID}")
        logger.info(f"DEBUG: {DEBUG}")
        logger.info(f"Max RPS: {API_CONFIG.get('max_rps', 10)}")
        logger.info(f"Token lifetime: {API_CONFIG.get('token_lifetime_minutes', 55)} minutes")
        logger.info("=" * 80)
        
        if DATE_FILTER.get('enabled'):
            logger.info(f"Date filter enabled:")
            logger.info(f"  Start: {DATE_FILTER.get('start_date', 'Not set')}")
            logger.info(f"  End:   {DATE_FILTER.get('end_date', 'Not set')}")
            logger.info(f"  TZ:    {DATE_FILTER.get('timezone', 'UTC')}")
        
        # Получаем начальный токен
        initial_token = token_manager.get_token()
        
        if not initial_token:
            logger.error("Failed to get initial token. Exiting.")
            sys.exit(1)
        
        # Получаем содержимое корзины
        trash_items = get_all_trash_items(page_size=API_CONFIG.get('page_size', 1000))
        
        # Показываем отфильтрованные элементы
        if DATE_FILTER.get('enabled') and (DATE_FILTER.get('start_date') or DATE_FILTER.get('end_date')):
            use_utc = DATE_FILTER.get('timezone', 'UTC').upper() == 'UTC'
            filtered_items = filter_items_by_date(
                trash_items,
                start_date=DATE_FILTER.get('start_date'),
                end_date=DATE_FILTER.get('end_date'),
                use_utc=use_utc
            )
            logger.info(f"Items matching date filter: {len(filtered_items)} / {len(trash_items)}")
            display_trash_info(filtered_items, show_items=True, max_display=50, date_filter=DATE_FILTER)
        else:
            display_trash_info(trash_items, show_items=True, max_display=50)
        
        # Восстанавливаем файлы
        if PATHS_TO_RESTORE:
            results = restore_files(PATHS_TO_RESTORE, trash_items, 
                                   RESTORE_CONFIG, TRACKING_CONFIG, DATE_FILTER)
            display_summary(results)
        else:
            logger.warning("No paths specified (PATHS_TO_RESTORE is empty)")
        
        logger.info('Complete')
        logger.info(f"Results saved to: {log_dir}")
        
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(130)
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
        
    finally:
        if logger:
            logger.close()
