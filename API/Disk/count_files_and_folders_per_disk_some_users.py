import requests
import time
from collections import deque

orgId = ''  # ID организации
org_token = ''  # токен OAuth приложения
client_id = ''  # id сервисного приложения
client_secret = ''  # secret сервисного приложения

# Настройки для /files метода
FILES_LIMIT = 1000
FILES_MAX_OFFSET = 100000

# Настройки retry
MAX_RETRIES = 2
RETRY_DELAY = 2

# Ограничения для защиты от циклических структур
MAX_PATH_LENGTH = 1000  # Максимальная длина пути
MAX_FOLDER_DEPTH = 50   # Максимальная глубина вложенности

# Время жизни токена
TOKEN_LIFETIME = 55 * 60  # 55 минут в секундах


def get_user(user_id):
    url = f'https://api360.yandex.net/directory/v1/org/{orgId}/users/{user_id}'
    headers = {'Authorization': f'OAuth {org_token}'}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()


def get_token(user_id):
    """Получение нового токена для пользователя"""
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': f'{client_id}',
        'client_secret': f'{client_secret}',
        'subject_token': f'{user_id}',
        'subject_token_type': f'urn:yandex:params:oauth:token-type:uid'
    }
    response = requests.post(url, headers=headers, data=data)
    response.raise_for_status()
    return response.json()['access_token']


def enable_user(user_id):
    url = f'https://api360.yandex.net/directory/v1/org/{orgId}/users/{user_id}'
    headers = {'Authorization': f'OAuth {org_token}'}
    body = {'isEnabled': True}
    response = requests.patch(url, headers=headers, json=body)
    response.raise_for_status()
    print(f"  ✓ {user_id} включен")


def disable_user(user_id):
    url = f'https://api360.yandex.net/directory/v1/org/{orgId}/users/{user_id}'
    headers = {'Authorization': f'OAuth {org_token}'}
    body = {'isEnabled': False}
    response = requests.patch(url, headers=headers, json=body)
    response.raise_for_status()
    print(f"  ✓ {user_id} отключен")


class TokenManager:
    """Управление токеном с автоматическим обновлением"""
    
    def __init__(self, user_id, initial_token):
        self.user_id = user_id
        self.token = initial_token
        self.created_at = time.time()
        self.refresh_count = 0
    
    def get_token(self):
        """Получить актуальный токен, обновить если нужно"""
        elapsed = time.time() - self.created_at
        
        if elapsed >= TOKEN_LIFETIME:
            self.refresh()
        
        return self.token
    
    def refresh(self):
        """Принудительно обновить токен"""
        try:
            minutes_elapsed = (time.time() - self.created_at) / 60
            print(f"      🔄 Обновление токена (прошло {minutes_elapsed:.1f} мин)")
            self.token = get_token(self.user_id)
            self.created_at = time.time()
            self.refresh_count += 1
            print(f"      ✓ Токен обновлён (раз #{self.refresh_count})")
        except Exception as e:
            print(f"      ❌ Не удалось обновить токен: {e}")
            raise
    
    def get_stats(self):
        """Статистика по токену"""
        elapsed = time.time() - self.created_at
        return {
            'refresh_count': self.refresh_count,
            'age_minutes': elapsed / 60,
            'expires_in_minutes': (TOKEN_LIFETIME - elapsed) / 60
        }


def get_files_page(limit, offset, token_manager):
    """Получение одной страницы файлов через /files"""
    url = 'https://cloud-api.yandex.net/v1/disk/resources/files'
    params = {
        'limit': limit,
        'offset': offset,
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            token = token_manager.get_token()
            headers = {'Authorization': f'OAuth {token}'}
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"      ⚠️ Попытка {attempt + 1}/{MAX_RETRIES} не удалась: {type(e).__name__}")
                time.sleep(RETRY_DELAY)
            else:
                raise


def get_folder_contents_page(path, token_manager, limit=1000, offset=0):
    """Получение одной страницы содержимого папки"""
    url = 'https://cloud-api.yandex.net/v1/disk/resources'
    params = {
        'path': path,
        'limit': limit,
        'offset': offset,
    }
    
    for attempt in range(MAX_RETRIES):
        try:
            token = token_manager.get_token()
            headers = {'Authorization': f'OAuth {token}'}
            response = requests.get(url, params=params, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                print(f"      ⚠️ Попытка {attempt + 1}/{MAX_RETRIES} не удалась: {type(e).__name__}")
                time.sleep(RETRY_DELAY)
            else:
                raise


def count_files_flat(token_manager):
    """Быстрый подсчёт через /files endpoint"""
    print("  📋 Метод 1: Быстрый подсчёт через /files")
    total_count = 0
    offset = 0
    
    try:
        while offset < FILES_MAX_OFFSET:
            response = get_files_page(FILES_LIMIT, offset, token_manager)
            items = response.get('items', [])
            
            if not items:
                print(f"  ✅ Метод /files успешно завершён: {total_count} файлов")
                return total_count, True
            
            total_count += len(items)
            offset += len(items)
            
            if total_count % 5000 == 0:
                stats = token_manager.get_stats()
                print(f"    ├─ Обработано: {total_count} файлов (токен обновлён {stats['refresh_count']} раз)")
            
            if len(items) < FILES_LIMIT:
                print(f"  ✅ Метод /files успешно завершён: {total_count} файлов")
                return total_count, True
        
        print(f"  ⚠️ Достигнут лимит offset={FILES_MAX_OFFSET}")
        return total_count, False
        
    except Exception as e:
        print(f"  ❌ Ошибка в методе /files: {type(e).__name__}: {e}")
        print(f"  ⚠️ Частично обработано: {total_count} файлов")
        return total_count, False


def count_files_recursive(token_manager):
    """Полный рекурсивный обход диска"""
    print("  🔄 Метод 2: Полный рекурсивный обход")
    
    file_count = 0
    folder_count = 0
    processed_folders = 0
    errors_count = 0
    skipped_folders = 0
    
    folders_queue = deque([('disk:/', 0)])
    
    start_time = time.time()
    last_progress_time = start_time
    
    while folders_queue:
        current_path, depth = folders_queue.popleft()
        processed_folders += 1
        
        if len(current_path) > MAX_PATH_LENGTH:
            skipped_folders += 1
            continue
        
        if depth > MAX_FOLDER_DEPTH:
            if skipped_folders % 100 == 1:
                print(f"      ⚠️ Пропуск глубоких папок (depth>{MAX_FOLDER_DEPTH}), всего пропущено: {skipped_folders}")
            skipped_folders += 1
            continue
        
        current_time = time.time()
        if processed_folders % 100 == 0 or current_time - last_progress_time > 10:
            elapsed = current_time - start_time
            rate = file_count / elapsed if elapsed > 0 else 0
            stats = token_manager.get_stats()
            print(f"    📁 Папок: {processed_folders}, файлов: {file_count}, "
                  f"в очереди: {len(folders_queue)}, глубина: {depth}, "
                  f"скорость: {rate:.0f} ф/с, токен: {stats['refresh_count']} обновлений")
            last_progress_time = current_time
        
        offset = 0
        
        try:
            while True:
                try:
                    response = get_folder_contents_page(current_path, token_manager, limit=1000, offset=offset)
                    
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        break
                    elif e.response.status_code == 401:
                        print(f"      ❌ 401 UNAUTHORIZED для '{current_path[:80]}...'")
                        errors_count += 1
                        skipped_folders += 1
                        break
                    else:
                        raise
                
                embedded = response.get('_embedded', {})
                items = embedded.get('items', [])
                total = embedded.get('total', 0)
                
                if not items:
                    break
                
                for item in items:
                    item_type = item.get('type')
                    if item_type == 'file':
                        file_count += 1
                    elif item_type == 'dir':
                        folder_count += 1
                        item_path = item.get('path', '')
                        
                        if len(item_path) <= MAX_PATH_LENGTH:
                            folders_queue.append((item_path, depth + 1))
                        else:
                            skipped_folders += 1
                
                offset += len(items)
                
                if offset >= total:
                    break
                    
        except Exception as e:
            errors_count += 1
            if errors_count % 10 == 1:
                print(f"      ❌ Ошибка '{current_path[:80]}...': {type(e).__name__}")
            continue
    
    elapsed = time.time() - start_time
    stats = token_manager.get_stats()
    
    print(f"  ✅ Рекурсивный метод завершён за {elapsed:.1f}с ({elapsed/60:.1f} мин):")
    print(f"     • Файлов: {file_count}")
    print(f"     • Папок обработано: {processed_folders}")
    print(f"     • Папок пропущено: {skipped_folders}")
    print(f"     • Ошибок: {errors_count}")
    print(f"     • Токен обновлялся: {stats['refresh_count']} раз")
    
    return file_count


def count_files(user_id, initial_token, force_recursive=False):
    """Главная функция подсчёта"""
    
    token_manager = TokenManager(user_id, initial_token)
    
    if force_recursive:
        print("  ⚙️ Принудительно используем рекурсивный метод\n")
        count = count_files_recursive(token_manager)
        return count, "recursive_forced"
    
    count, success = count_files_flat(token_manager)
    
    if not success:
        print(f"\n  🔄 Переключаемся на рекурсивный обход...")
        print(f"  💾 Промежуточный результат: {count} файлов\n")
        time.sleep(1)
        
        recursive_count = count_files_recursive(token_manager)
        return recursive_count, "recursive_fallback"
    
    return count, "flat"


if __name__ == '__main__':
    file_path = 'files_count.csv'

    UIDS = [
        '1', #заполнить правильными UID
		'2', #заполнить правильными UID
		'3' #заполнить правильными UID
    ]

    FORCE_RECURSIVE = False
    FORCE_RECURSIVE_UIDS = set([])

    with open(file_path, 'w', encoding='utf-8') as file:
        file.write("user_id;total;status;method\n")

        for uid in UIDS:
            print(f'\n{"="*60}')
            print(f'👤 Пользователь: {uid}')
            print("="*60)

            try:
                user = get_user(uid)
                was_enabled = user['isEnabled']

                if not was_enabled:
                    enable_user(uid)
                    time.sleep(2)

                try:
                    token = get_token(uid)
                    print(f"  ✓ Токен получен")
                    
                    force = FORCE_RECURSIVE or (uid in FORCE_RECURSIVE_UIDS)
                    num, method = count_files(uid, token, force_recursive=force)
                    
                    print(f'\n{"="*60}')
                    print(f'✅ Результат для {uid}:')
                    print(f'   Файлов: {num}')
                    print(f'   Метод: {method}')
                    print("="*60)
                    
                    file.write(f"{uid};{num};OK;{method}\n")
                    file.flush()
                    
                finally:
                    if not was_enabled:
                        time.sleep(1)
                        disable_user(uid)
                        
            except Exception as e:
                import traceback
                error_msg = f"{type(e).__name__}: {str(e)[:100]}"
                print(f'\n{"="*60}')
                print(f'❌ ОШИБКА для пользователя {uid}:')
                print(f'   {error_msg}')
                print("="*60)
                traceback.print_exc()
                
                file.write(f"{uid};0;ERROR;{error_msg}\n")
                file.flush()

    print('\n' + '='*60)
    print('✅ Обработка завершена')
    print('='*60)
