#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт для переноса опубликованных ресурсов Яндекс.Диска в указанную папку
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
import time
import asyncio
import aiohttp
import ssl
import os
import logging
from pathlib import Path
import warnings
from urllib3.exceptions import InsecureRequestWarning

# ================== НАСТРОЙКИ ==================
CLIENT_ID = ''  # ID сервисного приложения
CLIENT_SECRET = ''  # Secret сервисного приложения

# Укажите UID или EMAIL (заполните только одно поле!)
USER_UID = ''  # например: '123456789'
USER_EMAIL = ''  # например: 'user@yandex.ru' vorobevval@test-support360-sso.net

# Папка назначения для переноса файлов
DESTINATION_FOLDER = 'disk:/Перенос шаринга'  # можно изменить на любую папку

# Настройки
LIMIT = 100  # количество ресурсов за один запрос (макс. 100)
MAX_CONCURRENT_OPERATIONS = 20  # максимальное количество одновременных операций
VERIFY_SSL = True  # Изменить на False, если есть проблемы с SSL сертификатами

# Папка для логов (к ней добавится время запуска)
LOGS_FOLDER_BASE = 'transfer_logs'
# ===============================================

if not VERIFY_SSL:
    warnings.filterwarnings('ignore', category=InsecureRequestWarning)


class TransferLogger:
    
    def __init__(self, logs_folder):
        self.logs_folder = logs_folder
        self.timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        Path(logs_folder).mkdir(parents=True, exist_ok=True)
        
        self.main_log = os.path.join(logs_folder, f'transfer_{self.timestamp}.log')
        
        self.failed_transfers_log = os.path.join(logs_folder, f'failed_transfers_{self.timestamp}.txt')
        
        self.logger = logging.getLogger('TransferLogger')
        self.logger.setLevel(logging.INFO)
        
        self.logger.handlers = []
        
        fh = logging.FileHandler(self.main_log, encoding='utf-8')
        fh.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        fh.setFormatter(formatter)
        
        self.logger.addHandler(fh)
        
        self.failed_folders = []
        self.failed_files_from_folders = []
        self.failed_standalone_files = []
        
    def info(self, message):
        self.logger.info(message)
        
    def error(self, message):
        self.logger.error(message)
        
    def warning(self, message):
        self.logger.warning(message)
        
    def add_failed_folder(self, folder_path, reason):
        self.failed_folders.append((folder_path, reason))
        
    def add_failed_file_from_folder(self, folder_path, file_info, reason):
        self.failed_files_from_folders.append((folder_path, file_info, reason))
        
    def add_failed_standalone_file(self, file_info, reason):
        self.failed_standalone_files.append((file_info, reason))
        
    def save_failed_transfers_report(self):
        has_failures = (self.failed_folders or 
                       self.failed_files_from_folders or 
                       self.failed_standalone_files)
        
        if not has_failures:
            return
            
        with open(self.failed_transfers_log, 'w', encoding='utf-8') as f:
            f.write('='*100 + '\n')
            f.write('ОТЧЁТ О НЕУДАЧНЫХ ПЕРЕНОСАХ\n')
            f.write(f'Дата: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n')
            f.write('='*100 + '\n\n')
            
            if self.failed_folders:
                f.write('╔' + '═'*98 + '╗\n')
                f.write('║ 📁 ПАПКИ, КОТОРЫЕ НЕ УДАЛОСЬ ПЕРЕНЕСТИ' + ' '*56 + '║\n')
                f.write('╚' + '═'*98 + '╝\n\n')
                
                for i, (folder_path, reason) in enumerate(self.failed_folders, 1):
                    f.write(f'{i}. Папка: {folder_path}\n')
                    f.write(f'   ❌ Причина: {reason}\n')
                    f.write('-'*100 + '\n\n')
            
            if self.failed_files_from_folders:
                f.write('╔' + '═'*98 + '╗\n')
                f.write('║ 📄 ФАЙЛЫ ИЗ НЕПЕРЕНЕСЕННЫХ ПАПОК' + ' '*63 + '║\n')
                f.write('╚' + '═'*98 + '╝\n\n')
                
                current_folder = None
                file_count = 1
                
                for folder_path, file_info, reason in self.failed_files_from_folders:
                    if folder_path != current_folder:
                        if current_folder is not None:
                            f.write('\n')
                        f.write(f'📁 Родительская папка: {folder_path}\n')
                        f.write('─'*100 + '\n')
                        current_folder = folder_path
                    
                    f.write(f'   {file_count}. Файл: {file_info.get("name")}\n')
                    f.write(f'      Путь: {file_info.get("path")}\n')
                    
                    size = file_info.get("size", 0)
                    size_mb = size / (1024 * 1024) if size else 0
                    f.write(f'      Размер: {size_mb:.2f} MB\n')
                    
                    f.write(f'      ❌ Причина: {reason}\n')
                    f.write('\n')
                    file_count += 1
            
            if self.failed_standalone_files:
                f.write('╔' + '═'*98 + '╗\n')
                f.write('║ 📄 ОТДЕЛЬНЫЕ ФАЙЛЫ, КОТОРЫЕ НЕ УДАЛОСЬ ПЕРЕНЕСТИ' + ' '*47 + '║\n')
                f.write('╚' + '═'*98 + '╝\n\n')
                
                for i, (file_info, reason) in enumerate(self.failed_standalone_files, 1):
                    f.write(f'{i}. Файл: {file_info.get("name")}\n')
                    f.write(f'   Путь: {file_info.get("path")}\n')
                    
                    size = file_info.get("size", 0)
                    size_mb = size / (1024 * 1024) if size else 0
                    f.write(f'   Размер: {size_mb:.2f} MB\n')
                    
                    f.write(f'   ❌ Причина: {reason}\n')
                    f.write('-'*100 + '\n\n')
            
            f.write('='*100 + '\n')
            f.write('ИТОГОВАЯ СТАТИСТИКА ОШИБОК\n')
            f.write('='*100 + '\n')
            f.write(f'Папок не перенесено: {len(self.failed_folders)}\n')
            f.write(f'Файлов из неперенесенных папок не перенесено: {len(self.failed_files_from_folders)}\n')
            f.write(f'Отдельных файлов не перенесено: {len(self.failed_standalone_files)}\n')
            f.write(f'Всего ошибок: {len(self.failed_folders) + len(self.failed_files_from_folders) + len(self.failed_standalone_files)}\n')
            f.write('='*100 + '\n')
                
        print(f'\n⚠️  Создан файл с отчётом о неудачных переносах: {self.failed_transfers_log}')


transfer_logger = None


def get_user_token_by_uid(uid):
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'subject_token': uid,
        'subject_token_type': 'urn:yandex:params:oauth:token-type:uid'
    }
    
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        response = session.post(url, data=data, headers=headers, verify=VERIFY_SSL)
        print(f'{datetime.now()} | get_user_token_by_uid | status: {response.status_code}')
        transfer_logger.info(f'get_user_token_by_uid | status: {response.status_code}')
        
        if response.status_code != 200:
            print(f'Ошибка получения токена по UID: {response.text}')
            transfer_logger.error(f'Ошибка получения токена по UID: {response.text}')
            return None, None
        
        user_token = response.json()['access_token']
        ttl = int(response.json()['expires_in']) - 100
        expiry_time = datetime.now() + timedelta(seconds=ttl)
        return user_token, expiry_time
    except Exception as e:
        print(f'Ошибка при получении токена по UID: {e}')
        transfer_logger.error(f'Ошибка при получении токена по UID: {e}')
        return None, None


def get_user_token_by_email(email):
    url = 'https://oauth.yandex.ru/token'
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    data = {
        'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET,
        'subject_token': email,
        'subject_token_type': 'urn:yandex:params:oauth:token-type:email'
    }
    
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        response = session.post(url, data=data, headers=headers, verify=VERIFY_SSL)
        print(f'{datetime.now()} | get_user_token_by_email | status: {response.status_code}')
        transfer_logger.info(f'get_user_token_by_email | status: {response.status_code}')
        
        if response.status_code != 200:
            print(f'Ошибка получения токена по EMAIL: {response.text}')
            transfer_logger.error(f'Ошибка получения токена по EMAIL: {response.text}')
            return None, None
        
        user_token = response.json()['access_token']
        ttl = int(response.json()['expires_in']) - 100
        expiry_time = datetime.now() + timedelta(seconds=ttl)
        return user_token, expiry_time
    except Exception as e:
        print(f'Ошибка при получении токена по EMAIL: {e}')
        transfer_logger.error(f'Ошибка при получении токена по EMAIL: {e}')
        return None, None


def get_token():
    
    if USER_UID:
        print(f'{datetime.now()} | Используем UID: {USER_UID}')
        transfer_logger.info(f'Используем UID: {USER_UID}')
        return get_user_token_by_uid(USER_UID)
    elif USER_EMAIL:
        print(f'{datetime.now()} | Используем EMAIL: {USER_EMAIL}')
        transfer_logger.info(f'Используем EMAIL: {USER_EMAIL}')
        return get_user_token_by_email(USER_EMAIL)
    else:
        print('ОШИБКА: Необходимо указать USER_UID или USER_EMAIL')
        transfer_logger.error('Не указан USER_UID или USER_EMAIL')
        return None, None


def get_folder_contents(token, folder_path):
    url = 'https://cloud-api.yandex.net/v1/disk/resources'
    headers = {'Authorization': f'OAuth {token}'}
    params = {
        'path': folder_path,
        'limit': 1000
    }
    
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        response = session.get(url, headers=headers, params=params, verify=VERIFY_SSL)
        
        if response.status_code == 200:
            data = response.json()
            return data.get('_embedded', {}).get('items', [])
        else:
            return []
    except Exception as e:
        print(f'Ошибка при получении содержимого папки {folder_path}: {e}')
        transfer_logger.error(f'Ошибка при получении содержимого папки {folder_path}: {e}')
        return []


def get_published_resources(token):
    url = 'https://cloud-api.yandex.net/v1/disk/resources/public'
    headers = {'Authorization': f'OAuth {token}'}
    
    all_resources = []
    offset = 0
    
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    while True:
        params = {
            'limit': LIMIT,
            'offset': offset
        }
        
        try:
            response = session.get(url, headers=headers, params=params, verify=VERIFY_SSL)
            print(f'{datetime.now()} | get_published_resources | offset: {offset} | status: {response.status_code}')
            transfer_logger.info(f'get_published_resources | offset: {offset} | status: {response.status_code}')
            
            if response.status_code != 200:
                print(f'Ошибка получения опубликованных ресурсов: {response.text}')
                transfer_logger.error(f'Ошибка получения опубликованных ресурсов: {response.text}')
                break
                
            data = response.json()
            items = data.get('items', [])
            
            if not items:
                break
                
            all_resources.extend(items)
            offset += LIMIT
            
            if len(items) < LIMIT:
                break
                
            time.sleep(0.1)
            
        except Exception as e:
            print(f'Ошибка при получении списка ресурсов: {e}')
            transfer_logger.error(f'Ошибка при получении списка ресурсов: {e}')
            break
    
    print(f'{datetime.now()} | Всего найдено опубликованных ресурсов: {len(all_resources)}')
    transfer_logger.info(f'Всего найдено опубликованных ресурсов: {len(all_resources)}')
    return all_resources


def display_resources_tree(token, resources):
    print(f'\n{"="*80}')
    print(f'СПИСОК РЕСУРСОВ ДЛЯ ПЕРЕНОСА')
    print(f'{"="*80}\n')
    
    transfer_logger.info('='*80)
    transfer_logger.info('СПИСОК РЕСУРСОВ ДЛЯ ПЕРЕНОСА')
    transfer_logger.info('='*80)
    
    folders = [r for r in resources if r.get('type') == 'dir']
    files = [r for r in resources if r.get('type') == 'file']
    
    folder_files = {}
    
    if folders:
        print(f'📁 ПАПКИ ({len(folders)}):\n')
        transfer_logger.info(f'ПАПКИ ({len(folders)}):')
        
        for i, folder in enumerate(folders, 1):
            folder_name = folder.get('name')
            folder_path = folder.get('path')
            print(f'{i}. 📁 {folder_name}')
            print(f'   Путь: {folder_path}')
            transfer_logger.info(f'{i}. Папка: {folder_name} (путь: {folder_path})')
            
            contents = get_folder_contents(token, folder_path)
            if contents:
                print(f'   Содержит файлов: {len(contents)}')
                transfer_logger.info(f'   Содержит файлов: {len(contents)}')
                folder_files[folder_path] = contents
                
                for j, item in enumerate(contents[:5], 1):
                    item_type = '📄' if item.get('type') == 'file' else '📁'
                    print(f'      {item_type} {item.get("name")}')
                    transfer_logger.info(f'      - {item.get("name")}')
                    
                if len(contents) > 5:
                    remaining = len(contents) - 5
                    print(f'      ... и ещё {remaining} файлов')
                    transfer_logger.info(f'      ... и ещё {remaining} файлов')
            else:
                print(f'   Папка пуста')
                transfer_logger.info(f'   Папка пуста')
                
            print()
    
    if files:
        print(f'\n📄 ОТДЕЛЬНЫЕ ФАЙЛЫ ({len(files)}):\n')
        transfer_logger.info(f'ОТДЕЛЬНЫЕ ФАЙЛЫ ({len(files)}):')
        
        for i, file in enumerate(files, 1):
            file_name = file.get('name')
            file_path = file.get('path')
            file_size = file.get('size', 0)
            size_mb = file_size / (1024 * 1024) if file_size else 0
            
            print(f'{i}. 📄 {file_name} ({size_mb:.2f} MB)')
            print(f'   Путь: {file_path}')
            transfer_logger.info(f'{i}. Файл: {file_name} (размер: {size_mb:.2f} MB, путь: {file_path})')
    
    print(f'\n{"="*80}')
    print(f'ИТОГО: {len(folders)} папок, {len(files)} отдельных файлов')
    print(f'{"="*80}\n')
    
    transfer_logger.info('='*80)
    transfer_logger.info(f'ИТОГО: {len(folders)} папок, {len(files)} отдельных файлов')
    transfer_logger.info('='*80)
    
    return folders, files, folder_files


def create_folder_if_not_exists(token, folder_path):
    url = 'https://cloud-api.yandex.net/v1/disk/resources'
    headers = {'Authorization': f'OAuth {token}'}
    params = {'path': folder_path}
    
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    
    try:
        response = session.get(url, headers=headers, params=params, verify=VERIFY_SSL)
        
        if response.status_code == 200:
            print(f'{datetime.now()} | Папка {folder_path} уже существует')
            transfer_logger.info(f'Папка {folder_path} уже существует')
            return True
        elif response.status_code == 404:
            response = session.put(url, headers=headers, params=params, verify=VERIFY_SSL)
            if response.status_code == 201:
                print(f'{datetime.now()} | Папка {folder_path} успешно создана')
                transfer_logger.info(f'Папка {folder_path} успешно создана')
                return True
            else:
                print(f'{datetime.now()} | Ошибка создания папки: {response.text}')
                transfer_logger.error(f'Ошибка создания папки: {response.text}')
                return False
        else:
            print(f'{datetime.now()} | Ошибка проверки папки: {response.text}')
            transfer_logger.error(f'Ошибка проверки папки: {response.text}')
            return False
    except Exception as e:
        print(f'Ошибка при создании папки: {e}')
        transfer_logger.error(f'Ошибка при создании папки: {e}')
        return False


def parse_error_reason(status, text):
    if status == 409:
        return "Конфликт: файл/папка уже существует в месте назначения или конфликт имён"
    elif status == 403:
        return "Доступ запрещён: недостаточно прав для операции"
    elif status == 404:
        return "Не найдено: исходный ресурс не существует"
    elif status == 507:
        return "Недостаточно места на диске"
    elif status == 429:
        return "Слишком много запросов: превышен лимит API"
    elif status >= 500:
        return f"Ошибка сервера Яндекс.Диска (код {status})"
    else:
        try:
            import json
            data = json.loads(text)
            if 'message' in data:
                return f"Код {status}: {data['message']}"
            elif 'error' in data:
                return f"Код {status}: {data['error']}"
        except:
            pass
        return f"Неизвестная ошибка (код {status})"


async def move_resource(session, token, source_path, destination_folder, resource_name, 
                       semaphore, resource_type='file', resource_info=None):
    async with semaphore:
        url = 'https://cloud-api.yandex.net/v1/disk/resources/move'
        headers = {'Authorization': f'OAuth {token}'}
        
        destination_path = f'{destination_folder}/{resource_name}'
        
        params = {
            'from': source_path,
            'path': destination_path,
            'overwrite': 'false'
        }
        
        try:
            async with session.post(url, headers=headers, params=params, ssl=False if not VERIFY_SSL else None) as response:
                status = response.status
                text = await response.text()
                
                if status == 201:
                    icon = '📁' if resource_type == 'dir' else '📄'
                    msg = f'{icon} Перенесён: {source_path} -> {destination_path}'
                    print(f'{datetime.now()} | ✓ {msg}')
                    transfer_logger.info(f'SUCCESS: {msg}')
                    return True, None
                elif status == 202:
                    data = await response.json()
                    operation_url = data.get('href')
                    
                    if operation_url:
                        success = await wait_for_operation(session, token, operation_url)
                        if success:
                            icon = '📁' if resource_type == 'dir' else '📄'
                            msg = f'{icon} Перенесён (async): {source_path} -> {destination_path}'
                            print(f'{datetime.now()} | ✓ {msg}')
                            transfer_logger.info(f'SUCCESS (async): {msg}')
                            return True, None
                        else:
                            reason = "Асинхронная операция завершилась с ошибкой"
                            msg = f'Ошибка переноса (async): {source_path} - {reason}'
                            print(f'{datetime.now()} | ✗ {msg}')
                            transfer_logger.error(f'FAILED (async): {msg}')
                            return False, reason
                else:
                    reason = parse_error_reason(status, text)
                    msg = f'Ошибка переноса: {source_path} - {reason}'
                    print(f'{datetime.now()} | ✗ {msg}')
                    transfer_logger.error(f'FAILED: {msg}')
                    return False, reason
                    
        except Exception as e:
            reason = f"Исключение: {str(e)}"
            msg = f'Исключение при переносе {source_path}: {reason}'
            print(f'{datetime.now()} | ✗ {msg}')
            transfer_logger.error(f'EXCEPTION: {msg}')
            return False, reason


async def wait_for_operation(session, token, operation_url, max_attempts=30):
    headers = {'Authorization': f'OAuth {token}'}
    
    for attempt in range(max_attempts):
        try:
            async with session.get(operation_url, headers=headers, ssl=False if not VERIFY_SSL else None) as response:
                if response.status == 200:
                    data = await response.json()
                    status = data.get('status')
                    
                    if status == 'success':
                        return True
                    elif status == 'failed':
                        return False
                    
            await asyncio.sleep(1)
            
        except Exception as e:
            print(f'Ошибка при проверке статуса операции: {e}')
            transfer_logger.error(f'Ошибка при проверке статуса операции: {e}')
            return False
    
    print(f'Превышено время ожидания операции: {operation_url}')
    transfer_logger.error(f'Превышено время ожидания операции: {operation_url}')
    return False


async def move_resources_async(token, folders, files, folder_files, destination_folder):
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_OPERATIONS)
    
    if VERIFY_SSL:
        ssl_context = ssl.create_default_context()
    else:
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
    
    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_OPERATIONS, ssl=ssl_context)
    timeout = aiohttp.ClientTimeout(total=300)
    
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        if folders:
            print(f'\n{"="*80}')
            print(f'ЭТАП 1: ПЕРЕНОС ПАПОК ({len(folders)} шт.)')
            print(f'{"="*80}\n')
            transfer_logger.info('='*80)
            transfer_logger.info(f'ЭТАП 1: ПЕРЕНОС ПАПОК ({len(folders)} шт.)')
            transfer_logger.info('='*80)
            
            folder_tasks = []
            for folder in folders:
                source_path = folder.get('path')
                resource_name = folder.get('name')
                
                if source_path and resource_name:
                    task = move_resource(session, token, source_path, destination_folder, 
                                       resource_name, semaphore, 'dir', folder)
                    folder_tasks.append((task, folder))
            
            if folder_tasks:
                tasks, folder_objects = zip(*folder_tasks)
                results = await asyncio.gather(*tasks)
                
                files_to_transfer_separately = []
                
                for i, (result_tuple, folder) in enumerate(zip(results, folder_objects)):
                    success, reason = result_tuple
                    folder_path = folder.get('path')
                    
                    if not success:
                        transfer_logger.add_failed_folder(folder_path, reason)
                        
                        if folder_path in folder_files:
                            for file_info in folder_files[folder_path]:
                                files_to_transfer_separately.append((folder_path, file_info))
                
                success_count = sum(1 for r, _ in results if r)
                print(f'\n{datetime.now()} | Результаты переноса папок:')
                print(f'  ✓ Успешно: {success_count}')
                print(f'  ✗ Ошибок: {len(results) - success_count}')
                print(f'  Всего: {len(results)}')
                
                transfer_logger.info(f'Результаты переноса папок: Успешно={success_count}, Ошибок={len(results) - success_count}, Всего={len(results)}')
                
                if files_to_transfer_separately:
                    print(f'\n{"="*80}')
                    print(f'ЭТАП 1.5: ПЕРЕНОС ФАЙЛОВ ИЗ НЕПЕРЕНЕСЕННЫХ ПАПОК ({len(files_to_transfer_separately)} файлов)')
                    print(f'{"="*80}\n')
                    transfer_logger.info('='*80)
                    transfer_logger.info(f'ЭТАП 1.5: ПЕРЕНОС ФАЙЛОВ ИЗ НЕПЕРЕНЕСЕННЫХ ПАПОК ({len(files_to_transfer_separately)} файлов)')
                    transfer_logger.info('='*80)
                    
                    failed_folder_file_tasks = []
                    for folder_path, file_info in files_to_transfer_separately:
                        source_path = file_info.get('path')
                        resource_name = file_info.get('name')
                        
                        if source_path and resource_name:
                            task = move_resource(session, token, source_path, destination_folder, 
                                               resource_name, semaphore, 'file', file_info)
                            failed_folder_file_tasks.append((task, folder_path, file_info))
                    
                    if failed_folder_file_tasks:
                        tasks_only = [t for t, _, _ in failed_folder_file_tasks]
                        ff_results = await asyncio.gather(*tasks_only)
                        
                        for i, (result_tuple, folder_path, file_info) in enumerate(zip(ff_results, 
                                                                                       [fp for _, fp, _ in failed_folder_file_tasks],
                                                                                       [fi for _, _, fi in failed_folder_file_tasks])):
                            success, reason = result_tuple
                            if not success:
                                transfer_logger.add_failed_file_from_folder(folder_path, file_info, reason)
                        
                        ff_success = sum(1 for r, _ in ff_results if r)
                        
                        print(f'\n{datetime.now()} | Результаты переноса файлов из неперенесенных папок:')
                        print(f'  ✓ Успешно: {ff_success}')
                        print(f'  ✗ Ошибок: {len(ff_results) - ff_success}')
                        print(f'  Всего: {len(ff_results)}')
                        
                        transfer_logger.info(f'Результаты переноса файлов из неперенесенных папок: Успешно={ff_success}, Ошибок={len(ff_results) - ff_success}, Всего={len(ff_results)}')
        
        if files:
            print(f'\n{"="*80}')
            print(f'ЭТАП 2: ПЕРЕНОС ОТДЕЛЬНЫХ ФАЙЛОВ ({len(files)} шт.)')
            print(f'{"="*80}\n')
            transfer_logger.info('='*80)
            transfer_logger.info(f'ЭТАП 2: ПЕРЕНОС ОТДЕЛЬНЫХ ФАЙЛОВ ({len(files)} шт.)')
            transfer_logger.info('='*80)
            
            file_tasks = []
            for file in files:
                source_path = file.get('path')
                resource_name = file.get('name')
                
                if source_path and resource_name:
                    task = move_resource(session, token, source_path, destination_folder, 
                                       resource_name, semaphore, 'file', file)
                    file_tasks.append((task, file))
            
            if file_tasks:
                tasks, file_objects = zip(*file_tasks)
                results = await asyncio.gather(*tasks)
                
                for result_tuple, file_info in zip(results, file_objects):
                    success, reason = result_tuple
                    if not success:
                        transfer_logger.add_failed_standalone_file(file_info, reason)
                
                success_count = sum(1 for r, _ in results if r)
                print(f'\n{datetime.now()} | Результаты переноса файлов:')
                print(f'  ✓ Успешно: {success_count}')
                print(f'  ✗ Ошибок: {len(results) - success_count}')
                print(f'  Всего: {len(results)}')
                
                transfer_logger.info(f'Результаты переноса файлов: Успешно={success_count}, Ошибок={len(results) - success_count}, Всего={len(results)}')


def main():
    global transfer_logger
    
    print(f'\n{"="*60}')
    print(f'Скрипт переноса опубликованных ресурсов Яндекс.Диска')
    print(f'{"="*60}\n')
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logs_folder = f'{LOGS_FOLDER_BASE}_{timestamp}'
    
    transfer_logger = TransferLogger(logs_folder)
    transfer_logger.info('='*80)
    transfer_logger.info('НАЧАЛО РАБОТЫ СКРИПТА')
    transfer_logger.info('='*80)
    
    if not CLIENT_ID or not CLIENT_SECRET:
        print('ОШИБКА: Необходимо указать CLIENT_ID и CLIENT_SECRET')
        transfer_logger.error('Не указан CLIENT_ID или CLIENT_SECRET')
        return
    
    if not USER_UID and not USER_EMAIL:
        print('ОШИБКА: Необходимо указать USER_UID или USER_EMAIL')
        transfer_logger.error('Не указан USER_UID или USER_EMAIL')
        return
    
    if not VERIFY_SSL:
        print('⚠️  ВНИМАНИЕ: Проверка SSL сертификатов отключена\n')
        transfer_logger.warning('Проверка SSL сертификатов отключена')
    
    token, expiry_time = get_token()
    if not token:
        print('ОШИБКА: Не удалось получить токен доступа')
        transfer_logger.error('Не удалось получить токен доступа')
        return
    
    print(f'{datetime.now()} | Токен получен успешно (действителен до {expiry_time})\n')
    transfer_logger.info(f'Токен получен успешно (действителен до {expiry_time})')
    
    if not create_folder_if_not_exists(token, DESTINATION_FOLDER):
        print('ОШИБКА: Не удалось создать папку назначения')
        transfer_logger.error('Не удалось создать папку назначения')
        return
    
    print()
    
    resources = get_published_resources(token)
    
    if not resources:
        print(f'\n{datetime.now()} | Опубликованных ресурсов не найдено или произошла ошибка')
        transfer_logger.warning('Опубликованных ресурсов не найдено')
        return
    
    folders, files, folder_files = display_resources_tree(token, resources)
    
    total = len(folders) + len(files)
    answer = input(f'\nПеренести {total} ресурсов ({len(folders)} папок, {len(files)} файлов) в папку "{DESTINATION_FOLDER}"? (yes/no): ')
    if answer.lower() not in ['yes', 'y', 'да', 'д']:
        print('Операция отменена')
        transfer_logger.info('Операция отменена пользователем')
        return
    
    transfer_logger.info(f'Пользователь подтвердил перенос {total} ресурсов')
    print()
    
    asyncio.run(move_resources_async(token, folders, files, folder_files, DESTINATION_FOLDER))
    
    transfer_logger.save_failed_transfers_report()
    
    print(f'\n{"="*80}')
    print(f'СКРИПТ ЗАВЕРШЁН')
    print(f'{"="*80}')
    print(f'📁 Папка с логами: {logs_folder}')
    print(f'📋 Лог-файл: {transfer_logger.main_log}')
    if (transfer_logger.failed_folders or 
        transfer_logger.failed_files_from_folders or 
        transfer_logger.failed_standalone_files):
        print(f'⚠️  Файл с отчётом об ошибках: {transfer_logger.failed_transfers_log}')
    print(f'{"="*80}\n')
    
    transfer_logger.info('='*80)
    transfer_logger.info('СКРИПТ ЗАВЕРШЁН')
    transfer_logger.info('='*80)


if __name__ == '__main__':
    main()
