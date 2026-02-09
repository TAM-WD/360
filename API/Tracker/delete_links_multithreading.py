# Скрипт предназначен для массового удаления связей, полученных из ответа метода getIssueLinks в браузере

import os
import requests
import json
from typing import List, Dict, Any
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from threading import Lock

ORGID = '' # ID организации
TOKEN = '' # OAuth токен с правами на запись в Трекер
ORG_TYPE = 'X-Org-Id' # или X-Cloud-Org-ID - тип организации (Яндекс 360 / Cloud Organization)

FILE_PATH = '' # полный путь к файлу .json с ответом на запрос метода getIssueLinks в браузере
FILTER = '' # Название очереди, связи с которой нужно убрать (например, TRASH)
ISSUE_KEY = '' # Ключ задачи, связи которой нужно удалить (Например, EXAMPLE-42)

SAVE_TO_FILE = True # Заменить на True , чтобы выгрузить связи в файл и проверить, какие будут удалены
DELETE_LINKS = False # Заменить на True для запуска удаления связей

# Настройки многопоточности
MAX_RPS = 10 # Максимальное количество запросов в секунду
MAX_WORKERS = 5 # Максимальное количество одновременных потоков

# Глобальный счетчик для контроля RPS
request_times = []
request_lock = Lock()


def rate_limit():
    with request_lock:
        current_time = time.time()
        # Удаляем запросы старше 1 секунды
        global request_times
        request_times = [t for t in request_times if current_time - t < 1.0]
        
        # Если достигли лимита, ждем
        if len(request_times) >= MAX_RPS:
            sleep_time = 1.0 - (current_time - request_times[0])
            if sleep_time > 0:
                time.sleep(sleep_time)
            # Очищаем старые запросы после ожидания
            current_time = time.time()
            request_times = [t for t in request_times if current_time - t < 1.0]
        
        # Добавляем текущий запрос
        request_times.append(time.time())


def delete_link(issue_key: str, link_id: str, link_key: str = None):
    rate_limit()  # Применяем ограничение RPS
    url = f'https://api.tracker.yandex.net/v3/issues/{issue_key}/links/{link_id}'
    headers = {
        ORG_TYPE: ORGID,
        'Authorization': f"OAuth {TOKEN}"
    }
    try:
        response = requests.delete(url, headers=headers, timeout=10)
        success = response.status_code in [200, 204]
        return (link_key, link_id, response.status_code, success)
    except Exception as e:
        print(f"Ошибка при удалении связи {link_key} ({link_id}): {e}")
        return (link_key, link_id, None, False)

def delete_links_multithreaded(issue_key: str, links_list: List[Dict[str, Any]]):
    stats = {
        'total': len(links_list),
        'success': 0,
        'failed': 0,
        'results': []
    }
    
    if not links_list:
        print("Нет связей для удаления")
        return stats
    
    print(f"\nНачинаем удаление {len(links_list)} связей...")
    print(f"Настройки: MAX_RPS={MAX_RPS}, MAX_WORKERS={MAX_WORKERS}")
    
    start_time = time.time()
    
    # Используем ThreadPoolExecutor для параллельного выполнения
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Создаем задачи для каждой связи
        future_to_link = {
            executor.submit(
                delete_link, 
                issue_key, 
                link['id'], 
                link['key']
            ): link for link in links_list
        }
        
        # Обрабатываем результаты по мере выполнения
        for future in as_completed(future_to_link):
            link_key, link_id, status_code, success = future.result()
            
            if success:
                stats['success'] += 1
                print(f"✓ Удалена связь: {link_key} (ID: {link_id}), status: {status_code}")
            else:
                stats['failed'] += 1
                print(f"✗ Ошибка удаления связи: {link_key} (ID: {link_id}), status: {status_code}")
            
            stats['results'].append({
                'key': link_key,
                'id': link_id,
                'status': status_code,
                'success': success
            })
    
    elapsed_time = time.time() - start_time
    
    print(f"\n{'='*60}")
    print(f"Удаление завершено за {elapsed_time:.2f} секунд")
    print(f"Успешно удалено: {stats['success']}/{stats['total']}")
    print(f"Ошибок: {stats['failed']}/{stats['total']}")
    print(f"Средний RPS: {stats['total']/elapsed_time:.2f}")
    print(f"{'='*60}\n")
    
    return stats


def save_deletion_stats(stats: dict):
    """Сохраняет статистику удаления в CSV файл"""
    dir = os.path.dirname(__file__)
    output_file = os.path.abspath(f'{dir}/deletion_results.csv')
    
    with open(output_file, 'w', encoding='utf-8', newline='') as file:
        fieldnames = ['key', 'id', 'status', 'success']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(stats['results'])
    
    print(f"Статистика удаления сохранена в {output_file}")


def parse_file(filepath: str = None, filter: str = None):
    to_delete_list = []
    path = os.path.abspath(filepath)
    with open(path, "r", encoding='utf-8') as file:
        data = json.load(file)
    links = data.get('issueLinksInternal').get("data").get('links', {})
    for relationship_type, issues in links.items():
        if not isinstance(issues, list):
            continue
        for issue in issues:
            if 'key' in issue and filter in issue['key']:
                result_item = {
                    'id': issue.get('id'),
                    'key': issue.get('key')
                }
                to_delete_list.append(result_item)
    return to_delete_list


def save_results_to_csv(results: List[Dict[str, Any]] = None):
    dir = os.path.dirname(__file__)
    output_file = os.path.abspath(f'{dir}/links_to_delete.csv')
    with open(output_file, 'w', encoding='utf-8', newline='') as file:
        fieldnames = ['id', 'key']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"Результаты сохранены в {output_file}")
    return


if __name__ == "__main__":
    links_list = parse_file(FILE_PATH, FILTER)
    print(f"Всего найдено {len(links_list)} задач на удаление")

    if SAVE_TO_FILE == True:
        save_results_to_csv(links_list)
    else:
        print("File skipped")

    if DELETE_LINKS == True:
        # Используем многопоточное удаление
        stats = delete_links_multithreaded(ISSUE_KEY, links_list)
        
        # Сохраняем статистику удаления
        save_deletion_stats(stats)
    else:
        print("Deletion skipped")
