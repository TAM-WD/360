### Скрипт для logout заблокированных пользователей в организации
# используется API Яндекс 360 для бизнеса

import os
import csv
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime
import time

PERPAGE = 1000
TOKEN = ''  # токен OAuth приложения администратора организации, с правами directory:read_users и security:domain_sessions_write
ORGID = ''  # ID организации

def api360_get_users(orgid, page):
    """Получение списка пользователей организации"""
    url = f'https://api360.yandex.net/directory/v1/org/{orgid}/users'
    params = {
        'page': page,
        'perPage': PERPAGE
    }
    headers = {'Authorization': f"OAuth {TOKEN}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    request = session.get(url, params=params, headers=headers)
    response = request.json()
    return response['users'], response['pages']

def api360_logout_user(orgid, userid):
    """Выход из аккаунта пользователя на всех устройствах"""
    url = f'https://api360.yandex.net/security/v1/org/{orgid}/domain_sessions/users/{userid}/logout'
    headers = {'Authorization': f"OAuth {TOKEN}"}
    retries = Retry(total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    try:
        request = session.put(url, headers=headers)
        if request.status_code == 200:
            return True, "Выполнен"
        else:
            return False, f"Ошибка (код: {request.status_code})"
    except Exception as e:
        return False, f"Ошибка: {str(e)}"

def get_blocked_users(orgid):
    """Получение списка всех заблокированных пользователей"""
    blocked_users = []
    skipped_yandex_users = 0
    page = 1
    total_pages = 1
    
    print(f"Начинаем поиск заблокированных пользователей в организации {orgid}...")
    
    while page <= total_pages:
        users, total_pages = api360_get_users(orgid, page)
        
        for user in users:
            # Проверяем, заблокирован ли пользователь (isEnabled = False)
            if not user.get('isEnabled', True):
                user_email = user.get('email', '')
                
                # Пропускаем пользователей с доменом @yandex.ru
                if user_email.endswith('@yandex.ru'):
                    skipped_yandex_users += 1
                    print(f"Пропущен пользователь с доменом @yandex.ru: {user_email} (ID: {user.get('id')})")
                    continue
                
                first_name = user.get('name', {}).get('first', '')
                last_name = user.get('name', {}).get('last', '')
                middle_name = user.get('name', {}).get('middle', '')
                
                # Формируем полное имя
                full_name_parts = [first_name, middle_name, last_name]
                full_name = ' '.join([part for part in full_name_parts if part]).strip()
                
                blocked_users.append({
                    'id': user.get('id'),
                    'nickname': user.get('nickname'),
                    'email': user_email,
                    'full_name': full_name if full_name else user.get('nickname', 'Имя не указано')
                })
                print(f"Найден заблокированный пользователь: {user_email} (ID: {user.get('id')})")
        
        print(f"Обработана страница {page} из {total_pages}")
        page += 1
    
    if skipped_yandex_users > 0:
        print(f"\nПропущено пользователей с доменом @yandex.ru: {skipped_yandex_users}")
    
    return blocked_users

if __name__ == '__main__':
    start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    # Создаем директорию для результатов, если её нет
    dir_path = os.path.dirname(__file__)
    results_dir = os.path.abspath(f'{dir_path}/results')
    os.makedirs(results_dir, exist_ok=True)
    
    log_file = os.path.abspath(f'{results_dir}/logout_blocked_users_{ORGID}_{start_time}.log')
    csv_file = os.path.abspath(f'{results_dir}/logout_blocked_users_{ORGID}_{start_time}.csv')
    
    print("=" * 60)
    print(f"Запуск скрипта: {start_time}")
    print("=" * 60)
    
    # Получаем список заблокированных пользователей
    blocked_users = get_blocked_users(ORGID)
    
    print("\n" + "=" * 60)
    print(f"Найдено заблокированных пользователей для logout: {len(blocked_users)}")
    print("=" * 60 + "\n")
    
    if not blocked_users:
        print("Заблокированные пользователи не найдены. Скрипт завершен.")
        exit(0)
    
    # Выполняем logout для каждого заблокированного пользователя
    success_count = 0
    failed_count = 0
    
    # Открываем CSV файл для записи
    with open(csv_file, 'w', encoding='utf-8-sig', newline='') as csvfile:
        fieldnames = ['Email', 'Имя', 'ID', 'Logout']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        
        # Открываем лог файл
        with open(log_file, 'w', encoding='utf-8') as log:
            log.write(f"Лог выполнения logout для заблокированных пользователей\n")
            log.write(f"Дата и время: {start_time}\n")
            log.write(f"Организация ID: {ORGID}\n")
            log.write(f"Всего заблокированных пользователей для logout: {len(blocked_users)}\n")
            log.write(f"Примечание: Пользователи с доменом @yandex.ru исключены\n")
            log.write("=" * 80 + "\n\n")
            
            for index, user in enumerate(blocked_users, 1):
                user_id = user['id']
                user_email = user['email']
                user_name = user['full_name']
                
                print(f"[{index}/{len(blocked_users)}] Выполняем logout для пользователя: {user_email} (ID: {user_id})")
                
                success, message = api360_logout_user(ORGID, user_id)
                
                # Записываем в CSV
                writer.writerow({
                    'Email': user_email,
                    'Имя': user_name,
                    'ID': user_id,
                    'Logout': message
                })
                
                if success:
                    success_count += 1
                    log_message = f"✓ SUCCESS | {user_email} | {user_name} | ID: {user_id}"
                    print(f"  ✓ Успешно")
                else:
                    failed_count += 1
                    log_message = f"✗ FAILED  | {user_email} | {user_name} | ID: {user_id} | Error: {message}"
                    print(f"  ✗ {message}")
                
                log.write(log_message + "\n")
                
                # Небольшая задержка между запросами
                time.sleep(0.5)
            
            # Итоговая статистика
            log.write("\n" + "=" * 80 + "\n")
            log.write(f"ИТОГО:\n")
            log.write(f"Успешно: {success_count}\n")
            log.write(f"Ошибок: {failed_count}\n")
            log.write(f"Всего обработано: {len(blocked_users)}\n")
    
    print("\n" + "=" * 60)
    print("ИТОГОВАЯ СТАТИСТИКА:")
    print(f"Успешно выполнен logout: {success_count}")
    print(f"Ошибок при выполнении: {failed_count}")
    print(f"Всего обработано: {len(blocked_users)}")
    print("=" * 60)
    print(f"\nПодробный лог сохранен в файл: {log_file}")
    print(f"CSV отчет сохранен в файл: {csv_file}")
    print("\nСкрипт завершен.")
