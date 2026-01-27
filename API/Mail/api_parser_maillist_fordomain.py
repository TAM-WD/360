import csv
import os
import requests
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime
import re

# === Конфигурация ===
Token = ''  # Вставьте ваш OAuth-токен
headers = {"Authorization": f"OAuth {Token}"}
Orgid = ''  # ID вашей организации
#UID = ''    # UID пользователя (если нужно ограничить выборку)

# === ФИЛЬТР ПО ДОМЕНУ ОТПРАВИТЕЛЯ ===
# Укажите домен для фильтрации (например, 'example.com')
# Оставьте пустым '' для получения всех писем
FILTER_DOMAIN = ' '  # Например: 'gmail.com', 'yandex.ru', 'company.com'

# === Базовый URL запроса ===
base_url = (
    f'https://api360.yandex.net/security/v1/org/{Orgid}/audit_log/mail'
    '?pageSize=100'
    '&afterDate=2025-09-01T23:59:59+03:00'
    #f'&includeUids={UID}'
    '&message_receive'
)

# === Функция для извлечения домена из email ===
def extract_domain(email_address):
    """
    Извлекает домен из email адреса
    Примеры:
        'user@example.com' -> 'example.com'
        'Name <user@example.com>' -> 'example.com'
        'user@sub.example.com' -> 'sub.example.com'
    """
    if not email_address:
        return None
    
    # Извлечение email из строки типа "Name <email@domain.com>"
    email_match = re.search(r'[\w\.-]+@[\w\.-]+\.\w+', str(email_address))
    if email_match:
        email_clean = email_match.group(0)
        # Извлечение домена
        domain_match = re.search(r'@([\w\.-]+\.\w+)$', email_clean)
        if domain_match:
            return domain_match.group(1).lower()
    
    return None

# === Функция для проверки соответствия домена ===
def matches_domain_filter(email_address, filter_domain):
    """
    Проверяет, соответствует ли email указанному домену
    """
    if not filter_domain:  # Если фильтр не установлен
        return True
    
    domain = extract_domain(email_address)
    if not domain:
        return False
    
    return domain == filter_domain.lower()

# === Функция для получения данных из API с пагинацией ===
def api360_get_maillist(headers, url):
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504]
    )
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))

    response = session.get(url, headers=headers)
    response.raise_for_status()  # Выбросит исключение, если статус != 2xx
    data = response.json()
    return data.get('events', []), data.get('nextPageToken', None)

# === Основной блок ===
if __name__ == '__main__':
    # === Подготовка файла для записи ===
    start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    dir_path = os.path.dirname(__file__)
    
    # Добавление домена в имя файла, если фильтр активен
    domain_suffix = f'_domain_{FILTER_DOMAIN}' if FILTER_DOMAIN else ''
    file_path = os.path.join(dir_path, f'maillist_orgid_{Orgid}{domain_suffix}_{start_time}.csv')

    field_names = [
        'eventType', 'date', 'orgId', 'userUid', 'userLogin', 'userName',
        'requestId', 'uniqId', 'source', 'mid', 'folderName', 'folderType',
        'labels', 'msgId', 'subject', 'from', 'to', 'cc', 'bcc', 'clientIp',
        'senderDomain'  # Добавлено поле для домена отправителя
    ]

    # === Статистика ===
    total_events = 0
    filtered_events = 0
    written_events = 0

    print("=" * 60)
    print("ПАРСЕР ПОЧТОВЫХ ЛОГОВ ЯНДЕКС 360")
    print("=" * 60)
    if FILTER_DOMAIN:
        print(f"✓ ФИЛЬТР ПО ДОМЕНУ: {FILTER_DOMAIN}")
    else:
        print("✓ ФИЛЬТР ПО ДОМЕНУ: Отключен (все письма)")
    print("=" * 60)

    with open(file_path, 'w', encoding='utf-8', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=field_names, extrasaction='ignore', delimiter=';')
        writer.writeheader()

        page_token = None
        page_number = 1

        while True:
            # === Формирование URL с учётом pageToken ===
            if page_token:
                current_url = f"{base_url}&pageToken={page_token}"
            else:
                current_url = base_url

            # === Получение данных из API ===
            print(f"\n📥 Запрашивается страница {page_number}...")
            events, next_page_token = api360_get_maillist(headers, current_url)

            # === Фильтрация и запись событий в CSV ===
            if events:
                total_events += len(events)
                
                # Фильтрация по домену
                filtered_batch = []
                for event in events:
                    sender_email = event.get('from', '')
                    sender_domain = extract_domain(sender_email)
                    
                    # Добавление домена в данные события
                    event['senderDomain'] = sender_domain if sender_domain else ''
                    
                    # Проверка фильтра
                    if matches_domain_filter(sender_email, FILTER_DOMAIN):
                        filtered_batch.append(event)
                    else:
                        filtered_events += 1
                
                # Запись отфильтрованных событий
                if filtered_batch:
                    writer.writerows(filtered_batch)
                    written_events += len(filtered_batch)
                    print(f"   ✓ Записано: {len(filtered_batch)} событий")
                    print(f"   ✗ Отфильтровано: {len(events) - len(filtered_batch)} событий")
                else:
                    print(f"   ✗ Все события отфильтрованы ({len(events)} шт.)")
                
                print(f"   📊 Всего обработано: {total_events} | Записано: {written_events} | Отфильтровано: {filtered_events}")
            else:
                print("   ⚠ Нет данных на этой странице.")

            # === Проверка на окончание пагинации ===
            if not next_page_token:
                print("\n" + "=" * 60)
                print("✓ Все страницы обработаны.")
                break

            # === Переход к следующей странице ===
            page_token = next_page_token
            page_number += 1

    # === Итоговая статистика ===
    print("=" * 60)
    print("ИТОГОВАЯ СТАТИСТИКА")
    print("=" * 60)
    print(f"📊 Всего получено событий: {total_events}")
    print(f"✓ Записано в CSV: {written_events}")
    print(f"✗ Отфильтровано: {filtered_events}")
    if FILTER_DOMAIN:
        print(f"🔍 Фильтр по домену: {FILTER_DOMAIN}")
    print("=" * 60)
    print(f'💾 Результат сохранён в: {file_path}')
    print("=" * 60)
