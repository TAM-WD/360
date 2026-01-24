"""
Скрипт для парсинга аудит-логов Яндекс Диска.
Фильтрует события удаления в корзину (fs-trash-append) с платформы Windows.
"""

import requests
import csv
import logging
from pathlib import Path
from datetime import datetime
from contextlib import contextmanager

# ==================== НАСТРОЙКИ ====================
ORG_ID = "" # ID организации из панели управления
OAUTH_TOKEN = "" # Токен с правами ya360_security:audit_log_disk

# Дата начала выборки
AFTER_DATE = ""  # Формат: YYYY-MM-DD
AFTER_TIME = "23:59:59"
TIMEZONE = "+03:00"  # Часовой пояс

PAGE_SIZE = 100
FLUSH_EVERY = 10
# ===================================================

# Порядок колонок в CSV
CSV_FIELDNAMES = [
    "date", "eventType", "userLogin", "userName",
    "ownerLogin", "ownerName", "path", "resourceFileId",
    "size", "lastModificationDate", "clientIp",
    "requestId", "uniqId", "orgId", "userUid", "ownerUid", "rights"
]


def setup_logging(output_dir: Path) -> logging.Logger:
    #Логирование в файл и консоль
    log_file = output_dir / "audit.log"
    
    logger = logging.getLogger("audit_parser")
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


def fetch_page(after_date: str, page_token: str = None) -> dict:
    #Запрос одной страницы событий из API
    url = f"https://api360.yandex.net/security/v1/org/{ORG_ID}/audit_log/disk"
    
    headers = {"Authorization": f"Bearer {OAUTH_TOKEN}"}
    params = {"pageSize": PAGE_SIZE, "afterDate": after_date}
    
    if page_token:
        params["pageToken"] = page_token
    
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()


def is_windows_trash_event(event: dict) -> bool:
    #Проверяет, является ли событие удалением в корзину с Windows
    if event.get("eventType") != "fs-trash-append":
        return False
    return event.get("requestId", "").startswith("rest_win")


@contextmanager
def csv_writer(filepath: Path):
    #Контекстный менеджер для потоковой записи в CSV
    file = open(filepath, "w", newline="", encoding="utf-8-sig")
    try:
        writer = csv.DictWriter(file, fieldnames=CSV_FIELDNAMES, extrasaction="ignore")
        writer.writeheader()
        writer.flush = file.flush
        yield writer
    finally:
        file.close()


class Stats:
    #Cчётчик статистики
    def __init__(self):
        self.total_events = 0
        self.filtered_events = 0
        self.pages = 0


def process_events(after_date: str, writer, logger: logging.Logger) -> Stats:
    #Получение события с пагинацией, фильтрация и запись в CSV. Возвращает статистику.
    stats = Stats()
    page_token = None
    
    while True:
        stats.pages += 1
        logger.info(f"Страница {stats.pages}: запрос...")
        
        try:
            data = fetch_page(after_date, page_token)
        except requests.RequestException as e:
            logger.error(f"Ошибка запроса: {e}")
            logger.info(f"Сохранено до сбоя: {stats.filtered_events} событий")
            raise
        
        events = data.get("events", [])
        stats.total_events += len(events)
        
        page_filtered = 0
        for event in events:
            if is_windows_trash_event(event):
                writer.writerow(event)
                stats.filtered_events += 1
                page_filtered += 1
        
        logger.info(
            f"Страница {stats.pages}: получено {len(events)}, "
            f"отфильтровано {page_filtered}, всего записано {stats.filtered_events}"
        )
        
        if stats.pages % FLUSH_EVERY == 0:
            writer.flush()
            logger.debug(f"Буфер сброшен на диск")
        
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    
    return stats

def count_deletions_by_owner(csv_path: Path, logger: logging.Logger) -> dict:
    #Читаем CSV и подсчитываем количество удалений по каждому владельцу
    owners = {}
    
    logger.info("Анализ CSV: подсчёт удалений по владельцам...")
    
    with open(csv_path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        
        for row in reader:
            uid = row.get("ownerUid", "")
            login = row.get("ownerLogin", "")
            name = row.get("ownerName", "")
            
            key = uid or login
            if not key:
                continue
            
            if key not in owners:
                owners[key] = {
                    "uid": uid,
                    "login": login,
                    "name": name,
                    "count": 0
                }
            owners[key]["count"] += 1
    
    logger.info(f"Найдено уникальных владельцев: {len(owners)}")
    return owners


def write_owners_report(
    owners: dict,
    filepath: Path,
    min_count: int,
    logger: logging.Logger
) -> int:
    #Записываем отчёт по владельцам с количеством удалений >= min_count.

    filtered = [
        o for o in owners.values()
        if o["count"] >= min_count
    ]
    filtered.sort(key=lambda x: (-x["count"], x["login"]))
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("UID\tLogin\tName\tCount\n")
        f.write("-" * 80 + "\n")
        
        for owner in filtered:
            f.write(
                f"{owner['uid']}\t"
                f"{owner['login']}\t"
                f"{owner['name']}\t"
                f"{owner['count']}\n"
            )
    
    logger.info(f"  {filepath.name}: {len(filtered)} владельцев")
    return len(filtered)


def generate_owner_reports(csv_path: Path, output_dir: Path, logger: logging.Logger):
    #Генерируем 3 отчёта по владельцам на основе CSV
    
    logger.info("=" * 60)
    logger.info("Формирование отчётов по владельцам")
    logger.info("=" * 60)
    
    owners = count_deletions_by_owner(csv_path, logger)
    
    if not owners:
        logger.warning("Нет данных для отчётов")
        return
    
    logger.info("Запись отчётов:")
    
    write_owners_report(
        owners,
        output_dir / "owners_all.txt",
        min_count=1,
        logger=logger
    )
    
    write_owners_report(
        owners,
        output_dir / "owners_more_than_10.txt",
        min_count=11,
        logger=logger
    )
    
    write_owners_report(
        owners,
        output_dir / "owners_more_than_100.txt",
        min_count=101,
        logger=logger
    )
    
    count_10_plus = sum(1 for o in owners.values() if o["count"] > 10)
    count_100_plus = sum(1 for o in owners.values() if o["count"] > 100)
    
    logger.info("-" * 40)
    logger.info(f"Всего владельцев: {len(owners)}")
    logger.info(f"С более чем 10 удалениями: {count_10_plus}")
    logger.info(f"С более чем 100 удалениями: {count_100_plus}")


def main():
    output_dir = Path(f"disk_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    output_dir.mkdir(exist_ok=True)
    
    logger = setup_logging(output_dir)
    csv_path = output_dir / "deleted_files.csv"
    after_date = f"{AFTER_DATE}T{AFTER_TIME}{TIMEZONE}"
    
    logger.info("=" * 60)
    logger.info("Парсинг аудит-логов Яндекс Диска")
    logger.info(f"Организация: {ORG_ID}")
    logger.info(f"Дата начала: {after_date}")
    logger.info(f"Фильтр: fs-trash-append + Windows")
    logger.info(f"Результат: {csv_path}")
    logger.info("=" * 60)
    
    try:
        with csv_writer(csv_path) as writer:
            stats = process_events(after_date, writer, logger)
    except KeyboardInterrupt:
        logger.warning("Прервано пользователем (Ctrl+C)")
        logger.info("Частичные данные сохранены в CSV")
        return
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        logger.info("Частичные данные сохранены в CSV")
        return
    
    logger.info("=" * 60)
    logger.info("ЭТАП 1 ЗАВЕРШЁН: Сбор данных")
    logger.info(f"Обработано страниц: {stats.pages}")
    logger.info(f"Всего событий в API: {stats.total_events}")
    logger.info(f"Записано в CSV: {stats.filtered_events}")
    
    if stats.filtered_events > 0:
        generate_owner_reports(csv_path, output_dir, logger)
    else:
        logger.info("Нет событий для анализа — отчёты не создаются")
    
    logger.info("=" * 60)
    logger.info("ВСЕ ЭТАПЫ ЗАВЕРШЕНЫ")
    logger.info(f"Результаты в папке: {output_dir}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
