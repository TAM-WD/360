'''
Скрипт предназначен для удаления прочитанных писем в ящиках по определенным критериям

1. Скрипт использует Basic авторизацию. Для общего ящика понадобится использовать формат логина: домен вашей организации/ваш логин на Яндексе/имя ящика (например: domain.ru/test/obschiyaschik») Подробнее в статье из Справки - https://yandex.ru/support/yandex-360/business/mail/ru/mail-clients/shared-mailboxes
Для пароля используется пароль приложений, выданный для учётной записи, у которой есть доступ для управления общим ящиком по IMAP 

Логин и пароль необходимо указать в переменных LOGIN и APP_PASS

Доступ для управления по IMAP для общего ящика выдаётся в панели администратора. 
Пароль приложений для аккаунта, который управляет общим ящиком получается на странице https://id.yandex.ru/security/app-passwords

2. В самом общем ящике необходимо перейти в почтовые настройки и в разделе "Почтовые программы" установить активной галочку рядом с пунктом "С сервера imap.yandex.ru по протоколу IMAP"
3. Для работы скрипта также понадобится установить библиотеку aioimaplib с помощью команды pip install aioimaplib
4. Когда все предварительные настройки выполнены и скрипт запустился, предлагается выбрать 1 из режимов его работы:
— recent : удалить все прочитанные сообщения в папке "Входящие" кроме последних 24 ч
— range  : удалить прочитанные сообщения в папке "Входящие" за указанный период

Для 1 режима выбирать ничего не нужно, для второго режима необходимо:
— Ввести дату начала в формате ДД-ММ-ГГГГ, например 01-01-2025
— Ввести дату конца или нажать Enter чтобы взять вчерашний день

4. При подтверждении удаления нужно написать - yes
5. Исходя из наших лимитов по IMAP в 1000 на 10 минут, установлены задержки и ретраи
6. После завершения работы скрипта будет сформирован лог файл с результатами
7. Письма по IMAP удаляются безвозвратно мимо Корзины и не складываются в папку "Удаленные"
'''

import asyncio
import binascii
import gc
import io
import logging
import os
import re
import ssl
import sys
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from threading import Lock
from typing import Optional

# ---------------------------------------------------------------------------
# Кодировка вывода
# ---------------------------------------------------------------------------
if sys.stdout.encoding != 'utf-8':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

import aioimaplib

# ---------------------------------------------------------------------------
# Учётные данные — заполните перед запуском
# ---------------------------------------------------------------------------
LOGIN    = ""      # Логин почтового ящика
APP_PASS = ""  # Пароль приложения из настроек Яндекс ID

# ---------------------------------------------------------------------------
# Директория вывода и логи
# ---------------------------------------------------------------------------
SCRIPT_NAME   = "delete_read_emails"
RUN_TIMESTAMP = datetime.now().strftime('%Y%m%d_%H%M%S')
OUTPUT_DIR    = f"{SCRIPT_NAME}_{RUN_TIMESTAMP}"
os.makedirs(OUTPUT_DIR, exist_ok=True)

LOG_FILE              = os.path.join(OUTPUT_DIR, "logs.log")
DELETED_MESSAGES_FILE = os.path.join(OUTPUT_DIR, "deleted_messages.txt")
SKIPPED_FILE          = os.path.join(OUTPUT_DIR, "skipped.txt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("delete_read_emails")
logging.getLogger('aioimaplib.aioimaplib').setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Константы IMAP
# ---------------------------------------------------------------------------
DEFAULT_IMAP_SERVER = "imap.yandex.ru"
DEFAULT_IMAP_PORT   = 993

IMAP_CONNECT_TIMEOUT = 60
IMAP_SELECT_TIMEOUT  = 90
IMAP_SEARCH_TIMEOUT  = 120
EXPUNGE_TIMEOUT      = 120
STORE_TIMEOUT        = 15
NOOP_TIMEOUT         = 5
LIST_TIMEOUT         = 30

EXIT_CODE = 1

# Папка для удаления
INBOX_FOLDER = "INBOX"

# ---------------------------------------------------------------------------
# Лимиты IMAP и защита от rate limit
#
# Не более 1000 запросов за 10 минут от одного пользователя.
# Каждая IMAP-команда (SELECT, SEARCH, STORE, EXPUNGE, NOOP) = 1 запрос.
#
# Безопасный порог — 900 запросов за 10 минут (запас 10%).
# Это ~1.5 запроса в секунду, или ~0.67 сек между запросами.
#
# Мы используем:
#   DELAY_BETWEEN_COMMANDS — пауза между командами STORE (самые частые)
#   DELAY_AFTER_EXPUNGE    — пауза после EXPUNGE
#   DELAY_AFTER_SEARCH     — пауза после SEARCH
#   RATE_LIMIT_WINDOW      — окно подсчёта запросов (секунды)
#   RATE_LIMIT_MAX         — максимум запросов в окне (с запасом)
#   RATE_LIMIT_PAUSE       — пауза при достижении лимита
# ---------------------------------------------------------------------------
DELAY_BETWEEN_COMMANDS = 0.7    # сек между командами STORE
DELAY_AFTER_SEARCH     = 1.0    # сек после SEARCH
DELAY_AFTER_EXPUNGE    = 2.0    # сек после EXPUNGE
DELAY_AFTER_SELECT     = 0.5    # сек после SELECT
NOOP_EVERY             = 50     # NOOP каждые N команд STORE
DELAY_AFTER_NOOP       = 0.5    # сек после NOOP

RATE_LIMIT_WINDOW      = 600    # 10 минут в секундах
RATE_LIMIT_MAX         = 900    # максимум запросов в окне
RATE_LIMIT_PAUSE       = 60     # сек паузы при превышении лимита

# Ретраи
MAX_RETRIES_CONNECT    = 3      # попыток подключения
MAX_RETRIES_COMMAND    = 3      # попыток выполнения команды
RETRY_BASE_DELAY       = 5      # базовая пауза между ретраями (сек)

_imap_connection_lock = asyncio.Lock()

# ---------------------------------------------------------------------------
# Счётчик запросов (rate limiter)
# ---------------------------------------------------------------------------
@dataclass
class RateLimiter:
    """
    Отслеживает количество IMAP-запросов в скользящем окне.
    При приближении к лимиту автоматически делает паузу.
    """
    window:    int   # размер окна в секундах
    max_calls: int   # максимум вызовов в окне
    pause:     int   # пауза при достижении лимита (сек)
    _timestamps: list = field(default_factory=list)
    _lock: object     = field(default_factory=Lock)

    def _cleanup(self):
        """Удаляет устаревшие метки времени."""
        cutoff = time.time() - self.window
        self._timestamps = [t for t in self._timestamps if t > cutoff]

    def count(self) -> int:
        """Текущее количество запросов в окне."""
        with self._lock:
            self._cleanup()
            return len(self._timestamps)

    def register(self):
        """Регистрирует один запрос."""
        with self._lock:
            self._timestamps.append(time.time())

    def remaining(self) -> int:
        """Сколько запросов ещё можно сделать в текущем окне."""
        return max(0, self.max_calls - self.count())

    async def check_and_wait(self):
        """
        Проверяет лимит. Если достигнут — ждёт и логирует.
        Вызывать ПЕРЕД каждой IMAP-командой.
        """
        with self._lock:
            self._cleanup()
            current = len(self._timestamps)

        if current >= self.max_calls:
            logger.warning(
                f"⏸️  Достигнут лимит IMAP запросов ({current}/{self.max_calls} "
                f"за последние {self.window // 60} мин). "
                f"Пауза {self.pause} сек ..."
            )
            await asyncio.sleep(self.pause)
            # После паузы чистим снова
            with self._lock:
                self._cleanup()

        self.register()


# Глобальный экземпляр rate limiter
rate_limiter = RateLimiter(
    window=RATE_LIMIT_WINDOW,
    max_calls=RATE_LIMIT_MAX,
    pause=RATE_LIMIT_PAUSE,
)

# ---------------------------------------------------------------------------
# Статистика
# ---------------------------------------------------------------------------
_stats_lock = Lock()
_stats = {
    'total_deleted': 0,
    'total_skipped': 0,
    'total_commands': 0,
    'rate_limit_hits': 0,
    'retries': 0,
}


def stats_add(**kwargs):
    with _stats_lock:
        for k, v in kwargs.items():
            if k in _stats:
                _stats[k] += v


def stats_get() -> dict:
    with _stats_lock:
        return dict(_stats)


def stats_reset():
    with _stats_lock:
        for k in _stats:
            _stats[k] = 0


# ---------------------------------------------------------------------------
# Утилиты
# ---------------------------------------------------------------------------
def force_cleanup():
    try:
        gc.collect()
    except Exception:
        pass


def write_deleted_log(count: int, criteria: str):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(DELETED_MESSAGES_FILE, 'a', encoding='utf-8') as f:
        f.write(
            f"{ts} | {LOGIN} | {INBOX_FOLDER} | "
            f"deleted={count} | {criteria}\n"
        )


def write_skipped_log(reason: str):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(SKIPPED_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{ts} | {LOGIN} | {INBOX_FOLDER} | {reason}\n")


# ---------------------------------------------------------------------------
# Настройки
# ---------------------------------------------------------------------------
@dataclass
class Settings:
    login:    str
    app_pass: str


def get_settings() -> Settings:
    if not LOGIN:
        raise ValueError(
            "Не задан логин. Заполните константу LOGIN в начале скрипта."
        )
    if not APP_PASS:
        raise ValueError(
            "Не задан пароль. Заполните константу APP_PASS в начале скрипта."
        )
    return Settings(login=LOGIN, app_pass=APP_PASS)


# ---------------------------------------------------------------------------
# Критерий IMAP SEARCH
# ---------------------------------------------------------------------------
@dataclass
class SearchCriteria:
    imap_query:  str
    description: str


def build_criteria_recent() -> SearchCriteria:
    cutoff = (datetime.utcnow() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return SearchCriteria(
        imap_query=f'SEEN BEFORE {cutoff.strftime("%d-%b-%Y")}',
        description=(
            f"Удаляем прочитанные ДО {cutoff.strftime('%d-%m-%Y')} (UTC) — "
            f"письма за последние 24 ч сохраняются"
        ),
    )


def build_criteria_range(date_from: datetime, date_to: datetime) -> SearchCriteria:
    before = date_to + timedelta(days=1)
    return SearchCriteria(
        imap_query=(
            f'SEEN SINCE {date_from.strftime("%d-%b-%Y")} '
            f'BEFORE {before.strftime("%d-%b-%Y")}'
        ),
        description=(
            f"Удаляем прочитанные с {date_from.strftime('%d-%m-%Y')} "
            f"по {date_to.strftime('%d-%m-%Y')} включительно"
        ),
    )


# ---------------------------------------------------------------------------
# Интерактивный диалог
# ---------------------------------------------------------------------------
def ask_mode() -> str:
    print()
    print("=  v1_deleting_read_emails .py:308 - deleting_read_emails.py:322" * 60)
    print("Выберите режим удаления:  v1_deleting_read_emails .py:309 - deleting_read_emails.py:323")
    print("=  v1_deleting_read_emails .py:310 - deleting_read_emails.py:324" * 60)
    print("1  recent : удалить всё прочитанное кроме последних 24 ч  v1_deleting_read_emails .py:311 - deleting_read_emails.py:325")
    print("2  range  : удалить прочитанное за указанный период  v1_deleting_read_emails .py:312 - deleting_read_emails.py:326")
    print("=  v1_deleting_read_emails .py:313 - deleting_read_emails.py:327" * 60)

    while True:
        choice = input("\n  Ваш выбор (1 или 2): ").strip()
        if choice == "1":
            return "recent"
        if choice == "2":
            return "range"
        print("⚠️  Введите 1 или 2  v1_deleting_read_emails .py:321 - deleting_read_emails.py:335")


def ask_date(prompt: str, default: Optional[datetime] = None) -> datetime:
    default_hint = (
        f" (Enter = {default.strftime('%d-%m-%Y')})" if default else ""
    )
    while True:
        raw = input(f"  {prompt}{default_hint}: ").strip()
        if not raw and default:
            return default
        try:
            return datetime.strptime(raw, "%d-%m-%Y")
        except ValueError:
            print("⚠️  Неверный формат. Используйте DDMMYYYY, например 01012025  v1_deleting_read_emails .py:335 - deleting_read_emails.py:349")


def ask_criteria() -> SearchCriteria:
    mode = ask_mode()

    if mode == "recent":
        return build_criteria_recent()

    print()
    print("Укажите период удаления (даты включительно):  v1_deleting_read_emails .py:345 - deleting_read_emails.py:359")

    yesterday = (datetime.utcnow() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    while True:
        date_from = ask_date("Дата начала (DD-MM-YYYY)")
        date_to   = ask_date("Дата конца  (DD-MM-YYYY)", default=yesterday)

        if date_from <= date_to:
            break

        print(
            f"\n  ⚠️  Дата начала ({date_from.strftime('%d-%m-%Y')}) "
            f"не может быть позже даты конца ({date_to.strftime('%d-%m-%Y')}). "
            f"Попробуйте снова."
        )

    return build_criteria_range(date_from, date_to)


def ask_confirmation(login: str, criteria: SearchCriteria) -> bool:
    print()
    print("=  v1_deleting_read_emails .py:369 - deleting_read_emails.py:383" * 60)
    print("⚠️  ВНИМАНИЕ: письма будут удалены БЕЗВОЗВРАТНО  v1_deleting_read_emails .py:370 - deleting_read_emails.py:384")
    print(f"Ящик   : {login}  v1_deleting_read_emails .py:371 - deleting_read_emails.py:385")
    print(f"Папка  : {INBOX_FOLDER} (Входящие)  v1_deleting_read_emails .py:372 - deleting_read_emails.py:386")
    print(f"{criteria.description}  v1_deleting_read_emails .py:373 - deleting_read_emails.py:387")
    print("=  v1_deleting_read_emails .py:374 - deleting_read_emails.py:388" * 60)
    answer = input("\n  Введите 'yes' для подтверждения: ").strip().lower()
    return answer == "yes"


# ---------------------------------------------------------------------------
# IMAP — обёртка с ретраями и rate limiting
# ---------------------------------------------------------------------------
async def imap_command(coro_fn, description: str,
                       timeout: float, max_retries: int = MAX_RETRIES_COMMAND):
    """
    Выполняет IMAP-команду с:
      - проверкой rate limit перед каждой попыткой
      - таймаутом
      - ретраями при ошибках
      - экспоненциальной задержкой между ретраями

    coro_fn  — callable без аргументов, возвращающий корутину
    description — название команды для логов
    timeout  — таймаут одной попытки в секундах
    """
    last_error = None

    for attempt in range(1, max_retries + 1):
        # Проверяем rate limit перед каждой командой
        await rate_limiter.check_and_wait()
        stats_add(total_commands=1)

        try:
            status, data = await asyncio.wait_for(coro_fn(), timeout=timeout)
            return status, data

        except (asyncio.TimeoutError, aioimaplib.aioimaplib.CommandTimeout) as e:
            last_error = e
            stats_add(retries=1)
            wait = RETRY_BASE_DELAY * attempt
            logger.warning(
                f"⏱ Таймаут {description} "
                f"(попытка {attempt}/{max_retries}), ждём {wait}с ..."
            )
            await asyncio.sleep(wait)

        except aioimaplib.aioimaplib.Abort as e:
            last_error = e
            stats_add(retries=1)
            wait = RETRY_BASE_DELAY * attempt
            logger.warning(
                f"⚠️  IMAP Abort на {description} "
                f"(попытка {attempt}/{max_retries}), ждём {wait}с ..."
            )
            await asyncio.sleep(wait)

        except Exception as e:
            last_error = e
            stats_add(retries=1)
            wait = RETRY_BASE_DELAY * attempt
            logger.warning(
                f"⚠️  Ошибка {description}: {type(e).__name__} "
                f"(попытка {attempt}/{max_retries}), ждём {wait}с ..."
            )
            await asyncio.sleep(wait)

    raise RuntimeError(
        f"{description} не удалось после {max_retries} попыток: "
        f"{type(last_error).__name__} — {last_error}"
    )


# ---------------------------------------------------------------------------
# IMAP — подключение
# ---------------------------------------------------------------------------
async def connect_imap(login: str, app_pass: str) -> aioimaplib.IMAP4_SSL:
    """
    Подключается к IMAP и выполняет LOGIN.
    Ретраи с экспоненциальной задержкой.
    """
    async with _imap_connection_lock:
        last_error = None

        for attempt in range(1, MAX_RETRIES_CONNECT + 1):
            connector = None
            try:
                ssl_ctx = ssl.create_default_context()
                ssl_ctx.check_hostname = False
                ssl_ctx.verify_mode    = ssl.CERT_NONE

                timeout = 30 if sys.platform == 'win32' else IMAP_CONNECT_TIMEOUT

                connector = aioimaplib.IMAP4_SSL(
                    host=DEFAULT_IMAP_SERVER,
                    port=DEFAULT_IMAP_PORT,
                    ssl_context=ssl_ctx,
                    timeout=timeout,
                )

                await asyncio.wait_for(
                    connector.wait_hello_from_server(), timeout=timeout
                )

                # LOGIN — тоже считаем за запрос
                await rate_limiter.check_and_wait()
                stats_add(total_commands=1)

                status, data = await asyncio.wait_for(
                    connector.login(login, app_pass), timeout=timeout
                )
                if status != "OK":
                    raise ConnectionError(
                        f"LOGIN failed (status={status}): {data}"
                    )

                logger.info(f"✅ IMAP LOGIN OK для {login}")
                return connector

            except Exception as e:
                last_error = e
                if connector:
                    try:
                        await asyncio.wait_for(connector.logout(), timeout=3)
                    except Exception:
                        pass

                wait = RETRY_BASE_DELAY * attempt
                logger.warning(
                    f"⚠️  Подключение: попытка {attempt}/{MAX_RETRIES_CONNECT} "
                    f"не удалась: {type(e).__name__} — {str(e)[:100]}. "
                    f"Ждём {wait}с ..."
                )
                if attempt < MAX_RETRIES_CONNECT:
                    await asyncio.sleep(wait)

        raise ConnectionError(
            f"Не удалось подключиться после {MAX_RETRIES_CONNECT} попыток: "
            f"{type(last_error).__name__}"
        )


async def safe_close(connector):
    """Аккуратно завершает IMAP-сессию."""
    if not connector:
        return
    try:
        state = getattr(connector.protocol, 'state', None)
        if state == "SELECTED":
            try:
                await asyncio.wait_for(connector.close(), timeout=2)
            except Exception:
                pass
        try:
            await asyncio.wait_for(connector.logout(), timeout=3)
        except Exception:
            pass
    except Exception:
        pass
    finally:
        try:
            if hasattr(connector, '_transport') and connector._transport:
                connector._transport.abort()
        except Exception:
            pass
        await asyncio.sleep(0.3)


# ---------------------------------------------------------------------------
# Удаление в INBOX
# ---------------------------------------------------------------------------
async def delete_in_inbox(connector, criteria: SearchCriteria) -> int:
    """
    Основная логика:
      1. SELECT INBOX
      2. SEARCH по критерию
      3. STORE +FLAGS \\Deleted для каждого письма (с паузами и rate limiting)
      4. EXPUNGE один раз в конце
    """
    logger.info(f"")
    logger.info(f"📂 Открываем папку: {INBOX_FOLDER}")

    # --- SELECT ---
    try:
        status, _ = await imap_command(
            coro_fn=lambda: connector.select(INBOX_FOLDER),
            description=f"SELECT {INBOX_FOLDER}",
            timeout=IMAP_SELECT_TIMEOUT,
        )
        if status != "OK":
            logger.error(f"❌ SELECT вернул: {status}")
            write_skipped_log(f"SELECT failed: {status}")
            return 0
    except RuntimeError as e:
        logger.error(f"❌ {e}")
        write_skipped_log(str(e))
        return 0

    await asyncio.sleep(DELAY_AFTER_SELECT)

    # --- SEARCH ---
    logger.info(f"🔍 SEARCH: {criteria.imap_query}")
    logger.info(
        f"   Запросов использовано: {rate_limiter.count()}/{RATE_LIMIT_MAX} "
        f"за последние {RATE_LIMIT_WINDOW // 60} мин "
        f"(осталось: {rate_limiter.remaining()})"
    )

    try:
        status, data = await imap_command(
            coro_fn=lambda: connector.uid_search(criteria.imap_query),
            description="SEARCH",
            timeout=IMAP_SEARCH_TIMEOUT,
        )
    except RuntimeError as e:
        logger.error(f"❌ SEARCH не удался: {e}")
        write_skipped_log(f"SEARCH failed: {e}")
        return 0

    await asyncio.sleep(DELAY_AFTER_SEARCH)

    if status != "OK" or not data or not data[0]:
        logger.info("ℹ️  Подходящих писем не найдено")
        return 0

    uids = [u for u in data[0].split() if u]
    if not uids:
        logger.info("ℹ️  Подходящих писем не найдено")
        return 0

    total = len(uids)
    logger.info(f"📬 Найдено {total} писем для удаления")
    logger.info(
        f"   Оценка времени: ~{total * DELAY_BETWEEN_COMMANDS / 60:.1f} мин "
        f"(по {DELAY_BETWEEN_COMMANDS}с между командами)"
    )

    # --- STORE +FLAGS \Deleted ---
    deleted = 0
    skipped = 0

    for idx, uid in enumerate(uids, start=1):

        # Периодический NOOP — поддерживает соединение живым
        if idx % NOOP_EVERY == 0:
            try:
                await imap_command(
                    coro_fn=lambda: connector.noop(),
                    description="NOOP",
                    timeout=NOOP_TIMEOUT,
                    max_retries=1,
                )
                await asyncio.sleep(DELAY_AFTER_NOOP)
            except Exception:
                pass

            used = rate_limiter.count()
            logger.info(
                f"⏳ Прогресс: {idx}/{total} | "
                f"Удалено: {deleted} | "
                f"Запросов: {used}/{RATE_LIMIT_MAX}"
            )

        try:
            msg_uid = int(uid)
        except ValueError:
            skipped += 1
            continue

        try:
            status, _ = await imap_command(
                coro_fn=lambda u=msg_uid: connector.uid(
                    'store', u, "+FLAGS", "\\Deleted"
                ),
                description=f"STORE UID {msg_uid}",
                timeout=STORE_TIMEOUT,
            )
            if status == "OK":
                deleted += 1
            else:
                skipped += 1

        except RuntimeError as e:
            logger.warning(f"⚠️  Не удалось пометить UID {msg_uid}: {e}")
            skipped += 1
            continue

        # Пауза между командами STORE — ключевой элемент защиты от rate limit
        await asyncio.sleep(DELAY_BETWEEN_COMMANDS)

    logger.info(f"")
    logger.info(f"✅ Помечено \\Deleted: {deleted} / {total}")
    if skipped:
        logger.info(f"⚠️  Пропущено: {skipped}")

    # --- EXPUNGE ---
    if deleted > 0:
        logger.info(f"🗑️  Выполняем EXPUNGE ...")
        try:
            await imap_command(
                coro_fn=lambda: connector.expunge(),
                description="EXPUNGE",
                timeout=EXPUNGE_TIMEOUT,
            )
            logger.info(f"✅ EXPUNGE выполнен. Удалено писем: {deleted}")
            write_deleted_log(deleted, criteria.imap_query)
            stats_add(total_deleted=deleted, total_skipped=skipped)

        except RuntimeError as e:
            logger.error(f"❌ EXPUNGE не удался: {e}")
            write_skipped_log(f"EXPUNGE failed: {e}")
            stats_add(total_skipped=deleted + skipped)
            return 0

        await asyncio.sleep(DELAY_AFTER_EXPUNGE)
    else:
        logger.info("ℹ️  Нечего удалять — EXPUNGE не нужен")
        stats_add(total_skipped=skipped)

    return deleted


# ---------------------------------------------------------------------------
# Главная корутина
# ---------------------------------------------------------------------------
async def process_mailbox(login: str, app_pass: str, criteria: SearchCriteria):
    stats_reset()

    logger.info(f"")
    logger.info(f"{'='*60}")
    logger.info(f"📧 Ящик  : {login}")
    logger.info(f"📁 Папка : {INBOX_FOLDER}")
    logger.info(f"📋 {criteria.description}")
    logger.info(f"⚡ Лимит : {RATE_LIMIT_MAX} запросов / {RATE_LIMIT_WINDOW // 60} мин")
    logger.info(f"⏱️  Пауза : {DELAY_BETWEEN_COMMANDS}с между командами")
    logger.info(f"{'='*60}")

    try:
        connector = await connect_imap(login, app_pass)
    except ConnectionError as e:
        logger.error(f"❌ Не удалось подключиться: {e}")
        return

    try:
        await delete_in_inbox(connector, criteria)
    finally:
        await safe_close(connector)
        force_cleanup()

    # Итоговая статистика
    s = stats_get()
    logger.info(f"")
    logger.info(f"{'='*60}")
    logger.info(f"📊 ИТОГ")
    logger.info(f"{'='*60}")
    logger.info(f"  🗑️  Удалено писем           : {s['total_deleted']}")
    logger.info(f"  ⚠️  Пропущено (ошибки)      : {s['total_skipped']}")
    logger.info(f"  📡 Всего IMAP-команд        : {s['total_commands']}")
    logger.info(f"  🔁 Ретраев                  : {s['retries']}")
    logger.info(f"  ⏸️  Срабатываний rate limit  : {s['rate_limit_hits']}")
    logger.info(f"")
    logger.info(f"📄 Лог удалений : {os.path.abspath(DELETED_MESSAGES_FILE)}")


# ---------------------------------------------------------------------------
# Точка входа
# ---------------------------------------------------------------------------
def main():
    try:
        settings = get_settings()
    except ValueError as e:
        print(f"\n❌ Ошибка настроек: {e}  v1_deleting_read_emails .py:740 - deleting_read_emails.py:754")
        sys.exit(EXIT_CODE)

    print(f"\n{'='*60}  v1_deleting_read_emails .py:743 - deleting_read_emails.py:757")
    print(f"Удаление прочитанных писем из папки Входящие  v1_deleting_read_emails .py:744 - deleting_read_emails.py:758")
    print(f"Ящик: {settings.login}  v1_deleting_read_emails .py:745 - deleting_read_emails.py:759")
    print(f"{'='*60}  v1_deleting_read_emails .py:746 - deleting_read_emails.py:760")

    try:
        criteria = ask_criteria()
    except KeyboardInterrupt:
        print("\n\nОтменено пользователем.  v1_deleting_read_emails .py:751 - deleting_read_emails.py:765")
        return

    try:
        confirmed = ask_confirmation(settings.login, criteria)
    except KeyboardInterrupt:
        print("\n\nОтменено пользователем.  v1_deleting_read_emails .py:757 - deleting_read_emails.py:771")
        return

    if not confirmed:
        print("\nОперация отменена.  v1_deleting_read_emails .py:761 - deleting_read_emails.py:775")
        logger.info("Операция отменена пользователем.")
        return

    logger.info(f"📂 Директория вывода: {os.path.abspath(OUTPUT_DIR)}")

    try:
        asyncio.run(
            process_mailbox(
                login=settings.login,
                app_pass=settings.app_pass,
                criteria=criteria,
            )
        )
    except KeyboardInterrupt:
        logger.info("Прервано пользователем (Ctrl+C).")
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка: {e}")
        logger.error(traceback.format_exc())
        sys.exit(EXIT_CODE)

    logger.info("Скрипт завершён.")

    if sys.platform == 'win32':
        force_cleanup()
        time.sleep(1)


# ---------------------------------------------------------------------------
# Интерактивный диалог (вынесен после main чтобы не мешать читать логику)
# ---------------------------------------------------------------------------
def ask_mode() -> str:
    print()
    print("=  v1_deleting_read_emails .py:794 - deleting_read_emails.py:808" * 60)
    print("Выберите режим удаления:  v1_deleting_read_emails .py:795 - deleting_read_emails.py:809")
    print("=  v1_deleting_read_emails .py:796 - deleting_read_emails.py:810" * 60)
    print("1  recent : удалить всё прочитанное кроме последних 24 ч  v1_deleting_read_emails .py:797 - deleting_read_emails.py:811")
    print("2  range  : удалить прочитанное за указанный период  v1_deleting_read_emails .py:798 - deleting_read_emails.py:812")
    print("=  v1_deleting_read_emails .py:799 - deleting_read_emails.py:813" * 60)
    while True:
        choice = input("\n  Ваш выбор (1 или 2): ").strip()
        if choice == "1":
            return "recent"
        if choice == "2":
            return "range"
        print("⚠️  Введите 1 или 2  v1_deleting_read_emails .py:806 - deleting_read_emails.py:820")


def ask_date(prompt: str, default: Optional[datetime] = None) -> datetime:
    default_hint = (
        f" (Enter = {default.strftime('%d-%m-%Y')})" if default else ""
    )
    while True:
        raw = input(f"  {prompt}{default_hint}: ").strip()
        if not raw and default:
            return default
        try:
            return datetime.strptime(raw, "%d-%m-%Y")
        except ValueError:
            print("⚠️  Неверный формат. Используйте DDMMYYYY, например 01012025  v1_deleting_read_emails .py:820 - deleting_read_emails.py:834")


def ask_criteria() -> SearchCriteria:
    mode = ask_mode()
    if mode == "recent":
        return build_criteria_recent()

    print()
    print("Укажите период удаления (даты включительно):  v1_deleting_read_emails .py:829 - deleting_read_emails.py:843")
    yesterday = (datetime.utcnow() - timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    while True:
        date_from = ask_date("Дата начала (DD-MM-YYYY)")
        date_to   = ask_date("Дата конца  (DD-MM-YYYY)", default=yesterday)
        if date_from <= date_to:
            break
        print(
            f"\n  ⚠️  Дата начала ({date_from.strftime('%d-%m-%Y')}) "
            f"не может быть позже даты конца ({date_to.strftime('%d-%m-%Y')}). "
            f"Попробуйте снова."
        )
    return build_criteria_range(date_from, date_to)


def ask_confirmation(login: str, criteria: SearchCriteria) -> bool:
    print()
    print("=  v1_deleting_read_emails .py:848 - deleting_read_emails.py:862" * 60)
    print("⚠️  ВНИМАНИЕ: письма будут удалены БЕЗВОЗВРАТНО  v1_deleting_read_emails .py:849 - deleting_read_emails.py:863")
    print(f"Ящик   : {login}  v1_deleting_read_emails .py:850 - deleting_read_emails.py:864")
    print(f"Папка  : {INBOX_FOLDER} (Входящие)  v1_deleting_read_emails .py:851 - deleting_read_emails.py:865")
    print(f"{criteria.description}  v1_deleting_read_emails .py:852 - deleting_read_emails.py:866")
    print("=  v1_deleting_read_emails .py:853 - deleting_read_emails.py:867" * 60)
    answer = input("\n  Введите 'yes' для подтверждения: ").strip().lower()
    return answer == "yes"


if __name__ == "__main__":
    main()
