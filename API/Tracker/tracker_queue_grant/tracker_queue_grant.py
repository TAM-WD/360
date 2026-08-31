#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#Скрипт по выдаче доступов на просмотр каждой очереди Трекера для конкретной учетной записи

#Необходимо заполнить переменные в файле param.env:
#YANDEX_TRACKER_TOKEN=
#YANDEX_TRACKER_ORG_ID=
#GRANT_ACCOUNT_LOGIN=
#DRY_RUN=True

import logging
import logging.handlers
import os
import random
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple
from urllib.parse import quote

import requests

# ============================================================
# Загрузка param.env без зависимости от python-dotenv
# ============================================================

ENV_FILE_PATH: Optional[Path] = None


def find_env_file() -> Optional[Path]:
    """
    Ищет param.env:
    1. Путь из переменной ENV_FILE, если задан.
    2. param.env в текущей директории.
    3. param.env рядом со скриптом.
    """
    candidates = []

    env_file_from_env = os.getenv("ENV_FILE", "").strip()
    if env_file_from_env:
        candidates.append(Path(env_file_from_env))

    candidates.append(Path.cwd() / "param.env")
    candidates.append(Path(__file__).resolve().parent / "param.env")

    for path in candidates:
        if path.is_file():
            return path

    return None


def load_env_file() -> None:
    """
    Простой парсер env-файла.
    Читает строки вида KEY=VALUE, игнорирует комментарии.
    """
    global ENV_FILE_PATH

    path = find_env_file()
    if not path:
        return

    ENV_FILE_PATH = path

    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()

            if not line or line.startswith("#") or "=" not in line:
                continue

            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()

            if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
                value = value[1:-1]

            if key and (key not in os.environ or os.environ.get(key, "") == ""):
                os.environ[key] = value


load_env_file()


# ============================================================
# Настройки
# ============================================================

BASE_URL = os.getenv("YANDEX_TRACKER_API_URL", "https://api.tracker.yandex.net").rstrip(
    "/"
)
TOKEN = os.getenv("YANDEX_TRACKER_TOKEN", "").strip()
ORG_ID = os.getenv("YANDEX_TRACKER_ORG_ID", "").strip()
GRANT_ACCOUNT_LOGIN = os.getenv("GRANT_ACCOUNT_LOGIN", "").strip()

LOG_DIR = Path(os.getenv("LOG_DIR", "logs"))
DRY_RUN = os.getenv("DRY_RUN", "false").strip().lower() in {"1", "true", "yes", "y"}

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", "30"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "0.0"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "50"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "10000"))

# Ограничение RPS на каждую ручку API отдельно (по требованию - 10 rps на ручку)
RPS_LIMIT = float(os.getenv("RPS_LIMIT", "10"))

LOGGER: Optional[logging.Logger] = None
SESSION: Optional[requests.Session] = None


# ============================================================
# Ограничитель RPS (по одному "ведру" на каждую ручку API)
# ============================================================


class RateLimiter:
    """
    Простой ограничитель RPS для конкретной ручки API.
    Гарантирует, что между двумя последовательными вызовами
    этой ручки пройдёт не меньше 1/rps секунд.
    Потокобезопасен (на случай будущего распараллеливания).
    """

    def __init__(self, rps: float):
        self._min_interval = 1.0 / rps if rps > 0 else 0.0
        self._lock = threading.Lock()
        self._last_call_at = 0.0

    def wait(self) -> None:
        if self._min_interval <= 0:
            return

        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call_at
            delay = self._min_interval - elapsed

            if delay > 0:
                time.sleep(delay)

            self._last_call_at = time.monotonic()


# Отдельный лимитер на каждую из используемых ручек API,
# чтобы не превышать RPS_LIMIT (по умолчанию 10) по каждой из них независимо друг от друга.
RATE_LIMITERS = {
    "get_queues": RateLimiter(RPS_LIMIT),          # GET /v3/queues/
    "get_user_access": RateLimiter(RPS_LIMIT),     # GET /v3/queues/<id>/permissions/users/<login>
    "manage_access": RateLimiter(RPS_LIMIT),       # PATCH /v3/queues/<id>/permissions
    "resolve_user": RateLimiter(RPS_LIMIT),        # GET /v3/users/<login_or_id>
}


# ============================================================
# Логирование
# ============================================================


def configure_logging() -> logging.Logger:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("tracker_queue_grant")
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    file_handler = logging.handlers.TimedRotatingFileHandler(
        LOG_DIR / "tracker_grant.log",
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


# ============================================================
# Проверка конфигурации
# ============================================================


def validate_config(logger: logging.Logger) -> None:
    missing = []

    if not TOKEN:
        missing.append("YANDEX_TRACKER_TOKEN")

    if not ORG_ID:
        missing.append("YANDEX_TRACKER_ORG_ID")

    if not GRANT_ACCOUNT_LOGIN:
        missing.append("GRANT_ACCOUNT_LOGIN")

    if missing:
        logger.error(
            "Не заданы обязательные переменные окружения: %s",
            ", ".join(missing),
        )
        sys.exit(2)


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"OAuth {TOKEN}",
            "X-Org-ID": ORG_ID,
            "X-Cloud-Org-ID": ORG_ID,
            "Content-Type": "application/json",
            "User-Agent": "tracker-queue-grant-read/8.0",
        }
    )
    return session


# ============================================================
# HTTP
# ============================================================


def call_api(
    method: str,
    path: str,
    params: Optional[dict] = None,
    payload: Optional[dict] = None,
    expected_codes: Tuple[int, ...] = (200,),
    logger: Optional[logging.Logger] = None,
    rate_limit_key: Optional[str] = None,
) -> Optional[requests.Response]:
    logger = logger or LOGGER
    url = f"{BASE_URL}{path}"
    last_response = None

    limiter = RATE_LIMITERS.get(rate_limit_key) if rate_limit_key else None

    for attempt in range(1, MAX_RETRIES + 1):
        if limiter:
            limiter.wait()

        try:
            response = SESSION.request(
                method=method,
                url=url,
                params=params,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )
            last_response = response

            if response.status_code in expected_codes:
                return response

            if response.status_code == 429 or response.status_code >= 500:
                retry_after = response.headers.get("Retry-After")

                if retry_after:
                    delay = float(retry_after)
                else:
                    delay = min(60.0, (2**attempt) + random.uniform(0, 0.5))

                logger.warning(
                    "HTTP %s for %s %s, retry %s/%s after %.2fs",
                    response.status_code,
                    method,
                    path,
                    attempt,
                    MAX_RETRIES,
                    delay,
                )
                time.sleep(delay)
                continue

            return response

        except requests.RequestException as exc:
            delay = min(60.0, (2**attempt) + random.uniform(0, 0.5))
            logger.warning(
                "Network error for %s %s: %s; retry %s/%s after %.2fs",
                method,
                path,
                exc,
                attempt,
                MAX_RETRIES,
                delay,
            )
            time.sleep(delay)

    return last_response


def safe_json(response: Optional[requests.Response]):
    if response is None or not getattr(response, "content", None):
        return {}

    try:
        return response.json()
    except ValueError:
        return {}


# ============================================================
# Резолв логина аккаунта, которому выдаём доступ
# (GET /v3/users/<login_or_id>)
# ============================================================


def resolve_grant_account_login(raw_value: str, logger: logging.Logger) -> str:
    """
    Ручки /v3/users/<...> и /v3/queues/<id>/permissions/users/<...>
    не принимают email с доменом (например trse-t@yandex.ru) - только
    "чистый" Tracker-логин (например trse-t) или числовой идентификатор.

    Пробуем по очереди:
    1. Значение как есть (вдруг это уже чистый логин или id).
    2. Часть до "@", если в значении есть домен.
    """
    candidates = [raw_value]

    if "@" in raw_value:
        local_part = raw_value.split("@", 1)[0].strip()
        if local_part and local_part not in candidates:
            candidates.append(local_part)

    for candidate in candidates:
        response = call_api(
            "GET",
            f"/v3/users/{quote(candidate, safe='')}",
            expected_codes=(200, 400, 404),
            logger=logger,
            rate_limit_key="resolve_user",
        )

        if response is not None and response.status_code == 200:
            data = safe_json(response)
            login = str(data.get("login") or "").strip()

            if login:
                if candidate != raw_value:
                    logger.info(
                        "GRANT_ACCOUNT_LOGIN '%s' не найден напрямую, "
                        "но по части до '@' ('%s') найден пользователь. "
                        "Канонический логин Tracker: '%s'",
                        raw_value,
                        candidate,
                        login,
                    )
                else:
                    logger.info(
                        "GRANT_ACCOUNT_LOGIN подтверждён как валидный логин: '%s'",
                        login,
                    )
                return login

        logger.warning(
            "Не удалось разрешить '%s' через /v3/users/ (HTTP %s: %s)",
            candidate,
            getattr(response, "status_code", "нет ответа"),
            safe_json(response) if response is not None else "",
        )

    logger.error(
        "Не удалось разрешить GRANT_ACCOUNT_LOGIN='%s' ни одним из способов (пробовали: %s). "
        "Укажите в param.env точный Tracker-логин пользователя (без @домена).",
        raw_value,
        ", ".join(candidates),
    )
    sys.exit(3)


# ============================================================
# Очереди (GET /v3/queues/)
# ============================================================


def get_all_queues(logger: logging.Logger) -> List[dict]:
    queues: List[dict] = []
    seen: set = set()
    page = 1

    while page <= MAX_PAGES:
        response = call_api(
            "GET",
            "/v3/queues/",
            params={"page": page, "perPage": PAGE_SIZE},
            expected_codes=(200, 400, 403, 404),
            logger=logger,
            rate_limit_key="get_queues",
        )

        if response is None:
            raise RuntimeError("Нет ответа при получении списка очередей")

        if response.status_code != 200:
            raise RuntimeError(
                f"Не удалось получить очереди: HTTP {response.status_code}: {response.text[:500]}"
            )

        data = safe_json(response)

        if isinstance(data, dict):
            items = data.get("queues") or data.get("result") or data.get("data") or []
        else:
            items = data

        if not isinstance(items, list):
            raise RuntimeError(f"Некорректный формат списка очередей: {type(items)!r}")

        if not items:
            break

        for queue in items:
            if not isinstance(queue, dict):
                continue

            queue_id = str(queue.get("id") or "").strip()
            queue_key = str(queue.get("key") or "").strip()
            identifier = queue_key or queue_id

            if not identifier or identifier in seen:
                continue

            seen.add(identifier)
            queues.append(queue)

        if len(items) < PAGE_SIZE:
            break

        page += 1

    return queues


def queue_display(queue: dict) -> str:
    key = str(queue.get("key") or "")
    name = str(queue.get("name") or "")
    identifier = str(queue.get("id") or "")

    if key and name:
        return f"{key} ({name})"
    elif key:
        return key
    elif name:
        return name
    else:
        return f"queue#{identifier}"


def queue_api_id(queue: dict) -> str:
    """
    Идентификатор очереди для использования в запросах к ручкам
    permissions/users и manage-access.

    ВАЖНО: ручка GET /v3/queues/<id>/permissions/users/<login>
    на практике НЕ принимает числовой id очереди (возвращает
    HTTP 400 "Incorrect data format"), а принимает только
    буквенный key. Поэтому здесь приоритет key, а не id -
    в отличие от общего списка очередей, где id тоже валиден.
    """
    return str(queue.get("key") or queue.get("id") or "")


# ============================================================
# Права пользователя в очереди
# (GET /v3/queues/<id>/permissions/users/<login>)
# ============================================================


def get_user_queue_permissions(
    queue_id: str,
    login: str,
    display_name: str,
    logger: logging.Logger,
) -> Tuple[str, Optional[dict]]:
    """
    Возвращает (status, data):
    - "ok"        - запрос успешен, data содержит разобранный JSON ответа;
    - "not_found" - очередь удалена / в архиве / недоступна - НЕ ошибка;
    - "forbidden" - реально не хватает прав токена - настоящая ошибка;
    - "error"     - прочая ошибка запроса (некорректные данные и т.п.).
    """
    if not queue_id:
        logger.warning(
            "У очереди %s нет подходящего идентификатора (key/id) - пропуск",
            display_name,
        )
        return "not_found", None

    response = call_api(
        "GET",
        f"/v3/queues/{quote(queue_id, safe='')}/permissions/users/{quote(login, safe='')}",
        expected_codes=(200, 400, 403, 404),
        logger=logger,
        rate_limit_key="get_user_access",
    )

    if response is None:
        logger.error(
            "Нет ответа при получении прав пользователя %s в очереди %s",
            login,
            display_name,
        )
        return "error", None

    if response.status_code == 200:
        return "ok", safe_json(response)

    body = safe_json(response)
    messages = body.get("errorMessages") or body.get("errors") or body or response.text[:300]

    # API отдаёт HTTP 403, но в errorsData.queue.deleted=true
    # видно, что очередь фактически удалена. Это не нехватка прав токена,
    # а удаленная очередь - относим в "not_found" (не ошибка).
    if response.status_code == 403:
        error_data = body.get("errorsData") or {}
        queue_info = error_data.get("queue") or {}

        if queue_info.get("deleted") is True:
            logger.warning(
                "Очередь %s удалена (deleted=true в ответе API), доступ невозможен: %s",
                display_name,
                messages,
            )
            return "not_found", None

        logger.error(
            "Нет прав на просмотр permissions очереди %s (HTTP 403). Ответ API: %s",
            display_name,
            messages,
        )
        return "forbidden", None

    if response.status_code == 404:
        logger.warning(
            "Очередь %s недоступна (HTTP 404) при проверке прав пользователя %s. "
            "Вероятно, очередь удалена или в архиве. Ответ API: %s",
            display_name,
            login,
            messages,
        )
        return "not_found", None

    logger.error(
        "Не удалось получить права пользователя %s в очереди %s: HTTP %s: %s",
        login,
        display_name,
        response.status_code,
        messages,
    )
    return "error", None


def parse_user_permission_status(data: dict) -> Tuple[bool, bool, bool]:
    """
    Разбирает ответ GET /v3/queues/<id>/permissions/users/<login>.

    Ключи блоков в этой ручке - ВЕРХНИМ регистром: CREATE/READ/WRITE/GRANT/DENY
    (в отличие от общей ручки /permissions, где ключи нижним регистром).

    Возвращает:
    - has_personal_access: есть ли у пользователя ЛИЧНАЯ (персональная)
      запись хоть в одном из блоков (не через группу/роль);
    - has_group_or_role_access: есть ли у пользователя доступ ТОЛЬКО через
      группу (например "Все сотрудники") или через роль (например
      "assignee" по текущей задаче) - это не персональная запись, её
      нельзя "снять" точечным PATCH по конкретному пользователю;
    - has_read: есть ли у пользователя доступ READ (лично или через
      группу/роль) - главный признак, нужно ли добавлять право чтения.
    """
    if not isinstance(data, dict):
        return False, False, False

    permissions = data.get("permissions") or {}
    if not isinstance(permissions, dict):
        return False, False, False

    def has_personal(block) -> bool:
        return isinstance(block, dict) and bool(block.get("users"))

    def has_group_or_role(block) -> bool:
        return isinstance(block, dict) and bool(block.get("groups") or block.get("roles"))

    read_block = permissions.get("READ") or {}
    has_read = has_personal(read_block) or has_group_or_role(read_block)

    has_personal_access = any(
        has_personal(permissions.get(name))
        for name in ("CREATE", "READ", "WRITE", "GRANT", "DENY")
    )
    has_group_or_role_access = any(
        has_group_or_role(permissions.get(name))
        for name in ("CREATE", "READ", "WRITE", "GRANT", "DENY")
    )

    return has_personal_access, has_group_or_role_access, has_read


# ============================================================
# Выдача прав (PATCH /v3/queues/<id>/permissions)
# ============================================================


def grant_read_permissions(queue_id: str, logger: logging.Logger) -> bool:
    """
    Аддитивно добавляет GRANT_ACCOUNT_LOGIN в блок read.users.add.
    Это НЕ перезаписывает список читателей и никак не затрагивает
    другие блоки (create/write/grant/deny), другие персональные
    записи, группы (например "Все сотрудники") и роли.
    """
    payload = {"read": {"users": {"add": [GRANT_ACCOUNT_LOGIN]}}}

    response = call_api(
        "PATCH",
        f"/v3/queues/{quote(queue_id, safe='')}/permissions",
        payload=payload,
        expected_codes=(200, 400, 403, 404, 409, 422),
        logger=logger,
        rate_limit_key="manage_access",
    )

    if response is None:
        logger.error("Нет ответа при выдаче права чтения для очереди %s", queue_id)
        return False

    if response.status_code in (200, 201, 204):
        return True

    if response.status_code == 409:
        logger.info("Право чтения уже есть в очереди %s (HTTP 409)", queue_id)
        return True

    body = safe_json(response)
    messages = body.get("errorMessages") or body.get("errors") or body or response.text[:500]

    if response.status_code == 403:
        logger.error(
            "Недостаточно прав для выдачи права чтения в очереди %s. Ответ API: %s",
            queue_id,
            messages,
        )
        return False

    if response.status_code == 404:
        logger.error(
            "Очередь %s не найдена при выдаче прав (404). Ответ API: %s",
            queue_id,
            messages,
        )
        return False

    logger.error(
        "Не удалось выдать право чтения для очереди %s: HTTP %s: %s",
        queue_id,
        response.status_code,
        messages,
    )
    return False


# ============================================================
# Отчеты
# ============================================================


def format_report(
    started_at: datetime,
    finished_at: datetime,
    added_to_queue: List[str],
    granted_read: List[str],
    skipped: List[str],
    deleted_or_inaccessible: List[str],
    failed: List[str],
    interrupted: bool,
) -> str:
    lines = []

    lines.append(
        f"=== Запуск {started_at.isoformat(timespec='seconds')} "
        f"(завершен {finished_at.isoformat(timespec='seconds')}"
        f"{', ПРЕРВАН Ctrl-C' if interrupted else ''}) ==="
    )

    lines.append(f"Режим: {'DRY_RUN' if DRY_RUN else 'REAL'}")
    lines.append(f"Аккаунт: {GRANT_ACCOUNT_LOGIN}")

    lines.append(f"Добавлено в очередь с правом чтения: {len(added_to_queue)}")
    if added_to_queue:
        for queue_name in added_to_queue:
            lines.append(f"  NEW_IN_QUEUE: {queue_name}")

    lines.append(
        f"Выдано право чтения уже существующим участникам: {len(granted_read)}"
    )
    if granted_read:
        for queue_name in granted_read:
            lines.append(f"  READ_GRANTED: {queue_name}")

    lines.append(f"Пропущено, право чтения уже есть: {len(skipped)}")

    if deleted_or_inaccessible:
        lines.append(
            f"Удалены / в архиве / недоступны (не ошибка): {len(deleted_or_inaccessible)}"
        )
        for queue_name in deleted_or_inaccessible:
            lines.append(f"  DELETED_OR_NO_ACCESS: {queue_name}")

    if failed:
        lines.append(f"С реальными ошибками: {len(failed)}")
        for queue_name in failed:
            lines.append(f"  FAILED: {queue_name}")

    return "\n".join(lines) + "\n"


def write_reports(
    started_at: datetime,
    finished_at: datetime,
    added_to_queue: List[str],
    granted_read: List[str],
    skipped: List[str],
    deleted_or_inaccessible: List[str],
    failed: List[str],
    interrupted: bool,
) -> Tuple[Path, Path]:
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    content = format_report(
        started_at,
        finished_at,
        added_to_queue,
        granted_read,
        skipped,
        deleted_or_inaccessible,
        failed,
        interrupted,
    )

    daily_report = LOG_DIR / f"added_{started_at.date().isoformat()}.log"
    with open(daily_report, "a", encoding="utf-8") as file:
        file.write(content)
        file.write("\n")

    run_report = LOG_DIR / f"run_{started_at.strftime('%Y%m%d_%H%M%S_%f')}.log"
    with open(run_report, "w", encoding="utf-8") as file:
        file.write(content)

    return daily_report, run_report


# ============================================================
# Основной прогон
# ============================================================


def run_once(logger: logging.Logger) -> int:
    started_at = datetime.now()
    critical_error = False
    interrupted = False

    added_to_queue: List[str] = []
    granted_read: List[str] = []
    skipped: List[str] = []
    deleted_or_inaccessible: List[str] = []
    failed: List[str] = []

    logger.info("=== Начало разового обхода очередей ===")
    logger.info("Аккаунт для выдачи права чтения: %s", GRANT_ACCOUNT_LOGIN)
    logger.info("DRY_RUN: %s", DRY_RUN)
    logger.info("Ограничение RPS на ручку API: %s", RPS_LIMIT)

    try:
        queues = get_all_queues(logger)
        logger.info("Получено очередей: %s", len(queues))

        try:
            for queue in queues:
                queue_id = queue_api_id(queue)
                display_name = queue_display(queue)

                try:
                    status, data = get_user_queue_permissions(
                        queue_id, GRANT_ACCOUNT_LOGIN, display_name, logger
                    )

                    if status == "not_found":
                        deleted_or_inaccessible.append(display_name)
                        continue

                    if status in ("forbidden", "error"):
                        failed.append(display_name)
                        continue

                    has_personal_access, has_group_or_role_access, has_read = (
                        parse_user_permission_status(data)
                    )

                    if has_read:
                        logger.info(
                            "Пропуск: право чтения уже есть в очереди %s",
                            display_name,
                        )
                        skipped.append(display_name)
                        continue

                    # Текст лога уточняем в зависимости от того, ЧЕМ именно
                    # обусловлено присутствие пользователя в очереди -
                    # персональной записью или доступом через группу/роль.
                    if has_personal_access:
                        situation = "личная запись без права чтения"
                    elif has_group_or_role_access:
                        situation = "доступ только через группу/роль (не персонально)"
                    else:
                        situation = "пользователя нет в очереди"

                    if DRY_RUN:
                        if has_personal_access or has_group_or_role_access:
                            logger.info(
                                "[DRY_RUN] %s, будет добавлено персональное право чтения: %s",
                                situation,
                                display_name,
                            )
                            granted_read.append(display_name)
                        else:
                            logger.info(
                                "[DRY_RUN] %s, будет добавлена с правом чтения: %s",
                                situation,
                                display_name,
                            )
                            added_to_queue.append(display_name)

                        continue

                    if grant_read_permissions(queue_id, logger):
                        if has_personal_access or has_group_or_role_access:
                            logger.info(
                                "%s, добавлено персональное право чтения: %s",
                                situation,
                                display_name,
                            )
                            granted_read.append(display_name)
                        else:
                            logger.info(
                                "%s, добавлена с правом чтения: %s",
                                situation,
                                display_name,
                            )
                            added_to_queue.append(display_name)
                    else:
                        failed.append(display_name)

                except Exception as exc:
                    logger.exception(
                        "Ошибка при обработке очереди %s: %s", display_name, exc
                    )
                    failed.append(display_name)

                if REQUEST_DELAY > 0:
                    time.sleep(REQUEST_DELAY)

        except KeyboardInterrupt:
            interrupted = True
            logger.warning(
                "Получен Ctrl-C во время обхода очередей — прерываю обработку, "
                "формирую отчёт по уже обработанным очередям"
            )

    except KeyboardInterrupt:
        interrupted = True
        logger.warning("Получен Ctrl-C во время получения списка очередей")

    except Exception as exc:
        logger.exception("Критическая ошибка запуска: %s", exc)
        critical_error = True

    finished_at = datetime.now()

    daily_report, run_report = write_reports(
        started_at=started_at,
        finished_at=finished_at,
        added_to_queue=added_to_queue,
        granted_read=granted_read,
        skipped=skipped,
        deleted_or_inaccessible=deleted_or_inaccessible,
        failed=failed,
        interrupted=interrupted,
    )

    logger.info(
        "Итог: добавлено в очередь %s, выдано право чтения %s, пропущено %s, "
        "удалено/недоступно %s, с ошибками %s%s",
        len(added_to_queue),
        len(granted_read),
        len(skipped),
        len(deleted_or_inaccessible),
        len(failed),
        " (ПРЕРВАНО Ctrl-C)" if interrupted else "",
    )

    if added_to_queue:
        logger.info("Добавлены в очередь: %s", ", ".join(added_to_queue))

    if granted_read:
        logger.info("Выдано право чтения: %s", ", ".join(granted_read))

    if not added_to_queue and not granted_read:
        logger.info("Изменений не было")

    logger.info("Дневной отчет: %s", daily_report)
    logger.info("Отчет за запуск: %s", run_report)

    if interrupted:
        return 130  # стандартный код завершения процесса по SIGINT

    if critical_error or failed:
        return 1

    return 0


# ============================================================
# Точка входа
# ============================================================


def main() -> None:
    global LOGGER, SESSION, GRANT_ACCOUNT_LOGIN

    LOGGER = configure_logging()

    if ENV_FILE_PATH:
        LOGGER.info("Загружен файл окружения: %s", ENV_FILE_PATH)
    else:
        LOGGER.warning(
            "Файл param.env не найден. Используются переменные окружения системы."
        )

    validate_config(LOGGER)
    SESSION = make_session()

    # Приводим логин из конфига к формату, который принимает
    # ручка /v3/queues/<id>/permissions/users/<login> (без @домена).
    GRANT_ACCOUNT_LOGIN = resolve_grant_account_login(GRANT_ACCOUNT_LOGIN, LOGGER)

    exit_code = run_once(LOGGER)
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
