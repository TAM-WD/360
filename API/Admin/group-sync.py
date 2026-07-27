#!/usr/bin/env python3
"""
Скрипт для распределения сотрудников организации Яндекс 360 по группам рассылки.
Обходит ограничение групп, автоматически разбивая всех сотрудников на несколько групп с 
настраиваемым префиксом (по умолчанию SENDING 1, SENDING 2, ... ). 
Скрипт идемпотентен: повторный запуск безопасно доливает только тех, кто ещё не размещён, без создания дублей.

ТРЕБОВАНИЯ:
Python 3.10+
Библиотека requests. Установить её можно так: pip install requests

ДОСТУПЫ И ТОКЕНЫ:
OAuth-токен со следующими правами:
  directory:read_users
  directory:read_groups
  directory:write_groups

НАСТРОЙКА ПЕРЕД ЗАПУСКОМ:
Скрипт не требует правки кода. Все параметры передаются аргументами командной строки.
Токен рекомендуется задавать через переменную окружения, а не в командной строке
(иначе он попадёт в историю оболочки и будет виден в списке процессов):

  Linux/macOS:  export YANDEX_360_TOKEN="ваш_oauth_токен"
  Windows CMD:  set YANDEX_360_TOKEN=ваш_oauth_токен
  Windows PS:   $env:YANDEX_360_TOKEN="ваш_oauth_токен"

ОСНОВНЫЕ ПАРАМЕТРЫ ЗАПУСКА:
  --org-id            (обязательный) Идентификатор организации.
  --token             OAuth-токен. Если не указан — берётся из YANDEX_360_TOKEN.
  --group-prefix      Префикс имён групп. Может быть кириллицей. По умолчанию 'SENDING'.
  --label-prefix      ASCII-префикс для почтового login группы (label). Если не задан —
                      автоматически транслитерируется из --group-prefix.
  --capacity          Максимум участников в одной группе. По умолчанию 9999.
  --dry-run           Тестовый прогон: показывает план и пишет отчёт, НО ничего не меняет.
  --include-dismissed Включать уволенных сотрудников (по умолчанию исключены).
  --include-robots    Включать служебных роботов (по умолчанию исключены).
  --include-disabled  Включать заблокированные аккаунты (по умолчанию исключены).
  --report-dir        Корневая папка для логов. По умолчанию — рядом со скриптом.
  --add-batch         Размер пачки при добавлении участников. По умолчанию 500.
  --page-size         Размер страницы при чтении сотрудников/групп. По умолчанию 1000.
  --members-page      Размер страницы при чтении состава группы. По умолчанию 1000.
  -v, --verbose       Подробный вывод (DEBUG-логи).

ЛОГИКА РАБОТЫ (идемпотентная, безопасная для повторных запусков):
  1. Читаем все группы -> находим существующие '<PREFIX> N' по имени (name).
  2. Для каждой такой группы читаем ФАКТИЧЕСКИЙ состав через members-эндпоинт
     -> строим множество уже размещённых user_id и реальную заполненность.
  3. Читаем всех сотрудников (с учётом фильтров include-*).
  4. Нераспределённых (нет ни в одной группе) доливаем: сначала в существующие
     группы с запасом ёмкости до capacity, затем создаём новые '<PREFIX> N'.
  5. Добавляем участников пачками через members/add.
  6. Пишем отчёт (CSV + JSON + run.log) в папку "<ScriptName> Logs".

РЕЖИМЫ РАБОТЫ:
  Режим 1: Тестовый прогон (ОБЯЗАТЕЛЬНО начинать с него)
    python group-sync.py --org-id 123456 --dry-run -v
    Результат: покажет план распределения и запишет отчёт, но НЕ изменит группы.

  Режим 2: Боевой запуск
    python group-sync.py --org-id 123456 -v
    Результат: создаст недостающие группы и добавит в них нераспределённых сотрудников.

ЗАПУСК:

  Windows
    Откройте командную строку (cmd)
    Перейдите в папку со скриптом:
      cd C:\путь\к\скрипту
    Задайте токен и запустите:
      set YANDEX_360_TOKEN=ваш_токен
      python group-sync.py --org-id 123456 --dry-run -v

  Linux/macOS
      cd /путь/к/скрипту
      export YANDEX_360_TOKEN="ваш_токен"
      python3 group-sync.py --org-id 123456 --dry-run -v

РЕЗУЛЬТАТЫ РАБОТЫ СКРИПТА:
Все отчёты сохраняются в папку "<ScriptName> Logs/<дата_время>_<режим>/", где режим —
это dry-run или apply. Внутри создаются файлы:

  summary.json    — сводка прогона: параметры запуска, статистика, счётчики статусов.
  assignments.csv — построчный маппинг «сотрудник -> группа». Формат полей:
       Название поля   Описание поля
       user_id         Идентификатор сотрудника
       group_index     Порядковый номер группы (1, 2, 3, ...)
       group_name      Название группы (например, 'Рассылка 1')
       group_id        Идентификатор группы (пусто в dry-run для новых групп)
       action          Тип действия: add (долив) или create+add (создание + долив)
       status          planned (dry-run) | added (успех) | failed (ошибка)
       error           Текст ошибки, если status=failed
  errors.csv      — только строки со status=failed (создаётся при наличии ошибок).
  run.log         — полный лог прогона.

ОТКАЗОУСТОЙЧИВОСТЬ:
  - Автоматические повторы (retry) на ошибки API 429/5xx и обрывы соединения
    с экспоненциальной задержкой и учётом заголовка Retry-After.
  - При длительной потере интернет-соединения скрипт ждёт восстановления
    (проверка каждые 10 секунд, максимум до 1 часа) и продолжает работу.
  - Идемпотентность: если скрипт упал или был прерван, повторный запуск с тем же
    --group-prefix безопасно дольёт только недостающих сотрудников.
  - Остановка по Ctrl+C: отчёт по выполненной части сохраняется.

КОДЫ ВОЗВРАТА (для мониторинга/CI/cron):
  0   — успешно, ошибок нет.
  1   — были ошибки при добавлении или критический сбой (см. errors.csv).
  2   — не задан токен.
  130 — прервано пользователем (Ctrl+C).

ВОЗМОЖНЫЕ ПРОБЛЕМЫ:

  Ошибка: "Не задан токен (--token или YANDEX_360_TOKEN)"
  Решение: задайте переменную окружения YANDEX_360_TOKEN или передайте --token.

  Ошибка: HTTP 403 Forbidden
  Причина: у токена нет нужных прав.
  Решение: выдайте приложению directory:read_users, directory:read_groups,
           directory:write_groups.

  Ошибка: HTTP 400 login.prohibitedsymbols при создании группы
  Причина: в почтовый login (label) попали недопустимые символы (например, кириллица).
  Решение: задайте корректный ASCII-логин через --label-prefix.

  Предупреждение в логе: "РАСХОЖДЕНИЕ: members_count=... а из состава получено ..."
  Причина: количество участников по данным группы не совпало с фактическим составом.
  Решение: скрипт использует фактический состав и продолжает работу; при сильном
           расхождении проверьте пагинацию через --members-page.

  Проблема: скрипт создаёт лишние группы / дубли сотрудников
  Причина: запуск с другим --group-prefix, чем в прошлый раз.
  Решение: используйте один и тот же --group-prefix для одной задачи рассылки.

ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ:

  Пример 1: Проверка плана без изменений (всегда начинать с этого)
    python group-sync.py --org-id 123456 --dry-run -v

  Пример 2: Боевое распределение с префиксом по умолчанию (SENDING)
    python group-sync.py --org-id 123456 -v

  Пример 3: Кириллический префикс с автоматической транслитерацией login
    python group-sync.py --org-id 123456 --group-prefix "Рассылка" -v

  Пример 4: Кириллическое имя + явный ASCII-логin почты
    python group-sync.py --org-id 123456 --group-prefix "Рассылка" --label-prefix "mailing" -v

  Пример 5: Уменьшенная ёмкость групп и включение всех категорий сотрудников
    python group-sync.py --org-id 123456 --capacity 9000 \
        --include-dismissed --include-robots --include-disabled -v

ПРИ ВОЗНИКНОВЕНИИ ПРОБЛЕМ ПРОВЕРЬТЕ:
  - Лог-файл (run.log) и summary.json в папке "<ScriptName> Logs".
  - Права доступа OAuth-токена.
  - Наличие интернет-соединения.
  - Что используется один и тот же --group-prefix между запусками.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import socket
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --------------------------------------------------------------------------- #
# Конфигурация
# --------------------------------------------------------------------------- #

CLOUD_API_BASE = "https://cloud-api.yandex.net/v1/directory"   # users/groups read
API360_V1_BASE = "https://api360.yandex.net/directory/v1"       # create/patch group
API360_V2_BASE = "https://api360.yandex.net/directory/v2"       # members list / add

DEFAULT_CAPACITY = 9999          # запас относительно жёсткого лимита 10000
DEFAULT_PAGE_SIZE = 1000         # размер страницы при чтении users/groups
DEFAULT_ADD_BATCH = 500          # сколько userIds отправляем в одном members/add
DEFAULT_MEMBERS_PAGE = 1000      # размер страницы при чтении состава группы
DEFAULT_GROUP_PREFIX = "SENDING" # префикс имён групп (переопределяется --group-prefix)

# Хост для проверки доступности сети
NETWORK_PROBE_HOST = "api360.yandex.net"
NETWORK_PROBE_PORT = 443

log = logging.getLogger("sending_splitter")


# --------------------------------------------------------------------------- #
# Отказоустойчивость: ожидание сети + прикладной retry
# --------------------------------------------------------------------------- #

class NetworkError(RuntimeError):
    """Сетевая ошибка, при которой имеет смысл ждать восстановления связи."""


def wait_for_network(host: str = NETWORK_PROBE_HOST,
                     port: int = NETWORK_PROBE_PORT,
                     timeout: float = 5.0,
                     max_wait: float = 3600.0,
                     probe_interval: float = 10.0) -> None:
    """
    Блокируется, пока не появится сеть (TCP-коннект до host:port),
    но не дольше max_wait. Полезно при длительных обрывах интернета.
    """
    waited = 0.0
    while waited < max_wait:
        try:
            with socket.create_connection((host, port), timeout=timeout):
                if waited > 0:
                    log.info("Сеть восстановлена (ждали %.0f c).", waited)
                return
        except OSError:
            log.warning("Нет сети. Ждём %.0f c... (всего ждём %.0f c)",
                        probe_interval, waited)
            time.sleep(probe_interval)
            waited += probe_interval
    raise NetworkError(f"Сеть не восстановилась за {max_wait:.0f} c.")


def resilient(max_attempts: int = 6, base_delay: float = 2.0,
              max_delay: float = 60.0):
    """
    Декоратор прикладного retry поверх сетевого слоя.
    Обрабатывает ситуации, которые не ловит urllib3.Retry:
      - длительный обрыв сети (ждём восстановления);
      - спорадические ConnectionError/Timeout между запросами.
    Экспоненциальный backoff с потолком.
    """
    def deco(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except (requests.ConnectionError, requests.Timeout,
                        socket.error) as exc:
                    if attempt >= max_attempts:
                        raise NetworkError(
                            f"{func.__name__}: исчерпаны попытки "
                            f"({max_attempts}). Последняя ошибка: {exc}"
                        ) from exc
                    # Сначала дождёмся, что сеть в принципе есть
                    wait_for_network()
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    log.warning("%s: сетевая ошибка (попытка %d/%d): %s. "
                                "Повтор через %.0f c.",
                                func.__name__, attempt, max_attempts, exc, delay)
                    time.sleep(delay)
        return wrapper
    return deco


# --------------------------------------------------------------------------- #
# Транслитерация для почтового label (ASCII-only)
# --------------------------------------------------------------------------- #

_TRANSLIT_MAP = {
    "а": "a", "б": "b", "в": "v", "г": "g", "д": "d", "е": "e", "ё": "e",
    "ж": "zh", "з": "z", "и": "i", "й": "y", "к": "k", "л": "l", "м": "m",
    "н": "n", "о": "o", "п": "p", "р": "r", "с": "s", "т": "t", "у": "u",
    "ф": "f", "х": "h", "ц": "ts", "ч": "ch", "ш": "sh", "щ": "sch",
    "ъ": "", "ы": "y", "ь": "", "э": "e", "ю": "yu", "я": "ya",
}


def transliterate(text: str) -> str:
    """Грубая транслитерация кириллицы в латиницу (для почтового login)."""
    out = []
    for ch in text.lower():
        out.append(_TRANSLIT_MAP.get(ch, ch))
    return "".join(out)


def make_label(prefix: str, index: int, label_prefix: str | None = None) -> str:
    """
    Формирует ASCII-безопасный login почтовой рассылки (label).
    login.prohibitedsymbols: разрешены только латиница, цифры, дефис.
    """
    base = label_prefix if label_prefix else transliterate(prefix)
    # оставляем только [a-z0-9-], всё прочее -> дефис
    base = re.sub(r"[^a-z0-9]+", "-", base.lower()).strip("-")
    if not base:
        base = "group"   # запасной вариант, если после чистки ничего не осталось
    return f"{base}-{index}"


# --------------------------------------------------------------------------- #
# Модели
# --------------------------------------------------------------------------- #

@dataclass
class User:
    id: str
    email: str
    is_robot: bool
    is_dismissed: bool
    is_enabled: bool

    @classmethod
    def from_json(cls, data: dict) -> "User":
        return cls(
            id=str(data["id"]),
            email=data.get("email", ""),
            is_robot=bool(data.get("is_robot", False)),
            is_dismissed=bool(data.get("is_dismissed", False)),
            is_enabled=bool(data.get("is_enabled", True)),
        )


@dataclass
class SendingGroup:
    id: int
    index: int
    name: str
    members_count: int


@dataclass
class PlanItem:
    index: int
    group: Optional[SendingGroup]   # None -> нужно создать
    user_ids: list[str]

    @property
    def is_new(self) -> bool:
        return self.group is None


# --------------------------------------------------------------------------- #
# HTTP-клиент
# --------------------------------------------------------------------------- #

class YandexDirectoryClient:
    def __init__(self, org_id: int, token: str,
                 page_size: int = DEFAULT_PAGE_SIZE,
                 members_page: int = DEFAULT_MEMBERS_PAGE,
                 timeout: int = 30):
        self.org_id = org_id
        self.page_size = page_size
        self.members_page = members_page
        self.timeout = timeout
        self.session = self._build_session(token)

    @staticmethod
    def _build_session(token: str) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=5,
            connect=5,
            read=5,
            backoff_factor=1.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST", "PATCH"]),
            respect_retry_after_header=True,
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=20)
        session.mount("https://", adapter)
        session.headers.update({
            "Authorization": f"OAuth {token}",
            "Content-Type": "application/json",
        })
        return session

    @resilient()
    def _request(self, method: str, url: str, **kwargs) -> dict:
        resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
        if resp.status_code >= 400:
            raise RuntimeError(
                f"{method} {url} -> {resp.status_code}: {resp.text[:500]}"
            )
        if not resp.content:
            return {}
        return resp.json()

    # ---- Пагинированное чтение (cloud-api: limit/offset/total) ----------- #

    def _paginate(self, url: str) -> Iterator[dict]:
        offset = 0
        while True:
            data = self._request(
                "GET", url,
                params={"limit": self.page_size, "offset": offset},
            )
            items = data.get("items", [])
            for item in items:
                yield item
            total = data.get("total", 0)
            offset += self.page_size
            if offset >= total or not items:
                break

    def iter_users(self) -> Iterator[User]:
        url = f"{CLOUD_API_BASE}/organizations/{self.org_id}/users"
        for raw in self._paginate(url):
            yield User.from_json(raw)

    def iter_groups(self) -> Iterator[dict]:
        url = f"{CLOUD_API_BASE}/organizations/{self.org_id}/groups"
        yield from self._paginate(url)

    # ---- Фактический состав группы (авторитетный источник членства) ------ #

    def get_group_member_user_ids(self, group_id: int) -> set[str]:
        """
        GET https://api360.yandex.net/directory/v2/org/{orgId}/groups/{groupId}/members

        Возвращает множество user_id, реально состоящих в группе.

        Поддержана пагинация через perPage/page (если эндпоинт её применяет).
        Если ответ не содержит признаков пагинации — читаем один раз.
        """
        url = f"{API360_V2_BASE}/org/{self.org_id}/groups/{group_id}/members"
        result: set[str] = set()

        page = 1
        while True:
            data = self._request(
                "GET", url,
                params={"perPage": self.members_page, "page": page},
            )
            users = data.get("users") or []
            for u in users:
                if "id" in u:
                    result.add(str(u["id"]))

            # Определяем, есть ли следующая страница.
            pages = data.get("pages")
            if pages is not None:
                # Эндпоинт пагинирует: идём до последней страницы.
                if page >= int(pages):
                    break
                page += 1
                continue

            # Признаков пагинации нет — считаем, что получили всё за один запрос.
            break

        return result

    # ---- Изменения ------------------------------------------------------- #

    def create_group(self, name: str, label: str,
                     description: str = "",
                     external_id: str = "") -> int:
        """
        POST https://api360.yandex.net/directory/v1/org/{orgId}/groups
        Возвращает id созданной группы (integer).
        """
        url = f"{API360_V1_BASE}/org/{self.org_id}/groups"
        body: dict = {
            "name": name,
            "label": label,
            "description": description,
        }
        if external_id:
            body["externalId"] = external_id
        data = self._request("POST", url, json=body)
        group_id = data.get("id")
        if group_id is None:
            raise RuntimeError(f"Не удалось получить id созданной группы: {data}")
        return int(group_id)

    def add_users_to_group(self, group_id: int, user_ids: list[str]) -> None:
        """
        PATCH https://api360.yandex.net/directory/v2/org/{orgId}/groups/{groupId}/members/add
        """
        url = f"{API360_V2_BASE}/org/{self.org_id}/groups/{group_id}/members/add"
        self._request("PATCH", url, json={"userIds": user_ids})


# --------------------------------------------------------------------------- #
# Отчёты
# --------------------------------------------------------------------------- #

class Reporter:
    """
    Пишет отчёт в папку "<ScriptName> Logs" (рядом со скриптом по умолчанию):
      <ScriptName> Logs/
          <timestamp>_<mode>/
              summary.json      — сводка (статистика, параметры, счётчики)
              assignments.csv   — построчно: user_id, group_index, group_name,
                                  group_id, action, status, error
              errors.csv        — только строки со status=failed (если есть)
              run.log           — полный лог прогона
    """

    def __init__(self, dry_run: bool, base_dir: str | None = None):
        script_name = Path(sys.argv[0]).stem or "script"
        root = Path(base_dir) if base_dir else Path(sys.argv[0]).resolve().parent
        logs_root = root / f"{script_name} Logs"

        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        suffix = "dry-run" if dry_run else "apply"
        self.run_dir = logs_root / f"{ts}_{suffix}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.dry_run = dry_run
        self._rows: list[dict] = []
        log.info("Отчёт будет сохранён в: %s", self.run_dir.resolve())

    def attach_file_logger(self) -> None:
        """Дублирует все логи в run.log внутри папки прогона."""
        handler = logging.FileHandler(self.run_dir / "run.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter(
            "%(asctime)s %(levelname)-7s %(message)s"))
        logging.getLogger().addHandler(handler)

    def add_row(self, *, user_id: str, group_index: int, group_name: str,
                group_id: int | None, action: str, status: str,
                error: str = "") -> None:
        self._rows.append({
            "user_id": user_id,
            "group_index": group_index,
            "group_name": group_name,
            "group_id": group_id if group_id is not None else "",
            "action": action,        # create+add | add
            "status": status,        # planned | added | failed
            "error": error,
        })

    def has_failures(self) -> bool:
        return any(r["status"] == "failed" for r in self._rows)

    def write(self, stats: dict, run_params: dict) -> None:
        fieldnames = ["user_id", "group_index", "group_name", "group_id",
                      "action", "status", "error"]

        csv_path = self.run_dir / "assignments.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self._rows)

        failed_rows = [r for r in self._rows if r["status"] == "failed"]
        if failed_rows:
            err_path = self.run_dir / "errors.csv"
            with err_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(failed_rows)
            log.warning("Обнаружены ошибки (%d). См. %s",
                        len(failed_rows), err_path.name)

        summary = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "dry_run": self.dry_run,
            "run_params": run_params,
            "stats": stats,
            "result_counts": self._count_by_status(),
        }
        summary_path = self.run_dir / "summary.json"
        with summary_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        log.info("Отчёт сохранён в %s", self.run_dir.resolve())

    def _count_by_status(self) -> dict:
        counts: dict[str, int] = {}
        for row in self._rows:
            counts[row["status"]] = counts.get(row["status"], 0) + 1
        return counts


# --------------------------------------------------------------------------- #
# Бизнес-логика
# --------------------------------------------------------------------------- #

def build_group_regex(prefix: str) -> re.Pattern:
    """Компилирует regex для поиска групп '<prefix> N' (re.escape на всякий случай)."""
    return re.compile(rf"^{re.escape(prefix)}\s*(\d+)$", re.IGNORECASE)


def parse_sending_index(name: str, pattern: re.Pattern) -> Optional[int]:
    m = pattern.match((name or "").strip())
    return int(m.group(1)) if m else None


def load_sending_groups(client: YandexDirectoryClient,
                        prefix: str) -> list[SendingGroup]:
    pattern = build_group_regex(prefix)
    groups: list[SendingGroup] = []
    for g in client.iter_groups():
        if g.get("removed"):
            continue
        idx = parse_sending_index(g.get("name", ""), pattern)
        if idx is None:
            continue
        groups.append(SendingGroup(
            id=int(g["id"]),
            index=idx,
            name=g["name"],
            members_count=int(g.get("members_count", 0)),
        ))
    groups.sort(key=lambda x: x.index)
    return groups


def collect_current_membership(
        client: YandexDirectoryClient,
        sending_groups: list[SendingGroup]) -> tuple[set[str], dict[int, int]]:
    """
    Строит АВТОРИТЕТНОЕ множество уже размещённых user_id и реальную
    заполненность каждой группы по фактическому составу (members-эндпоинт).
    """
    assigned_ids: set[str] = set()
    occupancy: dict[int, int] = {}
    for g in sending_groups:
        member_ids = client.get_group_member_user_ids(g.id)
        occupancy[g.id] = len(member_ids)
        assigned_ids |= member_ids
        log.info("Группа '%s' id=%s: фактически %d участников (members_count=%d)",
                 g.name, g.id, len(member_ids), g.members_count)
        if g.members_count and len(member_ids) != g.members_count:
            log.warning(
                "  РАСХОЖДЕНИЕ: members_count=%d, а из состава получено %d. "
                "Использую фактический состав.",
                g.members_count, len(member_ids))
    return assigned_ids, occupancy


def load_users(client: YandexDirectoryClient,
               include_dismissed: bool,
               include_robots: bool,
               include_disabled: bool) -> list[User]:
    users: list[User] = []
    for u in client.iter_users():
        if not include_dismissed and u.is_dismissed:
            continue
        if not include_robots and u.is_robot:
            continue
        if not include_disabled and not u.is_enabled:
            continue
        users.append(u)
    return users


def build_plan(users: list[User],
               sending_groups: list[SendingGroup],
               assigned_ids: set[str],
               occupancy: dict[int, int],
               capacity: int) -> tuple[list[PlanItem], dict]:
    """
    Возвращает (план, статистика).
    План — что и в какую группу дописать. Уже размещённых (assigned_ids) не трогаем.
    Распределённость определяется по фактическому составу групп (не user.groups).
    """
    unassigned: list[User] = [u for u in users if u.id not in assigned_ids]
    assigned_count = len(users) - len(unassigned)

    plan: list[PlanItem] = []
    q = unassigned
    qi = 0

    # 1. Доливаем существующие группы до capacity (по фактической заполненности)
    for g in sorted(sending_groups, key=lambda x: x.index):
        used = occupancy.get(g.id, 0)
        free = capacity - used
        if free <= 0:
            log.info("Группа '%s' заполнена (%d/%d) — пропускаем.",
                     g.name, used, capacity)
            continue
        batch = q[qi:qi + free]
        qi += len(batch)
        if batch:
            plan.append(PlanItem(g.index, g, [u.id for u in batch]))
        if qi >= len(q):
            break

    # 2. Создаём новые группы для остатка
    next_index = (max((g.index for g in sending_groups), default=0)) + 1
    while qi < len(q):
        batch = q[qi:qi + capacity]
        qi += len(batch)
        plan.append(PlanItem(next_index, None, [u.id for u in batch]))
        next_index += 1

    stats = {
        "total_users": len(users),
        "already_assigned": assigned_count,
        "to_distribute": len(unassigned),
        "existing_groups": len(sending_groups),
        "new_groups": sum(1 for p in plan if p.is_new),
    }
    return plan, stats


def execute_plan(client: YandexDirectoryClient,
                 plan: list[PlanItem],
                 add_batch: int,
                 dry_run: bool,
                 reporter: Reporter,
                 prefix: str,
                 label_prefix: str | None) -> None:
    for item in plan:
        group_id: int | None
        action = "add"

        if item.is_new:
            name = f"{prefix} {item.index}"                          # кириллица OK
            label = make_label(prefix, item.index, label_prefix)     # ASCII-safe
            action = "create+add"
            if dry_run:
                log.info("[dry-run] Создать группу '%s' (label=%s) и добавить %d чел.",
                         name, label, len(item.user_ids))
                group_id = None
            else:
                group_id = client.create_group(
                    name, label, description="Auto-created for mailing split")
                log.info("Создана группа '%s' (label=%s) id=%s",
                         name, label, group_id)
        else:
            name = item.group.name
            group_id = item.group.id
            log.info("Группа '%s' id=%s: доливаем %d чел.",
                     name, group_id, len(item.user_ids))

        added_total = 0  # накопительный счётчик по текущей группе

        for start in range(0, len(item.user_ids), add_batch):
            chunk = item.user_ids[start:start + add_batch]

            if dry_run:
                added_total += len(chunk)
                log.info("[dry-run] Добавлено %d участников в группу %s",
                         added_total, name)
                for uid in chunk:
                    reporter.add_row(user_id=uid, group_index=item.index,
                                     group_name=name, group_id=group_id,
                                     action=action, status="planned")
                continue

            try:
                client.add_users_to_group(group_id, chunk)
                added_total += len(chunk)
                log.info("Добавлено %d участников в группу %s",
                         added_total, name)
                for uid in chunk:
                    reporter.add_row(user_id=uid, group_index=item.index,
                                     group_name=name, group_id=group_id,
                                     action=action, status="added")
            except Exception as exc:  # noqa: BLE001
                log.error("   ОШИБКА при добавлении батча в '%s': %s", name, exc)
                for uid in chunk:
                    reporter.add_row(user_id=uid, group_index=item.index,
                                     group_name=name, group_id=group_id,
                                     action=action, status="failed",
                                     error=str(exc))


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #

def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Разделение сотрудников по группам <PREFIX>-N")
    p.add_argument("--org-id", type=int, required=True)
    p.add_argument("--token", default=os.environ.get("YANDEX_360_TOKEN"),
                   help="OAuth-токен (или переменная окружения YANDEX_360_TOKEN)")
    p.add_argument("--group-prefix", default=DEFAULT_GROUP_PREFIX,
                   help=f"Префикс имён групп рассылки, может быть кириллицей "
                        f"(по умолчанию '{DEFAULT_GROUP_PREFIX}')")
    p.add_argument("--label-prefix", default=None,
                   help="ASCII-префикс для почтового login группы (label). "
                        "Если не задан — транслитерируется из --group-prefix. "
                        "Пример: 'sending' -> sending-1@домен")
    p.add_argument("--capacity", type=int, default=DEFAULT_CAPACITY,
                   help=f"Макс. участников в группе (по умолчанию {DEFAULT_CAPACITY})")
    p.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    p.add_argument("--members-page", type=int, default=DEFAULT_MEMBERS_PAGE,
                   help="Размер страницы при чтении состава группы")
    p.add_argument("--add-batch", type=int, default=DEFAULT_ADD_BATCH)
    p.add_argument("--report-dir", default=None,
                   help="Корень для папки логов (по умолчанию — рядом со скриптом)")
    p.add_argument("--include-dismissed", action="store_true")
    p.add_argument("--include-robots", action="store_true")
    p.add_argument("--include-disabled", action="store_true")
    p.add_argument("--dry-run", action="store_true",
                   help="Только показать план и записать отчёт, ничего не менять")
    p.add_argument("-v", "--verbose", action="store_true")
    return p.parse_args(argv)


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )
    if not args.token:
        log.error("Не задан токен (--token или YANDEX_360_TOKEN).")
        return 2

    prefix = args.group_prefix

    client = YandexDirectoryClient(args.org_id, args.token,
                                   page_size=args.page_size,
                                   members_page=args.members_page)

    log.info("Читаем группы (префикс '%s')...", prefix)
    sending_groups = load_sending_groups(client, prefix)
    log.info("Найдено '%s'-групп: %d -> %s",
             prefix, len(sending_groups), [g.name for g in sending_groups])

    log.info("Читаем фактический состав групп...")
    assigned_ids, occupancy = collect_current_membership(client, sending_groups)
    log.info("Уже размещено уникальных сотрудников: %d", len(assigned_ids))

    log.info("Читаем сотрудников...")
    users = load_users(client,
                       include_dismissed=args.include_dismissed,
                       include_robots=args.include_robots,
                       include_disabled=args.include_disabled)
    log.info("Сотрудников к обработке: %d", len(users))

    plan, stats = build_plan(users, sending_groups, assigned_ids,
                             occupancy, args.capacity)
    log.info("Статистика: %s", stats)

    reporter = Reporter(dry_run=args.dry_run, base_dir=args.report_dir)
    reporter.attach_file_logger()

    run_params = {
        "org_id": args.org_id,
        "group_prefix": prefix,
        "label_prefix": args.label_prefix,
        "capacity": args.capacity,
        "page_size": args.page_size,
        "members_page": args.members_page,
        "add_batch": args.add_batch,
        "include_dismissed": args.include_dismissed,
        "include_robots": args.include_robots,
        "include_disabled": args.include_disabled,
    }

    if not plan:
        log.info("Все сотрудники уже распределены. Завершаю работу.")
        reporter.write(stats, run_params)
        return 0

    log.info("=== ПЛАН%s ===", " (DRY-RUN)" if args.dry_run else "")
    for item in plan:
        tag = "NEW" if item.is_new else "EXIST"
        log.info("  [%s] %s %d: +%d чел.",
                 tag, prefix, item.index, len(item.user_ids))

    exit_code = 0
    try:
        execute_plan(client, plan, args.add_batch, args.dry_run,
                     reporter, prefix, args.label_prefix)
    except KeyboardInterrupt:
        log.warning("Прервано пользователем (Ctrl+C). "
                    "Сохраняю отчёт по выполненной части. "
                    "Можно сделать повторный запуск с тем же префиксом '%s' "
                    "для добавления оставшихся сотрудников.", prefix)
        exit_code = 130
    except NetworkError as exc:
        log.error("Сетевой сбой: %s. Повторите запуск позже — "
                  "скрипт продолжит безопасно.", exc)
        exit_code = 1
    except Exception as exc:  # noqa: BLE001
        log.error("Критическая ошибка выполнения плана: %s", exc)
        exit_code = 1
    finally:
        reporter.write(stats, run_params)

    if reporter.has_failures() and exit_code == 0:
        exit_code = 1

    log.info("Готово. Код возврата: %d", exit_code)
    return exit_code


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv[1:]))
    except KeyboardInterrupt:
        log.warning("Прервано пользователем на этапе подготовки.")
        sys.exit(130)
