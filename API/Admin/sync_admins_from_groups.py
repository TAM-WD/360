"""
Скрипт предназначен для синхронизации роли администратора организации по указанным группам.

В папке со скриптом нужно создать .env файл с переменными окружения:

ADMIN_TOKEN=y0__aaaaa # токен с правами directory:read_groups directory:write_users
ORGID=12345678 # ID организации
GROUP_IDS=9,12 # ID групп, которым нужно назначать права админа
BOT_TOKEN=y0__bbbbb # (опционально) токен бота Мессенджера
CHAT_ID=0/0/abcdefgh-1234-5678-90ab-cdefghijklmn # (опционально) ID чата, в который нужно отправлять уведомления
DRYRUN=true # флаг тестового запуска. Используйте значение true, чтобы проверить, какие изменения будут внесены пользователям 
"""


from dataclasses import dataclass
from environs import Env, exceptions
import logging
import logging.handlers
import sys
from pathlib import Path
import requests
from typing import Any, Dict, Optional, Union
import json



log = logging.getLogger(__name__)

DEFAULT_FORMAT = (
    "[%(asctime)s] %(levelname)-8s %(name)s "
    "%(filename)s:%(lineno)d — %(message)s"
)

DEFAULT_MAX_BYTES = 2 * 1024 * 1024
DEFAULT_BACKUP_COUNT = 10

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_DEFAULT_LOG_DIR = _PROJECT_ROOT / "logs"


def setup_logging(
    *,
    log_dir: str | Path = _DEFAULT_LOG_DIR,
    log_file: str = f"{Path(__file__).stem}.log",
    file_level: int | str = logging.DEBUG,
    console_level: int | str = logging.INFO,
    fmt: str = DEFAULT_FORMAT,
    max_bytes: int = DEFAULT_MAX_BYTES,
    backup_count: int = DEFAULT_BACKUP_COUNT,
) -> Path:
    """Настроить root-логгер: файл с ротацией + консоль.

    Возвращает путь к активному файлу лога — удобно писать в стартовое
    сообщение, чтобы админ сразу видел, куда смотреть.

    Относительный `log_dir` резолвится от корня проекта (родителя
    `settings/`), а не от текущей рабочей директории — иначе при запуске
    из произвольной папки логи разъедутся куда попало.
    """
    log_dir = Path(log_dir)
    if not log_dir.is_absolute():
        log_dir = _PROJECT_ROOT / log_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / log_file

    formatter = logging.Formatter(fmt)

    file_handler = logging.handlers.RotatingFileHandler(
        log_path,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(_min_level(file_level, console_level))

    for h in list(root.handlers):
        root.removeHandler(h)
        h.close()

    root.addHandler(file_handler)
    root.addHandler(console_handler)

    return log_path


def _min_level(a: int | str, b: int | str) -> int:
    """logging.getLevelName принимает и строку, и int; возвращает
    противоположное. Нормализуем к int и берём минимум.
    """
    return min(_as_int_level(a), _as_int_level(b))


def _as_int_level(level: int | str) -> int:
    if isinstance(level, int):
        return level
    resolved = logging.getLevelName(level.upper())
    if not isinstance(resolved, int):
        raise ValueError(f"unknown log level: {level!r}")
    return resolved

log_path = setup_logging()


@dataclass
class Config:
    dryrun: bool
    orgid: int
    admin_token: str
    group_ids: list[int]
    bot_token: Optional[str] = None
    chat_id: Optional[str] = None

def load_config(path: str | None = None) -> Config:
    """init config from env"""
    env = Env()
    env.read_env(path)

    try:
        config = Config(
            dryrun = env.bool("DRYRUN"),
            orgid = env("ORGID"),
            admin_token = env("ADMIN_TOKEN"),
            group_ids = env.list("GROUP_IDS")
        )
    except Exception as e:
        log.error('one of the evirons is missing %s', e)
        raise e

    try:
        config.bot_token = env("BOT_TOKEN")
        config.chat_id = env("CHAT_ID")
    except exceptions.EnvError as e:
        log.info('bot_token or chat_id is missing, sending message will be skipped %s', e)

    return config


def make_request(
    url: str,
    method: str,
    params: Optional[Dict[str, Any]] = None,
    body: Optional[Union[Dict, str, bytes]] = None,
    headers: Optional[Dict[str, str]] = None,
    timeout: int = 30,
    verify_ssl: bool = True
) -> Optional[Union[Dict, str, bytes]]:
    """Universal function for requests"""

    try:
        method = method.upper()
        if headers is None:
            headers = {}

        # Обычный JSON запрос
        elif isinstance(body, dict):
            if 'Content-Type' not in headers:
                headers['Content-Type'] = 'application/json'
            response = requests.request(
                method=method,
                url=url,
                params=params,
                data=json.dumps(body),
                headers=headers,
                timeout=timeout,
                verify=verify_ssl
            )

        # Строка или bytes или None
        else:
            response = requests.request(
                method=method,
                url=url,
                params=params,
                data=body,
                headers=headers,
                timeout=timeout,
                verify=verify_ssl
            )
        log.info("making request: %s %s", response.status_code, url)
        response.raise_for_status()
        response.encoding = 'utf-8'
            
        # Для остальных запросов пытаемся парсить JSON
        try:
            return response.json()
        except json.JSONDecodeError:
            log.error("Error occured while parsing JSON: %s", response.text)
            return response.text if response.text else response.content

    except Exception as e:
        log.error("Error occured while making request: %s", e)
        return None

def send_text(token: str, chat_id: str, text: str) -> None:
    url = 'https://botapi.messenger.yandex.net/bot/v1/messages/sendText/'
    body = {"chat_id": chat_id, "text": text}
    headers = {"Authorization": f"OAuth {token}"}
    return make_request(url, method='post', body=body, headers=headers)

def get_group_users(token: str, orgId: int, groupId: int) -> Dict[str, Any] | None:
    url = f'https://api360.yandex.net/directory/v2/org/{orgId}/groups/{groupId}/members'
    headers = {"Authorization": f"OAuth {token}"}
    return make_request(url, method="GET", headers=headers)

def patch_user(token: str, orgId: int, userId: str, body: Dict[str, Any]) -> Dict[str, Any] | None:
    url = f'https://api360.yandex.net/directory/v1/org/{orgId}/users/{userId}'
    headers = {"Authorization": f"OAuth {token}"}
    return make_request(url, method="PATCH", headers=headers, body=body)



ADMIN_GROUP_ID = 2

MESSENGER_TEXT_LIMIT = 6000


def get_group_users_map(token: str, orgId: int, groupId: int) -> Dict[str, str]:
    """Вернуть участников группы как отображение id -> nickname.

    Запросы к Directory API идут по id (он стабилен и обязателен для
    PATCH), а nickname тащим рядом, чтобы показывать человекочитаемые
    логины в сообщениях бота.
    """
    users_map: Dict[str, str] = {}
    response = get_group_users(token, orgId, groupId)
    if response:
        for user in response.get("users") or []:
            user_id = user.get("id")
            if user_id:
                users_map[user_id] = user.get("nickname") or user_id
    return users_map


def format_user_line(user_id: str, nicknames: Dict[str, str]) -> str:
    """Одна строка на пользователя: `nickname (id)`."""
    nickname = nicknames.get(user_id)
    return f"{nickname} ({user_id})" if nickname else user_id


def format_user_block(user_ids: set[str], nicknames: Dict[str, str]) -> str:
    """Список пользователей — по одному в строке, отсортированы по логину."""
    if not user_ids:
        return "—"
    ordered = sorted(user_ids, key=lambda uid: nicknames.get(uid, uid).lower())
    return "\n".join(format_user_line(uid, nicknames) for uid in ordered)


def format_section(title: str, user_ids: set[str], nicknames: Dict[str, str]) -> str:
    """Заголовок категории + список пользователей в ``` блоке."""
    return f"{title}:\n```\n{format_user_block(user_ids, nicknames)}\n```"


def build_report_sections(
    dryrun: bool,
    users_to_add: set[str],
    users_to_remove: set[str],
    all_admins: set[str],
    nicknames: Dict[str, str],
) -> list[str]:
    """Отчёт для бота, по секции на категорию — каждый список в своём ``` блоке.

    Секции возвращаются раздельно, чтобы при превышении лимита Мессенджера
    их можно было разослать по одной (одна категория — одно сообщение).
    """
    return [
        f"Is dryrun: `{dryrun}`" if dryrun else "",
        format_section("Added admins", users_to_add, nicknames),
        format_section("Removed admins", users_to_remove, nicknames),
        format_section("All admins", all_admins, nicknames),
    ]


def send_report(config: Config, sections: list[str]) -> None:
    """Отправить отчёт: одним сообщением, если помещается, иначе по секциям."""
    combined = "\n\n".join(sections)
    if len(combined) <= MESSENGER_TEXT_LIMIT:
        send_messages(config, combined)
        return
    for section in sections:
        send_messages(config, section)


def send_messages(config: Config, text: str) -> None:
    """Отправить текст в чат, разбивая на части по лимиту Мессенджера.

    Разрез идёт по строкам, чтобы не рвать ``` блок посреди строки; если
    одна секция всё равно длиннее лимита — режем жёстко по символам.
    """
    for chunk in _split_by_limit(text, MESSENGER_TEXT_LIMIT):
        send_text(config.bot_token, config.chat_id, chunk)


def _split_by_limit(text: str, limit: int) -> list[str]:
    if len(text) <= limit:
        return [text]

    chunks: list[str] = []
    current = ""
    for line in text.split("\n"):
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) <= limit:
            current = candidate
            continue
        if current:
            chunks.append(current)

        while len(line) > limit:
            chunks.append(line[:limit])
            line = line[limit:]
        current = line
    if current:
        chunks.append(current)
    return chunks


def main(config: Config):
    nicknames: Dict[str, str] = {}

    current_admins = get_group_users_map(config.admin_token, config.orgid, ADMIN_GROUP_ID)
    nicknames.update(current_admins)
    current_admin_ids = set(current_admins)
    log.debug("Current admin ids: %s", current_admin_ids)

    group_users_ids: set[str] = set()
    for group_id in config.group_ids:
        members = get_group_users_map(config.admin_token, config.orgid, group_id)
        nicknames.update(members)
        group_users_ids.update(members)
    log.debug("Group users ids: %s", group_users_ids)

    users_to_add_admin = group_users_ids - current_admin_ids
    users_to_remove_admin = current_admin_ids - group_users_ids
    log.info("Users to add admin: %s", users_to_add_admin)
    log.info("Users to remove admin: %s", users_to_remove_admin)

    if not config.dryrun:
        for user_id in users_to_add_admin:
            log.info("Adding admin to user: %s", format_user_line(user_id, nicknames))
            patch_user(config.admin_token, config.orgid, user_id, {"isAdmin": True})
        for user_id in users_to_remove_admin:
            log.info("Removing admin from user: %s", format_user_line(user_id, nicknames))
            patch_user(config.admin_token, config.orgid, user_id, {"isAdmin": False})
    else:
        log.info("Dryrun is active, skipping user changes")

    if config.bot_token and config.chat_id:
        final_admins = get_group_users_map(config.admin_token, config.orgid, ADMIN_GROUP_ID)
        nicknames.update(final_admins)

        sections = build_report_sections(
            dryrun=config.dryrun,
            users_to_add=users_to_add_admin,
            users_to_remove=users_to_remove_admin,
            all_admins=set(final_admins),
            nicknames=nicknames,
        )
        send_report(config, sections)


if __name__ == "__main__":
    config = load_config()
    main(config)