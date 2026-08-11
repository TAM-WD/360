from __future__ import annotations

import re
import unicodedata

# Формат логина мессенджер-бота Яндекса: yndx-mssngr-<идентификатор>-bot.
#
# Проверка по формату применяется ТОЛЬКО к ручной таблице. Причина:
# справочник сотрудников мессенджер-ботов не возвращает вообще, а в
# аудит-логе у каждого участника есть готовый флаг is_robot. Значит вид
# логина — единственная зацепка для случая, когда участник есть в таблице,
# а события о его добавлении в аудит-логе нет (чат создан раньше, чем
# начинается доступная история).
MESSENGER_BOT_LOGIN = re.compile(r"^yndx-mssngr-[A-Za-z0-9_-]+-bot$", re.I)

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[A-Za-z]{2,}$")


def clean_text(value) -> str | None:
    """Убирает переводы строк, табуляции, неразрывные пробелы;
    схлопывает подряд идущие пробелы."""
    if value is None:
        return None
    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("\u00a0", " ").replace("\u200b", "")
    text = re.sub(r"[\r\n\t]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text or None


def looks_like_messenger_bot_login(value: str | None) -> bool:
    """Похож ли идентификатор на логин мессенджер-бота.

    Это догадка по виду строки, а не факт от API. Вызывается только при
    разборе ручной таблицы. Если формат логинов у ботов изменится, проверка
    молча перестанет срабатывать — следить за этим нужно по счётчику
    «определены по виду логина» в итогах прогона.
    """
    if not value:
        return False
    local_part = value.strip().casefold().split("@", 1)[0]
    return bool(MESSENGER_BOT_LOGIN.match(local_part))


def looks_like_email(value: str | None) -> bool:
    return bool(value and EMAIL_RE.match(value.strip()))


def position_quality_flags(position: str | None) -> list[str]:
    """Ловит случай, когда в колонку с должностью попал текст о себе.

    Пустая должность замечанием не считается: поле необязательное.
    """
    if not position:
        return []
    if re.search(r"[,.!?]|привет|hello|\bя\s+\w+", position, re.I):
        return ["position_looks_like_about"]
    return []
