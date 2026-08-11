from __future__ import annotations

from datetime import datetime

# --------------------------------------------------------------- словари
ROLE_RU = {
    "admin": "администратор",
    "member": "участник",
    "subscriber": "подписчик",
}

CHAT_TYPE_RU = {
    "group": "групповой чат",
    "channel": "канал",
    "private": "личный чат",
}

CHAT_TYPE_SHORT_RU = {
    "group": "групповой",
    "channel": "канал",
    "private": "личный",
}

COVERAGE_RU = {
    "audit_only": "только аудит-лог",
    "audit+manual": "аудит-лог и таблица",
    "manual_only": "только ручная таблица",
}

SOURCE_RU = {
    "audit_projection": "точно из аудит-лога",
    "audit_projection+manual": "аудит-лог, подтверждено таблицей",
    "audit_projection+manual_identity": "аудит-лог, логин взят из таблицы",
    "manual_import": "только из ручной таблицы",
    "group_expansion": "восстановлено по составу групп",
}

CONFIDENCE_RU = {
    "high": "точные данные",
    "medium": "данные из ручной таблицы",
    "low": "приблизительные данные",
}

EVIDENCE_RU = {
    "audit_log": "подтверждены аудит-логом",
    "directory": "подтверждены справочником",
    "manual_login_pattern": "определены по виду логина",
    "unknown": "источник признака неизвестен",
}

STATUS_RU = {
    "not_found": "не найден в справочнике",
    "bot_outside_directory": "бот — в справочнике его и не должно быть",
    "empty": "идентификатор не указан",
    "no_resolver": "справочник не использовался",
}

QUALITY_RU = {
    "position_looks_like_about": "в должности написан текст о себе",
    "identity_is_login_not_email": "вместо адреса почты указан логин",
    "identity_unrecognized": "не удалось разобрать идентификатор",
}

MANUAL_ISSUE_RU = {
    "parse_error": "ошибка чтения строки",
    "description_mismatch": "у одного чата разные описания",
    "duplicate_member": "участник указан дважды",
    "chat_created_ambiguous": "дата создания чата неоднозначна",
}

DISCREPANCY_RU = {
    "only_in_manual": "есть в таблице, нет в аудит-логе",
    "only_in_audit": "есть в аудит-логе, нет в таблице",
    "added_at_mismatch": "не совпали даты добавления",
    "full_name_mismatch": "не совпали ФИО",
    "chat_description_mismatch": "не совпали описания чата",
    "chat_type_mismatch": "не совпали типы чата",
    "bot_flag_conflict": "логин похож на бота, но API говорит иначе",
}


# --------------------------------------------------------------- числа
def plural(count: int, one: str, few: str, many: str) -> str:
    """5 -> '5 участников', 1 -> '1 участник', 2 -> '2 участника'."""
    number = abs(int(count))
    if number % 10 == 1 and number % 100 != 11:
        word = one
    elif 2 <= number % 10 <= 4 and not 12 <= number % 100 <= 14:
        word = few
    else:
        word = many
    return f"{count} {word}"


def share(part: int, total: int) -> str:
    """191, 3841 -> '191 из 3841 (5%)'."""
    if not total:
        return str(part)
    return f"{part} из {total} ({round(part * 100 / total)}%)"


def duration(seconds: float) -> str:
    seconds = int(round(seconds))
    if seconds < 60:
        return f"{seconds} с"
    minutes, rest = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes} мин {rest} с"
    hours, minutes = divmod(minutes, 60)
    return f"{hours} ч {minutes} мин"


# --------------------------------------------------------------- даты
def dt_human(value, *, with_time: bool = True) -> str:
    """ISO-строка или datetime -> '17.02.2026 12:12'."""
    if value in (None, ""):
        return "—"
    moment = value
    if isinstance(value, str):
        try:
            moment = datetime.fromisoformat(value)
        except ValueError:
            return value
    if not isinstance(moment, datetime):
        return str(value)
    return moment.strftime("%d.%m.%Y %H:%M" if with_time else "%d.%m.%Y")


def date_human(value) -> str:
    return dt_human(value, with_time=False)


# --------------------------------------------------------------- переводы
def _translate(value, dictionary: dict, default_prefix: str = "") -> str:
    if value in (None, ""):
        return "не указано"
    return dictionary.get(str(value), f"{default_prefix}{value}")


def role_ru(role) -> str:
    return _translate(role, ROLE_RU)


def chat_type_ru(chat_type) -> str:
    return _translate(chat_type, CHAT_TYPE_RU)


def chat_type_short_ru(chat_type) -> str:
    return _translate(chat_type, CHAT_TYPE_SHORT_RU)


def coverage_ru(status) -> str:
    return _translate(status, COVERAGE_RU)


def source_ru(source) -> str:
    return _translate(source, SOURCE_RU)


def confidence_ru(level) -> str:
    return _translate(level, CONFIDENCE_RU)


def evidence_ru(evidence) -> str:
    return _translate(evidence, EVIDENCE_RU)


def status_ru(status) -> str:
    if status and str(status).startswith("resolved"):
        return "найден в справочнике"
    return _translate(status, STATUS_RU)


def quality_ru(flag) -> str:
    return _translate(flag, QUALITY_RU)


def manual_issue_ru(kind) -> str:
    return _translate(kind, MANUAL_ISSUE_RU)


def discrepancy_ru(kind) -> str:
    return _translate(kind, DISCREPANCY_RU)


# --------------------------------------------------------------- блоки
def counters(mapping: dict, translator=None, *, indent: str = "  ",
             width: int = 32, note: dict | None = None) -> str:
    """Аккуратный столбик 'название   число' вместо питоновского словаря."""
    if not mapping:
        return f"{indent}нет данных"
    lines = []
    for key, value in sorted(mapping.items(), key=lambda item: -item[1]):
        label = translator(key) if translator else str(key)
        tail = f"   {note[key]}" if note and key in note else ""
        lines.append(f"{indent}{label:<{width}.{width}} {value:>6}{tail}")
    return "\n".join(lines)


def bullet_list(items: list[str], indent: str = "  ") -> str:
    return "\n".join(f"{indent}{item}" for item in items) if items else ""
