from __future__ import annotations

import csv
import json
import os

ENCODING = "utf-8-sig"     # с меткой BOM — Excel правильно открывает кириллицу
DELIMITER = ";"            # точка с запятой удобна для русской локали Excel


def _ensure(out_dir: str) -> None:
    os.makedirs(out_dir, exist_ok=True)


def _writer(out_dir: str, filename: str, header: list[str]):
    _ensure(out_dir)
    handle = open(os.path.join(out_dir, filename), "w",
                  newline="", encoding=ENCODING)
    writer = csv.writer(handle, delimiter=DELIMITER)
    writer.writerow(header)
    return handle, writer


def export_chats(out_dir: str, chats: dict, include_private: bool) -> None:
    handle, writer = _writer(out_dir, "chats.csv", [
        "chat_key", "chat_id_raw", "origin", "coverage_status", "type", "name",
        "description", "created_at", "created_by_login", "manual_confirmed",
        "ambiguous", "incomplete", "is_thread", "members_count", "bots_count",
        "groups_count", "departments_count"])
    with handle:
        for chat in chats.values():
            if not include_private and chat.type == "private":
                continue
            writer.writerow([
                chat.chat_key, chat.chat_id_raw, chat.origin, chat.coverage_status,
                chat.type, chat.name, chat.description, chat.created_at,
                chat.created_by_login, chat.manual_confirmed, chat.ambiguous,
                chat.incomplete, chat.is_thread, len(chat.members), chat.bots_count,
                len(chat.groups), len(chat.departments)])


def export_members(out_dir: str, chats: dict, include_private: bool) -> None:
    handle, writer = _writer(out_dir, "members.csv", [
        "chat_key", "chat_name", "chat_type", "login", "uid", "full_name",
        "position", "role", "added_at", "added_by_login", "is_bot",
        "bot_evidence", "identity_kind", "source", "confidence", "fio_source",
        "resolve_status", "manual_confirmed", "via"])
    with handle:
        for chat in chats.values():
            if not include_private and chat.type == "private":
                continue
            for member in chat.members.values():
                writer.writerow([
                    chat.chat_key, chat.name, chat.type, member.login, member.uid,
                    member.full_name, member.position, member.role, member.added_at,
                    member.added_by_login, member.is_bot, member.bot_evidence,
                    member.identity_kind, member.source, member.confidence,
                    member.fio_source, member.resolve_status,
                    member.manual_confirmed, member.via])


def export_bots(out_dir: str, chats: dict, include_private: bool) -> None:
    """Боты в чатах — главный отчёт для информационной безопасности."""
    handle, writer = _writer(out_dir, "bots_in_chats.csv", [
        "chat_key", "chat_name", "chat_type", "bot_login", "display_name",
        "bot_evidence", "role", "added_at", "added_by_login", "source",
        "resolve_status", "chat_members_total", "chat_bots_total"])
    with handle:
        for chat in chats.values():
            if not include_private and chat.type == "private":
                continue
            if not chat.bots_count:
                continue
            for member in chat.members.values():
                if not member.is_bot:
                    continue
                writer.writerow([
                    chat.chat_key, chat.name, chat.type, member.login,
                    member.full_name, member.bot_evidence, member.role,
                    member.added_at, member.added_by_login, member.source,
                    member.resolve_status, len(chat.members), chat.bots_count])


def export_unresolved_uids(out_dir: str, chats: dict) -> None:
    """Участники, у которых известен только номер, без логина."""
    aggregated: dict[str, dict] = {}
    for chat in chats.values():
        for member in chat.members.values():
            if member.uid and not member.login:
                entry = aggregated.setdefault(member.uid, {
                    "chats": 0, "sample_chat": chat.name or chat.chat_key,
                    "sources": set(), "is_bot": False})
                entry["chats"] += 1
                entry["sources"].add(member.source)
                entry["is_bot"] = entry["is_bot"] or member.is_bot

    handle, writer = _writer(out_dir, "unresolved_uids.csv", [
        "uid", "chats_count", "sample_chat", "is_bot", "sources"])
    with handle:
        for uid, data in sorted(aggregated.items(),
                                key=lambda item: -item[1]["chats"]):
            writer.writerow([uid, data["chats"], data["sample_chat"],
                             data["is_bot"], ",".join(sorted(data["sources"]))])


def export_manual_issues(out_dir: str, manual) -> None:
    handle, writer = _writer(out_dir, "manual_issues.csv",
                             ["kind", "row_no", "chat_name", "detail"])
    with handle:
        for row_no, message in manual.errors:
            writer.writerow(["parse_error", row_no, "", message])
        for conflict in manual.conflicts:
            writer.writerow([conflict["kind"], conflict.get("row_no", ""),
                             conflict.get("chat_name", ""),
                             conflict.get("detail", "")])


def export_quality(out_dir: str, manual) -> None:
    handle, writer = _writer(out_dir, "data_quality.csv", [
        "row_no", "chat_name", "identity", "flag", "value"])
    with handle:
        for issue in manual.quality_issues:
            writer.writerow([issue["row_no"], issue["chat_name"],
                             issue["identity"], issue["flag"],
                             issue.get("value", "")])


def export_discrepancies(out_dir: str, report) -> None:
    handle, writer = _writer(out_dir, "discrepancies.csv", [
        "kind", "chat_key", "chat_name", "login", "audit_value", "manual_value"])
    with handle:
        for item in report.discrepancies:
            writer.writerow([item["kind"], item["chat_key"], item["chat_name"],
                             item["login"], item["audit_value"],
                             item["manual_value"]])

    handle, writer = _writer(out_dir, "chat_match_issues.csv", [
        "kind", "chat_name", "reason", "candidates"])
    with handle:
        for item in report.ambiguous_chats:
            writer.writerow(["ambiguous", item["chat_name"], item["reason"],
                             item.get("candidates", "")])
        for name in report.manual_only_chats:
            writer.writerow(["manual_only", name,
                             "в аудит-логе не найден — возможно, чат создан "
                             "раньше доступной истории", ""])
        for name in report.audit_only_chats:
            writer.writerow(["audit_only", name, "в ручной таблице не указан", ""])

    handle, writer = _writer(out_dir, "unresolved_identities.csv", [
        "chat_name", "identity", "identity_kind", "status", "row_no",
        "full_name", "is_bot", "hint"])
    with handle:
        for item in report.unresolved_identities:
            writer.writerow([item["chat_name"], item["identity"],
                             item["identity_kind"], item["status"],
                             item["row_no"], item["full_name"],
                             item["is_bot"], item.get("hint", "")])

    handle, writer = _writer(out_dir, "learned_identities.csv", [
        "chat_key", "chat_name", "uid", "identity", "is_bot", "reason"])
    with handle:
        for item in report.learned_identities:
            writer.writerow([item["chat_key"], item["chat_name"], item["uid"],
                             item["identity"], item.get("is_bot", False),
                             item["reason"]])


def export_run_diff(out_dir: str, diff) -> None:
    handle, writer = _writer(out_dir, "run_diff.csv", [
        "change", "chat_key", "member_key", "login", "role_or_old", "new_role"])
    with handle:
        for chat_key, member_key, login, role in diff.added:
            writer.writerow(["added", chat_key, member_key, login, role, ""])
        for chat_key, member_key, login, role in diff.removed:
            writer.writerow(["removed", chat_key, member_key, login, role, ""])
        for chat_key, member_key, login, old, new in diff.role_changed:
            writer.writerow(["role_changed", chat_key, member_key, login, old, new])


def export_readme(out_dir: str) -> None:
    """Пояснение к файлам отчёта — чтобы не гадать, что значат колонки."""
    text = """ЧТО ЗА ФАЙЛЫ В ЭТОЙ ПАПКЕ

Внимание: файлы содержат персональные данные сотрудников — логины, ФИО,
должности, состав чатов. Обращайтесь с ними соответственно.

Файлы в формате CSV, разделитель — точка с запятой. Открываются Excel
двойным щелчком, кириллица не ломается.

chats.csv
    Все найденные чаты и каналы.
    coverage_status — откуда взяты данные о чате:
        audit_only    — только из аудит-лога, в ручной таблице чата нет
        audit+manual  — есть и в логе, и в таблице
        manual_only   — только в таблице; скорее всего чат создан раньше,
                        чем начинается доступная история аудит-лога
    incomplete = True — в чат добавляли группу или подразделение целиком,
                        поэтому по отдельным людям дат добавления нет
    ambiguous  = True — в логе несколько чатов с таким же названием,
                        сопоставить однозначно не удалось

members.csv
    Участники чатов. Одна строка — один человек в одном чате.
    source — откуда сведения об участнике:
        audit_projection        точно из аудит-лога
        audit_projection+manual из лога, подтверждено ручной таблицей
        manual_import           только из ручной таблицы
        group_expansion         восстановлено по составу группы
    confidence — насколько можно верить дате добавления:
        high    дата из аудит-лога, точная
        medium  дата из ручной таблицы
        low     дата привязки группы к чату, а не конкретного человека
    added_by_login — кто добавил. Пусто, если человек попал в чат вместе
        с группой или если сведения только из таблицы.
    fio_source — откуда ФИО: directory (справочник) или manual (таблица).
        Справочник всегда важнее таблицы.
    via — через какую группу или подразделение человек попал в чат.

bots_in_chats.csv
    Только боты. Главный файл для проверки: бот в чате может читать всю
    переписку.
    bot_evidence — откуда известно, что это бот:
        audit_log             флаг is_robot в событии аудит-лога — факт
        directory             флаг is_robot в справочнике — факт
        manual_login_pattern  догадка по виду логина. Так распознаются
                              боты, добавленные раньше доступной истории
                              аудит-лога.

discrepancies.csv
    Расхождения между аудит-логом и ручной таблицей. Ничего не исправлялось
    молча — все спорные места здесь.
        only_in_manual      есть в таблице, нет в логе
        only_in_audit       есть в логе, нет в таблице
        added_at_mismatch   даты добавления разошлись
        full_name_mismatch  ФИО в таблице и в справочнике разные
        bot_flag_conflict   логин похож на бота, но аудит-лог говорит, что
                            это человек. Поверили аудит-логу.

chat_match_issues.csv
    Чаты, которые не удалось однозначно связать с таблицей.

unresolved_identities.csv
    Адреса и логины из таблицы, которых нет в справочнике сотрудников.
    Колонка hint подсказывает вероятную причину. Мессенджер-ботов в
    справочнике нет вообще — для них это нормально.

unresolved_uids.csv
    Участники, у которых известен только номер, а логин узнать не удалось.
    Обычно это сотрудники других организаций или уволенные.

learned_identities.csv
    Связки «номер сотрудника — логин», которые удалось установить по ручной
    таблице. Для мессенджер-ботов это единственный способ узнать логин.

data_quality.csv
    Замечания к ручной таблице.
        position_looks_like_about    в графе должности написан текст о себе
        identity_is_login_not_email  вместо адреса почты указан логин
        identity_unrecognized        идентификатор не разобрать

manual_issues.csv
    Ошибки и противоречия при чтении таблицы.

run_diff.csv
    Что изменилось в составе чатов с прошлого запуска.

summary.json
    Сводные числа по прогону.

manifest.json
    Условия запуска: время, ключи, входные файлы. Нужен, чтобы сравнение
    двух прогонов было честным. Токены в него не попадают.

compare_with_previous/
    Сравнение с предыдущим запуском, если он был.

ЧЕГО В ОТЧЁТЕ ПРИНЦИПИАЛЬНО НЕ БУДЕТ

  * Дат добавления по каждому человеку, если его добавили вместе с группой
    или подразделением — аудит-лог таких событий по отдельным людям не
    создаёт.
  * Полной истории старше срока хранения аудит-лога (около полугода).
  * ФИО и должностей для участников из других организаций — их нет в
    справочнике.
  * Логинов и имён мессенджер-ботов, если их не удалось связать с ручной
    таблицей: справочник таких ботов не возвращает.
"""
    _ensure(out_dir)
    with open(os.path.join(out_dir, "README.txt"), "w", encoding="utf-8") as handle:
        handle.write(text)


def export_summary(out_dir: str, *, chats: dict, manual=None,
                   merge_report=None, diff=None) -> dict:
    _ensure(out_dir)
    by_status: dict[str, int] = {}
    by_source: dict[str, int] = {}
    by_confidence: dict[str, int] = {}
    bot_evidence: dict[str, int] = {}
    bots_with_login = 0
    bots_without_login = 0

    for chat in chats.values():
        by_status[chat.coverage_status] = by_status.get(chat.coverage_status, 0) + 1
        for member in chat.members.values():
            by_source[member.source] = by_source.get(member.source, 0) + 1
            by_confidence[member.confidence] = \
                by_confidence.get(member.confidence, 0) + 1
            if member.is_bot:
                key = member.bot_evidence or "unknown"
                bot_evidence[key] = bot_evidence.get(key, 0) + 1
                if member.login:
                    bots_with_login += 1
                else:
                    bots_without_login += 1

    summary = {
        "total_chats": len(chats),
        "chats_by_type": _count(chats, lambda chat: chat.type or "unknown"),
        "coverage_by_status": by_status,
        "members_total": sum(len(chat.members) for chat in chats.values()),
        "members_by_source": by_source,
        "members_by_confidence": by_confidence,
        "bots": {
            "chats_with_bots": sum(1 for chat in chats.values() if chat.bots_count),
            "bot_memberships": sum(chat.bots_count for chat in chats.values()),
            "by_evidence": bot_evidence,
            "with_login": bots_with_login,
            "without_login": bots_without_login,
        },
        "unresolved_uid_members": sum(
            1 for chat in chats.values() for member in chat.members.values()
            if member.uid and not member.login),
        "threads": sum(1 for chat in chats.values() if chat.is_thread),
        "manual": None if not manual else {
            "file": manual.source_file, "rows_total": manual.rows_total,
            "rows_ok": manual.rows_ok, "chats": len(manual.chats),
            "bots": manual.bots_total, "errors": len(manual.errors),
            "conflicts": len(manual.conflicts),
            "quality_issues": len(manual.quality_issues)},
        "merge": None if not merge_report else merge_report.counts(),
        "unresolved_split": (None if not merge_report
                             else merge_report.unresolved_split()),
        "snapshot_diff": None if not diff else diff.counts(),
    }
    with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as handle:
        json.dump(summary, handle, ensure_ascii=False, indent=2)
    return summary


def _count(chats: dict, key_fn) -> dict:
    result: dict[str, int] = {}
    for chat in chats.values():
        key = key_fn(chat)
        result[key] = result.get(key, 0) + 1
    return result
