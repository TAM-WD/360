from __future__ import annotations

import csv
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime

from manual_import import ManualChat, ManualImportResult, ManualRow, normalize_name
from projection import ChatState, MemberState

log = logging.getLogger("merge")


@dataclass
class MergeReport:
    matched_chats: list[tuple[str, str]] = field(default_factory=list)
    manual_only_chats: list[str] = field(default_factory=list)
    ambiguous_chats: list[dict] = field(default_factory=list)
    audit_only_chats: list[str] = field(default_factory=list)
    discrepancies: list[dict] = field(default_factory=list)
    unresolved_identities: list[dict] = field(default_factory=list)
    learned_identities: list[dict] = field(default_factory=list)

    def counts(self) -> dict:
        return {
            "matched_chats": len(self.matched_chats),
            "manual_only_chats": len(self.manual_only_chats),
            "ambiguous_chats": len(self.ambiguous_chats),
            "audit_only_chats": len(self.audit_only_chats),
            "discrepancies": len(self.discrepancies),
            "unresolved_identities": len(self.unresolved_identities),
            "learned_identities": len(self.learned_identities),
        }

    def unresolved_split(self) -> dict:
        bots = sum(1 for item in self.unresolved_identities if item.get("is_bot"))
        return {"total": len(self.unresolved_identities), "bots": bots,
                "people": len(self.unresolved_identities) - bots}


@dataclass
class _Resolution:
    """Промежуточный результат сопоставления строки таблицы с участником чата."""
    identity: str
    row: ManualRow
    uid: str | None
    status: str
    existing: MemberState | None


def load_manual_map(path: str | None) -> dict[tuple[str, str], str]:
    """Ручное разрешение одноимённых чатов: CSV с колонками
    chat_name;type;chat_key."""
    if not path:
        return {}
    mapping: dict[tuple[str, str], str] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as handle:
        sample = handle.read(4096)
        handle.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample, delimiters=";,\t")
        except csv.Error:
            dialect = csv.excel
        for row in csv.DictReader(handle, dialect=dialect):
            name = normalize_name(row.get("chat_name", ""))
            chat_type = (row.get("type") or "").strip()
            chat_key = (row.get("chat_key") or "").strip()
            if name and chat_key:
                mapping[(name, chat_type)] = chat_key
    log.info("Ручных сопоставлений чатов загружено: %s", len(mapping))
    return mapping


def _index_audit_chats(chats: dict[str, ChatState]):
    index: dict[tuple[str, str], list[ChatState]] = defaultdict(list)
    for chat in chats.values():
        if not chat.name or chat.origin == "manual":
            continue
        index[(normalize_name(chat.name), chat.type or "")].append(chat)
    return index


def merge_manual(chats: dict[str, ChatState], manual: ManualImportResult, resolver,
                 *, date_tolerance_days: int = 1,
                 manual_map: dict[tuple[str, str], str] | None = None) -> MergeReport:
    """Соединяет данные ручной таблицы с тем, что восстановили из аудит-лога.

    Правила простые:
      * даты добавления, роли и «кто добавил» берём из аудит-лога — таблица
        может только заполнить пропуски;
      * ФИО и должность берём из справочника сотрудников — таблица опять же
        только заполняет пропуски;
      * любое расхождение не исправляем молча, а выписываем в отчёт.
    """
    report = MergeReport()
    manual_map = manual_map or {}
    audit_index = _index_audit_chats(chats)
    matched_keys: set[str] = set()

    for manual_key, manual_chat in manual.chats.items():
        target = _resolve_target_chat(manual_chat, chats, audit_index,
                                      manual_map, report)
        if target is None:
            continue
        if target.origin == "manual":
            report.manual_only_chats.append(manual_chat.chat_name)
        else:
            report.matched_chats.append((manual_key, target.chat_key))
            matched_keys.add(target.chat_key)
            _compare_metadata(target, manual_chat, report)

        _merge_members(target, manual_chat, resolver, report, date_tolerance_days)

    for chat in chats.values():
        if chat.origin == "manual" or chat.type == "private":
            continue
        if chat.chat_key not in matched_keys:
            report.audit_only_chats.append(chat.name or chat.chat_key)

    log.info("Свели таблицу с аудит-логом: чатов совпало %s, только в таблице %s, "
             "только в логе %s, расхождений %s",
             len(report.matched_chats), len(report.manual_only_chats),
             len(report.audit_only_chats), len(report.discrepancies))
    return report


# --------------------------------------------------------------------------
# сопоставление чатов
# --------------------------------------------------------------------------
def _resolve_target_chat(manual_chat: ManualChat, chats: dict[str, ChatState],
                         audit_index, manual_map, report: MergeReport):
    name_key = normalize_name(manual_chat.chat_name)
    type_key = manual_chat.chat_type or ""

    forced = manual_map.get((name_key, type_key)) or manual_map.get((name_key, ""))
    if forced:
        target = chats.get(forced)
        if target:
            target.manual_confirmed = True
            return target
        report.ambiguous_chats.append({
            "chat_name": manual_chat.chat_name,
            "reason": "указанный вручную chat_key в данных не найден",
            "candidates": forced})
        return None

    candidates = list(audit_index.get((name_key, type_key), []))
    if not candidates and type_key:
        candidates = list(audit_index.get((name_key, ""), []))

    if len(candidates) == 1:
        target = candidates[0]
        target.manual_confirmed = True
        return target

    if len(candidates) > 1:
        # Сами выбирать не будем: угадаешь неверно — испортишь данные.
        report.ambiguous_chats.append({
            "chat_name": manual_chat.chat_name,
            "reason": "в аудит-логе несколько чатов с таким названием и типом",
            "candidates": ";".join(chat.chat_key for chat in candidates)})
        return _make_manual_chat(manual_chat, chats, ambiguous=True)

    return _make_manual_chat(manual_chat, chats, ambiguous=False)


def _make_manual_chat(manual_chat: ManualChat, chats: dict[str, ChatState],
                      *, ambiguous: bool) -> ChatState:
    chat = chats.get(manual_chat.manual_key)
    if chat is None:
        chat = ChatState(chat_id_raw=manual_chat.manual_key,
                         chat_key=manual_chat.manual_key)
        chats[manual_chat.manual_key] = chat
    chat.type = chat.type or manual_chat.chat_type
    chat.name = chat.name or manual_chat.chat_name
    chat.description = chat.description or manual_chat.description
    if manual_chat.created_at and not chat.created_at:
        chat.created_at = manual_chat.created_at.isoformat()
    chat.origin = "manual"
    chat.manual_confirmed = True
    chat.ambiguous = ambiguous
    return chat


def _compare_metadata(chat: ChatState, manual_chat: ManualChat,
                      report: MergeReport) -> None:
    if manual_chat.description and chat.description and \
            normalize_name(manual_chat.description) != normalize_name(chat.description):
        report.discrepancies.append({
            "kind": "chat_description_mismatch", "chat_key": chat.chat_key,
            "chat_name": chat.name, "login": "",
            "audit_value": chat.description, "manual_value": manual_chat.description})
    if manual_chat.chat_type and chat.type and manual_chat.chat_type != chat.type:
        report.discrepancies.append({
            "kind": "chat_type_mismatch", "chat_key": chat.chat_key,
            "chat_name": chat.name, "login": "",
            "audit_value": chat.type, "manual_value": manual_chat.chat_type})
    if not chat.description and manual_chat.description:
        chat.description = manual_chat.description
    if not chat.created_at and manual_chat.created_at:
        chat.created_at = manual_chat.created_at.isoformat()


# --------------------------------------------------------------------------
# сопоставление участников
# --------------------------------------------------------------------------
def _unresolved_hint(row: ManualRow) -> str:
    if row.is_bot:
        return ("мессенджер-бот — в справочнике сотрудников таких нет, "
                "это нормально")
    if row.identity_kind == "login":
        return "в колонке Email указан логин, а не адрес почты"
    if row.identity_kind == "unknown":
        return "не удалось разобрать идентификатор"
    domain = (row.identity or "").split("@", 1)[-1]
    return f"адрес на домене {domain} — возможно, сотрудник другой организации"


def _merge_members(chat: ChatState, manual_chat: ManualChat, resolver,
                   report: MergeReport, tolerance_days: int) -> None:
    by_uid = {member.uid: member for member in chat.members.values() if member.uid}
    by_login = {member.login.casefold(): member
                for member in chat.members.values() if member.login}

    # --- шаг 1: пробуем найти каждую строку таблицы среди участников ---
    resolutions: list[_Resolution] = []
    for identity, row in manual_chat.members.items():
        uid, status = (resolver.resolve_identity(identity) if resolver
                       else (None, "no_resolver"))
        existing = None
        if uid and uid in by_uid:
            existing = by_uid[uid]
        elif identity in by_login:
            existing = by_login[identity]
        # Ботов справочник не знает по определению — уточняем статус,
        # чтобы это не путали с настоящей проблемой.
        if row.is_bot and status == "not_found":
            status = "bot_outside_directory"
        resolutions.append(_Resolution(identity=identity, row=row, uid=uid,
                                       status=status, existing=existing))

    # --- шаг 2: связываем безымянных участников лога со строками таблицы ---
    _reverse_learn(chat, resolutions, resolver, report)

    # --- шаг 3: собственно слияние ---
    seen_uids: set[str] = set()
    seen_logins: set[str] = set()

    for res in resolutions:
        if res.existing is None and not res.status.startswith("resolved"):
            report.unresolved_identities.append({
                "chat_name": manual_chat.chat_name, "identity": res.identity,
                "identity_kind": res.row.identity_kind, "status": res.status,
                "row_no": res.row.row_no, "full_name": res.row.full_name or "",
                "is_bot": res.row.is_bot, "hint": _unresolved_hint(res.row)})

        if res.existing is not None:
            seen_uids.add(res.existing.uid or "")
            seen_logins.add((res.existing.login or "").casefold())
            _reconcile_existing(chat, res.existing, res.row, resolver,
                                report, tolerance_days)
            continue

        member = MemberState(
            uid=res.uid,
            login=((resolver.login_for_uid(res.uid) if (resolver and res.uid) else None)
                   or res.identity),
            role=res.row.role,          # если колонки нет — так и оставляем пустым
            added_at=res.row.date.isoformat() if res.row.date else None,
            added_by_login=res.row.added_by,
            source="manual_import",
            confidence="medium",
            identity_kind=res.row.identity_kind,
            resolve_status=res.status,
            manual_confirmed=True,
        )

        directory_fio = (resolver.full_name_for_uid(res.uid)
                         if (resolver and res.uid) else None)
        member.full_name = directory_fio or res.row.full_name
        member.fio_source = ("directory" if directory_fio
                             else ("manual" if res.row.full_name else None))
        directory_position = (resolver.position_for_uid(res.uid)
                              if (resolver and res.uid) else None)
        member.position = directory_position or res.row.position

        # Признак «бот»: факт от справочника важнее догадки по логину.
        robot_by_api = bool(res.uid and resolver and resolver.is_robot_uid(res.uid))
        if robot_by_api:
            member.is_bot = True
            member.bot_evidence = "directory"
        elif res.row.is_bot:
            member.is_bot = True
            member.bot_evidence = "manual_login_pattern"

        if directory_fio and res.row.full_name and \
                normalize_name(directory_fio) != normalize_name(res.row.full_name):
            report.discrepancies.append({
                "kind": "full_name_mismatch", "chat_key": chat.chat_key,
                "chat_name": chat.name or manual_chat.chat_name,
                "login": res.identity,
                "audit_value": directory_fio, "manual_value": res.row.full_name})

        chat.members[f"manual::{res.identity}"] = member
        report.discrepancies.append({
            "kind": "only_in_manual", "chat_key": chat.chat_key,
            "chat_name": chat.name or manual_chat.chat_name, "login": res.identity,
            "audit_value": "",
            "manual_value": res.row.date.isoformat() if res.row.date else ""})

    # --- шаг 4: кто есть в логе, но отсутствует в таблице ---
    for member in list(chat.members.values()):
        if member.source.startswith("manual_import"):
            continue
        if (member.uid or "") in seen_uids or \
                (member.login or "").casefold() in seen_logins:
            continue
        report.discrepancies.append({
            "kind": "only_in_audit", "chat_key": chat.chat_key,
            "chat_name": chat.name, "login": member.login or member.uid or "",
            "audit_value": member.added_at or "", "manual_value": ""})


def _reverse_learn(chat: ChatState, resolutions: list[_Resolution],
                   resolver, report: MergeReport) -> None:
    """Связывает участника аудит-лога, у которого известен только номер,
    со строкой ручной таблицы.

    Ботов и людей разбираем по отдельности, чтобы случайно не связать бота
    с человеком. Связываем только когда с каждой стороны ровно один
    неопознанный — угадывать при большем количестве нельзя.

    Для ботов это единственный способ узнать логин: справочник их не
    возвращает, а в аудит-логе есть только номер и флаг is_robot.
    """
    if resolver is None:
        return

    for want_bot in (True, False):
        manual_side = [res for res in resolutions
                       if res.existing is None and res.row.is_bot is want_bot]
        audit_side = [member for member in chat.members.values()
                      if member.uid and not member.login
                      and member.source.startswith("audit_projection")
                      and member.is_bot is want_bot]
        if len(manual_side) != 1 or len(audit_side) != 1:
            continue

        res, member = manual_side[0], audit_side[0]
        if not res.identity:
            continue

        resolver.learn_identity(member.uid, res.identity, source="manual")
        member.login = res.identity
        member.identity_kind = res.row.identity_kind
        member.source = f"{member.source}+manual_identity"
        member.manual_confirmed = True
        if res.row.full_name and not member.full_name:
            member.full_name = res.row.full_name
            member.fio_source = "manual"
        res.existing = member
        res.status = "resolved:manual_identity"

        report.learned_identities.append({
            "chat_key": chat.chat_key, "chat_name": chat.name,
            "uid": member.uid, "identity": res.identity,
            "is_bot": want_bot,
            "reason": "в чате остался ровно один неопознанный участник "
                      "и ровно одна нераспознанная строка таблицы"})
        log.info("Связали номер %s с %s в чате «%s»%s",
                 member.uid, res.identity, chat.name,
                 " (бот)" if want_bot else "")


def _reconcile_existing(chat: ChatState, existing: MemberState, row: ManualRow,
                        resolver, report: MergeReport, tolerance_days: int) -> None:
    """Дополняет уже найденного участника, не переписывая данные аудит-лога."""
    existing.manual_confirmed = True
    existing.identity_kind = existing.identity_kind or row.identity_kind

    # Признак «бот». Факт от API сильнее догадки по виду логина.
    if existing.bot_evidence in ("audit_log", "directory"):
        if row.is_bot is False and existing.is_bot:
            pass                     # лог сказал «бот» — верим логу
    elif row.is_bot:
        robot_by_api = (resolver.is_robot_uid(existing.uid)
                        if (resolver and existing.uid) else False)
        if robot_by_api:
            existing.is_bot = True
            existing.bot_evidence = "directory"
        elif existing.source.startswith("audit_projection") and not existing.is_bot:
            # Аудит-лог прямо сказал is_robot=false, а логин похож на бота.
            # Верим аудит-логу, но расхождение не прячем.
            report.discrepancies.append({
                "kind": "bot_flag_conflict", "chat_key": chat.chat_key,
                "chat_name": chat.name,
                "login": existing.login or row.identity or "",
                "audit_value": "по данным аудит-лога это не бот",
                "manual_value": "логин похож на логин мессенджер-бота"})
        else:
            existing.is_bot = True
            existing.bot_evidence = "manual_login_pattern"

    # ФИО: справочник важнее таблицы
    directory_fio = (resolver.full_name_for_uid(existing.uid)
                     if (resolver and existing.uid) else None)
    if directory_fio:
        existing.full_name = directory_fio
        existing.fio_source = "directory"
        if row.full_name and \
                normalize_name(directory_fio) != normalize_name(row.full_name):
            report.discrepancies.append({
                "kind": "full_name_mismatch", "chat_key": chat.chat_key,
                "chat_name": chat.name,
                "login": existing.login or existing.uid or "",
                "audit_value": directory_fio, "manual_value": row.full_name})
    elif row.full_name and not existing.full_name:
        existing.full_name = row.full_name
        existing.fio_source = "manual"

    # должность: справочник важнее таблицы
    directory_position = (resolver.position_for_uid(existing.uid)
                          if (resolver and existing.uid) else None)
    if directory_position:
        existing.position = directory_position
    elif row.position and not existing.position:
        existing.position = row.position

    # роль: аудит-лог важнее, таблица только заполняет пропуск
    if row.role and not existing.role:
        existing.role = row.role

    # дата добавления: данные аудит-лога не переписываем
    if row.date and not existing.added_at:
        existing.added_at = row.date.isoformat()
        existing.confidence = "medium"
    elif row.date and existing.added_at:
        try:
            audit_dt = datetime.fromisoformat(existing.added_at)
            delta_days = abs((audit_dt - row.date).total_seconds()) / 86400.0
            if delta_days > tolerance_days:
                report.discrepancies.append({
                    "kind": "added_at_mismatch", "chat_key": chat.chat_key,
                    "chat_name": chat.name,
                    "login": existing.login or existing.uid or "",
                    "audit_value": existing.added_at,
                    "manual_value": row.date.isoformat()})
        except ValueError:
            pass

    if "+manual" not in existing.source:
        existing.source = f"{existing.source}+manual"
