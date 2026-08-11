from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Iterable, Optional

log = logging.getLogger("projection")

CHAT_EVENT_TYPES = [
    "messenger_chat.created",
    "messenger_chat.info_changed",
    "messenger_chat.member.added",
    "messenger_chat.member.role_changed",
    "messenger_chat.member.removed",
    "messenger_chat.group_added",
    "messenger_chat.group_removed",
    "messenger_chat.department_added",
    "messenger_chat.department_removed",
]

_warned_types: set[str] = set()


def normalize_chat_id(raw: Optional[str]) -> Optional[str]:
    """Ключ джойна между источниками.
    group/channel: '1/0/<uuid>' -> '<uuid>'.
    private: 'guidA_guidB' -> 'priv::<sorted>' (симметрично)."""
    if not raw:
        return None
    if "/" not in raw and "_" in raw:
        return "priv::" + "_".join(sorted(raw.split("_")))
    return raw.rsplit("/", 1)[-1]


@dataclass
class MemberState:
    uid: Optional[str] = None
    login: Optional[str] = None
    role: Optional[str] = None
    added_at: Optional[str] = None
    added_by_login: Optional[str] = None
    source: str = "audit_projection"      # audit_projection | group_expansion | manual_import
    confidence: str = "low"               # high | medium | low
    via: Optional[str] = None             # через какую группу/подразделение
    full_name: Optional[str] = None
    position: Optional[str] = None
    manual_confirmed: bool = False
    is_bot: bool = False
    bot_evidence: Optional[str] = None    # audit_log | directory | manual_login_pattern
    identity_kind: Optional[str] = None   # email | bot_login | login
    fio_source: Optional[str] = None      # directory | manual
    resolve_status: Optional[str] = None  # resolved | bot_outside_directory | not_found


@dataclass
class ChatState:
    chat_id_raw: str
    chat_key: str
    type: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None
    created_at: Optional[str] = None
    created_by_login: Optional[str] = None
    members: dict[str, MemberState] = field(default_factory=dict)
    groups: dict[str, dict] = field(default_factory=dict)
    departments: dict[str, dict] = field(default_factory=dict)
    incomplete: bool = False              # были групповые добавления
    is_thread: bool = False
    origin: str = "audit"                 # audit | manual
    manual_confirmed: bool = False
    ambiguous: bool = False               # не удалось однозначно сматчить с manual

    @property
    def coverage_status(self) -> str:
        if self.origin == "manual":
            return "manual_only"
        return "audit+manual" if self.manual_confirmed else "audit_only"

    @property
    def bots_count(self) -> int:
        return sum(1 for m in self.members.values() if m.is_bot)


def _extract_id(meta: dict, *candidates: str) -> Optional[str]:
    """Достаёт идентификатор объекта из meta по нескольким возможным ключам."""
    for key in candidates:
        val = meta.get(key)
        if isinstance(val, (str, int)):
            return str(val)
        if isinstance(val, dict) and val.get("id") is not None:
            return str(val["id"])
    for key in candidates:
        info = meta.get(key.replace("_id", "_info"))
        if isinstance(info, dict) and info.get("id") is not None:
            return str(info["id"])
    return None


def build_projection(events_ordered: Iterable[dict]) -> dict[str, ChatState]:
    """Проигрывает события в хронологическом порядке -> состояние чатов."""
    chats: dict[str, ChatState] = {}

    for enriched in events_ordered:
        ev = enriched.get("event", {}) or {}
        meta = ev.get("meta") or {}
        raw_id = meta.get("chat_id")
        key = normalize_chat_id(raw_id)
        if not key:
            continue

        etype = ev.get("type")
        info = meta.get("chat_info") or {}
        initiator = enriched.get("user_login")

        chat = chats.get(key)
        if chat is None:
            chat = ChatState(chat_id_raw=raw_id, chat_key=key)
            chats[key] = chat
        if info.get("is_thread"):
            chat.is_thread = True

        if etype == "messenger_chat.created":
            chat.type = info.get("type") or chat.type
            chat.name = info.get("name")
            chat.description = info.get("description")
            chat.created_at = ev.get("occurred_at")
            chat.created_by_login = initiator

        elif etype == "messenger_chat.info_changed":
            if info.get("name") is not None:
                chat.name = info["name"]
            if info.get("description") is not None:
                chat.description = info["description"]
            if info.get("type"):
                chat.type = info["type"]

        elif etype == "messenger_chat.member.added":
            uid = _extract_id(meta, "object_uid")
            if uid:
                member_info = meta.get("member_info") or {}
                chat.type = chat.type or info.get("type")
                chat.name = chat.name or info.get("name")
                is_robot = bool(member_info.get("is_robot"))
                chat.members[uid] = MemberState(
                    uid=uid,
                    role=member_info.get("role"),
                    added_at=ev.get("occurred_at"),
                    added_by_login=initiator,
                    source="audit_projection",
                    confidence="high",
                    is_bot=is_robot,
                    bot_evidence="audit_log" if is_robot else None,
                )

        elif etype == "messenger_chat.member.role_changed":
            uid = _extract_id(meta, "object_uid")
            if uid and uid in chat.members:
                mi = meta.get("member_info") or {}
                chat.members[uid].role = mi.get("role")

        elif etype == "messenger_chat.member.removed":
            uid = _extract_id(meta, "object_uid")
            if uid:
                chat.members.pop(uid, None)

        elif etype == "messenger_chat.group_added":
            gid = _extract_id(meta, "group_id", "object_id", "object_group_id")
            if gid:
                chat.groups[gid] = {"added_at": ev.get("occurred_at"),
                                    "added_by_login": initiator}
                chat.incomplete = True
            elif etype not in _warned_types:
                _warned_types.add(etype)
                log.warning("Не найден group_id в meta (%s): keys=%s",
                            etype, sorted(meta.keys()))

        elif etype == "messenger_chat.group_removed":
            gid = _extract_id(meta, "group_id", "object_id", "object_group_id")
            if gid:
                chat.groups.pop(gid, None)

        elif etype == "messenger_chat.department_added":
            did = _extract_id(meta, "department_id", "object_id", "object_department_id")
            if did:
                chat.departments[did] = {"added_at": ev.get("occurred_at"),
                                         "added_by_login": initiator}
                chat.incomplete = True
            elif etype not in _warned_types:
                _warned_types.add(etype)
                log.warning("Не найден department_id в meta (%s): keys=%s",
                            etype, sorted(meta.keys()))

        elif etype == "messenger_chat.department_removed":
            did = _extract_id(meta, "department_id", "object_id", "object_department_id")
            if did:
                chat.departments.pop(did, None)

    return chats


def expand_memberships(chats: dict[str, ChatState], resolver) -> None:
    """Разворачивает group_added / department_added в конкретных участников.
    ВАЖНО: даёт ТЕКУЩИЙ состав группы, added_at = дата привязки группы."""
    for chat in chats.values():
        for did, meta in list(chat.departments.items()):
            dept_name = resolver.department_name(did) or did
            for uid in resolver.expand_department(did):
                _merge_expanded(chat, uid, resolver, meta, f"department:{dept_name}")
        for gid, meta in list(chat.groups.items()):
            group_name = resolver.group_name(gid) or gid
            for uid in resolver.expand_group(gid):
                _merge_expanded(chat, uid, resolver, meta, f"group:{group_name}")


def _merge_expanded(chat: ChatState, uid: str, resolver,
                    source_meta: dict, origin: str) -> None:
    existing = chat.members.get(uid)
    if existing is not None and existing.source.startswith(
            ("audit_projection", "manual_import")):
        return                                    # точные данные не перетираем
    if existing is not None and existing.source == "group_expansion":
        existing.via = f"{existing.via},{origin}" if existing.via else origin
        return

    info = resolver.user_info(uid)
    member = MemberState(
        uid=uid,
        login=(info.login if info else None) or resolver.login_for_uid(uid),
        role="member",
        added_at=source_meta.get("added_at"),
        added_by_login=source_meta.get("added_by_login"),
        source="group_expansion",
        confidence="low",
        via=origin,
        full_name=(info.full_name if info else None),
        position=(info.position if info else None),
        fio_source="directory" if info and info.full_name else None,
        is_bot=bool(info and info.is_robot),
        bot_evidence="directory" if (info and info.is_robot) else None,
    )
    chat.members[uid] = member
