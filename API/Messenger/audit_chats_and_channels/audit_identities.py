from __future__ import annotations

import json
import logging

log = logging.getLogger("audit-id")


def harvest_identities_from_audit(store) -> dict[str, str]:
    """Извлекает пары uid -> login из инициаторов событий аудит-лога.
    Работает БЕЗ Directory: каждое событие содержит event.uid и user_login."""
    pairs: dict[str, str] = {}
    conflicts = 0

    cursor = store.conn.execute(
        "SELECT uid, user_login, COUNT(*) AS cnt FROM raw_events "
        "WHERE uid IS NOT NULL AND user_login IS NOT NULL "
        "GROUP BY uid, user_login ORDER BY cnt DESC")
    for row in cursor:
        uid, login = str(row["uid"]), row["user_login"]
        if uid in pairs and pairs[uid] != login:
            conflicts += 1          # берём наиболее частый (ORDER BY cnt DESC)
            continue
        pairs.setdefault(uid, login)

    log.info("Из аудит-лога извлечено пар uid->login: %s (конфликтов: %s)",
             len(pairs), conflicts)
    return pairs


def harvest_partner_uids(store) -> dict[str, str]:
    """Дополнительно: partner_uid из приватных чатов, если рядом есть логин."""
    extra: dict[str, str] = {}
    cursor = store.conn.execute(
        "SELECT payload FROM raw_events WHERE type='messenger_chat.created'")
    for row in cursor:
        enriched = json.loads(row["payload"])
        event = enriched.get("event") or {}
        meta = event.get("meta") or {}
        info = meta.get("chat_info") or {}
        partner = info.get("partner_uid")
        if partner and event.get("uid") and enriched.get("user_login"):
            extra.setdefault(str(event["uid"]), enriched["user_login"])
    return extra
