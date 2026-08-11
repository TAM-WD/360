from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Iterable, Iterator, Optional

log = logging.getLogger("store")

BATCH = 500


def parse_dt(value: str) -> datetime:
    """occurred_at: '2026-07-06T11:45:53.437000+00:00'."""
    return datetime.fromisoformat(value)


class EventStore:
    """Вечный event store. Дедуп по idempotency_id, чекпоинт по occurred_at."""

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            PRAGMA journal_mode=WAL;
            CREATE TABLE IF NOT EXISTS raw_events (
                idempotency_id TEXT PRIMARY KEY,
                occurred_at    TEXT NOT NULL,
                type           TEXT NOT NULL,
                chat_id        TEXT,
                uid            TEXT,
                user_login     TEXT,
                payload        TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_chat ON raw_events(chat_id);
            CREATE INDEX IF NOT EXISTS idx_events_time ON raw_events(occurred_at);
            CREATE TABLE IF NOT EXISTS checkpoint (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            """
        )
        self.conn.commit()

    def upsert_events(self, enriched_events: Iterable[dict]) -> int:
        before = self.conn.total_changes
        batch: list[tuple] = []

        def flush() -> None:
            if batch:
                self.conn.executemany(
                    "INSERT OR IGNORE INTO raw_events "
                    "(idempotency_id, occurred_at, type, chat_id, uid, user_login, payload) "
                    "VALUES (?,?,?,?,?,?,?)",
                    batch,
                )
                self.conn.commit()
                batch.clear()

        for enriched in enriched_events:
            ev = enriched.get("event", {}) or {}
            meta = ev.get("meta") or {}
            idem = ev.get("idempotency_id")
            if not idem:
                continue
            batch.append((
                idem,
                ev.get("occurred_at"),
                ev.get("type"),
                meta.get("chat_id"),
                str(ev["uid"]) if ev.get("uid") is not None else None,
                enriched.get("user_login"),
                json.dumps(enriched, ensure_ascii=False),
            ))
            if len(batch) >= BATCH:
                flush()
        flush()
        return self.conn.total_changes - before

    def get_checkpoint(self) -> Optional[datetime]:
        row = self.conn.execute(
            "SELECT value FROM checkpoint WHERE key='last_occurred_at'"
        ).fetchone()
        return parse_dt(row["value"]) if row else None

    def set_checkpoint(self, moment: datetime) -> None:
        self.conn.execute(
            "INSERT INTO checkpoint(key, value) VALUES('last_occurred_at', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (moment.astimezone(timezone.utc).isoformat(),),
        )
        self.conn.commit()

    def max_occurred_at(self) -> Optional[datetime]:
        row = self.conn.execute("SELECT MAX(occurred_at) AS m FROM raw_events").fetchone()
        return parse_dt(row["m"]) if row and row["m"] else None

    def count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) AS c FROM raw_events").fetchone()["c"]

    def iter_all_events_ordered(self) -> Iterator[dict]:
        cur = self.conn.execute(
            "SELECT payload FROM raw_events ORDER BY occurred_at ASC, idempotency_id ASC"
        )
        for row in cur:
            yield json.loads(row["payload"])

    def close(self) -> None:
        self.conn.close()
