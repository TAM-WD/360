from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone

log = logging.getLogger("identity")


class IdentityStore:
    """Персистентный кэш uid <-> login/email. Пополняется из всех источников,
    никогда не забывает (важно для уволенных и внешних участников)."""

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS identity_cache (
                uid         TEXT PRIMARY KEY,
                login       TEXT,
                full_name   TEXT,
                position    TEXT,
                is_robot    INTEGER DEFAULT 0,
                is_dismissed INTEGER DEFAULT 0,
                source      TEXT,
                updated_at  TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS identity_alias (
                alias   TEXT PRIMARY KEY,
                uid     TEXT NOT NULL,
                source  TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_alias_uid ON identity_alias(uid);
            """
        )
        self.conn.commit()

    def upsert_user(self, uid: str, *, login: str | None = None,
                    full_name: str | None = None, position: str | None = None,
                    is_robot: bool = False, is_dismissed: bool = False,
                    source: str = "unknown") -> None:
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            """
            INSERT INTO identity_cache(uid, login, full_name, position, is_robot,
                                       is_dismissed, source, updated_at)
            VALUES(?,?,?,?,?,?,?,?)
            ON CONFLICT(uid) DO UPDATE SET
                login=COALESCE(excluded.login, identity_cache.login),
                full_name=COALESCE(excluded.full_name, identity_cache.full_name),
                position=COALESCE(excluded.position, identity_cache.position),
                is_robot=MAX(excluded.is_robot, identity_cache.is_robot),
                is_dismissed=excluded.is_dismissed,
                source=excluded.source,
                updated_at=excluded.updated_at
            """,
            (str(uid), login, full_name, position,
             1 if is_robot else 0, 1 if is_dismissed else 0, source, now))

    def upsert_alias(self, alias: str, uid: str, source: str = "unknown") -> None:
        if not alias or not uid:
            return
        self.conn.execute(
            "INSERT INTO identity_alias(alias, uid, source, updated_at) "
            "VALUES(?,?,?,?) ON CONFLICT(alias) DO UPDATE SET "
            "uid=excluded.uid, source=excluded.source, updated_at=excluded.updated_at",
            (alias.strip().casefold(), str(uid), source,
             datetime.now(timezone.utc).isoformat()))

    def commit(self) -> None:
        self.conn.commit()

    def load_all(self) -> tuple[dict, dict]:
        """-> (users_by_uid, alias_to_uid)."""
        users = {r["uid"]: dict(r) for r in
                 self.conn.execute("SELECT * FROM identity_cache")}
        aliases = {r["alias"]: r["uid"] for r in
                   self.conn.execute("SELECT alias, uid FROM identity_alias")}
        return users, aliases

    def stats(self) -> dict:
        users = self.conn.execute(
            "SELECT COUNT(*) c FROM identity_cache").fetchone()["c"]
        aliases = self.conn.execute(
            "SELECT COUNT(*) c FROM identity_alias").fetchone()["c"]
        by_source = {r["source"]: r["c"] for r in self.conn.execute(
            "SELECT source, COUNT(*) c FROM identity_cache GROUP BY source")}
        return {"users": users, "aliases": aliases, "by_source": by_source}

    def close(self) -> None:
        self.conn.close()
