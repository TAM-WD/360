from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class RunDiff:
    run_id: str
    run_at: str
    scopes: tuple[str, ...] = ()
    added: list = field(default_factory=list)
    removed: list = field(default_factory=list)
    role_changed: list = field(default_factory=list)

    def counts(self) -> dict:
        return {"scopes": list(self.scopes), "added": len(self.added),
                "removed": len(self.removed), "role_changed": len(self.role_changed)}


def member_key(member) -> str:
    if member.uid:
        return f"uid::{member.uid}"
    if member.login:
        return f"login::{member.login.casefold()}"
    return "anon::unknown"


def scope_of(source: str | None) -> str:
    """Scope определяется ПЕРВИЧНЫМ источником, чтобы прогон без manual
    не закрывал audit-записи (и наоборот)."""
    src = source or ""
    if src.startswith("manual_import"):
        return "manual"
    if src.startswith("group_expansion"):
        return "expansion"
    return "audit"


class SnapshotStore:
    """SCD2-историзация состава чатов. Закрывает версии только в scope прогона."""

    def __init__(self, db_path: str):
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                run_at TEXT NOT NULL,
                scopes TEXT,
                chats_total INTEGER,
                members_total INTEGER,
                added INTEGER,
                removed INTEGER,
                role_changed INTEGER
            );
            CREATE TABLE IF NOT EXISTS membership_scd (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_key TEXT NOT NULL,
                member_key TEXT NOT NULL,
                scope TEXT NOT NULL,
                login TEXT,
                role TEXT,
                source TEXT,
                confidence TEXT,
                is_bot INTEGER DEFAULT 0,
                full_name TEXT,
                position TEXT,
                added_at TEXT,
                added_by_login TEXT,
                valid_from TEXT NOT NULL,
                valid_to TEXT,
                first_run_id TEXT,
                last_run_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_scd_open
                ON membership_scd(chat_key, member_key, scope, valid_to);
            CREATE TABLE IF NOT EXISTS chat_scd (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_key TEXT NOT NULL,
                name TEXT,
                description TEXT,
                type TEXT,
                valid_from TEXT NOT NULL,
                valid_to TEXT,
                last_run_id TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_chat_scd_open
                ON chat_scd(chat_key, valid_to);
            """
        )
        self.conn.commit()

    def apply_run(self, chats: dict, include_private: bool,
                  scopes: tuple[str, ...] = ("audit",)) -> RunDiff:
        run_id = str(uuid.uuid4())
        run_at = datetime.now(timezone.utc).isoformat()
        diff = RunDiff(run_id=run_id, run_at=run_at, scopes=scopes)

        current: dict[tuple[str, str, str], object] = {}
        for chat in chats.values():
            if not include_private and chat.type == "private":
                continue
            self._track_chat(chat, run_at, run_id)
            for member in chat.members.values():
                scope = scope_of(member.source)
                if scope not in scopes:
                    continue
                current[(chat.chat_key, member_key(member), scope)] = member

        placeholders = ",".join("?" * len(scopes))
        cursor = self.conn.execute(
            f"SELECT * FROM membership_scd WHERE valid_to IS NULL "
            f"AND scope IN ({placeholders})", scopes)
        open_rows = {(r["chat_key"], r["member_key"], r["scope"]): r for r in cursor}

        current_keys, open_keys = set(current), set(open_rows)

        for key in current_keys - open_keys:
            member = current[key]
            self._insert(key, member, run_at, run_id)
            diff.added.append((key[0], key[1], member.login, member.role))

        for key in open_keys - current_keys:
            row = open_rows[key]
            self._close("membership_scd", row["id"], run_at)
            diff.removed.append((key[0], key[1], row["login"], row["role"]))

        for key in current_keys & open_keys:
            member, old = current[key], open_rows[key]
            if (member.role or "") != (old["role"] or ""):
                self._close("membership_scd", old["id"], run_at)
                self._insert(key, member, run_at, run_id,
                             first_run_id=old["first_run_id"] or run_id)
                diff.role_changed.append(
                    (key[0], key[1], member.login, old["role"], member.role))
            else:
                self.conn.execute(
                    "UPDATE membership_scd SET last_run_id=? WHERE id=?",
                    (run_id, old["id"]))

        self.conn.execute(
            "INSERT INTO runs(run_id, run_at, scopes, chats_total, members_total, "
            "added, removed, role_changed) VALUES(?,?,?,?,?,?,?,?)",
            (run_id, run_at, ",".join(scopes), len(chats), len(current),
             len(diff.added), len(diff.removed), len(diff.role_changed)))
        self.conn.commit()
        return diff

    def _track_chat(self, chat, run_at: str, run_id: str) -> None:
        row = self.conn.execute(
            "SELECT * FROM chat_scd WHERE chat_key=? AND valid_to IS NULL",
            (chat.chat_key,)).fetchone()
        payload = (chat.name, chat.description, chat.type)
        if row is None:
            self.conn.execute(
                "INSERT INTO chat_scd(chat_key, name, description, type, "
                "valid_from, valid_to, last_run_id) VALUES(?,?,?,?,?,NULL,?)",
                (chat.chat_key, *payload, run_at, run_id))
        elif (row["name"], row["description"], row["type"]) != payload:
            self._close("chat_scd", row["id"], run_at)
            self.conn.execute(
                "INSERT INTO chat_scd(chat_key, name, description, type, "
                "valid_from, valid_to, last_run_id) VALUES(?,?,?,?,?,NULL,?)",
                (chat.chat_key, *payload, run_at, run_id))
        else:
            self.conn.execute("UPDATE chat_scd SET last_run_id=? WHERE id=?",
                              (run_id, row["id"]))

    def _insert(self, key, member, run_at: str, run_id: str,
                first_run_id: str | None = None) -> None:
        self.conn.execute(
            "INSERT INTO membership_scd(chat_key, member_key, scope, login, role, "
            "source, confidence, is_bot, full_name, position, added_at, "
            "added_by_login, valid_from, valid_to, first_run_id, last_run_id) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,?)",
            (key[0], key[1], key[2], member.login, member.role, member.source,
             member.confidence, 1 if member.is_bot else 0,
             member.full_name, member.position, member.added_at,
             member.added_by_login, run_at, first_run_id or run_id, run_id))

    def _close(self, table: str, row_id: int, valid_to: str) -> None:
        self.conn.execute(f"UPDATE {table} SET valid_to=? WHERE id=?",
                          (valid_to, row_id))

    def members_at(self, chat_key: str, at_iso: str) -> list[dict]:
        """Состав чата на момент времени: 'кто был в чате тогда'."""
        cursor = self.conn.execute(
            "SELECT member_key, login, role, source, scope, is_bot, added_at, "
            "full_name, position FROM membership_scd "
            "WHERE chat_key=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)",
            (chat_key, at_iso, at_iso))
        return [dict(row) for row in cursor]

    def chat_at(self, chat_key: str, at_iso: str) -> dict | None:
        row = self.conn.execute(
            "SELECT name, description, type FROM chat_scd "
            "WHERE chat_key=? AND valid_from<=? AND (valid_to IS NULL OR valid_to>?)",
            (chat_key, at_iso, at_iso)).fetchone()
        return dict(row) if row else None

    def close(self) -> None:
        self.conn.close()
