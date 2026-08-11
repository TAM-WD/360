from __future__ import annotations

import csv
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime

from run_layout import RUN_DIR_RE, RunInfo

log = logging.getLogger("compare")

ENCODING = "utf-8-sig"
DELIMITER = ";"

CHAT_FIELDS = ["type", "name", "description", "created_at", "created_by_login",
               "coverage_status", "origin", "members_count", "bots_count",
               "incomplete", "ambiguous"]

MEMBER_FIELDS = ["role", "added_at", "added_by_login", "source", "confidence",
                 "is_bot", "bot_evidence", "full_name", "position",
                 "identity_kind", "fio_source", "resolve_status", "via"]

# Флаги, различие которых делает сравнение некорректным
CRITICAL_FLAGS = ["expand_groups", "include_private", "manual_present",
                  "manual_date_semantics"]


@dataclass
class CompareResult:
    from_run: str
    to_run: str
    warnings: list[str] = field(default_factory=list)
    chats_added: list[dict] = field(default_factory=list)
    chats_removed: list[dict] = field(default_factory=list)
    chats_changed: list[dict] = field(default_factory=list)
    members_added: list[dict] = field(default_factory=list)
    members_removed: list[dict] = field(default_factory=list)
    members_changed: list[dict] = field(default_factory=list)
    metrics_delta: list[dict] = field(default_factory=list)

    def counts(self) -> dict:
        return {
            "chats_added": len(self.chats_added),
            "chats_removed": len(self.chats_removed),
            "chats_changed": len(self.chats_changed),
            "members_added": len(self.members_added),
            "members_removed": len(self.members_removed),
            "members_changed": len(self.members_changed),
        }

    @property
    def is_empty(self) -> bool:
        return all(value == 0 for value in self.counts().values())


def _read_csv(path: str) -> list[dict]:
    if not os.path.isfile(path):
        return []
    with open(path, "r", encoding=ENCODING, newline="") as handle:
        return list(csv.DictReader(handle, delimiter=DELIMITER))


def member_key(row: dict) -> str:
    """Стабильный ключ участника — совпадает с логикой SCD2."""
    uid = (row.get("uid") or "").strip()
    if uid:
        return f"uid::{uid}"
    login = (row.get("login") or "").strip().casefold()
    if login:
        return f"login::{login}"
    return "anon::unknown"


def _load_artifacts(run: RunInfo) -> tuple[dict, dict, dict]:
    chats = {row["chat_key"]: row
             for row in _read_csv(os.path.join(run.path, "chats.csv"))
             if row.get("chat_key")}
    members: dict[tuple[str, str], dict] = {}
    for row in _read_csv(os.path.join(run.path, "members.csv")):
        chat_key = row.get("chat_key")
        if not chat_key:
            continue
        members[(chat_key, member_key(row))] = row
    summary_path = os.path.join(run.path, "summary.json")
    summary = {}
    if os.path.isfile(summary_path):
        with open(summary_path, "r", encoding="utf-8") as handle:
            summary = json.load(handle)
    return chats, members, summary


def _check_flags(from_run: RunInfo, to_run: RunInfo) -> list[str]:
    warnings: list[str] = []
    flags_a, flags_b = from_run.flags, to_run.flags

    if not flags_a or not flags_b:
        warnings.append(
            "Внимание: у одного из запусков нет описания условий, поэтому "
            "проверить сопоставимость режимов не получилось.")
        return warnings

    if flags_a.get("expand_groups") != flags_b.get("expand_groups"):
        was, now = flags_a.get("expand_groups"), flags_b.get("expand_groups")
        direction = ("в прошлый раз группы не разворачивались, в этот — "
                     "разворачивались" if now else
                     "в прошлый раз группы разворачивались, в этот — нет")
        warnings.append(
            f"Внимание: запуски делались в разных режимах — {direction}.\n"
            f"  Поэтому в списке ниже окажутся сотни людей, которых на самом "
            f"деле никто не добавлял и не удалял.\n"
            f"  Чтобы сравнение было честным, запускайте с одинаковыми ключами.")

    if flags_a.get("include_private") != flags_b.get("include_private"):
        warnings.append(
            "Внимание: в одном из запусков личные чаты учитывались, в другом "
            "нет — часть различий вызвана этим.")

    if flags_a.get("manual_present") != flags_b.get("manual_present"):
        warnings.append(
            "Внимание: в одном из запусков ручная таблица использовалась, в "
            "другом нет — часть различий вызвана этим.")

    if flags_a.get("manual_date_semantics") != flags_b.get("manual_date_semantics"):
        warnings.append(
            "Внимание: даты в таблице трактовались по-разному "
            "(дата добавления участника или дата создания чата).")

    input_a = (from_run.manifest.get("inputs") or {}).get("manual")
    input_b = (to_run.manifest.get("inputs") or {}).get("manual")
    if input_a and input_b and input_a.get("sha1") != input_b.get("sha1"):
        warnings.append(
            "Внимание: ручная таблица между запусками изменилась.\n"
            "  Часть различий ниже — из-за правок в файле, а не из-за "
            "изменений в чатах.")

    return warnings


def _flatten_metrics(summary: dict, prefix: str = "") -> dict:
    flat: dict[str, float] = {}
    for key, value in (summary or {}).items():
        name = f"{prefix}{key}"
        if isinstance(value, dict):
            flat.update(_flatten_metrics(value, prefix=f"{name}."))
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            flat[name] = value
    return flat


def compare_runs(from_run: RunInfo, to_run: RunInfo) -> CompareResult:
    result = CompareResult(from_run=from_run.run_id, to_run=to_run.run_id)
    result.warnings = _check_flags(from_run, to_run)

    chats_a, members_a, summary_a = _load_artifacts(from_run)
    chats_b, members_b, summary_b = _load_artifacts(to_run)

    if not chats_a and not chats_b:
        result.warnings.append("В обоих прогонах нет chats.csv — нечего сравнивать")

    # ---- чаты ----
    for key in sorted(set(chats_b) - set(chats_a)):
        row = chats_b[key]
        result.chats_added.append({
            "chat_key": key, "chat_name": row.get("name"),
            "chat_type": row.get("type"), "created_at": row.get("created_at"),
            "created_by_login": row.get("created_by_login"),
            "members_count": row.get("members_count"),
            "bots_count": row.get("bots_count")})
    for key in sorted(set(chats_a) - set(chats_b)):
        row = chats_a[key]
        result.chats_removed.append({
            "chat_key": key, "chat_name": row.get("name"),
            "chat_type": row.get("type"), "created_at": row.get("created_at"),
            "created_by_login": row.get("created_by_login"),
            "members_count": row.get("members_count"),
            "bots_count": row.get("bots_count")})
    for key in sorted(set(chats_a) & set(chats_b)):
        row_a, row_b = chats_a[key], chats_b[key]
        for field_name in CHAT_FIELDS:
            value_a = (row_a.get(field_name) or "").strip()
            value_b = (row_b.get(field_name) or "").strip()
            if value_a != value_b:
                result.chats_changed.append({
                    "chat_key": key,
                    "chat_name": row_b.get("name") or row_a.get("name"),
                    "field": field_name, "from_value": value_a,
                    "to_value": value_b})

    # ---- участники ----
    for key in sorted(set(members_b) - set(members_a)):
        row = members_b[key]
        result.members_added.append(_member_row(key, row))
    for key in sorted(set(members_a) - set(members_b)):
        row = members_a[key]
        result.members_removed.append(_member_row(key, row))
    for key in sorted(set(members_a) & set(members_b)):
        row_a, row_b = members_a[key], members_b[key]
        for field_name in MEMBER_FIELDS:
            value_a = (row_a.get(field_name) or "").strip()
            value_b = (row_b.get(field_name) or "").strip()
            if value_a != value_b:
                result.members_changed.append({
                    "chat_key": key[0], "member_key": key[1],
                    "chat_name": row_b.get("chat_name") or row_a.get("chat_name"),
                    "login": row_b.get("login") or row_a.get("login"),
                    "field": field_name, "from_value": value_a,
                    "to_value": value_b})

    # ---- метрики ----
    flat_a, flat_b = _flatten_metrics(summary_a), _flatten_metrics(summary_b)
    for name in sorted(set(flat_a) | set(flat_b)):
        value_a = flat_a.get(name)
        value_b = flat_b.get(name)
        if value_a == value_b:
            continue
        delta = None
        if isinstance(value_a, (int, float)) and isinstance(value_b, (int, float)):
            delta = value_b - value_a
        result.metrics_delta.append({
            "metric": name,
            "from_value": "" if value_a is None else value_a,
            "to_value": "" if value_b is None else value_b,
            "delta": "" if delta is None else delta})

    return result


def _member_row(key: tuple[str, str], row: dict) -> dict:
    return {"chat_key": key[0], "member_key": key[1],
            "chat_name": row.get("chat_name"), "login": row.get("login"),
            "uid": row.get("uid"), "full_name": row.get("full_name"),
            "role": row.get("role"), "added_at": row.get("added_at"),
            "added_by_login": row.get("added_by_login"),
            "is_bot": row.get("is_bot"), "source": row.get("source"),
            "confidence": row.get("confidence")}


# ---------------------------------------------------------------- экспорт
def _write(path: str, header: list[str], rows: list[dict]) -> None:
    with open(path, "w", newline="", encoding=ENCODING) as handle:
        writer = csv.writer(handle, delimiter=DELIMITER)
        writer.writerow(header)
        for row in rows:
            writer.writerow([row.get(column, "") for column in header])


def export_compare(out_dir: str, result: CompareResult) -> None:
    os.makedirs(out_dir, exist_ok=True)

    chat_rows = ([{**r, "change": "added"} for r in result.chats_added] +
                 [{**r, "change": "removed"} for r in result.chats_removed])
    _write(os.path.join(out_dir, "chats_delta.csv"),
           ["change", "chat_key", "chat_name", "chat_type", "created_at",
            "created_by_login", "members_count", "bots_count"], chat_rows)

    _write(os.path.join(out_dir, "chats_changed.csv"),
           ["chat_key", "chat_name", "field", "from_value", "to_value"],
           result.chats_changed)

    member_rows = ([{**r, "change": "added"} for r in result.members_added] +
                   [{**r, "change": "removed"} for r in result.members_removed])
    _write(os.path.join(out_dir, "members_delta.csv"),
           ["change", "chat_key", "chat_name", "member_key", "login", "uid",
            "full_name", "role", "added_at", "added_by_login", "is_bot",
            "source", "confidence"], member_rows)

    _write(os.path.join(out_dir, "members_changed.csv"),
           ["chat_key", "chat_name", "member_key", "login", "field",
            "from_value", "to_value"], result.members_changed)

    _write(os.path.join(out_dir, "metrics_delta.csv"),
           ["metric", "from_value", "to_value", "delta"], result.metrics_delta)

    payload = {"from_run": result.from_run, "to_run": result.to_run,
               "counts": result.counts(), "warnings": result.warnings,
               "metrics_delta": result.metrics_delta}
    with open(os.path.join(out_dir, "compare_summary.json"), "w",
              encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def print_compare(result: CompareResult, out_dir: str | None = None) -> None:
    from human import (chat_type_short_ru, dt_human, plural, role_ru)

    def _run_time(run_id: str) -> str:
        """Достаёт дату, время и метку из названия папки запуска."""
        match = RUN_DIR_RE.match(run_id)
        if not match:
            return run_id
        try:
            moment = datetime.strptime(f"{match.group(2)}{match.group(3)}",
                                       "%Y%m%d%H%M%S")
        except ValueError:
            return run_id
        text = moment.strftime("%d.%m.%Y %H:%M")
        tag = match.group(4)
        return f"{text}, метка «{tag}»" if tag else text


    print("\n" + "=" * 68)
    print("ЧТО ИЗМЕНИЛОСЬ")
    print(f"  было:  {_run_time(result.from_run)}")
    print(f"  стало: {_run_time(result.to_run)}")
    print("=" * 68)

    for warning in result.warnings:
        print(f"  {warning}")
    if result.warnings:
        print("-" * 68)

    counts = result.counts()
    print(f"  Чаты:      появилось {counts['chats_added']}, "
          f"исчезло {counts['chats_removed']}, "
          f"изменилось полей {counts['chats_changed']}")
    print(f"  Участники: добавлено {counts['members_added']}, "
          f"удалено {counts['members_removed']}, "
          f"изменилось полей {counts['members_changed']}")

    if result.is_empty:
        print("\n  Изменений нет — состояние точно такое же.")
    else:
        print()
        for row in result.chats_added[:10]:
            print(f"  + новый чат    {str(row['chat_name'])[:32]:32.32} "
                  f"{chat_type_short_ru(row['chat_type']):11.11} "
                  f"{plural(int(row['members_count'] or 0), 'участник', 'участника', 'участников')}")
        for row in result.chats_removed[:10]:
            print(f"  - пропал чат   {str(row['chat_name'])[:32]:32.32} "
                  f"{chat_type_short_ru(row['chat_type']):11.11}")
        for row in result.members_added[:15]:
            who = "бот      " if row.get("is_bot") == "True" else "участник "
            print(f"  + {who}    {str(row['chat_name'])[:24]:24.24} "
                  f"{str(row['login'] or row['member_key'])[:32]:32.32} "
                  f"роль: {role_ru(row['role'])}")
        for row in result.members_removed[:15]:
            who = "бот      " if row.get("is_bot") == "True" else "участник "
            print(f"  - {who}    {str(row['chat_name'])[:24]:24.24} "
                  f"{str(row['login'] or row['member_key'])[:32]:32.32}")
        for row in result.members_changed[:15]:
            field_name = row["field"]
            old_value, new_value = row["from_value"], row["to_value"]
            if field_name == "role":
                label = "роль"
                old_value, new_value = role_ru(old_value), role_ru(new_value)
            elif field_name == "added_at":
                label = "дата добавления"
                old_value, new_value = dt_human(old_value), dt_human(new_value)
            elif field_name == "full_name":
                label = "ФИО"
            elif field_name == "position":
                label = "должность"
            else:
                label = field_name
            print(f"  ~ {label:14.14} {str(row['chat_name'])[:20]:20.20} "
                  f"{str(row['login'])[:26]:26.26} "
                  f"{str(old_value)[:18]} → {str(new_value)[:18]}")

        shown = (min(len(result.chats_added), 10) +
                 min(len(result.chats_removed), 10) +
                 min(len(result.members_added), 15) +
                 min(len(result.members_removed), 15) +
                 min(len(result.members_changed), 15))
        total = sum(counts.values())
        if total > shown:
            print(f"\n  Показано {shown} из {total}. Полные списки — в файлах CSV.")

    watched = {
        "total_chats": "Чатов стало",
        "members_total": "Участников стало",
        "bots.bot_memberships": "Ботов в чатах",
        "bots.chats_with_bots": "Чатов с ботами",
        "unresolved_uid_members": "Без логина",
    }
    important = [item for item in result.metrics_delta if item["metric"] in watched]
    if important:
        print("-" * 68)
        for item in important:
            label = watched[item["metric"]]
            delta = item["delta"]
            suffix = f"  ({delta:+g})" if delta not in ("", None) else ""
            print(f"  {label:20.20} {item['from_value']} → "
                  f"{item['to_value']}{suffix}")

    if out_dir:
        print("-" * 68)
        print(f"  Подробные таблицы: {os.path.abspath(out_dir)}")
    print("=" * 68 + "\n")
