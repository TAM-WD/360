from __future__ import annotations

import argparse
import csv
import logging
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone

from audit_identities import harvest_identities_from_audit
from clients import Api360Client, AuditLogClient, DirectoryClient
from compare_runs import compare_runs, export_compare, print_compare
from config import Config
from human import (chat_type_ru, chat_type_short_ru, confidence_ru, counters,
                   coverage_ru, date_human, dt_human, evidence_ru, plural,
                   quality_ru, role_ru, share, source_ru)
from identity_store import IdentityStore
from manual_import import (ManualChat, ManualImportResult, ManualRow,
                           classify_identity, excel_serial_to_dt, load_manual,
                           make_manual_key, normalize_identity,
                           resolve_manual_path)
from merge import load_manual_map, merge_manual
from projection import (CHAT_EVENT_TYPES, ChatState, MemberState, build_projection,
                        expand_memberships, normalize_chat_id)
from report import (export_bots, export_chats, export_discrepancies,
                    export_manual_issues, export_members, export_quality,
                    export_readme, export_run_diff, export_summary,
                    export_unresolved_uids)
from resolver import DirectoryResolver, DirectoryUnavailableError
from run_layout import (COMPARE_DIR, create_run_dir, file_fingerprint, list_runs,
                        prune_runs, resolve_run, update_latest, write_manifest)
from snapshots import SnapshotStore
from store import EventStore
from text_utils import clean_text, looks_like_messenger_bot_login

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)-11s %(message)s",
)
log = logging.getLogger("cli")


# ============================== collect ==============================
def cmd_collect(cfg: Config) -> None:
    """Забирает из аудит-лога новые события и складывает их в локальную базу."""
    store = EventStore(cfg.db_path)
    audit = AuditLogClient(cfg.audit_base, cfg.audit_token)
    try:
        now = datetime.now(timezone.utc)
        checkpoint = store.get_checkpoint()
        if checkpoint is None:
            started = now - timedelta(days=cfg.backfill_days)
            log.info("Первый запуск: забираем всю доступную историю, "
                     "начиная с %s (%s дней назад). "
                     "Дальше будем брать только новое.",
                     date_human(started), cfg.backfill_days)
        else:
            started = checkpoint - timedelta(minutes=cfg.overlap_minutes)
            log.info("Забираем события с %s. Взяли запас в %s минут назад, "
                     "чтобы не пропустить те, что пришли с задержкой.",
                     dt_human(started), cfg.overlap_minutes)

        inserted = store.upsert_events(audit.iter_events(
            cfg.org_id, started.isoformat(), now.isoformat(), CHAT_EVENT_TYPES))
        newest = store.max_occurred_at()
        if newest:
            store.set_checkpoint(newest)

        if inserted:
            log.info("Получено новых событий: %s. Всего в базе: %s. "
                     "В следующий раз начнём с %s.",
                     inserted, store.count(), dt_human(newest))
        else:
            log.info("Новых событий нет — с прошлого запуска в чатах ничего "
                     "не менялось. Всего в базе: %s.", store.count())
    finally:
        audit.close()
        store.close()


# ============================== analyze ==============================
def cmd_analyze(cfg: Config) -> None:
    """Собирает отчёты в отдельную папку и сравнивает их с прошлым запуском."""
    if cfg.manual_path:
        cfg.manual_path = resolve_manual_path(cfg.manual_path)

    # прошлый запуск запоминаем до создания новой папки
    previous = None
    if cfg.compare_previous:
        existing = list_runs(cfg.results_dir, command="analyze", only_ok=True)
        previous = existing[-1] if existing else None

    run = create_run_dir(cfg.results_dir, command="analyze", tag=cfg.run_tag)
    out_dir = run.path

    store = EventStore(cfg.db_path)
    directory = DirectoryClient(cfg.directory_base, cfg.directory_token)
    api360 = Api360Client(cfg.api360_base, cfg.directory_token)
    identity_store = IdentityStore(cfg.db_path)
    manual = None
    merge_rep = None
    diff = None
    summary: dict = {}
    load_report: dict = {}
    status = "ok"
    errors: list[str] = []

    try:
        # --- 1. восстанавливаем состояние чатов по событиям ---
        chats = build_projection(store.iter_all_events_ordered())
        log.info("Восстановили состояние %s по %s событиям аудит-лога.",
                 plural(len(chats), "чата", "чатов", "чатов"), store.count())
        if not chats:
            log.warning("Ни одного чата не нашлось. Скорее всего, ещё не "
                        "выполнялась команда collect.")

        # --- 2. выясняем, кто есть кто ---
        resolver = DirectoryResolver(
            directory, api360, cfg.org_id,
            identity_store=identity_store,
            source_mode=cfg.directory_source,
            fail_on_empty=not cfg.allow_empty_directory,
        )
        if cfg.resolve_uids or cfg.manual_path:
            try:
                resolver.preload_users(store=store)
            except DirectoryUnavailableError as exc:
                status = "failed"
                errors.append(str(exc))
                log.error("%s", exc)
                raise SystemExit(2) from exc
            load_report = resolver.load_report

            for chat in chats.values():
                for member in chat.members.values():
                    if member.uid and not member.login:
                        member.login = resolver.login_for_uid(member.uid)
                    info = resolver.user_info(member.uid)
                    if info:
                        if not member.full_name and info.full_name:
                            member.full_name = info.full_name
                            member.fio_source = "directory"
                        if not member.position and info.position:
                            member.position = info.position
                        if info.is_robot and not member.is_bot:
                            member.is_bot = True
                            member.bot_evidence = "directory"

        # --- 3. разворачиваем группы и подразделения в людей ---
        if cfg.expand_groups:
            before = sum(len(chat.members) for chat in chats.values())
            resolver.preload_departments()
            resolver.preload_groups()
            expand_memberships(chats, resolver)
            after = sum(len(chat.members) for chat in chats.values())
            log.info("Развернули группы и подразделения в конкретных людей: "
                     "было %s участников, стало %s.", before, after)

        # --- 4. ручная таблица ---
        scopes = ["audit"]
        if cfg.expand_groups:
            scopes.append("expansion")

        if cfg.manual_path:
            manual = load_manual(cfg.manual_path, sheet=cfg.manual_sheet,
                                 tz_offset=cfg.manual_tz,
                                 date_semantics=cfg.manual_date_semantics)
            export_manual_issues(out_dir, manual)
            export_quality(out_dir, manual)
            merge_rep = merge_manual(
                chats, manual, resolver,
                date_tolerance_days=cfg.date_tolerance_days,
                manual_map=load_manual_map(cfg.manual_map_path))
            export_discrepancies(out_dir, merge_rep)
            identity_store.commit()
            scopes.append("manual")
        else:
            log.info("Ручная таблица не передана. Сведения из прошлых таблиц "
                     "сохраняются — они не будут помечены как удалённые.")

        # --- 5. отчёты ---
        export_chats(out_dir, chats, cfg.include_private)
        export_members(out_dir, chats, cfg.include_private)
        export_bots(out_dir, chats, cfg.include_private)
        export_unresolved_uids(out_dir, chats)
        export_readme(out_dir)

        # --- 6. запоминаем состав, чтобы потом видеть изменения ---
        snapshots = SnapshotStore(cfg.db_path)
        try:
            diff = snapshots.apply_run(chats, cfg.include_private,
                                       scopes=tuple(scopes))
            counts = diff.counts()
            if any(counts[key] for key in ("added", "removed", "role_changed")):
                log.info("Изменения с прошлого запуска: добавлено участников %s, "
                         "удалено %s, сменилась роль у %s.",
                         counts["added"], counts["removed"],
                         counts["role_changed"])
            else:
                log.info("Состав чатов с прошлого запуска не изменился.")
            export_run_diff(out_dir, diff)
        finally:
            snapshots.close()

        summary = export_summary(out_dir, chats=chats, manual=manual,
                                 merge_report=merge_rep, diff=diff)
        _print_summary(summary, load_report, identity_store.stats())
    finally:
        checkpoint = store.get_checkpoint()
        write_manifest(
            run, cfg=cfg, status=status, metrics=summary,
            flags=cfg.flags_snapshot(), errors=errors,
            inputs={"manual": file_fingerprint(cfg.manual_path),
                    "manual_map": file_fingerprint(cfg.manual_map_path)},
            db={"path": os.path.abspath(cfg.db_path),
                "events": store.count(),
                "checkpoint": checkpoint.isoformat() if checkpoint else None})
        identity_store.close()
        directory.close()
        api360.close()
        store.close()

    update_latest(cfg.results_dir, run)
    log.info("Отчёты сохранены: %s", os.path.abspath(out_dir))

    # --- 7. сравнение с прошлым запуском ---
    if cfg.compare_previous and previous is not None:
        result = compare_runs(previous, run)
        compare_dir = os.path.join(run.path, "compare_with_previous")
        export_compare(compare_dir, result)
        print_compare(result, compare_dir)
    elif cfg.compare_previous:
        log.info("Это первый запуск с отчётами — сравнивать пока не с чем.")

    if cfg.keep_runs:
        removed = prune_runs(cfg.results_dir, cfg.keep_runs, command="analyze")
        if removed:
            log.info("Удалили %s, чтобы хранить только последние %s.",
                     plural(len(removed), "старый запуск", "старых запуска",
                            "старых запусков"), cfg.keep_runs)


def _print_summary(summary: dict, load_report: dict | None = None,
                   identity_stats: dict | None = None) -> None:
    print("\n" + "=" * 68)
    print("ИТОГИ")
    print("=" * 68)

    total_chats = summary.get("total_chats", 0)
    by_type = summary.get("chats_by_type", {})
    type_parts = [f"{chat_type_short_ru(key)} {value}"
                  for key, value in sorted(by_type.items(), key=lambda x: -x[1])]
    print(f"Чатов найдено:  {total_chats}")
    if type_parts:
        print(f"  {', '.join(type_parts)}")

    print("\nОткуда сведения о чатах:")
    print(counters(summary.get("coverage_by_status", {}), coverage_ru))

    members_total = summary.get("members_total", 0)
    print(f"\nУчастников:  {members_total}")
    note = {"group_expansion": "← даты добавления приблизительные"}
    print(counters(summary.get("members_by_source", {}), source_ru, note=note))

    confidence = summary.get("members_by_confidence", {})
    if confidence:
        print("\nНасколько можно верить датам добавления:")
        print(counters(confidence, confidence_ru))

    bots = summary.get("bots") or {}
    if bots.get("bot_memberships"):
        print(f"\nБоты в чатах:  {bots['bot_memberships']} "
              f"в {plural(bots.get('chats_with_bots', 0), 'чате', 'чатах', 'чатах')}")
        by_evidence = bots.get("by_evidence") or {}
        pattern_note = {"manual_login_pattern":
                        "← только из таблицы, событий в логе нет"}
        print(counters(by_evidence, evidence_ru, note=pattern_note))
        if bots.get("without_login"):
            print(f"  {'логин узнать не удалось':<32.32} "
                  f"{bots['without_login']:>6}   ← пары в таблице не нашлось")
    else:
        print("\nБотов в чатах не обнаружено.")

    unresolved = summary.get("unresolved_uid_members", 0)
    if unresolved:
        print(f"\nНе удалось узнать логин:  {share(unresolved, members_total)}")
        print("  Обычно это сотрудники других организаций или уволенные.")
        print("  Подробности — в файле unresolved_uids.csv")

    if load_report:
        found = {key: value for key, value in load_report.items()
                 if key != "errors" and value}
        if found:
            labels = {"cloud_api": "справочник, основной адрес",
                      "api360": "справочник, резервный адрес",
                      "audit_log": "из самого аудит-лога",
                      "cache": "из локальной базы"}
            print("\nГде нашли сведения о сотрудниках:")
            print(counters(found, lambda key: labels.get(key, key)))
        if load_report.get("errors"):
            print("  Что не сработало:")
            for item in load_report["errors"]:
                print(f"    {item}")

    if identity_stats and identity_stats.get("users"):
        print(f"\nВ локальной базе сохранено {identity_stats['users']} сотрудников "
              f"и {identity_stats.get('aliases', 0)} их адресов.")
        print("  Пригодится, если справочник станет недоступен или кто-то уволится.")

    manual = summary.get("manual")
    if manual:
        print(f"\nРучная таблица:  {os.path.basename(manual.get('file') or '')}")
        print(f"  строк {manual['rows_total']}, приняли {manual['rows_ok']}, "
              f"чатов {manual['chats']}, ботов {manual['bots']}")
        if manual.get("errors") or manual.get("conflicts"):
            print(f"  ошибок {manual['errors']}, противоречий {manual['conflicts']} "
                  f"— смотрите manual_issues.csv")
        if manual.get("quality_issues"):
            print(f"  замечаний к качеству {manual['quality_issues']} "
                  f"— смотрите data_quality.csv")

    merge = summary.get("merge")
    if merge:
        print("\nСведение таблицы с аудит-логом:")
        print(f"  чатов совпало {merge['matched_chats']}, "
              f"только в таблице {merge['manual_only_chats']}, "
              f"только в логе {merge['audit_only_chats']}")
        if merge.get("ambiguous_chats"):
            print(f"  не удалось сопоставить однозначно: "
                  f"{merge['ambiguous_chats']} — смотрите chat_match_issues.csv")
        if merge.get("learned_identities"):
            print(f"  установили логин по таблице: {merge['learned_identities']} "
                  f"— смотрите learned_identities.csv")
        if merge.get("discrepancies"):
            print(f"\nРасхождений с таблицей:  {merge['discrepancies']}")
            print("  Ничего не исправлялось молча — все спорные места "
                  "в discrepancies.csv")

        split = summary.get("unresolved_split") or {}
        if split.get("total"):
            print(f"\nНе нашлись в справочнике:  {split['total']}")
            if split.get("people"):
                print(f"  {'возможно, из других организаций':<32.32} "
                      f"{split['people']:>6}   ← стоит проверить")
            if split.get("bots"):
                print(f"  {'мессенджер-боты':<32.32} "
                      f"{split['bots']:>6}   ← это нормально, их там и нет")

    print("=" * 68 + "\n")


# ============================== runs ==============================
def cmd_runs(cfg: Config) -> None:
    """Показывает список сделанных запусков."""
    runs = list_runs(cfg.results_dir)
    if not runs:
        print(f"\nВ папке {os.path.abspath(cfg.results_dir)} запусков пока нет.")
        print("Начните с команды:")
        print("  python cli.py run\n")
        return

    command_ru = {"analyze": "отчёты", "validate": "проверка таблицы",
                  "run": "отчёты"}

    print("\n" + "=" * 92)
    print(f"ЗАПУСКИ  ({os.path.abspath(cfg.results_dir)})")
    print("=" * 92)
    print(f"{'№':>3}  {'когда':17}  {'что делали':18}  {'чатов':>6}  "
          f"{'ботов':>6}  {'без логина':>10}  режим")
    print("-" * 92)

    analyze_index = 0
    for run in runs:
        metrics = run.metrics
        flags = run.flags
        modes = []
        if flags.get("expand_groups"):
            modes.append("с группами")
        if flags.get("manual_present"):
            modes.append("с таблицей")
        if flags.get("include_private"):
            modes.append("с личными")
        if run.tag:
            modes.append(f"метка «{run.tag}»")

        position = ""
        if run.command == "analyze":
            analyze_index += 1
            position = str(analyze_index)

        bots = (metrics.get("bots") or {}).get("bot_memberships", "—")
        chats = metrics.get("total_chats", "—")
        unresolved = metrics.get("unresolved_uid_members", "—")
        broken = "  (не завершён)" if run.status != "ok" else ""

        print(f"{position:>3}  {dt_human(run.dt):17.17}  "
              f"{command_ru.get(run.command, run.command):18.18}  "
              f"{str(chats):>6}  {str(bots):>6}  {str(unresolved):>10}  "
              f"{', '.join(modes) or '—'}{broken}")

    print("-" * 92)
    print("Сравнить последний с предыдущим:  python cli.py compare")
    print("Сравнить любые два по номерам:    python cli.py compare --from 1 --to 3")
    print("Номер в первой колонке относится только к запускам с отчётами.\n")


# ============================== compare ==============================
def cmd_compare(cfg: Config, from_ref: str, to_ref: str,
                out_dir: str | None = None) -> None:
    """Сравнивает отчёты двух запусков."""
    from_run = resolve_run(cfg.results_dir, from_ref, command="analyze")
    to_run = resolve_run(cfg.results_dir, to_ref, command="analyze")
    if from_run.run_id == to_run.run_id:
        raise SystemExit(
            f"Указан один и тот же запуск: {from_run.run_id}. "
            f"Посмотрите список: python cli.py runs")

    result = compare_runs(from_run, to_run)
    target = out_dir or os.path.join(
        cfg.results_dir, COMPARE_DIR, f"{from_run.run_id}__vs__{to_run.run_id}")
    export_compare(target, result)
    print_compare(result, target)


# ============================== validate ==============================
def cmd_validate(cfg: Config) -> None:
    """Проверяет ручную таблицу, не обращаясь ни к каким сервисам."""
    if not cfg.manual_path:
        raise SystemExit(
            "Нужно указать файл: python cli.py validate --manual выгрузка.xlsx")

    cfg.manual_path = resolve_manual_path(cfg.manual_path)
    run = create_run_dir(cfg.results_dir, command="validate", tag=cfg.run_tag)
    out_dir = run.path

    manual = load_manual(cfg.manual_path, sheet=cfg.manual_sheet,
                         tz_offset=cfg.manual_tz,
                         date_semantics=cfg.manual_date_semantics)
    export_manual_issues(out_dir, manual)
    export_quality(out_dir, manual)

    print("\n" + "=" * 74)
    print("ПРОВЕРКА ТАБЛИЦЫ")
    print(f"Файл: {os.path.basename(cfg.manual_path)}")
    print(f"Даты в таблице читаем по часовому поясу {cfg.manual_tz}")
    print("=" * 74)
    print(f"Строк: {manual.rows_total}, приняли {manual.rows_ok}. "
          f"Чатов: {len(manual.chats)}. Ботов среди участников: "
          f"{manual.bots_total}.")
    print(f"Ошибок: {len(manual.errors)}. "
          f"Противоречий: {len(manual.conflicts)}. "
          f"Замечаний к качеству: {len(manual.quality_issues)}.")

    multi_date_chats = 0
    for chat in manual.chats.values():
        dates = {row.date for row in chat.members.values() if row.date}
        if len(dates) > 1:
            multi_date_chats += 1

        print(f"\n{chat_type_ru(chat.chat_type).capitalize()} «{chat.chat_name}»")
        line = (f"  участников {len(chat.members)}"
                + (f", из них ботов {chat.bots_count}" if chat.bots_count else ""))
        print(line)
        if len(dates) > 1:
            print("  даты добавления у участников разные — значит это дата "
                  "добавления участника, а не создания чата")
        if chat.created_at:
            print(f"  самая ранняя дата: {date_human(chat.created_at)}")
        print()

        for row in chat.members.values():
            mark = "бот " if row.is_bot else "    "
            date_text = date_human(row.date) if row.date else "—"
            print(f"  {mark}{(row.identity or '—'):42.42} "
                  f"{(row.full_name or '—'):26.26} {date_text}")
            for flag in row.quality_flags:
                detail = (f": «{row.position}»"
                          if flag == "position_looks_like_about" and row.position
                          else "")
                print(f"          {quality_ru(flag)}{detail}")

    if manual.errors:
        print(f"\nОшибки чтения ({len(manual.errors)}):")
        for row_no, message in manual.errors[:20]:
            print(f"  строка {row_no}: {message}")
        if len(manual.errors) > 20:
            print(f"  ещё {len(manual.errors) - 20} — в файле manual_issues.csv")

    if manual.conflicts:
        print(f"\nПротиворечия ({len(manual.conflicts)}):")
        for conflict in manual.conflicts[:20]:
            name = conflict.get("chat_name", "")
            prefix = f"«{name}»: " if name else ""
            print(f"  {prefix}{conflict.get('detail', '')}")

    if manual.quality_issues:
        grouped: dict[str, int] = {}
        for issue in manual.quality_issues:
            grouped[issue["flag"]] = grouped.get(issue["flag"], 0) + 1
        print(f"\nЗамечания к качеству ({len(manual.quality_issues)}):")
        for flag, count in sorted(grouped.items(), key=lambda item: -item[1]):
            print(f"  {plural(count, 'строка', 'строки', 'строк')}: "
                  f"{quality_ru(flag)}")

    if multi_date_chats:
        print(f"\nПодсказка: в {plural(multi_date_chats, 'чате', 'чатах', 'чатах')} "
              f"даты у участников различаются. Значит колонка с датой — это дата "
              f"добавления участника.\nЕсли на самом деле там дата создания чата, "
              f"добавьте ключ --manual-date-semantics chat_created")

    write_manifest(run, cfg=cfg, status="ok", flags=cfg.flags_snapshot(),
                   inputs={"manual": file_fingerprint(cfg.manual_path)},
                   metrics={"rows_total": manual.rows_total,
                            "rows_ok": manual.rows_ok,
                            "chats": len(manual.chats),
                            "bots": manual.bots_total,
                            "errors": len(manual.errors),
                            "conflicts": len(manual.conflicts),
                            "quality_issues": len(manual.quality_issues)})
    print(f"\nПодробности сохранены: {os.path.abspath(out_dir)}")
    print("=" * 74 + "\n")


# ============================== at ==============================
def cmd_at(cfg: Config, chat_key: str, at_iso: str) -> None:
    """Показывает, кто был в чате на указанный момент времени."""
    snapshots = SnapshotStore(cfg.db_path)
    try:
        meta = snapshots.chat_at(chat_key, at_iso)
        members = snapshots.members_at(chat_key, at_iso)

        print("\n" + "=" * 74)
        print(f"СОСТАВ ЧАТА НА {dt_human(at_iso)}")
        print("=" * 74)
        if meta:
            print(f"Название: {meta['name'] or '—'}")
            print(f"Тип:      {chat_type_ru(meta['type'])}")
            if meta.get("description"):
                print(f"Описание: {meta['description']}")
        else:
            print("Сведений о чате на эту дату нет.")
            print("Возможно, на тот момент чата ещё не существовало либо запуск "
                  "с отчётами тогда не делался.")
        print(f"\nУчастников: {len(members)}")
        if members:
            print()
        for row in sorted(members, key=lambda item: (item["scope"],
                                                     item["login"] or "")):
            mark = "бот " if row["is_bot"] else "    "
            print(f"  {mark}{(row['login'] or row['member_key']):42.42} "
                  f"{role_ru(row['role']):12.12} "
                  f"добавлен {dt_human(row['added_at'])}")
        print("=" * 74 + "\n")
    finally:
        snapshots.close()


# ============================== selftest ==============================
# Все данные ниже выдуманы: номера сотрудников, логины, имена и названия
# чатов не относятся ни к одной реальной организации. Домен example.org
# зарезервирован стандартом RFC 2606 именно для примеров.
DEMO_CHAT_ID = "0/0/11111111-2222-3333-4444-555555555555"
DEMO_CHAT_KEY = "11111111-2222-3333-4444-555555555555"
DEMO_ADMIN_UID = "1000000000000001"
DEMO_MEMBER_UID = "1000000000000002"
DEMO_BOT_UID = "1000000000000003"
DEMO_ADMIN_LOGIN = "admin@example.org"
DEMO_ADMIN_NAME = "Петров Пётр Петрович"

SAMPLE_EVENTS = [
    {"user_login": DEMO_ADMIN_LOGIN, "user_name": DEMO_ADMIN_NAME,
     "event": {"uid": int(DEMO_ADMIN_UID), "org_id": 1,
               "occurred_at": "2026-02-16T05:49:57.714000+00:00",
               "type": "messenger_chat.created", "service": "Web",
               "idempotency_id": "ev-001", "status": "Success", "is_system": False,
               "ip": "127.0.0.1", "request_id": "r1",
               "meta": {"chat_id": DEMO_CHAT_ID, "revision": "1",
                        "chat_info": {"name": "рабочий чат", "type": "group",
                                      "description": ""}}}},
    {"user_login": DEMO_ADMIN_LOGIN, "user_name": DEMO_ADMIN_NAME,
     "event": {"uid": int(DEMO_ADMIN_UID), "org_id": 1,
               "occurred_at": "2026-02-16T05:49:57.714000+00:00",
               "type": "messenger_chat.member.added", "service": "Web",
               "idempotency_id": "ev-002", "status": "Success", "is_system": False,
               "ip": "127.0.0.1", "request_id": "r2",
               "meta": {"chat_id": DEMO_CHAT_ID,
                        "chat_info": {"name": "рабочий чат", "type": "group"},
                        "object_uid": DEMO_ADMIN_UID,
                        "member_info": {"role": "admin", "is_robot": False}}}},
    {"user_login": DEMO_ADMIN_LOGIN, "user_name": DEMO_ADMIN_NAME,
     "event": {"uid": int(DEMO_ADMIN_UID), "org_id": 1,
               "occurred_at": "2026-02-16T05:50:10.000000+00:00",
               "type": "messenger_chat.member.added", "service": "Web",
               "idempotency_id": "ev-003", "status": "Success", "is_system": False,
               "ip": "127.0.0.1", "request_id": "r3",
               "meta": {"chat_id": DEMO_CHAT_ID,
                        "chat_info": {"name": "рабочий чат", "type": "group"},
                        "object_uid": DEMO_MEMBER_UID,
                        "member_info": {"role": "member", "is_robot": False}}}},
    {"user_login": DEMO_ADMIN_LOGIN, "user_name": DEMO_ADMIN_NAME,
     "event": {"uid": int(DEMO_ADMIN_UID), "org_id": 1,
               "occurred_at": "2026-02-16T05:50:20.000000+00:00",
               "type": "messenger_chat.member.added", "service": "Web",
               "idempotency_id": "ev-004", "status": "Success", "is_system": False,
               "ip": "127.0.0.1", "request_id": "r4",
               "meta": {"chat_id": DEMO_CHAT_ID,
                        "chat_info": {"name": "рабочий чат", "type": "group"},
                        "object_uid": DEMO_BOT_UID,
                        "member_info": {"role": "member", "is_robot": True}}}},
    # повтор с тем же idempotency_id — проверяем, что дубли не попадают в базу
    {"user_login": DEMO_ADMIN_LOGIN, "user_name": DEMO_ADMIN_NAME,
     "event": {"uid": int(DEMO_ADMIN_UID), "org_id": 1,
               "occurred_at": "2026-02-16T05:50:10.000000+00:00",
               "type": "messenger_chat.member.added", "service": "Web",
               "idempotency_id": "ev-003", "status": "Success", "is_system": False,
               "ip": "127.0.0.1", "request_id": "r3",
               "meta": {"chat_id": DEMO_CHAT_ID,
                        "chat_info": {"name": "рабочий чат", "type": "group"},
                        "object_uid": DEMO_MEMBER_UID,
                        "member_info": {"role": "member", "is_robot": False}}}},
]

MANUAL_CSV = (
    "messenger_chat.created,chat_name,chat_description,Тип,Full name,Job Position,Email\n"
    '46069.24302083333,рабочий чат,,Групповой чат,'
    '"\nПетров Пётр Петрович",,admin@example.org\n'
    '46069.24302083333,рабочий чат,,Групповой чат,'
    'Сидорова Анна,,sidorova@example.org\n'
    '46218.65975694444,старый канал,Информация по компании,Групповой чат,'
    '"\nИванов Иван",Инженер,ivanov@example.org\n'
    '46220.659756886576,старый канал,Информация по компании,Групповой чат,'
    '"\nКузнецова Мария","Привет, я Маша",kuznetsova@example.org\n'
    '46222.659756886576,старый канал,Информация по компании,Групповой чат,'
    'Помощник,,yndx-mssngr-DemoBot01-bot\n'
)

TEST_FLAGS = {"expand_groups": False, "include_private": False,
              "resolve_uids": True, "manual_present": True,
              "manual_date_semantics": "member_added"}


class _StubResolver:
    """Заглушка справочника для проверки связывания по ручной таблице."""

    def __init__(self, robots: set[str] | None = None):
        self.learned: dict[str, str] = {}
        self.robots = robots or set()

    def resolve_identity(self, identity):
        for uid, login in self.learned.items():
            if login == identity:
                return uid, "resolved:manual_identity"
        return None, "not_found"

    def login_for_uid(self, uid):
        return self.learned.get(str(uid))

    def full_name_for_uid(self, uid):
        return None

    def position_for_uid(self, uid):
        return None

    def is_robot_uid(self, uid):
        return str(uid) in self.robots

    def learn_identity(self, uid, login, source="manual"):
        self.learned[str(uid)] = login


def _make_manual_result(chat_name: str, chat_type: str,
                        rows: list[ManualRow]) -> ManualImportResult:
    result = ManualImportResult(source_file="<для проверки>")
    key = make_manual_key(chat_name, chat_type)
    chat = ManualChat(chat_name=chat_name, chat_type=chat_type,
                      description=None, manual_key=key)
    for row in rows:
        chat.members[row.identity] = row
    result.chats[key] = chat
    return result


def _manual_row(row_no: int, chat_name: str, identity: str,
                full_name: str | None = None) -> ManualRow:
    # как и при чтении настоящей таблицы, приводим идентификатор
    # к нижнему регистру
    normalized = normalize_identity(identity)
    kind, is_bot = classify_identity(normalized)
    return ManualRow(row_no=row_no, date=None, chat_name=chat_name,
                     chat_description=None, chat_type="group",
                     full_name=full_name, position=None, identity=normalized,
                     identity_kind=kind, is_bot=is_bot)


def cmd_selftest() -> None:
    """Проверка логики без обращения к сервисам и без токенов."""
    print("\n=== ПРОВЕРКА РАБОТОСПОСОБНОСТИ ===")

    # 1. приведение идентификатора чата к общему виду
    tail = DEMO_CHAT_KEY
    assert normalize_chat_id(f"0/0/{tail}") == tail
    assert normalize_chat_id(f"1/0/{tail}") == tail
    assert normalize_chat_id(f"0/22/{tail}") == tail
    assert normalize_chat_id("guidA_guidB") == normalize_chat_id("guidB_guidA")
    print("[ок] идентификаторы чатов приводятся к единому виду")

    # 2. очистка текста и распознавание логина бота
    assert clean_text("\nИванов  Иван\t") == "Иванов Иван"
    assert looks_like_messenger_bot_login("yndx-mssngr-DemoBot01-bot")
    assert looks_like_messenger_bot_login("yndx-mssngr-Abc123-bot")
    assert not looks_like_messenger_bot_login("ivanov@example.org")
    assert not looks_like_messenger_bot_login("support-bot"), \
        "обычный человек с логином support-bot не должен считаться ботом"
    assert not looks_like_messenger_bot_login("yndx-some-service-bot"), \
        "проверяем только формат мессенджер-ботов"
    print("[ок] логин мессенджер-бота распознаётся, посторонние — нет")

    # 3. даты из Excel
    parsed = excel_serial_to_dt(46069.24302083333, "+03:00")
    assert parsed.strftime("%Y-%m-%d") == "2026-02-16", parsed
    print(f"[ок] дата из Excel читается: 46069.243 → {date_human(parsed)}")

    with tempfile.TemporaryDirectory() as tmp:
        db_path = os.path.join(tmp, "selftest.sqlite3")
        results_dir = os.path.join(tmp, "result")
        manual_path = os.path.join(tmp, "manual.csv")
        with open(manual_path, "w", encoding="utf-8") as handle:
            handle.write(MANUAL_CSV)

        # 4. база событий не принимает дубли
        store = EventStore(db_path)
        inserted = store.upsert_events(SAMPLE_EVENTS)
        assert inserted == 4, f"ожидали 4 записи, дубль лишний, получили {inserted}"
        assert store.count() == 4
        print("[ок] повторные события в базу не попадают")

        # 5. логины из самого аудит-лога, без справочника
        pairs = harvest_identities_from_audit(store)
        assert pairs.get(DEMO_ADMIN_UID) == DEMO_ADMIN_LOGIN, pairs
        print(f"[ок] логины восстанавливаются из аудит-лога без справочника "
              f"({len(pairs)} шт.)")

        # 6. локальная база помнит сотрудников
        identity_store = IdentityStore(db_path)
        identity_store.upsert_user(DEMO_ADMIN_UID, login=DEMO_ADMIN_LOGIN,
                                   full_name=DEMO_ADMIN_NAME, source="audit_log")
        identity_store.upsert_alias(DEMO_ADMIN_LOGIN, DEMO_ADMIN_UID, "audit_log")
        identity_store.commit()
        users, aliases = identity_store.load_all()
        assert DEMO_ADMIN_UID in users
        assert aliases[DEMO_ADMIN_LOGIN] == DEMO_ADMIN_UID
        print(f"[ок] локальная база хранит сотрудников: {identity_store.stats()}")
        identity_store.close()

        # 7. восстановление состояния чата по событиям
        chats = build_projection(store.iter_all_events_ordered())
        assert len(chats) == 1, chats
        chat = next(iter(chats.values()))
        assert chat.chat_key == DEMO_CHAT_KEY
        assert chat.name == "рабочий чат"
        assert chat.type == "group"
        assert chat.created_by_login == DEMO_ADMIN_LOGIN
        assert len(chat.members) == 3
        admin = chat.members[DEMO_ADMIN_UID]
        assert admin.role == "admin"
        assert admin.confidence == "high"
        assert admin.added_by_login == DEMO_ADMIN_LOGIN
        print("[ок] состояние чата и его участников восстанавливается")

        # 8. бот из аудит-лога помечен фактом, а не догадкой
        assert chat.bots_count == 1
        bot_member = chat.members[DEMO_BOT_UID]
        assert bot_member.is_bot is True
        assert bot_member.bot_evidence == "audit_log", bot_member.bot_evidence
        assert admin.bot_evidence is None
        print("[ок] бот из аудит-лога помечен по флагу is_robot")

        # 9. чтение ручной таблицы
        manual = load_manual(manual_path, tz_offset="+03:00")
        assert len(manual.chats) == 2, list(manual.chats.keys())
        assert manual.bots_total == 1
        flags = {issue["flag"] for issue in manual.quality_issues}
        assert "position_looks_like_about" in flags
        assert "glued_full_name" not in flags, \
            "проверка склеенного ФИО должна быть убрана"
        assert "empty_position" not in flags, \
            "пустая должность больше не считается замечанием"
        assert "empty_full_name" not in flags, \
            "пустое ФИО больше не считается замечанием"
        print(f"[ок] таблица прочитана: чатов {len(manual.chats)}, "
              f"ботов {manual.bots_total}, замечаний {len(manual.quality_issues)}")

        # 10. к ботам замечания к качеству не применяются
        old_chat = next(item for item in manual.chats.values()
                        if item.chat_name == "старый канал")
        bot_row = next(row for row in old_chat.members.values() if row.is_bot)
        assert bot_row.identity_kind == "bot_login"
        assert bot_row.quality_flags == [], \
            "у бота не должно быть замечаний: логин вместо почты для него норма"
        print("[ок] логин бота не считается ошибкой заполнения")

        # 11. сведение таблицы с логом без справочника
        merge_rep = merge_manual(chats, manual, None, date_tolerance_days=1)
        assert len(merge_rep.matched_chats) == 1, "чат «рабочий чат» должен совпасть"
        assert chat.manual_confirmed is True
        assert chat.coverage_status == "audit+manual"
        assert len(merge_rep.manual_only_chats) == 1, \
            "чат «старый канал» только в таблице"
        kinds = {item["kind"] for item in merge_rep.discrepancies}
        assert "only_in_manual" in kinds
        assert "only_in_audit" in kinds
        print(f"[ок] таблица сведена с аудит-логом: {merge_rep.counts()}")

        # 12. подсказка о причине для каждой ненайденной строки
        unresolved = merge_rep.unresolved_identities
        assert unresolved, "должны быть строки без пары в справочнике"
        assert all(item.get("hint") for item in unresolved), \
            "к каждой строке нужна подсказка о причине"
        print(f"[ок] у ненайденных есть подсказка о причине "
              f"({len(unresolved)} шт.)")

        # 13. связывание человека: один безымянный участник и одна строка
        solo_chat = ChatState(chat_id_raw="0/0/solo", chat_key="solo",
                              type="group", name="соло")
        solo_chat.members["901"] = MemberState(
            uid="901", role="member", added_at="2026-02-16T05:00:00+00:00",
            added_by_login=DEMO_ADMIN_LOGIN, source="audit_projection",
            confidence="high")
        solo_manual = _make_manual_result("соло", "group", [
            _manual_row(2, "соло", "sidorova@example.org", "Сидорова Анна")])
        stub = _StubResolver()
        solo_report = merge_manual({"solo": solo_chat}, solo_manual, stub)
        assert solo_report.learned_identities, "связывание не сработало"
        assert solo_chat.members["901"].login == "sidorova@example.org"
        assert stub.learned["901"] == "sidorova@example.org"
        print(f"[ок] логин человека установлен по таблице: {stub.learned['901']}")

        # 14. связывание бота: логин и имя бота узнать больше негде
        bot_chat = ChatState(chat_id_raw="0/0/botchat", chat_key="botchat",
                             type="group", name="чат с ботом")
        bot_chat.members["801"] = MemberState(
            uid="801", role="member", added_at="2026-02-16T05:00:00+00:00",
            added_by_login=DEMO_ADMIN_LOGIN, source="audit_projection",
            confidence="high", is_bot=True, bot_evidence="audit_log")
        bot_chat.members["802"] = MemberState(
            uid="802", login="ivanov@example.org", role="admin",
            added_at="2026-02-16T05:00:00+00:00", source="audit_projection",
            confidence="high")
        bot_manual = _make_manual_result("чат с ботом", "group", [
            _manual_row(2, "чат с ботом", "yndx-mssngr-Helper02-bot", "Помощник"),
            _manual_row(3, "чат с ботом", "ivanov@example.org", "Иванов Иван")])
        bot_report = merge_manual({"botchat": bot_chat}, bot_manual, _StubResolver())
        linked = bot_chat.members["801"]
        assert linked.login == "yndx-mssngr-helper02-bot", linked.login
        assert linked.bot_evidence == "audit_log", \
            "факт из аудит-лога не должен подменяться догадкой"
        assert linked.full_name == "Помощник", \
            "имя бота берётся из таблицы — больше его взять негде"
        assert any(item.get("is_bot") for item in bot_report.learned_identities)
        print(f"[ок] логин и имя бота установлены по таблице: {linked.login}")

        # 15. бот только из таблицы: догадка помечена как догадка
        pattern_chat = ChatState(chat_id_raw="0/0/oldchat", chat_key="oldchat",
                                 type="group", name="старый чат")
        pattern_manual = _make_manual_result("старый чат", "group", [
            _manual_row(2, "старый чат", "yndx-mssngr-Legacy03-bot", "Робот")])
        pattern_report = merge_manual({"oldchat": pattern_chat}, pattern_manual,
                                      _StubResolver())
        added = next(member for member in pattern_chat.members.values())
        assert added.is_bot is True
        assert added.bot_evidence == "manual_login_pattern", added.bot_evidence
        assert any(item["status"] == "bot_outside_directory"
                   for item in pattern_report.unresolved_identities), \
            "для бота статус должен отличаться от обычного «не найден»"
        print("[ок] бот из таблицы помечен догадкой и отдельным статусом")

        # 16. расхождение: логин похож на бота, но лог говорит иначе
        conflict_chat = ChatState(chat_id_raw="0/0/conflict", chat_key="conflict",
                                  type="group", name="спорный чат")
        conflict_chat.members["701"] = MemberState(
            uid="701", login="yndx-mssngr-Doubt04-bot", role="member",
            added_at="2026-02-16T05:00:00+00:00", source="audit_projection",
            confidence="high", is_bot=False)
        conflict_manual = _make_manual_result("спорный чат", "group", [
            _manual_row(2, "спорный чат", "yndx-mssngr-Doubt04-bot", "Неясно")])
        conflict_report = merge_manual({"conflict": conflict_chat},
                                       conflict_manual, _StubResolver())
        assert any(item["kind"] == "bot_flag_conflict"
                   for item in conflict_report.discrepancies), \
            "расхождение между логом и видом логина нужно показать"
        assert conflict_chat.members["701"].is_bot is False, \
            "верим аудит-логу, а не виду логина"
        print("[ок] спор между логом и видом логина решается в пользу лога")

        # 17. запоминание состава: повторный запуск ничего не меняет
        snapshots = SnapshotStore(db_path)
        first = snapshots.apply_run(chats, include_private=False,
                                    scopes=("audit", "manual"))
        assert first.added and not first.removed
        second = snapshots.apply_run(chats, include_private=False,
                                     scopes=("audit", "manual"))
        assert second.counts()["added"] == 0
        assert second.counts()["removed"] == 0
        assert second.counts()["role_changed"] == 0
        print(f"[ок] состав запомнен ({len(first.added)} записей), "
              f"повторный запуск изменений не даёт")

        # 18. запуск без таблицы не удаляет её данные
        third = snapshots.apply_run(chats, include_private=False, scopes=("audit",))
        assert third.counts()["removed"] == 0, \
            "запуск без таблицы не должен помечать её участников удалёнными"
        print("[ок] запуск без таблицы не теряет её данные")

        # 19. состав на дату
        at_iso = datetime.now(timezone.utc).isoformat()
        at_members = snapshots.members_at(chat.chat_key, at_iso)
        assert at_members
        snapshots.close()
        print(f"[ок] состав чата на дату восстанавливается "
              f"({len(at_members)} участников)")

        # 20. файлы отчёта
        run_a = create_run_dir(results_dir, command="analyze", tag="base")
        export_chats(run_a.path, chats, False)
        export_members(run_a.path, chats, False)
        export_bots(run_a.path, chats, False)
        export_unresolved_uids(run_a.path, chats)
        export_manual_issues(run_a.path, manual)
        export_quality(run_a.path, manual)
        export_discrepancies(run_a.path, merge_rep)
        export_run_diff(run_a.path, first)
        export_readme(run_a.path)
        summary_a = export_summary(run_a.path, chats=chats, manual=manual,
                                   merge_report=merge_rep, diff=first)
        write_manifest(run_a, cfg=None, status="ok", flags=TEST_FLAGS,
                       metrics=summary_a)
        update_latest(results_dir, run_a)

        with open(os.path.join(run_a.path, "members.csv"), "rb") as handle:
            assert handle.read(3) == b"\xef\xbb\xbf", \
                "нужна метка BOM, иначе Excel ломает кириллицу"
        with open(os.path.join(run_a.path, "members.csv"), "r",
                  encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter=";"))
        assert any(row["is_bot"] == "True" for row in rows)
        assert any(row["bot_evidence"] == "audit_log" for row in rows), \
            "в отчёте должно быть видно, откуда взялся признак бота"
        assert any("Петров" in (row["full_name"] or "") for row in rows), \
            "кириллица должна читаться без искажений"
        assert os.path.isfile(os.path.join(run_a.path, "README.txt"))
        assert os.path.isfile(os.path.join(run_a.path, "manifest.json"))
        assert summary_a["bots"]["by_evidence"].get("audit_log") == 1
        print(f"[ок] отчёты записаны ({len(os.listdir(run_a.path))} файлов), "
              f"кириллица и пояснение README на месте")

        # 21. сравнение двух запусков
        victim_key = next(key for key, member in chat.members.items()
                          if member.role == "member" and not member.is_bot)
        removed_member = chat.members.pop(victim_key)
        chat.members[DEMO_BOT_UID].role = "admin"

        run_b = create_run_dir(results_dir, command="analyze", tag="base")
        export_chats(run_b.path, chats, False)
        export_members(run_b.path, chats, False)
        summary_b = export_summary(run_b.path, chats=chats, manual=manual,
                                   merge_report=merge_rep, diff=first)
        write_manifest(run_b, cfg=None, status="ok", flags=TEST_FLAGS,
                       metrics=summary_b)
        update_latest(results_dir, run_b)

        all_runs = list_runs(results_dir, command="analyze")
        assert len(all_runs) == 2, [item.run_id for item in all_runs]
        assert all_runs[-1].run_id == run_b.run_id

        cmp_result = compare_runs(run_a, run_b)
        assert len(cmp_result.members_removed) == 1, cmp_result.counts()
        assert any(item["field"] == "role" for item in cmp_result.members_changed)
        assert any(item["field"] == "members_count"
                   for item in cmp_result.chats_changed)
        assert not cmp_result.warnings, cmp_result.warnings
        compare_dir = os.path.join(results_dir, COMPARE_DIR, "selftest")
        export_compare(compare_dir, cmp_result)
        assert os.path.isfile(os.path.join(compare_dir, "members_delta.csv"))
        print(f"[ок] сравнение запусков работает: {cmp_result.counts()}")

        # 22. предупреждение о разных условиях запуска
        run_c = create_run_dir(results_dir, command="analyze", tag="expand")
        export_chats(run_c.path, chats, False)
        export_members(run_c.path, chats, False)
        export_summary(run_c.path, chats=chats)
        write_manifest(run_c, cfg=None, status="ok",
                       flags={**TEST_FLAGS, "expand_groups": True},
                       metrics={"total_chats": len(chats)})
        warned = compare_runs(run_b, run_c)
        assert any("разных режимах" in item for item in warned.warnings), \
            warned.warnings
        print("[ок] о несовпадении условий запуска предупреждаем")

        # 23. чистка старых запусков
        removed_runs = prune_runs(results_dir, keep=2, command="analyze")
        assert len(removed_runs) == 1, removed_runs
        assert len(list_runs(results_dir, command="analyze")) == 2
        print("[ок] лишние старые запуски удаляются")

        chat.members[victim_key] = removed_member
        store.close()

    print("\n=== ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ ===\n")


# ============================== main ==============================
OFFLINE_COMMANDS = {"validate", "selftest", "runs", "compare"}


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="msgaudit",
        description="Опись чатов и каналов Мессенджера: аудит-лог, справочник "
                    "сотрудников и ручная таблица. Хранит историю и показывает "
                    "изменения между запусками.")
    parser.add_argument(
        "command",
        choices=["collect", "analyze", "run", "at", "validate", "selftest",
                 "doctor", "runs", "compare"],
        help="collect — забрать новые события; analyze — собрать отчёты; "
             "run — сделать и то, и другое; runs — список запусков; "
             "compare — сравнить два запуска; at — состав чата на дату; "
             "validate — проверить таблицу без обращения к сервисам; "
             "selftest — самопроверка; doctor — проверка доступов")

    # --- ручная таблица ---
    parser.add_argument("--manual", dest="manual_path",
                        help="файл .xlsx, .xlsm или .csv с ручной выгрузкой")
    parser.add_argument("--manual-sheet",
                        help="название листа в Excel (по умолчанию первый)")
    parser.add_argument("--manual-tz", default=None,
                        help="часовой пояс дат в таблице, например +03:00")
    parser.add_argument("--manual-date-semantics", default=None,
                        choices=["member_added", "chat_created"],
                        help="что означает колонка с датой: дату добавления "
                             "участника (по умолчанию) или создания чата")
    parser.add_argument("--manual-map", dest="manual_map_path",
                        help="файл chat_name;type;chat_key для чатов "
                             "с одинаковыми названиями")

    # --- справочник сотрудников ---
    parser.add_argument("--directory-source", default=None,
                        choices=["auto", "cloud", "api360", "none"],
                        help="какой адрес справочника использовать "
                             "(по умолчанию auto — пробуем оба)")
    parser.add_argument("--allow-empty-directory", action="store_true",
                        help="продолжать, даже если справочник недоступен; "
                             "логины и ФИО тогда будут пустыми")

    # --- что включать в отчёт ---
    parser.add_argument("--expand-groups", action="store_true",
                        help="разворачивать группы и подразделения в людей")
    parser.add_argument("--include-private", action="store_true",
                        help="учитывать личные переписки один на один")
    parser.add_argument("--no-resolve-uids", action="store_true",
                        help="не искать логины по номерам сотрудников")
    parser.add_argument("--backfill-days", type=int, default=None,
                        help="за сколько дней забрать историю при первом "
                             "запуске collect (по умолчанию 180)")

    # --- результаты ---
    parser.add_argument("--results-dir", default=None,
                        help="папка с результатами (по умолчанию ./result)")
    parser.add_argument("--run-tag", default=None,
                        help="пометка в названии папки запуска")
    parser.add_argument("--keep-runs", type=int, default=None,
                        help="хранить только указанное число последних запусков")
    parser.add_argument("--no-compare", action="store_true",
                        help="не сравнивать автоматически с прошлым запуском")
    parser.add_argument("--from", dest="from_ref", default="prev",
                        help="для compare: prev, latest, номер из списка runs "
                             "или название папки")
    parser.add_argument("--to", dest="to_ref", default="latest",
                        help="для compare: latest, номер или название папки")
    parser.add_argument("--compare-out", default=None,
                        help="куда положить отчёт сравнения")

    parser.add_argument("--db", dest="db_path", default=None,
                        help="файл локальной базы")
    parser.add_argument("--chat-key",
                        help="для команды at: идентификатор чата из колонки "
                             "chat_key файла chats.csv")
    parser.add_argument("--at",
                        help="для команды at: момент времени в формате ISO 8601, "
                             "например 2026-03-01T00:00:00+00:00")
    args = parser.parse_args()

    if args.command == "selftest":
        cmd_selftest()
        return 0

    cfg = Config.from_env(require_network=args.command not in OFFLINE_COMMANDS)

    if args.manual_path:
        cfg.manual_path = args.manual_path
    if args.manual_sheet:
        cfg.manual_sheet = args.manual_sheet
    if args.manual_tz:
        cfg.manual_tz = args.manual_tz
    if args.manual_date_semantics:
        cfg.manual_date_semantics = args.manual_date_semantics
    if args.manual_map_path:
        cfg.manual_map_path = args.manual_map_path
    if args.directory_source:
        cfg.directory_source = args.directory_source
    if args.backfill_days:
        cfg.backfill_days = args.backfill_days
    if args.results_dir:
        cfg.results_dir = args.results_dir
    if args.run_tag:
        cfg.run_tag = args.run_tag
    if args.keep_runs is not None:
        cfg.keep_runs = args.keep_runs
    if args.db_path:
        cfg.db_path = args.db_path
    cfg.expand_groups = args.expand_groups or cfg.expand_groups
    cfg.include_private = args.include_private or cfg.include_private
    cfg.allow_empty_directory = (args.allow_empty_directory
                                 or cfg.allow_empty_directory)
    if args.no_resolve_uids:
        cfg.resolve_uids = False
    if args.no_compare:
        cfg.compare_previous = False

    if args.command == "collect":
        cmd_collect(cfg)
    elif args.command == "analyze":
        cmd_analyze(cfg)
    elif args.command == "run":
        cmd_collect(cfg)
        cmd_analyze(cfg)
    elif args.command == "validate":
        cmd_validate(cfg)
    elif args.command == "runs":
        cmd_runs(cfg)
    elif args.command == "compare":
        cmd_compare(cfg, args.from_ref, args.to_ref, args.compare_out)
    elif args.command == "doctor":
        from doctor import run_doctor
        run_doctor(cfg)
    elif args.command == "at":
        if not (args.chat_key and args.at):
            parser.error("для команды at нужны --chat-key и --at")
        cmd_at(cfg, args.chat_key, args.at)

    return 0


if __name__ == "__main__":
    sys.exit(main())
