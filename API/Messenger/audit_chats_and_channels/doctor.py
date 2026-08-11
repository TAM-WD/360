from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from clients import Api360Client, AuditLogClient, DirectoryClient
from http_base import HttpError
from store import EventStore

log = logging.getLogger("doctor")


def _mask(token: str) -> str:
    if not token:
        return "не задан"
    return f"{token[:8]}…{token[-4:]}"


def _probe(label: str, fn):
    """Пробный запрос. Ошибку переводим в понятную причину."""
    try:
        result = fn()
        print(f"  [есть]  {label}: {result}")
        return True, result
    except HttpError as exc:
        text = str(exc)
        if "-> 403" in text:
            print(f"  [нет прав]   {label}")
            print("               Сервис отказал в доступе. "
                  "Проверьте права токена.")
        elif "-> 401" in text:
            print(f"  [токен]      {label}")
            print("               Токен недействителен или истёк — нужен новый.")
        elif "-> 404" in text:
            print(f"  [не найден]  {label}")
            print("               Неверный номер организации либо сервис "
                  "недоступен на текущем тарифе.")
        elif "-> 429" in text:
            print(f"  [лимит]      {label}")
            print("               Слишком много запросов. Подождите минуту "
                  "и повторите проверку.")
        elif "failed after" in text:
            print(f"  [нет ответа] {label}")
            print("               Сервис не ответил после нескольких попыток — "
                  "скорее всего, временная неполадка на его стороне.")
        else:
            print(f"  [ошибка]     {label}: {text[:180]}")
        return False, None
    except Exception as exc:                      # noqa: BLE001
        print(f"  [сбой]       {label}: {type(exc).__name__}: {exc}")
        return False, None


def run_doctor(cfg) -> None:
    print("\n" + "=" * 70)
    print("ПРОВЕРКА ДОСТУПОВ")
    print("=" * 70)
    print(f"  Организация:            {cfg.org_id}")
    print(f"  Токен для аудит-лога:   {_mask(cfg.audit_token)}")
    same_token = cfg.audit_token == cfg.directory_token
    suffix = "  (тот же самый)" if same_token else ""
    print(f"  Токен для справочника:  {_mask(cfg.directory_token)}{suffix}")
    if same_token:
        print("  Учтите: одному токену нужны права и на аудит-лог, "
              "и на справочник сотрудников.")

    audit = AuditLogClient(cfg.audit_base, cfg.audit_token)
    directory = DirectoryClient(cfg.directory_base, cfg.directory_token)
    api360 = Api360Client(cfg.api360_base, cfg.directory_token)

    directory_users: list[dict] = []
    api360_users: list[dict] = []

    try:
        print("\nАудит-лог")
        now = datetime.now(timezone.utc)

        def probe_audit():
            events = audit.iter_events(
                cfg.org_id, (now - timedelta(days=7)).isoformat(),
                now.isoformat(), ["messenger_chat.created"])
            first = next(iter(events), None)
            return ("события читаются" if first
                    else "доступ есть, но за последнюю неделю событий не было")

        ok_audit, _ = _probe("чтение событий", probe_audit)

        print("\nСправочник сотрудников")

        def probe_directory_users():
            nonlocal directory_users
            directory_users = list(_take(directory.iter_users(cfg.org_id), 5))
            return f"отвечает, для пробы получили {len(directory_users)} записей"

        ok_users, _ = _probe("основной адрес", probe_directory_users)

        def probe_api360_users():
            nonlocal api360_users
            api360_users = list(_take(api360.iter_users(cfg.org_id), 5))
            return f"отвечает, для пробы получили {len(api360_users)} записей"

        ok_api360, _ = _probe("резервный адрес", probe_api360_users)

        print("\nГруппы и подразделения")
        _probe("группы", lambda: f"{len(list(_take(directory.iter_groups(cfg.org_id), 3)))} шт. (проба)")
        _probe("подразделения", lambda: f"{len(list(_take(directory.iter_departments(cfg.org_id), 3)))} шт. (проба)")

        # Номера сотрудников — персональные данные, поэтому в вывод они не
        # попадают: сравниваем только форму записи. Вывод doctor часто
        # копируют в переписку и тикеты.
        print("\nСверка номеров сотрудников")
        sample = directory_users or api360_users
        if not sample:
            print("  Пропущена: сведений о сотрудниках получить не удалось.")
        else:
            directory_ids = {str(user.get("id")) for user in sample if user.get("id")}
            if directory_ids:
                length = len(next(iter(directory_ids)))
                print(f"  из справочника получено номеров: {len(directory_ids)}, "
                      f"длина номера {length} знаков")
            store = EventStore(cfg.db_path)
            try:
                audit_uids = _audit_object_uids(store, limit=5)
            finally:
                store.close()
            if not audit_uids:
                print("  из аудит-лога: событий пока нет — сначала выполните "
                      "python cli.py collect")
            elif directory_ids:
                length = len(next(iter(audit_uids)))
                print(f"  из аудит-лога получено номеров: {len(audit_uids)}, "
                      f"длина номера {length} знаков")
                if _same_id_shape(directory_ids, audit_uids):
                    print("  Форма записи совпадает — связать данные получится.")
                else:
                    print("  Форма записи НЕ совпадает. Значит номера сотрудников "
                          "в аудит-логе и в справочнике относятся к разным "
                          "системам, и связать их напрямую нельзя.")

        print("\nИТОГ")
        if ok_users or ok_api360:
            print("  Справочник отвечает — в отчёте будут логины, ФИО и должности.")
        elif ok_audit:
            print("  Справочник недоступен, но аудит-лог читается.")
            print("  Логины частично восстановятся из самого аудит-лога,")
            print("  а ФИО и должности останутся пустыми.")
            print("  Запуск в таком режиме: добавьте ключ --allow-empty-directory")
        else:
            print("  Ни аудит-лог, ни справочник не отвечают.")
            print("  Проверьте номер организации и права токенов, затем")
            print("  повторите проверку.")
        print("=" * 70 + "\n")
    finally:
        audit.close()
        directory.close()
        api360.close()


def _take(iterator, count: int):
    for index, item in enumerate(iterator):
        if index >= count:
            return
        yield item


def _audit_object_uids(store: EventStore, limit: int = 5) -> set[str]:
    import json
    found: set[str] = set()
    cursor = store.conn.execute(
        "SELECT payload FROM raw_events "
        "WHERE type='messenger_chat.member.added' LIMIT 200")
    for row in cursor:
        meta = (json.loads(row["payload"]).get("event") or {}).get("meta") or {}
        uid = meta.get("object_uid")
        if uid:
            found.add(str(uid))
        if len(found) >= limit:
            break
    return found


def _same_id_shape(directory_ids: set[str], audit_uids: set[str]) -> bool:
    """Грубая проверка: совпадают ли длина и начало номеров."""
    if not directory_ids or not audit_uids:
        return False
    directory_lengths = {len(item) for item in directory_ids}
    audit_lengths = {len(item) for item in audit_uids}
    if not (directory_lengths & audit_lengths):
        return False
    directory_prefixes = {item[:4] for item in directory_ids}
    audit_prefixes = {item[:4] for item in audit_uids}
    return bool(directory_prefixes & audit_prefixes)
