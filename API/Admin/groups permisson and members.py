#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# Скрипт читает все группы организации и выгружает в три CSV-файла
# информацию о группах с 50 и более участниками:
# - сам список групп
# - участников каждой группы
# - список тех, кто имеет право писать на почтовую рассылку группы
#
# Необходимые права OAuth-токена:
# - directory:read_groups
# - directory:read_users
# - ya360_admin:mail_read_mail_list_permissions
#
# Лимиты:
# - 10 RPS на получение групп и участников
# - 30 RPS на выгрузку доступов к рассылкам
# - 10 RPS на получение данных пользователей
#

import csv
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Tuple

import requests


# ============================================================
# НАСТРОЙКИ
# ============================================================

ORG_ID = 8260785
OAUTH_TOKEN = ""

MIN_MEMBERS = 50
PER_PAGE = 100

GROUPS_CSV = "groups.csv"
MEMBERS_CSV = "group_members.csv"
PERMISSIONS_CSV = "group_permissions.csv"

WORKERS = 20
GROUPS_RPS = 10
PERMISSIONS_RPS = 30
USERS_RPS = 10

DIRECTORY_API = "https://api360.yandex.net/directory/v1"
MAIL_API = "https://cloud-api.yandex.net/v1/admin"


# ============================================================
# RATE LIMITER
# ============================================================

class RateLimiter:
    def __init__(self, rps: float):
        self.interval = 1.0 / rps
        self.lock = threading.Lock()
        self.next_allowed = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            if now < self.next_allowed:
                time.sleep(self.next_allowed - now)
                now = time.monotonic()
            self.next_allowed = now + self.interval


groups_limiter = RateLimiter(GROUPS_RPS)
permissions_limiter = RateLimiter(PERMISSIONS_RPS)
users_limiter = RateLimiter(USERS_RPS)


# ============================================================
# HTTP
# ============================================================

def get(url: str, limiter: RateLimiter, params: dict = None) -> dict:
    headers = {
        "Authorization": f"OAuth {OAUTH_TOKEN}",
        "Accept": "application/json",
    }

    for attempt in range(1, 4):
        limiter.wait()
        try:
            r = requests.get(url, headers=headers, params=params, timeout=30)

            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 5))
                print(f"[WARN] 429  ждем {wait}с. - groups permisson and members.py:96", file=sys.stderr)
                time.sleep(wait)
                continue

            if r.status_code >= 500:
                print(f"[WARN] {r.status_code} попытка {attempt}/3 - groups permisson and members.py:101", file=sys.stderr)
                time.sleep(2 * attempt)
                continue

            r.raise_for_status()
            return r.json() if r.text else {}

        except requests.RequestException as e:
            print(f"[WARN] Ошибка: {e} попытка {attempt}/3 - groups permisson and members.py:109", file=sys.stderr)
            time.sleep(2 * attempt)

    raise RuntimeError(f"Не удалось выполнить запрос: {url}")


# ============================================================
# API
# ============================================================

def fetch_groups() -> List[dict]:
    """Получает все группы организации, возвращает только с MIN_MEMBERS+ участниками."""
    all_groups = []
    page = 1

    while True:
        data = get(
            f"{DIRECTORY_API}/org/{ORG_ID}/groups",
            groups_limiter,
            params={"page": page, "perPage": PER_PAGE},
        )

        groups = data.get("groups", [])
        all_groups.extend(groups)

        pages = data.get("pages", 1)
        print(f"[INFO] Страница групп {page}/{pages}, получено: {len(groups)} - groups permisson and members.py:135")

        if page >= pages:
            break
        page += 1

    filtered = [
        g for g in all_groups
        if int(g.get("membersCount") or 0) >= MIN_MEMBERS
    ]

    print(f"[INFO] Всего групп: {len(all_groups)}, с {MIN_MEMBERS}+ участниками: {len(filtered)} - groups permisson and members.py:146")
    return filtered


def fetch_group_members(group_id: str) -> List[dict]:
    """Получает полный список участников группы."""
    data = get(
        f"{DIRECTORY_API}/org/{ORG_ID}/groups/{group_id}",
        groups_limiter,
    )
    return data.get("members") or []


def fetch_permissions(email_id: str) -> List[dict]:
    """
    Получает список субъектов, которым разрешено писать на рассылку.
    Поддерживает три формата ответа API.
    """
    data = get(
        f"{MAIL_API}/org/{ORG_ID}/mail-lists/{email_id}/permissions",
        permissions_limiter,
    )

    subjects = []

    # Формат 1: grants.items[].subject — актуальный формат
    for item in (data.get("grants") or {}).get("items") or []:
        subject = item.get("subject")
        if subject:
            subjects.append({
                "type": subject.get("type", ""),
                "id": str(subject.get("id", "")),
                "org_id": str(subject.get("org_id", "")),
            })

    # Формат 2: permissions[].subjects — запасной вариант
    for permission in data.get("permissions") or []:
        for s in permission.get("subjects") or []:
            subjects.append({
                "type": s.get("type", ""),
                "id": str(s.get("id", "")),
                "org_id": str(s.get("org_id", "")),
            })

    # Формат 3: role_actions[].subjects — запасной вариант
    for action in data.get("role_actions") or []:
        for s in action.get("subjects") or []:
            subjects.append({
                "type": s.get("type", ""),
                "id": str(s.get("id", "")),
                "org_id": str(s.get("org_id", "")),
            })

    return subjects


def fetch_user(user_id: str) -> dict:
    """Получает данные пользователя по ID."""
    return get(
        f"{DIRECTORY_API}/org/{ORG_ID}/users/{user_id}",
        users_limiter,
    )


# ============================================================
# USER PARSING
# ============================================================

def parse_user(data: dict) -> Tuple[str, str, str]:
    """Извлекает (ФИО, email, должность) из ответа API пользователя."""
    u = data.get("user", data) if isinstance(data.get("user"), dict) else data
    name = u.get("name") if isinstance(u.get("name"), dict) else {}

    parts = [
        u.get("lastName") or u.get("last_name") or name.get("last", ""),
        u.get("firstName") or u.get("first_name") or name.get("first", ""),
        u.get("middleName") or u.get("middle_name") or name.get("middle", ""),
    ]

    full_name = " ".join(p for p in parts if p).strip()

    if not full_name:
        display = u.get("displayName") or u.get("display_name", "")
        full_name = display if isinstance(display, str) else ""

    email = (
        u.get("email")
        or u.get("defaultEmail")
        or u.get("default_email")
        or ""
    )

    if not email:
        for contact in u.get("contacts") or []:
            if not isinstance(contact, dict):
                continue
            ctype = str(contact.get("type", "")).lower()
            cval = contact.get("value") or contact.get("email", "")
            if cval and ("email" in ctype or "mail" in ctype):
                email = cval
                break

    position = (
        u.get("position")
        or u.get("jobTitle")
        or u.get("job_title")
        or ""
    )

    return str(full_name), str(email), str(position)


# ============================================================
# ОБОГАЩЕНИЕ ПОЛЬЗОВАТЕЛЕЙ
# ============================================================

def enrich_users(user_ids: List[str]) -> Dict[str, Tuple[str, str, str]]:
    """Параллельно получает (ФИО, email, должность) для всех user_id."""
    result: Dict[str, Tuple[str, str, str]] = {}

    if not user_ids:
        return result

    print(f"[INFO] Получаем данные {len(user_ids)} пользователей... - groups permisson and members.py:269")

    def worker(uid: str) -> Tuple[str, Tuple[str, str, str]]:
        try:
            return uid, parse_user(fetch_user(uid))
        except Exception as e:
            print(f"[ERROR] Пользователь {uid}: {e} - groups permisson and members.py:275", file=sys.stderr)
            return uid, ("", "", f"ERROR: {e}")

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(worker, uid): uid for uid in user_ids}
        done, total = 0, len(futures)

        for f in as_completed(futures):
            uid, data = f.result()
            result[uid] = data
            done += 1
            if done % 50 == 0 or done == total:
                print(f"[INFO] Пользователи: {done}/{total} - groups permisson and members.py:287")

    return result


# ============================================================
# СБОР ДАННЫХ ПО ГРУППАМ
# ============================================================

def collect_group_data(index: int, total: int, group: dict) -> dict:
    """Для одной группы получает участников и permissions."""
    group_id = str(group.get("id", ""))
    print(f"[INFO] [{index}/{total}] {group.get('name')} (id={group_id}) - groups permisson and members.py:299")

    members, members_error = [], None
    try:
        members = fetch_group_members(group_id)
    except Exception as e:
        members_error = str(e)
        print(f"[ERROR] Участники группы {group_id}: {e} - groups permisson and members.py:306", file=sys.stderr)

    permissions, permissions_error = [], None
    email_id = str(group.get("emailId") or "")

    if not email_id:
        permissions_error = "Нет emailId — у группы нет рассылки"
    else:
        try:
            permissions = fetch_permissions(email_id)
        except Exception as e:
            permissions_error = str(e)
            print(f"[ERROR] Permissions группы {group_id}: {e} - groups permisson and members.py:318", file=sys.stderr)

    return {
        "id": group_id,
        "name": group.get("name", ""),
        "description": group.get("description", ""),
        "members_count": int(group.get("membersCount") or 0),
        "email": group.get("email", ""),
        "email_id": email_id,
        "members": members,
        "members_error": members_error,
        "permissions": permissions,
        "permissions_error": permissions_error,
    }


def collect_all(groups: List[dict]) -> List[dict]:
    """Параллельно собирает данные по всем группам."""
    result = []
    total = len(groups)

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {
            ex.submit(collect_group_data, i, total, g): str(g.get("id"))
            for i, g in enumerate(groups, 1)
        }
        for f in as_completed(futures):
            try:
                result.append(f.result())
            except Exception as e:
                print(f"[ERROR] {e} - groups permisson and members.py:348", file=sys.stderr)

    return sorted(result, key=lambda x: x["name"])


# ============================================================
# CSV
# ============================================================

SUBJECT_LABELS = {
    "anonymous": "Любой отправитель",
    "organization": "Вся организация",
    "group": "Группа",
    "department": "Подразделение",
    "shared_mailbox": "Общий почтовый ящик",
}


def write_csv(path: str, fieldnames: List[str], rows: List[dict]) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, delimiter=";")
        w.writeheader()
        w.writerows(rows)
    print(f"[INFO] {path}  {len(rows)} строк - groups permisson and members.py:371")


def export_groups(groups_data: List[dict]) -> None:
    write_csv(
        GROUPS_CSV,
        ["Название группы", "Описание", "Количество участников", "Адрес рассылки"],
        [
            {
                "Название группы": g["name"],
                "Описание": g["description"],
                "Количество участников": g["members_count"],
                "Адрес рассылки": g["email"],
            }
            for g in groups_data
        ],
    )


def export_members(
    groups_data: List[dict],
    users: Dict[str, Tuple[str, str, str]],
) -> None:
    fields = [
        "Название группы", "Адрес рассылки", "Количество участников",
        "Тип", "ID", "ФИО", "email", "Должность",
    ]
    rows = []

    for g in groups_data:
        base = {
            "Название группы": g["name"],
            "Адрес рассылки": g["email"],
            "Количество участников": g["members_count"],
        }

        if g["members_error"]:
            rows.append({**base, "Тип": "ERROR", "ID": "", "ФИО": "", "email": "", "Должность": g["members_error"]})
            continue

        if not g["members"]:
            rows.append({**base, "Тип": "НЕТ УЧАСТНИКОВ", "ID": "", "ФИО": "", "email": "", "Должность": ""})
            continue

        for m in g["members"]:
            uid = str(m.get("id", ""))
            mtype = m.get("type", "")
            full_name, email, position = (
                users.get(uid, ("", "", ""))
                if mtype == "user"
                else (SUBJECT_LABELS.get(mtype, mtype), "", "")
            )
            rows.append({**base, "Тип": mtype, "ID": uid, "ФИО": full_name, "email": email, "Должность": position})

    write_csv(MEMBERS_CSV, fields, rows)


def export_permissions(
    groups_data: List[dict],
    users: Dict[str, Tuple[str, str, str]],
) -> None:
    fields = [
        "Название группы", "Адрес рассылки",
        "Тип", "ID", "ФИО", "email", "Должность",
    ]
    rows = []

    for g in groups_data:
        base = {
            "Название группы": g["name"],
            "Адрес рассылки": g["email"],
        }

        if g["permissions_error"]:
            rows.append({**base, "Тип": "ERROR", "ID": "", "ФИО": "", "email": "", "Должность": g["permissions_error"]})
            continue

        if not g["permissions"]:
            rows.append({**base, "Тип": "НЕТ ДОСТУПОВ", "ID": "", "ФИО": "", "email": "", "Должность": ""})
            continue

        for s in g["permissions"]:
            stype = s.get("type", "")
            sid = str(s.get("id", ""))
            full_name, email, position = (
                users.get(sid, ("", "", ""))
                if stype == "user"
                else (SUBJECT_LABELS.get(stype, stype), "", "")
            )
            rows.append({**base, "Тип": stype, "ID": sid, "ФИО": full_name, "email": email, "Должность": position})

    write_csv(PERMISSIONS_CSV, fields, rows)


# ============================================================
# MAIN
# ============================================================

def main():
    if not OAUTH_TOKEN:
        raise RuntimeError("Укажите OAUTH_TOKEN в начале скрипта.")

    # Шаг 1: группы с MIN_MEMBERS+ участниками
    groups = fetch_groups()
    if not groups:
        print(f"[INFO] Нет групп с {MIN_MEMBERS}+ участниками. - groups permisson and members.py:476")
        return

    # Шаг 2: участники и permissions по каждой группе
    groups_data = collect_all(groups)

    # Шаг 3: ФИО/email/должность для всех пользователей
    user_ids = set()
    for g in groups_data:
        for m in g["members"]:
            if m.get("type") == "user":
                user_ids.add(str(m.get("id", "")))
        for s in g["permissions"]:
            if s.get("type") == "user":
                user_ids.add(str(s.get("id", "")))

    users = enrich_users(sorted(user_ids))

    # Шаг 4: сохраняем CSV
    export_groups(groups_data)
    export_members(groups_data, users)
    export_permissions(groups_data, users)

    print("\n[INFO] Готово: - groups permisson and members.py:499")
    print(f"{GROUPS_CSV}       список групп - groups permisson and members.py:500")
    print(f"{MEMBERS_CSV}   участники групп - groups permisson and members.py:501")
    print(f"{PERMISSIONS_CSV}   кто может писать на рассылку - groups permisson and members.py:502")


if __name__ == "__main__":
    main()
