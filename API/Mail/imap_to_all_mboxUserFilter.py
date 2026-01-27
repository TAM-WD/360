#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт предназначен для выгрузки писем в заданном ящике
---------------------------------------------
Особенности:
  • A↔B режим (обоюдная переписка)
  • Fallback для Sent по получателям
  • Фильтр по датам SINCE/UNTIL
  • Полная устойчивость к IMAP разрывам
  • Rate limiter (15 RPS) для обхода лимитов запросов
  • Автообновление токена при AUTHENTICATIONFAILED
  • Поддержка кириллических имён папок (IMAP-UTF7)
"""

import os
import re
import time
import base64
import imaplib
import email
import requests
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime, getaddresses
from requests.adapters import HTTPAdapter, Retry
import imaplib

# ======================= CONFIG ============================

CLIENT_ID = ""  # ← вставь client_id сервисного приложения
CLIENT_SECRET = ""  # ← вставь client_secret сервисного приложения
EMAIL_LOGIN = "example@email.ru"  # ← вставь

MAIL_SCOPE = "mail:imap_ro"
IMAP_HOST = "imap.yandex.ru"
IMAP_PORT = 993

# Ограничение IMAP команд
RPS_LIMIT = 6
_last_call = 0.0


def rate_limit():
    global _last_call
    now = time.time()
    delta = now - _last_call
    if delta < 1.0 / RPS_LIMIT:
        time.sleep((1.0 / RPS_LIMIT) - delta)
    _last_call = time.time()


# Папки
INCLUDE_FOLDERS = ["INBOX", "Sent", "Отправленные"]
EXCLUDE_FOLDERS = []

# Фильтр по датам
SINCE_DATE = ""
UNTIL_DATE = ""

# Пара A↔B
PAIR_A = ""
PAIR_B = ""
ALIASES_A = []
ALIASES_B = []
OWNER_ALIASES = [""]

# Поведение
READONLY_SELECT = True
HEADERS_BATCH_UID = 400
RFC822_BATCH_UID = 400
FETCH_RETRY_ATTEMPTS = 5

# Путь вывода
OUT_ROOT = os.path.join(
    os.path.expanduser("~/Desktop"),
    f"{(EMAIL_LOGIN or 'Mailbox').replace('@','_')}_MBOX",
)

# ===========================================================


def parse_cfg_date(s):
    return datetime.strptime(s, "%Y-%m-%d") if s else None


SINCE_DT = parse_cfg_date(SINCE_DATE)
UNTIL_DT = parse_cfg_date(UNTIL_DATE)
if UNTIL_DT:
    UNTIL_DT += timedelta(days=1)


def _canon(addr):
    addr = (addr or "").strip().lower()
    m = re.match(r"^([^@+]+)(\+[^@]*)?@(.+)$", addr)
    if m:
        local, _, domain = m.groups()
        return f"{local}@{domain}"
    return addr


def _addr_set_from_headers(*headers):
    out = set()
    for _, a in getaddresses(headers):
        if a:
            out.add(_canon(a))
    return out


# ========== IMAP UTF-7 ==========
def encode_imap_utf7(s):
    out, buf = [], []

    def flush():
        if not buf:
            return
        b = "".join(buf).encode("utf-16-be")
        enc = base64.b64encode(b).decode("ascii").replace("/", ",").rstrip("=")
        out.append("&" + enc + "-")
        buf.clear()

    for ch in s:
        if 0x20 <= ord(ch) <= 0x7E and ch != "&":
            flush()
            out.append(ch)
        elif ch == "&":
            flush()
            out.append("&-")
        else:
            buf.append(ch)
    flush()
    return "".join(out)


def decode_imap_utf7(s):
    try:
        s.encode("ascii")
        return s
    except:
        pass
    res = []
    i = 0
    while i < len(s):
        if s[i] != "&":
            res.append(s[i])
            i += 1
            continue
        j = i + 1
        while j < len(s) and s[j] != "-":
            j += 1
        if j == i + 1:
            res.append("&")
            i = j + 1
            continue
        enc = s[i + 1 : j].replace(",", "/")
        pad = (4 - len(enc) % 4) % 4
        enc += "=" * pad
        try:
            raw = base64.b64decode(enc)
            seg = raw.decode("utf-16-be")
        except:
            seg = s[i : j + 1]
        res.append(seg)
        i = j + 1
    return "".join(res)


def ensure_imap_mailbox(name):
    try:
        name.encode("ascii")
        return name
    except:
        return encode_imap_utf7(name)


# ========== TOKEN + AUTH ==========
def token_exchange(email_addr, scope):
    s = requests.Session()
    s.mount("https://", HTTPAdapter(max_retries=Retry(total=3, backoff_factor=0.5)))
    print("→ Getting token...")
    r = s.post(
        "https://oauth.yandex.ru/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "subject_token": email_addr,
            "subject_token_type": "urn:yandex:params:oauth:token-type:email",
            "scope": scope,
        },
        timeout=30,
    )
    print(datetime.now(), "token:", r.status_code)
    r.raise_for_status()
    return r.json()["access_token"]


def xoauth2_login(login, token):
    print(f"→ Connecting to IMAP ({login})...")
    M = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    ir = base64.b64encode(
        f"user={login}\x01auth=Bearer {token}\x01\x01".encode()
    ).decode()
    tag = M._new_tag()
    rate_limit()
    M.send(f"{tag} AUTHENTICATE XOAUTH2 {ir}\r\n".encode())
    try:
        typ, data = M._command_complete("AUTHENTICATE", tag)
        if typ != "OK":
            raise imaplib.IMAP4.error(f"XOAUTH2 failed: {typ} {data}")
        print("✓ AUTH OK")
        return M
    except imaplib.IMAP4.abort as e:
        if "AUTHENTICATE Completed" in str(e):
            M.state = "AUTH"
            print("✓ AUTH workaround ok")
            return M
        raise


# ========== HELPERS ==========
def is_sent_like_folder(raw_name: str) -> bool:
    """
    Определяет, что папка — исходящие/отправленные письма.
    """
    if not raw_name:
        return False
    human = decode_imap_utf7(raw_name).lower()
    return any(
        x in human
        for x in [
            "sent",
            "sent items",
            "отправлен",
            "исходящ",
            "outbox",
            "отправленные",
        ]
    )


def safe_noop(M):
    try:
        rate_limit()
        tag = M._new_tag()
        M.send(f"{tag} NOOP\r\n".encode())
        typ, _ = M._command_complete("NOOP", tag)
        return typ == "OK"
    except:
        return False


def reconnect_and_reselect(login, token, folder, auth_failed=False):
    """
    Безопасное переподключение с возможным обновлением токена.
    """
    print(f"↻ Reconnecting ({folder})... {'[refresh token]' if auth_failed else ''}")
    if auth_failed:
        token = token_exchange(login, MAIL_SCOPE)

    for attempt in range(1, 6):
        try:
            M = xoauth2_login(login, token)
            mb = ensure_imap_mailbox(folder)
            rate_limit()
            try:
                typ, _ = M.select(f'"{mb}"', readonly=READONLY_SELECT)
            except Exception:
                typ, _ = M.select(mb, readonly=READONLY_SELECT)
            if typ == "OK":
                print(f"✓ Reconnected (attempt {attempt})")
                return M, token
        except Exception as e:
            print(f"⚠ reconnect attempt {attempt}/5 failed: {type(e).__name__} → {e}")
            time.sleep(1.5 * attempt)
    raise RuntimeError(f"❌ Failed to reconnect after 5 attempts ({folder})")


def uid_fetch_safe(M, token, login, folder, batch, cmd):
    """
    Устойчивый FETCH с перезапуском IMAP и обновлением токена при необходимости.
    """
    seq = b",".join(batch).decode()
    last_exc = None

    for attempt in range(1, FETCH_RETRY_ATTEMPTS + 1):
        try:
            rate_limit()
            typ, data = M.uid("FETCH", seq, cmd)
            if typ == "OK" and data:
                return [item[1] for item in data if isinstance(item, tuple)], M, token

            # если сервер вернул AUTHENTICATIONFAILED в ответе
            if data and any(
                isinstance(d, bytes) and b"AUTHENTICATIONFAILED" in d for d in data
            ):
                raise imaplib.IMAP4.abort("AUTHENTICATIONFAILED")

        except imaplib.IMAP4.abort as e:
            auth_fail = "AUTHENTICATIONFAILED" in str(e)
            print(
                f"↪ IMAP abort ({'auth fail' if auth_fail else 'eof'}) → reconnecting..."
            )
            M, token = reconnect_and_reselect(
                login, token, folder, auth_failed=auth_fail
            )
            time.sleep(1.2 * attempt)
            continue

        except Exception as e:
            last_exc = e
            print(
                f"↪ FETCH attempt {attempt}/{FETCH_RETRY_ATTEMPTS} failed: {type(e).__name__} → {e}"
            )
            try:
                M, token = reconnect_and_reselect(login, token, folder)
            except Exception as e2:
                last_exc = e2
            time.sleep(1.5 * attempt)
            continue

    raise last_exc


# ========== MATCH ==========
def match_by_rules(hdr_bytes: bytes, folder_name: str) -> bool:
    """
    Ищет письма, относящиеся к владельцу:
      • если указаны PAIR_A/PAIR_B — ищет переписку A↔B (в обе стороны)
      • если указаны OWNER_ALIASES — они равноправны EMAIL_LOGIN
      • если указаны SINCE/UNTIL_DATE — фильтрует по дате
      • если ничего не задано — выгружает всё
    """
    msg = email.message_from_bytes(hdr_bytes)
    text = hdr_bytes.decode("utf-8", "ignore").lower()

    # --- 1️⃣ Дата ---
    try:
        dh = msg.get("Date")
        dt = parsedate_to_datetime(dh) if dh else None
        if dt and dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    except Exception:
        dt = None

    if SINCE_DT and dt and dt < SINCE_DT:
        return False
    if UNTIL_DT and dt and dt >= UNTIL_DT:
        return False

    # --- 2️⃣ Извлекаем все адреса ---
    def addrset(*headers):
        out = set()
        for _, a in getaddresses(headers):
            if a:
                out.add(_canon(a))
        return out

    from_like = addrset(
        msg.get("From", ""),
        msg.get("Sender", ""),
        msg.get("Resent-From", ""),
        msg.get("Resent-Sender", ""),
    )
    to_like = addrset(
        msg.get("To", ""),
        msg.get("Cc", ""),
        msg.get("Bcc", ""),
        msg.get("Delivered-To", ""),
        msg.get("X-Original-To", ""),
        msg.get("Resent-To", ""),
        msg.get("Resent-Cc", ""),
        msg.get("Resent-Bcc", ""),
    )
    participants = from_like | to_like

    # --- 3️⃣ Владелец и алиасы ---
    owner_equiv = set()
    if EMAIL_LOGIN:
        owner_equiv.add(_canon(EMAIL_LOGIN))
    if OWNER_ALIASES:
        owner_equiv |= {_canon(x) for x in OWNER_ALIASES}

    # --- 4️⃣ Если задана пара (A↔B) ---
    if PAIR_A and PAIR_B:
        equiv_A = {_canon(PAIR_A)} | {_canon(x) for x in (ALIASES_A or [])}
        equiv_B = {_canon(PAIR_B)} | {_canon(x) for x in (ALIASES_B or [])}
        hasA = any(a in participants for a in equiv_A)
        hasB = any(b in participants for b in equiv_B)

        # Переписка A↔B в обе стороны
        if hasA and hasB:
            return True

        # Sent: владелец = отправитель, ищем вторую сторону где угодно
        if is_sent_like_folder(folder_name):
            if any(x in from_like for x in equiv_A) and any(y in text for y in equiv_B):
                return True
            if any(x in from_like for x in equiv_B) and any(y in text for y in equiv_A):
                return True
        return False

    # --- 5️⃣ Если нет пары — ищем письма владельца ---
    if owner_equiv:
        # Входящие (владельца видно в To, Cc, Bcc)
        if any(a in participants for a in owner_equiv):
            return True

        # Исходящие (в Sent, где владелец в From)
        if is_sent_like_folder(folder_name) and any(
            a in from_like for a in owner_equiv
        ):
            return True

        # fallback: если адрес встречается хоть где-то в заголовках
        if any(a in text for a in owner_equiv):
            return True

        return False

    # --- 6️⃣ Если вообще ничего не задано — выгружаем всё ---
    return True


# ========== MBOX ==========
def append_raw_to_mbox(path, raw, owner):
    try:
        d = email.message_from_bytes(raw).get("Date")
        dt = parsedate_to_datetime(d) if d else None
    except:
        dt = None
    when = (dt or datetime.now()).strftime("%a %b %d %H:%M:%S %Y")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "ab") as f:
        f.write(f"From {owner} {when}\n".encode())
        f.write(raw.replace(b"\r\n", b"\n").replace(b"\n", b"\r\n"))
        f.write(b"\r\n")


# ========== DUMP ==========
def dump_folder_to_mbox(M, token, folder, out_mbox_path):
    if "|" in folder:
        folder = folder.split("|")[-1].strip().strip('"')
    folder = folder.strip()
    human_name = decode_imap_utf7(folder)
    print(f"→ Dumping {human_name}...")

    mb = ensure_imap_mailbox(folder)
    rate_limit()
    try:
        typ, _ = M.select(f'"{mb}"', readonly=READONLY_SELECT)
    except imaplib.IMAP4.error:
        try:
            typ, _ = M.select(mb, readonly=READONLY_SELECT)
        except Exception as e:
            print(f"⚠️ SELECT failed: {folder} ({e})")
            return

    if typ != "OK":
        print(f"⚠️ skip: {folder}")
        return

    rate_limit()
    typ, data = M.uid("SEARCH", None, "ALL")
    if typ != "OK" or not data or not data[0]:
        print(f"{human_name}: empty")
        return

    uids = data[0].split()
    total = len(uids)
    print(f"{human_name} | {total} msgs")

    header_cmd = b"(BODY.PEEK[HEADER.FIELDS (From To Cc Bcc Date Delivered-To)])"
    selected = []

    for i in range(0, total, HEADERS_BATCH_UID):
        batch = uids[i : i + HEADERS_BATCH_UID]
        hdrs, M, token = uid_fetch_safe(
            M, token, EMAIL_LOGIN, folder, batch, header_cmd
        )
        for hdr, uid in zip(hdrs, batch):
            if match_by_rules(hdr, folder):
                selected.append(uid)
        print(
            f"  headers {min(i+HEADERS_BATCH_UID,total)}/{total}, matched={len(selected)}"
        )

    if not selected:
        print(f"{human_name}: no matches → skip")
        return

    added = 0
    for i in range(0, len(selected), RFC822_BATCH_UID):
        batch = selected[i : i + RFC822_BATCH_UID]
        raws, M, token = uid_fetch_safe(
            M, token, EMAIL_LOGIN, folder, batch, b"(RFC822)"
        )
        for raw in raws:
            append_raw_to_mbox(out_mbox_path, raw, EMAIL_LOGIN)
            added += 1
        print(
            f"  RFC822 {min(i+RFC822_BATCH_UID,len(selected))}/{len(selected)} → saved {added}"
        )

    print(f"{human_name}: done, saved {added}")


# ========== MAIN ==========
def main():
    print(f"→ START: {datetime.now()}")
    token = token_exchange(EMAIL_LOGIN, MAIL_SCOPE)
    M = xoauth2_login(EMAIL_LOGIN, token)

    print("→ Checking folders...")
    typ, data = M.list()
    folders = []
    if typ == "OK" and data:
        for raw in data:
            nm = raw.decode().split(' "/" ')[-1].strip('"')
            folders.append(decode_imap_utf7(nm))
    if not folders:
        folders = ["INBOX", "Sent", "Отправленные"]

    folders = [
        f
        for f in folders
        if (not INCLUDE_FOLDERS or any(x.lower() in f.lower() for x in INCLUDE_FOLDERS))
        and not any(x.lower() in f.lower() for x in EXCLUDE_FOLDERS)
    ]

    print(f"✓ Found {len(folders)} folders")
    os.makedirs(OUT_ROOT, exist_ok=True)
    print("OUT:", OUT_ROOT)

    for folder in folders:
        folder_dir = os.path.join(OUT_ROOT, re.sub(r'[\\/:*?"<>|]+', " ", folder))
        out_path = os.path.join(folder_dir, "folder.mbox")
        dump_folder_to_mbox(M, token, folder, out_path)

    try:
        M.logout()
    except:
        pass
    print("✓ Done.")


if __name__ == "__main__":
    main()