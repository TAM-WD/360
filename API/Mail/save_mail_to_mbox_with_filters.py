#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для выгрузки писем из почтового ящика Яндекс 360 с выбором режима фильтрации

Особенности:
  • A↔B режим (обоюдная переписка)
  • Fallback для Sent по получателям
  • Фильтр по датам SINCE/UNTIL
  • Улучшенная устойчивость к IMAP разрывам
  • Rate limiter для обхода лимитов запросов
  • Автообновление токена при AUTHENTICATIONFAILED
  • Поддержка кириллических имён папок (IMAP-UTF7)
  • Поддержка спецсимволов
  • Выбор режима фильтрации

"""

import os
import re
import sys
import time
import json
import base64
import imaplib
import email
import socket
import ssl
import gc
import requests
import traceback
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime, getaddresses
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from threading import Lock
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple, Set
from enum import Enum

# ======================= CONFIG ============================

CLIENT_ID = "" # ← вставь client_id сервисного приложения c правами mail:imap_ro
CLIENT_SECRET = "" # ← вставь client_secret сервисного приложения c правами mail:imap_ro
EMAIL_LOGIN = "example@email.ru"

MAIL_SCOPE = "mail:imap_ro"
IMAP_HOST = "imap.yandex.ru"
IMAP_PORT = 993

RPS_LIMIT = 6

# ======================= РЕЖИМ ФИЛЬТРАЦИИ ==================
# 
# Выберите один из режимов:
#
# "ALL"      - Выгрузить ВСЕ письма без фильтрации
#              (только фильтр по датам, если указан)
#
# "OWNER"    - Выгрузить письма, где владелец ящика (EMAIL_LOGIN)
#              присутствует в полях From/To/Cc/Bcc
#              Также можно добавить алиасы в OWNER_ALIASES
#
# "PAIR"     - Выгрузить переписку между двумя адресами (A ↔ B)
#              Укажите PAIR_A и PAIR_B ниже
#
FILTER_MODE = "ALL"  # "ALL", "OWNER" или "PAIR"

# ======================= ФИЛЬТР ПО ПАПКАМ ==================

# Папки для обработки (пустой список = все папки)
INCLUDE_FOLDERS = []  # Например: ["INBOX", "Sent"]

# Папки для исключения
EXCLUDE_FOLDERS = []

# ======================= ФИЛЬТР ПО ДАТАМ ==================
# Формат: "YYYY-MM-DD" или пусто для отключения

SINCE_DATE = ""  # Письма начиная с этой даты (включительно)
UNTIL_DATE = ""  # Письма до этой даты (не включая)

# ======================= РЕЖИМ "OWNER" =====================
# Дополнительные адреса владельца (алиасы)
OWNER_ALIASES = []  # Например: ["alias@domain.ru", "old-email@domain.ru"]

# ======================= РЕЖИМ "PAIR" ======================
# Адреса для поиска переписки A ↔ B
PAIR_A = ""  # Например: "user1@domain.ru"
PAIR_B = ""  # Например: "user2@domain.ru"

# Алиасы для каждой стороны
ALIASES_A = []  # Например: ["user1-alias@domain.ru"]
ALIASES_B = []  # Например: ["user2-old@domain.ru"]

# ======================= ТЕХНИЧЕСКИЕ НАСТРОЙКИ =============

READONLY_SELECT = True
HEADERS_BATCH_UID = 150
RFC822_BATCH_UID = 50

MAX_RETRIES = 5
RETRY_BACKOFF = 1.5
IMAP_CONNECT_TIMEOUT = 60
KEEPALIVE_INTERVAL = 25

BATCH_PAUSE = 1.5
GC_INTERVAL = 5

# Путь для сохранения
OUT_ROOT = os.path.join(
    os.path.expanduser("~/Desktop"),
    f"{(EMAIL_LOGIN or 'Mailbox').replace('@', '_')}_MBOX",
)

# ===========================================================

_last_call = 0.0
_rate_lock = Lock()
_token_cache: Dict[str, Tuple[str, float]] = {}
_token_cache_lock = Lock()
_http_session: Optional[requests.Session] = None

logger = None


def setup_logging():
    import logging
    log_dir = OUT_ROOT
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger("mail_export")


# ======================= PROGRESS =======================

@dataclass
class ExportProgress:
    email_login: str
    started_at: str
    filter_mode: str = "ALL"
    folders: Dict[str, dict] = field(default_factory=dict)
    completed_folders: List[str] = field(default_factory=list)
    skipped_folders: List[str] = field(default_factory=list)
    current_folder: Optional[str] = None
    last_updated: Optional[str] = None
    
    def to_dict(self) -> dict:
        return {
            'email_login': self.email_login,
            'started_at': self.started_at,
            'filter_mode': self.filter_mode,
            'folders': self.folders,
            'completed_folders': self.completed_folders,
            'skipped_folders': self.skipped_folders,
            'current_folder': self.current_folder,
            'last_updated': self.last_updated
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'ExportProgress':
        return cls(
            email_login=data.get('email_login', ''),
            started_at=data.get('started_at', ''),
            filter_mode=data.get('filter_mode', 'ALL'),
            folders=data.get('folders', {}),
            completed_folders=data.get('completed_folders', []),
            skipped_folders=data.get('skipped_folders', []),
            current_folder=data.get('current_folder'),
            last_updated=data.get('last_updated')
        )


class ProgressManager:
    def __init__(self, out_root: str, email_login: str):
        self.progress_file = os.path.join(out_root, "export_progress.json")
        self.email_login = email_login
        self.progress: Optional[ExportProgress] = None
        self._lock = Lock()
    
    def load(self) -> ExportProgress:
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                if data.get('email_login') == self.email_login:
                    saved_mode = data.get('filter_mode', 'ALL')
                    if saved_mode != FILTER_MODE:
                        logger.warning(f"⚠️ Filter mode changed from '{saved_mode}' to '{FILTER_MODE}'")
                        logger.warning("   Starting fresh export (progress reset)")
                    else:
                        self.progress = ExportProgress.from_dict(data)
                        logger.info(f"✓ Loaded progress: {len(self.progress.completed_folders)} completed, "
                                   f"{len(self.progress.skipped_folders)} skipped")
                        return self.progress
            except Exception as e:
                logger.warning(f"Could not load progress: {e}")
        
        self.progress = ExportProgress(
            email_login=self.email_login,
            started_at=datetime.now().isoformat(),
            filter_mode=FILTER_MODE
        )
        return self.progress
    
    def save(self):
        if not self.progress:
            return
        with self._lock:
            try:
                self.progress.last_updated = datetime.now().isoformat()
                temp_file = self.progress_file + ".tmp"
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(self.progress.to_dict(), f, ensure_ascii=False, indent=2)
                os.replace(temp_file, self.progress_file)
            except Exception as e:
                logger.warning(f"Could not save progress: {e}")
    
    def mark_folder_started(self, folder: str, total_uids: int):
        self.progress.current_folder = folder
        self.progress.folders[folder] = {
            'total_uids': total_uids,
            'processed_uids': [],
            'matched_uids': [],
            'saved_uids': [],
            'failed_uids': [],
            'completed': False,
            'last_batch_index': 0
        }
        self.save()
    
    def update_folder_batch(self, folder: str, batch_index: int, processed: List[str], matched: List[str]):
        if folder in self.progress.folders:
            fp = self.progress.folders[folder]
            fp['last_batch_index'] = batch_index
            fp['processed_uids'].extend(processed)
            fp['matched_uids'].extend(matched)
            self.save()
    
    def update_folder_saved(self, folder: str, saved_uids: List[str], failed_uids: List[str] = None):
        if folder in self.progress.folders:
            self.progress.folders[folder]['saved_uids'].extend(saved_uids)
            if failed_uids:
                self.progress.folders[folder]['failed_uids'].extend(failed_uids)
            self.save()
    
    def mark_folder_completed(self, folder: str):
        if folder in self.progress.folders:
            self.progress.folders[folder]['completed'] = True
        if folder not in self.progress.completed_folders:
            self.progress.completed_folders.append(folder)
        self.progress.current_folder = None
        self.save()
    
    def mark_folder_skipped(self, folder: str, reason: str):
        if folder not in self.progress.skipped_folders:
            self.progress.skipped_folders.append(folder)
        self.progress.folders[folder] = {'skipped': True, 'reason': reason}
        self.save()
    
    def mark_folder_error(self, folder: str, error: str):
        if folder in self.progress.folders:
            self.progress.folders[folder]['error'] = error
        self.save()
    
    def get_folder_state(self, folder: str) -> Optional[dict]:
        return self.progress.folders.get(folder)
    
    def is_folder_completed(self, folder: str) -> bool:
        return folder in self.progress.completed_folders
    
    def is_folder_skipped(self, folder: str) -> bool:
        return folder in self.progress.skipped_folders
    
    def clear(self):
        if os.path.exists(self.progress_file):
            os.remove(self.progress_file)


# ======================= HELPERS =======================

def rate_limit():
    global _last_call
    with _rate_lock:
        now = time.time()
        delta = now - _last_call
        min_interval = 1.0 / RPS_LIMIT
        if delta < min_interval:
            time.sleep(min_interval - delta)
        _last_call = time.time()


def get_http_session() -> requests.Session:
    global _http_session
    if _http_session is None:
        _http_session = requests.Session()
        retry_strategy = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=20)
        _http_session.mount('https://', adapter)
        _http_session.mount('http://', adapter)
    return _http_session


def get_cached_token(email: str) -> Optional[str]:
    with _token_cache_lock:
        if email in _token_cache:
            token, expiry = _token_cache[email]
            if time.time() < (expiry - 300):
                return token
            del _token_cache[email]
    return None


def cache_token(email: str, token: str, expires_in: int):
    with _token_cache_lock:
        _token_cache[email] = (token, time.time() + expires_in)


def force_cleanup():
    try:
        gc.collect()
    except:
        pass


def parse_cfg_date(s: str) -> Optional[datetime]:
    return datetime.strptime(s, "%Y-%m-%d") if s else None


SINCE_DT = parse_cfg_date(SINCE_DATE)
UNTIL_DT = parse_cfg_date(UNTIL_DATE)
if UNTIL_DT:
    UNTIL_DT += timedelta(days=1)


def _canon(addr: str) -> str:
    addr = (addr or "").strip().lower()
    m = re.match(r"^([^@+]+)(\+[^@]*)?@(.+)$", addr)
    if m:
        return f"{m.group(1)}@{m.group(3)}"
    return addr


def _addr_set_from_headers(*headers) -> Set[str]:
    out = set()
    for _, a in getaddresses(headers):
        if a:
            out.add(_canon(a))
    return out


# ======================= IMAP UTF-7 =======================

def encode_imap_utf7(s: str) -> str:
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


def decode_imap_utf7(s: str) -> str:
    if not s or '&' not in s:
        return s
    try:
        s.encode("ascii")
    except:
        return s
    
    res, i = [], 0
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
        enc = s[i + 1:j].replace(",", "/")
        enc += "=" * ((4 - len(enc) % 4) % 4)
        try:
            res.append(base64.b64decode(enc).decode("utf-16-be"))
        except:
            res.append(s[i:j + 1])
        i = j + 1
    return "".join(res)


# ======================= TOKEN =======================

def token_exchange(email_addr: str, scope: str, force_refresh: bool = False) -> str:
    if not force_refresh:
        cached = get_cached_token(email_addr)
        if cached:
            return cached
    
    logger.info("→ Getting token...")
    session = get_http_session()
    
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.post(
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
            if r.status_code == 200:
                data = r.json()
                token = data["access_token"]
                expires_in = data.get("expires_in", 3600)
                cache_token(email_addr, token, expires_in)
                logger.info(f"✓ Token obtained (expires in {expires_in}s)")
                return token
            logger.warning(f"Token request failed: {r.status_code}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Token error (attempt {attempt}): {e}")
        
        if attempt < MAX_RETRIES:
            time.sleep(RETRY_BACKOFF ** attempt)
    
    raise RuntimeError("Failed to obtain token")


# ======================= IMAP CONNECTION =======================

PERMANENT_FOLDER_ERRORS = [
    "Bad folder name",
    "Nonselectable folder", 
    "No such folder",
    "Folder encoding error",
    "NONEXISTENT",
]


def is_permanent_folder_error(error_msg: str) -> bool:
    error_lower = error_msg.lower()
    for pattern in PERMANENT_FOLDER_ERRORS:
        if pattern.lower() in error_lower:
            return True
    return False


def get_skip_reason(error_msg: str) -> str:
    error_lower = error_msg.lower()
    if "bad folder name" in error_lower:
        return "Invalid folder name (server cannot access)"
    if "nonselectable" in error_lower:
        return "Folder is not selectable (container only)"
    if "no such folder" in error_lower or "nonexistent" in error_lower:
        return "Folder does not exist"
    if "encoding error" in error_lower:
        return "Folder name encoding error"
    return f"Server error: {error_msg[:50]}"


class IMAPConnection:
    def __init__(self, login: str, token: str):
        self.login = login
        self.token = token
        self.conn: Optional[imaplib.IMAP4_SSL] = None
        self.current_folder: Optional[str] = None
        self.current_folder_raw: Optional[str] = None
        self.last_activity = time.time()
        self._lock = Lock()
        self._tag_counter = 0
    
    def _get_tag(self) -> str:
        self._tag_counter += 1
        return f"TAG{self._tag_counter:04d}"
    
    def connect(self) -> bool:
        with self._lock:
            return self._connect_internal()
    
    def _connect_internal(self) -> bool:
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                self._close_internal()
                socket.setdefaulttimeout(IMAP_CONNECT_TIMEOUT)
                
                ctx = ssl.create_default_context()
                self.conn = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT, ssl_context=ctx, timeout=IMAP_CONNECT_TIMEOUT)
                
                auth_string = f"user={self.login}\x01auth=Bearer {self.token}\x01\x01"
                auth_bytes = base64.b64encode(auth_string.encode()).decode()
                
                rate_limit()
                tag = self.conn._new_tag()
                self.conn.send(f"{tag} AUTHENTICATE XOAUTH2 {auth_bytes}\r\n".encode())
                
                try:
                    typ, data = self.conn._command_complete("AUTHENTICATE", tag)
                    if typ != "OK":
                        raise imaplib.IMAP4.error(f"XOAUTH2 failed: {typ}")
                except imaplib.IMAP4.abort as e:
                    if "AUTHENTICATE Completed" in str(e):
                        self.conn.state = "AUTH"
                    else:
                        raise
                
                self.last_activity = time.time()
                logger.info(f"✓ IMAP connected to {self.login}")
                return True
                
            except imaplib.IMAP4.error as e:
                if "AUTHENTICATIONFAILED" in str(e):
                    logger.warning("Auth failed, refreshing token...")
                    self.token = token_exchange(self.login, MAIL_SCOPE, force_refresh=True)
                else:
                    logger.warning(f"IMAP error (attempt {attempt}): {e}")
            except Exception as e:
                logger.warning(f"Connection error (attempt {attempt}): {type(e).__name__}: {e}")
            
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** attempt)
        
        return False
    
    def _close_internal(self):
        if self.conn:
            try:
                self.conn.logout()
            except:
                pass
            try:
                if self.conn.socket():
                    self.conn.socket().close()
            except:
                pass
            self.conn = None
            self.current_folder = None
            self.current_folder_raw = None
    
    def close(self):
        with self._lock:
            self._close_internal()
    
    def is_alive(self) -> bool:
        if not self.conn:
            return False
        try:
            rate_limit()
            typ, _ = self.conn.noop()
            self.last_activity = time.time()
            return typ == "OK"
        except:
            return False
    
    def keepalive(self) -> bool:
        if time.time() - self.last_activity < KEEPALIVE_INTERVAL:
            return True
        return self.is_alive()
    
    def ensure_connected(self) -> bool:
        with self._lock:
            if self.conn and self.is_alive():
                return True
            return self._connect_internal()
    
    def _select_with_literal(self, folder_imap: str, readonly: bool) -> Tuple[bool, str]:
        cmd = "EXAMINE" if readonly else "SELECT"
        folder_bytes = folder_imap.encode('utf-8')
        
        try:
            sock = self.conn.socket()
            tag = self._get_tag()
            
            cmd_line = f"{tag} {cmd} {{{len(folder_bytes)}}}\r\n".encode()
            sock.sendall(cmd_line)
            
            buf = b""
            while b'\r\n' not in buf:
                chunk = sock.recv(4096)
                if not chunk:
                    return False, "Connection closed"
                buf += chunk
            
            first_line = buf.split(b'\r\n')[0]
            
            if not first_line.startswith(b'+'):
                return False, first_line.decode(errors='replace')
            
            sock.sendall(folder_bytes + b'\r\n')
            
            buf = buf.split(b'\r\n', 1)[1] if b'\r\n' in buf else b""
            tag_bytes = tag.encode()
            
            while tag_bytes not in buf:
                chunk = sock.recv(4096)
                if not chunk:
                    return False, "Connection closed during response"
                buf += chunk
            
            for line in buf.split(b'\r\n'):
                if line.startswith(tag_bytes):
                    if b' OK' in line:
                        self.conn.state = 'SELECTED'
                        return True, ""
                    else:
                        return False, line.decode(errors='replace')
            
            return False, "No tagged response found"
            
        except Exception as e:
            return False, f"Literal error: {e}"
    
    def select_folder(self, folder_decoded: str, folder_raw: str = None, readonly: bool = True) -> Tuple[bool, str]:
        if not self.ensure_connected():
            return False, "Connection failed"
        
        if folder_raw is None:
            folder_raw = folder_decoded
        
        try:
            folder_raw.encode('ascii')
            folder_imap = folder_raw
        except UnicodeEncodeError:
            folder_imap = encode_imap_utf7(folder_raw)
        
        last_error = ""
        
        for attempt in range(1, MAX_RETRIES + 1):
            rate_limit()
            
            try:
                escaped = folder_imap.replace('\\', '\\\\').replace('"', '\\"')
                typ, data = self.conn.select(f'"{escaped}"', readonly=readonly)
                if typ == "OK":
                    self.current_folder = folder_decoded
                    self.current_folder_raw = folder_raw
                    self.last_activity = time.time()
                    return True, ""
                last_error = f"{typ} {data}"
            except imaplib.IMAP4.error as e:
                last_error = str(e)
            
            try:
                typ, data = self.conn.select(folder_imap, readonly=readonly)
                if typ == "OK":
                    self.current_folder = folder_decoded
                    self.current_folder_raw = folder_raw
                    self.last_activity = time.time()
                    return True, ""
                last_error = f"{typ} {data}"
            except imaplib.IMAP4.error as e:
                last_error = str(e)
            
            success, error = self._select_with_literal(folder_imap, readonly)
            if success:
                self.current_folder = folder_decoded
                self.current_folder_raw = folder_raw
                self.last_activity = time.time()
                return True, ""
            last_error = error
            
            if is_permanent_folder_error(last_error):
                return False, last_error
            
            logger.warning(f"SELECT failed (attempt {attempt}): {last_error[:80]}")
            
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF ** attempt)
                if not self.ensure_connected():
                    return False, "Reconnection failed"
        
        return False, f"Max retries: {last_error}"
    
    def uid_search(self, criteria: str = "ALL") -> Optional[List[bytes]]:
        if not self.ensure_connected():
            return None
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                rate_limit()
                typ, data = self.conn.uid("SEARCH", None, criteria)
                if typ == "OK" and data and data[0]:
                    self.last_activity = time.time()
                    return data[0].split()
                return []
            except imaplib.IMAP4.abort as e:
                logger.warning(f"IMAP abort on SEARCH: {e}")
                if "AUTHENTICATIONFAILED" in str(e):
                    self.token = token_exchange(self.login, MAIL_SCOPE, force_refresh=True)
                if not self._reconnect_to_folder():
                    return None
            except Exception as e:
                logger.warning(f"SEARCH error (attempt {attempt}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF ** attempt)
                    if not self._reconnect_to_folder():
                        return None
        return None
    
    def uid_fetch(self, uids: List[bytes], cmd: bytes) -> Tuple[List[Tuple[bytes, bytes]], bool]:
        if not uids:
            return [], True
        if not self.ensure_connected():
            return [], False
        
        seq = b",".join(uids).decode()
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                rate_limit()
                typ, data = self.conn.uid("FETCH", seq, cmd.decode() if isinstance(cmd, bytes) else cmd)
                
                if typ == "OK" and data:
                    self.last_activity = time.time()
                    results = []
                    for item in data:
                        if isinstance(item, tuple) and len(item) >= 2:
                            header = item[0]
                            uid_match = re.search(rb'UID (\d+)', header) if isinstance(header, bytes) else None
                            uid = uid_match.group(1) if uid_match else None
                            results.append((uid, item[1]))
                    return results, True
                
                return [], True
                
            except imaplib.IMAP4.abort as e:
                logger.warning(f"IMAP abort on FETCH: {e}")
                if "AUTHENTICATIONFAILED" in str(e):
                    self.token = token_exchange(self.login, MAIL_SCOPE, force_refresh=True)
                if not self._reconnect_to_folder():
                    return [], False
            except Exception as e:
                logger.warning(f"FETCH error (attempt {attempt}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF ** attempt)
                    if not self._reconnect_to_folder():
                        return [], False
        
        return [], False
    
    def _reconnect_to_folder(self) -> bool:
        folder = self.current_folder
        folder_raw = self.current_folder_raw
        if not self._connect_internal():
            return False
        if folder:
            success, _ = self.select_folder(folder, folder_raw, READONLY_SELECT)
            return success
        return True
    
    def list_folders(self) -> List[Tuple[str, str]]:
        if not self.ensure_connected():
            return []
        
        folders = []
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                rate_limit()
                typ, data = self.conn.list('""', '*')
                
                if typ != "OK" or not data:
                    if attempt < MAX_RETRIES:
                        time.sleep(RETRY_BACKOFF ** attempt)
                        continue
                    return []
                
                i = 0
                while i < len(data):
                    item = data[i]
                    
                    if item is None or item == b'LIST Completed.' or item == b'':
                        i += 1
                        continue
                    
                    if isinstance(item, tuple) and len(item) >= 2:
                        raw_name = item[1].decode('utf-8', errors='replace').strip()
                        decoded_name = decode_imap_utf7(raw_name)
                        header = item[0].decode('utf-8', errors='replace') if isinstance(item[0], bytes) else str(item[0])
                        if '\\Noselect' not in header:
                            folders.append((raw_name, decoded_name))
                        i += 1
                        continue
                    
                    if isinstance(item, bytes):
                        try:
                            line = item.decode('utf-8', errors='replace')
                        except:
                            i += 1
                            continue
                        
                        if '\\Noselect' in line:
                            i += 1
                            continue
                        
                        match = re.search(r'\)\s+"([^"]+)"\s+"([^"]+)"$', line)
                        if match:
                            raw_name = match.group(2)
                            decoded_name = decode_imap_utf7(raw_name)
                            folders.append((raw_name, decoded_name))
                            i += 1
                            continue
                        
                        match = re.search(r'\)\s+"([^"]+)"\s+(\S+)$', line)
                        if match:
                            raw_name = match.group(2).strip('"')
                            decoded_name = decode_imap_utf7(raw_name)
                            folders.append((raw_name, decoded_name))
                    
                    i += 1
                
                return folders
                
            except Exception as e:
                logger.warning(f"LIST error (attempt {attempt}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_BACKOFF ** attempt)
        
        return folders


# ======================= MATCHING =======================

def is_sent_like_folder(name: str) -> bool:
    if not name:
        return False
    human = decode_imap_utf7(name).lower()
    return any(x in human for x in ["sent", "sent items", "отправлен", "исходящ", "outbox", "отправленные"])


def match_by_rules(hdr_bytes: bytes, folder_name: str) -> bool:
    """
    Фильтрация писем в зависимости от FILTER_MODE:
    
    - "ALL": Все письма (только фильтр по датам)
    - "OWNER": Письма владельца ящика (EMAIL_LOGIN + OWNER_ALIASES)
    - "PAIR": Переписка между PAIR_A и PAIR_B
    """
    try:
        msg = email.message_from_bytes(hdr_bytes)
    except:
        return False
    
    # ============ ФИЛЬТР ПО ДАТЕ (работает всегда) ============
    try:
        dh = msg.get("Date")
        dt = parsedate_to_datetime(dh) if dh else None
        if dt and dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    except:
        dt = None

    if SINCE_DT and dt and dt < SINCE_DT:
        return False
    if UNTIL_DT and dt and dt >= UNTIL_DT:
        return False
    
    # ============ РЕЖИМ "ALL" — сохраняем всё ============
    if FILTER_MODE == "ALL":
        return True
    
    # ============ Извлекаем адреса для других режимов ============
    text = hdr_bytes.decode("utf-8", "ignore").lower()
    
    from_like = _addr_set_from_headers(
        msg.get("From", ""), 
        msg.get("Sender", ""),
        msg.get("Resent-From", ""),
    )
    to_like = _addr_set_from_headers(
        msg.get("To", ""), 
        msg.get("Cc", ""), 
        msg.get("Bcc", ""),
        msg.get("Delivered-To", ""), 
        msg.get("X-Original-To", ""),
        msg.get("Resent-To", ""),
    )
    participants = from_like | to_like
    
    # ============ РЕЖИМ "PAIR" — переписка A ↔ B ============
    if FILTER_MODE == "PAIR":
        if not PAIR_A or not PAIR_B:
            logger.warning("PAIR mode requires PAIR_A and PAIR_B to be set!")
            return True  # Fallback: сохраняем всё
        
        equiv_A = {_canon(PAIR_A)} | {_canon(x) for x in (ALIASES_A or []) if x}
        equiv_B = {_canon(PAIR_B)} | {_canon(x) for x in (ALIASES_B or []) if x}
        
        has_A = any(a in participants for a in equiv_A)
        has_B = any(b in participants for b in equiv_B)
        
        # Прямое совпадение: оба участника в заголовках
        if has_A and has_B:
            return True
        
        # Fallback для Sent: отправитель один из пары, получатель ищем в тексте
        if is_sent_like_folder(folder_name):
            if any(x in from_like for x in equiv_A) and any(y in text for y in equiv_B):
                return True
            if any(x in from_like for x in equiv_B) and any(y in text for y in equiv_A):
                return True
        
        return False
    
    # ============ РЕЖИМ "OWNER" — письма владельца ============
    if FILTER_MODE == "OWNER":
        owner_equiv = {_canon(EMAIL_LOGIN)} if EMAIL_LOGIN else set()
        if OWNER_ALIASES:
            owner_equiv |= {_canon(x) for x in OWNER_ALIASES if x}
        
        if not owner_equiv:
            logger.warning("OWNER mode requires EMAIL_LOGIN to be set!")
            return True  # Fallback: сохраняем всё
        
        # Владелец в участниках
        if any(a in participants for a in owner_equiv):
            return True
        
        # В Sent: владелец как отправитель
        if is_sent_like_folder(folder_name) and any(a in from_like for a in owner_equiv):
            return True
        
        # Fallback: адрес владельца где-то в заголовках
        if any(a in text for a in owner_equiv):
            return True
        
        return False
    
    # Неизвестный режим — сохраняем всё
    logger.warning(f"Unknown FILTER_MODE: {FILTER_MODE}, saving all messages")
    return True


# ======================= MBOX =======================

def append_raw_to_mbox(path: str, raw: bytes, owner: str) -> bool:
    try:
        msg = email.message_from_bytes(raw)
        d = msg.get("Date")
        dt = parsedate_to_datetime(d) if d else None
    except:
        dt = None
    
    when = (dt or datetime.now()).strftime("%a %b %d %H:%M:%S %Y")
    
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "ab") as f:
            f.write(f"From {owner} {when}\n".encode())
            body = raw.replace(b"\r\n", b"\n")
            for line in body.split(b"\n"):
                if line.startswith(b"From "):
                    f.write(b">")
                f.write(line + b"\n")
            f.write(b"\n")
        return True
    except Exception as e:
        logger.error(f"Failed to write mbox: {e}")
        return False


# ======================= DUMP =======================

def dump_folder_to_mbox(conn: IMAPConnection, folder_decoded: str, folder_raw: str,
                        out_mbox_path: str, progress_mgr: ProgressManager) -> int:
    logger.info(f"→ Processing folder: {folder_decoded}")
    
    if progress_mgr.is_folder_completed(folder_decoded):
        logger.info(f"  ✓ Already completed, skipping")
        return 0
    
    if progress_mgr.is_folder_skipped(folder_decoded):
        logger.info(f"  ⏭ Already skipped")
        return 0
    
    success, error = conn.select_folder(folder_decoded, folder_raw, READONLY_SELECT)
    
    if not success:
        if is_permanent_folder_error(error):
            reason = get_skip_reason(error)
            logger.info(f"  ⏭ Skipping: {reason}")
            progress_mgr.mark_folder_skipped(folder_decoded, reason)
        else:
            logger.warning(f"  ⚠️ Cannot select: {error}")
            progress_mgr.mark_folder_error(folder_decoded, error)
        return 0
    
    uids = conn.uid_search("ALL")
    if uids is None:
        logger.warning(f"  ⚠️ Search failed")
        progress_mgr.mark_folder_error(folder_decoded, "Search failed")
        return 0
    
    if not uids:
        logger.info(f"  {folder_decoded}: empty")
        progress_mgr.mark_folder_completed(folder_decoded)
        return 0
    
    total = len(uids)
    logger.info(f"  {folder_decoded}: {total} messages")
    
    if FILTER_MODE == "ALL" and not SINCE_DT and not UNTIL_DT:
        logger.info(f"  Mode: ALL (no filters) — saving all {total} messages")
        return _save_all_messages(conn, folder_decoded, uids, out_mbox_path, progress_mgr)
    
    folder_state = progress_mgr.get_folder_state(folder_decoded)
    start_batch = 0
    already_matched = set()
    already_saved = set()
    
    if folder_state and not folder_state.get('skipped'):
        start_batch = folder_state.get('last_batch_index', 0)
        already_matched = set(folder_state.get('matched_uids', []))
        already_saved = set(folder_state.get('saved_uids', []))
        if start_batch > 0:
            logger.info(f"  Resuming: batch {start_batch}, {len(already_matched)} matched, {len(already_saved)} saved")
    else:
        progress_mgr.mark_folder_started(folder_decoded, total)
    
    header_cmd = b"(BODY.PEEK[HEADER.FIELDS (From To Cc Bcc Date Delivered-To X-Original-To Sender Resent-From Resent-To)])"
    selected = list(already_matched)
    
    batch_num = 0
    for i in range(0, total, HEADERS_BATCH_UID):
        if batch_num < start_batch:
            batch_num += 1
            continue
        
        batch = uids[i:i + HEADERS_BATCH_UID]
        batch_to_process = [uid for uid in batch if uid.decode() not in already_matched]
        
        if not batch_to_process:
            batch_num += 1
            continue
        
        conn.keepalive()
        results, success = conn.uid_fetch(batch_to_process, header_cmd)
        
        if not success:
            logger.warning(f"  ⚠️ Fetch failed at batch {batch_num}")
            progress_mgr.mark_folder_error(folder_decoded, f"Fetch failed at batch {batch_num}")
            return len(selected)
        
        new_matched, processed = [], []
        for idx, (uid_bytes, hdr) in enumerate(results):
            uid_str = uid_bytes.decode() if uid_bytes else (batch_to_process[idx].decode() if idx < len(batch_to_process) else None)
            if uid_str:
                processed.append(uid_str)
                if match_by_rules(hdr, folder_decoded):
                    selected.append(uid_str)
                    new_matched.append(uid_str)
        
        progress_mgr.update_folder_batch(folder_decoded, batch_num, processed, new_matched)
        
        pct = min(i + HEADERS_BATCH_UID, total) * 100 // total
        logger.info(f"    Headers: {min(i + HEADERS_BATCH_UID, total)}/{total} ({pct}%), matched: {len(selected)}")
        
        batch_num += 1
        if batch_num % GC_INTERVAL == 0:
            force_cleanup()
        time.sleep(BATCH_PAUSE)
    
    if not selected:
        logger.info(f"  {folder_decoded}: no matches")
        progress_mgr.mark_folder_completed(folder_decoded)
        return 0
    
    to_save = [uid for uid in selected if uid not in already_saved]
    
    if not to_save:
        logger.info(f"  {folder_decoded}: all {len(selected)} already saved")
        progress_mgr.mark_folder_completed(folder_decoded)
        return len(selected)
    
    logger.info(f"  Saving {len(to_save)} messages...")
    
    added = len(already_saved)
    failed_count = 0
    
    for i in range(0, len(to_save), RFC822_BATCH_UID):
        batch = [uid.encode() if isinstance(uid, str) else uid for uid in to_save[i:i + RFC822_BATCH_UID]]
        
        conn.keepalive()
        results, success = conn.uid_fetch(batch, b"(RFC822)")
        
        if not success:
            logger.warning(f"  ⚠️ RFC822 fetch failed, saved {added}")
            return added
        
        saved_batch, failed_batch = [], []
        for idx, (uid_bytes, raw) in enumerate(results):
            uid_str = uid_bytes.decode() if uid_bytes else (batch[idx].decode() if idx < len(batch) else f"unknown_{i+idx}")
            
            if append_raw_to_mbox(out_mbox_path, raw, EMAIL_LOGIN):
                added += 1
                saved_batch.append(uid_str)
            else:
                failed_count += 1
                failed_batch.append(uid_str)
        
        progress_mgr.update_folder_saved(folder_decoded, saved_batch, failed_batch)
        
        pct = min(i + RFC822_BATCH_UID, len(to_save)) * 100 // len(to_save)
        logger.info(f"    Saved: {min(i + RFC822_BATCH_UID, len(to_save))}/{len(to_save)} ({pct}%)")
        time.sleep(BATCH_PAUSE)
    
    if failed_count:
        logger.warning(f"  ⚠️ {failed_count} messages failed")
    
    logger.info(f"  ✓ {folder_decoded}: saved {added} messages")
    progress_mgr.mark_folder_completed(folder_decoded)
    return added


def _save_all_messages(conn: IMAPConnection, folder_decoded: str, uids: List[bytes],
                       out_mbox_path: str, progress_mgr: ProgressManager) -> int:
    """Оптимизированное сохранение всех писем без фильтрации заголовков"""
    
    total = len(uids)
    
    folder_state = progress_mgr.get_folder_state(folder_decoded)
    already_saved = set()
    
    if folder_state and not folder_state.get('skipped'):
        already_saved = set(folder_state.get('saved_uids', []))
        if already_saved:
            logger.info(f"  Resuming: {len(already_saved)} already saved")
    else:
        progress_mgr.mark_folder_started(folder_decoded, total)
    
    to_save = [uid for uid in uids if uid.decode() not in already_saved]
    
    if not to_save:
        logger.info(f"  {folder_decoded}: all {total} already saved")
        progress_mgr.mark_folder_completed(folder_decoded)
        return total
    
    added = len(already_saved)
    failed_count = 0
    
    for i in range(0, len(to_save), RFC822_BATCH_UID):
        batch = to_save[i:i + RFC822_BATCH_UID]
        
        conn.keepalive()
        results, success = conn.uid_fetch(batch, b"(RFC822)")
        
        if not success:
            logger.warning(f"  ⚠️ RFC822 fetch failed, saved {added}")
            return added
        
        saved_batch, failed_batch = [], []
        for idx, (uid_bytes, raw) in enumerate(results):
            uid_str = uid_bytes.decode() if uid_bytes else (batch[idx].decode() if idx < len(batch) else f"unknown_{i+idx}")
            
            if append_raw_to_mbox(out_mbox_path, raw, EMAIL_LOGIN):
                added += 1
                saved_batch.append(uid_str)
            else:
                failed_count += 1
                failed_batch.append(uid_str)
        
        progress_mgr.update_folder_saved(folder_decoded, saved_batch, failed_batch)
        
        pct = min(i + RFC822_BATCH_UID, len(to_save)) * 100 // len(to_save)
        logger.info(f"    Saved: {min(i + RFC822_BATCH_UID, len(to_save))}/{len(to_save)} ({pct}%)")
        
        if (i // RFC822_BATCH_UID) % GC_INTERVAL == 0:
            force_cleanup()
        time.sleep(BATCH_PAUSE)
    
    if failed_count:
        logger.warning(f"  ⚠️ {failed_count} messages failed")
    
    logger.info(f"  ✓ {folder_decoded}: saved {added} messages")
    progress_mgr.mark_folder_completed(folder_decoded)
    return added


# ======================= MAIN =======================

def print_config_summary():
    """Выводит сводку текущей конфигурации"""
    logger.info("")
    logger.info("📋 Configuration:")
    logger.info(f"   Email: {EMAIL_LOGIN}")
    logger.info(f"   Filter mode: {FILTER_MODE}")
    
    if FILTER_MODE == "ALL":
        logger.info("   → Will export ALL messages")
    elif FILTER_MODE == "OWNER":
        aliases = [EMAIL_LOGIN] + [a for a in OWNER_ALIASES if a]
        logger.info(f"   → Will export messages where owner is participant")
        logger.info(f"   → Owner addresses: {', '.join(aliases)}")
    elif FILTER_MODE == "PAIR":
        logger.info(f"   → Will export correspondence between:")
        logger.info(f"      A: {PAIR_A} {ALIASES_A if ALIASES_A else ''}")
        logger.info(f"      B: {PAIR_B} {ALIASES_B if ALIASES_B else ''}")
    
    if SINCE_DT or UNTIL_DT:
        date_range = []
        if SINCE_DT:
            date_range.append(f"from {SINCE_DATE}")
        if UNTIL_DT:
            date_range.append(f"until {UNTIL_DATE}")
        logger.info(f"   Date filter: {' '.join(date_range)}")
    
    if INCLUDE_FOLDERS:
        logger.info(f"   Include folders: {INCLUDE_FOLDERS}")
    if EXCLUDE_FOLDERS:
        logger.info(f"   Exclude folders: {EXCLUDE_FOLDERS}")
    
    logger.info("")


def main():
    global logger
    
    os.makedirs(OUT_ROOT, exist_ok=True)
    logger = setup_logging()
    
    logger.info("=" * 60)
    logger.info(f"Mail Export Started: {datetime.now()}")
    logger.info(f"Output: {OUT_ROOT}")
    logger.info("=" * 60)
    
    print_config_summary()
    
    progress_mgr = ProgressManager(OUT_ROOT, EMAIL_LOGIN)
    progress_mgr.load()
    
    try:
        token = token_exchange(EMAIL_LOGIN, MAIL_SCOPE)
    except Exception as e:
        logger.error(f"Failed to get token: {e}")
        return
    
    conn = IMAPConnection(EMAIL_LOGIN, token)
    
    if not conn.connect():
        logger.error("Failed to connect to IMAP")
        return
    
    logger.info("→ Getting folder list...")
    folder_list = conn.list_folders()
    
    if not folder_list:
        logger.warning("No folders found, using defaults")
        for name in ["INBOX", "Sent"]:
            success, _ = conn.select_folder(name, name, READONLY_SELECT)
            if success:
                folder_list.append((name, name))
    
    logger.info(f"✓ Found {len(folder_list)} folders")
    
    filtered = []
    for raw, decoded in folder_list:
        if INCLUDE_FOLDERS and not any(inc.lower() in decoded.lower() for inc in INCLUDE_FOLDERS):
            continue
        if EXCLUDE_FOLDERS and any(exc.lower() in decoded.lower() for exc in EXCLUDE_FOLDERS):
            continue
        filtered.append((raw, decoded))
    
    logger.info(f"✓ {len(filtered)} folders to process")
    
    for raw, decoded in filtered:
        if progress_mgr.is_folder_completed(decoded):
            status = "✓"
        elif progress_mgr.is_folder_skipped(decoded):
            status = "⏭"
        else:
            status = "○"
        logger.info(f"  {status} {decoded}")
    
    total_saved = 0
    
    for idx, (raw, decoded) in enumerate(filtered, 1):
        logger.info(f"\n[{idx}/{len(filtered)}] Processing: {decoded}")
        
        safe_name = re.sub(r'[\\/:*?"<>|]+', "_", decoded)
        out_path = os.path.join(OUT_ROOT, safe_name, "folder.mbox")
        
        try:
            saved = dump_folder_to_mbox(conn, decoded, raw, out_path, progress_mgr)
            total_saved += saved
        except KeyboardInterrupt:
            logger.info("\n⚠️ Interrupted. Progress saved.")
            break
        except Exception as e:
            logger.error(f"Error: {e}")
            logger.debug(traceback.format_exc())
            progress_mgr.mark_folder_error(decoded, str(e))
        
        force_cleanup()
    
    conn.close()
    
    completed = len(progress_mgr.progress.completed_folders)
    skipped = len(progress_mgr.progress.skipped_folders)
    remaining = len(filtered) - completed - skipped
    
    logger.info("\n" + "=" * 60)
    logger.info(f"Export completed: {datetime.now()}")
    logger.info(f"Filter mode: {FILTER_MODE}")
    logger.info(f"Total saved: {total_saved}")
    logger.info(f"Folders: {completed} completed, {skipped} skipped, {remaining} remaining")
    logger.info(f"Output: {OUT_ROOT}")
    logger.info("=" * 60)
    
    if remaining == 0:
        progress_mgr.clear()
        logger.info("✓ All done!")
    else:
        logger.info("⚠️ Run again to continue.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ Interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Fatal: {e}")
        traceback.print_exc()
        sys.exit(1)
