#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Скрипт переносит файлы с Яндекс Диска сотрудника на Диск руководителя.

Для запуска необходимы библиотеки requests, urllib3 и aiohttp

pip install requests aiohttp urllib3


Что делает:

1. Проверяет статус сотрудника, при необходимости разблокирует
2. (опционально) Восстанавливает файлы из Корзины
3. Собирает все файлы в одну папку (или публикует по отдельности)
4. Расшаривает папку руководителю через персональный доступ
5. Сохраняет данные на Диск руководителя
6. Если сотрудник был заблокирован — блокирует обратно после завершения
7. Предварительная настройка
8. Перед запуском заполните переменные в начале скрипта (блок КОНФИГУРАЦИЯ) или передайте через флаги:

ПРЕДВАРИТЕЛЬНАЯ НАСТРОЙКА

Переменная	   Флаг	                Описание
CLIENT_ID	    --client-id	        ID сервисного OAuth-приложения
CLIENT_SECRET   --client-secret	    Секрет сервисного OAuth-приложения
DOMAINID	    --domain-id	        ID домена для SCIM API
SCIM_TOKEN	    --scim-token	    Токен для SCIM API
ORG_ID	        --org-id	        ID организации (для персонального доступа)

БЫСТРЫЙ СТАРТ

python transfer.py --employee-uid 123456 --manager-uid 789012

Этот вызов выполнит перенос с настройками по умолчанию:

- файлы консолидируются в одну папку
- shared-ресурсы, где сотрудник не владелец — пропускаются
- скрипт ждёт завершения загрузки на Диск руководителя

ОБЯЗАТЕЛЬНЫЕ ФЛАГИ

Флаг	            Описание
--employee-uid	    UID сотрудника (источник)
--manager-uid	    UID руководителя (получатель)

ОПЦИОНАЛЬНЫЕ ФЛАГИ

Режим ожидания загрузки
Флаг	    Описание
--wait	    Ждать завершения загрузки на Диск руководителя (по умолчанию)
--no-wait	Инициировать загрузку и сразу завершить скрипт
--timeout	Таймаут ожидания загрузки в секундах (по умолчанию 3600)

# Не ждать завершения
python transfer.py --employee-uid 123 --manager-uid 456 --no-wait

# Ждать, но не более 30 минут
python transfer.py --employee-uid 123 --manager-uid 456 --wait --timeout 1800

Режим без консолидации
Флаг	            Описание
--no-consolidate	Не перемещать все файлы в одну папку, а публиковать каждый ресурс отдельно

Зачем: снижает вероятность ошибки 423 Locked (ресурс занят другой операцией), поскольку исключает массовое перемещение.

python transfer.py --employee-uid 123 --manager-uid 456 --no-consolidate

Восстановление из Корзины

Флаг	            Описание
--restore-trash	    Восстановить файлы из Корзины перед переносом
--trash-date-from	Фильтр: восстанавливать удалённые после этой даты
--trash-date-to	    Фильтр: восстанавливать удалённые до этой даты
--trash-overwrite	Перезаписывать файлы, если они уже существуют на Диске
Формат дат: YYYY-MM-DD или YYYY-MM-DD HH:MM

# Восстановить всю Корзину
python transfer.py --employee-uid 123 --manager-uid 456 --restore-trash

# Восстановить файлы, удалённые в определённый период
python transfer.py --employee-uid 123 --manager-uid 456 --restore-trash \
    --trash-date-from "2025-01-15" --trash-date-to "2025-01-20"

# С перезаписью существующих
python transfer.py --employee-uid 123 --manager-uid 456 --restore-trash --trash-overwrite

Shared-ресурсы (не владелец)
Флаг	                Описание
--skip-not-owned	    Пропускать shared-ресурсы, где сотрудник не владелец (по умолчанию)
--include-not-owned	    Включить в перенос и shared-ресурсы, где сотрудник не владелец

# Перенести всё, включая чужие shared-папки
python transfer.py --employee-uid 123 --manager-uid 456 --include-not-owned

Обработка ошибки 423 Locked

Ошибка 423 возникает, когда над ресурсом уже выполняется другая операция (перемещение, копирование и т.д.).

Флаг	                Описание
--retry-423	            Включить длинные ретраи при 423
--retry-423-interval	Интервал между попытками в секундах (по умолчанию 900 = 15 мин)
--retry-423-max	        Максимальное количество попыток (по умолчанию 10)

# Включить ретраи с настройками по умолчанию (15 мин × 10 попыток)
python transfer.py --employee-uid 123 --manager-uid 456 --retry-423

# Свои интервалы: ждать 10 минут, до 5 попыток
python transfer.py --employee-uid 123 --manager-uid 456 \
    --retry-423 --retry-423-interval 600 --retry-423-max 5

ДОПОЛНИТЕЛЬНЫЕ
Флаг	            Описание
--employee-email	Email сотрудника (если не указан — определяется автоматически через SCIM)

СВОДНАЯ ТАБЛИЦА ЗНАЧЕНИЙ ПО УМОЛЧАНИЮ

Параметр	                Значение по умолчанию
Ожидание загрузки	        Да (--wait)
Таймаут загрузки	        3600 сек (1 час)
Консолидация в папку	    Да (без --no-consolidate)
Пропуск shared не-owner	    Да (--skip-not-owned)
Восстановление корзины	    Нет
Ретраи 423	                Нет
Интервал ретраев 423	    900 сек (15 мин)
Макс. попыток 423	        10

ПРИМЕРЫ ТИПОВЫХ СЦЕНАРИЕВ

Стандартный перенос
python transfer.py --employee-uid 123 --manager-uid 456

Полный перенос с восстановлением Корзины
python transfer.py --employee-uid 123 --manager-uid 456 \
    --restore-trash --include-not-owned

Безопасный перенос (защита от 423)
python transfer.py --employee-uid 123 --manager-uid 456 \
    --no-consolidate --retry-423

Быстрый запуск без ожидания
python transfer.py --employee-uid 123 --manager-uid 456 --no-wait

Полный набор параметров
python transfer.py \
    --employee-uid 123 \
    --manager-uid 456 \
    --client-id "my_client_id" \
    --client-secret "my_secret" \
    --domain-id "my_domain" \
    --scim-token "my_token" \
    --restore-trash \
    --trash-date-from "2025-06-01" \
    --trash-date-to "2025-06-15" \
    --no-consolidate \
    --retry-423 \
    --retry-423-interval 600 \
    --retry-423-max 5 \
    --wait \
    --timeout 7200

ЛОГИ

Логи сохраняются в папку transfer_logs/ (создаётся автоматически).

Имя файла: transfer_YYYYMMDD_HHMMSS.log

В консоль выводятся сообщения уровня INFO и выше
В файл пишется всё, включая DEBUG

КОДЫ ЗАВЕРШЕНИЯ
Код	            Значение
0	            Успешное завершение
1	            Ошибка

"""

import os
import sys
import time
import json
import logging
import threading
import asyncio
import aiohttp
import ssl
import csv
import argparse
from pathlib import Path
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Optional, Dict, Any, List, Tuple
from threading import RLock, Event
from urllib.parse import quote, urlparse, parse_qs, unquote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import warnings
from urllib3.exceptions import InsecureRequestWarning


# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# Сервисное приложение (права: cloud_api:disk.read, cloud_api:disk.write, cloud_api:disk.info)
CLIENT_ID = ''
CLIENT_SECRET = ''

# SCIM API (из конфига YandexADSCIM)
DOMAINID = ''
SCIM_TOKEN = ''

# ID организации (для персонального доступа)
ORG_ID = ''

VERIFY_SSL = False
MAX_CONCURRENT_OPERATIONS = 20
MAX_RPS = 40
TOKEN_REFRESH_MINUTES = 55
PAGE_LIMIT = 1000
BAN_CHECK_INTERVAL_SECONDS = 60
REQUEST_TIMEOUT = 30

WAIT_FOR_DOWNLOAD = True
DOWNLOAD_CHECK_INTERVAL = 10
DOWNLOAD_TIMEOUT = 3600

SKIP_NOT_OWNED_SHARED = True

# Ретраи при 423 DiskResourceLockedError (ресурс заблокирован другой операцией)
RETRY_423_ENABLED = False
RETRY_423_INTERVAL = 900       # 15 минут — время ожидания между попытками
RETRY_423_MAX_ATTEMPTS = 10

RESTORE_TRASH_ENABLED = False
TRASH_DATE_FROM = None
TRASH_DATE_TO = None
TRASH_OVERWRITE = False

NO_CONSOLIDATE = False

LOGS_FOLDER = 'transfer_logs'

# ============================================================================

if not VERIFY_SSL:
    warnings.filterwarnings('ignore', category=InsecureRequestWarning)


# ============================================================================
# ЛОГИРОВАНИЕ
# ============================================================================

def setup_logging(log_folder: str) -> logging.Logger:
    Path(log_folder).mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = Path(log_folder) / f'transfer_{timestamp}.log'

    log = logging.getLogger('DiskTransfer')
    log.setLevel(logging.DEBUG)
    log.handlers = []

    fh = logging.FileHandler(log_file, encoding='utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    log.addHandler(fh)

    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(logging.Formatter('%(asctime)s | %(levelname)s | %(message)s'))
    log.addHandler(ch)

    return log


logger: Optional[logging.Logger] = None


# ============================================================================
# HTTP SESSION + RATE LIMITER
# ============================================================================

def create_session(max_retries: int = 3) -> requests.Session:
    """
    Создание сессии с retry.
    423 НЕ входит в status_forcelist — обрабатывается отдельно с длинными интервалами.
    """
    retries = Retry(
        total=max_retries,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST", "PATCH"]
    )
    session = requests.Session()
    session.mount('https://', HTTPAdapter(max_retries=retries))
    return session


class RateLimiter:
    def __init__(self, max_rps: int):
        self.max_rps = max_rps
        self.tokens = float(max_rps)
        self.last_update = time.time()
        self.lock = RLock()

    def acquire(self):
        with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.max_rps, self.tokens + elapsed * self.max_rps)
            self.last_update = now
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.max_rps
                time.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1


class AsyncRateLimiter:
    def __init__(self, max_rps: int):
        self.max_rps = max_rps
        self.tokens = float(max_rps)
        self.last_update = time.time()
        self.lock = asyncio.Lock()

    async def acquire(self):
        async with self.lock:
            now = time.time()
            elapsed = now - self.last_update
            self.tokens = min(self.max_rps, self.tokens + elapsed * self.max_rps)
            self.last_update = now
            if self.tokens < 1:
                sleep_time = (1 - self.tokens) / self.max_rps
                await asyncio.sleep(sleep_time)
                self.tokens = 0
            else:
                self.tokens -= 1


rate_limiter: Optional[RateLimiter] = None


# ============================================================================
# USER DATA
# ============================================================================

@dataclass
class UserData:
    uid: str
    email: Optional[str] = None
    token: Optional[str] = None
    token_expires_at: Optional[float] = None
    was_blocked: bool = False

    @property
    def is_token_valid(self) -> bool:
        if self.token is None or self.token_expires_at is None:
            return False
        return time.time() < (self.token_expires_at - 60)

    def set_token(self, token: str, expires_in: int) -> None:
        self.token = token
        self.token_expires_at = time.time() + expires_in


# ============================================================================
# SCIM API
# ============================================================================

def scim_get_user_status(user_id: str) -> bool:
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
    session = create_session(max_retries=3)
    try:
        response = session.get(url, headers=headers, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            active = response.json().get('active', False)
            if logger:
                logger.info(f'scim_get_user_status | uid={user_id} | active={active}')
            return active
        else:
            if logger:
                logger.error(f'scim_get_user_status | uid={user_id} | {response.status_code} | {response.text}')
            return False
    except Exception as e:
        if logger:
            logger.error(f'scim_get_user_status | uid={user_id} | error: {e}')
        return False


def scim_enable_user(user_id: str) -> bool:
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
    body = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [{"op": "replace", "path": "active", "value": True}]
    }
    session = create_session(max_retries=3)
    try:
        response = session.patch(url, json=body, headers=headers, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
        success = response.status_code in (200, 204)
        if logger:
            logger.info(f'scim_enable_user | uid={user_id} | {response.status_code} | success={success}')
        return success
    except Exception as e:
        if logger:
            logger.error(f'scim_enable_user | uid={user_id} | error: {e}')
        return False


def scim_disable_user(user_id: str) -> bool:
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
    body = {
        "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
        "Operations": [{"op": "replace", "path": "active", "value": False}]
    }
    session = create_session(max_retries=3)
    try:
        response = session.patch(url, json=body, headers=headers, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
        success = response.status_code in (200, 204)
        if logger:
            logger.info(f'scim_disable_user | uid={user_id} | {response.status_code}')
        return success
    except Exception as e:
        if logger:
            logger.error(f'scim_disable_user | uid={user_id} | error: {e}')
        return False


def scim_get_user_email(user_id: str) -> Optional[str]:
    url = f'https://{DOMAINID}.scim-api.passport.yandex.net/v2/Users/{user_id}'
    headers = {'Authorization': f'Bearer {SCIM_TOKEN}'}
    session = create_session(max_retries=3)
    try:
        response = session.get(url, headers=headers, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            data = response.json()
            emails = data.get('emails', [])
            if emails:
                for email in emails:
                    if email.get('primary'):
                        return email.get('value')
                return emails[0].get('value')
            return data.get('userName')
        return None
    except Exception as e:
        if logger:
            logger.error(f'scim_get_user_email | uid={user_id} | error: {e}')
        return None


# ============================================================================
# TOKEN MANAGER
# ============================================================================

class TokenManager:
    def __init__(self):
        self._users: Dict[str, UserData] = {}
        self._lock = RLock()
        self._stop_event = Event()
        self._monitor_thread: Optional[threading.Thread] = None
        self._monitored_uid: Optional[str] = None

    def _fetch_token(self, uid: str) -> Tuple[str, int]:
        url = 'https://oauth.yandex.ru/token'
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        data = {
            'grant_type': 'urn:ietf:params:oauth:grant-type:token-exchange',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET,
            'subject_token': uid,
            'subject_token_type': 'urn:yandex:params:oauth:token-type:uid'
        }
        session = create_session(max_retries=5)
        response = session.post(url, data=data, headers=headers, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
        if response.status_code != 200:
            raise RuntimeError(f"Токен uid={uid}: {response.status_code} {response.text}")
        result = response.json()
        token = result.get('access_token')
        expires_in = result.get('expires_in', 3600)
        if not token:
            raise RuntimeError(f"Нет токена в ответе: {result}")
        if logger:
            logger.info(f'TokenManager | Токен uid={uid}, expires_in={expires_in}')
        return token, expires_in

    def _ensure_user_active(self, uid: str) -> bool:
        is_active = scim_get_user_status(uid)
        if not is_active:
            if logger:
                logger.warning(f'TokenManager | uid={uid} заблокирован, разблокируем...')
            success = scim_enable_user(uid)
            if success:
                time.sleep(2)
            return success
        return True

    def _get_user(self, uid: str) -> UserData:
        if uid not in self._users:
            self._users[uid] = UserData(uid=uid)
        return self._users[uid]

    def get_token(self, uid: str, force_refresh: bool = False) -> str:
        with self._lock:
            user = self._get_user(uid)
            if user.is_token_valid and not force_refresh:
                return user.token
            self._ensure_user_active(uid)
            token, expires_in = self._fetch_token(uid)
            user.set_token(token, expires_in)
            return token

    def handle_401(self, uid: str) -> str:
        if logger:
            logger.warning(f'TokenManager | 401 uid={uid}, проверяем...')
        with self._lock:
            is_active = scim_get_user_status(uid)
            if not is_active:
                scim_enable_user(uid)
                time.sleep(2)
            token, expires_in = self._fetch_token(uid)
            self._get_user(uid).set_token(token, expires_in)
            return token

    def start_ban_monitor(self, uid: str):
        self._monitored_uid = uid
        self._stop_event.clear()
        self._monitor_thread = threading.Thread(target=self._ban_monitor_loop, daemon=True)
        self._monitor_thread.start()
        if logger:
            logger.info(f'BanMonitor | Запущен для uid={uid}')

    def stop_ban_monitor(self):
        self._stop_event.set()
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5)
        if logger:
            logger.info('BanMonitor | Остановлен')

    def _ban_monitor_loop(self):
        while not self._stop_event.is_set():
            if self._monitored_uid:
                try:
                    is_active = scim_get_user_status(self._monitored_uid)
                    if not is_active:
                        if logger:
                            logger.warning(f'BanMonitor | uid={self._monitored_uid} заблокирован!')
                        scim_enable_user(self._monitored_uid)
                        with self._lock:
                            user = self._users.get(self._monitored_uid)
                            if user:
                                user.token = None
                                user.token_expires_at = None
                except Exception as e:
                    if logger:
                        logger.error(f'BanMonitor | Ошибка: {e}')
            self._stop_event.wait(timeout=BAN_CHECK_INTERVAL_SECONDS)

    def set_user_email(self, uid: str, email: str):
        with self._lock:
            self._get_user(uid).email = email

    def mark_was_blocked(self, uid: str, was_blocked: bool):
        with self._lock:
            self._get_user(uid).was_blocked = was_blocked

    def was_user_blocked(self, uid: str) -> bool:
        with self._lock:
            user = self._users.get(uid)
            return user.was_blocked if user else False


token_manager: Optional[TokenManager] = None


# ============================================================================
# DISK API  (с поддержкой длинных ретраев при 423 Locked)
# ============================================================================

def disk_request(
    uid: str,
    method: str,
    endpoint: str,
    params: Optional[Dict] = None,
    json_body: Optional[Dict] = None,
    max_retries_401: int = 3,
    retry_423: Optional[bool] = None,
    retry_423_interval: Optional[int] = None,
    retry_423_max: Optional[int] = None,
) -> Optional[Dict]:
    """
    Универсальный запрос к Disk API.

    Обработка ошибок:
      * 401 — проверка бана, перезапрос токена, до max_retries_401 попыток.
      * 423 DiskResourceLockedError — ресурс заблокирован другой операцией.
        Если retry_423=True, скрипт ждёт retry_423_interval секунд (по умолч. 15 мин)
        и повторяет, до retry_423_max раз. Если False — сразу возвращает None.
      * 429 — обрабатывается автоматически urllib3 Retry (короткие ретраи 1-2-4 сек).
      * 5xx — обрабатываются urllib3 Retry.
    """
    url = f'https://cloud-api.yandex.net/v1/disk{endpoint}'

    do_retry_423 = retry_423 if retry_423 is not None else RETRY_423_ENABLED
    interval_423 = retry_423_interval if retry_423_interval is not None else RETRY_423_INTERVAL
    max_423 = retry_423_max if retry_423_max is not None else RETRY_423_MAX_ATTEMPTS

    session = create_session(max_retries=3)
    retries_423_done = 0

    while True:
        for attempt in range(max_retries_401):
            try:
                rate_limiter.acquire()
                token = token_manager.get_token(uid)
                headers = {'Authorization': f'OAuth {token}'}

                m = method.upper()
                if m == 'GET':
                    response = session.get(url, headers=headers, params=params,
                                           verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
                elif m == 'POST':
                    response = session.post(url, headers=headers, params=params, json=json_body,
                                            verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
                elif m == 'PUT':
                    response = session.put(url, headers=headers, params=params, json=json_body,
                                           verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
                elif m == 'DELETE':
                    response = session.delete(url, headers=headers, params=params,
                                              verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
                else:
                    raise ValueError(f"Неподдерживаемый метод: {method}")

                # --- Успех ---
                if response.status_code in (200, 201, 202, 204):
                    if logger:
                        logger.debug(f'disk_request | {method} {endpoint} | {response.status_code}')
                    try:
                        return response.json()
                    except Exception:
                        return {'status': 'ok'}

                # --- 401 Unauthorized ---
                if response.status_code == 401:
                    if logger:
                        logger.warning(f'disk_request | 401 | attempt {attempt+1}/{max_retries_401}')
                    token_manager.handle_401(uid)
                    continue

                # --- 423 Locked (DiskResourceLockedError) ---
                if response.status_code == 423:
                    if do_retry_423 and retries_423_done < max_423:
                        retries_423_done += 1
                        if logger:
                            logger.warning(
                                f'disk_request | 423 Locked (ресурс занят другой операцией) | '
                                f'{method} {endpoint} | '
                                f'ожидание {interval_423}s '
                                f'({retries_423_done}/{max_423})'
                            )
                        time.sleep(interval_423)
                        # Пересоздаём сессию на случай если соединение устарело
                        session = create_session(max_retries=3)
                        break  # выходим из 401-цикла → повторяем во внешнем while
                    else:
                        if logger:
                            logger.error(
                                f'disk_request | 423 Locked | {method} {endpoint} | '
                                f'{"ретраи исчерпаны" if retries_423_done >= max_423 else "ретраи выключены"} | '
                                f'{response.text[:300]}'
                            )
                        return None

                # --- 404 ---
                if response.status_code == 404:
                    return None

                # --- 409 Conflict ---
                if response.status_code == 409:
                    if logger:
                        logger.warning(f'disk_request | 409 Conflict | {response.text[:200]}')
                    return {'status': 'conflict', 'error': response.text}

                # --- Прочие ошибки ---
                if logger:
                    logger.error(
                        f'disk_request | {method} {endpoint} | {response.status_code} | {response.text[:300]}'
                    )
                return None

            except requests.exceptions.Timeout:
                if logger:
                    logger.error(f'disk_request | {method} {endpoint} | TIMEOUT')
                return None
            except Exception as e:
                if logger:
                    logger.error(f'disk_request | {method} {endpoint} | error: {e}')
                return None
        else:
            return None
        continue


# ============================================================================
# DISK API — Вспомогательные функции
# ============================================================================

def disk_get_resources(uid: str, path: str = '/', limit: int = 1000, offset: int = 0,
                       fields: str = '') -> Optional[Dict]:
    params: Dict[str, Any] = {'path': path, 'limit': limit, 'offset': offset, 'sort': 'path'}
    if fields:
        params['fields'] = fields
    return disk_request(uid, 'GET', '/resources', params=params)


def disk_create_folder(uid: str, path: str) -> bool:
    result = disk_request(uid, 'GET', '/resources', params={'path': path})
    if result and result.get('status') != 'conflict':
        if logger:
            logger.info(f'disk_create_folder | уже существует: {path}')
        return True
    result = disk_request(uid, 'PUT', '/resources', params={'path': path})
    if result:
        if logger:
            logger.info(f'disk_create_folder | создана: {path}')
        return True
    return False


def disk_publish_resource(uid: str, path: str, manager_uid: str) -> Optional[Dict]:
    params = {'path': path, 'allow_address_access': 'true'}
    body = {
        "public_settings": {
            "accesses": [
                {"user_ids": [manager_uid], "rights": ["read"]}
            ]
        }
    }
    return disk_request(uid, 'PUT', '/resources/publish', params=params, json_body=body)


def disk_get_public_key(uid: str, path: str) -> Optional[str]:
    result = disk_request(uid, 'GET', '/resources',
                          params={'path': path, 'fields': 'public_key,public_url'})
    return result.get('public_key') if result else None


def disk_check_operation_status(uid: str, operation_url: str) -> Optional[str]:
    session = create_session(max_retries=3)
    try:
        rate_limiter.acquire()
        token = token_manager.get_token(uid)
        headers = {'Authorization': f'OAuth {token}'}
        response = session.get(operation_url, headers=headers, verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            return response.json().get('status', 'unknown')
        elif response.status_code == 401:
            token_manager.handle_401(uid)
        return None
    except Exception as e:
        if logger:
            logger.error(f'disk_check_operation_status | error: {e}')
        return None


def disk_save_public_resource_with_tracking(
    uid: str, public_key: str, name: str,
    wait_for_completion: bool = True,
    check_interval: int = DOWNLOAD_CHECK_INTERVAL,
    timeout: int = DOWNLOAD_TIMEOUT,
) -> Tuple[bool, str]:
    url = 'https://cloud-api.yandex.net/v1/disk/public/resources/save-to-disk'
    params = {'public_key': public_key, 'name': name}
    session = create_session(max_retries=3)
    operation_url = None

    for attempt in range(3):
        try:
            rate_limiter.acquire()
            token = token_manager.get_token(uid)
            headers = {'Authorization': f'OAuth {token}'}
            response = session.post(url, headers=headers, params=params,
                                    verify=VERIFY_SSL, timeout=REQUEST_TIMEOUT)

            if response.status_code == 201:
                if logger:
                    logger.info(f'save_public | Синхронно: {name}')
                return True, "Сохранено синхронно"
            elif response.status_code == 202:
                try:
                    operation_url = response.json().get('href')
                except Exception:
                    pass
                if logger:
                    logger.info(f'save_public | Асинхронная операция: {name}')
                break
            elif response.status_code == 401:
                token_manager.handle_401(uid)
                continue
            elif response.status_code == 423:
                # 423 при save-to-disk — ждём и повторяем
                if RETRY_423_ENABLED:
                    if logger:
                        logger.warning(f'save_public | 423 Locked, ожидание {RETRY_423_INTERVAL}s...')
                    time.sleep(RETRY_423_INTERVAL)
                    continue
                else:
                    return False, f"423 Locked: {response.text[:200]}"
            else:
                if logger:
                    logger.error(f'save_public | {response.status_code} | {response.text[:200]}')
                return False, f"Ошибка API: {response.status_code}"
        except Exception as e:
            if attempt == 2:
                return False, str(e)

    if not wait_for_completion:
        return True, "Загрузка инициирована (режим --no-wait)"

    if not operation_url:
        return True, "Операция запущена (URL статуса недоступен)"

    if logger:
        logger.info(f'Отслеживание загрузки (timeout={timeout}s)...')

    start_time = time.time()
    last_log_time = start_time
    while True:
        elapsed = time.time() - start_time
        if elapsed > timeout:
            return False, f"Таймаут ожидания ({timeout}s)"
        status = disk_check_operation_status(uid, operation_url)
        if status == 'success':
            if logger:
                logger.info(f'Загрузка завершена за {int(elapsed)}s')
            return True, f"Загрузка завершена за {int(elapsed)}s"
        elif status == 'failed':
            return False, "Операция завершилась с ошибкой"
        if time.time() - last_log_time >= 30:
            if logger:
                logger.info(f'Ожидание загрузки... ({int(elapsed)}s/{timeout}s)')
            last_log_time = time.time()
        time.sleep(check_interval)


# ============================================================================
# ФИЛЬТРАЦИЯ SHARED (не owner)
# ============================================================================

def filter_owned_resources(uid: str, resources: List[Dict], skip_not_owned: bool) -> List[Dict]:
    if not skip_not_owned:
        return resources

    filtered = []
    skipped = 0
    for r in resources:
        share = r.get('share')
        if share is not None and share.get('is_owned') is False:
            skipped += 1
            if logger:
                logger.info(f'  ⊘ Пропущен (shared, не owner): {r.get("path")}')
            continue
        filtered.append(r)

    if skipped and logger:
        logger.info(f'filter_owned | Пропущено {skipped} ресурсов (shared, не owner)')
    return filtered


# ============================================================================
# ВОССТАНОВЛЕНИЕ ИЗ КОРЗИНЫ
# ============================================================================

def parse_date(date_string: Optional[str]) -> Optional[datetime]:
    if not date_string:
        return None
    for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
        try:
            return datetime.strptime(date_string.strip(), fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Не удалось распарсить дату: '{date_string}'")


def parse_api_date(date_string: str) -> Optional[datetime]:
    if not date_string:
        return None
    try:
        return datetime.fromisoformat(date_string.replace('Z', '+00:00'))
    except Exception:
        return None


def trash_get_all_items(uid: str, page_size: int = 1000) -> List[Dict]:
    all_items: List[Dict] = []
    offset = 0
    total = None
    while True:
        result = disk_request(uid, 'GET', '/trash/resources',
                              params={'path': '/', 'limit': page_size, 'offset': offset})
        if not result:
            break
        embedded = result.get('_embedded', {})
        items = embedded.get('items', [])
        if total is None:
            total = embedded.get('total', 0)
            if logger:
                logger.info(f'trash | Всего в корзине: {total}')
        if not items:
            break
        all_items.extend(items)
        if logger:
            logger.info(f'trash | Получено: {len(all_items)}/{total}')
        if len(all_items) >= total or len(items) < page_size:
            break
        offset += page_size
    return all_items


def trash_filter_by_date(items: List[Dict], start_date: Optional[datetime],
                         end_date: Optional[datetime]) -> List[Dict]:
    if start_date is None and end_date is None:
        return items
    filtered = []
    for item in items:
        deleted_date = parse_api_date(item.get('deleted', ''))
        if deleted_date is None:
            continue
        if start_date and deleted_date < start_date:
            continue
        if end_date and deleted_date > end_date:
            continue
        filtered.append(item)
    return filtered


def trash_restore_item(uid: str, trash_path: str, overwrite: bool = False) -> Dict:
    params: Dict[str, Any] = {'path': trash_path}
    if overwrite:
        params['overwrite'] = 'true'
    result = disk_request(uid, 'PUT', '/trash/resources/restore', params=params)
    if result is None:
        return {'success': False, 'status': 'failed', 'error': 'Request failed'}
    if result.get('status') == 'conflict':
        return {'success': False, 'status': 'skipped', 'error': 'Already exists'}
    return {'success': True, 'status': 'success', 'error': None}


def restore_trash(uid: str, date_from: Optional[str] = None, date_to: Optional[str] = None,
                  overwrite: bool = False) -> Tuple[int, int, int]:
    if logger:
        logger.info('=' * 60)
        logger.info('ВОССТАНОВЛЕНИЕ ИЗ КОРЗИНЫ')
        logger.info('=' * 60)

    items = trash_get_all_items(uid)
    if not items:
        if logger:
            logger.info('Корзина пуста')
        return 0, 0, 0

    start_date = parse_date(date_from)
    end_date = parse_date(date_to)

    if start_date or end_date:
        items = trash_filter_by_date(items, start_date, end_date)
        if logger:
            logger.info(f'После фильтра по дате: {len(items)} элементов')

    if not items:
        if logger:
            logger.info('Нет элементов для восстановления')
        return 0, 0, 0

    ok_count, skip_count, fail_count = 0, 0, 0
    for idx, item in enumerate(items):
        trash_path = item.get('path', '')
        name = item.get('name', 'N/A')
        if logger:
            logger.info(f'  [{idx+1}/{len(items)}] {name}')

        res = trash_restore_item(uid, trash_path, overwrite=overwrite)
        if res['status'] == 'success':
            ok_count += 1
            if logger:
                logger.info(f'    ✓ Восстановлено')
        elif res['status'] == 'skipped':
            skip_count += 1
            if logger:
                logger.warning(f'    ⚠ Уже существует')
        else:
            fail_count += 1
            if logger:
                logger.error(f'    ✗ {res.get("error")}')

    if logger:
        logger.info(f'Корзина: ok={ok_count}, skip={skip_count}, fail={fail_count}')
    return ok_count, skip_count, fail_count


# ============================================================================
# ASYNC MOVE  (с обработкой 423)
# ============================================================================

async def async_move_resource(
    session: aiohttp.ClientSession, rl: AsyncRateLimiter,
    uid: str, source: str, destination: str, semaphore: asyncio.Semaphore,
) -> Tuple[bool, str, Optional[str]]:
    async with semaphore:
        url = 'https://cloud-api.yandex.net/v1/disk/resources/move'
        params = {'from': source, 'path': destination, 'overwrite': 'false'}

        retries_423_left = RETRY_423_MAX_ATTEMPTS if RETRY_423_ENABLED else 0

        for attempt in range(3 + retries_423_left):
            try:
                await rl.acquire()
                token = token_manager.get_token(uid)
                headers = {'Authorization': f'OAuth {token}'}

                async with session.post(url, headers=headers, params=params,
                                        ssl=False if not VERIFY_SSL else None,
                                        timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                    status = resp.status

                    if status in (201, 202):
                        if status == 202:
                            data = await resp.json()
                            op_url = data.get('href')
                            if op_url:
                                ok = await _async_wait_op(session, rl, uid, op_url)
                                return (ok, source, None if ok else "Async op failed")
                        return (True, source, None)

                    elif status == 401:
                        token_manager.handle_401(uid)
                        continue

                    elif status == 423:
                        if RETRY_423_ENABLED and retries_423_left > 0:
                            retries_423_left -= 1
                            text = await resp.text()
                            if logger:
                                logger.warning(
                                    f'async_move | 423 Locked | {source} | '
                                    f'ожидание {RETRY_423_INTERVAL}s '
                                    f'(осталось попыток: {retries_423_left})'
                                )
                            await asyncio.sleep(RETRY_423_INTERVAL)
                            continue
                        else:
                            text = await resp.text()
                            return (False, source, f"423 Locked: {text[:200]}")

                    elif status == 409:
                        return (False, source, "Ресурс уже существует")

                    else:
                        text = await resp.text()
                        return (False, source, f"Ошибка {status}: {text[:200]}")

            except asyncio.TimeoutError:
                if attempt >= 2 and retries_423_left <= 0:
                    return (False, source, "Таймаут")
                await asyncio.sleep(1)
            except Exception as e:
                if attempt >= 2 and retries_423_left <= 0:
                    return (False, source, str(e))
                await asyncio.sleep(1)

        return (False, source, "Превышено кол-во попыток")


async def _async_wait_op(session, rl, uid, op_url, max_attempts=60):
    for _ in range(max_attempts):
        try:
            await rl.acquire()
            token = token_manager.get_token(uid)
            headers = {'Authorization': f'OAuth {token}'}
            async with session.get(op_url, headers=headers,
                                   ssl=False if not VERIFY_SSL else None,
                                   timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as resp:
                if resp.status == 200:
                    st = (await resp.json()).get('status')
                    if st == 'success':
                        return True
                    elif st == 'failed':
                        return False
                elif resp.status == 401:
                    token_manager.handle_401(uid)
        except Exception:
            pass
        await asyncio.sleep(1)
    return False


async def move_all_resources_async(uid: str, resources: List[Dict],
                                   destination_folder: str) -> Tuple[int, int, List[Dict]]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_OPERATIONS)
    rl = AsyncRateLimiter(MAX_RPS)

    ssl_ctx = ssl.create_default_context()
    if not VERIFY_SSL:
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl.CERT_NONE

    connector = aiohttp.TCPConnector(limit=MAX_CONCURRENT_OPERATIONS, ssl=ssl_ctx)
    timeout = aiohttp.ClientTimeout(total=600)

    ok_count, err_count = 0, 0
    failed: List[Dict] = []

    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = []
        for r in resources:
            src, name = r.get('path'), r.get('name')
            if src and name:
                dst = f'{destination_folder}/{name}'
                tasks.append((async_move_resource(session, rl, uid, src, dst, semaphore), r))

        if tasks:
            results = await asyncio.gather(*[t[0] for t in tasks], return_exceptions=True)
            for (_, resource), result in zip(tasks, results):
                if isinstance(result, Exception):
                    err_count += 1
                    failed.append({'resource': resource, 'error': str(result)})
                else:
                    success, source, error = result
                    if success:
                        ok_count += 1
                        if logger:
                            logger.info(f'  ✓ Перемещён: {source}')
                    else:
                        err_count += 1
                        failed.append({'resource': resource, 'error': error})
                        if logger:
                            logger.warning(f'  ✗ {source} | {error}')
    return ok_count, err_count, failed


# ============================================================================
# ПОЛУЧЕНИЕ РЕСУРСОВ
# ============================================================================

def get_all_root_resources(uid: str, exclude_folder: Optional[str] = None,
                           include_share: bool = False) -> List[Dict]:
    all_resources: List[Dict] = []
    offset = 0
    fields = ''
    if include_share:
        fields = ('path,name,type,size,modified,share,'
                  '_embedded.items.path,_embedded.items.name,'
                  '_embedded.items.type,_embedded.items.size,'
                  '_embedded.items.share')

    while True:
        result = disk_get_resources(uid, path='disk:/', limit=PAGE_LIMIT, offset=offset, fields=fields)
        if not result:
            break
        items = result.get('_embedded', {}).get('items', [])
        if not items:
            break
        filtered = [i for i in items if exclude_folder is None or i.get('path') != exclude_folder]
        all_resources.extend(filtered)
        if len(items) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT
        time.sleep(0.1)

    if logger:
        logger.info(f'get_all_root_resources | Найдено: {len(all_resources)}')
    return all_resources


# ============================================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================================

def transfer_disk_to_manager(
    employee_uid: str,
    manager_uid: str,
    employee_email: Optional[str] = None,
    wait_for_download: bool = True,
    download_timeout: int = DOWNLOAD_TIMEOUT,
    skip_not_owned: bool = SKIP_NOT_OWNED_SHARED,
    no_consolidate: bool = NO_CONSOLIDATE,
    restore_trash_enabled: bool = RESTORE_TRASH_ENABLED,
    trash_date_from: Optional[str] = None,
    trash_date_to: Optional[str] = None,
    trash_overwrite: bool = TRASH_OVERWRITE,
) -> bool:
    global token_manager, rate_limiter, logger

    logger = setup_logging(LOGS_FOLDER)
    token_manager = TokenManager()
    rate_limiter = RateLimiter(MAX_RPS)

    logger.info('=' * 80)
    logger.info('НАЧАЛО ПЕРЕНОСА ДАННЫХ')
    logger.info(f'  Сотрудник:               {employee_uid}')
    logger.info(f'  Руководитель:            {manager_uid}')
    logger.info(f'  DOMAINID:                {DOMAINID}')
    logger.info(f'  Ожидание загрузки:       {"Да" if wait_for_download else "Нет"}')
    logger.info(f'  Пропуск shared !owner:   {"Да" if skip_not_owned else "Нет"}')
    logger.info(f'  Без консолидации:        {"Да" if no_consolidate else "Нет"}')
    logger.info(f'  Восстановление корзины:  {"Да" if restore_trash_enabled else "Нет"}')
    logger.info(f'  Ретраи 423 Locked:       {"Да (" + str(RETRY_423_INTERVAL) + "s × " + str(RETRY_423_MAX_ATTEMPTS) + ")" if RETRY_423_ENABLED else "Нет"}')
    logger.info('=' * 80)

    try:
        # ====== ШАГ 1: Проверка / разблокировка ======
        logger.info('ШАГ 1: Проверка статуса сотрудника...')
        is_active = scim_get_user_status(employee_uid)
        logger.info(f'Статус: {"активен" if is_active else "заблокирован"}')
        was_blocked = not is_active
        token_manager.mark_was_blocked(employee_uid, was_blocked)

        if was_blocked:
            logger.info(f'Разблокировка {employee_uid}...')
            if not scim_enable_user(employee_uid):
                logger.error('Не удалось разблокировать')
                return False
            time.sleep(2)

        if not employee_email:
            employee_email = scim_get_user_email(employee_uid)
        if employee_email:
            token_manager.set_user_email(employee_uid, employee_email)
            logger.info(f'Email: {employee_email}')
        else:
            employee_email = employee_uid

        logger.info('Получение токена сотрудника...')
        try:
            token_manager.get_token(employee_uid)
            logger.info('Токен сотрудника ОК')
        except Exception as e:
            logger.error(f'Токен сотрудника: {e}')
            return False

        logger.info('Проверка руководителя...')
        if not scim_get_user_status(manager_uid):
            logger.error(f'Руководитель {manager_uid} заблокирован!')
            return False
        try:
            token_manager.get_token(manager_uid)
            logger.info('Токен руководителя ОК')
        except Exception as e:
            logger.error(f'Токен руководителя: {e}')
            return False

        token_manager.start_ban_monitor(employee_uid)

        # ====== ШАГ 1.5: Восстановление корзины ======
        if restore_trash_enabled:
            logger.info('ШАГ 1.5: Восстановление из Корзины...')
            ok, skip, fail = restore_trash(
                uid=employee_uid, date_from=trash_date_from,
                date_to=trash_date_to, overwrite=trash_overwrite,
            )
            logger.info(f'Корзина: ok={ok}, skip={skip}, fail={fail}')

        # ====== ШАГ 2: Подготовка ======
        date_str = datetime.now().strftime('%Y-%m-%d')
        safe_email = employee_email.replace('@', '_at_').replace('.', '_')

        logger.info('Получение ресурсов...')
        resources = get_all_root_resources(employee_uid, include_share=skip_not_owned)

        if resources:
            resources = filter_owned_resources(employee_uid, resources, skip_not_owned)
            folders = [r for r in resources if r.get('type') == 'dir']
            files = [r for r in resources if r.get('type') == 'file']
            logger.info(f'К переносу: {len(folders)} папок, {len(files)} файлов')
            for r in resources[:20]:
                logger.info(f'  - {r.get("type")}: {r.get("name")}')
            if len(resources) > 20:
                logger.info(f'  ... и ещё {len(resources) - 20}')
        else:
            logger.warning('Корень Диска пуст')

        # ====== РЕЖИМ A: С консолидацией ======
        if not no_consolidate:
            transfer_folder = f'disk:/data_transfer_{safe_email}_{date_str}'
            resources = [r for r in resources if r.get('path') != transfer_folder]

            logger.info(f'ШАГ 2: Создание {transfer_folder}...')
            if not disk_create_folder(employee_uid, transfer_folder):
                logger.error('Не удалось создать папку')
                return False

            if resources:
                logger.info('Перемещение ресурсов...')
                ok, errs, failed = asyncio.run(
                    move_all_resources_async(employee_uid, resources, transfer_folder)
                )
                logger.info(f'Перемещение: ok={ok}, errors={errs}')
                if failed:
                    for item in failed[:10]:
                        logger.warning(f'  ✗ {item["resource"].get("path")}: {item["error"]}')

            logger.info(f'ШАГ 3: Расшаривание {transfer_folder}...')
            if not disk_publish_resource(employee_uid, transfer_folder, manager_uid):
                logger.error('Не удалось расшарить')
                return False

            time.sleep(2)
            public_key = disk_get_public_key(employee_uid, transfer_folder)
            if not public_key:
                logger.error('Не удалось получить public_key')
                return False

            save_name = f'transfer_from_{safe_email}_{date_str}'
            logger.info(f'ШАГ 4: Сохранение → {save_name}...')
            success, message = disk_save_public_resource_with_tracking(
                uid=manager_uid, public_key=public_key, name=save_name,
                wait_for_completion=wait_for_download, timeout=download_timeout,
            )
            if not success:
                logger.error(f'Ошибка: {message}')
                return False
            logger.info(f'Результат: {message}')

        # ====== РЕЖИМ B: Без консолидации ======
        else:
            if not resources:
                logger.warning('Нечего переносить')
            else:
                logger.info('ШАГ 2 (no-consolidate): Публикация каждого ресурса...')
                published: List[Tuple[str, str]] = []

                for idx, r in enumerate(resources):
                    rpath, rname = r.get('path', ''), r.get('name', '')
                    logger.info(f'  [{idx+1}/{len(resources)}] {rname}')

                    if not disk_publish_resource(employee_uid, rpath, manager_uid):
                        logger.warning(f'    ✗ Не опубликовано: {rpath}')
                        continue

                    time.sleep(0.5)
                    pk = disk_get_public_key(employee_uid, rpath)
                    if pk:
                        published.append((pk, rname))
                        logger.info(f'    ✓ OK')
                    else:
                        logger.warning(f'    ✗ Нет public_key: {rpath}')

                logger.info(f'Опубликовано: {len(published)}/{len(resources)}')

                logger.info('ШАГ 3 (no-consolidate): Сохранение на Диск руководителя...')
                saved = 0
                for idx, (pk, rname) in enumerate(published):
                    save_name_r = f'{employee_uid}_{rname}'
                    is_last = (idx == len(published) - 1)
                    wait_this = wait_for_download and is_last

                    ok, msg = disk_save_public_resource_with_tracking(
                        uid=manager_uid, public_key=pk, name=save_name_r,
                        wait_for_completion=wait_this, timeout=download_timeout,
                    )
                    if ok:
                        saved += 1
                        logger.info(f'  [{idx+1}/{len(published)}] ✓ {msg}')
                    else:
                        logger.warning(f'  [{idx+1}/{len(published)}] ✗ {msg}')

                logger.info(f'Сохранено: {saved}/{len(published)}')

        logger.info('=' * 80)
        logger.info('ПЕРЕНОС ЗАВЕРШЁН')
        logger.info('=' * 80)
        return True

    except Exception as e:
        if logger:
            logger.exception(f'Критическая ошибка: {e}')
        return False

    finally:
        if token_manager:
            token_manager.stop_ban_monitor()
            if token_manager.was_user_blocked(employee_uid):
                if logger:
                    logger.info(f'Блокируем {employee_uid} обратно...')
                scim_disable_user(employee_uid)


# ============================================================================
# CLI
# ============================================================================

def parse_args():
    p = argparse.ArgumentParser(
        description='Перенос файлов с Диска сотрудника на Диск руководителя',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры:
  python %(prog)s --employee-uid 123 --manager-uid 456
  python %(prog)s --employee-uid 123 --manager-uid 456 --no-consolidate
  python %(prog)s --employee-uid 123 --manager-uid 456 --retry-423
  python %(prog)s --employee-uid 123 --manager-uid 456 --retry-423 --retry-423-interval 600
  python %(prog)s --employee-uid 123 --manager-uid 456 --restore-trash
  python %(prog)s --employee-uid 123 --manager-uid 456 --restore-trash \\
      --trash-date-from "2026-01-29 13:00" --trash-date-to "2026-01-29 18:00"
        """,
    )
    p.add_argument('--employee-uid', required=True, help='UID сотрудника')
    p.add_argument('--manager-uid', required=True, help='UID руководителя')
    p.add_argument('--employee-email', help='Email сотрудника (авто)')
    p.add_argument('--client-id', help='Client ID')
    p.add_argument('--client-secret', help='Client Secret')
    p.add_argument('--domain-id', help='Domain ID (SCIM)')
    p.add_argument('--scim-token', help='SCIM Token')
    p.add_argument('--org-id', help='ID организации')

    wg = p.add_mutually_exclusive_group()
    wg.add_argument('--wait', action='store_true', dest='wait_for_download', default=None,
                    help='Ждать завершения загрузки (по умолч.)')
    wg.add_argument('--no-wait', action='store_false', dest='wait_for_download',
                    help='Не ждать загрузки')
    p.add_argument('--timeout', type=int, default=None,
                   help=f'Таймаут загрузки, с (по умолч. {DOWNLOAD_TIMEOUT})')

    p.add_argument('--restore-trash', action='store_true', default=False,
                   help='Восстановить файлы из Корзины')
    p.add_argument('--trash-date-from', default=None,
                   help='Корзина: от (YYYY-MM-DD HH:MM)')
    p.add_argument('--trash-date-to', default=None,
                   help='Корзина: до (YYYY-MM-DD HH:MM)')
    p.add_argument('--trash-overwrite', action='store_true', default=False,
                   help='Перезаписывать при восстановлении')

    p.add_argument('--no-consolidate', action='store_true', default=False,
                   help='Не перемещать в одну папку (защита от 423 Locked)')

    sg = p.add_mutually_exclusive_group()
    sg.add_argument('--skip-not-owned', action='store_true', dest='skip_not_owned', default=None,
                    help='Пропускать shared не-owner (по умолч.)')
    sg.add_argument('--include-not-owned', action='store_false', dest='skip_not_owned',
                    help='Переносить и shared не-owner')

    p.add_argument('--retry-423', action='store_true', default=False,
                   help='Длинные ретраи при 423 Locked (ресурс занят)')
    p.add_argument('--retry-423-interval', type=int, default=None,
                   help=f'Интервал ретраев 423, с (по умолч. {RETRY_423_INTERVAL})')
    p.add_argument('--retry-423-max', type=int, default=None,
                   help=f'Макс. попыток 423 (по умолч. {RETRY_423_MAX_ATTEMPTS})')

    return p.parse_args()


def main():
    global CLIENT_ID, CLIENT_SECRET, DOMAINID, SCIM_TOKEN, ORG_ID
    global RETRY_423_ENABLED, RETRY_423_INTERVAL, RETRY_423_MAX_ATTEMPTS

    args = parse_args()

    if args.client_id:
        CLIENT_ID = args.client_id
    if args.client_secret:
        CLIENT_SECRET = args.client_secret
    if args.domain_id:
        DOMAINID = args.domain_id
    if args.scim_token:
        SCIM_TOKEN = args.scim_token
    if args.org_id:
        ORG_ID = args.org_id

    if args.retry_423:
        RETRY_423_ENABLED = True
    if args.retry_423_interval is not None:
        RETRY_423_INTERVAL = args.retry_423_interval
    if args.retry_423_max is not None:
        RETRY_423_MAX_ATTEMPTS = args.retry_423_max

    wait_for_download = args.wait_for_download if args.wait_for_download is not None else WAIT_FOR_DOWNLOAD
    download_timeout = args.timeout if args.timeout else DOWNLOAD_TIMEOUT
    skip_not_owned = args.skip_not_owned if args.skip_not_owned is not None else SKIP_NOT_OWNED_SHARED

    missing = []
    if not CLIENT_ID:
        missing.append('CLIENT_ID')
    if not CLIENT_SECRET:
        missing.append('CLIENT_SECRET')
    if not DOMAINID:
        missing.append('DOMAINID')
    if not SCIM_TOKEN:
        missing.append('SCIM_TOKEN')
    if missing:
        print('ОШИБКА: Не указаны:', ', '.join(missing))
        sys.exit(1)

    print('=' * 60)
    print('КОНФИГУРАЦИЯ')
    print('=' * 60)
    print(f'  Employee UID:          {args.employee_uid}')
    print(f'  Manager UID:           {args.manager_uid}')
    print(f'  Domain ID:             {DOMAINID}')
    print(f'  Ожидание загрузки:     {"Да" if wait_for_download else "Нет"}')
    print(f'  Без консолидации:      {"Да" if args.no_consolidate else "Нет"}')
    print(f'  Пропуск shared !own:   {"Да" if skip_not_owned else "Нет"}')
    print(f'  Восстановл. корзины:   {"Да" if args.restore_trash else "Нет"}')
    print(f'  Ретраи 423 Locked:     {"Да (" + str(RETRY_423_INTERVAL) + "s × " + str(RETRY_423_MAX_ATTEMPTS) + ")" if RETRY_423_ENABLED else "Нет"}')
    print('=' * 60)
    print()

    success = transfer_disk_to_manager(
        employee_uid=args.employee_uid,
        manager_uid=args.manager_uid,
        employee_email=args.employee_email,
        wait_for_download=wait_for_download,
        download_timeout=download_timeout,
        skip_not_owned=skip_not_owned,
        no_consolidate=args.no_consolidate,
        restore_trash_enabled=args.restore_trash,
        trash_date_from=args.trash_date_from,
        trash_date_to=args.trash_date_to,
        trash_overwrite=args.trash_overwrite,
    )

    sys.exit(0 if success else 1)


if __name__ == '__main__':
    main()
