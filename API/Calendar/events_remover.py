"""
Скрипт для просмотра и удаления событий конкретного пользователя через CalDAV.

Установка зависимостей:
pip install requests

Перед запуском заполните константы:
CLIENT_ID     - Client ID сервисного приложения с правами calendar:all
CLIENT_SECRET - Client Secret того же приложения
USER_EMAIL    - Email пользователя вида username@domain.ru
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from datetime import datetime, timedelta
import re
import logging
import time
import threading
from collections import deque

# ─────────────────────────────── Логгирование ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ───────────────────────────────── Константы ───────────────────────────────────
CLIENT_ID     = 'ВАШ_CLIENT_ID'
CLIENT_SECRET = 'ВАШ_CLIENT_SECRET'
USER_EMAIL    = 'user@example.ru'

OAUTH_URL           = 'https://oauth.yandex.ru/token'
CALDAV_CALENDAR_URL = 'https://caldav.yandex.ru/calendars/{email}/events-default'

# ───────────────────────────── HTTP-сессия ─────────────────────────────────────
SESSION = requests.Session()
SESSION.mount(
    'https://',
    HTTPAdapter(
        max_retries=Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
    )
)

# ──────────────────────── Rate-limiter: 200 RPS (CalDAV) ───────────────────────
_rps_lock         = threading.Lock()
_rps_last_request = 0.0
_RPS_MIN_INTERVAL = 1.0 / 200

def _caldav_rate_limit() -> None:
    global _rps_last_request
    with _rps_lock:
        delta = _RPS_MIN_INTERVAL - (time.time() - _rps_last_request)
        if delta > 0:
            time.sleep(delta)
        _rps_last_request = time.time()

# ─────────────────────── Rate-limiter: 1000 токенов/час ────────────────────────
_token_lock       = threading.RLock()
_token_timestamps = deque(maxlen=1000)

def _token_rate_limit() -> None:
    with _token_lock:
        now      = time.time()
        hour_ago = now - 3600

        while _token_timestamps and _token_timestamps[0] < hour_ago:
            _token_timestamps.popleft()

        if len(_token_timestamps) >= 1000:
            wait = _token_timestamps[0] - hour_ago + 0.1
            logger.warning(f'Token rate limit — ожидание {wait:.1f} сек.')
            time.sleep(max(0.0, wait))
            return _token_rate_limit()

        _token_timestamps.append(now)

# ────────────────────────────── Токен доступа ──────────────────────────────────
_token_cache: dict = {}  # email → {'token': str, 'expiry': datetime}


def get_token(email: str) -> str:
    """
    Возвращает актуальный OAuth-токен пользователя через token-exchange по email.
    Результат кешируется до истечения TTL токена.
    """
    cached = _token_cache.get(email)
    if cached and cached['expiry'] > datetime.now():
        logger.debug(f'Токен из кеша для {email}')
        return cached['token']

    _token_rate_limit()

    response = SESSION.post(
        url     = OAUTH_URL,
        headers = {'Content-Type': 'application/x-www-form-urlencoded'},
        data    = {
            'grant_type':         'urn:ietf:params:oauth:grant-type:token-exchange',
            'client_id':          CLIENT_ID,
            'client_secret':      CLIENT_SECRET,
            'subject_token':      email,
            'subject_token_type': 'urn:yandex:params:oauth:token-type:email',
        }
    )

    if not response.ok:
        logger.error(
            f'Ошибка получения токена для {email}: '
            f'{response.status_code} — {response.text}'
        )
    response.raise_for_status()

    data   = response.json()
    token  = data['access_token']
    expiry = datetime.now() + timedelta(seconds=int(data['expires_in']) - 100)

    _token_cache[email] = {'token': token, 'expiry': expiry}
    logger.info(f'Токен получен для {email}, действует до {expiry:%H:%M:%S}')
    return token

# ─────────────────────────── CalDAV: базовые запросы ───────────────────────────
def _auth_header(token: str) -> dict:
    return {'Authorization': f'OAuth {token}'}


def _caldav_get(url: str, token: str) -> requests.Response:
    _caldav_rate_limit()
    r = SESSION.get(url, headers=_auth_header(token))
    r.raise_for_status()
    return r


def _caldav_delete(url: str, token: str) -> None:
    _caldav_rate_limit()
    r = SESSION.delete(url, headers=_auth_header(token))
    r.raise_for_status()

# ──────────────────────────── CalDAV: события ──────────────────────────────────
def _calendar_url(email: str) -> str:
    return CALDAV_CALENDAR_URL.format(email=email)


def _event_url(email: str, event_id: str) -> str:
    return f'{_calendar_url(email)}/{event_id}'


def list_event_ids(email: str, token: str) -> list[str]:
    """Возвращает список имён .ics-файлов из календаря пользователя."""
    response = _caldav_get(_calendar_url(email), token)
    logger.debug(f'Ответ CalDAV (первые 500 символов):\n{response.text[:500]}')
    return re.findall(r'([^\s/]+\.ics)', response.text)


def fetch_event_ical(email: str, event_id: str, token: str) -> str | None:
    """Загружает iCalendar-текст события. При ошибке возвращает None."""
    try:
        return _caldav_get(_event_url(email, event_id), token).text
    except requests.HTTPError as e:
        logger.warning(f'Не удалось загрузить {event_id}: {e}')
        return None


def delete_event(email: str, event_id: str, token: str) -> bool:
    """Удаляет событие. Возвращает True при успехе."""
    try:
        _caldav_delete(_event_url(email, event_id), token)
        logger.info(f'Удалено: {event_id}')
        return True
    except requests.HTTPError as e:
        logger.error(f'Ошибка удаления {event_id}: {e}')
        return False

# ──────────────────────────── Парсинг iCalendar ────────────────────────────────
def _unfold_ical(text: str) -> str:
    """Разворачивает перенесённые строки iCalendar (RFC 5545 §3.1)."""
    return re.sub(r'\r?\n[ \t]', '', text)


def parse_vevent(ical_text: str) -> dict:
    """
    Разбирает первый блок VEVENT в плоский словарь.
    Ключ — имя свойства (с параметрами, например DTSTART;TZID=Europe/Moscow).
    """
    result   = {}
    unfolded = _unfold_ical(ical_text)
    in_event = False

    for line in unfolded.splitlines():
        line = line.strip()
        if line == 'BEGIN:VEVENT':
            in_event = True
            continue
        if line == 'END:VEVENT':
            break
        if not in_event or ':' not in line:
            continue
        prop, _, value = line.partition(':')
        result[prop.strip()] = value.strip()

    return result


def _get_field(event: dict, *keys: str) -> str:
    """
    Ищет первое совпадение среди переданных ключей.
    Учитывает свойства с параметрами (DTSTART;TZID=Europe/Moscow).
    """
    for key in keys:
        for prop, val in event.items():
            if prop == key or prop.startswith(f'{key};'):
                return val
    return ''


def _parse_dt(value: str) -> datetime | None:
    """Парсит дату/время из iCalendar-значения (несколько форматов)."""
    raw = value.split(':')[-1] if ':' in value else value
    for fmt in ('%Y%m%dT%H%M%SZ', '%Y%m%dT%H%M%S', '%Y%m%d'):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue
    return None

# ──────────────────────── Фильтрация по диапазону дат ──────────────────────────
def is_in_range(event: dict, date_from: datetime, date_to: datetime) -> bool:
    """Возвращает True если событие пересекается с диапазоном [date_from, date_to]."""
    dtstart = _parse_dt(_get_field(event, 'DTSTART'))
    if dtstart is None:
        return False

    dtend_raw = _get_field(event, 'DTEND')
    dtend     = _parse_dt(dtend_raw) if dtend_raw else dtstart + timedelta(hours=1)

    return dtstart <= date_to and dtend >= date_from

# ─────────────────────────── Загрузка событий ──────────────────────────────────
def load_events_in_range(
    email: str,
    date_from: datetime,
    date_to: datetime,
) -> tuple[list[dict], str]:
    """
    Загружает события пользователя и возвращает только те,
    что попадают в диапазон [date_from, date_to].

    Возвращает (events, token) — токен нужен для последующего удаления.
    """
    token   = get_token(email)
    all_ids = list_event_ids(email, token)
    logger.info(f'Всего событий в календаре: {len(all_ids)}')

    result = []
    for event_id in all_ids:
        ical = fetch_event_ical(email, event_id, token)
        if not ical:
            continue

        parsed = parse_vevent(ical)
        if not parsed or not is_in_range(parsed, date_from, date_to):
            continue

        result.append({
            'index':     len(result) + 1,
            'event_id':  event_id,
            'summary':   _get_field(parsed, 'SUMMARY') or '(без названия)',
            'dt_start':  _parse_dt(_get_field(parsed, 'DTSTART')),
            'dt_end':    _parse_dt(_get_field(parsed, 'DTEND')),
            'is_series': bool(_get_field(parsed, 'RRULE')),
            'uid':       _get_field(parsed, 'UID'),
            'raw':       parsed,
        })

    logger.info(f'Найдено событий в диапазоне: {len(result)}')
    return result, token

# ──────────────────────────────── UI: вывод ────────────────────────────────────
def _fmt_dt(dt: datetime | None) -> str:
    return dt.strftime('%d.%m.%Y %H:%M') if dt else '?'


def print_events(events: list[dict]) -> None:
    if not events:
        print('\n  Событий в указанном диапазоне не найдено.\n')
        return

    col_n    = 4
    col_name = 38
    col_dt   = 18
    header   = f'  {"№":<{col_n}} {"Название":<{col_name}} {"Начало":<{col_dt}} Серия?'
    sep      = '─' * len(header)

    print(f'\n{sep}')
    print(header)
    print(sep)
    for ev in events:
        repeat = '🔁 да' if ev['is_series'] else 'нет'
        name   = ev['summary'][:col_name - 1]
        print(f'  {ev["index"]:<{col_n}} {name:<{col_name}} {_fmt_dt(ev["dt_start"]):<{col_dt}} {repeat}')
    print(f'{sep}\n')

# ──────────────────────────────── UI: ввод ─────────────────────────────────────
def _ask_date(prompt: str, default: datetime) -> datetime:
    while True:
        raw = input(f'{prompt} [{default:%d.%m.%Y}]: ').strip()
        if not raw:
            return default
        try:
            return datetime.strptime(raw, '%d.%m.%Y')
        except ValueError:
            print('  Неверный формат. Используйте ДД.ММ.ГГГГ')


def ask_date_range() -> tuple[datetime, datetime]:
    today      = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    default_to = today + timedelta(days=30)

    print('\nУкажите диапазон дат для поиска событий:')
    date_from = _ask_date('  Начало', today)
    date_to   = _ask_date('  Конец ', default_to)

    if date_to < date_from:
        print('  ⚠ Дата окончания раньше начала — они поменяны местами.')
        date_from, date_to = date_to, date_from

    date_to = date_to.replace(hour=23, minute=59, second=59)
    return date_from, date_to


def ask_indices(events: list[dict]) -> list[int]:
    """
    Форматы ввода:
        1        - одно событие
        1 3 5    - несколько через пробел
        2-6      - диапазон включительно
        all      - все события
    """
    max_idx = len(events)
    print('Введите номера событий для удаления (например: 1  |  1 3 5  |  2-6  |  all):')
    raw = input('>>> ').strip().lower()

    if raw == 'all':
        return list(range(1, max_idx + 1))

    indices: set[int] = set()
    for part in raw.split():
        if '-' in part:
            try:
                lo, hi = part.split('-', 1)
                indices.update(range(int(lo), int(hi) + 1))
            except ValueError:
                print(f'  ⚠ Пропускаю некорректный диапазон: "{part}"')
        else:
            try:
                indices.add(int(part))
            except ValueError:
                print(f'  ⚠ Пропускаю некорректное значение: "{part}"')

    valid   = sorted(i for i in indices if 1 <= i <= max_idx)
    invalid = sorted(i for i in indices if not (1 <= i <= max_idx))
    if invalid:
        print(f'  ⚠ Номера вне диапазона проигнорированы: {invalid}')
    return valid


def ask_delete_mode(event: dict) -> str:
    """
    Для повторяющегося события уточняет режим удаления.
    Возвращает: 'single' | 'all' | 'cancel'
    """
    if not event['is_series']:
        return 'single'

    print(f'\n  Событие «{event["summary"]}» является серией.')
    print('  Что удалить?')
    print('    1 - только это вхождение')
    print('    2 - всю серию (все вхождения с тем же UID)')
    print('    0 - пропустить')

    choice = input('  Ваш выбор: ').strip()
    return {'1': 'single', '2': 'all', '0': 'cancel'}.get(choice, 'cancel')

# ─────────────────────────── Логика удаления ───────────────────────────────────
def delete_selected(
    events: list[dict],
    indices: list[int],
    email: str,
    token: str,
) -> None:
    selected       = [ev for ev in events if ev['index'] in indices]
    deleted_series = set()

    for ev in selected:
        print(f'\n  [{ev["index"]}] {ev["summary"]} — {_fmt_dt(ev["dt_start"])}')

        if ev['is_series'] and ev['uid'] in deleted_series:
            print('    ↳ серия уже удалена, пропускаю.')
            continue

        mode = ask_delete_mode(ev)

        if mode == 'cancel':
            print('    ↳ пропущено.')
            continue

        if mode == 'single':
            if delete_event(email, ev['event_id'], token):
                print('    ✓ Удалено.')

        elif mode == 'all':
            series = [e for e in events if e['uid'] == ev['uid']]
            ok     = sum(delete_event(email, e['event_id'], token) for e in series)
            deleted_series.add(ev['uid'])
            print(f'    ✓ Удалена вся серия ({ok}/{len(series)} вхождений).')

# ──────────────────────────────── Точка входа ──────────────────────────────────
def main() -> None:
    print('═' * 50)
    print('  Управление событиями календаря (CalDAV)')
    print('═' * 50)

    date_from, date_to = ask_date_range()
    logger.info(f'Диапазон: {date_from:%d.%m.%Y} — {date_to:%d.%m.%Y}')

    events, token = load_events_in_range(USER_EMAIL, date_from, date_to)
    print_events(events)

    if not events:
        return

    indices = ask_indices(events)
    if not indices:
        print('Ничего не выбрано.')
        return

    chosen = [ev for ev in events if ev['index'] in indices]
    print(f'\nВыбрано {len(chosen)} событий для удаления:')
    for ev in chosen:
        marker = ' [серия]' if ev['is_series'] else ''
        print(f'  - {ev["summary"]}{marker} ({_fmt_dt(ev["dt_start"])})')

    confirm = input('\nПодтвердить удаление? (да/нет): ').strip().lower()
    if confirm not in ('да', 'y', 'yes', 'д'):
        print('Отменено.')
        return

    delete_selected(events, indices, USER_EMAIL, token)
    print('\nГотово.')


if __name__ == '__main__':
    try:
        main()
    finally:
        SESSION.close()