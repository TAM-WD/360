import os
import re
import sys
import json
import time
import uuid
import struct
import logging
import sqlite3
import tempfile
import threading
import subprocess
import requests
import imageio_ffmpeg

from contextlib import contextmanager
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, request, jsonify
from docx import Document
from docx.shared import Pt, Cm, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml.ns import qn

# ============================================================
# Конфигурация
# ============================================================

MESSENGER_BOT_TOKEN = os.environ["MESSENGER_BOT_TOKEN"]
MESSENGER_BASE_URL  = "https://botapi.messenger.yandex.net/bot/v1"

YC_API_KEY   = os.environ["YC_API_KEY"]
YC_FOLDER_ID = os.environ["YC_FOLDER_ID"]

SPEECHKIT_V1_URL = "https://stt.api.cloud.yandex.net/speech/v1/stt:recognize"

ALICEAI_COMPLETION_URL = "https://ai.api.cloud.yandex.net/v1/chat/completions"
ALICEAI_MODEL_URI      = f"gpt://{YC_FOLDER_ID}/aliceai-llm/latest"

FFMPEG_PATH = imageio_ffmpeg.get_ffmpeg_exe()

MAX_V1_SIZE = 1 * 1024 * 1024

AUDIO_EXTENSIONS = {
    ".ogg", ".opus", ".mp3", ".wav", ".m4a", ".flac",
    ".amr", ".aac", ".wma", ".webm", ".spx",
}

DB_PATH = os.environ.get("BOT_DB_PATH", "bot_state.db")

# ============================================================
# ЛОГИРОВАНИЕ
# ============================================================

LOG_DIR = os.environ.get("BOT_LOG_DIR", "logs")
LOG_LLM_DIR = os.path.join(LOG_DIR, "llm")
LOG_AUDIO_DIR = os.path.join(LOG_DIR, "audio")
os.makedirs(LOG_LLM_DIR, exist_ok=True)
os.makedirs(LOG_AUDIO_DIR, exist_ok=True)

logger = logging.getLogger("voicebot")
logger.setLevel(logging.INFO)
logger.propagate = False

_console = logging.StreamHandler()
_console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

_file = RotatingFileHandler(
    os.path.join(LOG_DIR, "bot.log"), maxBytes=10 * 1024 * 1024,
    backupCount=5, encoding="utf-8")
_file.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"))

logger.addHandler(_console)
logger.addHandler(_file)


def dump_llm_call(session_id: str, step: str, system_prompt: str,
                   user_message: str, raw_response: str, status: str = "",
                   attempt: int = 1) -> str:
    """Сохраняет на диск полный запрос и ответ модели — без этого сложно
    разбираться, почему конкретный вызов вернул некорректный или
    обрезанный JSON."""
    fname = f"{session_id}_{step}_attempt{attempt}_{int(time.time() * 1000)}.json"
    path = os.path.join(LOG_LLM_DIR, fname)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump({
                "session_id": session_id,
                "step": step,
                "attempt": attempt,
                "status": status,
                "system_prompt": system_prompt,
                "user_message": user_message,
                "raw_response": raw_response,
                "raw_response_len": len(raw_response),
            }, f, ensure_ascii=False, indent=2)
    except OSError as e:
        logger.warning("Не удалось сохранить дамп вызова модели: %s", e)
    return path


def dump_audio_chunk(session_id: str, chunk_idx: int, chunk_bytes: bytes,
                      transcript: str, ok: bool):
    fname = f"{session_id}_chunk{chunk_idx}.ogg"
    path = os.path.join(LOG_AUDIO_DIR, fname)
    try:
        with open(path, "wb") as f:
            f.write(chunk_bytes)
    except OSError as e:
        logger.warning("Не удалось сохранить аудиофрагмент: %s", e)
    logger.info(
        "[%s] Фрагмент %d: размер %d байт, распознан=%s, "
        "символов в расшифровке=%d, сохранён: %s",
        session_id, chunk_idx, len(chunk_bytes), ok, len(transcript), path)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def pluralize(n: int, form1: str, form2: str, form5: str) -> str:
    """Возвращает нужную форму слова по числу n:
    form1 — для 1, 21, 31…    (например, «сообщение»)
    form2 — для 2-4, 22-24…   (например, «сообщения»)
    form5 — для 0, 5-20, 25-30… (например, «сообщений»)"""
    n_abs = abs(n) % 100
    last_digit = n_abs % 10
    if 11 <= n_abs <= 14:
        return form5
    if last_digit == 1:
        return form1
    if 2 <= last_digit <= 4:
        return form2
    return form5


def voices_phrase(n: int) -> str:
    word = pluralize(n, "голосовое сообщение", "голосовых сообщения", "голосовых сообщений")
    return f"{n} {word}"


def items_word(n: int) -> str:
    return pluralize(n, "пункт", "пункта", "пунктов")


def fragments_word(n: int) -> str:
    return pluralize(n, "фрагмент", "фрагмента", "фрагментов")


# ============================================================
# СТРУКТУРА ЧЕК-ЛИСТА (21 блок)
# ============================================================

CHECKLIST_BLOCKS = [
    {
        "number": 1,
        "title": "Обучение сотрудников",
        "items": [
            "1.1 Проводится ли обучение для каждого сотрудника, назначенного на новую должность?",
            "1.2 Включает ли первоначальное обучение подробный разбор возможных производственных рисков и опасностей?",
            "1.3 Проводится ли достаточно наглядный инструктаж по использованию СИЗ?",
            "1.4 Осуществляется ли обучение по использованию аварийно-спасательного оборудования?",
        ],
    },
    {
        "number": 2,
        "title": "Окружающая среда",
        "items": [
            "2.1 Имеются ли средства для работы в очень жарких/холодных условиях?",
            "2.2 Водоотталкивающие/зимние СИЗ удобны, не создают рисков при ношении?",
            "2.3 Безопасны ли рабочие и опорные поверхности при намокании?",
            "2.4 Знают ли работники симптомы теплового удара, обморожения, гипотермии?",
        ],
    },
    {
        "number": 3,
        "title": "Освещённость",
        "items": [
            "3.1 Достаточен ли уровень освещения для безопасной и комфортной работы?",
            "3.2 Вызывает ли освещение блики на рабочих поверхностях, экранах?",
            "3.3 Достаточен ли аварийный свет и проходит ли он регулярную проверку?",
        ],
    },
    {
        "number": 4,
        "title": "Проемы/отверстия в полу и стенах",
        "items": [
            "4.1 Оснащены ли проёмы и двери ограждениями?",
            "4.2 Ограждены ли временные проёмы или находятся ли поблизости сотрудники для контроля?",
        ],
    },
    {
        "number": 5,
        "title": "Лестницы, стремянки и платформы",
        "items": [
            "5.1 Лестницы и перила находятся в хорошем состоянии?",
            "5.2 Лестницы не имеют визуальных дефектов и повреждений?",
            "5.3 Лестницы/стремянки правильно установлены до начала работы?",
            "5.4 Надлежащим ли образом закреплены приподнятые платформы, есть ли поручни?",
        ],
    },
    {
        "number": 6,
        "title": "Подъемные устройства",
        "items": [
            "6.1 Подъемные устройства используются только в рамках своей грузоподъемности?",
            "6.2 Указаны ли лимиты по грузоподъемности на оборудовании?",
            "6.3 Проходит ли оборудование регулярный технический осмотр и обслуживание?",
            "6.4 Операторы обучены работе с подъемным оборудованием?",
        ],
    },
    {
        "number": 7,
        "title": "Ограниченные пространства",
        "items": [
            "7.1 Доступны и соблюдаются ли процедуры по работе в замкнутых пространствах?",
            "7.2 Достаточны ли процедуры входа и выхода из ограниченных пространств?",
            "7.3 Внедрены ли аварийные и спасательные процедуры при ЧП в ограниченных пространствах?",
        ],
    },
    {
        "number": 8,
        "title": "Хозяйственная часть (АБП)",
        "items": [
            "8.1 В помещениях поддерживается чистота и порядок?",
            "8.2 Нет ли на полах торчащих гвоздей, заноз, дыр, незакрепленных досок?",
            "8.3 Проходы и коридоры не загромождены?",
            "8.4 Чётко ли обозначены постоянные проходы и коридоры?",
            "8.5 Оборудованы ли открытые ямы/проемы, резервуары крышками/перилами?",
        ],
    },
    {
        "number": 9,
        "title": "Электробезопасность",
        "items": [
            "9.1 Соблюдаются ли стандарты при эксплуатации и обслуживании электрооборудования?",
            "9.2 Всё оборудование заземлено правильно?",
            "9.3 Ручной электроинструмент заземлен или с двойной изоляцией?",
            "9.4 Распределительные коробки закрыты?",
            "9.5 Удлинители не находятся в проходах, где возможны повреждения?",
            "9.6 Используется ли стационарная проводка вместо удлинителей?",
        ],
    },
    {
        "number": 10,
        "title": "Инструменты и оборудование",
        "items": [
            "10.1 Инструкции производителя хранятся и используются при работе?",
            "10.2 Электроинструмент соответствует стандартам предприятия?",
            "10.3 Бракованный инструмент маркируется и выводится из эксплуатации?",
            "10.4 Организовано ли обучение по безопасному использованию инструментов?",
        ],
    },
    {
        "number": 11,
        "title": "Ограждения оборудования",
        "items": [
            "11.1 Всё движущееся оборудование защищено ограждениями?",
            "11.2 Ограждения соответствуют стандартам безопасности?",
            "11.3 Все ограждения установлены и выполняют своё назначение?",
            "11.4 Соблюдаются ли процедуры блокировки при обслуживании без ограждений?",
        ],
    },
    {
        "number": 12,
        "title": "Уровень шума",
        "items": [
            "12.1 Проводятся ли регулярные замеры уровня шума?",
            "12.2 СИЗ для защиты слуха предоставлены и используются правильно?",
        ],
    },
    {
        "number": 13,
        "title": "Временные рабочие конструкции",
        "items": [
            "13.1 Временные конструкции используются только при невозможности постоянных?",
            "13.2 Надлежащим ли образом укреплены выемки/ямы/котлованы?",
        ],
    },
    {
        "number": 14,
        "title": "Средства индивидуальной защиты (СИЗ)",
        "items": [
            "14.1 Все необходимые СИЗ предоставлены, хранение правильное, использование по назначению?",
            "14.2 Выбор СИЗ выполнен с учетом конкретных опасных факторов?",
            "14.3 Используемые СИЗ надёжны?",
            "14.4 Обозначены ли опасные зоны, требующие ношения СИЗ?",
        ],
    },
    {
        "number": 15,
        "title": "Обращение с материалами и их хранение",
        "items": [
            "15.1 Обеспечено ли безопасное расстояние для проезда техники?",
            "15.2 Материалы складированы устойчивым и надёжным способом?",
            "15.3 В зоне хранения нет рисков опрокидывания?",
            "15.4 К управлению погрузчиком допущены только обученные специалисты?",
            "15.5 Зарядка аккумуляторов — только в отведённых местах?",
            "15.6 На ж/д путях применяются необходимые предупреждающие знаки?",
            "15.7 Указаны ли нормы допустимых нагрузок на стеллажи, полы?",
            "15.8 Стеллажи нагружаются только в пределах максимальной грузоподъемности?",
            "15.9 Тали, канаты и стропы соответствуют требованиям по грузоподъемности?",
            "15.10 Стропы проходят ежедневный осмотр перед применением?",
            "15.11 Используются только проверенные поддоны и платформы?",
            "15.12 Персонал поднимает грузы правильной техникой?",
        ],
    },
    {
        "number": 16,
        "title": "Опасные продукты и химикаты",
        "items": [
            "16.1 Паспорт безопасности изучен до взаимодействия с опасными веществами?",
            "16.2 Для работы с опасными продуктами используются соответствующие СИЗ?",
            "16.3 Хранение не допускает совместимости несочетаемых продуктов?",
            "16.4 Опасные вещества размещены в отдалении от источников тепла?",
            "16.5 Контейнеры проверяются на протечки, повреждения?",
            "16.6 Используются ли поддоны под ёмкостями?",
            "16.7 Вся опасная продукция промаркирована?",
            "16.8 Средства для ликвидации разливов доступны?",
            "16.9 Для горючих веществ предусмотрены устройства заземления?",
            "16.10 Вентиляция зоны хранения соответствует требованиям?",
        ],
    },
    {
        "number": 17,
        "title": "Технологические процессы",
        "items": [
            "17.1 Повторяющиеся действия на рабочем месте оптимизированы?",
            "17.2 Паспорта безопасности доступны для всех сотрудников?",
            "17.3 Риски и опасности отмечены предупреждающими табличками?",
            "17.4 Техника проходит обязательный осмотр перед сменой и периодическое ТО?",
            "17.5 Процедуры LOTO внедрены и строго соблюдаются?",
            "17.6 Вентиляционное оборудование исправно и работает эффективно?",
            "17.7 Система вытяжки/пылеулавливания в исправном состоянии?",
            "17.8 Души/станции самоспасения и фонтанчики для глаз доступны и исправны?",
        ],
    },
    {
        "number": 18,
        "title": "Бытовые помещения для сотрудников",
        "items": [
            "18.1 Помещения содержатся в чистоте и надлежащей санитарии?",
            "18.2 Мебель, шкафчики, сантехника в исправном состоянии?",
            "18.3 Столовая отделена от зоны хранения опасных веществ?",
            "18.4 Помещения оборудованы исправными мойками с гигиеническими средствами?",
        ],
    },
    {
        "number": 19,
        "title": "Пожарная безопасность",
        "items": [
            "19.1 На каждом рабочем месте размещён актуальный план эвакуации?",
            "19.2 Все сотрудники знают этот план?",
            "19.3 Огнетушители подобраны с учетом масштабов возможных пожаров?",
            "19.4 Количество огнетушителей достаточное?",
            "19.5 Места расположения огнетушителей визуально отмечены?",
            "19.6 Огнетушители правильно установлены и легкодоступны?",
            "19.7 Все огнетушители не просроченные, заряжены и готовы?",
            "19.8 Огнетушители специального назначения промаркированы?",
        ],
    },
    {
        "number": 20,
        "title": "Пути эвакуации и эвакуационные выходы",
        "items": [
            "20.1 Достаточно ли выходов для быстрой эвакуации?",
            "20.2 Доступ к выходам свободен, пути не загромождены?",
            "20.3 Двери открываются в экстренной ситуации беспрепятственно?",
            "20.4 Все эвакуационные выходы чётко промаркированы?",
            "20.5 На выходах и путях эвакуации есть аварийное освещение?",
            "20.6 Проходы и выходы эвакуации свободны?",
        ],
    },
    {
        "number": 21,
        "title": "Медицинская помощь и первая помощь",
        "items": [
            "21.1 Все сотрудники знают, как получить первую помощь?",
            "21.2 Лица, оказывающие первую помощь, знают куда доставлять пострадавшего?",
            "21.3 На каждой смене есть обученные оказанию первой помощи сотрудники?",
            "21.4 Аптечки полностью укомплектованы, ЛС не просрочены?",
            "21.5 Медикаменты пополняются своевременно по мере расходования?",
        ],
    },
]

# ============================================================
# СИСТЕМНЫЕ ПРОМПТЫ (для модели — намеренно оставлены без изменений
# формулировок, чтобы не влиять на качество работы нейросети)
# ============================================================

SYSTEM_PROMPT_EXTRACT = """Ты — ассистент по охране труда и промышленной безопасности.

Тебе приходит расшифровка одного или нескольких голосовых сообщений сотрудника,
описывающего обход объекта (сообщения могут быть разделены пометками
"[Голосовое N, источник: ...]" — это просто маркеры порядка записи, не часть смысла).

Извлеки ВСЮ информацию и верни строго валидный JSON (без markdown):

{
  "object_name": "название объекта или пустая строка",
  "address": "адрес или пустая строка",
  "inspector": "имя проверяющего или пустая строка",
  "director": "имя директора/управляющего или пустая строка",
  "employees_on_shift": "сотрудники на смене или пустая строка",
  "notes": "общие замечания",
  "findings": [
    {
      "item_id": "номер пункта (например 19.3), или пустая строка если не определить точно",
      "block_number": числовой номер блока от 1 до 21,
      "description": "что обнаружено",
      "status": "ok | violation | warning | not_checked",
      "comment": "детали",
      "deadline": "срок устранения или пустая строка"
    }
  ]
}

Правила:
- Извлекай ВСЮ информацию из всех голосовых сразу, они относятся к одной проверке.
- «Всё нормально» = status: "ok".
- Проблема = "violation" или "warning".
- Соотноси с номерами блоков (1-21) максимально точно.
- ТОЛЬКО валидный JSON, без комментариев и markdown."""

SYSTEM_PROMPT_FILL_BLOCK_TEMPLATE = """Ты — ассистент по охране труда.

Тебе приходят данные из голосового отчёта проверяющего (JSON) и список
критериев ОДНОГО блока чек-листа. Заполни оценку ТОЛЬКО для этих критериев.

Критерии блока:
{block_items}

Ответь строго в формате JSON-массива (без markdown), по одному объекту
на КАЖДЫЙ критерий из списка выше:

[
  {{"item_id": "X.Y", "assessment": "ДА | НЕТ | Н/П | НЕ ПРОВЕРЕНО",
    "comment": "комментарий или пустая строка",
    "deadline": "срок устранения или пустая строка"}}
]

Правила:
- Ровно один объект на каждый критерий блока, не пропускай и не добавляй лишние.
- Если данных по пункту нет — «НЕ ПРОВЕРЕНО», пустой комментарий.
- Ответ должен начинаться с [ и заканчиваться ], без пояснений."""

SYSTEM_PROMPT_SUMMARY = """Ты — ассистент по охране труда.

Тебе приходит заполненный чек-лист. Сформируй КРАТКИЙ ИТОГОВЫЙ АКТ:

1. ШАПКА: объект, дата, проверяющий
2. КРИТИЧЕСКИЕ НАРУШЕНИЯ — список с номерами пунктов
3. ПРЕДУПРЕЖДЕНИЯ — список
4. РЕКОМЕНДАЦИИ ПО УСТРАНЕНИЮ — действия и сроки
5. СТАТИСТИКА: проверено / нарушений / предупреждений / не проверено
6. ОБЩИЙ ВЫВОД

Кратко, деловым языком, формат официального документа."""

app = Flask(__name__)

# ============================================================
# КОМАНДЫ ЧАТ-БОТА
# ============================================================

HELP_CMDS   = {"/start", "/help", "помощь", "help", "start"}
BLOCKS_CMDS = {"/blocks", "блоки", "список блоков"}
STATUS_CMDS = {"/status", "статус"}
CANCEL_CMDS = {"отмена", "очистить", "сброс", "/cancel"}
TRIGGER_CMDS = {
    "запусти обработку", "запустить обработку",
    "начать обработку", "старт обработки", "обработать",
}


def normalize_cmd(text: str) -> str:
    t = text.strip().lower()
    t = re.sub(r"\s+", " ", t)
    t = t.strip(" .!?,;:")
    return t


# ============================================================
# СЛОЙ ХРАНЕНИЯ СОСТОЯНИЯ (SQLite)
# ============================================================

_locks: dict = {}
_locks_guard = threading.Lock()


def get_lock(key: str) -> threading.Lock:
    """Лок на конкретный чат/пользователя, чтобы параллельные апдейты
    (например несколько голосовых, пришедших почти одновременно) не
    портили очередь друг друга."""
    with _locks_guard:
        lock = _locks.get(key)
        if lock is None:
            lock = threading.Lock()
            _locks[key] = lock
        return lock


def _raw_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def db():
    conn = _raw_conn()
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def init_db():
    with db() as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS processed_updates (
            update_id INTEGER PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS chat_state (
            chat_key   TEXT PRIMARY KEY,
            seen_help  INTEGER NOT NULL DEFAULT 0,
            status     TEXT NOT NULL DEFAULT 'idle',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS voice_items (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_key    TEXT NOT NULL,
            seq         INTEGER NOT NULL,
            source      TEXT,
            file_name   TEXT,
            transcript  TEXT,
            duration_ms INTEGER,
            created_at  TEXT NOT NULL
        );
        """)
    logger.info("База данных готова к работе: %s", DB_PATH)


def chat_key_from_reply(reply_to: dict) -> str:
    if "chat_id" in reply_to:
        return f"chat:{reply_to['chat_id']}"
    return f"user:{reply_to['login']}"


def ensure_chat_state(key: str):
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO chat_state "
            "(chat_key, seen_help, status, created_at, updated_at) "
            "VALUES (?, 0, 'idle', ?, ?)",
            (key, now_iso(), now_iso()),
        )


def has_seen_help(key: str) -> bool:
    with db() as conn:
        row = conn.execute(
            "SELECT seen_help FROM chat_state WHERE chat_key=?", (key,)
        ).fetchone()
        return bool(row and row["seen_help"])


def mark_seen_help(key: str):
    with db() as conn:
        conn.execute(
            "UPDATE chat_state SET seen_help=1, updated_at=? WHERE chat_key=?",
            (now_iso(), key),
        )


def is_update_processed(update_id: int) -> bool:
    with db() as conn:
        row = conn.execute(
            "SELECT 1 FROM processed_updates WHERE update_id=?", (update_id,)
        ).fetchone()
        return row is not None


def mark_update_processed(update_id: int):
    with db() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO processed_updates (update_id) VALUES (?)",
            (update_id,),
        )


def add_voice_item(key: str, source: str, file_name: str, transcript: str,
                    duration_ms: int = None):
    with db() as conn:
        row = conn.execute(
            "SELECT COALESCE(MAX(seq), 0) + 1 AS next_seq "
            "FROM voice_items WHERE chat_key=?", (key,)
        ).fetchone()
        next_seq = row["next_seq"]
        conn.execute(
            "INSERT INTO voice_items "
            "(chat_key, seq, source, file_name, transcript, duration_ms, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (key, next_seq, source, file_name, transcript, duration_ms, now_iso()),
        )
        conn.execute(
            "UPDATE chat_state SET status='collecting', updated_at=? WHERE chat_key=?",
            (now_iso(), key),
        )


def get_pending_items(key: str) -> list:
    with db() as conn:
        rows = conn.execute(
            "SELECT seq, source, file_name, transcript, duration_ms "
            "FROM voice_items WHERE chat_key=? ORDER BY seq ASC", (key,)
        ).fetchall()
        return [dict(r) for r in rows]


def count_pending(key: str) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM voice_items WHERE chat_key=?", (key,)
        ).fetchone()
        return row["c"]


def clear_session(key: str):
    with db() as conn:
        conn.execute("DELETE FROM voice_items WHERE chat_key=?", (key,))
        conn.execute(
            "UPDATE chat_state SET status='idle', updated_at=? WHERE chat_key=?",
            (now_iso(), key),
        )


# ============================================================
# MESSENGER BOT API
# ============================================================

def messenger_headers(content_type=None) -> dict:
    headers = {"Authorization": f"OAuth {MESSENGER_BOT_TOKEN}"}
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def send_text(text: str, chat_id: str = None, login: str = None,
              reply_message_id: int = None):
    url = f"{MESSENGER_BASE_URL}/messages/sendText/"
    payload = {"text": text[:6000]}
    if chat_id:
        payload["chat_id"] = chat_id
    elif login:
        payload["login"] = login
    else:
        raise ValueError("Нужен chat_id или login")
    if reply_message_id:
        payload["reply_message_id"] = reply_message_id
    resp = requests.post(url, headers=messenger_headers("application/json"),
                         json=payload)
    resp.raise_for_status()
    data = resp.json()
    if not data.get("ok"):
        logger.error("Ошибка при отправке сообщения: %s", data.get("description"))
    return data


def send_long_text(text: str, chat_id: str = None, login: str = None,
                   reply_message_id: int = None):
    chunks = [text[i:i + 6000] for i in range(0, len(text), 6000)]
    for i, chunk in enumerate(chunks):
        send_text(chunk, chat_id=chat_id, login=login,
                  reply_message_id=reply_message_id if i == 0 else None)


def send_file(file_path: str, chat_id: str = None, login: str = None):
    url = f"{MESSENGER_BASE_URL}/messages/sendFile/"
    data = {}
    if chat_id:
        data["chat_id"] = chat_id
    elif login:
        data["login"] = login
    else:
        raise ValueError("Нужен chat_id или login")
    file_name = os.path.basename(file_path)
    with open(file_path, "rb") as f:
        files = {"document": (file_name, f)}
        resp = requests.post(
            url,
            headers={"Authorization": f"OAuth {MESSENGER_BOT_TOKEN}"},
            data=data, files=files)
    resp.raise_for_status()
    return resp.json()


def get_file(file_id: str) -> bytes:
    url = f"{MESSENGER_BASE_URL}/messages/getFile/"
    resp = requests.get(url,
                        headers={"Authorization": f"OAuth {MESSENGER_BOT_TOKEN}"},
                        params={"file_id": file_id}, stream=True)
    resp.raise_for_status()
    ct = resp.headers.get("Content-Type", "")
    if "application/json" in ct:
        data = resp.json()
        if not data.get("ok"):
            raise RuntimeError(f"Ошибка получения файла: {data.get('description')}")
    return resp.content


def get_updates(offset: int = 0, limit: int = 100) -> dict:
    url = f"{MESSENGER_BASE_URL}/messages/getUpdates/"
    resp = requests.get(url, headers=messenger_headers(),
                        params={"offset": offset, "limit": limit})
    resp.raise_for_status()
    return resp.json()


def set_webhook(webhook_url):
    url = f"{MESSENGER_BASE_URL}/self/update/"
    resp = requests.post(url, headers=messenger_headers("application/json"),
                         json={"webhook_url": webhook_url})
    resp.raise_for_status()
    return resp.json()


# ============================================================
# АУДИО: УТИЛИТЫ
# ============================================================

def is_audio_file(file_name: str) -> bool:
    if not file_name:
        return False
    return os.path.splitext(file_name)[1].lower() in AUDIO_EXTENSIONS


def detect_audio_format(audio_bytes: bytes, file_name: str = "") -> str:
    header = audio_bytes[:16] if len(audio_bytes) >= 16 else audio_bytes
    if header[:4] == b'OggS':
        return "oggopus"
    if header[:3] == b'ID3' or (len(header) >= 2 and header[0] == 0xFF
                                 and (header[1] & 0xE0) == 0xE0):
        return "mp3"
    if header[:4] == b'RIFF':
        return "lpcm"
    if header[:4] in (b'fLaC', b'\x1a\x45\xdf\xa3'):
        return None
    if len(header) >= 8 and header[4:8] == b'ftyp':
        return None
    if header[:6] == b'#!AMR\n':
        return None
    ext = os.path.splitext(file_name)[1].lower() if file_name else ""
    return {".ogg": "oggopus", ".opus": "oggopus",
            ".mp3": "mp3", ".wav": "lpcm"}.get(ext)


def get_wav_sample_rate(audio_bytes: bytes) -> int:
    if len(audio_bytes) >= 28 and audio_bytes[:4] == b'RIFF':
        return struct.unpack_from('<I', audio_bytes, 24)[0]
    return 48000


def convert_to_ogg_opus(audio_bytes: bytes, file_name: str = "") -> bytes:
    ext = os.path.splitext(file_name)[1].lower() if file_name else ".bin"
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp_in:
        tmp_in.write(audio_bytes)
        tmp_in_path = tmp_in.name
    tmp_out_path = tmp_in_path + ".ogg"
    try:
        subprocess.run(
            [FFMPEG_PATH, "-y", "-i", tmp_in_path, "-acodec", "libopus",
             "-ac", "1", "-ar", "48000", "-b:a", "48k", tmp_out_path],
            capture_output=True, text=True, timeout=30, check=True)
        with open(tmp_out_path, "rb") as f:
            return f.read()
    finally:
        for p in (tmp_in_path, tmp_out_path):
            try: os.unlink(p)
            except OSError: pass


def get_audio_duration(file_path: str) -> float:
    probe = subprocess.run(
        [FFMPEG_PATH, "-i", file_path, "-f", "null", "-"],
        capture_output=True, text=True, timeout=30)
    for line in probe.stderr.split("\n"):
        if "Duration:" in line:
            parts = line.split("Duration:")[1].split(",")[0].strip()
            try:
                h, m, s = parts.split(":")
                return int(h) * 3600 + int(m) * 60 + float(s)
            except ValueError:
                return 0.0
    return 0.0


def detect_silences(file_path: str, noise_db: str = "-30dB",
                    min_silence_dur: float = 0.3) -> list:
    """Определяет паузы в аудио через ffmpeg silencedetect. Используется,
    чтобы резать длинные записи по паузам речи, а не вслепую по
    фиксированному времени — иначе можно обрубить слово прямо на границе
    фрагмента и потерять кусок расшифровки."""
    result = subprocess.run(
        [FFMPEG_PATH, "-i", file_path, "-af",
         f"silencedetect=noise={noise_db}:d={min_silence_dur}",
         "-f", "null", "-"],
        capture_output=True, text=True, timeout=60)
    silences = []
    start = None
    for line in result.stderr.split("\n"):
        if "silence_start" in line:
            m = re.search(r"silence_start:\s*([\d.]+)", line)
            if m:
                start = float(m.group(1))
        elif "silence_end" in line and start is not None:
            m = re.search(r"silence_end:\s*([\d.]+)", line)
            if m:
                silences.append((start, float(m.group(1))))
                start = None
    return silences


def split_audio_smart(audio_bytes: bytes, file_name: str,
                      session_id: str, max_chunk_duration: float = 22.0,
                      lookahead: float = 6.0) -> list:
    """Режет аудио по паузам речи рядом с отметкой max_chunk_duration.
    Если рядом с точкой разреза паузы не нашлось — режет жёстко, но это
    явно фиксируется в логе как потенциальная зона риска потери слов
    на стыке фрагментов."""
    ext = os.path.splitext(file_name)[1].lower() if file_name else ".ogg"
    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp_in:
        tmp_in.write(audio_bytes)
        tmp_in_path = tmp_in.name

    chunks = []
    try:
        duration = get_audio_duration(tmp_in_path)
        if duration <= 0:
            logger.warning(
                "[%s] Не удалось определить длительность аудио — считаю "
                "запись длинной и режу с фиксированным шагом %s сек",
                session_id, max_chunk_duration)
            duration = 9999

        if duration <= max_chunk_duration:
            return [audio_bytes]

        silences = detect_silences(tmp_in_path)
        logger.info("[%s] Длительность записи: %.1f сек, найдено пауз: %d",
                   session_id, duration, len(silences))

        cut_points = [0.0]
        cursor = max_chunk_duration
        while cursor < duration:
            candidate = None
            for s_start, s_end in silences:
                mid = (s_start + s_end) / 2
                if cursor <= mid <= cursor + lookahead:
                    candidate = mid
                    break
            cut_point = candidate if candidate else cursor
            if not candidate:
                logger.warning(
                    "[%s] Рядом с %.1f сек пауза не найдена, разрез сделан "
                    "в фиксированной точке — возможна потеря слова на стыке",
                    session_id, cursor)
            cut_points.append(cut_point)
            cursor = cut_point + max_chunk_duration
        cut_points.append(duration)

        for idx in range(len(cut_points) - 1):
            start, end = cut_points[idx], cut_points[idx + 1]
            if end - start < 0.5:
                continue
            tmp_out = tmp_in_path + f"_chunk{idx}.ogg"
            subprocess.run(
                [FFMPEG_PATH, "-y", "-i", tmp_in_path, "-ss", str(start),
                 "-t", str(end - start), "-acodec", "libopus", "-ac", "1",
                 "-ar", "48000", "-b:a", "48k", tmp_out],
                capture_output=True, text=True, timeout=30)
            if os.path.exists(tmp_out):
                with open(tmp_out, "rb") as f:
                    data = f.read()
                if data:
                    if len(data) > MAX_V1_SIZE:
                        logger.warning(
                            "[%s] Фрагмент %d превышает 1 МБ (%d байт) — "
                            "SpeechKit может отклонить запрос",
                            session_id, idx, len(data))
                    chunks.append(data)
                try: os.unlink(tmp_out)
                except OSError: pass
    finally:
        try: os.unlink(tmp_in_path)
        except OSError: pass

    return chunks


# ============================================================
# SPEECHKIT v1
# ============================================================

def recognize_speech_v1(audio_bytes: bytes, file_name: str = "") -> str:
    fmt = detect_audio_format(audio_bytes, file_name) or "oggopus"
    params = {"folderId": YC_FOLDER_ID, "lang": "ru-RU",
              "topic": "general", "format": fmt}
    if fmt == "lpcm":
        params["sampleRateHertz"] = str(get_wav_sample_rate(audio_bytes))
    resp = requests.post(SPEECHKIT_V1_URL,
                         headers={"Authorization": f"Api-Key {YC_API_KEY}"},
                         params=params, data=audio_bytes)
    if resp.status_code != 200:
        raise RuntimeError(f"Ошибка SpeechKit (код {resp.status_code}): {resp.text}")
    return resp.json().get("result", "")


def recognize_speech(audio_bytes: bytes, file_name: str = "",
                     session_id: str = None) -> str:
    session_id = session_id or uuid.uuid4().hex[:8]
    fmt = detect_audio_format(audio_bytes, file_name)
    if fmt is None:
        audio_bytes = convert_to_ogg_opus(audio_bytes, file_name)
        file_name = "converted.ogg"

    chunks = split_audio_smart(audio_bytes, file_name, session_id)
    logger.info("[%s] Аудио разбито на %d %s",
               session_id, len(chunks), fragments_word(len(chunks)))

    texts = []
    for i, chunk in enumerate(chunks):
        ok = False
        t = ""
        try:
            t = recognize_speech_v1(chunk, "chunk.ogg")
            if t.strip():
                texts.append(t)
                ok = True
        except Exception as e:
            logger.warning("[%s] Фрагмент %d не распознан: %s",
                          session_id, i + 1, e)
        dump_audio_chunk(session_id, i, chunk, t, ok)

    full_text = " ".join(texts)
    logger.info(
        "[%s] Итоговая расшифровка получена: %d символов, обработано фрагментов: %d",
        session_id, len(full_text), len(chunks))
    return full_text


# ============================================================
# ALICEAI
# ============================================================

def call_aliceai_full(system_prompt: str, user_message: str,
                      temperature: float = 0.3, max_tokens: int = 4000) -> dict:
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Api-Key {YC_API_KEY}",
        "OpenAI-Project": YC_FOLDER_ID,
    }
    payload = {
        "model": ALICEAI_MODEL_URI,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_message},
        ],
    }

    resp = requests.post(ALICEAI_COMPLETION_URL, headers=headers,
                         json=payload, timeout=90)
    if resp.status_code != 200:
        raise RuntimeError(f"Ошибка AliceAI (код {resp.status_code}): {resp.text}")
    data = resp.json()
    try:
        choice = data["choices"][0]
        return {
            "text": choice["message"]["content"],
            "status": choice.get("finish_reason", ""),
        }
    except (KeyError, IndexError):
        raise RuntimeError(f"Некорректный формат ответа AliceAI: {data}")


def call_aliceai(system_prompt: str, user_message: str,
                   temperature: float = 0.3, max_tokens: int = 4000) -> str:
    return call_aliceai_full(system_prompt, user_message,
                               temperature, max_tokens)["text"]


def parse_json_from_llm(text: str):
    text = re.sub(r'```json\s*', '', text)
    text = re.sub(r'```\s*', '', text)
    text = text.strip()
    return json.loads(text)


def validate_checklist_fill(checklist_items: list) -> dict:
    """Считает, сколько пунктов реально заполнено, а не просто
    сгенерировано пустым списком."""
    stats = {"total": 0, "yes": 0, "no": 0, "na": 0,
             "not_checked": 0, "filled_total": 0}
    if not isinstance(checklist_items, list):
        return stats
    stats["total"] = len(checklist_items)
    for item in checklist_items:
        a = str(item.get("assessment", "")).upper().strip()
        if a == "ДА":
            stats["yes"] += 1
        elif a == "НЕТ":
            stats["no"] += 1
        elif a == "Н/П":
            stats["na"] += 1
        else:
            stats["not_checked"] += 1
    stats["filled_total"] = stats["yes"] + stats["no"] + stats["na"]
    return stats


def fill_single_block(session_id: str, block: dict, extracted_json: str) -> list:
    """Заполняет один блок чек-листа отдельным вызовом модели. Благодаря
    небольшому размеру ответа (2-12 пунктов) риск обрезания генерации
    практически нулевой — в отличие от одного огромного вызова на все
    100+ пунктов сразу, который на практике обрывался посередине JSON."""
    items_text = "\n".join(block["items"])
    system_prompt = SYSTEM_PROMPT_FILL_BLOCK_TEMPLATE.format(block_items=items_text)
    user_message = f"Данные:\n\n{extracted_json}"

    for attempt in (1, 2):
        temp = 0.2 if attempt == 1 else 0.0
        try:
            result = call_aliceai_full(system_prompt, user_message,
                                         temperature=temp, max_tokens=1500)
        except Exception as e:
            logger.warning(
                "[%s] Блок %d: не удалось получить ответ от модели (попытка %d): %s",
                session_id, block["number"], attempt, e)
            continue

        dump_llm_call(session_id, f"fill_block_{block['number']}",
                     system_prompt, user_message, result["text"],
                     status=result["status"], attempt=attempt)

        if result["status"] and "TRUNCATED" in result["status"]:
            logger.warning(
                "[%s] Блок %d: ответ модели обрезан по лимиту токенов (попытка %d)",
                session_id, block["number"], attempt)
            continue

        try:
            parsed = parse_json_from_llm(result["text"])
            if isinstance(parsed, list):
                return parsed
        except json.JSONDecodeError as e:
            logger.warning(
                "[%s] Блок %d: не удалось разобрать JSON (попытка %d): %s",
                session_id, block["number"], attempt, e)
            continue

    logger.error("[%s] Блок %d: не удалось заполнить после двух попыток",
                session_id, block["number"])
    item_ids = []
    for t in block["items"]:
        m = re.match(r'^(\d+\.\d+)', t)
        item_ids.append(m.group(1) if m else "")
    return [{"item_id": iid, "assessment": "НЕ ПРОВЕРЕНО",
             "comment": "Не удалось обработать этот блок из-за сбоя нейросети",
             "deadline": ""}
            for iid in item_ids]


def fill_checklist_parallel(session_id: str, extracted: dict) -> list:
    extracted_json = json.dumps(extracted, ensure_ascii=False)
    all_items = []
    failed_blocks = []

    with ThreadPoolExecutor(max_workers=5) as pool:
        futures = {
            pool.submit(fill_single_block, session_id, block, extracted_json): block
            for block in CHECKLIST_BLOCKS
        }
        for future in as_completed(futures):
            block = futures[future]
            try:
                items = future.result()
            except Exception as e:
                logger.exception("[%s] Блок %d: непредвиденная ошибка: %s",
                                session_id, block["number"], e)
                items = []
                failed_blocks.append(block["number"])
            all_items.extend(items)

    logger.info(
        "[%s] Заполнение чек-листа завершено: %d пунктов, проблемные блоки: %s",
        session_id, len(all_items), failed_blocks or "нет")
    return all_items


def process_with_llm(recognized_text: str, session_id: str = None) -> dict:
    session_id = session_id or uuid.uuid4().hex[:12]
    results = {"session_id": session_id}

    logger.info("[%s] Шаг 1: извлечение данных из расшифровки", session_id)
    extract_result = call_aliceai_full(
        SYSTEM_PROMPT_EXTRACT, f"Расшифровка:\n\n{recognized_text}",
        temperature=0.1, max_tokens=3000)
    dump_llm_call(session_id, "extract", SYSTEM_PROMPT_EXTRACT,
                 recognized_text, extract_result["text"],
                 status=extract_result["status"])
    try:
        extracted = parse_json_from_llm(extract_result["text"])
    except json.JSONDecodeError:
        logger.warning(
            "[%s] Не удалось разобрать JSON с извлечёнными данными, "
            "использую исходный текст ответа модели", session_id)
        extracted = {"notes": extract_result["text"], "findings": []}
    results["extracted"] = extracted

    logger.info("[%s] Шаг 2: заполнение чек-листа (по блокам, всего %d)",
               session_id, len(CHECKLIST_BLOCKS))
    checklist_items = fill_checklist_parallel(session_id, extracted)
    results["checklist_items"] = checklist_items

    stats_preview = validate_checklist_fill(checklist_items)
    logger.info("[%s] Статистика заполнения чек-листа: %s",
               session_id, stats_preview)

    checklist_map = {item.get("item_id", ""): item for item in checklist_items}
    results["checklist_map"] = checklist_map

    logger.info("[%s] Шаг 3: формирование итогового акта", session_id)
    summary_input = json.dumps(checklist_items, ensure_ascii=False)
    summary_result = call_aliceai_full(
        SYSTEM_PROMPT_SUMMARY, f"Чек-лист:\n\n{summary_input}",
        temperature=0.2, max_tokens=3000)
    dump_llm_call(session_id, "summary", SYSTEM_PROMPT_SUMMARY,
                 summary_input, summary_result["text"],
                 status=summary_result["status"])
    results["summary"] = summary_result["text"]

    return results


# ============================================================
# ГЕНЕРАЦИЯ DOCX
# ============================================================

COLOR_GREEN  = RGBColor(0x27, 0xAE, 0x60)
COLOR_RED    = RGBColor(0xE7, 0x4C, 0x3C)
COLOR_ORANGE = RGBColor(0xF3, 0x9C, 0x12)
COLOR_GRAY   = RGBColor(0x95, 0xA5, 0xA6)
COLOR_WHITE  = RGBColor(0xFF, 0xFF, 0xFF)
COLOR_DARK   = RGBColor(0x2C, 0x3E, 0x50)


def set_cell_shading(cell, color_hex: str):
    shading = cell._element.get_or_add_tcPr()
    shading_elem = shading.find(qn('w:shd'))
    if shading_elem is None:
        from lxml import etree
        shading_elem = etree.SubElement(shading, qn('w:shd'))
    shading_elem.set(qn('w:fill'), color_hex)
    shading_elem.set(qn('w:val'), 'clear')


def set_cell_text(cell, text: str, bold: bool = False, size: int = 9,
                  color: RGBColor = None, alignment=None):
    cell.text = ""
    p = cell.paragraphs[0]
    if alignment:
        p.alignment = alignment
    run = p.add_run(text)
    run.font.size = Pt(size)
    run.font.name = "Arial"
    if bold:
        run.bold = True
    if color:
        run.font.color.rgb = color


def assessment_to_color(assessment: str) -> str:
    a = assessment.upper().strip()
    if a == "ДА":
        return "D5F5E3"
    elif a == "НЕТ":
        return "FADBD8"
    elif a == "Н/П":
        return "FCF3CF"
    else:
        return "F2F3F4"


def generate_checklist_docx(results: dict, recognized_text: str,
                            inspector_login: str = "") -> str:
    doc = Document()

    style = doc.styles['Normal']
    style.font.name = 'Arial'
    style.font.size = Pt(10)
    style.paragraph_format.space_after = Pt(2)

    section = doc.sections[0]
    section.page_width = Cm(21)
    section.page_height = Cm(29.7)
    section.left_margin = Cm(1.5)
    section.right_margin = Cm(1.5)
    section.top_margin = Cm(1.5)
    section.bottom_margin = Cm(1.5)

    extracted = results.get("extracted", {})
    checklist_map = results.get("checklist_map", {})
    summary = results.get("summary", "")
    now = datetime.now()

    doc.add_paragraph("")
    doc.add_paragraph("")

    title = doc.add_paragraph()
    title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = title.add_run("ЧЕК-ЛИСТ\nПРОМЫШЛЕННОЙ И ПРОИЗВОДСТВЕННОЙ\nБЕЗОПАСНОСТИ")
    run.bold = True
    run.font.size = Pt(22)
    run.font.color.rgb = COLOR_DARK

    doc.add_paragraph("")

    subtitle = doc.add_paragraph()
    subtitle.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = subtitle.add_run("Сформирован автоматически на основе голосового отчёта")
    run.font.size = Pt(12)
    run.font.color.rgb = COLOR_GRAY

    doc.add_paragraph("")
    doc.add_paragraph("")

    info_table = doc.add_table(rows=8, cols=2)
    info_table.style = 'Table Grid'
    info_table.alignment = WD_TABLE_ALIGNMENT.CENTER

    info_data = [
        ("Объект проверки", extracted.get("object_name", "—")),
        ("Адрес", extracted.get("address", "—")),
        ("Дата проверки", now.strftime("%d.%m.%Y")),
        ("Время проверки", now.strftime("%H:%M")),
        ("Проверяющий / Аудитор", extracted.get("inspector", inspector_login or "—")),
        ("Директор / Управляющий", extracted.get("director", "—")),
        ("Сотрудники на смене", extracted.get("employees_on_shift", "—")),
        ("Примечание", extracted.get("notes", "—")),
    ]

    for i, (label, value) in enumerate(info_data):
        set_cell_shading(info_table.cell(i, 0), "ECF0F1")
        set_cell_text(info_table.cell(i, 0), label, bold=True, size=10)
        set_cell_text(info_table.cell(i, 1), str(value) if value else "—", size=10)
        info_table.cell(i, 0).width = Cm(6)
        info_table.cell(i, 1).width = Cm(12)

    doc.add_page_break()

    total_checked = 0
    total_yes = 0
    total_no = 0
    total_na = 0
    total_not_checked = 0

    for block in CHECKLIST_BLOCKS:
        heading = doc.add_paragraph()
        run = heading.add_run(f"БЛОК {block['number']}. {block['title']}")
        run.bold = True
        run.font.size = Pt(12)
        run.font.color.rgb = COLOR_DARK

        num_items = len(block["items"])
        table = doc.add_table(rows=num_items + 1, cols=4)
        table.style = 'Table Grid'
        table.alignment = WD_TABLE_ALIGNMENT.CENTER

        for row in table.rows:
            row.cells[0].width = Cm(9)
            row.cells[1].width = Cm(3)
            row.cells[2].width = Cm(4)
            row.cells[3].width = Cm(2)

        headers = ["Критерий проверки", "Оценка", "Комментарий", "Срок устр."]
        for j, h in enumerate(headers):
            set_cell_shading(table.cell(0, j), "2C3E50")
            set_cell_text(table.cell(0, j), h, bold=True, size=9,
                         color=COLOR_WHITE,
                         alignment=WD_ALIGN_PARAGRAPH.CENTER)

        for i, item_text in enumerate(block["items"]):
            row_idx = i + 1
            item_id_match = re.match(r'^(\d+\.\d+)', item_text)
            item_id = item_id_match.group(1) if item_id_match else ""

            llm_data = checklist_map.get(item_id, {})
            assessment = llm_data.get("assessment", "НЕ ПРОВЕРЕНО")
            comment = llm_data.get("comment", "")
            deadline = llm_data.get("deadline", "")

            a = assessment.upper().strip()
            if a == "ДА":
                total_yes += 1
                total_checked += 1
            elif a == "НЕТ":
                total_no += 1
                total_checked += 1
            elif a == "Н/П":
                total_na += 1
                total_checked += 1
            else:
                total_not_checked += 1

            set_cell_text(table.cell(row_idx, 0), item_text, size=8)

            bg = assessment_to_color(assessment)
            set_cell_shading(table.cell(row_idx, 1), bg)
            set_cell_text(table.cell(row_idx, 1), assessment, bold=True,
                         size=9, alignment=WD_ALIGN_PARAGRAPH.CENTER)

            set_cell_text(table.cell(row_idx, 2), comment or "", size=8)

            set_cell_text(table.cell(row_idx, 3), deadline or "", size=8,
                         alignment=WD_ALIGN_PARAGRAPH.CENTER)

        doc.add_paragraph("")

    doc.add_page_break()

    heading = doc.add_paragraph()
    run = heading.add_run("ИТОГИ ПРОВЕРКИ")
    run.bold = True
    run.font.size = Pt(14)
    run.font.color.rgb = COLOR_DARK

    total_items = total_yes + total_no + total_na + total_not_checked

    stats_table = doc.add_table(rows=6, cols=2)
    stats_table.style = 'Table Grid'

    stats = [
        ("Всего пунктов", str(total_items)),
        ("Соответствует (ДА)", str(total_yes)),
        ("Не соответствует (НЕТ)", str(total_no)),
        ("Неприменимо (Н/П)", str(total_na)),
        ("Не проверено", str(total_not_checked)),
        ("Процент выполнения",
         f"{(total_yes / total_checked * 100):.0f}%" if total_checked > 0 else "—"),
    ]

    colors_stats = ["ECF0F1", "D5F5E3", "FADBD8", "FCF3CF", "F2F3F4", "D6EAF8"]
    for i, ((label, value), bg) in enumerate(zip(stats, colors_stats)):
        set_cell_shading(stats_table.cell(i, 0), "ECF0F1")
        set_cell_text(stats_table.cell(i, 0), label, bold=True, size=10)
        set_cell_shading(stats_table.cell(i, 1), bg)
        set_cell_text(stats_table.cell(i, 1), value, bold=True, size=12,
                     alignment=WD_ALIGN_PARAGRAPH.CENTER)

    doc.add_paragraph("")

    heading = doc.add_paragraph()
    run = heading.add_run("ИТОГОВЫЙ АКТ")
    run.bold = True
    run.font.size = Pt(14)
    run.font.color.rgb = COLOR_DARK

    if summary:
        for line in summary.split("\n"):
            p = doc.add_paragraph(line)
            p.paragraph_format.space_after = Pt(2)

    doc.add_page_break()

    heading = doc.add_paragraph()
    run = heading.add_run("ПРИЛОЖЕНИЕ: ИСХОДНАЯ РАСШИФРОВКА АУДИО")
    run.bold = True
    run.font.size = Pt(12)
    run.font.color.rgb = COLOR_GRAY

    p = doc.add_paragraph(recognized_text)
    p.style.font.size = Pt(9)

    doc.add_paragraph("")
    doc.add_paragraph("")

    sign_table = doc.add_table(rows=2, cols=2)
    set_cell_text(sign_table.cell(0, 0), "Проверяющий:", bold=True, size=10)
    set_cell_text(sign_table.cell(0, 1),
                  f"{extracted.get('inspector', inspector_login)} _______________",
                  size=10)
    set_cell_text(sign_table.cell(1, 0), "Дата:", bold=True, size=10)
    set_cell_text(sign_table.cell(1, 1),
                  f"{now.strftime('%d.%m.%Y')} _______________", size=10)

    date_str = now.strftime("%Y%m%d_%H%M%S")
    obj_name = extracted.get("object_name", "object")
    safe_name = re.sub(r'[^\w\s-]', '', obj_name)[:30].strip().replace(" ", "_")
    if not safe_name:
        safe_name = "checklist"

    file_path = os.path.join(
        tempfile.gettempdir(),
        f"checklist_{safe_name}_{date_str}.docx"
    )
    doc.save(file_path)
    logger.info("Документ сформирован: %s", file_path)
    return file_path


# ============================================================
# ГОЛОСОВЫЕ: ЗАБОР ГОТОВОГО ТЕКСТА / РАСПОЗНАВАНИЕ СВОИМ STT
# ============================================================

def _walk_voice_updates(u: dict, acc: list):
    """Рекурсивно собираем все элементы с voice/аудио-файлом, включая
    вложенные forwarded_messages (случай пересылки пачки голосовых)."""
    has_voice = bool(u.get("voice"))
    has_audio_file = bool(u.get("file") and is_audio_file(u["file"].get("name", "")))
    if has_voice or has_audio_file:
        acc.append(u)
    for fwd in (u.get("forwarded_messages") or []):
        _walk_voice_updates(fwd, acc)


def iter_voice_bearing_updates(update: dict) -> list:
    acc = []
    _walk_voice_updates(update, acc)
    return acc


def handle_single_voice_entry(u: dict, key: str) -> str:
    """Обрабатывает один элемент с голосовым/аудио.

    Приоритет отдаётся собственному распознаванию через SpeechKit (с
    нарезкой по паузам для длинных записей), а не встроенной расшифровке
    Мессенджера. Встроенная расшифровка — чёрный ящик без контроля
    качества: на длинных голосовых она нередко возвращает неполный текст
    без возможности понять, где и почему теряются данные. Готовый текст
    от Мессенджера используется только как запасной вариант — если файл
    не удалось скачать или собственное распознавание не сработало.
    """
    session_id = uuid.uuid4().hex[:12]
    voice = u.get("voice")
    file_name = ""
    fallback_text = ""

    try:
        if voice:
            file_info = voice.get("file", {})
            file_name = file_info.get("name", "voice.ogg")
            duration_ms = voice.get("duration")
            fallback_text = (u.get("text", "") or "").strip()
            file_id = file_info.get("id")

            transcript = ""
            source = None

            if file_id:
                try:
                    audio_bytes = get_file(file_id)
                    transcript = recognize_speech(audio_bytes, file_name, session_id)
                    source = "own_speechkit"
                except Exception as e:
                    logger.warning(
                        "[%s] Не удалось скачать или распознать аудиофайл «%s»: %s. "
                        "Использую запасной вариант — расшифровку от Мессенджера.",
                        session_id, file_name, e)
            else:
                logger.warning(
                    "[%s] В сообщении нет ссылки на файл (file_id), "
                    "доступен только текст от Мессенджера", session_id)

            if not transcript.strip():
                if fallback_text:
                    logger.info(
                        "[%s] Собственное распознавание не дало результата, "
                        "использую расшифровку от Мессенджера (%d символов). "
                        "Обращаю внимание: качество этого текста бот не контролирует.",
                        session_id, len(fallback_text))
                    transcript = fallback_text
                    source = "messenger_stt_fallback"
                else:
                    logger.warning(
                        "[%s] Расшифровка отсутствует — не удалось получить "
                        "ни собственную, ни запасную", session_id)
                    return None
        else:
            file_info = u.get("file", {})
            file_name = file_info.get("name", "")
            file_id = file_info.get("id")
            duration_ms = None
            if not file_id:
                return None
            audio_bytes = get_file(file_id)
            transcript = recognize_speech(audio_bytes, file_name, session_id)
            source = "own_speechkit"

    except Exception as e:
        logger.exception(
            "[%s] Ошибка при обработке голосового сообщения «%s»: %s",
            session_id, file_name, e)
        return None

    if not transcript or not transcript.strip():
        logger.warning("[%s] Расшифровка файла «%s» оказалась пустой",
                      session_id, file_name)
        return None

    logger.info("[%s] Расшифровка получена (источник: %s)", session_id, source)
    add_voice_item(key, source, file_name, transcript, duration_ms)
    return transcript


def handle_voice_batch(entries: list, key: str, reply_to: dict):
    """Обрабатывает пачку голосовых из одного апдейта под общим локом
    на чат, чтобы очередь не портилась при параллельных апдейтах."""
    with get_lock(key):
        ensure_chat_state(key)
        added = 0
        failed = 0
        for u in entries:
            transcript = handle_single_voice_entry(u, key)
            if transcript:
                added += 1
            else:
                failed += 1
        pending_count = count_pending(key)

    if added == 0:
        send_text(
            "Не получилось распознать ни одно из голосовых сообщений. "
            "Попробуйте записать заново — в тихом месте и ближе к микрофону.",
            **reply_to)
        return

    msg = f"Принял {voices_phrase(added)}."
    if failed:
        msg += f" Не удалось распознать: {failed}."
    msg += (
        f"\nВсего в очереди: {voices_phrase(pending_count)}.\n"
        "Можете наговорить ещё или написать «Запусти обработку», "
        "чтобы собрать чек-лист."
    )
    send_text(msg, **reply_to)


# ============================================================
# ЗАПУСК ОБРАБОТКИ
# ============================================================

def run_processing(key: str, reply_to: dict, from_login: str):
    with get_lock(key):
        items = get_pending_items(key)
        if not items:
            send_text(
                "Пока нет ни одного голосового сообщения для обработки — "
                "сначала наговорите хотя бы одно.", **reply_to)
            return

        session_id = uuid.uuid4().hex[:12]
        logger.info("[%s] Начата обработка: %s, чат %s",
                   session_id, voices_phrase(len(items)), key)

        combined_text = "\n\n".join(
            f"[Голосовое {i + 1}, источник: {it['source']}]\n{it['transcript']}"
            for i, it in enumerate(items)
        )

        send_text(f"Обрабатываю {voices_phrase(len(items))} — это может занять пару минут…",
                  **reply_to)

        try:
            results = process_with_llm(combined_text, session_id)
            stats = validate_checklist_fill(results.get("checklist_items", []))
            logger.info("[%s] Статистика заполнения чек-листа: %s",
                       session_id, stats)

            if stats["filled_total"] == 0:
                send_text(
                    "Не получилось извлечь данные из расшифровки — чек-лист "
                    "вышел пустым. Попробуйте рассказать подробнее: какой "
                    "объект, что проверяли, что нашли — и отправьте «Запусти "
                    "обработку» ещё раз. Голосовые сообщения не потеряны, они "
                    f"остались в очереди. (ID сессии для поддержки: {session_id})",
                    **reply_to)
                return

            failed_count = sum(
                1 for it in results["checklist_items"]
                if "сбоя нейросети" in (it.get("comment") or ""))
            if failed_count:
                send_text(
                    f"Не удалось обработать {failed_count} {items_word(failed_count)} "
                    "чек-листа из-за сбоя нейросети — они отмечены как «не "
                    "проверено» с пояснением в комментарии. Остальные пункты "
                    "обработаны штатно.",
                    **reply_to)

            summary = results.get("summary", "")
            if summary:
                send_long_text(f"Итоговый акт\n\n{summary}", **reply_to)

            send_text(
                f"Заполнено пунктов чек-листа: {stats['filled_total']} из "
                f"{stats['total']} (да — {stats['yes']}, нет — {stats['no']}, "
                f"неприменимо — {stats['na']}, не проверено — {stats['not_checked']})",
                **reply_to)

            send_text("Формирую документ…", **reply_to)
            docx_path = generate_checklist_docx(results, combined_text, from_login)
            send_file(docx_path, **reply_to)
            send_text("Готово — чек-лист отправлен файлом выше.", **reply_to)

            try: os.unlink(docx_path)
            except OSError: pass

            clear_session(key)

        except Exception as e:
            logger.exception("[%s] Обработка завершилась с ошибкой: %s",
                            session_id, e)
            send_text(
                f"Не получилось обработать данные из-за ошибки: {e}\n"
                f"(ID сессии для поддержки: {session_id})",
                **reply_to)


# ============================================================
# ТЕКСТОВЫЕ КОМАНДЫ И HELP
# ============================================================

def send_help(reply_to: dict):
    send_text(
        "Здравствуйте! Я помогаю оформлять акты проверки промышленной "
        "безопасности по голосовым сообщениям.\n\n"
        "Как со мной работать:\n"
        "Наговорите одно или несколько голосовых сообщений о том, что вы "
        "увидели при обходе объекта — можно частями, я всё сохраню и "
        "объединю. Когда закончите, напишите «Запусти обработку»: я "
        "расшифрую записи, разберу их по чек-листу из 21 блока и пришлю "
        "готовый акт в виде документа Word.\n\n"
        "Что мне можно сказать:\n"
        "«Запусти обработку» — собрать акт по накопленным голосовым\n"
        "«Статус» — узнать, сколько сообщений уже накопилось\n"
        "«Отмена» — очистить накопленные голосовые без обработки\n"
        "/blocks — показать список блоков чек-листа\n"
        "/help — показать эту справку",
        **reply_to)


def handle_text_command(text: str, key: str, reply_to: dict, from_login: str):
    cmd = normalize_cmd(text)

    if cmd in HELP_CMDS:
        send_help(reply_to)
        return

    if cmd in BLOCKS_CMDS:
        blocks = "\n".join(f"{b['number']}. {b['title']}" for b in CHECKLIST_BLOCKS)
        send_text(f"Чек-лист состоит из 21 блока:\n\n{blocks}", **reply_to)
        return

    if cmd in STATUS_CMDS:
        n = count_pending(key)
        send_text(
            f"Сейчас в очереди {voices_phrase(n)}.\n"
            "Напишите «Запусти обработку», чтобы собрать чек-лист, "
            "или «Отмена», чтобы очистить очередь.",
            **reply_to)
        return

    if cmd in CANCEL_CMDS:
        n = count_pending(key)
        clear_session(key)
        send_text(f"Очередь очищена: удалено {voices_phrase(n)}.", **reply_to)
        return

    if cmd in TRIGGER_CMDS:
        run_processing(key, reply_to, from_login)
        return

    send_text(
        "Отправьте голосовое сообщение с описанием обхода объекта. "
        "Когда наговорите всё нужное, напишите «Запусти обработку». "
        "Список команд — /help.",
        **reply_to)


# ============================================================
# ОБРАБОТКА ОБНОВЛЕНИЙ
# ============================================================

def resolve_reply_target(update: dict) -> dict:
    chat = update.get("chat", {})
    chat_type = chat.get("type")
    from_user = update.get("from", {})
    if chat_type in ("group", "channel"):
        return {"chat_id": chat["id"]}
    login = from_user.get("login")
    if login:
        return {"login": login}
    return {}


def process_update(update: dict):
    update_id = update.get("update_id")
    if update_id is not None:
        if is_update_processed(update_id):
            logger.info("Сообщение update_id=%s уже обработано, пропускаю",
                       update_id)
            return
        mark_update_processed(update_id)

    logger.info("Обрабатываю сообщение update_id=%s от %s",
                update_id, update.get("from", {}).get("login", "?"))

    reply_to = resolve_reply_target(update)
    if not reply_to:
        return

    key = chat_key_from_reply(reply_to)
    from_login = update.get("from", {}).get("login", "unknown")

    ensure_chat_state(key)
    first_time = not has_seen_help(key)
    if first_time:
        mark_seen_help(key)
        send_help(reply_to)

    # ─── ГОЛОСОВЫЕ / АУДИО (в т.ч. пересланные пачкой) ────────
    voice_entries = iter_voice_bearing_updates(update)
    if voice_entries:
        handle_voice_batch(voice_entries, key, reply_to)
        return

    # ─── НЕАУДИО-ФАЙЛ ──────────────────────────────────────────
    file_info = update.get("file")
    if file_info and not is_audio_file(file_info.get("name", "")):
        send_text(
            f"Файл «{file_info.get('name')}» не похож на аудиозапись — "
            "пришлите, пожалуйста, голосовое сообщение.",
            **reply_to)
        return

    # ─── ТЕКСТ / КОМАНДЫ ────────────────────────────────────────
    text = update.get("text", "")
    if text:
        handle_text_command(text, key, reply_to, from_login)
        return

    if update.get("sticker") or update.get("images"):
        if not first_time:
            send_text(
                "Я работаю только с голосовыми сообщениями — пришлите, "
                "пожалуйста, аудиозапись обхода объекта.",
                **reply_to)


# ============================================================
# WEBHOOK / POLLING / SELFTEST / MAIN
# ============================================================

@app.route("/webhook", methods=["POST"])
def webhook_handler():
    data = request.get_json(force=True)
    if not data.get("ok"):
        return jsonify({"status": "error"}), 200
    for upd in data.get("updates", []):
        try: process_update(upd)
        except Exception as e:
            logger.exception("Ошибка обработки входящего сообщения: %s", e)
    return jsonify({"status": "ok"}), 200


@app.route("/health", methods=["GET"])
def health():
    return "OK", 200


def run_polling():
    logger.info("Запускаю опрос сервера (polling)…")
    offset = 0
    while True:
        try:
            data = get_updates(offset=offset, limit=100)
            if not data.get("ok"):
                time.sleep(5)
                continue
            for upd in data.get("updates", []):
                try: process_update(upd)
                except Exception as e:
                    logger.exception("Ошибка при обработке сообщения: %s", e)
                uid = upd.get("update_id", 0)
                if uid >= offset:
                    offset = uid + 1
            if not data.get("updates"):
                time.sleep(1)
        except requests.exceptions.RequestException as e:
            logger.error("Сетевая ошибка при опросе сервера: %s", e)
            time.sleep(5)
        except Exception as e:
            logger.exception("Непредвиденная ошибка в цикле опроса: %s", e)
            time.sleep(5)


def run_selftest(sample_text: str = None):
    """Прогон пайплайна обработки без Мессенджера — быстрая проверка,
    что чек-лист реально заполняется, а не остаётся пустым."""
    init_db()
    if not sample_text:
        sample_text = (
            "Проверил объект Склад №3 по адресу ул. Ленина 10. "
            "Проверяющий Иванов, директор Петров. На смене 5 человек. "
            "Огнетушители на месте, но один просрочен, нужно заменить до 01.09. "
            "Освещение в норме. Пути эвакуации свободны. Аптечка укомплектована."
        )
    session_id = "selftest_" + uuid.uuid4().hex[:8]
    print("=== ИДЕНТИФИКАТОР СЕССИИ ===")
    print(session_id)
    print("\n=== ВХОДНОЙ ТЕКСТ ===")
    print(sample_text)

    results = process_with_llm(sample_text, session_id)
    stats = validate_checklist_fill(results.get("checklist_items", []))

    print("\n=== ИЗВЛЕЧЁННЫЕ ДАННЫЕ ===")
    print(json.dumps(results.get("extracted"), ensure_ascii=False, indent=2))

    print("\n=== СТАТИСТИКА ЗАПОЛНЕНИЯ ===")
    print(json.dumps(stats, ensure_ascii=False, indent=2))

    if stats["filled_total"] == 0:
        print("\nВНИМАНИЕ: чек-лист получился полностью пустым — "
              f"подробности смотрите в дампах: {LOG_LLM_DIR}")
    else:
        print(f"\nЗаполнено {stats['filled_total']} из {stats['total']} пунктов")
        print(f"Дампы вызовов модели: {LOG_LLM_DIR}/{session_id}_*")


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "polling"

    if mode == "selftest":
        sample = sys.argv[2] if len(sys.argv) > 2 else None
        run_selftest(sample)
    elif mode == "webhook":
        init_db()
        webhook_url = os.environ.get("WEBHOOK_URL", "https://example.com/webhook")
        set_webhook(webhook_url)
        app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
    elif mode == "polling":
        init_db()
        try: set_webhook(None)
        except: pass
        run_polling()
    else:
        print(f"Неизвестный режим запуска: {mode}. "
              "Доступные варианты: polling, webhook, selftest")
