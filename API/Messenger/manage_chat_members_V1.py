"""
Скрипт для управления участниками чата или канала Яндекс Мессенджера.
Версия 3: поддержка CSV/XLSX файлов, разных разделителей, сохранение токена между циклами.

Использует Bot API метод POST /bot/v1/chats/updateMembers/
Справка: https://yandex.ru/dev/messenger/doc/ru/api-requests/chat-members

Возможности:
  1. Добавление пользователей в чат (роль: members - не более 500 за раз или admins - не более 100 за раз)
  2. Добавление подписчиков в канал (роль: subscribers)
  3. Удаление пользователей из чата или канала

Ввод пользователей:
  — Вручную через запятую (или другой разделитель)
  — Из файла .csv или .xlsx (первый столбец — логины-почты)
"""

import csv
import getpass
import os
import sys

import requests

# Попытка импортировать openpyxl для поддержки .xlsx
try:
    import openpyxl
    XLSX_SUPPORTED = True
except ImportError:
    XLSX_SUPPORTED = False


API_URL = "https://botapi.messenger.yandex.net/bot/v1/chats/updateMembers/"


def clear_screen():
    """Очищает экран консоли."""
    os.system("cls" if os.name == "nt" else "clear")


def print_greeting():
    """Выводит приветствие и инструкцию по работе со скриптом."""
    clear_screen()
    print("=" * 70)
    print("  Скрипт управления участниками чатов и каналов в Яндекс Мессенджере  ")
    print("=" * 70)
    print()
    print("Этот скрипт позволяет:")
    print("  1 — Добавить пользователей в чат (members или admins)")
    print("  2 — Добавить подписчиков в канал (subscribers)")
    print("  3 — Удалить пользователей из чата или канала")
    print()
    print("Ввод пользователей:")
    print("  — Вручную: логины-почты через разделитель (запятая, точка с запятой, пробел)")
    print("  — Из файла: .csv или .xlsx (логины-почты в первом столбце)")
    print()
    print("Для работы вам понадобятся:")
    print("  • OAuth-токен бота, который является админом чата/канала")
    print("  • ID чата или канала (например: 0/0/xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx)")
    print("  • Логины-почты пользователей")
    if not XLSX_SUPPORTED:
        print()
        print("  ⚠ Библиотека openpyxl не установлена — .xlsx файлы не поддерживаются.")
        print("    Установите: pip install openpyxl")
    print()
    print("-" * 70)
    print()


# ---------------------------------------------------------------------------
#  Ввод данных
# ---------------------------------------------------------------------------

def get_token():
    """Запрашивает у пользователя OAuth-токен бота. Возвращает None при ошибке."""
    print("Вставьте OAuth-токен бота-админа чата или канала: ", end="", flush=True)
    token = getpass.getpass(prompt="").strip()
    if not token:
        print("Ошибка: OAuth-токен не может быть пустым.")
        return None
    print(f"Токен принят: xxxxx")
    return token


def get_chat_id():
    """Запрашивает у пользователя ID чата или канала. Возвращает None при ошибке."""
    chat_id = input("Вставьте ID чата или канала: ").strip()
    if not chat_id:
        print("Ошибка: ID чата или канала не может быть пустым.")
        return None
    return chat_id


def get_action():
    """Запрашивает у пользователя выбор действия. Возвращает None при ошибке."""
    print()
    print("Что нужно сделать?")
    print("  1 — Добавить пользователей в чат")
    print("  2 — Добавить подписчиков в канал")
    print("  3 — Удалить пользователей из чата или канала")
    print()
    choice = input("Введите цифру (1, 2 или 3): ").strip()
    if choice not in ("1", "2", "3"):
        print("Ошибка: неверный выбор. Введите 1, 2 или 3.")
        return None
    return choice


def get_role_for_chat():
    """Запрашивает роль для добавления в чат. Возвращает None при ошибке."""
    print()
    print("С какой ролью добавить пользователей в чат?")
    print("  members — обычные участники")
    print("  admins  — администраторы")
    print()
    role = input("Введите роль (members или admins): ").strip().lower()
    if role not in ("members", "admins"):
        print("Ошибка: неверная роль. Введите members или admins.")
        return None
    return role


# ---------------------------------------------------------------------------
#  Чтение пользователей: ручной ввод, CSV, XLSX
# ---------------------------------------------------------------------------

def detect_delimiter(line):
    """
    Определяет разделитель в строке.
    Проверяет: точку с запятой, запятую, табуляцию, пробел.
    """
    for delim in (";", ",", "\t", " "):
        if delim in line:
            return delim
    return ","


def read_logins_from_csv(filepath):
    """
    Читает логины из CSV-файла (первый столбец).
    Автоматически определяет разделитель.
    Возвращает список логинов или None при ошибке.
    """
    logins = []
    try:
        with open(filepath, "r", encoding="utf-8-sig") as f:
            # Прочитаем первую строку для определения разделителя
            first_line = f.readline()
            if not first_line.strip():
                print("Ошибка: файл пуст.")
                return None
            delimiter = detect_delimiter(first_line)
            f.seek(0)

            reader = csv.reader(f, delimiter=delimiter)
            for row in reader:
                if row:
                    value = row[0].strip()
                    if value:
                        logins.append(value)
    except FileNotFoundError:
        print(f"Ошибка: файл не найден — {filepath}")
        return None
    except Exception as exc:
        print(f"Ошибка при чтении CSV: {exc}")
        return None

    if not logins:
        print("Ошибка: в файле не найдено ни одного логина.")
        return None
    return logins


def read_logins_from_xlsx(filepath):
    """
    Читает логины из XLSX-файла (первый столбец первого листа).
    Возвращает список логинов или None при ошибке.
    """
    if not XLSX_SUPPORTED:
        print("Ошибка: для чтения .xlsx файлов необходима библиотека openpyxl.")
        print("Установите: pip install openpyxl")
        return None

    logins = []
    try:
        wb = openpyxl.load_workbook(filepath, read_only=True, data_only=True)
        ws = wb.active
        for row in ws.iter_rows(min_col=1, max_col=1, values_only=True):
            value = row[0]
            if value is not None:
                value = str(value).strip()
                if value:
                    logins.append(value)
        wb.close()
    except FileNotFoundError:
        print(f"Ошибка: файл не найден — {filepath}")
        return None
    except Exception as exc:
        print(f"Ошибка при чтении XLSX: {exc}")
        return None

    if not logins:
        print("Ошибка: в файле не найдено ни одного логина.")
        return None
    return logins


def parse_manual_input(raw):
    """
    Разбирает строку с логинами, автоматически определяя разделитель.
    Возвращает список логинов или None при ошибке.
    """
    if not raw:
        print("Ошибка: список пользователей не может быть пустым.")
        return None
    delimiter = detect_delimiter(raw)
    logins = [login.strip() for login in raw.split(delimiter) if login.strip()]
    if not logins:
        print("Ошибка: не удалось распознать ни одного логина.")
        return None
    return logins


def get_users():
    """
    Запрашивает список пользователей: ручной ввод или путь к файлу.
    Возвращает список логинов или None при ошибке.
    """
    print()
    print("Как ввести пользователей?")
    print("  1 — Ввести вручную (логины через разделитель)")
    print("  2 — Загрузить из файла (.csv или .xlsx)")
    print()
    method = input("Введите цифру (1 или 2): ").strip()

    if method == "2":
        filepath = input("Введите путь к файлу: ").strip().strip('"').strip("'")
        if not filepath:
            print("Ошибка: путь к файлу не может быть пустым.")
            return None

        ext = os.path.splitext(filepath)[1].lower()
        if ext == ".csv":
            return read_logins_from_csv(filepath)
        elif ext in (".xlsx",):
            return read_logins_from_xlsx(filepath)
        else:
            print(f"Ошибка: неподдерживаемый формат файла «{ext}». Используйте .csv или .xlsx.")
            return None
    else:
        # Ручной ввод (по умолчанию, если ввели не "2")
        raw = input("Введите логины-почты пользователей через разделитель: ").strip()
        return parse_manual_input(raw)


# ---------------------------------------------------------------------------
#  API
# ---------------------------------------------------------------------------

def build_payload(chat_id, action, logins, role=None):
    """
    Формирует тело запроса для API.

    Параметры:
      chat_id — ID чата/канала
      action  — "1" (добавить в чат), "2" (подписчики канала), "3" (удалить)
      logins  — список логинов-почт
      role    — "members" или "admins" (только для action="1")
    """
    users_list = [{"login": login} for login in logins]

    payload = {"chat_id": chat_id}

    if action == "1":
        payload[role] = users_list
    elif action == "2":
        payload["subscribers"] = users_list
    elif action == "3":
        payload["remove"] = users_list

    return payload


def send_request(token, payload):
    """
    Отправляет POST-запрос к API updateMembers.

    Возвращает кортеж (status_code, response_json).
    """
    headers = {
        "Authorization": f"OAuth {token}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(API_URL, json=payload, headers=headers, timeout=60)
        return response.status_code, response.json()
    except requests.exceptions.ConnectionError:
        return None, {"error": "Не удалось подключиться к серверу. Проверьте сетевое соединение."}
    except requests.exceptions.Timeout:
        return None, {"error": "Превышено время ожидания ответа от сервера (60 сек)."}
    except requests.exceptions.RequestException as exc:
        return None, {"error": f"Ошибка при выполнении запроса: {exc}"}
    except ValueError:
        return response.status_code, {"error": "Сервер вернул ответ, который не является JSON."}


# ---------------------------------------------------------------------------
#  Отчёт
# ---------------------------------------------------------------------------

def print_report(action, role, logins, status_code, response_body):
    """Выводит итоговый отчёт о выполненной операции. Возвращает True если операция успешна."""
    print()
    print("=" * 70)
    print("  ОТЧЁТ")
    print("=" * 70)

    # Описание действия
    if action == "1":
        action_desc = f"Добавление пользователей в чат с ролью {role}"
    elif action == "2":
        action_desc = "Добавление подписчиков в канал с ролью subscribers"
    else:
        action_desc = "Удаление пользователей из чата/канала"

    print(f"  Действие: {action_desc}")
    print(f"  Количество пользователей: {len(logins)}")
    print(f"  Пользователи: {', '.join(logins)}")
    print()

    success = False
    if status_code is None:
        print(f"  ❌ ОШИБКА: {response_body.get('error', 'Неизвестная ошибка')}")
    elif status_code == 200 and response_body.get("ok") is True:
        print(f"  ✅ УСПЕХ! Операция выполнена успешно.")
        print(f"  HTTP-статус: {status_code}")
        success = True
    else:
        print(f"  ❌ ОШИБКА при выполнении операции.")
        print(f"  HTTP-статус: {status_code}")
        if "description" in response_body:
            print(f"  Описание: {response_body['description']}")
        if "code" in response_body:
            print(f"  Код ошибки: {response_body['code']}")
        if "error" in response_body:
            print(f"  Детали: {response_body['error']}")

    print()
    print("=" * 70)
    return success


# ---------------------------------------------------------------------------
#  Основной цикл
# ---------------------------------------------------------------------------

def ask_continue(action):
    """
    Спрашивает пользователя, что делать дальше.
    Возвращает:
      "same"  — продолжить работу с тем же чатом/каналом
      "other" — ввести новый ID чата/канала
    """
    print()
    if action == "2":
        entity = "каналом"
    else:
        entity = "чатом"

    print(f"Желаете продолжить работу с этим же {entity}?")
    print(f"  1 — Да, продолжить работу с этим {entity}")
    print(f"  2 — Нет, работать с другим чатом или каналом")
    print()
    choice = input("Введите цифру (1 или 2): ").strip()
    if choice == "2":
        return "other"
    return "same"


def main():
    print_greeting()

    # --- Шаг 1: Токен (вводится один раз) ---
    token = get_token()
    if token is None:
        input("\nНажмите Enter для перезапуска...")
        main()
        return

    # --- Шаг 2: ID чата/канала ---
    print()
    chat_id = get_chat_id()
    if chat_id is None:
        input("\nНажмите Enter для перезапуска...")
        main()
        return

    # --- Основной цикл работы ---
    while True:
        action = get_action()
        if action is None:
            input("\nНажмите Enter чтобы попробовать снова...")
            continue

        role = None
        if action == "1":
            role = get_role_for_chat()
            if role is None:
                input("\nНажмите Enter чтобы попробовать снова...")
                continue
        elif action == "2":
            role = "subscribers"

        logins = get_users()
        if logins is None:
            input("\nНажмите Enter чтобы попробовать снова...")
            continue

        payload = build_payload(chat_id, action, logins, role)

        print()
        print("Выполняю запрос к API...")
        status_code, response_body = send_request(token, payload)

        print_report(action, role, logins, status_code, response_body)

        # --- Вопрос о продолжении ---
        next_step = ask_continue(action)

        if next_step == "same":
            # Продолжаем с тем же chat_id — цикл повторится
            continue
        else:
            # Ввод нового ID чата/канала
            print()
            new_chat_id = get_chat_id()
            if new_chat_id is None:
                input("\nНажмите Enter чтобы попробовать снова...")
                continue
            chat_id = new_chat_id


if __name__ == "__main__":
    main()
