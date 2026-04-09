### Скрипт предназначен для фильтрации аудит-лога, полученного скриптом tam-wd \ get_auditlog

import csv
import os
from datetime import datetime

# полный путь к csv-файлу с аудит-логом
AUDITLOG_PATH = r''


# FILTER — это фильтр, работающий на логических операторах. Поддерживаются операторы AND / OR / с вложенностью через словари.
# 
# Ключи словаря (например, event.meta.owner_uid) должны совпадать с заголовками столбцов csv-таблицы
#
# Листовой узел  — {"колонка": "подстрока"}           — проверка вхождения подстроки
# AND-узел       — {"AND": [узел1, узел2, ...]}       — все условия узел1, узел2 .. должны совпасть
# OR-узел        — {"OR":  [узел1, узел2, ...]}       — хотя бы одно условие из узел1, узел2 .. должно совпасть
#
# Пример:
#
# {"AND": [
#     {"event.meta.owner_uid": "1134567890123456"},
#     {"OR": [
#         {"event.meta.tgt_rawaddress": "папка"},
#         {"event.meta.src_rawaddress": "тест"},
#     ]}
# ]}
#
# Результат — события, где "event.meta.owner_uid": "1134567890123456" и "event.meta.tgt_rawaddress" содержит строку "папка" ИЛИ "event.meta.owner_uid": "1134567890123456" и "event.meta.src_rawaddress" содержит строку "тест")


# фильтр, по которому будут выбираться события аудит-лога
FILTER = {}


def _collect_columns(node: dict | list) -> list[str]:
    """Рекурсивно собирает все названия колонок из узла запроса"""
    if isinstance(node, list):
        cols = []
        for item in node:
            cols.extend(_collect_columns(item))
        return cols
    if "AND" in node:
        return _collect_columns(node["AND"])
    if "OR" in node:
        return _collect_columns(node["OR"])
    return list(node.keys())


def _match(row: dict, node: dict) -> bool:
    """Рекурсивно вычисляет узел запроса для строки CSV"""
    if "AND" in node:
        return all(_match(row, child) for child in node["AND"])
    if "OR" in node:
        return any(_match(row, child) for child in node["OR"])
    col, val = next(iter(node.items()))
    return val in (row.get(col) or '')


def filter_auditlog(query: dict) -> list[dict]:
    """Фильтрует CSV аудитлога по вложенному AND/OR-запросу"""

    matched_rows = []

    with open(AUDITLOG_PATH, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        print(f"Колонки ({len(headers)}):\n  {', '.join(headers)}\n")

        unknown_columns = [col for col in _collect_columns(query) if col not in headers]
        if unknown_columns:
            raise ValueError(f"Неизвестные колонки в фильтре: {unknown_columns}")

        for row in reader:
            if _match(row, query):
                matched_rows.append(dict(row))

    return matched_rows


def save_results(rows: list[dict]) -> str:
    """Сохраняет отфильтрованные результаты в файл"""
    output_dir = os.path.dirname(os.path.abspath(__file__))
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = os.path.join(output_dir, f'filtered_{timestamp}.csv')

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return output_path


if __name__ == '__main__':

    print(f"Применяемый фильтр:\n{FILTER}\n")
    results = filter_auditlog(FILTER)
    print(f"Найдено совпадений: {len(results)}.")

    if results:
        path = save_results(results)
        print(f"Сохранено в: {path}")
