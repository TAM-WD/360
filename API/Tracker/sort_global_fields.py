### Скрипт упорядочивает глобальные поля Трекера согласно порядку, заданному в текстовом файле.
###
###   1. Нужно подготовить текстовый файл с id глобальных полей в том порядке, который нужен в Трекере.
###      Одно поле на одну строку, id поля должен быть в конце строки
###   2. Поля должны быть внутри одной категории.
###   3. Скроипт пересчитывает параметр order так, чтобы он строго возрастал в порядке файла,
###      занимая свободные (не используемые ни одним глобальным полем) значения order,
###      за минимальное число запросов. Первое поле из файла получает минимальный order
###      категории. 
###
### Режим запуска задаётся переменной MODE в начале скрипта (или флагом --mode):
###   check  — ничего не меняет, считает план и пишет JSON с итоговым (смоделированным)
###            списком полей и отчётом о ненайденных полях. По умолчанию.
###   apply  — реально вызывает patch_global_field и в конце проверяет монотонность order.
###
### Примеры:
###   python global_fields_orderer.py
###   python global_fields_orderer.py --mode apply --threads 8 --file text.txt

import argparse
import json
import logging
import re

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# --- Конфигурация -----------------------------------------------------------

TOKEN = ''                   # OAuth токен с правами на чтение и запись в Трекере
ORGID = ''                   # ID организации
ORG_TYPE = 'X-Org-Id'        # или X-Cloud-Org-ID - тип организации (Яндекс 360 / Cloud Organization)

INPUT_FILENAME = 'file.txt'  # имя текстового файла с порядком полей (рядом со скриптом или абсолютный путь)
MODE = 'check'               # 'check' — только смоделировать и записать JSON; 'apply' — применить изменения

DEFAULT_THREADS = 5          # число потоков по умолчанию

headers = {ORG_TYPE: ORGID, 'Authorization': f"OAuth {TOKEN}"}

logging.basicConfig(
    level='INFO',
    format="[%(asctime)s] %(levelname)-2s %(filename)s:%(lineno)d - %(name)s - %(message)s",
)
log = logging.getLogger(__name__)

# Сессия с ретраями (общий для проекта паттерн).
_session = requests.Session()
_session.mount(
    'https://',
    HTTPAdapter(max_retries=Retry(total=5, backoff_factor=1, status_forcelist=[429, 502, 503, 504])),
)

# id поля — последний латинский токен строки (camelCase допускается).
_ID_RE = re.compile(r'([A-Za-z][A-Za-z0-9_]*)\s*$')


# --- HTTP -------------------------------------------------------------------

def get_global_fields() -> Optional[List[Dict[str, Any]]]:
    url = 'https://api.tracker.yandex.net/v3/fields'
    try:
        response = _session.get(url, headers=headers)
        return response.json()
    except Exception as e:
        log.error(f'Got exception while getting global fields: {e}')
        return None


def patch_global_field(id: str, version: int, body: Dict[str, Any]) -> Tuple[Optional[int], Optional[Dict[str, Any]]]:
    """Патчит глобальное поле. Возвращает (http_status, тело_ответа)."""
    url = f"https://api.tracker.yandex.net/v3/fields/{id}"
    params = {'version': version}
    response = None
    try:
        response = _session.patch(url, params=params, headers=headers, json=body)
        return response.status_code, response.json()
    except Exception as e:
        status = response.status_code if response is not None else None
        if status == 500:
            log.info(f"Got http.500 while patching global field: {id}")
        else:
            log.error(f'Got exception while patching global field {id}: {e}')
        return status, None


# --- Парсинг файла ----------------------------------------------------------

def read_text(path: Path) -> str:
    """Читает текстовый файл, поддерживая кодировки macOS (utf-8) и Windows (cp1251)."""
    for encoding in ('utf-8-sig', 'utf-8', 'cp1251'):
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    # Последняя попытка — читаем максимально терпимо.
    return path.read_text(encoding='utf-8', errors='replace')


def parse_field_ids(text: str) -> List[str]:
    """Достаёт id полей (последний латинский токен строки) с сохранением порядка и без дублей."""
    ids: List[str] = []
    seen = set()
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        match = _ID_RE.search(stripped)
        if not match:
            continue
        field_id = match.group(1)
        if field_id in seen:
            continue
        seen.add(field_id)
        ids.append(field_id)
    return ids


# --- Поиск полей ------------------------------------------------------------

def _field_matches(field: Dict[str, Any], field_id: str) -> bool:
    """Сопоставляет id из файла с глобальным полем: по id, по key или по суффиксу '<org>--<key>'."""
    raw_id = str(field.get('id', ''))
    return (
        raw_id == field_id
        or field.get('key') == field_id
        or raw_id.split('--')[-1] == field_id
    )


def find_fields(
    global_fields: List[Dict[str, Any]],
    file_ids: List[str],
) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Возвращает (найденные поля в порядке файла, список ненайденных id)."""
    found: List[Dict[str, Any]] = []
    not_found: List[str] = []
    for field_id in file_ids:
        match = next((f for f in global_fields if _field_matches(f, field_id)), None)
        if match is None:
            not_found.append(field_id)
        else:
            found.append(match)
    return found, not_found


def _category_id(field: Dict[str, Any]) -> Optional[str]:
    category = field.get('category') or {}
    return category.get('id') if isinstance(category, dict) else None


# --- Планирование order -----------------------------------------------------

def _next_free(start: int, occupied: set, taken: set) -> int:
    """Наименьший order >= start, не занятый ни одним полем и ещё не выбранный в плане."""
    value = start
    while value in occupied or value in taken:
        value += 1
    return value


def build_plan(
    global_fields: List[Dict[str, Any]],
    found_fields: List[Dict[str, Any]],
) -> Tuple[str, List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Считает целевые значения order для полей из файла.

    Возвращает (category_id, plan, category_mismatch), где plan — список словарей
    {field, id, name, version, current_order, target_order, changed, is_anchor}
    в порядке файла, а category_mismatch — поля, чей category.id отличается от первого.

    Гарантии плана:
      * order строго возрастает в порядке файла;
      * первое поле получает минимальный order категории (anchor); если этот
        order не уникален по ВСЕМ глобальным полям и останется занятым даже
        после освобождения внутри категории, anchor берётся как минимальный
        свободный order среди всех глобальных полей и полей категории;
      * остальные поля занимают свободные order (минимальное число изменений);
      * новые order не пересекаются с уже занятыми у любых глобальных полей
        (уникальность проверяется по ВСЕМ глобальным полям, а не только внутри
        категории), поэтому патчи безопасны для параллельного выполнения.
    """
    if not found_fields:
        return '', [], []

    target_category = _category_id(found_fields[0])

    # Поля файла, относящиеся к целевой категории, и «лишние» (с другой категорией).
    in_category: List[Dict[str, Any]] = []
    category_mismatch: List[Dict[str, Any]] = []
    for field in found_fields:
        if _category_id(field) == target_category:
            in_category.append(field)
        else:
            category_mismatch.append(field)

    if not in_category:
        return target_category or '', [], category_mismatch

    # Все поля целевой категории и все занятые значения order по всем глобальным полям.
    category_fields = [f for f in global_fields if _category_id(f) == target_category]
    occupied = {f['order'] for f in global_fields if isinstance(f.get('order'), int)}

    category_orders = [f['order'] for f in category_fields if isinstance(f.get('order'), int)]
    category_min = min(category_orders) if category_orders else 0

    # in_category — единственные поля, которые скрипт патчит, поэтому их текущие
    # order можно переиспользовать (они освободятся). Order всех ОСТАЛЬНЫХ глобальных
    # полей защищён — его нельзя назначить никому (уникальность по ВСЕМ полям).
    in_category_ids = {f.get('id') for f in in_category}
    protected = {
        f['order'] for f in global_fields
        if isinstance(f.get('order'), int) and f.get('id') not in in_category_ids
    }

    # Anchor — минимальный order категории, если им владеет поле из файла; иначе
    # минимум среди полей файла (поле вне файла трогать нельзя).
    found_orders = [f['order'] for f in in_category if isinstance(f.get('order'), int)]
    min_holder = next((f for f in category_fields if f.get('order') == category_min), None)
    if min_holder is not None and any(min_holder is f for f in in_category):
        anchor = category_min
    else:
        anchor = min(found_orders) if found_orders else category_min
        if category_min != anchor:
            log.warning(
                "Минимальный order категории (%s) принадлежит полю вне файла — "
                "первое поле файла будет установлено на %s.", category_min, anchor,
            )

    # Уникальность anchor по ВСЕМ глобальным полям. anchor занимает существующее
    # значение order, которое должен освободить его владелец из файла. Если этим
    # значением владеет ещё и поле вне плана (другая категория, дубль order), то
    # после освобождения внутри категории оно останется занятым — берём минимальный
    # свободный order среди всех глобальных полей и полей категории.
    if anchor in protected:
        blockers = [
            f for f in global_fields
            if f.get('order') == anchor and f.get('id') not in in_category_ids
        ]
        new_anchor = _next_free(1, protected, set())
        log.warning(
            "order=%s для первого поля файла занят полем(ями) вне плана (%s) и не "
            "освободится — выбран минимальный свободный order=%s.",
            anchor,
            ', '.join(f"{b.get('id')} ({b.get('name')})" for b in blockers),
            new_anchor,
        )
        anchor = new_anchor

    plan: List[Dict[str, Any]] = []
    taken: set = set()

    def add(field: Dict[str, Any], target: int, is_anchor: bool) -> None:
        current = field.get('order')
        taken.add(target)
        plan.append({
            'field': field,
            'id': field.get('id'),
            'name': field.get('name'),
            'version': field.get('version'),
            'current_order': current,
            'target_order': target,
            'changed': current != target,
            'is_anchor': is_anchor,
        })

    # Первое поле -> anchor.
    add(in_category[0], anchor, is_anchor=True)
    prev = anchor

    # Остальные поля: сохраняем текущий order, если он уже валиден (возрастает и уникален),
    # иначе занимаем ближайший свободный — это минимизирует число патчей.
    for field in in_category[1:]:
        current = field.get('order')
        if isinstance(current, int) and current > prev and current not in taken:
            target = current
        else:
            target = _next_free(prev + 1, occupied, taken)
        add(field, target, is_anchor=False)
        prev = target

    return target_category or '', plan, category_mismatch


# --- Применение плана -------------------------------------------------------

def apply_plan(plan: List[Dict[str, Any]], threads: int) -> List[Dict[str, Any]]:
    """
    Применяет план патчами в несколько потоков.

    Сначала параллельно патчатся все НЕ-anchor поля (их target — свободные order),
    затем anchor — после того как его старый владелец освободил минимальный order.
    """
    results: List[Dict[str, Any]] = []
    changed = [step for step in plan if step['changed']]

    def patch_step(step: Dict[str, Any]) -> Dict[str, Any]:
        status, response = patch_global_field(
            id=step['id'],
            version=step['version'],
            body={'order': step['target_order']},
        )
        # http.500 у Трекера на patch полей считаем успехом ("ок 500").
        is_500 = status == 500
        ok = is_500 or (
            status is not None and 200 <= status < 300
            and not (isinstance(response, dict) and 'errors' in response)
        )
        label = 'ок 500' if is_500 else ('OK  ' if ok else 'FAIL')
        log.info(
            "%s %s: order %s -> %s",
            label,
            step['id'], step['current_order'], step['target_order'],
        )
        return {'id': step['id'], 'target_order': step['target_order'],
                'ok': ok, 'status': status, 'response': response}

    phase_a = [s for s in changed if not s['is_anchor']]
    phase_b = [s for s in changed if s['is_anchor']]

    # Фаза A — освобождаем минимальный order и расставляем остальные поля параллельно.
    if phase_a:
        with ThreadPoolExecutor(max_workers=max(1, threads)) as pool:
            results.extend(pool.map(patch_step, phase_a))

    # Фаза B — устанавливаем anchor (минимальный order) на первое поле файла.
    for step in phase_b:
        results.append(patch_step(step))

    return results


# --- Проверки и вывод -------------------------------------------------------

def verify_order(file_ids: List[str]) -> bool:
    """Заново запрашивает поля и проверяет, что order строго возрастает в порядке файла."""
    fields = get_global_fields()
    if not fields:
        log.error("Не удалось получить поля для проверки.")
        return False

    found, not_found = find_fields(fields, file_ids)
    if not_found:
        log.warning("При проверке не найдены поля: %s", ', '.join(not_found))

    ok = True
    prev_order = None
    prev_id = None
    for field in found:
        order = field.get('order')
        if prev_order is not None and not (isinstance(order, int) and order > prev_order):
            log.error("Порядок нарушен: %s (order=%s) не больше %s (order=%s)",
                      field.get('id'), order, prev_id, prev_order)
            ok = False
        prev_order, prev_id = order, field.get('id')

    log.info("Проверка монотонности order: %s", 'ПРОЙДЕНА' if ok else 'НЕ ПРОЙДЕНА')
    return ok


def simulate_result(
    global_fields: List[Dict[str, Any]],
    plan: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Возвращает копию списка полей с применённым планом (для режима check)."""
    simulated = deepcopy(global_fields)
    target_by_id = {step['id']: step['target_order'] for step in plan}
    for field in simulated:
        if field.get('id') in target_by_id:
            field['order'] = target_by_id[field['id']]
    simulated.sort(key=lambda f: (f.get('order') is None, f.get('order')))
    return simulated


def write_check_report(
    out_dir: Path,
    global_fields: List[Dict[str, Any]],
    file_ids: List[str],
    plan: List[Dict[str, Any]],
    category_id: str,
    not_found: List[str],
    category_mismatch: List[Dict[str, Any]],
) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    out_path = out_dir / f'global_fields_orderer_check_{timestamp}.json'

    report = {
        'mode': 'check',
        'category_id': category_id,
        'file_order': file_ids,
        'not_found_in_get_global_fields': not_found,
        'category_mismatch': [
            {'id': f.get('id'), 'name': f.get('name'), 'category_id': _category_id(f)}
            for f in category_mismatch
        ],
        'plan': [
            {
                'id': step['id'],
                'name': step['name'],
                'old_order': step['current_order'],
                'new_order': step['target_order'],
                'changed': step['changed'],
            }
            for step in plan
        ],
        'resulting_global_fields': simulate_result(global_fields, plan),
    }

    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return out_path


# --- main -------------------------------------------------------------------

def resolve_input_path(filename: str) -> Path:
    path = Path(filename)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description='Упорядочивание глобальных полей Трекера по текстовому файлу.')
    parser.add_argument('--mode', choices=['check', 'apply'], default=MODE,
                        help='check — только смоделировать и записать JSON; apply — применить изменения. '
                             'По умолчанию берётся из переменной MODE в начале скрипта.')
    parser.add_argument('--threads', type=int, default=DEFAULT_THREADS, help='Число потоков (по умолчанию 5).')
    parser.add_argument('--file', default=INPUT_FILENAME, help='Имя/путь текстового файла с порядком полей.')
    args = parser.parse_args()

    input_path = resolve_input_path(args.file)
    if not input_path.exists():
        log.error("Файл не найден: %s", input_path)
        return

    file_ids = parse_field_ids(read_text(input_path))
    if not file_ids:
        log.error("В файле не найдено ни одного id поля: %s", input_path)
        return
    log.info("Из файла прочитано %d id полей: %s", len(file_ids), ', '.join(file_ids))

    global_fields = get_global_fields()
    if not global_fields:
        log.error("Не удалось получить список глобальных полей.")
        return

    found_fields, not_found = find_fields(global_fields, file_ids)
    if not_found:
        log.warning("Не найдены в get_global_fields: %s", ', '.join(not_found))
    if not found_fields:
        log.error("Ни одно поле из файла не найдено среди глобальных полей.")
        return

    category_id, plan, category_mismatch = build_plan(global_fields, found_fields)
    if category_mismatch:
        log.warning(
            "Поля с другим category.id (исключены из упорядочивания): %s",
            ', '.join(str(f.get('id')) for f in category_mismatch),
        )

    log.info("Целевая категория: %s, полей к упорядочиванию: %d, патчей: %d",
             category_id, len(plan), sum(1 for s in plan if s['changed']))
    for step in plan:
        log.info("  %s %s: %s -> %s%s",
                 '[anchor]' if step['is_anchor'] else '       ',
                 step['id'], step['current_order'], step['target_order'],
                 '' if step['changed'] else ' (без изменений)')

    if args.mode == 'check':
        out_path = write_check_report(
            Path(__file__).resolve().parent / 'results',
            global_fields, file_ids, plan, category_id, not_found, category_mismatch,
        )
        log.info("Режим check — изменения не применялись. Отчёт: %s", out_path)
        return

    # mode == apply
    apply_plan(plan, args.threads)
    verify_order([f.get('id') for f in found_fields if _category_id(f) == category_id])


if __name__ == '__main__':
    main()
