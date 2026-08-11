from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone

log = logging.getLogger("runs")

RUN_DIR_RE = re.compile(r"^(analyze|validate|run)-(\d{8})-(\d{6})Z(?:_(.+))?$")
MANIFEST = "manifest.json"
LATEST_LINK = "latest"
COMPARE_DIR = "_compare"


@dataclass
class RunInfo:
    run_id: str
    path: str
    command: str = "analyze"
    started_at: str = ""
    finished_at: str | None = None
    status: str = "running"
    tag: str | None = None
    manifest: dict = field(default_factory=dict)

    @property
    def dt(self) -> datetime:
        match = RUN_DIR_RE.match(self.run_id)
        if not match:
            return datetime.min.replace(tzinfo=timezone.utc)
        return datetime.strptime(f"{match.group(2)}{match.group(3)}",
                                 "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)

    @property
    def metrics(self) -> dict:
        return self.manifest.get("metrics") or {}

    @property
    def flags(self) -> dict:
        return self.manifest.get("flags") or {}


# ---------------------------------------------------------------- создание
def make_run_id(command: str, tag: str | None = None,
                now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    stamp = now.strftime("%Y%m%d-%H%M%S")
    safe_tag = re.sub(r"[^\w.-]+", "-", tag).strip("-") if tag else None
    return f"{command}-{stamp}Z" + (f"_{safe_tag}" if safe_tag else "")


def create_run_dir(results_dir: str, command: str = "analyze",
                   tag: str | None = None) -> RunInfo:
    os.makedirs(results_dir, exist_ok=True)
    now = datetime.now(timezone.utc)
    run_id = make_run_id(command, tag, now)
    path = os.path.join(results_dir, run_id)
    suffix = 1
    while os.path.exists(path):              # два прогона в одну секунду
        path = os.path.join(results_dir, f"{run_id}-{suffix}")
        suffix += 1
    os.makedirs(path)
    run = RunInfo(run_id=os.path.basename(path), path=path, command=command,
                  started_at=now.isoformat(), tag=tag)
    log.info("Каталог прогона: %s", os.path.abspath(path))
    return run


def file_fingerprint(path: str | None) -> dict | None:
    """Отпечаток входного файла — чтобы понимать, менялась ли таблица."""
    if not path or not os.path.isfile(path):
        return None
    digest = hashlib.sha1()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    stat = os.stat(path)
    return {"path": os.path.abspath(path), "sha1": digest.hexdigest()[:16],
            "size": stat.st_size,
            "mtime": datetime.fromtimestamp(stat.st_mtime,
                                            timezone.utc).isoformat()}


def sanitize_config(cfg) -> dict:
    """Снимок конфигурации без токенов."""
    data = {}
    for key, value in vars(cfg).items():
        if "token" in key.lower():
            continue
        data[key] = value
    return data


def write_manifest(run: RunInfo, *, cfg=None, status: str = "ok",
                   metrics: dict | None = None, flags: dict | None = None,
                   inputs: dict | None = None, db: dict | None = None,
                   errors: list | None = None) -> None:
    run.finished_at = datetime.now(timezone.utc).isoformat()
    run.status = status
    run.manifest = {
        "run_id": run.run_id,
        "command": run.command,
        "tag": run.tag,
        "started_at": run.started_at,
        "finished_at": run.finished_at,
        "status": status,
        "flags": flags or {},
        "inputs": inputs or {},
        "db": db or {},
        "metrics": metrics or {},
        "errors": errors or [],
        "config": sanitize_config(cfg) if cfg else {},
    }
    with open(os.path.join(run.path, MANIFEST), "w", encoding="utf-8") as handle:
        json.dump(run.manifest, handle, ensure_ascii=False, indent=2)


def update_latest(results_dir: str, run: RunInfo) -> None:
    """Симлинк result/latest. На системах без симлинков — текстовый файл."""
    link = os.path.join(results_dir, LATEST_LINK)
    try:
        if os.path.islink(link) or os.path.exists(link):
            if os.path.islink(link):
                os.unlink(link)
            elif os.path.isfile(link):
                os.remove(link)
        os.symlink(run.run_id, link)
    except OSError:
        with open(os.path.join(results_dir, "latest.txt"), "w",
                  encoding="utf-8") as handle:
            handle.write(run.run_id + "\n")


# ---------------------------------------------------------------- чтение
def load_run(path: str) -> RunInfo | None:
    run_id = os.path.basename(os.path.normpath(path))
    if not os.path.isdir(path):
        return None
    manifest: dict = {}
    manifest_path = os.path.join(path, MANIFEST)
    if os.path.isfile(manifest_path):
        try:
            with open(manifest_path, "r", encoding="utf-8") as handle:
                manifest = json.load(handle)
        except (json.JSONDecodeError, OSError):
            manifest = {}
    return RunInfo(
        run_id=run_id, path=path,
        command=manifest.get("command", _command_from_id(run_id)),
        started_at=manifest.get("started_at", ""),
        finished_at=manifest.get("finished_at"),
        status=manifest.get("status", "unknown"),
        tag=manifest.get("tag"), manifest=manifest)


def _command_from_id(run_id: str) -> str:
    match = RUN_DIR_RE.match(run_id)
    return match.group(1) if match else "unknown"


def list_runs(results_dir: str, command: str | None = None,
              only_ok: bool = False) -> list[RunInfo]:
    """Прогоны по возрастанию времени."""
    if not os.path.isdir(results_dir):
        return []
    runs: list[RunInfo] = []
    for entry in sorted(os.listdir(results_dir)):
        if entry in (LATEST_LINK, COMPARE_DIR, "latest.txt"):
            continue
        full = os.path.join(results_dir, entry)
        if os.path.islink(full) or not os.path.isdir(full):
            continue
        if not RUN_DIR_RE.match(entry):
            continue
        run = load_run(full)
        if run is None:
            continue
        if command and run.command != command:
            continue
        if only_ok and run.status != "ok":
            continue
        runs.append(run)
    runs.sort(key=lambda r: (r.dt, r.run_id))
    return runs


def resolve_run(results_dir: str, ref: str, *, command: str = "analyze") -> RunInfo:
    """Поддерживает: latest, prev, -1/-2, точное имя каталога, путь."""
    ref = (ref or "").strip()
    runs = list_runs(results_dir, command=command)

    if ref in ("latest", "last"):
        if not runs:
            raise SystemExit(f"В {results_dir} нет прогонов '{command}'")
        return runs[-1]
    if ref in ("prev", "previous"):
        if len(runs) < 2:
            raise SystemExit("Нужно минимум два прогона для 'prev'")
        return runs[-2]
    if re.fullmatch(r"-\d+", ref):
        index = int(ref)
        if len(runs) < abs(index):
            raise SystemExit(f"Прогонов всего {len(runs)}, запрошен {ref}")
        return runs[index]

    candidate = os.path.join(results_dir, ref)
    if os.path.isdir(candidate):
        run = load_run(candidate)
        if run:
            return run
    if os.path.isdir(ref):
        run = load_run(ref)
        if run:
            return run

    matches = [r for r in runs if r.run_id.startswith(ref)]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        names = ", ".join(r.run_id for r in matches[:5])
        raise SystemExit(f"Неоднозначная ссылка {ref!r}: {names}")
    raise SystemExit(f"Прогон не найден: {ref!r}. "
                     f"Смотрите 'python cli.py runs'")


def prune_runs(results_dir: str, keep: int, command: str = "analyze") -> list[str]:
    """Удаляет старые прогоны, оставляя keep последних."""
    if keep <= 0:
        return []
    runs = list_runs(results_dir, command=command)
    removed: list[str] = []
    for run in runs[:-keep]:
        shutil.rmtree(run.path, ignore_errors=True)
        removed.append(run.run_id)
    if removed:
        log.info("Удалено старых прогонов: %s", len(removed))
    return removed
