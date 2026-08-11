from __future__ import annotations

import logging
import os
from dataclasses import dataclass

log = logging.getLogger("config")


@dataclass
class Config:
    """Конфигурация пайплайна. Токены берутся из окружения."""

    org_id: int = 0
    audit_token: str = ""
    directory_token: str = ""

    audit_base: str = "https://cloud-api.yandex.net"
    directory_base: str = "https://cloud-api.yandex.net"
    api360_base: str = "https://api360.yandex.net"

    # --- сбор аудит-лога ---
    backfill_days: int = 180
    overlap_minutes: int = 10

    # --- скоуп ---
    include_private: bool = False
    expand_groups: bool = False
    resolve_uids: bool = True

    # --- источники идентичностей ---
    directory_source: str = "auto"          # auto | cloud | api360 | none
    allow_empty_directory: bool = False

    # --- ручная таблица ---
    manual_path: str | None = None
    manual_sheet: str | None = None
    manual_tz: str = "+00:00"
    manual_date_semantics: str = "member_added"
    manual_map_path: str | None = None
    date_tolerance_days: int = 1

    # --- хранение результатов ---
    db_path: str = "./msgaudit.sqlite3"
    results_dir: str = "./result"
    run_tag: str | None = None
    keep_runs: int = 0                 # 0 = не удалять старые прогоны
    compare_previous: bool = True      # авто-сравнение с предыдущим прогоном

    @classmethod
    def from_env(cls, *, require_network: bool = True) -> "Config":
        def _req(name: str) -> str:
            value = os.environ.get(name)
            if not value and require_network:
                raise SystemExit(f"Не задана переменная окружения {name}")
            return value or ""

        org_raw = os.environ.get("ORG_ID")
        if not org_raw and require_network:
            raise SystemExit("Не задана переменная окружения ORG_ID")

        results_dir = os.environ.get("RESULTS_DIR")
        if not results_dir and os.environ.get("OUT_DIR"):
            results_dir = os.environ["OUT_DIR"]
            log.warning("OUT_DIR устарел, используйте RESULTS_DIR")

        audit_token = _req("AUDIT_TOKEN")
        return cls(
            org_id=int(org_raw) if org_raw else 0,
            audit_token=audit_token,
            directory_token=os.environ.get("DIRECTORY_TOKEN") or audit_token,
            include_private=os.environ.get("INCLUDE_PRIVATE", "0") == "1",
            expand_groups=os.environ.get("EXPAND_GROUPS", "0") == "1",
            resolve_uids=os.environ.get("RESOLVE_UIDS", "1") == "1",
            directory_source=os.environ.get("DIRECTORY_SOURCE", "auto"),
            allow_empty_directory=os.environ.get("ALLOW_EMPTY_DIRECTORY", "0") == "1",
            manual_path=os.environ.get("MANUAL_PATH") or None,
            manual_sheet=os.environ.get("MANUAL_SHEET") or None,
            manual_tz=os.environ.get("MANUAL_TZ", "+00:00"),
            manual_map_path=os.environ.get("MANUAL_MAP_PATH") or None,
            db_path=os.environ.get("DB_PATH", "./msgaudit.sqlite3"),
            results_dir=results_dir or "./result",
            run_tag=os.environ.get("RUN_TAG") or None,
            keep_runs=int(os.environ.get("KEEP_RUNS", "0")),
            compare_previous=os.environ.get("COMPARE_PREVIOUS", "1") == "1",
        )

    def flags_snapshot(self) -> dict:
        """Флаги, влияющие на состав отчётов при сравнении прогонов."""
        return {
            "expand_groups": self.expand_groups,
            "include_private": self.include_private,
            "resolve_uids": self.resolve_uids,
            "directory_source": self.directory_source,
            "allow_empty_directory": self.allow_empty_directory,
            "manual_present": bool(self.manual_path),
            "manual_date_semantics": self.manual_date_semantics,
            "manual_tz": self.manual_tz,
            "date_tolerance_days": self.date_tolerance_days,
        }
