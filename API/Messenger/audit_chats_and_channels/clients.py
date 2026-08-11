from __future__ import annotations

from typing import Iterator

from http_base import BaseClient


class AuditLogClient(BaseClient):
    """cloud-api.yandex.net /v1/auditlog — события организации."""

    def iter_events(self, org_id: int, started_at: str, ended_at: str,
                    types: list[str]) -> Iterator[dict]:
        path = f"/v1/auditlog/organizations/{org_id}/events"
        iteration_key = None
        while True:
            params = {
                "started_at": started_at,
                "ended_at": ended_at,
                "types": ",".join(types),
                "count": 100,
            }
            if iteration_key:
                params["iteration_key"] = iteration_key
            data = self.get(path, params=params)
            for item in data.get("items", []):
                yield item                    # enrichedEvent
            iteration_key = data.get("iteration_key")
            if not iteration_key:
                break


class DirectoryClient(BaseClient):
    """cloud-api.yandex.net /v1/directory — users, groups, departments."""

    def iter_users(self, org_id: int, limit: int = 100) -> Iterator[dict]:
        yield from self._paged(f"/v1/directory/organizations/{org_id}/users", limit)

    def iter_groups(self, org_id: int, limit: int = 100) -> Iterator[dict]:
        yield from self._paged(f"/v1/directory/organizations/{org_id}/groups", limit)

    def iter_departments(self, org_id: int, limit: int = 100) -> Iterator[dict]:
        yield from self._paged(f"/v1/directory/organizations/{org_id}/departments", limit)

    def _paged(self, path: str, limit: int) -> Iterator[dict]:
        offset = 0
        while True:
            data = self.get(path, params={"limit": limit, "offset": offset})
            items = data.get("items", [])
            for item in items:
                yield item
            total = data.get("total", 0)
            offset += limit
            if not items or offset >= total:
                break


class Api360Client(BaseClient):
    """api360.yandex.net — рекурсивный состав группы."""

    def group_members(self, org_id: int, group_id: str | int) -> dict:
        return self.get(f"/directory/v2/org/{org_id}/groups/{group_id}/members")
