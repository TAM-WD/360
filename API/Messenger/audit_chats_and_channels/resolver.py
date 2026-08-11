from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from clients import Api360Client, DirectoryClient
from http_base import HttpError
from identity_store import IdentityStore

log = logging.getLogger("resolver")


class DirectoryUnavailableError(RuntimeError):
    """Не удалось получить ни одной записи о сотрудниках, а работать
    вслепую запрещено настройками."""


@dataclass
class UserInfo:
    uid: str
    login: Optional[str] = None
    email: Optional[str] = None
    full_name: Optional[str] = None
    display_name: Optional[str] = None
    position: Optional[str] = None
    department_id: Optional[str] = None
    is_robot: bool = False
    is_dismissed: bool = False
    source: str = "unknown"      # directory | api360 | audit_log | cache


def _compose_full_name(user: dict) -> Optional[str]:
    name = user.get("name") or {}
    parts = [name.get("last") or name.get("lastName"),
             name.get("first") or name.get("firstName"),
             name.get("middle") or name.get("middleName")]
    fio = " ".join(str(part).strip() for part in parts if part and str(part).strip())
    return fio or (user.get("display_name") or user.get("displayName") or None)


class DirectoryResolver:
    """Выясняет, какой сотрудник стоит за идентификатором.

    Задача такая. В аудит-логе участники чатов записаны длинными числовыми
    номерами. В ручной таблице — адресами почты. Чтобы в отчёте получилась
    одна строка на человека, а не две, эти два мира надо связать. Попутно
    достаём ФИО и должность.

    Ищем в четырёх местах по очереди. Каждое следующее только заполняет
    пропуски и не портит то, что уже нашли:

      1. Справочник сотрудников (cloud-api). Основной вариант: сразу даёт
         логин, ФИО, должность и подразделение.

      2. Тот же справочник по другому адресу (api360). Пробуем, если первый
         не ответил — например, у токена нет нужных прав.

      3. Сам аудит-лог. В каждом событии рядом лежат номер сотрудника и его
         логин — это тот, кто событие совершил. Так узнаём логины даже когда
         справочник закрыт совсем.

      4. Копия в локальной базе. Всё, что нашли в предыдущие запуски.
         Выручает, когда человек уволился и из справочника уже исчез,
         а в старых чатах остался.

    Мессенджер-ботов в справочнике нет вообще, поэтому по номеру их не
    опознать. Признак «бот» берётся из флага is_robot в аудит-логе, а логин
    бота — только из ручной таблицы.

    Если не сработал ни один источник, работа останавливается с понятным
    объяснением. Раньше в этом случае прогон молча продолжался и все
    участники помечались как «не найдены» — искать причину было тяжело.
    """

    def __init__(self, directory: DirectoryClient, api360: Api360Client, org_id: int,
                 *, identity_store: IdentityStore | None = None,
                 source_mode: str = "auto", fail_on_empty: bool = True):
        self.directory = directory
        self.api360 = api360
        self.org_id = org_id
        self.identity_store = identity_store
        self.source_mode = source_mode          # auto | cloud | api360 | none
        self.fail_on_empty = fail_on_empty

        self._users: dict[str, UserInfo] = {}
        self._identity_to_uid: dict[str, str] = {}
        self._dept_users: dict[str, list[str]] = {}
        self._dept_children: dict[str, list[str]] = {}
        self._dept_name: dict[str, str] = {}
        self._group_name: dict[str, str] = {}
        self._group_cache: dict[str, list[str]] = {}

        self.load_report: dict = {"cloud_api": 0, "api360": 0,
                                  "audit_log": 0, "cache": 0, "errors": []}
        self.users_loaded = False

    # ------------------------------------------------------------------
    def _register(self, info: UserInfo, aliases: list[str]) -> None:
        existing = self._users.get(info.uid)
        if existing is None:
            self._users[info.uid] = info
        else:
            # дополняем пропуски, но не затираем то, что уже знаем
            for field_name in ("login", "email", "full_name", "position",
                               "department_id", "display_name"):
                if not getattr(existing, field_name) and getattr(info, field_name):
                    setattr(existing, field_name, getattr(info, field_name))
            existing.is_robot = existing.is_robot or info.is_robot
        if info.department_id:
            bucket = self._dept_users.setdefault(info.department_id, [])
            if info.uid not in bucket:
                bucket.append(info.uid)
        for alias in aliases:
            if alias:
                self._identity_to_uid.setdefault(str(alias).strip().casefold(),
                                                 info.uid)
        if self.identity_store:
            self.identity_store.upsert_user(
                info.uid, login=info.login, full_name=info.full_name,
                position=info.position, is_robot=info.is_robot,
                is_dismissed=info.is_dismissed, source=info.source)
            for alias in aliases:
                if alias:
                    self.identity_store.upsert_alias(str(alias), info.uid,
                                                     info.source)

    # ------------------------------------------------------------------
    def preload_users(self, store=None) -> dict:
        """Собирает сведения о сотрудниках из всех доступных источников."""
        if self.users_loaded:
            return self.load_report

        if self.source_mode in ("auto", "cloud"):
            self._load_cloud_api()
        if self.source_mode in ("auto", "api360") and not self.load_report["cloud_api"]:
            self._load_api360()
        if store is not None:
            self._load_from_audit(store)
        self._load_from_cache()

        if self.identity_store:
            self.identity_store.commit()

        total = len(self._users)
        found = {key: value for key, value in self.load_report.items()
                 if key != "errors" and value}
        log.info("Знаем сотрудников: %s, у них %s адресов и логинов. "
                 "Где нашли: %s", total, len(self._identity_to_uid),
                 found or "нигде")

        if total == 0:
            message = self._empty_directory_message()
            if self.fail_on_empty:
                raise DirectoryUnavailableError(message)
            log.error("%s", message)

        self.users_loaded = True
        return self.load_report

    def _empty_directory_message(self) -> str:
        problems = "\n".join(f"    {item}" for item in self.load_report["errors"]) \
            or "    подробностей от сервисов не поступило"
        return (
            "Не удалось получить сведения о сотрудниках.\n\n"
            "Что это значит: вместо логинов и ФИО в отчёте останутся длинные\n"
            "числовые номера — читать такой отчёт неудобно.\n\n"
            "Что не получилось:\n"
            f"{problems}\n\n"
            "Что делать:\n"
            "    1. Проверить доступы:       python cli.py doctor\n"
            "    2. У токена в DIRECTORY_TOKEN должно быть право\n"
            "       directory:read_users\n"
            "    3. Продолжить без логинов:  добавьте ключ "
            "--allow-empty-directory"
        )

    def _load_cloud_api(self) -> None:
        try:
            count = 0
            for user in self.directory.iter_users(self.org_id):
                uid = str(user.get("id") or "")
                if not uid:
                    continue
                info = UserInfo(
                    uid=uid,
                    login=user.get("nickname") or user.get("email"),
                    email=user.get("email"),
                    full_name=_compose_full_name(user),
                    display_name=user.get("display_name"),
                    position=user.get("position") or None,
                    department_id=(str(user["department_id"])
                                   if user.get("department_id") is not None else None),
                    is_robot=bool(user.get("is_robot")),
                    is_dismissed=bool(user.get("is_dismissed")),
                    source="directory",
                )
                aliases = [user.get("email"), user.get("default_email"),
                           user.get("nickname"), *(user.get("aliases") or [])]
                aliases += [contact.get("value")
                            for contact in (user.get("contacts") or [])
                            if contact.get("type") == "email"]
                self._register(info, [alias for alias in aliases if alias])
                count += 1
            self.load_report["cloud_api"] = count
            log.info("Справочник сотрудников (основной адрес): получили %s", count)
        except HttpError as exc:
            self.load_report["errors"].append(f"основной адрес справочника: {exc}")
            log.warning("Справочник по основному адресу не ответил: %s", exc)

    def _load_api360(self) -> None:
        try:
            count = 0
            for user in self.api360.iter_users(self.org_id):
                uid = str(user.get("id") or "")
                if not uid:
                    continue
                info = UserInfo(
                    uid=uid,
                    login=user.get("nickname") or user.get("email"),
                    email=user.get("email"),
                    full_name=_compose_full_name(user),
                    display_name=user.get("displayName"),
                    position=user.get("position") or None,
                    department_id=(str(user["departmentId"])
                                   if user.get("departmentId") is not None else None),
                    is_robot=bool(user.get("isRobot")),
                    is_dismissed=bool(user.get("isDismissed")),
                    source="api360",
                )
                aliases = [user.get("email"), user.get("nickname"),
                           *(user.get("aliases") or [])]
                aliases += [contact.get("value")
                            for contact in (user.get("contacts") or [])
                            if contact.get("type") == "email"]
                self._register(info, [alias for alias in aliases if alias])
                count += 1
            self.load_report["api360"] = count
            log.info("Справочник сотрудников (резервный адрес): получили %s", count)
        except HttpError as exc:
            self.load_report["errors"].append(f"резервный адрес справочника: {exc}")
            log.warning("Справочник по резервному адресу не ответил: %s", exc)

    def _load_from_audit(self, store) -> None:
        """Аудит-лог сам себе справочник: в событии рядом лежат номер
        сотрудника и его логин."""
        from audit_identities import harvest_identities_from_audit, harvest_partner_uids
        pairs = harvest_identities_from_audit(store)
        pairs.update({key: value for key, value in harvest_partner_uids(store).items()
                      if key not in pairs})
        count = 0
        for uid, login in pairs.items():
            if uid in self._users and self._users[uid].login:
                continue
            # Признак «бот» здесь не выставляем: это инициаторы событий,
            # то есть люди, которые создавали чаты и добавляли участников.
            self._register(UserInfo(uid=uid, login=login, email=login,
                                    source="audit_log"), [login])
            count += 1
        self.load_report["audit_log"] = count
        if count:
            log.info("Из самого аудит-лога узнали ещё %s логинов", count)

    def _load_from_cache(self) -> None:
        if not self.identity_store:
            return
        users, aliases = self.identity_store.load_all()
        count = 0
        for uid, row in users.items():
            if uid in self._users:
                continue
            self._register(UserInfo(
                uid=uid, login=row.get("login"), email=row.get("login"),
                full_name=row.get("full_name"), position=row.get("position"),
                is_robot=bool(row.get("is_robot")),
                is_dismissed=bool(row.get("is_dismissed")),
                source="cache"), [row.get("login")])
            count += 1
        for alias, uid in aliases.items():
            self._identity_to_uid.setdefault(alias, uid)
        self.load_report["cache"] = count
        if count:
            log.info("Из локальной базы подняли ещё %s сотрудников "
                     "(в том числе уволенных)", count)

    # ------------------------------------------------------------------
    def resolve_identity(self, identity: Optional[str]) -> tuple[Optional[str], str]:
        """Ищет сотрудника по адресу почты или логину.

        Возвращает (номер сотрудника, статус). Статусы: resolved,
        resolved_by_local_part, not_found, empty. Отдельного статуса для
        ботов здесь нет — их распознаёт разбор ручной таблицы.
        """
        if not identity:
            return None, "empty"
        wanted = identity.strip().casefold()
        if wanted in self._identity_to_uid:
            uid = self._identity_to_uid[wanted]
            if uid in self._users:
                return uid, f"resolved:{self._users[uid].source}"
            return uid, "resolved"
        local_part = wanted.split("@", 1)[0]
        if local_part in self._identity_to_uid:
            return self._identity_to_uid[local_part], "resolved_by_local_part"
        for alias, uid in self._identity_to_uid.items():
            if alias.split("@", 1)[0] == local_part:
                return uid, "resolved_by_local_part"
        return None, "not_found"

    def uid_for_email(self, email: Optional[str]) -> Optional[str]:
        return self.resolve_identity(email)[0]

    def user_info(self, uid: Optional[str]) -> Optional[UserInfo]:
        return self._users.get(str(uid)) if uid else None

    def login_for_uid(self, uid: Optional[str]) -> Optional[str]:
        info = self.user_info(uid)
        return info.login if info else None

    def full_name_for_uid(self, uid: Optional[str]) -> Optional[str]:
        info = self.user_info(uid)
        return info.full_name if info else None

    def position_for_uid(self, uid: Optional[str]) -> Optional[str]:
        info = self.user_info(uid)
        return info.position if info else None

    def is_robot_uid(self, uid: Optional[str]) -> bool:
        info = self.user_info(uid)
        return bool(info and info.is_robot)

    def learn_identity(self, uid: str, login: str, source: str = "manual") -> None:
        """Запоминает связку «номер сотрудника — логин», найденную по
        ручной таблице. Так узнаём логины мессенджер-ботов."""
        if not uid or not login:
            return
        self._register(UserInfo(uid=str(uid), login=login, email=login,
                                source=source), [login])

    # ------------------------------------------------------------------
    def preload_departments(self) -> None:
        try:
            for dept in self.directory.iter_departments(self.org_id):
                dept_id = str(dept["id"])
                self._dept_name[dept_id] = dept.get("name")
                parent = dept.get("parent_id")
                if parent is None and isinstance(dept.get("parent"), dict):
                    parent = dept["parent"].get("id")
                if parent is not None:
                    self._dept_children.setdefault(str(parent), []).append(dept_id)
            log.info("Подразделений в организации: %s", len(self._dept_name))
        except HttpError as exc:
            log.warning("Список подразделений получить не удалось: %s", exc)

    def preload_groups(self) -> None:
        try:
            for group in self.directory.iter_groups(self.org_id):
                self._group_name[str(group["id"])] = group.get("name")
            log.info("Групп в организации: %s", len(self._group_name))
        except HttpError as exc:
            log.warning("Список групп получить не удалось: %s", exc)

    def department_name(self, dept_id: str) -> Optional[str]:
        return self._dept_name.get(str(dept_id))

    def group_name(self, group_id: str) -> Optional[str]:
        return self._group_name.get(str(group_id))

    def expand_department(self, dept_id: str) -> list[str]:
        """Все сотрудники подразделения, включая вложенные."""
        result: list[str] = []
        seen: set[str] = set()
        stack = [str(dept_id)]
        while stack:
            current = stack.pop()
            if current in seen:
                continue
            seen.add(current)
            result.extend(self._dept_users.get(current, []))
            stack.extend(self._dept_children.get(current, []))
        return list(dict.fromkeys(result))

    def expand_group(self, group_id: str) -> list[str]:
        """Разворачивает группу в конкретных сотрудников, спускаясь во
        вложенные группы и подразделения. Есть защита от закольцованности."""
        gid = str(group_id)
        if gid in self._group_cache:
            return self._group_cache[gid]
        result: list[str] = []
        visited: set[str] = set()
        self._group_cache[gid] = result

        def walk(current: str) -> None:
            if current in visited:
                return
            visited.add(current)
            try:
                body = self.api360.group_members(self.org_id, current)
            except HttpError as exc:
                log.warning("Состав группы %s получить не удалось: %s", current, exc)
                return
            for user in body.get("users") or []:
                uid = str(user.get("id") or "")
                if not uid:
                    continue
                result.append(uid)
                if uid not in self._users:
                    self._register(UserInfo(
                        uid=uid, login=user.get("nickname") or user.get("email"),
                        email=user.get("email"), full_name=_compose_full_name(user),
                        position=user.get("position") or None,
                        department_id=(str(user["departmentId"])
                                       if user.get("departmentId") else None),
                        source="api360"),
                        [user.get("email"), user.get("nickname")])
            for dept in body.get("departments") or []:
                result.extend(self.expand_department(str(dept.get("id"))))
            for nested in body.get("groups") or []:
                walk(str(nested.get("id")))

        walk(gid)
        deduped = list(dict.fromkeys(result))
        self._group_cache[gid] = deduped
        return deduped
