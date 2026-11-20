import time
import uuid
from datetime import timedelta

from django.conf import settings
from django.core.exceptions import ValidationError
from django.core.validators import validate_email
from django.db import transaction
from django.db.models import Q
from django.utils import timezone

from .clients import DirectoryClient
from .errors import ServiceError
from .models import DirectoryState, Employee


def parse_employee(raw):
    if not isinstance(raw, dict):
        raise ServiceError("directory_format", "Некорректная запись каталога.")
    uid = str(raw.get("id", ""))
    if not uid.isdigit() or len(uid) > 32:
        raise ServiceError("directory_format", "Некорректный идентификатор сотрудника.")
    name = raw.get("name", {})
    if not isinstance(name, dict):
        raise ServiceError("directory_format", "Некорректное имя сотрудника.")
    full_name = " ".join(str(name.get(k) or "").strip() for k in ("last", "first", "middle")).strip()
    email = raw.get("email") or ""
    if not isinstance(email, str):
        raise ServiceError("directory_format", "Некорректная почта сотрудника.")
    if len(email) > 254:
        email = ""
    try:
        validate_email(email)
    except ValidationError:
        email = ""  # Keep the UID, but disable contact instead of inventing an address.
    return Employee(
        namespace=settings.DIRECTORY_NAMESPACE, uid=uid,
        name=(full_name or str(raw.get("display_name") or "Сотрудник организации"))[:300],
        email=email, is_enabled=raw.get("is_enabled") is True,
        is_dismissed=raw.get("is_dismissed") is not False,
        is_robot=raw.get("is_robot") is not False,
    )


def refresh_directory(force=False):
    """Cross-process lease; network I/O stays outside transactions; swap only a complete snapshot."""
    namespace = settings.DIRECTORY_NAMESPACE
    state, _ = DirectoryState.objects.get_or_create(namespace=namespace)
    initial_generation = state.generation
    deadline = time.monotonic() + settings.DIRECTORY_WAIT_SECONDS
    while True:
        state.refresh_from_db()
        now = timezone.now()
        if state.lease_until and state.lease_until > now:
            if time.monotonic() >= deadline:
                raise ServiceError("directory_busy", "Каталог обновляется. Повторите поиск через несколько секунд.")
            time.sleep(0.1)
            continue
        if state.generation != initial_generation:
            return state
        if not force and state.next_refresh_at and state.next_refresh_at > now:
            if state.last_error:
                raise ServiceError("directory_unavailable", "Не удалось обновить каталог. Попробуйте позже.")
            return state
        token = uuid.uuid4()
        candidates = DirectoryState.objects.filter(namespace=namespace, generation=state.generation).filter(
            Q(lease_until__isnull=True) | Q(lease_until__lte=now)
        )
        if not force:
            candidates = candidates.filter(Q(next_refresh_at__isnull=True) | Q(next_refresh_at__lte=now))
        acquired = candidates.update(
            lease_token=token, lease_until=now + timedelta(seconds=settings.DIRECTORY_LEASE_SECONDS),
            attempted_at=now, next_refresh_at=now + timedelta(seconds=settings.DIRECTORY_COOLDOWN_SECONDS),
        )
        if acquired:
            break
        if time.monotonic() >= deadline:
            raise ServiceError("directory_busy", "Каталог обновляется. Повторите поиск позже.")

    try:
        employees, seen = [], set()
        for page in DirectoryClient().pages():
            owned = DirectoryState.objects.filter(namespace=namespace, lease_token=token).update(
                lease_until=timezone.now() + timedelta(seconds=settings.DIRECTORY_LEASE_SECONDS)
            )
            if not owned:
                raise ServiceError("directory_lease", "Обновление каталога было перезапущено. Повторите поиск.")
            for raw in page:
                employee = parse_employee(raw)
                if employee.uid in seen:
                    raise ServiceError("directory_duplicate", "Каталог изменился во время загрузки. Повторите позже.")
                seen.add(employee.uid)
                employees.append(employee)
        with transaction.atomic():
            locked = DirectoryState.objects.select_for_update().get(namespace=namespace)
            if locked.lease_token != token:
                raise ServiceError("directory_lease", "Обновление каталога было перезапущено.")
            Employee.objects.filter(namespace=namespace).delete()
            Employee.objects.bulk_create(employees, batch_size=500)
            locked.generation += 1
            locked.synced_at = timezone.now()
            locked.next_refresh_at = locked.synced_at + timedelta(seconds=settings.DIRECTORY_COOLDOWN_SECONDS)
            locked.last_error = ""
            locked.lease_until = None
            locked.lease_token = None
            locked.save()
        return locked
    except Exception as exc:
        code = exc.code if isinstance(exc, ServiceError) else "directory_internal"
        DirectoryState.objects.filter(namespace=namespace, lease_token=token).update(
            lease_until=None, lease_token=None, last_error=code,
            next_refresh_at=timezone.now() + timedelta(seconds=settings.DIRECTORY_COOLDOWN_SECONDS),
        )
        if isinstance(exc, ServiceError):
            raise
        raise ServiceError("directory_internal", "Не удалось обновить каталог сотрудников.") from None


def resolve_employee(uid):
    employee = Employee.objects.filter(namespace=settings.DIRECTORY_NAMESPACE, uid=uid).first()
    if employee:
        return employee
    refresh_directory()
    return Employee.objects.filter(namespace=settings.DIRECTORY_NAMESPACE, uid=uid).first()
