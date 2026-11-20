from datetime import timedelta

from django.conf import settings
from django.db import IntegrityError, transaction
from django.utils import timezone

from .clients import DiskClient
from .branding import employee_display_name, organization_label, support_label
from .directory import resolve_employee
from .errors import ServiceError
from .limits import private_key
from .models import AccessRequest, Employee, RequestGate, ResourceLookup


def find_owner(link, session_key):
    uid = DiskClient().owner(link.public_key)
    employee = resolve_employee(uid)
    lookup = ResourceLookup.objects.create(
        session_key=session_key, namespace=settings.DIRECTORY_NAMESPACE,
        resource_url=link.url, public_key=link.public_key, resource_digest=link.digest,
        owner_uid=uid, expires_at=timezone.now() + timedelta(seconds=settings.LOOKUP_TTL_SECONDS),
    )
    if employee is None:
        reason = f"Владелец определён, но его почта не найдена в каталоге. Обратитесь в {support_label()}."
    elif not employee.contactable:
        reason = f"Отправка этому владельцу недоступна: проверьте контакт с поддержкой {organization_label()}."
    else:
        reason = ""
    return {
        "lookup_id": str(lookup.pk), "resource_url": link.url,
        "owner": {"name": employee_display_name(employee)},
        "can_request": bool(employee and employee.contactable), "message": reason,
        "expires_at": lookup.expires_at.isoformat(),
    }


def get_lookup(lookup_id, session_key):
    lookup = ResourceLookup.objects.filter(
        pk=lookup_id, session_key=session_key, namespace=settings.DIRECTORY_NAMESPACE,
    ).first()
    if lookup is None:
        raise ServiceError("lookup_not_found", "Результат поиска не найден. Найдите ресурс заново.", 404)
    return lookup


def enqueue_request(lookup, data):
    previous = AccessRequest.objects.filter(lookup=lookup).first()
    if previous:
        return previous, False
    now = timezone.now()
    if lookup.expires_at <= now:
        raise ServiceError("lookup_expired", "Результат поиска устарел. Найдите владельца заново.", 409)
    # Recheck resource ownership, but resolve email from the persistent local directory.
    if DiskClient().owner(lookup.public_key) != lookup.owner_uid:
        raise ServiceError("owner_changed", "Владелец изменился. Найдите ресурс заново.", 409)
    employee = resolve_employee(lookup.owner_uid)
    if not employee or not employee.contactable:
        raise ServiceError("owner_unavailable", f"Почта владельца недоступна. Обратитесь в {support_label()}.", 409)
    gate_key = private_key(lookup.namespace, lookup.resource_digest, data["account_email"])
    try:
        with transaction.atomic():
            # The lookup row serializes same-lookup submissions on PostgreSQL.
            ResourceLookup.objects.select_for_update().get(pk=lookup.pk)
            previous = AccessRequest.objects.filter(lookup=lookup).first()
            if previous:
                return previous, False
            RequestGate.objects.get_or_create(key=gate_key, defaults={"blocked_until": now})
            if not RequestGate.objects.filter(key=gate_key, blocked_until__lte=now).update(
                blocked_until=now + timedelta(seconds=settings.REQUEST_DEDUP_SECONDS)
            ):
                raise ServiceError("duplicate_request", "Для этого аккаунта недавно уже отправляли заявку на этот ресурс.", 409)
            request = AccessRequest.objects.create(
                lookup=lookup, namespace=lookup.namespace, session_key=lookup.session_key,
                resource_url=lookup.resource_url, owner_uid=employee.uid,
                owner_name=employee.name, owner_email=employee.email,
                requester_name=data["requester_name"], account_email=data["account_email"],
                contact_email=data["contact_email"], rights=data["rights"], comment=data["comment"],
                available_at=now,
            )
            # A request row is itself the durable outbox record; no in-memory enqueue can get lost.
            return request, True
    except IntegrityError:
        previous = AccessRequest.objects.filter(lookup=lookup).first()
        if previous:
            return previous, False
        raise


def serialize_request(request):
    messages = {
        "queued": "Заявка сохранена и ожидает отправки.",
        "sending": "Письмо отправляется владельцу.",
        "retry": "Временная ошибка отправки. Сервис попробует ещё раз автоматически.",
        "accepted": "Почтовый сервер принял письмо.",
        "saved": "Тестовое письмо сохранено в папку data/mail. Реальное письмо не отправлялось.",
        "failed": "Письмо не удалось отправить. Сообщите администратору номер заявки.",
    }
    return {
        "request_id": str(request.pk), "status": request.status,
        "status_label": request.get_status_display(), "message": messages[request.status],
        "created_at": request.created_at.isoformat(),
    }
