import smtplib
import uuid
from datetime import timedelta

from django.conf import settings
from django.core.mail import EmailMessage
from django.db.models import F, Q
from django.template.loader import render_to_string
from django.utils import timezone

from .models import AccessRequest, Employee


def claim_message():
    now = timezone.now()
    eligible = (
        Q(status__in=["queued", "retry"], available_at__lte=now)
        | Q(status="sending", lease_until__lte=now)
    )
    for _ in range(10):
        candidate = AccessRequest.objects.filter(namespace=settings.DIRECTORY_NAMESPACE).filter(eligible).order_by("available_at").first()
        if candidate is None:
            return None
        token = uuid.uuid4()
        claimed = AccessRequest.objects.filter(pk=candidate.pk).filter(eligible).update(
            status="sending", lease_token=token, lease_until=now + timedelta(seconds=settings.MAIL_LEASE_SECONDS),
            attempts=F("attempts") + 1,
        )
        if claimed:
            candidate.refresh_from_db()
            return candidate
    return None


def finish(request, **fields):
    return AccessRequest.objects.filter(pk=request.pk, status="sending", lease_token=request.lease_token).update(
        lease_token=None, lease_until=None, **fields,
    )


def error_kind(exc):
    if isinstance(exc, smtplib.SMTPRecipientsRefused):
        temporary = bool(exc.recipients) and all(400 <= value[0] < 500 for value in exc.recipients.values())
        return "smtp_recipient", temporary
    if isinstance(exc, smtplib.SMTPResponseException):
        return "smtp_rejected", 400 <= exc.smtp_code < 500
    if isinstance(exc, (OSError, smtplib.SMTPServerDisconnected)):
        return "mail_connection", True
    return "mail_configuration", False


def process_one():
    request = claim_message()
    if request is None:
        return None
    if request.attempts > settings.MAIL_MAX_ATTEMPTS:
        finish(request, status="failed", last_error="attempts_exhausted", finished_at=timezone.now())
        return request.pk
    employee = Employee.objects.filter(namespace=request.namespace, uid=request.owner_uid).first()
    if not employee or not employee.contactable:
        finish(request, status="failed", last_error="owner_unavailable", finished_at=timezone.now())
        return request.pk
    # If a directory refresh occurred while queued, use the current local address.
    request.owner_email = employee.email
    request.owner_name = employee.name
    AccessRequest.objects.filter(pk=request.pk, lease_token=request.lease_token).update(
        owner_email=employee.email, owner_name=employee.name,
    )
    try:
        message = EmailMessage(
            subject=f"Запрос доступа к ресурсу Яндекс Диска · {str(request.pk)[:8]}",
            body=render_to_string("portal/request_email.txt", {"item": request}),
            from_email=settings.DEFAULT_FROM_EMAIL, to=[request.owner_email], reply_to=[request.contact_email],
            headers={"Message-ID": f"<{request.pk}@{settings.MESSAGE_ID_DOMAIN}>"},
        )
        if message.send(fail_silently=False) != 1:
            raise OSError("mail_not_accepted")
    except Exception as exc:
        code, temporary = error_kind(exc)
        if temporary and request.attempts < settings.MAIL_MAX_ATTEMPTS:
            delay = min(settings.MAIL_RETRY_BASE_SECONDS * (2 ** (request.attempts - 1)), 3600)
            finish(request, status="retry", last_error=code, available_at=timezone.now() + timedelta(seconds=delay))
        else:
            finish(request, status="failed", last_error=code, finished_at=timezone.now())
    else:
        finish(request, status="saved" if settings.MAIL_TRANSPORT == "file" else "accepted",
               last_error="", finished_at=timezone.now())
    return request.pk
