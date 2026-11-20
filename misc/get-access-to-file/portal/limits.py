import hashlib
import hmac
import ipaddress
from datetime import timedelta

from django.conf import settings
from django.db import transaction
from django.db.models import F
from django.utils import timezone

from .errors import ServiceError
from .models import RateBucket


def private_key(*parts):
    value = "\0".join(str(p) for p in parts)
    return hmac.new(settings.SECRET_KEY.encode(), value.encode(), hashlib.sha256).hexdigest()


def client_ip(request):
    raw = request.META.get("REMOTE_ADDR", "")
    if settings.TRUST_PROXY_HEADERS:
        raw = request.META.get("HTTP_X_REAL_IP", raw)
    try:
        address = ipaddress.ip_address(raw)
        if address.version == 6:
            return str(ipaddress.ip_network(f"{address}/64", strict=False))
        return str(address)
    except ValueError:
        return "unknown"


def consume_rate(scope, identity, limit, seconds):
    now = timezone.now()
    bucket = int(now.timestamp()) // seconds
    key = private_key(settings.DIRECTORY_NAMESPACE, scope, identity)
    with transaction.atomic():
        row, _ = RateBucket.objects.get_or_create(
            key=key, bucket=bucket, defaults={"expires_at": now + timedelta(seconds=seconds * 2)}
        )
        if not RateBucket.objects.filter(pk=row.pk, count__lt=limit).update(count=F("count") + 1):
            raise ServiceError("rate_limited", "Слишком много запросов. Попробуйте позже.", 429)
