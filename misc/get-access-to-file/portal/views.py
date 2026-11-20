import json
import logging
from functools import wraps

from django.conf import settings
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import ensure_csrf_cookie
from django.views.decorators.http import require_GET, require_POST

from .errors import ServiceError
from .branding import organization_label
from .forms import AccessRequestForm, LookupForm
from .limits import client_ip, consume_rate
from .models import AccessRequest
from .services import enqueue_request, find_owner, get_lookup, serialize_request

logger = logging.getLogger(__name__)


def api_errors(view):
    @wraps(view)
    def wrapped(request, *args, **kwargs):
        try:
            return view(request, *args, **kwargs)
        except ServiceError as exc:
            # No resource URLs, email addresses, tokens or upstream response bodies in logs.
            logger.warning(
                "API error code=%s status=%s upstream_service=%s upstream_status=%s path=%s",
                exc.code, exc.status, exc.upstream_service or "none", exc.upstream_status or "none", request.path,
            )
            response = JsonResponse({"error": {"code": exc.code, "message": exc.message}}, status=exc.status)
            if exc.status in {429, 503}:
                response["Retry-After"] = "60"
            return response
    return wrapped


def payload(request, form_class):
    if request.content_type != "application/json":
        raise ServiceError("content_type", "Ожидается JSON-запрос.", 415)
    if len(request.body) > 16384:
        raise ServiceError("too_large", "Запрос слишком большой.", 413)
    try:
        data = json.loads(request.body)
    except (ValueError, UnicodeError):
        raise ServiceError("bad_json", "Некорректный JSON.", 400) from None
    if not isinstance(data, dict) or set(data) - set(form_class.base_fields):
        raise ServiceError("bad_fields", "Некорректные поля запроса.", 400)
    if any(not isinstance(v, str) for v in data.values()):
        raise ServiceError("bad_fields", "Поля формы должны быть строками.", 400)
    form = form_class(data)
    if not form.is_valid():
        errors = {key: list(values) for key, values in form.errors.items()}
        return None, JsonResponse({"error": {"code": "validation", "message": "Проверьте поля формы.", "fields": errors}}, status=400)
    return form.cleaned_data, None


def session_key(request):
    if not request.session.session_key:
        request.session.create()
    return request.session.session_key


@require_GET
@ensure_csrf_cookie
def index(request):
    session_key(request)
    return render(request, "portal/index.html", {
        "demo_mode": settings.DEMO_MODE, "file_mail": settings.MAIL_TRANSPORT == "file",
        "organization_label": organization_label(),
    })


@require_GET
def health(request):
    return JsonResponse({"status": "ok"})


@require_POST
@api_errors
def lookup(request):
    consume_rate("lookup-ip", client_ip(request), settings.LOOKUP_RATE_LIMIT, 60)
    data, error = payload(request, LookupForm)
    if error is not None:
        return error
    return JsonResponse(find_owner(data["resource_url"], session_key(request)), status=201)


@require_POST
@api_errors
def create_request(request):
    data, error = payload(request, AccessRequestForm)
    if error is not None:
        return error
    found = get_lookup(data["lookup_id"], session_key(request))
    existing = AccessRequest.objects.filter(lookup=found).first()
    if existing:
        return JsonResponse(serialize_request(existing))
    consume_rate("request-ip", client_ip(request), settings.REQUEST_RATE_LIMIT, 3600)
    consume_rate("request-email", data["account_email"], settings.REQUEST_EMAIL_RATE_LIMIT, 3600)
    result, created = enqueue_request(found, data)
    return JsonResponse(serialize_request(result), status=202 if created else 200)


@require_GET
@api_errors
def request_status(request, request_id):
    result = AccessRequest.objects.filter(
        pk=request_id, session_key=request.session.session_key or "", namespace=settings.DIRECTORY_NAMESPACE,
    ).first()
    if result is None:
        raise ServiceError("request_not_found", "Заявка не найдена в этой сессии браузера.", 404)
    return JsonResponse(serialize_request(result))


def csrf_failure(request, reason=""):
    return JsonResponse({"error": {"code": "csrf", "message": "Сессия формы устарела. Обновите страницу."}}, status=403)
