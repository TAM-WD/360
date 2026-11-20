import asyncio
import json
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from django.conf import settings

from .errors import ServiceError
from .branding import organization_label

API_ROOT = "https://cloud-api.yandex.net"


def upstream_error(status, service):
    def error(code, message, http_status=503):
        return ServiceError(code, message, http_status, upstream_status=status, upstream_service=service)

    if status == 401:
        return error("integration_auth", "Сервис не смог авторизоваться в Яндексе. Обратитесь к администратору сервиса.")
    if status == 403:
        message = (
            "Яндекс не разрешил сервису получить сведения об этом файле или папке. Обратитесь к администратору сервиса."
            if service == "disk" else "Яндекс не разрешил сервису прочитать каталог сотрудников. Обратитесь к администратору сервиса."
        )
        return error("integration_forbidden", message)
    if status in {404, 410} and service == "disk":
        return error("resource_unavailable", f"Файл или папка не найдены. Проверьте ссылку: ресурс мог быть удалён, доступ по ссылке отключён, либо ресурс находится вне {organization_label()}.", 404)
    if status in {400, 422} and service == "disk":
        return error("unsupported_resource", "Яндекс не смог обработать эту ссылку. Скопируйте её заново через «Поделиться» в Яндекс Диске. Если ошибка повторится, обратитесь к администратору сервиса.", 422)
    if status == 429:
        return error("upstream_rate_limited", "Яндекс временно ограничил количество запросов от сервиса. Попробуйте через несколько минут.")
    if 500 <= status <= 599:
        message = (
            f"Ошибка при проверке ссылки, повторите поиск позже. Если ошибка повторится, проверьте, что ссылка принадлежит сотруднику {organization_label()}."
            if service == "disk" else "Яндекс вернул ошибку при загрузке каталога сотрудников. Повторите поиск позже. Если ошибка повторится, обратитесь к администратору сервиса."
        )
        return error("upstream_server_error", message)
    return error("upstream_unavailable", "Сервис получил неожиданный ответ Яндекса и не смог завершить проверку. Обратитесь к администратору сервиса.")


def api_get(path, token, params, service):
    # Django views and directory commands are synchronous; network work has its own
    # cancellable deadline, including response-body reads and all retry attempts.
    return asyncio.run(_api_get(path, token, params, service))


def timeout_error(service):
    message = (
        "Не удалось дождаться ответа Яндекс Диска. Попробуйте ещё раз или проверьте другую ссылку."
        if service == "disk" else "Каталог Яндекса не ответил вовремя. Повторите запрос позже."
    )
    return ServiceError("upstream_timeout", message, 504)


async def _api_get(path, token, params, service):
    try:
        async with asyncio.timeout(settings.API_DEADLINE_SECONDS):
            # Never follow user-provided URLs or redirects with administrator credentials.
            async with httpx.AsyncClient(timeout=settings.API_TIMEOUT, follow_redirects=False, trust_env=False) as client:
                for attempt in range(settings.API_RETRIES + 1):
                    try:
                        response = await client.get(API_ROOT + path, params=params, headers={"Authorization": f"OAuth {token}"})
                    except httpx.RequestError as exc:
                        # Do not repeat a slow interactive Disk lookup after a socket timeout.
                        if service == "disk" and isinstance(exc, httpx.TimeoutException):
                            raise timeout_error(service) from None
                        if attempt < settings.API_RETRIES:
                            await asyncio.sleep(0.25 * (2 ** attempt))
                            continue
                        raise ServiceError("upstream_network", "Сервису не удалось установить соединение с Яндексом. Повторите поиск через несколько минут.") from None
                    if response.status_code in {429, 500, 502, 503, 504} and attempt < settings.API_RETRIES:
                        await asyncio.sleep(0.25 * (2 ** attempt))
                        continue
                    if response.status_code != 200:
                        raise upstream_error(response.status_code, service)
                    try:
                        data = response.json()
                    except (ValueError, UnicodeError):
                        raise ServiceError("upstream_format", "Получен некорректный ответ Яндекса.") from None
                    if not isinstance(data, dict):
                        raise ServiceError("upstream_format", "Получен некорректный ответ Яндекса.")
                    return data
    except TimeoutError:
        raise timeout_error(service) from None


class DiskClient:
    def owner(self, public_key):
        if settings.DEMO_MODE:
            key = urlsplit(public_key).path.rsplit("/", 1)[-1]
            mapping = {"demo-file": "1001", "demo-new": "1002", "demo-blocked": "1003", "demo-missing": "9999"}
            if key == "demo-external":
                raise upstream_error(404, "disk")
            if key not in mapping:
                raise ServiceError("demo_link", "В деморежиме используйте одну из ссылок-примеров под формой.", 422)
            return mapping[key]
        data = api_get("/v1/disk/public/resources/admin/public-settings", settings.YANDEX_DISK_TOKEN,
                       {"public_key": public_key}, "disk")
        accesses = data.get("accesses")
        if not isinstance(accesses, list):
            raise ServiceError("owner_format", "Яндекс не вернул настройки владельца.")
        owners = [x for x in accesses if isinstance(x, dict) and x.get("type") == "owner"]
        if len(owners) != 1:
            raise ServiceError("owner_missing", "Не удалось однозначно определить владельца ресурса.", 422)
        owner = owners[0]
        if str(owner.get("org_id")) != settings.YANDEX_ORG_ID:
            raise ServiceError("outside_org", f"Этот файл или папка принадлежат другой организации. Сервис может найти владельца только в {organization_label()}.", 404)
        uid = str(owner.get("id", ""))
        if not uid.isdigit() or len(uid) > 32:
            raise ServiceError("owner_format", "Яндекс вернул некорректный идентификатор владельца.")
        return uid


class DirectoryClient:
    def pages(self):
        if settings.DEMO_MODE:
            path = Path(__file__).parent / "fixtures" / "demo_directory.json"
            with path.open(encoding="utf-8") as fixture:
                yield json.load(fixture)
            return
        offset = 0
        limit = settings.DIRECTORY_PAGE_SIZE
        expected_total = None
        for _ in range(settings.DIRECTORY_MAX_PAGES):
            data = api_get(f"/v1/directory/organizations/{settings.YANDEX_ORG_ID}/users",
                           settings.YANDEX_DIRECTORY_TOKEN, {"limit": limit, "offset": offset}, "directory")
            items, total = data.get("items"), data.get("total")
            returned_limit = data.get("limit")
            if (not isinstance(items, list) or type(total) is not int or total < 0
                    or type(returned_limit) is not int or returned_limit < 1
                    or data.get("offset") != offset or len(items) > returned_limit):
                raise ServiceError("directory_format", "Каталог вернул некорректную страницу.")
            if expected_total is None:
                expected_total = total
                limit = returned_limit
            if total != expected_total or returned_limit != limit:
                raise ServiceError("directory_changed", "Каталог изменился во время загрузки. Повторите позже.")
            if offset + len(items) > total or (offset + len(items) < total and len(items) != limit):
                raise ServiceError("directory_incomplete", "Каталог вернул неполный список сотрудников.")
            yield items
            if offset + len(items) >= total:
                return
            offset += limit
        raise ServiceError("directory_too_large", "Превышен лимит страниц каталога. Обратитесь к администратору.")
