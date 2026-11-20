from datetime import timedelta
from unittest.mock import patch

from django.test import Client, TestCase, override_settings
from django.utils import timezone

from portal.models import AccessRequest, ResourceLookup
from portal.errors import ServiceError
from portal.clients import upstream_error


@override_settings(DEMO_MODE=True, DIRECTORY_NAMESPACE="demo:12345", LOOKUP_RATE_LIMIT=1000,
                   REQUEST_RATE_LIMIT=1000, REQUEST_EMAIL_RATE_LIMIT=1000)
class APITests(TestCase):
    @override_settings(ORGANIZATION_NAME='ООО «Пример» <script>alert(1)</script>')
    def test_organization_name_is_escaped_in_page_and_used_in_missing_owner_response(self):
        with override_settings(DEMO_MODE=False):
            response = self.client.get("/")
        self.assertContains(response, 'Сервис организации ООО «Пример» &lt;script&gt;alert(1)&lt;/script&gt;')
        self.assertNotContains(response, '<script>alert(1)</script>')
        data = self.lookup(key="demo-missing")
        self.assertFalse(data["can_request"])
        self.assertIn('поддержку ООО «Пример» <script>alert(1)</script>', data["message"])
        self.assertEqual(data["owner"]["name"], 'Сотрудник организации ООО «Пример» <script>alert(1)</script>')

    @override_settings(ORGANIZATION_NAME="")
    def test_organization_name_is_optional_and_does_not_require_directory_refresh(self):
        data = self.lookup()
        with override_settings(ORGANIZATION_NAME="Пример"), patch("portal.directory.DirectoryClient.pages") as pages:
            renamed = self.lookup()
        pages.assert_not_called()
        self.assertEqual(data["owner"], renamed["owner"])
        response = self.client.get("/")
        self.assertContains(response, "Сервис работает только с ресурсами нашей организации.")

    def test_360_link_is_accepted_and_retained_in_request(self):
        link = "https://disk.360.yandex.ru/i/demo-file"
        response = self.client.post("/api/resource-lookups", {"resource_url": link}, content_type="application/json")
        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json()["resource_url"], link)
        response = self.client.post("/api/access-requests", self.submission(response.json()["lookup_id"]), content_type="application/json")
        self.assertEqual(response.status_code, 202)
        self.assertEqual(AccessRequest.objects.get().resource_url, link)

    def test_upstream_status_is_logged_without_resource_or_credentials(self):
        error = upstream_error(500, "disk")
        with patch("portal.services.DiskClient.owner", side_effect=error), self.assertLogs("portal.views", level="WARNING") as logs:
            response = self.client.post("/api/resource-lookups", {
                "resource_url": "https://disk.360.yandex.ru/i/private-test-resource",
            }, content_type="application/json")
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["error"]["code"], "upstream_server_error")
        self.assertIn("upstream_status=500", " ".join(logs.output))
        self.assertNotIn("private-test-resource", " ".join(logs.output))
        self.assertNotIn("upstream_status", response.content.decode())

    def test_missing_and_plain_text_link_return_russian_validation_without_api_call(self):
        for value in ["", "это не ссылка"]:
            with self.subTest(value=value), patch("portal.services.DiskClient.owner") as owner:
                response = self.client.post("/api/resource-lookups", {"resource_url": value}, content_type="application/json")
                self.assertEqual(response.status_code, 400)
                self.assertEqual(response.json()["error"]["code"], "validation")
                self.assertRegex(response.json()["error"]["fields"]["resource_url"][0], "[А-Яа-я]")
                owner.assert_not_called()

    def lookup(self, client=None, key="demo-file"):
        response = (client or self.client).post("/api/resource-lookups", {"resource_url": f"https://disk.yandex.ru/d/{key}"}, content_type="application/json")
        self.assertEqual(response.status_code, 201, response.content)
        return response.json()

    def submission(self, lookup_id, **extra):
        return {"lookup_id": lookup_id, "requester_name": "Тестовый подрядчик", "account_email": "contractor@example.com",
                "contact_email": "contact@example.com", "rights": "read", "comment": "Нужен отчёт", **extra}

    def test_full_submission_and_same_lookup_idempotency(self):
        data = self.lookup()
        self.assertTrue(data["can_request"])
        self.assertNotIn("email", data["owner"])
        with patch("portal.directory.DirectoryClient.pages") as pages:
            response = self.client.post("/api/access-requests", self.submission(data["lookup_id"]), content_type="application/json")
            self.assertEqual(response.status_code, 202, response.content)
            repeated = self.client.post("/api/access-requests", self.submission(data["lookup_id"]), content_type="application/json")
            self.assertEqual(repeated.status_code, 200)
        pages.assert_not_called()
        self.assertEqual(AccessRequest.objects.count(), 1)
        self.assertEqual(repeated.json()["request_id"], response.json()["request_id"])
        item = AccessRequest.objects.get()
        self.assertEqual(item.owner_email, "anna.sokolova@company.example")
        self.assertEqual(item.contact_email, "contact@example.com")
        status = self.client.get(f"/api/access-requests/{item.pk}")
        self.assertEqual(status.status_code, 200)
        self.assertNotIn("contractor", status.content.decode())

    def test_other_session_cannot_submit_or_read_request(self):
        data = self.lookup()
        intruder = Client()
        response = intruder.post("/api/access-requests", self.submission(data["lookup_id"]), content_type="application/json")
        self.assertEqual(response.status_code, 404)
        self.client.post("/api/access-requests", self.submission(data["lookup_id"]), content_type="application/json")
        self.assertEqual(intruder.get(f"/api/access-requests/{AccessRequest.objects.get().pk}").status_code, 404)

    def test_browser_cannot_override_recipient(self):
        data = self.lookup()
        response = self.client.post("/api/access-requests", self.submission(data["lookup_id"], owner_email="attacker@example.com"), content_type="application/json")
        self.assertEqual(response.status_code, 400)
        self.assertEqual(AccessRequest.objects.count(), 0)

    def test_expired_lookup_does_not_send(self):
        data = self.lookup()
        ResourceLookup.objects.update(expires_at=timezone.now() - timedelta(seconds=1))
        response = self.client.post("/api/access-requests", self.submission(data["lookup_id"]), content_type="application/json")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(response.json()["error"]["code"], "lookup_expired")

    def test_owner_changed_between_lookup_and_submission(self):
        data = self.lookup()
        with patch("portal.services.DiskClient.owner", return_value="1002"):
            response = self.client.post("/api/access-requests", self.submission(data["lookup_id"]), content_type="application/json")
        self.assertEqual(response.status_code, 409)
        self.assertEqual(AccessRequest.objects.count(), 0)

    def test_duplicate_across_sessions_is_blocked_without_revealing_request_id(self):
        data = self.lookup()
        self.client.post("/api/access-requests", self.submission(data["lookup_id"]), content_type="application/json")
        other = Client()
        data2 = self.lookup(other)
        response = other.post("/api/access-requests", self.submission(data2["lookup_id"]), content_type="application/json")
        self.assertEqual(response.status_code, 409)
        self.assertNotIn(str(AccessRequest.objects.get().pk), response.content.decode())

    def test_inactive_and_missing_owner_cannot_receive_request(self):
        for key in ["demo-blocked", "demo-missing"]:
            with self.subTest(key=key):
                data = self.lookup(key=key)
                self.assertFalse(data["can_request"])
                response = self.client.post("/api/access-requests", self.submission(data["lookup_id"]), content_type="application/json")
                self.assertEqual(response.status_code, 409)

    def test_csrf_is_required(self):
        client = Client(enforce_csrf_checks=True)
        self.assertEqual(client.post("/api/resource-lookups", {"resource_url": "https://disk.yandex.ru/d/demo-file"}, content_type="application/json").status_code, 403)
        client.get("/")
        response = client.post("/api/resource-lookups", {"resource_url": "https://disk.yandex.ru/d/demo-file"},
                               content_type="application/json", HTTP_X_CSRFTOKEN=client.cookies["csrftoken"].value)
        self.assertEqual(response.status_code, 201)

    @override_settings(LOOKUP_RATE_LIMIT=1)
    def test_lookup_rate_limit(self):
        self.lookup()
        response = self.client.post("/api/resource-lookups", {"resource_url": "https://disk.yandex.ru/d/demo-file"}, content_type="application/json")
        self.assertEqual(response.status_code, 429)

    def test_unknown_link_and_invalid_json_have_safe_errors(self):
        self.assertEqual(self.client.post("/api/resource-lookups", "{", content_type="application/json").status_code, 400)
        self.assertEqual(self.client.post("/api/resource-lookups", {"resource_url": 123}, content_type="application/json").status_code, 400)
        response = self.client.post("/api/resource-lookups", {"resource_url": "https://disk.yandex.ru/d/demo-external"}, content_type="application/json")
        self.assertEqual(response.status_code, 404)
        self.assertIn("или", response.json()["error"]["message"])

    def test_email_header_injection_rejected(self):
        data = self.lookup()
        response = self.client.post("/api/access-requests", self.submission(data["lookup_id"], contact_email="a@example.com\r\nBcc: other@example.com"), content_type="application/json")
        self.assertEqual(response.status_code, 400)

    def test_failed_external_lookup_does_not_block_next_lookup_in_same_session(self):
        for code, status in [("outside_org", 404), ("resource_unavailable", 404),
                             ("integration_forbidden", 503), ("upstream_timeout", 504)]:
            with self.subTest(code=code):
                with patch("portal.services.DiskClient.owner", side_effect=ServiceError(code, "Ресурс недоступен", status)), \
                     patch("portal.services.resolve_employee") as directory:
                    response = self.client.post("/api/resource-lookups", {
                        "resource_url": "https://disk.yandex.ru/d/external",
                    }, content_type="application/json")
                self.assertEqual(response.status_code, status)
                self.assertEqual(response.json()["error"]["code"], code)
                directory.assert_not_called()
                self.assertTrue(self.lookup()["can_request"])
