import asyncio
import time
from unittest.mock import patch

import httpx
from django.test import SimpleTestCase, override_settings

from portal.clients import DirectoryClient, DiskClient, api_get, upstream_error
from portal.errors import ServiceError


@override_settings(DEMO_MODE=False, YANDEX_ORG_ID="12345", YANDEX_DISK_TOKEN="test-token", API_RETRIES=0)
class ClientTests(SimpleTestCase):
    def test_upstream_failures_do_not_claim_a_resource_is_outside_the_org(self):
        for status, code in [(429, "upstream_rate_limited"), (500, "upstream_server_error"),
                             (502, "upstream_server_error"), (403, "integration_forbidden")]:
            with self.subTest(status=status):
                error = upstream_error(status, "disk")
                self.assertEqual(error.code, code)
                self.assertEqual(error.upstream_status, status)
                self.assertEqual(error.upstream_service, "disk")
                self.assertNotIn("другой организации", error.message)
                self.assertNotIn("не найдены", error.message)

    def test_360_public_url_is_sent_as_a_parameter_without_changing_host(self):
        link = "https://disk.360.yandex.ru/i/Example123"
        seen = []
        def respond(request):
            seen.append(request)
            return httpx.Response(200, json={"accesses": [{"type": "owner", "id": 1001, "org_id": 12345}]})
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with patch("portal.clients.httpx.AsyncClient", return_value=client):
            self.assertEqual(DiskClient().owner(link), "1001")
        self.assertEqual(seen[0].url.host, "cloud-api.yandex.net")
        self.assertEqual(seen[0].url.params["public_key"], link)

    @override_settings(API_DEADLINE_SECONDS=0.05, API_RETRIES=2)
    def test_deadline_includes_retry_backoff(self):
        calls = []
        def respond(request):
            calls.append(request)
            return httpx.Response(503)
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with patch("portal.clients.httpx.AsyncClient", return_value=client), self.assertRaises(ServiceError) as error:
            DiskClient().owner("https://disk.yandex.ru/d/retry")
        self.assertEqual(error.exception.code, "upstream_timeout")
        self.assertEqual(len(calls), 1)
        self.assertTrue(client.is_closed)

    @override_settings(API_DEADLINE_SECONDS=0.05)
    def test_stalled_response_is_cancelled_and_next_lookup_works(self):
        cancelled = []
        async def stalled(request):
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(True)
        client = httpx.AsyncClient(transport=httpx.MockTransport(stalled))
        started = time.monotonic()
        with patch("portal.clients.httpx.AsyncClient", return_value=client), self.assertRaises(ServiceError) as error:
            DiskClient().owner("https://disk.yandex.ru/d/external")
        self.assertEqual(error.exception.code, "upstream_timeout")
        self.assertEqual(error.exception.status, 504)
        self.assertTrue(cancelled)
        self.assertTrue(client.is_closed)
        self.assertLess(time.monotonic() - started, 1)
        healthy = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, json={
            "accesses": [{"type": "owner", "id": 1001, "org_id": 12345}],
        })))
        with patch("portal.clients.httpx.AsyncClient", return_value=healthy):
            self.assertEqual(DiskClient().owner("https://disk.yandex.ru/d/valid"), "1001")

    @override_settings(API_DEADLINE_SECONDS=0.05)
    def test_deadline_includes_slow_body_and_closes_stream(self):
        class SlowBody(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                yield b'{"accesses":'
                while True:
                    await asyncio.sleep(0.01)
                    yield b' '
            async def aclose(self):
                self.closed = True
        body = SlowBody()
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(200, stream=body)))
        with patch("portal.clients.httpx.AsyncClient", return_value=client), self.assertRaises(ServiceError) as error:
            DiskClient().owner("https://disk.yandex.ru/d/slow")
        self.assertEqual(error.exception.status, 504)
        self.assertTrue(body.closed)
        self.assertTrue(client.is_closed)

    def test_finds_owner_not_first_user_with_write_access(self):
        with patch("portal.clients.api_get", return_value={"accesses": [
            {"type": "user", "id": 88, "rights": ["write"]},
            {"type": "owner", "id": 1001, "org_id": 12345},
        ]}):
            self.assertEqual(DiskClient().owner("https://disk.yandex.ru/d/abc"), "1001")

    def test_owner_must_belong_to_configured_org(self):
        with patch("portal.clients.api_get", return_value={"accesses": [{"type": "owner", "id": 1, "org_id": 54321}]}):
            with self.assertRaises(ServiceError) as error:
                DiskClient().owner("x")
            self.assertEqual(error.exception.code, "outside_org")

    def test_ambiguous_owner_is_not_guessed(self):
        with patch("portal.clients.api_get", return_value={"accesses": [{"type": "owner"}, {"type": "owner"}]}):
            with self.assertRaises(ServiceError) as error:
                DiskClient().owner("x")
            self.assertEqual(error.exception.code, "owner_missing")

    def test_credentials_go_only_to_fixed_api_with_encoded_parameter(self):
        link = "https://disk.yandex.ru/public/?hash=abc%2Bdef%3D"
        seen = []
        def respond(request):
            seen.append(request)
            return httpx.Response(200, json={"accesses": [{"type": "owner", "id": 1001, "org_id": 12345}]})
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with patch("portal.clients.httpx.AsyncClient", return_value=client):
            self.assertEqual(DiskClient().owner(link), "1001")
        self.assertEqual(seen[0].url.host, "cloud-api.yandex.net")
        self.assertEqual(seen[0].url.params["public_key"], link)
        self.assertEqual(seen[0].headers["Authorization"], "OAuth test-token")

    def test_redirects_are_not_followed(self):
        seen = []
        def respond(request):
            seen.append(request)
            return httpx.Response(302, headers={"Location": "https://evil.test/"})
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond), follow_redirects=False)
        with patch("portal.clients.httpx.AsyncClient", return_value=client), self.assertRaises(ServiceError):
            DiskClient().owner("x")
        self.assertEqual(len(seen), 1)

    def test_network_errors_do_not_expose_tokens(self):
        def respond(request):
            raise httpx.ConnectError("test-token secret details", request=request)
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with patch("portal.clients.httpx.AsyncClient", return_value=client):
            with self.assertRaises(ServiceError) as error:
                DiskClient().owner("x")
        self.assertNotIn("test-token", str(error.exception))

    @override_settings(API_RETRIES=2)
    def test_disk_timeout_is_reported_without_repeating_slow_lookup(self):
        seen = []
        def respond(request):
            seen.append(request)
            raise httpx.ReadTimeout("secret upstream details", request=request)
        client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
        with patch("portal.clients.httpx.AsyncClient", return_value=client), patch("portal.clients.asyncio.sleep") as sleep:
            with self.assertRaises(ServiceError) as error:
                DiskClient().owner("https://disk.yandex.ru/d/slow")
        self.assertEqual(error.exception.code, "upstream_timeout")
        self.assertEqual(error.exception.status, 504)
        self.assertNotIn("secret", str(error.exception))
        self.assertEqual(len(seen), 1)
        sleep.assert_not_called()

    @override_settings(API_RETRIES=2)
    def test_inaccessible_disk_resource_is_not_retried(self):
        for status, code in [(403, "integration_forbidden"), (404, "resource_unavailable")]:
            with self.subTest(status=status):
                seen = []
                def respond(request):
                    seen.append(request)
                    return httpx.Response(status)
                client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
                with patch("portal.clients.httpx.AsyncClient", return_value=client), self.assertRaises(ServiceError) as error:
                    DiskClient().owner("https://disk.yandex.ru/d/external")
                self.assertEqual(error.exception.code, code)
                self.assertEqual(len(seen), 1)

    @override_settings(API_RETRIES=2)
    def test_retries_temporary_errors_then_succeeds(self):
        replies = iter([httpx.Response(503), httpx.Response(429), httpx.Response(200, json={"ok": True})])
        client = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: next(replies)))
        with patch("portal.clients.httpx.AsyncClient", return_value=client), patch("portal.clients.asyncio.sleep"):
            self.assertEqual(api_get("/test", "token", {}, "directory"), {"ok": True})

    @override_settings(DIRECTORY_PAGE_SIZE=2)
    def test_downloads_every_directory_page(self):
        pages = [{"items": [{"id": "1"}, {"id": "2"}], "total": 3, "limit": 2, "offset": 0},
                 {"items": [{"id": "3"}], "total": 3, "limit": 2, "offset": 2}]
        with patch("portal.clients.api_get", side_effect=pages) as get:
            self.assertEqual(list(DirectoryClient().pages()), [pages[0]["items"], pages[1]["items"]])
        self.assertEqual(get.call_args_list[1].args[2], {"limit": 2, "offset": 2})

    @override_settings(DIRECTORY_PAGE_SIZE=2)
    def test_incomplete_directory_page_is_an_error(self):
        with patch("portal.clients.api_get", return_value={"items": [], "total": 3, "limit": 2, "offset": 0}):
            with self.assertRaises(ServiceError) as error:
                list(DirectoryClient().pages())
            self.assertEqual(error.exception.code, "directory_incomplete")

    @override_settings(DIRECTORY_PAGE_SIZE=2)
    def test_changed_total_aborts_pagination(self):
        pages = [{"items": [{}, {}], "total": 3, "limit": 2, "offset": 0},
                 {"items": [{}], "total": 4, "limit": 2, "offset": 2}]
        with patch("portal.clients.api_get", side_effect=pages), self.assertRaises(ServiceError):
            list(DirectoryClient().pages())
