import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from django.db import close_old_connections
from django.test import TransactionTestCase, override_settings, skipUnlessDBFeature

from portal.directory import refresh_directory
from portal.links import normalize_link
from portal.mailqueue import claim_message
from portal.models import DirectoryState, Employee
from portal.services import enqueue_request, find_owner, get_lookup

from .test_directory import user


@skipUnlessDBFeature("has_select_for_update")
@override_settings(DEMO_MODE=True, DIRECTORY_NAMESPACE="demo:12345", DIRECTORY_WAIT_SECONDS=5)
class DatabaseConcurrencyTests(TransactionTestCase):
    def in_connection(self, callable):
        close_old_connections()
        try:
            return callable()
        finally:
            close_old_connections()

    def test_two_refreshes_share_one_download(self):
        DirectoryState.objects.create(namespace="demo:12345")
        downloading, release, waiting = threading.Event(), threading.Event(), threading.Event()
        actual_sleep = time.sleep
        def pages():
            downloading.set()
            if not release.wait(timeout=4):
                raise AssertionError("Concurrent refresh did not wait")
            yield [user(1)]
        def wait_for_lease(seconds):
            waiting.set()
            actual_sleep(seconds)
        with patch("portal.directory.DirectoryClient.pages", side_effect=pages) as get_pages, patch("portal.directory.time.sleep", side_effect=wait_for_lease):
            with ThreadPoolExecutor(max_workers=2) as pool:
                first = pool.submit(self.in_connection, refresh_directory)
                self.assertTrue(downloading.wait(timeout=3))
                second = pool.submit(self.in_connection, refresh_directory)
                self.assertTrue(waiting.wait(timeout=3))
                release.set()
                self.assertEqual(first.result(timeout=5).generation, 1)
                self.assertEqual(second.result(timeout=5).generation, 1)
        self.assertEqual(get_pages.call_count, 1)
        self.assertEqual(Employee.objects.count(), 1)

    def test_two_workers_cannot_claim_same_letter(self):
        lookup = find_owner(normalize_link("https://disk.yandex.ru/d/demo-file"), "parallel-test")
        enqueue_request(get_lookup(lookup["lookup_id"], "parallel-test"), {
            "requester_name": "Тест", "account_email": "test@example.com", "contact_email": "test@example.com",
            "rights": "read", "comment": "",
        })
        barrier = threading.Barrier(2)
        def claim():
            barrier.wait(timeout=3)
            item = claim_message()
            return item.pk if item else None
        with ThreadPoolExecutor(max_workers=2) as pool:
            tasks = [pool.submit(self.in_connection, claim) for _ in range(2)]
            claimed = [task.result(timeout=5) for task in tasks]
        self.assertEqual(sum(item is not None for item in claimed), 1)
