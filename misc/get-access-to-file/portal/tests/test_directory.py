import uuid
from datetime import timedelta
from unittest.mock import patch

from django.conf import settings
from django.test import TestCase, override_settings
from django.utils import timezone

from portal.directory import refresh_directory, resolve_employee
from portal.errors import ServiceError
from portal.models import DirectoryState, Employee


def user(uid, email="owner@company.example"):
    return {"id": str(uid), "email": email, "name": {"first": "Анна", "last": "Соколова"},
            "is_enabled": True, "is_dismissed": False, "is_robot": False}


@override_settings(DEMO_MODE=True, DIRECTORY_NAMESPACE="demo:12345")
class DirectoryTests(TestCase):
    def initial(self):
        with patch("portal.directory.DirectoryClient.pages", return_value=iter([[user(1)]])):
            return refresh_directory(force=True)

    def test_known_uid_never_triggers_api(self):
        self.initial()
        with patch("portal.directory.DirectoryClient.pages") as pages:
            for _ in range(3):
                self.assertEqual(resolve_employee("1").email, "owner@company.example")
        pages.assert_not_called()

    def test_missing_uid_refreshes_whole_snapshot(self):
        self.initial()
        DirectoryState.objects.update(next_refresh_at=None)
        with patch("portal.directory.DirectoryClient.pages", return_value=iter([[user(1, "changed@company.example")], [user(2)]])) as pages:
            self.assertEqual(resolve_employee("2").uid, "2")
        pages.assert_called_once()
        self.assertEqual(Employee.objects.get(uid="1").email, "changed@company.example")
        self.assertEqual(DirectoryState.objects.get().generation, 2)

    def test_unresolved_uid_does_not_cause_refresh_storm(self):
        with patch("portal.directory.DirectoryClient.pages", side_effect=lambda: iter([[user(1)]])) as pages:
            self.assertIsNone(resolve_employee("9999"))
            self.assertIsNone(resolve_employee("9999"))
            self.assertIsNone(resolve_employee("8888"))
        self.assertEqual(pages.call_count, 1)

    def test_partial_refresh_preserves_old_snapshot(self):
        old = self.initial()
        def broken_pages():
            yield [user(2)]
            raise ServiceError("upstream_network", "Ошибка сети")
        with patch("portal.directory.DirectoryClient.pages", side_effect=broken_pages), self.assertRaises(ServiceError):
            refresh_directory(force=True)
        state = DirectoryState.objects.get()
        self.assertEqual(state.generation, old.generation)
        self.assertEqual(state.synced_at, old.synced_at)
        self.assertIsNone(state.lease_token)
        self.assertEqual(list(Employee.objects.values_list("uid", flat=True)), ["1"])

    def test_successful_refresh_removes_departed_records(self):
        self.initial()
        with patch("portal.directory.DirectoryClient.pages", return_value=iter([[user(2)]])):
            refresh_directory(force=True)
        self.assertEqual(list(Employee.objects.values_list("uid", flat=True)), ["2"])

    def test_duplicate_uid_aborts_whole_refresh(self):
        self.initial()
        with patch("portal.directory.DirectoryClient.pages", return_value=iter([[user(2)], [user(2)]])), self.assertRaises(ServiceError):
            refresh_directory(force=True)
        self.assertTrue(Employee.objects.filter(uid="1").exists())

    @override_settings(DIRECTORY_WAIT_SECONDS=0)
    def test_other_process_lease_prevents_second_download(self):
        self.initial()
        DirectoryState.objects.update(lease_token=uuid.uuid4(), lease_until=timezone.now() + timedelta(minutes=2))
        with patch("portal.directory.DirectoryClient.pages") as pages:
            with self.assertRaises(ServiceError) as error:
                refresh_directory(force=True)
        self.assertEqual(error.exception.code, "directory_busy")
        pages.assert_not_called()

    def test_expired_lease_can_be_recovered(self):
        self.initial()
        DirectoryState.objects.update(lease_token=uuid.uuid4(), lease_until=timezone.now() - timedelta(seconds=1))
        with patch("portal.directory.DirectoryClient.pages", return_value=iter([[user(2)]])):
            refresh_directory(force=True)
        self.assertEqual(list(Employee.objects.values_list("uid", flat=True)), ["2"])

    def test_lost_lease_cannot_overwrite_newer_snapshot(self):
        self.initial()
        def replaced_lease():
            DirectoryState.objects.update(lease_token=uuid.uuid4())
            yield [user(2)]
        with patch("portal.directory.DirectoryClient.pages", side_effect=replaced_lease), self.assertRaises(ServiceError):
            refresh_directory(force=True)
        self.assertTrue(Employee.objects.filter(uid="1").exists())

    def test_bad_email_disables_contact(self):
        with patch("portal.directory.DirectoryClient.pages", return_value=iter([[user(1, "invalid")]])):
            refresh_directory(force=True)
        self.assertFalse(Employee.objects.get(uid="1").contactable)

    def test_demo_and_live_cache_are_isolated(self):
        self.initial()
        with override_settings(DIRECTORY_NAMESPACE="live:12345"):
            with patch("portal.directory.DirectoryClient.pages", return_value=iter([[user(2)]])):
                self.assertIsNone(resolve_employee("1"))
        self.assertEqual(DirectoryState.objects.count(), 2)
