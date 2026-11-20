import smtplib
from datetime import timedelta
from unittest.mock import patch

from django.core import mail
from django.core.management import call_command
from django.core.management.base import CommandError
from django.test import TestCase, override_settings
from django.utils import timezone

from portal.links import normalize_link
from portal.mailqueue import claim_message, process_one
from portal.models import AccessRequest, Employee
from portal.services import enqueue_request, find_owner, get_lookup


@override_settings(DEMO_MODE=True, DIRECTORY_NAMESPACE="demo:12345", MAIL_TRANSPORT="smtp",
                   EMAIL_BACKEND="django.core.mail.backends.locmem.EmailBackend")
class MailQueueTests(TestCase):
    def setUp(self):
        data = find_owner(normalize_link("https://disk.yandex.ru/d/demo-file?path=%2FReport#view"), "test-session")
        self.item, _ = enqueue_request(get_lookup(data["lookup_id"], "test-session"), {
            "requester_name": "Подрядчик", "account_email": "person@example.com", "contact_email": "contact@example.com",
            "rights": "read", "comment": "Тестовый комментарий",
        })

    def test_sends_expected_letter_once(self):
        self.assertEqual(process_one(), self.item.pk)
        self.assertIsNone(process_one())
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "accepted")
        self.assertEqual(len(mail.outbox), 1)
        message = mail.outbox[0]
        self.assertEqual(message.to, ["anna.sokolova@company.example"])
        self.assertEqual(message.reply_to, ["contact@example.com"])
        self.assertIn("?path=%2FReport#view", message.body)
        self.assertIn("person@example.com", message.body)
        self.assertIn("не проверены", message.body)
        self.assertIn(str(self.item.pk), message.extra_headers["Message-ID"])

    def test_temporary_error_retries_with_same_message_id(self):
        with patch("portal.mailqueue.EmailMessage.send", side_effect=smtplib.SMTPServerDisconnected("private server details")):
            process_one()
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "retry")
        self.assertGreater(self.item.available_at, timezone.now())
        self.assertNotIn("private", self.item.last_error)
        AccessRequest.objects.filter(pk=self.item.pk).update(available_at=timezone.now() - timedelta(seconds=1))
        process_one()
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "accepted")
        self.assertEqual(self.item.attempts, 2)

    def test_permanent_smtp_failure_is_terminal(self):
        with patch("portal.mailqueue.EmailMessage.send", side_effect=smtplib.SMTPRecipientsRefused({"owner@example.com": (550, b"no mailbox")})):
            process_one()
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "failed")
        self.assertIsNone(process_one())

    def test_active_lease_excludes_other_worker_and_expired_lease_is_recovered(self):
        claimed = claim_message()
        self.assertIsNotNone(claimed)
        self.assertIsNone(claim_message())
        AccessRequest.objects.filter(pk=self.item.pk).update(lease_until=timezone.now() - timedelta(seconds=1))
        process_one()
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "accepted")
        self.assertEqual(self.item.attempts, 2)

    def test_inactive_owner_prevents_queued_send(self):
        Employee.objects.filter(uid="1001").update(is_enabled=False)
        with patch("portal.mailqueue.EmailMessage.send") as send:
            process_one()
        send.assert_not_called()
        self.item.refresh_from_db()
        self.assertEqual(self.item.last_error, "owner_unavailable")

    def test_local_email_update_is_used_without_directory_api_call(self):
        Employee.objects.filter(uid="1001").update(email="updated@company.example")
        with patch("portal.directory.DirectoryClient.pages") as pages:
            process_one()
        pages.assert_not_called()
        self.assertEqual(mail.outbox[0].to, ["updated@company.example"])

    def test_live_worker_does_not_send_demo_queue(self):
        with override_settings(DIRECTORY_NAMESPACE="live:12345"):
            self.assertIsNone(process_one())
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "queued")

    @override_settings(MAIL_TRANSPORT="file")
    def test_file_transport_is_not_labeled_delivered(self):
        process_one()
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "saved")

    @override_settings(MAIL_MAX_ATTEMPTS=2)
    def test_retry_limit(self):
        with patch("portal.mailqueue.EmailMessage.send", side_effect=TimeoutError):
            process_one()
            AccessRequest.objects.update(available_at=timezone.now() - timedelta(seconds=1))
            process_one()
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "failed")
        self.assertIsNone(process_one())

    def test_operator_can_retry_failed_but_not_successful_mail(self):
        import io
        AccessRequest.objects.filter(pk=self.item.pk).update(status="failed", attempts=5)
        call_command("retry_mail", str(self.item.pk), stdout=io.StringIO())
        self.item.refresh_from_db()
        self.assertEqual(self.item.status, "queued")
        self.assertEqual(self.item.attempts, 0)
        process_one()
        with self.assertRaises(CommandError):
            call_command("retry_mail", str(self.item.pk), stdout=io.StringIO())
