import email.policy
import socketserver
import threading
from email.parser import BytesParser

from django.test import TestCase, override_settings

from portal.links import normalize_link
from portal.mailqueue import process_one
from portal.services import enqueue_request, find_owner, get_lookup


class CaptureSMTP(socketserver.StreamRequestHandler):
    def handle(self):
        self.wfile.write(b"220 localhost test SMTP\r\n")
        while True:
            line = self.rfile.readline()
            if not line:
                return
            command = line.split(b" ", 1)[0].strip().upper()
            if command in {b"EHLO", b"HELO"}:
                self.wfile.write(b"250-localhost\r\n250 SIZE 1000000\r\n")
            elif command == b"DATA":
                self.wfile.write(b"354 Send data\r\n")
                content = []
                while True:
                    body_line = self.rfile.readline()
                    if body_line in {b".\r\n", b""}:
                        break
                    content.append(body_line[1:] if body_line.startswith(b"..") else body_line)
                self.server.messages.append(b"".join(content))
                self.wfile.write(b"250 Queued\r\n")
            elif command == b"QUIT":
                self.wfile.write(b"221 Bye\r\n")
                return
            else:
                self.wfile.write(b"250 OK\r\n")


@override_settings(DEMO_MODE=True, DIRECTORY_NAMESPACE="demo:12345", MAIL_TRANSPORT="smtp",
                   EMAIL_BACKEND="django.core.mail.backends.smtp.EmailBackend",
                   EMAIL_HOST="127.0.0.1", EMAIL_HOST_USER="", EMAIL_HOST_PASSWORD="",
                   EMAIL_USE_TLS=False, EMAIL_USE_SSL=False, EMAIL_TIMEOUT=3)
class SMTPIntegrationTest(TestCase):
    def test_real_smtp_handshake_and_russian_mime_body(self):
        with socketserver.TCPServer(("127.0.0.1", 0), CaptureSMTP) as server:
            server.messages = []
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            try:
                lookup = find_owner(normalize_link("https://disk.yandex.ru/d/demo-file"), "smtp-test")
                request, _ = enqueue_request(get_lookup(lookup["lookup_id"], "smtp-test"), {
                    "requester_name": "Анна & Алексей", "account_email": "contractor@example.com",
                    "contact_email": "reply@example.com", "rights": "read", "comment": "Тест кириллицы и MIME",
                })
                with override_settings(EMAIL_PORT=server.server_address[1]):
                    process_one()
                request.refresh_from_db()
                self.assertEqual(request.status, "accepted")
                self.assertEqual(len(server.messages), 1)
                message = BytesParser(policy=email.policy.default).parsebytes(server.messages[0])
                self.assertEqual(str(message["To"]), "anna.sokolova@company.example")
                self.assertEqual(str(message["Reply-To"]), "reply@example.com")
                self.assertIn("Анна & Алексей", message.get_content())
                self.assertIn("Тест кириллицы и MIME", message.get_content())
            finally:
                server.shutdown()
                thread.join(timeout=3)
