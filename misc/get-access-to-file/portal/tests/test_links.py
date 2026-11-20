from django.core.exceptions import ValidationError
from django.test import SimpleTestCase

from portal.links import normalize_link


class LinkTests(SimpleTestCase):
    def test_normalizes_scheme_and_host_without_losing_target(self):
        link = normalize_link("  http://DISK.YANDEX.RU/d/abc-123/folder/file.pdf?path=%2Ffile%20name#preview  ")
        self.assertEqual(link.url, "https://disk.yandex.ru/d/abc-123/folder/file.pdf?path=%2Ffile%20name#preview")
        self.assertEqual(link.public_key, "https://disk.yandex.ru/d/abc-123")

    def test_hash_is_not_decoded_twice_or_dropped(self):
        link = normalize_link("https://disk.yandex.ru/public/?hash=abc%2B%2F%3D&path=%2Freport")
        self.assertIn("hash=abc%2B%2F%3D&path=%2Freport", link.public_key)

    def test_supported_legacy_and_image_links(self):
        for url in ("https://yadi.sk/i/ABC", "https://disk.yandex.com/d/Abc",
                    "https://disk.360.yandex.ru/i/Example123", "https://disk.360.yandex.ru/d/Example123"):
            with self.subTest(url=url):
                self.assertEqual(normalize_link(url).url, url)

    def test_360_links_keep_original_host_and_nested_target(self):
        link = normalize_link(" https://DISK.360.YANDEX.RU/i/Example123/photo.jpg?path=%2Ffolder#preview ")
        self.assertEqual(link.url, "https://disk.360.yandex.ru/i/Example123/photo.jpg?path=%2Ffolder#preview")
        self.assertEqual(link.public_key, "https://disk.360.yandex.ru/i/Example123")
        hashed = normalize_link("https://disk.360.yandex.ru/public/?hash=abc%2B%3D&path=%2Ffile")
        self.assertEqual(hashed.public_key, hashed.url)

    def test_rejects_unsafe_or_nonpublic_inputs(self):
        urls = [
            "https://127.0.0.1/d/x", "https://disk.yandex.ru.evil.test/d/x", "javascript:alert(1)",
            "https://disk.360.yandex.ru.evil.test/i/x", "https://disk.360.yandex.ru@evil.test/i/x",
            "https://evil.360.yandex.ru/d/x", "https://disk.360.yandex.ru:8080/i/x",
            "https://user:pass@disk.yandex.ru/d/x", "https://disk.yandex.ru:8080/d/x",
            "https://disk.yandex.ru/client/disk/report", "https://disk.yandex.ru/public/?hash=",
            "https://disk.yandex.ru/public/?hash=a&hash=b", "https://disk.yandex.ru/d/x%0AInjected",
            "https://disk.yandex.ru/d/x\nInjected", "https://disk.yandex.ru/d/x%xx", "https://[invalid/d/x",
        ]
        for url in urls:
            with self.subTest(url=url), self.assertRaises(ValidationError):
                normalize_link(url)
