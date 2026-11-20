import hashlib
import re
from dataclasses import dataclass
from urllib.parse import parse_qs, quote, urlsplit, urlunsplit

from django.core.exceptions import ValidationError

HOSTS = {"disk.yandex.ru", "disk.360.yandex.ru", "disk.yandex.com", "yadi.sk"}


@dataclass(frozen=True)
class DiskLink:
    url: str
    public_key: str

    @property
    def digest(self):
        return hashlib.sha256(self.url.encode()).hexdigest()


def normalize_link(value):
    if not isinstance(value, str):
        raise ValidationError("Вставьте ссылку на файл или папку в Яндекс Диске.")
    value = value.strip()
    if not value:
        raise ValidationError("Вставьте ссылку на файл или папку в Яндекс Диске.")
    if len(value) > 2048:
        raise ValidationError("Ссылка слишком длинная. Скопируйте её заново через «Поделиться» в Яндекс Диске.")
    if re.search(r"[\x00-\x20\x7f\\]", value):
        raise ValidationError("В ссылке есть пробелы или недопустимые символы. Скопируйте её заново через «Поделиться» в Яндекс Диске.")
    if re.search(r"%(?![0-9a-fA-F]{2})|%(?:0[0-9a-f]|1[0-9a-f]|7f)", value, re.I):
        raise ValidationError("Некорректное URL-кодирование ссылки.")
    try:
        parts = urlsplit(value)
        if parts.scheme.lower() not in {"http", "https"} or not parts.hostname:
            raise ValidationError("Введите полную ссылку, начинающуюся с https://. Например: https://disk.360.yandex.ru/i/…")
        if parts.hostname not in HOSTS:
            raise ValueError
        if parts.username or parts.password or parts.port not in {None, 443}:
            raise ValueError
    except ValueError:
        raise ValidationError("Нужна ссылка Яндекс Диска: disk.360.yandex.ru, disk.yandex.ru, disk.yandex.com или yadi.sk. Скопируйте её через «Поделиться».")
    host = parts.hostname
    path = quote(parts.path, safe="/%:@!$&'()*+,;=-._~")
    query = quote(parts.query, safe="%=&/?+:;,@!$'()*-._~")
    fragment = quote(parts.fragment, safe="%=&/?+:;,@!$'()*-._~")
    url = urlunsplit(("https", host, path, query, fragment))
    if len(url) > 2048:
        raise ValidationError("Ссылка слишком длинная.")
    short = re.fullmatch(r"/(d|i)/([A-Za-z0-9_-]+)(?:/.*)?", path)
    if short:
        # Ownership belongs to the published root; keep nested location in the displayed URL.
        key = f"https://{host}/{short.group(1)}/{short.group(2)}"
    elif path in {"/public", "/public/"}:
        hashes = parse_qs(query, keep_blank_values=True).get("hash", [])
        if len(hashes) != 1 or not hashes[0].strip():
            raise ValidationError("В полной публичной ссылке должен быть один непустой параметр hash.")
        key = urlunsplit(("https", host, path, query, ""))
    else:
        raise ValidationError("Эта ссылка не подходит для поиска владельца. Откройте файл или папку в Яндекс Диске и скопируйте ссылку через «Поделиться».")
    return DiskLink(url=url, public_key=key)
