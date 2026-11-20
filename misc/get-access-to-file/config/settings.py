import os
from pathlib import Path

from django.core.exceptions import ImproperlyConfigured
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env", override=False)


def boolean(name, default=False):
    return os.getenv(name, str(default)).lower() in {"1", "true", "yes"}


def integer(name, default, minimum=1):
    value = int(os.getenv(name, str(default)))
    if value < minimum:
        raise ImproperlyConfigured(f"{name} must be >= {minimum}")
    return value


DEMO_MODE = boolean("DEMO_MODE", True)
DEBUG = boolean("DEBUG", DEMO_MODE)
SECRET_KEY = os.getenv("DJANGO_SECRET_KEY", "local-demo-only-do-not-use-in-production-2f59ec")
YANDEX_ORG_ID = os.getenv("YANDEX_ORG_ID", "12345" if DEMO_MODE else "")
ORGANIZATION_NAME = os.getenv("ORGANIZATION_NAME", "").strip()
YANDEX_DISK_TOKEN = os.getenv("YANDEX_DISK_TOKEN", "")
YANDEX_DIRECTORY_TOKEN = os.getenv("YANDEX_DIRECTORY_TOKEN", "") or YANDEX_DISK_TOKEN
DIRECTORY_NAMESPACE = f"{'demo' if DEMO_MODE else 'live'}:{YANDEX_ORG_ID}"
if not DEMO_MODE:
    if len(SECRET_KEY) < 50 or SECRET_KEY.startswith("local-demo"):
        raise ImproperlyConfigured("Set a random DJANGO_SECRET_KEY (at least 50 characters).")
    if not YANDEX_ORG_ID.isdigit() or not YANDEX_DISK_TOKEN or not YANDEX_DIRECTORY_TOKEN:
        raise ImproperlyConfigured("Set YANDEX_ORG_ID and Yandex API tokens in the environment.")
    if DEBUG:
        raise ImproperlyConfigured("DEBUG must be false outside demo mode.")

ALLOWED_HOSTS = [x.strip() for x in os.getenv("ALLOWED_HOSTS", "localhost,127.0.0.1,[::1]").split(",") if x.strip()]
CSRF_TRUSTED_ORIGINS = [x.strip() for x in os.getenv("CSRF_TRUSTED_ORIGINS", "").split(",") if x.strip()]
INSTALLED_APPS = ["django.contrib.contenttypes", "django.contrib.sessions", "django.contrib.staticfiles", "portal"]
MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "whitenoise.middleware.WhiteNoiseMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "portal.middleware.ResponseHeadersMiddleware",
]
ROOT_URLCONF = "config.urls"
WSGI_APPLICATION = "config.wsgi.application"
TEMPLATES = [{
    "BACKEND": "django.template.backends.django.DjangoTemplates",
    "DIRS": [], "APP_DIRS": True,
    "OPTIONS": {"context_processors": ["django.template.context_processors.request"]},
}]

DATA_DIR = Path(os.getenv("DATA_DIR", str(BASE_DIR / "data")))
DATA_DIR.mkdir(parents=True, exist_ok=True)
if os.getenv("DB_HOST"):
    DATABASES = {"default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": os.environ["DB_HOST"], "PORT": os.getenv("DB_PORT", "5432"),
        "NAME": os.getenv("DB_NAME", "disk_access"),
        "USER": os.getenv("DB_USER", "disk_access"), "PASSWORD": os.getenv("DB_PASSWORD", ""),
        "CONN_MAX_AGE": 60, "OPTIONS": {"connect_timeout": 10},
    }}
else:
    DATABASES = {"default": {
        "ENGINE": "django.db.backends.sqlite3", "NAME": DATA_DIR / "db.sqlite3",
        "OPTIONS": {"timeout": 20, "transaction_mode": "IMMEDIATE"},
    }}

LANGUAGE_CODE = "ru"
TIME_ZONE = "UTC"
USE_TZ = True
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
STATIC_URL = "/static/"
STATIC_ROOT = BASE_DIR / "staticfiles"
WHITENOISE_USE_FINDERS = DEBUG
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
SESSION_COOKIE_AGE = 86400
SESSION_COOKIE_SECURE = boolean("COOKIE_SECURE", not DEMO_MODE)
CSRF_COOKIE_SECURE = SESSION_COOKIE_SECURE
SECURE_SSL_REDIRECT = boolean("SECURE_SSL_REDIRECT", False)
SECURE_REDIRECT_EXEMPT = [r"^healthz$"]
SECURE_HSTS_SECONDS = integer("SECURE_HSTS_SECONDS", 0, 0)
SECURE_HSTS_INCLUDE_SUBDOMAINS = boolean("SECURE_HSTS_INCLUDE_SUBDOMAINS", False)
SECURE_HSTS_PRELOAD = boolean("SECURE_HSTS_PRELOAD", False)
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_REFERRER_POLICY = "no-referrer"
TRUST_PROXY_HEADERS = boolean("TRUST_PROXY_HEADERS", False)
if TRUST_PROXY_HEADERS:
    SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
CSRF_FAILURE_VIEW = "portal.views.csrf_failure"
DATA_UPLOAD_MAX_MEMORY_SIZE = 16384

API_TIMEOUT = integer("API_TIMEOUT", 10)
API_DEADLINE_SECONDS = integer("API_DEADLINE_SECONDS", 20)
API_RETRIES = integer("API_RETRIES", 2, 0)
DIRECTORY_PAGE_SIZE = integer("DIRECTORY_PAGE_SIZE", 100)
DIRECTORY_MAX_PAGES = integer("DIRECTORY_MAX_PAGES", 1000)
DIRECTORY_COOLDOWN_SECONDS = integer("DIRECTORY_COOLDOWN_SECONDS", 60)
DIRECTORY_LEASE_SECONDS = integer("DIRECTORY_LEASE_SECONDS", 180)
DIRECTORY_WAIT_SECONDS = integer("DIRECTORY_WAIT_SECONDS", 5)
LOOKUP_TTL_SECONDS = integer("LOOKUP_TTL_SECONDS", 1800)
LOOKUP_RATE_LIMIT = integer("LOOKUP_RATE_LIMIT", 30)
REQUEST_RATE_LIMIT = integer("REQUEST_RATE_LIMIT", 5)
REQUEST_EMAIL_RATE_LIMIT = integer("REQUEST_EMAIL_RATE_LIMIT", 3)
REQUEST_DEDUP_SECONDS = integer("REQUEST_DEDUP_SECONDS", 86400)
MAIL_LEASE_SECONDS = integer("MAIL_LEASE_SECONDS", 120)
MAIL_MAX_ATTEMPTS = integer("MAIL_MAX_ATTEMPTS", 5)
MAIL_RETRY_BASE_SECONDS = integer("MAIL_RETRY_BASE_SECONDS", 30)

MAIL_TRANSPORT = os.getenv("MAIL_TRANSPORT", "file")
if MAIL_TRANSPORT not in {"file", "smtp"}:
    raise ImproperlyConfigured("MAIL_TRANSPORT must be file or smtp")
EMAIL_BACKEND = (
    "django.core.mail.backends.smtp.EmailBackend" if MAIL_TRANSPORT == "smtp"
    else "django.core.mail.backends.filebased.EmailBackend"
)
EMAIL_FILE_PATH = DATA_DIR / "mail"
EMAIL_HOST = os.getenv("SMTP_HOST", "localhost")
EMAIL_PORT = integer("SMTP_PORT", 587)
EMAIL_HOST_USER = os.getenv("SMTP_USER", "")
EMAIL_HOST_PASSWORD = os.getenv("SMTP_PASSWORD", "")
EMAIL_USE_TLS = boolean("SMTP_USE_TLS", True)
EMAIL_USE_SSL = boolean("SMTP_USE_SSL", False)
EMAIL_TIMEOUT = integer("SMTP_TIMEOUT", 15)
if EMAIL_USE_TLS and EMAIL_USE_SSL:
    raise ImproperlyConfigured("Use either SMTP_USE_TLS or SMTP_USE_SSL")
if MAIL_LEASE_SECONDS <= EMAIL_TIMEOUT * 3:
    raise ImproperlyConfigured("MAIL_LEASE_SECONDS must exceed 3 * SMTP_TIMEOUT")
DEFAULT_FROM_EMAIL = os.getenv("MAIL_FROM", "disk-access@company.example")
MESSAGE_ID_DOMAIN = os.getenv("MESSAGE_ID_DOMAIN", "disk-access.local")

LOGGING = {
    "version": 1, "disable_existing_loggers": False,
    "handlers": {"console": {"class": "logging.StreamHandler"}},
    "root": {"handlers": ["console"], "level": "WARNING"},
    "loggers": {"httpx": {"level": "WARNING"}, "httpcore": {"level": "WARNING"}},
}
