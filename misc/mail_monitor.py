#!/usr/bin/env python3

import argparse
import imaplib
import logging
import logging.handlers
import os
import signal
import smtplib
import socket
import ssl
import sys
import time

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


# Каталог, в котором находится сам скрипт.
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_LOG_FILE = SCRIPT_DIR / "mail_monitor.log"


@dataclass
class Settings:
    # SMTP
    smtp_host: str = os.getenv("SMTP_HOST", "smtp.yandex.ru")
    smtp_port: int = int(os.getenv("SMTP_PORT", "465"))
    smtp_security: str = os.getenv("SMTP_SECURITY", "ssl").lower()

    # IMAP
    imap_host: str = os.getenv("IMAP_HOST", "imap.yandex.ru")
    imap_port: int = int(os.getenv("IMAP_PORT", "993"))
    imap_security: str = os.getenv("IMAP_SECURITY", "ssl").lower()

    # Авторизация
    username: str = os.getenv("MAIL_USERNAME", "login@domain.ru")
    password: str = os.getenv("MAIL_PASSWORD", "password") # Пароль приложений https://yandex.ru/support/id/ru/authorization/app-passwords

    # Периодичность и тайм-аут
    check_interval: int = int(os.getenv("CHECK_INTERVAL", "60"))
    connection_timeout: int = int(os.getenv("CONNECTION_TIMEOUT", "15"))

    # Лог по умолчанию создаётся рядом со скриптом
    log_file: str = os.getenv("LOG_FILE", str(DEFAULT_LOG_FILE))
    log_level: str = os.getenv("LOG_LEVEL", "INFO").upper()

    # Ротация логов: 10 файлов по 10 МБ
    log_max_bytes: int = int(
        os.getenv("LOG_MAX_BYTES", str(10 * 1024 * 1024))
    )
    log_backup_count: int = int(os.getenv("LOG_BACKUP_COUNT", "10"))


stop_requested = False


def setup_logging(settings: Settings) -> logging.Logger:
    """Настройка записи логов в файл и консоль."""
    logger = logging.getLogger("mail_monitor")
    logger.setLevel(getattr(logging, settings.log_level, logging.INFO))
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_path = Path(settings.log_file).expanduser()

    if not log_path.is_absolute():
        log_path = SCRIPT_DIR / log_path

    log_path = log_path.resolve()
    log_path.parent.mkdir(parents=True, exist_ok=True)

    file_handler = logging.handlers.RotatingFileHandler(
        filename=str(log_path),
        maxBytes=settings.log_max_bytes,
        backupCount=settings.log_backup_count,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    logger.info("LOG | Файл журнала: %s", log_path)

    return logger


def decode_response(value) -> str:
    """Преобразование ответа почтового сервера в читаемую строку."""
    if value is None:
        return ""

    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")

    if isinstance(value, (list, tuple)):
        return "; ".join(decode_response(item) for item in value)

    return str(value)


def format_exception(error: BaseException) -> str:
    """Формирование подробного описания исключения."""
    details = [
        f"type={type(error).__name__}",
        f"message={error}",
    ]

    error_number = getattr(error, "errno", None)
    if error_number is not None:
        details.append(f"errno={error_number}")

    reason = getattr(error, "reason", None)
    if reason:
        details.append(f"reason={reason}")

    return ", ".join(details)


def classify_network_error(
    logger: logging.Logger,
    protocol: str,
    host: str,
    port: int,
    error: BaseException,
) -> None:
    """Классификация и запись сетевой ошибки."""
    destination = f"{host}:{port}"

    if isinstance(error, socket.gaierror):
        logger.error(
            "%s | DNS_ERROR | Не удалось определить IP-адрес сервера %s | %s",
            protocol,
            destination,
            format_exception(error),
        )

    elif isinstance(error, (socket.timeout, TimeoutError)):
        logger.error(
            "%s | TIMEOUT | Сервер %s не ответил за отведённое время | %s",
            protocol,
            destination,
            format_exception(error),
        )

    elif isinstance(error, ConnectionRefusedError):
        logger.error(
            "%s | CONNECTION_REFUSED | Сервер %s отклонил TCP-соединение | %s",
            protocol,
            destination,
            format_exception(error),
        )

    elif isinstance(error, ConnectionResetError):
        logger.error(
            "%s | CONNECTION_RESET | Сервер %s сбросил соединение | %s",
            protocol,
            destination,
            format_exception(error),
        )

    elif isinstance(error, ssl.SSLCertVerificationError):
        logger.error(
            "%s | TLS_CERTIFICATE_ERROR | "
            "Ошибка проверки SSL-сертификата сервера %s | %s",
            protocol,
            destination,
            format_exception(error),
        )

    elif isinstance(error, ssl.SSLError):
        logger.error(
            "%s | TLS_ERROR | Ошибка SSL/TLS при подключении к %s | %s",
            protocol,
            destination,
            format_exception(error),
        )

    elif isinstance(error, OSError):
        logger.error(
            "%s | NETWORK_ERROR | "
            "Не удалось достучаться до сервера %s | %s",
            protocol,
            destination,
            format_exception(error),
        )

    else:
        logger.exception(
            "%s | UNEXPECTED_ERROR | "
            "Непредвиденная ошибка при подключении к %s",
            protocol,
            destination,
        )


def check_smtp(settings: Settings, logger: logging.Logger) -> bool:
    """Проверка SMTP-сервера."""
    started = time.monotonic()
    server: Optional[smtplib.SMTP] = None
    destination = f"{settings.smtp_host}:{settings.smtp_port}"

    logger.info(
        "SMTP | START | Начало проверки %s, security=%s",
        destination,
        settings.smtp_security,
    )

    try:
        ssl_context = ssl.create_default_context()

        if settings.smtp_security == "ssl":
            server = smtplib.SMTP_SSL(
                host=settings.smtp_host,
                port=settings.smtp_port,
                timeout=settings.connection_timeout,
                context=ssl_context,
            )

            logger.info(
                "SMTP | CONNECTED | TCP/SSL-соединение с %s установлено",
                destination,
            )

            code, response = server.ehlo()

        elif settings.smtp_security == "starttls":
            server = smtplib.SMTP(
                host=settings.smtp_host,
                port=settings.smtp_port,
                timeout=settings.connection_timeout,
            )

            logger.info(
                "SMTP | CONNECTED | TCP-соединение с %s установлено",
                destination,
            )

            code, response = server.ehlo()

            logger.info(
                "SMTP | EHLO_BEFORE_TLS | code=%s, response=%s",
                code,
                decode_response(response),
            )

            if code >= 400:
                raise smtplib.SMTPResponseException(code, response)

            code, response = server.starttls(context=ssl_context)

            logger.info(
                "SMTP | STARTTLS | code=%s, response=%s",
                code,
                decode_response(response),
            )

            code, response = server.ehlo()

        elif settings.smtp_security == "none":
            server = smtplib.SMTP(
                host=settings.smtp_host,
                port=settings.smtp_port,
                timeout=settings.connection_timeout,
            )

            logger.info(
                "SMTP | CONNECTED | TCP-соединение с %s установлено",
                destination,
            )

            code, response = server.ehlo()

        else:
            raise ValueError(
                "SMTP_SECURITY должен иметь значение: ssl, starttls или none"
            )

        logger.info(
            "SMTP | EHLO | code=%s, response=%s",
            code,
            decode_response(response),
        )

        if code >= 400:
            raise smtplib.SMTPResponseException(code, response)

        if settings.username:
            logger.info(
                "SMTP | AUTH_START | Начало авторизации пользователя %s",
                settings.username,
            )

            code, response = server.login(
                settings.username,
                settings.password,
            )

            logger.info(
                "SMTP | AUTH_OK | "
                "Авторизация успешна, code=%s, response=%s",
                code,
                decode_response(response),
            )

        else:
            logger.warning(
                "SMTP | AUTH_SKIPPED | "
                "Логин не задан, проверено только подключение к серверу"
            )

        logger.info(
            "SMTP | SUCCESS | Проверка успешно завершена за %.3f сек.",
            time.monotonic() - started,
        )
        return True

    except smtplib.SMTPAuthenticationError as error:
        logger.error(
            "SMTP | AUTH_ERROR | "
            "Сервер доступен, но отклонил авторизацию "
            "| code=%s, response=%s",
            error.smtp_code,
            decode_response(error.smtp_error),
        )

    except smtplib.SMTPConnectError as error:
        logger.error(
            "SMTP | SERVER_CONNECT_ERROR | "
            "TCP-соединение установлено, но SMTP-сервер "
            "отклонил подключение | code=%s, response=%s",
            error.smtp_code,
            decode_response(error.smtp_error),
        )

    except smtplib.SMTPHeloError as error:
        logger.error(
            "SMTP | HELO_ERROR | "
            "SMTP-сервер отклонил команду EHLO/HELO "
            "| code=%s, response=%s",
            error.smtp_code,
            decode_response(error.smtp_error),
        )

    except smtplib.SMTPResponseException as error:
        logger.error(
            "SMTP | SERVER_RESPONSE_ERROR | "
            "SMTP-сервер вернул ошибку "
            "| code=%s, response=%s",
            error.smtp_code,
            decode_response(error.smtp_error),
        )

    except smtplib.SMTPServerDisconnected as error:
        logger.error(
            "SMTP | SERVER_DISCONNECTED | "
            "SMTP-сервер неожиданно закрыл соединение | %s",
            format_exception(error),
        )

    except (
        socket.gaierror,
        socket.timeout,
        TimeoutError,
        ConnectionRefusedError,
        ConnectionResetError,
        ssl.SSLError,
        OSError,
    ) as error:
        classify_network_error(
            logger=logger,
            protocol="SMTP",
            host=settings.smtp_host,
            port=settings.smtp_port,
            error=error,
        )

    except ValueError as error:
        logger.error(
            "SMTP | CONFIG_ERROR | Ошибка конфигурации: %s",
            error,
        )

    except Exception:
        logger.exception(
            "SMTP | UNEXPECTED_ERROR | Непредвиденная ошибка"
        )

    finally:
        if server is not None:
            try:
                server.quit()
            except Exception as error:
                logger.debug(
                    "SMTP | CLOSE_WARNING | "
                    "Не удалось корректно выполнить QUIT: %s",
                    format_exception(error),
                )

    logger.error(
        "SMTP | FAILED | Проверка завершилась ошибкой за %.3f сек.",
        time.monotonic() - started,
    )
    return False


def check_imap(settings: Settings, logger: logging.Logger) -> bool:
    """Проверка IMAP-сервера."""
    started = time.monotonic()
    server = None
    destination = f"{settings.imap_host}:{settings.imap_port}"

    logger.info(
        "IMAP | START | Начало проверки %s, security=%s",
        destination,
        settings.imap_security,
    )

    try:
        ssl_context = ssl.create_default_context()

        if settings.imap_security == "ssl":
            server = imaplib.IMAP4_SSL(
                host=settings.imap_host,
                port=settings.imap_port,
                ssl_context=ssl_context,
                timeout=settings.connection_timeout,
            )

            logger.info(
                "IMAP | CONNECTED | TCP/SSL-соединение с %s установлено",
                destination,
            )

        elif settings.imap_security == "starttls":
            server = imaplib.IMAP4(
                host=settings.imap_host,
                port=settings.imap_port,
                timeout=settings.connection_timeout,
            )

            logger.info(
                "IMAP | CONNECTED | TCP-соединение с %s установлено",
                destination,
            )

            result, data = server.starttls(ssl_context=ssl_context)

            logger.info(
                "IMAP | STARTTLS | result=%s, response=%s",
                result,
                decode_response(data),
            )

            if result.upper() != "OK":
                logger.error(
                    "IMAP | STARTTLS_ERROR | "
                    "Сервер отклонил переход на TLS "
                    "| result=%s, response=%s",
                    result,
                    decode_response(data),
                )
                return False

        elif settings.imap_security == "none":
            server = imaplib.IMAP4(
                host=settings.imap_host,
                port=settings.imap_port,
                timeout=settings.connection_timeout,
            )

            logger.info(
                "IMAP | CONNECTED | TCP-соединение с %s установлено",
                destination,
            )

        else:
            raise ValueError(
                "IMAP_SECURITY должен иметь значение: ssl, starttls или none"
            )

        logger.info(
            "IMAP | GREETING | welcome=%s",
            decode_response(getattr(server, "welcome", None)),
        )

        if settings.username:
            logger.info(
                "IMAP | AUTH_START | Начало авторизации пользователя %s",
                settings.username,
            )

            result, data = server.login(
                settings.username,
                settings.password,
            )

            if result.upper() != "OK":
                logger.error(
                    "IMAP | AUTH_ERROR | "
                    "Сервер доступен, но авторизация отклонена "
                    "| result=%s, response=%s",
                    result,
                    decode_response(data),
                )
                return False

            logger.info(
                "IMAP | AUTH_OK | "
                "Авторизация успешна, result=%s, response=%s",
                result,
                decode_response(data),
            )

        else:
            logger.warning(
                "IMAP | AUTH_SKIPPED | "
                "Логин не задан, проверено только подключение к серверу"
            )

        result, data = server.noop()

        logger.info(
            "IMAP | NOOP | result=%s, response=%s",
            result,
            decode_response(data),
        )

        if result.upper() != "OK":
            logger.error(
                "IMAP | SERVER_RESPONSE_ERROR | "
                "Сервер вернул ошибку на команду NOOP "
                "| result=%s, response=%s",
                result,
                decode_response(data),
            )
            return False

        logger.info(
            "IMAP | SUCCESS | Проверка успешно завершена за %.3f сек.",
            time.monotonic() - started,
        )
        return True

    except imaplib.IMAP4.readonly as error:
        logger.error(
            "IMAP | READONLY_ERROR | "
            "Сервер изменил состояние почтового ящика на read-only | %s",
            format_exception(error),
        )

    except imaplib.IMAP4.abort as error:
        logger.error(
            "IMAP | SERVER_ABORT | "
            "IMAP-сервер аварийно закрыл соединение | %s",
            format_exception(error),
        )

    except imaplib.IMAP4.error as error:
        logger.error(
            "IMAP | SERVER_OR_AUTH_ERROR | "
            "Сервер доступен, но вернул ошибку протокола "
            "или авторизации | %s",
            format_exception(error),
        )

    except (
        socket.gaierror,
        socket.timeout,
        TimeoutError,
        ConnectionRefusedError,
        ConnectionResetError,
        ssl.SSLError,
        OSError,
    ) as error:
        classify_network_error(
            logger=logger,
            protocol="IMAP",
            host=settings.imap_host,
            port=settings.imap_port,
            error=error,
        )

    except ValueError as error:
        logger.error(
            "IMAP | CONFIG_ERROR | Ошибка конфигурации: %s",
            error,
        )

    except Exception:
        logger.exception(
            "IMAP | UNEXPECTED_ERROR | Непредвиденная ошибка"
        )

    finally:
        if server is not None:
            try:
                server.logout()
            except Exception as error:
                logger.debug(
                    "IMAP | CLOSE_WARNING | "
                    "Не удалось корректно выполнить LOGOUT: %s",
                    format_exception(error),
                )

    logger.error(
        "IMAP | FAILED | Проверка завершилась ошибкой за %.3f сек.",
        time.monotonic() - started,
    )
    return False


def handle_stop_signal(signum, frame) -> None:
    """Обработчик остановки программы."""
    global stop_requested
    stop_requested = True


def interruptible_sleep(seconds: float) -> None:
    """Ожидание с возможностью остановки программы."""
    end_time = time.monotonic() + seconds

    while not stop_requested:
        remaining = end_time - time.monotonic()

        if remaining <= 0:
            return

        time.sleep(min(remaining, 1.0))


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Проверка доступности SMTP и IMAP "
            "с подробным логированием"
        )
    )

    parser.add_argument(
        "--once",
        action="store_true",
        help="Выполнить одну проверку и завершить работу",
    )

    return parser.parse_args()


def register_signal_handlers() -> None:
    """Регистрация обработчиков сигналов с учётом Windows."""
    signal.signal(signal.SIGINT, handle_stop_signal)

    # SIGTERM присутствует в современных версиях Python под Windows,
    # но проверка оставлена для совместимости.
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_stop_signal)

    # Обработка закрытия консоли через Ctrl+Break в Windows.
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, handle_stop_signal)


def main() -> int:
    args = parse_arguments()

    try:
        settings = Settings()
    except ValueError as error:
        print(
            f"Ошибка в числовых параметрах конфигурации: {error}",
            file=sys.stderr,
        )
        return 2

    try:
        logger = setup_logging(settings)
    except OSError as error:
        print(
            "Не удалось создать файл журнала рядом со скриптом.\n"
            f"Каталог: {SCRIPT_DIR}\n"
            f"Ошибка: {error}\n"
            "Проверьте права пользователя на запись в этот каталог.",
            file=sys.stderr,
        )
        return 2

    register_signal_handlers()

    if settings.check_interval <= 0:
        logger.error(
            "CONFIG | CHECK_INTERVAL должен быть больше нуля"
        )
        return 2

    if settings.connection_timeout <= 0:
        logger.error(
            "CONFIG | CONNECTION_TIMEOUT должен быть больше нуля"
        )
        return 2

    if settings.smtp_port <= 0 or settings.smtp_port > 65535:
        logger.error(
            "CONFIG | SMTP_PORT должен быть в диапазоне от 1 до 65535"
        )
        return 2

    if settings.imap_port <= 0 or settings.imap_port > 65535:
        logger.error(
            "CONFIG | IMAP_PORT должен быть в диапазоне от 1 до 65535"
        )
        return 2

    if settings.username and not settings.password:
        logger.warning(
            "CONFIG | MAIL_USERNAME задан, но MAIL_PASSWORD пустой"
        )

    logger.info(
        "MONITOR | STARTED | "
        "Интервал=%s сек., timeout=%s сек., "
        "SMTP=%s:%s (%s), IMAP=%s:%s (%s), authentication=%s",
        settings.check_interval,
        settings.connection_timeout,
        settings.smtp_host,
        settings.smtp_port,
        settings.smtp_security,
        settings.imap_host,
        settings.imap_port,
        settings.imap_security,
        "enabled" if settings.username else "disabled",
    )

    last_result = True

    while not stop_requested:
        cycle_started = time.monotonic()

        logger.info(
            "MONITOR | CYCLE_START | Начало цикла проверки"
        )

        smtp_ok = check_smtp(settings, logger)
        imap_ok = check_imap(settings, logger)
        last_result = smtp_ok and imap_ok

        cycle_duration = time.monotonic() - cycle_started

        logger.info(
            "MONITOR | CYCLE_END | "
            "SMTP=%s, IMAP=%s, общий результат=%s, "
            "длительность=%.3f сек.",
            "OK" if smtp_ok else "ERROR",
            "OK" if imap_ok else "ERROR",
            "OK" if last_result else "ERROR",
            cycle_duration,
        )

        if args.once:
            break

        sleep_time = max(
            0.0,
            settings.check_interval - cycle_duration,
        )

        if sleep_time == 0:
            logger.warning(
                "MONITOR | SLOW_CYCLE | "
                "Проверка заняла больше установленного интервала. "
                "Следующий цикл запускается сразу."
            )
        else:
            logger.info(
                "MONITOR | WAIT | Следующая проверка через %.1f сек.",
                sleep_time,
            )
            interruptible_sleep(sleep_time)

    logger.info(
        "MONITOR | STOPPED | Работа программы завершена"
    )

    return 0 if last_result else 1


if __name__ == "__main__":
    sys.exit(main())
