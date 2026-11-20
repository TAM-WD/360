import signal
import threading

from django.core.management.base import BaseCommand
from django.db import close_old_connections

from portal.mailqueue import process_one
from portal.models import AccessRequest


class Command(BaseCommand):
    help = "Обработчик постоянной очереди писем. --once обрабатывает готовые записи и завершается."

    def add_arguments(self, parser):
        parser.add_argument("--once", action="store_true")
        parser.add_argument("--limit", type=int, default=100)

    def handle(self, *args, **options):
        stopping = threading.Event()
        if not options["once"]:
            signal.signal(signal.SIGTERM, lambda *_: stopping.set())
            signal.signal(signal.SIGINT, lambda *_: stopping.set())
        processed = 0
        while not stopping.is_set():
            close_old_connections()
            request_id = process_one()
            if request_id is not None:
                processed += 1
                status = AccessRequest.objects.values_list("status", flat=True).get(pk=request_id)
                self.stdout.write(f"{request_id}: {status}")
            if options["once"] and (request_id is None or processed >= options["limit"]):
                break
            if request_id is None:
                stopping.wait(1)
        self.stdout.write(f"Обработано писем: {processed}.")
