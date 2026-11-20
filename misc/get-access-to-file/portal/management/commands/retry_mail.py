import uuid

from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from portal.models import AccessRequest


class Command(BaseCommand):
    help = "После устранения причины ошибки вернуть failed-заявку в очередь. Принятые SMTP письма не переотправляет."

    def add_arguments(self, parser):
        parser.add_argument("request_id", type=uuid.UUID)

    def handle(self, *args, **options):
        updated = AccessRequest.objects.filter(
            pk=options["request_id"], namespace=settings.DIRECTORY_NAMESPACE, status="failed",
        ).update(status="queued", attempts=0, last_error="", available_at=timezone.now(),
                 finished_at=None, lease_token=None, lease_until=None)
        if not updated:
            raise CommandError("В текущем режиме нет failed-заявки с этим ID.")
        self.stdout.write("Заявка возвращена в очередь. Запустите обработчик, если он остановлен.")
