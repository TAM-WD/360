from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from portal.models import DirectoryState, Employee


class Command(BaseCommand):
    help = "Только демо: убрать UID 1002 из локального снимка для проверки автоматического обновления."

    def handle(self, *args, **options):
        if not settings.DEMO_MODE:
            raise CommandError("Эта команда доступна только при DEMO_MODE=true.")
        with transaction.atomic():
            state = DirectoryState.objects.select_for_update().filter(namespace=settings.DIRECTORY_NAMESPACE).first()
            if state is None or not state.synced_at:
                raise CommandError("Сначала выполните sync_directory.")
            if state.lease_token:
                raise CommandError("Каталог сейчас обновляется. Повторите позже.")
            Employee.objects.filter(namespace=settings.DIRECTORY_NAMESPACE, uid="1002").delete()
            state.next_refresh_at = None
            state.save(update_fields=["next_refresh_at"])
        self.stdout.write("Теперь найдите https://disk.yandex.ru/d/demo-new — UID 1002 восстановится из полного каталога.")
