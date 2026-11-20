from django.conf import settings
from django.core.management.base import BaseCommand, CommandError

from portal.directory import refresh_directory
from portal.errors import ServiceError
from portal.models import DirectoryState, Employee


class Command(BaseCommand):
    help = "Загрузить полный каталог. --if-empty использует существующий снимок без API-запроса."

    def add_arguments(self, parser):
        parser.add_argument("--if-empty", action="store_true")

    def handle(self, *args, **options):
        state = DirectoryState.objects.filter(namespace=settings.DIRECTORY_NAMESPACE).first()
        if options["if_empty"] and state and state.synced_at:
            self.stdout.write(f"Сохранённый каталог используется без обновления. Версия: {state.generation}.")
            return
        try:
            state = refresh_directory(force=not options["if_empty"])
        except ServiceError as exc:
            raise CommandError(f"{exc.code}: {exc.message}") from None
        count = Employee.objects.filter(namespace=settings.DIRECTORY_NAMESPACE).count()
        self.stdout.write(self.style.SUCCESS(f"Каталог обновлён: {count} сотрудников, версия {state.generation}."))
