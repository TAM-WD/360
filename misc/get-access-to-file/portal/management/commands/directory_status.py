from django.conf import settings
from django.core.management.base import BaseCommand

from portal.models import DirectoryState, Employee


class Command(BaseCommand):
    help = "Показать состояние локального каталога без обращения к API и вывода адресов сотрудников."

    def handle(self, *args, **options):
        state = DirectoryState.objects.filter(namespace=settings.DIRECTORY_NAMESPACE).first()
        if not state:
            self.stdout.write("Каталог ещё не загружен.")
            return
        self.stdout.write(f"Режим: {'demo' if settings.DEMO_MODE else 'live'}")
        self.stdout.write(f"Версия: {state.generation}")
        self.stdout.write(f"Сотрудников: {Employee.objects.filter(namespace=settings.DIRECTORY_NAMESPACE).count()}")
        self.stdout.write(f"Успешная синхронизация UTC: {state.synced_at}")
        self.stdout.write(f"Последняя ошибка: {state.last_error or 'нет'}")
