from datetime import timedelta

from django.core.management.base import BaseCommand, CommandError
from django.utils import timezone

from portal.models import AccessRequest, RateBucket, RequestGate, ResourceLookup


class Command(BaseCommand):
    help = "Удалить технические счётчики и завершённые заявки старше N дней. По умолчанию только показать количество."

    def add_arguments(self, parser):
        parser.add_argument("--days", type=int, default=30)
        parser.add_argument("--apply", action="store_true")

    def handle(self, *args, **options):
        if options["days"] < 1:
            raise CommandError("--days должен быть положительным")
        now = timezone.now()
        cutoff = now - timedelta(days=options["days"])
        old_requests = AccessRequest.objects.filter(status__in=["accepted", "saved", "failed"], finished_at__lt=cutoff)
        old_lookups = ResourceLookup.objects.filter(expires_at__lt=cutoff, accessrequest__isnull=True)
        self.stdout.write(f"Завершённых заявок: {old_requests.count()}; старых результатов поиска без заявок: {old_lookups.count()}.")
        if not options["apply"]:
            self.stdout.write("Для удаления передайте --apply. Каталог сотрудников и ожидающие письма сохраняются.")
            return
        old_requests.delete()
        ResourceLookup.objects.filter(expires_at__lt=cutoff, accessrequest__isnull=True).delete()
        RateBucket.objects.filter(expires_at__lt=now).delete()
        RequestGate.objects.filter(blocked_until__lt=now).delete()
        self.stdout.write("Старые записи удалены. Для сессий используйте clearsessions; файлы тестовых писем очищаются отдельно.")
