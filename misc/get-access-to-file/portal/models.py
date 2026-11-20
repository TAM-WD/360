import uuid
from django.db import models


class DirectoryState(models.Model):
    namespace = models.CharField(max_length=100, primary_key=True)
    synced_at = models.DateTimeField(null=True)
    attempted_at = models.DateTimeField(null=True)
    next_refresh_at = models.DateTimeField(null=True)
    generation = models.PositiveIntegerField(default=0)
    lease_token = models.UUIDField(null=True)
    lease_until = models.DateTimeField(null=True)
    last_error = models.CharField(max_length=60, blank=True)


class Employee(models.Model):
    namespace = models.CharField(max_length=100)
    uid = models.CharField(max_length=32)
    name = models.CharField(max_length=300)
    email = models.EmailField(max_length=254, blank=True)
    is_enabled = models.BooleanField(default=False)
    is_dismissed = models.BooleanField(default=False)
    is_robot = models.BooleanField(default=False)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["namespace", "uid"], name="employee_namespace_uid")]

    @property
    def contactable(self):
        return bool(self.email and self.is_enabled and not self.is_dismissed and not self.is_robot)


class ResourceLookup(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    session_key = models.CharField(max_length=64, db_index=True)
    namespace = models.CharField(max_length=100)
    resource_url = models.URLField(max_length=2048)
    public_key = models.CharField(max_length=2048)
    resource_digest = models.CharField(max_length=64)
    owner_uid = models.CharField(max_length=32)
    created_at = models.DateTimeField(auto_now_add=True)
    expires_at = models.DateTimeField()


class AccessRequest(models.Model):
    class Status(models.TextChoices):
        QUEUED = "queued", "Заявка в очереди"
        SENDING = "sending", "Письмо отправляется"
        RETRY = "retry", "Повторная попытка запланирована"
        ACCEPTED = "accepted", "Письмо принято SMTP-сервером"
        SAVED = "saved", "Письмо сохранено в файл"
        FAILED = "failed", "Не удалось отправить письмо"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    lookup = models.OneToOneField(ResourceLookup, on_delete=models.PROTECT)
    namespace = models.CharField(max_length=100)
    session_key = models.CharField(max_length=64, db_index=True)
    resource_url = models.URLField(max_length=2048)
    owner_uid = models.CharField(max_length=32)
    owner_name = models.CharField(max_length=300)
    owner_email = models.EmailField(max_length=254)
    requester_name = models.CharField(max_length=150)
    account_email = models.EmailField(max_length=254)
    contact_email = models.EmailField(max_length=254)
    rights = models.CharField(max_length=5, choices=[("read", "Просмотр"), ("write", "Редактирование")])
    comment = models.TextField(blank=True)
    status = models.CharField(max_length=12, choices=Status.choices, default=Status.QUEUED)
    attempts = models.PositiveSmallIntegerField(default=0)
    available_at = models.DateTimeField(db_index=True)
    lease_token = models.UUIDField(null=True)
    lease_until = models.DateTimeField(null=True)
    last_error = models.CharField(max_length=100, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    finished_at = models.DateTimeField(null=True)

    class Meta:
        indexes = [models.Index(fields=["namespace", "status", "available_at"], name="outbox_pending")]


class RequestGate(models.Model):
    key = models.CharField(max_length=64, primary_key=True)
    blocked_until = models.DateTimeField()


class RateBucket(models.Model):
    key = models.CharField(max_length=64)
    bucket = models.BigIntegerField()
    count = models.PositiveIntegerField(default=0)
    expires_at = models.DateTimeField(db_index=True)

    class Meta:
        constraints = [models.UniqueConstraint(fields=["key", "bucket"], name="rate_bucket_unique")]
