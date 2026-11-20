from django.urls import path
from portal import views

urlpatterns = [
    path("", views.index, name="index"),
    path("healthz", views.health, name="health"),
    path("api/resource-lookups", views.lookup, name="lookup"),
    path("api/access-requests", views.create_request, name="create_request"),
    path("api/access-requests/<uuid:request_id>", views.request_status, name="request_status"),
]
