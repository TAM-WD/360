from django.conf import settings


def organization_label():
    """Display name only; API authorization still uses YANDEX_ORG_ID."""
    return f"организации {settings.ORGANIZATION_NAME}" if settings.ORGANIZATION_NAME else "нашей организации"


def support_label():
    return f"поддержку {settings.ORGANIZATION_NAME}" if settings.ORGANIZATION_NAME else "поддержку организации"


def employee_display_name(employee):
    # Keep the configured name out of the cached directory snapshot.
    if employee and employee.name and employee.name != "Сотрудник организации":
        return employee.name
    return f"Сотрудник {organization_label()}"
