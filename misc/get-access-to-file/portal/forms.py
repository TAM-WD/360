from django import forms

from .links import normalize_link


class LookupForm(forms.Form):
    resource_url = forms.CharField(max_length=2048, error_messages={
        "required": "Вставьте ссылку на файл или папку в Яндекс Диске.",
        "max_length": "Ссылка слишком длинная. Скопируйте её заново через «Поделиться» в Яндекс Диске.",
    })

    def clean_resource_url(self):
        return normalize_link(self.cleaned_data["resource_url"])


class AccessRequestForm(forms.Form):
    lookup_id = forms.UUIDField()
    requester_name = forms.CharField(max_length=150)
    account_email = forms.EmailField(max_length=254)
    contact_email = forms.EmailField(max_length=254, required=False)
    rights = forms.ChoiceField(choices=[("read", "Просмотр"), ("write", "Редактирование")])
    comment = forms.CharField(max_length=2000, required=False)

    def clean_requester_name(self):
        value = self.cleaned_data["requester_name"]
        if any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise forms.ValidationError("Имя должно быть одной строкой.")
        return value

    def clean(self):
        cleaned = super().clean()
        for key in ("account_email", "contact_email"):
            if cleaned.get(key):
                cleaned[key] = cleaned[key].lower()
        if not cleaned.get("contact_email"):
            cleaned["contact_email"] = cleaned.get("account_email", "")
        return cleaned
