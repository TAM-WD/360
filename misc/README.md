# Описание
- [remove_appdata_telemost_and_fix_registry.ps1](https://github.com/TAM-WD/360/blob/main/misc/remove_appdata_telemost_and_fix_registry.ps1) — удалить per_user клиенты Телемоста (скрипт для пользователя)
- [remove_telemost_cu.ps1](https://github.com/TAM-WD/360/blob/main/misc/remove_telemost_cu.ps1) — удалить ассоциации telemost:// у пользователей (запуск через GPO*)
- [remove_oldexe_telemost_from_users.ps1](https://github.com/TAM-WD/360/blob/main/misc/remove_oldexe_telemost_from_users.ps1) — удалить старые EXE-установки Телемоста с ПК пользователей (запуск через GPO*)
- [delete_peruser_telemost_msi.ps1](https://github.com/TAM-WD/360/blob/main/misc/delete_peruser_telemost_msi.ps1) — удалить per_user MSI-установку Телемоста с ПК пользователей (запуск через GPO*)
- [mail_monitor.py](https://github.com/TAM-WD/360/blob/main/misc/mail_monitor.py) — периодическая проверка доступности SMTP- и IMAP-серверов с подробным логированием
- [get_install_logs.ps1](https://github.com/TAM-WD/360/blob/main/misc/get_install_logs.ps1) — собрать логи установки заданного ПО с ПК (sccm, gpo, events)
*рекомендуется проверить работу скрипта на тестовой выборке машин перед массовым запуском.
