# О проекте
Данный репозиторий содержит неофициальные скрипты и примеры для работы с API Яндекс 360. Здесь вы найдете инструменты для автоматизации задач через REST API, включая управление структурой организации, общими дисками, сотрудниками, настройками безопасности и другими компонентами. Код использует только стандартные библиотеки и базовые конструкции языков для решения конкретных задач. 
#
Ниже указаны цели каждого скрипта и примеры применения:
- [ban_status_of_user](https://github.com/TAM-WD/360/blob/main/ban_status_of_user.py) - блокировка, разблокировка и проверка текущего статуса федеративного пользователя (заблокирован/разблокирован) по UID.
- [clear_disk_for_user](https://github.com/TAM-WD/360/blob/main/clear_disk_for_user.py) - удаление файлов федеративного пользователя с Диска по user_id (uid).
- [disk_space_of_shared](https://github.com/TAM-WD/360/blob/main/disk_space_of_shared.py) - получение занятого/свободного места на Общих Дисках организации.
- [disk_space_of_users](https://github.com/TAM-WD/360/blob/main/disk_space_of_users.py) - получение занятого/свободного места на Дисках у сотрудников организации при использовании SSO + SCIM.
- [list_of_groups](https://github.com/TAM-WD/360/blob/main/list_of_groups.py) - получение списка всех групп в организации.
- [list_of_users](https://github.com/TAM-WD/360/blob/main/list_of_users.py) - получение списка всех пользователей в организации.
- [list_of_users_scim](https://github.com/TAM-WD/360/blob/main/list_of_users_scim.py) - получение списка всех пользователей в организации с помощью SCIM-API.
- [public_links_of_users](https://github.com/TAM-WD/360/blob/main/public_links_of_users.py) - получение списка публичных ссылок среди всех пользователей на Дисках организации.
- [public_links_of_blocked_users](https://github.com/TAM-WD/360/blob/main/public_links_of_blocked_users.py) - получение списка публичных ссылок среди заблокированных пользователей на Дисках организации.
- [regdate_of_shared](https://github.com/TAM-WD/360/blob/main/regdate_of_shared.py) - определение даты регистрации Общих Дисков в организации.
- [remove_domain_from_username](https://github.com/TAM-WD/360/blob/main/remove_domain_from_username.py) - обновление значение userName (NameID) на аналогичный userName без домена по списку UID пользователей.
- [shared_disks_of_users](https://github.com/TAM-WD/360/blob/main/shared_disks_of_users.py) - получение информации об Общих Дисках, доступных списку пользователей по e-mail.

## Дисклеймер
Данные скрипты не связаны с официальными продуктами или поддержкой Яндекса. Авторы репозитория не несут ответственности за любые последствия использования этих материалов.
