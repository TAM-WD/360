#!/usr/bin/env python3
"""
Мониторинг отключённых (Disabled) пользователей в MS Active Directory
и выполнение Global Logout в Яндекс 360

Логика:
1. Получаем список Disabled пользователей из AD
2. Сопоставляем их с пользователями Яндекс 360 по email
3. С подтверждением выполняем logout
"""

import os
import csv
import json
import requests
import ldap3
from ldap3 import Server, Connection, ALL, SUBTREE
from requests.adapters import HTTPAdapter, Retry
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional
import time
import logging

# ============================================================
# НАСТРОЙКИ
# ============================================================

# --- Active Directory ---
# Обязательно замените данные на свои!
LDAP_CONFIG = {
    'server': 'ldap://your-dc.ms1.st1.domain.net',
    'port': 389,
    'use_ssl': False,
    'bind_dn': 'CN=service_account,OU=Service,OU=Stend,DC=ms1,DC=st1,DC=domain,DC=net',
    'bind_password': 'your_password',
    'base_dn': 'DC=ms1,DC=st1,DC=domain,DC=net',
    'user_search_base': 'OU=Stend,DC=ms1,DC=st1,DC=domain,DC=net',
}

# --- Яндекс 360 ---
YANDEX_CONFIG = {
    'token': '',  # OAuth токен с правами directory:read_users и security:domain_sessions_write
    'org_id': '',  # ID организации
    'per_page': 1000,
    'skip_yandex_domain': True,  # Пропускать @yandex.ru
}

# --- Общие настройки ---
SETTINGS = {
    'results_dir': './results',  # Директория для логов и CSV
    'request_delay': 0.5,  # Задержка между запросами к API (секунды)
    'auto_confirm': False,  # True = без подтверждения, False = с подтверждением
}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================
# МОДЕЛИ ДАННЫХ
# ============================================================

@dataclass
class DisabledUser:
    """Отключённый пользователь из AD"""
    username: str
    cn: str
    email: Optional[str]
    dn: str
    when_changed: Optional[str]
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class Yandex360User:
    """Пользователь Яндекс 360"""
    id: str
    email: str
    nickname: str
    full_name: str
    is_enabled: bool
    
    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class MatchedUser:
    """Сопоставленный пользователь (AD + Яндекс 360)"""
    ad_username: str
    ad_cn: str
    ad_email: str
    ad_when_changed: str
    yandex_id: str
    yandex_email: str
    yandex_full_name: str
    logout_status: Optional[str] = None
    
    def to_dict(self) -> dict:
        return asdict(self)


# ============================================================
# ACTIVE DIRECTORY
# ============================================================

class ADMonitor:
    """Мониторинг отключённых аккаунтов в Active Directory"""
    
    def __init__(self, config: dict):
        self.config = config
        self.conn = None
        
    def connect(self) -> bool:
        """Подключение к AD"""
        try:
            server = Server(
                self.config['server'],
                port=self.config['port'],
                use_ssl=self.config['use_ssl'],
                get_info=ALL
            )
            
            self.conn = Connection(
                server,
                user=self.config['bind_dn'],
                password=self.config['bind_password'],
                auto_bind=True
            )
            
            logger.info(f"AD: Подключение {'успешно' if self.conn.bound else 'ошибка'}")
            return self.conn.bound
            
        except Exception as e:
            logger.error(f"AD: Ошибка подключения: {e}")
            raise
    
    def disconnect(self):
        """Отключение от AD"""
        if self.conn:
            self.conn.unbind()
            logger.info("AD: Отключение")
    
    def get_disabled_users(self) -> list[DisabledUser]:
        """Получить список отключённых (Disabled) аккаунтов"""
        
        search_filter = """(&
            (objectClass=user)
            (objectCategory=person)
            (userAccountControl:1.2.840.113556.1.4.803:=2)
        )""".replace('\n', '').replace(' ', '')
        
        attributes = [
            'sAMAccountName',
            'cn',
            'mail',
            'whenChanged',
            'distinguishedName',
        ]
        
        self.conn.search(
            search_base=self.config['user_search_base'],
            search_filter=search_filter,
            search_scope=SUBTREE,
            attributes=attributes
        )
        
        users = []
        for entry in self.conn.entries:
            user = DisabledUser(
                username=str(entry.sAMAccountName) if entry.sAMAccountName else "",
                cn=str(entry.cn) if entry.cn else "",
                email=str(entry.mail).lower() if hasattr(entry, 'mail') and entry.mail.value else None,
                dn=str(entry.entry_dn),
                when_changed=str(entry.whenChanged) if hasattr(entry, 'whenChanged') and entry.whenChanged.value else None,
            )
            users.append(user)
        
        logger.info(f"AD: Найдено отключённых аккаунтов: {len(users)}")
        return users


# ============================================================
# ЯНДЕКС 360 API
# ============================================================

class Yandex360API:
    """Работа с API Яндекс 360"""
    
    def __init__(self, config: dict):
        self.config = config
        self.token = config['token']
        self.org_id = config['org_id']
        self.per_page = config['per_page']
        self.session = self._create_session()
        
    def _create_session(self) -> requests.Session:
        """Создание сессии с retry"""
        session = requests.Session()
        retries = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[500, 502, 503, 504]
        )
        session.mount('https://', HTTPAdapter(max_retries=retries))
        return session
    
    def _get_headers(self) -> dict:
        """Заголовки для API"""
        return {'Authorization': f"OAuth {self.token}"}
    
    def get_all_users(self) -> list[Yandex360User]:
        """Получение всех пользователей организации"""
        
        all_users = []
        page = 1
        total_pages = 1
        
        logger.info(f"Яндекс 360: Начинаем загрузку пользователей организации {self.org_id}...")
        
        while page <= total_pages:
            url = f'https://api360.yandex.net/directory/v1/org/{self.org_id}/users'
            params = {
                'page': page,
                'perPage': self.per_page
            }
            
            response = self.session.get(url, params=params, headers=self._get_headers())
            response.raise_for_status()
            
            data = response.json()
            total_pages = data.get('pages', 1)
            
            for user in data.get('users', []):
                user_email = user.get('email', '').lower()
                
                # Пропускаем @yandex.ru если настроено
                if self.config['skip_yandex_domain'] and user_email.endswith('@yandex.ru'):
                    continue
                
                # Формируем полное имя
                name = user.get('name', {})
                full_name_parts = [
                    name.get('first', ''),
                    name.get('middle', ''),
                    name.get('last', '')
                ]
                full_name = ' '.join([p for p in full_name_parts if p]).strip()
                
                yandex_user = Yandex360User(
                    id=str(user.get('id', '')),
                    email=user_email,
                    nickname=user.get('nickname', ''),
                    full_name=full_name if full_name else user.get('nickname', 'Без имени'),
                    is_enabled=user.get('isEnabled', True)
                )
                all_users.append(yandex_user)
            
            logger.info(f"Яндекс 360: Загружена страница {page}/{total_pages}")
            page += 1
        
        logger.info(f"Яндекс 360: Всего загружено пользователей: {len(all_users)}")
        return all_users
    
    def logout_user(self, user_id: str) -> tuple[bool, str]:
        """Выход из аккаунта пользователя на всех устройствах"""
        
        url = f'https://api360.yandex.net/security/v1/org/{self.org_id}/domain_sessions/users/{user_id}/logout'
        
        try:
            response = self.session.put(url, headers=self._get_headers())
            
            if response.status_code == 200:
                return True, "Успешно"
            else:
                return False, f"Ошибка (код: {response.status_code})"
                
        except Exception as e:
            return False, f"Ошибка: {str(e)}"


# ============================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================

def match_users(
    ad_disabled: list[DisabledUser],
    yandex_users: list[Yandex360User]
) -> list[MatchedUser]:
    """Сопоставление пользователей AD с Яндекс 360 по email"""
    
    # Создаём словарь Яндекс пользователей по email
    yandex_by_email = {user.email: user for user in yandex_users}
    
    matched = []
    not_found = []
    
    for ad_user in ad_disabled:
        if not ad_user.email:
            logger.warning(f"AD пользователь {ad_user.username} не имеет email, пропускаем")
            continue
        
        yandex_user = yandex_by_email.get(ad_user.email)
        
        if yandex_user:
            matched_user = MatchedUser(
                ad_username=ad_user.username,
                ad_cn=ad_user.cn,
                ad_email=ad_user.email,
                ad_when_changed=ad_user.when_changed or "",
                yandex_id=yandex_user.id,
                yandex_email=yandex_user.email,
                yandex_full_name=yandex_user.full_name,
            )
            matched.append(matched_user)
        else:
            not_found.append(ad_user)
    
    logger.info(f"Сопоставлено: {len(matched)}, Не найдено в Яндекс 360: {len(not_found)}")
    
    if not_found:
        logger.warning("Пользователи AD без соответствия в Яндекс 360:")
        for user in not_found:
            logger.warning(f"  - {user.username} ({user.email})")
    
    return matched


def print_users_table(users: list[MatchedUser]):
    """Вывод таблицы пользователей"""
    
    print("\n" + "=" * 100)
    print(f"{'№':<4} {'AD Username':<20} {'Email':<40} {'Яндекс ID':<15} {'Имя':<30}")
    print("=" * 100)
    
    for i, user in enumerate(users, 1):
        print(f"{i:<4} {user.ad_username:<20} {user.ad_email:<40} {user.yandex_id:<15} {user.yandex_full_name[:30]:<30}")
    
    print("=" * 100)
    print(f"Всего: {len(users)}")
    print()


def confirm_action(message: str) -> bool:
    """Запрос подтверждения от пользователя"""
    
    while True:
        response = input(f"{message} [y/n]: ").strip().lower()
        if response in ['y', 'yes', 'да', 'д']:
            return True
        elif response in ['n', 'no', 'нет', 'н']:
            return False
        else:
            print("Пожалуйста, введите 'y' или 'n'")


def save_results(
    matched_users: list[MatchedUser],
    results_dir: str,
    org_id: str,
    start_time: str
):
    """Сохранение результатов в CSV и лог"""
    
    os.makedirs(results_dir, exist_ok=True)
    
    csv_file = os.path.join(results_dir, f'ad_disabled_logout_{org_id}_{start_time}.csv')
    log_file = os.path.join(results_dir, f'ad_disabled_logout_{org_id}_{start_time}.log')
    
    # CSV
    with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
        fieldnames = [
            'AD_Username', 'AD_CN', 'AD_Email', 'AD_WhenChanged',
            'Yandex_ID', 'Yandex_Email', 'Yandex_FullName', 'Logout_Status'
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        
        for user in matched_users:
            writer.writerow({
                'AD_Username': user.ad_username,
                'AD_CN': user.ad_cn,
                'AD_Email': user.ad_email,
                'AD_WhenChanged': user.ad_when_changed,
                'Yandex_ID': user.yandex_id,
                'Yandex_Email': user.yandex_email,
                'Yandex_FullName': user.yandex_full_name,
                'Logout_Status': user.logout_status or 'Не выполнялся',
            })
    
    # Лог
    with open(log_file, 'w', encoding='utf-8') as f:
        f.write(f"Лог выполнения AD Disabled → Яндекс 360 Logout\n")
        f.write(f"Дата и время: {start_time}\n")
        f.write(f"Организация ID: {org_id}\n")
        f.write(f"Всего обработано: {len(matched_users)}\n")
        f.write("=" * 80 + "\n\n")
        
        success_count = sum(1 for u in matched_users if u.logout_status == "Успешно")
        failed_count = sum(1 for u in matched_users if u.logout_status and u.logout_status != "Успешно" and u.logout_status != "Не выполнялся")
        
        for user in matched_users:
            status_icon = "✓" if user.logout_status == "Успешно" else "✗" if user.logout_status else "-"
            f.write(f"{status_icon} {user.ad_username} | {user.ad_email} | {user.logout_status or 'Не выполнялся'}\n")
        
        f.write("\n" + "=" * 80 + "\n")
        f.write(f"Успешно: {success_count}\n")
        f.write(f"Ошибок: {failed_count}\n")
    
    logger.info(f"CSV сохранён: {csv_file}")
    logger.info(f"Лог сохранён: {log_file}")
    
    return csv_file, log_file


def main():
    """Основная функция"""
    
    start_time = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    
    print("=" * 70)
    print("AD DISABLED → ЯНДЕКС 360 GLOBAL LOGOUT")
    print(f"Запуск: {start_time}")
    print("=" * 70)
    
    # ========================================
    # ШАГ 1: Получение Disabled из AD
    # ========================================
    print("\n[ШАГ 1] Подключение к Active Directory...")
    
    ad_monitor = ADMonitor(LDAP_CONFIG)
    
    try:
        ad_monitor.connect()
        ad_disabled_users = ad_monitor.get_disabled_users()
    finally:
        ad_monitor.disconnect()
    
    if not ad_disabled_users:
        print("\n✓ Отключённых пользователей в AD не найдено. Завершение.")
        return
    
    print(f"\nНайдено отключённых в AD: {len(ad_disabled_users)}")
    for user in ad_disabled_users:
        print(f"  • {user.username} ({user.email})")
    
    # ========================================
    # ШАГ 2: Получение пользователей Яндекс 360
    # ========================================
    print("\n[ШАГ 2] Загрузка пользователей из Яндекс 360...")
    
    yandex_api = Yandex360API(YANDEX_CONFIG)
    yandex_users = yandex_api.get_all_users()
    
    if not yandex_users:
        print("\n✗ Не удалось получить пользователей из Яндекс 360. Завершение.")
        return
    
    # ========================================
    # ШАГ 3: Сопоставление по email
    # ========================================
    print("\n[ШАГ 3] Сопоставление пользователей по email...")
    
    matched_users = match_users(ad_disabled_users, yandex_users)
    
    if not matched_users:
        print("\n✓ Нет пользователей для logout (не найдено соответствий). Завершение.")
        return
    
    # ========================================
    # ШАГ 4: Подтверждение
    # ========================================
    print("\n[ШАГ 4] Пользователи для Global Logout:")
    print_users_table(matched_users)
    
    if not SETTINGS['auto_confirm']:
        print("⚠️  ВНИМАНИЕ: Будет выполнен принудительный выход из всех сессий!")
        print("    Пользователям потребуется повторная авторизация на всех устройствах.")
        print()
        
        if not confirm_action("Выполнить Global Logout для указанных пользователей?"):
            print("\n✗ Операция отменена пользователем.")
            
            # Сохраняем отчёт без выполнения logout
            save_results(
                matched_users,
                SETTINGS['results_dir'],
                YANDEX_CONFIG['org_id'],
                start_time
            )
            return
    
    # ========================================
    # ШАГ 5: Выполнение Logout
    # ========================================
    print("\n[ШАГ 5] Выполнение Global Logout...")
    
    success_count = 0
    failed_count = 0
    
    for i, user in enumerate(matched_users, 1):
        print(f"  [{i}/{len(matched_users)}] {user.ad_email}...", end=" ")
        
        success, message = yandex_api.logout_user(user.yandex_id)
        user.logout_status = message
        
        if success:
            success_count += 1
            print(f"✓ {message}")
        else:
            failed_count += 1
            print(f"✗ {message}")
        
        # Задержка между запросами
        if i < len(matched_users):
            time.sleep(SETTINGS['request_delay'])
    
    # ========================================
    # ШАГ 6: Сохранение результатов
    # ========================================
    print("\n[ШАГ 6] Сохранение результатов...")
    
    csv_file, log_file = save_results(
        matched_users,
        SETTINGS['results_dir'],
        YANDEX_CONFIG['org_id'],
        start_time
    )
    
    # ========================================
    # ИТОГИ
    # ========================================
    print("\n" + "=" * 70)
    print("ИТОГИ:")
    print(f"  Отключённых в AD:        {len(ad_disabled_users)}")
    print(f"  Сопоставлено с Яндекс:   {len(matched_users)}")
    print(f"  Logout успешно:          {success_count}")
    print(f"  Logout с ошибкой:        {failed_count}")
    print("=" * 70)
    print(f"\nФайлы:")
    print(f"  CSV: {csv_file}")
    print(f"  Лог: {log_file}")
    print("\n✓ Скрипт завершён.")


if __name__ == "__main__":
    main()