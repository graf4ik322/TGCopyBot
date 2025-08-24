"""
Secure Telegram Client - Безопасная инициализация клиента без конфликтов сессий
Предотвращает выбрасывание из других активных сессий при авторизации.
"""

import asyncio
import logging
import platform
import random
from typing import Optional, Dict, Any
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, FloodWaitError


class SecureTelegramClient:
    """
    Безопасная обертка для TelegramClient с правильными параметрами сессии.
    Предотвращает конфликты сессий и выбрасывание из других устройств.
    """
    
    # Стабильные параметры устройства (имитация популярных устройств)
    DEVICE_PROFILES = [
        {
            'device_model': 'Samsung SM-G991B',
            'system_version': 'SDK 31',
            'app_version': '8.9.2',
            'lang_code': 'en',
            'system_lang_code': 'en-US'
        },
        {
            'device_model': 'iPhone 13 Pro',
            'system_version': 'iOS 15.6.1',
            'app_version': '8.9.2',
            'lang_code': 'en',
            'system_lang_code': 'en-US'
        },
        {
            'device_model': 'Pixel 6',
            'system_version': 'Android 12',
            'app_version': '8.9.2',
            'lang_code': 'en',
            'system_lang_code': 'en-US'
        }
    ]
    
    def __init__(self, 
                 session_name: str,
                 api_id: int,
                 api_hash: str,
                 phone: str,
                 proxy: Optional[dict] = None,
                 device_profile: Optional[dict] = None):
        """
        Инициализация безопасного Telegram клиента.
        
        Args:
            session_name: Имя файла сессии
            api_id: API ID от my.telegram.org
            api_hash: API Hash от my.telegram.org
            phone: Номер телефона
            proxy: Настройки прокси (опционально)
            device_profile: Профиль устройства (опционально)
        """
        self.session_name = session_name
        self.api_id = api_id
        self.api_hash = api_hash
        self.phone = phone
        self.proxy = proxy
        
        # Выбираем стабильный профиль устройства
        if device_profile:
            self.device_profile = device_profile
        else:
            # Выбираем профиль детерминированно на основе session_name
            # Это гарантирует, что одна и та же сессия всегда использует один профиль
            profile_index = hash(session_name) % len(self.DEVICE_PROFILES)
            self.device_profile = self.DEVICE_PROFILES[profile_index]
        
        self.logger = logging.getLogger('secure_telegram_client')
        self.client: Optional[TelegramClient] = None
    
    async def create_client(self) -> TelegramClient:
        """
        Создание TelegramClient с безопасными параметрами.
        
        Returns:
            Настроенный TelegramClient
        """
        try:
            self.logger.info(f"🔐 Создание безопасного клиента для сессии: {self.session_name}")
            self.logger.info(f"📱 Профиль устройства: {self.device_profile['device_model']}")
            
            # Создаем клиента с полным набором параметров
            self.client = TelegramClient(
                session=self.session_name,
                api_id=self.api_id,
                api_hash=self.api_hash,
                proxy=self.proxy,
                
                # КРИТИЧЕСКИ ВАЖНЫЕ ПАРАМЕТРЫ для предотвращения конфликтов сессий
                device_model=self.device_profile['device_model'],
                system_version=self.device_profile['system_version'],
                app_version=self.device_profile['app_version'],
                lang_code=self.device_profile['lang_code'],
                system_lang_code=self.device_profile['system_lang_code'],
                
                # Дополнительные параметры безопасности
                connection_retries=5,
                retry_delay=1,
                auto_reconnect=True,
                
                # Параметры для стабильности
                timeout=30,
                request_retries=3,
                
                # Отключаем некоторые автоматические функции для стабильности
                receive_updates=False  # Отключаем получение обновлений для копировщика
            )
            
            return self.client
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка создания клиента: {e}")
            raise
    
    async def start_safely(self) -> TelegramClient:
        """
        Безопасный запуск и авторизация клиента.
        
        Returns:
            Авторизованный TelegramClient
        """
        if not self.client:
            await self.create_client()
        
        try:
            self.logger.info("🚀 Запуск клиента...")
            
            # Запускаем клиента
            await self.client.start(phone=self.phone)
            
            # Проверяем авторизацию
            if not await self.client.is_user_authorized():
                self.logger.info("📱 Требуется авторизация...")
                await self._handle_authorization()
            
            # Проверяем успешную авторизацию
            me = await self.client.get_me()
            self.logger.info(f"✅ Авторизация успешна: {me.first_name} (ID: {me.id})")
            self.logger.info(f"📱 Устройство: {self.device_profile['device_model']}")
            
            return self.client
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка запуска клиента: {e}")
            raise
    
    async def _handle_authorization(self):
        """Обработка процесса авторизации."""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"📞 Попытка авторизации {attempt + 1}/{max_attempts}")
                
                # Отправляем код
                await self.client.send_code_request(self.phone)
                
                # Получаем код от пользователя
                code = input(f"Введите код авторизации для {self.phone}: ").strip()
                
                if not code:
                    self.logger.warning("⚠️ Код не введен")
                    continue
                
                try:
                    # Пытаемся авторизоваться с кодом
                    await self.client.sign_in(self.phone, code)
                    self.logger.info("✅ Авторизация по коду успешна")
                    return
                    
                except SessionPasswordNeededError:
                    self.logger.info("🔐 Требуется двухфакторная аутентификация")
                    password = input("Введите пароль 2FA: ").strip()
                    
                    if password:
                        await self.client.sign_in(password=password)
                        self.logger.info("✅ Авторизация с 2FA успешна")
                        return
                    else:
                        self.logger.warning("⚠️ Пароль 2FA не введен")
                        continue
                
            except FloodWaitError as e:
                wait_time = e.seconds
                self.logger.warning(f"⏳ FloodWait: ожидание {wait_time} секунд")
                await asyncio.sleep(wait_time)
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка авторизации: {e}")
                if attempt == max_attempts - 1:
                    raise
                
                self.logger.info("🔄 Повторная попытка через 5 секунд...")
                await asyncio.sleep(5)
        
        raise Exception("Не удалось авторизоваться после всех попыток")
    
    async def disconnect(self):
        """Безопасное отключение клиента."""
        if self.client:
            try:
                await self.client.disconnect()
                self.logger.info("🔒 Клиент отключен")
            except Exception as e:
                self.logger.error(f"❌ Ошибка отключения: {e}")
    
    def get_device_info(self) -> Dict[str, Any]:
        """Получение информации об устройстве."""
        return {
            'session_name': self.session_name,
            'device_profile': self.device_profile.copy(),
            'proxy': bool(self.proxy)
        }


class TelegramClientFactory:
    """Фабрика для создания безопасных Telegram клиентов."""
    
    @staticmethod
    def create_stable_client(session_name: str,
                           api_id: int,
                           api_hash: str,
                           phone: str,
                           proxy: Optional[dict] = None) -> SecureTelegramClient:
        """
        Создание стабильного клиента с детерминированными параметрами.
        
        Args:
            session_name: Имя сессии
            api_id: API ID
            api_hash: API Hash
            phone: Номер телефона
            proxy: Настройки прокси
            
        Returns:
            Настроенный SecureTelegramClient
        """
        return SecureTelegramClient(
            session_name=session_name,
            api_id=api_id,
            api_hash=api_hash,
            phone=phone,
            proxy=proxy
        )
    
    @staticmethod
    def create_custom_client(session_name: str,
                           api_id: int,
                           api_hash: str,
                           phone: str,
                           device_model: str,
                           system_version: str,
                           app_version: str = "8.9.2",
                           proxy: Optional[dict] = None) -> SecureTelegramClient:
        """
        Создание клиента с кастомными параметрами устройства.
        
        Args:
            session_name: Имя сессии
            api_id: API ID
            api_hash: API Hash
            phone: Номер телефона
            device_model: Модель устройства
            system_version: Версия системы
            app_version: Версия приложения
            proxy: Настройки прокси
            
        Returns:
            Настроенный SecureTelegramClient
        """
        device_profile = {
            'device_model': device_model,
            'system_version': system_version,
            'app_version': app_version,
            'lang_code': 'en',
            'system_lang_code': 'en-US'
        }
        
        return SecureTelegramClient(
            session_name=session_name,
            api_id=api_id,
            api_hash=api_hash,
            phone=phone,
            proxy=proxy,
            device_profile=device_profile
        )


async def test_secure_client():
    """Тестирование безопасного клиента."""
    # Пример использования
    client_manager = TelegramClientFactory.create_stable_client(
        session_name='test_secure_session',
        api_id=12345,  # Замените на ваш API_ID
        api_hash='your_api_hash',  # Замените на ваш API_HASH
        phone='+1234567890'  # Замените на ваш номер
    )
    
    try:
        client = await client_manager.start_safely()
        print(f"Информация об устройстве: {client_manager.get_device_info()}")
        
        # Тест базовых функций
        me = await client.get_me()
        print(f"Пользователь: {me.first_name}")
        
    finally:
        await client_manager.disconnect()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(test_secure_client())