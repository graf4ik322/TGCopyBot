#!/usr/bin/env python3
"""
Telegram Copier v3.0 - Main Entry Point
Использует полностью переписанную реализацию с исправлением всех критических проблем.
"""

import asyncio
import sys
import signal
import logging
from typing import Optional
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError

from config import Config
from telegram_copier_v3 import TelegramCopierV3
from utils import setup_logging, ProcessLock


class TelegramCopierAppV3:
    """Главное приложение для Telegram Copier v3.0."""
    
    def __init__(self):
        """Инициализация приложения."""
        self.config = Config()
        self.logger = setup_logging()
        self.client: Optional[TelegramClient] = None
        self.copier: Optional[TelegramCopierV3] = None
        self.running = False
        
        # Обработчики сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для корректного завершения."""
        self.logger.info(f"Получен сигнал {signum}, завершаем работу...")
        self.running = False
        if self.copier:
            self.copier.stop()
    
    async def run(self) -> bool:
        """Главный цикл выполнения."""
        try:
            # Валидация конфигурации
            if not self.config.validate():
                self.logger.error("❌ Ошибка валидации конфигурации")
                return False
            
            # Блокировка для предотвращения множественного запуска
            with ProcessLock():
                self.logger.info("🔒 Блокировка процесса установлена")
                
                # Инициализация клиента
                await self._initialize_client()
                
                # Инициализация копировщика v3.0
                await self._initialize_copier()
                
                # Запуск процесса копирования
                self.running = True
                await self._run_copying_process()
                
        except KeyboardInterrupt:
            self.logger.info("⏹️ Получено прерывание от пользователя")
        except Exception as e:
            self.logger.error(f"❌ Критическая ошибка: {e}")
            import traceback
            self.logger.error(f"Детали: {traceback.format_exc()}")
            return False
        finally:
            await self._cleanup()
        
        return True
    
    async def _initialize_client(self):
        """Инициализация Telegram клиента."""
        try:
            # Прокси конфигурация
            proxy_config = self.config.get_proxy_config()
            if proxy_config:
                self.logger.info(f"🌐 Использование прокси: {proxy_config['addr']}:{proxy_config['port']}")
            
            # ИСПРАВЛЕНО: Создание клиента с реальными параметрами системы
            import platform
            import socket
            
            # Получаем реальную информацию о системе
            hostname = socket.gethostname()
            system_info = platform.system()
            system_version = platform.release()
            
            self.logger.info(f"📱 Устройство: {hostname}")
            self.logger.info(f"💻 Система: {system_info} {system_version}")
            
            self.client = TelegramClient(
                session=self.config.session_name,
                api_id=self.config.api_id,
                api_hash=self.config.api_hash,
                proxy=proxy_config,
                
                # РЕАЛЬНЫЕ параметры системы (как в старой версии)
                device_model=hostname,
                system_version=f"{system_info} {system_version}",
                app_version="1.0.0, Telegram Copier Script",
                lang_code='en',
                system_lang_code='en-US',
                
                # УЛУЧШЕННЫЕ параметры стабильности и производительности
                connection_retries=5,
                retry_delay=1,
                auto_reconnect=True,
                timeout=30,
                request_retries=3,
                
                # Дополнительные параметры для улучшения работы с entities
                catch_up=True,  # Синхронизация состояния при подключении
                sequential_updates=True,  # Последовательная обработка обновлений
                receive_updates=False  # Отключаем получение обновлений для стабильности
            )
            
            # Подключение
            await self.client.start()
            
            # Проверка авторизации
            if not await self.client.is_user_authorized():
                self.logger.info("📱 Требуется авторизация...")
                
                # Отправка кода
                await self.client.send_code_request(self.config.phone)
                
                # Получение кода от пользователя
                code = input("Введите код авторизации: ")
                
                try:
                    await self.client.sign_in(self.config.phone, code)
                except SessionPasswordNeededError:
                    password = input("Введите двухфакторный пароль: ")
                    await self.client.sign_in(password=password)
            
            # Проверка успешной авторизации
            me = await self.client.get_me()
            self.logger.info(f"✅ Авторизация успешна: {me.first_name}")
            
            # НОВОЕ: Принудительная синхронизация кэша entities
            self.logger.info("🔄 Синхронизация кэша entities...")
            try:
                dialogs = await self.client.get_dialogs(limit=200)
                self.logger.info(f"   Загружено {len(dialogs)} диалогов в кэш")
            except Exception as e:
                self.logger.warning(f"⚠️ Не удалось синхронизировать кэш: {e}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации клиента: {e}")
            raise
    
    async def _initialize_copier(self):
        """Инициализация копировщика v3.0."""
        try:
            # Создание копировщика v3.0
            self.copier = TelegramCopierV3(
                client=self.client,
                source_channel_id=self.config.source_group_id,
                target_channel_id=self.config.target_group_id,
                database_path="telegram_copier_v3.db",
                dry_run=self.config.dry_run,
                delay_seconds=self.config.delay_seconds,
                flatten_structure=self.config.flatten_structure
            )
            
            # Инициализация копировщика
            await self.copier.initialize()
            
            self.logger.info("✅ Telegram Copier v3.0 инициализирован")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации копировщика: {e}")
            raise
    
    async def _run_copying_process(self):
        """Запуск процесса копирования."""
        try:
            self.logger.info("🚀 Запуск Telegram Copier v3.0")
            self.logger.info("📋 Основные улучшения:")
            self.logger.info("   ✅ Персистентная SQLite база данных")
            self.logger.info("   ✅ Правильная обработка альбомов")
            self.logger.info("   ✅ Корректное копирование комментариев")
            self.logger.info("   ✅ Хронологический порядок обработки")
            self.logger.info("   ✅ Поддержка всех типов медиа")
            self.logger.info("   ✅ Устранение MediaProxy ошибок")
            
            if self.config.dry_run:
                self.logger.info("🔧 РЕЖИМ DRY RUN - реальная отправка отключена")
            
            # Этап 1: Полное сканирование канала (выполняется только один раз)
            self.logger.info("🔍 Этап 1: Сканирование канала и сохранение в БД...")
            scan_success = await self.copier.scan_and_save_all_messages()
            
            if not scan_success:
                self.logger.error("❌ Ошибка сканирования канала")
                return
            
            if not self.running:
                return
            
            # Этап 2: Копирование в хронологическом порядке
            self.logger.info("📋 Этап 2: Копирование сообщений в хронологическом порядке...")
            copy_success = await self.copier.copy_all_messages_chronologically()
            
            if copy_success:
                self.logger.info("🎉 Все этапы завершены успешно!")
            else:
                self.logger.error("❌ Ошибка на этапе копирования")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка процесса копирования: {e}")
            raise
    
    async def _cleanup(self):
        """Очистка ресурсов."""
        try:
            if self.copier:
                self.copier.close()
            
            if self.client:
                await self.client.disconnect()
            
            self.logger.info("🔒 Ресурсы освобождены")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка очистки ресурсов: {e}")


async def main():
    """Главная функция."""
    print("🚀 Telegram Copier v3.0")
    print("=" * 50)
    print("Полностью переписанная реализация с исправлением всех критических проблем:")
    print("✅ Комментарии копируются правильно")
    print("✅ Альбомы отправляются целиком с правильным порядком")
    print("✅ Персистентная база данных для восстановления при перезапуске")
    print("✅ Хронологический порядок постов и комментариев")
    print("✅ Устранены ошибки MediaProxy")
    print("✅ Поддержка всех типов медиа в постах и комментариях")
    print("=" * 50)
    print()
    
    app = TelegramCopierAppV3()
    success = await app.run()
    return 0 if success else 1


if __name__ == "__main__":
    # Проверка версии Python
    if sys.version_info < (3, 7):
        print("❌ Требуется Python 3.7 или выше")
        sys.exit(1)
    
    # Запуск приложения
    exit_code = asyncio.run(main())
    sys.exit(exit_code)