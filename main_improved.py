#!/usr/bin/env python3
"""
Улучшенная главная точка входа с правильной персистентностью и хронологией.
Использует новый DatabaseManager для полного сканирования и правильного порядка копирования.
"""

import asyncio
import sys
import signal
import os
from typing import Optional
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError, PhoneCodeInvalidError, PhoneNumberInvalidError

from config import Config
from utils import setup_logging, RateLimiter, ProcessLock
from improved_copier import ImprovedTelegramCopier


class ImprovedTelegramCopierApp:
    """Улучшенное приложение для копирования с правильной персистентностью."""
    
    def __init__(self):
        """Инициализация приложения."""
        self.config = Config()
        self.logger = setup_logging()
        self.client: Optional[TelegramClient] = None
        self.copier: Optional[ImprovedTelegramCopier] = None
        self.running = False
        
        # Обработчики сигналов для graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Обработчик сигналов для корректного завершения."""
        self.logger.info(f"Получен сигнал {signum}, завершаем работу...")
        self.running = False
    
    async def run(self):
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
                
                # Инициализация копировщика
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
            
            # Создание клиента
            self.client = TelegramClient(
                self.config.session_name,
                self.config.api_id,
                self.config.api_hash,
                proxy=proxy_config
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
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации клиента: {e}")
            raise
    
    async def _initialize_copier(self):
        """Инициализация копировщика."""
        try:
            # Создание rate limiter
            rate_limiter = RateLimiter(
                messages_per_hour=self.config.messages_per_hour,
                delay_seconds=self.config.delay_seconds
            )
            
            # Создание копировщика
            self.copier = ImprovedTelegramCopier(
                client=self.client,
                source_group_id=self.config.source_group_id,
                target_group_id=self.config.target_group_id,
                rate_limiter=rate_limiter,
                dry_run=self.config.dry_run,
                database_path="telegram_copier_improved.db"
            )
            
            # Инициализация копировщика
            await self.copier.initialize()
            
            self.logger.info("✅ Копировщик инициализирован")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации копировщика: {e}")
            raise
    
    async def _run_copying_process(self):
        """Запуск процесса копирования."""
        try:
            self.logger.info("🚀 Начинаем процесс копирования...")
            
            if self.config.dry_run:
                self.logger.info("🔧 РЕЖИМ DRY RUN - реальная отправка отключена")
            
            # Запуск полного сканирования и копирования
            await self.copier.full_scan_and_copy()
            
            # Получение статистики
            stats = self.copier.get_statistics()
            
            self.logger.info("🎉 Процесс копирования завершен!")
            self.logger.info(f"📊 Финальная статистика:")
            self.logger.info(f"   ✅ Скопировано сообщений: {stats['copied_messages']}")
            self.logger.info(f"   💬 Скопировано комментариев: {stats['copied_comments']}")
            self.logger.info(f"   ❌ Ошибок сообщений: {stats['failed_messages']}")
            self.logger.info(f"   ❌ Ошибок комментариев: {stats['failed_comments']}")
            
            db_stats = stats.get('database_stats', {})
            if db_stats:
                self.logger.info(f"   📋 Всего в БД: {db_stats.get('total_messages', 0)} сообщений")
                self.logger.info(f"   💬 Комментариев в БД: {db_stats.get('total_comments', 0)}")
                self.logger.info(f"   📷 Альбомов: {db_stats.get('albums_count', 0)}")
                self.logger.info(f"   📈 Прогресс: {db_stats.get('progress_percentage', 0):.1f}%")
            
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
    app = ImprovedTelegramCopierApp()
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