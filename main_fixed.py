#!/usr/bin/env python3
"""
Telegram Copier v3.0 - Main Entry Point (ИСПРАВЛЕННАЯ ВЕРСИЯ)
ИСПРАВЛЕНИЯ:
1. ✅ Безопасная инициализация TelegramClient (НЕ выбрасывает другие сессии)
2. ✅ Правильные параметры устройства (device_model, system_version, app_version)
3. ✅ Улучшенная обработка ошибок get_entity
4. ✅ Retry механизм для entity resolution
"""

import asyncio
import sys
import signal
import logging
from typing import Optional

from config import Config
from telegram_copier_v3 import TelegramCopierV3
from utils import setup_logging, ProcessLock
from secure_telegram_client import TelegramClientFactory
from telegram_entity_manager import TelegramEntityManager


class TelegramCopierAppV3Fixed:
    """Исправленное приложение для Telegram Copier v3.0 без конфликтов сессий."""
    
    def __init__(self):
        """Инициализация приложения."""
        self.config = Config()
        self.logger = setup_logging()
        self.client_manager = None
        self.client = None
        self.entity_manager = None
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
                
                # ИСПРАВЛЕНО: Безопасная инициализация клиента
                await self._initialize_secure_client()
                
                # ИСПРАВЛЕНО: Инициализация entity manager
                await self._initialize_entity_manager()
                
                # ИСПРАВЛЕНО: Инициализация копировщика с entity manager
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
    
    async def _initialize_secure_client(self):
        """ИСПРАВЛЕНО: Безопасная инициализация Telegram клиента без конфликтов сессий."""
        try:
            self.logger.info("🔐 Инициализация безопасного Telegram клиента...")
            
            # Прокси конфигурация
            proxy_config = self.config.get_proxy_config()
            if proxy_config:
                self.logger.info(f"🌐 Использование прокси: {proxy_config['addr']}:{proxy_config['port']}")
            
            # ИСПРАВЛЕНО: Создаем клиент через безопасную фабрику
            self.client_manager = TelegramClientFactory.create_stable_client(
                session_name=self.config.session_name,
                api_id=self.config.api_id,
                api_hash=self.config.api_hash,
                phone=self.config.phone,
                proxy=proxy_config
            )
            
            # Показываем информацию об устройстве
            device_info = self.client_manager.get_device_info()
            self.logger.info(f"📱 Профиль устройства: {device_info['device_profile']['device_model']}")
            self.logger.info(f"🔧 Версия системы: {device_info['device_profile']['system_version']}")
            self.logger.info(f"📦 Версия приложения: {device_info['device_profile']['app_version']}")
            
            # ИСПРАВЛЕНО: Безопасный запуск (НЕ выбрасывает другие сессии)
            self.client = await self.client_manager.start_safely()
            
            self.logger.info("✅ Безопасная авторизация завершена")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации безопасного клиента: {e}")
            raise
    
    async def _initialize_entity_manager(self):
        """ИСПРАВЛЕНО: Инициализация менеджера entities с retry механизмом."""
        try:
            self.logger.info("🔍 Инициализация менеджера entities...")
            
            # Создаем entity manager с retry механизмом
            self.entity_manager = TelegramEntityManager(
                client=self.client,
                max_retries=5
            )
            
            self.logger.info("✅ Менеджер entities инициализирован")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации entity manager: {e}")
            raise
    
    async def _initialize_copier(self):
        """ИСПРАВЛЕНО: Инициализация копировщика с улучшенной обработкой entities."""
        try:
            self.logger.info("🚀 Инициализация Telegram Copier v3.0...")
            
            # ИСПРАВЛЕНО: Предварительная проверка доступности каналов
            await self._verify_channels()
            
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
            
            # ИСПРАВЛЕНО: Инициализация с обработкой ошибок entities
            await self._initialize_copier_safely()
            
            self.logger.info("✅ Telegram Copier v3.0 инициализирован безопасно")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации копировщика: {e}")
            raise
    
    async def _verify_channels(self):
        """НОВОЕ: Предварительная проверка доступности каналов."""
        try:
            self.logger.info("🔍 Проверка доступности каналов...")
            
            # Проверяем исходный канал
            source_info = await self.entity_manager.get_entity_info(self.config.source_group_id)
            if not source_info.get('access', False):
                error_msg = f"❌ Нет доступа к исходному каналу: {self.config.source_group_id}"
                if 'error' in source_info:
                    error_msg += f" ({source_info['error']})"
                self.logger.error(error_msg)
                
                # Предлагаем альтернативы
                await self._suggest_channel_alternatives(self.config.source_group_id, "исходного")
                raise Exception(f"Недоступен исходный канал: {self.config.source_group_id}")
            
            # Проверяем целевой канал
            target_info = await self.entity_manager.get_entity_info(self.config.target_group_id)
            if not target_info.get('access', False):
                error_msg = f"❌ Нет доступа к целевому каналу: {self.config.target_group_id}"
                if 'error' in target_info:
                    error_msg += f" ({target_info['error']})"
                self.logger.error(error_msg)
                
                # Предлагаем альтернативы
                await self._suggest_channel_alternatives(self.config.target_group_id, "целевого")
                raise Exception(f"Недоступен целевой канал: {self.config.target_group_id}")
            
            # Логируем успешную проверку
            self.logger.info(f"✅ Исходный канал: {source_info.get('title', 'N/A')} (ID: {source_info.get('id')})")
            self.logger.info(f"✅ Целевой канал: {target_info.get('title', 'N/A')} (ID: {target_info.get('id')})")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки каналов: {e}")
            raise
    
    async def _suggest_channel_alternatives(self, channel_id: str, channel_type: str):
        """НОВОЕ: Предложение альтернативных вариантов поиска канала."""
        try:
            self.logger.info(f"🔍 Поиск альтернативных вариантов для {channel_type} канала...")
            
            # Попробуем найти похожие каналы в диалогах
            dialogs = await self.client.get_dialogs(limit=100)
            suggestions = []
            
            for dialog in dialogs:
                entity = dialog.entity
                if hasattr(entity, 'title') and entity.title:
                    suggestions.append({
                        'id': entity.id,
                        'title': entity.title,
                        'username': getattr(entity, 'username', None),
                        'type': entity.__class__.__name__
                    })
            
            if suggestions:
                self.logger.info(f"💡 Найдены доступные каналы:")
                for i, suggestion in enumerate(suggestions[:10], 1):
                    username_info = f" (@{suggestion['username']})" if suggestion['username'] else ""
                    self.logger.info(f"   {i}. {suggestion['title']}{username_info} (ID: {suggestion['id']})")
                    
        except Exception as e:
            self.logger.debug(f"Ошибка поиска альтернатив: {e}")
    
    async def _initialize_copier_safely(self):
        """НОВОЕ: Безопасная инициализация копировщика с retry механизмом."""
        max_attempts = 3
        
        for attempt in range(max_attempts):
            try:
                self.logger.info(f"🔄 Попытка инициализации копировщика {attempt + 1}/{max_attempts}")
                
                # Инициализация копировщика
                await self.copier.initialize()
                
                self.logger.info("✅ Копировщик инициализирован успешно")
                return
                
            except Exception as e:
                self.logger.warning(f"⚠️ Попытка {attempt + 1} не удалась: {e}")
                
                if attempt < max_attempts - 1:
                    # Очищаем кэш entity manager перед повторной попыткой
                    if self.entity_manager:
                        self.entity_manager.clear_cache()
                    
                    wait_time = 2 ** attempt
                    self.logger.info(f"⏳ Ожидание {wait_time} секунд перед повторной попыткой...")
                    await asyncio.sleep(wait_time)
                else:
                    self.logger.error("❌ Не удалось инициализировать копировщик после всех попыток")
                    raise
    
    async def _run_copying_process(self):
        """Запуск процесса копирования."""
        try:
            self.logger.info("🚀 Запуск Telegram Copier v3.0 (БЕЗОПАСНАЯ ВЕРСИЯ)")
            self.logger.info("📋 Основные улучшения:")
            self.logger.info("   ✅ Безопасная авторизация (НЕ выбрасывает другие сессии)")
            self.logger.info("   ✅ Правильные параметры устройства")
            self.logger.info("   ✅ Улучшенная обработка entities с retry")
            self.logger.info("   ✅ Персистентная SQLite база данных")
            self.logger.info("   ✅ Правильная обработка альбомов")
            self.logger.info("   ✅ Корректное копирование комментариев")
            self.logger.info("   ✅ Хронологический порядок обработки")
            self.logger.info("   ✅ Поддержка всех типов медиа")
            
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
                
                # Показываем статистику entity manager
                if self.entity_manager:
                    stats = self.entity_manager.get_stats()
                    self.logger.info(f"📊 Статистика Entity Manager: {stats}")
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
            
            # ИСПРАВЛЕНО: Безопасное отключение через client_manager
            if self.client_manager:
                await self.client_manager.disconnect()
            elif self.client:
                await self.client.disconnect()
            
            self.logger.info("🔒 Ресурсы освобождены безопасно")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка очистки ресурсов: {e}")


async def main():
    """Главная функция."""
    print("🚀 Telegram Copier v3.0 (ИСПРАВЛЕННАЯ БЕЗОПАСНАЯ ВЕРСИЯ)")
    print("=" * 70)
    print("🔐 КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ:")
    print("✅ Безопасная авторизация - НЕ выбрасывает другие сессии")
    print("✅ Правильные параметры устройства (device_model, system_version)")
    print("✅ Улучшенная обработка entities с retry механизмом")
    print("✅ Предварительная проверка доступности каналов")
    print()
    print("📋 ДОПОЛНИТЕЛЬНЫЕ УЛУЧШЕНИЯ:")
    print("✅ Комментарии копируются правильно")
    print("✅ Альбомы отправляются целиком с правильным порядком")
    print("✅ Персистентная база данных для восстановления при перезапуске")
    print("✅ Хронологический порядок постов и комментариев")
    print("✅ Устранены ошибки MediaProxy")
    print("✅ Поддержка всех типов медиа в постах и комментариях")
    print("=" * 70)
    print()
    
    app = TelegramCopierAppV3Fixed()
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