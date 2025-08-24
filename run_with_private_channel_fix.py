#!/usr/bin/env python3
"""
Запуск копировщика с патчами для приватных каналов.
Автоматически применяет исправления для корректной работы с комментариями.
"""

import asyncio
import sys
import signal
import os
from typing import Optional
from telethon import TelegramClient

# Импортируем основные модули
from config import Config
from utils import setup_logging, RateLimiter, ProcessLock
from copier import TelegramCopier
from fix_private_channel_comments import apply_private_channel_patch


class PrivateChannelCopierApp:
    """Приложение для копирования приватных каналов с исправлениями."""
    
    def __init__(self):
        """Инициализация приложения."""
        self.config = Config()
        self.logger = setup_logging()
        self.client: Optional[TelegramClient] = None
        self.copier: Optional[TelegramCopier] = None
        self.running = False
        
        # Обработчики сигналов
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
                self._show_config_help()
                return False
            
            # Блокировка процесса
            with ProcessLock():
                self.logger.info("🔒 Блокировка процесса установлена")
                
                # Инициализация клиента
                await self._initialize_client()
                
                # Инициализация копировщика с патчами
                await self._initialize_copier_with_patches()
                
                # Диагностика перед запуском
                await self._run_diagnostics()
                
                # Запуск копирования
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
    
    def _show_config_help(self):
        """Показывает справку по конфигурации для приватных каналов."""
        self.logger.info("📋 Для работы с приватными каналами укажите в .env:")
        self.logger.info("   SOURCE_GROUP_ID=<ID или @username исходного канала>")
        self.logger.info("   TARGET_GROUP_ID=<ID или @username целевого канала>")
        self.logger.info("   DISCUSSION_GROUP_ID=<ID или @username группы комментариев> (опционально)")
        self.logger.info("   TARGET_DISCUSSION_GROUP_ID=<ID или @username целевой группы комментариев> (опционально)")
        self.logger.info("")
        self.logger.info("💡 Для получения ID групп используйте: python diagnose_private_channel.py")
    
    async def _initialize_client(self):
        """Инициализация Telegram клиента."""
        try:
            proxy_config = self.config.get_proxy_config()
            if proxy_config:
                self.logger.info(f"🌐 Использование прокси: {proxy_config['addr']}:{proxy_config['port']}")
            
            self.client = TelegramClient(
                self.config.session_name,
                self.config.api_id,
                self.config.api_hash,
                proxy=proxy_config
            )
            
            await self.client.start()
            
            if not await self.client.is_user_authorized():
                self.logger.info("📱 Требуется авторизация...")
                await self.client.send_code_request(self.config.phone)
                code = input("Введите код авторизации: ")
                
                try:
                    await self.client.sign_in(self.config.phone, code)
                except Exception as e:
                    if "password" in str(e).lower():
                        password = input("Введите двухфакторный пароль: ")
                        await self.client.sign_in(password=password)
                    else:
                        raise
            
            me = await self.client.get_me()
            self.logger.info(f"✅ Авторизация успешна: {me.first_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации клиента: {e}")
            raise
    
    async def _initialize_copier_with_patches(self):
        """Инициализация копировщика с применением патчей для приватных каналов."""
        try:
            # Создание rate limiter
            rate_limiter = RateLimiter(
                messages_per_hour=self.config.messages_per_hour,
                delay_seconds=self.config.delay_seconds
            )
            
            # Создание копировщика
            self.copier = TelegramCopier(
                client=self.client,
                source_group_id=self.config.source_group_id,
                target_group_id=self.config.target_group_id,
                rate_limiter=rate_limiter,
                dry_run=self.config.dry_run,
                resume_file=self.config.resume_file,
                use_message_tracker=self.config.use_message_tracker,
                tracker_file=self.config.tracker_file,
                add_debug_tags=self.config.add_debug_tags,
                flatten_structure=self.config.flatten_structure,
                debug_message_ids=self.config.debug_message_ids,
                batch_size=self.config.batch_size
            )
            
            # Передаем конфигурацию копировщику для доступа к discussion_group_id
            self.copier.config = self.config
            
            # Инициализация копировщика
            await self.copier.initialize()
            
            # КРИТИЧЕСКИ ВАЖНО: Применяем патч для приватных каналов
            self.logger.info("🔧 Применяем патчи для приватных каналов...")
            apply_private_channel_patch(self.copier)
            
            self.logger.info("✅ Копировщик с патчами инициализирован")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации копировщика: {e}")
            raise
    
    async def _run_diagnostics(self):
        """Запуск диагностики перед началом копирования."""
        try:
            self.logger.info("🔍 Запуск диагностики приватного канала...")
            
            # Используем встроенную диагностику из патча
            if hasattr(self.copier, 'diagnose_comments_setup'):
                await self.copier.diagnose_comments_setup()
            
            # Дополнительные проверки
            await self._check_channel_access()
            
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка диагностики: {e}")
    
    async def _check_channel_access(self):
        """Проверка доступа к каналам и группам."""
        try:
            # Проверяем доступ к исходному каналу
            try:
                source_entity = await self.client.get_entity(self.config.source_group_id)
                self.logger.info(f"✅ Доступ к исходному каналу: {getattr(source_entity, 'title', 'N/A')}")
                
                # Проверяем чтение сообщений
                async for _ in self.client.iter_messages(source_entity, limit=1):
                    self.logger.info("✅ Чтение сообщений исходного канала: OK")
                    break
            except Exception as e:
                self.logger.error(f"❌ Проблема с исходным каналом: {e}")
                raise
            
            # Проверяем доступ к целевому каналу
            try:
                target_entity = await self.client.get_entity(self.config.target_group_id)
                self.logger.info(f"✅ Доступ к целевому каналу: {getattr(target_entity, 'title', 'N/A')}")
                
                if not self.config.dry_run:
                    # Тестируем запись (только если не dry_run)
                    test_msg = await self.client.send_message(target_entity, "🔧 Тест доступа")
                    await self.client.delete_messages(target_entity, test_msg)
                    self.logger.info("✅ Запись в целевой канал: OK")
            except Exception as e:
                self.logger.error(f"❌ Проблема с целевым каналом: {e}")
                raise
            
            # Проверяем доступ к группе комментариев (если указана)
            if hasattr(self.config, 'discussion_group_id') and self.config.discussion_group_id:
                try:
                    discussion_entity = await self.client.get_entity(self.config.discussion_group_id)
                    self.logger.info(f"✅ Доступ к группе комментариев: {getattr(discussion_entity, 'title', 'N/A')}")
                    
                    # Проверяем чтение сообщений
                    async for _ in self.client.iter_messages(discussion_entity, limit=1):
                        self.logger.info("✅ Чтение сообщений группы комментариев: OK")
                        break
                except Exception as e:
                    self.logger.warning(f"⚠️ Проблема с группой комментариев: {e}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки доступа: {e}")
            raise
    
    async def _run_copying_process(self):
        """Запуск процесса копирования."""
        try:
            self.logger.info("🚀 Начинаем процесс копирования приватного канала...")
            
            if self.config.dry_run:
                self.logger.info("🔧 РЕЖИМ DRY RUN - реальная отправка отключена")
            
            # Запуск батчевого копирования
            stats = await self.copier.batch_copy_all_messages()
            
            # Финальная статистика
            self.logger.info("🎉 Процесс копирования завершен!")
            self.logger.info(f"📊 Финальная статистика:")
            self.logger.info(f"   ✅ Скопировано сообщений: {stats.get('copied_messages', 0)}")
            self.logger.info(f"   ❌ Ошибок сообщений: {stats.get('failed_messages', 0)}")
            self.logger.info(f"   ⏱️ Время выполнения: {stats.get('execution_time', 'N/A')}")
            
            # Дополнительная диагностика кэша комментариев
            cache_size = len(self.copier.comments_cache)
            if cache_size > 0:
                total_comments = sum(len(comments) for comments in self.copier.comments_cache.values())
                self.logger.info(f"   💬 Комментариев в кэше: {total_comments} для {cache_size} постов")
            else:
                self.logger.warning("   ⚠️ Кэш комментариев пуст - возможно, комментарии не найдены")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка процесса копирования: {e}")
            raise
    
    async def _cleanup(self):
        """Очистка ресурсов."""
        try:
            if self.client:
                await self.client.disconnect()
            self.logger.info("🔒 Ресурсы освобождены")
        except Exception as e:
            self.logger.error(f"❌ Ошибка очистки ресурсов: {e}")


async def main():
    """Главная функция."""
    app = PrivateChannelCopierApp()
    success = await app.run()
    return 0 if success else 1


if __name__ == "__main__":
    # Проверка версии Python
    if sys.version_info < (3, 7):
        print("❌ Требуется Python 3.7 или выше")
        sys.exit(1)
    
    print("🔧 Запуск Telegram Copier для приватных каналов")
    print("📋 Для диагностики используйте: python diagnose_private_channel.py")
    print("📋 Для обычных каналов используйте: python main.py")
    print("")
    
    # Запуск приложения
    exit_code = asyncio.run(main())
    sys.exit(exit_code)