#!/usr/bin/env python3
"""
Test script for the improved entity resolution functionality.
"""

import asyncio
import sys
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from config import Config
from telegram_copier_v3 import TelegramCopierV3
from utils import setup_logging


async def test_entity_resolution():
    """Тестирование улучшенного механизма поиска entities."""
    config = Config()
    logger = setup_logging()
    
    if not config.validate():
        logger.error("❌ Ошибка валидации конфигурации")
        return False
    
    client = None
    try:
        # Создание клиента
        client = TelegramClient(
            session=config.session_name,
            api_id=config.api_id,
            api_hash=config.api_hash,
            proxy=config.get_proxy_config()
        )
        
        # Подключение
        await client.start()
        
        # Проверка авторизации
        if not await client.is_user_authorized():
            logger.info("📱 Требуется авторизация...")
            await client.send_code_request(config.phone)
            code = input("Введите код авторизации: ")
            
            try:
                await client.sign_in(config.phone, code)
            except SessionPasswordNeededError:
                password = input("Введите двухфакторный пароль: ")
                await client.sign_in(password=password)
        
        me = await client.get_me()
        logger.info(f"✅ Авторизован как: {me.first_name}")
        
        # Создание копировщика для тестирования
        logger.info("🧪 Тестирование улучшенного механизма entity resolution...")
        
        copier = TelegramCopierV3(
            client=client,
            source_channel_id=config.source_group_id,
            target_channel_id=config.target_group_id,
            database_path="test_telegram_copier.db",
            dry_run=True,  # Тестовый режим
            delay_seconds=1,
            flatten_structure=False
        )
        
        # Тестирование инициализации
        logger.info("🔍 Попытка инициализации с улучшенной диагностикой...")
        try:
            await copier.initialize()
            logger.info("✅ Инициализация прошла успешно!")
            logger.info(f"   Источник: {getattr(copier.source_entity, 'title', 'N/A')}")
            logger.info(f"   Цель: {getattr(copier.target_entity, 'title', 'N/A')}")
            
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации: {e}")
            logger.info("💡 Проверьте отчет об ошибке выше для диагностики проблемы")
            
        finally:
            copier.close()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка тестирования: {e}")
        import traceback
        logger.error(f"Детали: {traceback.format_exc()}")
        return False
        
    finally:
        if client:
            await client.disconnect()


if __name__ == "__main__":
    print("🧪 Тест улучшенного механизма entity resolution")
    print("=" * 60)
    
    if sys.version_info < (3, 7):
        print("❌ Требуется Python 3.7 или выше")
        sys.exit(1)
    
    success = asyncio.run(test_entity_resolution())
    sys.exit(0 if success else 1)