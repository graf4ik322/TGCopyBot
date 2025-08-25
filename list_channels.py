#!/usr/bin/env python3
"""
Utility script to list available channels for debugging channel ID configuration.
"""

import asyncio
import sys
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
from config import Config
from utils import setup_logging


async def list_available_channels():
    """Список доступных каналов для отладки конфигурации."""
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
        
        # Получение списка диалогов
        logger.info("📋 Получение списка доступных каналов...")
        dialogs = await client.get_dialogs(limit=200)
        
        channels = []
        groups = []
        users = []
        
        for dialog in dialogs:
            entity = dialog.entity
            entity_info = {
                'id': entity.id,
                'title': getattr(entity, 'title', f"{getattr(entity, 'first_name', '')} {getattr(entity, 'last_name', '')}".strip()),
                'username': getattr(entity, 'username', None),
                'type': type(entity).__name__
            }
            
            if 'Channel' in entity_info['type']:
                channels.append(entity_info)
            elif 'Chat' in entity_info['type']:
                groups.append(entity_info)
            else:
                users.append(entity_info)
        
        # Вывод результатов
        print("\n" + "="*80)
        print("📋 ДОСТУПНЫЕ КАНАЛЫ И ГРУППЫ")
        print("="*80)
        
        if channels:
            print(f"\n📺 КАНАЛЫ ({len(channels)}):")
            print("-" * 50)
            for channel in channels:
                username_str = f"@{channel['username']}" if channel['username'] else "Без username"
                print(f"ID: {channel['id']}")
                print(f"Название: {channel['title']}")
                print(f"Username: {username_str}")
                print(f"Тип: {channel['type']}")
                print("-" * 30)
        
        if groups:
            print(f"\n👥 ГРУППЫ ({len(groups)}):")
            print("-" * 50)
            for group in groups:
                username_str = f"@{group['username']}" if group['username'] else "Без username"
                print(f"ID: {group['id']}")
                print(f"Название: {group['title']}")
                print(f"Username: {username_str}")
                print(f"Тип: {group['type']}")
                print("-" * 30)
        
        print(f"\n📊 СТАТИСТИКА:")
        print(f"Всего диалогов: {len(dialogs)}")
        print(f"Каналы: {len(channels)}")
        print(f"Группы: {len(groups)}")
        print(f"Личные чаты: {len(users)}")
        
        print(f"\n💡 ПОДСКАЗКИ:")
        print("• Для конфигурации используйте ID из списка выше")
        print("• ID каналов обычно отрицательные числа")
        print("• Можно использовать username с символом @ (например, @channel_name)")
        print("• Убедитесь, что вы подписаны на канал перед копированием")
        
        # Проверка текущей конфигурации
        if config.source_group_id:
            print(f"\n🔍 ПРОВЕРКА ТЕКУЩЕЙ КОНФИГУРАЦИИ:")
            print(f"Исходный канал в конфиге: {config.source_group_id}")
            
            source_found = False
            for channel in channels + groups:
                if (str(channel['id']) == str(config.source_group_id) or 
                    (channel['username'] and f"@{channel['username']}" == config.source_group_id)):
                    print(f"✅ Исходный канал найден: {channel['title']}")
                    source_found = True
                    break
            
            if not source_found:
                print(f"❌ Исходный канал '{config.source_group_id}' не найден в доступных!")
                print("   Возможно, вы не подписаны на этот канал.")
        
        if config.target_group_id:
            print(f"Целевой канал в конфиге: {config.target_group_id}")
            
            target_found = False
            for channel in channels + groups:
                if (str(channel['id']) == str(config.target_group_id) or 
                    (channel['username'] and f"@{channel['username']}" == config.target_group_id)):
                    print(f"✅ Целевой канал найден: {channel['title']}")
                    target_found = True
                    break
            
            if not target_found:
                print(f"❌ Целевой канал '{config.target_group_id}' не найден в доступных!")
                print("   Возможно, вы не являетесь администратором этого канала.")
        
        print("\n" + "="*80)
        return True
        
    except Exception as e:
        logger.error(f"❌ Ошибка: {e}")
        import traceback
        logger.error(f"Детали: {traceback.format_exc()}")
        return False
        
    finally:
        if client:
            await client.disconnect()


if __name__ == "__main__":
    print("🔍 Утилита для просмотра доступных каналов")
    print("=" * 50)
    
    if sys.version_info < (3, 7):
        print("❌ Требуется Python 3.7 или выше")
        sys.exit(1)
    
    success = asyncio.run(list_available_channels())
    sys.exit(0 if success else 1)