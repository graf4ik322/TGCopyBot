#!/usr/bin/env python3
"""
Тестовый скрипт для проверки доступа к каналу/группе
"""

import asyncio
import os
from telethon import TelegramClient
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

async def test_channel_access():
    """Тестируем доступ к каналу"""
    
    # Получаем конфигурацию
    api_id_str = os.getenv('API_ID', '0')
    api_hash = os.getenv('API_HASH', '')
    phone = os.getenv('PHONE', '')
    source_group_id = os.getenv('SOURCE_GROUP_ID', '')
    target_group_id = os.getenv('TARGET_GROUP_ID', '')
    
    # Проверяем, что значения не являются placeholder-ами
    if api_id_str in ['your_api_id_here', '0'] or not api_id_str.isdigit():
        print("❌ Ошибка: API_ID не задан или содержит placeholder-значение")
        print("   Получите API_ID на https://my.telegram.org/apps")
        return
    
    api_id = int(api_id_str)
    
    if api_hash in ['your_api_hash_here', '']:
        print("❌ Ошибка: API_HASH не задан или содержит placeholder-значение")
        print("   Получите API_HASH на https://my.telegram.org/apps")
        return
    
    if phone in ['+1234567890', '']:
        print("❌ Ошибка: PHONE не задан или содержит placeholder-значение")
        print("   Укажите ваш реальный номер телефона с кодом страны")
        return
    
    if source_group_id == '':
        print("❌ Ошибка: SOURCE_GROUP_ID не задан")
        print("   Укажите ID или username исходной группы/канала")
        return
    
    if target_group_id in ['@target_group_username', '']:
        print("❌ Ошибка: TARGET_GROUP_ID не задан или содержит placeholder-значение")
        print("   Укажите ID или username целевой группы/канала")
        return
    
    print(f"API_ID: {api_id}")
    print(f"API_HASH: {api_hash[:10]}..." if api_hash else "API_HASH: не задан")
    print(f"PHONE: {phone}")
    print(f"SOURCE_GROUP_ID: {source_group_id} (тип: {type(source_group_id)})")
    print(f"TARGET_GROUP_ID: {target_group_id} (тип: {type(target_group_id)})")
    
    print("\n✅ Конфигурация выглядит корректной!")
    
    # Создаем клиент
    client = TelegramClient('test_session', api_id, api_hash)
    
    try:
        # Подключаемся
        await client.start(phone)
        print("Клиент подключен")
        
        # Получаем информацию о себе
        me = await client.get_me()
        print(f"Авторизован как: {me.first_name} (@{me.username or 'no_username'})")
        
        # Тестируем доступ к исходному каналу/группе
        print(f"\nТестируем доступ к исходному каналу: {source_group_id}")
        
        try:
            # Пробуем разные способы получения entity
            print("Способ 1: get_entity с исходным ID")
            source_entity = await client.get_entity(source_group_id)
            print(f"✅ Успех! Название: {source_entity.title}")
            print(f"   Тип: {type(source_entity).__name__}")
            print(f"   ID: {source_entity.id}")
            
        except Exception as e:
            print(f"❌ Ошибка get_entity: {e}")
            
            # Пробуем преобразовать в число
            try:
                if source_group_id.startswith('-100'):
                    numeric_id = int(source_group_id)
                    print(f"Пробуем числовой ID: {numeric_id}")
                    source_entity = await client.get_entity(numeric_id)
                    print(f"✅ Успех с числовым ID! Название: {source_entity.title}")
                else:
                    print("ID не начинается с -100, пропускаем числовое преобразование")
            except Exception as e2:
                print(f"❌ Ошибка с числовым ID: {e2}")
            
            # Пробуем получить через username если это username
            if source_group_id.startswith('@'):
                try:
                    print(f"Пробуем через username: {source_group_id}")
                    source_entity = await client.get_entity(source_group_id)
                    print(f"✅ Успех через username! Название: {source_entity.title}")
                except Exception as e3:
                    print(f"❌ Ошибка через username: {e3}")
        
        # Тестируем доступ к целевому каналу/группе
        print(f"\nТестируем доступ к целевому каналу: {target_group_id}")
        
        try:
            target_entity = await client.get_entity(target_group_id)
            print(f"✅ Успех! Название: {target_entity.title}")
            print(f"   Тип: {type(target_entity).__name__}")
            print(f"   ID: {target_entity.id}")
            
        except Exception as e:
            print(f"❌ Ошибка get_entity: {e}")
            
            # Пробуем преобразовать в число
            try:
                if target_group_id.startswith('-100'):
                    numeric_id = int(target_group_id)
                    print(f"Пробуем числовой ID: {numeric_id}")
                    target_entity = await client.get_entity(numeric_id)
                    print(f"✅ Успех с числовым ID! Название: {target_entity.title}")
                else:
                    print("ID не начинается с -100, пропускаем числовое преобразование")
            except Exception as e2:
                print(f"❌ Ошибка с числовым ID: {e2}")
            
            # Пробуем получить через username если это username
            if target_group_id.startswith('@'):
                try:
                    print(f"Пробуем через username: {target_group_id}")
                    target_entity = await client.get_entity(target_group_id)
                    print(f"✅ Успех через username! Название: {target_entity.title}")
                except Exception as e3:
                    print(f"❌ Ошибка через username: {e3}")
        
        # Проверяем права доступа
        if 'source_entity' in locals():
            print(f"\nПроверяем права доступа к исходному каналу...")
            try:
                # Пытаемся получить сообщения
                message_count = 0
                async for message in client.iter_messages(source_entity, limit=5):
                    message_count += 1
                    if message_count == 1:
                        print(f"✅ Первое сообщение получено: {message.text[:50]}...")
                        break
                
                if message_count == 0:
                    print("⚠️  Сообщения не найдены или нет доступа")
                    
            except Exception as e:
                print(f"❌ Ошибка получения сообщений: {e}")
        
        if 'target_entity' in locals():
            print(f"\nПроверяем права доступа к целевому каналу...")
            try:
                # Проверяем, можем ли мы отправлять сообщения
                me = await client.get_me()
                print(f"✅ Информация о себе получена: {me.first_name}")
                
                # Пытаемся получить участников (если это группа)
                try:
                    participant_count = 0
                    async for participant in client.iter_participants(target_entity, limit=5):
                        participant_count += 1
                        if participant.id == me.id:
                            print(f"✅ Вы являетесь участником целевого канала")
                            break
                    
                    if participant_count == 0:
                        print("⚠️  Участники не найдены или нет доступа")
                        
                except Exception as e:
                    print(f"⚠️  Не удалось проверить участников: {e}")
                    
            except Exception as e:
                print(f"❌ Ошибка проверки прав: {e}")
        
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
    
    finally:
        await client.disconnect()
        print("\nКлиент отключен")

if __name__ == "__main__":
    asyncio.run(test_channel_access())