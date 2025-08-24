#!/usr/bin/env python3
"""
Скрипт для диагностики проблем с получением комментариев из discussion groups.
"""

import asyncio
import logging
import sys
from telethon import TelegramClient
from telethon.tl.types import PeerChannel
from config import API_ID, API_HASH, SESSION_NAME

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
    ]
)

async def debug_comments():
    """Диагностика получения комментариев."""
    
    # Инициализация клиента
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    
    try:
        # ID каналов из ваших логов
        source_channel_id = -1002399927446
        discussion_group_id = 2265091305  # Из ваших логов
        
        # Тестовые ID сообщений из ваших логов
        test_message_ids = [8565, 8566, 8567, 8568, 8569]
        
        print(f"🔍 Диагностика комментариев для канала {source_channel_id}")
        print(f"🔍 Discussion group ID: {discussion_group_id}")
        
        # Получаем сущности
        source_channel = await client.get_entity(PeerChannel(abs(source_channel_id)))
        discussion_group = await client.get_entity(PeerChannel(discussion_group_id))
        
        print(f"✅ Исходный канал: {source_channel.title}")
        print(f"✅ Discussion group: {discussion_group.title}")
        
        # Тестируем несколько сообщений
        for message_id in test_message_ids:
            print(f"\n🧪 Тестируем сообщение ID: {message_id}")
            
            try:
                # Получаем сообщение из исходного канала
                message = await client.get_messages(source_channel, ids=message_id)
                if not message:
                    print(f"❌ Сообщение {message_id} не найдено в исходном канале")
                    continue
                
                print(f"📝 Сообщение найдено: '{getattr(message, 'message', 'Без текста')[:50]}...'")
                
                # Проверяем replies
                if hasattr(message, 'replies') and message.replies:
                    print(f"✅ Есть replies: comments={message.replies.comments if hasattr(message.replies, 'comments') else 'N/A'}")
                    print(f"✅ Channel ID в replies: {message.replies.channel_id if hasattr(message.replies, 'channel_id') else 'N/A'}")
                else:
                    print(f"❌ Нет replies у сообщения {message_id}")
                    continue
                
                # Ищем соответствующее сообщение в discussion group
                print(f"🔍 Ищем соответствующее сообщение в discussion group...")
                
                found_message = None
                checked_count = 0
                
                async for disc_message in client.iter_messages(discussion_group, limit=100):
                    checked_count += 1
                    
                    # Проверяем forward
                    if (hasattr(disc_message, 'forward') and disc_message.forward and 
                        hasattr(disc_message.forward, 'channel_post')):
                        
                        if disc_message.forward.channel_post == message_id:
                            found_message = disc_message
                            print(f"✅ Найдено через forward: discussion ID {disc_message.id}")
                            break
                    
                    # Проверяем по тексту
                    if hasattr(message, 'message') and message.message and hasattr(disc_message, 'message') and disc_message.message:
                        if message.message.strip() == disc_message.message.strip():
                            found_message = disc_message
                            print(f"✅ Найдено через текст: discussion ID {disc_message.id}")
                            break
                
                print(f"🔍 Проверено {checked_count} сообщений в discussion group")
                
                if found_message:
                    # Ищем комментарии к найденному сообщению
                    print(f"🔍 Ищем комментарии к discussion сообщению {found_message.id}...")
                    
                    comments = []
                    async for comment in client.iter_messages(
                        discussion_group, 
                        reply_to=found_message.id,
                        limit=50
                    ):
                        comments.append(comment)
                    
                    print(f"💬 Найдено {len(comments)} комментариев")
                    
                    for i, comment in enumerate(comments[:5]):  # Показываем первые 5
                        comment_text = getattr(comment, 'message', 'Без текста')[:30]
                        print(f"   {i+1}. ID {comment.id}: '{comment_text}...'")
                else:
                    print(f"❌ Соответствующее сообщение в discussion group не найдено")
                    
            except Exception as e:
                print(f"❌ Ошибка при обработке сообщения {message_id}: {e}")
        
        print(f"\n🎯 Диагностика завершена")
        
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(debug_comments())