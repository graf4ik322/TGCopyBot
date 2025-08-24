#!/usr/bin/env python3
"""
Диагностический скрипт для анализа приватного канала и связанной группы комментариев.
Помогает выявить проблемы с обнаружением и связыванием discussion groups.
"""

import asyncio
import logging
from telethon import TelegramClient
from telethon.tl.types import PeerChannel
from telethon.tl.functions.channels import GetFullChannelRequest

from config import Config


class PrivateChannelDiagnostic:
    """Диагностика приватного канала с комментариями."""
    
    def __init__(self):
        """Инициализация диагностики."""
        self.config = Config()
        self.logger = logging.getLogger('diagnostic')
        self.client = None
        
        # Настройка логирования
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    async def run_diagnosis(self):
        """Запуск диагностики."""
        try:
            # Инициализация клиента
            self.client = TelegramClient(
                self.config.session_name,
                self.config.api_id,
                self.config.api_hash
            )
            
            await self.client.start()
            
            if not await self.client.is_user_authorized():
                self.logger.error("❌ Клиент не авторизован")
                return
            
            self.logger.info("✅ Клиент авторизован")
            
            # Получаем информацию о канале
            await self._analyze_source_channel()
            
            # Анализируем отдельную группу комментариев (если указана)
            if self.config.discussion_group_id:
                await self._analyze_discussion_group()
            
            # Проверяем связи между каналом и группой
            await self._check_channel_group_links()
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка диагностики: {e}")
            import traceback
            self.logger.error(f"Детали: {traceback.format_exc()}")
        
        finally:
            if self.client:
                await self.client.disconnect()
    
    async def _analyze_source_channel(self):
        """Анализ исходного канала."""
        try:
            self.logger.info(f"🔍 Анализируем исходный канал: {self.config.source_group_id}")
            
            # Получаем сущность канала
            source_entity = await self.client.get_entity(self.config.source_group_id)
            self.logger.info(f"   📋 Тип: {type(source_entity).__name__}")
            self.logger.info(f"   📋 ID: {source_entity.id}")
            self.logger.info(f"   📋 Название: {getattr(source_entity, 'title', 'N/A')}")
            self.logger.info(f"   📋 Username: {getattr(source_entity, 'username', 'N/A')}")
            
            # Получаем полную информацию о канале
            try:
                full_channel = await self.client(GetFullChannelRequest(source_entity))
                full_chat = full_channel.full_chat
                
                self.logger.info(f"   📋 Подписчики: {getattr(full_chat, 'participants_count', 'N/A')}")
                self.logger.info(f"   📋 Linked chat ID: {getattr(full_chat, 'linked_chat_id', 'N/A')}")
                
                # Проверяем наличие связанной группы
                if hasattr(full_chat, 'linked_chat_id') and full_chat.linked_chat_id:
                    linked_chat_id = full_chat.linked_chat_id
                    self.logger.info(f"✅ Найдена связанная группа: {linked_chat_id}")
                    
                    # Получаем информацию о связанной группе
                    try:
                        linked_entity = await self.client.get_entity(PeerChannel(linked_chat_id))
                        self.logger.info(f"   📋 Связанная группа: {getattr(linked_entity, 'title', 'N/A')}")
                        self.logger.info(f"   📋 Username связанной группы: {getattr(linked_entity, 'username', 'N/A')}")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Не удалось получить информацию о связанной группе: {e}")
                else:
                    self.logger.warning("⚠️ Связанная группа не найдена в метаданных канала")
                
            except Exception as e:
                self.logger.warning(f"⚠️ Не удалось получить полную информацию о канале: {e}")
            
            # Анализируем несколько сообщений канала
            await self._analyze_channel_messages(source_entity)
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа канала: {e}")
    
    async def _analyze_channel_messages(self, source_entity):
        """Анализ сообщений канала для поиска групп комментариев."""
        try:
            self.logger.info("🔍 Анализируем сообщения канала...")
            
            discussion_groups_found = set()
            messages_with_comments = 0
            total_analyzed = 0
            
            # Анализируем первые 100 сообщений
            async for message in self.client.iter_messages(source_entity, limit=100):
                total_analyzed += 1
                
                # Проверяем наличие replies
                if hasattr(message, 'replies') and message.replies:
                    messages_with_comments += 1
                    
                    self.logger.info(f"   💬 Сообщение {message.id}:")
                    self.logger.info(f"      - Есть replies: {bool(message.replies)}")
                    
                    if hasattr(message.replies, 'comments'):
                        self.logger.info(f"      - Комментарии включены: {message.replies.comments}")
                        
                    if hasattr(message.replies, 'replies'):
                        self.logger.info(f"      - Количество ответов: {message.replies.replies}")
                        
                    if hasattr(message.replies, 'channel_id'):
                        channel_id = message.replies.channel_id
                        self.logger.info(f"      - Channel ID для комментариев: {channel_id}")
                        discussion_groups_found.add(channel_id)
                        
                        # Пытаемся получить информацию о группе комментариев
                        try:
                            discussion_entity = await self.client.get_entity(PeerChannel(channel_id))
                            self.logger.info(f"      - Название группы комментариев: {getattr(discussion_entity, 'title', 'N/A')}")
                        except Exception as e:
                            self.logger.warning(f"      - Не удалось получить информацию о группе {channel_id}: {e}")
                
                # Показываем прогресс каждые 10 сообщений
                if total_analyzed % 10 == 0:
                    self.logger.info(f"   📊 Проанализировано {total_analyzed} сообщений, найдено {messages_with_comments} с комментариями")
            
            self.logger.info(f"📊 Анализ завершен:")
            self.logger.info(f"   - Всего проанализировано: {total_analyzed} сообщений")
            self.logger.info(f"   - Сообщений с комментариями: {messages_with_comments}")
            self.logger.info(f"   - Найдено discussion groups: {len(discussion_groups_found)}")
            
            for group_id in discussion_groups_found:
                self.logger.info(f"   - Discussion group ID: {group_id}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа сообщений: {e}")
    
    async def _analyze_discussion_group(self):
        """Анализ отдельной группы комментариев."""
        try:
            self.logger.info(f"🔍 Анализируем группу комментариев: {self.config.discussion_group_id}")
            
            # Получаем сущность группы
            discussion_entity = await self.client.get_entity(self.config.discussion_group_id)
            self.logger.info(f"   📋 Тип: {type(discussion_entity).__name__}")
            self.logger.info(f"   📋 ID: {discussion_entity.id}")
            self.logger.info(f"   📋 Название: {getattr(discussion_entity, 'title', 'N/A')}")
            self.logger.info(f"   📋 Username: {getattr(discussion_entity, 'username', 'N/A')}")
            
            # Анализируем сообщения в группе
            await self._analyze_discussion_messages(discussion_entity)
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа группы комментариев: {e}")
    
    async def _analyze_discussion_messages(self, discussion_entity):
        """Анализ сообщений в группе комментариев."""
        try:
            self.logger.info("🔍 Анализируем сообщения в группе комментариев...")
            
            forwarded_messages = 0
            comments = 0
            total_analyzed = 0
            reply_patterns = {}
            
            # Анализируем первые 50 сообщений
            async for message in self.client.iter_messages(discussion_entity, limit=50):
                total_analyzed += 1
                
                # Проверяем пересланные сообщения
                if hasattr(message, 'fwd_from') and message.fwd_from:
                    forwarded_messages += 1
                    self.logger.info(f"   📤 Сообщение {message.id} переслано:")
                    
                    if hasattr(message.fwd_from, 'channel_post'):
                        channel_post_id = message.fwd_from.channel_post
                        self.logger.info(f"      - Исходный пост в канале: {channel_post_id}")
                    
                    if hasattr(message.fwd_from, 'from_id'):
                        from_id = message.fwd_from.from_id
                        self.logger.info(f"      - От пользователя/канала: {from_id}")
                
                # Проверяем ответы
                if hasattr(message, 'reply_to') and message.reply_to:
                    comments += 1
                    reply_to_id = message.reply_to.reply_to_msg_id
                    
                    if reply_to_id not in reply_patterns:
                        reply_patterns[reply_to_id] = 0
                    reply_patterns[reply_to_id] += 1
                    
                    self.logger.info(f"   💬 Сообщение {message.id} - ответ на {reply_to_id}")
            
            self.logger.info(f"📊 Анализ группы комментариев завершен:")
            self.logger.info(f"   - Всего проанализировано: {total_analyzed} сообщений")
            self.logger.info(f"   - Пересланных сообщений: {forwarded_messages}")
            self.logger.info(f"   - Комментариев (ответов): {comments}")
            self.logger.info(f"   - Уникальных постов с комментариями: {len(reply_patterns)}")
            
            # Показываем топ постов по количеству комментариев
            if reply_patterns:
                sorted_patterns = sorted(reply_patterns.items(), key=lambda x: x[1], reverse=True)
                self.logger.info("   📈 Топ постов по комментариям:")
                for post_id, comment_count in sorted_patterns[:5]:
                    self.logger.info(f"      - Пост {post_id}: {comment_count} комментариев")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка анализа сообщений группы: {e}")
    
    async def _check_channel_group_links(self):
        """Проверка связей между каналом и группой."""
        try:
            self.logger.info("🔗 Проверяем связи между каналом и группой...")
            
            if not self.config.discussion_group_id:
                self.logger.warning("⚠️ ID группы комментариев не указан в конфигурации")
                return
            
            source_entity = await self.client.get_entity(self.config.source_group_id)
            discussion_entity = await self.client.get_entity(self.config.discussion_group_id)
            
            # Проверяем доступ к обеим сущностям
            self.logger.info(f"✅ Доступ к каналу: {getattr(source_entity, 'title', 'N/A')}")
            self.logger.info(f"✅ Доступ к группе: {getattr(discussion_entity, 'title', 'N/A')}")
            
            # Тестируем получение сообщений
            try:
                async for message in self.client.iter_messages(source_entity, limit=1):
                    self.logger.info(f"✅ Чтение сообщений канала: OK")
                    break
            except Exception as e:
                self.logger.error(f"❌ Ошибка чтения канала: {e}")
            
            try:
                async for message in self.client.iter_messages(discussion_entity, limit=1):
                    self.logger.info(f"✅ Чтение сообщений группы: OK")
                    break
            except Exception as e:
                self.logger.error(f"❌ Ошибка чтения группы: {e}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки связей: {e}")


async def main():
    """Главная функция диагностики."""
    diagnostic = PrivateChannelDiagnostic()
    await diagnostic.run_diagnosis()


if __name__ == "__main__":
    asyncio.run(main())