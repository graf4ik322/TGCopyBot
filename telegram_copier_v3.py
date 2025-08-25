"""
Telegram Copier v3.0 - Completely rewritten implementation
Following Telethon best practices and addressing all critical issues:

1. ✅ Proper album handling using grouped_id
2. ✅ Correct comment copying from discussion groups  
3. ✅ Persistent SQLite database for state management
4. ✅ Chronological order processing
5. ✅ Proper media handling without MediaProxy issues
6. ✅ Support for all media types in posts and comments

Based on Telethon documentation: https://tl.telethon.dev/
"""

import asyncio
import sqlite3
import logging
import json
import os
import tempfile
from typing import List, Dict, Optional, Any, Tuple, Union
from datetime import datetime, timezone
from dataclasses import dataclass

from telethon import TelegramClient, events
from telethon.tl.types import (
    Message, MessageMediaPhoto, MessageMediaDocument, 
    PeerChannel, DocumentAttributeFilename, MessageEntityTextUrl,
    MessageEntityUrl, MessageEntityMention, MessageEntityHashtag,
    MessageEntityBold, MessageEntityItalic, MessageEntityCode,
    MessageEntityPre, MessageEntityStrike, MessageEntityUnderline,
    MessageEntitySpoiler, MessageEntityBlockquote
)
from telethon.tl import functions
from telethon.tl.functions.contacts import ResolveUsernameRequest
from telethon.tl.functions.channels import GetChannelsRequest
from telethon.errors import FloodWaitError, PeerFloodError, MediaInvalidError, ChannelPrivateError, ChatInvalidError


@dataclass
class ProcessingStats:
    """Статистика обработки."""
    total_posts: int = 0
    copied_posts: int = 0
    failed_posts: int = 0
    total_comments: int = 0
    copied_comments: int = 0
    failed_comments: int = 0
    total_albums: int = 0
    copied_albums: int = 0
    failed_albums: int = 0


class TelegramCopierV3:
    """
    Telegram Copier v3.0 - Полностью переписанная реализация.
    
    Основные улучшения:
    - Использование SQLite для персистентного хранения
    - Правильная обработка альбомов через grouped_id
    - Корректное копирование комментариев из discussion groups
    - Хронологический порядок обработки
    - Поддержка всех типов медиа
    """
    
    def __init__(self, 
                 client: TelegramClient,
                 source_channel_id: Union[int, str],
                 target_channel_id: Union[int, str],
                 database_path: str = "telegram_copier_v3.db",
                 dry_run: bool = False,
                 delay_seconds: int = 3,
                 flatten_structure: bool = False):
        """
        Инициализация копировщика.
        
        Args:
            client: Авторизованный Telegram клиент
            source_channel_id: ID или username исходного канала
            target_channel_id: ID или username целевого канала  
            database_path: Путь к файлу базы данных
            dry_run: Режим симуляции без реальной отправки
            delay_seconds: Задержка между отправками сообщений
            flatten_structure: Режим антивложенности - комментарии как обычные посты
        """
        self.client = client
        self.source_channel_id = source_channel_id
        self.target_channel_id = target_channel_id
        self.database_path = database_path
        self.dry_run = dry_run
        self.delay_seconds = delay_seconds
        self.flatten_structure = flatten_structure
        
        self.logger = logging.getLogger('telegram_copier_v3')
        
        # Логируем режим антивложенности
        if self.flatten_structure:
            self.logger.info("🔄 Включен режим антивложенности - комментарии будут превращены в обычные посты")
        
        # Кэш для entities
        self.source_entity = None
        self.target_entity = None
        self.discussion_entity = None
        self.target_discussion_entity = None
        
        # Статистика
        self.stats = ProcessingStats()
        
        # База данных
        self.db_connection = None
        
        # Флаг остановки
        self.stop_requested = False
    
    async def initialize(self):
        """ИСПРАВЛЕНО: Инициализация копировщика с улучшенной обработкой entities."""
        try:
            # Инициализация базы данных
            self._init_database()
            
            # ИСПРАВЛЕНО: Безопасное получение entities каналов с retry
            self.source_entity = await self._get_entity_safe(self.source_channel_id, "исходного канала")
            self.target_entity = await self._get_entity_safe(self.target_channel_id, "целевого канала")
            
            self.logger.info(f"✅ Источник: {getattr(self.source_entity, 'title', 'N/A')}")
            self.logger.info(f"✅ Цель: {getattr(self.target_entity, 'title', 'N/A')}")
            
            # Попытка найти discussion groups
            await self._find_discussion_groups()
            
            self.logger.info("✅ Копировщик v3.0 инициализирован")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации: {e}")
            raise
    
    async def _get_entity_safe(self, entity_id: Union[str, int], entity_name: str, max_retries: int = 5):
        """
        УЛУЧШЕНО: Безопасное получение entity с усиленным retry механизмом.
        
        Args:
            entity_id: ID или username entity
            entity_name: Название для логирования
            max_retries: Максимальное количество попыток
            
        Returns:
            Entity объект
            
        Raises:
            Exception: Если entity не найдена после всех попыток
        """
        from telethon.errors import FloodWaitError, PeerFloodError, ChannelPrivateError, ChatInvalidError
        import asyncio
        
        # НОВОЕ: Обработка специальных ошибок Telegram API
        try:
            # Проверяем состояние клиента перед началом поиска
            if not self.client.is_connected():
                await self.client.connect()
                await asyncio.sleep(1)  # Даем время на стабилизацию соединения
        except Exception as e:
            self.logger.debug(f"Проверка подключения: {e}")
        
        self.logger.info(f"🔍 Поиск {entity_name}: {entity_id}")
        
        # НОВОЕ: Предварительная диагностика
        await self._diagnose_entity_access(entity_id, entity_name)
        
        # РАСШИРЕННЫЕ стратегии поиска для совместимости с предыдущими версиями
        strategies = [
            lambda: self._get_entity_direct(entity_id),
            lambda: self._get_entity_via_full_dialogs(entity_id),
            lambda: self._get_entity_via_peer_resolver(entity_id),
            lambda: self._get_entity_legacy_mode(entity_id),  # НОВОЕ: как в старых версиях
            lambda: self._get_entity_via_username_resolve(entity_id),
            lambda: self._get_entity_via_search(entity_id),
            lambda: self._get_entity_force_fetch(entity_id)   # НОВОЕ: принудительный поиск
        ]
        
        last_exception = None
        
        for attempt in range(max_retries):
            self.logger.debug(f"Попытка {attempt + 1}/{max_retries} для {entity_name}")
            
            for strategy_idx, strategy in enumerate(strategies):
                try:
                    entity = await strategy()
                    if entity:
                        # НОВОЕ: Проверяем доступ к найденной entity
                        if await self._validate_entity_access(entity, entity_name):
                            self.logger.info(f"✅ {entity_name} найден и доступен (стратегия {strategy_idx + 1})")
                            return entity
                        else:
                            self.logger.warning(f"⚠️ {entity_name} найден, но доступ ограничен")
                            continue
                        
                except (FloodWaitError, PeerFloodError) as e:
                    wait_time = getattr(e, 'seconds', 30)
                    self.logger.warning(f"FloodWait для {entity_name}: ожидание {wait_time}с")
                    await asyncio.sleep(wait_time)
                    
                except (ChannelPrivateError, ChatInvalidError) as e:
                    self.logger.error(f"❌ {entity_name} недоступен: {e}")
                    raise Exception(f"{entity_name} недоступен - канал приватный или не существует")
                
                except Exception as e:
                    last_exception = e
                    error_msg = str(e)
                    
                    # НОВОЕ: Специальная обработка ошибок Telegram API
                    if "HistoryGetFailedError" in error_msg or "GetChannelDifferenceRequest" in error_msg:
                        self.logger.warning(f"⚠️ Проблемы с Telegram API, пропускаем стратегию {strategy_idx + 1}")
                        await asyncio.sleep(2)  # Больше времени для восстановления API
                        continue
                    elif "internal issues" in error_msg.lower():
                        self.logger.warning(f"⚠️ Внутренние проблемы Telegram, ждем...")
                        await asyncio.sleep(5)
                        continue
                    
                    self.logger.debug(f"Стратегия {strategy_idx + 1} для {entity_name} не сработала: {e}")
                    continue
            
            # Пауза между попытками с экспоненциальным backoff
            if attempt < max_retries - 1:
                sleep_time = min(2 ** attempt, 30)  # Максимум 30 секунд
                self.logger.debug(f"Пауза {sleep_time}с перед следующей попыткой")
                await asyncio.sleep(sleep_time)
        
        # Все попытки исчерпаны
        error_msg = f"Entity {entity_name} ({entity_id}) не найдена после {max_retries} попыток"
        if last_exception:
            error_msg += f". Последняя ошибка: {last_exception}"
        
        # НОВОЕ: Добавляем детальную диагностику
        self.logger.error(f"❌ {error_msg}")
        await self._log_diagnostic_info(entity_id, entity_name)
        
        raise Exception(error_msg)
    
    async def _diagnose_entity_access(self, entity_id: Union[str, int], entity_name: str):
        """НОВОЕ: Предварительная диагностика доступа к entity."""
        try:
            self.logger.debug(f"🔍 Диагностика доступа к {entity_name}")
            
            # Проверяем статус авторизации
            if not await self.client.is_user_authorized():
                self.logger.warning("⚠️ Клиент не авторизован")
                return
            
            # Получаем информацию о текущем пользователе
            me = await self.client.get_me()
            self.logger.debug(f"📱 Авторизован как: {me.first_name} (ID: {me.id})")
            
            # Проверяем тип entity_id
            if isinstance(entity_id, int):
                self.logger.debug(f"🔢 Поиск по числовому ID: {entity_id}")
            else:
                self.logger.debug(f"📝 Поиск по строковому ID: {entity_id}")
                
        except Exception as e:
            self.logger.debug(f"⚠️ Ошибка диагностики: {e}")
    
    async def _get_entity_direct(self, entity_id: Union[str, int]):
        """Прямой поиск entity."""
        try:
            return await self.client.get_entity(entity_id)
        except Exception:
            return None
    
    async def _get_entity_via_full_dialogs(self, entity_id: Union[str, int]):
        """Поиск entity через полную синхронизацию диалогов."""
        try:
            self.logger.debug("🔄 Синхронизация диалогов...")
            
            # Получаем ПОЛНЫЙ список диалогов без ограничений
            dialogs = await self.client.get_dialogs()
            self.logger.debug(f"📁 Найдено {len(dialogs)} диалогов")
            
            # Поиск в диалогах
            for dialog in dialogs:
                if self._match_entity(dialog.entity, entity_id):
                    self.logger.debug(f"✅ Найден в диалогах: {getattr(dialog.entity, 'title', 'N/A')}")
                    return dialog.entity
                    
            # Повторная попытка get_entity после полной синхронизации
            return await self.client.get_entity(entity_id)
            
        except Exception as e:
            self.logger.debug(f"❌ Ошибка синхронизации диалогов: {e}")
            return None
    
    async def _get_entity_via_username_resolve(self, entity_id: Union[str, int]):
        """Поиск entity через разрешение username."""
        try:
            if isinstance(entity_id, str):
                # Убираем @ если есть
                username = entity_id.lstrip('@')
                
                # Пробуем через resolve_username
                result = await self.client(functions.contacts.ResolveUsernameRequest(username))
                
                if result.chats:
                    return result.chats[0]
                if result.users:
                    return result.users[0]
                    
        except Exception as e:
            self.logger.debug(f"❌ Ошибка resolve username: {e}")
            return None
    
    async def _get_entity_via_peer_resolver(self, entity_id: Union[str, int]):
        """Поиск entity через PeerChannel resolver."""
        try:
            if isinstance(entity_id, int):
                # Пробуем создать PeerChannel и получить entity
                from telethon.tl.types import PeerChannel
                peer = PeerChannel(entity_id)
                
                # Получаем информацию о канале
                result = await self.client(functions.channels.GetChannelsRequest([peer]))
                if result.chats:
                    return result.chats[0]
                    
        except Exception as e:
            self.logger.debug(f"❌ Ошибка peer resolver: {e}")
            return None
    
    async def _get_entity_legacy_mode(self, entity_id: Union[str, int]):
        """НОВОЕ: Поиск entity в режиме совместимости со старыми версиями."""
        try:
            self.logger.debug("🔄 Режим совместимости со старыми версиями...")
            
            # Стратегия 1: Прямой поиск как в старых версиях
            try:
                return await self.client.get_entity(entity_id)
            except Exception:
                pass
            
            # Стратегия 2: Поиск через InputPeerChannel (как в v2)
            if isinstance(entity_id, int):
                try:
                    from telethon.tl.types import InputPeerChannel
                    from telethon.utils import get_peer_id, resolve_id
                    
                    # Пробуем разные варианты ID
                    channel_ids_to_try = [
                        entity_id,
                        abs(entity_id),
                        entity_id + 1000000000000,  # Добавляем offset как в старых версиях
                    ]
                    
                    for channel_id in channel_ids_to_try:
                        try:
                            peer = InputPeerChannel(channel_id, 0)  # access_hash = 0 для попытки
                            result = await self.client(functions.channels.GetChannelsRequest([peer]))
                            if result.chats:
                                return result.chats[0]
                        except Exception:
                            continue
                            
                except Exception:
                    pass
            
            return None
            
        except Exception as e:
            self.logger.debug(f"❌ Ошибка legacy mode: {e}")
            return None
    
    async def _get_entity_force_fetch(self, entity_id: Union[str, int]):
        """НОВОЕ: Принудительный поиск через различные API методы."""
        try:
            self.logger.debug("🔄 Принудительный поиск через API...")
            
            # Метод 1: Через GetDialogsRequest с принудительной загрузкой
            try:
                from telethon.tl.functions.messages import GetDialogsRequest
                from telethon.tl.types import InputPeerEmpty
                
                result = await self.client(GetDialogsRequest(
                    offset_date=None,
                    offset_id=0,
                    offset_peer=InputPeerEmpty(),
                    limit=200,
                    hash=0
                ))
                
                # Ищем среди загруженных чатов
                for chat in result.chats:
                    if self._match_entity(chat, entity_id):
                        return chat
                        
            except Exception:
                pass
            
            # Метод 2: Прямой запрос к GetFullChannelRequest
            if isinstance(entity_id, int):
                try:
                    from telethon.tl.types import PeerChannel
                    from telethon.tl.functions.channels import GetFullChannelRequest
                    
                    peer = PeerChannel(abs(entity_id))
                    full_channel = await self.client(GetFullChannelRequest(peer))
                    
                    if hasattr(full_channel, 'chats') and full_channel.chats:
                        return full_channel.chats[0]
                        
                except Exception:
                    pass
            
            return None
            
        except Exception as e:
            self.logger.debug(f"❌ Ошибка force fetch: {e}")
            return None

    async def _get_entity_via_dialogs(self, entity_id: Union[str, int]):
        """Поиск entity через синхронизацию диалогов (старый метод)."""
        try:
            # Принудительная синхронизация диалогов
            dialogs = await self.client.get_dialogs(limit=200)
            
            # Поиск в диалогах
            for dialog in dialogs:
                if self._match_entity(dialog.entity, entity_id):
                    return dialog.entity
                    
            # Повторная попытка get_entity после синхронизации
            return await self.client.get_entity(entity_id)
            
        except Exception:
            return None
    
    async def _get_entity_via_search(self, entity_id: Union[str, int]):
        """Поиск entity через глобальный поиск."""
        try:
            if isinstance(entity_id, str) and entity_id.startswith('@'):
                username = entity_id[1:]
                
                # Поиск через API
                result = await self.client(functions.contacts.SearchRequest(
                    q=username,
                    limit=10
                ))
                
                # Проверяем результаты
                for chat in result.chats:
                    if hasattr(chat, 'username') and chat.username == username:
                        return chat
                        
                for user in result.users:
                    if hasattr(user, 'username') and user.username == username:
                        return user
                        
        except Exception:
            pass
            
        return None
    
    def _match_entity(self, entity, target_id: Union[str, int]) -> bool:
        """Проверка соответствия entity целевому ID."""
        try:
            # По ID
            if isinstance(target_id, int) and hasattr(entity, 'id'):
                return entity.id == target_id
                
            # По username
            if isinstance(target_id, str):
                if target_id.startswith('@'):
                    username = target_id[1:]
                else:
                    username = target_id
                    
                if hasattr(entity, 'username') and entity.username:
                    return entity.username.lower() == username.lower()
                    
        except Exception:
            pass
            
        return False
    
    def _init_database(self):
        """Инициализация SQLite базы данных."""
        try:
            self.db_connection = sqlite3.connect(self.database_path)
            self.db_connection.execute("PRAGMA foreign_keys = ON")
            
            # Таблица для постов канала
            self.db_connection.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    message_text TEXT,
                    date_posted TIMESTAMP,
                    media_type TEXT,
                    media_data TEXT,  -- JSON с информацией о медиа
                    entities TEXT,    -- JSON с форматированием
                    grouped_id INTEGER,  -- ID альбома
                    has_discussion BOOLEAN DEFAULT FALSE,
                    discussion_group_id TEXT,
                    processed BOOLEAN DEFAULT FALSE,
                    target_message_id INTEGER,
                    processed_at TIMESTAMP,
                    raw_message TEXT  -- Полные данные сообщения
                )
            """)
            
            # Таблица для комментариев
            self.db_connection.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY,
                    post_id INTEGER NOT NULL,
                    discussion_group_id TEXT,
                    comment_text TEXT,
                    date_posted TIMESTAMP,
                    media_type TEXT,
                    media_data TEXT,
                    entities TEXT,
                    reply_to_comment_id INTEGER,  -- Для вложенных комментариев
                    processed BOOLEAN DEFAULT FALSE,
                    target_message_id INTEGER,
                    processed_at TIMESTAMP,
                    raw_message TEXT,
                    FOREIGN KEY (post_id) REFERENCES posts (id)
                )
            """)
            
            # Таблица для состояния копирования
            self.db_connection.execute("""
                CREATE TABLE IF NOT EXISTS copy_state (
                    id INTEGER PRIMARY KEY,
                    channel_id TEXT UNIQUE,
                    last_scanned_message_id INTEGER,
                    scan_completed BOOLEAN DEFAULT FALSE,
                    last_copied_message_id INTEGER,
                    total_messages INTEGER DEFAULT 0,
                    processed_messages INTEGER DEFAULT 0,
                    last_update TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Индексы для производительности
            self.db_connection.execute("CREATE INDEX IF NOT EXISTS idx_posts_date ON posts (date_posted)")
            self.db_connection.execute("CREATE INDEX IF NOT EXISTS idx_posts_processed ON posts (processed)")
            self.db_connection.execute("CREATE INDEX IF NOT EXISTS idx_posts_grouped ON posts (grouped_id)")
            self.db_connection.execute("CREATE INDEX IF NOT EXISTS idx_comments_post ON comments (post_id)")
            self.db_connection.execute("CREATE INDEX IF NOT EXISTS idx_comments_processed ON comments (processed)")
            
            self.db_connection.commit()
            self.logger.info(f"✅ База данных инициализирована: {self.database_path}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            raise
    
    async def _find_discussion_groups(self):
        """Поиск discussion groups для комментариев."""
        try:
            # Проверяем, есть ли linked_chat у исходного канала
            full_channel = await self.client(functions.channels.GetFullChannelRequest(self.source_entity))
            
            if hasattr(full_channel.full_chat, 'linked_chat_id') and full_channel.full_chat.linked_chat_id:
                self.discussion_entity = await self.client.get_entity(full_channel.full_chat.linked_chat_id)
                self.logger.info(f"✅ Найдена discussion group источника: {getattr(self.discussion_entity, 'title', 'N/A')}")
            
            # Аналогично для целевого канала
            target_full_channel = await self.client(functions.channels.GetFullChannelRequest(self.target_entity))
            
            if hasattr(target_full_channel.full_chat, 'linked_chat_id') and target_full_channel.full_chat.linked_chat_id:
                self.target_discussion_entity = await self.client.get_entity(target_full_channel.full_chat.linked_chat_id)
                self.logger.info(f"✅ Найдена discussion group цели: {getattr(self.target_discussion_entity, 'title', 'N/A')}")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка поиска discussion groups: {e}")
    
    async def scan_and_save_all_messages(self) -> bool:
        """
        Полное сканирование канала и сохранение всех сообщений в БД.
        Выполняется только один раз при первом запуске.
        """
        try:
            # Проверяем, было ли уже выполнено сканирование
            cursor = self.db_connection.execute("""
                SELECT scan_completed FROM copy_state WHERE channel_id = ?
            """, (str(self.source_entity.id),))
            
            result = cursor.fetchone()
            if result and result[0]:
                self.logger.info("✅ Сканирование уже выполнено, пропускаем")
                return True
            
            self.logger.info("🔍 Начинаем полное сканирование канала...")
            
            total_messages = 0
            total_comments = 0
            
            # Сканируем все сообщения канала
            async for message in self.client.iter_messages(self.source_entity):
                if self.stop_requested:
                    break
                
                await self._save_post_to_db(message)
                total_messages += 1
                
                # Сканируем комментарии для этого сообщения
                if message.replies and message.replies.comments:
                    comments = await self._get_comments_for_post(message)
                    for comment in comments:
                        await self._save_comment_to_db(comment, message.id)
                        total_comments += 1
                
                # Логируем прогресс
                if total_messages % 100 == 0:
                    self.logger.info(f"📊 Сканировано {total_messages} постов, {total_comments} комментариев")
            
            # Отмечаем сканирование как завершенное
            self.db_connection.execute("""
                INSERT OR REPLACE INTO copy_state 
                (channel_id, scan_completed, total_messages, last_update)
                VALUES (?, TRUE, ?, CURRENT_TIMESTAMP)
            """, (str(self.source_entity.id), total_messages))
            
            self.db_connection.commit()
            
            self.logger.info(f"✅ Сканирование завершено: {total_messages} постов, {total_comments} комментариев")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сканирования: {e}")
            return False
    
    async def _save_post_to_db(self, message: Message):
        """Сохранение поста в базу данных."""
        try:
            # Подготовка данных медиа
            media_type = None
            media_data = {}
            
            if message.media:
                media_type = message.media.__class__.__name__
                if hasattr(message.media, 'document') and message.media.document:
                    media_data = {
                        'document_id': message.media.document.id,
                        'mime_type': getattr(message.media.document, 'mime_type', ''),
                        'size': getattr(message.media.document, 'size', 0),
                        'filename': self._extract_filename(message.media.document)
                    }
                elif hasattr(message.media, 'photo') and message.media.photo:
                    media_data = {
                        'photo_id': message.media.photo.id
                    }
            
            # Подготовка entities
            entities = []
            if message.entities:
                for entity in message.entities:
                    entities.append({
                        'type': entity.__class__.__name__,
                        'offset': entity.offset,
                        'length': entity.length,
                        'url': getattr(entity, 'url', None)
                    })
            
            # Сохранение в БД
            self.db_connection.execute("""
                INSERT OR REPLACE INTO posts
                (id, channel_id, message_text, date_posted, media_type, media_data,
                 entities, grouped_id, has_discussion, discussion_group_id, raw_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                message.id,
                str(self.source_entity.id),
                message.message or '',
                message.date.isoformat() if message.date else None,
                media_type,
                json.dumps(media_data) if media_data else None,
                json.dumps(entities) if entities else None,
                message.grouped_id,
                bool(message.replies and message.replies.comments),
                str(message.replies.channel_id) if message.replies and message.replies.channel_id else None,
                json.dumps({
                    'id': message.id,
                    'message': message.message,
                    'date': message.date.isoformat() if message.date else None,
                    'grouped_id': message.grouped_id,
                    'views': getattr(message, 'views', None),
                    'forwards': getattr(message, 'forwards', None)
                })
            ))
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения поста {message.id}: {e}")
    
    async def _save_comment_to_db(self, comment: Message, post_id: int):
        """Сохранение комментария в базу данных."""
        try:
            # Подготовка данных медиа
            media_type = None
            media_data = {}
            
            if comment.media:
                media_type = comment.media.__class__.__name__
                if hasattr(comment.media, 'document') and comment.media.document:
                    media_data = {
                        'document_id': comment.media.document.id,
                        'mime_type': getattr(comment.media.document, 'mime_type', ''),
                        'size': getattr(comment.media.document, 'size', 0),
                        'filename': self._extract_filename(comment.media.document)
                    }
                elif hasattr(comment.media, 'photo') and comment.media.photo:
                    media_data = {
                        'photo_id': comment.media.photo.id
                    }
            
            # Подготовка entities
            entities = []
            if comment.entities:
                for entity in comment.entities:
                    entities.append({
                        'type': entity.__class__.__name__,
                        'offset': entity.offset,
                        'length': entity.length,
                        'url': getattr(entity, 'url', None)
                    })
            
            # Определение родительского комментария
            reply_to_comment_id = None
            if comment.reply_to and hasattr(comment.reply_to, 'reply_to_msg_id'):
                reply_to_comment_id = comment.reply_to.reply_to_msg_id
            
            # Сохранение в БД
            self.db_connection.execute("""
                INSERT OR REPLACE INTO comments
                (id, post_id, discussion_group_id, comment_text, date_posted,
                 media_type, media_data, entities, reply_to_comment_id, raw_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                comment.id,
                post_id,
                str(self.discussion_entity.id) if self.discussion_entity else None,
                comment.message or '',
                comment.date.isoformat() if comment.date else None,
                media_type,
                json.dumps(media_data) if media_data else None,
                json.dumps(entities) if entities else None,
                reply_to_comment_id,
                json.dumps({
                    'id': comment.id,
                    'message': comment.message,
                    'date': comment.date.isoformat() if comment.date else None,
                    'views': getattr(comment, 'views', None)
                })
            ))
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения комментария {comment.id}: {e}")
    
    async def _get_comments_for_post(self, post: Message) -> List[Message]:
        """Получение всех комментариев для поста."""
        comments = []
        
        try:
            if not self.discussion_entity or not post.replies or not post.replies.comments:
                return comments
            
            # Ищем связанное сообщение в discussion group
            discussion_message_id = await self._find_discussion_message_id(post)
            
            if discussion_message_id:
                # Получаем все комментарии
                async for comment in self.client.iter_messages(
                    self.discussion_entity, 
                    reply_to=discussion_message_id
                ):
                    comments.append(comment)
            
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка получения комментариев для поста {post.id}: {e}")
        
        return comments
    
    async def _find_discussion_message_id(self, post: Message) -> Optional[int]:
        """Поиск ID сообщения в discussion group, соответствующего посту канала."""
        try:
            # Метод 1: Прямой доступ по ID (часто ID совпадают)
            try:
                message = await self.client.get_messages(self.discussion_entity, ids=post.id)
                if message and not message.empty:
                    return post.id
            except:
                pass
            
            # Метод 2: Поиск по forward header
            async for message in self.client.iter_messages(self.discussion_entity, limit=200):
                if (message.forward and 
                    hasattr(message.forward, 'channel_post') and 
                    message.forward.channel_post == post.id):
                    return message.id
            
            # Метод 3: Поиск по содержимому (если есть текст)
            if post.message and len(post.message.strip()) > 20:
                search_text = post.message.strip()[:100]
                async for message in self.client.iter_messages(self.discussion_entity, limit=100):
                    if message.message and search_text in message.message:
                        return message.id
            
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка поиска discussion message для поста {post.id}: {e}")
        
        return None
    
    def _extract_filename(self, document) -> Optional[str]:
        """Извлечение имени файла из документа."""
        try:
            for attr in getattr(document, 'attributes', []):
                if isinstance(attr, DocumentAttributeFilename):
                    return attr.file_name
        except:
            pass
        return None
    
    def _get_file_attributes_from_media(self, media, message_id: int) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """
        НОВОЕ: Извлечение атрибутов файла из медиа (на основе рабочего коммита 907d630).
        
        Args:
            media: Медиа объект из сообщения
            message_id: ID сообщения для генерации имени
            
        Returns:
            Tuple (suggested_filename, original_mime_type, extension)
        """
        suggested_filename = None
        original_mime_type = None
        extension = None
        
        try:
            if hasattr(media, 'document') and media.document:
                doc = media.document
                original_attributes = doc.attributes if hasattr(doc, 'attributes') else []
                original_mime_type = getattr(doc, 'mime_type', None)
                
                # Пытаемся извлечь имя файла из атрибутов
                for attr in original_attributes:
                    if isinstance(attr, DocumentAttributeFilename):
                        suggested_filename = attr.file_name
                        break
                
                # Если имя файла не найдено, генерируем на основе MIME-типа
                if not suggested_filename and original_mime_type:
                    if original_mime_type.startswith('image/'):
                        extension = original_mime_type.split('/')[-1]
                        if extension == 'jpeg':
                            extension = 'jpg'
                        suggested_filename = f"image_{message_id}.{extension}"
                    elif original_mime_type.startswith('video/'):
                        extension = original_mime_type.split('/')[-1]
                        suggested_filename = f"video_{message_id}.{extension}"
                    elif original_mime_type.startswith('audio/'):
                        extension = original_mime_type.split('/')[-1]
                        suggested_filename = f"audio_{message_id}.{extension}"
                    else:
                        suggested_filename = f"document_{message_id}"
                
                if not suggested_filename:
                    suggested_filename = f"document_{message_id}"
                    
            elif isinstance(media, MessageMediaPhoto):
                suggested_filename = f"photo_{message_id}.jpg"
                original_mime_type = "image/jpeg"
                extension = "jpg"
            
            # Fallback для неизвестных типов
            if not suggested_filename:
                suggested_filename = f"media_{message_id}"
                
        except Exception as e:
            self.logger.debug(f"❌ Ошибка извлечения атрибутов файла: {e}")
            suggested_filename = f"media_{message_id}"
        
        return suggested_filename, original_mime_type, extension
    
    async def _download_media_with_attributes(self, media, message_id: int):
        """
        НОВОЕ: Скачивание медиа с сохранением атрибутов файла (на основе коммита 907d630).
        
        Args:
            media: Медиа объект из сообщения
            message_id: ID сообщения для генерации имени
            
        Returns:
            Tuple (media_file, filename) где filename=None для fallback к прямой ссылке
        """
        try:
            # Получаем атрибуты файла
            suggested_filename, original_mime_type, extension = self._get_file_attributes_from_media(
                media, message_id
            )
            
            # Скачиваем файл как bytes
            downloaded_file = await self.client.download_media(media, file=bytes)
            
            if downloaded_file and suggested_filename:
                self.logger.debug(f"Медиа файл скачан как: {suggested_filename}")
                return (downloaded_file, suggested_filename)
            else:
                self.logger.debug("Fallback к оригинальному медиа")
                return (media, None)
                
        except Exception as e:
            self.logger.warning(f"⚠️ Ошибка скачивания медиа: {e}, используем fallback")
            return (media, None)
    
    async def copy_all_messages_chronologically(self) -> bool:
        """
        Копирование всех сообщений в хронологическом порядке.
        ПОРЯДОК: пост → комментарии → пост → комментарии → пост → комментарии...
        """
        try:
            self.logger.info("🚀 Начинаем копирование в хронологическом порядке...")
            self.logger.info("📋 Порядок: пост → комментарии → пост → комментарии...")
            
            # Получаем все необработанные посты в хронологическом порядке
            cursor = self.db_connection.execute("""
                SELECT * FROM posts 
                WHERE processed = FALSE 
                ORDER BY date_posted ASC
            """)
            
            posts_data = cursor.fetchall()
            
            # Группируем альбомы
            albums = {}  # grouped_id -> [posts]
            single_posts = []
            
            for post_row in posts_data:
                grouped_id = post_row[7]  # индекс grouped_id в таблице
                if grouped_id:
                    if grouped_id not in albums:
                        albums[grouped_id] = []
                    albums[grouped_id].append(post_row)
                else:
                    single_posts.append(post_row)
            
            # Создаем единый список для обработки в хронологическом порядке
            all_items = []
            
            # Добавляем одиночные посты
            for post_row in single_posts:
                all_items.append(('single_post', post_row))
            
            # Добавляем альбомы (каждый альбом как один элемент)
            for grouped_id, album_posts in albums.items():
                # Берем дату первого поста альбома для сортировки
                first_post_date = album_posts[0][3]  # date_posted
                all_items.append(('album', album_posts, first_post_date))
            
            # Сортируем все элементы по дате
            all_items.sort(key=lambda x: x[2] if len(x) > 2 else x[1][3])
            
            self.stats.total_posts = len(single_posts)
            self.stats.total_albums = len(albums)
            
            # Подсчитываем общее количество комментариев
            cursor = self.db_connection.execute("SELECT COUNT(*) FROM comments WHERE processed = FALSE")
            self.stats.total_comments = cursor.fetchone()[0]
            
            self.logger.info(f"📊 К копированию: {len(single_posts)} одиночных постов, {len(albums)} альбомов, {self.stats.total_comments} комментариев")
            
            # Обрабатываем элементы в хронологическом порядке
            for item in all_items:
                if self.stop_requested:
                    break
                
                if item[0] == 'single_post':
                    # Копируем одиночный пост
                    post_row = item[1]
                    post_id = post_row[0]
                    
                    self.logger.info(f"📝 Копируем пост {post_id}")
                    success = await self._copy_single_post_from_db(post_row)
                    
                    if success:
                        self.stats.copied_posts += 1
                        
                        # Сразу копируем комментарии к этому посту
                        await self._copy_comments_for_post(post_id)
                    else:
                        self.stats.failed_posts += 1
                
                elif item[0] == 'album':
                    # Копируем альбом
                    album_posts = item[1]
                    grouped_id = album_posts[0][7]
                    
                    self.logger.info(f"📷 Копируем альбом {grouped_id} ({len(album_posts)} сообщений)")
                    success = await self._copy_album_from_db(album_posts)
                    
                    if success:
                        self.stats.copied_albums += 1
                        
                        # Сразу копируем комментарии ко всем постам альбома
                        for post_row in album_posts:
                            post_id = post_row[0]
                            await self._copy_comments_for_post(post_id)
                    else:
                        self.stats.failed_albums += 1
                
                await asyncio.sleep(self.delay_seconds)
            
            # Финальная статистика
            self.logger.info("🎉 Копирование завершено!")
            self.logger.info(f"📊 Статистика:")
            self.logger.info(f"   📝 Посты: {self.stats.copied_posts}/{self.stats.total_posts}")
            self.logger.info(f"   📷 Альбомы: {self.stats.copied_albums}/{self.stats.total_albums}")
            self.logger.info(f"   💬 Комментарии: {self.stats.copied_comments}/{self.stats.total_comments}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования: {e}")
            return False
    
    async def _copy_comments_for_post(self, post_id: int):
        """
        Копирование всех комментариев для конкретного поста.
        В режиме flatten_structure комментарии отправляются как обычные посты в основной канал.
        
        Args:
            post_id: ID поста, для которого нужно скопировать комментарии
        """
        try:
            # В режиме антивложенности не нужна discussion group
            if not self.flatten_structure and not self.target_discussion_entity:
                return  # Нет целевой discussion group и не включен режим антивложенности
            
            # Получаем все необработанные комментарии для этого поста
            cursor = self.db_connection.execute("""
                SELECT c.*, p.target_message_id 
                FROM comments c
                JOIN posts p ON c.post_id = p.id
                WHERE c.post_id = ? AND c.processed = FALSE AND p.processed = TRUE
                ORDER BY c.date_posted ASC
            """, (post_id,))
            
            comments_data = cursor.fetchall()
            
            if not comments_data:
                return  # Нет комментариев для этого поста
            
            if self.flatten_structure:
                self.logger.info(f"   💬 Копируем {len(comments_data)} комментариев к посту {post_id} КАК ОБЫЧНЫЕ ПОСТЫ (режим антивложенности)")
            else:
                self.logger.info(f"   💬 Копируем {len(comments_data)} комментариев к посту {post_id}")
            
            for comment_row in comments_data:
                if self.stop_requested:
                    break
                
                success = await self._copy_single_comment_from_db(comment_row)
                if success:
                    self.stats.copied_comments += 1
                else:
                    self.stats.failed_comments += 1
                
                # Небольшая задержка между комментариями
                await asyncio.sleep(1)
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования комментариев для поста {post_id}: {e}")
    
    async def _copy_all_posts(self):
        """Копирование всех постов в хронологическом порядке."""
        try:
            # Получаем все необработанные посты в хронологическом порядке
            cursor = self.db_connection.execute("""
                SELECT * FROM posts 
                WHERE processed = FALSE 
                ORDER BY date_posted ASC
            """)
            
            posts_data = cursor.fetchall()
            
            # Группируем альбомы
            albums = {}  # grouped_id -> [posts]
            single_posts = []
            
            for post_row in posts_data:
                grouped_id = post_row[7]  # индекс grouped_id в таблице
                if grouped_id:
                    if grouped_id not in albums:
                        albums[grouped_id] = []
                    albums[grouped_id].append(post_row)
                else:
                    single_posts.append(post_row)
            
            self.stats.total_posts = len(single_posts)
            self.stats.total_albums = len(albums)
            
            self.logger.info(f"📊 К копированию: {len(single_posts)} одиночных постов, {len(albums)} альбомов")
            
            # Копируем одиночные посты
            for post_row in single_posts:
                if self.stop_requested:
                    break
                
                success = await self._copy_single_post_from_db(post_row)
                if success:
                    self.stats.copied_posts += 1
                else:
                    self.stats.failed_posts += 1
                
                await asyncio.sleep(self.delay_seconds)
            
            # Копируем альбомы
            for grouped_id, album_posts in albums.items():
                if self.stop_requested:
                    break
                
                success = await self._copy_album_from_db(album_posts)
                if success:
                    self.stats.copied_albums += 1
                else:
                    self.stats.failed_albums += 1
                
                await asyncio.sleep(self.delay_seconds)
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования постов: {e}")
    
    async def _copy_single_post_from_db(self, post_row: tuple) -> bool:
        """Копирование одиночного поста из данных БД."""
        try:
            post_id = post_row[0]
            message_text = post_row[2] or ''
            media_type = post_row[4]
            media_data = json.loads(post_row[5]) if post_row[5] else {}
            entities_data = json.loads(post_row[6]) if post_row[6] else []
            
            if self.dry_run:
                self.logger.info(f"🔧 [DRY RUN] Пост {post_id}: {message_text[:50]}...")
                self._mark_post_processed(post_id, 999999)  # Фиктивный ID для dry run
                return True
            
            # Подготовка параметров отправки
            send_kwargs = {
                'entity': self.target_entity,
                'message': message_text,
                'formatting_entities': self._restore_entities(entities_data),
                'link_preview': False
            }
            
            # ИСПРАВЛЕНО: Обработка медиа с сохранением атрибутов (на основе коммита 907d630)
            if media_type and media_data:
                original_message = await self._get_original_message(post_id)
                if original_message and original_message.media:
                    # Скачиваем медиа с атрибутами
                    media_file, filename = await self._download_media_with_attributes(original_message.media, post_id)
                    
                    if filename:
                        # Используем кортеж (data, filename) для правильного отображения
                        send_kwargs['file'] = (media_file, filename)
                        self.logger.debug(f"Пост медиа будет отправлен как: {filename}")
                    else:
                        # Fallback к прямой ссылке
                        send_kwargs['file'] = media_file
                    
                    send_kwargs['caption'] = message_text
                    del send_kwargs['message']  # Для медиа используем caption
            
            # Отправка
            sent_message = await self.client.send_message(**send_kwargs)
            
            # Отметка как обработанного
            self._mark_post_processed(post_id, sent_message.id)
            
            self.logger.debug(f"✅ Пост {post_id} -> {sent_message.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования поста {post_row[0]}: {e}")
            return False
    
    async def _copy_album_from_db(self, album_posts: List[tuple]) -> bool:
        """Копирование альбома из данных БД."""
        try:
            if not album_posts:
                return False
            
            # Сортируем по ID для правильного порядка
            album_posts.sort(key=lambda x: x[0])  # sort by post_id
            
            grouped_id = album_posts[0][7]  # grouped_id
            
            if self.dry_run:
                self.logger.info(f"🔧 [DRY RUN] Альбом {grouped_id}: {len(album_posts)} сообщений")
                for post_row in album_posts:
                    self._mark_post_processed(post_row[0], 999999)
                return True
            
            # Собираем медиа файлы и текст
            media_files = []
            caption = ""
            entities = []
            
            for post_row in album_posts:
                post_id = post_row[0]
                message_text = post_row[2] or ''
                entities_data = json.loads(post_row[6]) if post_row[6] else []
                
                # Берем текст из первого непустого сообщения
                if not caption and message_text.strip():
                    caption = message_text
                    entities = self._restore_entities(entities_data)
                
                # ИСПРАВЛЕНО: Получаем оригинальное сообщение для медиа с атрибутами (коммит 907d630)
                original_message = await self._get_original_message(post_id)
                if original_message and original_message.media:
                    # Скачиваем медиа с атрибутами
                    media_file, filename = await self._download_media_with_attributes(original_message.media, post_id)
                    
                    if filename:
                        # Используем кортеж (data, filename) для правильного отображения
                        media_files.append((media_file, filename))
                        self.logger.debug(f"Альбом медиа будет отправлен как: {filename}")
                    else:
                        # Fallback к прямой ссылке
                        media_files.append(media_file)
            
            if not media_files:
                self.logger.warning(f"⚠️ Альбом {grouped_id} не содержит медиа файлов")
                # Отправляем как текст, если есть
                if caption.strip():
                    sent_message = await self.client.send_message(
                        self.target_entity, 
                        caption, 
                        formatting_entities=entities
                    )
                    for post_row in album_posts:
                        self._mark_post_processed(post_row[0], sent_message.id)
                    return True
                return False
            
            # Отправляем альбом
            sent_messages = await self.client.send_file(
                self.target_entity,
                media_files,
                caption=caption,
                formatting_entities=entities
            )
            
            # Отмечаем все сообщения альбома как обработанные
            if isinstance(sent_messages, list):
                for i, post_row in enumerate(album_posts):
                    target_id = sent_messages[i].id if i < len(sent_messages) else sent_messages[0].id
                    self._mark_post_processed(post_row[0], target_id)
            else:
                # Одно сообщение для всего альбома
                for post_row in album_posts:
                    self._mark_post_processed(post_row[0], sent_messages.id)
            
            self.logger.info(f"✅ Альбом {grouped_id} ({len(album_posts)} сообщений) скопирован")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования альбома: {e}")
            return False
    
    async def _copy_all_comments(self):
        """Копирование всех комментариев в хронологическом порядке."""
        try:
            if not self.target_discussion_entity:
                self.logger.warning("⚠️ Целевая discussion group не найдена, комментарии пропускаются")
                return
            
            # Получаем все необработанные комментарии в хронологическом порядке
            cursor = self.db_connection.execute("""
                SELECT c.*, p.target_message_id 
                FROM comments c
                JOIN posts p ON c.post_id = p.id
                WHERE c.processed = FALSE AND p.processed = TRUE
                ORDER BY c.date_posted ASC
            """)
            
            comments_data = cursor.fetchall()
            self.stats.total_comments = len(comments_data)
            
            self.logger.info(f"📊 К копированию: {len(comments_data)} комментариев")
            
            for comment_row in comments_data:
                if self.stop_requested:
                    break
                
                success = await self._copy_single_comment_from_db(comment_row)
                if success:
                    self.stats.copied_comments += 1
                else:
                    self.stats.failed_comments += 1
                
                await asyncio.sleep(self.delay_seconds)
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования комментариев: {e}")
    
    async def _copy_single_comment_from_db(self, comment_row: tuple) -> bool:
        """
        Копирование одиночного комментария из данных БД.
        В режиме flatten_structure комментарии отправляются как обычные посты в основной канал.
        """
        try:
            comment_id = comment_row[0]
            comment_text = comment_row[3] or ''
            media_type = comment_row[5]
            media_data = json.loads(comment_row[6]) if comment_row[6] else {}
            entities_data = json.loads(comment_row[7]) if comment_row[7] else []
            target_post_id = comment_row[-1]  # target_message_id из JOIN
            
            if self.dry_run:
                mode_text = "КАК ОБЫЧНЫЙ ПОСТ" if self.flatten_structure else "как комментарий"
                self.logger.info(f"🔧 [DRY RUN] Комментарий {comment_id} {mode_text}: {comment_text[:50]}...")
                self._mark_comment_processed(comment_id, 999999)
                return True
            
            # Выбираем целевую сущность в зависимости от режима
            if self.flatten_structure:
                # В режиме антивложенности отправляем в основной канал как обычный пост
                target_entity = self.target_entity
                send_kwargs = {
                    'entity': target_entity,
                    'message': comment_text,
                    'formatting_entities': self._restore_entities(entities_data),
                    # НЕ добавляем reply_to - это обычный пост
                }
                self.logger.debug(f"📝 Отправляем комментарий {comment_id} как обычный пост в основной канал")
            else:
                # Обычный режим - отправляем в discussion group как комментарий
                target_entity = self.target_discussion_entity
                send_kwargs = {
                    'entity': target_entity,
                    'message': comment_text,
                    'formatting_entities': self._restore_entities(entities_data),
                    'reply_to': target_post_id  # Отвечаем на соответствующий пост
                }
                self.logger.debug(f"💬 Отправляем комментарий {comment_id} как ответ на пост {target_post_id}")
            
            # ИСПРАВЛЕНО: Обработка медиа с сохранением атрибутов (на основе коммита 907d630)
            if media_type and media_data:
                original_comment = await self._get_original_comment(comment_id)
                if original_comment and original_comment.media:
                    # Скачиваем медиа с атрибутами (для комментариев всегда используем скачивание)
                    media_file, filename = await self._download_media_with_attributes(original_comment.media, comment_id)
                    
                    if filename:
                        # Используем кортеж (data, filename) для правильного отображения
                        send_kwargs['file'] = (media_file, filename)
                        self.logger.debug(f"Комментарий медиа будет отправлен как: {filename}")
                    else:
                        # Fallback к прямой ссылке
                        send_kwargs['file'] = media_file
                    
                    send_kwargs['caption'] = comment_text
                    del send_kwargs['message']  # Для медиа используем caption
            
            # Отправка
            sent_message = await self.client.send_message(**send_kwargs)
            
            # Отметка как обработанного
            self._mark_comment_processed(comment_id, sent_message.id)
            
            if self.flatten_structure:
                self.logger.debug(f"✅ Комментарий {comment_id} -> обычный пост {sent_message.id}")
            else:
                self.logger.debug(f"✅ Комментарий {comment_id} -> ответ {sent_message.id}")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования комментария {comment_row[0]}: {e}")
            return False
    
    async def _get_original_message(self, message_id: int) -> Optional[Message]:
        """Получение оригинального сообщения по ID."""
        try:
            message = await self.client.get_messages(self.source_entity, ids=message_id)
            return message if message and not message.empty else None
        except:
            return None
    
    async def _get_original_comment(self, comment_id: int) -> Optional[Message]:
        """Получение оригинального комментария по ID."""
        try:
            if not self.discussion_entity:
                return None
            comment = await self.client.get_messages(self.discussion_entity, ids=comment_id)
            return comment if comment and not comment.empty else None
        except:
            return None
    
    def _restore_entities(self, entities_data: List[Dict]) -> List:
        """Восстановление entities из данных БД."""
        # Упрощенная реализация - можно расширить для полного восстановления
        return []
    
    def _mark_post_processed(self, post_id: int, target_message_id: int):
        """Отметка поста как обработанного."""
        try:
            self.db_connection.execute("""
                UPDATE posts 
                SET processed = TRUE, target_message_id = ?, processed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (target_message_id, post_id))
            self.db_connection.commit()
        except Exception as e:
            self.logger.error(f"❌ Ошибка отметки поста {post_id}: {e}")
    
    def _mark_comment_processed(self, comment_id: int, target_message_id: int):
        """Отметка комментария как обработанного."""
        try:
            self.db_connection.execute("""
                UPDATE comments 
                SET processed = TRUE, target_message_id = ?, processed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (target_message_id, comment_id))
            self.db_connection.commit()
        except Exception as e:
            self.logger.error(f"❌ Ошибка отметки комментария {comment_id}: {e}")
    
    def close(self):
        """Закрытие ресурсов."""
        if self.db_connection:
            self.db_connection.close()
            self.logger.info("🔒 База данных закрыта")
    
    def stop(self):
        """Остановка копирования."""
        self.stop_requested = True
        self.logger.info("⏹️ Запрошена остановка копирования")
    
    async def _validate_entity_access(self, entity, entity_name: str) -> bool:
        """НОВОЕ: Проверка доступа к найденной entity."""
        try:
            # Проверяем, что entity не None и имеет необходимые атрибуты
            if not entity or not hasattr(entity, 'id'):
                return False
            
            # Для каналов проверяем дополнительные права
            if hasattr(entity, 'megagroup') or hasattr(entity, 'broadcast'):
                # Пробуем получить несколько сообщений для проверки доступа
                try:
                    messages = await self.client.get_messages(entity, limit=1)
                    self.logger.debug(f"✅ Доступ к {entity_name} подтвержден")
                    return True
                except Exception as e:
                    self.logger.warning(f"⚠️ Ограниченный доступ к {entity_name}: {e}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.debug(f"❌ Ошибка валидации доступа к {entity_name}: {e}")
            return False
    
    async def _log_diagnostic_info(self, entity_id: Union[str, int], entity_name: str):
        """НОВОЕ: Детальная диагностика для troubleshooting."""
        try:
            self.logger.error("🔍 ДИАГНОСТИЧЕСКАЯ ИНФОРМАЦИЯ:")
            
            # Информация о клиенте
            if await self.client.is_user_authorized():
                me = await self.client.get_me()
                self.logger.error(f"   👤 Пользователь: {me.first_name} (ID: {me.id})")
            else:
                self.logger.error("   ❌ Клиент не авторизован")
                return
            
            # Информация о entity_id
            self.logger.error(f"   🎯 Искомый ID: {entity_id} (тип: {type(entity_id).__name__})")
            
            # Список доступных диалогов
            try:
                dialogs = await self.client.get_dialogs(limit=10)
                self.logger.error(f"   📁 Доступных диалогов: {len(dialogs)}")
                for i, dialog in enumerate(dialogs[:5]):
                    entity_info = f"ID: {dialog.entity.id}, Название: {getattr(dialog.entity, 'title', getattr(dialog.entity, 'first_name', 'N/A'))}"
                    self.logger.error(f"     {i+1}. {entity_info}")
                if len(dialogs) > 5:
                    self.logger.error(f"     ... и еще {len(dialogs) - 5} диалогов")
            except Exception as e:
                self.logger.error(f"   ❌ Ошибка получения диалогов: {e}")
            
            # Рекомендации по устранению
            self.logger.error("💡 РЕКОМЕНДАЦИИ:")
            self.logger.error("   1. Убедитесь, что ID канала корректен")
            self.logger.error("   2. Проверьте, что аккаунт является участником канала")
            self.logger.error("   3. Убедитесь, что канал не был удален или заблокирован")
            self.logger.error("   4. Попробуйте использовать @username вместо числового ID")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка диагностики: {e}")


async def main():
    """Пример использования."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    # Инициализация (замените на ваши данные)
    client = TelegramClient('session', 'api_id', 'api_hash')
    
    copier = TelegramCopierV3(
        client=client,
        source_channel_id='@source_channel',
        target_channel_id='@target_channel',
        dry_run=True  # Включите для тестирования
    )
    
    try:
        await client.start()
        await copier.initialize()
        
        # Этап 1: Сканирование (выполняется только один раз)
        await copier.scan_and_save_all_messages()
        
        # Этап 2: Копирование в хронологическом порядке
        await copier.copy_all_messages_chronologically()
        
    finally:
        copier.close()
        await client.disconnect()


if __name__ == '__main__':
    asyncio.run(main())