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
from telethon.errors import FloodWaitError, PeerFloodError, MediaInvalidError


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
                 delay_seconds: int = 3):
        """
        Инициализация копировщика.
        
        Args:
            client: Авторизованный Telegram клиент
            source_channel_id: ID или username исходного канала
            target_channel_id: ID или username целевого канала  
            database_path: Путь к файлу базы данных
            dry_run: Режим симуляции без реальной отправки
            delay_seconds: Задержка между отправками сообщений
        """
        self.client = client
        self.source_channel_id = source_channel_id
        self.target_channel_id = target_channel_id
        self.database_path = database_path
        self.dry_run = dry_run
        self.delay_seconds = delay_seconds
        
        self.logger = logging.getLogger('telegram_copier_v3')
        
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
        """Инициализация копировщика."""
        try:
            # Инициализация базы данных
            self._init_database()
            
            # Получение entities каналов
            self.source_entity = await self.client.get_entity(self.source_channel_id)
            self.target_entity = await self.client.get_entity(self.target_channel_id)
            
            self.logger.info(f"✅ Источник: {getattr(self.source_entity, 'title', 'N/A')}")
            self.logger.info(f"✅ Цель: {getattr(self.target_entity, 'title', 'N/A')}")
            
            # Попытка найти discussion groups
            await self._find_discussion_groups()
            
            self.logger.info("✅ Копировщик v3.0 инициализирован")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации: {e}")
            raise
    
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
        
        Args:
            post_id: ID поста, для которого нужно скопировать комментарии
        """
        try:
            if not self.target_discussion_entity:
                return  # Нет целевой discussion group
            
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
            
            # Обработка медиа
            if media_type and media_data:
                original_message = await self._get_original_message(post_id)
                if original_message and original_message.media:
                    send_kwargs['file'] = original_message.media
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
                
                # Получаем оригинальное сообщение для медиа
                original_message = await self._get_original_message(post_id)
                if original_message and original_message.media:
                    media_files.append(original_message.media)
            
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
        """Копирование одиночного комментария из данных БД."""
        try:
            comment_id = comment_row[0]
            comment_text = comment_row[3] or ''
            media_type = comment_row[5]
            media_data = json.loads(comment_row[6]) if comment_row[6] else {}
            entities_data = json.loads(comment_row[7]) if comment_row[7] else []
            target_post_id = comment_row[-1]  # target_message_id из JOIN
            
            if self.dry_run:
                self.logger.info(f"🔧 [DRY RUN] Комментарий {comment_id}: {comment_text[:50]}...")
                self._mark_comment_processed(comment_id, 999999)
                return True
            
            # Подготовка параметров отправки
            send_kwargs = {
                'entity': self.target_discussion_entity,
                'message': comment_text,
                'formatting_entities': self._restore_entities(entities_data),
                'reply_to': target_post_id  # Отвечаем на соответствующий пост
            }
            
            # Обработка медиа
            if media_type and media_data:
                original_comment = await self._get_original_comment(comment_id)
                if original_comment and original_comment.media:
                    send_kwargs['file'] = original_comment.media
                    send_kwargs['caption'] = comment_text
                    del send_kwargs['message']  # Для медиа используем caption
            
            # Отправка
            sent_message = await self.client.send_message(**send_kwargs)
            
            # Отметка как обработанного
            self._mark_comment_processed(comment_id, sent_message.id)
            
            self.logger.debug(f"✅ Комментарий {comment_id} -> {sent_message.id}")
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