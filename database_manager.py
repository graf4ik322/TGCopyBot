"""
Модуль для управления базой данных с постоянным хранением состояния копирования.
Обеспечивает хронологическое сохранение и восстановление данных.
"""

import sqlite3
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
from pathlib import Path
from telethon.tl.types import Message


class DatabaseManager:
    """Менеджер базы данных для постоянного хранения состояния копирования."""
    
    def __init__(self, db_path: str = "telegram_copier.db"):
        """
        Инициализация менеджера базы данных.
        
        Args:
            db_path: Путь к файлу базы данных
        """
        self.db_path = Path(db_path)
        self.logger = logging.getLogger('telegram_copier.database')
        self.connection = None
        self._init_database()
    
    def _init_database(self):
        """Инициализация базы данных с правильной схемой."""
        try:
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.connection.execute("PRAGMA foreign_keys = ON")
            
            # Таблица сообщений из источника
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS source_messages (
                    id INTEGER PRIMARY KEY,
                    channel_id TEXT NOT NULL,
                    message_text TEXT,
                    date_posted TIMESTAMP,
                    media_type TEXT,
                    media_info TEXT, -- JSON с информацией о медиа
                    entities TEXT,   -- JSON с форматированием
                    grouped_id INTEGER, -- ID альбома
                    reply_to_msg_id INTEGER,
                    views INTEGER,
                    forwards INTEGER,
                    raw_data TEXT,   -- Полные данные сообщения в JSON
                    processed BOOLEAN DEFAULT FALSE,
                    processed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Таблица комментариев
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    id INTEGER PRIMARY KEY,
                    post_id INTEGER NOT NULL,
                    discussion_group_id TEXT,
                    comment_text TEXT,
                    date_posted TIMESTAMP,
                    media_type TEXT,
                    media_info TEXT,
                    parent_comment_id INTEGER, -- Для вложенных комментариев
                    raw_data TEXT,
                    processed BOOLEAN DEFAULT FALSE,
                    processed_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (post_id) REFERENCES source_messages (id)
                )
            """)
            
            # Таблица скопированных сообщений (мэппинг)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS copied_messages (
                    source_id INTEGER,
                    target_id INTEGER,
                    source_channel TEXT,
                    target_channel TEXT,
                    copied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    message_type TEXT, -- 'post', 'comment', 'album'
                    album_group_id TEXT, -- Для группировки альбомов
                    PRIMARY KEY (source_id, target_id),
                    FOREIGN KEY (source_id) REFERENCES source_messages (id)
                )
            """)
            
            # Таблица состояния копирования
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS copy_state (
                    channel_id TEXT PRIMARY KEY,
                    last_processed_message_id INTEGER,
                    last_scan_completed_at TIMESTAMP,
                    total_messages INTEGER,
                    processed_messages INTEGER,
                    failed_messages INTEGER,
                    metadata TEXT -- JSON с дополнительной информацией
                )
            """)
            
            # Индексы для производительности
            self.connection.execute("CREATE INDEX IF NOT EXISTS idx_source_messages_date ON source_messages (date_posted)")
            self.connection.execute("CREATE INDEX IF NOT EXISTS idx_source_messages_processed ON source_messages (processed)")
            self.connection.execute("CREATE INDEX IF NOT EXISTS idx_comments_post_id ON comments (post_id)")
            self.connection.execute("CREATE INDEX IF NOT EXISTS idx_copied_messages_source ON copied_messages (source_id)")
            
            self.connection.commit()
            self.logger.info(f"✅ База данных инициализирована: {self.db_path}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            raise
    
    def save_source_message(self, message: Message, channel_id: str) -> bool:
        """
        Сохранение сообщения из источника в базу данных.
        
        Args:
            message: Сообщение Telethon
            channel_id: ID канала источника
            
        Returns:
            True если сохранение успешно
        """
        try:
            # Подготавливаем данные
            media_type = None
            media_info = {}
            
            if hasattr(message, 'media') and message.media:
                media_type = message.media.__class__.__name__
                if hasattr(message.media, 'document') and message.media.document:
                    media_info = {
                        'type': 'document',
                        'mime_type': getattr(message.media.document, 'mime_type', ''),
                        'size': getattr(message.media.document, 'size', 0),
                        'id': getattr(message.media.document, 'id', 0)
                    }
                elif hasattr(message.media, 'photo') and message.media.photo:
                    media_info = {
                        'type': 'photo',
                        'id': getattr(message.media.photo, 'id', 0)
                    }
            
            # Сериализуем entities
            entities_json = []
            if hasattr(message, 'entities') and message.entities:
                for entity in message.entities:
                    try:
                        entity_dict = {
                            'type': entity.__class__.__name__,
                            'offset': entity.offset,
                            'length': entity.length,
                        }
                        if hasattr(entity, 'url'):
                            entity_dict['url'] = entity.url
                        if hasattr(entity, 'user_id'):
                            entity_dict['user_id'] = entity.user_id
                        entities_json.append(entity_dict)
                    except Exception:
                        continue
            
            # Сохраняем в базу
            self.connection.execute("""
                INSERT OR REPLACE INTO source_messages 
                (id, channel_id, message_text, date_posted, media_type, media_info, 
                 entities, grouped_id, reply_to_msg_id, views, forwards, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                message.id,
                channel_id,
                getattr(message, 'message', '') or '',
                message.date.isoformat() if message.date else None,
                media_type,
                json.dumps(media_info, ensure_ascii=False) if media_info else None,
                json.dumps(entities_json, ensure_ascii=False) if entities_json else None,
                getattr(message, 'grouped_id', None),
                getattr(message, 'reply_to_msg_id', None),
                getattr(message, 'views', None),
                getattr(message, 'forwards', None),
                self._serialize_message_safely(message)
            ))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения сообщения {message.id}: {e}")
            return False
    
    def save_comment(self, comment: Message, post_id: int, discussion_group_id: str,
                     parent_comment_id: Optional[int] = None) -> bool:
        """
        Сохранение комментария в базу данных.
        
        Args:
            comment: Комментарий Telethon
            post_id: ID поста, к которому относится комментарий
            discussion_group_id: ID группы обсуждений
            parent_comment_id: ID родительского комментария (для вложенных)
            
        Returns:
            True если сохранение успешно
        """
        try:
            media_type = None
            media_info = {}
            
            if hasattr(comment, 'media') and comment.media:
                media_type = comment.media.__class__.__name__
                if hasattr(comment.media, 'document'):
                    media_info = {'type': 'document'}
                elif hasattr(comment.media, 'photo'):
                    media_info = {'type': 'photo'}
            
            self.connection.execute("""
                INSERT OR REPLACE INTO comments 
                (id, post_id, discussion_group_id, comment_text, date_posted, 
                 media_type, media_info, parent_comment_id, raw_data)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                comment.id,
                post_id,
                discussion_group_id,
                getattr(comment, 'message', '') or '',
                comment.date.isoformat() if comment.date else None,
                media_type,
                json.dumps(media_info, ensure_ascii=False) if media_info else None,
                parent_comment_id,
                self._serialize_message_safely(comment)
            ))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения комментария {comment.id}: {e}")
            return False
    
    def mark_message_copied(self, source_id: int, target_id: int, 
                           source_channel: str, target_channel: str,
                           message_type: str = 'post', album_group_id: Optional[str] = None) -> bool:
        """
        Отметка сообщения как скопированного.
        
        Args:
            source_id: ID исходного сообщения
            target_id: ID целевого сообщения  
            source_channel: Канал источник
            target_channel: Целевой канал
            message_type: Тип сообщения ('post', 'comment', 'album')
            album_group_id: ID группы альбома
            
        Returns:
            True если успешно
        """
        try:
            self.connection.execute("""
                INSERT OR REPLACE INTO copied_messages 
                (source_id, target_id, source_channel, target_channel, 
                 message_type, album_group_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (source_id, target_id, source_channel, target_channel, 
                  message_type, album_group_id))
            
            # Обновляем статус в source_messages
            self.connection.execute("""
                UPDATE source_messages 
                SET processed = TRUE, processed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (source_id,))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка отметки копирования {source_id}->{target_id}: {e}")
            return False
    
    def get_unprocessed_messages(self, channel_id: str, limit: Optional[int] = None,
                                chronological_order: bool = True) -> List[Dict[str, Any]]:
        """
        Получение необработанных сообщений в хронологическом порядке.
        
        Args:
            channel_id: ID канала
            limit: Лимит сообщений (None = все)
            chronological_order: Порядок по времени (True = от старых к новым)
            
        Returns:
            Список словарей с данными сообщений
        """
        try:
            order_clause = "ORDER BY date_posted ASC" if chronological_order else "ORDER BY date_posted DESC"
            limit_clause = f"LIMIT {limit}" if limit else ""
            
            cursor = self.connection.execute(f"""
                SELECT id, channel_id, message_text, date_posted, media_type, 
                       media_info, entities, grouped_id, reply_to_msg_id, 
                       views, forwards, raw_data
                FROM source_messages 
                WHERE channel_id = ? AND processed = FALSE
                {order_clause}
                {limit_clause}
            """, (channel_id,))
            
            messages = []
            for row in cursor.fetchall():
                message_dict = {
                    'id': row[0],
                    'channel_id': row[1],
                    'message_text': row[2],
                    'date_posted': row[3],
                    'media_type': row[4],
                    'media_info': json.loads(row[5]) if row[5] else {},
                    'entities': json.loads(row[6]) if row[6] else [],
                    'grouped_id': row[7],
                    'reply_to_msg_id': row[8],
                    'views': row[9],
                    'forwards': row[10],
                    'raw_data': row[11]
                }
                messages.append(message_dict)
            
            return messages
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения необработанных сообщений: {e}")
            return []
    
    def get_comments_for_post(self, post_id: int) -> List[Dict[str, Any]]:
        """
        Получение всех комментариев для поста.
        
        Args:
            post_id: ID поста
            
        Returns:
            Список словарей с данными комментариев
        """
        try:
            cursor = self.connection.execute("""
                SELECT id, post_id, discussion_group_id, comment_text, 
                       date_posted, media_type, media_info, parent_comment_id, raw_data
                FROM comments 
                WHERE post_id = ? AND processed = FALSE
                ORDER BY date_posted ASC
            """, (post_id,))
            
            comments = []
            for row in cursor.fetchall():
                comment_dict = {
                    'id': row[0],
                    'post_id': row[1],
                    'discussion_group_id': row[2],
                    'comment_text': row[3],
                    'date_posted': row[4],
                    'media_type': row[5],
                    'media_info': json.loads(row[6]) if row[6] else {},
                    'parent_comment_id': row[7],
                    'raw_data': row[8]
                }
                comments.append(comment_dict)
            
            return comments
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения комментариев для поста {post_id}: {e}")
            return []
    
    def get_copy_state(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """
        Получение состояния копирования для канала.
        
        Args:
            channel_id: ID канала
            
        Returns:
            Словарь с состоянием или None
        """
        try:
            cursor = self.connection.execute("""
                SELECT last_processed_message_id, last_scan_completed_at,
                       total_messages, processed_messages, failed_messages, metadata
                FROM copy_state 
                WHERE channel_id = ?
            """, (channel_id,))
            
            row = cursor.fetchone()
            if row:
                return {
                    'last_processed_message_id': row[0],
                    'last_scan_completed_at': row[1],
                    'total_messages': row[2],
                    'processed_messages': row[3],
                    'failed_messages': row[4],
                    'metadata': json.loads(row[5]) if row[5] else {}
                }
            return None
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения состояния копирования: {e}")
            return None
    
    def update_copy_state(self, channel_id: str, **kwargs) -> bool:
        """
        Обновление состояния копирования.
        
        Args:
            channel_id: ID канала
            **kwargs: Поля для обновления
            
        Returns:
            True если успешно
        """
        try:
            # Получаем текущее состояние
            current_state = self.get_copy_state(channel_id) or {}
            
            # Обновляем поля
            for key, value in kwargs.items():
                if key in ['last_processed_message_id', 'total_messages', 
                          'processed_messages', 'failed_messages']:
                    current_state[key] = value
                elif key == 'metadata' and isinstance(value, dict):
                    current_state.setdefault('metadata', {}).update(value)
            
            # Сохраняем
            self.connection.execute("""
                INSERT OR REPLACE INTO copy_state 
                (channel_id, last_processed_message_id, last_scan_completed_at,
                 total_messages, processed_messages, failed_messages, metadata)
                VALUES (?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
            """, (
                channel_id,
                current_state.get('last_processed_message_id'),
                current_state.get('total_messages', 0),
                current_state.get('processed_messages', 0),
                current_state.get('failed_messages', 0),
                json.dumps(current_state.get('metadata', {}), ensure_ascii=False)
            ))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка обновления состояния копирования: {e}")
            return False
    
    def is_full_scan_completed(self, channel_id: str) -> bool:
        """
        Проверка, завершено ли полное сканирование канала.
        
        Args:
            channel_id: ID канала
            
        Returns:
            True если полное сканирование завершено
        """
        state = self.get_copy_state(channel_id)
        return state and state.get('last_scan_completed_at') is not None
    
    def get_statistics(self, channel_id: str) -> Dict[str, Any]:
        """
        Получение статистики для канала.
        
        Args:
            channel_id: ID канала
            
        Returns:
            Словарь со статистикой
        """
        try:
            cursor = self.connection.execute("""
                SELECT 
                    COUNT(*) as total_messages,
                    COUNT(CASE WHEN processed = TRUE THEN 1 END) as processed_messages,
                    COUNT(CASE WHEN media_type IS NOT NULL THEN 1 END) as messages_with_media,
                    COUNT(DISTINCT grouped_id) - 1 as albums_count
                FROM source_messages 
                WHERE channel_id = ?
            """, (channel_id,))
            
            stats = cursor.fetchone()
            
            cursor = self.connection.execute("""
                SELECT COUNT(*) as total_comments
                FROM comments c
                JOIN source_messages m ON c.post_id = m.id
                WHERE m.channel_id = ?
            """, (channel_id,))
            
            comments_count = cursor.fetchone()[0]
            
            return {
                'total_messages': stats[0],
                'processed_messages': stats[1],
                'messages_with_media': stats[2],
                'albums_count': max(0, stats[3]),  # -1 потому что NULL тоже считается
                'total_comments': comments_count,
                'progress_percentage': (stats[1] / stats[0] * 100) if stats[0] > 0 else 0
            }
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
    
    def _serialize_message_safely(self, message: Message) -> str:
        """
        Безопасная сериализация сообщения в JSON.
        
        Args:
            message: Сообщение Telethon
            
        Returns:
            JSON строка с данными сообщения
        """
        try:
            # Создаем упрощенную копию без ссылок на клиент
            safe_data = {
                'id': message.id,
                'date': message.date.isoformat() if message.date else None,
                'message': getattr(message, 'message', '') or '',
                'from_id': str(message.from_id) if message.from_id else None,
                'to_id': str(message.peer_id) if message.peer_id else None,
                'reply_to_msg_id': getattr(message, 'reply_to_msg_id', None),
                'grouped_id': getattr(message, 'grouped_id', None),
                'views': getattr(message, 'views', None),
                'forwards': getattr(message, 'forwards', None),
                'class_name': message.__class__.__name__
            }
            
            return json.dumps(safe_data, ensure_ascii=False, default=str)
            
        except Exception as e:
            self.logger.warning(f"Ошибка сериализации сообщения {getattr(message, 'id', 'unknown')}: {e}")
            return json.dumps({
                'id': getattr(message, 'id', 0),
                'error': f"Serialization failed: {str(e)}"
            })
    
    def cleanup_old_data(self, days_old: int = 30):
        """
        Очистка старых данных.
        
        Args:
            days_old: Возраст данных в днях для удаления
        """
        try:
            self.connection.execute("""
                DELETE FROM copied_messages 
                WHERE copied_at < datetime('now', '-{} days')
            """.format(days_old))
            
            self.connection.commit()
            self.logger.info(f"🧹 Очищены данные старше {days_old} дней")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка очистки старых данных: {e}")
    
    def close(self):
        """Закрытие соединения с базой данных."""
        if self.connection:
            self.connection.close()
            self.logger.info("🔒 Соединение с базой данных закрыто")