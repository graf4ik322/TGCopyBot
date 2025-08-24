"""
Оптимизированная система кэширования для TelegramCopier.
Использует SQLite + LRU кэш для минимизации использования памяти.
"""

import sqlite3
import json
import pickle
import logging
from typing import List, Optional, Dict, Any
from functools import lru_cache
from dataclasses import dataclass
from collections import OrderedDict
import threading
import time
import psutil
import os

from telethon.tl.types import Message


def message_to_dict(message: Message) -> Dict[str, Any]:
    """
    Конвертирует Telethon Message в сериализуемый словарь.
    Убирает ссылки на клиент и другие несериализуемые объекты.
    """
    try:
        # Основные поля сообщения
        msg_dict = {
            'id': message.id,
            'date': message.date.isoformat() if message.date else None,
            'message': message.message or '',
            'from_id': getattr(message.from_id, 'user_id', None) if message.from_id else None,
            'to_id': getattr(message.peer_id, 'channel_id', None) if message.peer_id else None,
            'reply_to_msg_id': message.reply_to_msg_id,
            'entities': [],
            'media_type': None,
            'grouped_id': getattr(message, 'grouped_id', None),
            'views': getattr(message, 'views', None),
            'forwards': getattr(message, 'forwards', None),
        }
        
        # Сериализуем entities безопасно
        if hasattr(message, 'entities') and message.entities:
            for entity in message.entities:
                try:
                    entity_dict = {
                        'type': entity.__class__.__name__,
                        'offset': entity.offset,
                        'length': entity.length,
                    }
                    # Добавляем специфичные поля для разных типов entities
                    if hasattr(entity, 'url'):
                        entity_dict['url'] = entity.url
                    if hasattr(entity, 'user_id'):
                        entity_dict['user_id'] = entity.user_id
                    msg_dict['entities'].append(entity_dict)
                except Exception:
                    continue  # Пропускаем проблемные entities
        
        # Определяем тип медиа без сериализации самого объекта
        if hasattr(message, 'media') and message.media:
            msg_dict['media_type'] = message.media.__class__.__name__
        
        return msg_dict
        
    except Exception as e:
        # Fallback - минимальная информация
        return {
            'id': getattr(message, 'id', 0),
            'date': None,
            'message': getattr(message, 'message', '') or '',
            'from_id': None,
            'to_id': None,
            'reply_to_msg_id': getattr(message, 'reply_to_msg_id', None),
            'entities': [],
            'media_type': None,
            'grouped_id': None,
            'views': None,
            'forwards': None,
            'error': str(e)
        }


class MessageProxy:
    """
    Легкий объект-прокси для замены Telethon Message.
    Содержит только необходимые данные без ссылок на клиент.
    """
    def __init__(self, msg_dict: Dict[str, Any]):
        self.id = msg_dict.get('id', 0)
        self.message = msg_dict.get('message', '')
        self.from_id = msg_dict.get('from_id')
        self.to_id = msg_dict.get('to_id')
        self.reply_to_msg_id = msg_dict.get('reply_to_msg_id')
        self.grouped_id = msg_dict.get('grouped_id')
        self.views = msg_dict.get('views')
        self.forwards = msg_dict.get('forwards')
        self.media_type = msg_dict.get('media_type')
        
        # Парсим дату
        try:
            from datetime import datetime
            if msg_dict.get('date'):
                self.date = datetime.fromisoformat(msg_dict['date'])
            else:
                # Fallback - используем ID как timestamp
                self.date = datetime.fromtimestamp(self.id) if self.id > 1000000000 else None
        except:
            self.date = None
        
        # Флаги для совместимости с основным кодом
        self._is_from_discussion_group = False
        self._parent_message_id = None
        
        # Сохраняем оригинальные данные
        self._original_data = msg_dict
    
    def __str__(self):
        return f"MessageProxy(id={self.id}, message='{self.message[:50]}...')"
    
    def __repr__(self):
        return self.__str__()


def dict_to_message_proxy(msg_dict: Dict[str, Any]) -> MessageProxy:
    """
    Конвертирует словарь в MessageProxy объект.
    """
    return MessageProxy(msg_dict)


@dataclass
class MemoryStats:
    """Статистика использования памяти."""
    total_memory_mb: float
    cache_size: int
    database_size_mb: float
    efficiency_ratio: float


class LRUCache:
    """Простая реализация LRU кэша с ограничением по размеру."""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.cache = OrderedDict()
        self.lock = threading.RLock()
    
    def get(self, key: int) -> Optional[List[Dict[str, Any]]]:
        """Получить значение из кэша (возвращает словари для дальнейшей конвертации)."""
        with self.lock:
            if key in self.cache:
                # Перемещаем в конец (most recently used)
                value = self.cache.pop(key)
                self.cache[key] = value
                return value
            return None
    
    def put(self, key: int, value: List[Dict[str, Any]]) -> None:
        """Добавить значение в кэш."""
        with self.lock:
            if key in self.cache:
                # Обновляем существующее значение
                self.cache.pop(key)
            elif len(self.cache) >= self.max_size:
                # Удаляем oldest item
                self.cache.popitem(last=False)
            
            self.cache[key] = value
    
    def clear(self) -> None:
        """Очистить кэш."""
        with self.lock:
            self.cache.clear()
    
    def size(self) -> int:
        """Размер кэша."""
        return len(self.cache)


class OptimizedCommentsStorage:
    """
    Оптимизированное хранилище комментариев с минимальным использованием памяти.
    
    Архитектура:
    1. SQLite для постоянного хранения всех комментариев
    2. LRU кэш для часто используемых комментариев в памяти
    3. Автоматическая очистка при достижении лимитов памяти
    """
    
    def __init__(self, 
                 db_path: str = "comments_cache.db",
                 memory_limit_mb: int = 100,
                 lru_cache_size: int = 1000):
        """
        Инициализация оптимизированного хранилища.
        
        Args:
            db_path: Путь к SQLite базе данных
            memory_limit_mb: Лимит памяти для кэша в мегабайтах
            lru_cache_size: Размер LRU кэша (количество постов)
        """
        self.db_path = db_path
        self.memory_limit_mb = memory_limit_mb
        self.logger = logging.getLogger('telegram_copier.optimized_cache')
        
        # Инициализация компонентов
        self.lru_cache = LRUCache(lru_cache_size)
        self.connection = None
        
        # Статистика
        self.stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'db_reads': 0,
            'db_writes': 0,
            'memory_cleanups': 0
        }
        
        self._init_database()
        self.logger.info(f"🚀 Инициализировано оптимизированное хранилище комментариев")
        self.logger.info(f"   💾 База данных: {db_path}")
        self.logger.info(f"   🧠 Лимит памяти: {memory_limit_mb} MB")
        self.logger.info(f"   📦 Размер LRU кэша: {lru_cache_size} постов")
    
    def _init_database(self):
        """Инициализация SQLite базы данных."""
        try:
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.connection.execute("""
                CREATE TABLE IF NOT EXISTS comments (
                    post_id INTEGER PRIMARY KEY,
                    comments_data BLOB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    access_count INTEGER DEFAULT 0,
                    last_access TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Индекс для быстрого поиска
            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_post_id ON comments(post_id)
            """)
            
            # Индекс для очистки старых данных
            self.connection.execute("""
                CREATE INDEX IF NOT EXISTS idx_last_access ON comments(last_access)
            """)
            
            self.connection.commit()
            self.logger.info("✅ База данных инициализирована")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации базы данных: {e}")
            raise
    
    async def store_comments_batch(self, comments_by_post: Dict[int, List[Message]]) -> None:
        """
        Сохранить комментарии батчом в базу данных.
        
        Args:
            comments_by_post: Словарь {post_id: [comments]}
        """
        if not comments_by_post:
            return
        
        try:
            # Подготавливаем данные для вставки
            insert_data = []
            for post_id, comments in comments_by_post.items():
                # Конвертируем Telethon Message объекты в сериализуемые словари
                comments_dicts = []
                for comment in comments:
                    try:
                        comment_dict = message_to_dict(comment)
                        comments_dicts.append(comment_dict)
                    except Exception as e:
                        self.logger.warning(f"Не удалось сериализовать комментарий {getattr(comment, 'id', 'unknown')}: {e}")
                        continue
                
                if comments_dicts:  # Только если есть успешно сериализованные комментарии
                    # Сериализуем в JSON
                    comments_json = json.dumps(comments_dicts, ensure_ascii=False)
                    insert_data.append((post_id, comments_json))
            
            if insert_data:
                # Батчевая вставка
                self.connection.executemany("""
                    INSERT OR REPLACE INTO comments (post_id, comments_data) 
                    VALUES (?, ?)
                """, insert_data)
                
                self.connection.commit()
                self.stats['db_writes'] += len(insert_data)
                
                total_comments = sum(len(json.loads(data[1])) for data in insert_data)
                self.logger.info(f"💾 Сохранено {len(insert_data)} связей пост->комментарии в базу данных ({total_comments} комментариев)")
            else:
                self.logger.warning("⚠️ Нет данных для сохранения после сериализации")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения комментариев в базу: {e}")
            import traceback
            self.logger.error(f"Детали ошибки: {traceback.format_exc()}")
    
    async def get_comments_for_post(self, post_id: int) -> List[MessageProxy]:
        """
        Получить комментарии для поста с использованием многоуровневого кэширования.
        
        Args:
            post_id: ID поста канала
            
        Returns:
            Список MessageProxy объектов (совместимые с Message, но без ссылок на клиент)
        """
        # Уровень 1: Проверяем LRU кэш в памяти
        cached_comments = self.lru_cache.get(post_id)
        if cached_comments is not None:
            self.stats['cache_hits'] += 1
            self.logger.debug(f"💨 Кэш-попадание для поста {post_id}: {len(cached_comments)} комментариев")
            # Конвертируем словари в MessageProxy объекты
            return [dict_to_message_proxy(comment_dict) for comment_dict in cached_comments]
        
        # Уровень 2: Читаем из SQLite базы данных
        try:
            self.stats['cache_misses'] += 1
            self.stats['db_reads'] += 1
            
            cursor = self.connection.execute("""
                SELECT comments_data FROM comments 
                WHERE post_id = ?
            """, (post_id,))
            
            row = cursor.fetchone()
            if row:
                # Десериализуем комментарии из JSON
                comments_dicts = json.loads(row[0])
                
                # Добавляем в LRU кэш для будущих запросов (как словари)
                self.lru_cache.put(post_id, comments_dicts)
                
                # Обновляем статистику доступа
                self.connection.execute("""
                    UPDATE comments 
                    SET access_count = access_count + 1, last_access = CURRENT_TIMESTAMP
                    WHERE post_id = ?
                """, (post_id,))
                self.connection.commit()
                
                # Конвертируем в MessageProxy объекты для возврата
                message_proxies = [dict_to_message_proxy(comment_dict) for comment_dict in comments_dicts]
                
                self.logger.debug(f"💾 Загружено из БД для поста {post_id}: {len(message_proxies)} комментариев")
                return message_proxies
            else:
                # Комментариев нет
                empty_list = []
                self.lru_cache.put(post_id, empty_list)  # Кэшируем отсутствие комментариев
                return empty_list
                
        except Exception as e:
            self.logger.error(f"❌ Ошибка чтения комментариев для поста {post_id}: {e}")
            import traceback
            self.logger.error(f"Детали ошибки: {traceback.format_exc()}")
            return []
    
    async def preload_comments_optimized(self, sample_batch: List[Message], 
                                       discussion_groups: set) -> None:
        """
        Оптимизированная предзагрузка комментариев с контролем памяти.
        
        Args:
            sample_batch: Образец сообщений для анализа
            discussion_groups: Множество ID discussion groups
        """
        if not discussion_groups:
            self.logger.info("💬 Discussion groups не найдены")
            return
        
        total_comments = 0
        total_posts_with_comments = 0
        
        for discussion_group_id in discussion_groups:
            try:
                self.logger.info(f"📥 Загружаем комментарии из discussion group {discussion_group_id}")
                
                # Получаем комментарии батчами для экономии памяти
                comments_batch = {}
                batch_size = 500  # Обрабатываем по 500 комментариев за раз
                
                # Здесь должна быть логика получения комментариев из discussion group
                # Пока используем заглушку - в реальности это будет integration с основным кодом
                
                await self.store_comments_batch(comments_batch)
                total_comments += sum(len(comments) for comments in comments_batch.values())
                total_posts_with_comments += len(comments_batch)
                
                # Проверяем использование памяти после каждой группы
                await self._check_memory_usage()
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка загрузки из discussion group {discussion_group_id}: {e}")
        
        self.logger.info(f"🎯 ОПТИМИЗИРОВАННАЯ ПРЕДЗАГРУЗКА ЗАВЕРШЕНА:")
        self.logger.info(f"   📊 Загружено {total_comments} комментариев")
        self.logger.info(f"   📊 Для {total_posts_with_comments} постов")
        self.logger.info(f"   💾 Данные сохранены в базе данных")
    
    async def _check_memory_usage(self) -> None:
        """Проверка использования памяти и принудительная очистка при необходимости."""
        try:
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            if memory_mb > self.memory_limit_mb:
                self.logger.warning(f"⚠️ Превышен лимит памяти: {memory_mb:.1f} MB > {self.memory_limit_mb} MB")
                await self._cleanup_memory()
                self.stats['memory_cleanups'] += 1
                
            self.logger.debug(f"🧠 Использование памяти: {memory_mb:.1f} MB")
            
        except Exception as e:
            self.logger.warning(f"Не удалось проверить использование памяти: {e}")
    
    async def _cleanup_memory(self) -> None:
        """Принудительная очистка памяти."""
        # Очищаем LRU кэш
        old_cache_size = self.lru_cache.size()
        self.lru_cache.clear()
        
        # Принудительная сборка мусора
        import gc
        collected = gc.collect()
        
        self.logger.info(f"🧹 Очистка памяти: освобожден кэш ({old_cache_size} элементов), "
                        f"собрано {collected} объектов")
    
    def get_memory_stats(self) -> MemoryStats:
        """Получить статистику использования памяти."""
        try:
            # Размер процесса в памяти
            process = psutil.Process()
            total_memory_mb = process.memory_info().rss / 1024 / 1024
            
            # Размер базы данных
            db_size_mb = 0
            if os.path.exists(self.db_path):
                db_size_mb = os.path.getsize(self.db_path) / 1024 / 1024
            
            # Эффективность кэша
            total_requests = self.stats['cache_hits'] + self.stats['cache_misses']
            efficiency_ratio = self.stats['cache_hits'] / total_requests if total_requests > 0 else 0
            
            return MemoryStats(
                total_memory_mb=total_memory_mb,
                cache_size=self.lru_cache.size(),
                database_size_mb=db_size_mb,
                efficiency_ratio=efficiency_ratio
            )
            
        except Exception as e:
            self.logger.error(f"Ошибка получения статистики памяти: {e}")
            return MemoryStats(0, 0, 0, 0)
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Получить статистику производительности."""
        return {
            'cache_hits': self.stats['cache_hits'],
            'cache_misses': self.stats['cache_misses'],
            'db_reads': self.stats['db_reads'],
            'db_writes': self.stats['db_writes'],
            'memory_cleanups': self.stats['memory_cleanups'],
            'cache_hit_ratio': (
                self.stats['cache_hits'] / 
                (self.stats['cache_hits'] + self.stats['cache_misses'])
                if (self.stats['cache_hits'] + self.stats['cache_misses']) > 0 else 0
            )
        }
    
    def cleanup_old_data(self, days_old: int = 7) -> None:
        """
        Очистка старых данных из базы.
        
        Args:
            days_old: Удалить данные старше указанного количества дней
        """
        try:
            cursor = self.connection.execute("""
                DELETE FROM comments 
                WHERE last_access < datetime('now', '-{} days')
            """.format(days_old))
            
            deleted_count = cursor.rowcount
            self.connection.commit()
            
            if deleted_count > 0:
                self.logger.info(f"🧹 Удалено {deleted_count} старых записей из базы данных")
                
        except Exception as e:
            self.logger.error(f"Ошибка очистки старых данных: {e}")
    
    def close(self) -> None:
        """Закрытие соединения с базой данных."""
        if self.connection:
            self.connection.close()
            self.logger.info("🔒 Соединение с базой данных закрыто")


# Интеграционный класс для замены текущего кэширования
class OptimizedTelegramCopier:
    """
    Пример интеграции оптимизированного хранилища в основной класс.
    """
    
    def __init__(self, *args, **kwargs):
        # Инициализация как в оригинальном классе
        # ...
        
        # Заменяем in-memory кэш на оптимизированный
        self.optimized_storage = OptimizedCommentsStorage(
            memory_limit_mb=kwargs.get('memory_limit_mb', 100),
            lru_cache_size=kwargs.get('lru_cache_size', 1000)
        )
        
        # Удаляем старые переменные памяти
        # self.comments_cache = {}  # Больше не нужно!
        # self.comments_cache_loaded = False  # Больше не нужно!
    
    async def get_comments_from_cache(self, message) -> List[Message]:
        """Получение комментариев из оптимизированного хранилища."""
        return await self.optimized_storage.get_comments_for_post(message.id)
    
    async def preload_all_comments_cache(self, sample_batch) -> None:
        """Оптимизированная предзагрузка комментариев."""
        # Логика определения discussion groups остается той же
        discussion_groups = set()
        # ... код определения discussion groups ...
        
        # Используем оптимизированную предзагрузку
        await self.optimized_storage.preload_comments_optimized(
            sample_batch, discussion_groups
        )
    
    def cleanup_temp_files(self) -> None:
        """Дополнительная очистка."""
        # Очистка старых данных
        self.optimized_storage.cleanup_old_data(days_old=7)
        
        # Статистика производительности
        stats = self.optimized_storage.get_performance_stats()
        memory_stats = self.optimized_storage.get_memory_stats()
        
        self.logger.info(f"📊 Статистика оптимизированного кэша:")
        self.logger.info(f"   💨 Cache hit ratio: {stats['cache_hit_ratio']:.1%}")
        self.logger.info(f"   💾 Память: {memory_stats.total_memory_mb:.1f} MB")
        self.logger.info(f"   🗄️ База данных: {memory_stats.database_size_mb:.1f} MB")
        
        # Закрываем соединение
        self.optimized_storage.close()