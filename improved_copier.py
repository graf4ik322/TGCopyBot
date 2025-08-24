"""
Улучшенный модуль копирования с правильной обработкой комментариев и альбомов.
Использует персистентное хранилище и правильную работу с медиа.
"""

import asyncio
import logging
from typing import List, Optional, Dict, Any, Set
from telethon import TelegramClient
from telethon.tl.types import Message, MessageMediaPhoto, MessageMediaDocument
from telethon.errors import FloodWaitError, PeerFloodError, MediaInvalidError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetHistoryRequest

from database_manager import DatabaseManager
from utils import RateLimiter, handle_flood_wait, ProgressTracker


class ImprovedTelegramCopier:
    """Улучшенный копировщик с правильной обработкой всех типов контента."""
    
    def __init__(self, client: TelegramClient, source_group_id: str, target_group_id: str,
                 rate_limiter: RateLimiter, dry_run: bool = False, 
                 database_path: str = "telegram_copier.db"):
        """
        Инициализация улучшенного копировщика.
        
        Args:
            client: Авторизованный Telegram клиент
            source_group_id: ID или username исходной группы/канала
            target_group_id: ID или username целевой группы/канала
            rate_limiter: Ограничитель скорости отправки
            dry_run: Режим симуляции без реальной отправки
            database_path: Путь к файлу базы данных
        """
        self.client = client
        self.source_group_id = source_group_id
        self.target_group_id = target_group_id
        self.rate_limiter = rate_limiter
        self.dry_run = dry_run
        
        self.logger = logging.getLogger('telegram_copier.improved')
        self.db = DatabaseManager(database_path)
        
        # Кэш для групп обсуждений
        self.discussion_groups_cache: Set[str] = set()
        
        # Энтити для работы
        self.source_entity = None
        self.target_entity = None
        
        # Статистика
        self.copied_messages = 0
        self.copied_comments = 0
        self.failed_messages = 0
        self.failed_comments = 0
    
    async def initialize(self):
        """Инициализация энтити и проверка доступов."""
        try:
            # Получаем энтити
            self.source_entity = await self.client.get_entity(self.source_group_id)
            self.target_entity = await self.client.get_entity(self.target_group_id)
            
            self.logger.info(f"✅ Инициализация завершена:")
            self.logger.info(f"   Источник: {getattr(self.source_entity, 'title', self.source_group_id)}")
            self.logger.info(f"   Цель: {getattr(self.target_entity, 'title', self.target_group_id)}")
            
            # Проверяем права доступа
            await self._check_permissions()
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка инициализации: {e}")
            raise
    
    async def _check_permissions(self):
        """Проверка прав доступа к каналам."""
        try:
            # Проверяем чтение из источника
            async for message in self.client.iter_messages(self.source_entity, limit=1):
                break
            else:
                self.logger.warning("⚠️ Исходный канал пуст или нет доступа к сообщениям")
            
            # Проверяем запись в цель (только если не dry_run)
            if not self.dry_run:
                test_message = await self.client.send_message(
                    self.target_entity, 
                    "🔧 Тест доступа (будет удален)"
                )
                await self.client.delete_messages(self.target_entity, test_message)
                self.logger.info("✅ Права записи в целевой канал подтверждены")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка проверки прав: {e}")
            raise
    
    async def full_scan_and_copy(self):
        """
        Полное сканирование и копирование с персистентным хранением.
        Выполняет сканирование только если оно не было завершено ранее.
        """
        source_channel_id = str(self.source_entity.id)
        
        # Проверяем, завершено ли полное сканирование
        if self.db.is_full_scan_completed(source_channel_id):
            self.logger.info("📚 Полное сканирование уже завершено, переходим к копированию")
            await self._copy_from_database()
        else:
            self.logger.info("🔄 Начинаем полное сканирование канала")
            await self._perform_full_scan()
            await self._copy_from_database()
    
    async def _perform_full_scan(self):
        """Выполнение полного сканирования канала с сохранением в БД."""
        source_channel_id = str(self.source_entity.id)
        
        try:
            # ЭТАП 1: Сканируем все сообщения
            self.logger.info("📥 ЭТАП 1: Сканирование всех сообщений...")
            
            message_count = 0
            progress_tracker = ProgressTracker(target_count=0, log_interval=1000)
            
            async for message in self.client.iter_messages(self.source_entity, limit=None):
                # Сохраняем сообщение в БД
                success = self.db.save_source_message(message, source_channel_id)
                if success:
                    message_count += 1
                    
                    # Прогресс
                    if message_count % 1000 == 0:
                        self.logger.info(f"   📥 Обработано {message_count} сообщений...")
                        
                        # Обновляем состояние
                        self.db.update_copy_state(
                            source_channel_id,
                            total_messages=message_count,
                            last_processed_message_id=message.id
                        )
            
            self.logger.info(f"✅ ЭТАП 1 завершен: сохранено {message_count} сообщений")
            
            # ЭТАП 2: Сканируем комментарии
            await self._scan_comments(source_channel_id)
            
            # Помечаем полное сканирование как завершенное
            self.db.update_copy_state(
                source_channel_id,
                total_messages=message_count,
                metadata={'full_scan_completed': True}
            )
            
            self.logger.info("🎯 Полное сканирование завершено")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка полного сканирования: {e}")
            raise
    
    async def _scan_comments(self, source_channel_id: str):
        """Сканирование всех комментариев в связанных группах обсуждений."""
        self.logger.info("💬 ЭТАП 2: Сканирование комментариев...")
        
        try:
            # Находим все группы обсуждений
            discussion_groups = await self._find_discussion_groups()
            
            if not discussion_groups:
                self.logger.info("💬 Групп обсуждений не найдено")
                return
            
            total_comments = 0
            
            for discussion_group_id in discussion_groups:
                self.logger.info(f"💬 Сканируем группу обсуждений: {discussion_group_id}")
                
                try:
                    discussion_entity = await self.client.get_entity(int(discussion_group_id))
                    comment_count = 0
                    
                    # Сканируем все сообщения в группе обсуждений
                    async for comment in self.client.iter_messages(discussion_entity, limit=None):
                        # Определяем, к какому посту относится комментарий
                        post_id = await self._find_post_for_comment(comment, source_channel_id)
                        
                        if post_id:
                            # Сохраняем комментарий
                            success = self.db.save_comment(
                                comment, post_id, discussion_group_id
                            )
                            if success:
                                comment_count += 1
                                total_comments += 1
                        
                        if comment_count % 500 == 0 and comment_count > 0:
                            self.logger.info(f"   💬 Обработано {comment_count} комментариев из группы {discussion_group_id}")
                    
                    self.logger.info(f"✅ Группа {discussion_group_id}: сохранено {comment_count} комментариев")
                    
                except Exception as e:
                    self.logger.error(f"❌ Ошибка сканирования группы {discussion_group_id}: {e}")
            
            self.logger.info(f"✅ ЭТАП 2 завершен: сохранено {total_comments} комментариев")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка сканирования комментариев: {e}")
    
    async def _find_discussion_groups(self) -> Set[str]:
        """Поиск всех групп обсуждений, связанных с каналом."""
        discussion_groups = set()
        
        try:
            # Метод 1: Получаем основную группу обсуждений канала
            full_channel = await self.client(GetFullChannelRequest(self.source_entity))
            linked_chat_id = getattr(full_channel.full_chat, 'linked_chat_id', None)
            
            if linked_chat_id:
                discussion_groups.add(str(linked_chat_id))
                self.logger.info(f"💬 Найдена основная группа обсуждений: {linked_chat_id}")
            
            # Метод 2: Сканируем сообщения на предмет форвардов из других групп
            sample_count = 0
            async for message in self.client.iter_messages(self.source_entity, limit=100):
                sample_count += 1
                
                # Проверяем форварды
                if hasattr(message, 'fwd_from') and message.fwd_from:
                    if hasattr(message.fwd_from, 'from_id'):
                        from_id = message.fwd_from.from_id
                        if hasattr(from_id, 'channel_id'):
                            potential_group = str(from_id.channel_id)
                            if potential_group not in discussion_groups:
                                # Проверяем, что это действительно группа обсуждений
                                try:
                                    entity = await self.client.get_entity(int(potential_group))
                                    if hasattr(entity, 'megagroup') and entity.megagroup:
                                        discussion_groups.add(potential_group)
                                        self.logger.info(f"💬 Найдена дополнительная группа: {potential_group}")
                                except Exception:
                                    pass  # Игнорируем недоступные группы
            
            return discussion_groups
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка поиска групп обсуждений: {e}")
            return set()
    
    async def _find_post_for_comment(self, comment: Message, source_channel_id: str) -> Optional[int]:
        """
        Определение, к какому посту канала относится комментарий.
        
        Args:
            comment: Комментарий из группы обсуждений
            source_channel_id: ID исходного канала
            
        Returns:
            ID поста канала или None
        """
        try:
            # Метод 1: Прямая проверка reply_to_msg_id
            if hasattr(comment, 'reply_to_msg_id') and comment.reply_to_msg_id:
                return comment.reply_to_msg_id
            
            # Метод 2: Проверка forward_header
            if hasattr(comment, 'fwd_from') and comment.fwd_from:
                if hasattr(comment.fwd_from, 'channel_post') and comment.fwd_from.channel_post:
                    return comment.fwd_from.channel_post
            
            # Метод 3: Проверка по ID (часто ID совпадают)
            if hasattr(comment, 'id'):
                # Проверяем, существует ли сообщение с таким ID в нашем канале
                try:
                    test_message = await self.client.get_messages(
                        self.source_entity, ids=comment.id
                    )
                    if test_message and not test_message.empty:
                        return comment.id
                except Exception:
                    pass
            
            return None
            
        except Exception as e:
            self.logger.debug(f"Ошибка определения поста для комментария {comment.id}: {e}")
            return None
    
    async def _copy_from_database(self):
        """Копирование сообщений из базы данных в хронологическом порядке."""
        source_channel_id = str(self.source_entity.id)
        
        self.logger.info("📋 Начинаем копирование из базы данных...")
        
        # Получаем статистику
        stats = self.db.get_statistics(source_channel_id)
        self.logger.info(f"📊 Статистика базы данных:")
        self.logger.info(f"   📨 Всего сообщений: {stats.get('total_messages', 0)}")
        self.logger.info(f"   ✅ Обработано: {stats.get('processed_messages', 0)}")
        self.logger.info(f"   💬 Комментариев: {stats.get('total_comments', 0)}")
        self.logger.info(f"   📷 Альбомов: {stats.get('albums_count', 0)}")
        
        # Получаем необработанные сообщения в хронологическом порядке
        unprocessed_messages = self.db.get_unprocessed_messages(
            source_channel_id, 
            chronological_order=True
        )
        
        if not unprocessed_messages:
            self.logger.info("🎯 Все сообщения уже обработаны!")
            return
        
        self.logger.info(f"🔄 Копируем {len(unprocessed_messages)} необработанных сообщений...")
        
        # Группируем сообщения по альбомам
        albums, single_messages = self._group_messages_by_albums(unprocessed_messages)
        
        # Создаем общий список для хронологической обработки
        all_items = []
        
        # Добавляем альбомы
        for grouped_id, album_messages in albums.items():
            # Сортируем сообщения альбома по дате
            album_messages.sort(key=lambda m: m.get('date_posted', ''))
            all_items.append({
                'type': 'album',
                'date': album_messages[0].get('date_posted', ''),
                'data': album_messages
            })
        
        # Добавляем одиночные сообщения
        for message in single_messages:
            all_items.append({
                'type': 'message',
                'date': message.get('date_posted', ''),
                'data': message
            })
        
        # Сортируем все по дате
        all_items.sort(key=lambda item: item['date'] or '')
        
        # Обрабатываем в хронологическом порядке
        progress_tracker = ProgressTracker(len(all_items), log_interval=50)
        
        for item in all_items:
            try:
                if item['type'] == 'album':
                    success = await self._copy_album_from_db(item['data'])
                else:
                    success = await self._copy_single_message_from_db(item['data'])
                
                if success:
                    self.copied_messages += 1
                else:
                    self.failed_messages += 1
                
                # Копируем комментарии для сообщения/альбома
                if item['type'] == 'message':
                    await self._copy_comments_for_message(item['data']['id'])
                else:
                    # Для альбома копируем комментарии первого сообщения
                    if item['data']:
                        await self._copy_comments_for_message(item['data'][0]['id'])
                
                # Соблюдаем rate limit
                if not self.dry_run:
                    await self.rate_limiter.wait_if_needed()
                    self.rate_limiter.record_message_sent()
                
                progress_tracker.update(1)
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка копирования элемента: {e}")
                self.failed_messages += 1
        
        # Финальная статистика
        self.logger.info(f"🎉 Копирование завершено!")
        self.logger.info(f"   ✅ Скопировано сообщений: {self.copied_messages}")
        self.logger.info(f"   💬 Скопировано комментариев: {self.copied_comments}")
        self.logger.info(f"   ❌ Ошибок: {self.failed_messages + self.failed_comments}")
    
    def _group_messages_by_albums(self, messages: List[Dict[str, Any]]) -> tuple:
        """
        Группировка сообщений по альбомам.
        
        Args:
            messages: Список сообщений из БД
            
        Returns:
            Кортеж (albums, single_messages)
        """
        albums = {}
        single_messages = []
        
        for message in messages:
            grouped_id = message.get('grouped_id')
            if grouped_id:
                if grouped_id not in albums:
                    albums[grouped_id] = []
                albums[grouped_id].append(message)
            else:
                single_messages.append(message)
        
        return albums, single_messages
    
    async def _copy_single_message_from_db(self, message_data: Dict[str, Any]) -> bool:
        """
        Копирование одиночного сообщения из данных БД.
        
        Args:
            message_data: Данные сообщения из БД
            
        Returns:
            True если копирование успешно
        """
        try:
            message_text = message_data.get('message_text', '')
            media_type = message_data.get('media_type')
            media_info = message_data.get('media_info', {})
            
            if self.dry_run:
                self.logger.info(f"🔧 DRY RUN: Копирование сообщения {message_data['id']}")
                # Помечаем как обработанное
                self.db.mark_message_copied(
                    message_data['id'], 999999,  # Фиктивный target_id для dry_run
                    str(self.source_entity.id), str(self.target_entity.id)
                )
                return True
            
            sent_message = None
            
            if media_type and media_info:
                # Восстанавливаем исходное сообщение для получения медиа
                try:
                    original_message = await self.client.get_messages(
                        self.source_entity, ids=message_data['id']
                    )
                    
                    if original_message and not original_message.empty and original_message.media:
                        # Отправляем с медиа
                        sent_message = await self.client.send_file(
                            self.target_entity,
                            original_message.media,
                            caption=message_text,
                            formatting_entities=self._restore_entities(message_data.get('entities', []))
                        )
                    else:
                        # Медиа недоступно, отправляем как текст
                        sent_message = await self.client.send_message(
                            self.target_entity,
                            message_text,
                            formatting_entities=self._restore_entities(message_data.get('entities', []))
                        )
                
                except Exception as media_error:
                    self.logger.warning(f"⚠️ Медиа недоступно для сообщения {message_data['id']}, отправляем как текст: {media_error}")
                    # Отправляем как текст
                    if message_text.strip():
                        sent_message = await self.client.send_message(
                            self.target_entity,
                            message_text,
                            formatting_entities=self._restore_entities(message_data.get('entities', []))
                        )
            else:
                # Текстовое сообщение
                if message_text.strip():
                    sent_message = await self.client.send_message(
                        self.target_entity,
                        message_text,
                        formatting_entities=self._restore_entities(message_data.get('entities', []))
                    )
            
            if sent_message:
                # Помечаем как скопированное
                self.db.mark_message_copied(
                    message_data['id'], sent_message.id,
                    str(self.source_entity.id), str(self.target_entity.id)
                )
                self.logger.debug(f"✅ Сообщение {message_data['id']} -> {sent_message.id}")
                return True
            else:
                self.logger.warning(f"⚠️ Пустое сообщение {message_data['id']} пропущено")
                # Помечаем как обработанное
                self.db.mark_message_copied(
                    message_data['id'], 0,  # 0 для пропущенных
                    str(self.source_entity.id), str(self.target_entity.id)
                )
                return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования сообщения {message_data['id']}: {e}")
            return False
    
    async def _copy_album_from_db(self, album_messages: List[Dict[str, Any]]) -> bool:
        """
        Копирование альбома из данных БД.
        
        Args:
            album_messages: Список сообщений альбома из БД
            
        Returns:
            True если копирование успешно
        """
        try:
            if not album_messages:
                return False
            
            if self.dry_run:
                self.logger.info(f"🔧 DRY RUN: Копирование альбома из {len(album_messages)} сообщений")
                for msg_data in album_messages:
                    self.db.mark_message_copied(
                        msg_data['id'], 999999,
                        str(self.source_entity.id), str(self.target_entity.id),
                        'album'
                    )
                return True
            
            # Получаем исходные сообщения с медиа
            media_files = []
            caption = ""
            entities = []
            original_ids = [msg['id'] for msg in album_messages]
            
            try:
                original_messages = await self.client.get_messages(
                    self.source_entity, ids=original_ids
                )
                
                for original_msg in original_messages:
                    if hasattr(original_msg, 'media') and original_msg.media:
                        media_files.append(original_msg.media)
                
                # Берем текст из первого сообщения
                first_message = album_messages[0]
                caption = first_message.get('message_text', '')
                entities = self._restore_entities(first_message.get('entities', []))
                
            except Exception as e:
                self.logger.error(f"❌ Ошибка получения исходных сообщений альбома: {e}")
                return False
            
            if not media_files:
                self.logger.warning(f"⚠️ Альбом не содержит доступных медиа файлов")
                # Отправляем как текст, если есть
                if caption.strip():
                    sent_message = await self.client.send_message(
                        self.target_entity, caption, formatting_entities=entities
                    )
                    # Помечаем все сообщения альбома как обработанные
                    for msg_data in album_messages:
                        self.db.mark_message_copied(
                            msg_data['id'], sent_message.id,
                            str(self.source_entity.id), str(self.target_entity.id),
                            'album'
                        )
                    return True
                return False
            
            # Отправляем альбом
            sent_messages = await self.client.send_file(
                self.target_entity,
                media_files,
                caption=caption,
                formatting_entities=entities
            )
            
            # Помечаем все сообщения альбома как скопированные
            if isinstance(sent_messages, list):
                for i, msg_data in enumerate(album_messages):
                    target_id = sent_messages[i].id if i < len(sent_messages) else sent_messages[0].id
                    self.db.mark_message_copied(
                        msg_data['id'], target_id,
                        str(self.source_entity.id), str(self.target_entity.id),
                        'album'
                    )
            else:
                # Одно сообщение для всего альбома
                for msg_data in album_messages:
                    self.db.mark_message_copied(
                        msg_data['id'], sent_messages.id,
                        str(self.source_entity.id), str(self.target_entity.id),
                        'album'
                    )
            
            self.logger.debug(f"✅ Альбом из {len(album_messages)} сообщений скопирован")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования альбома: {e}")
            return False
    
    async def _copy_comments_for_message(self, post_id: int):
        """
        Копирование всех комментариев для сообщения.
        
        Args:
            post_id: ID поста канала
        """
        try:
            comments = self.db.get_comments_for_post(post_id)
            if not comments:
                return
            
            self.logger.debug(f"💬 Копируем {len(comments)} комментариев для поста {post_id}")
            
            for comment_data in comments:
                try:
                    success = await self._copy_single_comment_from_db(comment_data)
                    if success:
                        self.copied_comments += 1
                    else:
                        self.failed_comments += 1
                    
                    # Rate limit для комментариев
                    if not self.dry_run:
                        await self.rate_limiter.wait_if_needed()
                        self.rate_limiter.record_message_sent()
                        
                except Exception as e:
                    self.logger.error(f"❌ Ошибка копирования комментария {comment_data['id']}: {e}")
                    self.failed_comments += 1
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования комментариев для поста {post_id}: {e}")
    
    async def _copy_single_comment_from_db(self, comment_data: Dict[str, Any]) -> bool:
        """
        Копирование одиночного комментария из данных БД.
        
        Args:
            comment_data: Данные комментария из БД
            
        Returns:
            True если копирование успешно
        """
        try:
            comment_text = comment_data.get('comment_text', '')
            media_type = comment_data.get('media_type')
            
            if not comment_text.strip() and not media_type:
                # Пустой комментарий, помечаем как обработанный
                self.db.connection.execute("""
                    UPDATE comments 
                    SET processed = TRUE, processed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (comment_data['id'],))
                self.db.connection.commit()
                return True
            
            if self.dry_run:
                self.logger.debug(f"🔧 DRY RUN: Копирование комментария {comment_data['id']}")
                self.db.connection.execute("""
                    UPDATE comments 
                    SET processed = TRUE, processed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (comment_data['id'],))
                self.db.connection.commit()
                return True
            
            sent_message = None
            
            if media_type:
                # Пытаемся получить исходный комментарий с медиа
                try:
                    discussion_group_id = comment_data.get('discussion_group_id')
                    if discussion_group_id:
                        discussion_entity = await self.client.get_entity(int(discussion_group_id))
                        original_comment = await self.client.get_messages(
                            discussion_entity, ids=comment_data['id']
                        )
                        
                        if original_comment and not original_comment.empty and original_comment.media:
                            # Отправляем с медиа
                            sent_message = await self.client.send_file(
                                self.target_entity,
                                original_comment.media,
                                caption=comment_text
                            )
                        else:
                            # Медиа недоступно, отправляем как текст
                            if comment_text.strip():
                                sent_message = await self.client.send_message(
                                    self.target_entity, comment_text
                                )
                
                except Exception as media_error:
                    self.logger.warning(f"⚠️ Медиа недоступно для комментария {comment_data['id']}: {media_error}")
                    if comment_text.strip():
                        sent_message = await self.client.send_message(
                            self.target_entity, comment_text
                        )
            else:
                # Текстовый комментарий
                if comment_text.strip():
                    sent_message = await self.client.send_message(
                        self.target_entity, comment_text
                    )
            
            # Помечаем как обработанный
            self.db.connection.execute("""
                UPDATE comments 
                SET processed = TRUE, processed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (comment_data['id'],))
            self.db.connection.commit()
            
            if sent_message:
                self.logger.debug(f"✅ Комментарий {comment_data['id']} -> {sent_message.id}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка копирования комментария {comment_data['id']}: {e}")
            return False
    
    def _restore_entities(self, entities_data: List[Dict[str, Any]]) -> List:
        """
        Восстановление форматирования из данных БД.
        
        Args:
            entities_data: Список entities из БД
            
        Returns:
            Список восстановленных entities
        """
        # Упрощенная реализация - в реальном проекте нужно правильно
        # восстанавливать типы Telethon entities
        return []  # Пока возвращаем пустой список
    
    def get_statistics(self) -> Dict[str, Any]:
        """Получение статистики копирования."""
        source_channel_id = str(self.source_entity.id) if self.source_entity else ""
        db_stats = self.db.get_statistics(source_channel_id) if source_channel_id else {}
        
        return {
            'copied_messages': self.copied_messages,
            'copied_comments': self.copied_comments,
            'failed_messages': self.failed_messages,
            'failed_comments': self.failed_comments,
            'database_stats': db_stats
        }
    
    def close(self):
        """Закрытие ресурсов."""
        if self.db:
            self.db.close()
            self.logger.info("🔒 Ресурсы освобождены")